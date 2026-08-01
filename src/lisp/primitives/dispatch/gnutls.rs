use super::*;
use libloading::Library;
use std::ffi::{CStr, c_char, c_int, c_uint, c_void};
use zeroize::Zeroizing;

type AlgorithmList = unsafe extern "C" fn() -> *const c_int;
type AlgorithmName = unsafe extern "C" fn(c_int) -> *const c_char;
type AlgorithmUnsigned = unsafe extern "C" fn(c_int) -> c_uint;
type AlgorithmSize = unsafe extern "C" fn(c_int) -> usize;
type ErrorIsFatal = unsafe extern "C" fn(c_int) -> c_int;
type ErrorString = unsafe extern "C" fn(c_int) -> *const c_char;
type HmacInit = unsafe extern "C" fn(*mut *mut c_void, c_int, *const c_void, usize) -> c_int;
type HmacApply = unsafe extern "C" fn(*mut c_void, *const c_void, usize) -> c_int;
type HmacOutput = unsafe extern "C" fn(*mut c_void, *mut c_void);
type HmacDeinit = unsafe extern "C" fn(*mut c_void, *mut c_void);
type CipherInit =
    unsafe extern "C" fn(*mut *mut c_void, c_int, *const GnuTlsDatum, *const GnuTlsDatum) -> c_int;
type CipherSetIv = unsafe extern "C" fn(*mut c_void, *mut c_void, usize);
type CipherCrypt =
    unsafe extern "C" fn(*mut c_void, *const c_void, usize, *mut c_void, usize) -> c_int;
type CipherDeinit = unsafe extern "C" fn(*mut c_void);
type AeadInit = unsafe extern "C" fn(*mut *mut c_void, c_int, *const GnuTlsDatum) -> c_int;
type AeadCrypt = unsafe extern "C" fn(
    *mut c_void,
    *const c_void,
    usize,
    *const c_void,
    usize,
    usize,
    *const c_void,
    usize,
    *mut c_void,
    *mut usize,
) -> c_int;
type AeadDeinit = unsafe extern "C" fn(*mut c_void);

#[repr(C)]
struct GnuTlsDatum {
    data: *mut u8,
    size: c_uint,
}

unsafe extern "C" fn no_nonce_size(_: c_int) -> usize {
    0
}

#[derive(Clone, Copy)]
struct GnuTlsApi {
    cipher_list: AlgorithmList,
    cipher_name: AlgorithmName,
    cipher_tag_size: AlgorithmUnsigned,
    cipher_block_size: AlgorithmUnsigned,
    cipher_key_size: AlgorithmSize,
    cipher_iv_size: AlgorithmUnsigned,
    mac_list: AlgorithmList,
    mac_name: AlgorithmName,
    mac_length: AlgorithmUnsigned,
    mac_key_size: AlgorithmSize,
    mac_nonce_size: AlgorithmSize,
    hmac_init: HmacInit,
    hmac_apply: HmacApply,
    hmac_output: HmacOutput,
    hmac_deinit: HmacDeinit,
    cipher_init: CipherInit,
    cipher_set_iv: CipherSetIv,
    cipher_encrypt: CipherCrypt,
    cipher_decrypt: CipherCrypt,
    cipher_deinit: CipherDeinit,
    aead_init: Option<AeadInit>,
    aead_encrypt: Option<AeadCrypt>,
    aead_decrypt: Option<AeadCrypt>,
    aead_deinit: Option<AeadDeinit>,
    error_is_fatal: ErrorIsFatal,
    error_string: ErrorString,
}

struct GnuTlsLibrary {
    _library: Library,
    api: GnuTlsApi,
}

fn gnutls_load_error(message: impl Into<String>) -> LispError {
    LispError::Signal(format!("GnuTLS library is unavailable: {}", message.into()))
}

unsafe fn load_symbol<T: Copy>(library: &Library, name: &[u8]) -> Result<T, LispError> {
    // SAFETY: Each requested name is paired with the corresponding public
    // GnuTLS C signature.  Copying the function pointer is valid while the
    // owning library, retained by `GnuTlsLibrary`, remains loaded.
    unsafe { library.get::<T>(name) }
        .map(|symbol| *symbol)
        .map_err(|error| gnutls_load_error(error.to_string()))
}

unsafe fn load_optional_symbol<T: Copy>(library: &Library, name: &[u8]) -> Option<T> {
    // SAFETY: The caller supplies the public signature paired with `name`.
    unsafe { library.get::<T>(name) }.ok().map(|symbol| *symbol)
}

fn load_gnutls() -> Result<GnuTlsLibrary, LispError> {
    #[cfg(target_os = "macos")]
    let candidates = [
        "libgnutls.30.dylib",
        "libgnutls.dylib",
        "/opt/homebrew/opt/gnutls/lib/libgnutls.30.dylib",
        "/usr/local/opt/gnutls/lib/libgnutls.30.dylib",
    ];
    #[cfg(target_os = "linux")]
    let candidates = ["libgnutls.so.30", "libgnutls.so"];
    #[cfg(target_os = "windows")]
    let candidates = ["libgnutls-30.dll", "libgnutls.dll"];
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    let candidates = ["libgnutls.so.30", "libgnutls.so"];

    let mut last_error = None;
    for candidate in candidates {
        // SAFETY: Loading a named shared library is the intended libloading
        // boundary.  No initializer-owned state is exposed; all calls below
        // use GnuTLS's documented C ABI.
        let library = match unsafe { Library::new(candidate) } {
            Ok(library) => library,
            Err(error) => {
                last_error = Some(error.to_string());
                continue;
            }
        };
        // SAFETY: `load_symbol` documents and checks this public C ABI
        // boundary, and the library is moved into the returned owner.
        let api = unsafe {
            GnuTlsApi {
                cipher_list: load_symbol(&library, b"gnutls_cipher_list")?,
                cipher_name: load_symbol(&library, b"gnutls_cipher_get_name")?,
                cipher_tag_size: load_symbol(&library, b"gnutls_cipher_get_tag_size")?,
                cipher_block_size: load_symbol(&library, b"gnutls_cipher_get_block_size")?,
                cipher_key_size: load_symbol(&library, b"gnutls_cipher_get_key_size")?,
                cipher_iv_size: load_symbol(&library, b"gnutls_cipher_get_iv_size")?,
                mac_list: load_symbol(&library, b"gnutls_mac_list")?,
                mac_name: load_symbol(&library, b"gnutls_mac_get_name")?,
                mac_length: load_symbol(&library, b"gnutls_hmac_get_len")?,
                mac_key_size: load_symbol(&library, b"gnutls_mac_get_key_size")?,
                mac_nonce_size: load_symbol(&library, b"gnutls_mac_get_nonce_size")
                    .unwrap_or(no_nonce_size),
                hmac_init: load_symbol(&library, b"gnutls_hmac_init")?,
                hmac_apply: load_symbol(&library, b"gnutls_hmac")?,
                hmac_output: load_symbol(&library, b"gnutls_hmac_output")?,
                hmac_deinit: load_symbol(&library, b"gnutls_hmac_deinit")?,
                cipher_init: load_symbol(&library, b"gnutls_cipher_init")?,
                cipher_set_iv: load_symbol(&library, b"gnutls_cipher_set_iv")?,
                cipher_encrypt: load_symbol(&library, b"gnutls_cipher_encrypt2")?,
                cipher_decrypt: load_symbol(&library, b"gnutls_cipher_decrypt2")?,
                cipher_deinit: load_symbol(&library, b"gnutls_cipher_deinit")?,
                aead_init: load_optional_symbol(&library, b"gnutls_aead_cipher_init"),
                aead_encrypt: load_optional_symbol(&library, b"gnutls_aead_cipher_encrypt"),
                aead_decrypt: load_optional_symbol(&library, b"gnutls_aead_cipher_decrypt"),
                aead_deinit: load_optional_symbol(&library, b"gnutls_aead_cipher_deinit"),
                error_is_fatal: load_symbol(&library, b"gnutls_error_is_fatal")?,
                error_string: load_symbol(&library, b"gnutls_strerror")?,
            }
        };
        return Ok(GnuTlsLibrary {
            _library: library,
            api,
        });
    }
    Err(gnutls_load_error(
        last_error.unwrap_or_else(|| "no library candidates".into()),
    ))
}

fn c_string(pointer: *const c_char) -> Option<String> {
    if pointer.is_null() {
        return None;
    }
    // SAFETY: GnuTLS name and error APIs return a borrowed, NUL-terminated
    // string whose storage remains valid while the library is loaded.
    Some(
        unsafe { CStr::from_ptr(pointer) }
            .to_string_lossy()
            .into_owned(),
    )
}

fn algorithm_ids(list: AlgorithmList) -> Vec<c_int> {
    // SAFETY: GnuTLS returns a static, zero-terminated array of algorithm IDs.
    let mut current = unsafe { list() };
    let mut ids = Vec::new();
    if current.is_null() {
        return ids;
    }
    loop {
        // SAFETY: `current` starts at GnuTLS's static array and advances only
        // until its documented zero terminator.
        let id = unsafe { *current };
        if id == 0 {
            return ids;
        }
        ids.push(id);
        // SAFETY: The zero-terminated array contains the next readable slot.
        current = unsafe { current.add(1) };
    }
}

struct DigestSpec {
    name: &'static str,
    id: i64,
    algorithm: &'static str,
    length: i64,
}

// GnuTLS 3 enumerates these in the reverse order.  `gnutls-digests' conses
// each descriptor, producing this stable public order in GNU Emacs 30.2.
const DIGESTS: &[DigestSpec] = &[
    DigestSpec {
        name: "STREEBOG-512",
        id: 17,
        algorithm: "streebog-512",
        length: 64,
    },
    DigestSpec {
        name: "STREEBOG-256",
        id: 16,
        algorithm: "streebog-256",
        length: 32,
    },
    DigestSpec {
        name: "GOSTR341194",
        id: 15,
        algorithm: "gost94-cryptopro",
        length: 32,
    },
    DigestSpec {
        name: "MD5",
        id: 2,
        algorithm: "md5",
        length: 16,
    },
    DigestSpec {
        name: "SHA224",
        id: 9,
        algorithm: "sha224",
        length: 28,
    },
    DigestSpec {
        name: "SHA512",
        id: 8,
        algorithm: "sha512",
        length: 64,
    },
    DigestSpec {
        name: "SHA384",
        id: 7,
        algorithm: "sha384",
        length: 48,
    },
    DigestSpec {
        name: "SHA256",
        id: 6,
        algorithm: "sha256",
        length: 32,
    },
    DigestSpec {
        name: "SHA1",
        id: 3,
        algorithm: "sha1",
        length: 20,
    },
];

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "gnutls-asynchronous-parameters"
            | "gnutls-ciphers"
            | "gnutls-deinit"
            | "gnutls-digests"
            | "gnutls-error-fatalp"
            | "gnutls-error-string"
            | "gnutls-errorp"
            | "gnutls-get-initstage"
            | "gnutls-hash-digest"
            | "gnutls-hash-mac"
            | "gnutls-macs"
            | "gnutls-peer-status"
            | "gnutls-peer-status-warning-describe"
            | "gnutls-symmetric-decrypt"
            | "gnutls-symmetric-encrypt"
    )
}

fn descriptor(spec: &DigestSpec) -> Value {
    Value::list([
        Value::symbol(spec.name),
        Value::symbol(":digest-algorithm-id"),
        Value::Integer(spec.id),
        Value::symbol(":type"),
        Value::symbol("gnutls-digest-algorithm"),
        Value::symbol(":digest-algorithm-length"),
        Value::Integer(spec.length),
    ])
}

fn plist_integer(plist: &Value, property: &str) -> Option<i64> {
    let mut current = plist.clone();
    while let Value::Cons(key, rest) = current {
        let rest = rest.borrow().clone();
        let Value::Cons(value, tail) = rest else {
            return None;
        };
        if matches!(&*key.borrow(), Value::Symbol(name) if name == property) {
            return value.borrow().as_integer().ok();
        }
        current = tail.borrow().clone();
    }
    None
}

fn digest_spec(method: &Value) -> Option<&'static DigestSpec> {
    let id = match method {
        Value::Integer(id) => Some(*id),
        Value::String(_) | Value::StringObject(_) => {
            let name = string_text(method).ok()?;
            return DIGESTS.iter().find(|spec| spec.name == name);
        }
        Value::Symbol(name) => return DIGESTS.iter().find(|spec| spec.name == name),
        Value::Cons(..) => plist_integer(method, ":digest-algorithm-id"),
        _ => None,
    };
    DIGESTS.iter().find(|spec| Some(spec.id) == id)
}

fn invalid_digest_method(method: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("GnuTLS digest-method is invalid or not found"),
        method.clone(),
    ]))
}

fn cipher_catalog(library: &GnuTlsLibrary) -> Value {
    let api = library.api;
    let mut entries = algorithm_ids(api.cipher_list)
        .into_iter()
        .filter_map(|id| {
            // GnuTLS includes its NULL cipher in the raw list; GNU Emacs
            // deliberately omits it from the Lisp catalog.
            if id == 1 {
                return None;
            }
            // SAFETY: `id` came directly from `gnutls_cipher_list`.
            let name = c_string(unsafe { (api.cipher_name)(id) })?;
            // SAFETY: All size queries accept IDs returned by the cipher list.
            let (tag_size, block_size, key_size, iv_size) = unsafe {
                (
                    (api.cipher_tag_size)(id),
                    (api.cipher_block_size)(id),
                    (api.cipher_key_size)(id),
                    (api.cipher_iv_size)(id),
                )
            };
            Some(Value::list([
                Value::symbol(&name),
                Value::symbol(":cipher-id"),
                Value::Integer(i64::from(id)),
                Value::symbol(":type"),
                Value::symbol("gnutls-symmetric-cipher"),
                Value::symbol(":cipher-aead-capable"),
                if tag_size == 0 { Value::Nil } else { Value::T },
                Value::symbol(":cipher-tagsize"),
                Value::Integer(tag_size as i64),
                Value::symbol(":cipher-blocksize"),
                Value::Integer(block_size as i64),
                Value::symbol(":cipher-keysize"),
                Value::Integer(key_size as i64),
                Value::symbol(":cipher-ivsize"),
                Value::Integer(iv_size as i64),
            ]))
        })
        .collect::<Vec<_>>();
    // GNU conses each descriptor while traversing GnuTLS's forward list.
    entries.reverse();
    Value::list(entries)
}

fn mac_catalog(library: &GnuTlsLibrary) -> Value {
    let api = library.api;
    let mut entries = algorithm_ids(api.mac_list)
        .into_iter()
        .filter_map(|id| {
            // SAFETY: `id` came directly from `gnutls_mac_list`.
            let name = c_string(unsafe { (api.mac_name)(id) })?;
            // SAFETY: All size queries accept IDs returned by the MAC list.
            let (length, key_size, nonce_size) = unsafe {
                (
                    (api.mac_length)(id),
                    (api.mac_key_size)(id),
                    (api.mac_nonce_size)(id),
                )
            };
            Some(Value::list([
                Value::symbol(&name),
                Value::symbol(":mac-algorithm-id"),
                Value::Integer(i64::from(id)),
                Value::symbol(":type"),
                Value::symbol("gnutls-mac-algorithm"),
                Value::symbol(":mac-algorithm-length"),
                Value::Integer(length as i64),
                Value::symbol(":mac-algorithm-keysize"),
                Value::Integer(key_size as i64),
                Value::symbol(":mac-algorithm-noncesize"),
                Value::Integer(nonce_size as i64),
            ]))
        })
        .collect::<Vec<_>>();
    entries.reverse();
    Value::list(entries)
}

fn invalid_mac_method(method: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("GnuTLS MAC-method is invalid or not found"),
        method.clone(),
    ]))
}

fn mac_method_id(method: &Value, library: &GnuTlsLibrary) -> Result<c_int, LispError> {
    let normalized = match method {
        Value::String(_) | Value::StringObject(_) => Value::symbol(&string_text(method)?),
        method => method.clone(),
    };
    let id = match &normalized {
        Value::Integer(id) => c_int::try_from(*id).ok(),
        Value::Symbol(name) => algorithm_ids(library.api.mac_list).into_iter().find(|id| {
            // SAFETY: Every candidate came from `gnutls_mac_list`.
            c_string(unsafe { (library.api.mac_name)(*id) }).as_deref() == Some(name)
        }),
        Value::Cons(..) => {
            plist_integer(&normalized, ":mac-algorithm-id").and_then(|id| c_int::try_from(id).ok())
        }
        _ => None,
    }
    .ok_or_else(|| invalid_mac_method(&normalized))?;
    // SAFETY: GnuTLS's length query accepts every C enum value and reports
    // zero for unknown or non-HMAC methods.
    if unsafe { (library.api.mac_length)(id) } == 0 {
        return Err(invalid_mac_method(&normalized));
    }
    Ok(id)
}

fn invalid_cipher_method(method: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("GnuTLS cipher is invalid or not found"),
        method.clone(),
    ]))
}

fn cipher_method_id(method: &Value, library: &GnuTlsLibrary) -> Result<c_int, LispError> {
    let normalized = match method {
        Value::String(_) | Value::StringObject(_) => Value::symbol(&string_text(method)?),
        method => method.clone(),
    };
    let id = match &normalized {
        Value::Integer(id) => c_int::try_from(*id).ok(),
        Value::Symbol(name) => algorithm_ids(library.api.cipher_list)
            .into_iter()
            .find(|id| {
                // SAFETY: Every candidate came from `gnutls_cipher_list`.
                c_string(unsafe { (library.api.cipher_name)(*id) }).as_deref() == Some(name)
            }),
        Value::Cons(..) => {
            plist_integer(&normalized, ":cipher-id").and_then(|id| c_int::try_from(id).ok())
        }
        _ => None,
    }
    .ok_or_else(|| invalid_cipher_method(&normalized))?;
    // SAFETY: GnuTLS's size query accepts every C enum value and reports zero
    // for unknown or non-cipher methods.
    if unsafe { (library.api.cipher_key_size)(id) } == 0 {
        return Err(invalid_cipher_method(&normalized));
    }
    Ok(id)
}

fn require_crypto_input(value: &Value) -> Result<(), LispError> {
    if value.is_string() || matches!(value, Value::Buffer(..)) || value.is_cons() {
        Ok(())
    } else {
        Err(wrong_type_argument("consp", value.clone()))
    }
}

fn clear_crypto_key(value: &Value) {
    let source = if value.is_string() {
        value.clone()
    } else {
        safe_car(value)
    };
    if let Value::StringObject(state) = &source {
        let mut state = state.borrow_mut();
        state.text = "\0".repeat(state.text.len());
        state.props.clear();
        state.multibyte = false;
    }
}

fn gnutls_error_description(library: &GnuTlsLibrary, code: c_int) -> String {
    // SAFETY: GnuTLS accepts every C integer at its error-string boundary.
    c_string(unsafe { (library.api.error_string)(code) }).unwrap_or_else(|| "unknown".into())
}

fn gnutls_symmetric(
    interp: &mut Interpreter,
    args: &[Value],
    encrypting: bool,
) -> Result<Value, LispError> {
    require_crypto_input(&args[1])?;
    require_crypto_input(&args[3])?;
    require_crypto_input(&args[2])?;

    let library = load_gnutls()?;
    let method = cipher_method_id(&args[0], &library)?;
    // SAFETY: `method` was validated against the host cipher catalog.
    let method_name = c_string(unsafe { (library.api.cipher_name)(method) })
        .unwrap_or_else(|| method.to_string());
    let operation = if encrypting { "encrypt" } else { "decrypt" };

    let key = Zeroizing::new(digest_input_bytes(interp, &args[1])?);
    // SAFETY: `method` was validated against the host cipher catalog.
    let key_size = unsafe { (library.api.cipher_key_size)(method) };
    if key.len() != key_size {
        return Err(LispError::Signal(format!(
            "GnuTLS cipher {method_name}/{operation} key length {} is not equal to the required {key_size}",
            key.len()
        )));
    }

    let iv = digest_input_bytes(interp, &args[2])?;
    // SAFETY: `method` was validated against the host cipher catalog.
    let iv_size = unsafe { (library.api.cipher_iv_size)(method) } as usize;
    if iv.len() != iv_size {
        return Err(LispError::Signal(format!(
            "GnuTLS cipher {method_name}/{operation} IV length {} is not equal to the required {iv_size}",
            iv.len()
        )));
    }
    let actual_iv = bytes_to_shared_unibyte_value(&iv);
    let input = digest_input_bytes(interp, &args[3])?;
    // SAFETY: `method` was validated against the host cipher catalog.
    let tag_size = unsafe { (library.api.cipher_tag_size)(method) } as usize;
    let key_size = c_uint::try_from(key.len())
        .map_err(|_| LispError::Signal("GnuTLS cipher key is too large".into()))?;
    let key_datum = GnuTlsDatum {
        data: key.as_ptr().cast_mut(),
        size: key_size,
    };

    if tag_size > 0 {
        let (Some(init), Some(crypt), Some(deinit)) = (
            library.api.aead_init,
            if encrypting {
                library.api.aead_encrypt
            } else {
                library.api.aead_decrypt
            },
            library.api.aead_deinit,
        ) else {
            return Err(LispError::Signal(format!(
                "GnuTLS AEAD cipher {method} is invalid or not found"
            )));
        };
        let mut handle = std::ptr::null_mut();
        // SAFETY: The key datum borrows the validated key for this call.
        let result = unsafe { init(&mut handle, method, &key_datum) };
        if result < 0 {
            return Err(LispError::Signal(format!(
                "GnuTLS AEAD cipher {method_name}/{operation} initialization failed: {}",
                gnutls_error_description(&library, result)
            )));
        }

        let auth = if args.get(4).is_none_or(Value::is_nil) {
            Vec::new()
        } else {
            require_crypto_input(&args[4])?;
            match digest_input_bytes(interp, &args[4]) {
                Ok(auth) => auth,
                Err(error) => {
                    // SAFETY: Successful initialization returned a live handle.
                    unsafe { deinit(handle) };
                    return Err(error);
                }
            }
        };
        let mut output = Zeroizing::new(vec![0; input.len().saturating_add(tag_size)]);
        let mut output_length = output.len();
        // SAFETY: All pointers borrow live slices; GnuTLS writes at most the
        // supplied output capacity and updates `output_length`.
        let result = unsafe {
            crypt(
                handle,
                iv.as_ptr().cast(),
                iv.len(),
                auth.as_ptr().cast(),
                auth.len(),
                tag_size,
                input.as_ptr().cast(),
                input.len(),
                output.as_mut_ptr().cast(),
                &mut output_length,
            )
        };
        // SAFETY: Successful initialization returned a live handle.
        unsafe { deinit(handle) };
        if result < 0 {
            let action = if encrypting {
                "encryption"
            } else {
                "decryption"
            };
            return Err(LispError::Signal(format!(
                "GnuTLS AEAD cipher {method_name} {action} failed: {}",
                gnutls_error_description(&library, result)
            )));
        }
        output.truncate(output_length);
        clear_crypto_key(&args[1]);
        return Ok(Value::list([
            bytes_to_shared_unibyte_value(&output),
            actual_iv,
        ]));
    }

    // SAFETY: `method` was validated against the host cipher catalog.
    let block_size = unsafe { (library.api.cipher_block_size)(method) } as usize;
    if input.len() % block_size != 0 {
        return Err(LispError::Signal(format!(
            "GnuTLS cipher {method_name}/{operation} input block length {} is not a multiple of the required {block_size}",
            input.len()
        )));
    }

    let mut handle = std::ptr::null_mut();
    // SAFETY: The key datum borrows the validated key for initialization; GNU
    // sets the IV separately, so the optional IV datum is null here too.
    let result =
        unsafe { (library.api.cipher_init)(&mut handle, method, &key_datum, std::ptr::null()) };
    if result < 0 {
        return Err(LispError::Signal(format!(
            "GnuTLS cipher {method_name}/{operation} initialization failed: {}",
            gnutls_error_description(&library, result)
        )));
    }
    // SAFETY: The initialized handle accepts the exact advertised IV length.
    unsafe {
        (library.api.cipher_set_iv)(handle, iv.as_ptr().cast_mut().cast(), iv.len());
    }
    let mut output = Zeroizing::new(vec![0; input.len()]);
    let crypt = if encrypting {
        library.api.cipher_encrypt
    } else {
        library.api.cipher_decrypt
    };
    // SAFETY: Input and output are equally sized live slices, as required by
    // GnuTLS's no-padding `encrypt2`/`decrypt2` APIs.
    let result = unsafe {
        crypt(
            handle,
            input.as_ptr().cast(),
            input.len(),
            output.as_mut_ptr().cast(),
            output.len(),
        )
    };
    clear_crypto_key(&args[1]);
    // SAFETY: Successful initialization returned a live handle.
    unsafe { (library.api.cipher_deinit)(handle) };
    if result < 0 {
        let action = if encrypting {
            "encryption"
        } else {
            "decryption"
        };
        return Err(LispError::Signal(format!(
            "GnuTLS cipher {method_name} {action} failed: {}",
            gnutls_error_description(&library, result)
        )));
    }
    Ok(Value::list([
        bytes_to_shared_unibyte_value(&output),
        actual_iv,
    ]))
}

fn gnutls_error_code(interp: &Interpreter, error: &Value) -> Result<c_int, &'static str> {
    let resolved = match error {
        Value::Symbol(symbol) => match interp.get_symbol_property(symbol, "gnutls-code") {
            Some(code)
                if matches!(
                    code,
                    Value::Integer(_) | Value::BigInteger(_) | Value::Float(_)
                ) =>
            {
                code
            }
            _ => return Err("Symbol has no numeric gnutls-code property"),
        },
        error => error.clone(),
    };
    let Value::Integer(code) = resolved else {
        return Err("Not an error symbol or code");
    };
    c_int::try_from(code).map_err(|_| "Not an error symbol or code")
}

fn safe_car(value: &Value) -> Value {
    value
        .cons_values()
        .map(|(car, _)| car)
        .unwrap_or(Value::Nil)
}

fn safe_cdr(value: &Value) -> Value {
    value
        .cons_values()
        .map(|(_, cdr)| cdr)
        .unwrap_or(Value::Nil)
}

fn digest_input_bytes(interp: &mut Interpreter, input: &Value) -> Result<Vec<u8>, LispError> {
    if input.is_string() || matches!(input, Value::Buffer(..)) {
        return secure_hash_source_bytes(interp, input, None, None);
    }
    if !input.is_cons() {
        return Err(wrong_type_argument("consp", input.clone()));
    }

    let source = safe_car(input);
    let tail = safe_cdr(input);
    let start = safe_car(&tail);
    let end = safe_car(&safe_cdr(&tail));
    secure_hash_source_bytes(
        interp,
        &source,
        (!start.is_nil()).then_some(&start),
        (!end.is_nil()).then_some(&end),
    )
}

fn peer_status_warning_description(status: &str) -> Option<&'static str> {
    Some(match status {
        ":invalid" => "certificate could not be verified",
        ":revoked" => "certificate was revoked (CRL)",
        ":self-signed" => "certificate signer was not found (self-signed)",
        ":unknown-ca" => {
            "the certificate was signed by an unknown and therefore untrusted authority"
        }
        ":not-ca" => "certificate signer is not a CA",
        ":insecure" => "certificate was signed with an insecure algorithm",
        ":not-activated" => "certificate is not yet activated",
        ":expired" => "certificate has expired",
        ":no-host-match" => "certificate host does not match hostname",
        ":signature-failure" => "certificate signature could not be verified",
        ":revocation-data-superseded" => {
            "certificate revocation data are old and have been superseded"
        }
        ":revocation-data-issued-in-future" => {
            "certificate revocation data have a future issue date"
        }
        ":signer-constraints-failure" => "certificate signer constraints were violated",
        ":purpose-mismatch" => "certificate does not match the intended purpose",
        ":missing-ocsp-status" => {
            "certificate requires the server to send a OCSP certificate status, but no status was received"
        }
        ":invalid-ocsp-status" => "the received OCSP certificate status is invalid",
        _ => return None,
    })
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    match name {
        "gnutls-asynchronous-parameters" => {
            need_args(name, args, 2)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            interp.set_process_gnutls_boot_parameters(process_id, args[1].clone());
            Ok(Value::Nil)
        }
        "gnutls-ciphers" => {
            need_args(name, args, 0)?;
            Ok(cipher_catalog(&load_gnutls()?))
        }
        "gnutls-deinit" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            Ok(if interp.deinit_process_gnutls(process_id) == Some(true) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "gnutls-digests" => {
            need_args(name, args, 0)?;
            Ok(Value::list(DIGESTS.iter().map(descriptor)))
        }
        "gnutls-error-fatalp" => {
            need_args(name, args, 1)?;
            if args[0] == Value::T {
                return Ok(Value::Nil);
            }
            let code = gnutls_error_code(interp, &args[0])
                .map_err(|message| LispError::Signal(message.into()))?;
            let library = load_gnutls()?;
            // SAFETY: `code` is a C int, the complete input domain accepted by
            // GnuTLS's pure error classifier.
            Ok(if unsafe { (library.api.error_is_fatal)(code) } == 0 {
                Value::Nil
            } else {
                Value::T
            })
        }
        "gnutls-error-string" => {
            need_args(name, args, 1)?;
            if args[0] == Value::T {
                return Ok(Value::string("Not an error"));
            }
            let code = match gnutls_error_code(interp, &args[0]) {
                Ok(code) => code,
                Err(message) => return Ok(Value::string(message)),
            };
            let library = load_gnutls()?;
            // SAFETY: `code` is a C int, the complete input domain accepted by
            // GnuTLS's pure error-description API.
            let description = c_string(unsafe { (library.api.error_string)(code) })
                .unwrap_or_else(|| "unknown".into());
            Ok(Value::string(&description))
        }
        "gnutls-errorp" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::T)
                    || matches!(&args[0], Value::Symbol(symbol) if symbol == "gnutls-e-again")
                {
                    Value::Nil
                } else {
                    Value::T
                },
            )
        }
        "gnutls-get-initstage" => {
            need_args(name, args, 1)?;
            let process_id = interp.resolve_process_id(&args[0])?;
            Ok(Value::Integer(
                interp.process_gnutls_initstage(process_id).unwrap_or(0),
            ))
        }
        "gnutls-hash-digest" => {
            need_args(name, args, 2)?;
            let spec = digest_spec(&args[0]).ok_or_else(|| invalid_digest_method(&args[0]))?;
            let input = digest_input_bytes(interp, &args[1])?;
            let digest = secure_hash_digest(spec.algorithm, &input)?;
            Ok(bytes_to_shared_unibyte_value(&digest))
        }
        "gnutls-hash-mac" => {
            need_args(name, args, 3)?;
            require_crypto_input(&args[2])?;
            require_crypto_input(&args[1])?;
            let library = load_gnutls()?;
            let method = mac_method_id(&args[0], &library)?;
            let key = Zeroizing::new(digest_input_bytes(interp, &args[1])?);
            let mut handle = std::ptr::null_mut();
            // SAFETY: GnuTLS receives borrowed byte slices for the duration of
            // the call and initializes `handle` on success.
            let result = unsafe {
                (library.api.hmac_init)(&mut handle, method, key.as_ptr().cast(), key.len())
            };
            // SAFETY: `method` was validated against the host MAC catalog.
            let method_name = c_string(unsafe { (library.api.mac_name)(method) })
                .unwrap_or_else(|| method.to_string());
            if result < 0 {
                // SAFETY: Every GnuTLS error code is accepted by strerror.
                let error = c_string(unsafe { (library.api.error_string)(result) })
                    .unwrap_or_else(|| "unknown".into());
                return Err(LispError::Signal(format!(
                    "GnuTLS MAC {method_name} initialization failed: {error}"
                )));
            }

            let input = digest_input_bytes(interp, &args[2])?;
            // SAFETY: Successful initialization returned a live handle, and
            // the input slice remains borrowed for this call.
            let result =
                unsafe { (library.api.hmac_apply)(handle, input.as_ptr().cast(), input.len()) };
            clear_crypto_key(&args[1]);
            if result < 0 {
                // SAFETY: The initialized handle must be released exactly once.
                unsafe { (library.api.hmac_deinit)(handle, std::ptr::null_mut()) };
                // SAFETY: Every GnuTLS error code is accepted by strerror.
                let error = c_string(unsafe { (library.api.error_string)(result) })
                    .unwrap_or_else(|| "unknown".into());
                return Err(LispError::Signal(format!(
                    "GnuTLS MAC {method_name} application failed: {error}"
                )));
            }

            // SAFETY: The validated method has a nonzero output size.
            let mut digest = vec![0; unsafe { (library.api.mac_length)(method) } as usize];
            // SAFETY: The live handle writes exactly the method's advertised
            // digest length, then is deinitialized without a second output.
            unsafe {
                (library.api.hmac_output)(handle, digest.as_mut_ptr().cast());
                (library.api.hmac_deinit)(handle, std::ptr::null_mut());
            }
            Ok(bytes_to_shared_unibyte_value(&digest))
        }
        "gnutls-macs" => {
            need_args(name, args, 0)?;
            Ok(mac_catalog(&load_gnutls()?))
        }
        "gnutls-peer-status" => {
            need_args(name, args, 1)?;
            interp.resolve_process_id(&args[0])?;
            // Until `gnutls-boot` advances a process to READY, GNU returns nil
            // without attempting to inspect a certificate or session.
            Ok(Value::Nil)
        }
        "gnutls-peer-status-warning-describe" => {
            need_args(name, args, 1)?;
            let Value::Symbol(status) = &args[0] else {
                return Err(wrong_type_argument("symbolp", args[0].clone()));
            };
            Ok(peer_status_warning_description(status)
                .map(Value::string)
                .unwrap_or(Value::Nil))
        }
        "gnutls-symmetric-decrypt" | "gnutls-symmetric-encrypt" => {
            need_arg_range(name, args, 4, 5)?;
            gnutls_symmetric(interp, args, name == "gnutls-symmetric-encrypt")
        }
        _ => unreachable!("unhandled GnuTLS builtin {name}"),
    }
}
