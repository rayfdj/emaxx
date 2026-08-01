use super::*;
use libloading::Library;
use std::ffi::{CStr, c_char, c_int};

type AlgorithmList = unsafe extern "C" fn() -> *const c_int;
type AlgorithmName = unsafe extern "C" fn(c_int) -> *const c_char;
type AlgorithmSize = unsafe extern "C" fn(c_int) -> usize;
type ErrorIsFatal = unsafe extern "C" fn(c_int) -> c_int;
type ErrorString = unsafe extern "C" fn(c_int) -> *const c_char;

unsafe extern "C" fn no_nonce_size(_: c_int) -> usize {
    0
}

#[derive(Clone, Copy)]
struct GnuTlsApi {
    cipher_list: AlgorithmList,
    cipher_name: AlgorithmName,
    cipher_tag_size: AlgorithmSize,
    cipher_block_size: AlgorithmSize,
    cipher_key_size: AlgorithmSize,
    cipher_iv_size: AlgorithmSize,
    mac_list: AlgorithmList,
    mac_name: AlgorithmName,
    mac_length: AlgorithmSize,
    mac_key_size: AlgorithmSize,
    mac_nonce_size: AlgorithmSize,
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
            | "gnutls-macs"
            | "gnutls-peer-status"
            | "gnutls-peer-status-warning-describe"
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
        _ => unreachable!("unhandled GnuTLS builtin {name}"),
    }
}
