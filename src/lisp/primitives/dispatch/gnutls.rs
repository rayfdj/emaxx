use super::*;
use crate::lisp::eval::{GnuTlsSessionApi, ProcessGnuTlsSession};
use libloading::Library;
use std::ffi::{CStr, c_char, c_int, c_uint, c_void};
use zeroize::Zeroizing;

type AlgorithmList = unsafe extern "C" fn() -> *const c_int;
type AlgorithmName = unsafe extern "C" fn(c_int) -> *const c_char;
type AlgorithmUnsigned = unsafe extern "C" fn(c_int) -> c_uint;
type AlgorithmSize = unsafe extern "C" fn(c_int) -> usize;
type ErrorIsFatal = unsafe extern "C" fn(c_int) -> c_int;
type ErrorString = unsafe extern "C" fn(c_int) -> *const c_char;
type CheckVersion = unsafe extern "C" fn(*const c_char) -> *const c_char;
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
type X509CrtInit = unsafe extern "C" fn(*mut *mut c_void) -> c_int;
type X509CrtDeinit = unsafe extern "C" fn(*mut c_void);
type X509CrtImport = unsafe extern "C" fn(*mut c_void, *const GnuTlsDatum, c_int) -> c_int;
type X509CrtPrint = unsafe extern "C" fn(*mut c_void, c_int, *mut GnuTlsDatum) -> c_int;
type GnuTlsFree = unsafe extern "C" fn(*mut c_void);
type GlobalInit = unsafe extern "C" fn() -> c_int;
type SessionInit = unsafe extern "C" fn(*mut *mut c_void, c_uint) -> c_int;
type SessionDeinit = unsafe extern "C" fn(*mut c_void);
type PrioritySetDirect =
    unsafe extern "C" fn(*mut c_void, *const c_char, *mut *const c_char) -> c_int;
type TransportSetPtr = unsafe extern "C" fn(*mut c_void, *mut c_void);
type ServerNameSet = unsafe extern "C" fn(*mut c_void, c_uint, *const c_void, usize) -> c_int;
type CredentialsSet = unsafe extern "C" fn(*mut c_void, c_int, *mut c_void) -> c_int;
type Handshake = unsafe extern "C" fn(*mut c_void) -> c_int;
type RecordRecv = unsafe extern "C" fn(*mut c_void, *mut c_void, usize) -> isize;
type RecordSend = unsafe extern "C" fn(*mut c_void, *const c_void, usize) -> isize;
type SessionBye = unsafe extern "C" fn(*mut c_void, c_int) -> c_int;
type CertificateAllocate = unsafe extern "C" fn(*mut *mut c_void) -> c_int;
type CertificateFree = unsafe extern "C" fn(*mut c_void);
type CertificateSystemTrust = unsafe extern "C" fn(*mut c_void) -> c_int;
type CertificateFile = unsafe extern "C" fn(*mut c_void, *const c_char, c_int) -> c_int;
type CertificateKeyFile =
    unsafe extern "C" fn(*mut c_void, *const c_char, *const c_char, c_int) -> c_int;
type CertificateKeyFile2 = unsafe extern "C" fn(
    *mut c_void,
    *const c_char,
    *const c_char,
    c_int,
    *const c_char,
    c_uint,
) -> c_int;
type CertificateVerifyFlags = unsafe extern "C" fn(*mut c_void, c_uint);
type GlobalSetLogLevel = unsafe extern "C" fn(c_int);
type DhSetPrimeBits = unsafe extern "C" fn(*mut c_void, c_uint);
type AnonAllocate = unsafe extern "C" fn(*mut *mut c_void) -> c_int;
type AnonFree = unsafe extern "C" fn(*mut c_void);
type CertificateVerifyPeers =
    unsafe extern "C" fn(*mut c_void, *const c_char, *mut c_uint) -> c_int;
type CertificateGetPeers = unsafe extern "C" fn(*mut c_void, *mut c_uint) -> *const GnuTlsDatum;
type SessionAlgorithm = unsafe extern "C" fn(*mut c_void) -> c_int;
type X509CrtGetDn = unsafe extern "C" fn(*mut c_void, *mut c_char, *mut usize) -> c_int;

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
    global_init: GlobalInit,
    session_init: SessionInit,
    session_deinit: SessionDeinit,
    priority_set_direct: PrioritySetDirect,
    transport_set_ptr: TransportSetPtr,
    server_name_set: ServerNameSet,
    credentials_set: CredentialsSet,
    handshake: Handshake,
    record_recv: RecordRecv,
    record_send: RecordSend,
    bye: SessionBye,
    certificate_allocate: CertificateAllocate,
    certificate_free: CertificateFree,
    certificate_system_trust: Option<CertificateSystemTrust>,
    certificate_trust_file: CertificateFile,
    certificate_crl_file: CertificateFile,
    certificate_key_file: CertificateKeyFile,
    certificate_key_file2: Option<CertificateKeyFile2>,
    certificate_verify_flags: CertificateVerifyFlags,
    global_set_log_level: GlobalSetLogLevel,
    dh_set_prime_bits: DhSetPrimeBits,
    anon_allocate: AnonAllocate,
    anon_free: AnonFree,
    certificate_verify_peers: CertificateVerifyPeers,
    certificate_get_peers: CertificateGetPeers,
    protocol_get: SessionAlgorithm,
    protocol_name: AlgorithmName,
    key_exchange_get: SessionAlgorithm,
    key_exchange_name: AlgorithmName,
    session_cipher_get: SessionAlgorithm,
    session_mac_get: SessionAlgorithm,
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
    x509_crt_init: X509CrtInit,
    x509_crt_deinit: X509CrtDeinit,
    x509_crt_import: X509CrtImport,
    x509_crt_print: X509CrtPrint,
    x509_crt_get_issuer_dn: X509CrtGetDn,
    x509_crt_get_dn: X509CrtGetDn,
    free: GnuTlsFree,
    error_is_fatal: ErrorIsFatal,
    error_string: ErrorString,
    check_version: CheckVersion,
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

unsafe fn load_data_symbol<T: Copy>(library: &Library, name: &[u8]) -> Result<T, LispError> {
    // SAFETY: GnuTLS exposes allocator hooks as exported function-pointer
    // variables.  libloading returns the address of that variable.
    let symbol = unsafe { library.get::<*mut T>(name) }
        .map_err(|error| gnutls_load_error(error.to_string()))?;
    let pointer = *symbol;
    if pointer.is_null() {
        return Err(gnutls_load_error("exported data symbol is null"));
    }
    // SAFETY: The public data symbol stores a value with the requested type.
    Ok(unsafe { *pointer })
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
                global_init: load_symbol(&library, b"gnutls_global_init")?,
                session_init: load_symbol(&library, b"gnutls_init")?,
                session_deinit: load_symbol(&library, b"gnutls_deinit")?,
                priority_set_direct: load_symbol(&library, b"gnutls_priority_set_direct")?,
                transport_set_ptr: load_symbol(&library, b"gnutls_transport_set_ptr")?,
                server_name_set: load_symbol(&library, b"gnutls_server_name_set")?,
                credentials_set: load_symbol(&library, b"gnutls_credentials_set")?,
                handshake: load_symbol(&library, b"gnutls_handshake")?,
                record_recv: load_symbol(&library, b"gnutls_record_recv")?,
                record_send: load_symbol(&library, b"gnutls_record_send")?,
                bye: load_symbol(&library, b"gnutls_bye")?,
                certificate_allocate: load_symbol(
                    &library,
                    b"gnutls_certificate_allocate_credentials",
                )?,
                certificate_free: load_symbol(&library, b"gnutls_certificate_free_credentials")?,
                certificate_system_trust: load_optional_symbol(
                    &library,
                    b"gnutls_certificate_set_x509_system_trust",
                ),
                certificate_trust_file: load_symbol(
                    &library,
                    b"gnutls_certificate_set_x509_trust_file",
                )?,
                certificate_crl_file: load_symbol(
                    &library,
                    b"gnutls_certificate_set_x509_crl_file",
                )?,
                certificate_key_file: load_symbol(
                    &library,
                    b"gnutls_certificate_set_x509_key_file",
                )?,
                certificate_key_file2: load_optional_symbol(
                    &library,
                    b"gnutls_certificate_set_x509_key_file2",
                ),
                certificate_verify_flags: load_symbol(
                    &library,
                    b"gnutls_certificate_set_verify_flags",
                )?,
                global_set_log_level: load_symbol(&library, b"gnutls_global_set_log_level")?,
                dh_set_prime_bits: load_symbol(&library, b"gnutls_dh_set_prime_bits")?,
                anon_allocate: load_symbol(&library, b"gnutls_anon_allocate_client_credentials")?,
                anon_free: load_symbol(&library, b"gnutls_anon_free_client_credentials")?,
                certificate_verify_peers: load_symbol(
                    &library,
                    b"gnutls_certificate_verify_peers3",
                )?,
                certificate_get_peers: load_symbol(&library, b"gnutls_certificate_get_peers")?,
                protocol_get: load_symbol(&library, b"gnutls_protocol_get_version")?,
                protocol_name: load_symbol(&library, b"gnutls_protocol_get_name")?,
                key_exchange_get: load_symbol(&library, b"gnutls_kx_get")?,
                key_exchange_name: load_symbol(&library, b"gnutls_kx_get_name")?,
                session_cipher_get: load_symbol(&library, b"gnutls_cipher_get")?,
                session_mac_get: load_symbol(&library, b"gnutls_mac_get")?,
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
                x509_crt_init: load_symbol(&library, b"gnutls_x509_crt_init")?,
                x509_crt_deinit: load_symbol(&library, b"gnutls_x509_crt_deinit")?,
                x509_crt_import: load_symbol(&library, b"gnutls_x509_crt_import")?,
                x509_crt_print: load_symbol(&library, b"gnutls_x509_crt_print")?,
                x509_crt_get_issuer_dn: load_symbol(&library, b"gnutls_x509_crt_get_issuer_dn")?,
                x509_crt_get_dn: load_symbol(&library, b"gnutls_x509_crt_get_dn")?,
                free: load_data_symbol(&library, b"gnutls_free")?,
                error_is_fatal: load_symbol(&library, b"gnutls_error_is_fatal")?,
                error_string: load_symbol(&library, b"gnutls_strerror")?,
                check_version: load_symbol(&library, b"gnutls_check_version")?,
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

fn encoded_library_version(api: &GnuTlsApi) -> Option<i64> {
    // SAFETY: A null requirement asks GnuTLS for its own NUL-terminated
    // runtime version string; the pointer is library-owned static storage.
    let version = unsafe { (api.check_version)(std::ptr::null()) };
    if version.is_null() {
        return None;
    }
    // SAFETY: `gnutls_check_version' promises a NUL-terminated string.
    let version = unsafe { CStr::from_ptr(version) }.to_str().ok()?;
    let mut parts = version.split('.');
    let major = parts.next()?.parse::<i64>().ok()?;
    let minor = parts.next()?.parse::<i64>().ok()?;
    let patch = parts
        .next()?
        .split(|character: char| !character.is_ascii_digit())
        .next()?
        .parse::<i64>()
        .ok()?;
    Some(major * 10_000 + minor * 100 + patch)
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
    while let Value::Cons(cell) = current {
        let rest = cell.cdr.borrow().clone();
        let (value, tail) = rest.cons_cells()?;
        if matches!(&*cell.car.borrow(), Value::Symbol(name) if name == property) {
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
    if value.is_string() || matches!(value, Value::Buffer(_)) || value.is_cons() {
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

fn gnutls_format_certificate(cert: &Value) -> Result<Value, LispError> {
    let cert = string_like(cert)
        .map(|string| string.text)
        .ok_or_else(|| wrong_type_argument("stringp", cert.clone()))?;
    let library = load_gnutls()?;
    let mut certificate = std::ptr::null_mut();
    // SAFETY: GnuTLS initializes the opaque certificate handle on success.
    let result = unsafe { (library.api.x509_crt_init)(&mut certificate) };
    if result < 0 {
        return Err(LispError::Signal(format!(
            "gnutls-format-certificate error: {}",
            gnutls_error_description(&library, result)
        )));
    }

    let cert_bytes = cert.as_bytes();
    let cert_length = cert_bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(cert_bytes.len());
    let cert_size = c_uint::try_from(cert_length)
        .map_err(|_| LispError::Signal("gnutls-format-certificate input is too large".into()))?;
    let input = GnuTlsDatum {
        data: cert_bytes.as_ptr().cast_mut(),
        size: cert_size,
    };
    // GNUTLS_X509_FMT_PEM is the public enum value 1.
    // SAFETY: The datum borrows the live Lisp string for this import call.
    let result = unsafe { (library.api.x509_crt_import)(certificate, &input, 1) };
    if result < 0 {
        // SAFETY: Successful initialization returned a live handle.
        unsafe { (library.api.x509_crt_deinit)(certificate) };
        return Err(LispError::Signal(format!(
            "gnutls-format-certificate error: {}",
            gnutls_error_description(&library, result)
        )));
    }

    let mut output = GnuTlsDatum {
        data: std::ptr::null_mut(),
        size: 0,
    };
    // GNUTLS_CRT_PRINT_FULL is the public enum value 0.
    // SAFETY: The imported certificate and output datum are valid.
    let result = unsafe { (library.api.x509_crt_print)(certificate, 0, &mut output) };
    if result < 0 {
        // SAFETY: Successful initialization returned a live handle.
        unsafe { (library.api.x509_crt_deinit)(certificate) };
        return Err(LispError::Signal(format!(
            "gnutls-format-certificate error: {}",
            gnutls_error_description(&library, result)
        )));
    }

    // SAFETY: GnuTLS returned `output.size` initialized bytes and transfers
    // ownership to the caller through its exported allocator hook.
    let text = String::from_utf8_lossy(unsafe {
        std::slice::from_raw_parts(output.data, output.size as usize)
    })
    .into_owned();
    // SAFETY: Both allocations are live and released exactly once.
    unsafe {
        (library.api.free)(output.data.cast());
        (library.api.x509_crt_deinit)(certificate);
    }
    Ok(Value::String(text.into()))
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
    if input.is_string() || matches!(input, Value::Buffer(_)) {
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

struct ForeignHandle {
    pointer: *mut c_void,
    free: unsafe extern "C" fn(*mut c_void),
}

impl ForeignHandle {
    fn into_raw(mut self) -> *mut c_void {
        std::mem::replace(&mut self.pointer, std::ptr::null_mut())
    }
}

impl Drop for ForeignHandle {
    fn drop(&mut self) {
        if !self.pointer.is_null() {
            // SAFETY: This guard uniquely owns the initialized foreign handle.
            unsafe { (self.free)(self.pointer) };
        }
    }
}

fn gnutls_result(code: c_int) -> Value {
    match code {
        0 => Value::T,
        -10 => Value::symbol("gnutls-e-invalid-session"),
        -28 => Value::symbol("gnutls-e-again"),
        -52 => Value::symbol("gnutls-e-interrupted"),
        code => Value::Integer(i64::from(code)),
    }
}

fn c_parameter(value: &str, name: &str) -> Result<std::ffi::CString, LispError> {
    std::ffi::CString::new(value)
        .map_err(|_| LispError::Signal(format!("gnutls-boot: {name} contains a NUL byte")))
}

fn gnutls_files(value: Value, error: &'static str) -> Result<Vec<String>, LispError> {
    value
        .to_vec()
        .map_err(|_| LispError::Signal(error.into()))?
        .into_iter()
        .map(|value| {
            string_like(&value)
                .map(|string| string.text)
                .ok_or_else(|| LispError::Signal(error.into()))
        })
        .collect()
}

fn contains_symbol(value: &Value, wanted: &str) -> bool {
    value.to_vec().is_ok_and(|items| {
        items
            .iter()
            .any(|item| matches!(item, Value::Symbol(symbol) if symbol == wanted))
    })
}

fn plist_has_key(value: &Value, wanted: &str) -> bool {
    value.to_vec().is_ok_and(|items| {
        items
            .iter()
            .step_by(2)
            .any(|item| matches!(item, Value::Symbol(symbol) if symbol == wanted))
    })
}

fn log_level(value: &Value) -> Option<c_int> {
    match value {
        Value::Integer(value) => {
            Some((*value).clamp(i64::from(c_int::MIN), i64::from(c_int::MAX)) as c_int)
        }
        Value::BigInteger(value) => Some(value.to_i32().unwrap_or_else(|| {
            if value.sign() == Sign::Minus {
                c_int::MIN
            } else {
                c_int::MAX
            }
        })),
        _ => None,
    }
}

fn key_file_flags(value: &Value) -> c_uint {
    let Ok(flags) = value.to_vec() else {
        return 0;
    };
    flags.into_iter().fold(0, |bits, flag| {
        bits | match flag.as_symbol().unwrap_or_default() {
            "GNUTLS_PKCS_PLAIN" => 1,
            "GNUTLS_PKCS_PKCS12_3DES" => 1 << 1,
            "GNUTLS_PKCS_PKCS12_ARCFOUR" => 1 << 2,
            "GNUTLS_PKCS_PKCS12_RC2_40" => 1 << 3,
            "GNUTLS_PKCS_PBES2_3DES" => 1 << 4,
            "GNUTLS_PKCS_PBES2_AES_128" => 1 << 5,
            "GNUTLS_PKCS_PBES2_AES_192" => 1 << 6,
            "GNUTLS_PKCS_PBES2_AES_256" => 1 << 7,
            "GNUTLS_PKCS_NULL_PASSWORD" => 1 << 8,
            "GNUTLS_PKCS_PBES2_DES" => 1 << 9,
            "GNUTLS_PKCS_PBES1_DES_MD5" => 1 << 10,
            "GNUTLS_PKCS_PBES2_GOST_TC26Z" => 1 << 11,
            "GNUTLS_PKCS_PBES2_GOST_CPA" => 1 << 12,
            "GNUTLS_PKCS_PBES2_GOST_CPB" => 1 << 13,
            "GNUTLS_PKCS_PBES2_GOST_CPC" => 1 << 14,
            "GNUTLS_PKCS_PBES2_GOST_CPD" => 1 << 15,
            _ => 0,
        }
    })
}

fn x509_distinguished_name(get_name: X509CrtGetDn, certificate: *mut c_void) -> Option<String> {
    let mut size = 0;
    // SAFETY: A null output buffer is GnuTLS's documented size probe.
    let _ = unsafe { get_name(certificate, std::ptr::null_mut(), &mut size) };
    if size == 0 {
        return None;
    }
    let mut output = vec![0_u8; size];
    // SAFETY: OUTPUT is writable for SIZE bytes and CERTIFICATE is live.
    let result = unsafe { get_name(certificate, output.as_mut_ptr().cast(), &mut size) };
    if result < 0 {
        return None;
    }
    output.truncate(size.min(output.len()));
    while output.last() == Some(&0) {
        output.pop();
    }
    Some(String::from_utf8_lossy(&output).into_owned())
}

fn peer_certificates(api: &GnuTlsApi, state: *mut c_void) -> Vec<Value> {
    let mut count = 0;
    // SAFETY: STATE is a successfully handshaken session; GnuTLS owns the
    // returned certificate datum array for the session's lifetime.
    let peers = unsafe { (api.certificate_get_peers)(state, &mut count) };
    if peers.is_null() || count == 0 {
        return Vec::new();
    }
    // SAFETY: GnuTLS returned COUNT consecutive datum records.
    let peers = unsafe { std::slice::from_raw_parts(peers, count as usize) };
    peers
        .iter()
        .filter_map(|peer| {
            let mut certificate = std::ptr::null_mut();
            // SAFETY: GnuTLS initializes one opaque certificate handle.
            if unsafe { (api.x509_crt_init)(&mut certificate) } < 0 {
                return None;
            }
            let certificate = ForeignHandle {
                pointer: certificate,
                free: api.x509_crt_deinit,
            };
            // Peer certificates are DER-encoded (GNUTLS_X509_FMT_DER = 0).
            // SAFETY: PEER is borrowed from the live session and CERTIFICATE
            // is an initialized X.509 handle.
            if unsafe { (api.x509_crt_import)(certificate.pointer, peer, 0) } < 0 {
                return None;
            }
            let mut details = Vec::new();
            if let Some(issuer) =
                x509_distinguished_name(api.x509_crt_get_issuer_dn, certificate.pointer)
            {
                details.extend([Value::symbol(":issuer"), Value::String(issuer.into())]);
            }
            if let Some(subject) = x509_distinguished_name(api.x509_crt_get_dn, certificate.pointer)
            {
                details.extend([Value::symbol(":subject"), Value::String(subject.into())]);
            }
            Some(Value::list(details))
        })
        .collect()
}

fn negotiated_peer_status(
    api: &GnuTlsApi,
    state: *mut c_void,
    verification: c_uint,
    is_x509: bool,
) -> Value {
    let warning_bits = [
        (1 << 20, ":invalid-ocsp-status"),
        (1 << 19, ":missing-ocsp-status"),
        (1 << 18, ":purpose-mismatch"),
        (1 << 16, ":signer-constraints-failure"),
        (1 << 15, ":revocation-data-issued-in-future"),
        (1 << 12, ":revocation-data-superseded"),
        (1 << 11, ":signature-failure"),
        (1 << 10, ":expired"),
        (1 << 9, ":not-activated"),
        (1 << 8, ":insecure"),
        (1 << 7, ":not-ca"),
        (1 << 6, ":unknown-ca"),
        (1 << 5, ":revoked"),
        (1 << 1, ":invalid"),
    ];
    let mut result = Vec::new();
    let mut warnings = Vec::new();
    if verification & (1 << 14) != 0 {
        warnings.push(Value::symbol(":no-host-match"));
    }
    warnings.extend(
        warning_bits
            .into_iter()
            .filter(|(bit, _)| verification & bit != 0)
            .map(|(_, warning)| Value::symbol(warning)),
    );
    if !warnings.is_empty() {
        result.extend([Value::symbol(":warnings"), Value::list(warnings)]);
    }
    if is_x509 {
        let certificates = peer_certificates(api, state);
        if let Some(certificate) = certificates.first().cloned() {
            result.extend([
                Value::symbol(":certificates"),
                Value::list(certificates),
                Value::symbol(":certificate"),
                certificate,
            ]);
        }
    }
    let algorithms = [
        (":key-exchange", api.key_exchange_get, api.key_exchange_name),
        (":protocol", api.protocol_get, api.protocol_name),
        (":cipher", api.session_cipher_get, api.cipher_name),
        (":mac", api.session_mac_get, api.mac_name),
    ];
    for (key, get, name) in algorithms {
        // SAFETY: STATE is a live, successfully handshaken session and the
        // algorithm-name functions return static strings.
        let value = unsafe { c_string(name(get(state))) };
        if let Some(value) = value {
            result.extend([Value::symbol(key), Value::String(value.into())]);
        }
    }
    Value::list(result)
}

enum PeerVerification {
    Ready(Value),
    GnuTlsError(c_int),
}

fn completed_peer_status(
    api: &GnuTlsApi,
    state: *mut c_void,
    is_x509: bool,
    hostname: &str,
    verify_error: &Value,
) -> Result<PeerVerification, LispError> {
    let mut verification = 0;
    if is_x509 {
        let hostname_c = c_parameter(hostname, "hostname")?;
        // SAFETY: The session completed its handshake, HOSTNAME is
        // NUL-terminated, and VERIFICATION is writable output storage.
        let result = unsafe {
            (api.certificate_verify_peers)(state, hostname_c.as_ptr(), &mut verification)
        };
        if result < 0 {
            return Ok(PeerVerification::GnuTlsError(result));
        }
        let reject_trust =
            verify_error == &Value::T || contains_symbol(verify_error, ":trustfiles");
        let reject_hostname =
            verify_error == &Value::T || contains_symbol(verify_error, ":hostname");
        if reject_trust && verification & !(1 << 14) != 0 {
            return Err(LispError::Signal(format!(
                "Certificate validation failed {hostname}, verification code {verification:x}"
            )));
        }
        if reject_hostname && verification & (1 << 14) != 0 {
            return Err(LispError::Signal(format!(
                "The x509 certificate does not match \"{hostname}\""
            )));
        }
    }
    Ok(PeerVerification::Ready(negotiated_peer_status(
        api,
        state,
        verification,
        is_x509,
    )))
}

#[cfg(unix)]
fn gnutls_boot(
    interp: &mut Interpreter,
    process: &Value,
    credential_type: &Value,
    parameters: &Value,
) -> Result<Value, LispError> {
    let process_id = interp.resolve_process_id(process)?;
    let Value::Symbol(credential_type) = credential_type else {
        return Err(wrong_type_argument("symbolp", credential_type.clone()));
    };
    parameters
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", parameters.clone()))?;
    let is_x509 = match credential_type.as_str() {
        "gnutls-x509pki" => true,
        "gnutls-anon" => false,
        _ => return Err(LispError::Signal("Invalid GnuTLS credential type".into())),
    };
    let hostname_value = contact_plist_get(parameters, ":hostname");
    let hostname = string_like(&hostname_value)
        .map(|string| string.text)
        .ok_or_else(|| {
            LispError::Signal("gnutls-boot: invalid :hostname parameter (not a string)".into())
        })?;
    let priority = string_like(&contact_plist_get(parameters, ":priority"))
        .map(|string| string.text)
        .unwrap_or_else(|| "NORMAL".into());
    let complete = contact_plist_get(parameters, ":complete-negotiation").is_truthy();
    let verify_error = contact_plist_get(parameters, ":verify-error");
    if verify_error != Value::T && verify_error.to_vec().is_err() {
        return Err(LispError::Signal(
            "gnutls-boot: invalid :verify_error parameter (not a list)".into(),
        ));
    }
    let transport = interp.process_network_transport_handle(process_id)?;
    let library = load_gnutls()?;
    let api = library.api;

    if let Some(level) = log_level(&contact_plist_get(parameters, ":loglevel")) {
        // SAFETY: The global log level is an unconstrained C integer.
        unsafe { (api.global_set_log_level)(level) };
    }

    // SAFETY: Global initialization is idempotent and has no borrowed inputs.
    let result = unsafe { (api.global_init)() };
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    let _ = interp.deinit_process_gnutls(process_id);

    let mut credential_pointer = std::ptr::null_mut();
    // SAFETY: The selected allocator initializes CREDENTIAL_POINTER on success.
    let result = unsafe {
        if is_x509 {
            (api.certificate_allocate)(&mut credential_pointer)
        } else {
            (api.anon_allocate)(&mut credential_pointer)
        }
    };
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    let credential_free = if is_x509 {
        api.certificate_free
    } else {
        api.anon_free
    };
    let credential = ForeignHandle {
        pointer: credential_pointer,
        free: credential_free,
    };

    if is_x509 {
        let verify_flags = match contact_plist_get(parameters, ":verify-flags") {
            Value::Integer(flags) => c_uint::try_from(flags).unwrap_or(0),
            _ => 0,
        };
        // SAFETY: CREDENTIAL is a live X.509 credential handle.  Zero is
        // GNU's default GNUTLS_VERIFY_ALLOW_X509_V1_CA_CRT value on current
        // GnuTLS, and invalid Lisp values intentionally select that default.
        unsafe { (api.certificate_verify_flags)(credential.pointer, verify_flags) };
        if let Some(system_trust) = api.certificate_system_trust {
            // SAFETY: CREDENTIAL is a live X.509 credential handle.
            let _ = unsafe { system_trust(credential.pointer) };
        }
        for trustfile in gnutls_files(
            contact_plist_get(parameters, ":trustfiles"),
            "Invalid trustfile",
        )? {
            let trustfile = c_parameter(&trustfile, "trustfile")?;
            // SAFETY: CREDENTIAL is live and TRUSTFILE is NUL-terminated.
            let result =
                unsafe { (api.certificate_trust_file)(credential.pointer, trustfile.as_ptr(), 1) };
            if result < 0 {
                return Ok(gnutls_result(result));
            }
        }
        for crlfile in gnutls_files(
            contact_plist_get(parameters, ":crlfiles"),
            "Invalid CRL file",
        )? {
            let crlfile = c_parameter(&crlfile, "CRL file")?;
            // SAFETY: CREDENTIAL is live and CRLFILE is NUL-terminated.
            let result =
                unsafe { (api.certificate_crl_file)(credential.pointer, crlfile.as_ptr(), 1) };
            if result < 0 {
                return Ok(gnutls_result(result));
            }
        }
        let pass_present = plist_has_key(parameters, ":pass");
        let pass = string_like(&contact_plist_get(parameters, ":pass"))
            .map(|pass| c_parameter(&pass.text, "private-key password"))
            .transpose()?;
        let flags = key_file_flags(&contact_plist_get(parameters, ":flags"));
        for pair in contact_plist_get(parameters, ":keylist")
            .to_vec()
            .map_err(|_| LispError::Signal("Invalid client key file".into()))?
        {
            let pair = pair
                .to_vec()
                .map_err(|_| LispError::Signal("Invalid client key file".into()))?;
            let keyfile = pair
                .first()
                .and_then(string_like)
                .map(|string| string.text)
                .ok_or_else(|| LispError::Signal("Invalid client key file".into()))?;
            let certfile = pair
                .get(1)
                .and_then(string_like)
                .map(|string| string.text)
                .ok_or_else(|| LispError::Signal("Invalid client cert file".into()))?;
            let keyfile = c_parameter(&keyfile, "client key file")?;
            let certfile = c_parameter(&certfile, "client cert file")?;
            // SAFETY: CREDENTIAL is live, both paths are NUL-terminated, and
            // PASS (when non-null) remains live for this call.
            let result = unsafe {
                if pass_present && let Some(key_file2) = api.certificate_key_file2 {
                    key_file2(
                        credential.pointer,
                        certfile.as_ptr(),
                        keyfile.as_ptr(),
                        1,
                        pass.as_ref().map_or(std::ptr::null(), |pass| pass.as_ptr()),
                        flags,
                    )
                } else {
                    (api.certificate_key_file)(
                        credential.pointer,
                        certfile.as_ptr(),
                        keyfile.as_ptr(),
                        1,
                    )
                }
            };
            if result < 0 {
                return Ok(gnutls_result(result));
            }
        }
    }

    let mut state_pointer = std::ptr::null_mut();
    // Every Emaxx stream is non-blocking; tell GnuTLS as GNU does for the
    // corresponding process flag (GNUTLS_CLIENT | GNUTLS_NONBLOCK).
    // SAFETY: SESSION_INIT initializes STATE_POINTER as a client session.
    let result = unsafe { (api.session_init)(&mut state_pointer, (1 << 1) | (1 << 3)) };
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    let state = ForeignHandle {
        pointer: state_pointer,
        free: api.session_deinit,
    };
    let priority = c_parameter(&priority, "priority")?;
    // SAFETY: STATE is live and PRIORITY is NUL-terminated.
    let result = unsafe {
        (api.priority_set_direct)(state.pointer, priority.as_ptr(), std::ptr::null_mut())
    };
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    if let Value::Integer(bits) = contact_plist_get(parameters, ":min-prime-bits") {
        // GNU accepts only a fixnum and lets C narrow it to unsigned int.
        // SAFETY: STATE is a live initialized session.
        unsafe { (api.dh_set_prime_bits)(state.pointer, bits as c_uint) };
    }
    // SAFETY: Both handles are live and the credential type matches its
    // allocator (certificate=1, anonymous=2).
    let result = unsafe {
        (api.credentials_set)(
            state.pointer,
            if is_x509 { 1 } else { 2 },
            credential.pointer,
        )
    };
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    if hostname.parse::<std::net::IpAddr>().is_err() {
        // SAFETY: STATE is live and HOSTNAME remains borrowed for this call.
        let result = unsafe {
            (api.server_name_set)(state.pointer, 1, hostname.as_ptr().cast(), hostname.len())
        };
        if result < 0 {
            return Ok(gnutls_result(result));
        }
    }
    // GnuTLS's native socket transport interprets this opaque pointer as the
    // connected descriptor installed on the process.
    unsafe { (api.transport_set_ptr)(state.pointer, transport as *mut c_void) };

    let mut session = ProcessGnuTlsSession::new(
        library._library,
        state.into_raw(),
        credential.into_raw(),
        GnuTlsSessionApi {
            session_deinit: api.session_deinit,
            credential_deinit: credential_free,
            record_recv: api.record_recv,
            record_send: api.record_send,
            handshake: api.handshake,
            bye: api.bye,
            error_string: api.error_string,
            error_is_fatal: api.error_is_fatal,
        },
        false,
    );
    let result = session.handshake(complete)?;
    let peer_status = if result == 0 {
        match completed_peer_status(&api, session.raw_state(), is_x509, &hostname, &verify_error)? {
            PeerVerification::Ready(status) => status,
            PeerVerification::GnuTlsError(error) => return Ok(gnutls_result(error)),
        }
    } else {
        Value::Nil
    };
    interp.install_process_gnutls(
        process_id,
        session,
        if result == 0 { 9 } else { 8 },
        peer_status,
    )?;
    Ok(gnutls_result(result))
}

#[cfg(unix)]
pub(crate) enum AsyncGnuTlsProgress {
    NotRequested,
    Pending,
    Ready,
    Failed(Value),
}

#[cfg(unix)]
pub(crate) fn progress_async_gnutls(
    interp: &mut Interpreter,
    process_id: u64,
) -> Result<AsyncGnuTlsProgress, LispError> {
    let Some(parameters) = interp.process_gnutls_boot_parameters(process_id) else {
        return Ok(AsyncGnuTlsProgress::NotRequested);
    };
    if parameters.is_nil() {
        return Ok(AsyncGnuTlsProgress::NotRequested);
    }
    let items = parameters
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", parameters.clone()))?;
    let Some((credential_type, parameter_items)) = items.split_first() else {
        return Ok(AsyncGnuTlsProgress::NotRequested);
    };
    let Value::Symbol(credential_symbol) = credential_type else {
        return Err(wrong_type_argument("symbolp", credential_type.clone()));
    };
    let is_x509 = match credential_symbol.as_str() {
        "gnutls-x509pki" => true,
        "gnutls-anon" => false,
        _ => return Err(LispError::Signal("Invalid GnuTLS credential type".into())),
    };
    let parameter_list = Value::list(parameter_items.to_vec());
    let stage = interp.process_gnutls_initstage(process_id).unwrap_or(0);
    if stage == 0 {
        let result = gnutls_boot(
            interp,
            &Value::Record(process_id),
            credential_type,
            &parameter_list,
        )?;
        return match result {
            Value::T => {
                interp.clear_process_gnutls_boot_parameters(process_id);
                Ok(AsyncGnuTlsProgress::Ready)
            }
            Value::Symbol(symbol)
                if matches!(symbol.as_str(), "gnutls-e-again" | "gnutls-e-interrupted") =>
            {
                Ok(AsyncGnuTlsProgress::Pending)
            }
            error => Ok(AsyncGnuTlsProgress::Failed(error)),
        };
    }
    if stage != 8 {
        interp.clear_process_gnutls_boot_parameters(process_id);
        return Ok(AsyncGnuTlsProgress::Ready);
    }

    let library = load_gnutls()?;
    let (result, state) = interp.continue_process_gnutls_handshake(process_id)?;
    match result {
        0 => {
            let hostname = string_like(&contact_plist_get(&parameter_list, ":hostname"))
                .map(|string| string.text)
                .ok_or_else(|| {
                    LispError::Signal(
                        "gnutls-boot: invalid :hostname parameter (not a string)".into(),
                    )
                })?;
            let verify_error = contact_plist_get(&parameter_list, ":verify-error");
            let status = match completed_peer_status(
                &library.api,
                state,
                is_x509,
                &hostname,
                &verify_error,
            )? {
                PeerVerification::Ready(status) => status,
                PeerVerification::GnuTlsError(error) => {
                    return Ok(AsyncGnuTlsProgress::Failed(gnutls_result(error)));
                }
            };
            interp.finish_process_gnutls_handshake(process_id, status)?;
            Ok(AsyncGnuTlsProgress::Ready)
        }
        -28 | -52 => Ok(AsyncGnuTlsProgress::Pending),
        error => Ok(AsyncGnuTlsProgress::Failed(gnutls_result(error))),
    }
}

#[cfg(not(unix))]
fn gnutls_boot(
    interp: &mut Interpreter,
    process: &Value,
    credential_type: &Value,
    parameters: &Value,
) -> Result<Value, LispError> {
    interp.resolve_process_id(process)?;
    if !matches!(credential_type, Value::Symbol(_)) {
        return Err(wrong_type_argument("symbolp", credential_type.clone()));
    }
    parameters
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", parameters.clone()))?;
    Err(LispError::Signal(
        "GnuTLS process transport is unavailable on this platform".into(),
    ))
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
    ) -> Result<Value, LispError> {
        match name {
            "gnutls-available-p" => {
                need_args(name, args, 0)?;
                let Ok(library) = load_gnutls() else {
                    return Ok(Value::Nil);
                };
                interp.set_global_binding(
                    "libgnutls-version",
                    Value::Integer(encoded_library_version(&library.api).unwrap_or(-1)),
                );
                let mut capabilities = vec![
                    Value::symbol("macs"),
                    Value::symbol("ciphers"),
                    Value::symbol("digests"),
                    Value::symbol("gnutls3"),
                    Value::symbol("gnutls"),
                ];
                if library.api.aead_init.is_some()
                    && library.api.aead_encrypt.is_some()
                    && library.api.aead_decrypt.is_some()
                    && library.api.aead_deinit.is_some()
                {
                    capabilities.insert(1, Value::symbol("AEAD-ciphers"));
                }
                Ok(Value::list(capabilities))
            }
            "gnutls-asynchronous-parameters" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp.set_process_gnutls_boot_parameters(process_id, args[1].clone());
                Ok(Value::Nil)
            }
            "gnutls-boot" => {
                need_args(name, args, 3)?;
                gnutls_boot(interp, &args[0], &args[1], &args[2])
            }
            "gnutls-bye" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp
                    .process_gnutls_bye(process_id, args[1].is_truthy())
                    .map(gnutls_result)
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
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp
                    .process_gnutls_peer_status(process_id)
                    .unwrap_or(Value::Nil))
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
            "gnutls-format-certificate" => {
                need_args(name, args, 1)?;
                gnutls_format_certificate(&args[0])
            }
            "gnutls-symmetric-decrypt" | "gnutls-symmetric-encrypt" => {
                need_arg_range(name, args, 4, 5)?;
                gnutls_symmetric(interp, args, name == "gnutls-symmetric-encrypt")
            }
        }
    }
);
