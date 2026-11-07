use super::*;
use crate::lisp::eval::{GnuTlsSessionApi, ProcessGnuTlsSession};
use crate::lisp::types::Kind;
use chrono::{DateTime, Utc};
use libloading::Library;
use std::ffi::{CStr, c_char, c_int, c_uint, c_void};
use std::sync::{Arc, Mutex};
use zeroize::Zeroizing;

type AlgorithmList = unsafe extern "C" fn() -> *const c_int;
type AlgorithmName = unsafe extern "C" fn(c_int) -> *const c_char;
type ExtensionName = unsafe extern "C" fn(c_uint) -> *const c_char;
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
type TransportErrnoFunction = unsafe extern "C" fn(*mut c_void) -> c_int;
type TransportSetErrnoFunction = unsafe extern "C" fn(*mut c_void, Option<TransportErrnoFunction>);
type ServerNameSet = unsafe extern "C" fn(*mut c_void, c_uint, *const c_void, usize) -> c_int;
type CredentialsSet = unsafe extern "C" fn(*mut c_void, c_int, *mut c_void) -> c_int;
type Handshake = unsafe extern "C" fn(*mut c_void) -> c_int;
type RecordRecv = unsafe extern "C" fn(*mut c_void, *mut c_void, usize) -> isize;
type RecordCheckPending = unsafe extern "C" fn(*mut c_void) -> usize;
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
type CertificateVerifyPeers2 = unsafe extern "C" fn(*mut c_void, *mut c_uint) -> c_int;
type CertificateTypeGet = unsafe extern "C" fn(*mut c_void) -> c_int;
type X509CrtCheckHostname = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_uint;
type X509CrtCheckIssuer = unsafe extern "C" fn(*mut c_void, *mut c_void) -> c_uint;
type X509CrtGetUniqueId = unsafe extern "C" fn(*mut c_void, *mut c_char, *mut usize) -> c_int;
type X509CrtGetKeyId = unsafe extern "C" fn(*mut c_void, c_uint, *mut u8, *mut usize) -> c_int;
type X509CrtGetFingerprint =
    unsafe extern "C" fn(*mut c_void, c_int, *mut c_void, *mut usize) -> c_int;
type X509CrtExport = unsafe extern "C" fn(*mut c_void, c_int, *mut c_void, *mut usize) -> c_int;
type SessionStatus = unsafe extern "C" fn(*mut c_void) -> c_uint;
type LogFunction = unsafe extern "C" fn(c_int, *const c_char);
type AuditLogFunction = unsafe extern "C" fn(*mut c_void, *const c_char);
type GlobalSetLogFunction = unsafe extern "C" fn(Option<LogFunction>);
type GlobalSetAuditLogFunction = unsafe extern "C" fn(Option<AuditLogFunction>);
type CertificateGetPeers = unsafe extern "C" fn(*mut c_void, *mut c_uint) -> *const GnuTlsDatum;
type SessionAlgorithm = unsafe extern "C" fn(*mut c_void) -> c_int;
type X509CrtGetDn = unsafe extern "C" fn(*mut c_void, *mut c_char, *mut usize) -> c_int;
type X509CrtGetVersion = unsafe extern "C" fn(*mut c_void) -> c_int;
type X509CrtGetSerial = unsafe extern "C" fn(*mut c_void, *mut c_void, *mut usize) -> c_int;
type X509CrtGetTime = unsafe extern "C" fn(*mut c_void) -> libc::time_t;
type X509CrtGetPkAlgorithm = unsafe extern "C" fn(*mut c_void, *mut c_uint) -> c_int;
type X509CrtGetSignatureAlgorithm = unsafe extern "C" fn(*mut c_void) -> c_int;
type PkBitsToSecParam = unsafe extern "C" fn(c_int, c_uint) -> c_int;

/// gnutls.h's codes gnutls-boot tells apart.
const GNUTLS_CLIENT: c_uint = 1 << 1;
const GNUTLS_NONBLOCK: c_uint = 1 << 3;
const GNUTLS_E_INTERRUPTED: c_int = -52;
const GNUTLS_E_INVALID_SESSION: c_int = -10;
const GNUTLS_E_SHORT_MEMORY_BUFFER: c_int = -51;
const GNUTLS_CRT_X509: c_int = 1;
const GNUTLS_TLS1_2: c_int = 4;
const GNUTLS_DIG_SHA1: c_int = 3;
const GNUTLS_KEYID_USE_SHA256: c_uint = 1;
/// gnutls.c's extra_peer_verification_t.
const CERTIFICATE_NOT_MATCHING: u32 = 2;

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
    transport_set_errno_function: Option<TransportSetErrnoFunction>,
    server_name_set: ServerNameSet,
    credentials_set: CredentialsSet,
    handshake: Handshake,
    record_recv: RecordRecv,
    record_send: RecordSend,
    record_check_pending: RecordCheckPending,
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
    certificate_verify_peers2: CertificateVerifyPeers2,
    certificate_get_peers: CertificateGetPeers,
    certificate_type_get: CertificateTypeGet,
    dh_get_prime_bits: SessionAlgorithm,
    session_etm_status: Option<SessionStatus>,
    safe_renegotiation_status: SessionStatus,
    alert_get: SessionAlgorithm,
    alert_get_name: AlgorithmName,
    global_set_log_function: GlobalSetLogFunction,
    global_set_audit_log_function: Option<GlobalSetAuditLogFunction>,
    x509_crt_check_hostname: X509CrtCheckHostname,
    x509_crt_check_issuer: X509CrtCheckIssuer,
    x509_crt_get_issuer_unique_id: X509CrtGetUniqueId,
    x509_crt_get_subject_unique_id: X509CrtGetUniqueId,
    x509_crt_get_key_id: X509CrtGetKeyId,
    x509_crt_get_fingerprint: X509CrtGetFingerprint,
    x509_crt_export: X509CrtExport,
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
    digest_list: AlgorithmList,
    digest_name: AlgorithmName,
    digest_length: AlgorithmUnsigned,
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
    extension_name: Option<ExtensionName>,
    x509_crt_init: X509CrtInit,
    x509_crt_deinit: X509CrtDeinit,
    x509_crt_import: X509CrtImport,
    x509_crt_print: X509CrtPrint,
    x509_crt_get_issuer_dn: X509CrtGetDn,
    x509_crt_get_dn: X509CrtGetDn,
    x509_crt_get_version: X509CrtGetVersion,
    x509_crt_get_serial: X509CrtGetSerial,
    x509_crt_get_activation_time: X509CrtGetTime,
    x509_crt_get_expiration_time: X509CrtGetTime,
    x509_crt_get_pk_algorithm: X509CrtGetPkAlgorithm,
    x509_crt_get_signature_algorithm: X509CrtGetSignatureAlgorithm,
    pk_algorithm_name: AlgorithmName,
    sign_algorithm_name: AlgorithmName,
    pk_bits_to_sec_param: PkBitsToSecParam,
    sec_param_name: AlgorithmName,
    free: GnuTlsFree,
    error_is_fatal: ErrorIsFatal,
    error_string: ErrorString,
    check_version: CheckVersion,
}

struct GnuTlsLibrary {
    _library: Library,
    api: GnuTlsApi,
    initialized: Mutex<bool>,
}

impl GnuTlsLibrary {
    fn initialize(&self) -> c_int {
        let mut initialized = self
            .initialized
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *initialized {
            return 0;
        }
        // SAFETY: Initialization has no borrowed inputs and is serialized.
        // GNU's emacs_gnutls_global_init retains a successful initialization
        // for the process lifetime, but permits a retry after failure.
        let result = unsafe { (self.api.global_init)() };
        if result == 0 {
            *initialized = true;
        }
        result
    }
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

/// GNU links GnuTLS once (w32 loads the DLL once). Keep that process lifetime:
/// unloading at thread exit can invalidate GnuTLS's native thread destructors
/// before the host invokes them. Failed loads remain retryable.
fn gnutls_library() -> Result<Arc<GnuTlsLibrary>, LispError> {
    static GNUTLS_LIBRARY: Mutex<Option<Arc<GnuTlsLibrary>>> = Mutex::new(None);
    let mut slot = GNUTLS_LIBRARY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(library) = slot.as_ref() {
        return Ok(Arc::clone(library));
    }
    let library = Arc::new(load_gnutls()?);
    *slot = Some(Arc::clone(&library));
    Ok(library)
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
                transport_set_errno_function: load_optional_symbol(
                    &library,
                    b"gnutls_transport_set_errno_function",
                ),
                server_name_set: load_symbol(&library, b"gnutls_server_name_set")?,
                credentials_set: load_symbol(&library, b"gnutls_credentials_set")?,
                handshake: load_symbol(&library, b"gnutls_handshake")?,
                record_recv: load_symbol(&library, b"gnutls_record_recv")?,
                record_send: load_symbol(&library, b"gnutls_record_send")?,
                record_check_pending: load_symbol(&library, b"gnutls_record_check_pending")?,
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
                certificate_verify_peers2: load_symbol(
                    &library,
                    b"gnutls_certificate_verify_peers2",
                )?,
                certificate_get_peers: load_symbol(&library, b"gnutls_certificate_get_peers")?,
                certificate_type_get: load_symbol(&library, b"gnutls_certificate_type_get")?,
                dh_get_prime_bits: load_symbol(&library, b"gnutls_dh_get_prime_bits")?,
                // HAVE_GNUTLS_ETM_STATUS.
                session_etm_status: load_optional_symbol(&library, b"gnutls_session_etm_status"),
                safe_renegotiation_status: load_symbol(
                    &library,
                    b"gnutls_safe_renegotiation_status",
                )?,
                alert_get: load_symbol(&library, b"gnutls_alert_get")?,
                alert_get_name: load_symbol(&library, b"gnutls_alert_get_name")?,
                global_set_log_function: load_symbol(&library, b"gnutls_global_set_log_function")?,
                // HAVE_GNUTLS3.
                global_set_audit_log_function: load_optional_symbol(
                    &library,
                    b"gnutls_global_set_audit_log_function",
                ),
                x509_crt_check_hostname: load_symbol(&library, b"gnutls_x509_crt_check_hostname")?,
                x509_crt_check_issuer: load_symbol(&library, b"gnutls_x509_crt_check_issuer")?,
                x509_crt_get_issuer_unique_id: load_symbol(
                    &library,
                    b"gnutls_x509_crt_get_issuer_unique_id",
                )?,
                x509_crt_get_subject_unique_id: load_symbol(
                    &library,
                    b"gnutls_x509_crt_get_subject_unique_id",
                )?,
                x509_crt_get_key_id: load_symbol(&library, b"gnutls_x509_crt_get_key_id")?,
                x509_crt_get_fingerprint: load_symbol(
                    &library,
                    b"gnutls_x509_crt_get_fingerprint",
                )?,
                x509_crt_export: load_symbol(&library, b"gnutls_x509_crt_export")?,
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
                // gnutls.c:402,403,434 load exactly these three for
                // `gnutls-digests'.  The catalogue used to be a transcribed
                // constant table in this file while its cipher and mac
                // neighbours were queried live (audit finding 100).
                digest_list: load_symbol(&library, b"gnutls_digest_list")?,
                digest_name: load_symbol(&library, b"gnutls_digest_get_name")?,
                digest_length: load_symbol(&library, b"gnutls_hash_get_len")?,
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
                extension_name: load_optional_symbol(&library, b"gnutls_ext_get_name"),
                x509_crt_init: load_symbol(&library, b"gnutls_x509_crt_init")?,
                x509_crt_deinit: load_symbol(&library, b"gnutls_x509_crt_deinit")?,
                x509_crt_import: load_symbol(&library, b"gnutls_x509_crt_import")?,
                x509_crt_print: load_symbol(&library, b"gnutls_x509_crt_print")?,
                x509_crt_get_issuer_dn: load_symbol(&library, b"gnutls_x509_crt_get_issuer_dn")?,
                x509_crt_get_dn: load_symbol(&library, b"gnutls_x509_crt_get_dn")?,
                x509_crt_get_version: load_symbol(&library, b"gnutls_x509_crt_get_version")?,
                x509_crt_get_serial: load_symbol(&library, b"gnutls_x509_crt_get_serial")?,
                x509_crt_get_activation_time: load_symbol(
                    &library,
                    b"gnutls_x509_crt_get_activation_time",
                )?,
                x509_crt_get_expiration_time: load_symbol(
                    &library,
                    b"gnutls_x509_crt_get_expiration_time",
                )?,
                x509_crt_get_pk_algorithm: load_symbol(
                    &library,
                    b"gnutls_x509_crt_get_pk_algorithm",
                )?,
                x509_crt_get_signature_algorithm: load_symbol(
                    &library,
                    b"gnutls_x509_crt_get_signature_algorithm",
                )?,
                pk_algorithm_name: load_symbol(&library, b"gnutls_pk_algorithm_get_name")?,
                sign_algorithm_name: load_symbol(&library, b"gnutls_sign_get_name")?,
                pk_bits_to_sec_param: load_symbol(&library, b"gnutls_pk_bits_to_sec_param")?,
                sec_param_name: load_symbol(&library, b"gnutls_sec_param_get_name")?,
                free: load_data_symbol(&library, b"gnutls_free")?,
                error_is_fatal: load_symbol(&library, b"gnutls_error_is_fatal")?,
                error_string: load_symbol(&library, b"gnutls_strerror")?,
                check_version: load_symbol(&library, b"gnutls_check_version")?,
            }
        };
        return Ok(GnuTlsLibrary {
            _library: library,
            api,
            initialized: Mutex::new(false),
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
}

// NOT the `gnutls-digests' catalogue -- that is queried from the library now
// (audit finding 100).  This is only the mapping from a GnuTLS digest name to
// the internal hash implementation `gnutls-hash-digest' uses, which has no
// equivalent in the library and must live somewhere.
const DIGESTS: &[DigestSpec] = &[
    DigestSpec {
        name: "STREEBOG-512",
        id: 17,
        algorithm: "streebog-512",
    },
    DigestSpec {
        name: "STREEBOG-256",
        id: 16,
        algorithm: "streebog-256",
    },
    DigestSpec {
        name: "GOSTR341194",
        id: 15,
        algorithm: "gost94-cryptopro",
    },
    DigestSpec {
        name: "MD5",
        id: 2,
        algorithm: "md5",
    },
    DigestSpec {
        name: "SHA224",
        id: 9,
        algorithm: "sha224",
    },
    DigestSpec {
        name: "SHA512",
        id: 8,
        algorithm: "sha512",
    },
    DigestSpec {
        name: "SHA384",
        id: 7,
        algorithm: "sha384",
    },
    DigestSpec {
        name: "SHA256",
        id: 6,
        algorithm: "sha256",
    },
    DigestSpec {
        name: "SHA1",
        id: 3,
        algorithm: "sha1",
    },
];

fn plist_integer(plist: &Value, property: &str) -> Option<i64> {
    let mut current = *plist;
    while let Kind::Cons(cell) = current.kind() {
        let rest = cell.cdr.get();
        let (value, tail) = rest.cons_cells()?;
        if matches!(cell.car.get().kind(), Kind::Symbol(name) if name.is_named(property)) {
            return value.get().integer_value();
        }
        current = tail.get();
    }
    None
}

fn digest_spec(method: &Value) -> Option<&'static DigestSpec> {
    let id = match method.kind() {
        Kind::Integer(id) => Some(id),
        Kind::StringObject(_) => {
            let name = string_text(method).ok()?;
            return DIGESTS.iter().find(|spec| spec.name == name);
        }
        Kind::Symbol(name) => return DIGESTS.iter().find(|spec| name.is_named(spec.name)),
        Kind::Cons(..) => plist_integer(method, ":digest-algorithm-id"),
        _ => None,
    };
    DIGESTS.iter().find(|spec| Some(spec.id) == id)
}

fn invalid_digest_method(method: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("GnuTLS digest-method is invalid or not found"),
        *method,
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

/// gnutls.c:2713 `Fgnutls_digests': walk `gnutls_digest_list', and for each
/// algorithm cons a plist of its name, id, type and hash length.  GNU conses,
/// so the list comes back reversed relative to the library's own order --
/// which is why `entries.reverse()' is not cosmetic.
fn digest_catalog(library: &GnuTlsLibrary) -> Value {
    let api = library.api;
    let mut entries = algorithm_ids(api.digest_list)
        .into_iter()
        .filter_map(|id| {
            // SAFETY: `id` came directly from `gnutls_digest_list`.
            let name = c_string(unsafe { (api.digest_name)(id) })?;
            // SAFETY: `gnutls_hash_get_len` accepts any digest-list ID.
            let length = unsafe { (api.digest_length)(id) };
            Some(Value::list([
                Value::symbol(&name),
                Value::symbol(":digest-algorithm-id"),
                Value::Integer(i64::from(id)),
                Value::symbol(":type"),
                Value::symbol("gnutls-digest-algorithm"),
                Value::symbol(":digest-algorithm-length"),
                Value::Integer(length as i64),
            ]))
        })
        .collect::<Vec<_>>();
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
        *method,
    ]))
}

fn mac_method_id(method: &Value, library: &GnuTlsLibrary) -> Result<c_int, LispError> {
    let normalized = match method.kind() {
        Kind::StringObject(_) => Value::symbol(&string_text(method)?),
        method => method.value(),
    };
    let id = match normalized.kind() {
        Kind::Integer(id) => c_int::try_from(id).ok(),
        Kind::Symbol(name) => algorithm_ids(library.api.mac_list).into_iter().find(|id| {
            // SAFETY: Every candidate came from `gnutls_mac_list`.
            c_string(unsafe { (library.api.mac_name)(*id) }).as_deref() == Some(name.as_str())
        }),
        Kind::Cons(..) => {
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
        *method,
    ]))
}

fn cipher_method_id(method: &Value, library: &GnuTlsLibrary) -> Result<c_int, LispError> {
    let normalized = match method.kind() {
        Kind::StringObject(_) => Value::symbol(&string_text(method)?),
        method => method.value(),
    };
    let id = match normalized.kind() {
        Kind::Integer(id) => c_int::try_from(id).ok(),
        Kind::Symbol(name) => algorithm_ids(library.api.cipher_list)
            .into_iter()
            .find(|id| {
                // SAFETY: Every candidate came from `gnutls_cipher_list`.
                c_string(unsafe { (library.api.cipher_name)(*id) }).as_deref()
                    == Some(name.as_str())
            }),
        Kind::Cons(..) => {
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
    if value.is_string() || matches!(value.kind(), Kind::Buffer(_)) || value.is_cons() {
        Ok(())
    } else {
        Err(wrong_type_argument("consp", *value))
    }
}

fn clear_crypto_key(value: &Value) {
    let source = if value.is_string() {
        *value
    } else {
        safe_car(value)
    };
    if let Kind::StringObject(state) = source.kind() {
        let mut state = state.borrow_mut();
        state.clear();
    }
}

fn gnutls_error_description(library: &GnuTlsLibrary, code: c_int) -> String {
    // SAFETY: GnuTLS accepts every C integer at its error-string boundary.
    c_string(unsafe { (library.api.error_string)(code) }).unwrap_or_else(|| "unknown".into())
}

fn gnutls_format_certificate(cert: &Value) -> Result<Value, LispError> {
    let cert = string_like(cert)
        .map(|string| string.text)
        .ok_or_else(|| wrong_type_argument("stringp", *cert))?;
    let library = gnutls_library()?;
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

    let library = gnutls_library()?;
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
    let resolved = match error.kind() {
        Kind::Symbol(symbol) => match interp
            .get_symbol_property_of(&symbol, &cached_symbol!("gnutls-code").with(Clone::clone))
        {
            Some(code)
                if matches!(
                    code.kind(),
                    Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_)
                ) =>
            {
                code
            }
            _ => return Err("Symbol has no numeric gnutls-code property"),
        },
        error => error.value(),
    };
    let Kind::Integer(code) = resolved.kind() else {
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

/// gnutls.c's cryptographic inputs: a buffer or string is the spec
/// `list1' makes of it, anything else must be a cons (CHECK_CONS), and
/// the spec is fns.c:extract_data_from_object's (OBJECT START END
/// CODING-SYSTEM NOERROR), each element read by CAR_SAFE.
fn digest_input_bytes(interp: &mut Interpreter, input: &Value) -> Result<Vec<u8>, LispError> {
    let mut spec = if input.is_string() || matches!(input.kind(), Kind::Buffer(_)) {
        Value::list([*input])
    } else if input.is_cons() {
        *input
    } else {
        return Err(wrong_type_argument("consp", *input));
    };
    let mut fields = [
        safe_car(&spec),
        Value::Nil,
        Value::Nil,
        Value::Nil,
        Value::Nil,
    ];
    for field in &mut fields[1..] {
        spec = safe_cdr(&spec);
        *field = safe_car(&spec);
    }
    crate::lisp::primitives::md5_source_bytes(interp, &fields, &mut crate::lisp::types::Env::new())
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

/// data.c:Fcar: nil's car is nil, a cons's its car, anything else is
/// `listp''s error.
fn fcar(value: Value) -> Result<Value, LispError> {
    match value.cons_values() {
        Some((car, _)) => Ok(car),
        None if value.is_nil() => Ok(Value::Nil),
        None => Err(wrong_type_argument("listp", value)),
    }
}

/// data.c:Fcdr, as Fcar.
fn fcdr(value: Value) -> Result<Value, LispError> {
    match value.cons_values() {
        Some((_, cdr)) => Ok(cdr),
        None if value.is_nil() => Ok(Value::Nil),
        None => Err(wrong_type_argument("listp", value)),
    }
}

fn plist_has_key(value: &Value, wanted: &str) -> bool {
    value.to_vec().is_ok_and(|items| {
        items
            .iter()
            .step_by(2)
            .any(|item| matches!(item.kind(), Kind::Symbol(symbol) if symbol.is_named(wanted)))
    })
}

fn log_level(value: &Value) -> Option<c_int> {
    match value.kind() {
        Kind::Integer(value) => {
            Some((value).clamp(i64::from(c_int::MIN), i64::from(c_int::MAX)) as c_int)
        }
        Kind::BigInteger(value) => Some(value.to_i32().unwrap_or_else(|| {
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

/// The bytes of a GnuTLS size-probed getter, as gnutls.c reads them: a
/// first call with no buffer that must answer
/// GNUTLS_E_SHORT_MEMORY_BUFFER, then one into a buffer of the size it
/// gave, whose success leaves the size it wrote; None otherwise.
fn size_probed(mut get: impl FnMut(*mut c_void, &mut usize) -> c_int) -> Option<Vec<u8>> {
    let mut size = 0;
    if get(std::ptr::null_mut(), &mut size) != GNUTLS_E_SHORT_MEMORY_BUFFER {
        return None;
    }
    let mut buffer = vec![0_u8; size];
    if get(buffer.as_mut_ptr().cast(), &mut size) < 0 {
        return None;
    }
    buffer.truncate(size);
    Some(buffer)
}

/// gnutls.c:gnutls_hex_string: PREFIX, then each byte as two hex digits,
/// the bytes separated by colons.
fn gnutls_hex_string(bytes: &[u8], prefix: &str) -> Vec<u8> {
    let mut text = prefix.as_bytes().to_vec();
    for (index, byte) in bytes.iter().enumerate() {
        if index > 0 {
            text.push(b':');
        }
        text.extend_from_slice(format!("{byte:02x}").as_bytes());
    }
    text
}

/// alloc.c:make_string of BYTES (build_string of a C string's).
fn gnutls_string(interp: &Interpreter, bytes: Vec<u8>) -> Result<Value, LispError> {
    crate::lisp::primitives::strings::make_string(bytes).map_err(|error| {
        crate::lisp::primitives::strings::string_storage_error_shared(interp, error)
    })
}

/// build_string of a GnuTLS name (static NUL-terminated text), None for
/// a null one.
fn gnutls_name(interp: &Interpreter, pointer: *const c_char) -> Result<Option<Value>, LispError> {
    if pointer.is_null() {
        return Ok(None);
    }
    // SAFETY: GnuTLS names are static NUL-terminated strings.
    let bytes = unsafe { CStr::from_ptr(pointer) }.to_bytes().to_vec();
    gnutls_string(interp, bytes).map(Some)
}

/// gnutls.c: gmtime_r and strftime "%Y-%m-%d" of a certificate time.
fn x509_date(get_time: X509CrtGetTime, certificate: *mut c_void) -> Option<String> {
    // SAFETY: CERTIFICATE is a live imported X.509 handle.
    let timestamp = unsafe { get_time(certificate) };
    DateTime::<Utc>::from_timestamp(timestamp, 0).map(|date| date.format("%Y-%m-%d").to_string())
}

/// gnutls.c:emacs_gnutls_certificate_export_pem: the PEM text before its
/// NUL, nil when the probe asks for no buffer, an error for a failed
/// export.
fn x509_pem(
    interp: &Interpreter,
    api: &GnuTlsApi,
    certificate: *mut c_void,
) -> Result<Value, LispError> {
    let mut size = 0;
    // GNUTLS_X509_FMT_PEM = 1.
    // SAFETY: A null buffer is GnuTLS's size probe; CERTIFICATE is live.
    let error = unsafe { (api.x509_crt_export)(certificate, 1, std::ptr::null_mut(), &mut size) };
    let export_error = |error: c_int| {
        // SAFETY: gnutls_strerror takes any code.
        let text =
            unsafe { c_string((api.error_string)(error)) }.unwrap_or_else(|| "unknown".into());
        LispError::Signal(format!("GnuTLS certificate export error: {text}"))
    };
    if error == GNUTLS_E_SHORT_MEMORY_BUFFER {
        let mut buffer = vec![0_u8; size];
        // SAFETY: BUFFER holds SIZE writable bytes.
        let error =
            unsafe { (api.x509_crt_export)(certificate, 1, buffer.as_mut_ptr().cast(), &mut size) };
        if error < 0 {
            return Err(export_error(error));
        }
        let end = buffer
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(buffer.len());
        buffer.truncate(end);
        return gnutls_string(interp, buffer);
    }
    if error < 0 {
        return Err(export_error(error));
    }
    Ok(Value::Nil)
}

/// gnutls.c:emacs_gnutls_certificate_details of CERTIFICATE, in its
/// order: `:version' (when not an error), `:serial-number', `:issuer',
/// `:valid-from', `:valid-to', `:subject', `:public-key-algorithm' and
/// `:certificate-security-level', `:issuer-unique-id',
/// `:subject-unique-id', `:signature-algorithm', `:public-key-id',
/// `:public-key-id-sha256', `:certificate-id' and `:pem'.
fn certificate_details(
    interp: &Interpreter,
    api: &GnuTlsApi,
    certificate: *mut c_void,
) -> Result<Value, LispError> {
    let mut details = Vec::new();
    // SAFETY (each GnuTLS call below): CERTIFICATE is a live imported
    // X.509 handle; size-probed buffers are as long as GnuTLS asked.
    let version = unsafe { (api.x509_crt_get_version)(certificate) };
    if version >= 0 {
        details.extend([
            Value::symbol(":version"),
            Value::Integer(i64::from(version)),
        ]);
    }
    if let Some(serial) =
        size_probed(|buffer, size| unsafe { (api.x509_crt_get_serial)(certificate, buffer, size) })
    {
        details.extend([
            Value::symbol(":serial-number"),
            gnutls_string(interp, gnutls_hex_string(&serial, ""))?,
        ]);
    }
    if let Some(issuer) = size_probed(|buffer, size| unsafe {
        (api.x509_crt_get_issuer_dn)(certificate, buffer.cast(), size)
    }) {
        details.extend([Value::symbol(":issuer"), gnutls_string(interp, issuer)?]);
    }
    if let Some(valid_from) = x509_date(api.x509_crt_get_activation_time, certificate) {
        details.extend([
            Value::symbol(":valid-from"),
            gnutls_string(interp, valid_from.into_bytes())?,
        ]);
    }
    if let Some(valid_to) = x509_date(api.x509_crt_get_expiration_time, certificate) {
        details.extend([
            Value::symbol(":valid-to"),
            gnutls_string(interp, valid_to.into_bytes())?,
        ]);
    }
    if let Some(subject) = size_probed(|buffer, size| unsafe {
        (api.x509_crt_get_dn)(certificate, buffer.cast(), size)
    }) {
        details.extend([Value::symbol(":subject"), gnutls_string(interp, subject)?]);
    }
    let mut bits = 0;
    let public_key = unsafe { (api.x509_crt_get_pk_algorithm)(certificate, &mut bits) };
    if public_key >= 0 {
        if let Some(name) = gnutls_name(interp, unsafe { (api.pk_algorithm_name)(public_key) })? {
            details.extend([Value::symbol(":public-key-algorithm"), name]);
        }
        let parameter = unsafe { (api.pk_bits_to_sec_param)(public_key, bits) };
        if let Some(name) = gnutls_name(interp, unsafe { (api.sec_param_name)(parameter) })? {
            details.extend([Value::symbol(":certificate-security-level"), name]);
        }
    }
    if let Some(id) = size_probed(|buffer, size| unsafe {
        (api.x509_crt_get_issuer_unique_id)(certificate, buffer.cast(), size)
    }) {
        details.extend([
            Value::symbol(":issuer-unique-id"),
            gnutls_string(interp, id)?,
        ]);
    }
    if let Some(id) = size_probed(|buffer, size| unsafe {
        (api.x509_crt_get_subject_unique_id)(certificate, buffer.cast(), size)
    }) {
        details.extend([
            Value::symbol(":subject-unique-id"),
            gnutls_string(interp, id)?,
        ]);
    }
    let signature = unsafe { (api.x509_crt_get_signature_algorithm)(certificate) };
    if signature >= 0
        && let Some(name) = gnutls_name(interp, unsafe { (api.sign_algorithm_name)(signature) })?
    {
        details.extend([Value::symbol(":signature-algorithm"), name]);
    }
    if let Some(id) = size_probed(|buffer, size| unsafe {
        (api.x509_crt_get_key_id)(certificate, 0, buffer.cast(), size)
    }) {
        details.extend([
            Value::symbol(":public-key-id"),
            gnutls_string(interp, gnutls_hex_string(&id, "sha1:"))?,
        ]);
    }
    if let Some(id) = size_probed(|buffer, size| unsafe {
        (api.x509_crt_get_key_id)(certificate, GNUTLS_KEYID_USE_SHA256, buffer.cast(), size)
    }) {
        details.extend([
            Value::symbol(":public-key-id-sha256"),
            gnutls_string(interp, gnutls_hex_string(&id, "sha256:"))?,
        ]);
    }
    if let Some(fingerprint) = size_probed(|buffer, size| unsafe {
        (api.x509_crt_get_fingerprint)(certificate, GNUTLS_DIG_SHA1, buffer, size)
    }) {
        details.extend([
            Value::symbol(":certificate-id"),
            gnutls_string(interp, gnutls_hex_string(&fingerprint, "sha1:"))?,
        ]);
    }
    details.extend([Value::symbol(":pem"), x509_pem(interp, api, certificate)?]);
    Ok(Value::list(details))
}

/// Fgnutls_peer_status's warnings, in the order its list holds them:
/// each consed on as its bit of VERIFICATION is found (`:invalid' first),
/// then `:no-host-match' for CERTIFICATE_NOT_MATCHING in EXTRA and
/// `:self-signed' for a host certificate that is its own issuer.
fn peer_status_warnings(
    api: &GnuTlsApi,
    verification: c_uint,
    extra: u32,
    certificates: &[*mut c_void],
) -> Vec<&'static str> {
    let warning_bits = [
        (1 << 1, ":invalid"),
        (1 << 5, ":revoked"),
        (1 << 6, ":unknown-ca"),
        (1 << 7, ":not-ca"),
        (1 << 8, ":insecure"),
        (1 << 9, ":not-activated"),
        (1 << 10, ":expired"),
        (1 << 11, ":signature-failure"),
        (1 << 12, ":revocation-data-superseded"),
        (1 << 15, ":revocation-data-issued-in-future"),
        (1 << 16, ":signer-constraints-failure"),
        (1 << 18, ":purpose-mismatch"),
        (1 << 19, ":missing-ocsp-status"),
        (1 << 20, ":invalid-ocsp-status"),
    ];
    let mut warnings = Vec::new();
    for (bit, warning) in warning_bits {
        if verification & bit != 0 {
            warnings.push(warning);
        }
    }
    if extra & CERTIFICATE_NOT_MATCHING != 0 {
        warnings.push(":no-host-match");
    }
    // SAFETY: The stored host certificate is live while the process
    // holds it.
    if let Some(&host) = certificates.first()
        && unsafe { (api.x509_crt_check_issuer)(host, host) } != 0
    {
        warnings.push(":self-signed");
    }
    warnings.reverse();
    warnings
}

/// gnutls.c:Fgnutls_peer_status past its stage check: `:warnings' (each
/// warning consed on as its bit of p->gnutls_peer_verification is found,
/// then `:no-host-match' for p->gnutls_extra_peer_verification and
/// `:self-signed' for a host certificate that is its own issuer), the
/// stored certificates as `:certificates' and the first as
/// `:certificate', `:diffie-hellman-prime-bits' when positive,
/// `:key-exchange', `:protocol', `:cipher', `:mac', `:encrypt-then-mac'
/// (HAVE_GNUTLS_ETM_STATUS) and, up to TLS 1.2, `:safe-renegotiation'.
fn negotiated_peer_status(
    interp: &Interpreter,
    api: &GnuTlsApi,
    state: *mut c_void,
    verification: c_uint,
    extra_verification: u32,
    certificates: &[*mut c_void],
) -> Result<Value, LispError> {
    let mut warnings = Value::Nil;
    for warning in peer_status_warnings(api, verification, extra_verification, certificates)
        .into_iter()
        .rev()
    {
        warnings = Value::cons(Value::symbol(warning), warnings);
    }
    let mut result = Vec::new();
    if !warnings.is_nil() {
        result.extend([Value::symbol(":warnings"), warnings]);
    }
    if !certificates.is_empty() {
        let details = certificates
            .iter()
            .map(|&certificate| certificate_details(interp, api, certificate))
            .collect::<Result<Vec<_>, _>>()?;
        let host = details[0];
        result.extend([
            Value::symbol(":certificates"),
            Value::list(details),
            Value::symbol(":certificate"),
            host,
        ]);
    }
    // SAFETY (each call below): STATE is a live, successfully handshaken
    // session; the algorithm-name functions return static strings.
    let bits = unsafe { (api.dh_get_prime_bits)(state) };
    if bits > 0 {
        result.extend([
            Value::symbol(":diffie-hellman-prime-bits"),
            Value::Integer(i64::from(bits)),
        ]);
    }
    let protocol = unsafe { (api.protocol_get)(state) };
    let algorithms = [
        (":key-exchange", unsafe {
            (api.key_exchange_name)((api.key_exchange_get)(state))
        }),
        (":protocol", unsafe { (api.protocol_name)(protocol) }),
        (":cipher", unsafe {
            (api.cipher_name)((api.session_cipher_get)(state))
        }),
        (":mac", unsafe {
            (api.mac_name)((api.session_mac_get)(state))
        }),
    ];
    for (key, name) in algorithms {
        // gnutls.c builds fresh mutable strings for each report.
        if let Some(name) = gnutls_name(interp, name)? {
            result.extend([Value::symbol(key), name]);
        }
    }
    if let Some(etm_status) = api.session_etm_status {
        let etm = unsafe { etm_status(state) } != 0;
        result.extend([Value::symbol(":encrypt-then-mac"), Value::from_bool(etm)]);
    }
    if protocol <= GNUTLS_TLS1_2 {
        let safe = unsafe { (api.safe_renegotiation_status)(state) } != 0;
        result.extend([Value::symbol(":safe-renegotiation"), Value::from_bool(safe)]);
    }
    Ok(Value::list(result))
}

/// gnutls.c:boot_error: a non-blocking client's status (failed MESSAGE)
/// and nil, else `error' with MESSAGE.
#[cfg(unix)]
fn boot_error(
    interp: &mut Interpreter,
    process_id: u64,
    message: String,
) -> Result<Value, LispError> {
    if interp.network_setup(process_id).non_blocking_client {
        let data = Value::string(&message);
        interp.set_network_process_failed(
            process_id,
            data,
            false,
            Some(format!("failed with code {message}\n")),
        );
        return Ok(Value::Nil);
    }
    Err(LispError::Signal(message))
}

/// gnutls.c:gnutls_log_function, GnuTLS's log function once a
/// `:loglevel' was given: "gnutls.c: [LEVEL] STRING".
unsafe extern "C" fn gnutls_log_function(level: c_int, string: *const c_char) {
    let mut text = format!("gnutls.c: [{level}] ").into_bytes();
    if !string.is_null() {
        // SAFETY: GnuTLS passes a NUL-terminated message.
        text.extend_from_slice(unsafe { CStr::from_ptr(string) }.to_bytes());
    }
    crate::lisp::eval::push_gnutls_log(crate::lisp::eval::GnuTlsLogEntry::Shown(text));
}

/// gnutls.c:gnutls_audit_log_function: "gnutls.c: [audit] STRING" when
/// `gnutls-log-level' is at least 1.
unsafe extern "C" fn gnutls_audit_log_function(_session: *mut c_void, string: *const c_char) {
    let mut text = b"gnutls.c: [audit] ".to_vec();
    if !string.is_null() {
        // SAFETY: GnuTLS passes a NUL-terminated message.
        text.extend_from_slice(unsafe { CStr::from_ptr(string) }.to_bytes());
    }
    crate::lisp::eval::push_gnutls_log(crate::lisp::eval::GnuTlsLogEntry::Audit(text));
}

/// The gnutls.c `message's made where no Lisp could run, each shown as
/// vmessage shows it, in order: those weighed against
/// `gnutls-log-level' (clipped to an int) weighed now, as no Lisp has
/// changed it since.
pub(crate) fn flush_gnutls_log(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    use crate::lisp::eval::GnuTlsLogEntry;
    let entries = crate::lisp::eval::take_gnutls_log();
    if entries.is_empty() {
        return Ok(());
    }
    let global = interp
        .lookup_var_key(cached_symbol!("gnutls-log-level"), env)
        .and_then(|value| value.integer_value())
        .unwrap_or(0);
    let max = global.clamp(i64::from(c_int::MIN), i64::from(c_int::MAX)) as c_int;
    for entry in entries {
        let bytes = match entry {
            GnuTlsLogEntry::Shown(bytes) => bytes,
            GnuTlsLogEntry::AtGlobalLevel(level, bytes) if level <= max => bytes,
            GnuTlsLogEntry::Audit(bytes) if global >= 1 => bytes,
            GnuTlsLogEntry::AtGlobalLevel(..) | GnuTlsLogEntry::Audit(_) => continue,
        };
        super::display::vmessage(interp, bytes, env)?;
    }
    Ok(())
}

/// SSDATA of a Lisp string up to its first NUL, as a C string.
fn c_bytes(bytes: &[u8]) -> std::ffi::CString {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    std::ffi::CString::new(&bytes[..end]).unwrap_or_default()
}

/// ENCODE_FILE of the Lisp string NAME, its SSDATA as a C string.
fn encoded_file_name(
    interp: &mut Interpreter,
    env: &mut Env,
    name: Value,
) -> Result<std::ffi::CString, LispError> {
    let encoded = crate::lisp::primitives::system::encode_file_value(interp, env, name)?;
    Ok(c_bytes(&crate::lisp::primitives::font::string_sdata(
        &encoded,
    )?))
}

/// gnutls.c:gnutls_ip_address_p: whether HOST holds only dots, colons
/// and digits.
fn gnutls_ip_address_p(host: &[u8]) -> bool {
    host.iter()
        .all(|byte| *byte == b'.' || *byte == b':' || byte.is_ascii_digit())
}

/// Fmember of the keyword NAME in LIST: FOR_EACH_TAIL, an element `eq'
/// to it the answer, CHECK_LIST_END of the tail it ends on.
fn member_keyword(list: Value, name: &str) -> Result<bool, LispError> {
    let mut tail = list;
    let mut slow = list;
    let mut steps = 0_u64;
    while let Some((item, rest)) = tail.cons_values() {
        if matches!(item.kind(), Kind::Symbol(symbol) if symbol.is_named(name)) {
            return Ok(true);
        }
        tail = rest;
        steps += 1;
        if steps.is_multiple_of(2) {
            slow = safe_cdr(&slow);
            if slow == tail {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("circular-list"),
                    list,
                ])));
            }
        }
    }
    if !tail.is_nil() {
        return Err(wrong_type_argument("listp", list));
    }
    Ok(false)
}

/// gnutls.c:Fgnutls_boot in its order: CHECK_PROCESS, CHECK_SYMBOL of
/// TYPE, CHECK_LIST of PROPLIST; boot_error for no GnuTLS, a TYPE but
/// `gnutls-x509pki' or `gnutls-anon', a `:hostname' that is no string; an
/// integer `:loglevel' made GnuTLS's log level, with its log and audit
/// functions, and p->gnutls_log_level, which the boot's logging is
/// weighed against (else 0); "connecting to host:"; the global init,
/// whose error is the answer; emacs_gnutls_deinit, then the process
/// marked GNUTLS_STAGE_EMPTY; the credentials (GNUTLS_STAGE_CRED_ALLOC),
/// for X.509 with `:verify-flags', the system trust and each
/// `:trustfiles', `:crlfiles' and `:keylist' entry in turn, logged and
/// ENCODE_FILE'd (boot_error for one that is no string, a GnuTLS error
/// the answer); GNUTLS_STAGE_FILES and CALLBACKS; gnutls_init
/// (GNUTLS_NONBLOCK for a non-blocking client) and GNUTLS_STAGE_INIT; the
/// priority and GNUTLS_STAGE_PRIORITY; the prime bits, the credentials
/// and, for a host that is no IP address (gnutls_ip_address_p), the
/// server name; `:complete-negotiation' and GNUTLS_STAGE_CRED_SET;
/// emacs_gnutls_handshake, whose error is the answer; then
/// gnutls_verify_boot.
#[cfg(unix)]
pub(crate) fn gnutls_boot(
    interp: &mut Interpreter,
    env: &mut Env,
    process: &Value,
    credential_type: &Value,
    parameters: &Value,
) -> Result<Value, LispError> {
    use crate::lisp::eval::{
        GNUTLS_STAGE_CALLBACKS, GNUTLS_STAGE_CRED_ALLOC, GNUTLS_STAGE_FILES, GNUTLS_STAGE_INIT,
        GNUTLS_STAGE_PRIORITY, gnutls_log, gnutls_log2, gnutls_log2i,
    };
    use crate::lisp::primitives::font::string_sdata;
    let process_id = interp.resolve_process_id(process)?;
    let Kind::Symbol(credential_type) = credential_type.kind() else {
        return Err(wrong_type_argument("symbolp", *credential_type));
    };
    if !parameters.is_list() {
        return Err(wrong_type_argument("listp", *parameters));
    }
    let Ok(library) = gnutls_library() else {
        return boot_error(interp, process_id, "GnuTLS not available".into());
    };
    let is_x509 = match credential_type.as_str() {
        "gnutls-x509pki" => true,
        "gnutls-anon" => false,
        _ => return boot_error(interp, process_id, "Invalid GnuTLS credential type".into()),
    };
    let hostname_value = contact_plist_get(parameters, ":hostname");
    if string_like(&hostname_value).is_none() {
        return boot_error(
            interp,
            process_id,
            "gnutls-boot: invalid :hostname parameter (not a string)".into(),
        );
    }
    let hostname = c_bytes(&string_sdata(&hostname_value)?);
    let transport = interp.network_socket_fd(process_id).unwrap_or(-1);
    let api = library.api;

    let mut max_log_level = 0;
    if let Some(level) = log_level(&contact_plist_get(parameters, ":loglevel")) {
        // SAFETY: the functions are plain C callbacks and the level an
        // unconstrained C integer.
        unsafe {
            (api.global_set_log_function)(Some(gnutls_log_function));
            if let Some(set_audit_log_function) = api.global_set_audit_log_function {
                set_audit_log_function(Some(gnutls_audit_log_function));
            }
            (api.global_set_log_level)(level);
        }
        max_log_level = level;
        interp.set_process_gnutls_log_level(process_id, level);
    }
    gnutls_log2(1, max_log_level, "connecting to host:", hostname.as_bytes());

    let result = library.initialize();
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    // Before allocating new credentials, deallocate any credentials
    // that PROC might already have.
    let _ = interp.deinit_process_gnutls(process_id);
    // Mark PROC as a GnuTLS process.
    interp.reset_process_gnutls(process_id);

    gnutls_log(1, max_log_level, "allocating credentials");
    let mut credential_pointer = std::ptr::null_mut();
    let result = if is_x509 {
        gnutls_log(2, max_log_level, "allocating x509 credentials");
        // SAFETY: the allocator initializes CREDENTIAL_POINTER on success.
        unsafe { (api.certificate_allocate)(&mut credential_pointer) }
    } else {
        gnutls_log(2, max_log_level, "allocating anon credentials");
        // SAFETY: as above.
        unsafe { (api.anon_allocate)(&mut credential_pointer) }
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
        let verify_flags = contact_plist_get(parameters, ":verify-flags");
        // GNUTLS_VERIFY_ALLOW_X509_V1_CA_CRT, 0 on current GnuTLS.
        let flags = match verify_flags.kind() {
            Kind::Integer(flags) if (0..=i64::from(c_uint::MAX)).contains(&flags) => {
                gnutls_log(2, max_log_level, "setting verification flags");
                flags as c_uint
            }
            _ if verify_flags.is_nil() => {
                gnutls_log(2, max_log_level, "using default verification flags");
                0
            }
            _ => {
                gnutls_log(2, max_log_level, "ignoring invalid verify-flags");
                0
            }
        };
        // SAFETY: CREDENTIAL is a live X.509 credential handle.
        unsafe { (api.certificate_verify_flags)(credential.pointer, flags) };
    }
    interp.set_process_gnutls_initstage(process_id, GNUTLS_STAGE_CRED_ALLOC);

    if is_x509 {
        if let Some(system_trust) = api.certificate_system_trust {
            // SAFETY: CREDENTIAL is a live X.509 credential handle.
            let result = unsafe { system_trust(credential.pointer) };
            if result < 0 {
                gnutls_log2i(
                    4,
                    max_log_level,
                    "setting system trust failed with code ",
                    result,
                );
            }
        }
        let mut tail = contact_plist_get(parameters, ":trustfiles");
        while let Some((trustfile, rest)) = tail.cons_values() {
            tail = rest;
            if string_like(&trustfile).is_none() {
                let _ = interp.deinit_process_gnutls(process_id);
                return boot_error(interp, process_id, "Invalid trustfile".into());
            }
            gnutls_log2(
                1,
                max_log_level,
                "setting the trustfile: ",
                c_bytes(&string_sdata(&trustfile)?).as_bytes(),
            );
            let trustfile = encoded_file_name(interp, env, trustfile)?;
            // SAFETY: CREDENTIAL is live and TRUSTFILE is NUL-terminated;
            // GNUTLS_X509_FMT_PEM is 1.
            let result =
                unsafe { (api.certificate_trust_file)(credential.pointer, trustfile.as_ptr(), 1) };
            if result < 0 {
                return Ok(gnutls_result(result));
            }
        }
        let mut tail = contact_plist_get(parameters, ":crlfiles");
        while let Some((crlfile, rest)) = tail.cons_values() {
            tail = rest;
            if string_like(&crlfile).is_none() {
                let _ = interp.deinit_process_gnutls(process_id);
                return boot_error(interp, process_id, "Invalid CRL file".into());
            }
            gnutls_log2(
                1,
                max_log_level,
                "setting the CRL file: ",
                c_bytes(&string_sdata(&crlfile)?).as_bytes(),
            );
            let crlfile = encoded_file_name(interp, env, crlfile)?;
            // SAFETY: CREDENTIAL is live and CRLFILE is NUL-terminated.
            let result =
                unsafe { (api.certificate_crl_file)(credential.pointer, crlfile.as_ptr(), 1) };
            if result < 0 {
                return Ok(gnutls_result(result));
            }
        }
        let pass_present = plist_has_key(parameters, ":pass");
        let pass_value = contact_plist_get(parameters, ":pass");
        let pass = match string_like(&pass_value) {
            Some(_) => Some(c_bytes(&string_sdata(&pass_value)?)),
            None => None,
        };
        let flags = key_file_flags(&contact_plist_get(parameters, ":flags"));
        let mut keylist = contact_plist_get(parameters, ":keylist");
        while let Some((pair, rest)) = keylist.cons_values() {
            keylist = rest;
            // Fcar (XCAR (tail)) and Fcar (Fcdr (XCAR (tail))).
            let keyfile = fcar(pair)?;
            let certfile = fcar(fcdr(pair)?)?;
            if string_like(&keyfile).is_none() || string_like(&certfile).is_none() {
                let message = if string_like(&keyfile).is_some() {
                    "Invalid client cert file"
                } else {
                    "Invalid client key file"
                };
                let _ = interp.deinit_process_gnutls(process_id);
                return boot_error(interp, process_id, message.into());
            }
            gnutls_log2(
                1,
                max_log_level,
                "setting the client key file: ",
                c_bytes(&string_sdata(&keyfile)?).as_bytes(),
            );
            gnutls_log2(
                1,
                max_log_level,
                "setting the client cert file: ",
                c_bytes(&string_sdata(&certfile)?).as_bytes(),
            );
            let keyfile = encoded_file_name(interp, env, keyfile)?;
            let certfile = encoded_file_name(interp, env, certfile)?;
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

    interp.set_process_gnutls_initstage(process_id, GNUTLS_STAGE_FILES);
    gnutls_log(1, max_log_level, "gnutls callbacks");
    interp.set_process_gnutls_initstage(process_id, GNUTLS_STAGE_CALLBACKS);

    gnutls_log(1, max_log_level, "gnutls_init");
    let mut gnutls_flags = GNUTLS_CLIENT;
    if interp.network_setup(process_id).non_blocking_client {
        gnutls_flags |= GNUTLS_NONBLOCK;
    }
    let mut state_pointer = std::ptr::null_mut();
    // SAFETY: SESSION_INIT initializes STATE_POINTER as a client session.
    let result = unsafe { (api.session_init)(&mut state_pointer, gnutls_flags) };
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    let state = ForeignHandle {
        pointer: state_pointer,
        free: api.session_deinit,
    };
    interp.set_process_gnutls_initstage(process_id, GNUTLS_STAGE_INIT);

    let priority_value = contact_plist_get(parameters, ":priority");
    let priority = if string_like(&priority_value).is_some() {
        let priority = c_bytes(&string_sdata(&priority_value)?);
        gnutls_log2(
            1,
            max_log_level,
            "got non-default priority string:",
            priority.as_bytes(),
        );
        priority
    } else {
        gnutls_log2(
            1,
            max_log_level,
            "using default priority string:",
            b"NORMAL",
        );
        c_bytes(b"NORMAL")
    };
    gnutls_log(1, max_log_level, "setting the priority string");
    // SAFETY: STATE is live and PRIORITY is NUL-terminated.
    let result = unsafe {
        (api.priority_set_direct)(state.pointer, priority.as_ptr(), std::ptr::null_mut())
    };
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    interp.set_process_gnutls_initstage(process_id, GNUTLS_STAGE_PRIORITY);

    if let Kind::Integer(bits) = contact_plist_get(parameters, ":min-prime-bits").kind() {
        // XUFIXNUM, narrowed to unsigned int as C narrows it.
        // SAFETY: STATE is a live initialized session.
        unsafe { (api.dh_set_prime_bits)(state.pointer, bits as c_uint) };
    }
    // SAFETY: Both handles are live and the credential type matches its
    // allocator (GNUTLS_CRD_CERTIFICATE 1, GNUTLS_CRD_ANON 2).
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
    if !gnutls_ip_address_p(hostname.as_bytes()) {
        let name = hostname.as_bytes();
        // SAFETY: STATE is live and NAME remains borrowed for this call;
        // GNUTLS_NAME_DNS is 1.
        let result =
            unsafe { (api.server_name_set)(state.pointer, 1, name.as_ptr().cast(), name.len()) };
        if result < 0 {
            return Ok(gnutls_result(result));
        }
    }
    let session = ProcessGnuTlsSession::new(
        library.clone(),
        state.into_raw(),
        credential.into_raw(),
        is_x509,
        GnuTlsSessionApi {
            session_deinit: api.session_deinit,
            credential_deinit: credential_free,
            record_recv: api.record_recv,
            record_send: api.record_send,
            record_check_pending: api.record_check_pending,
            handshake: api.handshake,
            bye: api.bye,
            error_is_fatal: api.error_is_fatal,
            strerror: api.error_string,
            alert_get: api.alert_get,
            alert_get_name: api.alert_get_name,
        },
        false,
    );
    let complete = !contact_plist_get(parameters, ":complete-negotiation").is_nil();
    interp.install_process_gnutls(process_id, session, complete)?;
    // emacs_gnutls_handshake: the socket as the transport (and, for a
    // non-blocking client, emacs_gnutls_nonblock_errno), then
    // gnutls_try_handshake.
    if let Some(state) = interp.process_gnutls_raw_state(process_id) {
        // SAFETY: STATE is the live session just installed; GnuTLS's
        // socket transport reads the pointer as the descriptor.
        unsafe { (api.transport_set_ptr)(state, transport as isize as *mut c_void) };
        if interp.network_setup(process_id).non_blocking_client
            && let Some(set_errno_function) = api.transport_set_errno_function
        {
            // SAFETY: as above; the function is a plain C callback.
            unsafe { set_errno_function(state, Some(emacs_gnutls_nonblock_errno)) };
        }
    }
    interp.set_process_gnutls_initstage(
        process_id,
        crate::lisp::eval::GNUTLS_STAGE_TRANSPORT_POINTERS_SET,
    );
    let result = gnutls_try_handshake(interp, env, process_id)?;
    if result < 0 {
        return Ok(gnutls_result(result));
    }
    gnutls_verify_boot(interp, process_id, *parameters)
}

/// gnutls.c:emacs_gnutls_nonblock_errno: a connect still under way is
/// EAGAIN to GnuTLS.
unsafe extern "C" fn emacs_gnutls_nonblock_errno(_transport: *mut c_void) -> c_int {
    match std::io::Error::last_os_error().raw_os_error().unwrap_or(0) {
        libc::EINPROGRESS | libc::ENOTCONN => libc::EAGAIN,
        error => error,
    }
}

/// gnutls.c:gnutls_try_handshake: gnutls_handshake until it is done or
/// fails fatally, maybe_quit between tries; a non-blocking client but
/// for `:complete-negotiation' (which sets p->gnutls_p) stops at any
/// error but GNUTLS_E_INTERRUPTED, any other sleeps 10 ms between tries.
/// The stage is then GNUTLS_STAGE_HANDSHAKE_TRIED, READY once done.
#[cfg(unix)]
pub(crate) fn gnutls_try_handshake(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
) -> Result<c_int, LispError> {
    let non_blocking = interp.network_setup(process_id).non_blocking_client
        && !interp.process_gnutls_complete_negotiation(process_id);
    if non_blocking {
        interp.set_process_gnutls_p(process_id, true);
    }
    let result = loop {
        let Some((result, fatal)) = interp.process_gnutls_handshake_step(process_id) else {
            break GNUTLS_E_INVALID_SESSION;
        };
        if result >= 0 || fatal {
            break result;
        }
        flush_gnutls_log(interp, env)?;
        interp.maybe_quit(env)?;
        if non_blocking && result != GNUTLS_E_INTERRUPTED {
            break result;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    };
    interp.set_process_gnutls_initstage(
        process_id,
        if result == 0 {
            crate::lisp::eval::GNUTLS_STAGE_READY
        } else {
            crate::lisp::eval::GNUTLS_STAGE_HANDSHAKE_TRIED
        },
    );
    Ok(result)
}

/// gnutls.c:gnutls_verify_boot: PROPLIST, nil meaning the cdr of the
/// contact's `:tls-parameters'; boot_error for a `:verify-error' that is
/// neither t nor a list and a `:hostname' that is no string; then
/// gnutls_certificate_verify_peers2, whose error is the answer, kept as
/// p->gnutls_peer_verification; each warning `gnutls-peer-status' gives
/// logged ("verification:"); a failed verification that `:verify-error'
/// rejects (t, or a list with `:trustfiles') is emacs_gnutls_deinit and
/// boot_error ("Certificate validation failed HOST, verification code
/// HEX"), else logged; for an X.509 session the peer's chain ("No x509
/// certificate was found\n" when there is none) imported, the last
/// first, and kept as p->gnutls_certificates (a failed import the
/// answer, the chain freed), the host certificate checked against HOST,
/// a mismatch CERTIFICATE_NOT_MATCHING in
/// p->gnutls_extra_peer_verification, which `:verify-error' rejects (t,
/// or a list with `:hostname'; "The x509 certificate does not match
/// \"HOST\""), else logged; then p->gnutls_p, and gnutls_make_error of
/// the last result.
#[cfg(unix)]
pub(crate) fn gnutls_verify_boot(
    interp: &mut Interpreter,
    process_id: u64,
    proplist: Value,
) -> Result<Value, LispError> {
    use crate::lisp::eval::gnutls_log2;
    let max_log_level = interp.process_gnutls_log_level(process_id);
    let proplist = if proplist.is_nil() {
        let contact = interp
            .process_contact_plist(process_id)
            .unwrap_or(Value::Nil);
        fcdr(contact_plist_get(&contact, ":tls-parameters"))?
    } else {
        proplist
    };
    let verify_error = contact_plist_get(&proplist, ":verify-error");
    let hostname_value = contact_plist_get(&proplist, ":hostname");
    let verify_error_all = verify_error == Value::T;
    if !verify_error_all && !verify_error.is_list() {
        return boot_error(
            interp,
            process_id,
            "gnutls-boot: invalid :verify_error parameter (not a list)".into(),
        );
    }
    let Some(host_text) = string_like(&hostname_value).map(|string| string.text) else {
        return boot_error(
            interp,
            process_id,
            "gnutls-boot: invalid :hostname parameter (not a string)".into(),
        );
    };
    // c_hostname, SSDATA as C reads it: up to its first NUL.
    let host_text = host_text.split('\0').next().unwrap_or_default().to_string();
    let hostname = c_bytes(&crate::lisp::primitives::font::string_sdata(
        &hostname_value,
    )?);
    let library = gnutls_library()?;
    let api = library.api;
    let Some(state) = interp.process_gnutls_raw_state(process_id) else {
        return Ok(gnutls_result(GNUTLS_E_INVALID_SESSION));
    };
    let mut peer_verification = 0;
    // SAFETY: STATE is the process's live session and the status writable.
    let mut ret = unsafe { (api.certificate_verify_peers2)(state, &mut peer_verification) };
    if ret < 0 {
        return Ok(gnutls_result(ret));
    }
    interp.set_process_gnutls_peer_verification(process_id, peer_verification);
    // warnings = plist_get (Fgnutls_peer_status (proc), QCwarnings).
    if let Some((_, verification, extra, certificates)) =
        interp.process_gnutls_peer_state(process_id)
    {
        for warning in peer_status_warnings(&api, verification, extra, &certificates) {
            if let Some(message) = peer_status_warning_description(warning) {
                gnutls_log2(1, max_log_level, "verification:", message.as_bytes());
            }
        }
    }
    if peer_verification != 0 {
        if verify_error_all || member_keyword(verify_error, ":trustfiles")? {
            let _ = interp.deinit_process_gnutls(process_id);
            return boot_error(
                interp,
                process_id,
                format!(
                    "Certificate validation failed {host_text}, verification code {peer_verification:x}"
                ),
            );
        }
        gnutls_log2(
            1,
            max_log_level,
            "certificate validation failed:",
            hostname.as_bytes(),
        );
    }
    // Up to here the process is the same for X.509 certificates and
    // OpenPGP keys.
    // SAFETY: STATE is live.
    if unsafe { (api.certificate_type_get)(state) } == GNUTLS_CRT_X509 {
        let mut count = 0;
        // SAFETY: STATE completed its handshake; GnuTLS owns the datum
        // array for the session's lifetime.
        let peers = unsafe { (api.certificate_get_peers)(state, &mut count) };
        if peers.is_null() {
            let _ = interp.deinit_process_gnutls(process_id);
            return boot_error(interp, process_id, "No x509 certificate was found\n".into());
        }
        // SAFETY: GnuTLS returned COUNT consecutive datum records.
        let peers = unsafe { std::slice::from_raw_parts(peers, count as usize) };
        // "Check only the first certificate in the given chain, but store
        // them all."
        let mut handles = vec![std::ptr::null_mut(); peers.len()];
        let mut failed_import = 0;
        for index in (0..peers.len()).rev() {
            let mut certificate = std::ptr::null_mut();
            // SAFETY: GnuTLS initializes one certificate handle (its
            // result unchecked, as gnutls.c leaves it).
            unsafe { (api.x509_crt_init)(&mut certificate) };
            if ret < 0 {
                failed_import = ret;
            } else {
                // GNUTLS_X509_FMT_DER is 0.
                // SAFETY: the datum is the session's and CERTIFICATE live.
                ret = unsafe { (api.x509_crt_import)(certificate, &peers[index], 0) };
                if ret < 0 {
                    failed_import = ret;
                }
            }
            handles[index] = certificate;
        }
        let host_certificate = handles.first().copied();
        interp.set_process_gnutls_certificates(
            process_id,
            Some(crate::lisp::eval::GnuTlsCertificates::new(
                library.clone(),
                handles,
                api.x509_crt_deinit,
            )),
        );
        if failed_import != 0 {
            interp.set_process_gnutls_certificates(process_id, None);
            return Ok(gnutls_result(failed_import));
        }
        if let Some(host_certificate) = host_certificate {
            // SAFETY: the host certificate is live while the process holds
            // the chain; HOSTNAME is NUL-terminated.
            let matched =
                unsafe { (api.x509_crt_check_hostname)(host_certificate, hostname.as_ptr()) };
            if matched == 0 {
                interp.add_process_gnutls_extra_peer_verification(
                    process_id,
                    CERTIFICATE_NOT_MATCHING,
                );
                if verify_error_all || member_keyword(verify_error, ":hostname")? {
                    let _ = interp.deinit_process_gnutls(process_id);
                    return boot_error(
                        interp,
                        process_id,
                        format!("The x509 certificate does not match \"{host_text}\""),
                    );
                }
                gnutls_log2(
                    1,
                    max_log_level,
                    "x509 certificate does not match:",
                    hostname.as_bytes(),
                );
            }
        }
    }
    // Set this flag only if the whole initialization succeeded.
    interp.set_process_gnutls_p(process_id, true);
    Ok(gnutls_result(ret))
}

#[cfg(not(unix))]
pub(crate) fn gnutls_boot(
    interp: &mut Interpreter,
    _env: &mut Env,
    process: &Value,
    credential_type: &Value,
    parameters: &Value,
) -> Result<Value, LispError> {
    interp.resolve_process_id(process)?;
    if !matches!(credential_type.kind(), Kind::Symbol(_)) {
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
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "gnutls-available-p" => {
                need_args(name, args, 0)?;
                let Ok(library) = gnutls_library() else {
                    return Ok(Value::Nil);
                };
                let version = encoded_library_version(&library.api);
                if option_env!("EMAXX_GNUTLS_VERSION").is_none() {
                    interp.set_global_binding_key(
                        cached_symbol!("libgnutls-version"),
                        Value::Integer(version.unwrap_or(-1)),
                    );
                }
                // gnutls.c conses its capabilities in this order, and then
                // each extension name GnuTLS knows below ID 100.
                let mut capabilities = Value::list([Value::symbol("gnutls")]);
                if version.is_some_and(|version| version >= 30205) {
                    capabilities = Value::cons(Value::symbol("ClientHello Padding"), capabilities);
                }
                for capability in ["gnutls3", "digests", "ciphers"] {
                    capabilities = Value::cons(Value::symbol(capability), capabilities);
                }
                if library.api.aead_init.is_some()
                    && library.api.aead_encrypt.is_some()
                    && library.api.aead_decrypt.is_some()
                    && library.api.aead_deinit.is_some()
                {
                    capabilities = Value::cons(Value::symbol("AEAD-ciphers"), capabilities);
                }
                capabilities = Value::cons(Value::symbol("macs"), capabilities);
                if let Some(extension_name) = library.api.extension_name {
                    for extension in 0..100 {
                        // SAFETY: gnutls_ext_get_name takes any ID and returns
                        // null or a library-owned NUL-terminated name.
                        let name = unsafe { extension_name(extension) };
                        if name.is_null() {
                            continue;
                        }
                        // SAFETY: Checked non-null above; the name is static.
                        let name = unsafe { CStr::from_ptr(name) }.to_string_lossy();
                        if !crate::lisp::primitives::hooks_overlays::memq(&*name, capabilities) {
                            capabilities = Value::cons(Value::symbol(&name), capabilities);
                        }
                    }
                }
                Ok(capabilities)
            }
            "gnutls-asynchronous-parameters" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp.set_process_gnutls_boot_parameters(process_id, args[1]);
                Ok(Value::Nil)
            }
            "gnutls-boot" => {
                need_args(name, args, 3)?;
                let result = gnutls_boot(interp, env, &args[0], &args[1], &args[2]);
                #[cfg(unix)]
                flush_gnutls_log(interp, env)?;
                result
            }
            "gnutls-bye" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let result = interp
                    .process_gnutls_bye(process_id, args[1].is_truthy())
                    .map(gnutls_result);
                #[cfg(unix)]
                flush_gnutls_log(interp, env)?;
                result
            }
            "gnutls-ciphers" => {
                need_args(name, args, 0)?;
                {
                    let library = gnutls_library()?;
                    Ok(cipher_catalog(&library))
                }
            }
            "gnutls-deinit" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let deinit = interp.deinit_process_gnutls(process_id) == Some(true);
                #[cfg(unix)]
                flush_gnutls_log(interp, env)?;
                Ok(Value::from_bool(deinit))
            }
            "gnutls-digests" => {
                need_args(name, args, 0)?;
                let library = gnutls_library()?;
                Ok(digest_catalog(&library))
            }
            "gnutls-error-fatalp" => {
                need_args(name, args, 1)?;
                if args[0] == Value::T {
                    return Ok(Value::Nil);
                }
                let code = gnutls_error_code(interp, &args[0])
                    .map_err(|message| LispError::Signal(message.into()))?;
                let library = gnutls_library()?;
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
                let library = gnutls_library()?;
                // SAFETY: `code` is a C int, the complete input domain accepted by
                // GnuTLS's pure error-description API.
                let description = c_string(unsafe { (library.api.error_string)(code) })
                    .unwrap_or_else(|| "unknown".into());
                Ok(Value::string(&description))
            }
            "gnutls-errorp" => {
                need_args(name, args, 1)?;
                Ok(
                    if matches!(args[0].kind(), Kind::T)
                        || crate::lisp::types::eq_symbol_named(args[0], "gnutls-e-again")
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
                let library = gnutls_library()?;
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
                {
                    let library = gnutls_library()?;
                    Ok(mac_catalog(&library))
                }
            }
            "gnutls-peer-status" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let Some((state, verification, extra, certificates)) =
                    interp.process_gnutls_peer_state(process_id)
                else {
                    return Ok(Value::Nil);
                };
                // gnutls.c:Fgnutls_peer_status constructs a new Lisp report
                // from the session on each call. No report is a process root,
                // and a caller's mutation cannot change a later query.
                let library = gnutls_library()?;
                let status = negotiated_peer_status(
                    interp,
                    &library.api,
                    state,
                    verification,
                    extra,
                    &certificates,
                );
                // What GnuTLS logged while the report was made.
                #[cfg(unix)]
                flush_gnutls_log(interp, env)?;
                status
            }
            "gnutls-peer-status-warning-describe" => {
                need_args(name, args, 1)?;
                let Kind::Symbol(status) = args[0].kind() else {
                    return Err(wrong_type_argument("symbolp", args[0]));
                };
                Ok(peer_status_warning_description(status.as_str())
                    .map(Value::string)
                    .unwrap_or(Value::Nil))
            }
            "gnutls-format-certificate" => {
                need_args(name, args, 1)?;
                let formatted = gnutls_format_certificate(&args[0]);
                #[cfg(unix)]
                flush_gnutls_log(interp, env)?;
                formatted
            }
            "gnutls-symmetric-decrypt" | "gnutls-symmetric-encrypt" => {
                need_arg_range(name, args, 4, 5)?;
                gnutls_symmetric(interp, args, name == "gnutls-symmetric-encrypt")
            }
        }
    }
);
