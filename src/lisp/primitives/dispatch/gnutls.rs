use super::*;

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
            | "gnutls-deinit"
            | "gnutls-digests"
            | "gnutls-errorp"
            | "gnutls-get-initstage"
            | "gnutls-hash-digest"
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
