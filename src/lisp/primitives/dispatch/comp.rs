use super::*;

const NATIVE_COMPILER_UNAVAILABLE: &str = "Native compiler backend is unavailable";

fn native_compiler_unavailable() -> LispError {
    LispError::Signal(NATIVE_COMPILER_UNAVAILABLE.into())
}

fn string_argument(value: &Value) -> Result<String, LispError> {
    string_like(value)
        .map(|string| string.text)
        .ok_or_else(|| wrong_type_argument("stringp", value.clone()))
}

fn md5_prefix(bytes: &[u8]) -> String {
    format!("{:x}", md5::compute(bytes))
        .chars()
        .take(8)
        .collect()
}

fn source_bytes(path: &Path) -> Result<Vec<u8>, LispError> {
    let file = fs::File::open(path).map_err(|error| {
        LispError::SignalValue(Value::list([
            Value::symbol("file-error"),
            Value::string("Opening source file"),
            Value::String(error.to_string()),
            Value::String(path.display().to_string()),
        ]))
    })?;
    let mut bytes = Vec::new();
    let result = if path.extension().is_some_and(|extension| extension == "gz") {
        GzDecoder::new(file).read_to_end(&mut bytes)
    } else {
        std::io::BufReader::new(file).read_to_end(&mut bytes)
    };
    result.map_err(|_| {
        LispError::SignalValue(Value::list([
            Value::symbol("file-notify-error"),
            Value::string("hashing failed"),
            Value::String(path.display().to_string()),
        ]))
    })?;
    Ok(bytes)
}

fn source_basename(path: &str) -> Result<String, LispError> {
    let uncompressed = path.strip_suffix(".gz").unwrap_or(path);
    let basename = file_name_nondirectory(uncompressed);
    let length = basename.chars().count();
    if length < 3 {
        return Err(LispError::Signal("Args out of range".into()));
    }
    Ok(basename.chars().take(length - 3).collect())
}

fn comp_el_to_eln_rel_filename(
    interp: &mut Interpreter,
    filename: &Value,
    env: &mut Env,
) -> Result<String, LispError> {
    let filename = string_argument(filename)?;
    let expanded = expand_file_name_runtime(interp, env, &filename, None)?;
    let canonical = fs::canonicalize(&expanded).map_err(|_| {
        LispError::SignalValue(Value::list([
            Value::symbol("file-missing"),
            Value::String(expanded.clone()),
        ]))
    })?;
    let canonical = canonical.display().to_string();
    let content_hash = md5_prefix(&source_bytes(Path::new(&canonical))?);
    let hash_path = canonical.strip_suffix(".gz").unwrap_or(&canonical);
    let path_hash = md5_prefix(hash_path.as_bytes());
    Ok(format!(
        "{}-{path_hash}-{content_hash}.eln",
        source_basename(hash_path)?
    ))
}

fn comp_el_to_eln_filename(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let relative = comp_el_to_eln_rel_filename(interp, &args[0], env)?;
    let base = match args.get(1).filter(|value| value.is_truthy()) {
        Some(base) => string_argument(base)?,
        None => {
            let temporary_directory = interp
                .lookup_var("temporary-file-directory", env)
                .and_then(|value| string_like(&value).map(|value| value.text))
                .unwrap_or_else(|| std::env::temp_dir().display().to_string());
            expand_file_name_runtime(interp, env, "eln-cache", Some(&temporary_directory))?
        }
    };
    let invocation_directory = interp
        .lookup_var("invocation-directory", env)
        .and_then(|value| string_like(&value).map(|value| value.text));
    let mut directory =
        expand_file_name_runtime(interp, env, &base, invocation_directory.as_deref())?;
    if let Some(version) = interp
        .lookup_var("comp-native-version-dir", env)
        .filter(Value::is_truthy)
    {
        directory =
            expand_file_name_runtime(interp, env, &string_argument(&version)?, Some(&directory))?;
    }
    Ok(Value::String(expand_file_name_runtime(
        interp,
        env,
        &relative,
        Some(&directory),
    )?))
}

fn native_elisp_load(
    interp: &mut Interpreter,
    filename: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let filename = string_argument(filename)?;
    let expanded = expand_file_name_runtime(interp, env, &filename, None)?;
    if !Path::new(&expanded).exists() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("native-lisp-load-failed"),
            Value::string("file does not exists"),
            Value::String(filename),
        ])));
    }
    Err(LispError::SignalValue(Value::list([
        Value::symbol("native-lisp-load-failed"),
        Value::String(filename),
        Value::string(NATIVE_COMPILER_UNAVAILABLE),
    ])))
}

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "comp--compile-ctxt-to-file0"
            | "comp--init-ctxt"
            | "comp--install-trampoline"
            | "comp--late-register-subr"
            | "comp--register-lambda"
            | "comp--register-subr"
            | "comp--release-ctxt"
            | "comp-el-to-eln-filename"
            | "comp-el-to-eln-rel-filename"
            | "native-elisp-load"
    )
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    match name {
        "comp-el-to-eln-rel-filename" => {
            need_args(name, args, 1)?;
            comp_el_to_eln_rel_filename(interp, &args[0], env).map(Value::String)
        }
        "comp-el-to-eln-filename" => {
            need_arg_range(name, args, 1, 2)?;
            comp_el_to_eln_filename(interp, args, env)
        }
        "comp--release-ctxt" => {
            need_args(name, args, 0)?;
            Ok(Value::T)
        }
        "comp--init-ctxt" => {
            need_args(name, args, 0)?;
            Err(native_compiler_unavailable())
        }
        "comp--compile-ctxt-to-file0" => {
            need_args(name, args, 1)?;
            string_argument(&args[0])?;
            Err(native_compiler_unavailable())
        }
        "comp--install-trampoline" => {
            need_args(name, args, 2)?;
            let Value::Symbol(symbol) = &args[0] else {
                return Err(wrong_type_argument("symbolp", args[0].clone()));
            };
            if !matches!(args[1], Value::BuiltinFunc(_)) {
                return Err(wrong_type_argument("subrp", args[1].clone()));
            }
            let original = interp.lookup_function(symbol, env)?;
            if !matches!(original, Value::BuiltinFunc(_)) {
                return Err(wrong_type_argument("subrp", original));
            }
            Err(native_compiler_unavailable())
        }
        "comp--register-lambda" | "comp--register-subr" | "comp--late-register-subr" => {
            need_args(name, args, 7)?;
            Err(native_compiler_unavailable())
        }
        "native-elisp-load" => {
            need_arg_range(name, args, 1, 2)?;
            native_elisp_load(interp, &args[0], env)
        }
        _ => unreachable!("unhandled native compiler builtin {name}"),
    }
}
