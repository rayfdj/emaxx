use super::*;

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
            Value::String(error.to_string().into()),
            Value::String(path.display().to_string().into()),
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
            Value::String(path.display().to_string().into()),
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

fn normalized_loadsearch_path(interp: &Interpreter, filename: &str, env: &Env) -> String {
    let source = Path::new(filename);
    let load_path = interp
        .lookup_var("load-path", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    for entry in load_path {
        let Some(directory) = string_like(&entry).map(|string| PathBuf::from(string.text)) else {
            continue;
        };
        // comp.c replaces both the configured dump Lisp directory and the
        // versioned installed Lisp directory with `//'.  The root `lisp'
        // entry is present in GNU's standard load-path in both layouts.
        if !directory
            .file_name()
            .is_some_and(|component| component == "lisp")
        {
            continue;
        }
        let Ok(relative) = source.strip_prefix(&directory) else {
            continue;
        };
        return format!("//{}", relative.to_string_lossy());
    }
    filename.to_string()
}

pub(crate) fn comp_el_to_eln_rel_filename(
    interp: &mut Interpreter,
    filename: &Value,
    env: &mut Env,
) -> Result<String, LispError> {
    let filename = string_argument(filename)?;
    let expanded = expand_file_name_runtime(interp, env, &filename, None)?;
    let canonical = fs::canonicalize(&expanded).map_err(|_| {
        LispError::SignalValue(Value::list([
            Value::symbol("file-missing"),
            Value::String(expanded.clone().into()),
        ]))
    })?;
    let canonical = canonical.display().to_string();
    let content_hash = md5_prefix(&source_bytes(Path::new(&canonical))?);
    let hash_path = canonical.strip_suffix(".gz").unwrap_or(&canonical);
    let normalized_hash_path = normalized_loadsearch_path(interp, hash_path, env);
    let path_hash = md5_prefix(normalized_hash_path.as_bytes());
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
    Ok(Value::String(
        expand_file_name_runtime(interp, env, &relative, Some(&directory))?.into(),
    ))
}

fn native_elisp_load(
    interp: &mut Interpreter,
    filename: &Value,
    late: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let filename = string_argument(filename)?;
    let expanded = expand_file_name_runtime(interp, env, &filename, None)?;
    if !Path::new(&expanded).exists() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("native-lisp-load-failed"),
            Value::string("file does not exists"),
            Value::String(filename.into()),
        ])));
    }
    crate::lisp::native_comp::load(interp, env, &expanded, late)
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "comp-el-to-eln-rel-filename" => {
                need_args(name, args, 1)?;
                comp_el_to_eln_rel_filename(interp, &args[0], env)
                    .map(|value| Value::String(value.into()))
            }
            "comp-el-to-eln-filename" => {
                need_arg_range(name, args, 1, 2)?;
                comp_el_to_eln_filename(interp, args, env)
            }
            "comp--release-ctxt" => {
                need_args(name, args, 0)?;
                interp.native_compiler.release();
                Ok(Value::T)
            }
            "comp--init-ctxt" => {
                need_args(name, args, 0)?;
                interp.native_compiler.acquire().map_err(|message| {
                    LispError::SignalValue(Value::list([
                        Value::symbol("native-compiler-error"),
                        Value::String(message.into()),
                    ]))
                })?;
                Ok(Value::Nil)
            }
            "comp--compile-ctxt-to-file0" => {
                need_args(name, args, 1)?;
                let filename = string_argument(&args[0])?;
                let mut state = std::mem::take(&mut interp.native_compiler);
                let compiled = state.compile_current_unit(interp, env, &filename);
                interp.native_compiler = state;
                let temporary = compiled?;
                crate::lisp::native_comp::call_lisp(
                    interp,
                    env,
                    "comp-clean-up-stale-eln",
                    &[Value::string(&filename)],
                )?;
                crate::lisp::native_comp::call_lisp(
                    interp,
                    env,
                    "comp-delete-or-replace-file",
                    &[Value::string(&filename), Value::string(&temporary)],
                )?;
                Ok(Value::string(&filename))
            }
            "comp--install-trampoline" => {
                need_args(name, args, 2)?;
                let Value::Symbol(symbol) = &args[0] else {
                    return Err(wrong_type_argument("symbolp", args[0].clone()));
                };
                let Value::Record(trampoline_id) = args[1] else {
                    return Err(wrong_type_argument("subrp", args[1].clone()));
                };
                if !interp.find_record(trampoline_id).is_some_and(|record| {
                    record.kind == crate::lisp::eval::RecordKind::NativeCompiledFunction
                }) {
                    return Err(wrong_type_argument("subrp", args[1].clone()));
                }
                let original = interp.lookup_function(symbol, env)?;
                let Value::BuiltinFunc(original_name) = original else {
                    return Err(wrong_type_argument("subrp", original));
                };
                let subroutine_index = crate::lisp::native_comp::subroutine_index(&original_name)
                    .ok_or_else(|| {
                    LispError::SignalValue(Value::list([
                        Value::symbol("error"),
                        Value::string("Trying to install trampoline for non existent subr"),
                        args[0].clone(),
                    ]))
                })?;
                crate::lisp::native_comp::install_trampoline(
                    interp,
                    subroutine_index,
                    trampoline_id,
                )?;
                let installed = interp
                    .lookup_var("comp-installed-trampolines-h", env)
                    .unwrap_or(Value::Nil);
                crate::lisp::native_comp::call_lisp(
                    interp,
                    env,
                    "puthash",
                    &[args[0].clone(), args[1].clone(), installed],
                )?;
                Ok(Value::T)
            }
            "comp--register-lambda" | "comp--register-subr" | "comp--late-register-subr" => {
                need_args(name, args, 7)?;
                let kind = match name {
                    "comp--register-lambda" => crate::lisp::native_comp::RegistrationKind::Lambda,
                    "comp--register-subr" => crate::lisp::native_comp::RegistrationKind::Subroutine,
                    _ => crate::lisp::native_comp::RegistrationKind::LateSubroutine,
                };
                crate::lisp::native_comp::register(interp, env, args, kind)
            }
            "native-elisp-load" => {
                need_arg_range(name, args, 1, 2)?;
                native_elisp_load(
                    interp,
                    &args[0],
                    args.get(1).is_some_and(Value::is_truthy),
                    env,
                )
            }
        }
    }
);
