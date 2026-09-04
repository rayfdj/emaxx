use super::*;
use crate::lisp::eval::RecordKind;

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

/// A call through the Lisp function cell, matching comp.c's `CALLNI` helpers.
fn lisp(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    arguments: &[Value],
) -> Result<Value, LispError> {
    crate::lisp::native_comp::call_lisp(interp, env, name, arguments)
}

fn c_primitive(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    arguments: &[Value],
) -> Result<Value, LispError> {
    crate::lisp::native_comp::call_c_primitive(interp, env, name, arguments)
}

/// comp.c:comp_hash_string: the first eight hex digits of MD5.
fn comp_hash_string(text: &str) -> String {
    md5_prefix(text.as_bytes())
}

/// GNU's build-time `PATH_REL_LOADSEARCH`, the versioned relative Lisp
/// directory of an installed tree, and `PATH_DUMPLOADSEARCH`, the Lisp
/// directory of the source tree the image was dumped from.
fn loadsearch_regexps(interp: &mut Interpreter, env: &mut Env) -> Result<[Value; 2], LispError> {
    let relative = format!("/{}/lisp/", crate::lisp::primitives::emacs_version_value());
    let quoted_relative = c_primitive(
        interp,
        env,
        "regexp-quote",
        &[Value::String(relative.into())],
    )?;
    let system = c_primitive(
        interp,
        env,
        "concat",
        &[Value::string("\\`[[:ascii:]]+"), quoted_relative],
    )?;
    let source_directory = interp
        .lookup_var("source-directory", env)
        .unwrap_or(Value::Nil);
    let dump_load_search = c_primitive(
        interp,
        env,
        "expand-file-name",
        &[Value::string("lisp/"), source_directory],
    )?;
    let quoted_dump = c_primitive(interp, env, "regexp-quote", &[dump_load_search])?;
    Ok([system, quoted_dump])
}

pub(crate) fn comp_el_to_eln_rel_filename(
    interp: &mut Interpreter,
    filename: &Value,
    env: &mut Env,
) -> Result<String, LispError> {
    string_argument(filename)?;
    // Use the realpath so that any symlink always compares equal (bug#44701).
    let expanded = c_primitive(
        interp,
        env,
        "expand-file-name",
        &[filename.clone(), Value::Nil],
    )?;
    let mut filename = match fs::canonicalize(string_argument(&expanded)?) {
        Ok(canonical) => canonical.display().to_string(),
        Err(_) => string_argument(&expanded)?,
    };
    if !c_primitive(interp, env, "file-exists-p", &[Value::string(&filename)])?.is_truthy() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("file-missing"),
            Value::String(filename.into()),
        ])));
    }
    let content_hash = md5_prefix(&source_bytes(Path::new(&filename))?);
    if let Some(uncompressed) = filename.strip_suffix(".gz") {
        filename = uncompressed.to_string();
    }
    // Installing .eln files compiled during the build changes their
    // absolute path, so the path hash replaces a match of either load
    // search directory with `//'.
    for regexp in loadsearch_regexps(interp, env)? {
        let index = c_primitive(
            interp,
            env,
            "string-match",
            &[regexp, Value::string(&filename), Value::Nil, Value::Nil],
        )?;
        if matches!(index, Value::Integer(0)) {
            let replaced = c_primitive(
                interp,
                env,
                "replace-match",
                &[
                    Value::string("//"),
                    Value::T,
                    Value::T,
                    Value::string(&filename),
                    Value::Nil,
                ],
            )?;
            filename = string_argument(&replaced)?;
            break;
        }
    }
    let path_hash = comp_hash_string(&filename);
    Ok(format!(
        "{}-{path_hash}-{content_hash}.eln",
        source_basename(&filename)?
    ))
}

/// comp.c's `make_directory_wrapper` under `internal_condition_case_1`:
/// true when `(make-directory DIR t)` completed, false when it signaled.
fn try_make_directory(
    interp: &mut Interpreter,
    env: &mut Env,
    directory: &Value,
) -> Result<bool, LispError> {
    match lisp(
        interp,
        env,
        "make-directory",
        &[directory.clone(), Value::T],
    ) {
        Ok(_) => Ok(true),
        Err(
            error @ (LispError::Throw(..) | LispError::VmReturn(..) | LispError::Terminate(..)),
        ) => Err(error),
        Err(_) => Ok(false),
    }
}

fn comp_el_to_eln_filename(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let source_filename = args[0].clone();
    let relative = comp_el_to_eln_rel_filename(interp, &args[0], env)?;
    // If BASE-DIR was not specified, search `native-comp-eln-load-path' for
    // the first directory where we have write access.
    let mut base_dir = args.get(1).cloned().filter(Value::is_truthy);
    if base_dir.is_none() {
        let load_path = interp
            .lookup_var("native-comp-eln-load-path", env)
            .unwrap_or(Value::Nil)
            .to_vec()?;
        for directory in load_path {
            if c_primitive(
                interp,
                env,
                "file-exists-p",
                std::slice::from_ref(&directory),
            )?
            .is_truthy()
            {
                if c_primitive(
                    interp,
                    env,
                    "file-writable-p",
                    std::slice::from_ref(&directory),
                )?
                .is_truthy()
                {
                    base_dir = Some(directory);
                    break;
                }
            } else if try_make_directory(interp, env, &directory)? {
                base_dir = Some(directory);
                break;
            }
        }
    }
    let Some(mut base_dir) = base_dir else {
        return Err(LispError::Signal(
            "Cannot find suitable directory for output in `native-comp-eln-load-path'.".into(),
        ));
    };
    if !c_primitive(interp, env, "file-name-absolute-p", &[base_dir.clone()])?.is_truthy() {
        let invocation_directory = interp
            .lookup_var("invocation-directory", env)
            .unwrap_or(Value::Nil);
        base_dir = c_primitive(
            interp,
            env,
            "expand-file-name",
            &[base_dir, invocation_directory],
        )?;
    }
    // A file named in LISP_PRELOADED, or compiled while
    // `comp-file-preloaded-p' is set, targets the `preloaded' subfolder.
    let lisp_preloaded = c_primitive(
        interp,
        env,
        "getenv-internal",
        &[Value::string("LISP_PRELOADED"), Value::Nil],
    )?;
    let version_dir = interp
        .lookup_var("comp-native-version-dir", env)
        .unwrap_or(Value::Nil);
    base_dir = c_primitive(interp, env, "expand-file-name", &[version_dir, base_dir])?;
    let preloaded_p = interp
        .lookup_var("comp-file-preloaded-p", env)
        .is_some_and(|value| value.is_truthy());
    let preloaded = preloaded_p
        || (lisp_preloaded.is_truthy() && {
            let source_base = lisp(interp, env, "file-name-base", &[source_filename])?;
            let preloaded_names = lisp(interp, env, "split-string", &[lisp_preloaded])?;
            let preloaded_bases = c_primitive(
                interp,
                env,
                "mapcar",
                &[Value::symbol("file-name-base"), preloaded_names],
            )?;
            c_primitive(interp, env, "member", &[source_base, preloaded_bases])?.is_truthy()
        });
    if preloaded {
        base_dir = c_primitive(
            interp,
            env,
            "expand-file-name",
            &[Value::string("preloaded"), base_dir],
        )?;
    }
    c_primitive(
        interp,
        env,
        "expand-file-name",
        &[Value::String(relative.into()), base_dir],
    )
}

/// comp.c:file_in_eln_sys_dir: whether FILENAME lives under the last entry
/// of `native-comp-eln-load-path', the system directory.
fn file_in_eln_sys_dir(
    interp: &mut Interpreter,
    env: &mut Env,
    filename: &Value,
) -> Result<bool, LispError> {
    let system_directory = interp
        .lookup_var("native-comp-eln-load-path", env)
        .unwrap_or(Value::Nil)
        .to_vec()?
        .pop()
        .unwrap_or(Value::Nil);
    let expanded_directory = c_primitive(
        interp,
        env,
        "expand-file-name",
        &[system_directory, Value::Nil],
    )?;
    let quoted = c_primitive(interp, env, "regexp-quote", &[expanded_directory])?;
    let expanded_file = c_primitive(
        interp,
        env,
        "expand-file-name",
        &[filename.clone(), Value::Nil],
    )?;
    Ok(c_primitive(
        interp,
        env,
        "string-match",
        &[quoted, expanded_file, Value::Nil, Value::Nil],
    )?
    .is_truthy())
}

/// comp.c:Fnative_elisp_load.
pub(crate) fn native_elisp_load(
    interp: &mut Interpreter,
    filename: &Value,
    late: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let name = string_argument(filename)?;
    if !c_primitive(interp, env, "file-exists-p", std::slice::from_ref(filename))?.is_truthy() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("native-lisp-load-failed"),
            Value::string("file does not exists"),
            filename.clone(),
        ])));
    }
    // comp.c:Fnative_elisp_load allocates this zeroed pseudovector before
    // ENCODE_FILE, the loaded-unit lookup, and dlopen.  It is deliberately
    // allocated even when dlopen returns an already-loaded unit and the
    // candidate is subsequently discarded.
    let candidate_unit = interp.create_pseudovector(
        RecordKind::NativeCompUnit,
        "native-comp-unit",
        vec![Value::Nil; 7],
    );
    let loaded_units = interp
        .lookup_var("comp-loaded-comp-units-h", env)
        .unwrap_or(Value::Nil);
    let loaded_before = c_primitive(
        interp,
        env,
        "gethash",
        &[filename.clone(), loaded_units, Value::Nil],
    )?
    .is_truthy();
    let library = if loaded_before
        && !file_in_eln_sys_dir(interp, env, filename)?
        && c_primitive(
            interp,
            env,
            "file-writable-p",
            std::slice::from_ref(filename),
        )?
        .is_truthy()
    {
        // If in this session there was ever a file loaded with this name,
        // rename it before loading, to make sure we always get a new handle!
        let temporary = c_primitive(
            interp,
            env,
            "make-temp-file-internal",
            &[
                filename.clone(),
                Value::Nil,
                Value::string(".eln.tmp"),
                Value::Nil,
            ],
        )?;
        if c_primitive(
            interp,
            env,
            "file-writable-p",
            std::slice::from_ref(&temporary),
        )?
        .is_truthy()
        {
            c_primitive(
                interp,
                env,
                "rename-file",
                &[filename.clone(), temporary.clone(), Value::T],
            )?;
            let opened =
                crate::lisp::native_comp::open_unit(filename, &string_argument(&temporary)?);
            c_primitive(
                interp,
                env,
                "rename-file",
                &[temporary, filename.clone(), Value::Nil],
            )?;
            opened?
        } else {
            crate::lisp::native_comp::open_unit(filename, &name)?
        }
    } else {
        crate::lisp::native_comp::open_unit(filename, &name)?
    };
    let Value::Record(candidate_id) = candidate_unit else {
        unreachable!("native compilation unit is a pseudovector")
    };
    interp
        .find_record_mut(candidate_id)
        .expect("new native compilation unit remains live")
        .slots[0] = filename.clone();
    let lambda_guard = crate::lisp::json::make_hash_table(interp, "eq", Vec::new());
    interp
        .find_record_mut(candidate_id)
        .expect("new native compilation unit remains live")
        .slots[2] = lambda_guard;
    let lambda_name_index = crate::lisp::json::make_hash_table(interp, "equal", Vec::new());
    interp
        .find_record_mut(candidate_id)
        .expect("new native compilation unit remains live")
        .slots[3] = lambda_name_index;
    crate::lisp::native_comp::load(
        interp,
        env,
        filename,
        library,
        &Value::Record(candidate_id),
        late,
    )
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
                if crate::lisp::native_comp::NativeCompilerState::release_active().is_none() {
                    interp.native_compiler.release();
                }
                Ok(Value::T)
            }
            "comp--init-ctxt" => {
                need_args(name, args, 0)?;
                let acquired = crate::lisp::native_comp::NativeCompilerState::acquire_active()
                    .unwrap_or_else(|| interp.native_compiler.acquire());
                acquired.map_err(|message| {
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
                let compiled =
                    crate::lisp::native_comp::NativeCompilerState::compile_current_unit_active(
                        interp, env, &filename,
                    )
                    .unwrap_or_else(|| {
                        let mut state = std::mem::take(&mut interp.native_compiler);
                        let compiled = state.compile_current_unit(interp, env, &filename);
                        interp.native_compiler = state;
                        compiled
                    });
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
                crate::lisp::native_comp::call_c_primitive(
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
