use super::*;

struct LoadRequest {
    file: Value,
    noerror: Value,
    nomessage: Value,
    nosuffix: Value,
    must_suffix: Value,
}

struct LoadCompletion {
    history: Value,
    kind: &'static str,
}

impl LoadRequest {
    fn new(args: &[Value]) -> Result<Self, LispError> {
        if args.is_empty() || args.len() > 5 {
            return Err(LispError::WrongNumberOfArgs("load".into(), args.len()));
        }
        string_text(&args[0])?;
        Ok(Self {
            file: args[0].clone(),
            noerror: args.get(1).cloned().unwrap_or(Value::Nil),
            nomessage: args.get(2).cloned().unwrap_or(Value::Nil),
            nosuffix: args.get(3).cloned().unwrap_or(Value::Nil),
            must_suffix: args.get(4).cloned().unwrap_or(Value::Nil),
        })
    }
}

/// lread.c:Fload. Returns the Lisp result and found filename; the latter is
/// retained only for existing host-side callers that report the loaded path.
pub(crate) fn load_file(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<(Value, Value), LispError> {
    let mut request = LoadRequest::new(args)?;
    let original = string_text(&request.file)?;
    if let Some(handler) = find_file_name_handler(interp, env, &original, "load")? {
        let result = interp.call_function_value(
            handler,
            None,
            &[
                Value::symbol("load"),
                request.file.clone(),
                request.noerror,
                request.nomessage,
                request.nosuffix,
                request.must_suffix,
            ],
            env,
        )?;
        return Ok((result, request.file));
    }
    request.file = match super::super::call(
        interp,
        "substitute-in-file-name",
        std::slice::from_ref(&request.file),
        env,
    ) {
        Ok(file) => file,
        Err(
            error @ (LispError::Throw(_, _) | LispError::VmReturn(_) | LispError::Terminate(_)),
        ) => return Err(error),
        Err(_) if request.noerror.is_truthy() => return Ok((Value::Nil, Value::Nil)),
        Err(error) => return Err(error),
    };
    let requested = string_text(&request.file)?;
    let (found, errno) = find_load_file(
        interp,
        &requested,
        env,
        request.nosuffix.is_truthy(),
        request.must_suffix.is_truthy(),
    )?;
    let Some(mut found) = found else {
        if request.noerror.is_truthy() {
            return Ok((Value::Nil, Value::Nil));
        }
        return Err(LispError::SignalValue(file_errno_error_value(
            "Cannot open load file",
            errno,
            &requested,
        )));
    };

    // Native substitution applies only to locally opened files. Retain the
    // original descriptor if the native candidate cannot actually be opened.
    if found.file.is_some() {
        let path = PathBuf::from(string_text(&found.name)?);
        let native = maybe_swap_for_native(interp, &requested, &path, env)?;
        if native != path
            && let Ok(file) = fs::File::open(&native)
        {
            found.name = Value::string(&native.display().to_string());
            found.file = Some(file);
        }
    }
    if interp
        .forwarded_c_value("user-init-file", &Env::new())
        .is_some_and(|value| values_eq_in_env(interp, &value, &Value::T, env))
    {
        if let Some(slot) = interp
            .detached_forwarded_variables
            .get_mut("user-init-file")
        {
            *slot = found.name.clone();
        } else {
            // A raw C assignment does not invoke variable watchers.
            interp.set_symbol_value_cell("user-init-file", found.name.clone());
        }
    }
    if found.file.is_none() {
        let operation = if values_equal(interp, &found.name, &request.file) {
            "load"
        } else {
            "t"
        };
        if let Some(handler) =
            find_file_name_handler(interp, env, &string_text(&found.name)?, operation)?
        {
            let result = interp.call_function_value(
                handler,
                None,
                &[
                    Value::symbol("load"),
                    found.name.clone(),
                    request.noerror,
                    request.nomessage,
                    Value::T,
                ],
                env,
            )?;
            return Ok((result, found.name));
        }
    }

    let saved_loads = interp.loads_in_progress.clone();
    let mut nesting = 0;
    let mut tail = search::SearchTail::new(saved_loads.clone());
    while let Some((file, _)) = tail.current.cons_values() {
        if values_equal_signaling(interp, &file, &found.name, env)? {
            nesting += 1;
            if nesting > 3 {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("error"),
                    Value::string("Recursive load"),
                    Value::cons(found.name.clone(), saved_loads),
                ])));
            }
        }
        tail.advance(interp, env, true)?;
    }
    interp.loads_in_progress = Value::cons(found.name.clone(), saved_loads.clone());
    let result = load_with_context(interp, &request, found, env);
    interp.loads_in_progress = saved_loads;
    let (result, found, after_load) = result?;
    if let Some(completion) = after_load {
        if let Ok(function) = interp.lookup_function("do-after-load-evaluation", env) {
            interp.call_function_value(
                function,
                Some("do-after-load-evaluation"),
                &[completion.history],
                env,
            )?;
        }
        if !interp
            .forwarded_c_value("noninteractive", &Env::new())
            .is_some_and(|value| value.is_truthy())
        {
            load_message(interp, &request, completion.kind, true, env)?;
        }
    }
    Ok((result, found))
}

fn load_with_context(
    interp: &mut Interpreter,
    request: &LoadRequest,
    mut found: search::SearchMatch,
    env: &mut Env,
) -> Result<(Value, Value, Option<LoadCompletion>), LispError> {
    let lexical = interp.bind_special_variable("lexical-binding", Value::Nil, env)?;
    let result = (|| {
        let path = PathBuf::from(string_text(&found.name)?);
        let native = path.extension().is_some_and(|extension| extension == "eln");
        let module = crate::lisp::eval::dynamic_library_suffix_values()
            .iter()
            .any(|suffix| {
                string_like(suffix)
                    .is_some_and(|suffix| path.to_string_lossy().ends_with(&suffix.text))
            });
        let effective = if native {
            native_effective_filename(interp, &found.name, env)?
        } else {
            found.name.clone()
        };
        let history = if interp
            .forwarded_c_value("purify-flag", &Env::new())
            .is_some_and(|value| value.is_truthy())
        {
            let directory = super::super::call(
                interp,
                "file-name-directory",
                std::slice::from_ref(&request.file),
                env,
            )?;
            let basename = super::super::call(interp, "file-name-nondirectory", &[effective], env)?;
            super::super::call(interp, "concat", &[directory, basename], env)?
        } else {
            effective
        };
        let warning =
            interp.bind_special_variable("lread--unescaped-character-literals", Value::Nil, env)?;
        let mut result = (|| {
            let compiled = path.extension().is_some_and(|extension| extension == "elc")
                || match found.file.as_mut() {
                    Some(file) => safe_to_load_version(interp, &request.file, file, env)? > 1,
                    None => false,
                };
            let source_loader = interp
                .forwarded_c_value("load-source-file-function", &Env::new())
                .unwrap_or(Value::Nil);
            if !compiled && !module && !native && source_loader.is_truthy() {
                // The unchanged Lisp owner opens/decodes its own source. It
                // owns load-history and after-load hooks on this branch too.
                drop(found.file.take());
                let force = interp
                    .forwarded_c_value("force-load-messages", &Env::new())
                    .is_some_and(|value| value.is_truthy());
                let result = interp.call_function_value(
                    source_loader,
                    None,
                    &[
                        found.name.clone(),
                        history.clone(),
                        if request.noerror.is_truthy() {
                            Value::T
                        } else {
                            Value::Nil
                        },
                        if request.nomessage.is_truthy() && !force {
                            Value::T
                        } else {
                            Value::Nil
                        },
                    ],
                    env,
                )?;
                return Ok((result, None));
            }
            let Some(file) = found.file.take() else {
                return Err(LispError::SignalValue(file_errno_error_value(
                    "Opening stdio stream",
                    libc::EINVAL,
                    &string_text(&request.file)?,
                )));
            };
            let kind = if module {
                " (module)"
            } else if native {
                " (native compiled elisp)"
            } else if compiled {
                ""
            } else {
                " (source)"
            };
            load_message(interp, request, kind, false, env)?;
            if native || module {
                drop(file);
                if native {
                    interp.load_native_resolved_path(&path, &string_text(&history)?, env)?;
                } else {
                    super::super::call(
                        interp,
                        "module-load",
                        std::slice::from_ref(&found.name),
                        env,
                    )?;
                }
            } else {
                crate::lisp::load_file_strict_opened(interp, &path, file, &string_text(&history)?)?;
            }
            Ok((Value::T, Some(kind)))
        })();
        // load_warn_unescaped_character_literals runs during unbinding,
        // before the warning list and lexical-binding are restored.
        if let Err(error) = emit_load_warning(interp, &request.file, env) {
            result = Err(error);
        }
        if let Err(error) = interp.restore_special_binding(warning, env) {
            result = Err(error);
        }
        result.map(|(result, after_load)| (result, after_load, history))
    })();
    let restore = interp.restore_special_binding(lexical, env);
    restore?;
    let (result, after_load, history) = result?;
    Ok((
        result,
        found.name,
        after_load.map(|kind| LoadCompletion { history, kind }),
    ))
}

fn load_message(
    interp: &mut Interpreter,
    request: &LoadRequest,
    kind: &str,
    done: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    if request.nomessage.is_nil()
        || interp
            .forwarded_c_value("force-load-messages", &Env::new())
            .is_some_and(|value| value.is_truthy())
    {
        super::super::call(
            interp,
            "message",
            &[
                Value::string(&format!(
                    "Loading %s{kind}...{}",
                    if done { "done" } else { "" }
                )),
                request.file.clone(),
            ],
            env,
        )?;
    }
    Ok(())
}

fn native_effective_filename(
    interp: &mut Interpreter,
    name: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let basename = super::super::call(
        interp,
        "file-name-nondirectory",
        std::slice::from_ref(name),
        env,
    )?;
    let Some(table) = interp.forwarded_c_value("comp-eln-to-el-h", &Env::new()) else {
        return Ok(name.clone());
    };
    let source = super::super::call(interp, "gethash", &[basename, table, Value::Nil], env)?;
    let Some(source) = string_like(&source) else {
        return Ok(name.clone());
    };
    Ok(Value::string(&format!(
        "{}c",
        source.text.strip_suffix(".gz").unwrap_or(&source.text)
    )))
}

fn emit_load_warning(
    interp: &mut Interpreter,
    requested: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Some(warning) = crate::lisp::unescaped_character_literal_warning(interp, env)? {
        super::super::call(
            interp,
            "message",
            &[
                Value::string("Loading `%s': %s"),
                requested.clone(),
                Value::string(&warning),
            ],
            env,
        )?;
    }
    Ok(())
}

fn safe_to_load_version(
    interp: &mut Interpreter,
    name: &Value,
    file: &mut fs::File,
    env: &mut Env,
) -> Result<i32, LispError> {
    if file.metadata().is_ok_and(|metadata| !metadata.is_file()) {
        return Ok(0);
    }
    let mut header = [0u8; 512];
    let count = loop {
        match file.read(&mut header) {
            Ok(count) => break count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                interp.maybe_quit(env)?
            }
            Err(_) => break 0,
        }
    };
    let mut version = 1;
    if count > 0 {
        let newline = header[..count].iter().position(|byte| *byte == b'\n');
        if let Some(newline) = newline {
            if newline > 4 {
                version = i32::from(header[4] as i8);
            }
            let pattern = interp
                .forwarded_c_value("bytecomp-version-regexp", &Env::new())
                .unwrap_or(Value::Nil);
            if !super::super::regexp::fast_c_string_match_ignore_case(
                interp,
                &pattern,
                &header[newline..count],
            )? {
                version = 0;
            }
        } else {
            version = 0;
        }
    }
    file.rewind().map_err(|error| {
        file_operation_error(
            "Seeking to start of file",
            &error,
            &string_text(name).unwrap_or_default(),
        )
    })?;
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::super::tests::{gnu_root, source_callback};
    use super::*;
    use std::os::fd::AsRawFd;

    #[test]
    fn source_handoff_closes_the_selected_descriptor_before_calling_elisp() {
        let path = gnu_root().join("test/src/comp-resources/comp-test-45603.el");
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        interp.set_variable("purify-flag", Value::Nil, &mut env);
        let file = fs::File::open(&path).expect("open unchanged GNU fixture");
        let descriptor = Value::string(&format!("/dev/fd/{}", file.as_raw_fd()));
        let before = super::super::super::call(
            &mut interp,
            "file-exists-p",
            std::slice::from_ref(&descriptor),
            &mut env,
        )
        .expect("inspect the selected descriptor");
        assert!(
            before.is_truthy(),
            "negative control: descriptor is open before handoff"
        );
        // bytecode.c: constant function, constant argument, call-one, return.
        let callback = source_callback(
            &mut interp,
            &[192, 193, 33, 135],
            vec![Value::symbol("file-exists-p"), descriptor],
        );
        interp.set_variable("load-source-file-function", callback, &mut env);
        let name = Value::string(&path.display().to_string());
        let request = LoadRequest::new(&[name.clone(), Value::Nil, Value::T])
            .expect("construct a valid load request");
        let (result, _, completion) = load_with_context(
            &mut interp,
            &request,
            search::SearchMatch {
                name,
                file: Some(file),
            },
            &mut env,
        )
        .expect("run the ordinary source handoff");
        assert!(
            result.is_nil(),
            "the Elisp callback must see a closed descriptor"
        );
        assert!(completion.is_none(), "Elisp owns completion on this branch");
    }

    #[test]
    fn direct_load_keeps_the_selected_inode_when_its_filename_is_replaced() {
        let mut interp = crate::test_support::initialized_gnu_early_lisp_interpreter();
        let mut env = Env::new();
        interp.set_variable("load-source-file-function", Value::Nil, &mut env);
        let root = std::env::temp_dir().join(format!(
            "load-descriptor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after the Unix epoch")
                .as_nanos(),
        ));
        fs::create_dir(&root).expect("create isolated fixture directory");
        let selected = root.join("selected.el");
        let moved = root.join("original.el");
        fs::copy(
            gnu_root().join("test/src/comp-resources/comp-test-45603.el"),
            &selected,
        )
        .expect("copy unchanged GNU fixture");
        let file = fs::File::open(&selected).expect("select original inode");
        fs::rename(&selected, &moved).expect("move the selected file without closing it");
        fs::copy(gnu_root().join("lisp/emacs-lisp/seq.el"), &selected)
            .expect("replace the name with another unchanged GNU file");
        let name = Value::string(&selected.display().to_string());
        let request = LoadRequest::new(&[name.clone(), Value::Nil, Value::T])
            .expect("construct a valid load request");
        let result = load_with_context(
            &mut interp,
            &request,
            search::SearchMatch {
                name,
                file: Some(file),
            },
            &mut env,
        );
        fs::remove_file(&selected).expect("remove replacement fixture copy");
        fs::remove_file(&moved).expect("remove original fixture copy");
        fs::remove_dir(&root).expect("remove the empty isolated fixture directory");
        let (value, _, completion) = result.expect("read the descriptor selected by openp");
        assert_eq!(value, Value::T);
        assert!(completion.is_some());
        assert!(
            interp.has_feature("comp-test-45603"),
            "must execute the originally opened file"
        );
        assert!(
            !interp.has_feature("seq"),
            "must not reopen and execute its replacement"
        );
    }
}
