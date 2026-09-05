use super::*;

/// lisp.h:FOR_EACH_TAIL_INTERNAL. Keep the current cons, not a snapshot of
/// its cars: a file-name handler or predicate can change a later entry.
pub(super) struct SearchTail {
    pub current: Value,
    tortoise: Value,
    max: isize,
    remaining: isize,
    quit_count: u16,
}

impl SearchTail {
    pub fn new(list: Value) -> Self {
        Self {
            tortoise: list.clone(),
            current: list,
            max: 2,
            remaining: 0,
            quit_count: 2,
        }
    }

    pub fn advance(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        safe: bool,
    ) -> Result<(), LispError> {
        self.current = self.current.cdr()?;
        self.quit_count = self.quit_count.wrapping_sub(1);
        if self.quit_count == 0 {
            if !safe {
                interp.maybe_quit(env)?;
            }
            self.remaining -= 1;
            if self.remaining <= 0 {
                self.max = self.max.wrapping_mul(2);
                self.quit_count = self.max as u16;
                self.remaining = self.max >> u16::BITS;
                self.tortoise = self.current.clone();
                return Ok(());
            }
        }
        if self.current.cons_id().is_some() && self.current.cons_id() == self.tortoise.cons_id() {
            if !safe {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("circular-list"),
                    self.current.clone(),
                ])));
            }
            self.current = Value::Nil;
        }
        Ok(())
    }
}

/// A local match retains its open descriptor while the remaining suffixes
/// are examined. Predicate/handler matches have no local descriptor.
pub(super) struct SearchMatch {
    pub name: Value,
    pub file: Option<fs::File>,
}

impl SearchMatch {
    pub fn into_name(self) -> Value {
        let Self { name, file } = self;
        drop(file);
        name
    }
}

/// The POSIX search portion of lread.c:openp, shared by Fload's resolver,
/// Flocate_file_internal, and process executable lookup. Native substitution
/// still belongs to the existing load caller, not locate-file-internal.
pub(super) fn openp_search(
    interp: &mut Interpreter,
    file: &Value,
    path: &Value,
    suffixes: &Value,
    predicate: &Value,
    newer: bool,
    env: &mut Env,
) -> Result<(Option<SearchMatch>, i32), LispError> {
    let requested = string_text(file)?;
    let mut tail = SearchTail::new(suffixes.clone());
    while let Some((suffix, _)) = tail.current.cons_values() {
        string_text(&suffix)?;
        tail.advance(interp, env, true)?;
    }

    // complete_filename_p is not file-name-absolute-p: on POSIX a leading
    // tilde still needs expansion, while a leading slash is complete.
    let absolute = requested.starts_with('/');
    let just_use_str = Value::list([Value::Nil]);
    let mut directories = SearchTail::new(if path.is_nil() {
        just_use_str.clone()
    } else {
        path.clone()
    });
    let mut last_errno = libc::ENOENT;
    while let Some((directory, _)) = directories.current.cons_values() {
        let mut filename = if directories.current.cons_id() == just_use_str.cons_id() {
            file.clone()
        } else {
            super::super::call(interp, "expand-file-name", &[file.clone(), directory], env)?
        };
        if !string_text(&filename)?.starts_with('/') {
            filename =
                super::super::call(interp, "expand-file-name", &[filename, Value::Nil], env)?;
            if !string_text(&filename)?.starts_with('/') {
                directories.advance(interp, env, true)?;
                continue;
            }
        }
        let filename = string_like(&filename).expect("expanded filename was checked above");
        let base = if filename.text.len() > 2 {
            filename.text.strip_prefix("/:").unwrap_or(&filename.text)
        } else {
            &filename.text
        };
        let mut candidates = SearchTail::new(if suffixes.is_nil() {
            Value::list([Value::string("")])
        } else {
            suffixes.clone()
        });
        let mut newest: Option<(SearchMatch, std::time::SystemTime)> = None;
        while let Some((suffix, _)) = candidates.current.cons_values() {
            let suffix_text = string_text(&suffix)?;
            let suffix_multibyte = string_like(&suffix)
                .expect("suffix was checked above")
                .multibyte;
            let text = format!("{base}{suffix_text}");
            let name = make_shared_string_value_with_multibyte(
                text.clone(),
                Vec::new(),
                filename.multibyte || suffix_multibyte,
            );
            let handler = find_file_name_handler(interp, env, &text, "file-exists-p")?;
            let mask = match predicate {
                Value::Integer(mask) if *mask >= 0 => Some(*mask),
                _ => None,
            };
            let ordinary =
                predicate.is_nil() || values_eq_in_env(interp, predicate, &Value::T, env);
            if (handler.is_some() || !ordinary) && mask.is_none() {
                let exists = if ordinary {
                    super::super::call(interp, "file-readable-p", std::slice::from_ref(&name), env)?
                        .is_truthy()
                } else {
                    let answer = interp.call_function_value(
                        predicate.clone(),
                        predicate.as_symbol().ok(),
                        std::slice::from_ref(&name),
                        env,
                    )?;
                    if answer.is_nil() {
                        false
                    } else if values_eq_in_env(interp, &answer, &Value::symbol("dir-ok"), env)
                        || super::super::call(
                            interp,
                            "file-directory-p",
                            std::slice::from_ref(&name),
                            env,
                        )?
                        .is_nil()
                    {
                        true
                    } else {
                        last_errno = libc::EISDIR;
                        false
                    }
                };
                if exists {
                    return Ok((Some(SearchMatch { name, file: None }), last_errno));
                }
            } else {
                let opened = if let Some(mask) = mask {
                    locate_file_access_probe(mask, &text).map(|()| None)
                } else {
                    open_local_candidate(&text).map(Some)
                };
                match opened {
                    Ok(Some((file, modified))) if newer => {
                        if newest.as_ref().is_none_or(|(_, saved)| modified > *saved) {
                            newest = Some((
                                SearchMatch {
                                    name,
                                    file: Some(file),
                                },
                                modified,
                            ));
                        }
                    }
                    Ok(opened) => {
                        return Ok((
                            Some(SearchMatch {
                                name,
                                file: opened.map(|(file, _)| file),
                            }),
                            last_errno,
                        ));
                    }
                    Err(errno) => {
                        if errno != libc::ENOENT && errno != libc::ENOTDIR {
                            last_errno = errno;
                        }
                    }
                }
                // GNU considers newer suffixes only in this path entry;
                // equally old files retain the earlier suffix's descriptor.
                if candidates.current.cdr()?.cons_id().is_none()
                    && let Some((found, _)) = newest.take()
                {
                    return Ok((Some(found), last_errno));
                }
            }
            candidates.advance(interp, env, true)?;
        }
        if absolute {
            break;
        }
        directories.advance(interp, env, true)?;
    }
    Ok((None, last_errno))
}

fn open_local_candidate(path: &str) -> Result<(fs::File, std::time::SystemTime), i32> {
    let errno = |error: std::io::Error| error.raw_os_error().unwrap_or(libc::EIO);
    let file = fs::File::open(path).map_err(errno)?;
    let metadata = file.metadata().map_err(errno)?;
    if metadata.is_dir() {
        return Err(libc::EISDIR);
    }
    let modified = metadata.modified().map_err(errno)?;
    Ok((file, modified))
}
