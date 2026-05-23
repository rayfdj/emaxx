use super::*;

pub(crate) fn format_source_props(
    value: &Value,
    from: usize,
    to: usize,
) -> Option<Vec<(String, Value)>> {
    let string = string_like(value)?;
    let mut props = Vec::new();
    for span in string.props {
        if span.start < to && from < span.end {
            for (name, value) in span.props {
                if !props.iter().any(|(existing, _)| existing == &name) {
                    props.push((name, value));
                }
            }
        }
    }
    if props.is_empty() { None } else { Some(props) }
}

pub(crate) fn props_at_string_offset(
    spans: &[TextPropertySpan],
    pos: usize,
) -> Vec<(String, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

pub(crate) fn file_modtime(path: &str) -> Result<Option<crate::buffer::FileModTime>, LispError> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(crate::buffer::FileModTime {
            modified: metadata
                .modified()
                .map_err(|error| LispError::Signal(error.to_string()))?,
        })),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

pub(crate) fn system_time_seconds_value(time: SystemTime) -> Result<Value, LispError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    Ok(Value::Integer(duration.as_secs() as i64))
}

pub(crate) fn file_attribute_field(attributes: &Value, index: usize) -> Result<Value, LispError> {
    Ok(attributes
        .to_vec()?
        .get(index)
        .cloned()
        .unwrap_or(Value::Nil))
}

pub(crate) fn file_modtime_from_value(
    interp: &Interpreter,
    value: &Value,
) -> Result<crate::buffer::FileModTime, LispError> {
    let now = current_time_value()?;
    let exact = exact_time_from_value(interp, value, &now)?;
    let (whole_seconds, _) = time_floor_parts(&exact);
    let seconds = whole_seconds
        .to_i64()
        .ok_or_else(|| LispError::Signal("Time out of range".into()))?;
    let modified = if seconds >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds as u64))
            .ok_or_else(|| LispError::Signal("Time out of range".into()))?
    } else {
        UNIX_EPOCH
            .checked_sub(Duration::from_secs(seconds.unsigned_abs()))
            .ok_or_else(|| LispError::Signal("Time out of range".into()))?
    };
    Ok(crate::buffer::FileModTime { modified })
}

#[cfg(unix)]
pub(crate) fn system_time_to_timeval(time: SystemTime) -> Result<libc::timeval, LispError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    Ok(libc::timeval {
        tv_sec: duration.as_secs() as libc::time_t,
        tv_usec: duration.subsec_micros() as libc::suseconds_t,
    })
}

pub(crate) fn set_file_times_path(
    path: &str,
    modified: SystemTime,
    nofollow: bool,
) -> Result<(), LispError> {
    #[cfg(unix)]
    {
        let c_path = CString::new(Path::new(path).as_os_str().as_bytes())
            .map_err(|_| LispError::Signal("File name contains nul byte".into()))?;
        let metadata = if nofollow {
            fs::symlink_metadata(path)
        } else {
            fs::metadata(path)
        }
        .map_err(|error| LispError::Signal(error.to_string()))?;
        let accessed = metadata.accessed().unwrap_or(modified);
        let times = [
            system_time_to_timeval(accessed)?,
            system_time_to_timeval(modified)?,
        ];
        let result = if nofollow {
            // SAFETY: c_path is a valid nul-terminated path, and times points to
            // two initialized timeval values for the duration of this call.
            unsafe { libc::lutimes(c_path.as_ptr(), times.as_ptr()) }
        } else {
            // SAFETY: c_path is a valid nul-terminated path, and times points to
            // two initialized timeval values for the duration of this call.
            unsafe { libc::utimes(c_path.as_ptr(), times.as_ptr()) }
        };
        if result == 0 {
            Ok(())
        } else {
            Err(LispError::Signal(
                std::io::Error::last_os_error().to_string(),
            ))
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (modified, nofollow);
        fs::metadata(path)
            .map(|_| ())
            .map_err(|error| LispError::Signal(error.to_string()))
    }
}

pub(crate) fn lock_path_for_file(path: &str) -> PathBuf {
    let expanded = PathBuf::from(expand_file_name(path, None));
    let directory = expanded.parent().map(Path::to_path_buf).unwrap_or_default();
    let file_name = expanded
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(path);
    directory.join(format!(".#{file_name}"))
}

pub(crate) fn file_error_value(message: &str, path: &str) -> Value {
    Value::list([
        Value::Symbol("file-error".into()),
        Value::String(message.into()),
        Value::String(path.into()),
    ])
}

pub(crate) fn file_error_with_detail_value(message: &str, detail: &str, path: &str) -> Value {
    Value::list([
        Value::Symbol("file-error".into()),
        Value::String(message.into()),
        Value::String(detail.into()),
        Value::String(path.into()),
    ])
}

pub(crate) fn file_output_error(path: &str, error: &std::io::Error) -> LispError {
    let rendered = error.to_string();
    let detail = rendered
        .split_once(" (os error")
        .map(|(detail, _)| detail)
        .unwrap_or(rendered.as_str());
    LispError::SignalValue(file_error_with_detail_value(
        "Opening output file",
        detail,
        path,
    ))
}

pub(crate) fn file_locked_p(path: &str) -> Result<Value, LispError> {
    let lock_path = lock_path_for_file(path);
    match fs::metadata(&lock_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                Err(LispError::SignalValue(file_error_value(
                    "Testing file lock",
                    path,
                )))
            } else {
                Ok(Value::T)
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Value::Nil),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

pub(crate) fn gensym_prefix(value: Option<&Value>) -> Result<String, LispError> {
    match value {
        None | Some(Value::Nil) => Ok("g".into()),
        Some(value) => string_text(value),
    }
}

pub(crate) fn file_change_cache_key(path: &str, tag: Option<&Value>) -> Result<String, LispError> {
    let tag_name = match tag {
        None | Some(Value::Nil) => "nil".into(),
        Some(value) => value.as_symbol()?.to_string(),
    };
    Ok(format!("{tag_name}@{path}"))
}

pub(crate) fn file_change_cache_value(path: &str) -> Result<Option<(u64, u128)>, LispError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(LispError::Signal(error.to_string())),
    };
    let modified = metadata
        .modified()
        .map_err(|error| LispError::Signal(error.to_string()))?
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LispError::Signal(error.to_string()))?
        .as_nanos();
    Ok(Some((metadata.len(), modified)))
}

pub(crate) fn is_circular_list_value(value: &Value) -> bool {
    matches!(
        value.to_vec(),
        Err(LispError::SignalValue(signal)) if circular_list_signal_p(&signal)
    )
}

pub(crate) fn make_temp_file_internal(
    prefix: &str,
    dir_flag: &Value,
    suffix: &str,
    text: Option<&Value>,
) -> Result<String, LispError> {
    let mut attempt = 0u64;
    loop {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LispError::Signal(error.to_string()))?
            .as_nanos();
        let path = format!("{prefix}{stamp:x}{attempt:x}{suffix}");
        let candidate = PathBuf::from(&path);
        if candidate.exists() {
            attempt = attempt.saturating_add(1);
            continue;
        }
        if dir_flag.is_nil() {
            let mut file = fs::File::create(&candidate)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            if let Some(text) = text.and_then(string_like) {
                file.write_all(text.text.as_bytes())
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }
        } else if !matches!(dir_flag, Value::Integer(0)) {
            fs::create_dir(&candidate).map_err(|error| LispError::Signal(error.to_string()))?;
        }
        return Ok(path);
    }
}

pub(crate) fn maybe_prompt_supersession_threat(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let Some(current_modtime) = file_modtime(&path)? else {
        return Ok(());
    };
    if interp.buffer.visited_file_modtime() != Some(current_modtime) {
        let _ = call_named_function(
            interp,
            "ask-user-about-supersession-threat",
            &[Value::String(path)],
            env,
        )?;
    }
    Ok(())
}

pub(crate) fn decode_inserted_bytes(bytes: &[u8], multibyte: bool, literal: bool) -> String {
    if literal || !multibyte {
        return bytes.iter().map(|byte| char::from(*byte)).collect();
    }
    String::from_utf8(bytes.to_vec())
        .unwrap_or_else(|_| bytes.iter().map(|byte| char::from(*byte)).collect())
}

pub(crate) fn read_insert_file_bytes(
    path: &str,
    start: Option<usize>,
    end: Option<usize>,
) -> Result<Vec<u8>, LispError> {
    validate_file_name(path)?;
    let metadata = fs::metadata(path).map_err(|error| LispError::Signal(error.to_string()))?;
    if metadata.is_dir() {
        return Err(LispError::SignalValue(file_error_with_detail_value(
            "Read error",
            "Is a directory",
            path,
        )));
    }
    if metadata.file_type().is_file() {
        let mut bytes = fs::read(path).map_err(|error| LispError::Signal(error.to_string()))?;
        let start = start.unwrap_or(0).min(bytes.len());
        let end = end.unwrap_or(bytes.len()).clamp(start, bytes.len());
        bytes.truncate(end);
        bytes.drain(..start);
        return Ok(bytes);
    }
    if start.is_some() {
        return Err(LispError::Signal("Cannot seek in non-regular file".into()));
    }
    let limit = end.unwrap_or(8192);
    let mut file = fs::File::open(path).map_err(|error| LispError::Signal(error.to_string()))?;
    let mut buffer = vec![0; limit];
    let read = file
        .read(&mut buffer)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    buffer.truncate(read);
    Ok(buffer)
}

pub(crate) fn coding_tag_from_buffer_text(text: &str) -> Option<String> {
    static CODING_TAG: OnceLock<Regex> = OnceLock::new();
    let regex = CODING_TAG.get_or_init(|| {
        Regex::new(r"coding:\s*([[:alnum:]-]+)").expect("coding tag regex is valid")
    });
    text.lines()
        .take(2)
        .find_map(|line| regex.captures(line))
        .and_then(|captures| captures.get(1).map(|value| value.as_str().to_string()))
}

pub(crate) fn coding_tag_from_bytes(bytes: &[u8]) -> Option<String> {
    let mut newlines = 0usize;
    let mut end = bytes.len();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            newlines += 1;
            if newlines == 2 {
                end = index + 1;
                break;
            }
        }
    }
    let prefix = String::from_utf8_lossy(&bytes[..end]);
    coding_tag_from_buffer_text(&prefix)
}

pub(crate) fn current_write_coding(
    interp: &Interpreter,
    env: &Env,
    text: &str,
    for_write_file: bool,
) -> Result<String, LispError> {
    if for_write_file && let Some(tag) = coding_tag_from_buffer_text(text) {
        let canonical = interp
            .coding_system_canonical_name(&tag)
            .ok_or_else(|| coding_system_error(tag.clone()))?;
        let base = interp
            .coding_system_base_name(&canonical)
            .unwrap_or(canonical.clone());
        let eol = interp.coding_system_eol_type_value(&canonical).or(Some(0));
        return Ok(coding_variant_name(interp, &base, eol));
    }
    if let Some(value) = interp.lookup_var("coding-system-for-write", env)
        && !value.is_nil()
    {
        return checked_coding_symbol(interp, &value);
    }
    if let Some(value) = interp.lookup_var("buffer-file-coding-system", env)
        && !value.is_nil()
    {
        let current = checked_coding_symbol(interp, &value)?;
        let base = interp
            .coding_system_base_name(&current)
            .unwrap_or(current.clone());
        let eol = interp.coding_system_eol_type_value(&current).or(Some(0));
        if for_write_file && base == "prefer-utf-8" && !ascii_only_text(text) {
            return Ok(coding_variant_name(interp, "utf-8", eol));
        }
        return Ok(coding_variant_name(interp, &base, eol));
    }
    if ascii_only_text(text) {
        Ok(coding_variant_name(interp, "prefer-utf-8", Some(0)))
    } else {
        Ok(coding_variant_name(interp, "utf-8", Some(0)))
    }
}

pub(crate) fn write_region_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let path = resolve_file_name_in_env(interp, env, &string_text(&args[2])?);
    validate_file_name(&path)?;
    let text = if args[0].is_nil() && args.get(1).is_none_or(Value::is_nil) {
        interp.buffer.buffer_string()
    } else if string_like(&args[0]).is_some() {
        string_text(&args[0])?
    } else {
        let start = position_from_value(interp, &args[0])?;
        let end = position_from_value(interp, &args[1])?;
        interp
            .buffer
            .buffer_substring(start, end)
            .map_err(|error| LispError::Signal(error.to_string()))?
    };
    let coding = current_write_coding(interp, env, &text, false)?;
    let bytes = encode_text_bytes(interp, &text, &coding)?;
    if args.get(3).is_some_and(Value::is_truthy) {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| file_output_error(&path, &error))?;
        file.write_all(&bytes)
            .map_err(|error| file_output_error(&path, &error))?;
    } else {
        fs::write(&path, &bytes).map_err(|error| file_output_error(&path, &error))?;
    }
    set_last_coding_system_used(interp, &coding, env);
    dispatch_file_notification(interp, env, &path, "changed")?;
    refresh_current_dired_buffer_for_path(interp, &path, env)?;
    Ok(Value::String(path))
}

pub(crate) fn write_file_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
    if directory_name_p(&path) {
        let base = interp
            .buffer
            .file
            .as_deref()
            .map(file_name_nondirectory)
            .unwrap_or_else(|| file_name_nondirectory(&interp.buffer.name));
        path = file_name_concat(&[path, base]);
    }
    let text = interp.buffer.buffer_string();
    let coding = current_write_coding(interp, env, &text, true)?;
    let bytes = encode_text_bytes(interp, &text, &coding)?;
    fs::write(&path, &bytes).map_err(|error| LispError::Signal(error.to_string()))?;
    interp.buffer.file = Some(path.clone());
    interp.buffer.file_truename = Some(path.clone());
    interp.buffer.set_unmodified();
    interp.set_buffer_local_value(
        interp.current_buffer_id(),
        "buffer-file-coding-system",
        Value::Symbol(coding.clone()),
    );
    set_last_coding_system_used(interp, &coding, env);
    Ok(Value::String(path))
}

pub(crate) fn append_external_debugging_output(
    interp: &mut Interpreter,
    text: &str,
) -> Result<(), LispError> {
    match interp.lookup_var("emaxx-external-debugging-output-target", &Vec::new()) {
        Some(Value::String(path)) => {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            file.write_all(text.as_bytes())
                .map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(())
        }
        Some(Value::StringObject(state)) => {
            let path = state.borrow().text.clone();
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            file.write_all(text.as_bytes())
                .map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(())
        }
        _ => {
            let buffer_id = interp
                .find_buffer(" *external-debugging-output*")
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(" *external-debugging-output*").0);
            let buffer = interp
                .get_buffer_by_id_mut(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
            let end = buffer.point_max();
            buffer.goto_char(end);
            buffer.insert(text);
            Ok(())
        }
    }
}

pub(crate) fn write_printer_output(
    interp: &mut Interpreter,
    text: &str,
    stream: Option<&Value>,
    env: &mut Env,
) -> Result<(), LispError> {
    match stream {
        None | Some(Value::Nil | Value::T) => {
            interp.buffer.insert(text);
            Ok(())
        }
        Some(Value::Buffer(_, _)) => {
            let buffer_id = interp.resolve_buffer_id(stream.expect("matched Some"))?;
            if buffer_id == interp.current_buffer_id() {
                interp.insert_current_buffer(text);
            } else {
                let pos = {
                    let buffer = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {buffer_id}"))
                    })?;
                    buffer.point()
                };
                let nchars = text.chars().count();
                let buffer = interp
                    .get_buffer_by_id_mut(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                buffer.insert(text);
                interp.adjust_markers_for_insert(buffer_id, pos, nchars, false);
            }
            Ok(())
        }
        Some(Value::Marker(id)) => {
            let (buffer_id, position) = {
                let marker = interp.find_marker(*id).ok_or_else(|| {
                    LispError::TypeError("marker".into(), format!("marker<{id}>"))
                })?;
                let buffer_id = marker
                    .buffer_id
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                let position = marker
                    .position
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                (buffer_id, position)
            };
            let new_position = {
                let buffer = interp
                    .get_buffer_by_id_mut(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                let saved_point = buffer.point();
                buffer.goto_char(position);
                buffer.insert(text);
                let new_position = buffer.point();
                buffer.goto_char(saved_point);
                new_position
            };
            interp.set_marker(*id, Some(new_position), Some(buffer_id))?;
            Ok(())
        }
        Some(Value::Symbol(name)) if name == "external-debugging-output" => {
            append_external_debugging_output(interp, text)
        }
        Some(Value::Symbol(_) | Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) => {
            let function = stream.expect("matched Some").clone();
            for ch in text.chars() {
                call_function_value(interp, &function, &[Value::Integer(ch as i64)], env)?;
            }
            Ok(())
        }
        Some(other) => Err(LispError::TypeError(
            "output-stream".into(),
            other.type_name(),
        )),
    }
}

pub(crate) fn printer_stream_value(
    interp: &Interpreter,
    env: &Env,
    explicit: Option<&Value>,
) -> Option<Value> {
    match explicit {
        Some(Value::Nil) => interp.lookup_var("standard-output", env),
        Some(value) => Some(value.clone()),
        None => interp.lookup_var("standard-output", env),
    }
}

pub(crate) fn printer_env_with_overrides(
    env: &Env,
    overrides: Option<&Value>,
) -> Result<Env, LispError> {
    let Some(overrides) = overrides else {
        return Ok(env.clone());
    };
    if overrides.is_nil() {
        return Ok(env.clone());
    }

    let mut adjusted = env.clone();
    let mut bindings = Vec::new();

    match overrides {
        Value::T => {
            bindings.push(("print-length".into(), Value::Nil));
            bindings.push(("print-level".into(), Value::Nil));
        }
        Value::Cons(_, _) => {
            let items = overrides
                .to_vec()
                .map_err(|_| LispError::Signal("invalid print overrides".into()))?;
            let mut start = 0usize;
            if matches!(items.first(), Some(Value::T)) {
                bindings.push(("print-length".into(), Value::Nil));
                bindings.push(("print-level".into(), Value::Nil));
                start = 1;
            }
            for item in &items[start..] {
                let (name, value) = if let Ok(spec) = item.to_vec() {
                    let [Value::Symbol(name), value] = spec.as_slice() else {
                        return Err(LispError::Signal("invalid print overrides".into()));
                    };
                    (name.clone(), value.clone())
                } else if let Some((car, cdr)) = item.cons_values() {
                    let Value::Symbol(name) = car else {
                        return Err(LispError::Signal("invalid print overrides".into()));
                    };
                    if matches!(cdr, Value::Nil | Value::Cons(_, _)) {
                        return Err(LispError::Signal("invalid print overrides".into()));
                    }
                    (name, cdr)
                } else {
                    return Err(LispError::Signal("invalid print overrides".into()));
                };
                match name.as_str() {
                    "length" => bindings.push(("print-length".into(), value)),
                    "level" => bindings.push(("print-level".into(), value)),
                    _ => return Err(LispError::Signal("invalid print overrides".into())),
                }
            }
        }
        _ => return Err(LispError::Signal("invalid print overrides".into())),
    }

    adjusted.push(bindings);
    Ok(adjusted)
}

pub(crate) fn printer_stream_at_line_start(
    interp: &Interpreter,
    stream: Option<&Value>,
) -> Result<bool, LispError> {
    match stream {
        None | Some(Value::Nil | Value::T) => Ok(buffer_position_at_line_start(
            &interp.buffer,
            interp.buffer.point(),
        )),
        Some(Value::Buffer(_, _)) => {
            let buffer_id = interp.resolve_buffer_id(stream.expect("matched Some"))?;
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
            Ok(buffer_position_at_line_start(buffer, buffer.point()))
        }
        Some(Value::Marker(id)) => {
            let marker = interp
                .find_marker(*id)
                .ok_or_else(|| LispError::TypeError("marker".into(), format!("marker<{id}>")))?;
            let buffer_id = marker
                .buffer_id
                .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
            let position = marker
                .position
                .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
            Ok(buffer_position_at_line_start(buffer, position))
        }
        Some(Value::Symbol(name)) if name == "external-debugging-output" => {
            let Some(buffer) = external_debugging_output_buffer(interp) else {
                return Ok(false);
            };
            let empty = buffer.point_min() == buffer.point_max();
            Ok(!empty && buffer_position_at_line_start(buffer, buffer.point()))
        }
        Some(Value::Symbol(_) | Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) => Ok(false),
        Some(other) => Err(LispError::TypeError(
            "output-stream".into(),
            other.type_name(),
        )),
    }
}

pub(crate) fn external_debugging_output_buffer(
    interp: &Interpreter,
) -> Option<&crate::buffer::Buffer> {
    let buffer_id = interp
        .find_buffer(" *external-debugging-output*")
        .map(|(id, _)| id)?;
    interp.get_buffer_by_id(buffer_id)
}

pub(crate) fn buffer_position_at_line_start(
    buffer: &crate::buffer::Buffer,
    position: usize,
) -> bool {
    position <= buffer.point_min() || buffer.char_at(position.saturating_sub(1)) == Some('\n')
}

pub(crate) fn render_princ(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::StringObject(state) => state.borrow().text.clone(),
        _ => value.to_string(),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ParsedFormatSpec {
    pub(crate) flags: Vec<char>,
    pub(crate) width: Option<usize>,
    pub(crate) precision: Option<usize>,
    pub(crate) specifier: char,
    pub(crate) end: usize,
}

pub(crate) fn parse_format_spec(chars: &[char], mut i: usize) -> Option<ParsedFormatSpec> {
    let mut flags = Vec::new();
    while i < chars.len() && matches!(chars[i], ' ' | '0' | '<' | '>' | '^' | '_' | '-') {
        flags.push(chars[i]);
        i += 1;
    }

    let mut width = None;
    if i < chars.len() && chars[i].is_ascii_digit() {
        let mut parsed = 0usize;
        while i < chars.len() && chars[i].is_ascii_digit() {
            parsed = parsed
                .saturating_mul(10)
                .saturating_add(chars[i] as usize - '0' as usize);
            i += 1;
        }
        width = Some(parsed);
    }

    let mut precision = None;
    if i < chars.len() && chars[i] == '.' {
        i += 1;
        if i >= chars.len() || !chars[i].is_ascii_digit() {
            return None;
        }
        let mut parsed = 0usize;
        while i < chars.len() && chars[i].is_ascii_digit() {
            parsed = parsed
                .saturating_mul(10)
                .saturating_add(chars[i] as usize - '0' as usize);
            i += 1;
        }
        precision = Some(parsed);
    }

    let specifier = *chars.get(i)?;
    if !specifier.is_alphabetic() {
        return None;
    }
    Some(ParsedFormatSpec {
        flags,
        width,
        precision,
        specifier,
        end: i + 1,
    })
}

pub(crate) fn format_spec_replacement(
    interp: &mut Interpreter,
    env: &mut Env,
    entries: &[Value],
    specifier: char,
) -> Result<Option<String>, LispError> {
    for entry in entries {
        let Value::Cons(key, value) = entry else {
            continue;
        };
        let key_value = key.borrow().clone();
        let key_char = match &key_value {
            Value::Integer(code) => char::from_u32(*code as u32),
            Value::String(text) => text.chars().next(),
            Value::StringObject(state) => state.borrow().text.chars().next(),
            _ => None,
        };
        if key_char != Some(specifier) {
            continue;
        }

        let mut value_value = value.borrow().clone();
        if value_value.is_nil() {
            return Ok(None);
        }
        let callable = resolve_callable(interp, &value_value, env).unwrap_or(value_value.clone());
        if matches!(callable, Value::BuiltinFunc(_) | Value::Lambda(_, _, _))
            || is_lambda_expression(&callable)
        {
            value_value = call_function_value(interp, &callable, &[], env)?;
        }
        return Ok(Some(
            string_like(&value_value)
                .map(|value| value.text)
                .unwrap_or_else(|| value_value.to_string()),
        ));
    }
    Ok(None)
}

pub(crate) fn format_spec_collapses_quoted_percent(ignore_missing: &Value) -> bool {
    match ignore_missing {
        Value::Nil => true,
        Value::Symbol(symbol) => symbol == "ignore" || symbol == "delete",
        _ => false,
    }
}

pub(crate) fn apply_format_spec_flags(mut text: String, spec: &ParsedFormatSpec) -> String {
    let chop_left = spec.flags.contains(&'<');
    let chop_right = spec.flags.contains(&'>');
    let pad_zero = spec.flags.contains(&'0');
    let pad_right = spec.flags.contains(&'-');
    if let Some(precision) = spec.precision {
        let width = text.chars().count();
        if width > precision {
            text = if chop_left {
                text.chars().skip(width - precision).collect()
            } else {
                text.chars().take(precision).collect()
            };
        }
    }

    if let Some(target_width) = spec.width {
        let width = text.chars().count();
        if width < target_width {
            let padding = target_width - width;
            let pad = if pad_zero { '0' } else { ' ' };
            let pad_text = std::iter::repeat_n(pad, padding).collect::<String>();
            text = if pad_right {
                format!("{text}{pad_text}")
            } else {
                format!("{pad_text}{text}")
            };
        } else if width > target_width {
            text = if chop_left {
                text.chars().skip(width - target_width).collect()
            } else if chop_right {
                text.chars().take(target_width).collect()
            } else {
                text
            };
        }
    }

    if spec.flags.contains(&'^') {
        text = text.to_uppercase();
    }
    if spec.flags.contains(&'_') {
        text = text.to_lowercase();
    }
    text
}

pub(crate) fn custom_choice_tag(choice: &Value) -> Option<Value> {
    let items = choice.to_vec().ok()?;
    let tag_index = items
        .iter()
        .position(|item| matches!(item, Value::Symbol(symbol) if symbol == ":tag"))?;
    items.get(tag_index + 1).cloned()
}

pub(crate) fn decode_file_contents(
    interp: &Interpreter,
    env: &Env,
    bytes: &[u8],
    literal: bool,
) -> Result<(String, String), LispError> {
    if literal {
        return Ok((
            decode_inserted_bytes(bytes, interp.buffer.is_multibyte(), true),
            "no-conversion".into(),
        ));
    }
    let requested = interp
        .lookup_var("coding-system-for-read", env)
        .map(|value| checked_coding_name(interp, &value))
        .transpose()?
        .flatten();
    if let Some(requested) = requested {
        if requested == "undecided" {
            let (detected, normalized) = auto_detect_coding(interp, bytes);
            return Ok((decode_text_bytes(interp, &normalized, &detected)?, detected));
        }
        if interp.coding_system_kind_name(&requested).as_deref() == Some("utf-8-auto") {
            let actual_eol = detect_eol_type(bytes);
            let normalized = decode_bytes_with_explicit_eol(bytes, actual_eol);
            let (has_bom, bomless) = strip_utf8_bom(&normalized);
            let detected = coding_variant_name(
                interp,
                if has_bom {
                    "utf-8-with-signature"
                } else {
                    "utf-8"
                },
                Some(actual_eol),
            );
            return Ok((
                decode_utf8_bytes(if has_bom { bomless } else { &normalized }),
                detected,
            ));
        }
        let actual_eol = detect_eol_type(bytes);
        let explicit_eol = interp.coding_system_eol_type_value(&requested);
        let requested_base = interp
            .coding_system_base_name(&requested)
            .unwrap_or(requested.clone());
        if matches!(requested_base.as_str(), "unix" | "dos" | "mac") {
            let eol = explicit_eol.unwrap_or(0);
            let normalized = decode_bytes_with_explicit_eol(bytes, eol);
            let detected_base = if std::str::from_utf8(&normalized).is_ok() {
                let decoded = decode_utf8_bytes(&normalized);
                if ascii_only_text(&decoded) {
                    requested_base.clone()
                } else {
                    "utf-8".into()
                }
            } else if normalized.iter().any(|byte| *byte > 0x7F) {
                "raw-text".into()
            } else {
                requested_base.clone()
            };
            let detected = if matches!(detected_base.as_str(), "unix" | "dos" | "mac") {
                requested.clone()
            } else {
                coding_variant_name(interp, &detected_base, Some(eol))
            };
            let text = match detected_base.as_str() {
                "utf-8" => decode_utf8_bytes(&normalized),
                "raw-text" => decode_raw_text_bytes(&normalized),
                _ => normalized.iter().map(|byte| char::from(*byte)).collect(),
            };
            return Ok((text, detected));
        }
        let normalized = decode_bytes_with_explicit_eol(bytes, explicit_eol.unwrap_or(actual_eol));
        let detected = if explicit_eol.is_some() {
            requested.clone()
        } else {
            coding_variant_name(interp, &requested_base, Some(actual_eol))
        };
        if interp.coding_system_kind_name(&requested).as_deref() == Some("utf-8-with-signature") {
            let (_, bomless) = strip_utf8_bom(&normalized);
            return Ok((decode_utf8_bytes(bomless), detected));
        }
        return Ok((
            decode_text_bytes(interp, &normalized, &requested)?,
            detected,
        ));
    }
    let (detected, normalized) = auto_detect_coding(interp, bytes);
    Ok((decode_text_bytes(interp, &normalized, &detected)?, detected))
}

pub(crate) fn insert_file_contents(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
    literal: bool,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 5 {
        return Err(LispError::WrongNumberOfArgs(
            if literal {
                "insert-file-contents-literally".into()
            } else {
                "insert-file-contents".into()
            },
            args.len(),
        ));
    }
    let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
    let visit = args.get(1).is_some_and(Value::is_truthy);
    let start = args
        .get(2)
        .filter(|value| !value.is_nil())
        .map(|value| value.as_integer().map(|value| value.max(0) as usize))
        .transpose()?;
    let end = args
        .get(3)
        .filter(|value| !value.is_nil())
        .map(|value| value.as_integer().map(|value| value.max(0) as usize))
        .transpose()?;
    let replace = args.get(4).is_some_and(Value::is_truthy);
    if let Some(coding) = interp.lookup_var("coding-system-for-read", env)
        && !coding.is_nil()
    {
        let _ = checked_coding_symbol(interp, &coding)?;
    }
    let mut bytes = read_insert_file_bytes(&path, start, end)?;
    if !literal && start.is_none() && end.is_none() && should_auto_decompress(interp, env, &path) {
        bytes = maybe_decompress_file_bytes(&path, bytes)?;
    }
    let (text, detected) = decode_file_contents(interp, env, &bytes, literal)?;
    if replace {
        maybe_prompt_supersession_threat(interp, env)?;
        let start = interp.buffer.point_min();
        let end = interp.buffer.point_max();
        interp.buffer.goto_char(start);
        interp
            .delete_region_current_buffer(start, end)
            .map_err(LispError::from)?;
        interp.buffer.goto_char(start);
    }
    if let Some(hooks) = interp.lookup_var("after-insert-file-functions", env)
        && is_circular_list_value(&hooks)
    {
        return Err(LispError::SignalValue(Value::list([
            Value::Symbol("circular-list".into()),
            Value::String("Circular list".into()),
        ])));
    }
    let insert_at = interp.buffer.point();
    interp.insert_current_buffer(&text);
    interp.buffer.goto_char(insert_at);
    interp.set_buffer_local_value(
        interp.current_buffer_id(),
        "buffer-file-coding-system",
        Value::Symbol(detected.clone()),
    );
    if visit {
        interp.buffer.file = Some(path.clone());
        interp.buffer.file_truename = Some(path.clone());
        interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
        interp.buffer.set_unmodified();
    }
    set_last_coding_system_used(interp, &detected, env);
    Ok(Value::list([
        Value::String(path),
        Value::Integer(text.chars().count() as i64),
    ]))
}

pub(crate) fn current_buffer_file(interp: &Interpreter) -> Option<&str> {
    interp
        .buffer
        .file_truename
        .as_deref()
        .or(interp.buffer.file.as_deref())
}

pub(crate) fn maybe_lock_current_buffer(
    interp: &mut Interpreter,
    env: &Env,
) -> Result<(), LispError> {
    if !interp
        .lookup_var("create-lockfiles", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    if !interp.buffer.is_modified() {
        return Ok(());
    }
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let lock_path = lock_path_for_file(&path);
    match fs::metadata(&lock_path) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            fs::write(lock_path, format!("emaxx:{}", std::process::id()))
                .map_err(|err| LispError::Signal(err.to_string()))
        }
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

pub(crate) fn unlock_current_buffer(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(Value::Nil);
    };
    let lock_path = lock_path_for_file(&path);
    match fs::metadata(&lock_path) {
        Ok(metadata) if metadata.is_dir() => {
            call_named_function(
                interp,
                "userlock--handle-unlock-error",
                &[file_error_value("Unlocking file", &path)],
                env,
            )?;
            Ok(Value::Nil)
        }
        Ok(_) => {
            fs::remove_file(&lock_path).map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(Value::Nil)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Value::Nil),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

pub(crate) fn current_buffer_file_text(
    interp: &Interpreter,
    env: &Env,
    path: &str,
) -> Result<(String, String, bool), LispError> {
    let literal = interp
        .buffer_local_value(interp.current_buffer_id(), "find-file-literally")
        .is_some_and(|value| value.is_truthy());
    let mut bytes = read_insert_file_bytes(path, None, None)?;
    if !literal && should_auto_decompress(interp, env, path) {
        bytes = maybe_decompress_file_bytes(path, bytes)?;
    }
    if literal || !interp.buffer.is_multibyte() {
        return Ok((decode_raw_text_bytes(&bytes), "no-conversion".into(), false));
    }
    let (text, coding) = decode_file_contents(interp, env, &bytes, false)?;
    Ok((text, coding, true))
}

pub(crate) fn ensure_no_supersession_threat(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let Some(current_modtime) = file_modtime(&path)? else {
        return Ok(());
    };
    if interp.buffer.visited_file_modtime() == Some(current_modtime) {
        return Ok(());
    }
    let (disk_text, _, _) = current_buffer_file_text(interp, env, &path)?;
    if disk_text == interp.buffer.saved_text() {
        interp
            .buffer
            .set_visited_file_modtime(Some(current_modtime));
        return Ok(());
    }
    if interp
        .lookup_var("noninteractive", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::Signal(
            "Cannot resolve conflict in batch mode".into(),
        ));
    }
    let prompt = format!(
        "{} changed on disk; really edit the buffer?",
        file_name_nondirectory(&path)
    );
    let answer = call_named_function(
        interp,
        "read-char-choice",
        &[
            Value::String(prompt),
            Value::list([
                Value::Integer('y' as i64),
                Value::Integer('n' as i64),
                Value::Integer('r' as i64),
            ]),
        ],
        env,
    )?;
    match answer.as_integer()? as u8 as char {
        'y' => {
            let _ = call(
                interp,
                "message",
                &[Value::String(
                    "File on disk now will become a backup file if you save these changes.".into(),
                )],
                env,
            )?;
            interp
                .buffer
                .set_visited_file_modtime(Some(current_modtime));
            Ok(())
        }
        'r' => {
            revert_current_buffer(interp, env)?;
            Err(LispError::SignalValue(Value::list([
                Value::Symbol("file-supersession".into()),
                Value::String("File reverted".into()),
                Value::String(path),
            ])))
        }
        _ => Err(LispError::SignalValue(Value::list([
            Value::Symbol("file-supersession".into()),
            Value::String("File changed on disk".into()),
            Value::String(path),
        ]))),
    }
}

pub(crate) fn revert_current_buffer(interp: &mut Interpreter, env: &Env) -> Result<(), LispError> {
    let Some(path) = interp.buffer.file.clone() else {
        return Ok(());
    };
    let (text, coding, multibyte) = current_buffer_file_text(interp, env, &path)?;
    let current_id = interp.current_buffer_id();
    let related = interp.related_buffer_ids(current_id);
    let name = interp.buffer.name.clone();
    let file = interp.buffer.file.clone();
    let file_truename = interp.buffer.file_truename.clone();
    let inhibit_hooks = interp.buffer.inhibit_hooks;
    interp.buffer = crate::buffer::Buffer::from_text(&name, &text);
    interp.buffer.set_multibyte(multibyte);
    interp.buffer.file = file;
    interp.buffer.file_truename = file_truename;
    interp.buffer.inhibit_hooks = inhibit_hooks;
    interp.buffer.set_unmodified();
    interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
    interp.set_buffer_local_value(
        interp.current_buffer_id(),
        "buffer-file-coding-system",
        Value::Symbol(coding.clone()),
    );
    let visited_file_modtime = file_modtime(&path)?;
    for buffer_id in related {
        if buffer_id == current_id {
            continue;
        }
        if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
            let name = buffer.name.clone();
            let file = buffer.file.clone();
            let file_truename = buffer.file_truename.clone();
            let inhibit_hooks = buffer.inhibit_hooks;
            let point = buffer.point().min(text.chars().count() + 1);
            *buffer = crate::buffer::Buffer::from_text(&name, &text);
            buffer.set_multibyte(multibyte);
            buffer.file = file;
            buffer.file_truename = file_truename;
            buffer.inhibit_hooks = inhibit_hooks;
            buffer.goto_char(point);
            buffer.set_unmodified();
            buffer.set_visited_file_modtime(visited_file_modtime);
        }
        interp.set_buffer_local_value(
            buffer_id,
            "buffer-file-coding-system",
            Value::Symbol(coding.clone()),
        );
    }
    Ok(())
}

pub(crate) fn shell_quote_argument(argument: &str) -> String {
    if argument.is_empty() {
        return "''".into();
    }

    let mut quoted = String::new();
    for ch in argument.chars() {
        match ch {
            '\n' => quoted.push_str("'\n'"),
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '/' => quoted.push(ch),
            _ => {
                quoted.push('\\');
                quoted.push(ch);
            }
        }
    }
    quoted
}

pub(crate) fn first_choice_value(choices: &Value) -> Option<Value> {
    choices
        .to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|item| {
            item.to_vec()
                .ok()
                .and_then(|nested| nested.first().cloned())
                .or(Some(item))
        })
}
