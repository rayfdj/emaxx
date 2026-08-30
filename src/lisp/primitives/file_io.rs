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
    let local_path = unquote_local_file_name(path).unwrap_or_else(|| path.to_string());
    match fs::metadata(local_path) {
        Ok(metadata) => Ok(Some(crate::buffer::FileModTime {
            modified: metadata
                .modified()
                .map_err(|error| LispError::Signal(error.to_string()))?,
        })),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

/// Compare modification times at whole-second resolution, like `stat'
/// output read over a Tramp connection.
pub(crate) fn modtimes_equal_whole_seconds(
    left: &Option<crate::buffer::FileModTime>,
    right: &Option<crate::buffer::FileModTime>,
) -> bool {
    fn whole_seconds(modtime: &Option<crate::buffer::FileModTime>) -> Option<u64> {
        modtime.as_ref().map(|modtime| {
            modtime
                .modified
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_secs())
                .unwrap_or(0)
        })
    }
    whole_seconds(left) == whole_seconds(right)
}

// GNU `file-attributes' time fields are (HIGH LOW USEC PSEC) lists.
pub(crate) fn system_time_list_value(time: SystemTime) -> Result<Value, LispError> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    Ok(unix_time_list_value(
        duration.as_secs() as i64,
        duration.subsec_nanos() as i64,
    ))
}

pub(crate) fn unix_time_list_value(seconds: i64, nanoseconds: i64) -> Value {
    Value::list([
        Value::Integer(seconds >> 16),
        Value::Integer(seconds & 0xffff),
        Value::Integer(nanoseconds.div_euclid(1_000)),
        Value::Integer(nanoseconds.rem_euclid(1_000) * 1_000),
    ])
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
        let timestamp = system_time_to_timeval(modified)?;
        // GNU set-file-times sets both atime and mtime to TIMESTAMP.
        let times = [timestamp, timestamp];
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
            let error = std::io::Error::last_os_error();
            Err(file_operation_error("Setting file times", &error, path))
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (modified, nofollow);
        fs::metadata(path)
            .map(|_| ())
            .map_err(|error| file_operation_error("Setting file times", &error, path))
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

fn current_lock_identity() -> (String, String, i64) {
    let user = current_user_login_name().unwrap_or_default();
    let host = system_name_value().replace('@', "-");
    (user, host, i64::from(std::process::id()))
}

fn current_lock_info() -> String {
    let (user, host, pid) = current_lock_identity();
    format!("{user}@{host}.{pid}")
}

fn read_lock_info(path: &Path, metadata: &fs::Metadata) -> Option<String> {
    if metadata.file_type().is_symlink() {
        fs::read_link(path)
            .ok()
            .map(|target| target.to_string_lossy().into_owned())
    } else {
        fs::read_to_string(path).ok()
    }
}

fn parse_lock_info(info: &str) -> Option<(&str, &str, i64)> {
    let (user, host_and_pid) = info.rsplit_once('@')?;
    let (host, pid_and_boot_time) = host_and_pid.rsplit_once('.')?;
    let pid = pid_and_boot_time
        .split_once(':')
        .map_or(pid_and_boot_time, |(pid, _)| pid)
        .parse()
        .ok()?;
    Some((user, host, pid))
}

#[cfg(unix)]
fn process_is_alive(pid: i64) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_alive(pid: i64) -> bool {
    pid == i64::from(std::process::id())
}

enum LockOwnership<'a> {
    CurrentProcess,
    Other(&'a str),
    Stale,
}

fn lock_ownership(info: &str) -> Option<LockOwnership<'_>> {
    let (user, host, pid) = parse_lock_info(info)?;
    let (_, current_host, current_pid) = current_lock_identity();
    if host != current_host {
        return Some(LockOwnership::Other(user));
    }
    if pid == current_pid {
        return Some(LockOwnership::CurrentProcess);
    }
    Some(if process_is_alive(pid) {
        LockOwnership::Other(user)
    } else {
        LockOwnership::Stale
    })
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

pub(crate) fn file_operation_error_value(
    message: &str,
    error: &std::io::Error,
    path: &str,
) -> Value {
    let condition = match error.kind() {
        ErrorKind::NotFound => "file-missing",
        ErrorKind::AlreadyExists => "file-already-exists",
        _ => "file-error",
    };
    let rendered = error.to_string();
    let detail = rendered
        .split_once(" (os error")
        .map_or(rendered.as_str(), |(detail, _)| detail);
    Value::list([
        Value::Symbol(condition.into()),
        Value::String(message.into()),
        Value::String(detail.into()),
        Value::String(path.into()),
    ])
}

pub(crate) fn file_operation_error(message: &str, error: &std::io::Error, path: &str) -> LispError {
    LispError::SignalValue(file_operation_error_value(message, error, path))
}

pub(crate) fn file_input_error_value(error: &std::io::Error, path: &str) -> Value {
    file_operation_error_value("Opening input file", error, path)
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

pub(crate) fn file_locked_p(
    interp: &Interpreter,
    env: &Env,
    logical_path: &str,
) -> Result<Value, LispError> {
    let path = resolve_file_name_in_env(interp, env, logical_path);
    let lock_path = lock_path_for_file(&path);
    match fs::symlink_metadata(&lock_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                return Err(LispError::SignalValue(file_error_value(
                    "Testing file lock",
                    &path,
                )));
            }
            let owner = read_lock_info(&lock_path, &metadata).ok_or_else(|| {
                LispError::SignalValue(file_error_value("Testing file lock", &path))
            })?;
            match lock_ownership(&owner) {
                Some(LockOwnership::CurrentProcess) => Ok(Value::T),
                Some(LockOwnership::Other(user)) => Ok(Value::String(user.into())),
                Some(LockOwnership::Stale) => {
                    fs::remove_file(&lock_path).map_err(|_| {
                        LispError::SignalValue(file_error_value("Testing file lock", &path))
                    })?;
                    Ok(Value::Nil)
                }
                None => Err(LispError::SignalValue(file_error_value(
                    "Testing file lock",
                    &path,
                ))),
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Value::Nil),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
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
    // fileio.c Fmake_temp_file_internal -> gen_tempname: PREFIX plus the
    // six-character [a-zA-Z0-9] segment plus SUFFIX, retried on collision;
    // the entry is created atomically (O_EXCL file / mkdir) unless
    // DIR-FLAG is 0, which only names it.
    let mut last = String::new();
    for _ in 0..62 * 62 {
        let path = format!(
            "{prefix}{}{suffix}",
            crate::lisp::primitives::coding::random_temp_suffix()
        );
        let candidate = PathBuf::from(&path);
        last = path;
        if matches!(dir_flag, Value::Integer(0)) {
            if candidate.exists() {
                continue;
            }
            return Ok(last);
        }
        if dir_flag.is_nil() {
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&candidate)
            {
                Ok(mut file) => {
                    if let Some(text) = text.and_then(string_like) {
                        file.write_all(text.text.as_bytes())
                            .map_err(|error| LispError::Signal(error.to_string()))?;
                    }
                    return Ok(last);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(LispError::Signal(error.to_string())),
            }
        }
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(last),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(LispError::Signal(error.to_string())),
        }
    }
    Ok(last)
}

pub(crate) fn decode_inserted_bytes(bytes: &[u8], multibyte: bool, literal: bool) -> String {
    if literal || !multibyte {
        return decode_raw_text_bytes(bytes);
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
    let input_error =
        |error: std::io::Error| LispError::SignalValue(file_input_error_value(&error, path));
    let metadata = fs::metadata(path).map_err(&input_error)?;
    if metadata.is_dir() {
        return Err(LispError::SignalValue(file_error_with_detail_value(
            "Read error",
            "Is a directory",
            path,
        )));
    }
    if metadata.file_type().is_file() {
        let mut bytes = fs::read(path).map_err(&input_error)?;
        let start = start.unwrap_or(0).min(bytes.len());
        let end = end.unwrap_or(bytes.len()).clamp(start, bytes.len());
        bytes.truncate(end);
        bytes.drain(..start);
        return Ok(bytes);
    }
    if start.is_some() {
        return Err(LispError::SignalValue(file_error_with_detail_value(
            "Read error",
            "Cannot seek in non-regular file",
            path,
        )));
    }
    let limit = end.unwrap_or(8192);
    let mut file = fs::File::open(path).map_err(&input_error)?;
    let mut buffer = vec![0; limit];
    let read = file.read(&mut buffer).map_err(&input_error)?;
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
    let logical_path = string_text(&args[2])?;
    write_region_value_with_logical_path(interp, args, &logical_path, None, env)
}

pub(crate) fn write_region_value_with_logical_path(
    interp: &mut Interpreter,
    args: &[Value],
    logical_path: &str,
    logical_lock_path: Option<&str>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let requested_path = string_text(&args[2])?;
    let path = resolve_file_name_in_env(interp, env, &requested_path);
    validate_file_name(&path)?;
    let (text, source_multibyte) = if args[0].is_nil() && args.get(1).is_none_or(Value::is_nil) {
        (interp.buffer.buffer_string(), interp.buffer.is_multibyte())
    } else if string_like(&args[0]).is_some() {
        let string = string_like(&args[0]).expect("checked string-like value");
        (string.text, string.multibyte)
    } else {
        let start = position_from_value(interp, &args[0])?;
        let end = position_from_value(interp, &args[1])?;
        (
            interp
                .buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?,
            interp.buffer.is_multibyte(),
        )
    };
    let visiting = args
        .get(4)
        .is_some_and(|visit| matches!(visit, Value::T) || string_like(visit).is_some());
    let coding = current_write_coding(interp, env, &text, visiting)?;
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    let no_conversion = interp
        .coding_system_base_name(&coding)
        .is_some_and(|base| base == "no-conversion");
    let bytes = if no_conversion && source_multibyte {
        encode_internal_multibyte_bytes(&text)?
    } else {
        encode_text_bytes(interp, &text, &coding, inhibit_eol_conversion)?
    };
    if let Some(mustbenew) = args.get(6).filter(|value| value.is_truthy())
        && fs::symlink_metadata(&path).is_ok()
    {
        let overwrite = if mustbenew.as_symbol().ok() == Some("excl") {
            false
        } else {
            call_named_function(
                interp,
                "yes-or-no-p",
                &[Value::String(
                    format!("File {logical_path} already exists; overwrite anyway? ").into(),
                )],
                env,
            )?
            .is_truthy()
        };
        if !overwrite {
            return Err(file_operation_error(
                "Opening output file",
                &std::io::Error::from(ErrorKind::AlreadyExists),
                logical_path,
            ));
        }
    }
    let lock_path = logical_lock_path
        .map(str::to_string)
        .or_else(|| args.get(5).and_then(string_like).map(|string| string.text))
        .or_else(|| args.get(4).and_then(string_like).map(|string| string.text))
        .unwrap_or_else(|| logical_path.to_string());
    let lock_enabled = interp
        .lookup_var("create-lockfiles", env)
        .is_some_and(|value| value.is_truthy());
    // GNU always enters lock-file here: that function performs the
    // supersession check even when `create-lockfiles' suppresses the actual
    // lock artifact.
    call_named_function(
        interp,
        "lock-file",
        &[Value::String(lock_path.clone().into())],
        env,
    )?;
    let write_result = (|| {
        if let Some(offset) = args
            .get(3)
            .filter(|value| value.is_truthy())
            .and_then(|value| value.as_integer().ok())
        {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .map_err(|error| file_output_error(&path, &error))?;
            file.seek(SeekFrom::Start(offset.max(0) as u64))
                .map_err(|error| file_output_error(&path, &error))?;
            file.write_all(&bytes)
                .map_err(|error| file_output_error(&path, &error))
        } else if args.get(3).is_some_and(Value::is_truthy) {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|error| file_output_error(&path, &error))?;
            file.write_all(&bytes)
                .map_err(|error| file_output_error(&path, &error))
        } else {
            fs::write(&path, &bytes).map_err(|error| file_output_error(&path, &error))
        }
    })();
    let unlock_result = if lock_enabled {
        call_named_function(
            interp,
            "unlock-file",
            &[Value::String(lock_path.into())],
            env,
        )
        .map(drop)
    } else {
        Ok(())
    };
    match (write_result, unlock_result) {
        (Err(error), _) | (Ok(()), Err(error)) => return Err(error),
        (Ok(()), Ok(())) => {}
    }
    set_last_coding_system_used(interp, &coding, env);
    dispatch_file_notification(interp, env, &path, "changed")?;
    if let Some(visit) = args.get(4)
        && (matches!(visit, Value::T) || string_like(visit).is_some())
    {
        let visited_name = if matches!(visit, Value::T) {
            logical_path.to_string()
        } else {
            string_text(visit)?
        };
        let visited_name = expand_file_name_runtime(interp, env, &visited_name, None)?;
        interp.buffer.file = Some(visited_name);
        interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
        interp.buffer.set_unmodified();
        unlock_current_buffer(interp, env)?;
    }
    if interp
        .lookup_var("noninteractive", env)
        .is_none_or(|value| value.is_nil())
        && args.get(4).is_none_or(|visit| {
            visit.is_nil() || matches!(visit, Value::T) || string_like(visit).is_some()
        })
    {
        call_named_function(
            interp,
            "message",
            &[Value::String(format!("Wrote {logical_path}").into())],
            env,
        )?;
    }
    Ok(Value::Nil)
}

pub(crate) fn append_external_debugging_output(
    interp: &mut Interpreter,
    text: &str,
) -> Result<(), LispError> {
    match interp.external_debugging_output_target.as_deref() {
        Some(path) => {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            file.write_all(text.as_bytes())
                .map_err(|error| LispError::Signal(error.to_string()))?;
            Ok(())
        }
        None => {
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
        // An explicit `t' stream prints to the echo area, which
        // `ert-with-message-capture' observes like the upstream print
        // advice; it never inserts into the current buffer.  In batch mode
        // GNU also treats that echo-area stream as the process stdout.
        Some(Value::T) => {
            interp.append_message_capture(text, false, env);
            if interp
                .lookup_var("noninteractive", env)
                .is_some_and(|value| value.is_truthy())
            {
                std::io::stdout()
                    .write_all(text.as_bytes())
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            } else {
                // An interactive session's `t' stream is the echo area
                // (print_string to Qt): eval-expression's result shows.
                crate::lisp::primitives::echo_area_print(text);
            }
            Ok(())
        }
        None | Some(Value::Nil) => {
            interp.append_message_capture(text, false, env);
            interp.buffer.insert(text);
            Ok(())
        }
        Some(Value::Buffer(_)) => {
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
        Some(Value::Symbol(_) | Value::BuiltinFunc(_) | Value::Lambda(_)) => {
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

/// Record output that GNU's printer sends through `printchar'.
///
/// Its batch `terpri' state is deliberately narrower than the last byte
/// written to stdout: bulk `strout' paths (notably plain strings and numbers)
/// do not update it.  Callers therefore opt in only for character-wise paths.
pub(crate) fn record_batch_standard_output_char(
    interp: &mut Interpreter,
    stream: Option<&Value>,
    env: &Env,
    ch: char,
) {
    if stream.is_some_and(|value| matches!(value, Value::T))
        && interp
            .lookup_var("noninteractive", env)
            .is_some_and(|value| value.is_truthy())
    {
        interp.batch_standard_output_last_char = Some(ch);
    }
}

/// Whether native `print' emits the final character of VALUE through GNU's
/// character-wise printer rather than its bulk string path.
pub(crate) fn native_print_updates_batch_last_char(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
    escaped: bool,
) -> bool {
    match value {
        Value::Integer(integer) => {
            interp
                .lookup_var("print-integers-as-characters", env)
                .is_some_and(|value| value.is_truthy())
                && render_princ_integer_as_character(&Value::Integer(*integer)).is_some()
        }
        Value::BigInteger(_) | Value::Float(_) => false,
        Value::String(_) | Value::StringObject(_) => escaped,
        Value::Symbol(name) => !name.is_empty(),
        _ => true,
    }
}

pub(crate) fn printer_stream_value(
    interp: &Interpreter,
    env: &Env,
    explicit: Option<&Value>,
) -> Option<Value> {
    let resolved = match explicit {
        Some(Value::Nil) => interp.lookup_var("standard-output", env),
        Some(value) => Some(value.clone()),
        None => interp.lookup_var("standard-output", env),
    };
    match resolved {
        // GNU's `print_prepare' turns a nil effective stream into t after
        // resolving `standard-output'.
        Some(Value::Nil) => Some(Value::T),
        other => other,
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
        Value::Cons(_) => {
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
                    if matches!(cdr, Value::Nil | Value::Cons(_)) {
                        return Err(LispError::Signal("invalid print overrides".into()));
                    }
                    (name, cdr)
                } else {
                    return Err(LispError::Signal("invalid print overrides".into()));
                };
                // GNU print.c's full OVERRIDES key set (print_bind_overrides).
                let variable = match name.as_str() {
                    "length" => "print-length",
                    "level" => "print-level",
                    "circle" => "print-circle",
                    "quoted" => "print-quoted",
                    "escape-newlines" => "print-escape-newlines",
                    "escape-control-characters" => "print-escape-control-characters",
                    "escape-nonascii" => "print-escape-nonascii",
                    "escape-multibyte" => "print-escape-multibyte",
                    "charset-text-property" => "print-charset-text-property",
                    "unreadable-function" => "print-unreadable-function",
                    "gensym" => "print-gensym",
                    "continuous-numbering" => "print-continuous-numbering",
                    "number-table" => "print-number-table",
                    "float-format" => "float-output-format",
                    "integers-as-characters" => "print-integers-as-characters",
                    _ => return Err(LispError::Signal("invalid print overrides".into())),
                };
                bindings.push((variable.into(), value));
            }
        }
        _ => return Err(LispError::Signal("invalid print overrides".into())),
    }

    adjusted.push(bindings.into());
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
        Some(Value::Buffer(_)) => {
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
        Some(Value::Symbol(_) | Value::BuiltinFunc(_) | Value::Lambda(_)) => Ok(false),
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

fn auto_coding_for_file(
    interp: &mut Interpreter,
    env: &Env,
    bytes: &[u8],
    filename: &str,
) -> Result<Option<String>, LispError> {
    let Some(function) = interp
        .lookup_var("set-auto-coding-function", env)
        .filter(|value| !value.is_nil())
    else {
        return Ok(None);
    };

    // GNU calls the Lisp-owned auto-coding policy while the undecoded file
    // bytes are visible in the current buffer.  Keep that policy on the Lisp
    // side; this work buffer is only the byte-oriented host adapter it needs.
    let saved_buffer_id = interp.current_buffer_id();
    let base_name = " *auto-coding-work*";
    let temp_name = if interp.has_buffer(base_name) {
        let mut suffix = 2;
        loop {
            let candidate = format!("{base_name}<{suffix}>");
            if !interp.has_buffer(&candidate) {
                break candidate;
            }
            suffix += 1;
        }
    } else {
        base_name.into()
    };
    let (temp_id, _) = interp.create_buffer(&temp_name);
    interp.set_buffer_hooks_inhibited(temp_id, true);
    interp.set_current_buffer_id(temp_id)?;
    interp.buffer.set_multibyte(false);
    interp.insert_current_buffer(&decode_raw_text_bytes(bytes));
    interp.buffer.goto_char(interp.buffer.point_min());

    let mut detection_env = env.clone();
    let result = interp.call_function_value(
        function,
        None,
        &[
            Value::String(filename.to_string().into()),
            Value::Integer(bytes.len() as i64),
        ],
        &mut detection_env,
    );
    if interp.has_buffer_id(saved_buffer_id) {
        let _ = interp.set_current_buffer_id(saved_buffer_id);
    }
    interp.kill_buffer_id(temp_id);
    let coding = result?;
    checked_coding_name(interp, &coding)
}

pub(crate) fn decode_file_contents(
    interp: &mut Interpreter,
    env: &Env,
    bytes: &[u8],
    literal: bool,
    filename: Option<&str>,
) -> Result<(String, String), LispError> {
    if literal {
        return Ok((
            decode_inserted_bytes(bytes, interp.buffer.is_multibyte(), true),
            "no-conversion".into(),
        ));
    }
    let mut requested = interp
        .lookup_var("coding-system-for-read", env)
        .map(|value| checked_coding_name(interp, &value))
        .transpose()?
        .flatten();
    if requested.is_none()
        && let Some(filename) = filename
    {
        requested = auto_coding_for_file(interp, env, bytes, filename)?;
    }
    // GNU's third source, after coding-system-for-read and the auto-coding
    // policy: `file-coding-system-alist' via Ffind_operation_coding_system
    // (fileio.c consults it from insert-file-contents itself).  This is where
    // "\.el\'" maps to prefer-utf-8 -- reading a pure-ASCII .el file must
    // set buffer-file-coding-system to prefer-utf-8-unix, not undecided-unix
    // (coding-tests' prefer-utf-8 row).  Emaxx already implements the
    // function; it was simply never consulted here.
    if requested.is_none()
        && let Some(filename) = filename
    {
        let mut operation_env = env.clone();
        let pair = interp.call_function_value(
            Value::Symbol("find-operation-coding-system".into()),
            Some("find-operation-coding-system"),
            &[
                Value::Symbol("insert-file-contents".into()),
                Value::String(filename.to_string().into()),
            ],
            &mut operation_env,
        )?;
        if let Some((decoding, _)) = pair.cons_values()
            && let Ok(name) = decoding.as_symbol()
        {
            requested = Some(name.to_string());
        }
    }
    // fileio.c Finsert_file_contents: "If enable-multibyte-characters is
    // nil, we must suppress all character code conversion except for
    // end-of-line conversion" -- a unibyte buffer reads EVERYTHING as
    // raw-text (valid UTF-8 included, which stays as its bytes), with the
    // eol either explicitly requested or detected, and the coding recorded
    // as raw-text/raw-text-<eol> accordingly (oracle-probed).  raw-text and
    // no-conversion requests keep their own conversion-free paths below.
    if !interp.buffer.is_multibyte()
        && !requested.as_deref().is_some_and(|request| {
            matches!(
                interp.coding_system_kind_name(request).as_deref(),
                Some("raw-text" | "no-conversion")
            )
        })
    {
        let explicit_eol = requested
            .as_deref()
            .and_then(|request| interp.coding_system_eol_type_value(request));
        let eol = explicit_eol
            .map(Some)
            .unwrap_or_else(|| detect_eol_type_opt(bytes));
        let normalized = decode_bytes_with_explicit_eol(bytes, eol.unwrap_or(0));
        return Ok((
            decode_raw_text_bytes(&normalized),
            coding_variant_name(interp, "raw-text", eol),
        ));
    }
    if let Some(requested) = requested {
        // `no-conversion' is the byte-preserving path used by the Lisp
        // `insert-file-contents-literally' wrapper.  In particular, CRLF
        // and CR bytes must not pass through the normal EOL decoder.
        if requested == "no-conversion" {
            return Ok((decode_raw_text_bytes(bytes), requested));
        }
        if requested == "undecided" {
            let (detected, normalized) = auto_detect_coding(interp, bytes);
            return Ok((decode_text_bytes(interp, &normalized, &detected)?, detected));
        }
        // `prefer-utf-8' is GNU's undecided-type system from mule-conf.el
        // (file-coding-system-alist maps "\.el\'" to it): detection runs
        // as for `undecided', except that a file which decides nothing
        // (pure ASCII) is recorded as prefer-utf-8 rather than undecided.
        // Valid non-ASCII UTF-8 already detects as utf-8 whatever the
        // priorities say, which is the other half of :prefer-utf-8 t.
        if interp.coding_system_base_name(&requested).as_deref() == Some("prefer-utf-8") {
            let (detected, normalized) = auto_detect_coding(interp, bytes);
            let text = decode_text_bytes(interp, &normalized, &detected)?;
            let detected =
                if interp.coding_system_base_name(&detected).as_deref() == Some("undecided") {
                    let eol = interp.coding_system_eol_type_value(&detected);
                    coding_variant_name(interp, "prefer-utf-8", eol)
                } else {
                    detected
                };
            return Ok((text, detected));
        }
        if coding_system_auto_detects_bom(interp, &requested) {
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
        // This branch serves two spellings of the same request: a bare EOL
        // name (`unix') and its canonicalized form (`undecided-unix').  The
        // `requested == "undecided"' check above compares the FULL name, so
        // the eol variants fell through to the raw decoder: reading a UTF-8
        // file with `coding-system-for-read 'unix' produced raw bytes and
        // left buffer-file-coding-system as undecided-unix, where GNU
        // detects the charset and answers utf-8-unix (coding-tests' latin/
        // prefer-utf-8/binary rows, finding 106's file-reading half).
        if matches!(requested_base.as_str(), "unix" | "dos" | "mac")
            || (requested_base == "undecided" && explicit_eol.is_some())
        {
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
    let bytes = match read_insert_file_bytes(&path, start, end) {
        Ok(bytes) => bytes,
        Err(error) => {
            // GNU completes the visiting part of `insert-file-contents'
            // before reporting that a visited file could not be opened.
            // `find-file-noselect' relies on this to create a correctly
            // named buffer for a file that does not exist yet.
            if visit {
                interp.buffer.file = Some(path.clone());
                interp.buffer.set_visited_file_modtime(None);
                interp.buffer.set_unmodified();
            }
            return Err(error);
        }
    };
    let (text, detected) = decode_file_contents(interp, env, &bytes, literal, Some(&path))?;
    // A REPLACE is one file-read transaction, not an ordinary user edit.
    // GNU dynamically hides `buffer-file-name' across its delete+insert so
    // the generic stale-file edit guard cannot interrupt the operation in
    // the middle and leave an empty buffer.
    let file_name_restore = if replace {
        Some(interp.bind_special_dynamic("buffer-file-name", Value::Nil, env)?)
    } else {
        None
    };
    let mut inserted_chars = text.chars().count();
    let original_point = interp.buffer.point();
    let edit_result = (|| {
        if replace {
            let old = interp.buffer.buffer_string();
            let old_chars = old.chars().collect::<Vec<_>>();
            let new_chars = text.chars().collect::<Vec<_>>();
            let prefix = old_chars
                .iter()
                .zip(&new_chars)
                .take_while(|(left, right)| left == right)
                .count();
            let suffix = old_chars[prefix..]
                .iter()
                .rev()
                .zip(new_chars[prefix..].iter().rev())
                .take_while(|(left, right)| left == right)
                .count();
            let old_end = old_chars.len() - suffix;
            let new_end = new_chars.len() - suffix;
            let start = interp.buffer.point_min() + prefix;
            let end = interp.buffer.point_min() + old_end;
            let replacement = new_chars[prefix..new_end].iter().collect::<String>();
            let replacement_len = replacement.chars().count();
            // Fileio's replacement keeps point before inserted text.  Point
            // in (or exactly at the end of) the discarded middle collapses
            // to its start; point after it follows the unchanged suffix.
            let replacement_point = if original_point <= start {
                original_point
            } else if original_point <= end {
                start
            } else {
                original_point - (end - start) + replacement_len
            };
            inserted_chars = replacement.chars().count();
            if start != end {
                // GNU runs the modification hooks only for the portion that
                // actually differs (track-changes relies on these bounds).
                crate::lisp::primitives::delete_region_with_hooks(interp, start, end, env)?;
            }
            interp.buffer.goto_char(start);
            if !replacement.is_empty() {
                crate::lisp::primitives::insert_text_with_hooks(
                    interp,
                    &replacement,
                    &[],
                    &[],
                    false,
                    false,
                    env,
                )?;
            }
            interp
                .buffer
                .goto_char(replacement_point.min(interp.buffer.point_max()));
            return Ok(());
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
        crate::lisp::primitives::insert_text_with_hooks(
            interp,
            &text,
            &[],
            &[],
            false,
            false,
            env,
        )?;
        interp.buffer.goto_char(insert_at);
        Ok(())
    })();
    let restore_result = file_name_restore
        .map(|restore| interp.restore_special_dynamic(restore, env))
        .unwrap_or(Ok(()));
    edit_result?;
    restore_result?;
    // Reading no characters decides nothing: GNU leaves the buffer's
    // coding system at its default and records `undecided'.
    //
    // A read whose detection stayed at bare `undecided' (pure ASCII with
    // no eol byte anywhere) also decides nothing: mule.el's
    // find-new-buffer-file-coding-system answers nil then, so GNU leaves
    // buffer-file-coding-system alone (nil in a fresh buffer) while
    // last-coding-system-used records the bare name.  Any other bare base
    // (a BOM-less UTF-8 file without a newline reads as plain `utf-8')
    // still sets the buffer's coding, with the eol defaulted to unix.
    let detected = if text.is_empty() {
        "undecided".to_string()
    } else {
        if detected != "undecided" {
            let buffer_coding = if interp.coding_system_eol_type_value(&detected).is_some() {
                detected.clone()
            } else {
                let base = interp
                    .coding_system_base_name(&detected)
                    .unwrap_or_else(|| detected.clone());
                coding_variant_name(interp, &base, Some(0))
            };
            interp.set_buffer_local_value(
                interp.current_buffer_id(),
                "buffer-file-coding-system",
                Value::Symbol(buffer_coding.into()),
            );
        }
        detected
    };
    if visit {
        interp.buffer.file = Some(path.clone());
        interp.buffer.file_truename = Some(canonical_file_name(&path));
        interp.buffer.set_visited_file_modtime(file_modtime(&path)?);
        interp.buffer.set_unmodified();
        // GNU restores the pre-read undo list when visiting (fileio.c keeps
        // it aside around the insertion), so the very first interactive undo
        // must not remove the file's own contents.
        interp.buffer.clear_undo();
    }
    // last-coding-system-used keeps the caller's own spelling (`unix',
    // `binary', a bare `utf-8') unless the detector actually resolved
    // something the request left open: a detected charset moves to the
    // canonical name, and a detected eol adds the canonical subsidiary
    // (euc-jp + LF answers japanese-iso-8bit-unix).  Oracle-probed rows
    // throughout; buffer-file-coding-system above stays canonical.
    let recorded = interp
        .lookup_var("coding-system-for-read", env)
        .filter(|value| !value.is_nil())
        .and_then(|value| value.as_symbol().ok().map(str::to_string))
        .filter(|request| {
            let canonical = interp
                .coding_system_canonical_name(request)
                .unwrap_or_else(|| request.clone());
            matches!(
                interp.coding_system_kind_name(&canonical).as_deref(),
                Some("raw-text" | "no-conversion")
            ) || (interp.coding_system_base_name(&detected)
                == interp.coding_system_base_name(&canonical)
                && (interp.coding_system_eol_type_value(&canonical).is_some()
                    || detect_eol_type_opt(&bytes).is_none()))
        })
        .unwrap_or_else(|| detected.clone());
    set_last_coding_system_used(interp, &recorded, env);
    let inserted = finish_insert_file_contents(interp, env, inserted_chars, &args[1..])?;
    Ok(Value::list([
        Value::String(path.into()),
        Value::Integer(inserted as i64),
    ]))
}

fn inserted_count(value: &Value, fallback: usize) -> Result<usize, LispError> {
    if value.is_nil() {
        return Ok(fallback);
    }
    let count = value.as_integer()?;
    usize::try_from(count)
        .map_err(|_| LispError::TypeError("inserted-chars".into(), value.type_name()))
}

/// Run the common tail of GNU `insert-file-contents' after either the host
/// reader or a Lisp file-name handler inserted the text.  File-name handlers
/// replace only the byte-producing front half of the operation; coding and
/// file-format policy still belongs to this shared primitive lifecycle.
pub(crate) fn finish_insert_file_contents(
    interp: &mut Interpreter,
    env: &mut Env,
    mut inserted: usize,
    trailing_args: &[Value],
) -> Result<usize, LispError> {
    let visit = trailing_args.first().cloned().unwrap_or(Value::Nil);

    if let Ok(function) = interp.lookup_function("after-insert-file-set-coding", env) {
        let result = interp.call_function_value(
            function,
            Some("after-insert-file-set-coding"),
            &[Value::Integer(inserted as i64), visit.clone()],
            env,
        )?;
        inserted = inserted_count(&result, inserted)?;
    }

    if inserted == 0 {
        return Ok(inserted);
    }

    if let Ok(function) = interp.lookup_function("format-decode", env) {
        let result = interp.call_function_value(
            function,
            Some("format-decode"),
            &[Value::Nil, Value::Integer(inserted as i64), visit],
            env,
        )?;
        inserted = inserted_count(&result, inserted)?;

        if let Some(hooks) = interp.lookup_var("after-insert-file-functions", env) {
            for hook in hooks.to_vec()? {
                let function = match &hook {
                    Value::Symbol(symbol) => interp.lookup_function(symbol, env)?,
                    function => function.clone(),
                };
                let original_name = hook.as_symbol().ok();
                let result = interp.call_function_value(
                    function,
                    original_name,
                    &[Value::Integer(inserted as i64)],
                    env,
                )?;
                inserted = inserted_count(&result, inserted)?;
            }
        }
    }

    Ok(inserted)
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
    env: &mut Env,
) -> Result<(), LispError> {
    if !interp.buffer.is_modified() {
        return Ok(());
    }
    maybe_lock_current_buffer_file(interp, env)
}

pub(crate) fn maybe_lock_current_buffer_on_change(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    // GNU's prepare_to_modify_buffer locks only ordinary first changes to a
    // real file-visiting buffer.  Silent/internal edits bind this variable
    // and must not leave a lock behind.
    if interp
        .lookup_var("inhibit-modification-hooks", env)
        .is_some_and(|value| value.is_truthy())
        || !interp
            .lookup_var("buffer-file-name", env)
            .is_some_and(|value| value.is_truthy())
        || !interp
            .lookup_var("buffer-file-truename", env)
            .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    maybe_lock_current_buffer(interp, env)
}

pub(crate) fn maybe_lock_current_buffer_file(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(logical_path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    call_named_function(
        interp,
        "lock-file",
        &[Value::String(logical_path.into())],
        env,
    )?;
    Ok(())
}

pub(crate) fn lock_file_path(
    interp: &mut Interpreter,
    env: &mut Env,
    logical_path: &str,
) -> Result<(), LispError> {
    let lock_enabled = interp
        .lookup_var("create-lockfiles", env)
        .is_some_and(|value| value.is_truthy());
    let path = resolve_file_name_in_env(interp, env, logical_path);
    let lock_path = lock_path_for_file(&path);
    let own_lock = lock_enabled
        && fs::symlink_metadata(&lock_path)
            .ok()
            .and_then(|metadata| read_lock_info(&lock_path, &metadata))
            .as_deref()
            .and_then(lock_ownership)
            .is_some_and(|owner| matches!(owner, LockOwnership::CurrentProcess));

    // GNU checks the visited buffer for supersession even when lock-file
    // creation is disabled.  `create-lockfiles' controls the filesystem
    // notice, not whether an external rewrite may be silently overwritten.
    if !own_lock {
        let truename = canonical_file_name(&path);
        let subject_id = interp.buffer_list.iter().find_map(|(id, _)| {
            interp
                .get_buffer_by_id(*id)
                .and_then(|buffer| buffer.file_truename.as_deref())
                .filter(|candidate| *candidate == truename)
                .map(|_| *id)
        });
        if let Some(subject_id) = subject_id {
            let saved_id = interp.current_buffer_id();
            if subject_id != saved_id {
                interp.set_current_buffer_id(subject_id)?;
            }
            let supersession = ensure_no_supersession_threat(interp, env);
            if subject_id != saved_id && interp.has_buffer_id(saved_id) {
                interp.set_current_buffer_id(saved_id)?;
            }
            supersession?;
        }
    }

    if !lock_enabled {
        return Ok(());
    }
    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(lock_path)
    {
        Ok(mut lock) => lock
            .write_all(current_lock_info().as_bytes())
            .map_err(|error| LispError::Signal(error.to_string())),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => Ok(()),
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
    call_named_function(interp, "unlock-file", &[Value::String(path.into())], env)
}

pub(crate) fn unlock_buffer_by_id(
    interp: &mut Interpreter,
    env: &mut Env,
    buffer_id: u64,
) -> Result<Value, LispError> {
    let Some(buffer) = interp.get_buffer_by_id(buffer_id) else {
        return Ok(Value::Nil);
    };
    if !buffer.is_modified() {
        return Ok(Value::Nil);
    }
    let Some(path) = buffer
        .file_truename
        .as_deref()
        .or(buffer.file.as_deref())
        .map(str::to_string)
    else {
        return Ok(Value::Nil);
    };
    call_named_function(interp, "unlock-file", &[Value::String(path.into())], env)
}

pub(crate) fn unlock_file_path(
    interp: &mut Interpreter,
    env: &mut Env,
    logical_path: &str,
) -> Result<Value, LispError> {
    let path = resolve_file_name_in_env(interp, env, logical_path);
    let lock_path = lock_path_for_file(&path);
    match fs::symlink_metadata(&lock_path) {
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
            let metadata = fs::symlink_metadata(&lock_path)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let owner = read_lock_info(&lock_path, &metadata);
            let own_lock = owner
                .as_deref()
                .and_then(lock_ownership)
                .is_some_and(|owner| matches!(owner, LockOwnership::CurrentProcess));
            if own_lock {
                fs::remove_file(&lock_path)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
            }
            Ok(Value::Nil)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(Value::Nil),
        Err(error) => Err(LispError::Signal(error.to_string())),
    }
}

pub(crate) fn ensure_no_supersession_threat(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    // GNU's check is gated on the `buffer-file-name' Lisp value, so a
    // let-binding of it to nil suppresses the conflict prompt entirely
    // (auto-revert-tail-handler relies on this while appending).
    if interp
        .lookup_var("buffer-file-name", env)
        .is_some_and(|value| value.is_nil())
    {
        return Ok(());
    }
    let Some(logical_path) = current_buffer_file(interp).map(str::to_string) else {
        return Ok(());
    };
    let path = resolve_file_name_in_env(interp, env, &logical_path);
    let Some(current_modtime) = file_modtime(&path)? else {
        return Ok(());
    };
    let visited_modtime = interp.buffer.visited_file_modtime();
    // An unknown recorded timestamp is GNU's explicit "do not verify"
    // state (used by `set-visited-file-name', among others).
    let unchanged = if interp
        .buffer_remote_prefix(interp.current_buffer_id())
        .is_some()
    {
        modtimes_equal_whole_seconds(&visited_modtime, &Some(current_modtime))
    } else {
        visited_modtime == Some(current_modtime)
    };
    if visited_modtime.is_none() || unchanged {
        return Ok(());
    }

    // filelock.c owns the stale-file decision above, then delegates every
    // user/content/revert policy choice to userlock.el.
    call_named_function(
        interp,
        "userlock--ask-user-about-supersession-threat",
        &[Value::String(logical_path.into())],
        env,
    )?;
    Ok(())
}
