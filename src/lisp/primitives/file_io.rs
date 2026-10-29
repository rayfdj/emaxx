use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;
use crate::lisp::types::PropertyName;

pub(crate) fn format_source_props(
    value: &Value,
    from: usize,
    to: usize,
) -> Option<Vec<(PropertyName, Value)>> {
    // The format string's intervals, read in place: `format' asks once per
    // conversion, and its text is never needed here.
    let Kind::StringObject(state) = value.kind() else {
        return None;
    };
    let state = state.borrow();
    let mut props: Vec<(PropertyName, Value)> = Vec::new();
    for span in state.props.iter() {
        if span.start < to && from < span.end {
            for (name, value) in &span.props {
                if !props.iter().any(|(existing, _)| existing == name) {
                    props.push((*name, *value));
                }
            }
        }
    }
    // The format string's own intervals reach the result through
    // add_text_properties_from_list too: reversed pairs.
    props.reverse();
    if props.is_empty() { None } else { Some(props) }
}

pub(crate) fn props_at_string_offset(
    spans: &[TextPropertySpan],
    pos: usize,
) -> Vec<(PropertyName, Value)> {
    spans
        .iter()
        .find(|span| span.start <= pos && pos < span.end)
        .map(|span| span.props.clone())
        .unwrap_or_default()
}

pub(crate) fn file_modtime(
    interp: &Interpreter,
    env: &Env,
    path: &str,
) -> Result<Option<crate::buffer::FileModTime>, LispError> {
    let local_path = unquote_local_file_name(path).unwrap_or_else(|| path.to_string());
    match fs::metadata(host_file_name(interp, env, &local_path)?) {
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
    // timefns.c lisp_time_argument / lisp_to_timespec: floor-divide
    // TICKS * 10^9 by HZ into whole nanoseconds, then split into the
    // seconds and the nanoseconds of the timespec -- the whole timestamp,
    // not its seconds (`set-file-times' with `current-time' set a file's
    // mtime a fraction of a second early, and multisession's file backend
    // then took its cached value over another Emacs's newer one).
    let _ = interp;
    let (seconds, subsecond) = lisp_time_argument(value)?;
    let modified = if seconds >= 0 {
        UNIX_EPOCH
            .checked_add(Duration::new(seconds as u64, subsecond))
            .ok_or_else(time_overflow)?
    } else {
        // Seconds before the epoch with a positive nanosecond part.
        UNIX_EPOCH
            .checked_sub(Duration::from_secs(seconds.unsigned_abs()))
            .and_then(|time| time.checked_add(Duration::from_nanos(u64::from(subsecond))))
            .ok_or_else(time_overflow)?
    };
    Ok(crate::buffer::FileModTime { modified })
}

/// The timespec fields of TIME: whole seconds (floored) and the
/// nanoseconds within them, as utimensat takes them.
#[cfg(unix)]
fn system_time_to_timespec(time: SystemTime) -> libc::timespec {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => libc::timespec {
            tv_sec: duration.as_secs() as libc::time_t,
            tv_nsec: duration.subsec_nanos() as libc::c_long,
        },
        Err(before) => {
            let duration = before.duration();
            let (mut seconds, mut nanoseconds) = (
                -(duration.as_secs() as i64),
                -(i64::from(duration.subsec_nanos())),
            );
            if nanoseconds < 0 {
                seconds -= 1;
                nanoseconds += 1_000_000_000;
            }
            libc::timespec {
                tv_sec: seconds as libc::time_t,
                tv_nsec: nanoseconds as libc::c_long,
            }
        }
    }
}

pub(crate) fn set_file_times_path(
    path: &str,
    host: &Path,
    modified: SystemTime,
    nofollow: bool,
) -> Result<(), LispError> {
    #[cfg(unix)]
    {
        let c_path = CString::new(host.as_os_str().as_bytes())
            .map_err(|_| LispError::Signal("File name contains nul byte".into()))?;
        let timestamp = system_time_to_timespec(modified);
        // fileio.c Fset_file_times: utimensat with both atime and mtime at
        // TIMESTAMP, to the nanosecond, AT_SYMLINK_NOFOLLOW for `nofollow'.
        let times = [timestamp, timestamp];
        let flags = if nofollow {
            libc::AT_SYMLINK_NOFOLLOW
        } else {
            0
        };
        // SAFETY: c_path is a valid nul-terminated path, and times points to
        // two initialized timespec values for the duration of this call.
        let result =
            unsafe { libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), flags) };
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
        fs::metadata(host)
            .map(|_| ())
            .map_err(|error| file_operation_error("Setting file times", &error, path))
    }
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
    Value::list([
        Value::Symbol(file_error_condition(error).into()),
        Value::String(message.into()),
        Value::String(errno_text(error).into()),
        Value::String(path.into()),
    ])
}

/// fileio.c report_file_errno: EEXIST, ENOENT/ENOTDIR and EACCES have their
/// own conditions; every other errno is a plain `file-error'.
pub(crate) fn file_error_condition(error: &std::io::Error) -> &'static str {
    match error.kind() {
        ErrorKind::NotFound => "file-missing",
        ErrorKind::AlreadyExists => "file-already-exists",
        _ if error.raw_os_error() == Some(libc::EACCES) => "permission-denied",
        _ => "file-error",
    }
}

/// The `strerror' text of an `io::Error', without Rust's "(os error N)".
pub(crate) fn errno_text(error: &std::io::Error) -> String {
    let rendered = error.to_string();
    rendered
        .split_once(" (os error")
        .map_or(rendered.as_str(), |(detail, _)| detail)
        .to_string()
}

/// `report_file_errno' with an explicit errno, for callers such as openp
/// that carry the number rather than an `io::Error'.
pub(crate) fn file_errno_error_value(message: &str, errno: i32, path: &str) -> Value {
    file_operation_error_value(message, &std::io::Error::from_raw_os_error(errno), path)
}

/// `report_file_errno (MESSAGE, Qnil, errno)': the data names no file, so it
/// is just the message and the errno text.
pub(crate) fn file_operation_error_value_without_file(
    message: &str,
    error: &std::io::Error,
) -> Value {
    Value::list([
        Value::Symbol(file_error_condition(error).into()),
        Value::String(message.into()),
        Value::String(errno_text(error).into()),
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

pub(crate) fn is_circular_list_value(value: &Value) -> bool {
    matches!(
        value.to_vec().as_ref().map_err(LispError::kind),
        Err(LispErrorKind::SignalValue(signal)) if circular_list_signal_p(signal)
    )
}

pub(crate) fn make_temp_file_internal(
    interp: &Interpreter,
    env: &Env,
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
        let candidate = host_file_name(interp, env, &path)?;
        last = path;
        if matches!(dir_flag.kind(), Kind::Integer(0)) {
            if candidate.exists() {
                continue;
            }
            return Ok(last);
        }
        if dir_flag.is_nil() {
            // gen_tempname opens with S_IRUSR | S_IWUSR (the umask still
            // applies), and makes a directory with 0700.
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&candidate) {
                Ok(mut file) => {
                    if let Some(text) = text.and_then(string_like) {
                        file.write_all(text.text.as_bytes()).map_err(|error| {
                            LispError::SignalValue(file_operation_error_value(
                                "Creating file with prefix",
                                &error,
                                prefix,
                            ))
                        })?;
                    }
                    return Ok(last);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                // Fmake_temp_file_internal: report_file_error with the
                // kind's message and PREFIX as the file.
                Err(error) => {
                    return Err(LispError::SignalValue(file_operation_error_value(
                        "Creating file with prefix",
                        &error,
                        prefix,
                    )));
                }
            }
        }
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&candidate) {
            Ok(()) => return Ok(last),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(LispError::SignalValue(file_operation_error_value(
                    "Creating directory with prefix",
                    &error,
                    prefix,
                )));
            }
        }
    }
    Ok(last)
}

pub(crate) fn read_insert_file_bytes(
    path: &str,
    host: &Path,
    start: Option<usize>,
    end: Option<usize>,
) -> Result<Vec<u8>, LispError> {
    let input_error =
        |error: std::io::Error| LispError::SignalValue(file_input_error_value(&error, path));
    let metadata = fs::metadata(host).map_err(input_error)?;
    if metadata.is_dir() {
        return Err(LispError::SignalValue(file_error_with_detail_value(
            "Read error",
            "Is a directory",
            path,
        )));
    }
    if metadata.file_type().is_file() {
        let mut bytes = fs::read(host).map_err(input_error)?;
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
    let mut file = fs::File::open(host).map_err(input_error)?;
    let mut buffer = vec![0; limit];
    let read = file.read(&mut buffer).map_err(input_error)?;
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

// coding.c:coding_inherit_eol_type preserves an already specified alias;
// otherwise it selects the actual subsidiary, with Unix as the host default.
pub(crate) fn inherit_write_coding_eol(
    interp: &Interpreter,
    coding: Value,
    parent: Value,
) -> Result<Value, LispError> {
    let coding = if coding.is_nil() {
        Value::symbol("raw-text")
    } else {
        checked_coding_symbol(interp, &coding)?;
        coding
    };
    let name = coding.as_symbol()?;
    if interp.coding_system_eol_type_value(name).is_some() {
        return Ok(coding);
    }
    let eol = if parent.is_nil() {
        0
    } else {
        let parent = checked_coding_symbol(interp, &parent)?;
        interp.coding_system_eol_type_value(&parent).unwrap_or(0)
    };
    let base = interp
        .coding_system_base_name(name)
        .unwrap_or_else(|| name.to_string());
    Ok(Value::symbol(&coding_variant_name(
        interp,
        &base,
        Some(eol),
    )))
}

/// fileio.c:choose_write_coding_system leaves file rules and safe-coding
/// policy in their GNU Lisp owners, then completes the external EOL choice.
fn choose_write_coding(
    interp: &mut Interpreter,
    args: &[Value; 7],
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut coding = interp
        .lookup_var("coding-system-for-write", env)
        .unwrap_or(Value::Nil);
    if !coding.is_nil() {
        let selector = interp
            .lookup_var("select-safe-coding-system-function", env)
            .unwrap_or(Value::Nil);
        if interp
            .lookup_var("coding-system-require-warning", env)
            .is_some_and(|value| value.is_truthy())
            && call(interp, "fboundp", &[selector], env)?.is_truthy()
        {
            coding = interp.call_function_value(
                selector,
                None,
                &[
                    args[0],
                    args[1],
                    Value::list([Value::T, coding]),
                    Value::Nil,
                    args[2],
                ],
                env,
            )?;
        }
        return inherit_write_coding_eol(interp, coding, Value::Nil);
    }

    coding = interp
        .lookup_var("buffer-file-coding-system", env)
        .unwrap_or(Value::Nil);
    let local =
        interp.has_buffer_local_binding(interp.current_buffer_id(), "buffer-file-coding-system");
    let mut force_raw = false;
    if coding.is_nil() || !local {
        coding = Value::Nil;
        force_raw = !interp.buffer.borrow().is_multibyte();
    }
    if coding.is_nil() {
        let selected = interp.call_function_value(
            Value::symbol("find-operation-coding-system"),
            Some("find-operation-coding-system"),
            &[
                Value::symbol("write-region"),
                args[0],
                args[1],
                args[2],
                args[3],
                args[4],
                args[5],
            ],
            env,
        )?;
        if let Some((_, selected)) = selected.cons_values()
            && !selected.is_nil()
        {
            coding = selected;
        }
    }
    let using_default = coding.is_nil();
    if using_default {
        coding = interp
            .lookup_var("buffer-file-coding-system", env)
            .unwrap_or(Value::Nil);
    }
    if !coding.is_nil() && !force_raw {
        let name = checked_coding_symbol(interp, &coding)?;
        force_raw = matches!(
            interp.coding_system_kind_name(&name).as_deref(),
            Some("raw-text" | "no-conversion")
        );
    }
    let selector = interp
        .lookup_var("select-safe-coding-system-function", env)
        .unwrap_or(Value::Nil);
    if !force_raw && call(interp, "fboundp", &[selector], env)?.is_truthy() {
        coding = interp.call_function_value(
            selector,
            None,
            &[args[0], args[1], coding, Value::Nil, args[2]],
            env,
        )?;
        if !coding.is_nil() {
            checked_coding_symbol(interp, &coding)?;
        }
    }
    if !using_default {
        let default = interp
            .default_value("buffer-file-coding-system")
            .unwrap_or(Value::Nil);
        if !default.is_nil() {
            coding = inherit_write_coding_eol(interp, coding, default)?;
        }
    }
    if force_raw {
        let raw = coding.symbol_str().is_some_and(|name| {
            matches!(
                interp.coding_system_kind_name(name).as_deref(),
                Some("raw-text" | "no-conversion")
            )
        });
        if !raw {
            let eol = if coding.is_nil() {
                None
            } else {
                let name = checked_coding_symbol(interp, &coding)?;
                interp.coding_system_eol_type_value(&name)
            };
            coding = Value::symbol(&coding_variant_name(interp, "raw-text", eol));
        }
    }
    inherit_write_coding_eol(interp, coding, Value::Nil)
}

pub(crate) fn write_region_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("write-region", args, 3, 7)?;
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
    need_arg_range("write-region", args, 3, 7)?;
    let mut normalized = [Value::Nil; 7];
    normalized[..args.len()].copy_from_slice(args);
    if !args[0].is_nil() && !matches!(args[0].kind(), Kind::StringObject(_)) {
        let start = position_from_value(interp, &args[0])?;
        let end = position_from_value(interp, &args[1])?;
        if start.min(end) < interp.buffer.borrow().point_min()
            || start.max(end) > interp.buffer.borrow().point_max()
        {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                Value::Buffer(interp.buffer),
                args[0],
                args[1],
            ])));
        }
        normalized[0] = Value::Integer(start.min(end) as i64);
        normalized[1] = Value::Integer(start.max(end) as i64);
    }
    let requested_path = string_text(&args[2])?;
    let path = resolve_file_name_in_env(interp, env, &requested_path);
    let existed_before_write = fs::symlink_metadata(host_file_name(interp, env, &path)?).is_ok();
    if normalized[6].is_truthy()
        && normalized[6].symbol_str() != Some("excl")
        && existed_before_write
        && !call_named_function(
            interp,
            "y-or-n-p",
            &[Value::String(
                format!("File {logical_path} already exists; overwrite anyway? ").into(),
            )],
            env,
        )?
        .is_truthy()
    {
        return Err(file_operation_error(
            "Opening output file",
            &std::io::Error::from(ErrorKind::AlreadyExists),
            logical_path,
        ));
    }
    // write_region saves restriction state even for explicit/string input;
    // nil START temporarily exposes the entire buffer to coding callbacks.
    let saved = interp.save_restriction_state();
    interp.restore_labeled_restrictions(interp.current_buffer_id(), Vec::new());
    if args[0].is_nil() {
        interp.buffer.borrow_mut().widen();
    }
    let result = write_region_with_coding(
        interp,
        &normalized,
        &path,
        existed_before_write,
        logical_path,
        logical_lock_path,
        env,
    );
    interp.restore_restriction_state(saved);
    result
}

/// callproc.c create_temp_file: the input `call-process-region' gives
/// its child, START..END (START a string, or nil for the whole buffer)
/// as write_region writes them with `coding-system-for-write' bound to
/// CODING and no file name handlers -- the coding recorded in
/// `last-coding-system-used'.
pub(crate) fn encode_process_input(
    interp: &mut Interpreter,
    start: Value,
    end: Value,
    coding: Value,
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    let restore_coding = interp.bind_special_dynamic("coding-system-for-write", coding, env)?;
    let result = match interp.bind_special_dynamic("file-name-handler-alist", Value::Nil, env) {
        Ok(restore_handlers) => {
            let saved = interp.save_restriction_state();
            interp.restore_labeled_restrictions(interp.current_buffer_id(), Vec::new());
            if start.is_nil() {
                interp.buffer.borrow_mut().widen();
            }
            let result = encode_region_for_write(interp, start, end, env);
            interp.restore_restriction_state(saved);
            let restored = interp.restore_special_dynamic(restore_handlers, env);
            result.and_then(|bytes| restored.map(|()| bytes))
        }
        Err(error) => Err(error),
    };
    let restored = interp.restore_special_dynamic(restore_coding, env);
    result.and_then(|bytes| restored.map(|()| bytes))
}

/// write_region's coding choice and e_write's encoding of START..END,
/// without the file.
fn encode_region_for_write(
    interp: &mut Interpreter,
    start: Value,
    end: Value,
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    let mut args = [
        start,
        end,
        Value::string(""),
        Value::Nil,
        Value::symbol("lambda"),
        Value::Nil,
        Value::Nil,
    ];
    if start.is_nil() {
        args[0] = Value::Integer(interp.buffer.borrow().point_min() as i64);
        args[1] = Value::Integer(interp.buffer.borrow().point_max() as i64);
    }
    let coding = choose_write_coding(interp, &args, env)?;
    let coding_name = coding.as_symbol()?;
    set_last_coding_system_used(interp, coding_name, env);
    let selective_display = !args[0].is_string()
        && interp
            .lookup_var("selective-display", env)
            .is_some_and(|value| value.eq_value(Value::T));
    let source = if let Some(string) = string_like(&args[0]) {
        string
    } else {
        let start = position_from_value(interp, &args[0])?;
        let end = position_from_value(interp, &args[1])?;
        let (start, end) = (start.min(end), start.max(end));
        let buffer = interp.buffer.borrow();
        StringLike {
            text: buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?,
            props: Vec::new(),
            multibyte: buffer.is_multibyte(),
            extended_chars: buffer.substring_extended_chars(start, end),
        }
    };
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    let translation = encode_translation_tables(interp, coding_name, env)?;
    encode_text_bytes(
        interp,
        &source,
        coding_name,
        inhibit_eol_conversion,
        selective_display,
        &translation,
    )
}

fn write_region_with_coding(
    interp: &mut Interpreter,
    args: &[Value; 7],
    path: &str,
    existed_before_write: bool,
    logical_path: &str,
    logical_lock_path: Option<&str>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut selected_args = *args;
    selected_args[2] = Value::string(path);
    if args[0].is_nil() {
        selected_args[0] = Value::Integer(interp.buffer.borrow().point_min() as i64);
        selected_args[1] = Value::Integer(interp.buffer.borrow().point_max() as i64);
    }
    if selected_args[5].is_nil() {
        selected_args[5] = if string_like(&selected_args[4]).is_some() {
            selected_args[4]
        } else {
            selected_args[2]
        };
    }
    let coding = choose_write_coding(interp, &selected_args, env)?;
    let coding_name = coding.as_symbol()?;
    set_last_coding_system_used(interp, coding_name, env);
    // Selection calls Lisp, which may mutate the supplied string or buffer.
    // Read its actual current contents after the callback has returned.
    // choose_write_coding: CODING_MODE_SELECTIVE_DISPLAY for a buffer
    // region whose `selective-display' is t.
    let selective_display = !selected_args[0].is_string()
        && interp
            .lookup_var("selective-display", env)
            .is_some_and(|value| value.eq_value(Value::T));
    let source = if let Some(string) = string_like(&selected_args[0]) {
        string
    } else {
        let start = position_from_value(interp, &selected_args[0])?;
        let end = position_from_value(interp, &selected_args[1])?;
        let buffer = interp.buffer.borrow();
        StringLike {
            text: buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?,
            props: Vec::new(),
            multibyte: buffer.is_multibyte(),
            extended_chars: buffer.substring_extended_chars(start, end),
        }
    };
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    let translation = encode_translation_tables(interp, coding_name, env)?;
    let bytes = encode_text_bytes(
        interp,
        &source,
        coding_name,
        inhibit_eol_conversion,
        selective_display,
        &translation,
    )?;
    let lock_path = logical_lock_path
        .map(str::to_string)
        .or_else(|| args.get(5).and_then(string_like).map(|string| string.text))
        .or_else(|| args.get(4).and_then(string_like).map(|string| string.text))
        .unwrap_or_else(|| logical_path.to_string());
    // fileio.c:write_region: Flock_file before the file is opened (its
    // lock_file asks about a supersession threat even when
    // `create-lockfiles' makes no lock), Funlock_file once it is written.
    flock_file(interp, env, Value::String(lock_path.clone().into()))?;
    let host = host_file_name(interp, env, path)?;
    let write_result = (|| {
        let exclusive = args[6].symbol_str() == Some("excl");
        if args[3].is_nil() && !exclusive {
            return fs::write(&host, &bytes).map_err(|error| file_output_error(path, &error));
        }
        let offset = args
            .get(3)
            .filter(|value| value.is_truthy())
            .and_then(|value| value.integer_value());
        let append = args[3].is_truthy() && offset.is_none();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .create_new(exclusive)
            .truncate(args[3].is_nil())
            .append(append)
            .write(true)
            .open(&host)
            .map_err(|error| {
                if exclusive && error.kind() == ErrorKind::AlreadyExists {
                    file_operation_error("Opening output file", &error, logical_path)
                } else {
                    file_output_error(path, &error)
                }
            })?;
        if let Some(offset) = offset {
            file.seek(SeekFrom::Start(offset.max(0) as u64))
                .map_err(|error| file_output_error(path, &error))?;
        }
        file.write_all(&bytes)
            .map_err(|error| file_output_error(path, &error))
    })();
    let unlock_result = funlock_file(interp, env, Value::String(lock_path.into()));
    match (write_result, unlock_result) {
        (Err(error), _) | (Ok(()), Err(error)) => return Err(error),
        (Ok(()), Ok(())) => {}
    }
    if !existed_before_write {
        dispatch_file_notification(interp, env, path, "created")?;
    }
    // Creating an empty file changes the directory but writes no file data;
    // kqueue therefore exposes CREATE without WRITE for that transition.
    if existed_before_write || !bytes.is_empty() {
        dispatch_file_notification(interp, env, path, "changed")?;
    }
    // Darwin kqueue reports the metadata transition caused by replacing the
    // file contents separately from the write readiness flag.  Watches that
    // did not request attribute changes filter this event at the backend.
    #[cfg(target_os = "macos")]
    if existed_before_write || !bytes.is_empty() {
        dispatch_file_notification(interp, env, path, "attribute-changed")?;
    }
    if let Some(visit) = args.get(4)
        && (matches!(visit.kind(), Kind::T) || string_like(visit).is_some())
    {
        let visited_name = if matches!(visit.kind(), Kind::T) {
            logical_path.to_string()
        } else {
            string_text(visit)?
        };
        let visited_name = expand_file_name_runtime(interp, env, &visited_name, None)?;
        interp.buffer.borrow_mut().file = Some(visited_name);
        interp
            .buffer
            .borrow_mut()
            .set_visited_file_modtime(file_modtime(interp, env, path)?);
        interp.buffer.borrow_mut().set_unmodified();
    }
    if interp
        .lookup_var("noninteractive", env)
        .is_none_or(|value| value.is_nil())
        && args.get(4).is_none_or(|visit| {
            visit.is_nil() || matches!(visit.kind(), Kind::T) || string_like(visit).is_some()
        })
    {
        // fileio.c write_region: the expanded VISIT name, else the
        // expanded FILENAME, through message_with_string.
        let visit_file = match args.get(4) {
            Some(visit) if string_like(visit).is_some() => string_text(visit)?,
            _ => logical_path.to_string(),
        };
        let visit_file = expand_file_name_runtime(interp, env, &visit_file, None)?;
        let format = match args.get(3).map(|append| append.kind()) {
            Some(Kind::Integer(_) | Kind::Float(_)) => "Updated %s",
            Some(_) if args[3].is_truthy() => "Added to %s",
            _ => "Wrote %s",
        };
        super::dispatch::display::message_with_string(
            interp,
            format,
            Value::string(&visit_file),
            env,
        )?;
    }
    Ok(Value::Nil)
}

/// print.c printchar_to_stream onto stderr: CH, or the characters among
/// the elements of the vector `standard-display-table' shows it as; an
/// ASCII character as its byte, any other as its multibyte form, encoded
/// by `coding-system-for-write', else `locale-coding-system', when either
/// is set (code_convert_string_norecord).
pub(crate) fn printchar_to_stderr(
    interp: &mut Interpreter,
    ch: u32,
    env: &mut Env,
) -> Result<(), LispError> {
    let mut chars = vec![ch];
    if ch <= MAX_CHAR
        && let Some(Kind::CharTable(table)) = interp
            .symbol_value_key(cached_symbol!("standard-display-table"))
            .map(|value| value.kind())
        && table.has_purpose("display-table")
        && let Some(entry) = interp.char_table_get(table, ch)
        && crate::lisp::primitives::interactive::is_vector_value(&entry)
    {
        chars = crate::lisp::primitives::buffers::vector_items(&entry)?
            .iter()
            .filter_map(|element| match element.kind() {
                Kind::Integer(c) if (0..=i64::from(MAX_CHAR)).contains(&c) => Some(c as u32),
                _ => None,
            })
            .collect();
    }
    let coding = interp
        .forwarded_c_value("coding-system-for-write", env)
        .filter(|value| !value.is_nil())
        .or_else(|| interp.forwarded_c_value("locale-coding-system", env))
        .filter(|value| !value.is_nil());
    let mut bytes = Vec::new();
    for c in chars {
        if c < 0x80 {
            bytes.push(c as u8);
            continue;
        }
        // CHAR_STRING's multibyte form.
        let mut one = PrintOutput::default();
        one.push_code(c);
        let Some(coding) = &coding else {
            bytes.extend_from_slice(one.bytes());
            continue;
        };
        let string = one.into_value(interp, env)?;
        let encoded = encode_coding_value_recording(
            interp,
            &string,
            Some(coding.as_symbol()?),
            true,
            false,
            env,
        )?;
        bytes.extend(crate::lisp::primitives::text::internal_string_bytes(
            &string_like(&encoded).expect("an encoded string"),
        )?);
    }
    std::io::stderr()
        .lock()
        .write_all(&bytes)
        .map_err(|error| LispError::Signal(error.to_string()))
}

/// print.c Fredirect_debugging_output's stderr_dup: STDERR_FILENO until
/// the first redirection duplicates the original stderr (close-on-exec).
#[cfg(unix)]
static STDERR_DUP: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(libc::STDERR_FILENO);

/// print.c Fredirect_debugging_output: point the stderr descriptor at FILE
/// (expanded), truncated unless APPEND, or back at the original stderr
/// for nil.
#[cfg(unix)]
pub(crate) fn redirect_debugging_output(
    interp: &mut Interpreter,
    file: &Value,
    append: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    use std::sync::atomic::Ordering;
    // fileio.c report_file_error: the errno's condition and text, then
    // the file when there is one.
    let report = |operation: &str, file: Value| {
        let error = std::io::Error::last_os_error();
        LispError::SignalValue(match string_like(&file) {
            Some(name) => file_operation_error_value(operation, &error, &name.text),
            None => file_operation_error_value_without_file(operation, &error),
        })
    };
    let mut fd = STDERR_DUP.load(Ordering::Relaxed);
    let mut file = *file;
    if !file.is_nil() {
        let name = string_like(&file)
            .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), file))?
            .text;
        let expanded = super::system::expand_file_name_runtime(interp, env, &name, None)?;
        file = Value::string(&expanded);
        if fd == libc::STDERR_FILENO {
            // SAFETY: fcntl on the process's own stderr descriptor.
            let n = unsafe {
                libc::fcntl(
                    libc::STDERR_FILENO,
                    libc::F_DUPFD_CLOEXEC,
                    libc::STDERR_FILENO + 1,
                )
            };
            if n < 0 {
                return Err(report("dup", file));
            }
            STDERR_DUP.store(n, Ordering::Relaxed);
        }
        let path = std::ffi::CString::new(expanded.as_bytes())
            .map_err(|_| LispError::WrongTypeArgument("filenamep".into(), file))?;
        let flags = libc::O_WRONLY
            | libc::O_CREAT
            | if append {
                libc::O_APPEND
            } else {
                libc::O_TRUNC
            };
        // SAFETY: PATH is a NUL-terminated name.
        fd = unsafe { libc::open(path.as_ptr(), flags | libc::O_CLOEXEC, 0o666) };
        if fd < 0 {
            return Err(report("Cannot open debugging output stream", file));
        }
    }
    let _ = std::io::stderr().lock().flush();
    // SAFETY: FD is open; dup2 replaces the stderr descriptor.
    if unsafe { libc::dup2(fd, libc::STDERR_FILENO) } < 0 {
        return Err(report("dup2", file));
    }
    if fd != STDERR_DUP.load(Ordering::Relaxed) {
        // SAFETY: FD was opened above and is no longer needed.
        unsafe { libc::close(fd) };
    }
    Ok(())
}

pub(crate) fn print_object_to_stream(
    interp: &mut Interpreter,
    value: &Value,
    stream: Option<Value>,
    env: &mut Env,
    escape: bool,
    newlines: bool,
) -> Result<Value, LispError> {
    with_printer_destination(interp, stream.as_ref(), env, |interp, env| {
        let multibyte = match stream.map(|value| value.kind()) {
            None | Some(Kind::Nil | Kind::Buffer(_) | Kind::Marker(_)) => {
                Some(interp.buffer.borrow().is_multibyte())
            }
            _ => None,
        };
        let output_is_function =
            multibyte.is_none() && stream.is_some_and(|value| !matches!(value.kind(), Kind::T));
        with_printer_buffer_escape(interp, env, multibyte, |interp, env| {
            let rendered = render_printer_output(interp, value, env, escape, output_is_function)?;
            let rendered = if newlines {
                rendered.enclosed("\n", "\n")
            } else {
                rendered
            };
            write_prepared_printer_output(interp, &rendered, stream.as_ref(), env)?;
            if (newlines || native_print_updates_batch_last_char(interp, value, env, escape))
                && let Some(last) = rendered.codes().last()
            {
                record_batch_standard_output_char(interp, stream.as_ref(), env, last);
            }
            Ok(*value)
        })
    })
}

/// print.c:print_prepare selects the output buffer before rendering, and
/// print_finish moves a marker and restores point after successful insertion.
/// The original current buffer is restored on either normal or error return.
fn with_printer_destination<T>(
    interp: &mut Interpreter,
    stream: Option<&Value>,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let (buffer_id, marker_position) = match stream.map(|value| value.kind()) {
        Some(Kind::Buffer(buffer)) => (buffer.id, None),
        Some(Kind::Marker(marker)) => {
            let buffer = marker
                .buffer()
                .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
            let position = marker
                .position()
                .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
            let state = buffer.borrow();
            if !(state.point_min()..=state.point_max()).contains(&position) {
                return Err(LispError::Signal(
                    "Marker is outside the accessible part of the buffer".into(),
                ));
            }
            (buffer.id, Some((marker, position, state.point())))
        }
        _ => return body(interp, env),
    };
    let saved_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(buffer_id)?;
    if let Some((_, position, _)) = marker_position {
        interp.buffer.borrow_mut().goto_char(position);
    }
    let result = body(interp, env).and_then(|value| {
        if let Some((marker, start, old_point)) = marker_position {
            let end = interp.buffer.borrow().point();
            interp.set_marker(marker, Some(end), Some(buffer_id))?;
            let restored_point = if old_point >= start {
                old_point.saturating_add(end).saturating_sub(start)
            } else {
                old_point
            };
            interp.buffer.borrow_mut().goto_char(restored_point);
        }
        Ok(value)
    });
    let restored = if interp.has_buffer_id(saved_buffer) {
        interp.set_current_buffer_id(saved_buffer)
    } else {
        Ok(())
    };
    match (result, restored) {
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

pub(crate) fn write_printer_output(
    interp: &mut Interpreter,
    text: &str,
    stream: Option<&Value>,
    env: &mut Env,
) -> Result<(), LispError> {
    with_printer_destination(interp, stream, env, |interp, env| {
        write_prepared_printer_output(interp, &PrintOutput::from(text), stream, env)
    })
}

/// Write printer OUTPUT, already in the internal encoding, to STREAM.
pub(crate) fn write_printer_output_codes(
    interp: &mut Interpreter,
    output: &PrintOutput,
    stream: Option<&Value>,
    env: &mut Env,
) -> Result<(), LispError> {
    with_printer_destination(interp, stream, env, |interp, env| {
        write_prepared_printer_output(interp, output, stream, env)
    })
}

fn write_prepared_printer_output(
    interp: &mut Interpreter,
    output: &PrintOutput,
    stream: Option<&Value>,
    env: &mut Env,
) -> Result<(), LispError> {
    match stream.map(|value| value.kind()) {
        Some(Kind::T) => {
            // Existing echo/message-capture APIs still accept host text.
            // Actual batch output consumes the canonical bytes directly.
            let text = output.host_text();
            interp.append_message_capture(&text, false, env);
            if interp
                .lookup_var("noninteractive", env)
                .is_some_and(|value| value.is_truthy())
            {
                crate::lisp::primitives::batch_stdout::write(output.bytes())
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                interp.batch_stdout_need_newline = true;
            } else {
                crate::lisp::primitives::echo_area_print_logged(interp, &text, env);
            }
            Ok(())
        }
        None | Some(Kind::Nil | Kind::Buffer(_) | Kind::Marker(_)) => {
            let (text, extended) = output.text_parts();
            insert_text_with_hooks(interp, &text, &[], &extended, false, false, env)
        }
        Some(_) => {
            let function = *stream.expect("matched Some");
            for code in output.codes() {
                call_function_value(interp, &function, &[Value::Integer(i64::from(code))], env)?;
            }
            Ok(())
        }
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
    ch: u32,
) {
    if stream.is_some_and(|value| matches!(value.kind(), Kind::T))
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
    match value.kind() {
        Kind::Integer(integer) => {
            interp
                .lookup_var("print-integers-as-characters", env)
                .is_some_and(|value| value.is_truthy())
                && render_princ_integer_as_character(&Value::Integer(integer)).is_some()
        }
        Kind::BigInteger(_) | Kind::Float(_) => false,
        Kind::StringObject(_) => escaped,
        Kind::Symbol(name) => !name.is_empty(),
        _ => true,
    }
}

pub(crate) fn printer_stream_value(
    interp: &Interpreter,
    env: &Env,
    explicit: Option<&Value>,
) -> Option<Value> {
    let resolved = match explicit.map(|v| v.kind()) {
        Some(Kind::Nil) => interp.lookup_var("standard-output", env),
        Some(value) => Some(value.value()),
        None => interp.lookup_var("standard-output", env),
    };
    match resolved.map(|v| v.kind()) {
        // GNU's `print_prepare' turns a nil effective stream into t after
        // resolving `standard-output'.
        Some(Kind::Nil) => Some(Value::T),
        other => other.map(|k| k.value()),
    }
}

/// print.c:print_bind_overrides around BODY: each of OVERRIDES' settings
/// `specbind's its print variable, and the bindings are undone when BODY
/// returns (`unbind_to'), so the printer and any Lisp it runs read them
/// as the variables' dynamic values.
pub(crate) fn with_print_overrides<T>(
    interp: &mut Interpreter,
    env: &mut Env,
    overrides: Option<&Value>,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let Some(overrides) = overrides.filter(|overrides| !overrides.is_nil()) else {
        return body(interp, env);
    };
    let bindings = print_override_bindings(overrides)?;
    let mut restores = Vec::with_capacity(bindings.len());
    let mut bound = Ok(());
    for (variable, value) in bindings {
        match interp.bind_special_dynamic(variable, value, env) {
            Ok(restore) => restores.push(restore),
            Err(error) => {
                bound = Err(error);
                break;
            }
        }
    }
    let result = bound.and_then(|()| body(interp, env));
    let mut restored = Ok(());
    for restore in restores.into_iter().rev() {
        let outcome = interp.restore_special_dynamic(restore, env);
        if restored.is_ok() {
            restored = outcome;
        }
    }
    restored.and(result)
}

/// print.c's `print--variable-mapping' (`print_create_variable_mapping'):
/// each OVERRIDES key, the variable it binds and the default a `t' setting
/// binds it to (`print-quoted' t, the others nil).  GNU spells the
/// `print-unreadable-function' key `unreadeable-function'.
const PRINT_VARIABLE_MAPPING: [(&str, &str, bool); 15] = [
    ("length", "print-length", false),
    ("level", "print-level", false),
    ("circle", "print-circle", false),
    ("quoted", "print-quoted", true),
    ("escape-newlines", "print-escape-newlines", false),
    (
        "escape-control-characters",
        "print-escape-control-characters",
        false,
    ),
    ("escape-nonascii", "print-escape-nonascii", false),
    ("escape-multibyte", "print-escape-multibyte", false),
    (
        "charset-text-property",
        "print-charset-text-property",
        false,
    ),
    ("unreadeable-function", "print-unreadable-function", false),
    ("gensym", "print-gensym", false),
    ("continuous-numbering", "print-continuous-numbering", false),
    ("number-table", "print-number-table", false),
    ("float-format", "float-output-format", false),
    (
        "integers-as-characters",
        "print-integers-as-characters",
        false,
    ),
];

/// print.c:print_bind_overrides' walk of OVERRIDES, as the variables it
/// binds and their values, in order: `t' (alone or as a setting) binds
/// every variable to its default (`print_bind_all_defaults'), a setting
/// (KEY . VALUE) binds KEY's variable to VALUE.  A non-list or a setting
/// that is no cons signals `(wrong-type-argument . consp)', an unknown key
/// `(wrong-type-argument symbolp nil)' (the failed lookup's nil).
fn print_override_bindings(overrides: &Value) -> Result<Vec<(&'static str, Value)>, LispError> {
    fn bind_all_defaults(bindings: &mut Vec<(&'static str, Value)>) {
        bindings.extend(
            PRINT_VARIABLE_MAPPING
                .iter()
                .map(|&(_, variable, default)| {
                    (variable, if default { Value::T } else { Value::Nil })
                }),
        );
    }
    let not_consp = || {
        LispError::SignalValue(Value::cons(
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol("consp".into()),
        ))
    };
    let mut bindings = Vec::new();
    if matches!(overrides.kind(), Kind::T) {
        bind_all_defaults(&mut bindings);
        return Ok(bindings);
    }
    let Some(mut cell) = overrides.cons_ref() else {
        return Err(not_consp());
    };
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("circular-list".into()),
                *overrides,
            ])));
        }
        let setting = cell.car.get();
        if matches!(setting.kind(), Kind::T) {
            bind_all_defaults(&mut bindings);
        } else {
            let Some((key, value)) = setting.cons_values() else {
                return Err(not_consp());
            };
            let variable = match key.kind() {
                Kind::Symbol(key) => PRINT_VARIABLE_MAPPING
                    .iter()
                    .find(|(name, _, _)| key == *name)
                    .map(|&(_, variable, _)| variable),
                _ => None,
            };
            let Some(variable) = variable else {
                return Err(LispError::WrongTypeArgument("symbolp".into(), Value::Nil));
            };
            bindings.push((variable, value));
        }
        let next = cell.cdr.get();
        if next.is_nil() {
            return Ok(bindings);
        }
        let Some(next) = next.cons_ref() else {
            return Err(not_consp());
        };
        cell = next;
    }
}

pub(crate) fn printer_stream_at_line_start(
    interp: &Interpreter,
    stream: Option<&Value>,
) -> Result<bool, LispError> {
    match stream.map(|v| v.kind()) {
        None | Some(Kind::Nil | Kind::T) => Ok(buffer_position_at_line_start(
            &interp.buffer.borrow(),
            interp.buffer.borrow().point(),
        )),
        Some(Kind::Buffer(_)) => {
            let buffer_id = interp.resolve_buffer_id(stream.expect("matched Some"))?;
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
            Ok(buffer_position_at_line_start(&buffer, buffer.point()))
        }
        Some(Kind::Marker(id)) => {
            let marker = id;
            let buffer_id = marker
                .buffer()
                .map(|buffer| buffer.id)
                .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
            let position = marker
                .position()
                .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
            Ok(buffer_position_at_line_start(&buffer, position))
        }
        Some(Kind::Symbol(_) | Kind::BuiltinFunc(_) | Kind::Closure(_)) => Ok(false),
        Some(other) => Err(LispError::TypeError(
            "output-stream".into(),
            other.value().type_name(),
        )),
    }
}

pub(crate) fn buffer_position_at_line_start(
    buffer: &crate::buffer::Buffer,
    position: usize,
) -> bool {
    position <= buffer.point_min() || buffer.char_at(position.saturating_sub(1)) == Some('\n')
}

/// fileio.c Finsert_file_contents's call of `set-auto-coding-function'
/// with the file name and how many bytes it may read from point.  In a
/// buffer holding text that is the first 1K of a regular file and up to
/// 3K more read after an lseek by the file's size less 3K (SEEK_CUR, so
/// the last 2K of a file larger than 4K), or all of a smaller one, in
/// the " *code-converting-work*" buffer, which stays.  In an empty buffer
/// it is every byte read, inserted in the buffer itself, made unibyte with
/// undo off for the call; decide_coding_unwind then takes the text out
/// again, as the bytes to decode (DECIDED_BYTES when the call changed it).
fn auto_coding_for_file(
    interp: &mut Interpreter,
    env: &Env,
    bytes: &[u8],
    filename: &str,
    decided_bytes: &mut Option<Vec<u8>>,
) -> Result<Option<String>, LispError> {
    let Some(function) = interp
        .lookup_var("set-auto-coding-function", env)
        .filter(|value| !value.is_nil())
    else {
        return Ok(None);
    };
    let coding = if interp.buffer.borrow().size_total() == 0 {
        if bytes.is_empty() {
            return Ok(None);
        }
        auto_coding_in_empty_buffer(interp, env, function, bytes, filename, decided_bytes)?
    } else {
        let host = host_file_name(interp, env, filename)?;
        let Some(head_and_tail) = auto_coding_file_sample(filename, &host)? else {
            return Ok(None);
        };
        auto_coding_in_work_buffer(interp, env, function, &head_and_tail, filename)?
    };
    checked_coding_given(interp, &coding)
}

/// The bytes Finsert_file_contents shows `set-auto-coding-function' for a
/// buffer holding text: none for a file that is not regular, all of one
/// of 4K or less, else its first 1K and what a read of 3K after
/// `lseek (fd, st_size - 3K, SEEK_CUR)' finds.
fn auto_coding_file_sample(path: &str, host: &Path) -> Result<Option<Vec<u8>>, LispError> {
    use std::io::{Seek, SeekFrom};
    let read_error = |error: std::io::Error| {
        LispError::SignalValue(file_operation_error_value("Read error", &error, path))
    };
    let Ok(metadata) = fs::metadata(host) else {
        return Ok(None);
    };
    if !metadata.is_file() {
        return Ok(None);
    }
    let mut file = fs::File::open(host).map_err(read_error)?;
    let size = metadata.len();
    let mut sample = Vec::with_capacity(4096);
    if size <= 4096 {
        (&mut file)
            .take(4096)
            .read_to_end(&mut sample)
            .map_err(read_error)?;
    } else {
        (&mut file)
            .take(1024)
            .read_to_end(&mut sample)
            .map_err(read_error)?;
        if sample.len() == 1024 {
            file.seek(SeekFrom::Current((size - 3072) as i64))
                .map_err(|error| file_operation_error("Setting file position", &error, path))?;
            file.take(3072)
                .read_to_end(&mut sample)
                .map_err(read_error)?;
        }
    }
    Ok((!sample.is_empty()).then_some(sample))
}

/// Finsert_file_contents's " *code-converting-work*" buffer, made (with
/// its hooks inhibited) the first time and kept: its overlays deleted,
/// the current buffer's directory, writable, visiting nothing, undo off,
/// erased, unibyte, SAMPLE inserted and point at its start, where
/// FUNCTION runs.
fn auto_coding_in_work_buffer(
    interp: &mut Interpreter,
    env: &Env,
    function: Value,
    sample: &[u8],
    filename: &str,
) -> Result<Value, LispError> {
    const WORK_BUFFER: &str = " *code-converting-work*";
    let saved_buffer_id = interp.current_buffer_id();
    let directory = interp
        .lookup_var("default-directory", env)
        .unwrap_or(Value::Nil);
    let work_id = match interp.find_buffer(WORK_BUFFER) {
        Some((id, _)) => id,
        None => {
            let (id, _) = interp.create_buffer(WORK_BUFFER);
            interp.set_buffer_hooks_inhibited(id, true);
            id
        }
    };
    interp.delete_buffer_overlays(work_id);
    interp.set_current_buffer_id(work_id)?;
    let mut work_env = env.clone();
    let result = (|| {
        interp.set_variable("default-directory", directory, &mut work_env);
        interp.set_variable("buffer-read-only", Value::Nil, &mut work_env);
        interp.set_variable("buffer-file-name", Value::Nil, &mut work_env);
        interp.set_variable("buffer-undo-list", Value::T, &mut work_env);
        call(interp, "erase-buffer", &[], &mut work_env)?;
        interp.buffer.borrow_mut().set_multibyte(false);
        interp.insert_current_buffer(&decode_raw_text_bytes(sample));
        {
            let mut buffer = interp.buffer.borrow_mut();
            let start = buffer.point_min();
            buffer.goto_char(start);
        }
        interp.call_function_value(
            function,
            None,
            &[
                Value::String(filename.to_string().into()),
                Value::Integer(sample.len() as i64),
            ],
            &mut work_env,
        )
    })();
    if interp.has_buffer_id(saved_buffer_id) {
        let _ = interp.set_current_buffer_id(saved_buffer_id);
    }
    result
}

/// Finsert_file_contents's decision in the empty buffer it reads into:
/// BYTES made the unibyte text, with undo off and point at the start, for
/// FUNCTION; then decide_coding_unwind, on any exit, empties the buffer
/// (markers, overlays and point at its start, the text's properties
/// gone) and restores its multibyteness and undo list.  The insertion
/// and the emptying count as no modification, as insert_from_gap_1 and
/// the gap's move are none.
fn auto_coding_in_empty_buffer(
    interp: &mut Interpreter,
    env: &Env,
    function: Value,
    bytes: &[u8],
    filename: &str,
    decided_bytes: &mut Option<Vec<u8>>,
) -> Result<Value, LispError> {
    let buffer_id = interp.current_buffer_id();
    let mut decide_env = env.clone();
    let undo_list = interp
        .lookup_var("buffer-undo-list", env)
        .unwrap_or(Value::Nil);
    let (multibyte, modiff, chars_modiff) = {
        let buffer = interp.buffer.borrow();
        (
            buffer.is_multibyte(),
            buffer.modified_tick(),
            buffer.chars_modified_tick(),
        )
    };
    interp.buffer.borrow_mut().set_multibyte(false);
    interp.set_variable("buffer-undo-list", Value::T, &mut decide_env);
    interp.insert_current_buffer(&decode_raw_text_bytes(bytes));
    let inserted_tick = {
        let mut buffer = interp.buffer.borrow_mut();
        buffer.goto_char(1);
        buffer.chars_modified_tick()
    };
    let result = interp.call_function_value(
        function,
        None,
        &[
            Value::String(filename.to_string().into()),
            Value::Integer(bytes.len() as i64),
        ],
        &mut decide_env,
    );
    // decide_coding_unwind.
    if interp.current_buffer_id() != buffer_id && interp.has_buffer_id(buffer_id) {
        interp.set_current_buffer_id(buffer_id)?;
    }
    if interp.current_buffer_id() == buffer_id {
        interp.buffer.borrow_mut().widen();
        let (changed, z) = {
            let buffer = interp.buffer.borrow();
            (
                buffer.chars_modified_tick() != inserted_tick,
                buffer.point_max(),
            )
        };
        if changed {
            let text = interp.buffer.borrow().buffer_string();
            *decided_bytes = Some(encode_raw_text_bytes(&text)?);
        }
        interp
            .delete_region_current_buffer(1, z)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let mut buffer = interp.buffer.borrow_mut();
        buffer.goto_char(1);
        buffer.set_multibyte(multibyte);
        buffer.set_modified_tick(modiff);
        buffer.set_chars_modified_tick(chars_modiff);
        drop(buffer);
        interp.set_variable("buffer-undo-list", undo_list, &mut decide_env);
    }
    result
}

/// What Finsert_file_contents's decoding leaves behind.
pub(crate) struct FileDecode {
    pub(crate) decoded: DecodedText,
    /// The coding system `last-coding-system-used' records: the one
    /// decided, as decode_coding_gap settled it.
    pub(crate) coding: String,
    /// The decided coding is for unibyte text, which makes a visited
    /// buffer unibyte.
    pub(crate) for_unibyte: bool,
    /// The `charset' properties the decoder annotated.
    pub(crate) charset_props: Vec<TextPropertySpan>,
}

/// fileio.c Finsert_file_contents's decoding of a file's BYTES into the
/// current buffer.  The coding system is `coding-system-for-read', else
/// what `set-auto-coding-function' finds in the bytes, else
/// `file-coding-system-alist' through find-operation-coding-system
/// (called with OPERATION_ARGS, the file name as given, VISIT, BEG, END
/// and REPLACE), else `undecided' (PATH is the expanded name); a unibyte buffer reads with the raw-text
/// coding of its eol type (raw_text_coding_system), and a visit with a
/// coding for unibyte text makes the buffer unibyte first (MAKE_UNIBYTE).
/// decode_coding_gap then decodes with it, so an empty file decides
/// nothing.
pub(crate) fn decode_file_contents(
    interp: &mut Interpreter,
    env: &Env,
    bytes: &[u8],
    literal: bool,
    path: &str,
    operation_args: &[Value],
    make_unibyte: bool,
) -> Result<FileDecode, LispError> {
    let request = interp
        .lookup_var("coding-system-for-read", env)
        .filter(|value| !value.is_nil());
    let mut coding = if literal {
        Some("no-conversion".to_string())
    } else if request
        .as_ref()
        .is_some_and(|value| value.symbol_str() == Some("auto-save-coding"))
    {
        Some("utf-8-emacs-unix".to_string())
    } else {
        match request {
            Some(value) => checked_coding_given(interp, &value)?,
            None => None,
        }
    };
    let buffer_was_empty = interp.buffer.borrow().size_total() == 0;
    let mut decided_bytes = None;
    if coding.is_none() {
        coding = auto_coding_for_file(interp, env, bytes, path, &mut decided_bytes)?;
    }
    let bytes = decided_bytes.as_deref().unwrap_or(bytes);
    if coding.is_none() {
        let mut operation_env = env.clone();
        let mut lookup_args = vec![Value::Symbol("insert-file-contents".into())];
        lookup_args.extend_from_slice(operation_args);
        // The decision made after reading into an empty buffer passes nil
        // for REPLACE.
        if buffer_was_empty && let Some(replace) = lookup_args.get_mut(5) {
            *replace = Value::Nil;
        }
        let pair = interp.call_function_value(
            Value::Symbol("find-operation-coding-system".into()),
            Some("find-operation-coding-system"),
            &lookup_args,
            &mut operation_env,
        )?;
        let decoding = pair
            .cons_values()
            .map(|(decoding, _)| decoding)
            .unwrap_or(pair);
        coding = checked_coding_given(interp, &decoding)?;
    }
    let mut coding = coding.unwrap_or_else(|| "undecided".into());
    let multibyte = interp.buffer.borrow().is_multibyte();
    if !multibyte {
        coding = raw_text_coding_system(interp, &coding);
    }
    let canonical = interp
        .coding_system_canonical_name(&coding)
        .unwrap_or_else(|| coding.clone());
    let for_unibyte = coding_system_property(interp, &canonical, ":for-unibyte")
        .is_some_and(|value| value.is_truthy());
    if bytes.is_empty() {
        return Ok(FileDecode {
            decoded: DecodedText::new(),
            coding,
            for_unibyte,
            charset_props: Vec::new(),
        });
    }
    let dst_multibyte = multibyte && !(make_unibyte && for_unibyte);
    let mut outcome = decode_coding_gap(interp, env, bytes, &coding, dst_multibyte)?;
    let charset_props = charset_runs_to_props(std::mem::take(&mut outcome.decoded.charsets));
    Ok(FileDecode {
        decoded: outcome.decoded,
        coding: outcome.coding,
        for_unibyte,
        charset_props,
    })
}

fn charset_runs_to_props(charsets: Vec<(usize, usize, String)>) -> Vec<TextPropertySpan> {
    charsets
        .into_iter()
        .map(|(start, end, charset)| TextPropertySpan {
            start,
            end,
            props: vec![(
                PropertyName::symbol("charset"),
                Value::Symbol(charset.into()),
            )],
        })
        .collect()
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
    let bytes = match host_file_name(interp, env, &path)
        .and_then(|host| read_insert_file_bytes(&path, &host, start, end))
    {
        Ok(bytes) => bytes,
        Err(error) => {
            // GNU completes the visiting part of `insert-file-contents'
            // before reporting that a visited file could not be opened.
            // `find-file-noselect' relies on this to create a correctly
            // named buffer for a file that does not exist yet.
            if visit {
                interp.buffer.borrow_mut().file = Some(path.clone());
                interp.buffer.borrow_mut().set_visited_file_modtime(None);
                interp.buffer.borrow_mut().set_unmodified();
            }
            return Err(error);
        }
    };
    let operation_args = [
        args[0],
        args.get(1).copied().unwrap_or(Value::Nil),
        args.get(2).copied().unwrap_or(Value::Nil),
        args.get(3).copied().unwrap_or(Value::Nil),
        args.get(4).copied().unwrap_or(Value::Nil),
    ];
    let FileDecode {
        decoded,
        coding: coding_used,
        for_unibyte,
        charset_props,
    } = decode_file_contents(
        interp,
        env,
        &bytes,
        literal,
        &path,
        &operation_args,
        visit && !replace,
    )?;
    // A visit with a coding for unibyte text makes the buffer unibyte
    // (not when part of it might be preserved).
    if visit && !replace && for_unibyte {
        interp.buffer.borrow_mut().set_multibyte(false);
    }
    let mut inserted_chars = decoded.char_count();
    let text = decoded.text;
    let extended = decoded.extended;
    // A REPLACE is one file-read transaction, not an ordinary user edit.
    // GNU dynamically hides `buffer-file-name' across its delete+insert so
    // the generic stale-file edit guard cannot interrupt the operation in
    // the middle and leave an empty buffer.
    let file_name_restore = if replace {
        Some(interp.bind_special_dynamic("buffer-file-name", Value::Nil, env)?)
    } else {
        None
    };
    let original_point = interp.buffer.borrow().point();
    let edit_result = (|| {
        if replace {
            // The texts compare by character code, those beyond Unicode
            // included.
            let (old_codes, begv) = {
                let buffer = interp.buffer.borrow();
                let (begv, zv) = (buffer.point_min(), buffer.point_max());
                let old = StringLike {
                    text: buffer.buffer_string(),
                    props: Vec::new(),
                    multibyte: true,
                    extended_chars: buffer.substring_extended_chars(begv, zv),
                };
                (old.character_codes_iter().collect::<Vec<_>>(), begv)
            };
            let new_chars = text.chars().collect::<Vec<_>>();
            let new_codes = StringLike {
                text: text.clone(),
                props: Vec::new(),
                multibyte: true,
                extended_chars: extended.clone(),
            }
            .character_codes_iter()
            .collect::<Vec<_>>();
            let prefix = old_codes
                .iter()
                .zip(&new_codes)
                .take_while(|(left, right)| left == right)
                .count();
            let suffix = old_codes[prefix..]
                .iter()
                .rev()
                .zip(new_codes[prefix..].iter().rev())
                .take_while(|(left, right)| left == right)
                .count();
            let old_chars = &old_codes;
            let old_end = old_chars.len() - suffix;
            let new_end = new_chars.len() - suffix;
            let start = begv + prefix;
            let end = begv + old_end;
            let replacement = new_chars[prefix..new_end].iter().collect::<String>();
            let replacement_extended = extended
                .iter()
                .filter(|(index, _)| (prefix..new_end).contains(index))
                .map(|&(index, code)| (index - prefix, code))
                .collect::<Vec<_>>();
            let replacement_len = replacement.chars().count();
            // fileio.c saves point as a marker across the delete+insert
            // (get_window_points_and_markers/restore_point_unwind), so it
            // first moves exactly as a marker would: point before the
            // replaced middle stays, point inside it collapses to the
            // start, point after it follows the unchanged suffix.
            let mut replacement_point = if original_point <= start {
                original_point
            } else if original_point <= end {
                start
            } else {
                original_point - (end - start) + replacement_len
            };
            // restore_window_points then overrides that for a point that
            // was strictly inside the replaced span, keeping it at the same
            // relative distance instead of at the span's start (bug#19161):
            //   newpos = same_at_start + growth * (oldpos - same_at_start)
            // with growth = inserted / (same_at_end - same_at_start),
            // computed in double and truncated.  GNU applies it only when
            // characters were actually inserted.
            if replacement_len > 0 && original_point > start && original_point <= end {
                let growth = replacement_len as f64 / (end - start) as f64;
                replacement_point =
                    (start as f64 + growth * (original_point - start) as f64) as usize;
            }
            inserted_chars = replacement.chars().count();
            if start != end {
                // GNU runs the modification hooks only for the portion that
                // actually differs (track-changes relies on these bounds).
                crate::lisp::primitives::delete_region_with_hooks(interp, start, end, env)?;
            }
            interp.buffer.borrow_mut().goto_char(start);
            if !replacement.is_empty() {
                crate::lisp::primitives::insert_text_with_hooks(
                    interp,
                    &replacement,
                    &[],
                    &replacement_extended,
                    false,
                    false,
                    env,
                )?;
            }
            {
                let point = replacement_point.min(interp.buffer.borrow().point_max());
                interp.buffer.borrow_mut().goto_char(point);
            }
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
        let insert_at = interp.buffer.borrow().point();
        // produce_charset's `charset' properties come in with the text
        // (the REPLACE path above inserts only the differing middle and
        // keeps none, a disclosed simplification).
        crate::lisp::primitives::insert_text_with_hooks(
            interp,
            &text,
            &charset_props,
            &extended,
            false,
            false,
            env,
        )?;
        interp.buffer.borrow_mut().goto_char(insert_at);
        // decode_coding_gap's post-read conversion: called with point at
        // the inserted text and its length, with undo off, the text it
        // adds or removes counting as inserted.
        let post_read = coding_system_property(
            interp,
            &interp
                .coding_system_canonical_name(&coding_used)
                .unwrap_or_else(|| coding_used.clone()),
            ":post-read-conversion",
        )
        .filter(|function| !function.is_nil());
        if let Some(function) = post_read
            && inserted_chars > 0
        {
            let prev_z = interp.buffer.borrow().size_total();
            let undo_list = interp
                .lookup_var("buffer-undo-list", env)
                .unwrap_or(Value::Nil);
            interp.set_variable("buffer-undo-list", Value::T, env);
            let result = interp.call_function_value(
                function,
                None,
                &[Value::Integer(inserted_chars as i64)],
                env,
            );
            interp.set_variable("buffer-undo-list", undo_list, env);
            let value = result?;
            if value.integer_value().is_none_or(|count| count < 0) {
                return Err(LispError::WrongTypeArgument("wholenump".into(), value));
            }
            let z = interp.buffer.borrow().size_total();
            inserted_chars = (inserted_chars as i64 + z as i64 - prev_z as i64).max(0) as usize;
        }
        Ok(())
    })();
    let restore_result = file_name_restore
        .map(|restore| interp.restore_special_dynamic(restore, env))
        .unwrap_or(Ok(()));
    edit_result?;
    restore_result?;
    if visit {
        interp.buffer.borrow_mut().file = Some(path.clone());
        interp.buffer.borrow_mut().file_truename = Some(canonical_file_name(interp, env, &path)?);
        interp
            .buffer
            .borrow_mut()
            .set_visited_file_modtime(file_modtime(interp, env, &path)?);
        interp.buffer.borrow_mut().set_unmodified();
        // GNU restores the pre-read undo list when visiting (fileio.c keeps
        // it aside around the insertion), so the very first interactive undo
        // must not remove the file's own contents.
        interp.buffer.borrow_mut().clear_undo();
    }
    // Finsert_file_contents records the coding decode_coding_gap settled
    // on; mule.el's after-insert-file-set-coding, run below, derives
    // `buffer-file-coding-system' from it.
    set_last_coding_system_used(interp, &coding_used, env);
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
            &[Value::Integer(inserted as i64), visit],
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
                let function = match hook.kind() {
                    Kind::Symbol(symbol) => interp.lookup_function(&symbol, env)?,
                    function => function.value(),
                };
                let original_name = hook.symbol_str();
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
