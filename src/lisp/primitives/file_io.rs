use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

/// emacs_fstatat of PATH as a visited file's `modtime' and
/// `modtime_size', or the error the call failed with.
pub(crate) fn stat_visited_modtime(
    interp: &Interpreter,
    env: &Env,
    path: &str,
) -> Result<Result<crate::buffer::VisitedModtime, std::io::Error>, LispError> {
    let host = host_file_name(interp, env, path)?;
    Ok(fs::metadata(host).and_then(|metadata| {
        Ok(crate::buffer::VisitedModtime {
            stamp: crate::buffer::ModtimeStamp::Time(metadata.modified()?),
            size: metadata.len() as i64,
        })
    }))
}

/// fileio.c:time_error_value: the stamp of a file whose status ERROR
/// kept from being read, NONEXISTENT_MODTIME_NSECS for ENOENT and
/// ENOTDIR.
pub(crate) fn time_error_value(error: &std::io::Error) -> crate::buffer::ModtimeStamp {
    if error.kind() == ErrorKind::NotFound || error.raw_os_error() == Some(libc::ENOTDIR) {
        crate::buffer::ModtimeStamp::Nonexistent
    } else {
        crate::buffer::ModtimeStamp::Unknown
    }
}

// GNU `file-attributes' time fields are (HIGH LOW USEC PSEC) lists.
/// timefns.c:make_lisp_time of TIME's timespec, one before the epoch
/// too (Tramp's `tramp-time-doesnt-exist' is (-1 65535)).
pub(crate) fn system_time_list_value(time: SystemTime) -> Result<Value, LispError> {
    let (seconds, nanoseconds) = system_time_seconds_nanos(time);
    Ok(unix_time_list_value(seconds, nanoseconds))
}

/// TIME as a timespec's floored seconds and the nanoseconds within them.
pub(crate) fn system_time_seconds_nanos(time: SystemTime) -> (i64, i64) {
    match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => (
            duration.as_secs() as i64,
            i64::from(duration.subsec_nanos()),
        ),
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
            (seconds, nanoseconds)
        }
    }
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
    let (seconds, nanoseconds) = system_time_seconds_nanos(time);
    libc::timespec {
        tv_sec: seconds as libc::time_t,
        tv_nsec: nanoseconds as libc::c_long,
    }
}

pub(crate) fn set_file_times_path(
    interp: &Interpreter,
    absname: Value,
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
            Err(report_file_errno(
                interp,
                "Setting file times",
                absname,
                &error,
            ))
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (modified, nofollow);
        fs::metadata(host)
            .map(|_| ())
            .map_err(|error| report_file_errno(interp, "Setting file times", absname, &error))
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

/// fileio.c barf_or_query_if_file_exists.  ABSNAME, unless KNOWN_TO_EXIST
/// already, is looked up without following a link, a directory being
/// `(file-error "File is a directory" ABSNAME)'.  An existing file is
/// `(file-already-exists "File already exists" ABSNAME)' unless
/// INTERACTIVE and the answer to "File ABSNAME already exists;
/// QUERYSTRING anyway? " is yes -- `y-or-n-p' when QUICK, else
/// `yes-or-no-p'.
pub(crate) fn barf_or_query_if_file_exists(
    interp: &mut Interpreter,
    env: &mut Env,
    absname: &str,
    mut known_to_exist: bool,
    querystring: &str,
    interactive: bool,
    quick: bool,
) -> Result<(), LispError> {
    if !known_to_exist
        && let Ok(metadata) = fs::symlink_metadata(host_file_name(interp, env, absname)?)
    {
        if metadata.is_dir() {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("file-error"),
                Value::string("File is a directory"),
                Value::string(absname),
            ])));
        }
        known_to_exist = true;
    }
    if !known_to_exist {
        return Ok(());
    }
    if interactive {
        let prompt = call(
            interp,
            "format",
            &[
                Value::string("File %s already exists; %s anyway? "),
                Value::string(absname),
                Value::string(querystring),
            ],
            env,
        )?;
        let ask = if quick { "y-or-n-p" } else { "yes-or-no-p" };
        if call_named_function(interp, ask, &[prompt], env)?.is_truthy() {
            return Ok(());
        }
    }
    Err(LispError::SignalValue(Value::list([
        Value::symbol("file-already-exists"),
        Value::string("File already exists"),
        Value::string(absname),
    ])))
}
/// fileio.c get_file_errno_data: EEXIST, ENOENT and EACCES have their own
/// conditions; every other errno (ENOTDIR included) is a plain
/// `file-error'.
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

/// `report_file_errno (MESSAGE, Qnil, errno)': the data names no file, so it
/// is just the message and the errno text.
pub(crate) fn file_operation_error_value_without_file(
    interp: &Interpreter,
    message: &str,
    error: &std::io::Error,
) -> LispError {
    report_file_errno_files(interp, message, &[], error)
}

/// fileio.c:report_file_errno with the Lisp file name FILE as given:
/// get_file_errno_data's `(CONDITION MESSAGE ERRSTRING FILE)', EEXIST's
/// `file-already-exists' without MESSAGE.
pub(crate) fn report_file_errno(
    interp: &Interpreter,
    message: &str,
    file: Value,
    error: &std::io::Error,
) -> LispError {
    // get_file_errno_data: CONSP (name) || NILP (name) ? name : list1 (name).
    if file.is_nil() {
        return report_file_errno_files(interp, message, &[], error);
    }
    if file.is_cons() {
        let mut files = Vec::new();
        let mut tail = file;
        while let Some((name, rest)) = tail.cons_values() {
            files.push(name);
            tail = rest;
        }
        // A dotted tail stays the list's.
        if !tail.is_nil() {
            return report_file_errno_list(interp, message, file, error);
        }
        return report_file_errno_files(interp, message, &files, error);
    }
    report_file_errno_files(interp, message, &[file], error)
}

/// get_file_errno_data with NAME a cons other than a proper list: the
/// list itself is the tail of the data.
fn report_file_errno_list(
    interp: &Interpreter,
    message: &str,
    name: Value,
    error: &std::io::Error,
) -> LispError {
    match get_file_errno_data_sharing(interp, message, name, error) {
        Ok(data) => LispError::SignalValue(data),
        Err(error) => error,
    }
}

/// fileio.c:get_file_errno_data (MESSAGE, NAME, errno) as GNU makes it:
/// a cons or nil NAME the data's own tail (a connection's contact, which
/// the data then shares), any other `list1 (name)'.
pub(crate) fn get_file_errno_data_sharing(
    interp: &Interpreter,
    message: &str,
    name: Value,
    error: &std::io::Error,
) -> Result<Value, LispError> {
    let data = if name.is_cons() || name.is_nil() {
        name
    } else {
        Value::list([name])
    };
    let errstring = errno_string(interp, error)?;
    let condition = file_error_condition(error);
    let errdata = Value::cons(errstring, data);
    Ok(if condition == "file-already-exists" {
        Value::cons(Value::symbol(condition), errdata)
    } else {
        Value::cons(
            Value::symbol(condition),
            Value::cons(Value::string(message), errdata),
        )
    })
}

/// get_file_errno_data's ERRSTRING: emacs_strerror's text through
/// code_convert_string_norecord with `locale-coding-system' and a string
/// destination.  There a nil coding system is `no-conversion' and another
/// passes CHECK_CODING_SYSTEM, which signals; an ASCII text whose coding
/// system is ASCII-compatible (and has no CR to convert) is copied as a
/// multibyte string, and any other text is decoded, `no-conversion'
/// making it unibyte.  The error of a bad coding system is the error
/// report_file_errno signals.
pub(crate) fn errno_string(
    interp: &Interpreter,
    error: &std::io::Error,
) -> Result<Value, LispError> {
    locale_message_string(interp, errno_text(error))
}

/// A C library's message TEXT (strerror's, gai_strerror's) as
/// code_convert_string_norecord with `locale-coding-system' makes it, as
/// errno_string describes.
pub(crate) fn locale_message_string(
    interp: &Interpreter,
    text: String,
) -> Result<Value, LispError> {
    let locale = interp
        .forwarded_c_value_key(cached_symbol!("locale-coding-system"))
        .unwrap_or(Value::Nil);
    let coding = if locale.is_nil() {
        "no-conversion".to_string()
    } else {
        crate::lisp::primitives::coding::checked_coding_name(interp, &locale)?
            .unwrap_or_else(|| "no-conversion".to_string())
    };
    let ascii_compatible =
        coding_attr(interp, coding.as_str(), ":ascii-compatible-p").is_some_and(|v| v.is_truthy());
    if ascii_compatible && text.is_ascii() && !text.contains('\r') {
        return crate::lisp::primitives::strings::make_specified_string(text.into_bytes(), true)
            .map_err(|error| {
                crate::lisp::primitives::strings::string_storage_error_shared(interp, error)
            });
    }
    let mut decoded = crate::lisp::primitives::coding::decode_text_bytes_annotated(
        interp,
        text.as_bytes(),
        &coding,
    )?;
    let for_unibyte =
        coding_attr(interp, coding.as_str(), ":for-unibyte").is_some_and(|v| v.is_truthy());
    // strerror's text holds no line end or composition, so it is one
    // decoder insertion with no layout to rebuild in the work buffer.
    let _layout = decoded.take_layout();
    let props = crate::lisp::primitives::coding::decoded_annotation_props(&mut decoded);
    Ok(
        crate::lisp::primitives::strings::make_shared_string_value_with_extended_chars(
            decoded.text,
            props,
            !for_unibyte,
            decoded.extended,
        ),
    )
}

/// fileio.c:report_file_errno with NAME a list of the Lisp file names
/// FILES as given, as `list2 (file, newname)' for the two-name
/// primitives.
pub(crate) fn report_file_errno_files(
    interp: &Interpreter,
    message: &str,
    files: &[Value],
    error: &std::io::Error,
) -> LispError {
    match get_file_errno_data(interp, message, files, error) {
        Ok(data) => LispError::SignalValue(data),
        Err(error) => error,
    }
}

/// fileio.c:get_file_errno_data (MESSAGE, FILES, errno): the error's
/// condition, MESSAGE (but for `file-already-exists'), its errno text and
/// FILES, the data report_file_errno signals and a failed non-blocking
/// connection keeps as its status.
pub(crate) fn get_file_errno_data(
    interp: &Interpreter,
    message: &str,
    files: &[Value],
    error: &std::io::Error,
) -> Result<Value, LispError> {
    // ERRSTRING first, as get_file_errno_data makes it before the data
    // around it: nothing it allocates is held only by a Rust vector.
    let errstring = errno_string(interp, error)?;
    let condition = file_error_condition(error);
    let mut data = vec![Value::symbol(condition)];
    if condition != "file-already-exists" {
        data.push(Value::string(message));
    }
    data.push(errstring);
    data.extend_from_slice(files);
    Ok(Value::list(data))
}

/// fileio.c:report_file_errno (SSDATA (STRING), FILE, errno): STRING's
/// bytes up to the first NUL made a string again by build_string, FILE
/// as given.
pub(crate) fn report_file_errno_sdata(
    interp: &Interpreter,
    string: &Value,
    file: Value,
    error: &std::io::Error,
) -> LispError {
    let errstring = match errno_string(interp, error) {
        Ok(errstring) => errstring,
        Err(error) => return error,
    };
    let condition = file_error_condition(error);
    let mut data = vec![Value::symbol(condition)];
    if condition != "file-already-exists" {
        let bytes = match string.kind() {
            Kind::StringObject(state) => state.borrow().bytes().to_vec(),
            _ => Vec::new(),
        };
        let end = bytes
            .iter()
            .position(|&byte| byte == 0)
            .unwrap_or(bytes.len());
        data.push(
            match crate::lisp::primitives::strings::make_string(bytes[..end].to_vec()) {
                Ok(message) => message,
                Err(error) => {
                    return crate::lisp::primitives::strings::string_storage_error_shared(
                        interp, error,
                    );
                }
            },
        );
    }
    data.push(errstring);
    // A nil FILE names no file (get_file_errno_data).
    if !file.is_nil() {
        data.push(file);
    }
    LispError::SignalValue(Value::list(data))
}

/// fileio.c:file_metadata_errno: nil for ENOENT, ENOTDIR (and no error),
/// otherwise report_file_errno (ACTION, FILE, ERR).
pub(crate) fn file_metadata_errno(
    interp: &Interpreter,
    action: &str,
    file: Value,
    error: &std::io::Error,
) -> Result<Value, LispError> {
    match error.raw_os_error() {
        Some(0 | libc::ENOENT | libc::ENOTDIR) => Ok(Value::Nil),
        _ => Err(report_file_errno(interp, action, file, error)),
    }
}

pub(crate) fn file_operation_error(
    interp: &Interpreter,
    message: &str,
    error: &std::io::Error,
    path: &str,
) -> LispError {
    report_file_errno(interp, message, Value::string(path), error)
}

/// fileio.c Fmake_temp_file_internal past its CHECK_STRINGs: the
/// ENCODE_FILE'd PREFIX, "XXXXXX" and the ENCODE_FILE'd SUFFIX in a new
/// unibyte string, which gen_tempname fills in, creating the file (DIR-FLAG
/// nil), the directory (DIR-FLAG otherwise) or nothing (DIR-FLAG 0); its
/// DECODE_FILE, into which a nonempty TEXT is written.  A failure is
/// report_file_error with the kind's message, naming PREFIX.
pub(crate) fn make_temp_file_internal(
    interp: &mut Interpreter,
    env: &mut Env,
    prefix: Value,
    dir_flag: &Value,
    suffix: Value,
    text: Option<&Value>,
) -> Result<Value, LispError> {
    let encoded_prefix = super::system::encode_file_value(interp, env, prefix)?;
    let encoded_suffix = super::system::encode_file_value(interp, env, suffix)?;
    let kind = if dir_flag.is_nil() {
        TempKind::File
    } else if matches!(dir_flag.kind(), Kind::Integer(0)) {
        TempKind::NoCreate
    } else {
        TempKind::Dir
    };
    let mut template = sdata_c_string(&encoded_prefix);
    let start = template.len();
    template.extend_from_slice(b"XXXXXX");
    template.extend_from_slice(&sdata_c_string(&encoded_suffix));
    let message = match kind {
        TempKind::File => "Creating file with prefix",
        TempKind::Dir => "Creating directory with prefix",
        TempKind::NoCreate => "Creating file name with prefix",
    };
    let file = gen_tempname(&mut template, start, kind)
        .map_err(|error| report_file_errno(interp, message, prefix, &error))?;
    let val = super::make_specified_string(template, false)
        .map_err(|error| super::string_storage_error(interp, env, error))?;
    let val = super::system::decode_file_value(interp, env, val)?;
    // gen_tempname's descriptor is 0 for GT_DIR and GT_NOCREATE, and
    // GNU's write_region of a TEXT then writes to the standard input (a
    // "Write error" when it is read-only).  Emaxx does not write a TEXT
    // given with a non-nil DIR-FLAG: writing into the process's standard
    // input is a GNU defect, not a behavior a caller relies on.
    let Some(mut file) = file else {
        return Ok(val);
    };
    // write_region (text, Qnil, val, Qnil, Qnil, Qnil, Qnil, fd) of a
    // nonempty TEXT: FILENAME Fexpand_file_name'd; its `write-region'
    // handler, if any, writes it by name (call8, LOCKNAME the file);
    // otherwise the descriptor made takes it, the coding chosen and
    // recorded as for any write, no lock taken.
    if let Some(text) = text.copied()
        && string_like(&text).is_some_and(|string| !string.text.is_empty())
    {
        let filename = super::system::fexpand_file_name(interp, env, val, Value::Nil)?;
        let name = string_text(&filename)?;
        let operation = Value::symbol("write-region");
        if let Some(handler) = super::system::find_file_name_handler(interp, env, &name, operation)?
        {
            interp.call_function_value(
                handler,
                None,
                &[
                    operation,
                    text,
                    Value::Nil,
                    filename,
                    Value::Nil,
                    Value::Nil,
                    filename,
                    Value::Nil,
                ],
                env,
            )?;
        } else {
            let args = [
                text,
                Value::Nil,
                filename,
                Value::Nil,
                Value::Nil,
                Value::Nil,
                Value::Nil,
            ];
            let count = interp.specpdl_index();
            interp.record_unwind_save_restriction();
            interp.restore_labeled_restrictions(interp.current_buffer_id(), Vec::new());
            let written = write_region_with_coding(
                interp,
                &args,
                &name,
                false,
                &name,
                None,
                Some(&mut file),
                env,
            );
            let unbind = interp.unbind_to(count, env);
            written?;
            unbind?;
        }
    }
    // failed = NILP (dir_flag) && emacs_close (fd) != 0.
    #[cfg(unix)]
    {
        use std::os::fd::IntoRawFd;
        // SAFETY: the descriptor was just released from FILE and is closed
        // exactly once.
        if unsafe { libc::close(file.into_raw_fd()) } != 0 {
            return Err(report_file_errno(
                interp,
                message,
                prefix,
                &std::io::Error::last_os_error(),
            ));
        }
    }
    Ok(val)
}

/// gen_tempname's KIND.
#[derive(Clone, Copy)]
enum TempKind {
    File,
    Dir,
    NoCreate,
}

/// tempname.c gen_tempname_len with six X's at START in TEMPLATE: up to
/// 62**3 names tried (ATTEMPTS_MIN, glibc's TMP_MAX), each made by the
/// kind's try function -- try_file's O_RDWR | O_CREAT | O_EXCL open with
/// S_IRUSR | S_IWUSR (close-on-exec), try_dir's mkdir with 0700,
/// try_nocreate's lstat, where an existing name (or EOVERFLOW) is EEXIST
/// and only ENOENT is success.  A failure other than EEXIST is final;
/// running out of names is EEXIST.  The file made, for GT_FILE.
fn gen_tempname(
    template: &mut [u8],
    start: usize,
    kind: TempKind,
) -> std::io::Result<Option<fs::File>> {
    for _ in 0..62 * 62 * 62 {
        let letters = crate::lisp::primitives::coding::random_temp_suffix();
        template[start..start + 6].copy_from_slice(letters.as_bytes());
        let path = {
            use std::os::unix::ffi::OsStrExt;
            Path::new(std::ffi::OsStr::from_bytes(template))
        };
        let attempt = match kind {
            TempKind::File => {
                use std::os::unix::fs::OpenOptionsExt;
                fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                    .map(Some)
            }
            TempKind::Dir => {
                use std::os::unix::fs::DirBuilderExt;
                fs::DirBuilder::new()
                    .mode(0o700)
                    .create(path)
                    .map(|()| None)
            }
            TempKind::NoCreate => match fs::symlink_metadata(path) {
                Ok(_) => Err(std::io::Error::from_raw_os_error(libc::EEXIST)),
                Err(error) => match error.raw_os_error() {
                    Some(libc::ENOENT) => Ok(None),
                    Some(libc::EOVERFLOW) => Err(std::io::Error::from_raw_os_error(libc::EEXIST)),
                    _ => Err(error),
                },
            },
        };
        match attempt {
            Err(error) if error.raw_os_error() == Some(libc::EEXIST) => continue,
            result => return result,
        }
    }
    Err(std::io::Error::from_raw_os_error(libc::EEXIST))
}

/// buffer.h BUF_BYTES_MAX: the most bytes a buffer holds.
pub(crate) const BUF_BYTES_MAX: i64 = (1 << 61) - 2;
/// fileio.c READ_BUF_SIZE (MAX_ALLOCA): the guess for a pipe or a
/// /proc file whose size reads as zero.
const READ_BUF_SIZE: i64 = 16 * 1024;

/// insdel.c buffer_overflow.
pub(crate) fn buffer_overflow() -> LispError {
    LispError::Signal("Maximum buffer size exceeded".into())
}

/// fileio.c file_offset: an integer from 0 to the largest off_t, or a
/// float with such an integral value.
fn file_offset(value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(offset) if offset >= 0 => return Ok(offset),
        Kind::BigInteger(offset) => {
            if let Some(offset) = offset.to_i64()
                && offset >= 0
            {
                return Ok(offset);
            }
        }
        Kind::Float(float) => {
            let float = float.get();
            if (0.0..1.0 + i64::MAX as f64).contains(&float) && float as i64 as f64 == float {
                return Ok(float as i64);
            }
        }
        _ => {}
    }
    Err(LispError::WrongTypeArgument("file-offset".into(), *value))
}

/// The file Finsert_file_contents opened, as fstat and lseek find it: a
/// directory counts as regular; a file that is neither cannot be
/// replaced but with `if-regular' (which erases the accessible
/// portion) or read from BEG, and is seekable when lseek works on it.
/// TOTAL is the reading's expected size: END - BEG, END defaulting to
/// the size (READ_BUF_SIZE for a size of zero) or, for a special file,
/// the largest off_t; READ_BUF_SIZE for an unseekable one without END.
pub(crate) struct InsertFileSource {
    file: fs::File,
    pub(crate) regular: bool,
    seekable: bool,
    end_given: bool,
    pub(crate) beg: i64,
    pub(crate) end: i64,
    pub(crate) size: i64,
    pub(crate) total: i64,
}

/// sysdep.c:emacs_open: HOST opened with FLAGS and O_CLOEXEC (MODE for a
/// file it creates), an open a signal interrupts tried again after
/// maybe_quit, which runs a pending fatal signal.  The outer error is the
/// quit's, the inner the open's.
pub(crate) fn emacs_open(
    interp: &mut Interpreter,
    env: &mut Env,
    host: &std::path::Path,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> Result<std::io::Result<fs::File>, LispError> {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::FromRawFd;
    let Ok(name) = std::ffi::CString::new(host.as_os_str().as_bytes()) else {
        return Ok(Err(std::io::Error::from_raw_os_error(libc::EINVAL)));
    };
    loop {
        // SAFETY: NAME is a valid NUL-terminated string for the call.
        let fd = unsafe {
            libc::open(
                name.as_ptr(),
                flags | libc::O_CLOEXEC,
                libc::c_uint::from(mode),
            )
        };
        if fd >= 0 {
            // SAFETY: FD was just opened and is owned by nothing else.
            return Ok(Ok(unsafe { fs::File::from_raw_fd(fd) }));
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != ErrorKind::Interrupted {
            return Ok(Err(error));
        }
        interp.maybe_quit(env)?;
    }
}

/// sysdep.c:emacs_read_quit: maybe_quit, then a read of at most WANT
/// bytes from FILE onto the end of BYTES, an interrupted read tried again
/// after maybe_quit.  The outer error is the quit's, the inner the
/// read's; zero is the end of the file.
fn emacs_read_quit(
    interp: &mut Interpreter,
    env: &mut Env,
    file: &fs::File,
    bytes: &mut Vec<u8>,
    want: usize,
) -> Result<std::io::Result<usize>, LispError> {
    use std::os::unix::io::AsRawFd;
    bytes.reserve(want);
    loop {
        interp.maybe_quit(env)?;
        let spare = bytes.spare_capacity_mut();
        let room = want.min(spare.len());
        // SAFETY: SPARE has room for ROOM bytes, which the read fills
        // before they are counted as the vector's.
        let read = unsafe { libc::read(file.as_raw_fd(), spare.as_mut_ptr().cast(), room) };
        if read >= 0 {
            let read = read as usize;
            // SAFETY: the read initialized READ bytes of the spare room.
            unsafe { bytes.set_len(bytes.len() + read) };
            return Ok(Ok(read));
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != ErrorKind::Interrupted {
            return Ok(Err(error));
        }
    }
}

/// sysdep.c:emacs_full_write with maybe_quit (emacs_write_quit): BYTES
/// written to FILE, a write a signal interrupts tried again after
/// maybe_quit.  The outer error is the quit's, the inner the write's.
pub(crate) fn emacs_write_quit(
    interp: &mut Interpreter,
    env: &mut Env,
    file: &mut fs::File,
    mut bytes: &[u8],
) -> Result<std::io::Result<()>, LispError> {
    while !bytes.is_empty() {
        match file.write(bytes) {
            Ok(0) => return Ok(Err(ErrorKind::WriteZero.into())),
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == ErrorKind::Interrupted => interp.maybe_quit(env)?,
            Err(error) => return Ok(Err(error)),
        }
    }
    Ok(Ok(()))
}

/// What the read loop got: the bytes, and the error a failed read ended
/// it with.
pub(crate) struct InsertFileRead {
    pub(crate) bytes: Vec<u8>,
    pub(crate) error: Option<std::io::Error>,
}

impl InsertFileSource {
    pub(crate) fn examine(
        interp: &Interpreter,
        mut file: fs::File,
        filename: Value,
        replace: Option<&Value>,
        beg: Option<i64>,
        end: Option<i64>,
    ) -> Result<Self, LispError> {
        use std::io::Seek;
        let metadata = file
            .metadata()
            .map_err(|error| report_file_errno(interp, "Input file status", filename, &error))?;
        let regular = metadata.is_file() || metadata.is_dir();
        let mut seekable = true;
        if !regular {
            if replace.is_some_and(|replace| !replace.is_key(cached_symbol!("if-regular"))) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("file-error"),
                    Value::String("not a regular file".into()),
                    filename,
                ])));
            }
            if beg.is_some() {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("file-error"),
                    Value::String(
                        "cannot use a start position in a non-seekable file/device".into(),
                    ),
                    filename,
                ])));
            }
            seekable = file.stream_position().is_ok();
        }
        let size = i64::try_from(metadata.len()).map_err(|_| buffer_overflow())?;
        let beg_offset = beg.unwrap_or(0);
        let end_offset = match end {
            Some(end) => end,
            None if !regular => i64::MAX,
            None if size == 0 => READ_BUF_SIZE,
            None => size,
        };
        let total = if seekable || end.is_some() {
            end_offset - beg_offset
        } else {
            READ_BUF_SIZE
        };
        Ok(Self {
            file,
            regular,
            seekable,
            end_given: end.is_some(),
            beg: beg_offset,
            end: end_offset,
            size,
            total,
        })
    }

    /// The buffer's growth checked before any work, in the likely case
    /// that the regular file's length is not changing: KEPT bytes of the
    /// buffer stay.
    pub(crate) fn check_likely_growth(&self, kept: usize) -> Result<(), LispError> {
        let likely_end = self.end.min(self.size);
        if self.regular
            && self.beg < likely_end
            && BUF_BYTES_MAX - (kept as i64) < likely_end - self.beg
        {
            return Err(buffer_overflow());
        }
        Ok(())
    }

    /// make_gap's check of a gap for TOTAL bytes in a buffer of
    /// BUFFER_BYTES.
    pub(crate) fn check_gap(&self, buffer_bytes: usize) -> Result<(), LispError> {
        if self.total > 0 && self.total >= BUF_BYTES_MAX - buffer_bytes as i64 {
            return Err(buffer_overflow());
        }
        Ok(())
    }

    /// The read loop, from BEG (lseek'd to for a nonzero BEG or REPLACE):
    /// at most TOTAL bytes of a seekable file or with END given, else to
    /// the end of the file.
    pub(crate) fn read(
        mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        filename: Value,
        replace: bool,
    ) -> Result<InsertFileRead, LispError> {
        use std::io::{Seek, SeekFrom};
        if self.beg != 0 || replace {
            self.file
                .seek(SeekFrom::Start(self.beg as u64))
                .map_err(|error| {
                    report_file_errno(interp, "Setting file position", filename, &error)
                })?;
        }
        let limit = (self.seekable || self.end_given).then(|| self.total.max(0) as u64);
        // Room for what a regular file holds past BEG, each read going
        // straight into it.
        let expected = if self.regular && self.size > self.beg {
            (self.size - self.beg).min(self.total.max(0))
        } else {
            READ_BUF_SIZE
        };
        let mut bytes = Vec::with_capacity(expected as usize);
        loop {
            let want = match limit {
                Some(limit) => (limit - bytes.len() as u64).min(READ_BUF_SIZE as u64) as usize,
                None => READ_BUF_SIZE as usize,
            };
            if want == 0 {
                break;
            }
            // emacs_read_quit of WANT bytes into the room past the bytes
            // read.
            match emacs_read_quit(interp, env, &self.file, &mut bytes, want)? {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) => {
                    return Ok(InsertFileRead {
                        bytes,
                        error: Some(error),
                    });
                }
            }
        }
        Ok(InsertFileRead { bytes, error: None })
    }
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
    let name = &crate::lisp::primitives::coding::coding_text_of(interp, &coding)?;
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
    // An auto-save file other than the visited one is utf-8-emacs, with
    // Unix line ends.
    if auto_saving() && !auto_saving_into_visited_file(interp) {
        return inherit_write_coding_eol(
            interp,
            Value::symbol("utf-8-emacs"),
            Value::symbol("unix"),
        );
    }
    let mut coding = interp
        .lookup_var_key(cached_symbol!("coding-system-for-write"), env)
        .unwrap_or(Value::Nil);
    if !coding.is_nil() {
        let selector = interp
            .lookup_var_key(cached_symbol!("select-safe-coding-system-function"), env)
            .unwrap_or(Value::Nil);
        if interp
            .lookup_var_key(cached_symbol!("coding-system-require-warning"), env)
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
        .lookup_var_key(cached_symbol!("buffer-file-coding-system"), env)
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
            .lookup_var_key(cached_symbol!("buffer-file-coding-system"), env)
            .unwrap_or(Value::Nil);
    }
    if !coding.is_nil() && !force_raw {
        load_coding_system(interp, &coding, env)?;
        let name = checked_coding_symbol(interp, &coding)?;
        force_raw = matches!(
            interp.coding_system_kind_name(&name).as_deref(),
            Some("raw-text" | "no-conversion")
        );
    }
    let selector = interp
        .lookup_var_key(cached_symbol!("select-safe-coding-system-function"), env)
        .unwrap_or(Value::Nil);
    if !force_raw && call(interp, "fboundp", &[selector], env)?.is_truthy() {
        coding = interp.call_function_value(
            selector,
            None,
            &[args[0], args[1], coding, Value::Nil, args[2]],
            env,
        )?;
        if !coding.is_nil() {
            load_coding_system(interp, &coding, env)?;
            checked_coding_symbol(interp, &coding)?;
        }
    }
    if !using_default {
        let default = interp
            .default_value_key(cached_symbol!("buffer-file-coding-system"))
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
                load_coding_system(interp, &coding, env)?;
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

/// editfns.c validate_region of write-region's START and END: the two
/// positions in order, within the accessible portion.
pub(crate) fn validate_write_region(
    interp: &Interpreter,
    start: Value,
    end: Value,
) -> Result<(usize, usize), LispError> {
    let from = position_from_value(interp, &start)?;
    let to = position_from_value(interp, &end)?;
    let (from, to) = (from.min(to), from.max(to));
    let buffer = interp.buffer.borrow();
    if from < buffer.point_min() || to > buffer.point_max() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            Value::Buffer(interp.buffer),
            start,
            end,
        ])));
    }
    Ok((from, to))
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
        let (start, end) = validate_write_region(interp, args[0], args[1])?;
        normalized[0] = Value::Integer(start as i64);
        normalized[1] = Value::Integer(end as i64);
    }
    // FILENAME, as the preamble expanded it.
    let path = string_text(&args[2])?;
    let existed_before_write = fs::symlink_metadata(host_file_name(interp, env, &path)?).is_ok();
    if normalized[6].is_truthy() && !normalized[6].is_key(cached_symbol!("excl")) {
        barf_or_query_if_file_exists(interp, env, logical_path, false, "overwrite", true, true)?;
    }
    // write_region saves restriction state even for explicit/string input;
    // nil START temporarily exposes the entire buffer to coding callbacks.
    let count = interp.specpdl_index();
    interp.record_unwind_save_restriction();
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
        None,
        env,
    );
    let unbind = interp.unbind_to(count, env);
    result.and_then(|value| unbind.map(|()| value))
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
            let count = interp.specpdl_index();
            interp.record_unwind_save_restriction();
            interp.restore_labeled_restrictions(interp.current_buffer_id(), Vec::new());
            if start.is_nil() {
                interp.buffer.borrow_mut().widen();
            }
            let result = encode_region_for_write(interp, start, end, env);
            let result = interp.unbind_to(count, env).and(result);
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
    let coding_name = &crate::lisp::primitives::coding::coding_text_of(interp, &coding)?;
    crate::lisp::primitives::coding::set_last_coding_system_used_value(interp, coding, env);
    let selective_display = !args[0].is_string()
        && interp
            .lookup_var_key(cached_symbol!("selective-display"), env)
            .is_some_and(|value| crate::lisp::types::eq(value, Value::T));
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
            props: None,
            multibyte: buffer.is_multibyte(),
            extended_chars: buffer.substring_extended_chars(start, end),
        }
    };
    let region = (!args[0].is_string()).then(|| {
        (
            position_from_value(interp, &args[0]),
            position_from_value(interp, &args[1]),
        )
    });
    let source = crate::lisp::primitives::coding::pre_write_for_file(
        interp,
        source,
        |interp| match region {
            Some((Ok(start), Ok(end))) => interp
                .buffer
                .borrow()
                .copy_intervals(start.min(end), start.max(end)),
            _ => None,
        },
        coding_name,
        env,
    )?;
    let inhibit_eol_conversion = interp
        .lookup_var_key(cached_symbol!("inhibit-eol-conversion"), env)
        .is_some_and(|value| value.is_truthy());
    let translation = encode_translation_tables(interp, coding_name, env)?;
    super::coding::encode_text_bytes_for_write(
        interp,
        &source,
        coding_name,
        inhibit_eol_conversion,
        selective_display,
        &translation,
    )
}

#[allow(clippy::too_many_arguments)]
fn write_region_with_coding(
    interp: &mut Interpreter,
    args: &[Value; 7],
    path: &str,
    existed_before_write: bool,
    logical_path: &str,
    logical_lock_path: Option<&str>,
    // write_region's DESC: a file already open, which is neither locked,
    // opened nor closed here.
    mut descriptor: Option<&mut fs::File>,
    env: &mut Env,
) -> Result<Value, LispError> {
    // FILENAME as given: the expanded name whose text is PATH.
    let mut selected_args = *args;
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
    let coding_name = &crate::lisp::primitives::coding::coding_text_of(interp, &coding)?;
    crate::lisp::primitives::coding::set_last_coding_system_used_value(interp, coding, env);
    // Selection calls Lisp, which may mutate the supplied string or buffer.
    // Read its actual current contents after the callback has returned.
    // choose_write_coding: CODING_MODE_SELECTIVE_DISPLAY for a buffer
    // region whose `selective-display' is t.
    let selective_display = !selected_args[0].is_string()
        && interp
            .lookup_var_key(cached_symbol!("selective-display"), env)
            .is_some_and(|value| crate::lisp::types::eq(value, Value::T));
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
            props: None,
            multibyte: buffer.is_multibyte(),
            extended_chars: buffer.substring_extended_chars(start, end),
        }
    };
    let region = (!selected_args[0].is_string()).then(|| {
        (
            position_from_value(interp, &selected_args[0]),
            position_from_value(interp, &selected_args[1]),
        )
    });
    let source = crate::lisp::primitives::coding::pre_write_for_file(
        interp,
        source,
        |interp| match region {
            Some((Ok(start), Ok(end))) => interp
                .buffer
                .borrow()
                .copy_intervals(start.min(end), start.max(end)),
            _ => None,
        },
        coding_name,
        env,
    )?;
    let inhibit_eol_conversion = interp
        .lookup_var_key(cached_symbol!("inhibit-eol-conversion"), env)
        .is_some_and(|value| value.is_truthy());
    let translation = encode_translation_tables(interp, coding_name, env)?;
    let bytes = super::coding::encode_text_bytes_for_write(
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
    // An auto-save neither locks nor unlocks.
    let auto_saving = auto_saving();
    let open_and_close_file = descriptor.is_none();
    if open_and_close_file && !auto_saving {
        flock_file(interp, env, Value::String(lock_path.clone().into()))?;
    }
    let host = host_file_name(interp, env, path)?;
    // fileio.c:write_region's open, lseek and write, each failure
    // report_file_errno with its errno: "Opening output file", "Lseek
    // error", and "Write error" for the write or the close (once the
    // file is open, after the lock is given back).
    let mut opened = false;
    // FILENAME, the Lisp name report_file_errno is given.
    let filename = args[2];
    let write_result = (|| {
        use std::os::unix::io::IntoRawFd;
        if let Some(file) = descriptor.as_deref_mut() {
            opened = true;
            return emacs_write_quit(interp, env, file, &bytes)?
                .map_err(|error| report_file_errno(interp, "Write error", filename, &error));
        }
        let exclusive = args[6].is_key(cached_symbol!("excl"));
        // An auto-save file is made with auto_save_mode_bits.
        let mode = if auto_saving {
            AUTO_SAVE_MODE_BITS.get()
        } else {
            0o666
        };
        let offset = if matches!(
            args[3].kind(),
            Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_)
        ) {
            Some(file_offset(&args[3])?)
        } else {
            None
        };
        // write_region's open_flags: O_WRONLY | O_CREAT, then O_EXCL for
        // `excl', nothing for an APPEND, else O_TRUNC; O_APPEND for an
        // APPEND that is not an offset.
        let mut flags = libc::O_WRONLY | libc::O_CREAT;
        if exclusive {
            flags |= libc::O_EXCL;
        } else if args[3].is_nil() {
            flags |= libc::O_TRUNC;
        }
        if offset.is_none() && args[3].is_truthy() {
            flags |= libc::O_APPEND;
        }
        let mut file = emacs_open(interp, env, &host, flags, mode)?
            .map_err(|error| report_file_errno(interp, "Opening output file", filename, &error))?;
        opened = true;
        if let Some(offset) = offset {
            file.seek(SeekFrom::Start(offset as u64))
                .map_err(|error| report_file_errno(interp, "Lseek error", filename, &error))?;
        }
        emacs_write_quit(interp, env, &mut file, &bytes)?
            .map_err(|error| report_file_errno(interp, "Write error", filename, &error))?;
        // NFS can report a write failure at the close.
        let fd = file.into_raw_fd();
        // SAFETY: the descriptor was just released by its File.
        if unsafe { libc::close(fd) } < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != ErrorKind::Interrupted {
                return Err(report_file_errno(interp, "Write error", filename, &error));
            }
        }
        Ok(())
    })();
    let unlock_result = if auto_saving || !open_and_close_file {
        Ok(())
    } else {
        funlock_file(interp, env, Value::String(lock_path.clone().into()))
    };
    // A visited file that was opened has its modtime recorded before a
    // write error is reported, so that the next save sees no change on
    // disk.
    let visiting = args
        .get(4)
        .is_some_and(|visit| matches!(visit.kind(), Kind::T) || string_like(visit).is_some());
    if write_result.is_err()
        && opened
        && visiting
        && let Ok(Ok(modtime)) = stat_visited_modtime(interp, env, path)
    {
        interp.buffer.borrow_mut().set_visited_file_modtime(modtime);
    }
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
    let into_visited_file = auto_saving_into_visited_file(interp);
    let visit = args.get(4).copied().unwrap_or(Value::Nil);
    if matches!(visit.kind(), Kind::T) || string_like(&visit).is_some() {
        // bset_filename (current_buffer, visit_file): FILENAME for a VISIT
        // of t, else Fexpand_file_name (VISIT, Qnil).
        let visit_file = if matches!(visit.kind(), Kind::T) {
            args[2]
        } else {
            crate::lisp::primitives::fexpand_file_name(interp, env, visit, Value::Nil)?
        };
        interp.buffer.borrow_mut().set_file_name(visit_file);
        // The time and size the written file has, unless its status cannot
        // be read.
        if let Ok(modtime) = stat_visited_modtime(interp, env, path)? {
            interp.buffer.borrow_mut().set_visited_file_modtime(modtime);
        }
        record_visit_saved(interp);
        if into_visited_file {
            funlock_file(interp, env, Value::String(lock_path.into()))?;
        }
    } else if visit.is_truthy() {
        // Quietly: an auto-save into the visited file saves it.
        if into_visited_file {
            interp.buffer.borrow_mut().set_unmodified();
            funlock_file(interp, env, Value::String(lock_path.into()))?;
        }
        return Ok(Value::Nil);
    }
    if !auto_saving
        && interp
            .lookup_var_key(cached_symbol!("noninteractive"), env)
            .is_none_or(|value| value.is_nil())
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
        // The coding as given, or the one an alias no name names stands
        // for (`checked_coding_given').
        let given = crate::lisp::primitives::coding::checked_coding_given(interp, coding)?;
        let encoded =
            encode_coding_value_recording(interp, &string, given.as_deref(), true, false, env)?;
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
    let report = |interp: &Interpreter, operation: &str, file: Value| {
        let error = std::io::Error::last_os_error();
        if file.is_string() {
            report_file_errno(interp, operation, file, &error)
        } else {
            report_file_errno_files(interp, operation, &[], &error)
        }
    };
    let mut fd = STDERR_DUP.load(Ordering::Relaxed);
    let mut file = *file;
    if !file.is_nil() {
        file = super::system::fexpand_file_name(interp, env, file, Value::Nil)?;
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
                return Err(report(interp, "dup", file));
            }
            STDERR_DUP.store(n, Ordering::Relaxed);
        }
        // SSDATA (ENCODE_FILE (file)): ENCODE_FILE rejects a NUL.
        let encoded = super::system::encode_file_value(interp, env, file)?;
        let path = std::ffi::CString::new(super::sdata_c_string(&encoded))
            .expect("ENCODE_FILE leaves no NUL");
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
            return Err(report(interp, "Cannot open debugging output stream", file));
        }
    }
    let _ = std::io::stderr().lock().flush();
    // SAFETY: FD is open; dup2 replaces the stderr descriptor.
    if unsafe { libc::dup2(fd, libc::STDERR_FILENO) } < 0 {
        return Err(report(interp, "dup2", file));
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

/// `print_object_to_stream' into the buffer STREAM, giving also the
/// octets of an output print.c's strout counted a character each (a C
/// string's non-ASCII bytes, as a subr's name), when the buffer is
/// multibyte: print_finish inserts those as that many characters, which a
/// buffer here does not hold, so a caller that takes the text back needs
/// them.
pub(crate) fn print_object_to_buffer(
    interp: &mut Interpreter,
    value: &Value,
    stream: Value,
    env: &mut Env,
    escape: bool,
) -> Result<Option<Vec<u8>>, LispError> {
    with_printer_destination(interp, Some(&stream), env, |interp, env| {
        let multibyte = interp.buffer.borrow().is_multibyte();
        with_printer_buffer_escape(interp, env, Some(multibyte), |interp, env| {
            let rendered = render_printer_output(interp, value, env, escape, false)?;
            let octets = (multibyte && !rendered.is_multibyte() && !rendered.bytes().is_ascii())
                .then(|| rendered.octets());
            write_prepared_printer_output(interp, &rendered, Some(&stream), env)?;
            Ok(octets)
        })
    })
}

/// print.c:print_prepare binds print--unreadable-callback-buffer to the
/// buffer current before it selects the output's, for BODY.
pub(crate) fn with_printer_destination<T>(
    interp: &mut Interpreter,
    stream: Option<&Value>,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let caller = interp.current_buffer_id();
    let callback = interp.print_unreadable_callback_buffer.replace(caller);
    let result = select_printer_destination(interp, stream, env, body);
    interp.print_unreadable_callback_buffer = callback;
    result
}

/// print.c:print_prepare selects the output buffer before rendering, and
/// print_finish moves a marker and restores point after successful insertion.
/// The original current buffer is restored on either normal or error return.
fn select_printer_destination<T>(
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
                .lookup_var_key(cached_symbol!("noninteractive"), env)
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
            insert_text_with_hooks(interp, &text, None, &extended, false, false, env)
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
            .lookup_var_key(cached_symbol!("noninteractive"), env)
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
                .lookup_var_key(cached_symbol!("print-integers-as-characters"), env)
                .is_some_and(|value| value.is_truthy())
                && render_princ_integer_as_character(interp, &Value::Integer(integer)).is_some()
        }
        Kind::BigInteger(_) | Kind::Float(_) => false,
        Kind::StringObject(_) => escaped,
        Kind::Symbol(name) => !name.as_str().is_empty(),
        _ => true,
    }
}

pub(crate) fn printer_stream_value(
    interp: &Interpreter,
    env: &Env,
    explicit: Option<&Value>,
) -> Option<Value> {
    let resolved = match explicit.map(|v| v.kind()) {
        Some(Kind::Nil) => interp.lookup_var_key(cached_symbol!("standard-output"), env),
        Some(value) => Some(value.value()),
        None => interp.lookup_var_key(cached_symbol!("standard-output"), env),
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
                    .find(|(name, _, _)| key.is_named(name))
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
    filename_value: Value,
    decided_bytes: &mut Option<Vec<u8>>,
) -> Result<Option<String>, LispError> {
    let Some(function) = interp
        .lookup_var_key(cached_symbol!("set-auto-coding-function"), env)
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
        let Some(head_and_tail) = auto_coding_file_sample(interp, filename_value, &host)? else {
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
fn auto_coding_file_sample(
    interp: &Interpreter,
    filename: Value,
    host: &Path,
) -> Result<Option<Vec<u8>>, LispError> {
    use std::io::{Seek, SeekFrom};
    let read_error =
        |error: std::io::Error| report_file_errno(interp, "Read error", filename, &error);
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
                .map_err(|error| {
                    report_file_errno(interp, "Setting file position", filename, &error)
                })?;
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
        .lookup_var_key(cached_symbol!("default-directory"), env)
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
        interp.set_variable_key(
            cached_symbol!("default-directory"),
            directory,
            &mut work_env,
        );
        interp.set_variable_key(
            cached_symbol!("buffer-read-only"),
            Value::Nil,
            &mut work_env,
        );
        interp.set_variable_key(
            cached_symbol!("buffer-file-name"),
            Value::Nil,
            &mut work_env,
        );
        interp.set_variable_key(cached_symbol!("buffer-undo-list"), Value::T, &mut work_env);
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
        .lookup_var_key(cached_symbol!("buffer-undo-list"), env)
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
    interp.set_variable_key(
        cached_symbol!("buffer-undo-list"),
        Value::T,
        &mut decide_env,
    );
    interp.insert_current_buffer_raw_bytes(bytes);
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
            .delete_region_current_buffer_unreturned(1, z)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let mut buffer = interp.buffer.borrow_mut();
        buffer.goto_char(1);
        buffer.set_multibyte(multibyte);
        buffer.set_modified_tick(modiff);
        buffer.set_chars_modified_tick(chars_modiff);
        drop(buffer);
        interp.set_variable_key(
            cached_symbol!("buffer-undo-list"),
            undo_list,
            &mut decide_env,
        );
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
    /// The `charset' properties the decoder annotated, as the decoded
    /// text's intervals.
    pub(crate) charset_props: Option<crate::intervals::Intervals>,
    /// How decode_coding inserts the text (see `DecodedText::layout').
    pub(crate) layout: Option<super::DecodeLayout>,
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
    let (coding, decided_bytes) =
        decide_file_coding(interp, env, bytes, literal, path, operation_args)?;
    decode_file_bytes(
        interp,
        env,
        decided_bytes.unwrap_or_else(|| bytes.to_vec()),
        coding,
        make_unibyte,
    )
}

/// The coding decode_file_contents decodes with, and the bytes
/// `set-auto-coding-function' left in an empty buffer when it changed
/// them.
pub(crate) fn decide_file_coding(
    interp: &mut Interpreter,
    env: &Env,
    bytes: &[u8],
    literal: bool,
    path: &str,
    operation_args: &[Value],
) -> Result<(String, Option<Vec<u8>>), LispError> {
    let request = interp
        .lookup_var_key(cached_symbol!("coding-system-for-read"), env)
        .filter(|value| !value.is_nil());
    let mut coding = if literal {
        Some("no-conversion".to_string())
    } else if request
        .as_ref()
        .is_some_and(|value| value.is_key(cached_symbol!("auto-save-coding")))
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
        // FILENAME, insert-file-contents' first argument, names a read
        // error.
        let filename = operation_args.first().copied().unwrap_or(Value::Nil);
        coding = auto_coding_for_file(interp, env, bytes, path, filename, &mut decided_bytes)?;
    }
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
    if !interp.buffer.borrow().is_multibyte() {
        coding = raw_text_coding_system(interp, &coding);
    }
    Ok((coding, decided_bytes))
}

/// decode_coding_gap of BYTES with CODING into the current buffer's
/// representation (unibyte for MAKE_UNIBYTE and a coding for unibyte
/// text).
pub(crate) fn decode_file_bytes(
    interp: &mut Interpreter,
    env: &Env,
    bytes: Vec<u8>,
    coding: String,
    make_unibyte: bool,
) -> Result<FileDecode, LispError> {
    let multibyte = interp.buffer.borrow().is_multibyte();
    let canonical = interp
        .coding_system_canonical_name(&coding)
        .unwrap_or_else(|| coding.clone());
    let for_unibyte =
        coding_attr(interp, &canonical, ":for-unibyte").is_some_and(|value| value.is_truthy());
    // Finsert_file_contents decodes nothing read only for a coding that
    // needs no flushing (CODING_REQUIRE_FLUSHING: a CCL decoder still
    // runs its end-of-input code).
    if bytes.is_empty() && !super::coding::coding_requires_flushing(interp, &canonical) {
        return Ok(FileDecode {
            decoded: DecodedText::new(),
            coding,
            for_unibyte,
            charset_props: None,
            layout: None,
        });
    }
    let dst_multibyte = multibyte && !(make_unibyte && for_unibyte);
    let mut outcome = decode_coding_gap(interp, env, bytes, &coding, dst_multibyte)?;
    let layout = outcome.decoded.take_layout();
    let charset_props = super::decoded_annotation_props(&mut outcome.decoded);
    Ok(FileDecode {
        decoded: outcome.decoded,
        coding: outcome.coding,
        for_unibyte,
        charset_props,
        layout,
    })
}

/// Finsert_file_contents's checks before its file-name handler: no
/// visit in an indirect buffer, and Fbarf_if_buffer_read_only of a
/// read-only one.  Returns empty_undo_list_p: "If the undo log only
/// contains the insertion, there's no point keeping it", a visit into an
/// empty buffer whose list is nil.
pub(crate) fn insert_file_contents_prologue(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<bool, LispError> {
    let visit = args.get(1).is_some_and(Value::is_truthy);
    let empty_undo_list_p = visit && {
        let buffer = interp.buffer.borrow();
        buffer.undo_list().is_nil() && buffer.size_total() == 0
    };
    if visit && interp.buffer_base_id(interp.current_buffer_id()).is_some() {
        return Err(LispError::Signal(
            "Cannot do file visiting in an indirect buffer".into(),
        ));
    }
    if interp
        .lookup_var_key(cached_symbol!("buffer-read-only"), env)
        .is_some_and(|value| value.is_truthy())
    {
        super::call(interp, "barf-if-buffer-read-only", &[], env)?;
    }
    Ok(empty_undo_list_p)
}

/// fileio.c's record of a visited file in the buffer: SAVE_MODIFF =
/// MODIFF and `buffer-saved-size' (save_length) the buffer's size.
pub(crate) fn record_visit_saved(interp: &mut Interpreter) {
    interp.buffer.borrow_mut().set_unmodified();
    let size = interp.buffer.borrow().size_total();
    let buffer_id = interp.current_buffer_id();

    interp.set_buffer_local_value_symbol(
        buffer_id,
        &cached_symbol!("buffer-saved-size").with(Clone::clone),
        Value::Integer(size as i64),
    );
}

/// Finsert_file_contents's record of a visit: saved as
/// `record_visit_saved', and auto-saved at the same count
/// (BUF_AUTOSAVE_MODIFF = MODIFF).
pub(crate) fn record_insert_visit_saved(interp: &mut Interpreter) {
    record_visit_saved(interp);
    interp.buffer.borrow_mut().set_autosaved();
}

/// Finsert_file_contents's record of a visit it read itself (the
/// modtime set by the caller): an undo list emptied for
/// EMPTY_UNDO_LIST_P, the file name, the buffer saved at its size, and
/// the locks of its truename and of the file released.
fn record_visit(
    interp: &mut Interpreter,
    env: &mut Env,
    filename: Value,
    encoded_filename: Value,
    empty_undo_list_p: bool,
) -> Result<(), LispError> {
    if empty_undo_list_p {
        interp.buffer.borrow_mut().set_undo_list(Value::Nil);
    }
    interp.buffer.borrow_mut().set_file_name(filename);
    record_insert_visit_saved(interp);
    if let Some(truename) = interp
        .lookup_var_key(cached_symbol!("buffer-file-truename"), env)
        .filter(|truename| !truename.is_nil())
    {
        super::call(interp, "unlock-file", &[truename], env)?;
    }
    // Funlock_file (filename): the name Finsert_file_contents ENCODE_FILEd
    // before its open, not orig_filename, nor a new encoding.
    super::call(interp, "unlock-file", &[encoded_filename], env)?;
    Ok(())
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
    let old_deactivate_mark = interp
        .lookup_var_key(cached_symbol!("deactivate-mark"), env)
        .unwrap_or(Value::Nil);
    // The prologue ran before the file-name handler lookup; its value is
    // the same here.
    let empty_undo_list_p = args.get(1).is_some_and(Value::is_truthy) && {
        let buffer = interp.buffer.borrow();
        buffer.undo_list().is_nil() && buffer.size_total() == 0
    };
    // FILENAME, as the preamble expanded it.
    let path = string_text(&args[0])?;
    let arg = |index: usize| args.get(index).copied().filter(|value| !value.is_nil());
    let visit = arg(1).is_some();
    let replace_arg = arg(4);
    if visit {
        if arg(2).is_some() || arg(3).is_some() {
            return Err(LispError::Signal(
                "Attempt to visit less than an entire file".into(),
            ));
        }
        if interp.buffer.borrow().size_total() > 0 && replace_arg.is_none() {
            return Err(LispError::Signal(
                "Cannot do file visiting in a non-empty buffer".into(),
            ));
        }
    }
    let beg = arg(2).as_ref().map(file_offset).transpose()?;
    let end = arg(3).as_ref().map(file_offset).transpose()?;
    if let Some(coding) = interp.lookup_var_key(cached_symbol!("coding-system-for-read"), env)
        && !coding.is_nil()
    {
        load_coding_system(interp, &coding, env)?;
        let _ = checked_coding_symbol(interp, &coding)?;
    }
    let operation_args = [
        args[0],
        args.get(1).copied().unwrap_or(Value::Nil),
        args.get(2).copied().unwrap_or(Value::Nil),
        args.get(3).copied().unwrap_or(Value::Nil),
        args.get(4).copied().unwrap_or(Value::Nil),
    ];
    let mut open_failure = crate::buffer::ModtimeStamp::Unknown;
    // orig_filename = filename; filename = ENCODE_FILE (filename), the name
    // emacs_open (O_RDONLY) opens and Funlock_file later unlocks.
    let encoded_filename = super::system::encode_file_value(interp, env, args[0])?;
    let host = super::system::encoded_host_path(&encoded_filename);
    let opened = match emacs_open(interp, env, &host, libc::O_RDONLY, 0)? {
        Ok(file) => Ok(file),
        Err(error) => {
            open_failure = time_error_value(&error);
            Err(report_file_errno(
                interp,
                "Opening input file",
                args[0],
                &error,
            ))
        }
    };
    let file = match opened {
        Ok(file) => file,
        Err(error) if !visit => return Err(error),
        Err(error) => {
            // notfound: a visit completes before the failure to open is
            // reported (`find-file-noselect' relies on it for a file that
            // does not exist yet), with `coding-system-for-read' made the
            // buffer's coding and the coding decided for no text.
            if let Some(coding) = interp
                .lookup_var_key(cached_symbol!("coding-system-for-read"), env)
                .filter(|coding| !coding.is_nil())
            {
                interp.set_variable_key(cached_symbol!("buffer-file-coding-system"), coding, env);
            }
            let (coding, _) =
                decide_file_coding(interp, env, &[], literal, &path, &operation_args)?;
            let canonical = interp
                .coding_system_canonical_name(&coding)
                .unwrap_or_else(|| coding.clone());
            if replace_arg.is_none()
                && coding_attr(interp, &canonical, ":for-unibyte")
                    .is_some_and(|value| value.is_truthy())
            {
                super::call(interp, "set-buffer-multibyte", &[Value::Nil], env)?;
            }
            // mtime = time_error_value (save_errno), st.st_size = -1.
            interp
                .buffer
                .borrow_mut()
                .set_visited_file_modtime(crate::buffer::VisitedModtime {
                    stamp: open_failure,
                    size: -1,
                });
            record_visit(interp, env, args[0], encoded_filename, empty_undo_list_p)?;
            set_last_coding_system_used(interp, &coding, env);
            finish_insert_file_contents(interp, env, 0, &args[1..], empty_undo_list_p)?;
            return Err(error);
        }
    };
    // emacs_fd_fstat of the opened file, before any of it is read: its
    // time and size.
    let opened_modtime = file
        .metadata()
        .and_then(|metadata| {
            Ok(crate::buffer::VisitedModtime {
                stamp: crate::buffer::ModtimeStamp::Time(metadata.modified()?),
                size: metadata.len() as i64,
            })
        })
        .map_err(|error| report_file_errno(interp, "Input file status", args[0], &error))?;
    // Replacement preserves point as it preserves markers
    // (get_window_points_and_markers): a marker at point that the change
    // moves and restore_window_points places, and point goes there at the
    // end (restore_point_unwind).
    let point_marker = replace_arg.is_some().then(|| {
        let point = interp.buffer.borrow().point();
        let marker = interp.make_marker();
        if let Kind::Marker(handle) = marker.kind() {
            let buffer_id = interp.current_buffer_id();
            let _ = interp.set_marker(handle, Some(point), Some(buffer_id));
        }
        marker
    });
    let read_file = |interp: &mut Interpreter, env: &mut Env| -> Result<Value, LispError> {
        let source =
            InsertFileSource::examine(interp, file, args[0], replace_arg.as_ref(), beg, end)?;
        {
            let buffer = interp.buffer.borrow();
            let kept = buffer.size_total()
                - if replace_arg.is_some() {
                    buffer.point_max() - buffer.point_min()
                } else {
                    0
                };
            source.check_likely_growth(kept)?;
        }
        let regular = source.regular;
        // A file that is not regular replaces with `if-regular' (REPLACE
        // Qunbound) by erasing the accessible portion and inserting.
        let replace = replace_arg.is_some() && regular;
        let (coding_used, inserted_chars) = if replace {
            let read = source.read(interp, env, args[0], true)?;
            if let Some(error) = read.error {
                return Err(report_file_errno(interp, "Read error", args[0], &error));
            }
            replace_file_contents(
                interp,
                env,
                &read.bytes,
                literal,
                &path,
                &operation_args,
                point_marker.unwrap_or(Value::Nil),
            )?
        } else {
            insert_file_bytes(
                interp,
                env,
                source,
                literal,
                &path,
                &operation_args,
                visit,
                replace_arg.is_some(),
                old_deactivate_mark,
                point_marker,
            )?
        };
        if visit {
            interp
                .buffer
                .borrow_mut()
                .set_visited_file_modtime(opened_modtime);
            record_visit(interp, env, args[0], encoded_filename, empty_undo_list_p)?;
            if !regular {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("file-error"),
                    Value::String("not a regular file".into()),
                    args[0],
                ])));
            }
        }
        // Finsert_file_contents records the coding decode_coding_gap settled
        // on; mule.el's after-insert-file-set-coding, run below, derives
        // `buffer-file-coding-system' from it.
        set_last_coding_system_used(interp, &coding_used, env);
        let inserted = finish_insert_file_contents(
            interp,
            env,
            inserted_chars,
            &args[1..],
            empty_undo_list_p,
        )?;
        Ok(Value::list([args[0], Value::Integer(inserted as i64)]))
    };
    let Some(marker) = point_marker else {
        return read_file(interp, env);
    };
    interp.with_lisp_stack_roots(&marker, |interp| {
        let result = read_file(interp, env);
        let restore = super::call(interp, "goto-char", &[marker], env);
        if let Kind::Marker(handle) = marker.kind() {
            let _ = interp.set_marker(handle, None, None);
        }
        let value = result?;
        restore?;
        Ok(value)
    })
}

/// Finsert_file_contents's reading of SOURCE as an insertion at point.
/// A buffer holding text decides the coding first; then, unless
/// visiting, the change is prepared at point when bytes are expected,
/// a special file replacing with `if-regular' (REPLACING) erases the
/// accessible portion, the gap's growth is checked, and the bytes read
/// are decoded (the coding decided now for an empty buffer) and
/// inserted before point, the after-change functions run unless
/// visiting without REPLACE.  With nothing read a file locked by the
/// preparation is unlocked and `deactivate-mark' gets back
/// OLD_DEACTIVATE_MARK, else it is t.  Returns the coding used and the
/// characters inserted.
#[allow(clippy::too_many_arguments)]
fn insert_file_bytes(
    interp: &mut Interpreter,
    env: &mut Env,
    source: InsertFileSource,
    literal: bool,
    path: &str,
    operation_args: &[Value],
    visit: bool,
    replacing: bool,
    old_deactivate_mark: Value,
    point_marker: Option<Value>,
) -> Result<(String, usize), LispError> {
    use super::hooks_overlays::{OverlayHookCalls, with_overlay_hook_roots};
    use super::insdel::{AfterChange, prepare_to_modify_buffer, signal_after_change};
    let buffer_was_empty = interp.buffer.borrow().size_total() == 0;
    let mut decision = if buffer_was_empty {
        None
    } else {
        Some(decide_file_coding(
            interp,
            env,
            &[],
            literal,
            path,
            operation_args,
        )?)
    };
    let total = source.total;
    let we_locked_file = !visit && total > 0 && {
        let modified = interp.buffer.borrow().is_modified();
        !modified
            && interp
                .lookup_var_key(cached_symbol!("buffer-file-truename"), env)
                .is_some_and(|value| value.is_truthy())
            && interp
                .lookup_var_key(cached_symbol!("buffer-file-name"), env)
                .is_some_and(|value| value.is_truthy())
    };
    let (point, begv, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point(), buffer.point_min(), buffer.point_max())
    };
    let overlay_calls = if !visit && total > 0 {
        prepare_to_modify_buffer(interp, point, point, false, env)?.overlay_calls
    } else {
        OverlayHookCalls::none()
    };
    with_overlay_hook_roots(interp, &overlay_calls, |interp| {
        let erase = !source.regular && replacing;
        if erase {
            let (begv, zv) = {
                let buffer = interp.buffer.borrow();
                (buffer.point_min(), buffer.point_max())
            };
            crate::lisp::primitives::delete_region_with_hooks(interp, begv, zv, env)?;
        }
        source.check_gap(interp.buffer.borrow().size_total())?;
        // FILENAME, insert-file-contents' first argument.
        let read = source.read(interp, env, operation_args[0], false)?;
        if read.bytes.is_empty() {
            if we_locked_file
                && let Some(truename) =
                    interp.lookup_var_key(cached_symbol!("buffer-file-truename"), env)
            {
                super::call(interp, "unlock-file", &[truename], env)?;
            }
            interp.set_c_variable("deactivate-mark", old_deactivate_mark);
        } else {
            interp.set_c_variable("deactivate-mark", Value::T);
        }
        if let Some(error) = read.error {
            return Err(report_file_errno(
                interp,
                "Read error",
                operation_args[0],
                &error,
            ));
        }
        let bytes = read.bytes;
        let (coding, decided_bytes) = match decision.take() {
            Some(decision) => decision,
            None => decide_file_coding(interp, env, &bytes, literal, path, operation_args)?,
        };
        let FileDecode {
            decoded,
            coding: coding_used,
            for_unibyte,
            charset_props,
            layout,
        } = decode_file_bytes(
            interp,
            env,
            decided_bytes.unwrap_or(bytes),
            coding,
            visit && !replacing,
        )?;
        // A visit with a coding for unibyte text makes the buffer unibyte
        // (not when part of it might be preserved).
        if visit && !replacing && for_unibyte {
            interp.buffer.borrow_mut().set_multibyte(false);
        }
        let mut inserted_chars = decoded.char_count();
        let start = interp.buffer.borrow().point();
        if !decoded.text.is_empty() {
            // decode_coding_gap's decode_coding: the text before point,
            // produce_charset's `charset' runs put through
            // Fput_text_property, a visit's among them.
            crate::lisp::primitives::insert_converted_text_at(
                interp,
                start,
                &decoded.text,
                charset_props.as_ref(),
                &decoded.extended,
                inserted_chars,
                true,
                layout.as_ref(),
                env,
            )?;
            interp.buffer.borrow_mut().goto_char(start);
        }
        // decode_coding_gap's post-read conversion: called with point at
        // the inserted text and its length, with undo off, the text it
        // adds or removes counting as inserted.
        let post_read = coding_attr(
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
                .lookup_var_key(cached_symbol!("buffer-undo-list"), env)
                .unwrap_or(Value::Nil);
            interp.set_variable_key(cached_symbol!("buffer-undo-list"), Value::T, env);
            let result = interp.call_function_value(
                function,
                None,
                &[Value::Integer(inserted_chars as i64)],
                env,
            );
            interp.set_variable_key(cached_symbol!("buffer-undo-list"), undo_list, env);
            let value = result?;
            if value.integer_value().is_none_or(|count| count < 0) {
                return Err(LispError::WrongTypeArgument("wholenump".into(), value));
            }
            let z = interp.buffer.borrow().size_total();
            inserted_chars = (inserted_chars as i64 + z as i64 - prev_z as i64).max(0) as usize;
        }
        // Visiting (without REPLACE) is done in a new buffer "before" it
        // is changed, so no after-change functions run.
        if inserted_chars > 0 && total > 0 && (!visit || replacing) {
            signal_after_change(
                interp,
                AfterChange::new(start, 0, inserted_chars),
                &overlay_calls,
                env,
            )?;
            let point = interp.buffer.borrow().point();
            update_compositions(interp, point, point, CHECK_BORDER, env)?;
        }
        // restore_window_points over the erased accessible portion.
        if erase
            && inserted_chars > 0
            && point > begv
            && point <= zv
            && let Some(Kind::Marker(marker)) = point_marker.map(|marker| marker.kind())
        {
            let growth = inserted_chars as f64 / (zv - begv) as f64;
            let position = (begv as f64 + growth * (point - begv) as f64) as usize;
            let buffer_id = interp.current_buffer_id();
            let _ = interp.set_marker(marker, Some(position), Some(buffer_id));
        }
        Ok((coding_used, inserted_chars))
    })
}

/// Finsert_file_contents's REPLACE of the accessible portion with the
/// regular file's BYTES: the parts at either end that match stay, the
/// middle is deleted and the file's middle inserted.  Returns the coding
/// used and the characters inserted.
fn replace_file_contents(
    interp: &mut Interpreter,
    env: &mut Env,
    bytes: &[u8],
    literal: bool,
    path: &str,
    operation_args: &[Value],
    point_marker: Value,
) -> Result<(String, usize), LispError> {
    let FileDecode {
        decoded,
        coding: coding_used,
        charset_props,
        ..
    } = decode_file_contents(interp, env, bytes, literal, path, operation_args, false)?;
    let mut inserted_chars = decoded.char_count();
    let text = decoded.text;
    let extended = decoded.extended;
    // A REPLACE is one file-read transaction, not an ordinary user edit.
    // GNU dynamically hides `buffer-file-name' across its delete+insert so
    // the generic stale-file edit guard cannot interrupt the operation in
    // the middle and leave an empty buffer.
    let file_name_restore = interp.bind_special_dynamic("buffer-file-name", Value::Nil, env)?;
    let original_point = interp.buffer.borrow().point();
    let edit_result: Result<(), LispError> = (|| {
        // The texts compare by character code, those beyond Unicode
        // included.
        let (old_codes, begv) = {
            let buffer = interp.buffer.borrow();
            let (begv, zv) = (buffer.point_min(), buffer.point_max());
            let old = StringLike {
                text: buffer.buffer_string(),
                props: None,
                multibyte: true,
                extended_chars: buffer.substring_extended_chars(begv, zv),
            };
            (old.character_codes_iter().collect::<Vec<_>>(), begv)
        };
        let new_chars = text.chars().collect::<Vec<_>>();
        let new_codes = StringLike {
            text: text.clone(),
            props: None,
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
        // insert_from_buffer of the conversion buffer's middle, the
        // `charset' runs its decoding put there going with it.
        let replacement_props = match charset_props.as_ref() {
            Some(intervals) => intervals.copy(prefix, new_end - prefix)?,
            None => None,
        };
        let replacement_len = replacement.chars().count();
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
                replacement_props.as_ref(),
                &replacement_extended,
                false,
                false,
                env,
            )?;
        }
        // Point before the text read, as the file's text is inserted;
        // restore_window_points keeps a point that was strictly inside the
        // replaced span at the same relative distance (bug#19161):
        //   newpos = same_at_start + growth * (oldpos - same_at_start)
        // with growth = inserted / (same_at_end - same_at_start), computed
        // in double and truncated, when characters were inserted.
        interp.buffer.borrow_mut().goto_char(start);
        if replacement_len > 0
            && original_point > start
            && original_point <= end
            && let Kind::Marker(marker) = point_marker.kind()
        {
            let growth = replacement_len as f64 / (end - start) as f64;
            let position = (start as f64 + growth * (original_point - start) as f64) as usize;
            let buffer_id = interp.current_buffer_id();
            let _ = interp.set_marker(marker, Some(position), Some(buffer_id));
        }
        Ok(())
    })();
    let restore_result = interp.restore_special_dynamic(file_name_restore, env);
    edit_result?;
    restore_result?;
    Ok((coding_used, inserted_chars))
}

/// A count Finsert_file_contents's format functions return:
/// RANGED_FIXNUMP (0, VALUE, ZV - PT), else (wrong-type-argument
/// inserted-chars VALUE).
fn inserted_chars_value(interp: &Interpreter, value: Value) -> Result<usize, LispError> {
    let room = {
        let buffer = interp.buffer.borrow();
        buffer.point_max().saturating_sub(buffer.point())
    };
    match value.kind() {
        Kind::Integer(count) if count >= 0 && count as usize <= room => Ok(count as usize),
        _ => Err(LispError::WrongTypeArgument("inserted-chars".into(), value)),
    }
}

/// One of Finsert_file_contents's format functions, FUNCTION called
/// with LEADING, the count and TRAILING.  Without REPLACE the count is
/// INSERTED, and becomes what FUNCTION returns; with it FUNCTION runs
/// from BEGV with the accessible size, and point goes back unless the
/// text's characters changed, when the count returned is taken.  For
/// NIL_KEEPS a nil return changes nothing.
#[allow(clippy::too_many_arguments)]
fn call_insert_format_function(
    interp: &mut Interpreter,
    env: &mut Env,
    function: Value,
    name: Option<&str>,
    leading: &[Value],
    trailing: &[Value],
    replace: bool,
    nil_keeps: bool,
    inserted: &mut usize,
) -> Result<(), LispError> {
    let call = |interp: &mut Interpreter, env: &mut Env, count: usize| {
        let mut args = leading.to_vec();
        args.push(Value::Integer(count as i64));
        args.extend_from_slice(trailing);
        interp.call_function_value(function, name, &args, env)
    };
    if !replace {
        let insval = call(interp, env, *inserted)?;
        if !(nil_keeps && insval.is_nil()) {
            *inserted = inserted_chars_value(interp, insval)?;
        }
        return Ok(());
    }
    let (opoint, oinserted, ochars_modiff) = {
        let buffer = interp.buffer.borrow();
        (
            buffer.point(),
            buffer.point_max() - buffer.point_min(),
            buffer.chars_modified_tick(),
        )
    };
    let begv = interp.buffer.borrow().point_min();
    interp.buffer.borrow_mut().goto_char(begv);
    let insval = call(interp, env, oinserted)?;
    if nil_keeps && insval.is_nil() {
        return Ok(());
    }
    let count = inserted_chars_value(interp, insval)?;
    if ochars_modiff == interp.buffer.borrow().chars_modified_tick() {
        interp.buffer.borrow_mut().goto_char(opoint);
    } else {
        *inserted = count;
    }
    Ok(())
}

/// Finsert_file_contents's tail past the insertion, which a file-name
/// handler's shares: `after-insert-file-set-coding', then for inserted
/// text `format-decode' and each of `after-insert-file-functions'
/// (FOR_EACH_TAIL) with point motion and modification hooks inhibited
/// and undo off.  The undo list comes back afterwards with its last
/// insertion record fitted to the new count, or for EMPTY_UNDO_LIST_P
/// empty (t staying t).  Returns the characters inserted.
pub(crate) fn finish_insert_file_contents(
    interp: &mut Interpreter,
    env: &mut Env,
    mut inserted: usize,
    trailing_args: &[Value],
    empty_undo_list_p: bool,
) -> Result<usize, LispError> {
    let visit = trailing_args.first().copied().unwrap_or(Value::Nil);
    let replace = trailing_args.get(3).is_some_and(Value::is_truthy);

    if let Ok(function) =
        interp.lookup_function_key(cached_symbol!("after-insert-file-set-coding"), env)
    {
        let insval = interp.call_function_value(
            function,
            Some("after-insert-file-set-coding"),
            &[Value::Integer(inserted as i64), visit],
            env,
        )?;
        if !insval.is_nil() {
            inserted = inserted_chars_value(interp, insval)?;
        }
    }

    if inserted == 0 {
        return Ok(inserted);
    }
    let Ok(format_decode) = interp.lookup_function_key(cached_symbol!("format-decode"), env) else {
        return Ok(inserted);
    };
    let old_inserted = inserted;
    let motion = interp.bind_special_dynamic("inhibit-point-motion-hooks", Value::T, env)?;
    let modification = interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
    let old_undo = interp.buffer.borrow().undo_list();
    interp.buffer.borrow_mut().set_undo_list(Value::T);
    let result = (|| {
        call_insert_format_function(
            interp,
            env,
            format_decode,
            Some("format-decode"),
            &[Value::Nil],
            &[visit],
            replace,
            false,
            &mut inserted,
        )?;
        let hooks = interp
            .lookup_var_key(cached_symbol!("after-insert-file-functions"), env)
            .unwrap_or(Value::Nil);
        let mut tail = hooks;
        let mut cycle = super::values::ForEachTail::new(tail);
        while let Some((hook, next)) = tail.cons_values() {
            let (function, name) = match hook.kind() {
                Kind::Symbol(symbol) => {
                    (interp.lookup_function_symbol(&symbol, env)?, Some(symbol))
                }
                _ => (hook, None),
            };
            call_insert_format_function(
                interp,
                env,
                function,
                name.as_ref().map(|name| name.as_str()),
                &[],
                &[],
                replace,
                true,
                &mut inserted,
            )?;
            tail = next;
            if cycle.step(tail) {
                return Err(crate::lisp::types::circular_list(tail));
            }
        }
        Ok(())
    })();
    // A signal leaves the undo list t, as no unwinding restores it.
    if result.is_ok() {
        if !empty_undo_list_p {
            interp.buffer.borrow_mut().set_undo_list(old_undo);
            // The last undo record fitted to the size the format
            // conversion left.
            let point = interp.buffer.borrow().point();
            if inserted != old_inserted
                && let Some((record, _)) = old_undo.cons_values()
                && let Some((beg, end)) = record.cons_values()
                && matches!(beg.kind(), Kind::Integer(_))
                && matches!(end.kind(), Kind::Integer(end) if end == (point + old_inserted) as i64)
            {
                record.set_cdr(Value::Integer((point + inserted) as i64))?;
            }
        } else {
            let undo = if matches!(old_undo.kind(), Kind::T) {
                Value::T
            } else {
                Value::Nil
            };
            interp.buffer.borrow_mut().set_undo_list(undo);
        }
    }
    let modification_restore = interp.restore_special_dynamic(modification, env);
    let motion_restore = interp.restore_special_dynamic(motion, env);
    result?;
    modification_restore?;
    motion_restore?;
    Ok(inserted)
}

thread_local! {
    /// fileio.c auto_saving: `write-region' is writing an auto-save file.
    static AUTO_SAVING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// auto_save_mode_bits: the mode an auto-save file is made with.
    static AUTO_SAVE_MODE_BITS: std::cell::Cell<u32> = const { std::cell::Cell::new(0o666) };
    /// auto_save_error_occurred.
    static AUTO_SAVE_ERROR_OCCURRED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// auto_saving_dir_umask: the mode bits `make-directory-internal'
    /// leaves out while the auto-save list's directory is made.
    static AUTO_SAVING_DIR_UMASK: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

pub(crate) fn auto_saving() -> bool {
    AUTO_SAVING.get()
}

pub(crate) fn auto_saving_dir_umask() -> u32 {
    AUTO_SAVING_DIR_UMASK.get()
}

/// fileio.c auto_saving_into_visited_file: an auto-save whose file is
/// the visited one (string-equal of the two names).
fn auto_saving_into_visited_file(interp: &Interpreter) -> bool {
    auto_saving() && {
        let id = interp.current_buffer_id();
        let file = interp.buffer.borrow().file_name_text();
        let auto_save = interp
            .buffer_local_value_key(id, cached_symbol!("buffer-auto-save-file-name"))
            .and_then(|name| string_like(&name).map(|name| name.text));
        file.is_some() && file == auto_save
    }
}

/// fileio.c auto_save_1: the visited file's mode with owner read and
/// write for the auto-save file's, then `write-region' of the whole
/// buffer to it, visiting it only for `auto-save-visited-file-name'.
fn auto_save_1(interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    use std::os::unix::fs::PermissionsExt;
    let mut bits = 0o666;
    let file = interp.buffer.borrow().file_name_text();
    if let Some(file) = file {
        match host_file_name(interp, env, &file)
            .ok()
            .and_then(|host| fs::metadata(host).ok())
        {
            Some(metadata) => bits = (metadata.permissions().mode() | 0o600) & 0o777,
            None => {
                // Remote files don't cooperate with fstatat.
                if let Kind::Integer(modes) = call(
                    interp,
                    "file-modes",
                    &[Value::string(&file), Value::Nil],
                    env,
                )?
                .kind()
                {
                    bits = ((modes as u32) | 0o600) & 0o777;
                }
            }
        }
    }
    AUTO_SAVE_MODE_BITS.set(bits);
    let id = interp.current_buffer_id();
    let name = interp
        .buffer_local_value_key(id, cached_symbol!("buffer-auto-save-file-name"))
        .unwrap_or(Value::Nil);
    let visit = if interp
        .lookup_var_key(cached_symbol!("auto-save-visited-file-name"), env)
        .is_some_and(|value| value.is_truthy())
    {
        Value::T
    } else {
        Value::symbol("lambda")
    };
    call(
        interp,
        "write-region",
        &[
            Value::Nil,
            Value::Nil,
            name,
            Value::Nil,
            visit,
            Value::Nil,
            Value::Nil,
        ],
        env,
    )
}

/// fileio.c auto_save_error: the bell, and a warning naming the buffer
/// and the error.
fn auto_save_error(interp: &mut Interpreter, env: &mut Env, error: &LispError) {
    AUTO_SAVE_ERROR_OCCURRED.set(true);
    let _ = super::dispatch::display::ring_bell(interp, env);
    let condition = crate::lisp::eval::error_condition_value(error);
    let name = interp.buffer.borrow().name();
    let message = call(interp, "error-message-string", &[condition], env).and_then(|text| {
        call(
            interp,
            "format",
            &[Value::string("Auto-saving %s: %s"), name, text],
            env,
        )
    });
    if let Ok(message) = message {
        let _ = call_named_function(
            interp,
            "display-warning",
            &[Value::symbol("auto-save"), message, Value::symbol(":error")],
            env,
        );
    }
}

/// fileio.c:Fdo_auto_save.  `auto-save-hook' first, then the list of
/// auto-saving buffers written to `auto-save-list-file-name' (its
/// directory made private when missing), then each live buffer that is
/// not indirect (only the current one for CURRENT-ONLY) auto-saved when
/// it has an auto-save file name, changed since its last real save and
/// its last auto-save, and has not turned auto-saving off -- those whose
/// auto-save file has no handler on the first pass, the others on the
/// second.  A failed or slow (over a minute) save waits twenty minutes;
/// a buffer much shorter than when last saved is reported and stops
/// auto-saving until saved.  An auto-save error is a warning.
pub(crate) fn do_auto_save(
    interp: &mut Interpreter,
    env: &mut Env,
    no_message: Value,
    current_only: Value,
) -> Result<Value, LispError> {
    let old = interp.current_buffer_id();
    let no_message = no_message.is_truthy() || interp.minibuffer_depth() > 0;
    // push_message: the echo area's message, put back at the end.
    let old_message = if no_message {
        Value::Nil
    } else {
        call(interp, "current-message", &[], env)?
    };
    let old_message_p = old_message.is_string();
    // Ordinarily don't quit within this function.
    let oquit = interp
        .lookup_var_key(cached_symbol!("quit-flag"), env)
        .unwrap_or(Value::Nil);
    interp.set_variable_key(cached_symbol!("quit-flag"), Value::Nil, env);
    crate::lisp::primitives::safe_run_hooks(
        interp,
        &cached_symbol!("auto-save-hook").symbol(),
        env,
    )?;
    let mut stream = None;
    if let Some(list_file) = interp
        .lookup_var_key(cached_symbol!("auto-save-list-file-name"), env)
        .filter(Value::is_string)
    {
        let list_file = call(interp, "expand-file-name", &[list_file, Value::Nil], env)?;
        let dir = call(interp, "file-name-directory", &[list_file], env)?;
        // Not when shutting down (Vrun_hooks cleared): an error there would
        // leave Emacs in a strange state.
        if !interp.hooks_cleared && !call(interp, "file-directory-p", &[dir], env)?.is_truthy() {
            // do_auto_save_make_dir under internal_condition_case_1:
            // private, and any error ignored.
            AUTO_SAVING_DIR_UMASK.set(0o077);
            let handlers =
                interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
            let made = call_named_function(interp, "make-directory", &[dir, Value::T], env);
            interp.pop_handler_bindings(handlers);
            AUTO_SAVING_DIR_UMASK.set(0);
            if let Err(error) = made
                && matches!(
                    error.kind(),
                    LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
                )
            {
                return Err(error);
            }
        }
        stream = host_file_name(interp, env, &string_text(&list_file)?)
            .ok()
            .and_then(|host| fs::File::create(host).ok());
    }
    let auto_raise = interp
        .lookup_var_key(cached_symbol!("minibuffer-auto-raise"), env)
        .unwrap_or(Value::Nil);
    interp.set_variable_key(cached_symbol!("minibuffer-auto-raise"), Value::Nil, env);
    AUTO_SAVING.set(true);
    AUTO_SAVE_ERROR_OCCURRED.set(false);
    let result = (|| -> Result<bool, LispError> {
        let mut auto_saved = false;
        for do_handled_files in [false, true] {
            let ids = interp
                .buffer_list
                .iter()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            for id in ids {
                let Some(buffer) = interp.buffer_object(id) else {
                    continue;
                };
                let auto_save_name = interp
                    .buffer_local_value_key(id, cached_symbol!("buffer-auto-save-file-name"))
                    .filter(Value::is_string);
                // The buffers that auto-save, by visited and auto-save
                // file names, in the special file that lists them.
                if let (Some(name), Some(stream), false) =
                    (&auto_save_name, stream.as_mut(), do_handled_files)
                {
                    let file = buffer.borrow().file_name_text().unwrap_or_default();
                    let name = string_text(name)?;
                    let _ = writeln!(stream, "{file}\n{name}");
                }
                if current_only.is_truthy() && id != interp.current_buffer_id() {
                    continue;
                }
                // Don't auto-save indirect buffers; the base buffer
                // takes care of it.
                if interp.buffer_base_id(id).is_some() {
                    continue;
                }
                let Some(auto_save_name) = auto_save_name else {
                    continue;
                };
                let save_length =
                    interp.buffer_local_value_key(id, cached_symbol!("buffer-saved-size"));
                let (changed, failure_time) = {
                    let state = buffer.borrow();
                    (state.changed_since_saves(), state.auto_save_failure_time)
                };
                let save_length = match save_length.map(|value| value.kind()) {
                    Some(Kind::Integer(length)) if length >= 0 => length,
                    _ => continue,
                };
                if !changed
                    || (!do_handled_files
                        && !call(
                            interp,
                            "find-file-name-handler",
                            &[auto_save_name, Value::symbol("write-region")],
                            env,
                        )?
                        .is_nil())
                {
                    continue;
                }
                let before_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |time| time.as_secs() as i64);
                // If we had a failure, don't try again for 20 minutes.
                if failure_time > 0 && before_time - failure_time < 1200 {
                    continue;
                }
                const GROWTH_FACTOR: i64 = 4;
                interp.set_current_buffer_id(id)?;
                let size = interp.buffer.borrow().size_total() as i64;
                if interp
                    .lookup_var_key(cached_symbol!("auto-save-include-big-deletions"), env)
                    .is_none_or(|value| value.is_nil())
                    && save_length > 5000
                    && GROWTH_FACTOR * size < (GROWTH_FACTOR - 1) * save_length
                    && buffer.borrow().visits_file()
                    && !no_message
                {
                    // It has shrunk too much; turn off auto-saving here.
                    interp.set_variable_key(
                        cached_symbol!("minibuffer-auto-raise"),
                        auto_raise,
                        env,
                    );
                    let name = buffer.borrow().name();
                    super::dispatch::display::message_with_string(
                        interp,
                        "Buffer %s has shrunk a lot; auto save disabled in that buffer until next real save",
                        name,
                        env,
                    )?;
                    interp.set_variable_key(
                        cached_symbol!("minibuffer-auto-raise"),
                        Value::Nil,
                        env,
                    );
                    interp.set_buffer_local_value_symbol(
                        id,
                        &cached_symbol!("buffer-saved-size").with(Clone::clone),
                        Value::Integer(-1),
                    );
                    call(interp, "sleep-for", &[Value::Integer(1), Value::Nil], env)?;
                    continue;
                }
                if !auto_saved && !no_message {
                    super::dispatch::display::message1(interp, "Auto-saving...", env)?;
                }
                let handlers =
                    interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
                let saved = auto_save_1(interp, env);
                interp.pop_handler_bindings(handlers);
                if let Err(error) = saved {
                    if matches!(
                        error.kind(),
                        LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
                    ) {
                        return Err(error);
                    }
                    auto_save_error(interp, env, &error);
                }
                auto_saved = true;
                buffer.borrow_mut().set_autosaved();
                let current = interp.current_buffer_id();
                let size = interp.buffer.borrow().size_total() as i64;
                interp.set_buffer_local_value_symbol(
                    current,
                    &cached_symbol!("buffer-saved-size").with(Clone::clone),
                    Value::Integer(size),
                );
                if interp.buffer_object(old).is_some() {
                    interp.set_current_buffer_id(old)?;
                }
                // If auto-save took more than 60 seconds, assume it was an
                // NFS failure that got a timeout.
                let after_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |time| time.as_secs() as i64);
                if after_time - before_time > 60 {
                    buffer.borrow_mut().auto_save_failure_time = after_time;
                }
            }
        }
        Ok(auto_saved)
    })();
    // do_auto_save_unwind.
    interp.set_variable_key(cached_symbol!("minibuffer-auto-raise"), auto_raise, env);
    AUTO_SAVING.set(false);
    drop(stream);
    let auto_saved = result?;
    if auto_saved && !no_message {
        if old_message_p {
            // If we are going to restore an old message, give time to
            // read ours.
            call_named_function(interp, "sit-for", &[Value::Integer(1)], env)?;
            // restore_message: message3_nolog of the old message.
            super::dispatch::display::set_echo_area_message_value(old_message)?;
        } else if !AUTO_SAVE_ERROR_OCCURRED.get() {
            super::dispatch::display::message1(interp, "Auto-saving...done", env)?;
        }
    }
    interp.set_variable_key(cached_symbol!("quit-flag"), oquit, env);
    Ok(Value::Nil)
}
