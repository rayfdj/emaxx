use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

/// Child-side setup shared by every Lisp-visible subprocess, mirroring GNU's
/// `emacs_spawn' (callproc.c:1441) -- the single choke point both
/// `call-process' and `make-process' children pass through.  Every child
/// gets, exactly as in GNU:
///
/// * Default signal dispositions for SIGINT, SIGQUIT, SIGPROF and (on
///   Darwin) SIGCHLD -- callproc.c:1385's sigdefault set.  SIG_IGN survives
///   exec, so without this a child of an Emaxx started as a background job
///   (where the shell ignores SIGINT/SIGQUIT) could never be interrupted.
///   Rust's `Command' resets the signal *mask* but only the SIGPIPE
///   *disposition*.
/// * A fresh session: POSIX_SPAWN_SETSID in GNU's non-pty path, `setsid' in
///   its fork path (callproc.c:1289).  A PTY child additionally acquires
///   stdin's slave as its controlling terminal.
#[cfg(unix)]
fn configure_emacs_spawn(command: &mut Command, controlling_pty: bool) {
    use std::os::unix::process::CommandExt;
    // SAFETY: signal/setsid/ioctl are async-signal-safe child-side calls;
    // fd 0 is already the PTY slave when CONTROLLING_PTY.
    unsafe {
        command.pre_exec(move || {
            libc::signal(libc::SIGINT, libc::SIG_DFL);
            libc::signal(libc::SIGQUIT, libc::SIG_DFL);
            libc::signal(libc::SIGPROF, libc::SIG_DFL);
            #[cfg(target_os = "macos")]
            libc::signal(libc::SIGCHLD, libc::SIG_DFL);
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            if controlling_pty
                && libc::ioctl(libc::STDIN_FILENO, libc::c_ulong::from(libc::TIOCSCTTY), 0) < 0
            {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

/// process.c's child_signal_read_fd: the pipe handle_child_signal writes a
/// byte to, which wait_reading_process_output selects on beside the
/// processes' channels, so a SIGCHLD arriving before or during the select
/// ends the wait.  catch_child_signal installs the handler before the
/// first child is made; the first spawn does it here (None when the pipe
/// or the handler cannot be had, leaving the waits to their timeouts).
#[cfg(unix)]
pub(crate) fn child_signal_read_fd() -> Option<std::os::fd::RawFd> {
    static CHILD_SIGNAL_READ_FD: OnceLock<Option<std::os::fd::RawFd>> = OnceLock::new();
    *CHILD_SIGNAL_READ_FD.get_or_init(|| {
        use std::os::fd::{FromRawFd, OwnedFd};
        let mut fds = [-1; 2];
        // SAFETY: PIPE writes exactly two descriptors to FDS, each owned at
        // once below.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
            return None;
        }
        let reader = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        let writer = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        for fd in fds {
            // SAFETY: FD is one of the two live descriptors above.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            if flags == -1
                || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
                || unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1
            {
                return None;
            }
        }
        // signal-hook's pipe handler is child_signal_notify: one
        // non-blocking write, its errors ignored.
        signal_hook::low_level::pipe::register(libc::SIGCHLD, writer).ok()?;
        Some(std::os::fd::IntoRawFd::into_raw_fd(reader))
    })
}

/// process.c:child_signal_read: empty the child signal pipe once the wait
/// has seen it.
#[cfg(unix)]
pub(crate) fn drain_child_signal_pipe(fd: std::os::fd::RawFd) {
    let mut bytes = [0u8; 64];
    // SAFETY: FD is the live, non-blocking read end of the pipe.
    while unsafe { libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len()) } == bytes.len() as isize
    {
    }
}

/// callproc.c Fcall_process and process.c Fmake_process both locate the
/// program with openp over `exec-path' and `exec-suffixes' asking for X_OK,
/// and report a miss as "Searching for program" with the errno openp kept.
/// `make-process' skips the search for a name that starts with a directory
/// separator (a directory there is an error) and expands what openp found;
/// `call-process' always searches, openp trying an absolute name as itself,
/// and keeps openp's answer.  Both strip a "/:" quote from the result.
pub(crate) fn locate_program_for_exec(
    interp: &mut Interpreter,
    env: &mut Env,
    program: &str,
    make_process: bool,
) -> Result<String, LispError> {
    if make_process && program.starts_with('/') {
        if fs::metadata(host_file_name(interp, env, program)?)
            .is_ok_and(|metadata| metadata.is_dir())
        {
            return Err(LispError::Signal(
                "Specified program for new process is a directory".into(),
            ));
        }
        return Ok(unquote_local_file_name(program).unwrap_or_else(|| program.to_string()));
    }
    let exec_path = interp.lookup_var("exec-path", env).unwrap_or(Value::Nil);
    let exec_suffixes = interp
        .lookup_var("exec-suffixes", env)
        .unwrap_or(Value::Nil);
    let (found, errno) = locate_file_search(
        interp,
        &Value::String(program.into()),
        &exec_path,
        &exec_suffixes,
        &Value::Integer(i64::from(libc::X_OK)),
        env,
    )?;
    if found.is_nil() {
        return Err(LispError::SignalValue(file_errno_error_value(
            "Searching for program",
            errno,
            program,
        )));
    }
    let found = string_text(&found)?;
    let found = unquote_local_file_name(&found).unwrap_or(found);
    Ok(if make_process {
        expand_file_name_in_env(interp, env, &found, None)
    } else {
        found
    })
}

/// A spawn failure is what GNU's parent sees from `emacs_spawn' and reports
/// as "Doing vfork" without naming a file.
fn vfork_error(error: &std::io::Error) -> LispError {
    LispError::SignalValue(file_operation_error_value_without_file(
        "Doing vfork",
        error,
    ))
}

/// The child's arguments on COMMAND as the C strings it reads: each one's
/// bytes up to its first NUL.
fn add_command_arguments(command: &mut Command, argv: &[Vec<u8>]) {
    let arguments = argv.iter().map(|arg| c_string_bytes(arg));
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        command.args(arguments.map(std::ffi::OsStr::from_bytes));
    }
    #[cfg(not(unix))]
    command.args(arguments.map(|arg| String::from_utf8_lossy(arg).into_owned()));
}

/// Everything the child-side exec needs, prepared before the fork so the
/// child touches no allocator.
#[cfg(unix)]
struct ExecImage {
    argv: Vec<*const libc::c_char>,
    envp: Vec<*const libc::c_char>,
    _strings: Vec<std::ffi::CString>,
    /// "<emacs argv[0]>: <program>: " for the vfork-path diagnostic.
    #[cfg(not(target_os = "macos"))]
    prefix: Vec<u8>,
}

// SAFETY: the pointers refer into `_strings', which the image owns, and the
// image is only ever used from the forked child.
#[cfg(unix)]
unsafe impl Send for ExecImage {}
#[cfg(unix)]
unsafe impl Sync for ExecImage {}

/// GNU's child runs `execve' (callproc.c emacs_exec_file), so a file the
/// kernel refuses with ENOEXEC is not retried through a shell the way
/// glibc's `execvp' does.  When exec fails on the vfork path used for
/// pseudo-terminals the child prints "<emacs argv[0]>: <program>:
/// <strerror>" on its stderr and `_exit's with EXIT_ENOENT (127) or
/// EXIT_CANNOT_INVOKE (126); on the posix_spawn path used without a pty the
/// errno goes back to the parent, which signals "Doing vfork".  Rust's
/// `Command' would `execvp' after this hook, so the hook execs first and,
/// on failure, either exits like GNU's child or hands Rust the errno to
/// report.  The pinned Darwin oracle preserves the exit status but does not
/// deliver the child-side diagnostic through the process PTY, so the macOS
/// path mirrors that observable contract.
#[cfg(unix)]
fn exec_like_gnu(
    command: &mut Command,
    program: &std::ffi::OsStr,
    argv: &[Vec<u8>],
    environment: Vec<Vec<u8>>,
    pty: bool,
) {
    use std::os::unix::process::CommandExt;

    let Some(image) = ExecImage::new(program, argv, environment) else {
        return;
    };

    // SAFETY: the child only calls execve, write, strerror and _exit on data
    // owned by `image', which the closure owns whole; GNU's own vfork child
    // makes the same strerror call.
    unsafe {
        command.pre_exec(move || image.exec_in_child(pty));
    }
}

#[cfg(unix)]
impl ExecImage {
    /// PROGRAM, ARGV and the environment ENVIRONMENT as execve takes them;
    /// None when one holds a NUL.
    fn new(program: &std::ffi::OsStr, argv: &[Vec<u8>], environment: Vec<Vec<u8>>) -> Option<Self> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let mut strings = Vec::with_capacity(argv.len() + 1);
        strings.push(CString::new(program.as_bytes()).ok()?);
        for arg in argv {
            strings.push(CString::new(c_string_bytes(arg)).ok()?);
        }
        let argv_end = strings.len();
        for entry in environment {
            // make_environment_block cut every entry at its first NUL.
            strings.push(CString::new(entry).ok()?);
        }
        let pointers = |range: std::ops::Range<usize>| {
            strings[range]
                .iter()
                .map(|string| string.as_ptr())
                .chain(std::iter::once(std::ptr::null()))
                .collect::<Vec<_>>()
        };
        #[cfg(not(target_os = "macos"))]
        let prefix = {
            let mut prefix = std::env::args_os()
                .next()
                .map(|argv0| argv0.as_bytes().to_vec())
                .filter(|argv0| !argv0.is_empty())
                .unwrap_or_else(|| b"emacs".to_vec());
            prefix.extend_from_slice(b": ");
            prefix.extend_from_slice(program.as_bytes());
            prefix.extend_from_slice(b": ");
            prefix
        };
        Some(ExecImage {
            argv: pointers(0..argv_end),
            envp: pointers(argv_end..strings.len()),
            _strings: strings,
            #[cfg(not(target_os = "macos"))]
            prefix,
        })
    }

    /// The child side of `exec_like_gnu'.
    ///
    /// # Safety
    /// Runs in the forked child between fork and exec.
    unsafe fn exec_in_child(&self, pty: bool) -> std::io::Result<()> {
        // SAFETY: argv and envp are NULL-terminated arrays of NUL-terminated
        // strings owned by `self'; the failure path writes owned bytes and
        // `strerror's static text.
        unsafe {
            libc::execve(self.argv[0], self.argv.as_ptr(), self.envp.as_ptr());
            let error = std::io::Error::last_os_error();
            if !pty {
                return Err(error);
            }
            let errno = error.raw_os_error().unwrap_or(0);
            let status = if errno == libc::ENOENT { 127 } else { 126 };
            #[cfg(not(target_os = "macos"))]
            {
                let text = libc::strerror(errno);
                libc::write(
                    libc::STDERR_FILENO,
                    self.prefix.as_ptr().cast(),
                    self.prefix.len(),
                );
                libc::write(libc::STDERR_FILENO, text.cast(), libc::strlen(text));
                libc::write(libc::STDERR_FILENO, b"\n".as_ptr().cast(), 1);
            }
            libc::_exit(status)
        }
    }
}

/// callproc.c call_process's reading of DESTINATION.  A list whose car
/// is not `:file' is (REAL-BUFFER STDERR-FILE): STDERR-FILE (when the
/// list has one) nil discards the child's stderr, t mixes it with the
/// output and a string names a file; REAL-BUFFER is read on.  A
/// `(:file FILE)' then sends the output to FILE.
pub(crate) struct ProcessDestination {
    pub(crate) buffer: Value,
    pub(crate) output_file: Option<Value>,
    pub(crate) error_file: Value,
}

pub(crate) fn process_destination(destination: &Value) -> ProcessDestination {
    let file_key = Value::Symbol(":file".into());
    let mut buffer = *destination;
    let mut error_file = Value::T;
    if let Some((head, tail)) = buffer.cons_values()
        && head != file_key
    {
        if let Some((stderr_file, _)) = tail.cons_values() {
            error_file = stderr_file;
        }
        buffer = head;
    }
    let mut output_file = None;
    if let Some((head, tail)) = buffer.cons_values()
        && head == file_key
    {
        output_file = Some(tail.cons_values().map_or(tail, |(file, _)| file));
        buffer = Value::Nil;
    }
    ProcessDestination {
        buffer,
        output_file,
        error_file,
    }
}

/// callproc.c returns an exit code or the host's signal description.
pub(crate) fn call_process_status(status: &std::process::ExitStatus) -> Value {
    if let Some(code) = status.code() {
        return Value::Integer(i64::from(code));
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            // SAFETY: strsignal returns a NUL-terminated static description;
            // copy it immediately, as GNU does before returning to Lisp.
            let description = unsafe {
                let pointer = libc::strsignal(signal);
                if pointer.is_null() {
                    format!("Unknown signal {signal}")
                } else {
                    std::ffi::CStr::from_ptr(pointer)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            return Value::string(&description);
        }
    }
    Value::Integer(1)
}

/// coding.c preferred_coding_system: the coding system standing for the
/// category first in detection priority.
fn preferred_coding_system(interp: &Interpreter) -> Value {
    interp
        .coding_category_priorities
        .first()
        .and_then(|&category| interp.coding_category_representatives.get(category))
        .and_then(Option::as_deref)
        .map_or(Value::Nil, Value::symbol)
}

/// coding.c complement_process_encoding_system: CODING with the text
/// conversion and the eol type it leaves open taken from the cdr of
/// `default-process-coding-system', then from the preferred coding
/// system.
pub(crate) fn complement_process_encoding_system(
    interp: &Interpreter,
    env: &Env,
    coding: Value,
) -> Result<Value, LispError> {
    let default = interp
        .lookup_var("default-process-coding-system", env)
        .and_then(|value| value.cons_values())
        .map_or(Value::Nil, |(_, encoding)| encoding);
    let candidates = [coding, default, preferred_coding_system(interp)];
    let mut coding_base = Value::Nil;
    let mut eol_base = Value::Nil;
    let mut complete_at = candidates.len();
    for (index, candidate) in candidates.iter().enumerate() {
        let Some(name) = candidate.symbol_str().filter(|_| !candidate.is_nil()) else {
            continue;
        };
        let Some(canonical) = interp.coding_system_canonical_name(name) else {
            continue;
        };
        if coding_base.is_nil()
            && interp.coding_system_kind_name(&canonical).as_deref() != Some("undecided")
        {
            coding_base = interp
                .coding_system_base_name(name)
                .map_or(Value::Nil, |base| Value::symbol(&base));
        }
        if eol_base.is_nil() && interp.coding_system_eol_type_value(name).is_some() {
            eol_base = *candidate;
        }
        if !coding_base.is_nil() && !eol_base.is_nil() {
            complete_at = index;
            break;
        }
    }
    if complete_at == 0 {
        return Ok(coding);
    }
    super::file_io::inherit_write_coding_eol(interp, coding_base, eol_base)
}

/// process.c send_process's encoding of TEXT (a string, or a buffer
/// region's text with MULTIBYTE the buffer's) for PROCESS_ID, after
/// send_process's status check.  `last-coding-system-used' becomes the
/// coding the output coding struct is set up with.  Multibyte text sends
/// with the process's encoding completed by
/// complement_process_encoding_system (set up afresh when the struct had
/// another); unibyte text with the raw-text form of the struct's coding
/// when that requires encoding (a coding that requires flushing ends its
/// last block first).  A struct that requires encoding encodes the text
/// as a block that is not the last; any other sends its bytes.
pub(crate) fn encode_process_input(
    interp: &mut Interpreter,
    process_id: u64,
    text: &Value,
    multibyte: bool,
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    interp.process_send_check(process_id)?;
    let encoding = interp
        .process_coding_system(process_id)?
        .cons_values()
        .map_or(Value::Nil, |(_, encoding)| encoding);
    let mut encoder = interp.take_process_encoder(process_id);
    let result = encode_process_input_with(
        interp,
        process_id,
        text,
        multibyte,
        encoding,
        &mut encoder,
        env,
    );
    interp.restore_process_encoder(process_id, encoder);
    result
}

fn encode_process_input_with(
    interp: &mut Interpreter,
    process_id: u64,
    text: &Value,
    multibyte: bool,
    encoding: Value,
    encoder: &mut super::coding::EncoderSpec,
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    use super::coding::{EncoderSpec, coding_requires_encoding, coding_requires_flushing};
    let name = |coding: Value| {
        if coding.is_nil() {
            Value::symbol("undecided")
        } else {
            coding
        }
    };
    // Vlast_coding_system_used = CODING_ID_NAME (coding->id).
    let mut coding = name(encoder.coding.unwrap_or(encoding));
    let mut last_used = coding;
    if multibyte {
        let complemented = name(complement_process_encoding_system(interp, env, encoding)?);
        interp.set_process_encoding(process_id, complemented);
        if coding.symbol_str() != complemented.symbol_str() {
            *encoder = EncoderSpec::default();
            coding = complemented;
            last_used = complemented;
        }
        encoder.coding = Some(coding);
    } else if coding_requires_encoding(interp, &coding, env) {
        let symbol = coding.as_symbol()?.to_string();
        if coding_requires_flushing(interp, &symbol) {
            // The flush, send_process (proc, "", 0, Qt): its object is
            // neither a buffer nor a string, so nothing is encoded or
            // sent; only the multibyte arm's complement takes effect.
            let complemented = name(complement_process_encoding_system(interp, env, encoding)?);
            interp.set_process_encoding(process_id, complemented);
            if last_used.symbol_str() != complemented.symbol_str() {
                last_used = complemented;
            }
        }
        let raw_text = raw_text_coding_system(interp, last_used.as_symbol()?);
        *encoder = EncoderSpec::default();
        encoder.coding = Some(Value::symbol(&raw_text));
    }
    interp.set_variable("last-coding-system-used", last_used, env);
    send_process_bytes(interp, text, encoder, env)
}

/// send_process after the coding struct is set up: TEXT encoded by it
/// when it requires encoding, else TEXT's bytes.
fn send_process_bytes(
    interp: &mut Interpreter,
    text: &Value,
    encoder: &mut super::coding::EncoderSpec,
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    let coding = encoder.coding.unwrap_or(Value::Nil);
    if super::coding::coding_requires_encoding(interp, &coding, env) {
        let symbol = coding.as_symbol()?.to_string();
        super::coding::encode_process_block(interp, text, &symbol, encoder, env)
    } else {
        let string = string_like(text)
            .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), *text))?;
        super::text::internal_string_bytes(&string)
    }
}

/// Fprocess_send_eof's flush of an output coding that requires flushing:
/// send_process of an empty unibyte text, which sets up raw-text and
/// sends nothing.
pub(crate) fn flush_process_encoder(
    interp: &mut Interpreter,
    process_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    interp.process_send_check(process_id)?;
    let encoding = interp
        .process_coding_system(process_id)?
        .cons_values()
        .map_or(Value::Nil, |(_, encoding)| encoding);
    let mut encoder = interp.take_process_encoder(process_id);
    let coding = encoder.coding.unwrap_or(encoding);
    let result = if coding.symbol_str().is_some_and(|symbol| {
        !coding.is_nil() && super::coding::coding_requires_flushing(interp, symbol)
    }) {
        encode_process_input_with(
            interp,
            process_id,
            &Value::string(""),
            false,
            encoding,
            &mut encoder,
            env,
        )
        .map(drop)
    } else {
        Ok(())
    };
    interp.restore_process_encoder(process_id, encoder);
    result
}

/// callproc.c call_process's arguments for the child: ARGUMENTS (each a
/// string) encoded by the coding `coding-system-for-write' names, else,
/// when one of them is multibyte, the cdr of what find-operation-coding-
/// system answers for `call-process' with OPERATION_ARGS, else raw-text;
/// complemented by complement_process_encoding_system and made raw-text
/// when not ASCII compatible.  CODING_REQUIRE_ENCODING holds for every
/// multibyte argument, and the encode_coding_string macro passes a
/// unibyte one through as its bytes.
pub(crate) fn call_process_arguments(
    interp: &mut Interpreter,
    env: &mut Env,
    operation_args: &[Value],
    arguments: &[Value],
) -> Result<Vec<Vec<u8>>, LispError> {
    if arguments.is_empty() {
        return Ok(Vec::new());
    }
    let mut must_encode = false;
    for argument in arguments {
        let Kind::StringObject(state) = argument.kind() else {
            return Err(LispError::WrongTypeArgument("stringp".into(), *argument));
        };
        must_encode |= state.borrow().is_multibyte();
    }
    let coding_for_write = interp
        .lookup_var("coding-system-for-write", env)
        .unwrap_or(Value::Nil);
    // Unibyte arguments alone pass through as they are, whatever raw-text
    // completes to.
    if coding_for_write.is_nil() && !must_encode {
        return arguments
            .iter()
            .map(|argument| match argument.kind() {
                Kind::StringObject(state) => Ok(state.borrow().bytes().to_vec()),
                _ => Err(LispError::WrongTypeArgument("stringp".into(), *argument)),
            })
            .collect();
    }
    let coding = if !coding_for_write.is_nil() {
        coding_for_write
    } else if !must_encode {
        Value::symbol("raw-text")
    } else {
        let mut lookup = Vec::with_capacity(operation_args.len() + 1);
        lookup.push(Value::symbol("call-process"));
        lookup.extend_from_slice(operation_args);
        find_operation_coding_system_value(interp, &lookup, env)?
            .cons_values()
            .map_or(Value::Nil, |(_, encoding)| encoding)
    };
    let coding = complement_process_encoding_system(interp, env, coding)?;
    // setup_coding_system reads nil as `undecided'.
    let mut coding = if coding.is_nil() {
        "undecided".to_string()
    } else {
        checked_coding_symbol(interp, &coding)?
    };
    let canonical = interp
        .coding_system_canonical_name(&coding)
        .unwrap_or_else(|| coding.clone());
    if !coding_system_is_ascii_compatible(interp, &canonical) {
        coding = raw_text_coding_system(interp, &coding);
    }
    arguments
        .iter()
        .map(|argument| {
            let multibyte = matches!(argument.kind(), Kind::StringObject(state)
                if state.borrow().is_multibyte());
            let argument = if multibyte {
                encode_coding_value_recording(interp, argument, Some(&coding), true, false, env)?
            } else {
                *argument
            };
            match argument.kind() {
                Kind::StringObject(state) => Ok(state.borrow().bytes().to_vec()),
                _ => Err(LispError::WrongTypeArgument("stringp".into(), argument)),
            }
        })
        .collect()
}

/// callproc.c's NULL_DEVICE.
#[cfg(not(windows))]
pub(crate) const NULL_DEVICE: &str = "/dev/null";
#[cfg(windows)]
pub(crate) const NULL_DEVICE: &str = "NUL";

/// The file create_temp_file made for `call-process-region', removed as
/// delete_temp_file removes it: when the call is over, or for a
/// DESTINATION of 0 once the child reading it has exited.
pub(crate) struct CallProcessTempFile(std::path::PathBuf);

impl Drop for CallProcessTempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// callproc.c create_temp_file's file, made from "emacsXXXXXX" in
/// `temporary-file-directory' (else $TMPDIR, else "/tmp/") and opened
/// close-on-exec, with its name.
pub(crate) fn create_temp_file(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(fs::File, String, CallProcessTempFile), LispError> {
    let tmpdir = match interp
        .lookup_var("temporary-file-directory", env)
        .and_then(|value| string_like(&value))
    {
        Some(directory) => directory.text,
        None => std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp/".into()),
    };
    let pattern = expand_file_name_in_env(interp, env, "emacsXXXXXX", Some(&tmpdir));
    let failed = |error: &std::io::Error| {
        file_operation_error(
            "Failed to open temporary file using pattern",
            error,
            &pattern,
        )
    };
    #[cfg(unix)]
    {
        use std::os::fd::FromRawFd;
        let mut template = std::ffi::CString::new(pattern.as_str())
            .map_err(|_| failed(&std::io::Error::from(ErrorKind::InvalidInput)))?
            .into_bytes_with_nul();
        // SAFETY: TEMPLATE is a writable NUL-terminated buffer mkostemp
        // fills in place; the descriptor it returns is owned at once.
        let fd = unsafe { libc::mkostemp(template.as_mut_ptr().cast(), libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(failed(&std::io::Error::last_os_error()));
        }
        // SAFETY: a fresh descriptor, owned by this File alone.
        let file = unsafe { fs::File::from_raw_fd(fd) };
        template.pop();
        let name = String::from_utf8_lossy(&template).into_owned();
        let guard = CallProcessTempFile(name.clone().into());
        Ok((file, name, guard))
    }
    #[cfg(not(unix))]
    {
        static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        loop {
            let serial = SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let suffix = format!(
                "{:06}",
                (u64::from(std::process::id()) * 1000 + serial) % 1_000_000
            );
            let name = format!("{}{suffix}", &pattern[..pattern.len() - 6]);
            match fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&name)
            {
                Ok(file) => {
                    let guard = CallProcessTempFile(name.clone().into());
                    return Ok((file, name, guard));
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(failed(&error)),
            }
        }
    }
}

/// A file call_process opens for the child to write: O_WRONLY | O_CREAT
/// | O_TRUNC with default_output_mode, close-on-exec.
fn open_child_output_file(path: &Path) -> std::io::Result<fs::File> {
    fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
}

/// callproc.c call_process, for `call-process' and `call-process-region':
/// ARGS are call-process's own (PROGRAM INFILE DESTINATION DISPLAY
/// ARGS...), FILEFD the child's standard input, TEMPFILE the file
/// `call-process-region' wrote that input to.  The parent opens every
/// descriptor the child gets: the null device, for writing, for output
/// that is discarded (so a policy forbidding that stops Emacs, not the
/// child), `(:file FILE)''s file, else a pipe read into the buffer;
/// standard error goes with the output unless STDERR-FILE names a file,
/// or is nil for output that is kept, which opens the null device.  A
/// DESTINATION of 0 returns nil at once, the child reaped, and TEMPFILE
/// removed, whenever it exits.
pub(crate) fn call_process(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
    filefd: fs::File,
    tempfile: Option<CallProcessTempFile>,
) -> Result<Value, LispError> {
    #[cfg(test)]
    crate::test_support::mark_process_test();

    let program = string_text(&args[0])?;
    let argv = call_process_arguments(interp, env, args, args.get(4..).unwrap_or(&[]))?;
    let ProcessDestination {
        mut buffer,
        output_file,
        error_file,
    } = process_destination(args.get(2).unwrap_or(&Value::Nil));
    let error_path = if error_file.is_nil() || error_file == Value::T {
        None
    } else {
        Some(resolve_file_name_in_env(
            interp,
            env,
            &string_text(&error_file)?,
        ))
    };
    let output_path = match output_file {
        Some(file) => Some(resolve_file_name_in_env(interp, env, &string_text(&file)?)),
        None => None,
    };
    let no_wait = matches!(buffer.kind(), Kind::Integer(_));
    if !(buffer.is_nil() || buffer == Value::T || no_wait) {
        buffer = call(interp, "get-buffer-create", &[buffer], env)?;
    }
    let directory = process_current_directory(interp, env)?;
    let program = locate_program_for_exec(interp, env, &program, false)?;
    let program = host_file_name(interp, env, &program)?.into_os_string();
    let discard_output = no_wait || (buffer.is_nil() && output_path.is_none());
    let (output_read, output) = if discard_output {
        let output = fs::OpenOptions::new()
            .write(true)
            .open(NULL_DEVICE)
            .map_err(|error| {
                LispError::SignalValue(file_operation_error_value_without_file(
                    "Opening null device",
                    &error,
                ))
            })?;
        (None, output)
    } else if let Some(path) = &output_path {
        let output = open_child_output_file(&host_file_name(interp, env, path)?)
            .map_err(|error| file_operation_error("Opening process output file", &error, path))?;
        (None, output)
    } else {
        let (read, write) = emacs_pipe().map_err(|error| {
            LispError::SignalValue(file_operation_error_value_without_file(
                "Creating process pipe",
                &error,
            ))
        })?;
        (Some(read), write)
    };
    let error = if error_path.is_some() || (error_file.is_nil() && !discard_output) {
        let path = error_path.as_deref().unwrap_or(NULL_DEVICE);
        Some(
            open_child_output_file(&host_file_name(interp, env, path)?)
                .map_err(|error| file_operation_error("Cannot redirect stderr", &error, path))?,
        )
    } else {
        None
    };
    let environment = child_environment(interp, env, directory)?;
    #[cfg(unix)]
    let _ = child_signal_read_fd();
    let mut child = spawn_call_process(
        &program,
        &argv,
        environment,
        &filefd,
        &output,
        error.as_ref().unwrap_or(&output),
    )?;
    // Only the child keeps what it was given, or the read below never ends.
    drop((filefd, output, error));
    if no_wait {
        // record_deleted_pid: handle_child_signal reaps it, and removes
        // the input file, when it exits.
        reap_deleted_child(child, tempfile);
        return Ok(Value::Nil);
    }
    // wait_for_termination (pid, &status, fd0 < 0).
    let wait_interruptibly = output_read.is_none();
    if let Some(mut reader) = output_read {
        let mut bytes = Vec::new();
        let read = read_child_output(interp, env, &mut reader, &mut bytes);
        drop(reader);
        let inserted = read.and_then(|()| {
            append_process_bytes_to_buffer(interp, &buffer, &bytes, true, "call-process", args, env)
        });
        if let Err(error) = inserted {
            // Fkill_emacs leaves without unwinding: no cleanup runs.
            if !matches!(error.kind(), LispErrorKind::Terminate(_)) {
                call_process_cleanup(interp, env, child)?;
            }
            return Err(error);
        }
    }
    let status = wait_for_termination(interp, env, &mut child, wait_interruptibly)?;
    drop(tempfile);
    match status {
        Ok(status) => Ok(call_process_status(&status)),
        Err(_) => Ok(Value::string("internal error")),
    }
}

/// record_deleted_pid: CHILD, which nothing waits for, reaped whenever it
/// exits, and TEMPFILE removed then.
fn reap_deleted_child(mut child: SpawnedChild, tempfile: Option<CallProcessTempFile>) {
    let _ = std::thread::Builder::new()
        .name("process-reaper".into())
        .spawn(move || {
            let _ = child.wait();
            drop(tempfile);
        });
}

/// call_process_cleanup, leaving call_process before the child's end: its
/// process group interrupted and waited for, the wait announced; a quit
/// in that wait leaves call_process_kill to kill the group.
fn call_process_cleanup(
    interp: &mut Interpreter,
    env: &mut Env,
    mut child: SpawnedChild,
) -> Result<(), LispError> {
    #[cfg(unix)]
    // SAFETY: a signal to the group of this unreaped child, which its new
    // session made it lead.
    unsafe {
        libc::kill(-(child.id() as libc::pid_t), libc::SIGINT);
    }
    super::dispatch::display::message1(
        interp,
        "Waiting for process to die...(type C-g again to kill it instantly)",
        env,
    )?;
    let waited = match wait_for_termination(interp, env, &mut child, true) {
        Ok(waited) => waited,
        Err(error) => {
            // A quit leaves call_process_kill to record_kill_process;
            // Fkill_emacs unwinds nothing.
            if !matches!(error.kind(), LispErrorKind::Terminate(_)) {
                #[cfg(unix)]
                // SAFETY: as above.
                unsafe {
                    libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
                }
            }
            reap_deleted_child(child, None);
            return Err(error);
        }
    };
    super::dispatch::display::message1(
        interp,
        if waited.is_ok() {
            "Waiting for process to die...done"
        } else {
            "Waiting for process to die...internal error"
        },
        env,
    )
}

/// call_process's emacs_read_quit loop over READER to its end: a signal
/// interrupting the read runs the quit check, a fatal signal's
/// `kill-emacs' among it; a failed read gives up, keeping what came
/// before.
fn read_child_output(
    interp: &mut Interpreter,
    env: &mut Env,
    reader: &mut fs::File,
    bytes: &mut Vec<u8>,
) -> Result<(), LispError> {
    use std::io::Read;
    let mut buffer = vec![0u8; 65536];
    loop {
        // The handlers restart a read they interrupt; a poll they do not.
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let mut fd = libc::pollfd {
                fd: reader.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: FD is one initialized pollfd for READER's descriptor.
            if unsafe { libc::poll(&mut fd, 1, -1) } < 0 {
                if std::io::Error::last_os_error().kind() != ErrorKind::Interrupted {
                    return Ok(());
                }
                interp.maybe_quit(env)?;
                continue;
            }
        }
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(read) => bytes.extend_from_slice(&buffer[..read]),
            Err(error) if error.kind() == ErrorKind::Interrupted => interp.maybe_quit(env)?,
            Err(_) => return Ok(()),
        }
    }
}

/// wait_for_termination: CHILD's status, a signal interrupting the wait
/// running a fatal signal's `kill-emacs' as GNU's handler does, and the
/// quit check too when INTERRUPTIBLE.  GNU blocks in waitpid, which its
/// handlers interrupt; here each wait polls what says the child may have
/// exited: on Linux a pidfd, which needs no SIGCHLD handler (libgccjit's
/// driver sets SIGCHLD to SIG_DFL when it compiles in this process), else
/// the child signal pipe, for at most 100 ms in case nothing writes to it.
fn wait_for_termination(
    interp: &mut Interpreter,
    env: &mut Env,
    child: &mut SpawnedChild,
    interruptible: bool,
) -> Result<std::io::Result<std::process::ExitStatus>, LispError> {
    #[cfg(unix)]
    {
        use std::os::fd::{AsRawFd, OwnedFd};
        #[cfg(target_os = "linux")]
        let pidfd = {
            use std::os::fd::FromRawFd;
            // SAFETY: pidfd_open reads a pid and flags, and a descriptor it
            // returns is owned at once.
            match unsafe { libc::syscall(libc::SYS_pidfd_open, child.id() as libc::pid_t, 0) } {
                fd if fd >= 0 => Some(unsafe { OwnedFd::from_raw_fd(fd as libc::c_int) }),
                _ => None,
            }
        };
        #[cfg(not(target_os = "linux"))]
        let pidfd: Option<OwnedFd> = None;
        let waited = match (&pidfd, child_signal_read_fd()) {
            (Some(pidfd), _) => Some((pidfd.as_raw_fd(), -1)),
            (None, Some(signal_fd)) => Some((signal_fd, 100)),
            (None, None) => None,
        };
        if let Some((fd, timeout)) = waited {
            loop {
                match child.try_wait() {
                    Ok(Some(status)) => return Ok(Ok(status)),
                    Ok(None) => {}
                    Err(error) => return Ok(Err(error)),
                }
                let mut pollfd = libc::pollfd {
                    fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                // SAFETY: POLLFD is one initialized pollfd for a live
                // descriptor.
                let ready = unsafe { libc::poll(&mut pollfd, 1, timeout) };
                if ready < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != ErrorKind::Interrupted {
                        return Ok(child.wait());
                    }
                    if interruptible {
                        interp.maybe_quit(env)?;
                    } else if crate::lisp::primitives::fatal_signal_pending() {
                        crate::lisp::primitives::run_pending_fatal_signal(interp, env)?;
                    }
                } else if ready > 0 && pidfd.is_none() {
                    drain_child_signal_pipe(fd);
                }
            }
        }
    }
    let _ = (interp, env, interruptible);
    Ok(child.wait())
}

/// emacs_spawn for call_process: PROGRAM with ARGV, in ENVIRONMENT's
/// directory and environment, reading STDIN and writing STDOUT and STDERR
/// (one descriptor when standard error goes with the output).
fn spawn_call_process(
    program: &std::ffi::OsStr,
    argv: &[Vec<u8>],
    environment: ChildEnvironment,
    stdin: &fs::File,
    stdout: &fs::File,
    stderr: &fs::File,
) -> Result<SpawnedChild, LispError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let (block, directory) = &environment;
        // An argument or directory holding a NUL takes the fork path.
        if let (Some(image), Ok(directory)) = (
            ExecImage::new(program, argv, block.clone()),
            directory.clone().map(std::ffi::CString::new).transpose(),
        ) {
            let stdio = [stdin, stdout, stderr].map(|file| SpawnFd::Fd(file.as_raw_fd()));
            let pid = emacs_posix_spawn(&image, directory.as_deref(), stdio, None)
                .map_err(|error| vfork_error(&error))?;
            return Ok(SpawnedChild::new(pid as u32, None, None, None));
        }
    }
    let mut command = Command::new(program);
    add_command_arguments(&mut command, argv);
    set_command_environment(&mut command, &environment);
    #[cfg(unix)]
    configure_emacs_spawn(&mut command, false);
    #[cfg(unix)]
    exec_like_gnu(&mut command, program, argv, environment.0, false);
    let duplicate = |file: &fs::File| {
        file.try_clone()
            .map(Stdio::from)
            .map_err(|error| LispError::Signal(error.to_string()))
    };
    command.stdin(duplicate(stdin)?);
    command.stdout(duplicate(stdout)?);
    command.stderr(duplicate(stderr)?);
    let child = command.spawn().map_err(|error| vfork_error(&error))?;
    #[cfg(unix)]
    return Ok(SpawnedChild::from_std(child));
    #[cfg(not(unix))]
    Ok(child)
}

/// A child's environment block and, when `default-directory' names one,
/// the directory it starts in.
pub(crate) type ChildEnvironment = (Vec<Vec<u8>>, Option<Vec<u8>>);

/// The child's environment and directory, the directory also set on
/// COMMAND.
pub(crate) fn configure_external_command(
    interp: &mut Interpreter,
    env: &mut Env,
    command: &mut Command,
) -> Result<ChildEnvironment, LispError> {
    let directory = process_current_directory(interp, env)?;
    let environment = child_environment(interp, env, directory)?;
    set_command_environment(command, &environment);
    Ok(environment)
}

/// callproc.c get_current_directory: the directory `default-directory'
/// names, as `unhandled-file-name-directory' answers it ("~" for its nil),
/// expanded and checked to be one; None when `default-directory' names
/// none, which leaves a child in this process's directory.
pub(crate) fn process_current_directory(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<String>, LispError> {
    let Some(default_directory) = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .filter(|directory| !directory.is_empty())
    else {
        return Ok(None);
    };
    // GNU's get_current_directory(true) asks a handler for the local
    // directory without delegating the process operation itself.  This
    // is what lets `:file-handler nil' run locally from a remote logical
    // default-directory.
    // callproc.c invokes Funhandled_file_name_directory directly, so a
    // user redefinition of the Lisp function cell cannot replace the C
    // mechanism.  Native dispatch still performs the legitimate
    // file-name-handler call made inside that primitive.
    let unhandled = call(
        interp,
        "unhandled-file-name-directory",
        &[Value::String(default_directory.clone().into())],
        env,
    )?;
    // A handler returns nil when its logical directory cannot be a host
    // cwd.  GNU falls back to HOME, then expands the result without
    // sending it back through remote-name resolution.
    let unhandled = string_like(&unhandled)
        .map(|directory| directory.text)
        .unwrap_or_else(|| "~".into());
    let directory = expand_file_name_in_env(interp, env, &unhandled, None);
    let metadata = fs::metadata(host_file_name(interp, env, &directory)?).map_err(|error| {
        LispError::SignalValue(file_operation_error_value(
            "Setting current directory",
            &error,
            &default_directory,
        ))
    })?;
    if !metadata.is_dir() {
        let error = std::io::Error::from(ErrorKind::NotADirectory);
        return Err(LispError::SignalValue(file_operation_error_value(
            "Setting current directory",
            &error,
            &default_directory,
        )));
    }
    Ok(Some(directory))
}

/// make_environment_block for a child starting in DIRECTORY (this
/// process's own when None), with that directory.
fn child_environment(
    interp: &mut Interpreter,
    env: &mut Env,
    directory: Option<String>,
) -> Result<ChildEnvironment, LispError> {
    // callproc.c encode_current_directory: ENCODE_FILE.
    let directory = directory
        .map(|directory| encode_file_name(interp, env, &directory))
        .transpose()?;
    let current_dir = directory.clone().unwrap_or_else(|| {
        let directory = std::env::current_dir().unwrap_or_default();
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            directory.into_os_string().into_vec()
        }
        #[cfg(not(unix))]
        directory.to_string_lossy().into_owned().into_bytes()
    });
    let block = make_environment_block(interp, env, &current_dir);
    Ok((block, directory))
}

/// COMMAND started in ENVIRONMENT's directory, with (where the exec does
/// not take the block itself) its environment.
fn set_command_environment(command: &mut Command, environment: &ChildEnvironment) {
    let (block, directory) = environment;
    if let Some(directory) = directory {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            command.current_dir(std::ffi::OsStr::from_bytes(directory));
        }
        #[cfg(not(unix))]
        command.current_dir(String::from_utf8_lossy(directory).into_owned());
    }
    #[cfg(not(unix))]
    {
        command.env_clear();
        for entry in block {
            let entry = String::from_utf8_lossy(entry);
            if let Some((name, value)) = entry.split_once('=') {
                command.env(name, value);
            }
        }
    }
    #[cfg(unix)]
    let _ = block;
}

pub(crate) fn spawn_persistent_process(
    interp: &mut Interpreter,
    program: &std::ffi::OsStr,
    argv: &[Vec<u8>],
    env: &mut Env,
    connection_type: Option<&Value>,
    separate_stderr: bool,
) -> Result<RunningProcess, LispError> {
    #[cfg(test)]
    crate::test_support::mark_process_test();

    #[cfg(unix)]
    let _ = child_signal_read_fd();
    let mut command = Command::new(program);
    add_command_arguments(&mut command, argv);
    let (environment, directory) = configure_external_command(interp, env, &mut command)?;

    let default_pty = interp
        .lookup_var("process-connection-type", env)
        .is_some_and(|value| value.is_truthy());
    let (input_pty, output_pty) = process_connection_pty_modes(connection_type, default_pty)?;
    #[cfg(target_os = "linux")]
    if let Some(runtime) = posix_spawn_process(
        program,
        argv,
        &environment,
        directory,
        input_pty,
        output_pty,
        separate_stderr,
    )? {
        return finish_spawned_runtime(runtime);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = directory;
    let mut pty_input = None;
    let mut pty_output = None;
    let mut pty_slave_guard = None;
    let mut pty_slave_name: Option<String> = None;
    // create_process's pipes, made here rather than by the spawn so that
    // their descriptors are known before it: the write end of the child's
    // input, the read ends of its output and its separate standard error.
    #[cfg(unix)]
    let (mut stdin_write, mut stdout_read, mut stderr_read) = (None, None, None);
    #[cfg(unix)]
    {
        let pipe = || {
            emacs_pipe().map_err(|error| {
                LispError::SignalValue(file_operation_error_value_without_file(
                    "Creating pipe",
                    &error,
                ))
            })
        };
        let mut piped_stdin = |command: &mut Command| -> Result<(), LispError> {
            let (read, write) = pipe()?;
            command.stdin(Stdio::from(read));
            stdin_write = Some(write);
            Ok(())
        };
        let piped_output = |command: &mut Command, error: bool| -> Result<fs::File, LispError> {
            let (read, write) = pipe()?;
            if error {
                command.stderr(Stdio::from(write));
            } else {
                command.stdout(Stdio::from(write));
            }
            Ok(read)
        };
        if input_pty && output_pty {
            let (master, slave, slave_name) = open_emacs_pty()?;
            pty_slave_name = slave_name;
            pty_slave_guard = Some(
                slave
                    .try_clone()
                    .map_err(|error| LispError::Signal(error.to_string()))?,
            );
            command.stdin(Stdio::from(
                slave
                    .try_clone()
                    .map_err(|error| LispError::Signal(error.to_string()))?,
            ));
            command.stdout(Stdio::from(
                slave
                    .try_clone()
                    .map_err(|error| LispError::Signal(error.to_string()))?,
            ));
            if separate_stderr {
                stderr_read = Some(piped_output(&mut command, true)?);
            } else {
                command.stderr(Stdio::from(slave));
            }
            pty_input = Some(
                master
                    .try_clone()
                    .map_err(|error| LispError::Signal(error.to_string()))?,
            );
            pty_output = Some(master);
        } else if input_pty {
            let (master, slave, slave_name) = open_emacs_pty()?;
            pty_slave_name = slave_name;
            command.stdin(Stdio::from(slave));
            stdout_read = Some(piped_output(&mut command, false)?);
            stderr_read = Some(piped_output(&mut command, true)?);
            pty_input = Some(master);
        } else {
            piped_stdin(&mut command)?;
            if output_pty {
                let (master, slave, slave_name) = open_emacs_pty()?;
                pty_slave_name = slave_name;
                pty_slave_guard = Some(
                    slave
                        .try_clone()
                        .map_err(|error| LispError::Signal(error.to_string()))?,
                );
                command.stdout(Stdio::from(
                    slave
                        .try_clone()
                        .map_err(|error| LispError::Signal(error.to_string()))?,
                ));
                if separate_stderr {
                    stderr_read = Some(piped_output(&mut command, true)?);
                } else {
                    command.stderr(Stdio::from(slave));
                }
                pty_output = Some(master);
            } else {
                stdout_read = Some(piped_output(&mut command, false)?);
                stderr_read = Some(piped_output(&mut command, true)?);
            }
        }
        // create_process: the channel Emacs reads and the one it writes
        // must fit an fd_set; none is spawned otherwise.
        use std::os::fd::AsRawFd;
        let inchannel = stdout_read.as_ref().or(pty_output.as_ref());
        let outchannel = stdin_write.as_ref().or(pty_input.as_ref());
        if beyond_fd_setsize(
            &[inchannel, outchannel].map(|channel| channel.map_or(-1, |file| file.as_raw_fd())),
        ) {
            return Err(fd_setsize_error("Creating pipe"));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (input_pty, output_pty, separate_stderr);
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
    }

    #[cfg(unix)]
    configure_emacs_spawn(&mut command, input_pty);
    #[cfg(unix)]
    exec_like_gnu(
        &mut command,
        program,
        argv,
        environment,
        pty_slave_name.is_some(),
    );

    let child = command.spawn().map_err(|error| vfork_error(&error))?;
    // Only the child keeps the ends it was given.
    drop(command);
    #[cfg(unix)]
    let child = SpawnedChild::new(child.id(), stdin_write, stdout_read, stderr_read);
    // Put the child under RunningProcess's terminate-and-reap Drop guard
    // before any fallible post-spawn setup.  A failed fcntl must not leak a
    // subprocess whose runtime was never installed in the interpreter.
    let runtime = RunningProcess {
        child,
        terminate_on_drop: true,
        pty_input,
        pty_output,
        pty_slave_guard,
        pty_slave_name,
        stdout_at_end: false,
        stderr_at_end: false,
    };
    finish_spawned_runtime(runtime)
}

/// create_process's fcntl (inchannel, F_SETFL, O_NONBLOCK) on the channels
/// Emacs reads.
fn finish_spawned_runtime(runtime: RunningProcess) -> Result<RunningProcess, LispError> {
    #[cfg(unix)]
    {
        if let Some(stdout) = runtime.child.stdout.as_ref() {
            set_nonblocking(stdout)?;
        }
        if let Some(stderr) = runtime.child.stderr.as_ref() {
            set_nonblocking(stderr)?;
        }
        if let Some(output) = runtime.pty_output.as_ref() {
            set_nonblocking(output)?;
        }
    }
    Ok(runtime)
}

/// process.c's FD_SETSIZE: a channel at or above it, which `pselect'
/// could not watch, makes its process's creation fail with EMFILE.
/// glibc's fd_set holds 1024 descriptors; the contracted macOS build is
/// configured with -DFD_SETSIZE=10000.
#[cfg(target_os = "macos")]
pub(crate) const FD_SETSIZE: std::os::fd::RawFd = 10000;
#[cfg(all(unix, not(target_os = "macos")))]
pub(crate) const FD_SETSIZE: std::os::fd::RawFd = libc::FD_SETSIZE as std::os::fd::RawFd;

/// report_file_errno (WHAT, Qnil, EMFILE): the error of a channel
/// FD_SETSIZE refuses.
#[cfg(unix)]
pub(crate) fn fd_setsize_error(what: &str) -> LispError {
    LispError::SignalValue(file_operation_error_value_without_file(
        what,
        &std::io::Error::from_raw_os_error(libc::EMFILE),
    ))
}

/// `FD_SETSIZE <= inchannel || FD_SETSIZE <= outchannel'.
#[cfg(unix)]
pub(crate) fn beyond_fd_setsize(channels: &[std::os::fd::RawFd]) -> bool {
    channels.iter().any(|fd| FD_SETSIZE <= *fd)
}

/// connect_network_socket's and server_accept_connection's `FD_SETSIZE
/// <= s': a socket at or above FD_SETSIZE is closed, and its connection
/// fails with EMFILE.
#[cfg(unix)]
pub(crate) fn socket_within_fd_setsize<T: std::os::fd::AsRawFd>(socket: T) -> std::io::Result<T> {
    if beyond_fd_setsize(&[socket.as_raw_fd()]) {
        drop(socket);
        return Err(std::io::Error::from_raw_os_error(libc::EMFILE));
    }
    Ok(socket)
}

#[cfg(not(unix))]
pub(crate) fn socket_within_fd_setsize<T>(socket: T) -> std::io::Result<T> {
    Ok(socket)
}

/// Fmake_pipe_process's two pipes, as (read end, write end): the one the
/// process writes into (SUBPROCESS_STDIN, WRITE_TO_SUBPROCESS) and the
/// one it reads (READ_FROM_SUBPROCESS, SUBPROCESS_STDOUT).
pub(crate) struct PipeProcessChannels {
    pub(crate) input: (fs::File, fs::File),
    pub(crate) output: (fs::File, fs::File),
}

/// Fmake_pipe_process's channels: both pipes, EMFILE when the channel it
/// reads or the one it writes lies at or above FD_SETSIZE, and both
/// channels non-blocking.  The descriptors close when an error drops
/// them, as remove_process's unwind closes them.
pub(crate) fn open_pipe_process_channels() -> Result<PipeProcessChannels, LispError> {
    let pipe_error = |error: std::io::Error| {
        LispError::SignalValue(file_operation_error_value_without_file(
            "Creating pipe",
            &error,
        ))
    };
    let input = emacs_pipe().map_err(pipe_error)?;
    let output = emacs_pipe().map_err(pipe_error)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        if beyond_fd_setsize(&[output.0.as_raw_fd(), input.1.as_raw_fd()]) {
            return Err(fd_setsize_error("Creating pipe"));
        }
        set_nonblocking(&output.0)?;
        set_nonblocking(&input.1)?;
    }
    Ok(PipeProcessChannels { input, output })
}

/// emacs_pipe: a pipe both of whose ends are close-on-exec, as (read end,
/// write end).
#[cfg(target_os = "linux")]
fn emacs_pipe() -> std::io::Result<(fs::File, fs::File)> {
    use std::os::fd::{FromRawFd, OwnedFd};
    let mut fds = [-1; 2];
    // SAFETY: PIPE2 writes exactly two descriptors to FDS on success, each
    // owned at once below.
    if unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fresh descriptors, each owned by exactly one File.
    unsafe {
        Ok((
            fs::File::from(OwnedFd::from_raw_fd(fds[0])),
            fs::File::from(OwnedFd::from_raw_fd(fds[1])),
        ))
    }
}

/// emacs_pipe where pipe2 is missing: std's pipe marks both ends
/// close-on-exec itself.
#[cfg(not(target_os = "linux"))]
fn emacs_pipe() -> std::io::Result<(fs::File, fs::File)> {
    #[cfg(unix)]
    use std::os::fd::OwnedFd as Owned;
    #[cfg(windows)]
    use std::os::windows::io::OwnedHandle as Owned;
    let (read, write) = std::io::pipe()?;
    Ok((
        fs::File::from(Owned::from(read)),
        fs::File::from(Owned::from(write)),
    ))
}

/// Where emacs_spawn puts one of the child's standard descriptors.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum SpawnFd {
    /// This descriptor of the parent's.
    Fd(std::os::fd::RawFd),
    /// The pty, which the child opens by name (input before output).
    Pty,
    /// The descriptor the child's standard output became.
    Stdout,
}

/// callproc.c:emacs_spawn's posix_spawn (emacs_posix_spawn_init_actions and
/// emacs_posix_spawn_init_attributes): IMAGE in a new session, in DIRECTORY,
/// with SIGINT, SIGQUIT, SIGPROF and SIGPIPE at their defaults and the
/// parent's signal mask.  A child on the pty opens PTY_NAME as its vfork
/// path does after setsid, which makes the pty its controlling terminal.
/// The error is posix_spawn's, the errno of a failed exec among them.
#[cfg(target_os = "linux")]
fn emacs_posix_spawn(
    image: &ExecImage,
    directory: Option<&std::ffi::CStr>,
    stdio: [SpawnFd; 3],
    pty_name: Option<&std::ffi::CStr>,
) -> std::io::Result<libc::pid_t> {
    use std::mem::MaybeUninit;
    let check = |error: libc::c_int| match error {
        0 => Ok(()),
        error => Err(std::io::Error::from_raw_os_error(error)),
    };
    let mut actions = MaybeUninit::<libc::posix_spawn_file_actions_t>::uninit();
    let mut attributes = MaybeUninit::<libc::posix_spawnattr_t>::uninit();
    // SAFETY: each object is initialized before use and destroyed once
    // below; every pointer handed over (names, IMAGE's vectors) outlives
    // the posix_spawn call.
    unsafe {
        check(libc::posix_spawn_file_actions_init(actions.as_mut_ptr()))?;
        if let Err(error) = check(libc::posix_spawnattr_init(attributes.as_mut_ptr())) {
            libc::posix_spawn_file_actions_destroy(actions.as_mut_ptr());
            return Err(error);
        }
        let actions = actions.as_mut_ptr();
        let attributes = attributes.as_mut_ptr();
        let result = (|| {
            let mut pty_fd = None;
            for (target, source) in stdio.into_iter().enumerate() {
                let target = target as libc::c_int;
                match source {
                    SpawnFd::Fd(fd) => {
                        check(libc::posix_spawn_file_actions_adddup2(actions, fd, target))?
                    }
                    SpawnFd::Pty => match pty_fd {
                        Some(fd) => {
                            check(libc::posix_spawn_file_actions_adddup2(actions, fd, target))?
                        }
                        None => {
                            let name = pty_name
                                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EINVAL))?;
                            check(libc::posix_spawn_file_actions_addopen(
                                actions,
                                target,
                                name.as_ptr(),
                                libc::O_RDWR,
                                0,
                            ))?;
                            pty_fd = Some(target);
                        }
                    },
                    SpawnFd::Stdout => check(libc::posix_spawn_file_actions_adddup2(
                        actions,
                        libc::STDOUT_FILENO,
                        target,
                    ))?,
                }
            }
            if let Some(directory) = directory {
                check(libc::posix_spawn_file_actions_addchdir_np(
                    actions,
                    directory.as_ptr(),
                ))?;
            }
            check(libc::posix_spawnattr_setflags(
                attributes,
                (libc::POSIX_SPAWN_SETSID
                    | libc::POSIX_SPAWN_SETSIGDEF
                    | libc::POSIX_SPAWN_SETSIGMASK) as libc::c_short,
            ))?;
            let mut default = MaybeUninit::<libc::sigset_t>::uninit();
            libc::sigemptyset(default.as_mut_ptr());
            for signal in [libc::SIGINT, libc::SIGQUIT, libc::SIGPROF, libc::SIGPIPE] {
                libc::sigaddset(default.as_mut_ptr(), signal);
            }
            check(libc::posix_spawnattr_setsigdefault(
                attributes,
                default.as_ptr(),
            ))?;
            let mut mask = MaybeUninit::<libc::sigset_t>::uninit();
            check(libc::pthread_sigmask(
                libc::SIG_SETMASK,
                std::ptr::null(),
                mask.as_mut_ptr(),
            ))?;
            check(libc::posix_spawnattr_setsigmask(attributes, mask.as_ptr()))?;
            let mut pid = 0;
            check(libc::posix_spawn(
                &mut pid,
                image.argv[0],
                actions,
                attributes,
                image.argv.as_ptr().cast(),
                image.envp.as_ptr().cast(),
            ))?;
            Ok(pid)
        })();
        libc::posix_spawn_file_actions_destroy(actions);
        libc::posix_spawnattr_destroy(attributes);
        result
    }
}

/// Fmake_process's create_process through emacs_spawn's posix_spawn: the
/// pipes (or the pty) its connection type asks for, and the child's
/// standard error on its own pipe when it goes to a process or buffer of
/// its own, else where its output goes.  None leaves the spawn to the fork
/// path: an argument with a NUL, a pty without a name, or an exec on the
/// pty that fails, whose report the vfork path's child makes.
#[cfg(target_os = "linux")]
fn posix_spawn_process(
    program: &std::ffi::OsStr,
    argv: &[Vec<u8>],
    environment: &[Vec<u8>],
    directory: Option<Vec<u8>>,
    input_pty: bool,
    output_pty: bool,
    separate_stderr: bool,
) -> Result<Option<RunningProcess>, LispError> {
    use std::os::fd::AsRawFd;
    let Some(image) = ExecImage::new(program, argv, environment.to_vec()) else {
        return Ok(None);
    };
    let Ok(directory) = directory.map(std::ffi::CString::new).transpose() else {
        return Ok(None);
    };
    let pipe_error = |error: std::io::Error| {
        LispError::SignalValue(file_operation_error_value_without_file(
            "Creating pipe",
            &error,
        ))
    };
    let (pty_master, pty_slave, pty_name) = if input_pty || output_pty {
        let (master, slave, name) = open_emacs_pty()?;
        let Some(name) = name.and_then(|name| std::ffi::CString::new(name).ok()) else {
            return Ok(None);
        };
        (Some(master), Some(slave), Some(name))
    } else {
        (None, None, None)
    };
    let input = (!input_pty)
        .then(emacs_pipe)
        .transpose()
        .map_err(pipe_error)?;
    let output = (!output_pty)
        .then(emacs_pipe)
        .transpose()
        .map_err(pipe_error)?;
    let errors = separate_stderr
        .then(emacs_pipe)
        .transpose()
        .map_err(pipe_error)?;
    // create_process: the channel Emacs reads and the one it writes, each
    // a pipe or the pty, must fit an fd_set; none is spawned otherwise.
    let master = pty_master.as_ref().map(|master| master.as_raw_fd());
    let inchannel = output.as_ref().map(|(read, _)| read.as_raw_fd()).or(master);
    let outchannel = input
        .as_ref()
        .map(|(_, write)| write.as_raw_fd())
        .or(master);
    if beyond_fd_setsize(&[inchannel, outchannel].map(|fd| fd.unwrap_or(-1))) {
        return Err(fd_setsize_error("Creating pipe"));
    }
    let stdio = [
        input
            .as_ref()
            .map_or(SpawnFd::Pty, |(read, _)| SpawnFd::Fd(read.as_raw_fd())),
        output
            .as_ref()
            .map_or(SpawnFd::Pty, |(_, write)| SpawnFd::Fd(write.as_raw_fd())),
        errors
            .as_ref()
            .map_or(SpawnFd::Stdout, |(_, write)| SpawnFd::Fd(write.as_raw_fd())),
    ];
    let pid = match emacs_posix_spawn(&image, directory.as_deref(), stdio, pty_name.as_deref()) {
        Ok(pid) => pid,
        Err(_) if pty_name.is_some() => return Ok(None),
        Err(error) => return Err(vfork_error(&error)),
    };
    // Only the child keeps its ends.
    let child = SpawnedChild::new(
        pid as u32,
        input.map(|(_, write)| write),
        output.map(|(read, _)| read),
        errors.map(|(read, _)| read),
    );
    // As on the fork path: with the pty on both sides, one slave is held
    // until the child's output is drained; the master is both channels.
    let (pty_input, pty_output, pty_slave_guard) = match (pty_master, input_pty, output_pty) {
        (Some(master), true, true) => {
            let input = master
                .try_clone()
                .map_err(|error| LispError::Signal(error.to_string()))?;
            (Some(input), Some(master), pty_slave)
        }
        (Some(master), true, false) => (Some(master), None, None),
        (Some(master), false, true) => (None, Some(master), pty_slave),
        _ => (None, None, None),
    };
    Ok(Some(RunningProcess {
        child,
        terminate_on_drop: true,
        pty_input,
        pty_output,
        pty_slave_guard,
        pty_slave_name: pty_name.map(|name| name.to_string_lossy().into_owned()),
        stdout_at_end: false,
        stderr_at_end: false,
    }))
}

fn process_connection_pty_modes(
    connection_type: Option<&Value>,
    default_pty: bool,
) -> Result<(bool, bool), LispError> {
    let Some(connection_type) = connection_type else {
        return Ok((default_pty, default_pty));
    };
    if let Some((input, output)) = connection_type.cons_values() {
        return Ok((
            process_connection_endpoint_uses_pty(&input, default_pty)?,
            process_connection_endpoint_uses_pty(&output, default_pty)?,
        ));
    }
    let pty = process_connection_endpoint_uses_pty(connection_type, default_pty)?;
    Ok((pty, pty))
}

fn process_connection_endpoint_uses_pty(
    value: &Value,
    default_pty: bool,
) -> Result<bool, LispError> {
    match value.kind() {
        Kind::Nil => Ok(default_pty),
        Kind::Symbol(name) if name == "pty" => Ok(true),
        Kind::Symbol(name) if name == "pipe" => Ok(false),
        _ => Err(LispError::Signal(format!(
            "Unknown connection type: {value}"
        ))),
    }
}

#[cfg(unix)]
fn open_emacs_pty() -> Result<(fs::File, fs::File, Option<String>), LispError> {
    let mut master = -1;
    let mut slave = -1;
    // SAFETY: `openpty' initializes both file descriptors; the optional
    // name/termios/winsize pointers are allowed to be null.
    if unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    } != 0
    {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: ownership of the fresh descriptors is transferred exactly once.
    let master = unsafe { fs::File::from_raw_fd(master) };
    // SAFETY: ownership of the fresh descriptor is transferred exactly once.
    let slave = unsafe { fs::File::from_raw_fd(slave) };
    // create_pty: a pty `pselect' could not watch is refused.
    if beyond_fd_setsize(&[master.as_raw_fd()]) {
        return Err(fd_setsize_error("Opening pty"));
    }
    for fd in [master.as_raw_fd(), slave.as_raw_fd()] {
        // SAFETY: both descriptors are live and owned by the File values.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            return Err(LispError::Signal(
                std::io::Error::last_os_error().to_string(),
            ));
        }
    }

    let mut attributes = std::mem::MaybeUninit::<libc::termios>::uninit();
    // SAFETY: `slave' is a live terminal fd and `tcgetattr' initializes the
    // termios value on success.
    if unsafe { libc::tcgetattr(slave.as_raw_fd(), attributes.as_mut_ptr()) } == 0 {
        // SAFETY: the successful `tcgetattr' initialized `attributes'.
        let mut attributes = unsafe { attributes.assume_init() };
        // GNU leaves canonical input and EOF processing enabled, while
        // disabling echo and CRLF expansion because the editor owns those
        // presentation concerns.  In particular, canonical VEOF is what
        // makes `process-send-eof' meaningful for PTY subprocesses.
        attributes.c_oflag |= libc::OPOST;
        attributes.c_oflag &= !libc::ONLCR;
        attributes.c_lflag &= !libc::ECHO;
        attributes.c_lflag |= libc::ISIG | libc::ICANON;
        attributes.c_iflag &= !libc::ISTRIP;
        attributes.c_cflag = (attributes.c_cflag & !libc::CSIZE) | libc::CS8;
        attributes.c_cc[libc::VEOF] = 4;
        // SAFETY: `tcsetattr' operates on initialized termios storage and
        // the same live terminal fd.
        if unsafe { libc::tcsetattr(slave.as_raw_fd(), libc::TCSANOW, &attributes) } < 0 {
            return Err(LispError::Signal(
                std::io::Error::last_os_error().to_string(),
            ));
        }
    }
    // ttyname on the slave names the pty device; GNU stores the same
    // string in the process's tty_name.
    let slave_name = {
        // SAFETY: `slave' is a live terminal descriptor.
        let name = unsafe { libc::ttyname(slave.as_raw_fd()) };
        if name.is_null() {
            None
        } else {
            // SAFETY: ttyname returned a valid NUL-terminated string.
            Some(
                unsafe { std::ffi::CStr::from_ptr(name) }
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    };
    Ok((master, slave, slave_name))
}

#[cfg(unix)]
pub(crate) fn set_nonblocking<T: AsRawFd>(stream: &T) -> Result<(), LispError> {
    let fd = stream.as_raw_fd();
    // SAFETY: `fd` is an open file descriptor we own for the lifetime of this call.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: `fd` is still valid here, and we only add the O_NONBLOCK flag.
    let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if result < 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn process_buffer_target(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Option<u64>, LispError> {
    if value.is_nil() {
        return Ok(None);
    }
    if let Some(buffer) = string_like(value) {
        return Ok(Some(
            interp
                .find_buffer(&buffer.text)
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(&buffer.text).0),
        ));
    }
    if matches!(value.kind(), Kind::Buffer(_)) {
        return Ok(Some(interp.resolve_buffer_id(value)?));
    }
    Err(LispError::TypeError(
        "buffer-or-name".into(),
        value.type_name(),
    ))
}

pub(crate) fn process_command_parts(value: &Value) -> Result<(String, Vec<String>), LispError> {
    let items = value.to_vec()?;
    let Some((program, argv)) = items.split_first() else {
        return Err(LispError::Signal(
            "Process command must not be empty".into(),
        ));
    };
    Ok((
        string_text(program)?,
        argv.iter()
            .map(string_text)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

/// Which of process.c's connection makers chooses the codings, and what
/// it asks find-operation-coding-system.
pub(crate) enum ConnectionCoding {
    /// Fmake_pipe_process: never asks, falling back on
    /// `default-process-coding-system'.
    Pipe,
    /// set_network_socket_coding_system: `open-network-stream' with NAME,
    /// the buffer, HOST and SERVICE when both are given.
    Network {
        name: Value,
        host: Value,
        service: Value,
    },
    /// Fmake_serial_process: no fallback at all.
    Serial,
}

/// The codings process.c gives a pipe, network or serial process: CODING's
/// (its car to decode and cdr to encode when a cons), else
/// `coding-system-for-read' and `coding-system-for-write', else none for
/// unibyte text (decoding into a unibyte BUFFER, or with none the default
/// `enable-multibyte-characters' nil; encoding from a unibyte current
/// buffer, or for a serial process a unibyte BUFFER), else what KIND's
/// find-operation-coding-system call answers, else `default-process-
/// coding-system's.
pub(crate) fn connection_coding_systems(
    interp: &mut Interpreter,
    env: &mut Env,
    kind: ConnectionCoding,
    coding: Value,
    buffer: Value,
) -> Result<(Value, Value), LispError> {
    let buffer_unibyte = match buffer.kind() {
        Kind::Nil => interp
            .default_value("enable-multibyte-characters")
            .is_some_and(|value| value.is_nil()),
        _ => interp
            .resolve_buffer_id(&buffer)
            .ok()
            .and_then(|id| interp.get_buffer_by_id(id))
            .is_some_and(|buffer| !buffer.is_multibyte()),
    };
    let current_unibyte = !interp.buffer.borrow().is_multibyte();
    let default = interp
        .lookup_var("default-process-coding-system", env)
        .and_then(|value| value.cons_values());
    let mut coding_systems = None;
    let mut operation_coding = |interp: &mut Interpreter, env: &mut Env| {
        if let Some(found) = coding_systems {
            return Ok::<Value, LispError>(found);
        }
        let found = match &kind {
            ConnectionCoding::Network {
                name,
                host,
                service,
            } if !host.is_nil() && !service.is_nil() => find_operation_coding_system_value(
                interp,
                &[
                    Value::symbol("open-network-stream"),
                    *name,
                    buffer,
                    *host,
                    *service,
                ],
                env,
            )?,
            _ => Value::Nil,
        };
        coding_systems = Some(found);
        Ok(found)
    };
    let serial = matches!(kind, ConnectionCoding::Serial);
    let decoding = if !coding.is_nil() {
        coding
            .cons_values()
            .map_or(coding, |(decoding, _)| decoding)
    } else if let Some(value) = interp
        .lookup_var("coding-system-for-read", env)
        .filter(|value| !value.is_nil())
    {
        value
    } else if buffer_unibyte || serial {
        Value::Nil
    } else {
        match operation_coding(interp, env)?.cons_values() {
            Some((found, _)) => found,
            None => default.map_or(Value::Nil, |(decoding, _)| decoding),
        }
    };
    let encoding = if !coding.is_nil() {
        coding
            .cons_values()
            .map_or(coding, |(_, encoding)| encoding)
    } else if let Some(value) = interp
        .lookup_var("coding-system-for-write", env)
        .filter(|value| !value.is_nil())
    {
        value
    } else if serial || current_unibyte {
        Value::Nil
    } else {
        match operation_coding(interp, env)?.cons_values() {
            Some((_, found)) => found,
            None => default.map_or(Value::Nil, |(_, encoding)| encoding),
        }
    };
    Ok((decoding, encoding))
}

pub(crate) struct MakeProcessArgs {
    pub(crate) buffer_id: Option<u64>,
    pub(crate) program: Option<String>,
    pub(crate) argv: Vec<String>,
    pub(crate) filter: Option<Value>,
    pub(crate) sentinel: Option<Value>,
    pub(crate) name: Option<String>,
    pub(crate) stderr_process_id: Option<u64>,
    pub(crate) stderr_buffer_id: Option<u64>,
    pub(crate) query_on_exit_flag: bool,
    pub(crate) connection_type: Option<Value>,
    /// The `:coding', `:name' and `:command' values as given.
    pub(crate) coding_value: Value,
    pub(crate) name_value: Value,
    pub(crate) command: Value,
}

pub(crate) fn parse_make_process_args(
    interp: &mut Interpreter,
    args: &[Value],
) -> Result<MakeProcessArgs, LispError> {
    if !args.len().is_multiple_of(2) {
        return Err(LispError::WrongNumberOfArgs(
            "make-process".into(),
            args.len(),
        ));
    }
    let mut buffer_id = None;
    let mut program = None;
    let mut argv = Vec::new();
    let mut filter = None;
    let mut sentinel = None;
    let mut name = None;
    let mut stderr_process_id = None;
    let mut stderr_buffer_id = None;
    let mut query_on_exit_flag = true;
    let mut connection_type = None;
    let mut coding_value = Value::Nil;
    let mut name_value = Value::Nil;
    let mut command = Value::Nil;
    // Fmake_process reads each keyword with plist_get: its first value.
    let mut seen: Vec<&str> = Vec::new();

    for pair in args.as_chunks::<2>().0 {
        let key = pair[0].as_symbol()?;
        let value = &pair[1];
        if seen.contains(&key) {
            continue;
        }
        seen.push(key);
        match key {
            ":name" => {
                name = Some(string_text(value)?);
                name_value = *value;
            }
            ":buffer" => buffer_id = process_buffer_target(interp, value)?,
            ":command" => {
                let (parsed_program, parsed_argv) = process_command_parts(value)?;
                program = Some(parsed_program);
                argv = parsed_argv;
                command = *value;
            }
            ":filter" => filter = (!value.is_nil()).then_some(*value),
            ":sentinel" => sentinel = (!value.is_nil()).then_some(*value),
            ":coding" => coding_value = *value,
            ":stderr" if !value.is_nil() => {
                if let Ok(process_id) = interp.resolve_process_id(value) {
                    stderr_process_id = Some(process_id);
                } else {
                    stderr_buffer_id = process_buffer_target(interp, value)?;
                }
            }
            ":noquery" => query_on_exit_flag = !value.is_truthy(),
            // Normal asynchronous subprocesses cannot start stopped.  GNU's
            // CHECK_TYPE accepts this compatibility keyword only when its
            // value is nil.
            ":stop" if value.is_truthy() => {
                return Err(wrong_type_argument("null", *value));
            }
            ":stop" => {}
            ":file-handler" => {}
            ":connection-type" => connection_type = Some(*value),
            _ => {}
        }
    }

    Ok(MakeProcessArgs {
        buffer_id,
        program,
        argv,
        filter,
        sentinel,
        name,
        stderr_process_id,
        stderr_buffer_id,
        query_on_exit_flag,
        connection_type,
        coding_value,
        name_value,
        command,
    })
}

/// process.c Fmake_process's codings for a new process: CODING's (its car
/// to decode and cdr to encode when a cons), else `coding-system-for-read'
/// and `coding-system-for-write', else, for a process with a program, what
/// find-operation-coding-system answers for `start-process' with NAME,
/// BUFFER and the COMMAND's elements, else `default-process-coding-
/// system's -- left for the first output or input to complete.
pub(crate) fn make_process_coding_systems(
    interp: &mut Interpreter,
    env: &mut Env,
    coding: Value,
    name: Value,
    buffer: Value,
    command: Value,
) -> Result<(Value, Value), LispError> {
    let default = interp
        .lookup_var("default-process-coding-system", env)
        .and_then(|value| value.cons_values());
    let mut coding_systems = None;
    let mut operation_coding = |interp: &mut Interpreter, env: &mut Env| {
        if let Some(found) = coding_systems {
            return Ok(found);
        }
        let found = if command.is_nil() {
            Value::Nil
        } else {
            let lookup = [Value::symbol("start-process"), name, buffer]
                .into_iter()
                .chain(command.to_vec()?)
                .collect::<Vec<_>>();
            find_operation_coding_system_value(interp, &lookup, env)?
        };
        coding_systems = Some(found);
        Ok::<Value, LispError>(found)
    };
    let mut decoding = if coding.is_nil() {
        interp
            .lookup_var("coding-system-for-read", env)
            .unwrap_or(Value::Nil)
    } else {
        coding
            .cons_values()
            .map_or(coding, |(decoding, _)| decoding)
    };
    if decoding.is_nil() {
        decoding = match operation_coding(interp, env)?.cons_values() {
            Some((found, _)) => found,
            None => default.map_or(Value::Nil, |(decoding, _)| decoding),
        };
    }
    let mut encoding = if coding.is_nil() {
        interp
            .lookup_var("coding-system-for-write", env)
            .unwrap_or(Value::Nil)
    } else {
        coding
            .cons_values()
            .map_or(coding, |(_, encoding)| encoding)
    };
    if encoding.is_nil() {
        encoding = match operation_coding(interp, env)?.cons_values() {
            Some((_, found)) => found,
            None => default.map_or(Value::Nil, |(_, encoding)| encoding),
        };
    }
    Ok((decoding, encoding))
}

/// Fmake_process's arguments for the child: each of ARGUMENTS a string, a
/// multibyte one encoded (code_convert_string_norecord) by ENCODING as
/// complement_process_encoding_system completes it.
pub(crate) fn make_process_arguments(
    interp: &mut Interpreter,
    env: &mut Env,
    arguments: &[Value],
    encoding: Value,
) -> Result<Vec<Vec<u8>>, LispError> {
    let mut argument_coding = None;
    let mut encoded = Vec::with_capacity(arguments.len());
    for argument in arguments {
        let Kind::StringObject(state) = argument.kind() else {
            return Err(LispError::WrongTypeArgument("stringp".into(), *argument));
        };
        let multibyte = state.borrow().is_multibyte();
        let argument = if multibyte {
            let coding = match argument_coding {
                Some(coding) => coding,
                None => {
                    let coding = complement_process_encoding_system(interp, env, encoding)?;
                    argument_coding = Some(coding);
                    coding
                }
            };
            let name = if coding.is_nil() {
                None
            } else {
                Some(checked_coding_symbol(interp, &coding)?)
            };
            encode_coding_value_recording(interp, argument, name.as_deref(), false, false, env)?
        } else {
            *argument
        };
        match argument.kind() {
            Kind::StringObject(state) => encoded.push(state.borrow().bytes().to_vec()),
            _ => return Err(LispError::WrongTypeArgument("stringp".into(), argument)),
        }
    }
    Ok(encoded)
}

pub(crate) fn deliver_process_output_decoded(
    interp: &mut Interpreter,
    process_id: u64,
    output: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if string_like(&output).is_none_or(|text| text.text.is_empty()) {
        return Ok(());
    }
    interp.note_process_output_delivery(process_id);

    // The process buffer may have been killed before straggler output is
    // delivered (epg-reset kills it right after completion); GNU discards
    // such output rather than erroring.
    if let Some(filter) = interp.process_filter(process_id) {
        // GNU uses t as a flow-control sentinel: the descriptor is removed
        // from the read set until a real/default filter is installed again.
        // The pump normally avoids reaching this branch, but retaining the
        // guard makes direct delivery equally safe.
        if filter == Value::T {
            return Ok(());
        }
        // read_process_output_call invokes an explicit filter in the buffer
        // that was current when output delivery began, not in the process
        // buffer.  It still protects that current-buffer choice against a
        // filter which calls set-buffer itself.
        let saved_buffer_id = interp.current_buffer_id();
        // GNU hands the filter an ordinary mutable string; filters such as
        // eshell's propertize it in place.
        let result = call_function_value(
            interp,
            &filter,
            &[interp.record_value(process_id), output],
            env,
        );
        if interp.current_buffer_id() != saved_buffer_id {
            interp.set_current_buffer_id(saved_buffer_id)?;
        }
        result?;
        return Ok(());
    }

    internal_default_process_filter(interp, process_id, output, env)
}

/// GNU `internal-default-process-filter'.  Insert at the process mark before
/// other markers, preserve the buffer's logical point, and discard output
/// when the process has no live buffer.
pub(crate) fn internal_default_process_filter(
    interp: &mut Interpreter,
    process_id: u64,
    output: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let target_buffer_id = interp
        .process_buffer_id(process_id)
        .filter(|buffer_id| interp.get_buffer_by_id(*buffer_id).is_some());
    let Some(buffer_id) = target_buffer_id else {
        return Ok(());
    };
    let Some(mark_id) = interp.process_mark_id(process_id) else {
        return Ok(());
    };
    let saved_buffer_id = interp.current_buffer_id();
    let switched = buffer_id != saved_buffer_id;
    if switched {
        interp.set_current_buffer_id(buffer_id)?;
    }
    let old_point = interp.buffer.borrow().point();
    let insert_at = interp
        .marker_position(mark_id)
        .unwrap_or_else(|| interp.current_buffer().point_max());
    interp.buffer.borrow_mut().goto_char(insert_at);
    // The text's multibyteness made the buffer's (string-as-unibyte or
    // string-to-multibyte), then insert_from_string_before_markers, its
    // properties with it, with the buffer's read-only flag cleared.
    let multibyte_buffer = interp.buffer.borrow().is_multibyte();
    let output = match string_like(&output) {
        Some(text) if text.multibyte && !multibyte_buffer => {
            call(interp, "string-as-unibyte", &[output], env)?
        }
        Some(text) if !text.multibyte && multibyte_buffer => {
            call(interp, "string-to-multibyte", &[output], env)?
        }
        _ => output,
    };
    let text = string_like(&output)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), output))?;
    let inserted = insert_with_read_only_cleared(interp, buffer_id, env, |interp, env| {
        insert_text_with_hooks(
            interp,
            &text.text,
            text.props.as_ref(),
            &text.extended_chars,
            false,
            true,
            env,
        )
    });
    let new_pos = interp.buffer.borrow().point();
    let result = inserted.and_then(|()| interp.set_marker(mark_id, Some(new_pos), Some(buffer_id)));
    let restored_point = if old_point >= insert_at {
        old_point + new_pos.saturating_sub(insert_at)
    } else {
        old_point
    };
    interp.buffer.borrow_mut().goto_char(restored_point);
    if switched {
        interp.set_current_buffer_id(saved_buffer_id)?;
    }
    result
}

/// BODY with the current buffer's `buffer-read-only' cleared, set back
/// after it (process.c's bset_read_only around its insertions).
fn insert_with_read_only_cleared(
    interp: &mut Interpreter,
    buffer_id: u64,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<(), LispError>,
) -> Result<(), LispError> {
    let read_only = interp
        .symbol_value_key(cached_symbol!("buffer-read-only"))
        .unwrap_or(Value::Nil);
    if !read_only.is_nil() {
        interp.set_buffer_local_value(buffer_id, "buffer-read-only", Value::Nil);
    }
    let result = interp.with_lisp_stack_roots(&read_only, |interp| body(interp, env));
    if !read_only.is_nil() && interp.has_buffer_id(buffer_id) {
        interp.set_buffer_local_value(buffer_id, "buffer-read-only", read_only);
    }
    result
}

/// GNU `internal-default-process-sentinel'.  Non-running state changes are
/// appended at the process mark without letting a process callback retarget
/// the caller's current buffer or point.
pub(crate) fn internal_default_process_sentinel(
    interp: &mut Interpreter,
    process_id: u64,
    message: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    if matches!(
        interp.process_status_value(process_id).map(|v| v.kind()),
        Some(Kind::Symbol(status)) if matches!(status.as_str(), "run" | "open")
    ) {
        return Ok(());
    }
    let Some(buffer_id) = interp
        .process_buffer_id(process_id)
        .filter(|buffer_id| interp.get_buffer_by_id(*buffer_id).is_some())
    else {
        return Ok(());
    };
    let Some(mark_id) = interp.process_mark_id(process_id) else {
        return Ok(());
    };
    let process_name = interp.process_name(process_id).unwrap_or_default();
    let saved_buffer_id = interp.current_buffer_id();
    let switched = buffer_id != saved_buffer_id;
    if switched {
        interp.set_current_buffer_id(buffer_id)?;
    }
    let old_point = interp.buffer.borrow().point();
    let insert_at = interp
        .marker_position(mark_id)
        .unwrap_or_else(|| interp.current_buffer().point_max());
    interp.buffer.borrow_mut().goto_char(insert_at);
    // process.c's four insertions, each through the change protocol.
    let inserted = insert_with_read_only_cleared(interp, buffer_id, env, |interp, env| {
        for text in ["\nProcess ", process_name.as_str(), " ", message] {
            insert_text_with_hooks(interp, text, None, &[], false, false, env)?;
        }
        Ok(())
    });
    let new_pos = interp.buffer.borrow().point();
    let result = inserted.and_then(|()| interp.set_marker(mark_id, Some(new_pos), Some(buffer_id)));
    let restored_point = if old_point >= insert_at {
        old_point + new_pos.saturating_sub(insert_at)
    } else {
        old_point
    };
    interp.buffer.borrow_mut().goto_char(restored_point);
    if switched {
        interp.set_current_buffer_id(saved_buffer_id)?;
    }
    result
}

/// SSDATA of a string as C code reads it: the internal bytes up to the
/// first NUL.
fn c_string_bytes(bytes: &[u8]) -> &[u8] {
    memchr::memchr(0, bytes).map_or(bytes, |end| &bytes[..end])
}

/// callproc.c add_env: STRING joins BLOCK unless an earlier entry already
/// names the variable STRING defines.  A lone name is kept for now, as the
/// placeholder that keeps later definitions of its variable out.
fn add_env(block: &mut Vec<Vec<u8>>, string: Vec<u8>) {
    let defined = block.iter().any(|existing| {
        for (index, &q) in string.iter().enumerate() {
            match existing.get(index) {
                Some(&p) if p != q => return false,
                Some(_) => {}
                // An earlier lone name: the same variable when STRING's
                // name ends here too.
                None => return q == b'=',
            }
            if q == b'=' {
                return true;
            }
        }
        false
    });
    if !defined {
        block.push(string);
    }
}

/// callproc.c make_environment_block: the child's environment in GNU's
/// order.  PWD, when the Lisp environment has one, comes first and names
/// CURRENT_DIR without its trailing slashes; DISPLAY comes from the
/// selected frame or `initial-environment' when `process-environment'
/// lacks it; then the strings of `process-environment' up to its first
/// non-string, each left out when an earlier entry names its variable.
/// The lone names are dropped last.
pub(crate) fn make_environment_block(
    interp: &Interpreter,
    env: &Env,
    current_dir: &[u8],
) -> Vec<Vec<u8>> {
    let process_environment = interp
        .lookup_var("process-environment", env)
        .unwrap_or(Value::Nil);
    let mut strings = Vec::new();
    let mut cursor = process_environment;
    while let Some((entry, rest)) = cursor.cons_values() {
        let Kind::StringObject(state) = entry.kind() else {
            break;
        };
        strings.push(c_string_bytes(state.borrow().bytes()).to_vec());
        cursor = rest;
    }
    let mut block = Vec::with_capacity(strings.len() + 2);
    if getenv_internal(interp, env, b"PWD", &Value::Nil)
        .ok()
        .flatten()
        .is_some()
    {
        let mut length = current_dir.len();
        while length > 2 && current_dir[length - 1] == b'/' {
            length -= 1;
        }
        let mut pwd = b"PWD=".to_vec();
        pwd.extend_from_slice(c_string_bytes(&current_dir[..length]));
        block.push(pwd);
    }
    let display_specified = strings.iter().any(|string| {
        string
            .strip_prefix(b"DISPLAY")
            .is_some_and(|rest| rest.first().is_none_or(|&byte| byte == b'='))
    });
    if !display_specified {
        let mut display = super::dispatch::frame_display_parameter(interp, &Value::Nil)
            .ok()
            .and_then(|display| string_object_bytes(&display));
        if display.is_none() {
            let initial = interp
                .lookup_var("initial-environment", env)
                .unwrap_or(Value::Nil);
            if initial.is_cons() {
                display = getenv_internal_1(b"DISPLAY", initial).flatten();
            }
        }
        if let Some(display) = display {
            let mut entry = b"DISPLAY=".to_vec();
            entry.extend_from_slice(c_string_bytes(&display));
            add_env(&mut block, entry);
        }
    }
    for string in strings {
        add_env(&mut block, string);
    }
    block.retain(|string| string.contains(&b'='));
    block
}

fn string_object_bytes(value: &Value) -> Option<Vec<u8>> {
    match value.kind() {
        Kind::StringObject(state) => Some(state.borrow().bytes().to_vec()),
        _ => None,
    }
}

/// callproc.c getenv_internal_1: the first string entry of ENVIRONMENT
/// whose bytes start with VARIABLE's.  `VARIABLE=VALUE' gives VALUE's
/// bytes and a lone `VARIABLE' (a negative entry) `Some(None)'; entries of
/// other types are passed over and an improper tail ends the walk.
pub(crate) fn getenv_internal_1(variable: &[u8], environment: Value) -> Option<Option<Vec<u8>>> {
    let mut cursor = environment;
    while let Some((entry, rest)) = cursor.cons_values() {
        if let Kind::StringObject(state) = entry.kind() {
            let state = state.borrow();
            if let Some(tail) = state.bytes().strip_prefix(variable) {
                match tail.split_first() {
                    Some((b'=', value)) => return Some(Some(value.to_vec())),
                    None => return Some(None),
                    Some(_) => {}
                }
            }
        }
        cursor = rest;
    }
    None
}

/// callproc.c getenv_internal: VARIABLE's value in `process-environment'
/// (none for a negative entry), else, for DISPLAY, FRAME's display
/// parameter (FRAME nil: the selected frame's) or `initial-environment's.
pub(crate) fn getenv_internal(
    interp: &Interpreter,
    env: &Env,
    variable: &[u8],
    frame: &Value,
) -> Result<Option<Vec<u8>>, LispError> {
    let process_environment = interp
        .lookup_var("process-environment", env)
        .unwrap_or(Value::Nil);
    if let Some(value) = getenv_internal_1(variable, process_environment) {
        return Ok(value);
    }
    if variable == b"DISPLAY" {
        let display = super::dispatch::frame_display_parameter(interp, frame)?;
        if let Some(display) = string_object_bytes(&display) {
            return Ok(Some(display));
        }
        let initial = interp
            .lookup_var("initial-environment", env)
            .unwrap_or(Value::Nil);
        if let Some(value) = getenv_internal_1(variable, initial) {
            return Ok(value);
        }
    }
    Ok(None)
}

/// call-process's output into DESTINATION's buffer, decoded as callproc.c
/// decodes it.  The process's output stream (OUTPUT) records the coding
/// in `last-coding-system-used' even when it was empty; a separate stderr
/// stream with nothing in it leaves the buffer and the variable alone.
pub(crate) fn append_process_bytes_to_buffer(
    interp: &mut Interpreter,
    destination: &Value,
    bytes: &[u8],
    output: bool,
    operation: &str,
    operation_args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    if bytes.is_empty() && !output {
        return Ok(());
    }
    let target_id = match destination.kind() {
        Kind::T => interp.current_buffer_id(),
        Kind::Buffer(_) => interp.resolve_buffer_id(destination)?,
        value => {
            let Some(name) = string_like(&value.value()) else {
                return Err(LispError::TypeError(
                    "buffer-or-name".into(),
                    destination.type_name(),
                ));
            };
            interp
                .find_buffer(&name.text)
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(&name.text).0)
        }
    };
    let original_id = interp.current_buffer_id();
    if target_id != original_id {
        interp.set_current_buffer_id(target_id)?;
    }
    // callproc.c: coding-system-for-read, else the car of
    // find-operation-coding-system's answer, else of
    // `default-process-coding-system' (nil decodes as undecided); a
    // unibyte buffer reads raw text with its eol type.
    let mut coding = match interp
        .lookup_var("coding-system-for-read", env)
        .filter(|value| !value.is_nil())
    {
        Some(value) => checked_coding_given(interp, &value)?,
        None => None,
    };
    if coding.is_none() {
        let mut lookup_args = Vec::with_capacity(operation_args.len() + 1);
        lookup_args.push(Value::Symbol(operation.into()));
        lookup_args.extend_from_slice(operation_args);
        let operation_coding = find_operation_coding_system_value(interp, &lookup_args, env)?;
        coding = match operation_coding.cons_values() {
            Some((decoding, _)) => checked_coding_given(interp, &decoding)?,
            None => match interp
                .lookup_var("default-process-coding-system", env)
                .and_then(|value| value.cons_values())
            {
                Some((decoding, _)) => checked_coding_given(interp, &decoding)?,
                None => None,
            },
        };
    }
    let multibyte = interp.buffer.borrow().is_multibyte();
    let coding = match coding {
        Some(coding) if !multibyte => raw_text_coding_system(interp, &coding),
        Some(coding) => coding,
        None => "undecided".into(),
    };
    let result = insert_process_output(interp, bytes, &coding, multibyte, env);
    let result = result.map(|coding_used| {
        interp.set_variable(
            "last-coding-system-used",
            Value::Symbol(coding_used.into()),
            env,
        );
    });
    if target_id != original_id {
        interp.set_current_buffer_id(original_id)?;
    }
    result
}

/// callproc.c's insertion of a bufferful the child wrote, at point:
/// prepare_to_modify_buffer there, then either the bytes as they are (a
/// unibyte buffer whose coding needs no decoding: insert_1_both) or
/// decode_coding_c_string into the buffer with the modification hooks
/// inhibited -- its `charset' annotations and post-read conversion
/// included, point left before the text -- after which point moves over
/// the characters produced and signal_after_change reports them.  The
/// whole output is one bufferful; the coding settled on is the value.
fn insert_process_output(
    interp: &mut Interpreter,
    bytes: &[u8],
    coding: &str,
    multibyte: bool,
    env: &mut Env,
) -> Result<String, LispError> {
    let canonical = interp
        .coding_system_canonical_name(coding)
        .unwrap_or_else(|| coding.to_string());
    let inhibit_eol_conversion = interp
        .lookup_var("inhibit-eol-conversion", env)
        .is_some_and(|value| value.is_truthy());
    // CODING_MAY_REQUIRE_DECODING: every coding but a raw-text one with a
    // unix eol and no post-read conversion requires decoding or detection.
    let requires_decoding = interp.coding_system_kind_name(&canonical).as_deref()
        != Some("raw-text")
        || !(inhibit_eol_conversion || coding_eol_type(interp, &canonical) == Some(0))
        || coding_system_property(interp, &canonical, ":post-read-conversion")
            .is_some_and(|function| !function.is_nil());
    // call_process's read loop over the child's output: BUFSIZE bytes a
    // block (CALLPROC_BUFFER_SIZE_MIN, doubling up to the MAX as the
    // output grows), the reads filling all but the last 1024 bytes of it
    // after the carryover, a read of nothing setting
    // CODING_MODE_LAST_BLOCK.  Each block runs the change hooks once and
    // is decoded after what the last one carried over, with the coding
    // the last one settled on.
    const CALLPROC_BUFFER_SIZE_MIN: usize = 16 * 1024;
    const CALLPROC_BUFFER_SIZE_MAX: usize = 4 * CALLPROC_BUFFER_SIZE_MIN;
    let mut bufsize = CALLPROC_BUFFER_SIZE_MIN;
    let mut total_read = 0usize;
    let mut offset = 0usize;
    let mut decoder = super::coding::BlockDecoder::default();
    let mut coding_used = coding.to_string();
    loop {
        let mut nread = decoder.carryover.len();
        let start = offset;
        while nread < bufsize - 1024 {
            let this_read = (bytes.len() - offset).min(bufsize - nread);
            if this_read == 0 {
                decoder.last_block = true;
                break;
            }
            offset += this_read;
            nread += this_read;
            total_read += this_read;
        }
        let block = &bytes[start..offset];
        if nread > 0 {
            let point = interp.buffer.borrow().point();
            coding_used =
                modify_current_buffer(interp, point, point, false, env, |interp, _, env| {
                    let start = interp.buffer.borrow().point();
                    if !multibyte && !requires_decoding {
                        interp.insert_current_buffer(&decode_raw_text_bytes(block));
                        return Ok((
                            Some(AfterChange::new(start, 0, block.len())),
                            coding_used.clone(),
                        ));
                    }
                    let restore =
                        interp.bind_special_dynamic("inhibit-modification-hooks", Value::T, env)?;
                    let decoded = decode_process_bytes_at_point(
                        interp,
                        block,
                        &coding_used,
                        multibyte,
                        &mut decoder,
                        env,
                    );
                    let restored = interp.restore_special_dynamic(restore, env);
                    let (produced, settled) = decoded?;
                    restored?;
                    // Point moves over the characters produced from where
                    // the decoding (a post-read conversion included) left
                    // it.
                    let point = interp.buffer.borrow().point();
                    interp.buffer.borrow_mut().goto_char(point + produced);
                    Ok((Some(AfterChange::new(point, 0, produced)), settled))
                })?;
        }
        if decoder.last_block {
            break;
        }
        if bufsize < CALLPROC_BUFFER_SIZE_MAX && total_read > 32 * bufsize {
            bufsize = (bufsize * 2).min(CALLPROC_BUFFER_SIZE_MAX);
        }
    }
    Ok(coding_used)
}

/// decode_coding_c_string of the block BYTES, after what DECODER carried
/// over, into the current buffer at point, which stays before the text:
/// the characters with produce_charset's
/// annotations, then the coding's post-read conversion called through
/// safe_calln with undo off, point left where it leaves it.  The
/// characters produced (the conversion's additions counted) and the
/// coding settled on.
fn decode_process_bytes_at_point(
    interp: &mut Interpreter,
    bytes: &[u8],
    coding: &str,
    multibyte: bool,
    decoder: &mut super::coding::BlockDecoder,
    env: &mut Env,
) -> Result<(usize, String), LispError> {
    let CodingDecode {
        decoded,
        coding: coding_used,
        ..
    } = super::coding::decode_coding_block(interp, env, bytes, coding, Some(multibyte), decoder)?;
    let mut produced = decoded.char_count();
    let props = charset_spans_to_props(decoded.charsets, &decoded.text, None);
    insert_text_before_point(
        interp,
        &decoded.text,
        props.as_ref(),
        &decoded.extended,
        produced,
        env,
    )?;
    let post_read = coding_system_property(
        interp,
        &interp
            .coding_system_canonical_name(&coding_used)
            .unwrap_or_else(|| coding_used.clone()),
        ":post-read-conversion",
    )
    .filter(|function| !function.is_nil());
    if let Some(function) = post_read {
        let prev_z = interp.buffer.borrow().size_total();
        let undo_list = interp
            .lookup_var("buffer-undo-list", env)
            .unwrap_or(Value::Nil);
        interp.set_variable("buffer-undo-list", Value::T, env);
        let result = interp.safe_funcall(function, &[Value::Integer(produced as i64)], env);
        interp.set_variable("buffer-undo-list", undo_list, env);
        let value = result?;
        if value.integer_value().is_none_or(|count| count < 0) {
            return Err(LispError::WrongTypeArgument("wholenump".into(), value));
        }
        let z = interp.buffer.borrow().size_total();
        produced = (produced as i64 + z as i64 - prev_z as i64).max(0) as usize;
    }
    Ok((produced, coding_used))
}

/// process.c read_process_output's decoding of BYTES a process sent:
/// decode_coding_c_string into a string with the process's decoding
/// coding (for the default filter into a unibyte buffer the raw-text
/// form setup_process_coding_systems gives it), produce_charset's
/// `charset' properties on it; then read_process_output_set_last_coding_
/// system: the coding settled on is `last-coding-system-used' and, when
/// it differs, the process's decoding coding, an encoding coding not yet
/// decided taking it with its eol type completed.
fn decode_process_output(
    interp: &mut Interpreter,
    process_id: u64,
    bytes: &[u8],
    env: &mut Env,
) -> Result<Value, LispError> {
    let (decoding, encoding) = interp
        .process_coding_system(process_id)?
        .cons_values()
        .unwrap_or((Value::Nil, Value::Nil));
    let unibyte_buffer = interp.process_filter(process_id).is_none()
        && interp
            .process_buffer_id(process_id)
            .and_then(|id| interp.get_buffer_by_id(id))
            .is_some_and(|buffer| !buffer.is_multibyte());
    let coding = match checked_coding_given(interp, &decoding)? {
        Some(coding) if unibyte_buffer => raw_text_coding_system(interp, &coding),
        Some(coding) => coding,
        None if unibyte_buffer => "raw-text".into(),
        None => "undecided".into(),
    };
    // decode_coding_c_string through the process's coding struct: the
    // carryover and decoder state the last read left, the read's bytes,
    // and CODING_MODE_LAST_BLOCK once a read found the end.
    let mut decoder = interp.take_process_decoder(process_id);
    let decoded =
        super::coding::decode_coding_block(interp, env, bytes, &coding, None, &mut decoder);
    interp.restore_process_decoder(process_id, decoder);
    let CodingDecode {
        decoded,
        coding: settled,
        for_unibyte,
    } = decoded?;
    let props = charset_spans_to_props(decoded.charsets, &decoded.text, None);
    let text = make_shared_string_value_with_extended_chars(
        decoded.text,
        props,
        !for_unibyte,
        decoded.extended,
    );
    let settled_value = Value::symbol(&settled);
    interp.set_variable("last-coding-system-used", settled_value, env);
    if decoding != settled_value {
        let encoding = if encoding.is_nil() && interp.process_is_live(process_id) {
            super::file_io::inherit_write_coding_eol(interp, settled_value, Value::Nil)?
        } else {
            encoding
        };
        interp.set_process_coding_system(process_id, settled_value, encoding)?;
    }
    Ok(text)
}

/// Deliver the two child streams to their GNU process objects.  A `:stderr'
/// pipe has its own filter/buffer; folding stderr into the primary process
/// loses that observable routing during synchronous send/EOF drains.
pub(crate) fn deliver_process_streams(
    interp: &mut Interpreter,
    process_id: u64,
    stdout: &[u8],
    stderr: &[u8],
    env: &mut Env,
) -> Result<bool, LispError> {
    let stderr_process_id = interp.process_stderr(process_id).unwrap_or(process_id);
    let mut delivered = deliver_process_reads(interp, process_id, stdout, env)?;
    delivered |= deliver_process_reads(interp, stderr_process_id, stderr, env)?;
    // A read that found a pipe's end: read_process_output sets
    // CODING_MODE_LAST_BLOCK and decodes what was carried over.
    let (stdout_eof, stderr_eof) = interp.take_process_pipe_eof(process_id);
    if stdout_eof {
        delivered |= flush_process_decoder(interp, process_id, env)?.1;
    }
    if stderr_eof {
        delivered |= flush_process_decoder(interp, stderr_process_id, env)?.1;
    }
    Ok(delivered)
}

/// read_process_output for each p->readmax bytes of BYTES, read from
/// PROCESS_ID's channel: decoded after the carryover and handed to the
/// filter.  A filter that ends the process stops the reading -- its
/// channel is closed -- and one that stops its output leaves the rest
/// unread for when it resumes.
fn deliver_process_reads(
    interp: &mut Interpreter,
    process_id: u64,
    bytes: &[u8],
    env: &mut Env,
) -> Result<bool, LispError> {
    let readmax = interp.process_read_max(process_id);
    let mut rest = bytes;
    while !rest.is_empty() {
        let (read, after) = rest.split_at(rest.len().min(readmax));
        let output = decode_process_output(interp, process_id, read, env)?;
        deliver_process_output_decoded(interp, process_id, output, env)?;
        rest = after;
        if rest.is_empty() || !interp.process_in_alist(process_id) {
            break;
        }
        if interp.process_output_paused(process_id) {
            interp.unread_process_output(process_id, rest);
            break;
        }
    }
    Ok(!bytes.is_empty())
}

/// read_process_output's read of zero bytes: the first sets
/// CODING_MODE_LAST_BLOCK and decodes the carryover, the decoder flushing
/// what it cannot finish, for the filter.  Whether the read was already
/// the end (CODING_MODE_LAST_BLOCK set, or nothing carried over), and
/// whether anything was delivered.
fn flush_process_decoder(
    interp: &mut Interpreter,
    process_id: u64,
    env: &mut Env,
) -> Result<(bool, bool), LispError> {
    let (was_last, carried) = interp.mark_process_last_block(process_id);
    if was_last {
        return Ok((true, false));
    }
    let output = decode_process_output(interp, process_id, &[], env)?;
    deliver_process_output_decoded(interp, process_id, output, env)?;
    Ok((carried == 0, carried > 0))
}

/// Drain live external process pipes into their buffers/filters.
/// Returns true if any output was delivered.
pub(crate) fn pump_external_process_output(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    pump_external_process_output_for(interp, env, None)
}

fn pump_external_process_output_for(
    interp: &mut Interpreter,
    env: &mut Env,
    only_process_id: Option<u64>,
) -> Result<bool, LispError> {
    let ids = interp.live_external_process_ids();
    let mut progressed = false;
    for process_id in ids {
        if only_process_id.is_some_and(|id| process_id != id) {
            // JUST-THIS-ONE takes the other descriptors out of the wait
            // set, not SIGCHLD: process.c status_notify still reads "any
            // output that remains" from a process whose status changed and
            // then runs its sentinel, whether or not it is wait_proc.  A
            // process that stays alive keeps its output unread.
            interp.refresh_process_id(process_id)?;
            if interp.process_exit_awaits_notification(process_id) {
                let (stdout, stderr) = interp.poll_process_output(process_id)?;
                progressed |= deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
            }
            continue;
        }
        if interp.process_output_paused(process_id) {
            // Status changes still arrive while output is held.  GNU removes
            // only the read descriptor, not SIGCHLD/status observation.
            interp.refresh_process_id(process_id)?;
            continue;
        }
        let (stdout, stderr) = interp.poll_process_output(process_id)?;
        progressed |= deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
    }
    // status_notify walks every process, so a JUST-THIS-ONE wait notifies
    // the others' exits as well.
    for (process_id, event) in interp.take_pending_subprocess_exit_events_for(None) {
        run_process_sentinel(interp, process_id, &event, env)?;
        progressed = true;
    }
    Ok(progressed)
}

// ── Network processes (GNU process.c make-network-process) ──

/// GNU conv_sockaddr_to_lisp: an inet address as [a b c d port] (ipv4)
/// or [w0 .. w7 port] (ipv6).
pub(crate) fn sockaddr_vector(addr: std::net::SocketAddr) -> Value {
    let mut items = Vec::new();
    match addr.ip() {
        std::net::IpAddr::V4(ip) => {
            items.extend(
                ip.octets()
                    .iter()
                    .map(|octet| Value::Integer(*octet as i64)),
            );
        }
        std::net::IpAddr::V6(ip) => {
            items.extend(
                ip.segments()
                    .iter()
                    .map(|segment| Value::Integer(*segment as i64)),
            );
        }
    }
    items.push(Value::Integer(addr.port() as i64));
    Value::vector(items)
}

pub(crate) fn socket_addr_from_value(value: &Value) -> Option<std::net::SocketAddr> {
    let items = vector_or_list_items(value).ok()?;
    let integers = items
        .iter()
        .map(Value::as_integer)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    match integers.as_slice() {
        [a, b, c, d, port] => Some(std::net::SocketAddr::from((
            [
                u8::try_from(*a).ok()?,
                u8::try_from(*b).ok()?,
                u8::try_from(*c).ok()?,
                u8::try_from(*d).ok()?,
            ],
            u16::try_from(*port).ok()?,
        ))),
        [a, b, c, d, e, f, g, h, port] => Some(std::net::SocketAddr::from((
            [
                u16::try_from(*a).ok()?,
                u16::try_from(*b).ok()?,
                u16::try_from(*c).ok()?,
                u16::try_from(*d).ok()?,
                u16::try_from(*e).ok()?,
                u16::try_from(*f).ok()?,
                u16::try_from(*g).ok()?,
                u16::try_from(*h).ok()?,
            ],
            u16::try_from(*port).ok()?,
        ))),
        _ => None,
    }
}

/// plist_get on a proper-list plist value by keyword name.
pub(crate) fn contact_plist_get(plist: &Value, key: &str) -> Value {
    let Ok(items) = plist.to_vec() else {
        return Value::Nil;
    };
    let mut index = 0;
    while index + 1 < items.len() {
        if items[index].as_symbol().is_ok_and(|name| name == key) {
            return items[index + 1];
        }
        index += 2;
    }
    Value::Nil
}

/// plist_put on a flat Vec of plist items: replace KEY's value in place,
/// or append the pair (GNU plist_put appends missing keys at the end).
fn plist_items_put(items: &mut Vec<Value>, key: &str, value: Value) {
    let mut index = 0;
    while index + 1 < items.len() {
        if items[index].as_symbol().is_ok_and(|name| name == key) {
            items[index + 1] = value;
            return;
        }
        index += 2;
    }
    items.push(Value::symbol(key));
    items.push(value);
}

/// GNU Fdelete_process runs status notification synchronously for every
/// process kind.  Marking the sentinel notified before this call prevents
/// the ordinary event pump from delivering the same terminal transition a
/// second time.  Sentinel errors are demoted to a message unless
/// debug-on-error asks for the debugger (exec_sentinel).
pub(crate) fn delete_process_notifying(
    interp: &mut Interpreter,
    process_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    let (event, notify_sentinel) = interp.delete_process(process_id)?;
    if !notify_sentinel {
        return Ok(());
    }
    match run_process_sentinel(interp, process_id, &event, env).map_err(LispError::into_kind) {
        Ok(()) => Ok(()),
        Err(error @ LispErrorKind::Throw(_, _)) => Err(LispError::from(error)),
        Err(error) => {
            if interp
                .lookup_var("debug-on-error", env)
                .is_some_and(|value| value.is_truthy())
            {
                return Err(LispError::from(error));
            }
            let _ = crate::lisp::primitives::call(
                interp,
                "message",
                &[
                    Value::String("error in process sentinel: %S".into()),
                    crate::lisp::eval::error_condition_value(&LispError::from(error.clone())),
                ],
                env,
            );
            Ok(())
        }
    }
}

pub(super) fn network_io_error_detail(error: &std::io::Error) -> String {
    let rendered = error.to_string();
    match rendered.split_once(" (os error") {
        Some((detail, _)) => detail.into(),
        None => rendered,
    }
}

/// Resolve HOST and a named SERVICE together, as process.c does when
/// `:host' is non-nil.  Besides selecting the service's port, this preserves
/// the host platform's `gai_strerror' diagnostic instead of baking one
/// operating system's wording into the runtime.
fn host_service_database_port(
    host: &str,
    service: &str,
    datagram: bool,
    family_ipv4: bool,
    family_ipv6: bool,
) -> Result<i64, LispError> {
    // Emacs passes SSDATA directly to the C resolver, so an embedded NUL
    // terminates the host or service as it would for any other C string.
    let c_string_prefix = |text: &str| {
        let prefix = text
            .as_bytes()
            .split(|byte| *byte == 0)
            .next()
            .unwrap_or_default();
        std::ffi::CString::new(prefix).expect("NUL-free C string prefix")
    };
    let host_c = c_string_prefix(host);
    let service_c = c_string_prefix(service);
    let mut hints = unsafe { std::mem::zeroed::<libc::addrinfo>() };
    hints.ai_family = if family_ipv4 {
        libc::AF_INET
    } else if family_ipv6 {
        libc::AF_INET6
    } else {
        libc::AF_UNSPEC
    };
    hints.ai_socktype = if datagram {
        libc::SOCK_DGRAM
    } else {
        libc::SOCK_STREAM
    };
    let mut addresses = std::ptr::null_mut();
    let status =
        unsafe { libc::getaddrinfo(host_c.as_ptr(), service_c.as_ptr(), &hints, &mut addresses) };
    if status != 0 {
        let detail = unsafe {
            let message = libc::gai_strerror(status);
            if message.is_null() {
                format!("getaddrinfo error {status}")
            } else {
                std::ffi::CStr::from_ptr(message)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        return Err(LispError::Signal(format!("{host}/{service} {detail}")));
    }

    let mut current = addresses;
    let mut port = None;
    while !current.is_null() {
        let entry = unsafe { &*current };
        port = match entry.ai_family {
            libc::AF_INET if !entry.ai_addr.is_null() => {
                let address = unsafe { &*(entry.ai_addr.cast::<libc::sockaddr_in>()) };
                Some(i64::from(u16::from_be(address.sin_port)))
            }
            libc::AF_INET6 if !entry.ai_addr.is_null() => {
                let address = unsafe { &*(entry.ai_addr.cast::<libc::sockaddr_in6>()) };
                Some(i64::from(u16::from_be(address.sin6_port)))
            }
            _ => None,
        };
        if port.is_some() {
            break;
        }
        current = entry.ai_next;
    }
    unsafe { libc::freeaddrinfo(addresses) };
    port.ok_or_else(|| {
        LispError::Signal(format!(
            "make-network-process: no matching address for {host}"
        ))
    })
}

pub(super) fn network_server_error(error: &std::io::Error) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("file-error"),
        Value::string("Cannot bind server socket"),
        Value::string(&network_io_error_detail(error)),
    ]))
}

/// connect_network_socket's loop for a non-blocking client: the first of
/// ADDRESSES whose connect has started (EINPROGRESS) or finished, the
/// address, and the addresses after it; else the last address's error.
#[cfg(unix)]
fn connect_nonblocking(
    addresses: &[std::net::SocketAddr],
) -> std::io::Result<(
    std::net::TcpStream,
    std::net::SocketAddr,
    Vec<std::net::SocketAddr>,
)> {
    use std::os::fd::{FromRawFd, OwnedFd};
    let mut last_error = std::io::Error::from_raw_os_error(libc::EADDRNOTAVAIL);
    for (index, address) in addresses.iter().enumerate() {
        let (family, storage, length) = socket_address(address);
        // SAFETY: socket makes a descriptor or fails; nothing is borrowed.
        let fd = unsafe { libc::socket(family, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            last_error = std::io::Error::last_os_error();
            continue;
        }
        // SAFETY: FD is the socket just made, owned here alone.
        let socket = unsafe { OwnedFd::from_raw_fd(fd) };
        // "Reject file descriptors that would be too large."
        if beyond_fd_setsize(&[fd]) {
            last_error = std::io::Error::from_raw_os_error(libc::EMFILE);
            continue;
        }
        // SAFETY: fcntl on the owned descriptor, as emacs_socket and
        // connect_network_socket set O_NONBLOCK and FD_CLOEXEC.
        unsafe {
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            libc::fcntl(
                fd,
                libc::F_SETFL,
                libc::fcntl(fd, libc::F_GETFL) | libc::O_NONBLOCK,
            );
        }
        // SAFETY: STORAGE holds ADDRESS as a sockaddr of LENGTH bytes.
        let started = unsafe { libc::connect(fd, (&raw const storage).cast(), length) } == 0;
        let error = std::io::Error::last_os_error();
        if started || error.raw_os_error() == Some(libc::EINPROGRESS) {
            let stream = std::net::TcpStream::from(socket);
            return Ok((stream, *address, addresses[index + 1..].to_vec()));
        }
        last_error = error;
    }
    Err(last_error)
}

/// connect_network_socket's loop for a blocking client: the first of
/// ADDRESSES that connects, else the last address's error.  A socket
/// beyond FD_SETSIZE is closed before it connects, and its address fails
/// with EMFILE.
#[cfg(unix)]
fn connect_blocking(addresses: &[std::net::SocketAddr]) -> std::io::Result<std::net::TcpStream> {
    use std::os::fd::{FromRawFd, OwnedFd};
    let mut last_error = std::io::Error::from_raw_os_error(libc::EADDRNOTAVAIL);
    for address in addresses {
        let (family, storage, length) = socket_address(address);
        // SAFETY: socket makes a descriptor or fails; nothing is borrowed.
        let fd = unsafe { libc::socket(family, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            last_error = std::io::Error::last_os_error();
            continue;
        }
        // SAFETY: FD is the socket just made, owned here alone.
        let socket = unsafe { OwnedFd::from_raw_fd(fd) };
        if beyond_fd_setsize(&[fd]) {
            last_error = std::io::Error::from_raw_os_error(libc::EMFILE);
            continue;
        }
        // SAFETY: fcntl on the owned descriptor, as emacs_socket sets
        // FD_CLOEXEC.
        unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
        // SAFETY: STORAGE holds ADDRESS as a sockaddr of LENGTH bytes.
        if unsafe { libc::connect(fd, (&raw const storage).cast(), length) } == 0 {
            return Ok(std::net::TcpStream::from(socket));
        }
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EISCONN) => return Ok(std::net::TcpStream::from(socket)),
            Some(libc::EINTR) => {
                return finish_interrupted_connect(fd).map(|()| std::net::TcpStream::from(socket));
            }
            _ => last_error = error,
        }
    }
    Err(last_error)
}

#[cfg(not(unix))]
fn connect_blocking(addresses: &[std::net::SocketAddr]) -> std::io::Result<std::net::TcpStream> {
    std::net::TcpStream::connect(addresses)
}

#[cfg(not(unix))]
fn connect_nonblocking(
    addresses: &[std::net::SocketAddr],
) -> std::io::Result<(
    std::net::TcpStream,
    std::net::SocketAddr,
    Vec<std::net::SocketAddr>,
)> {
    let stream = std::net::TcpStream::connect(addresses)?;
    stream.set_nonblocking(true)?;
    let remote = stream.peer_addr()?;
    Ok((stream, remote, Vec::new()))
}

/// ADDRESS as the sockaddr connect takes: its family, the bytes, their
/// length.
#[cfg(unix)]
fn socket_address(
    address: &std::net::SocketAddr,
) -> (libc::c_int, libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: an all-zero sockaddr_storage is an empty address.
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match address {
        std::net::SocketAddr::V4(v4) => {
            // SAFETY: sockaddr_storage holds and aligns any sockaddr.
            let inet = unsafe { &mut *(&raw mut storage).cast::<libc::sockaddr_in>() };
            inet.sin_family = libc::AF_INET as libc::sa_family_t;
            inet.sin_port = v4.port().to_be();
            inet.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            let length = std::mem::size_of::<libc::sockaddr_in>();
            #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
            {
                inet.sin_len = length as u8;
            }
            (libc::AF_INET, storage, length as libc::socklen_t)
        }
        std::net::SocketAddr::V6(v6) => {
            // SAFETY: as above.
            let inet6 = unsafe { &mut *(&raw mut storage).cast::<libc::sockaddr_in6>() };
            inet6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            inet6.sin6_port = v6.port().to_be();
            inet6.sin6_flowinfo = v6.flowinfo();
            inet6.sin6_addr.s6_addr = v6.ip().octets();
            inet6.sin6_scope_id = v6.scope_id();
            let length = std::mem::size_of::<libc::sockaddr_in6>();
            #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
            {
                inet6.sin6_len = length as u8;
            }
            (libc::AF_INET6, storage, length as libc::socklen_t)
        }
    }
}

/// connect_network_socket's report_file_errno ("make server process
/// failed", contact, EMFILE) for a server socket beyond FD_SETSIZE.
fn network_server_fd_setsize_error(error: &std::io::Error, args: &[Value]) -> LispError {
    let mut data = vec![
        Value::symbol("file-error"),
        Value::string("make server process failed"),
        Value::string(&network_io_error_detail(error)),
    ];
    data.extend_from_slice(args);
    LispError::SignalValue(Value::list(data))
}

/// connect_network_socket's loop for a local client: its socket, closed
/// with EMFILE when it lies beyond FD_SETSIZE, connected to PATH.
#[cfg(unix)]
fn connect_local(path: &str) -> std::io::Result<std::os::unix::net::UnixStream> {
    use std::os::fd::{FromRawFd, OwnedFd};
    // SAFETY: an all-zero sockaddr_un is an empty address.
    let mut storage: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if path.len() >= storage.sun_path.len() {
        return Err(std::io::Error::from_raw_os_error(libc::ENAMETOOLONG));
    }
    storage.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (slot, byte) in storage.sun_path.iter_mut().zip(path.as_bytes()) {
        *slot = *byte as libc::c_char;
    }
    let length =
        (std::mem::offset_of!(libc::sockaddr_un, sun_path) + path.len() + 1) as libc::socklen_t;
    // SAFETY: socket makes a descriptor or fails; nothing is borrowed.
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: FD is the socket just made, owned here alone.
    let socket = socket_within_fd_setsize(unsafe { OwnedFd::from_raw_fd(fd) })?;
    // SAFETY: fcntl on the owned descriptor, as emacs_socket sets
    // FD_CLOEXEC.
    unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    // SAFETY: STORAGE holds the address in LENGTH bytes.
    if unsafe { libc::connect(fd, (&raw const storage).cast(), length) } != 0 {
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EISCONN) => {}
            Some(libc::EINTR) => finish_interrupted_connect(fd)?,
            _ => return Err(error),
        }
    }
    Ok(std::os::unix::net::UnixStream::from(socket))
}

/// connect_network_socket's wait for a connect a signal interrupted,
/// which goes on and cannot be made again: pselect for the socket's
/// writability, then its SO_ERROR says how it ended.
#[cfg(unix)]
fn finish_interrupted_connect(fd: std::os::fd::RawFd) -> std::io::Result<()> {
    let mut poll = libc::pollfd {
        fd,
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: POLL describes the one live descriptor.
    while unsafe { libc::poll(&mut poll, 1, -1) } < 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(error);
        }
    }
    let mut code: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: CODE and SIZE are the int SO_ERROR fills in.
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            (&raw mut code).cast(),
            &mut size,
        )
    } < 0
    {
        return Err(std::io::Error::last_os_error());
    }
    if code == 0 {
        Ok(())
    } else {
        Err(std::io::Error::from_raw_os_error(code))
    }
}

pub(super) fn network_client_error(error: &std::io::Error, args: &[Value]) -> LispError {
    let condition = if error.kind() == std::io::ErrorKind::NotFound {
        "file-missing"
    } else {
        "file-error"
    };
    let mut data = vec![
        Value::symbol(condition),
        Value::string("make client process failed"),
        Value::string(&network_io_error_detail(error)),
    ];
    data.extend_from_slice(args);
    LispError::SignalValue(Value::list(data))
}

fn activate_network_client(
    interp: &mut Interpreter,
    process: Value,
    nowait: bool,
    untried: Vec<std::net::SocketAddr>,
    tls_parameters: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let process_id = interp.resolve_process_id(&process)?;
    if !tls_parameters.is_nil() {
        interp.set_process_gnutls_boot_parameters(process_id, *tls_parameters);
    }
    if nowait {
        interp.mark_network_process_connecting(process_id, untried);
        return Ok(process);
    }
    if !tls_parameters.is_nil() {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match progress_async_gnutls(interp, process_id)? {
                AsyncGnuTlsProgress::NotRequested | AsyncGnuTlsProgress::Ready => break,
                AsyncGnuTlsProgress::Pending if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                AsyncGnuTlsProgress::Pending => {
                    return Err(LispError::Signal("GnuTLS handshake timed out".into()));
                }
                AsyncGnuTlsProgress::Failed(error) => {
                    return Err(LispError::Signal(format!(
                        "GnuTLS negotiation failed: {error:?}"
                    )));
                }
            }
        }
    }
    run_process_sentinel(interp, process_id, "open\n", env)?;
    Ok(process)
}

/// Create a network server (`:server t`) or client stream.  emaxx models
/// the subset erc-d exercises: a local/ipv4 TCP server with :filter,
/// :sentinel and :log, plus TCP client streams.
pub(crate) fn make_network_process(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Ok(Value::Nil);
    }
    if !args.len().is_multiple_of(2) {
        return Err(LispError::WrongNumberOfArgs(
            "make-network-process".into(),
            args.len(),
        ));
    }
    let mut name = None;
    let mut buffer_id = None;
    let mut filter = None;
    let mut sentinel = None;
    let mut log = None;
    let mut plist = Value::Nil;
    let mut is_server = false;
    let mut datagram = false;
    let mut family_local = false;
    let mut family_ipv4 = false;
    let mut family_ipv6 = false;
    let mut host_local = false;
    let mut nowait = false;
    let mut tls_parameters = Value::Nil;
    let mut coding = Value::Nil;
    let mut host: Option<String> = None;
    let mut service: Option<i64> = None;
    let mut service_path: Option<String> = None;

    for pair in args.as_chunks::<2>().0 {
        let key = pair[0].as_symbol()?;
        let value = &pair[1];
        match key {
            ":name" => {
                name = Some(match string_like(value) {
                    Some(text) => text.text,
                    None => return Err(LispError::Signal(":name value not a string".into())),
                });
            }
            ":buffer" => buffer_id = process_buffer_target(interp, value)?,
            ":filter" => filter = (!value.is_nil()).then_some(*value),
            ":sentinel" => sentinel = (!value.is_nil()).then_some(*value),
            ":log" => log = (!value.is_nil()).then_some(*value),
            ":plist" => plist = *value,
            ":server" => is_server = value.is_truthy(),
            ":type" => match value.kind() {
                Kind::Nil => datagram = false,
                Kind::Symbol(kind) if kind == "datagram" => datagram = true,
                _ => return Err(LispError::Signal("Unsupported connection type".into())),
            },
            ":nowait" => nowait = value.is_truthy(),
            ":coding" => coding = *value,
            ":tls-parameters" => {
                value
                    .to_vec()
                    .map_err(|_| wrong_type_argument("listp", *value))?;
                tls_parameters = *value;
            }
            ":family" => {
                family_local = matches!(value.kind(), Kind::Symbol(symbol) if symbol == "local");
                family_ipv4 = matches!(value.kind(), Kind::Symbol(symbol) if symbol == "ipv4");
                family_ipv6 = matches!(value.kind(), Kind::Symbol(symbol) if symbol == "ipv6");
            }
            ":host" => {
                host = match value.kind() {
                    Kind::Nil => None,
                    Kind::Symbol(symbol) if symbol == "local" => {
                        host_local = true;
                        None
                    }
                    _ => Some(string_text(value)?),
                }
            }
            ":service" => {
                service = match value.kind() {
                    // `:service t' asks the OS to pick a free port.
                    Kind::T => Some(0),
                    Kind::Integer(port) => Some(port),
                    Kind::StringObject(_) => {
                        let text = string_text(value)?;
                        // `:family local' names a socket file, not a port.
                        service_path = Some(text.clone());
                        text.parse::<i64>().ok()
                    }
                    _ => None,
                }
            }
            _ => {}
        }
    }

    let name = name.ok_or_else(|| LispError::Signal("Missing :name keyword parameter".into()))?;

    let inherit_coding_system = buffer_id.is_some()
        && interp
            .lookup_var("inherit-process-coding-system", env)
            .is_some_and(|value| value.is_truthy());
    if host_local {
        host = Some(if family_ipv6 { "::1" } else { "127.0.0.1" }.into());
    }
    // process.c supplies an explicit loopback host when :host is nil, then
    // resolves that host and a non-numeric :service together through
    // getaddrinfo with the socket type as a hint.  This is observable in
    // both the selected port and the platform's error wording.
    if service.is_none()
        && !family_local
        && let Some(text) = service_path.as_deref()
    {
        let default_host = if family_ipv6 { "::1" } else { "127.0.0.1" };
        service = Some(host_service_database_port(
            host.as_deref().unwrap_or(default_host),
            text,
            datagram,
            family_ipv4,
            family_ipv6,
        )?);
    }
    let buffer = buffer_id
        .and_then(|id| interp.buffer_value(id))
        .unwrap_or(Value::Nil);
    let (decoding, encoding) = connection_coding_systems(
        interp,
        env,
        ConnectionCoding::Network {
            name: Value::string(&name),
            host: plist_member_value(args, ":host").unwrap_or(Value::Nil),
            service: plist_member_value(args, ":service").unwrap_or(Value::Nil),
        },
        coding,
        buffer,
    )?;
    let name = interp.unique_process_name(&name);
    // GNU keeps the original keyword plist as the process's contact info
    // (p->childp), updating :service and appending the resolved :local /
    // :remote address vectors after the socket is set up.
    let mut contact_items: Vec<Value> = args.to_vec();

    if datagram {
        if family_local {
            return Err(LispError::Signal(
                "Datagram local sockets are not supported".into(),
            ));
        }
        let address_host = host
            .clone()
            .unwrap_or_else(|| if family_ipv6 { "::1" } else { "127.0.0.1" }.into());
        let address_port = service.unwrap_or(0) as u16;
        let address =
            std::net::ToSocketAddrs::to_socket_addrs(&(address_host.as_str(), address_port))
                .map_err(|error| {
                    if is_server {
                        network_server_error(&error)
                    } else {
                        network_client_error(&error, args)
                    }
                })?
                .find(|address| {
                    (!family_ipv4 || address.is_ipv4()) && (!family_ipv6 || address.is_ipv6())
                })
                .ok_or_else(|| {
                    LispError::Signal(format!(
                        "make-network-process: no matching address for {address_host}"
                    ))
                })?;
        let socket = if is_server {
            std::net::UdpSocket::bind(address)
        } else {
            let local = match address {
                std::net::SocketAddr::V4(_) => std::net::SocketAddr::from(([0, 0, 0, 0], 0)),
                std::net::SocketAddr::V6(_) => {
                    std::net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0))
                }
            };
            std::net::UdpSocket::bind(local)
        }
        .and_then(socket_within_fd_setsize)
        .map_err(|error| {
            if is_server {
                network_server_error(&error)
            } else {
                network_client_error(&error, args)
            }
        })?;
        socket.set_nonblocking(true).map_err(|error| {
            if is_server {
                network_server_error(&error)
            } else {
                network_client_error(&error, args)
            }
        })?;
        let local_addr = socket.local_addr().ok();
        let remote = (!is_server).then_some(address);
        if let Some(local_addr) = local_addr {
            if is_server {
                plist_items_put(
                    &mut contact_items,
                    ":service",
                    Value::Integer(local_addr.port().into()),
                );
                plist_items_put(&mut contact_items, ":local", sockaddr_vector(local_addr));
            } else {
                let unspecified = match local_addr {
                    std::net::SocketAddr::V4(_) => std::net::SocketAddr::from(([0, 0, 0, 0], 0)),
                    std::net::SocketAddr::V6(_) => {
                        std::net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0))
                    }
                };
                plist_items_put(&mut contact_items, ":local", sockaddr_vector(unspecified));
            }
        }
        if let Some(remote) = remote {
            plist_items_put(&mut contact_items, ":remote", sockaddr_vector(remote));
        }
        let process = interp.create_network_process(
            &name,
            buffer_id,
            inherit_coding_system,
            filter,
            sentinel,
            log,
            plist,
            Some(crate::lisp::eval::NetworkRuntime::Datagram { socket, remote }),
            host,
            service,
            None,
            None,
            Value::list(contact_items),
            decoding,
            encoding,
        )?;
        if !is_server {
            let process_id = interp.resolve_process_id(&process)?;
            run_process_sentinel(interp, process_id, "open\n", env)?;
        }
        return Ok(process);
    }

    if family_local {
        // `:family local' — a unix domain socket named by :service.  GNU
        // conv_sockaddr_to_lisp yields the PATH string for :local/:remote
        // (a client's own end is unnamed, hence "").
        let path = service_path.ok_or_else(|| {
            LispError::Signal("make-network-process: local family needs a :service path".into())
        })?;
        let sun_path_capacity = std::mem::size_of::<libc::sockaddr_un>()
            - std::mem::offset_of!(libc::sockaddr_un, sun_path);
        if path.len() >= sun_path_capacity {
            return Err(LispError::Signal("Service name too long".into()));
        }
        if is_server {
            let listener = std::os::unix::net::UnixListener::bind(&path)
                .map_err(|error| network_server_error(&error))?;
            let listener = socket_within_fd_setsize(listener)
                .map_err(|error| network_server_fd_setsize_error(&error, args))?;
            listener
                .set_nonblocking(true)
                .map_err(|error| network_server_error(&error))?;
            plist_items_put(
                &mut contact_items,
                ":local",
                Value::String(path.clone().into()),
            );
            return interp.create_network_process(
                &name,
                buffer_id,
                inherit_coding_system,
                filter,
                sentinel,
                log,
                plist,
                Some(crate::lisp::eval::NetworkRuntime::UnixListener(listener)),
                Some(path),
                None,
                None,
                None,
                Value::list(contact_items),
                decoding,
                encoding,
            );
        }
        let stream = connect_local(&path).map_err(|error| network_client_error(&error, args))?;
        stream
            .set_nonblocking(true)
            .map_err(|error| network_client_error(&error, args))?;
        plist_items_put(
            &mut contact_items,
            ":remote",
            Value::String(path.clone().into()),
        );
        plist_items_put(
            &mut contact_items,
            ":local",
            Value::String(String::new().into()),
        );
        let process = interp.create_network_process(
            &name,
            buffer_id,
            inherit_coding_system,
            filter,
            sentinel,
            log,
            plist,
            Some(crate::lisp::eval::NetworkRuntime::UnixStream(stream)),
            Some(path),
            None,
            None,
            None,
            Value::list(contact_items),
            decoding,
            encoding,
        )?;
        return activate_network_client(interp, process, nowait, Vec::new(), &tls_parameters, env);
    }

    if is_server {
        let bind_host = host
            .clone()
            .unwrap_or_else(|| if family_ipv6 { "::1" } else { "127.0.0.1" }.into());
        let bind_port = service.unwrap_or(0) as u16;
        let listener = if family_ipv4 || family_ipv6 {
            let address =
                std::net::ToSocketAddrs::to_socket_addrs(&(bind_host.as_str(), bind_port))
                    .map_err(|error| network_server_error(&error))?
                    .find(|address| {
                        (!family_ipv4 || address.is_ipv4()) && (!family_ipv6 || address.is_ipv6())
                    })
                    .ok_or_else(|| {
                        LispError::Signal(format!(
                            "make-network-process: no requested-family address for {bind_host}"
                        ))
                    })?;
            std::net::TcpListener::bind(address)
        } else {
            std::net::TcpListener::bind((bind_host.as_str(), bind_port))
        }
        .map_err(|error| network_server_error(&error))?;
        let listener = socket_within_fd_setsize(listener)
            .map_err(|error| network_server_fd_setsize_error(&error, args))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| network_server_error(&error))?;
        let local_addr = listener.local_addr().ok();
        let bound_port = local_addr.map(|addr| addr.port() as i64).or(service);
        if let Some(port) = bound_port {
            plist_items_put(&mut contact_items, ":service", Value::Integer(port));
        }
        if let Some(addr) = local_addr {
            plist_items_put(&mut contact_items, ":local", sockaddr_vector(addr));
        }
        interp.create_network_process(
            &name,
            buffer_id,
            inherit_coding_system,
            filter,
            sentinel,
            log,
            plist,
            Some(crate::lisp::eval::NetworkRuntime::Listener(listener)),
            host,
            bound_port,
            None,
            None,
            Value::list(contact_items),
            decoding,
            encoding,
        )
    } else {
        let connect_host = host
            .clone()
            .unwrap_or_else(|| if family_ipv6 { "::1" } else { "127.0.0.1" }.into());
        let connect_port = service.unwrap_or(0) as u16;
        let addresses =
            std::net::ToSocketAddrs::to_socket_addrs(&(connect_host.as_str(), connect_port))
                .map_err(|error| network_client_error(&error, args))?
                .filter(|address| {
                    (!family_ipv4 || address.is_ipv4()) && (!family_ipv6 || address.is_ipv6())
                })
                .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(LispError::Signal(format!(
                "make-network-process: no requested-family address for {connect_host}"
            )));
        }
        // process.c:connect_network_socket tries the getaddrinfo results
        // in turn.  A blocking client connects before its process exists,
        // and a failure is an error.  A non-blocking one (`:nowait t')
        // takes the first address whose connect starts, and its process
        // waits as (connect . ADDRINFOS) until the event loop sees the
        // connect end; when none starts, the process is made failed.
        let (stream, remote, untried, failure) = if nowait {
            match connect_nonblocking(&addresses) {
                Ok((stream, remote, untried)) => (Some(stream), Some(remote), untried, None),
                Err(error) => (None, None, Vec::new(), Some(error)),
            }
        } else {
            let stream =
                connect_blocking(&addresses).map_err(|error| network_client_error(&error, args))?;
            stream
                .set_nonblocking(true)
                .map_err(|error| network_client_error(&error, args))?;
            let remote = stream.peer_addr().ok();
            (Some(stream), remote, Vec::new(), None)
        };
        if let Some(addr) = remote {
            plist_items_put(&mut contact_items, ":remote", sockaddr_vector(addr));
        }
        if let Some(addr) = stream.as_ref().and_then(|stream| stream.local_addr().ok()) {
            plist_items_put(&mut contact_items, ":local", sockaddr_vector(addr));
        }
        let process = interp.create_network_process(
            &name,
            buffer_id,
            inherit_coding_system,
            filter,
            sentinel,
            log,
            plist,
            stream.map(crate::lisp::eval::NetworkRuntime::Stream),
            host,
            service,
            None,
            None,
            Value::list(contact_items),
            decoding,
            encoding,
        )?;
        if let Some(error) = failure {
            let process_id = interp.resolve_process_id(&process)?;
            interp.fail_network_connect(process_id, error.raw_os_error().unwrap_or(0));
            return Ok(process);
        }
        // GNU runs the sentinel with "open\n" only after any requested TLS
        // negotiation has completed.
        activate_network_client(interp, process, nowait, untried, &tls_parameters, env)
    }
}

fn plist_member_value(items: &[Value], key: &str) -> Option<Value> {
    items.as_chunks::<2>().0.iter().find_map(|pair| {
        pair[0]
            .as_symbol()
            .is_ok_and(|candidate| candidate == key)
            .then(|| pair[1])
    })
}

fn serial_data_bits(value: &Value) -> Result<serialport::DataBits, LispError> {
    match value.kind() {
        Kind::Nil | Kind::Integer(8) => Ok(serialport::DataBits::Eight),
        Kind::Integer(7) => Ok(serialport::DataBits::Seven),
        _ => Err(LispError::Signal(
            ":bytesize must be nil (8), 7, or 8".into(),
        )),
    }
}

fn serial_parity(value: &Value) -> Result<serialport::Parity, LispError> {
    match value.kind() {
        Kind::Nil => Ok(serialport::Parity::None),
        Kind::Symbol(symbol) if symbol == "even" => Ok(serialport::Parity::Even),
        Kind::Symbol(symbol) if symbol == "odd" => Ok(serialport::Parity::Odd),
        _ => Err(LispError::Signal(
            ":parity must be nil (no parity), `even', or `odd'".into(),
        )),
    }
}

fn serial_stop_bits(value: &Value) -> Result<serialport::StopBits, LispError> {
    match value.kind() {
        Kind::Nil | Kind::Integer(1) => Ok(serialport::StopBits::One),
        Kind::Integer(2) => Ok(serialport::StopBits::Two),
        _ => Err(LispError::Signal(
            ":stopbits must be nil (1 stopbit), 1, or 2".into(),
        )),
    }
}

fn serial_flow_control(value: &Value) -> Result<serialport::FlowControl, LispError> {
    match value.kind() {
        Kind::Nil => Ok(serialport::FlowControl::None),
        Kind::Symbol(symbol) if symbol == "hw" => Ok(serialport::FlowControl::Hardware),
        Kind::Symbol(symbol) if symbol == "sw" => Ok(serialport::FlowControl::Software),
        _ => Err(LispError::Signal(
            ":flowcontrol must be nil (no flowcontrol), `hw', or `sw'".into(),
        )),
    }
}

struct SerialConfiguration {
    speed: u32,
    data_bits: serialport::DataBits,
    parity: serialport::Parity,
    stop_bits: serialport::StopBits,
    flow_control: serialport::FlowControl,
}

fn serial_configuration(
    updates: &[Value],
    stored_contact: &Value,
) -> Result<(SerialConfiguration, Value), LispError> {
    let mut contact_items = stored_contact.to_vec()?;
    let setting = |key: &str, default: Value| {
        let value = plist_member_value(updates, key)
            .unwrap_or_else(|| contact_plist_get(stored_contact, key));
        if value.is_nil() { default } else { value }
    };
    let speed_value = plist_member_value(updates, ":speed")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":speed"));
    let speed = u32::try_from(speed_value.as_integer()?)
        .map_err(|_| LispError::Signal("Unsupported speed".into()))?;
    let bytesize = setting(":bytesize", Value::Integer(8));
    let parity = plist_member_value(updates, ":parity")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":parity"));
    let stopbits = setting(":stopbits", Value::Integer(1));
    let flowcontrol = plist_member_value(updates, ":flowcontrol")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":flowcontrol"));
    let config = SerialConfiguration {
        speed,
        data_bits: serial_data_bits(&bytesize)?,
        parity: serial_parity(&parity)?,
        stop_bits: serial_stop_bits(&stopbits)?,
        flow_control: serial_flow_control(&flowcontrol)?,
    };
    plist_items_put(&mut contact_items, ":speed", Value::Integer(speed.into()));
    plist_items_put(&mut contact_items, ":bytesize", bytesize);
    plist_items_put(&mut contact_items, ":parity", parity);
    plist_items_put(&mut contact_items, ":stopbits", stopbits);
    plist_items_put(&mut contact_items, ":flowcontrol", flowcontrol);
    let parity_summary = match parity.kind() {
        Kind::Symbol(symbol) if symbol == "even" => 'E',
        Kind::Symbol(symbol) if symbol == "odd" => 'O',
        _ => 'N',
    };
    plist_items_put(
        &mut contact_items,
        ":summary",
        Value::String(
            format!(
                "{}{parity_summary}{}",
                bytesize.as_integer()?,
                stopbits.as_integer()?
            )
            .into(),
        ),
    );
    Ok((config, Value::list(contact_items)))
}

fn open_serial_port(path: &str) -> Result<serialport::TTYPort, LispError> {
    #[cfg(unix)]
    {
        use std::os::fd::IntoRawFd;
        use std::os::unix::fs::OpenOptionsExt;

        use std::os::fd::AsRawFd;
        // sysdep.c:serial_open's report_file_error, then
        // Fmake_serial_process's refusal of a port beyond FD_SETSIZE.
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOCTTY | libc::O_NONBLOCK)
            .open(path)
            .map_err(|error| file_operation_error("Opening serial port", &error, path))?;
        if beyond_fd_setsize(&[file.as_raw_fd()]) {
            return Err(file_operation_error(
                "Opening serial port",
                &std::io::Error::from_raw_os_error(libc::EMFILE),
                path,
            ));
        }
        // SAFETY: ownership of the descriptor is transferred from File to
        // TTYPort exactly once.  This path intentionally does not touch
        // termios: GNU's documented :speed nil contract preserves it.
        let mut port = unsafe { serialport::TTYPort::from_raw_fd(file.into_raw_fd()) };
        serialport::SerialPort::set_timeout(&mut port, Duration::ZERO)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        Ok(port)
    }

    #[cfg(not(unix))]
    Err(LispError::Signal(
        "Serial ports are unsupported on this platform".into(),
    ))
}

#[cfg(unix)]
fn serial_speed(speed: u32) -> libc::speed_t {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        speed as libc::speed_t
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        match speed {
            0 => libc::B0,
            50 => libc::B50,
            75 => libc::B75,
            110 => libc::B110,
            134 => libc::B134,
            150 => libc::B150,
            200 => libc::B200,
            300 => libc::B300,
            600 => libc::B600,
            1200 => libc::B1200,
            1800 => libc::B1800,
            2400 => libc::B2400,
            4800 => libc::B4800,
            9600 => libc::B9600,
            19200 => libc::B19200,
            38400 => libc::B38400,
            57600 => libc::B57600,
            115200 => libc::B115200,
            230400 => libc::B230400,
            _ => speed as libc::speed_t,
        }
    }
}

#[cfg(unix)]
fn configure_serial_descriptor(
    fd: std::os::fd::RawFd,
    configuration: &SerialConfiguration,
) -> Result<(), LispError> {
    let mut attributes = std::mem::MaybeUninit::<libc::termios>::uninit();
    // SAFETY: FD belongs to the live serial runtime and tcgetattr initializes
    // the termios structure on success.
    if unsafe { libc::tcgetattr(fd, attributes.as_mut_ptr()) } != 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: tcgetattr succeeded above.
    let mut attributes = unsafe { attributes.assume_init() };
    // SAFETY: attributes is initialized and exclusively borrowed.
    unsafe { libc::cfmakeraw(&mut attributes) };
    attributes.c_cflag |= libc::CLOCAL | libc::CREAD;
    // SAFETY: cfsetspeed only mutates the initialized termios structure.
    if unsafe { libc::cfsetspeed(&mut attributes, serial_speed(configuration.speed)) } != 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    attributes.c_cflag &= !libc::CSIZE;
    attributes.c_cflag |= match configuration.data_bits {
        serialport::DataBits::Seven => libc::CS7,
        serialport::DataBits::Eight => libc::CS8,
        _ => unreachable!("serial parser accepts only seven or eight data bits"),
    };
    attributes.c_cflag &= !(libc::PARENB | libc::PARODD);
    attributes.c_iflag &= !(libc::IGNPAR | libc::INPCK);
    match configuration.parity {
        serialport::Parity::None => {}
        serialport::Parity::Even => {
            attributes.c_cflag |= libc::PARENB;
            attributes.c_iflag |= libc::IGNPAR | libc::INPCK;
        }
        serialport::Parity::Odd => {
            attributes.c_cflag |= libc::PARENB | libc::PARODD;
            attributes.c_iflag |= libc::IGNPAR | libc::INPCK;
        }
    }
    attributes.c_cflag &= !libc::CSTOPB;
    if configuration.stop_bits == serialport::StopBits::Two {
        attributes.c_cflag |= libc::CSTOPB;
    }
    attributes.c_cflag &= !libc::CRTSCTS;
    attributes.c_iflag &= !(libc::IXON | libc::IXOFF);
    match configuration.flow_control {
        serialport::FlowControl::None => {}
        serialport::FlowControl::Hardware => attributes.c_cflag |= libc::CRTSCTS,
        serialport::FlowControl::Software => attributes.c_iflag |= libc::IXON | libc::IXOFF,
    }
    // SAFETY: FD remains live and tcsetattr reads the initialized structure.
    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &attributes) } != 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok(())
}

fn serial_process_designator(interp: &mut Interpreter, args: &[Value]) -> Result<u64, LispError> {
    let requested = [":process", ":name", ":buffer", ":port"]
        .into_iter()
        .find_map(|key| plist_member_value(args, key).filter(Value::is_truthy));
    let process = match requested.as_ref().map(|v| v.kind()) {
        None => interp.process_value_for_buffer(interp.current_buffer_id()),
        Some(process @ Kind::Record(_)) => Some(process.value()),
        Some(value) if string_like(&value.value()).is_some() => {
            let text = string_text(&value.value())?;
            interp
                .find_process_id_by_name(&text)
                .map(|id| interp.record_value(id))
                .or_else(|| {
                    interp
                        .resolve_buffer_id(&value.value())
                        .ok()
                        .and_then(|buffer_id| interp.process_value_for_buffer(buffer_id))
                })
        }
        Some(value) => interp
            .resolve_buffer_id(&value.value())
            .ok()
            .and_then(|buffer_id| interp.process_value_for_buffer(buffer_id)),
    }
    .ok_or_else(|| wrong_type_argument("processp", requested.unwrap_or(Value::Nil)))?;
    let process_id = interp.resolve_process_id(&process)?;
    if !interp.is_serial_process(process_id) {
        return Err(LispError::Signal("Not a serial process".into()));
    }
    Ok(process_id)
}

pub(crate) fn serial_process_configure(
    interp: &mut Interpreter,
    args: &[Value],
) -> Result<Value, LispError> {
    let process_id = serial_process_designator(interp, args)?;
    let stored_contact = interp
        .process_contact_plist(process_id)
        .unwrap_or(Value::Nil);
    if contact_plist_get(&stored_contact, ":speed").is_nil() {
        return Ok(Value::Nil);
    }
    let (configuration, contact) = serial_configuration(args, &stored_contact)?;
    #[cfg(unix)]
    configure_serial_descriptor(interp.serial_process_fd(process_id)?, &configuration)?;
    interp.set_process_contact_plist(process_id, contact)?;
    Ok(Value::Nil)
}

pub(crate) fn make_serial_process(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Ok(Value::Nil);
    }
    let port_value = plist_member_value(args, ":port")
        .filter(Value::is_truthy)
        .ok_or_else(|| LispError::Signal("No port specified".into()))?;
    let port_name = string_text(&port_value)?;
    let Some(speed_value) = plist_member_value(args, ":speed") else {
        return Err(LispError::Signal(":speed not specified".into()));
    };
    let speed = if speed_value.is_nil() {
        None
    } else {
        Some(
            u32::try_from(speed_value.as_integer()?)
                .map_err(|_| LispError::Signal("Unsupported speed".into()))?,
        )
    };
    let serial = open_serial_port(&port_name)?;
    let contact = Value::list(args.iter().cloned());
    let contact = if speed.is_some() {
        let (configuration, contact) = serial_configuration(args, &contact)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            configure_serial_descriptor(serial.as_raw_fd(), &configuration)?;
        }
        contact
    } else {
        contact
    };
    let name = plist_member_value(args, ":name")
        .filter(Value::is_truthy)
        .map(|value| string_text(&value))
        .transpose()?
        .unwrap_or_else(|| port_name.clone());
    let buffer = plist_member_value(args, ":buffer")
        .filter(Value::is_truthy)
        .unwrap_or_else(|| Value::String(name.clone().into()));
    let buffer_id =
        process_buffer_target(interp, &buffer)?.expect("a serial process always has a buffer");
    let filter = plist_member_value(args, ":filter").filter(Value::is_truthy);
    let sentinel = plist_member_value(args, ":sentinel").filter(Value::is_truthy);
    let plist = plist_member_value(args, ":plist").unwrap_or(Value::Nil);
    let coding = plist_member_value(args, ":coding").unwrap_or(Value::Nil);
    let buffer_value = interp.buffer_value(buffer_id).unwrap_or(Value::Nil);
    let (decoding, encoding) =
        connection_coding_systems(interp, env, ConnectionCoding::Serial, coding, buffer_value)?;
    let noquery = plist_member_value(args, ":noquery").is_some_and(|value| value.is_truthy());
    let stopped = plist_member_value(args, ":stop").is_some_and(|value| value.is_truthy());
    let process = interp.create_serial_process(
        &name,
        buffer_id,
        filter,
        sentinel,
        plist,
        crate::lisp::eval::SerialRuntime { port: serial },
        contact,
        decoding,
        encoding,
        !noquery,
        stopped,
    )?;
    let process_id = interp.resolve_process_id(&process)?;
    let inherit = coding.is_nil()
        && interp
            .lookup_var("inherit-process-coding-system", env)
            .is_some_and(|value| value.is_truthy());
    interp.set_process_inherit_coding_system_flag(process_id, inherit)?;
    Ok(process)
}

fn run_process_sentinel(
    interp: &mut Interpreter,
    process_id: u64,
    event: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Some(sentinel) = interp.process_sentinel(process_id) {
        call_function_value(
            interp,
            &sentinel,
            &[
                interp.record_value(process_id),
                Value::String(event.to_string().into()),
            ],
            env,
        )?;
    } else {
        internal_default_process_sentinel(interp, process_id, event, env)?;
    }
    Ok(())
}

fn run_process_log(
    interp: &mut Interpreter,
    server_id: u64,
    client_id: u64,
    message: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Some(log) = interp.process_log_function(server_id) {
        call_function_value(
            interp,
            &log,
            &[
                interp.record_value(server_id),
                interp.record_value(client_id),
                Value::String(message.to_string().into()),
            ],
            env,
        )?;
    }
    Ok(())
}

/// GNU wait_reading_process_output: wait up to TOTAL, handling process
/// output as it arrives and firing timers as they come due, instead of
/// sleeping blind.  While progress is being made the loop re-pumps
/// immediately, so an in-process client/server exchange completes at full
/// speed rather than one round-trip per wait call.  With RETURN_ON_DELIVERY
/// (accept-process-output) the wait ends as soon as output was handled.  A
/// status change or sentinel alone is progress, but GNU does not report it as
/// delivered process output.
/// Returns whether any process output was delivered.
pub(crate) fn wait_pumping_processes(
    interp: &mut Interpreter,
    env: &mut Env,
    total: Option<std::time::Duration>,
    return_on_delivery: bool,
    target_process_id: Option<u64>,
    only_process_id: Option<u64>,
    run_timers: bool,
) -> Result<bool, LispError> {
    let deadline = total.map(|total| std::time::Instant::now() + total);
    let mut delivered = false;
    let mut delivery_grace_deadline = None;
    let target_start =
        target_process_id.and_then(|process_id| interp.process_output_delivery_count(process_id));
    let mut all_processes_seen = interp.current_thread_process_output_delivery_count();
    // The last wait ended early, and how many waits in a row did so before
    // a pass that found nothing.  A pass can find nothing after a wake and
    // still change the wait set (a pipe read to its end leaves it, before
    // the child signal that follows), but a second such wake in a row
    // makes the next wait a plain sleep, so a descriptor that stays ready
    // (a child signal for another wait's process) cannot make it spin.
    let mut woke_early = false;
    let mut fruitless_wakes = 0;
    loop {
        // A fatal signal ends the wait as GNU's handler, run in the middle
        // of the select, does.
        if crate::lisp::primitives::fatal_signal_pending() {
            crate::lisp::primitives::run_pending_fatal_signal(interp, env)?;
        }
        let mut progressed = pump_external_process_output_for(interp, env, only_process_id)?;
        progressed |= pump_connection_processes_for(interp, env, only_process_id)?;
        // process.c after its select: "If we woke up due to SIGWINCH,
        // actually change size now" -- do_pending_window_change applies
        // the frame sizes change_frame_size queued (a `menu-bar-lines'
        // change, the batch frame's initial menu bar line).
        crate::lisp::primitives::do_pending_window_change(interp, env)?;
        // Output delivered since the previous pass: the grace below is
        // measured from the latest output, so a wait returns once output
        // has been quiet for one READ_OUTPUT_DELAY_INCREMENT.
        let delivery_count = interp.current_thread_process_output_delivery_count();
        let any_process_delivered = delivery_count != all_processes_seen;
        all_processes_seen = delivery_count;
        delivered |= any_process_delivered;
        if return_on_delivery && target_process_id.is_none() && any_process_delivered {
            // process.c:5657-5661 keeps waiting for up to one
            // READ_OUTPUT_DELAY_INCREMENT (10ms) after some output.  This
            // lets one readiness cycle drain sibling descriptors instead of
            // returning on the first short stderr write while stdout is
            // becoming readable (the ordinary JSON-RPC startup pattern).
            delivery_grace_deadline =
                Some(std::time::Instant::now() + std::time::Duration::from_millis(10));
        }
        if run_timers {
            interp.drive_threads(env, true)?;
        }
        if interp.waiting_for_user_input() && !unread_command_events(interp, env)?.is_empty() {
            break;
        }
        let requested_process_delivered = target_process_id.is_some_and(|process_id| {
            interp.process_output_delivery_count(process_id) != target_start
        });
        // process.c: once WAIT_PROC has read something, the wait reads
        // what else is available without waiting, and returns when a pass
        // finds nothing.
        if return_on_delivery
            && target_process_id.is_some()
            && requested_process_delivered
            && !progressed
        {
            break;
        }
        let now = std::time::Instant::now();
        if return_on_delivery
            && target_process_id.is_none()
            && delivered
            && delivery_grace_deadline.is_some_and(|grace| now >= grace)
        {
            break;
        }
        if deadline.is_some_and(|deadline| now >= deadline) {
            // Timer/thread callbacks above can consume the remainder of the
            // deadline while the requested child becomes readable.  Drain
            // readiness once more before reporting a timeout; otherwise the
            // bytes are left for the next caller, producing a deterministic
            // one-wait lag under load.
            let mut final_progress =
                pump_external_process_output_for(interp, env, only_process_id)?;
            final_progress |= pump_connection_processes_for(interp, env, only_process_id)?;
            delivered |= final_progress;
            break;
        }
        // process.c:wait_reading_process_output: "Don't wait for output
        // from a non-running process.  Just read whatever data has already
        // been received" -- with or without a timeout, so a loop of
        // `(accept-process-output PROC 10)' ends when PROC exits, not ten
        // seconds later (erc's subprocess tests spent that on every child).
        // Pending bytes keep the id live until the pump above has
        // delivered them.
        if target_process_id.is_some_and(|process_id| {
            !interp.live_external_process_ids().contains(&process_id)
                && !matches!(
                    interp.process_status_value(process_id).map(|v| v.kind()),
                    Some(Kind::Symbol(status))
                        if matches!(status.as_str(), "run" | "open" | "connect" | "listen")
                )
        }) {
            break;
        }
        if progressed {
            woke_early = false;
            fruitless_wakes = 0;
            continue;
        }
        // Idle: wait until input arrives, a child's status changes, the
        // deadline passes or the next timer is due, for at most 10ms at a
        // time, so that what the wait set leaves out is still looked at.
        let mut nap = deadline
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(std::time::Duration::from_millis(10));
        if let Some(due) = interp.next_timer_due() {
            nap = nap.min(due.saturating_duration_since(now));
        }
        if run_timers && let Some(delay) = crate::lisp::primitives::next_timer_delay(interp, env) {
            nap = nap.min(delay);
        }
        if let Some(grace) = delivery_grace_deadline {
            nap = nap.min(grace.saturating_duration_since(now));
        }
        nap = nap
            .min(std::time::Duration::from_millis(10))
            .max(std::time::Duration::from_millis(1));
        if std::mem::take(&mut woke_early) {
            fruitless_wakes += 1;
        }
        if fruitless_wakes >= 2 {
            fruitless_wakes = 0;
            interp.sleep_current_thread(env, nap)?;
        } else {
            woke_early = interp.wait_current_thread_for_process_input(env, only_process_id, nap)?;
        }
    }
    let result = if let Some(process_id) = target_process_id {
        interp.process_output_delivery_count(process_id) != target_start
    } else {
        delivered
    };
    Ok(result)
}

/// process.c:wait_while_connecting: what is sent to a connection still
/// connecting waits for the connect (and here its TLS negotiation) to end.
pub(crate) fn wait_while_connecting(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
) -> Result<(), LispError> {
    while interp.network_process_connecting(process_id) {
        wait_pumping_processes(
            interp,
            env,
            Some(std::time::Duration::from_millis(20)),
            false,
            None,
            None,
            true,
        )?;
    }
    Ok(())
}

/// Accept pending server connections and deliver stream input/closure to
/// filters and sentinels.  Returns true if anything was processed.
pub(crate) fn pump_connection_processes(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    pump_connection_processes_for(interp, env, None)
}

fn pump_connection_processes_for(
    interp: &mut Interpreter,
    env: &mut Env,
    only_process_id: Option<u64>,
) -> Result<bool, LispError> {
    let mut progressed = false;

    // wait_reading_process_output: a `:nowait t' client's socket that
    // turns writable has finished connecting.  A failure goes on to the
    // addresses left, as connect_network_socket does, and with none left
    // the status is (failed ERRNO), reported to the sentinel.
    for process_id in interp.connecting_network_processes() {
        if only_process_id.is_some_and(|id| process_id != id) {
            continue;
        }
        let Some(errno) = interp.network_connect_error(process_id) else {
            continue;
        };
        progressed = true;
        if errno != 0 {
            let addresses = interp.take_network_connect_addresses(process_id);
            if addresses.is_empty() {
                interp.fail_network_connect(process_id, errno);
                run_process_sentinel(
                    interp,
                    process_id,
                    &format!("failed with code {errno}\n"),
                    env,
                )?;
                continue;
            }
            match connect_nonblocking(&addresses) {
                Ok((stream, remote, untried)) => {
                    let mut contact = interp
                        .process_contact_plist(process_id)
                        .and_then(|contact| contact.to_vec().ok())
                        .unwrap_or_default();
                    plist_items_put(&mut contact, ":remote", sockaddr_vector(remote));
                    if let Ok(local) = stream.local_addr() {
                        plist_items_put(&mut contact, ":local", sockaddr_vector(local));
                    }
                    interp.set_process_contact_plist(process_id, Value::list(contact))?;
                    interp.restart_network_connect(process_id, stream, untried);
                }
                // connect_network_socket sets (failed ...) here and
                // notifies no one.
                Err(error) => {
                    interp.fail_network_connect(process_id, error.raw_os_error().unwrap_or(errno))
                }
            }
            continue;
        }
        match progress_async_gnutls(interp, process_id)? {
            AsyncGnuTlsProgress::NotRequested | AsyncGnuTlsProgress::Ready => {
                interp.mark_network_process_open(process_id);
                run_process_sentinel(interp, process_id, "open\n", env)?;
            }
            AsyncGnuTlsProgress::Pending => {}
            AsyncGnuTlsProgress::Failed(error) => {
                interp.mark_network_process_failed(process_id);
                run_process_sentinel(
                    interp,
                    process_id,
                    &format!("TLS negotiation failed: {error:?}\n"),
                    env,
                )?;
            }
        }
    }

    // Accept new connections on every server listener.
    for server_id in interp.network_listener_ids() {
        if only_process_id.is_some_and(|id| server_id != id) {
            continue;
        }
        loop {
            let (child_runtime, peer_addr) = match interp.accept_network_connection(server_id)? {
                None => break,
                Some(crate::lisp::eval::AcceptedConnection::Stream(runtime, peer_addr)) => {
                    (runtime, peer_addr)
                }
                // server_accept_connection: a failed accept is the log
                // function's to hear of, "accept failed with code" and the
                // errno run together, and the listener waits again.
                Some(crate::lisp::eval::AcceptedConnection::Failed(code)) => {
                    if let Some(log) = interp.process_log_function(server_id) {
                        call_function_value(
                            interp,
                            &log,
                            &[
                                interp.record_value(server_id),
                                Value::Nil,
                                Value::String(format!("accept failed with code{code}\n").into()),
                            ],
                            env,
                        )?;
                    }
                    progressed = true;
                    break;
                }
            };
            progressed = true;
            interp.network_connect_counter += 1;
            let connect_number = interp.network_connect_counter;
            // GNU server_accept_connection gives each child a fresh copy of
            // the server's plist (Fcopy_sequence), so the child's later
            // `process-put' (plist-put mutates value cells in place) never
            // clobbers the server's own properties.
            let server_plist = interp.process_plist_value(server_id).unwrap_or(Value::Nil);
            let server_plist = match server_plist.to_vec() {
                Ok(items) => Value::list(items),
                Err(_) => server_plist,
            };
            // The child's contact info is the server's with :server nil,
            // :host/:service naming the peer, and :remote its address.
            // Unix peers are unnamed: :host t, :remote "", :service and
            // :local keep the server's socket path.
            let mut contact_items = interp
                .process_contact_plist(server_id)
                .unwrap_or(Value::Nil)
                .to_vec()
                .unwrap_or_default();
            plist_items_put(&mut contact_items, ":server", Value::Nil);
            // server_accept_connection's HOST: HOST_FORMAT_IN, or
            // HOST_FORMAT_IN6 over the address's 16-bit words as memory
            // holds them; a unix peer has none (t, "-" in messages).
            let host = peer_addr.map(|peer_addr| match peer_addr.ip() {
                std::net::IpAddr::V4(ip) => ip.to_string(),
                std::net::IpAddr::V6(ip) => ip
                    .octets()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|word| format!("{:x}", u16::from_ne_bytes(*word)))
                    .collect::<Vec<_>>()
                    .join(":"),
            });
            let remote = match peer_addr {
                Some(peer_addr) => {
                    plist_items_put(
                        &mut contact_items,
                        ":host",
                        Value::String(host.clone().unwrap_or_default().into()),
                    );
                    plist_items_put(
                        &mut contact_items,
                        ":service",
                        Value::Integer(peer_addr.port() as i64),
                    );
                    if let crate::lisp::eval::NetworkRuntime::Stream(stream) = &child_runtime
                        && let Ok(addr) = stream.local_addr()
                    {
                        plist_items_put(&mut contact_items, ":local", sockaddr_vector(addr));
                    }
                    plist_items_put(&mut contact_items, ":remote", sockaddr_vector(peer_addr));
                    peer_addr.to_string()
                }
                None => {
                    plist_items_put(&mut contact_items, ":host", Value::T);
                    plist_items_put(
                        &mut contact_items,
                        ":remote",
                        Value::String(String::new().into()),
                    );
                    String::new()
                }
            };
            let server_filter = interp.process_filter(server_id);
            let server_sentinel = interp.process_sentinel(server_id);
            let server_log = interp.process_log_function(server_id);
            let server_buffer = interp.process_buffer_id(server_id);
            let (child_decoding, child_encoding) = interp
                .process_coding_system(server_id)?
                .cons_values()
                .unwrap_or((Value::Nil, Value::Nil));
            let child_inherit_coding_system = server_buffer.is_some()
                && interp
                    .process_inherit_coding_system_flag(server_id)
                    .unwrap_or(false);
            let base_name = interp.process_name(server_id).unwrap_or_default();
            // GNU names inet children "NAME <HOST:PORT>" (note the space)
            // and unnamed-peer children "NAME <N>" from connect_counter.
            let child_name = if let (Some(peer_addr), Some(host)) = (peer_addr, &host) {
                let port = peer_addr.port();
                interp.unique_process_name(&if peer_addr.is_ipv6() {
                    format!("{base_name} <[{host}]:{port}>")
                } else {
                    format!("{base_name} <{host}:{port}>")
                })
            } else {
                interp.unique_process_name(&format!("{base_name} <{connect_number}>"))
            };
            let child = interp.create_network_process(
                &child_name,
                // GNU's accepted process inherits the listener's filter,
                // sentinel, log function, plist, and coding systems, but it
                // does not inherit the listener's process buffer.  Giving
                // the child that buffer makes killing the listener buffer
                // prompt about a still-live accepted connection.
                None,
                child_inherit_coding_system,
                server_filter,
                server_sentinel,
                server_log,
                server_plist,
                Some(child_runtime),
                None,
                None,
                Some(remote.clone()),
                Some(server_id),
                Value::list(contact_items),
                child_decoding,
                child_encoding,
            )?;
            let child_id = interp.resolve_process_id(&child)?;
            let host = host.as_deref().unwrap_or("-");
            run_process_log(
                interp,
                server_id,
                child_id,
                &format!("accept from {host}\n"),
                env,
            )?;
            run_process_sentinel(interp, child_id, &format!("open from {host}\n"), env)?;
        }
    }

    // Deliver input / closure on every open network or serial stream.
    for stream_id in interp.connection_stream_ids() {
        if only_process_id.is_some_and(|id| stream_id != id) {
            continue;
        }
        if interp.process_output_paused(stream_id) {
            continue;
        }
        let (bytes, eof) = interp.poll_connection_stream(stream_id)?;
        if !bytes.is_empty() {
            progressed = true;
            let output = decode_process_output(interp, stream_id, &bytes, env)?;
            deliver_process_output_decoded(interp, stream_id, output, env)?;
        } else if eof {
            progressed = true;
            // A read that returns the flushed carryover is not the end
            // yet; the next one is.
            if flush_process_decoder(interp, stream_id, env)?.0 {
                // wait_reading_process_output: deactivate_process, status
                // (exit 256), and status_notify's sentinel.
                interp.close_connection_stream(stream_id);
                run_process_sentinel(interp, stream_id, "connection broken by remote peer\n", env)?;
            }
        }
    }

    Ok(progressed)
}
