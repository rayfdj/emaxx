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
///   its fork path (callproc.c:1289).
/// * For a child on a pty (PTY), emacs_spawn's vfork path: TIOCSCTTY of a
///   tty the parent opened (where USG_SUBTTY_WORKS or no USG has it do
///   so), then the pty's tty opened again by its name, which makes it the
///   controlling terminal, child_setup_tty's modes on it for output, and
///   it as the streams that are on the pty, standard error with standard
///   output unless `:stderr' gave one.  A tty that cannot be opened for
///   input is emacs_perror's line and _exit (EXIT_CANCELED).
#[cfg(unix)]
fn configure_emacs_spawn(command: &mut Command, pty: Option<PtyChildSetup>, merge_stderr: bool) {
    use std::os::unix::process::CommandExt;
    // SAFETY: signal, setsid, ioctl, open, tcgetattr, tcsetattr, dup2,
    // close, write, strerror and _exit are async-signal-safe child-side
    // calls on data the closure owns, as GNU's vfork child makes them.
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
            if let Some(pty) = &pty {
                pty.setup_in_child();
            }
            // if (std_err < 0) std_err = std_out.
            if merge_stderr {
                libc::dup2(libc::STDOUT_FILENO, libc::STDERR_FILENO);
            }
            Ok(())
        });
    }
}

/// What emacs_spawn's vfork child does with the pty: its name, which of
/// the child's input and output are on it, whether the parent opened the
/// tty (and gave it as the child's streams), and emacs_perror's command
/// name.
#[cfg(unix)]
struct PtyChildSetup {
    name: std::ffi::CString,
    input: bool,
    output: bool,
    parent_opened: bool,
    command_name: std::ffi::CString,
}

#[cfg(unix)]
impl PtyChildSetup {
    /// callproc.c:emacs_spawn's child after dissociate_controlling_tty.
    ///
    /// # Safety
    ///
    /// Only in the child between fork and exec: it calls async-signal-safe
    /// functions alone, on descriptors the child owns.
    unsafe fn setup_in_child(&self) {
        // SAFETY: as the function's contract states.
        unsafe {
            // Make the pty's terminal the controlling terminal.
            if self.input && self.parent_opened {
                libc::ioctl(libc::STDIN_FILENO, libc::c_ulong::from(libc::TIOCSCTTY), 0);
            }
            // Now close the pty (if we had it open) and reopen it.
            if self.input && self.parent_opened {
                libc::close(libc::STDIN_FILENO);
            }
            let fd = libc::open(self.name.as_ptr(), libc::O_RDWR);
            if self.input && fd < 0 {
                // emacs_perror (pty_name); _exit (EXIT_CANCELED).
                let text =
                    libc::strerror(std::io::Error::last_os_error().raw_os_error().unwrap_or(0));
                for part in [
                    self.command_name.as_ptr(),
                    c": ".as_ptr(),
                    self.name.as_ptr(),
                    c": ".as_ptr(),
                    text,
                    c"\n".as_ptr(),
                ] {
                    libc::write(libc::STDERR_FILENO, part.cast(), libc::strlen(part));
                }
                libc::_exit(EXIT_CANCELED);
            }
            if self.output {
                child_setup_tty(fd);
            }
            if fd >= 0 {
                if self.input {
                    libc::dup2(fd, libc::STDIN_FILENO);
                }
                if self.output {
                    libc::dup2(fd, libc::STDOUT_FILENO);
                }
                if fd > libc::STDERR_FILENO {
                    libc::close(fd);
                }
            }
        }
    }
}

/// callproc.c's EXIT_CANCELED: a child that could not be set up.
#[cfg(unix)]
const EXIT_CANCELED: libc::c_int = 125;

/// systty.h's CDISABLE: _POSIX_VDISABLE, the character that disables a
/// terminal function.
#[cfg(any(target_os = "linux", target_os = "android"))]
const CDISABLE: libc::cc_t = 0;
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
const CDISABLE: libc::cc_t = 0xff;

/// sysdep.c:child_setup_tty of OUT: emacs_get_tty (zeroed, then
/// tcgetattr), output postprocessing without NL to CR-NL, delays, tab
/// expansion or upcasing; no echo; signals; no input downcasing or 8th-bit
/// stripping; CS8; VERASE and VKILL disabled; VQUIT and VINTR C-\\ and
/// C-c where disabled (SIGNALS_VIA_CHARACTERS); ICANON with VEOF C-d; then
/// emacs_set_tty without flushing.  Async-signal-safe.
#[cfg(unix)]
pub(crate) fn child_setup_tty(out: libc::c_int) {
    // SAFETY: an all-zero termios is emacs_get_tty's memset; tcgetattr
    // fills it from OUT or fails and leaves it.
    let mut s: libc::termios = unsafe { std::mem::zeroed() };
    // SAFETY: as above.
    unsafe { libc::tcgetattr(out, &mut s) };
    s.c_oflag |= libc::OPOST;
    s.c_oflag &= !libc::ONLCR;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        s.c_oflag &=
            !(libc::NLDLY | libc::CRDLY | libc::TABDLY | libc::BSDLY | libc::VTDLY | libc::FFDLY);
    }
    #[cfg(target_os = "macos")]
    {
        s.c_oflag &=
            !(libc::NLDLY | libc::CRDLY | libc::TABDLY | libc::BSDLY | libc::VTDLY | libc::FFDLY);
    }
    s.c_lflag &= !libc::ECHO;
    s.c_lflag |= libc::ISIG;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        s.c_iflag &= !libc::IUCLC;
    }
    s.c_iflag &= !libc::ISTRIP;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        s.c_oflag &= !libc::OLCUC;
    }
    s.c_oflag &= !libc::TAB3;
    s.c_cflag = (s.c_cflag & !libc::CSIZE) | libc::CS8;
    s.c_cc[libc::VERASE] = CDISABLE;
    s.c_cc[libc::VKILL] = CDISABLE;
    if s.c_cc[libc::VQUIT] == CDISABLE {
        s.c_cc[libc::VQUIT] = b'\\' & 0o37;
    }
    if s.c_cc[libc::VINTR] == CDISABLE {
        s.c_cc[libc::VINTR] = b'C' & 0o37;
    }
    s.c_lflag |= libc::ICANON;
    s.c_cc[libc::VEOF] = b'D' & 0o37;
    emacs_set_tty(out, &s, false);
}

/// sysdep.c:emacs_set_tty of FD: tcsetattr (TCSAFLUSH for FLUSHP, else
/// TCSADRAIN), retried when interrupted, up to ten times until tcgetattr
/// reads back what was asked; -1 for any other failure.  Async-signal-safe.
#[cfg(unix)]
fn emacs_set_tty(fd: libc::c_int, settings: &libc::termios, flushp: bool) -> libc::c_int {
    let action = if flushp {
        libc::TCSAFLUSH
    } else {
        libc::TCSADRAIN
    };
    for _ in 0..10 {
        // SAFETY: SETTINGS is an initialized termios; FD may be any value.
        if unsafe { libc::tcsetattr(fd, action, settings) } < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return -1;
        }
        // SAFETY: an all-zero termios is the memset GNU makes; tcgetattr
        // fills it.
        let mut new: libc::termios = unsafe { std::mem::zeroed() };
        // SAFETY: as above.
        unsafe { libc::tcgetattr(fd, &mut new) };
        if new.c_iflag == settings.c_iflag
            && new.c_oflag == settings.c_oflag
            && new.c_cflag == settings.c_cflag
            && new.c_lflag == settings.c_lflag
            && new.c_cc == settings.c_cc
        {
            break;
        }
    }
    0
}

/// process.c's child_signal_read_fd: the pipe handle_child_signal writes a
/// byte to, which wait_reading_process_output selects on beside the
/// processes' channels, so a SIGCHLD arriving before or during the select
/// ends the wait.  -1 (None) until child_signal_init makes it.
#[cfg(unix)]
static CHILD_SIGNAL_READ_FD: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

/// The child signal pipe's read end, once child_signal_init has made it.
#[cfg(unix)]
pub(crate) fn child_signal_read_fd() -> Option<std::os::fd::RawFd> {
    let fd = CHILD_SIGNAL_READ_FD.load(std::sync::atomic::Ordering::Acquire);
    (fd >= 0).then_some(fd)
}

/// process.c:child_signal_init, which create_process and call_process run
/// before their first child: the pipe, both ends non-blocking, its read
/// end within an fd_set, and SIGCHLD's handler writing to it.  "We leave
/// the file descriptors open until the Emacs process exits."
#[cfg(unix)]
pub(crate) fn child_signal_init(interp: &Interpreter) -> Result<(), LispError> {
    use std::os::fd::{AsRawFd, IntoRawFd};
    static INIT: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _held = INIT.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if child_signal_read_fd().is_some() {
        return Ok(()); // "already done"
    }
    let (reader, writer) = emacs_pipe().map_err(|error| {
        file_operation_error_value_without_file(interp, "Creating pipe for child signal", &error)
    })?;
    if FD_SETSIZE <= reader.as_raw_fd() {
        // "Since we need to `pselect' on the read end, it has to fit
        // into an `fd_set'."
        return Err(fd_setsize_error(interp, "Creating pipe for child signal"));
    }
    for fd in [reader.as_raw_fd(), writer.as_raw_fd()] {
        // SAFETY: FD is one of the two live descriptors above.
        if unsafe { libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK) } != 0 {
            emacs_perror("fcntl", &std::io::Error::last_os_error());
        }
    }
    // handle_child_signal: every watched child asked waitpid for its
    // change of status, before the pipe's write wakes a wait.
    // SAFETY: the action calls waitpid and atomics alone, which are
    // async-signal-safe, and keeps errno.
    match unsafe {
        signal_hook::low_level::register(libc::SIGCHLD, crate::lisp::eval::handle_child_signal)
    } {
        Ok(_) => crate::lisp::eval::CHILD_SIGNAL_HANDLED
            .store(true, std::sync::atomic::Ordering::Release),
        Err(error) => emacs_perror("sigaction", &error),
    }
    // signal-hook's pipe handler is handle_child_signal's
    // child_signal_notify: one non-blocking write, its errors ignored.
    match signal_hook::low_level::pipe::register(libc::SIGCHLD, writer) {
        Ok(_) => {
            CHILD_SIGNAL_READ_FD.store(reader.into_raw_fd(), std::sync::atomic::Ordering::Release);
        }
        Err(error) => emacs_perror("sigaction", &error),
    }
    Ok(())
}

/// sysdep.c:emacs_perror: "COMMAND: MESSAGE: strerror (errno)" on standard
/// error in one write, ERROR's errno standing for errno.
#[cfg(unix)]
fn emacs_perror(message: &str, error: &std::io::Error) {
    use std::io::Write;
    use std::os::unix::ffi::OsStrExt;
    let errno = error.raw_os_error().unwrap_or(0);
    // SAFETY: strerror returns a NUL-terminated string valid until the
    // next call on this thread, copied at once.
    let error_string = unsafe { std::ffi::CStr::from_ptr(libc::strerror(errno)) }
        .to_bytes()
        .to_vec();
    let command = std::env::args_os().next().unwrap_or_else(|| "emacs".into());
    let mut line = command.as_bytes().to_vec();
    line.extend_from_slice(b": ");
    line.extend_from_slice(message.as_bytes());
    line.extend_from_slice(b": ");
    line.extend_from_slice(&error_string);
    line.push(b'\n');
    let _ = std::io::stderr().write_all(&line);
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
    program_value: Value,
    make_process: bool,
) -> Result<String, LispError> {
    let program = string_text(&program_value)?;
    if make_process && program.starts_with('/') {
        // Ffile_directory_p, which may have a handler.
        if call(interp, "file-directory-p", &[program_value], env)?.is_truthy() {
            return Err(LispError::Signal(
                "Specified program for new process is a directory".into(),
            ));
        }
        return Ok(remove_slash_colon(&program));
    }
    let exec_path = interp
        .lookup_var_key(cached_symbol!("exec-path"), env)
        .unwrap_or(Value::Nil);
    let exec_suffixes = interp
        .lookup_var_key(cached_symbol!("exec-suffixes"), env)
        .unwrap_or(Value::Nil);
    let (found, errno) = locate_file_search(
        interp,
        &program_value,
        &exec_path,
        &exec_suffixes,
        &Value::Integer(i64::from(libc::X_OK)),
        env,
    )?;
    if found.is_nil() {
        return Err(report_file_errno(
            interp,
            "Searching for program",
            program_value,
            &std::io::Error::from_raw_os_error(errno),
        ));
    }
    // Fmake_process expands openp's answer (Fexpand_file_name, which may
    // have a handler); both then remove a "/:".
    let found = if make_process {
        string_text(&fexpand_file_name(interp, env, found, Value::Nil)?)?
    } else {
        string_text(&found)?
    };
    Ok(remove_slash_colon(&found))
}

/// A spawn failure is what GNU's parent sees from `emacs_spawn' and reports
/// as "Doing vfork" without naming a file.
fn vfork_error(interp: &Interpreter, error: &std::io::Error) -> LispError {
    file_operation_error_value_without_file(interp, "Doing vfork", error)
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

    // callproc.c:child_setup's restore_nofile_limit: the vfork path a pty
    // takes gives the child the limit Emacs started with; a posix_spawn
    // child keeps the lowered one.
    let nofile_limit = if pty { nofile_limit_to_restore() } else { None };
    // SAFETY: the child only calls setrlimit, execve, write, strerror and
    // _exit on data owned by `image' and the closure; GNU's own vfork child
    // makes the same calls.
    unsafe {
        command.pre_exec(move || {
            if let Some(limit) = &nofile_limit {
                libc::setrlimit(libc::RLIMIT_NOFILE, limit);
            }
            image.exec_in_child(pty)
        });
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
        .lookup_var_key(cached_symbol!("default-process-coding-system"), env)
        .and_then(|value| value.cons_values())
        .map_or(Value::Nil, |(_, encoding)| encoding);
    let candidates = [coding, default, preferred_coding_system(interp)];
    let mut coding_base = Value::Nil;
    let mut eol_base = Value::Nil;
    let mut complete_at = candidates.len();
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate.is_nil() {
            continue;
        }
        // By EQ: an alias shares its target's spec and attributes.
        let Some(canonical) = interp.coding_system_canonical_name_value(candidate) else {
            continue;
        };
        if coding_base.is_nil()
            && interp.coding_system_kind_name(&canonical).as_deref() != Some("undecided")
        {
            coding_base = interp
                .coding_system_base_name(&canonical)
                .map_or(Value::Nil, |base| Value::symbol(&base));
        }
        if eol_base.is_nil() && interp.coding_system_eol_type_value(&canonical).is_some() {
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
        let symbol = super::coding::coding_text_of(interp, &coding)?;
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
        let raw_text =
            raw_text_coding_system(interp, &super::coding::coding_text_of(interp, &last_used)?);
        *encoder = EncoderSpec::default();
        encoder.coding = Some(Value::symbol(&raw_text));
    }
    interp.set_variable_key(cached_symbol!("last-coding-system-used"), last_used, env);
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
        let symbol = super::coding::coding_text_of(interp, &coding)?;
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
        .lookup_var_key(cached_symbol!("coding-system-for-write"), env)
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
        load_coding_system(interp, &coding, env)?;
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

/// callproc.c create_temp_file's file: Vtemp_file_name_pattern
/// ("emacsXXXXXX") expanded in `temporary-file-directory' (else $TMPDIR,
/// else "/tmp/"), a copy of its ENCODE_FILE given to mkostemp
/// (close-on-exec), and that copy, the name mkostemp made.
pub(crate) fn create_temp_file(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(fs::File, Value, CallProcessTempFile), LispError> {
    let tmpdir = interp
        .lookup_var_key(cached_symbol!("temporary-file-directory"), env)
        .filter(Value::is_string);
    let tmpdir = match tmpdir {
        Some(directory) => directory,
        None => {
            #[cfg(unix)]
            let outf = std::env::var_os("TMPDIR").map(|outf| {
                use std::os::unix::ffi::OsStringExt;
                outf.into_vec()
            });
            #[cfg(not(unix))]
            let outf = std::env::var("TMPDIR").ok().map(String::into_bytes);
            super::make_string(outf.unwrap_or_else(|| b"/tmp/".to_vec()))
                .map_err(|error| super::string_storage_error(interp, env, error))?
        }
    };
    let pattern = fexpand_file_name(interp, env, Value::string("emacsXXXXXX"), tmpdir)?;
    let encoded = encode_file_value(interp, env, pattern)?;
    let multibyte =
        matches!(encoded.kind(), Kind::StringObject(state) if state.borrow().is_multibyte());
    let mut template = sdata_c_string(&encoded);
    #[cfg(unix)]
    {
        use std::os::fd::FromRawFd;
        template.push(0);
        // SAFETY: TEMPLATE is a writable NUL-terminated buffer mkostemp
        // fills in place; the descriptor it returns is owned at once.
        let fd = unsafe { libc::mkostemp(template.as_mut_ptr().cast(), libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(report_file_errno(
                interp,
                "Failed to open temporary file using pattern",
                pattern,
                &std::io::Error::last_os_error(),
            ));
        }
        // SAFETY: a fresh descriptor, owned by this File alone.
        let file = unsafe { fs::File::from_raw_fd(fd) };
        template.pop();
        // Fcopy_sequence of the encoded pattern, which mkostemp filled in.
        let name = super::make_specified_string(template, multibyte)
            .map_err(|error| super::string_storage_error(interp, env, error))?;
        let guard = CallProcessTempFile(encoded_host_path(&name));
        Ok((file, name, guard))
    }
    #[cfg(not(unix))]
    {
        static SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let stem = template.len().saturating_sub(6);
        loop {
            let serial = SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let suffix = format!(
                "{:06}",
                (u64::from(std::process::id()) * 1000 + serial) % 1_000_000
            );
            template.truncate(stem);
            template.extend_from_slice(suffix.as_bytes());
            let name = super::make_specified_string(template.clone(), multibyte)
                .map_err(|error| super::string_storage_error(interp, env, error))?;
            match fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(encoded_host_path(&name))
            {
                Ok(file) => {
                    let guard = CallProcessTempFile(encoded_host_path(&name));
                    return Ok((file, name, guard));
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(report_file_errno(
                        interp,
                        "Failed to open temporary file using pattern",
                        pattern,
                        &error,
                    ));
                }
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

    // CHECK_STRING (args[0]).
    if !args[0].is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
    }
    let argv = call_process_arguments(interp, env, args, args.get(4..).unwrap_or(&[]))?;
    let ProcessDestination {
        mut buffer,
        output_file,
        error_file,
    } = process_destination(args.get(2).unwrap_or(&Value::Nil));
    // callproc.c: a stderr FILE Fexpand_file_name'd with a nil default,
    // an output FILE against the buffer's directory.
    let error_file = if error_file.is_nil() || error_file == Value::T {
        error_file
    } else {
        fexpand_file_name(interp, env, error_file, Value::Nil)?
    };
    let output_file = match output_file {
        Some(file) => {
            let directory = interp
                .lookup_var_key(cached_symbol!("default-directory"), env)
                .unwrap_or(Value::Nil);
            let file = fexpand_file_name(interp, env, file, directory)?;
            if !file.is_string() {
                return Err(LispError::WrongTypeArgument("stringp".into(), file));
            }
            Some(file)
        }
        None => None,
    };
    let no_wait = matches!(buffer.kind(), Kind::Integer(_));
    if !(buffer.is_nil() || buffer == Value::T || no_wait) {
        buffer = call(interp, "get-buffer-create", &[buffer], env)?;
    }
    let directory = process_current_directory(interp, env, true)?;
    // Each name without its "/:", ENCODE_FILE'd before the program is
    // searched for; an error names DECODE_FILE of what was opened.
    let error_file = if error_file.is_string() {
        let file = remove_slash_colon_value(interp, env, error_file)?;
        encode_file_value(interp, env, file)?
    } else {
        error_file
    };
    let output_file = match output_file {
        Some(file) => {
            let file = remove_slash_colon_value(interp, env, file)?;
            Some(encode_file_value(interp, env, file)?)
        }
        None => None,
    };
    let program = locate_program_for_exec(interp, env, args[0], false)?;
    let program = host_file_name(interp, env, &program)?.into_os_string();
    let discard_output = no_wait || (buffer.is_nil() && output_file.is_none());
    let (output_read, output) = if discard_output {
        let output = fs::OpenOptions::new()
            .write(true)
            .open(NULL_DEVICE)
            .map_err(|error| {
                file_operation_error_value_without_file(interp, "Opening null device", &error)
            })?;
        (None, output)
    } else if let Some(file) = output_file {
        match open_child_output_file(&encoded_host_path(&file)) {
            Ok(output) => (None, output),
            Err(error) => {
                let name = decode_file_value(interp, env, file)?;
                return Err(report_file_errno(
                    interp,
                    "Opening process output file",
                    name,
                    &error,
                ));
            }
        }
    } else {
        let (read, write) = emacs_pipe().map_err(|error| {
            file_operation_error_value_without_file(interp, "Creating process pipe", &error)
        })?;
        (Some(read), write)
    };
    let error = if error_file.is_string() || (error_file.is_nil() && !discard_output) {
        let path = if error_file.is_string() {
            encoded_host_path(&error_file)
        } else {
            std::path::PathBuf::from(NULL_DEVICE)
        };
        match open_child_output_file(&path) {
            Ok(error) => Some(error),
            Err(error) => {
                let name = if error_file.is_string() {
                    decode_file_value(interp, env, error_file)?
                } else {
                    Value::string(NULL_DEVICE)
                };
                return Err(report_file_errno(
                    interp,
                    "Cannot redirect stderr",
                    name,
                    &error,
                ));
            }
        }
    } else {
        None
    };
    let environment = child_environment(interp, env, directory)?;
    #[cfg(unix)]
    child_signal_init(interp)?;
    let mut child = spawn_call_process(
        interp,
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
    interp: &Interpreter,
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
        // An argument holding a NUL takes the fork path.
        if let (Some(image), Ok(directory)) = (
            ExecImage::new(program, argv, block.clone()),
            std::ffi::CString::new(directory.clone()),
        ) {
            let stdio = [stdin, stdout, stderr].map(|file| SpawnFd::Fd(file.as_raw_fd()));
            let pid = emacs_posix_spawn(&image, Some(&directory), stdio)
                .map_err(|error| vfork_error(interp, &error))?;
            return Ok(SpawnedChild::new(pid as u32, None, None, None));
        }
    }
    let mut command = Command::new(program);
    add_command_arguments(&mut command, argv);
    set_command_environment(&mut command, &environment);
    #[cfg(unix)]
    configure_emacs_spawn(&mut command, None, false);
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
    let child = command
        .spawn()
        .map_err(|error| vfork_error(interp, &error))?;
    #[cfg(unix)]
    return Ok(SpawnedChild::from_std(child));
    #[cfg(not(unix))]
    Ok(child)
}

/// A child's environment block and the directory it starts in.
pub(crate) type ChildEnvironment = (Vec<Vec<u8>>, Vec<u8>);

/// The child's environment and DIRECTORY, get_current_directory (true)'s
/// encoded name, the directory also set on COMMAND.
pub(crate) fn configure_external_command(
    interp: &mut Interpreter,
    env: &mut Env,
    command: &mut Command,
    directory: Value,
) -> Result<ChildEnvironment, LispError> {
    let environment = child_environment(interp, env, directory)?;
    set_command_environment(command, &environment);
    Ok(environment)
}

/// callproc.c get_current_directory: BVAR (current_buffer, directory) as
/// `unhandled-file-name-directory' answers it ("~" for its nil), through
/// expand_and_dir_to_file; that name without its "/:" ENCODE_FILE'd,
/// which file_accessible_directory_p must find an accessible directory
/// ("Setting current directory", naming `default-directory', with the
/// errno), when ENCODE (the child's directory), else the expanded name.
pub(crate) fn process_current_directory(
    interp: &mut Interpreter,
    env: &mut Env,
    encode: bool,
) -> Result<Value, LispError> {
    let curdir = interp
        .lookup_var_key(cached_symbol!("default-directory"), env)
        .unwrap_or(Value::Nil);
    // GNU's get_current_directory(true) asks a handler for the local
    // directory without delegating the process operation itself.  This
    // is what lets `:file-handler nil' run locally from a remote logical
    // default-directory.
    // callproc.c invokes Funhandled_file_name_directory directly, so a
    // user redefinition of the Lisp function cell cannot replace the C
    // mechanism.  Native dispatch still performs the legitimate
    // file-name-handler call made inside that primitive.
    let dir = call(interp, "unhandled-file-name-directory", &[curdir], env)?;
    // A handler returns nil when its logical directory cannot be a host
    // cwd; GNU falls back to "~".
    let dir = if dir.is_nil() {
        Value::string("~")
    } else {
        dir
    };
    let dir = super::system::expand_and_dir_to_file(interp, env, dir)?;
    let unquoted = remove_slash_colon_value(interp, env, dir)?;
    let encoded = encode_file_value(interp, env, unquoted)?;
    if let Err(error) = super::system::accessible_directory(&sdata_c_string(&encoded)) {
        return Err(report_file_errno(
            interp,
            "Setting current directory",
            curdir,
            &error,
        ));
    }
    Ok(if encode { encoded } else { dir })
}

/// make_environment_block for a child starting in DIRECTORY, the
/// ENCODE_FILE'd name get_current_directory (true) gave, with that
/// directory (SSDATA, which ENCODE_FILE leaves without a NUL).
fn child_environment(
    interp: &mut Interpreter,
    env: &mut Env,
    directory: Value,
) -> Result<ChildEnvironment, LispError> {
    let directory = sdata_c_string(&directory);
    let block = make_environment_block(interp, env, &directory);
    Ok((block, directory))
}

/// COMMAND started in ENVIRONMENT's directory, with (where the exec does
/// not take the block itself) its environment.
fn set_command_environment(command: &mut Command, environment: &ChildEnvironment) {
    let (block, directory) = environment;
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        command.current_dir(std::ffi::OsStr::from_bytes(directory));
    }
    #[cfg(not(unix))]
    command.current_dir(String::from_utf8_lossy(directory).into_owned());
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

/// What Fmake_process has decided of a child's channels when it calls
/// create_process: p->pty_in and p->pty_out, and p->stderrproc, the
/// `:stderr' pipe process whose SUBPROCESS_STDOUT is create_process's
/// forkerr, the child's standard error.
#[derive(Clone, Copy)]
pub(crate) struct ProcessChannels {
    pub(crate) pty_modes: (bool, bool),
    pub(crate) stderr_process_id: Option<u64>,
}

/// create_process: PROGRAM forked in DIRECTORY (get_current_directory
/// (true)'s name, which Fmake_process takes first) with ARGV, its input
/// and output a pty or pipes as PTY_MODES (p->pty_in, p->pty_out) say.
pub(crate) fn spawn_persistent_process(
    interp: &mut Interpreter,
    env: &mut Env,
    program: &std::ffi::OsStr,
    argv: &[Vec<u8>],
    directory: Value,
    channels: ProcessChannels,
    // setup_process_coding_systems, which may signal, once the channels
    // are made and before the spawn.
    setup_coding: &mut dyn FnMut(&mut Interpreter, &mut Env) -> Result<(), LispError>,
) -> Result<RunningProcess, LispError> {
    let ProcessChannels {
        pty_modes,
        stderr_process_id,
    } = channels;
    #[cfg(test)]
    crate::test_support::mark_process_test();

    #[cfg(unix)]
    child_signal_init(interp)?;
    let mut command = Command::new(program);
    add_command_arguments(&mut command, argv);
    let child_environment = configure_external_command(interp, env, &mut command, directory)?;
    let (input_pty, output_pty) = pty_modes;
    // create_process's forkerr: the `:stderr' pipe process's
    // SUBPROCESS_STDOUT itself, -1 once an earlier child was given it.
    #[cfg(target_os = "linux")]
    let stderr_fd =
        stderr_process_id.and_then(|stderr_id| interp.pipe_process_stderr_fd(stderr_id));
    #[cfg(target_os = "linux")]
    if let Some(runtime) = posix_spawn_process(
        interp,
        env,
        (program, argv, &child_environment),
        channels,
        stderr_fd,
        setup_coding,
    )? {
        close_stderr_process_output(interp, stderr_process_id);
        return finish_spawned_runtime(runtime);
    }
    #[cfg(target_os = "linux")]
    if let Some(runtime) = vfork_pty_process(
        interp,
        env,
        (program, argv, &child_environment),
        channels,
        stderr_fd,
        setup_coding,
    )? {
        close_stderr_process_output(interp, stderr_process_id);
        return finish_spawned_runtime(runtime);
    }
    // The fork path's Command takes the stream as a descriptor of its own.
    let stderr_writer = match stderr_process_id {
        Some(stderr_id) => interp.pipe_process_stderr_writer(stderr_id)?,
        None => None,
    };
    let stderr = stderr_writer.as_ref();
    let mut pty_input = None;
    let mut pty_output = None;
    let mut pty_slave_name: Option<String> = None;
    #[cfg(unix)]
    let mut pty_setup: Option<PtyChildSetup> = None;
    // create_process's pipes, made here rather than by the spawn so that
    // their descriptors are known before it: the write end of the child's
    // input and the read end of its output.  Its standard error is the
    // `:stderr' pipe process's, else its standard output (emacs_spawn's
    // `if (std_err < 0) std_err = std_out').
    #[cfg(unix)]
    let (mut stdin_write, mut stdout_read) = (None, None);
    #[cfg(unix)]
    {
        let pipe = || {
            emacs_pipe().map_err(|error| {
                file_operation_error_value_without_file(interp, "Creating pipe", &error)
            })
        };
        let clone = |file: &fs::File| {
            file.try_clone()
                .map_err(|error| LispError::Signal(error.to_string()))
        };
        // create_process: allocate_pty when a stream is to be on a pty;
        // when no pty is to be had, both streams are pipes.  Where the
        // parent opens the pty's tty (no USG, or USG_SUBTTY_WORKS) it is
        // the child's forkin and forkout, closed here once the child is
        // made; on GNU/Linux the child alone opens it.
        let pty = if input_pty || output_pty {
            allocate_pty()
        } else {
            None
        };
        let (input_pty, output_pty) = if pty.is_some() {
            (input_pty, output_pty)
        } else {
            (false, false)
        };
        let parent_tty = match &pty {
            Some((_, name)) if PARENT_OPENS_PTY_TTY => Some(open_pty_tty(interp, name)?),
            _ => None,
        };
        // The child's input: the pty (the parent's tty, or a placeholder
        // the child's own open replaces), or a pipe.
        if input_pty {
            command.stdin(match &parent_tty {
                Some(tty) => Stdio::from(clone(tty)?),
                None => Stdio::inherit(),
            });
        } else {
            let (read, write) = pipe()?;
            command.stdin(Stdio::from(read));
            stdin_write = Some(write);
        }
        // Its output, and its standard error with it unless given.
        if output_pty {
            command.stdout(match &parent_tty {
                Some(tty) => Stdio::from(clone(tty)?),
                None => Stdio::inherit(),
            });
            // Standard error is the output's in the child (std_err =
            // std_out), not a descriptor of the parent's.
            match stderr {
                Some(file) => command.stderr(Stdio::from(clone(file)?)),
                None => command.stderr(Stdio::inherit()),
            };
        } else {
            let (read, write) = pipe()?;
            match stderr {
                Some(file) => command.stderr(Stdio::from(clone(file)?)),
                None => command.stderr(Stdio::inherit()),
            };
            command.stdout(Stdio::from(write));
            stdout_read = Some(read);
        }
        if let Some((master, name)) = pty {
            pty_slave_name = Some(name.to_string_lossy().into_owned());
            // The master is both the channel Emacs reads and the one it
            // writes (ptychannel), one descriptor.
            let master = std::sync::Arc::new(master);
            if input_pty {
                pty_input = Some(std::sync::Arc::clone(&master));
            }
            if output_pty {
                pty_output = Some(master);
            } else {
                // create_process puts ptychannel in an open_fd slot only as
                // READ_FROM_SUBPROCESS, when the pty is the output: an
                // input-only pty's master is p->outfd alone, which
                // deactivate_process never closes, so it stays open for
                // the rest of the session.
                std::mem::forget(master);
            }
            use std::os::unix::ffi::OsStrExt;
            let command_name = std::env::args_os()
                .next()
                .and_then(|name| std::ffi::CString::new(name.as_bytes()).ok())
                .unwrap_or_else(|| c"emacs".to_owned());
            pty_setup = Some(PtyChildSetup {
                name,
                input: input_pty,
                output: output_pty,
                parent_opened: parent_tty.is_some(),
                command_name,
            });
        }
        // The parent's tty is in open_fd[SUBPROCESS_STDIN] when it is the
        // child's input, and closed once the child is made; as its output
        // only (forkout) no open_fd slot holds it, and it stays open.
        if let Some(tty) = parent_tty
            && !input_pty
        {
            std::mem::forget(tty);
        }
        close_stderr_process_input(interp, stderr_process_id);
        // create_process: the channel Emacs reads and the one it writes
        // must fit an fd_set; none is spawned otherwise.
        use std::os::fd::AsRawFd;
        let inchannel = stdout_read.as_ref().or(pty_output.as_deref());
        let outchannel = stdin_write.as_ref().or(pty_input.as_deref());
        if beyond_fd_setsize(
            &[inchannel, outchannel].map(|channel| channel.map_or(-1, |file| file.as_raw_fd())),
        ) {
            return Err(fd_setsize_error(interp, "Creating pipe"));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (input_pty, output_pty, stderr);
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
    }
    #[cfg(unix)]
    let exec_monitor = exec_monitor_pipe(interp)?;
    setup_coding(interp, env)?;

    #[cfg(unix)]
    configure_emacs_spawn(&mut command, pty_setup, stderr.is_none());
    #[cfg(unix)]
    exec_like_gnu(
        &mut command,
        program,
        argv,
        child_environment.0,
        pty_slave_name.is_some(),
    );

    let child = command
        .spawn()
        .map_err(|error| vfork_error(interp, &error))?;
    // Only the child keeps the ends it was given.
    drop(command);
    #[cfg(unix)]
    await_exec_monitor(exec_monitor);
    drop(stderr_writer);
    close_stderr_process_output(interp, stderr_process_id);
    #[cfg(unix)]
    let mut child = SpawnedChild::new(child.id(), stdin_write, stdout_read, None);
    // create_process's p->alive = 1, SIGCHLD held back until then.
    #[cfg(unix)]
    child.watch();
    // Put the child under RunningProcess's terminate-and-reap Drop guard
    // before any fallible post-spawn setup.  A failed fcntl must not leak a
    // subprocess whose runtime was never installed in the interpreter.
    let runtime = RunningProcess {
        child,
        terminate_on_drop: true,
        pty_input,
        pty_output,
        pty_slave_name,
        stdout_at_end: false,
        stderr_at_end: false,
        child_ended: false,
    };
    finish_spawned_runtime(runtime)
}

/// create_process's READ_FROM_EXEC_MONITOR and EXEC_MONITOR_OUTPUT: a pipe
/// made before the spawn ("Creating pipe" when none is to be had), whose
/// write end the child holds until it execs or exits.
#[cfg(unix)]
fn exec_monitor_pipe(interp: &Interpreter) -> Result<(fs::File, fs::File), LispError> {
    emacs_pipe()
        .map_err(|error| file_operation_error_value_without_file(interp, "Creating pipe", &error))
}

/// After the spawn, close_process_fd of EXEC_MONITOR_OUTPUT, then one
/// emacs_read of READ_FROM_EXEC_MONITOR, which returns once the child has
/// exec'd or exited (its copy of the write end closes then), and the read
/// end closed.
#[cfg(unix)]
fn await_exec_monitor((read, write): (fs::File, fs::File)) {
    drop(write);
    let mut dummy = [0u8; 1];
    loop {
        match std::io::Read::read(&mut &read, &mut dummy) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            _ => break,
        }
    }
}

/// create_process's close_process_fd of the stderr pipe process's
/// WRITE_TO_SUBPROCESS and SUBPROCESS_STDIN once the child's channels are
/// made: what is sent to it then finds its output closed.
fn close_stderr_process_input(interp: &mut Interpreter, stderr_process_id: Option<u64>) {
    if let Some(stderr_id) = stderr_process_id {
        interp.close_pipe_process_input(stderr_id);
    }
}

/// create_process's close_process_fd of the stderr pipe process's
/// SUBPROCESS_STDOUT once the child is made: the pipe process reads the
/// child's errors on its own channel and ends at their end.
fn close_stderr_process_output(interp: &mut Interpreter, stderr_process_id: Option<u64>) {
    if let Some(stderr_id) = stderr_process_id {
        interp.close_pipe_process_output_writer(stderr_id);
    }
}

/// create_process's fcntl (inchannel, F_SETFL, O_NONBLOCK) and fcntl
/// (outchannel, F_SETFL, O_NONBLOCK): the channels Emacs reads, and the
/// one it writes, whose send then waits in send_process rather than in
/// the kernel when the child does not read.
fn finish_spawned_runtime(runtime: RunningProcess) -> Result<RunningProcess, LispError> {
    #[cfg(unix)]
    {
        if let Some(stdin) = runtime.child.stdin.as_ref() {
            set_nonblocking(stdin)?;
        }
        if let Some(input) = runtime.pty_input.as_deref() {
            set_nonblocking(input)?;
        }
        if let Some(stdout) = runtime.child.stdout.as_ref() {
            set_nonblocking(stdout)?;
        }
        if let Some(stderr) = runtime.child.stderr.as_ref() {
            set_nonblocking(stderr)?;
        }
        if let Some(output) = runtime.pty_output.as_deref() {
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

/// process.c's `nofile_limit': RLIMIT_NOFILE as the process found it,
/// kept when init_process_emacs lowered its soft limit to FD_SETSIZE (GNU
/// leaves a soft limit of 0 there otherwise, which restores nothing).
#[cfg(unix)]
static NOFILE_LIMIT: OnceLock<Option<libc::rlimit>> = OnceLock::new();

/// process.c:init_process_emacs's "Don't allocate more than FD_SETSIZE
/// file descriptors for Emacs itself": a soft RLIMIT_NOFILE above
/// FD_SETSIZE comes down to it, so a descriptor no fd_set can hold is
/// EMFILE wherever Emacs opens one.  The limit found is restore_nofile_limit's.
#[cfg(unix)]
pub(crate) fn init_process_nofile_limit() {
    NOFILE_LIMIT.get_or_init(|| {
        let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        // SAFETY: getrlimit fills LIMIT on success, the only case read.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
            return None;
        }
        // SAFETY: the successful call above initialized LIMIT.
        let found = unsafe { limit.assume_init() };
        if found.rlim_cur <= FD_SETSIZE as libc::rlim_t {
            return None;
        }
        let lowered = libc::rlimit {
            rlim_cur: FD_SETSIZE as libc::rlim_t,
            ..found
        };
        // SAFETY: LOWERED is a valid rlimit, no larger than the one found.
        (unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lowered) } == 0).then_some(found)
    });
}

/// The limit process.c:restore_nofile_limit gives back to a child:
/// RLIMIT_NOFILE as Emacs started, when init_process_emacs lowered it.
#[cfg(unix)]
fn nofile_limit_to_restore() -> Option<libc::rlimit> {
    NOFILE_LIMIT.get().copied().flatten()
}

/// report_file_errno (WHAT, Qnil, EMFILE): the error of a channel
/// FD_SETSIZE refuses.
#[cfg(unix)]
pub(crate) fn fd_setsize_error(interp: &Interpreter, what: &str) -> LispError {
    file_operation_error_value_without_file(
        interp,
        what,
        &std::io::Error::from_raw_os_error(libc::EMFILE),
    )
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
pub(crate) fn open_pipe_process_channels(
    interp: &Interpreter,
) -> Result<PipeProcessChannels, LispError> {
    let pipe_error = |error: std::io::Error| {
        file_operation_error_value_without_file(interp, "Creating pipe", &error)
    };
    let input = emacs_pipe().map_err(pipe_error)?;
    let output = emacs_pipe().map_err(pipe_error)?;
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        if beyond_fd_setsize(&[output.0.as_raw_fd(), input.1.as_raw_fd()]) {
            return Err(fd_setsize_error(interp, "Creating pipe"));
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

/// strerror's text for every errno below 256 in the current LC_MESSAGES
/// locale, read in the parent: the vfork child, which shares the parent's
/// memory, must not reach the allocator that a first translation of a
/// message may call.  Read again when the locale has changed.
#[cfg(target_os = "linux")]
fn strerror_table() -> std::sync::Arc<Vec<std::ffi::CString>> {
    type Table = (std::ffi::CString, std::sync::Arc<Vec<std::ffi::CString>>);
    static TABLE: std::sync::Mutex<Option<Table>> = std::sync::Mutex::new(None);
    // SAFETY: setlocale with a null locale only queries; the name is copied
    // at once.
    let locale = unsafe {
        let name = libc::setlocale(libc::LC_MESSAGES, std::ptr::null());
        if name.is_null() {
            std::ffi::CString::default()
        } else {
            std::ffi::CStr::from_ptr(name).to_owned()
        }
    };
    let mut table = TABLE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some((name, texts)) = table.as_ref()
        && *name == locale
    {
        return std::sync::Arc::clone(texts);
    }
    let texts = std::sync::Arc::new(
        (0..256)
            .map(|errno| {
                // SAFETY: strerror returns a NUL-terminated string, copied
                // at once.
                unsafe { std::ffi::CStr::from_ptr(libc::strerror(errno)) }.to_owned()
            })
            .collect::<Vec<_>>(),
    );
    *table = Some((locale, std::sync::Arc::clone(&texts)));
    texts
}

/// What callproc.c:emacs_spawn's vfork child is given: the standard
/// descriptors create_process made (-1 for one on the pty, or a standard
/// error to be the output), the pty's name and which streams are on it,
/// the directory, the image, the limit to restore, the signal mask to
/// restore, and emacs_perror's command name.
#[cfg(target_os = "linux")]
struct VforkChild<'a> {
    std_in: libc::c_int,
    std_out: libc::c_int,
    std_err: libc::c_int,
    pty_name: &'a std::ffi::CStr,
    pty_in: bool,
    pty_out: bool,
    directory: &'a std::ffi::CStr,
    image: &'a ExecImage,
    nofile_limit: Option<libc::rlimit>,
    old_mask: libc::sigset_t,
    command: &'a std::ffi::CStr,
    strerror: &'a [std::ffi::CString],
}

/// sysdep.c:emacs_perror in the vfork child: "COMMAND: MESSAGE:
/// strerror (ERRNO)\n" in one write when it fits a pipe's atomic write,
/// else in pieces.
///
/// # Safety
///
/// Only in the vfork child: it writes stack bytes and owned strings.
#[cfg(target_os = "linux")]
unsafe fn vfork_child_perror(child: &VforkChild, message: &std::ffi::CStr, errno: libc::c_int) {
    let error = usize::try_from(errno)
        .ok()
        .and_then(|errno| child.strerror.get(errno))
        .map_or(c"Unknown error", |text| text.as_c_str());
    let parts = [
        child.command.to_bytes(),
        b": ",
        message.to_bytes(),
        b": ",
        error.to_bytes(),
        b"\n",
    ];
    let mut buffer = [0u8; libc::PIPE_BUF];
    let mut length = 0usize;
    let mut fits = true;
    for part in parts {
        match buffer.get_mut(length..length.saturating_add(part.len())) {
            Some(slot) => {
                slot.copy_from_slice(part);
                length = length.saturating_add(part.len());
            }
            None => fits = false,
        }
    }
    // snprintf's `nbytes < sizeof buf': one write for what is shorter than
    // the buffer.
    let fits = fits && length < buffer.len();
    // SAFETY: each pointer and length is a live slice's.
    unsafe {
        if fits {
            libc::write(libc::STDERR_FILENO, buffer.as_ptr().cast(), length);
        } else {
            for part in parts {
                libc::write(libc::STDERR_FILENO, part.as_ptr().cast(), part.len());
            }
        }
    }
}

/// callproc.c:emacs_spawn's vfork child, then child_setup: the new
/// session, the pty opened by its name (emacs_perror and _exit
/// (EXIT_CANCELED) when that fails for the input), the default signal
/// dispositions and the parent's mask, child_setup_tty on the output side,
/// standard error with standard output unless given, the directory
/// (_exit (EXIT_CANCELED) if it is gone), restore_nofile_limit, the dup2s,
/// setpgid and tcsetpgrp, execve, and exec_failed.
#[cfg(target_os = "linux")]
extern "C" fn vfork_child_main(argument: *mut libc::c_void) -> libc::c_int {
    // SAFETY: ARGUMENT is the VforkChild emacs_vfork passed, alive while the
    // parent waits for this child to exec or exit; everything below is a
    // system call on owned data, on the child's own stack.
    unsafe {
        let child = &*(argument as *const VforkChild);
        // The parent's handlers cannot run here, in its memory: every
        // caught signal is reset (execve would reset them anyway).
        for signal in 1..65 {
            let mut action: libc::sigaction = std::mem::zeroed();
            if libc::sigaction(signal, std::ptr::null(), &mut action) == 0
                && action.sa_sigaction != libc::SIG_DFL
                && action.sa_sigaction != libc::SIG_IGN
            {
                let mut default: libc::sigaction = std::mem::zeroed();
                default.sa_sigaction = libc::SIG_DFL;
                libc::sigaction(signal, &default, std::ptr::null_mut());
            }
        }
        let (mut std_in, mut std_out, mut std_err) = (child.std_in, child.std_out, child.std_err);
        // dissociate_controlling_tty.
        if libc::setsid() < 0 {
            let tty = libc::open(c"/dev/tty".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC);
            if tty >= 0 {
                libc::ioctl(tty, libc::TIOCNOTTY, 0);
                libc::close(tty);
            }
        }
        // Make the pty's terminal the controlling terminal.
        if child.pty_in && std_in >= 0 {
            libc::ioctl(std_in, libc::TIOCSCTTY, 0);
        }
        // Now close the pty (if we had it open) and reopen it.
        if child.pty_in || child.pty_out {
            if child.pty_in && std_in >= 0 {
                libc::close(std_in);
            }
            let ptyfd = libc::open(child.pty_name.as_ptr(), libc::O_RDWR | libc::O_CLOEXEC);
            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            if child.pty_in {
                std_in = ptyfd;
            }
            if child.pty_out {
                std_out = ptyfd;
            }
            if std_in < 0 {
                vfork_child_perror(child, child.pty_name, errno);
                libc::_exit(EXIT_CANCELED);
            }
        }
        for signal in [libc::SIGINT, libc::SIGQUIT, libc::SIGPROF, libc::SIGPIPE] {
            libc::signal(signal, libc::SIG_DFL);
        }
        // Stop blocking SIGCHLD in the child.
        libc::sigprocmask(libc::SIG_SETMASK, &child.old_mask, std::ptr::null_mut());
        if child.pty_out {
            child_setup_tty(std_out);
        }
        if std_err < 0 {
            std_err = std_out;
        }
        // child_setup.
        if libc::chdir(child.directory.as_ptr()) < 0 {
            libc::_exit(EXIT_CANCELED);
        }
        if let Some(limit) = &child.nofile_limit {
            libc::setrlimit(libc::RLIMIT_NOFILE, limit);
        }
        libc::dup2(std_in, libc::STDIN_FILENO);
        libc::dup2(std_out, libc::STDOUT_FILENO);
        libc::dup2(std_err, libc::STDERR_FILENO);
        libc::setpgid(0, 0);
        libc::tcsetpgrp(0, libc::getpid());
        let image = child.image;
        libc::execve(image.argv[0], image.argv.as_ptr(), image.envp.as_ptr());
        let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
        // exec_failed: a diagnostic that cannot block, then the exit.
        libc::fcntl(libc::STDERR_FILENO, libc::F_SETFL, libc::O_NONBLOCK);
        vfork_child_perror(child, std::ffi::CStr::from_ptr(image.argv[0]), errno);
        libc::_exit(if errno == libc::ENOENT { 127 } else { 126 })
    }
}

/// emacs_spawn's vfork () on GNU/Linux: clone with CLONE_VM and
/// CLONE_VFORK, so the child shares the parent's memory, as vfork's does,
/// until it execs or exits, while the parent waits.  The parent's signals
/// stay blocked meanwhile, so that none of its handlers runs on the child's
/// side before the child has reset them.
#[cfg(target_os = "linux")]
fn emacs_vfork(child: &VforkChild) -> std::io::Result<libc::pid_t> {
    const STACK: usize = 256 * 1024;
    let mut stack = vec![0u8; STACK];
    // SAFETY: the stack's top, aligned down to 16 bytes, is inside STACK.
    let top = unsafe { stack.as_mut_ptr().add(STACK) } as usize & !15;
    // SAFETY: a full set blocked around clone and restored after it; clone
    // runs vfork_child_main on its own stack with CHILD alive, and returns
    // once the child has exec'd or exited.
    unsafe {
        let mut all: libc::sigset_t = std::mem::zeroed();
        let mut old: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut all);
        libc::pthread_sigmask(libc::SIG_SETMASK, &all, &mut old);
        let pid = libc::clone(
            vfork_child_main,
            top as *mut libc::c_void,
            libc::CLONE_VM | libc::CLONE_VFORK | libc::SIGCHLD,
            (child as *const VforkChild).cast_mut().cast(),
        );
        let error = std::io::Error::last_os_error();
        libc::pthread_sigmask(libc::SIG_SETMASK, &old, std::ptr::null_mut());
        drop(stack);
        if pid < 0 { Err(error) } else { Ok(pid) }
    }
}

/// Fmake_process's create_process for a child with a stream on a pty, on
/// GNU/Linux: allocate_pty (posix_spawn with pipes when none is to be
/// had), the pipes for the streams not on it, the stderr pipe process's
/// input closed, the FD_SETSIZE check, the coding systems, and
/// emacs_spawn's vfork path, where the child opens the pty's tty itself
/// (USG without USG_SUBTTY_WORKS).  None leaves the spawn to the fork path:
/// no pty is asked for, or an argument holds a NUL.
#[cfg(target_os = "linux")]
fn vfork_pty_process(
    interp: &mut Interpreter,
    env: &mut Env,
    spawn: (&std::ffi::OsStr, &[Vec<u8>], &ChildEnvironment),
    channels: ProcessChannels,
    stderr: Option<std::os::fd::RawFd>,
    setup_coding: &mut dyn FnMut(&mut Interpreter, &mut Env) -> Result<(), LispError>,
) -> Result<Option<RunningProcess>, LispError> {
    use std::os::fd::AsRawFd;
    let (program, argv, (environment, directory)) = spawn;
    let ProcessChannels {
        pty_modes: (input_pty, output_pty),
        stderr_process_id,
    } = channels;
    if !(input_pty || output_pty) {
        return Ok(None);
    }
    let Some(image) = ExecImage::new(program, argv, environment.clone()) else {
        return Ok(None);
    };
    let Ok(directory) = std::ffi::CString::new(directory.clone()) else {
        return Ok(None);
    };
    let Some((master, pty_name)) = allocate_pty() else {
        // No pty: pty_name stays NULL, and emacs_spawn posix_spawns.
        return posix_spawn_process(
            interp,
            env,
            spawn,
            ProcessChannels {
                pty_modes: (false, false),
                stderr_process_id,
            },
            stderr,
            setup_coding,
        );
    };
    let pipe = |interp: &Interpreter| {
        emacs_pipe().map_err(|error| {
            file_operation_error_value_without_file(interp, "Creating pipe", &error)
        })
    };
    // The master is both the channel Emacs reads and the one it writes
    // (ptychannel), one descriptor.
    let master = std::sync::Arc::new(master);
    let pty_input = input_pty.then(|| std::sync::Arc::clone(&master));
    let (stdin_read, stdin_write) = if input_pty {
        (None, None)
    } else {
        let (read, write) = pipe(interp)?;
        (Some(read), Some(write))
    };
    let (stdout_read, stdout_write) = if output_pty {
        (None, None)
    } else {
        let (read, write) = pipe(interp)?;
        (Some(read), Some(write))
    };
    let pty_output = if output_pty {
        Some(master)
    } else {
        // create_process puts ptychannel in an open_fd slot only as
        // READ_FROM_SUBPROCESS, when the pty is the output: an input-only
        // pty's master is p->outfd alone, which deactivate_process never
        // closes, so it stays open for the rest of the session.
        std::mem::forget(master);
        None
    };
    close_stderr_process_input(interp, stderr_process_id);
    let inchannel = stdout_read.as_ref().or(pty_output.as_deref());
    let outchannel = stdin_write.as_ref().or(pty_input.as_deref());
    if beyond_fd_setsize(
        &[inchannel, outchannel].map(|channel| channel.map_or(-1, |file| file.as_raw_fd())),
    ) {
        return Err(fd_setsize_error(interp, "Creating pipe"));
    }
    #[cfg(unix)]
    let exec_monitor = exec_monitor_pipe(interp)?;
    setup_coding(interp, env)?;
    use std::os::unix::ffi::OsStrExt;
    // initial_argv[0], whatever it holds, "emacs" when there is none.
    let command = std::env::args_os()
        .next()
        .and_then(|name| std::ffi::CString::new(name.as_bytes()).ok())
        .unwrap_or_else(|| c"emacs".to_owned());
    let strerror = strerror_table();
    let strerror = strerror.as_slice();
    // SAFETY: an empty set filled by pthread_sigmask with the current mask.
    let old_mask = unsafe {
        let mut mask: libc::sigset_t = std::mem::zeroed();
        libc::pthread_sigmask(libc::SIG_SETMASK, std::ptr::null(), &mut mask);
        mask
    };
    let child = VforkChild {
        std_in: stdin_read.as_ref().map_or(-1, AsRawFd::as_raw_fd),
        std_out: stdout_write.as_ref().map_or(-1, AsRawFd::as_raw_fd),
        std_err: stderr.unwrap_or(-1),
        pty_name: &pty_name,
        pty_in: input_pty,
        pty_out: output_pty,
        directory: &directory,
        image: &image,
        nofile_limit: nofile_limit_to_restore(),
        old_mask,
        command: &command,
        strerror,
    };
    let pid = emacs_vfork(&child).map_err(|error| vfork_error(interp, &error))?;
    // Close the pipe ends that the child uses.
    drop((stdin_read, stdout_write));
    #[cfg(unix)]
    await_exec_monitor(exec_monitor);
    let mut child = SpawnedChild::new(pid as u32, stdin_write, stdout_read, None);
    // create_process's p->alive = 1, SIGCHLD held back until then.
    #[cfg(unix)]
    child.watch();
    Ok(Some(RunningProcess {
        child,
        terminate_on_drop: true,
        pty_input,
        pty_output,
        pty_slave_name: Some(pty_name.to_string_lossy().into_owned()),
        stdout_at_end: false,
        stderr_at_end: false,
        child_ended: false,
    }))
}

/// Where emacs_spawn puts one of the child's standard descriptors.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum SpawnFd {
    /// This descriptor of the parent's.
    Fd(std::os::fd::RawFd),
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
            for (target, source) in stdio.into_iter().enumerate() {
                let target = target as libc::c_int;
                match source {
                    SpawnFd::Fd(fd) => {
                        check(libc::posix_spawn_file_actions_adddup2(actions, fd, target))?
                    }
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

/// Fmake_process's create_process through emacs_spawn's posix_spawn,
/// which it takes only with no pty (`use_posix_spawn = pty_name ==
/// NULL'): the pipes, and the child's standard error on the `:stderr'
/// pipe process's pipe, else where its output goes.  None leaves the
/// spawn to the vfork path: a stream to be on a pty, or an argument with a
/// NUL.
#[cfg(target_os = "linux")]
fn posix_spawn_process(
    interp: &mut Interpreter,
    env: &mut Env,
    (program, argv, (environment, directory)): (&std::ffi::OsStr, &[Vec<u8>], &ChildEnvironment),
    ProcessChannels {
        pty_modes: (input_pty, output_pty),
        stderr_process_id,
    }: ProcessChannels,
    stderr: Option<std::os::fd::RawFd>,
    setup_coding: &mut dyn FnMut(&mut Interpreter, &mut Env) -> Result<(), LispError>,
) -> Result<Option<RunningProcess>, LispError> {
    use std::os::fd::AsRawFd;
    if input_pty || output_pty {
        return Ok(None);
    }
    let Some(image) = ExecImage::new(program, argv, environment.clone()) else {
        return Ok(None);
    };
    let Ok(directory) = std::ffi::CString::new(directory.clone()) else {
        return Ok(None);
    };
    let pipe_error = |error: std::io::Error| {
        file_operation_error_value_without_file(interp, "Creating pipe", &error)
    };
    let (input_read, input_write) = emacs_pipe().map_err(pipe_error)?;
    let (output_read, output_write) = emacs_pipe().map_err(pipe_error)?;
    close_stderr_process_input(interp, stderr_process_id);
    // create_process: the channel Emacs reads and the one it writes must
    // fit an fd_set; none is spawned otherwise.
    if beyond_fd_setsize(&[output_read.as_raw_fd(), input_write.as_raw_fd()]) {
        return Err(fd_setsize_error(interp, "Creating pipe"));
    }
    #[cfg(unix)]
    let exec_monitor = exec_monitor_pipe(interp)?;
    setup_coding(interp, env)?;
    let stdio = [
        SpawnFd::Fd(input_read.as_raw_fd()),
        SpawnFd::Fd(output_write.as_raw_fd()),
        stderr.map_or(SpawnFd::Stdout, SpawnFd::Fd),
    ];
    let pid = emacs_posix_spawn(&image, Some(&directory), stdio)
        .map_err(|error| vfork_error(interp, &error))?;
    // Only the child keeps its ends.
    drop((input_read, output_write));
    #[cfg(unix)]
    await_exec_monitor(exec_monitor);
    let mut child = SpawnedChild::new(pid as u32, Some(input_write), Some(output_read), None);
    // create_process's p->alive = 1, SIGCHLD held back until then.
    #[cfg(unix)]
    child.watch();
    Ok(Some(RunningProcess {
        child,
        terminate_on_drop: true,
        pty_input: None,
        pty_output: None,
        pty_slave_name: None,
        stdout_at_end: false,
        stderr_at_end: false,
        child_ended: false,
    }))
}

/// Fmake_process's p->pty_in and p->pty_out: CONNECTION_TYPE's car and
/// cdr, or it for both, through is_pty_from_symbol, `process-connection-
/// type' answering for nil.
pub(crate) fn process_connection_pty_modes(
    interp: &Interpreter,
    connection_type: Option<&Value>,
    default_pty: bool,
) -> Result<(bool, bool), LispError> {
    let Some(connection_type) = connection_type else {
        return Ok((default_pty, default_pty));
    };
    if let Some((input, output)) = connection_type.cons_values() {
        return Ok((
            is_pty_from_symbol(interp, &input, default_pty)?,
            is_pty_from_symbol(interp, &output, default_pty)?,
        ));
    }
    let pty = is_pty_from_symbol(interp, connection_type, default_pty)?;
    Ok((pty, pty))
}

/// process.c:is_pty_from_symbol: EQ `pty', EQ `pipe', NILP for the
/// default, else report_file_error ("Unknown connection type", SYMBOL)
/// with the errno that holds.
fn is_pty_from_symbol(
    interp: &Interpreter,
    value: &Value,
    default_pty: bool,
) -> Result<bool, LispError> {
    match crate::lisp::types::eq_dispatch(value).kind() {
        Kind::Nil => Ok(default_pty),
        Kind::Symbol(name) if name.is_named("pty") => Ok(true),
        Kind::Symbol(name) if name.is_named("pipe") => Ok(false),
        _ => Err(crate::lisp::primitives::report_file_errno(
            interp,
            "Unknown connection type",
            *value,
            &std::io::Error::last_os_error(),
        )),
    }
}

#[cfg(unix)]
/// process.c:allocate_pty with GNU/Linux's PTY_OPEN and
/// PTY_TTY_NAME_SPRINTF: posix_openpt (again without O_CLOEXEC on
/// EINVAL), grantpt and unlockpt with SIGCHLD blocked, ptsname, the
/// close-on-exec flag, faccessat of the slave for reading and writing,
/// then setup_pty's FIONBIO.  None, with nothing signaled, when a step
/// fails: create_process then uses pipes, and create_pty makes no pty.
pub(crate) fn allocate_pty() -> Option<(fs::File, std::ffi::CString)> {
    let flags = libc::O_RDWR | libc::O_NOCTTY;
    // SAFETY: posix_openpt returns a fresh descriptor or -1.
    let mut fd = unsafe { libc::posix_openpt(flags | libc::O_CLOEXEC) };
    if fd < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINVAL) {
        // SAFETY: as above.
        fd = unsafe { libc::posix_openpt(flags) };
    }
    if fd < 0 {
        return None;
    }
    // SAFETY: ownership of the fresh descriptor is transferred exactly once;
    // an early return below closes it, as emacs_close does.
    let master = unsafe { fs::File::from_raw_fd(fd) };
    // SAFETY: the signal set is initialized by sigemptyset before use;
    // grantpt, unlockpt and ptsname act on the live master, and ptsname's
    // static string is copied before anything can reuse it.
    let name = unsafe {
        let mut blocked = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
        libc::sigemptyset(blocked.as_mut_ptr());
        libc::sigaddset(blocked.as_mut_ptr(), libc::SIGCHLD);
        let blocked = blocked.assume_init();
        libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, std::ptr::null_mut());
        let name = if libc::grantpt(fd) != -1 && libc::unlockpt(fd) != -1 {
            libc::ptsname(fd)
        } else {
            std::ptr::null_mut()
        };
        let name = (!name.is_null()).then(|| std::ffi::CStr::from_ptr(name).to_owned());
        libc::pthread_sigmask(libc::SIG_UNBLOCK, &blocked, std::ptr::null_mut());
        name
    }?;
    // SAFETY: fcntl, faccessat and ioctl act on the live master and on a
    // NUL-terminated name.
    unsafe {
        libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
        if libc::faccessat(
            libc::AT_FDCWD,
            name.as_ptr(),
            libc::R_OK | libc::W_OK,
            libc::AT_EACCESS,
        ) != 0
        {
            return None;
        }
        let on: libc::c_int = 1;
        libc::ioctl(fd, libc::FIONBIO, &on);
    }
    Some((master, name))
}

/// process.c's `#if ! defined (USG) || defined (USG_SUBTTY_WORKS)': where
/// create_pty opens the pty's tty in Emacs, holding it as
/// WRITE_TO_SUBPROCESS.  GNU/Linux defines USG and not USG_SUBTTY_WORKS,
/// so there the tty is opened only by the child, or by whoever else opens
/// it, and a childless pty's master reads EIO once the last such opener
/// closes it; macOS defines neither.
pub(crate) const PARENT_OPENS_PTY_TTY: bool =
    !cfg!(any(target_os = "linux", target_os = "android"));

#[cfg(unix)]
/// create_process's and create_pty's emacs_open (PTY_NAME, O_RDWR |
/// O_NOCTTY, 0) of the pty's tty, where the parent opens it, and
/// report_file_error ("Opening pty", Qnil) when it fails.
pub(crate) fn open_pty_tty(
    interp: &Interpreter,
    name: &std::ffi::CStr,
) -> Result<fs::File, LispError> {
    // SAFETY: NAME is NUL-terminated; open returns a fresh descriptor or -1.
    let fd = unsafe {
        libc::open(
            name.as_ptr(),
            libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(file_operation_error_value_without_file(
            interp,
            "Opening pty",
            &std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: ownership of the fresh descriptor is transferred exactly once.
    Ok(unsafe { fs::File::from_raw_fd(fd) })
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

/// The process connection makers' `buffer = Fget_buffer_create (buffer,
/// Qnil)' of a non-nil buffer argument: a buffer object as it is, live or
/// killed, else the buffer of that name, made when there is none (an
/// empty name refused, `buffer-list-update-hook' run), CHECK_STRING of
/// anything else.  Nil stays nil.  With the buffer, its id while it is
/// live, which update_process_mark's marker needs.
pub(crate) fn process_buffer_target(
    interp: &mut Interpreter,
    env: &mut Env,
    value: &Value,
) -> Result<(Option<u64>, Value), LispError> {
    if value.is_nil() {
        return Ok((None, Value::Nil));
    }
    let buffer = call(interp, "get-buffer-create", &[*value], env)?;
    let buffer_id = match buffer.kind() {
        Kind::Buffer(buffer) => interp.live_buffer_id(buffer),
        _ => None,
    };
    Ok((buffer_id, buffer))
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
            .default_value_key(cached_symbol!("enable-multibyte-characters"))
            .is_some_and(|value| value.is_nil()),
        _ => interp
            .resolve_buffer_id(&buffer)
            .ok()
            .and_then(|id| interp.get_buffer_by_id(id))
            .is_some_and(|buffer| !buffer.is_multibyte()),
    };
    let current_unibyte = !interp.buffer.borrow().is_multibyte();
    // Vdefault_process_coding_system as each fallback reads it, after
    // the find-operation-coding-system call before it.
    let default = |interp: &Interpreter, env: &Env| {
        interp
            .lookup_var_key(cached_symbol!("default-process-coding-system"), env)
            .and_then(|value| value.cons_values())
    };
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
        .lookup_var_key(cached_symbol!("coding-system-for-read"), env)
        .filter(|value| !value.is_nil())
    {
        value
    } else if buffer_unibyte || serial {
        Value::Nil
    } else {
        match operation_coding(interp, env)?.cons_values() {
            Some((found, _)) => found,
            None => default(interp, env).map_or(Value::Nil, |(decoding, _)| decoding),
        }
    };
    let encoding = if !coding.is_nil() {
        coding
            .cons_values()
            .map_or(coding, |(_, encoding)| encoding)
    } else if let Some(value) = interp
        .lookup_var_key(cached_symbol!("coding-system-for-write"), env)
        .filter(|value| !value.is_nil())
    {
        value
    } else if serial || current_unibyte {
        Value::Nil
    } else {
        match operation_coding(interp, env)?.cons_values() {
            Some((_, found)) => found,
            None => default(interp, env).map_or(Value::Nil, |(_, encoding)| encoding),
        }
    };
    Ok((decoding, encoding))
}

/// process.c Fmake_process's codings for a new process: CODING's (its car
/// to decode and cdr to encode when a cons), else `coding-system-for-read'
/// and `coding-system-for-write', else, for a process with a program
/// (PROGRAM_GIVEN), what find-operation-coding-system answers for
/// `start-process' with NAME, BUFFER and the COMMAND's elements, else
/// `default-process-coding-system's -- left for the first output or input
/// to complete.  Each fallback takes list_length (COMMAND) first, a
/// program or none, so a COMMAND that is no list is CHECK_LIST_END's
/// error there.
pub(crate) fn make_process_coding_systems(
    interp: &mut Interpreter,
    env: &mut Env,
    coding: Value,
    name: Value,
    buffer: Value,
    command: Value,
    program_given: bool,
) -> Result<(Value, Value), LispError> {
    // Vdefault_process_coding_system as each fallback reads it, after
    // the find-operation-coding-system call before it.
    let default = |interp: &Interpreter, env: &Env| {
        interp
            .lookup_var_key(cached_symbol!("default-process-coding-system"), env)
            .and_then(|value| value.cons_values())
    };
    let mut coding_systems = None;
    let mut operation_coding = |interp: &mut Interpreter, env: &mut Env| {
        if let Some(found) = coding_systems {
            return Ok(found);
        }
        // list_length (command): CHECK_LIST_END of a proper list.
        let elements = command
            .to_vec()
            .map_err(|_| LispError::WrongTypeArgument("listp".into(), command))?;
        if !program_given {
            // coding_systems stays Qt; the next fallback counts again.
            return Ok(Value::Nil);
        }
        let lookup = [Value::symbol("start-process"), name, buffer]
            .into_iter()
            .chain(elements)
            .collect::<Vec<_>>();
        let found = find_operation_coding_system_value(interp, &lookup, env)?;
        coding_systems = Some(found);
        Ok::<Value, LispError>(found)
    };
    let mut decoding = if coding.is_nil() {
        interp
            .lookup_var_key(cached_symbol!("coding-system-for-read"), env)
            .unwrap_or(Value::Nil)
    } else {
        coding
            .cons_values()
            .map_or(coding, |(decoding, _)| decoding)
    };
    if decoding.is_nil() {
        decoding = match operation_coding(interp, env)?.cons_values() {
            Some((found, _)) => found,
            None => default(interp, env).map_or(Value::Nil, |(decoding, _)| decoding),
        };
    }
    let mut encoding = if coding.is_nil() {
        interp
            .lookup_var_key(cached_symbol!("coding-system-for-write"), env)
            .unwrap_or(Value::Nil)
    } else {
        coding
            .cons_values()
            .map_or(coding, |(_, encoding)| encoding)
    };
    if encoding.is_nil() {
        encoding = match operation_coding(interp, env)?.cons_values() {
            Some((_, found)) => found,
            None => default(interp, env).map_or(Value::Nil, |(_, encoding)| encoding),
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
                load_coding_system(interp, &coding, env)?;
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

/// process.c's frame around a process filter or sentinel
/// (read_process_output with read_and_dispose_of_process_output, and
/// exec_sentinel): the current buffer and `deactivate-mark' kept,
/// `inhibit-quit' and `last-nonmenu-event' bound to t, and the match data
/// protected.  running_asynch_code is set while BODY runs, so its first
/// search puts aside the match data it found (save_search_regs), restored
/// after; run inside another such run, the outer run's saved data is
/// restored first and the current data kept across this one by an unwind
/// of `set-match-data', as GNU's recursive case does.
fn run_asynch_code(
    interp: &mut Interpreter,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<(), LispError>,
) -> Result<(), LispError> {
    let odeactivate = interp
        .lookup_var_key(cached_symbol!("deactivate-mark"), env)
        .unwrap_or(Value::Nil);
    // record_unwind_current_buffer.
    let saved_buffer_id = interp.current_buffer_id();
    let count = interp.specpdl_index();
    interp.specbind_symbol(&"inhibit-quit".into(), Value::T, env)?;
    interp.specbind_symbol(&"last-nonmenu-event".into(), Value::T, env)?;
    let outer_running_asynch_code = interp.running_asynch_code;
    let mut outer_match_data = None;
    if outer_running_asynch_code {
        let current = call(interp, "match-data", &[], env)?;
        crate::lisp::primitives::restore_search_regs(interp);
        // record_unwind_save_match_data.
        outer_match_data = Some(call(interp, "match-data", &[], env)?);
        call(interp, "set-match-data", &[current, Value::T], env)?;
    }
    interp.running_asynch_code = true;
    let result = body(interp, env);
    crate::lisp::primitives::restore_search_regs(interp);
    interp.running_asynch_code = outer_running_asynch_code;
    // Handling the process output or event does not deactivate the mark.
    interp.set_c_variable("deactivate-mark", odeactivate);
    // unbind_to: the match data's unwind, the bindings, the buffer.
    let restored = match outer_match_data {
        Some(list) => call(interp, "set-match-data", &[list, Value::T], env).map(|_| ()),
        None => Ok(()),
    };
    let unbound = interp.unbind_to(count, env);
    if interp.current_buffer_id() != saved_buffer_id && interp.has_buffer_id(saved_buffer_id) {
        interp.set_current_buffer_id(saved_buffer_id)?;
    }
    result.and(restored).and(unbound)
}

/// process.c's internal_condition_case_1 around a filter's or a
/// sentinel's call: its handlers `error' unless `debug-on-error' is
/// non-nil (then none, every signal passing on).  A caught error's data
/// (made a cons first for a sentinel, as exec_sentinel_error_handler does)
/// goes to cmd_error_internal with CONTEXT, which in batch prints it and
/// kills Emacs; then `inhibit-quit' is t and, while
/// `process-error-pause-time' is positive, that many seconds pass.
/// Handlers `error' catch every signal, a quit with them; a throw passes
/// on.
fn call_catching_process_errors(
    interp: &mut Interpreter,
    env: &mut Env,
    context: &str,
    sentinel: bool,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<(), LispError>,
) -> Result<(), LispError> {
    if interp
        .lookup_var_key(cached_symbol!("debug-on-error"), env)
        .is_some_and(|value| value.is_truthy())
    {
        return body(interp, env);
    }
    let handlers = interp.push_internal_condition_case(crate::lisp::eval::InternalHandlers::Error);
    let result = body(interp, env);
    interp.pop_handler_bindings(handlers);
    let error = match result {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    // find_handler_clause: handlers `error' catch every signal, as t does.
    if matches!(
        error.kind(),
        LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
    ) {
        return Err(error);
    }
    interp.clear_batch_error_backtrace();
    let mut data = crate::lisp::eval::error_condition_value(&error);
    if sentinel && data.cons_ref().is_none() {
        data = Value::cons(Value::symbol("error"), data);
    }
    crate::lisp::primitives::interactive::cmd_error_internal(interp, env, data, context)?;
    interp.set_c_variable("inhibit-quit", Value::T);
    let pause = interp
        .lookup_var_key(cached_symbol!("process-error-pause-time"), env)
        .and_then(|value| value.integer_value())
        .unwrap_or(0);
    if pause > 0 {
        call(interp, "sleep-for", &[Value::Integer(pause)], env)?;
    }
    Ok(())
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
    // What the read logged (emacs_gnutls_read's), shown before the filter
    // runs, as read_process_output's messages are.
    crate::lisp::primitives::dispatch::gnutls::flush_gnutls_log(interp, env)?;
    interp.note_process_output_delivery(process_id);
    // read_and_dispose_of_process_output: with `fast-read-process-output'
    // and the default filter, read_and_insert_process_output, outside any
    // condition case, so its errors (an after-change function's) reach
    // the code that waited.
    let default_filter = interp.process_filter(process_id).is_none_or(|filter| {
        crate::lisp::types::eq_symbol_named(filter, "internal-default-process-filter")
    });
    let fast = default_filter
        && interp
            .lookup_var_key(cached_symbol!("fast-read-process-output"), env)
            .is_some_and(|value| value.is_truthy());
    run_asynch_code(interp, env, |interp, env| {
        if fast {
            return dispose_of_process_output(interp, process_id, output, env);
        }
        call_catching_process_errors(
            interp,
            env,
            "error in process filter: ",
            false,
            |interp, env| dispose_of_process_output(interp, process_id, output, env),
        )
    })
}

/// read_and_dispose_of_process_output's call of the filter (explicit, or
/// the default one inserting into the process buffer).
fn dispose_of_process_output(
    interp: &mut Interpreter,
    process_id: u64,
    output: Value,
    env: &mut Env,
) -> Result<(), LispError> {
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
        // buffer (run_asynch_code restores it after).  GNU hands the
        // filter an ordinary mutable string; filters such as eshell's
        // propertize it in place.
        call_function_value(
            interp,
            &filter,
            &[interp.record_value(process_id), output],
            env,
        )?;
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
        interp.set_buffer_local_value_key(
            buffer_id,
            cached_symbol!("buffer-read-only"),
            Value::Nil,
        );
    }
    let result = interp.with_lisp_stack_roots(&read_only, |interp| body(interp, env));
    if !read_only.is_nil() && interp.has_buffer_id(buffer_id) {
        interp.set_buffer_local_value_key(buffer_id, cached_symbol!("buffer-read-only"), read_only);
    }
    result
}

/// GNU `internal-default-process-sentinel'.  Non-running state changes are
/// appended at the process mark without letting a process callback retarget
/// the caller's current buffer or point.
pub(crate) fn internal_default_process_sentinel(
    interp: &mut Interpreter,
    process_id: u64,
    message: Value,
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
    let process_name = interp.process_name(process_id).unwrap_or(Value::Nil);
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
        // insert_string ("\nProcess "); Finsert (1, &p->name);
        // insert_string (" ").
        insert_text_with_hooks(interp, "\nProcess ", None, &[], false, false, env)?;
        crate::lisp::primitives::hash_insert::insert_impl(
            interp,
            &[process_name],
            env,
            false,
            false,
        )?;
        insert_text_with_hooks(interp, " ", None, &[], false, false, env)?;
        // Finsert (1, &msg): a message that is no string or character
        // signals once the rest is in, as GNU's does.
        crate::lisp::primitives::hash_insert::insert_impl(interp, &[message], env, false, false)
            .map(|_| ())
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
        .lookup_var_key(cached_symbol!("process-environment"), env)
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
                .lookup_var_key(cached_symbol!("initial-environment"), env)
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
        .lookup_var_key(cached_symbol!("process-environment"), env)
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
            .lookup_var_key(cached_symbol!("initial-environment"), env)
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
        Kind::StringObject(_) => interp
            .find_buffer_named(destination)
            .unwrap_or_else(|| interp.create_buffer_named(*destination)),
        // Fget_buffer_create's CHECK_STRING.
        _ => return Err(wrong_type_argument("stringp", *destination)),
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
        .lookup_var_key(cached_symbol!("coding-system-for-read"), env)
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
                .lookup_var_key(cached_symbol!("default-process-coding-system"), env)
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
        interp.set_variable_key(
            cached_symbol!("last-coding-system-used"),
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
        .lookup_var_key(cached_symbol!("inhibit-eol-conversion"), env)
        .is_some_and(|value| value.is_truthy());
    // CODING_MAY_REQUIRE_DECODING: every coding but a raw-text one with a
    // unix eol and no post-read conversion requires decoding or detection.
    let requires_decoding = interp.coding_system_kind_name(&canonical).as_deref()
        != Some("raw-text")
        || !(inhibit_eol_conversion || coding_eol_type(interp, &canonical) == Some(0))
        || coding_attr(interp, &canonical, ":post-read-conversion")
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
        mut decoded,
        coding: coding_used,
        ..
    } = super::coding::decode_coding_block(interp, env, bytes, coding, Some(multibyte), decoder)?;
    let mut produced = decoded.char_count();
    let layout = decoded.take_layout();
    let props = decoded_annotation_props(&mut decoded);
    insert_text_before_point(
        interp,
        &decoded.text,
        props.as_ref(),
        &decoded.extended,
        produced,
        true,
        layout.as_ref(),
        env,
    )?;
    let post_read = coding_attr(
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
            .lookup_var_key(cached_symbol!("buffer-undo-list"), env)
            .unwrap_or(Value::Nil);
        interp.set_variable_key(cached_symbol!("buffer-undo-list"), Value::T, env);
        let result = interp.safe_funcall(function, &[Value::Integer(produced as i64)], env);
        interp.set_variable_key(cached_symbol!("buffer-undo-list"), undo_list, env);
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
    load_coding_system(interp, &decoding, env)?;
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
    let text = decoded_string_value(interp, decoded, !for_unibyte, env)?;
    let settled_value = Value::symbol(&settled);
    interp.set_variable_key(
        cached_symbol!("last-coding-system-used"),
        settled_value,
        env,
    );
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
    // The messages a read made (emacs_gnutls_read's), shown before the
    // filter runs.
    crate::lisp::primitives::dispatch::gnutls::flush_gnutls_log(interp, env)?;
    let stderr_process_id = interp.process_stderr(process_id).unwrap_or(process_id);
    let mut delivered = deliver_process_reads(interp, process_id, stdout, env)?;
    delivered |= deliver_process_reads(interp, stderr_process_id, stderr, env)?;
    // A read that found a pipe's end: read_process_output sets
    // CODING_MODE_LAST_BLOCK and decodes what was carried over.
    let (stdout_eof, stderr_eof) = interp.take_process_pipe_eof(process_id);
    if stdout_eof {
        delivered |= flush_process_decoder(interp, process_id, env)?.1;
        interp.finish_pipe_process_at_eof(process_id);
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
    // handle_child_signal records every child's change as its SIGCHLD
    // comes, whatever thread the process is locked to, an ended child's
    // channel leaving the wait set before this pass reads it.
    for process_id in interp.child_process_ids() {
        interp.refresh_process_id(process_id)?;
    }
    let ids = interp.live_external_process_ids();
    let mut progressed = false;
    for process_id in ids {
        if only_process_id.is_some_and(|id| process_id != id) {
            // JUST-THIS-ONE takes the other descriptors out of the wait
            // set, not SIGCHLD; status_notify reads what remains of a
            // changed process's output, once no channel has input.
            continue;
        }
        if interp.process_output_paused(process_id) {
            // A stopped process's channel leaves the wait set; its status
            // changes still come by SIGCHLD.
            continue;
        }
        // handle_child_signal's delete_read_fd: an ended child's output
        // waits for status_notify, its `:stderr' pipe still read.
        if interp.process_read_fd_deleted(process_id) {
            let stderr = interp.poll_process_stderr(process_id)?;
            progressed |= deliver_process_streams(interp, process_id, &[], &stderr, env)?;
            continue;
        }
        let (stdout, stderr) = interp.poll_process_output(process_id)?;
        progressed |= deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
    }
    // Changes that came while this pass read; status_notify walks every
    // process, so a wait notifies the others' changes as well, once no
    // channel has input left.
    for process_id in interp.child_process_ids() {
        interp.refresh_process_id(process_id)?;
    }
    if interp.any_process_exit_awaits_notification() && !interp.process_input_available() {
        progressed |= status_notify(interp, env, None)?;
    }
    Ok(progressed)
}

/// process.c:status_notify (DELETING, NULL): each process of
/// Vprocess_alist, newest first, whose status changed since status_notify
/// last looked at it (p->tick != p->update_tick, asked as the walk reaches
/// it, the changes handle_child_signal recorded taken first): unless its
/// filter is t, it connects or listens, it is a stopped connection, it
/// has no input channel or it is DELETING, its output read until a read
/// finds nothing; status_message of its status then, a terminal status
/// removing it (`delete-exited-processes') or deactivating it; and its
/// sentinel.  True when any sentinel ran.
pub(crate) fn status_notify(
    interp: &mut Interpreter,
    env: &mut Env,
    deleting: Option<u64>,
) -> Result<bool, LispError> {
    Ok(status_notify_walk(interp, env, deleting, None)?.0)
}

/// status_notify (DELETING, WAIT_PROC): whether any sentinel ran, and its
/// got_some_output, the most one read of WAIT_PROC's output (any
/// process's, with none) took, -1 when none was read.
pub(crate) fn status_notify_walk(
    interp: &mut Interpreter,
    env: &mut Env,
    deleting: Option<u64>,
    wait_proc: Option<u64>,
) -> Result<(bool, i64), LispError> {
    // handle_child_signal records every child's change as its SIGCHLD
    // comes, whatever thread the process is locked to.
    for process_id in interp.child_process_ids() {
        interp.refresh_process_id(process_id)?;
    }
    interp.begin_status_notify();
    let mut ran = false;
    let mut got_some_output: i64 = -1;
    for process_id in interp.status_notify_order() {
        if interp.child_process_ids().contains(&process_id) {
            interp.refresh_process_id(process_id)?;
        }
        if !interp.take_process_status_change(process_id) {
            continue;
        }
        // "If process is still active, read any output that remains."
        while interp.status_notify_reads_output(process_id, deleting) {
            let nread = match read_process_output(interp, env, process_id)? {
                ProcessRead::Bytes(nread) => nread as i64,
                _ => -1,
            };
            if wait_proc.is_none_or(|id| id == process_id) && got_some_output < nread {
                got_some_output = nread;
            }
            if nread <= 0 {
                break;
            }
        }
        let Some(message) = interp.status_notify_message(process_id) else {
            continue;
        };
        run_process_sentinel(interp, process_id, &message, env)?;
        ran = true;
    }
    Ok((ran, got_some_output))
}

/// What read_process_output returned: the bytes it read with the
/// carryover the last read left (0 at the channel's end), or -1 with
/// errno EAGAIN, EIO or another.
pub(crate) enum ProcessRead {
    Bytes(usize),
    WouldBlock,
    Eio,
    Error,
}

/// process.c:read_process_output (PROC, p->infd): one read of at most
/// p->readmax bytes, the adaptive read buffering adjusted by it and the
/// bytes added to p->nbytes_read, then decoded after the carryover and
/// given to the filter (read_and_dispose_of_process_output); the first
/// read of zero sets CODING_MODE_LAST_BLOCK and decodes the carryover.
#[cfg(unix)]
pub(crate) fn read_process_output(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
) -> Result<ProcessRead, LispError> {
    use crate::lisp::eval::ChannelOutcome;
    let carryover = interp.process_decoding_carryover(process_id);
    let readmax = interp.process_read_max(process_id);
    let outcome = interp.read_process_channel(process_id)?;
    match outcome {
        ChannelOutcome::Data(bytes) => {
            let (count, skip) = interp.adapt_read_output_delay(process_id, bytes.len(), readmax);
            adjust_process_output_delay_count(count);
            if skip {
                PROCESS_OUTPUT_SKIP.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            interp.add_process_nbytes_read(process_id, bytes.len());
            let output = decode_process_output(interp, process_id, &bytes, env)?;
            deliver_process_output_decoded(interp, process_id, output, env)?;
            Ok(ProcessRead::Bytes(bytes.len() + carryover))
        }
        ChannelOutcome::End => {
            let (was_last, carried) = interp.mark_process_last_block(process_id);
            if was_last {
                return Ok(ProcessRead::Bytes(0));
            }
            let output = decode_process_output(interp, process_id, &[], env)?;
            deliver_process_output_decoded(interp, process_id, output, env)?;
            Ok(ProcessRead::Bytes(carried))
        }
        ChannelOutcome::WouldBlock => Ok(ProcessRead::WouldBlock),
        ChannelOutcome::Eio => Ok(ProcessRead::Eio),
        ChannelOutcome::Error => Ok(ProcessRead::Error),
    }
}

/// process.c's process_output_delay_count: the processes whose
/// read_output_delay is not zero.
static PROCESS_OUTPUT_DELAY_COUNT: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(0);

/// deactivate_process's `if (--process_output_delay_count < 0)
/// process_output_delay_count = 0' for a process that had a delay.
pub(crate) fn process_output_delay_given_up() {
    use std::sync::atomic::Ordering::Relaxed;
    let _ =
        PROCESS_OUTPUT_DELAY_COUNT.try_update(Relaxed, Relaxed, |count| Some((count - 1).max(0)));
}

/// send_process's process_output_delay_count-- once a write gave up a
/// process's delay.
pub(crate) fn process_output_delay_written() {
    PROCESS_OUTPUT_DELAY_COUNT.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
}
/// process.c's process_output_skip: some process's read_output_skip is
/// set.
static PROCESS_OUTPUT_SKIP: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
/// wait_reading_process_output's static last_read_channel.
static LAST_READ_CHANNEL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);

fn adjust_process_output_delay_count(change: i32) {
    if change != 0 {
        let count = PROCESS_OUTPUT_DELAY_COUNT.load(std::sync::atomic::Ordering::Relaxed) + change;
        PROCESS_OUTPUT_DELAY_COUNT.store(count.max(0), std::sync::atomic::Ordering::Relaxed);
    }
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

/// process.c:Fdelete_process after get_process: the change it makes to
/// PROCESS_ID notified by status_notify (P, NULL), whose sentinels catch
/// their own errors (exec_sentinel), then remove_process; a throw out of
/// a sentinel, or an error `debug-on-error' let through, leaves the
/// process where the walk left it.
pub(crate) fn delete_process_notifying(
    interp: &mut Interpreter,
    process_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    if interp.delete_process(process_id)? {
        status_notify(interp, env, Some(process_id))?;
    }
    interp.remove_process(process_id);
    // remove_process's deactivate_process: emacs_gnutls_deinit's message.
    crate::lisp::primitives::dispatch::gnutls::flush_gnutls_log(interp, env)
}

/// process.c:kill_buffer_processes: the processes of BUFFER_ID, or with
/// None every process, as FOR_EACH_PROCESS reaches each on Vprocess_alist:
/// a network, serial or pipe connection given to Fdelete_process, a
/// subprocess whose channel is open sent SIGHUP (process_send_signal
/// (proc, SIGHUP, Qnil, 1)).  The walk follows each cell's cdr after its
/// process is dealt with, so a process a deletion's status_notify took off
/// the alist before the walk reached it is passed over, unless the cell
/// that led to it had left the alist first.
pub(crate) fn kill_buffer_processes(
    interp: &mut Interpreter,
    env: &mut Env,
    buffer_id: Option<u64>,
) -> Result<(), LispError> {
    let cells = interp.process_alist_cells();
    let mut index = (!cells.is_empty()).then_some(0);
    while let Some(at) = index {
        let process_id = interp.process_alist_cell_id(cells[at]);
        match interp.kill_buffer_process_action(cells[at], buffer_id) {
            Some(true) => delete_process_notifying(interp, process_id, env)?,
            Some(false) => {
                #[cfg(unix)]
                interp.signal_process_group(process_id, libc::SIGHUP, &Value::Nil)?;
            }
            None => {}
        }
        index = interp.next_process_alist_cell(&cells, at);
    }
    Ok(())
}

/// gnutls.h GNUTLS_EMACS_HANDSHAKES_LIMIT: the handshakes
/// wait_reading_process_output tries before a non-blocking client's
/// negotiation fails.
const GNUTLS_EMACS_HANDSHAKES_LIMIT: u32 = 6000;

/// lisp.h MAX_ALLOCA: the most bytes SAFE_ALLOCA takes from the stack.
#[cfg(unix)]
const MAX_ALLOCA: usize = 16 * 1024;

/// get_lisp_to_sockaddr_size: the family and sockaddr size an address
/// object stands for, a vector of 5 (AF_INET) or 9 (AF_INET6), a string
/// (AF_LOCAL), or (FAMILY . VECTOR) with FAMILY an int; None for
/// anything else.
#[cfg(unix)]
pub(crate) fn lisp_to_sockaddr_size(address: &Value) -> Option<(libc::c_int, usize)> {
    match address.kind() {
        Kind::Vector(vector) => match vector.len() {
            5 => Some((libc::AF_INET, std::mem::size_of::<libc::sockaddr_in>())),
            9 => Some((libc::AF_INET6, std::mem::size_of::<libc::sockaddr_in6>())),
            _ => None,
        },
        Kind::StringObject(_) => Some((libc::AF_UNIX, std::mem::size_of::<libc::sockaddr_un>())),
        Kind::Cons(_) => {
            let (family, data) = address.cons_values()?;
            let Kind::Integer(family) = family.kind() else {
                return None;
            };
            let family = libc::c_int::try_from(family).ok()?;
            let Kind::Vector(vector) = data.kind() else {
                return None;
            };
            // MAX_ALLOCA - sizeof sa->sa_family < p->header.size: 0.
            let family_size = std::mem::size_of::<libc::sa_family_t>();
            (vector.len() <= MAX_ALLOCA - family_size)
                .then_some((family, vector.len() + family_size))
        }
        _ => None,
    }
}

/// conv_lisp_to_sockaddr: ADDRESS as the sockaddr of FAMILY and LEN
/// bytes it stands for, zeroed first; an element that is no fixnum
/// leaves its byte zero, and a FAMILY the address's form does not fit
/// leaves the whole address zero.
#[cfg(unix)]
pub(crate) fn conv_lisp_to_sockaddr(
    family: libc::c_int,
    address: &Value,
    len: usize,
) -> crate::lisp::eval::RawSockaddr {
    let mut raw = crate::lisp::eval::RawSockaddr::zeroed(len);
    let fixnum = |value: Value| match value.kind() {
        Kind::Integer(number) => Some(number),
        _ => None,
    };
    match address.kind() {
        Kind::Vector(vector) => {
            let slot = |index: usize| vector.get(index).and_then(fixnum);
            if family == libc::AF_INET {
                let inet = raw.view_mut::<libc::sockaddr_in>();
                inet.sin_port = (slot(4).unwrap_or(0) as u16).to_be();
                // `*cp++ = XFIXNAT (...) & 0xff' for each fixnum of the
                // first four: one that is not leaves no byte, the later
                // ones moving up.
                let mut octets = [0u8; 4];
                for (octet, number) in octets.iter_mut().zip((0..4).filter_map(slot)) {
                    *octet = (number & 0xff) as u8;
                }
                inet.sin_addr.s_addr = u32::from_ne_bytes(octets);
                inet.sin_family = libc::AF_INET as libc::sa_family_t;
            } else if family == libc::AF_INET6 {
                let inet6 = raw.view_mut::<libc::sockaddr_in6>();
                inet6.sin6_port = (slot(8).unwrap_or(0) as u16).to_be();
                let mut octets = [0u8; 16];
                for index in 0..8 {
                    if let Some(number) = slot(index) {
                        let word = (number & 0xffff) as u16;
                        octets[2 * index..2 * index + 2].copy_from_slice(&word.to_be_bytes());
                    }
                }
                inet6.sin6_addr.s6_addr = octets;
                inet6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            }
        }
        Kind::StringObject(_) => {
            if family == libc::AF_UNIX {
                let local = raw.view_mut::<libc::sockaddr_un>();
                let bytes = crate::lisp::primitives::sdata_c_string(address);
                for (slot, byte) in local.sun_path.iter_mut().zip(bytes.iter()) {
                    *slot = *byte as libc::c_char;
                }
                local.sun_family = libc::AF_UNIX as libc::sa_family_t;
            }
        }
        _ => {
            // (FAMILY . VECTOR): the vector's fixnums are the bytes after
            // sizeof (sa->sa_family), and sa_family stays as the zeroing
            // left it.
            if let Some((_, data)) = address.cons_values()
                && let Kind::Vector(vector) = data.kind()
            {
                let offset = std::mem::size_of::<libc::sa_family_t>();
                let bytes = raw.bytes_mut();
                let end = len.min(bytes.len());
                for (at, number) in (offset..end).zip(vector.slots().filter_map(fixnum)) {
                    bytes[at] = (number & 0xff) as u8;
                }
            }
        }
    }
    raw
}

/// conv_sockaddr_to_lisp: a vector of 5 for AF_INET, of 9 for AF_INET6,
/// the unibyte name of an AF_LOCAL address, else (FAMILY . VECTOR) of the
/// bytes after sa_family; an address too short for a family is "".
#[cfg(unix)]
pub(crate) fn conv_sockaddr_to_lisp(address: &crate::lisp::eval::RawSockaddr) -> Value {
    let bytes = address.bytes();
    let family_end =
        std::mem::offset_of!(libc::sockaddr, sa_family) + std::mem::size_of::<libc::sa_family_t>();
    if bytes.len() < family_end {
        return crate::lisp::primitives::coding::bytes_to_unibyte_value(&[]);
    }
    match address.family() {
        libc::AF_INET => {
            let inet = address.view::<libc::sockaddr_in>();
            let octets = inet.sin_addr.s_addr.to_ne_bytes();
            Value::vector(
                octets
                    .iter()
                    .map(|octet| Value::Integer(i64::from(*octet)))
                    .chain(std::iter::once(Value::Integer(i64::from(u16::from_be(
                        inet.sin_port,
                    )))))
                    .collect::<Vec<_>>(),
            )
        }
        libc::AF_INET6 => {
            let inet6 = address.view::<libc::sockaddr_in6>();
            let octets = inet6.sin6_addr.s6_addr;
            Value::vector(
                octets
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|word| Value::Integer(i64::from(u16::from_be_bytes(*word))))
                    .chain(std::iter::once(Value::Integer(i64::from(u16::from_be(
                        inet6.sin6_port,
                    )))))
                    .collect::<Vec<_>>(),
            )
        }
        libc::AF_UNIX => {
            let start = std::mem::offset_of!(libc::sockaddr_un, sun_path);
            let mut name = bytes.get(start..).unwrap_or_default();
            // A name whose first byte is NUL is a Linux abstract name and
            // may hold NULs; any other ends at its first NUL.
            if name.first().is_some_and(|byte| *byte != 0)
                && let Some(end) = name.iter().position(|byte| *byte == 0)
            {
                name = &name[..end];
            }
            crate::lisp::primitives::coding::bytes_to_unibyte_value(name)
        }
        family => Value::cons(
            Value::Integer(i64::from(family)),
            Value::vector(
                bytes[family_end..]
                    .iter()
                    .map(|byte| Value::Integer(i64::from(*byte)))
                    .collect::<Vec<_>>(),
            ),
        ),
    }
}

/// An addrinfo of ADDRINFOS as its Lisp form, (PROTOCOL . ADDRESS): a
/// (FAMILY . VECTOR) address, whose sockaddr has sa_family 0, as FAMILY
/// and the bytes after sa_family.
#[cfg(unix)]
pub(crate) fn addrinfo_to_lisp(addrinfo: &crate::lisp::eval::NetworkAddrinfo) -> Value {
    let address = if addrinfo.family != addrinfo.address.family() {
        let start = std::mem::size_of::<libc::sa_family_t>();
        Value::cons(
            Value::Integer(i64::from(addrinfo.family)),
            Value::vector(
                addrinfo
                    .address
                    .bytes()
                    .get(start..)
                    .unwrap_or_default()
                    .iter()
                    .map(|byte| Value::Integer(i64::from(*byte)))
                    .collect::<Vec<_>>(),
            ),
        )
    } else {
        conv_sockaddr_to_lisp(&addrinfo.address)
    };
    Value::cons(Value::Integer(i64::from(addrinfo.protocol)), address)
}

/// conv_addrinfo_to_lisp, then conv_lisp_to_sockaddr as
/// connect_network_socket makes of it: what of getaddrinfo's address
/// its Lisp form keeps (an AF_INET6 address loses its flow and scope).
#[cfg(unix)]
fn addrinfo_through_lisp(entry: &libc::addrinfo) -> Option<crate::lisp::eval::NetworkAddrinfo> {
    if entry.ai_addr.is_null() {
        return None;
    }
    let raw = crate::lisp::eval::RawSockaddr::copied(entry.ai_addr, entry.ai_addrlen as usize);
    let family = raw.family();
    let address = match family {
        libc::AF_INET | libc::AF_INET6 => {
            let family = raw.family();
            let size = if family == libc::AF_INET {
                std::mem::size_of::<libc::sockaddr_in>()
            } else {
                std::mem::size_of::<libc::sockaddr_in6>()
            };
            // The Lisp form keeps the address and port only.
            let mut kept = raw;
            if family == libc::AF_INET6 {
                let inet6 = kept.view_mut::<libc::sockaddr_in6>();
                inet6.sin6_flowinfo = 0;
                inet6.sin6_scope_id = 0;
            }
            kept.length = size as libc::socklen_t;
            kept
        }
        // (FAMILY . VECTOR), whose conv_lisp_to_sockaddr leaves sa_family
        // zero.
        _ => {
            let mut kept = raw;
            kept.set_family(0);
            kept
        }
    };
    Some(crate::lisp::eval::NetworkAddrinfo {
        protocol: entry.ai_protocol,
        family,
        address,
    })
}

/// getaddrinfo (HOST, SERVICE, HINTS of FAMILY and SOCKTYPE) and its
/// addresses in order, as conv_addrinfo_to_lisp keeps them, or its
/// error.  HOST and SERVICE are SSDATA: bytes up to the first NUL.
#[cfg(unix)]
fn getaddrinfo_addresses(
    host: &[u8],
    service: &[u8],
    family: libc::c_int,
    socktype: libc::c_int,
) -> Result<Vec<crate::lisp::eval::NetworkAddrinfo>, libc::c_int> {
    let c_prefix = |bytes: &[u8]| {
        let prefix = bytes.split(|byte| *byte == 0).next().unwrap_or_default();
        std::ffi::CString::new(prefix).unwrap_or_default()
    };
    let host = c_prefix(host);
    let service = c_prefix(service);
    // SAFETY: an all-zero addrinfo is getaddrinfo's empty hints.
    let mut hints: libc::addrinfo = unsafe { std::mem::zeroed() };
    hints.ai_family = family;
    hints.ai_socktype = socktype;
    let mut result = std::ptr::null_mut();
    // SAFETY: HOST and SERVICE are NUL-terminated; RESULT receives a list
    // freed below.
    let status = unsafe { libc::getaddrinfo(host.as_ptr(), service.as_ptr(), &hints, &mut result) };
    if status != 0 {
        return Err(status);
    }
    let mut addresses = Vec::new();
    let mut current = result;
    while !current.is_null() {
        // SAFETY: CURRENT walks getaddrinfo's list until its end.
        let entry = unsafe { &*current };
        addresses.extend(addrinfo_through_lisp(entry));
        current = entry.ai_next;
    }
    // SAFETY: RESULT is the list getaddrinfo made.
    unsafe { libc::freeaddrinfo(result) };
    Ok(addresses)
}

/// gai_strerror's text for STATUS, through code_convert_string_norecord
/// with `locale-coding-system' when that is non-nil.
#[cfg(unix)]
fn gai_strerror_string(interp: &Interpreter, status: libc::c_int) -> Result<Value, LispError> {
    // SAFETY: gai_strerror returns a static NUL-terminated message.
    let message = unsafe { libc::gai_strerror(status) };
    let text = if message.is_null() {
        String::new()
    } else {
        // SAFETY: as above.
        unsafe { std::ffi::CStr::from_ptr(message) }
            .to_string_lossy()
            .into_owned()
    };
    crate::lisp::primitives::locale_message_string(interp, text)
}

/// network_lookup_address_info_1 (HOST, SERVICE, HINTS): a multibyte
/// HOST with a non-ASCII character is an error naming its SSDATA; a
/// failing getaddrinfo is Fformat ("%s/%s %s", HOST, SERVICE, its
/// gai_strerror), which Fmake_network_process signals with error ("%s").
#[cfg(unix)]
fn network_lookup_address_info_1(
    interp: &mut Interpreter,
    env: &mut Env,
    host: &Value,
    service: &[u8],
    family: libc::c_int,
    socktype: libc::c_int,
) -> Result<Vec<crate::lisp::eval::NetworkAddrinfo>, LispError> {
    let Kind::StringObject(state) = host.kind() else {
        return Err(wrong_type_argument("stringp", *host));
    };
    let (bytes, non_ascii) = {
        let state = state.borrow();
        (
            state.bytes().to_vec(),
            state.is_multibyte() && state.bytes().len() != state.len(),
        )
    };
    if non_ascii {
        return Err(crate::lisp::primitives::values::doprnt_error_sdata(
            Some(interp),
            "Non-ASCII hostname {} detected, please use `puny-encode-domain'",
            &[*host],
        ));
    }
    match getaddrinfo_addresses(&bytes, service, family, socktype) {
        Ok(addresses) => Ok(addresses),
        Err(status) => {
            // build_string (str) of SSDATA of gai_strerror's text through
            // `locale-coding-system', and build_string (service): each
            // make_string of C bytes, unibyte unless they form multibyte
            // text.
            let build_string = |interp: &Interpreter, bytes: &[u8]| {
                let end = bytes
                    .iter()
                    .position(|byte| *byte == 0)
                    .unwrap_or(bytes.len());
                crate::lisp::primitives::strings::make_string(bytes[..end].to_vec()).map_err(
                    |error| {
                        crate::lisp::primitives::strings::string_storage_error_shared(interp, error)
                    },
                )
            };
            let reason = gai_strerror_string(interp, status)?;
            let reason_bytes = match reason.kind() {
                Kind::StringObject(state) => state.borrow().bytes().to_vec(),
                _ => Vec::new(),
            };
            let reason = build_string(interp, &reason_bytes)?;
            let service = build_string(interp, service)?;
            let msg = call(
                interp,
                "format",
                &[Value::string("%s/%s %s"), *host, service, reason],
                env,
            )?;
            // error ("%s", SSDATA (msg)).
            Err(crate::lisp::primitives::values::doprnt_error_sdata(
                Some(interp),
                "{}",
                &[msg],
            ))
        }
    }
}

/// Fmake_network_process with HAVE_GETADDRINFO_A: getaddrinfo_a
/// (GAI_NOWAIT) for HOST and SERVICE, a thread answering it.
#[cfg(target_os = "linux")]
fn start_dns_request(
    host: &[u8],
    service: &[u8],
    family: libc::c_int,
    socktype: libc::c_int,
) -> crate::lisp::eval::DnsRequest {
    let slot = std::sync::Arc::new(std::sync::Mutex::new(None));
    let answer = std::sync::Arc::clone(&slot);
    let (lookup_host, lookup_service) = (host.to_vec(), service.to_vec());
    std::thread::spawn(move || {
        let result = getaddrinfo_addresses(&lookup_host, &lookup_service, family, socktype);
        if let Ok(mut slot) = answer.lock() {
            *slot = Some(result);
        }
    });
    let host = host.split(|byte| *byte == 0).next().unwrap_or_default();
    crate::lisp::eval::DnsRequest {
        host: host.to_vec(),
        slot,
    }
}

/// get_required_string_keyword_param (CONTACT, KEY): plist_member's KEY,
/// which must have a value after it ("Missing %s keyword parameter"), a
/// string ("%s value not a string").
fn required_string_keyword(
    interp: &Interpreter,
    contact: &[Value],
    key: &str,
) -> Result<Value, LispError> {
    let position = contact
        .iter()
        .step_by(2)
        .position(|candidate| eq_named(interp, candidate, key));
    match position.and_then(|index| contact.get(2 * index + 1)) {
        None => Err(LispError::Signal(format!(
            "Missing {key} keyword parameter"
        ))),
        Some(value) if matches!(value.kind(), Kind::StringObject(_)) => Ok(*value),
        Some(_) => Err(LispError::Signal(format!("{key} value not a string"))),
    }
}

/// process.c:Fmake_network_process, in its order of steps: nil for no
/// arguments; `:type' (a stream, or `datagram'; "Unsupported connection
/// type"); the required string `:name'; `:server' with `:nowait' refused;
/// a `:remote' (or a server's `:local') address object as the one
/// address ("Malformed :address"); else `:family' ("Unknown address
/// family"), `:service' and `:host', a nil or `local' host being the
/// family's loopback, CHECK_STRING'd; a local family's `:service' as its
/// socket's name ("Service name too long"), the host dropped with a
/// message; else the service as getaddrinfo's port string (t "0", a
/// fixnum's digits, else CHECK_STRING), looked up then, or, for
/// `:nowait' where getaddrinfo_a is, by a request answered later.  Then
/// Fget_buffer_create of `:buffer', make_process, which remove_process
/// undoes if what follows signals, its contact, a copy of `:plist',
/// `:sentinel', `:filter', `:log', `:noquery' and `:stop'; the TLS
/// parameters (CHECK_LIST); set_network_socket_coding_system; `:server'
/// QLEN as the backlog; and connect_network_socket, unless a client's
/// name lookup is still pending.
#[cfg(unix)]
pub(crate) fn make_network_process(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Ok(Value::Nil);
    }
    let get = |interp: &Interpreter, key: &str| {
        plist_member_value(interp, args, key).unwrap_or(Value::Nil)
    };
    // Flist (nargs, args), whose :host plist_put may change.
    let mut contact: Vec<Value> = args.to_vec();
    let kind = get(interp, ":type");
    let socktype = if kind.is_nil() {
        libc::SOCK_STREAM
    } else if eq_named(interp, &kind, "datagram") {
        libc::SOCK_DGRAM
    } else {
        return Err(LispError::Signal("Unsupported connection type".into()));
    };
    let name = required_string_keyword(interp, args, ":name")?;
    let buffer = get(interp, ":buffer");
    let filter = get(interp, ":filter");
    let sentinel = get(interp, ":sentinel");
    let server = get(interp, ":server");
    let nowait = !get(interp, ":nowait").is_nil();
    if !server.is_nil() && nowait {
        return Err(LispError::Signal(
            "`:server' is incompatible with `:nowait'".into(),
        ));
    }
    let address = get(interp, if server.is_nil() { ":remote" } else { ":local" });
    let mut host = Value::Nil;
    let mut service = Value::Nil;
    let mut addrinfos = Vec::new();
    let mut dns_request = None;
    'lookup: {
        if !address.is_nil() {
            let Some((family, len)) = lisp_to_sockaddr_size(&address) else {
                return Err(LispError::Signal("Malformed :address".into()));
            };
            addrinfos.push(crate::lisp::eval::NetworkAddrinfo {
                protocol: 0,
                family,
                address: conv_lisp_to_sockaddr(family, &address, len),
            });
            break 'lookup;
        }
        let family_value = get(interp, ":family");
        let family = if family_value.is_nil() {
            libc::AF_UNSPEC
        } else if eq_named(interp, &family_value, "local") {
            libc::AF_UNIX
        } else if eq_named(interp, &family_value, "ipv6") {
            libc::AF_INET6
        } else if eq_named(interp, &family_value, "ipv4") {
            libc::AF_INET
        } else {
            match family_value.kind() {
                Kind::Integer(number) if libc::c_int::try_from(number).is_ok() => number as i32,
                _ => return Err(LispError::Signal("Unknown address family".into())),
            }
        };
        let loopback = || {
            Value::string(if family == libc::AF_INET6 {
                "::1"
            } else {
                "127.0.0.1"
            })
        };
        service = get(interp, ":service");
        host = get(interp, ":host");
        if host.is_nil() {
            if family != libc::AF_UNIX {
                host = loopback();
            }
        } else {
            if eq_named(interp, &host, "local") {
                host = loopback();
            }
            if !matches!(host.kind(), Kind::StringObject(_)) {
                return Err(wrong_type_argument("stringp", host));
            }
        }
        if family == libc::AF_UNIX {
            if !host.is_nil() {
                call(
                    interp,
                    "message",
                    &[Value::string(":family local ignores the :host property")],
                    env,
                )?;
                plist_items_put(&mut contact, ":host", Value::Nil);
                host = Value::Nil;
            }
            if !matches!(service.kind(), Kind::StringObject(_)) {
                return Err(wrong_type_argument("stringp", service));
            }
            let sun_path = std::mem::size_of::<libc::sockaddr_un>()
                - std::mem::offset_of!(libc::sockaddr_un, sun_path);
            let sbytes = match service.kind() {
                Kind::StringObject(state) => state.borrow().bytes().len(),
                _ => 0,
            };
            if sun_path <= sbytes {
                return Err(LispError::Signal("Service name too long".into()));
            }
            addrinfos.push(crate::lisp::eval::NetworkAddrinfo {
                protocol: 0,
                family: libc::AF_UNIX,
                address: conv_lisp_to_sockaddr(
                    libc::AF_UNIX,
                    &service,
                    std::mem::size_of::<libc::sockaddr_un>(),
                ),
            });
            break 'lookup;
        }
        let portstring: Vec<u8> = match service.kind() {
            Kind::T => b"0".to_vec(),
            Kind::Integer(port) => port.to_string().into_bytes(),
            Kind::StringObject(_) => crate::lisp::primitives::sdata_c_string(&service),
            _ => return Err(wrong_type_argument("stringp", service)),
        };
        #[cfg(target_os = "linux")]
        if nowait {
            let host_bytes = crate::lisp::primitives::sdata_c_string(&host);
            dns_request = Some(start_dns_request(
                &host_bytes,
                &portstring,
                family,
                socktype,
            ));
            break 'lookup;
        }
        addrinfos =
            network_lookup_address_info_1(interp, env, &host, &portstring, family, socktype)?;
    }
    // open_socket:
    let (buffer_id, buffer) = process_buffer_target(interp, env, &buffer)?;
    let present = |value: Value| (!value.is_nil()).then_some(value);
    let process = interp.create_network_process(
        name,
        None,
        false,
        None,
        None,
        None,
        Value::Nil,
        None,
        None,
        None,
        None,
        None,
        Value::list(contact.iter().copied()),
        Value::Nil,
        Value::Nil,
    )?;
    let process_id = interp.resolve_process_id(&process)?;
    let made = (|| -> Result<(), LispError> {
        let plist = call(interp, "copy-sequence", &[get(interp, ":plist")], env)?;
        interp.set_process_plist_value(process_id, plist);
        interp.set_process_buffer_only(process_id, buffer_id, buffer);
        interp.set_process_sentinel(process_id, present(sentinel));
        interp.set_process_filter(process_id, present(filter))?;
        interp.set_process_log(process_id, present(get(interp, ":log")));
        if !get(interp, ":noquery").is_nil() {
            interp.set_process_query_on_exit_flag(process_id, false)?;
        }
        if !get(interp, ":stop").is_nil() {
            interp.set_process_traffic_stopped(process_id, true)?;
        }
        let tls_parameters = get(interp, ":tls-parameters");
        if !tls_parameters.is_nil() && !matches!(tls_parameters.kind(), Kind::Cons(_)) {
            return Err(wrong_type_argument("listp", tls_parameters));
        }
        if !tls_parameters.is_nil() {
            interp.set_process_gnutls_boot_parameters(process_id, tls_parameters);
        }
        // set_network_socket_coding_system (proc, host, service, name).
        let (decoding, encoding) = connection_coding_systems(
            interp,
            env,
            ConnectionCoding::Network {
                name,
                host,
                service,
            },
            get(interp, ":coding"),
            buffer,
        )?;
        interp.set_process_coding_system(process_id, decoding, encoding)?;
        // !(!NILP (tem) || NILP (p->buffer) || !inherit_process_coding_system),
        // TEM being `:coding'.
        let inherit = get(interp, ":coding").is_nil()
            && !buffer.is_nil()
            && interp
                .lookup_var_key(cached_symbol!("inherit-process-coding-system"), env)
                .is_some_and(|value| value.is_truthy());
        interp.set_process_inherit_coding_system_flag(process_id, inherit)?;
        let is_server = !server.is_nil();
        let backlog = match server.kind() {
            Kind::Integer(qlen) => libc::c_int::try_from(qlen).unwrap_or(5),
            _ => 5,
        };
        interp.set_network_setup(
            process_id,
            crate::lisp::eval::NetworkSetup {
                socktype,
                is_server,
                non_blocking_client: !is_server && socktype != libc::SOCK_DGRAM && nowait,
                backlog,
            },
        );
        if let Some(request) = dns_request.take()
            && !is_server
        {
            interp.set_network_dns_request(process_id, request);
            return Ok(());
        }
        // connect_network_socket (proc, ip_addresses, plist_get (contact,
        // QCuse_external_socket)): with no HAVE_LIBSYSTEMD, as in the GNU
        // build this follows, external_sock_fd is -1, so `:use-external-
        // socket' leaves socket_to_use -1 and the loop makes its own.
        connect_network_socket(interp, env, process_id, addrinfos)
    })();
    if let Err(error) = made {
        interp.remove_process(process_id);
        return Err(error);
    }
    Ok(process)
}

#[cfg(not(unix))]
pub(crate) fn make_network_process(
    _interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut Env,
) -> Result<Value, LispError> {
    Err(LispError::Signal(
        "Network processes are not supported".into(),
    ))
}

/// status_message's CALLN (Fformat, "failed with code %s\n", DATA) of a
/// (failed DATA) status.
fn failed_status_message(
    interp: &mut Interpreter,
    env: &mut Env,
    data: Value,
) -> Result<Option<String>, LispError> {
    let text = call(
        interp,
        "format",
        &[Value::string("failed with code %s\n"), data],
        env,
    )?;
    Ok(string_text(&text).ok())
}

/// process.c's SOCK_CLOEXEC and SOCK_NONBLOCK: 0 where the system has
/// none, as on macOS, where connect_network_socket sets O_NONBLOCK with
/// fcntl instead.
#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
const SOCK_CLOEXEC: libc::c_int = libc::SOCK_CLOEXEC;
#[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
const SOCK_NONBLOCK: libc::c_int = libc::SOCK_NONBLOCK;
#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
const SOCK_CLOEXEC: libc::c_int = 0;
#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "freebsd")))]
const SOCK_NONBLOCK: libc::c_int = 0;

/// What of a socket connect_network_socket's loop made.
#[cfg(unix)]
struct MadeSocket {
    fd: std::os::fd::OwnedFd,
    family: libc::c_int,
    address: crate::lisp::eval::RawSockaddr,
}

/// connect_network_socket's pselect for a connect a signal interrupted,
/// then the socket's SO_ERROR ("Failed select", "Failed getsockopt").
#[cfg(unix)]
fn interrupted_connect_error(
    interp: &Interpreter,
    fd: std::os::fd::RawFd,
) -> Result<libc::c_int, LispError> {
    let mut poll = libc::pollfd {
        fd,
        events: libc::POLLOUT,
        revents: 0,
    };
    // SAFETY: POLL describes the one live descriptor.
    while unsafe { libc::poll(&mut poll, 1, -1) } < 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::EINTR) {
            return Err(file_operation_error_value_without_file(
                interp,
                "Failed select",
                &error,
            ));
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
        return Err(file_operation_error_value_without_file(
            interp,
            "Failed getsockopt",
            &std::io::Error::last_os_error(),
        ));
    }
    Ok(code)
}

/// process.c:connect_network_socket for PROCESS_ID and ADDRINFOS: for
/// each address in turn a socket of its family, the process's socket
/// type (non-blocking for a `:nowait' client) and its protocol (an error
/// goes on to the next, as does one at or above FD_SETSIZE, with EMFILE);
/// a datagram client takes it as it is; else each of the contact's
/// options through set_socket_option (CHECK_SYMBOL of each name, an
/// error from a known option signaled); a server sets SO_REUSEADDR
/// unless `:reuseaddr' was given ("Cannot set reuse option on server
/// socket"), binds ("Cannot bind server socket"), takes the port it got
/// as its `:service', and listens with its backlog ("Cannot listen on
/// server socket"); a client connects, a connect that started or a
/// connected socket ending the loop, an interrupted one waited for
/// ("Failed connect" on the last address), any other failure closing
/// the socket.  With none made, a non-blocking client fails with
/// get_file_errno_data's data, any other process signals "make client
/// process failed" or "make server process failed" with the contact and
/// the last errno.  The contact gets the address (`:remote', a server's
/// `:local') and a client's own `:local'; the process its channel, its
/// status (`listen', or (connect . ADDRINFOS) for a non-blocking
/// client), its datagram address, and its coding systems set up.
#[cfg(unix)]
pub(crate) fn connect_network_socket(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
    addrinfos: Vec<crate::lisp::eval::NetworkAddrinfo>,
) -> Result<(), LispError> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    let setup = interp.network_setup(process_id);
    let contact_items = |interp: &Interpreter| {
        interp
            .process_contact_plist(process_id)
            .and_then(|contact| contact.to_vec().ok())
            .unwrap_or_default()
    };
    let mut xerrno = 0;
    let mut made: Option<MadeSocket> = None;
    let mut remaining = addrinfos.into_iter().peekable();
    while let Some(addrinfo) = remaining.next() {
        let mut address = addrinfo.address;
        let family = addrinfo.family;
        let mut socktype = setup.socktype | SOCK_CLOEXEC;
        if setup.non_blocking_client {
            socktype |= SOCK_NONBLOCK;
        }
        // SAFETY: socket makes a descriptor or fails; nothing is borrowed.
        let fd = unsafe { libc::socket(family, socktype, addrinfo.protocol) };
        if fd < 0 {
            xerrno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
            continue;
        }
        // SAFETY: FD is the socket just made, owned here alone; dropping it
        // closes it, as close_file_unwind does.
        let socket = unsafe { OwnedFd::from_raw_fd(fd) };
        if beyond_fd_setsize(&[fd]) {
            xerrno = libc::EMFILE;
            continue;
        }
        // `p->is_non_blocking_client && ! (SOCK_NONBLOCK && socket_to_use
        // < 0)': where SOCK_NONBLOCK is 0 the socket is made non-blocking
        // here.
        if setup.non_blocking_client && SOCK_NONBLOCK == 0 {
            // SAFETY: fcntl on the live descriptor.
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            // SAFETY: as above.
            if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                xerrno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                continue;
            }
        }
        if !setup.is_server && setup.socktype == libc::SOCK_DGRAM {
            made = Some(MadeSocket {
                fd: socket,
                family,
                address,
            });
            break;
        }
        // The contact's options, each name CHECK_SYMBOL'd and matched by
        // its SYMBOL_NAME; a last key without a value has nil.
        let mut reuseaddr_given = false;
        for pair in contact_items(interp).chunks(2) {
            let key = pair[0];
            let value = pair.get(1).copied().unwrap_or(Value::Nil);
            let Some(symbol) = crate::lisp::primitives::lisp_symbol(interp, key) else {
                return Err(wrong_type_argument("symbolp", key));
            };
            let option = symbol
                .as_symbol()
                .map(|raw| crate::lisp::types::visible_symbol_name(raw).to_string())
                .unwrap_or_default();
            if crate::lisp::primitives::dispatch::files_process::set_socket_option(
                interp, fd, &option, &key, &value,
            )? && option == ":reuseaddr"
            {
                reuseaddr_given = true;
            }
        }
        if setup.is_server {
            if family != libc::AF_UNIX && !reuseaddr_given {
                let optval: libc::c_int = 1;
                // SAFETY: OPTVAL is the int SO_REUSEADDR takes.
                if unsafe {
                    libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_REUSEADDR,
                        (&raw const optval).cast(),
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    )
                } != 0
                {
                    return Err(file_operation_error_value_without_file(
                        interp,
                        "Cannot set reuse option on server socket",
                        &std::io::Error::last_os_error(),
                    ));
                }
            }
            // SAFETY: ADDRESS holds a sockaddr of its LENGTH.
            if unsafe { libc::bind(fd, address.as_ptr(), address.length) } != 0 {
                return Err(file_operation_error_value_without_file(
                    interp,
                    "Cannot bind server socket",
                    &std::io::Error::last_os_error(),
                ));
            }
            // p->port is 0 for an inet server: the port the bind got
            // becomes its :service, plist_put on the contact, and the
            // address's port.
            if family != libc::AF_UNIX {
                // SAFETY: an all-zero sockaddr_in is an empty address.
                let mut bound: libc::sockaddr_in = unsafe { std::mem::zeroed() };
                let mut length = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
                // SAFETY: BOUND holds LENGTH bytes.
                if unsafe { libc::getsockname(fd, (&raw mut bound).cast(), &mut length) } == 0 {
                    interp.process_contact_plist_put(
                        process_id,
                        Value::symbol(":service"),
                        Value::Integer(i64::from(u16::from_be(bound.sin_port))),
                    )?;
                    // sin_port and sin6_port share their offset and width.
                    address.view_mut::<libc::sockaddr_in>().sin_port = bound.sin_port;
                }
            }
            // SAFETY: listen on the bound socket.
            if setup.socktype != libc::SOCK_DGRAM && unsafe { libc::listen(fd, setup.backlog) } != 0
            {
                return Err(file_operation_error_value_without_file(
                    interp,
                    "Cannot listen on server socket",
                    &std::io::Error::last_os_error(),
                ));
            }
            made = Some(MadeSocket {
                fd: socket,
                family,
                address,
            });
            break;
        }
        // SAFETY: ADDRESS holds a sockaddr of its LENGTH.
        let ret = unsafe { libc::connect(fd, address.as_ptr(), address.length) };
        xerrno = if ret == 0 {
            0
        } else {
            std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
        };
        if ret == 0 || xerrno == libc::EISCONN {
            made = Some(MadeSocket {
                fd: socket,
                family,
                address,
            });
            break;
        }
        if setup.non_blocking_client && xerrno == libc::EINPROGRESS {
            made = Some(MadeSocket {
                fd: socket,
                family,
                address,
            });
            break;
        }
        if xerrno == libc::EINTR {
            xerrno = interrupted_connect_error(interp, fd)?;
            if xerrno == 0 {
                made = Some(MadeSocket {
                    fd: socket,
                    family,
                    address,
                });
                break;
            }
            if remaining.peek().is_none() {
                return Err(file_operation_error_value_without_file(
                    interp,
                    "Failed connect",
                    &std::io::Error::from_raw_os_error(xerrno),
                ));
            }
        }
    }
    let untried: Vec<_> = remaining.collect();
    let Some(MadeSocket {
        fd,
        family,
        address,
    }) = made
    else {
        let message = if setup.is_server {
            "make server process failed"
        } else {
            "make client process failed"
        };
        let error = std::io::Error::from_raw_os_error(xerrno);
        let contact = interp
            .process_contact_plist(process_id)
            .unwrap_or(Value::Nil);
        let data =
            crate::lisp::primitives::get_file_errno_data_sharing(interp, message, contact, &error)?;
        if setup.non_blocking_client {
            let message = failed_status_message(interp, env, data)?;
            interp.set_network_process_failed(process_id, data, true, message);
            return Ok(());
        }
        return Err(LispError::SignalValue(data));
    };
    let raw_fd = fd.as_raw_fd();
    // datagram_address: a client's peer, a server's zeroed address of the
    // same length unless its contact's :remote fits it.
    let datagram_address = (setup.socktype == libc::SOCK_DGRAM).then(|| {
        if !setup.is_server {
            return address.clone();
        }
        let mut zeroed = crate::lisp::eval::RawSockaddr::zeroed(address.length as usize);
        let remote = interp
            .process_contact_plist(process_id)
            .map_or(Value::Nil, |contact| contact_plist_get(&contact, ":remote"));
        if !remote.is_nil()
            && let Some((remote_family, len)) = lisp_to_sockaddr_size(&remote)
            && remote_family == family
            && len == address.length as usize
        {
            zeroed = conv_lisp_to_sockaddr(remote_family, &remote, len);
        }
        zeroed
    });
    interp.process_contact_plist_put(
        process_id,
        Value::symbol(if setup.is_server { ":local" } else { ":remote" }),
        conv_sockaddr_to_lisp(&address),
    )?;
    if !setup.is_server {
        let mut local =
            crate::lisp::eval::RawSockaddr::zeroed(std::mem::size_of::<libc::sockaddr_storage>());
        let mut length = local.capacity() as libc::socklen_t;
        // SAFETY: LOCAL holds LENGTH bytes.
        if unsafe { libc::getsockname(raw_fd, local.as_mut_ptr(), &mut length) } == 0 {
            local.length = length;
            interp.process_contact_plist_put(
                process_id,
                Value::symbol(":local"),
                conv_sockaddr_to_lisp(&local),
            )?;
        }
    }
    // fcntl (inch, F_SETFL, O_NONBLOCK).
    // SAFETY: fcntl on the live descriptor.
    unsafe {
        let flags = libc::fcntl(raw_fd, libc::F_GETFL);
        libc::fcntl(raw_fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
    let network = if let Some(datagram_address) = datagram_address {
        crate::lisp::eval::NetworkRuntime::Datagram {
            socket: std::net::UdpSocket::from(fd),
            address: datagram_address,
        }
    } else if family == libc::AF_UNIX {
        if setup.is_server {
            crate::lisp::eval::NetworkRuntime::UnixListener(std::os::unix::net::UnixListener::from(
                fd,
            ))
        } else {
            crate::lisp::eval::NetworkRuntime::UnixStream(std::os::unix::net::UnixStream::from(fd))
        }
    } else if setup.is_server {
        crate::lisp::eval::NetworkRuntime::Listener(std::net::TcpListener::from(fd))
    } else {
        crate::lisp::eval::NetworkRuntime::Stream(std::net::TcpStream::from(fd))
    };
    // The channel, `listen' for a stream server, and for a non-blocking
    // client (connect . ADDRINFOS) with NON_BLOCKING_CONNECT_FD.
    interp.install_network_socket(
        process_id,
        network,
        setup.non_blocking_client.then_some(untried),
    );
    // Make the process marker point into the process buffer (if any).
    interp.update_process_mark(process_id);
    // setup_process_coding_systems.
    let (decoding, encoding) = interp
        .process_coding_system(process_id)?
        .cons_values()
        .unwrap_or((Value::Nil, Value::Nil));
    interp.setup_process_coding_systems(decoding, encoding)?;
    // Continue the asynchronous connection: Fgnutls_boot of the
    // boot parameters; READY runs finish_after_tls_connection, a stage
    // short of a handshake tried closes the channel and fails the process
    // with the boot's error, or "TLS negotiation failed" for nil.
    let boot_parameters = interp
        .process_gnutls_boot_parameters(process_id)
        .unwrap_or(Value::Nil);
    if !boot_parameters.is_nil() {
        let (credential_type, parameters) = boot_parameters
            .cons_values()
            .unwrap_or((Value::Nil, Value::Nil));
        let boot = crate::lisp::primitives::dispatch::gnutls::gnutls_boot(
            interp,
            env,
            &interp.record_value(process_id),
            &credential_type,
            &parameters,
        )?;
        let stage = interp.process_gnutls_initstage(process_id).unwrap_or(0);
        if stage == crate::lisp::eval::GNUTLS_STAGE_READY {
            interp.clear_process_gnutls_boot_parameters(process_id);
            finish_after_tls_connection(interp, env, process_id)?;
        } else if stage != crate::lisp::eval::GNUTLS_STAGE_HANDSHAKE_TRIED {
            interp.close_network_socket(process_id);
            let data = if boot.is_nil() {
                Value::string("TLS negotiation failed")
            } else {
                boot
            };
            let message = failed_status_message(interp, env, data)?;
            interp.set_network_process_failed(process_id, data, false, message);
        }
    }
    Ok(())
}

/// process.c:finish_after_tls_connection: `nsm-verify-connection', when
/// it is fbound, of the process and its contact's `:host' and `:service';
/// nil fails the process ("The Network Security Manager stopped the
/// connections") and closes its channel, as a channel closed meanwhile
/// does with a bare `failed'; else, unless its connect is still under
/// way, the status `run' and its sentinel's "open\n".
#[cfg(unix)]
fn finish_after_tls_connection(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
) -> Result<(), LispError> {
    // Fgnutls_boot's and gnutls_verify_boot's messages, made before
    // `nsm-verify-connection' runs.
    crate::lisp::primitives::dispatch::gnutls::flush_gnutls_log(interp, env)?;
    let contact = interp
        .process_contact_plist(process_id)
        .unwrap_or(Value::Nil);
    let mut result = Value::T;
    if call(
        interp,
        "fboundp",
        &[Value::symbol("nsm-verify-connection")],
        env,
    )?
    .is_truthy()
    {
        let host = contact_plist_get(&contact, ":host");
        let service = contact_plist_get(&contact, ":service");
        // call3 (Qnsm_verify_connection, ...): an autoload loads nsm.
        result = call_function_value(
            interp,
            &Value::symbol("nsm-verify-connection"),
            &[interp.record_value(process_id), host, service],
            env,
        )?;
    }
    if result.is_nil() {
        let data = Value::string("The Network Security Manager stopped the connections");
        let message = failed_status_message(interp, env, data)?;
        interp.set_network_process_failed(process_id, data, true, message);
    } else if interp.network_socket_fd(process_id).is_none() {
        interp.set_network_process_failed(process_id, Value::Nil, true, None);
    } else if !interp.non_blocking_connect_fd(process_id) {
        interp.mark_network_process_open(process_id);
        run_process_sentinel(interp, process_id, "open\n", env)?;
    }
    Ok(())
}

/// plist_get (contact, KEY) over the arguments: the value after the first
/// name EQ to the interned keyword KEY (a symbol with position standing for
/// its symbol while symbols-with-pos-enabled).
fn plist_member_value(interp: &Interpreter, items: &[Value], key: &str) -> Option<Value> {
    items
        .as_chunks::<2>()
        .0
        .iter()
        .find_map(|pair| eq_named(interp, &pair[0], key).then(|| pair[1]))
}

/// EQ (VALUE, the interned symbol NAME): a symbol with position is its
/// symbol while symbols-with-pos-enabled.
fn eq_named(interp: &Interpreter, value: &Value, name: &str) -> bool {
    crate::lisp::primitives::lisp_symbol(interp, *value).is_some_and(
        |symbol| matches!(symbol.kind(), Kind::Symbol(symbol) if symbol.is_named(name)),
    )
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

fn serial_parity(interp: &Interpreter, value: &Value) -> Result<serialport::Parity, LispError> {
    match crate::lisp::types::eq_dispatch(value).kind() {
        Kind::Nil => Ok(serialport::Parity::None),
        Kind::Symbol(symbol) if symbol.is_named("even") => Ok(serialport::Parity::Even),
        Kind::Symbol(symbol) if symbol.is_named("odd") => Ok(serialport::Parity::Odd),
        _ => Err(crate::lisp::primitives::values::doprnt_error(
            interp,
            ":parity must be nil (no parity), `even', or `odd'",
            &[],
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

fn serial_flow_control(
    interp: &Interpreter,
    value: &Value,
) -> Result<serialport::FlowControl, LispError> {
    match crate::lisp::types::eq_dispatch(value).kind() {
        Kind::Nil => Ok(serialport::FlowControl::None),
        Kind::Symbol(symbol) if symbol.is_named("hw") => Ok(serialport::FlowControl::Hardware),
        Kind::Symbol(symbol) if symbol.is_named("sw") => Ok(serialport::FlowControl::Software),
        _ => Err(crate::lisp::primitives::values::doprnt_error(
            interp,
            ":flowcontrol must be nil (no flowcontrol), `hw', or `sw'",
            &[],
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
    interp: &Interpreter,
    updates: &[Value],
    stored_contact: &Value,
) -> Result<(SerialConfiguration, Value), LispError> {
    let mut contact_items = stored_contact.to_vec()?;
    let setting = |key: &str, default: Value| {
        let value = plist_member_value(interp, updates, key)
            .unwrap_or_else(|| contact_plist_get(stored_contact, key));
        if value.is_nil() { default } else { value }
    };
    let speed_value = plist_member_value(interp, updates, ":speed")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":speed"));
    let speed = u32::try_from(speed_value.as_integer()?)
        .map_err(|_| LispError::Signal("Unsupported speed".into()))?;
    let bytesize = setting(":bytesize", Value::Integer(8));
    let parity = plist_member_value(interp, updates, ":parity")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":parity"));
    let stopbits = setting(":stopbits", Value::Integer(1));
    let flowcontrol = plist_member_value(interp, updates, ":flowcontrol")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":flowcontrol"));
    let config = SerialConfiguration {
        speed,
        data_bits: serial_data_bits(&bytesize)?,
        parity: serial_parity(interp, &parity)?,
        stop_bits: serial_stop_bits(&stopbits)?,
        flow_control: serial_flow_control(interp, &flowcontrol)?,
    };
    plist_items_put(&mut contact_items, ":speed", Value::Integer(speed.into()));
    plist_items_put(&mut contact_items, ":bytesize", bytesize);
    plist_items_put(&mut contact_items, ":parity", parity);
    plist_items_put(&mut contact_items, ":stopbits", stopbits);
    plist_items_put(&mut contact_items, ":flowcontrol", flowcontrol);
    let parity_summary = match crate::lisp::types::eq_dispatch(parity).kind() {
        Kind::Symbol(symbol) if symbol.is_named("even") => 'E',
        Kind::Symbol(symbol) if symbol.is_named("odd") => 'O',
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

fn open_serial_port(interp: &Interpreter, port: Value) -> Result<serialport::TTYPort, LispError> {
    #[cfg(unix)]
    {
        use std::os::fd::IntoRawFd;
        use std::os::unix::fs::OpenOptionsExt;

        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStrExt;
        // sysdep.c:serial_open: emacs_open of SSDATA (port), not encoded,
        // and its report_file_error naming PORT; then
        // Fmake_serial_process's refusal of a port beyond FD_SETSIZE.
        let path = sdata_c_string(&port);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOCTTY | libc::O_NONBLOCK)
            .open(std::ffi::OsStr::from_bytes(&path))
            .map_err(|error| report_file_errno(interp, "Opening serial port", port, &error))?;
        if beyond_fd_setsize(&[file.as_raw_fd()]) {
            return Err(report_file_errno(
                interp,
                "Opening serial port",
                port,
                &std::io::Error::from_raw_os_error(libc::EMFILE),
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

/// process.c:Fserial_process_configure's process: the first of :process,
/// :name, :buffer and :port that is not nil, as get_process takes it.
fn serial_process_designator(interp: &mut Interpreter, args: &[Value]) -> Result<u64, LispError> {
    let requested = [":process", ":name", ":buffer", ":port"]
        .into_iter()
        .find_map(|key| plist_member_value(interp, args, key).filter(Value::is_truthy));
    let process = super::dispatch::files_process::get_process(interp, requested.as_ref())?;
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
    let (configuration, contact) = serial_configuration(interp, args, &stored_contact)?;
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
    let port_value = plist_member_value(interp, args, ":port")
        .filter(Value::is_truthy)
        .ok_or_else(|| LispError::Signal("No port specified".into()))?;
    // CHECK_STRING (port).
    if !port_value.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), port_value));
    }
    let Some(speed_value) = plist_member_value(interp, args, ":speed") else {
        return Err(LispError::Signal(":speed not specified".into()));
    };
    let speed = if speed_value.is_nil() {
        None
    } else {
        // CHECK_FIXNUM (plist_get (contact, QCspeed)).
        let Kind::Integer(speed) = speed_value.kind() else {
            return Err(wrong_type_argument("fixnump", speed_value));
        };
        Some(u32::try_from(speed).map_err(|_| LispError::Signal("Unsupported speed".into()))?)
    };
    // name = plist_get (contact, QCname), PORT when nil; CHECK_STRING,
    // before make_process and serial_open.
    let name = plist_member_value(interp, args, ":name")
        .filter(Value::is_truthy)
        .unwrap_or(port_value);
    if !matches!(name.kind(), Kind::StringObject(_)) {
        return Err(wrong_type_argument("stringp", name));
    }
    let serial = open_serial_port(interp, port_value)?;
    let contact = Value::list(args.iter().cloned());
    let contact = if speed.is_some() {
        let (configuration, contact) = serial_configuration(interp, args, &contact)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            configure_serial_descriptor(serial.as_raw_fd(), &configuration)?;
        }
        contact
    } else {
        contact
    };
    let buffer = plist_member_value(interp, args, ":buffer")
        .filter(Value::is_truthy)
        .unwrap_or(name);
    let (buffer_id, buffer_value) = process_buffer_target(interp, env, &buffer)?;
    let filter = plist_member_value(interp, args, ":filter").filter(Value::is_truthy);
    let sentinel = plist_member_value(interp, args, ":sentinel").filter(Value::is_truthy);
    let plist = plist_member_value(interp, args, ":plist").unwrap_or(Value::Nil);
    let coding = plist_member_value(interp, args, ":coding").unwrap_or(Value::Nil);
    let (decoding, encoding) =
        connection_coding_systems(interp, env, ConnectionCoding::Serial, coding, buffer_value)?;
    let noquery =
        plist_member_value(interp, args, ":noquery").is_some_and(|value| value.is_truthy());
    let stopped = plist_member_value(interp, args, ":stop").is_some_and(|value| value.is_truthy());
    let process = interp.create_serial_process(
        name,
        (buffer_id, buffer_value),
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
            .lookup_var_key(cached_symbol!("inherit-process-coding-system"), env)
            .is_some_and(|value| value.is_truthy());
    interp.set_process_inherit_coding_system_flag(process_id, inherit)?;
    Ok(process)
}

/// exec_sentinel: the process's sentinel (or the default one) called with
/// EVENT in run_asynch_code's frame; none once shut_down_emacs has set
/// inhibit_sentinels.
pub(crate) fn run_process_sentinel(
    interp: &mut Interpreter,
    process_id: u64,
    event: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    // What deactivate_process's emacs_gnutls_deinit and the walk's reads
    // logged, shown before the sentinel runs.
    crate::lisp::primitives::dispatch::gnutls::flush_gnutls_log(interp, env)?;
    if interp.inhibit_sentinels {
        return Ok(());
    }
    run_asynch_code(interp, env, |interp, env| {
        call_catching_process_errors(
            interp,
            env,
            "error in process sentinel: ",
            true,
            |interp, env| {
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
                    internal_default_process_sentinel(
                        interp,
                        process_id,
                        Value::string(event),
                        env,
                    )?;
                }
                Ok(())
            },
        )
    })
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

/// process.c's WAIT_READING_MAX: the most seconds a wait asks for.
pub(crate) const WAIT_READING_MAX: i64 = i64::MAX;

/// Gnulib's dtotimespec (atimer.rs has it): the seconds truncated toward
/// zero, the nanoseconds rounded up, a value past time_t's range clipped,
/// as the timespec's time_t and long.
pub(crate) fn dtotimespec(d: f64) -> (libc::time_t, libc::c_long) {
    let spec = crate::lisp::primitives::atimer::dtotimespec(d);
    (spec.tv_sec, spec.tv_nsec)
}

/// wait_reading_process_output (0, 20 * 1000 * 1000, 0, 0, Qnil, NULL,
/// 0), the wait of wait_for_socket_fds, wait_while_connecting,
/// wait_for_tls_negotiation and send_process.
#[cfg(unix)]
pub(crate) fn wait_twenty_milliseconds(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    wait_reading_process_output(
        interp,
        env,
        WaitRequest {
            time_limit: 0,
            nsecs: 20_000_000,
            read_kbd: 0,
            do_display: false,
            wait_proc: None,
            just_wait_proc: 0,
        },
    )
    .map(drop)
}

/// What a caller asks of wait_reading_process_output: TIME_LIMIT seconds
/// and NSECS nanoseconds (MINIMUM when either is negative, FOREVER when
/// both are zero), READ_KBD (0, 1 or -1), DO_DISPLAY, WAIT_PROC and
/// JUST_WAIT_PROC (0, 1, or -1, which also leaves the timers alone).
#[derive(Clone, Copy)]
pub(crate) struct WaitRequest {
    pub(crate) time_limit: i64,
    pub(crate) nsecs: i64,
    pub(crate) read_kbd: i32,
    /// DO_DISPLAY: redisplay_preserve_echo_area after timers that ran, a
    /// status_notify and the reads (none without a frontend: batch).
    pub(crate) do_display: bool,
    pub(crate) wait_proc: Option<u64>,
    pub(crate) just_wait_proc: i32,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WaitMode {
    Minimum,
    Timeout,
    Forever,
}

/// process.c's READ_OUTPUT_DELAY_INCREMENT and READ_OUTPUT_DELAY_MAX.
const READ_OUTPUT_DELAY_INCREMENT: std::time::Duration = std::time::Duration::from_millis(10);
const READ_OUTPUT_DELAY_MAX: std::time::Duration = std::time::Duration::from_millis(50);

/// What thread_select's pselect found: the descriptors ready to read and
/// to write, and nfds (-1 with EINTR).
#[cfg(unix)]
struct Selected {
    nfds: i32,
    readable: Vec<std::os::fd::RawFd>,
    writable: Vec<std::os::fd::RawFd>,
}

/// thread.c:thread_select around pselect: the global lock released, so
/// another Lisp thread runs while this one waits for READ (to read) or
/// WRITE (to write) to be ready, or TIMEOUT to pass.
#[cfg(unix)]
fn thread_select(
    interp: &mut Interpreter,
    env: &mut Env,
    read: &[std::os::fd::RawFd],
    write: &[std::os::fd::RawFd],
    timeout: std::time::Duration,
) -> Result<Selected, LispError> {
    let mut fds: Vec<libc::pollfd> = read
        .iter()
        .map(|&fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .chain(write.iter().map(|&fd| libc::pollfd {
            fd,
            events: libc::POLLOUT,
            revents: 0,
        }))
        .collect();
    let deadline = std::time::Instant::now() + timeout;
    let nfds = loop {
        let yielding = interp.other_threads_live();
        let ready = yielding && interp.other_threads_ready();
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        // With another thread ready, this one gives way after looking;
        // with others live but blocked, it waits a slice at most, as they
        // may wake (a sleep's end) with no descriptor of this thread's.
        let wait = if ready {
            std::time::Duration::ZERO
        } else if yielding {
            remaining.min(std::time::Duration::from_millis(10))
        } else {
            remaining
        };
        for fd in &mut fds {
            fd.revents = 0;
        }
        let millis = wait.as_micros().div_ceil(1000).min(i32::MAX as u128) as i32;
        // SAFETY: FDS holds descriptors of this interpreter's processes and
        // the child signal pipe; a closed one comes back POLLNVAL.
        let n = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, millis) };
        if n < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                break -1;
            }
            return Err(LispError::Signal("Failed select".into()));
        }
        if n > 0 || !yielding || remaining.is_zero() {
            break n;
        }
        // The lock released: a thread that can run now does, until it
        // blocks or yields, and this one selects again; none ready, it
        // goes on watching its descriptors.
        if ready {
            interp.let_other_threads_run(env, std::time::Duration::ZERO)?;
        }
    };
    let ready = |fd: &libc::pollfd, mask: libc::c_short| {
        fd.revents & libc::POLLNVAL == 0 && fd.revents & mask != 0
    };
    let readable = fds[..read.len()]
        .iter()
        .filter(|fd| ready(fd, libc::POLLIN | libc::POLLHUP | libc::POLLERR))
        .map(|fd| fd.fd)
        .collect();
    let writable = fds[read.len()..]
        .iter()
        .filter(|fd| ready(fd, libc::POLLOUT | libc::POLLHUP | libc::POLLERR))
        .map(|fd| fd.fd)
        .collect();
    Ok(Selected {
        nfds,
        readable,
        writable,
    })
}

/// process.c:wait_reading_process_output: wait for output from processes,
/// timers and, with READ_KBD, keyboard input, returning got_some_output
/// (positive when some process's output was read, WAIT_PROC's only when
/// it is given).
#[cfg(unix)]
pub(crate) fn wait_reading_process_output(
    interp: &mut Interpreter,
    env: &mut Env,
    request: WaitRequest,
) -> Result<i64, LispError> {
    if request.time_limit == 0
        && request.nsecs == 0
        && let Some(process_id) = request.wait_proc
        && interp
            .lookup_var_key(cached_symbol!("inhibit-quit"), env)
            .is_some_and(|value| value.is_truthy())
        && !interp.process_status_exit(process_id)
    {
        super::dispatch::display::message1(
            interp,
            "Blocking call to accept-process-output with quit inhibited!!",
            env,
        )?;
    }
    // waiting_for_user_input_p = read_kbd, restored with the waiting
    // threads' marks by wait_reading_process_output_unwind.
    let previous = interp.set_waiting_for_user_input(request.read_kbd != 0);
    let result = wait_reading_process_output_1(interp, env, request);
    interp.set_waiting_for_user_input(previous);
    interp.clear_waiting_thread_state();
    let (mut got_some_output, prev_wait_proc_nbytes_read) = result?;
    crate::lisp::primitives::dispatch::gnutls::flush_gnutls_log(interp, env)?;
    if request.read_kbd >= 0 {
        interp.maybe_quit(env)?;
    }
    // Timers and process filters that ran may themselves have called
    // `accept-process-output', reading some or all of WAIT_PROC's output
    // unnoticed here: what it read is the value.
    if let Some(process_id) = request.wait_proc {
        let read = interp.process_nbytes_read(process_id);
        if read != prev_wait_proc_nbytes_read {
            got_some_output =
                (read.wrapping_sub(prev_wait_proc_nbytes_read)).min(i32::MAX as u64) as i64;
        }
    }
    Ok(got_some_output)
}

#[cfg(unix)]
fn wait_reading_process_output_1(
    interp: &mut Interpreter,
    env: &mut Env,
    request: WaitRequest,
) -> Result<(i64, u64), LispError> {
    use std::sync::atomic::Ordering::Relaxed;
    let WaitRequest {
        time_limit,
        nsecs,
        read_kbd,
        do_display,
        wait_proc,
        just_wait_proc,
    } = request;
    let mut got_some_output: i64 = -1;
    let prev_wait_proc_nbytes_read = wait_proc.map_or(0, |id| interp.process_nbytes_read(id));
    // Close to the current time if known, None otherwise.
    let mut now: Option<std::time::Instant> = None;
    let (mut wait, end_time) = if time_limit < 0 || nsecs < 0 {
        (WaitMode::Minimum, None)
    } else if time_limit > 0 || nsecs > 0 {
        let span = std::time::Duration::from_secs(time_limit as u64)
            + std::time::Duration::from_nanos(nsecs as u64);
        let start = *now.insert(std::time::Instant::now());
        (WaitMode::Timeout, start.checked_add(span))
    } else {
        (WaitMode::Forever, None)
    };
    let mut got_output_end_time: Option<std::time::Instant> = None;
    loop {
        let mut process_skipped = false;
        // If calling from keyboard input, do not quit; otherwise do a
        // pending quit, and process the pending signals.
        if crate::lisp::primitives::atimer::signals_pending() {
            crate::lisp::primitives::atimer::process_pending_signals_in_wait(interp, env)?;
        }
        if read_kbd >= 0 {
            interp.maybe_quit(env)?;
        }
        crate::lisp::primitives::dispatch::gnutls::flush_gnutls_log(interp, env)?;

        // The pending name lookups and TLS negotiations of WAIT_PROC, or
        // of every process.
        let (_, retry_for_async) = check_async_connections(interp, env, wait_proc)?;

        // Compute time from now till when time limit is up; exit if
        // already run out.
        let mut timeout = match wait {
            WaitMode::Timeout => {
                let current = *now.get_or_insert_with(std::time::Instant::now);
                match end_time {
                    Some(end) if end > current => end - current,
                    Some(_) => break,
                    None => std::time::Duration::from_secs(100_000),
                }
            }
            WaitMode::Minimum => std::time::Duration::ZERO,
            WaitMode::Forever => std::time::Duration::from_secs(100_000),
        };

        // Normally we run timers here (timer_check).
        let mut timer_delay: Option<std::time::Duration> = None;
        if just_wait_proc >= 0 {
            interp.run_wait_loop_input_events(env)?;
            // do { timer_check; a redisplay after a timer ran, which may
            // have requeued itself } while (!detect_input_pending ()).
            loop {
                let ran = interp.run_pending_timer_events(env)?;
                if !(ran && do_display) {
                    break;
                }
                redisplay_preserve_echo_area(interp, env);
                if !interp.quit_flag_is_nil() || crate::lisp::primitives::tty_input_pending() {
                    break;
                }
            }
            let native = interp
                .next_timer_due()
                .map(|due| due.saturating_duration_since(std::time::Instant::now()));
            timer_delay = match (interp.timer_check_delay, native) {
                (Some(lisp), Some(native)) => Some(lisp.min(native)),
                (lisp, native) => lisp.or(native),
            };
            // If there is unread keyboard input, also return.
            if read_kbd != 0 && !unread_command_events(interp, env)?.is_empty() {
                break;
            }
        }

        // handle_child_signal's records, as the handler made them.
        for process_id in interp.child_process_ids() {
            interp.refresh_process_id(process_id)?;
        }
        // If status of something has changed, and no input is available,
        // notify the user of the change right away.
        if interp.status_changes_pending() {
            let (input, write, _) = wait_masks(interp, read_kbd, true);
            let write = if interp.connects_pending() {
                write
            } else {
                Vec::new()
            };
            let ready = poll_now(&input, &write);
            if ready <= 0 {
                got_some_output = status_notify_walk(interp, env, None, wait_proc)?.1;
                if do_display {
                    redisplay_preserve_echo_area(interp, env);
                }
            }
        }

        // Don't wait for output from a non-running process.  Just read
        // whatever data has already been received.
        if let Some(process_id) = wait_proc
            && !interp.wait_proc_running(process_id)
        {
            let mut read_some_bytes = false;
            if interp.process_infd(process_id).is_some() {
                while let ProcessRead::Bytes(nread) = read_process_output(interp, env, process_id)?
                {
                    let nread = nread as i64;
                    if got_some_output < nread {
                        got_some_output = nread;
                    }
                    if nread == 0 {
                        break;
                    }
                    read_some_bytes = true;
                }
            }
            if read_some_bytes && do_display {
                redisplay_preserve_echo_area(interp, env);
            }
            break;
        }

        // Wait till there is something to do.
        let (mut available, writeok, check_delay, check_write, channels) =
            if let Some(process_id) = wait_proc.filter(|_| just_wait_proc != 0) {
                let Some(fd) = interp.process_infd(process_id) else {
                    break;
                };
                let channel: Vec<crate::lisp::eval::WaitChannel> =
                    interp.channel_at(fd).into_iter().collect();
                (vec![fd], Vec::new(), 0, false, channel)
            } else {
                let (input, write, channels) = wait_masks(interp, read_kbd, true);
                let check_delay = if wait_proc.is_some() {
                    0
                } else {
                    PROCESS_OUTPUT_DELAY_COUNT.load(Relaxed)
                };
                (input, write, check_delay, true, channels)
            };
        // We have to be informed when we receive a SIGCHLD signal for an
        // asynchronous process: child_signal_init makes the pipe with the
        // first child.
        let child_fd = crate::lisp::primitives::child_signal_read_fd();
        if let Some(fd) = child_fd {
            available.push(fd);
        }

        // Adaptive read buffering: the processes with read_output_skip
        // left out of this select, its timeout the least of their delays.
        if PROCESS_OUTPUT_SKIP.load(Relaxed) && check_delay > 0 {
            let mut adaptive = if timeout > READ_OUTPUT_DELAY_MAX {
                READ_OUTPUT_DELAY_MAX
            } else {
                timeout
            };
            let mut check_delay = check_delay;
            for channel in interp.process_channels() {
                if check_delay <= 0 {
                    break;
                }
                let (delay, skip) = (channel.read_output_delay, channel.read_output_skip);
                if delay > 0 {
                    check_delay -= 1;
                    if !skip {
                        continue;
                    }
                    available.retain(|&fd| fd != channel.fd);
                    process_skipped = true;
                    interp.clear_process_read_output_skip(&channel);
                    let delay = std::time::Duration::from_nanos(delay as u64);
                    if delay < adaptive {
                        adaptive = delay;
                    }
                }
            }
            timeout = adaptive;
            PROCESS_OUTPUT_SKIP.store(false, Relaxed);
        }

        // If we've got some output and haven't limited our timeout with
        // adaptive read buffering, limit it.
        if got_some_output > 0 && !process_skipped && timeout > READ_OUTPUT_DELAY_INCREMENT {
            timeout = READ_OUTPUT_DELAY_INCREMENT;
        }

        if just_wait_proc >= 0
            && let Some(delay) = timer_delay
            && delay < timeout
        {
            let timeout_abs = *now.get_or_insert_with(std::time::Instant::now) + timeout;
            if got_output_end_time.is_none_or(|end| timeout_abs < end) {
                got_output_end_time = Some(timeout_abs);
            }
            timeout = delay;
        } else {
            got_output_end_time = None;
        }

        // NOW can become inaccurate if time can pass during pselect.
        if !timeout.is_zero() {
            now = None;
        }

        if retry_for_async && timeout > ASYNC_RETRY {
            timeout = ASYNC_RETRY;
        }

        // GnuTLS buffers data internally: a session holding decrypted
        // records is ready without the select.
        let tls_available: Vec<std::os::fd::RawFd> = channels
            .iter()
            .filter(|channel| channel.tls_pending && available.contains(&channel.fd))
            .map(|channel| channel.fd)
            .collect();
        if !tls_available.is_empty()
            && wait_proc.is_none_or(|process_id| {
                interp
                    .process_infd(process_id)
                    .is_some_and(|fd| tls_available.contains(&fd))
            })
        {
            timeout = std::time::Duration::ZERO;
        }

        let selected = thread_select(
            interp,
            env,
            &available,
            if check_write { &writeok } else { &[] },
            timeout,
        )?;
        let mut nfds = selected.nfds;
        let mut readable = selected.readable;
        let writable = selected.writable;
        // Merge tls_available into Available.
        if !tls_available.is_empty() {
            if nfds <= 0 {
                nfds = tls_available.len() as i32;
                readable = tls_available.clone();
            } else {
                for fd in &tls_available {
                    if !readable.contains(fd) {
                        readable.push(*fd);
                    }
                }
            }
        }

        // If we woke up due to SIGWINCH, actually change size now.
        crate::lisp::primitives::do_pending_window_change(interp, env)?;

        if nfds == 0 {
            // Exit the main loop if we've passed the requested timeout, or
            // have read some bytes from our wait_proc, or aren't skipping
            // processes and got some output and waited long enough.
            if wait == WaitMode::Minimum
                || wait_proc
                    .is_some_and(|id| interp.process_nbytes_read(id) != prev_wait_proc_nbytes_read)
            {
                break;
            }
            let mut cmp_time = if wait == WaitMode::Timeout {
                end_time
            } else {
                None
            };
            if !process_skipped && got_some_output > 0 && !timeout.is_zero() {
                let Some(end) = got_output_end_time else {
                    break;
                };
                if cmp_time.is_none_or(|cmp| end < cmp) {
                    cmp_time = Some(end);
                }
            }
            if let Some(cmp) = cmp_time
                && cmp <= *now.insert(std::time::Instant::now())
            {
                break;
            }
        }
        let no_avail = nfds < 0;

        // Check for keyboard input: if there is any, return immediately to
        // give it higher priority than subprocesses.
        if read_kbd != 0
            && (!unread_command_events(interp, env)?.is_empty()
                || crate::lisp::primitives::tty_input_pending())
        {
            break;
        }

        // Check for data from a process.
        if no_avail || nfds == 0 {
            continue;
        }

        // The callbacks: the child signal pipe's child_signal_read.
        if let Some(fd) = child_fd
            && readable.contains(&fd)
        {
            crate::lisp::primitives::drain_child_signal_pipe(fd);
        }

        // The channels in descriptor order, from last_read_channel + 1 round
        // about unless `process-prioritize-lower-fds'.
        let mut ready: Vec<std::os::fd::RawFd> = readable
            .iter()
            .chain(writable.iter())
            .copied()
            .filter(|fd| Some(*fd) != child_fd)
            .collect();
        ready.sort_unstable();
        ready.dedup();
        let prioritize = interp
            .lookup_var_key(cached_symbol!("process-prioritize-lower-fds"), env)
            .is_some_and(|value| value.is_truthy());
        let channel_start = if prioritize {
            0
        } else {
            LAST_READ_CHANNEL.load(Relaxed) + 1
        };
        let split = ready.partition_point(|&fd| fd < channel_start);
        ready.rotate_left(split);
        let mut available_set: Vec<std::os::fd::RawFd> = readable;
        for fd in ready {
            // FD_ISSET (channel, &Available) and the flags PROCESS_FD
            // without KEYBOARD_FD: a channel delete_read_fd took out (a
            // stopped connection's, one whose filter is t) is not read even
            // when JUST-THIS-ONE selected it.
            if available_set.contains(&fd)
                && let Some(channel) = interp.channel_at(fd)
                && (channel.read || channel.connect)
            {
                let process_id = channel.process_id;
                if interp.process_listening(process_id) {
                    // If this is a server stream socket, accept connection.
                    server_accept_connection(interp, env, process_id)?;
                    continue;
                }
                let read = read_process_output(interp, env, process_id)?;
                let nread = match read {
                    ProcessRead::Bytes(nread) => nread as i64,
                    _ => -1,
                };
                if wait_proc.is_none_or(|id| id == process_id) && got_some_output < nread {
                    got_some_output = nread;
                }
                match read {
                    ProcessRead::Bytes(nread) if nread > 0 => {
                        // Vacuum up any leftovers without waiting.
                        if wait_proc == Some(process_id) {
                            wait = WaitMode::Minimum;
                        }
                        // A filter may have called accept-process-output:
                        // no other process is read before the next select.
                        available_set.clear();
                        LAST_READ_CHANNEL.store(fd, Relaxed);
                        if do_display {
                            redisplay_preserve_echo_area(interp, env);
                        }
                    }
                    ProcessRead::WouldBlock => {}
                    ProcessRead::Eio => interp.delete_read_fd_after_eio(process_id),
                    ProcessRead::Bytes(_) => {
                        // If we can detect process termination, don't
                        // consider the process gone just because its pipe
                        // is closed.
                        if interp.process_connection_kind(process_id).0 {
                            interp.end_process_channel(process_id);
                        }
                    }
                    ProcessRead::Error => interp.end_process_channel(process_id),
                }
            }
            if writable.contains(&fd)
                && let Some(channel) = interp.channel_at(fd)
                && channel.connect
            {
                connect_completion(interp, env, channel.process_id)?;
            }
        }
    }
    Ok((got_some_output, prev_wait_proc_nbytes_read))
}

/// compute_input_wait_mask (READ_KBD) or compute_non_keyboard_wait_mask
/// and compute_write_mask: the process channels this thread may watch,
/// each marked as its own (fd_callback_info[fd].waiting_thread) when MARK,
/// with every channel as process_channels found it.
#[cfg(unix)]
fn wait_masks(
    interp: &mut Interpreter,
    _read_kbd: i32,
    mark: bool,
) -> (
    Vec<std::os::fd::RawFd>,
    Vec<std::os::fd::RawFd>,
    Vec<crate::lisp::eval::WaitChannel>,
) {
    let thread = interp.current_thread_id();
    let mut input = Vec::new();
    let mut write = Vec::new();
    let channels = interp.process_channels();
    for channel in &channels {
        if channel.thread.is_some_and(|owner| owner != thread)
            || channel.waiting.is_some_and(|waiter| waiter != thread)
        {
            continue;
        }
        if channel.read {
            input.push(channel.fd);
        }
        if channel.connect {
            write.push(channel.fd);
        }
        if mark && (channel.read || channel.connect) {
            interp.set_process_waiting_thread(channel);
        }
    }
    (input, write, channels)
}

/// xdisp.c:redisplay_preserve_echo_area: the frontend's redisplay, with the
/// echo area's message kept (none without a frontend: batch).
fn redisplay_preserve_echo_area(interp: &mut Interpreter, env: &mut Env) {
    crate::lisp::primitives::run_tty_frame_redraw(interp, env);
}

/// A pselect of INPUT and WRITE that does not wait: how many are ready.
#[cfg(unix)]
fn poll_now(input: &[std::os::fd::RawFd], write: &[std::os::fd::RawFd]) -> i32 {
    let mut fds: Vec<libc::pollfd> = input
        .iter()
        .map(|&fd| libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        })
        .chain(write.iter().map(|&fd| libc::pollfd {
            fd,
            events: libc::POLLOUT,
            revents: 0,
        }))
        .collect();
    if fds.is_empty() {
        return 0;
    }
    // SAFETY: FDS holds this interpreter's process descriptors.
    let n = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, 0) };
    if n <= 0 {
        return n;
    }
    fds.iter()
        .filter(|fd| fd.revents & libc::POLLNVAL == 0 && fd.revents != 0)
        .count() as i32
}

/// process.c:wait_for_socket_fds: while a connection has no socket yet
/// (its name lookup pending), "Waiting for socket from NAME..." logged
/// and wait_reading_process_output (0, 20 ms).
pub(crate) fn wait_for_socket_fds(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
    name: &str,
) -> Result<(), LispError> {
    while interp.network_socket_fd(process_id).is_none()
        && interp.network_process_connecting(process_id)
    {
        crate::lisp::primitives::dispatch::display::add_to_log(
            interp,
            "Waiting for socket from %s...",
            &[Value::string(name)],
            env,
        )?;
        wait_twenty_milliseconds(interp, env)?;
    }
    Ok(())
}

/// process.c:wait_while_connecting: while a connection is connecting,
/// "Waiting for connection..." logged and wait_reading_process_output
/// (0, 20 ms).
pub(crate) fn wait_while_connecting(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
) -> Result<(), LispError> {
    while interp.network_process_connecting(process_id) {
        crate::lisp::primitives::dispatch::display::add_to_log(
            interp,
            "Waiting for connection...",
            &[],
            env,
        )?;
        wait_twenty_milliseconds(interp, env)?;
    }
    Ok(())
}

/// process.c:wait_for_tls_negotiation: while p->gnutls_p and the
/// negotiation is not READY, "Waiting for TLS..." logged and
/// wait_reading_process_output (0, 20 ms).
pub(crate) fn wait_for_tls_negotiation(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
) -> Result<(), LispError> {
    while interp.process_gnutls_p(process_id)
        && interp.process_gnutls_initstage(process_id)
            != Some(crate::lisp::eval::GNUTLS_STAGE_READY)
    {
        crate::lisp::primitives::dispatch::display::add_to_log(
            interp,
            "Waiting for TLS...",
            &[],
            env,
        )?;
        wait_twenty_milliseconds(interp, env)?;
    }
    Ok(())
}

/// Accept pending server connections and deliver stream input/closure to
/// filters and sentinels.  Returns true if anything was processed.
pub(crate) fn pump_connection_processes(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let (progressed, _) = check_async_connections(interp, env, None)?;
    Ok(pump_connection_processes_for(interp, env, None, true)? || progressed)
}

/// process.c:ASYNC_RETRY_NSEC: how long a select may wait while a name
/// lookup or a TLS negotiation is pending.
const ASYNC_RETRY: std::time::Duration = std::time::Duration::from_millis(100);

/// The asynchronous checks at the top of each iteration of
/// wait_reading_process_output's loop, for each process `if (! wait_proc
/// || p == wait_proc)': check_for_dns of a pending name lookup, whose
/// addresses go to connect_network_socket and whose failure makes a
/// process still connecting (failed "Name lookup of HOST failed"), which
/// no sentinel hears; then the continuation of a non-blocking client's TLS
/// negotiation once its connect is done.  Whether anything was done, and
/// retry_for_async: a lookup that was looked at, or a negotiation tried
/// and not done.
fn check_async_connections(
    interp: &mut Interpreter,
    env: &mut Env,
    wait_process_id: Option<u64>,
) -> Result<(bool, bool), LispError> {
    let mut progressed = false;
    let mut retry_for_async = false;
    #[cfg(unix)]
    for process_id in interp.process_ids_for_async_checks() {
        if wait_process_id.is_some_and(|id| process_id != id) {
            continue;
        }
        // check_for_dns: Qt while in progress, nil for a failed or no
        // longer connecting lookup, either of which retries; addresses go
        // to connect_network_socket.
        let mut connected = false;
        let had_request = interp.process_has_dns_request(process_id);
        if let Some((host, answer)) = interp.take_finished_dns_request(process_id) {
            progressed = true;
            if interp.network_process_connecting(process_id) {
                match answer {
                    Ok(addrinfos) if !addrinfos.is_empty() => {
                        connected = true;
                        connect_network_socket(interp, env, process_id, addrinfos)?;
                    }
                    Ok(_) => {}
                    Err(_) => {
                        // concat3 of build_string pieces, ar_name's a
                        // make_string of the host's C bytes.
                        let host = crate::lisp::primitives::strings::make_string(host).map_err(
                            |error| {
                                crate::lisp::primitives::strings::string_storage_error_shared(
                                    interp, error,
                                )
                            },
                        )?;
                        let message = call(
                            interp,
                            "concat",
                            &[
                                Value::string("Name lookup of "),
                                host,
                                Value::string(" failed"),
                            ],
                            env,
                        )?;
                        let text = failed_status_message(interp, env, message)?;
                        interp.set_network_process_failed(process_id, message, true, text);
                    }
                }
            }
        }
        retry_for_async |= had_request && !connected;
        if interp.process_gnutls_initstage(process_id)
            == Some(crate::lisp::eval::GNUTLS_STAGE_HANDSHAKE_TRIED)
            && interp.network_setup(process_id).non_blocking_client
            && interp.network_socket_fd(process_id).is_some()
            && !interp.non_blocking_connect_fd(process_id)
        {
            crate::lisp::primitives::dispatch::gnutls::gnutls_try_handshake(
                interp, env, process_id,
            )?;
            let tried = interp.count_gnutls_handshake(process_id);
            if interp.process_gnutls_initstage(process_id)
                == Some(crate::lisp::eval::GNUTLS_STAGE_READY)
            {
                progressed = true;
                crate::lisp::primitives::dispatch::gnutls::gnutls_verify_boot(
                    interp,
                    process_id,
                    Value::Nil,
                )?;
                finish_after_tls_connection(interp, env, process_id)?;
            } else {
                retry_for_async = true;
                if tried > GNUTLS_EMACS_HANDSHAKES_LIMIT {
                    progressed = true;
                    interp.close_network_socket(process_id);
                    let data = Value::string("TLS negotiation failed");
                    let message = failed_status_message(interp, env, data)?;
                    interp.set_network_process_failed(process_id, data, false, message);
                }
            }
        }
    }
    Ok((progressed, retry_for_async))
}

/// wait_reading_process_output's handling of a channel with
/// NON_BLOCKING_CONNECT_FD that its select found writable (delete_write_fd):
/// a failure goes on to the addresses left, as connect_network_socket
/// does, and with none left the status is (failed ERRNO), reported to the
/// sentinel; a success is `run' and "open\n" unless TLS has the
/// connection to finish.  False when the connect has not ended.
fn connect_completion(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
) -> Result<bool, LispError> {
    let Some(errno) = interp.network_connect_error(process_id) else {
        return Ok(false);
    };
    interp.clear_non_blocking_connect_fd(process_id);
    if errno != 0 {
        let addresses = interp.take_network_connect_addresses(process_id);
        if addresses.is_empty() {
            // p->tick and (failed ERRNO), deactivate_process: the sentinel
            // is status_notify's.
            interp.fail_network_connect(process_id, errno);
            return Ok(true);
        }
        // deactivate_process, then connect_network_socket on the
        // addresses left.
        interp.close_network_socket(process_id);
        #[cfg(unix)]
        connect_network_socket(interp, env, process_id, addresses)?;
        return Ok(true);
    }
    let boot_parameters = interp
        .process_gnutls_boot_parameters(process_id)
        .unwrap_or(Value::Nil);
    if boot_parameters.is_nil() && !interp.process_gnutls_p(process_id) {
        interp.mark_network_process_open(process_id);
        run_process_sentinel(interp, process_id, "open\n", env)?;
    }
    Ok(true)
}

/// What one server_accept_connection did.
#[derive(PartialEq)]
enum Accept {
    Accepted,
    Failed,
    Nothing,
}

/// process.c:server_accept_connection: one accept on SERVER_ID's socket,
/// which its select found readable; the new connection made a process
/// whose log function and sentinel hear of it.  A failed accept is the
/// log function's to hear of, and the listener waits again.
fn server_accept_connection(
    interp: &mut Interpreter,
    env: &mut Env,
    server_id: u64,
) -> Result<Accept, LispError> {
    let (child_runtime, peer_addr) = match interp.accept_network_connection(server_id)? {
        None => return Ok(Accept::Nothing),
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
            return Ok(Accept::Failed);
        }
    };
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
    let base_name = interp
        .process_name(server_id)
        .unwrap_or_else(|| Value::string(""));
    // GNU names inet children "NAME <HOST:PORT>" (note the space)
    // and unnamed-peer children "NAME <N>" from connect_counter,
    // Fformat of the server's name and the caller; make_process
    // makes it unique.
    let suffix = if let (Some(peer_addr), Some(host)) = (peer_addr, &host) {
        let port = peer_addr.port();
        if peer_addr.is_ipv6() {
            format!(" <[{host}]:{port}>")
        } else {
            format!(" <{host}:{port}>")
        }
    } else {
        format!(" <{connect_number}>")
    };
    let child_name = call(interp, "concat", &[base_name, Value::string(&suffix)], env)?;
    let child = interp.create_network_process(
        child_name,
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
    Ok(Accept::Accepted)
}

/// One pass of wait_reading_process_output over the connections after its
/// select, ONLY_PROCESS_ID its JUST_WAIT_PROC's: with CONNECTS, the
/// sockets whose connect the select saw end; then the servers' new
/// connections and the streams' input.
fn pump_connection_processes_for(
    interp: &mut Interpreter,
    env: &mut Env,
    only_process_id: Option<u64>,
    connects: bool,
) -> Result<bool, LispError> {
    let mut progressed = false;

    // wait_reading_process_output: a socket with NON_BLOCKING_CONNECT_FD
    // that turns writable has finished connecting (delete_write_fd).  A
    // failure goes on to the addresses left, as connect_network_socket
    // does, and with none left the status is (failed ERRNO), reported to
    // the sentinel; a success is `run' and "open\n" unless TLS has the
    // connection to finish.
    for process_id in interp.connecting_network_processes() {
        if !connects || only_process_id.is_some_and(|id| process_id != id) {
            continue;
        }
        progressed |= connect_completion(interp, env, process_id)?;
    }

    // Accept new connections on every server listener.
    for server_id in interp.network_listener_ids() {
        if only_process_id.is_some_and(|id| server_id != id) {
            continue;
        }
        loop {
            match server_accept_connection(interp, env, server_id)? {
                Accept::Accepted => progressed = true,
                Accept::Failed => {
                    progressed = true;
                    break;
                }
                Accept::Nothing => break,
            }
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
                let message = interp.close_connection_stream(stream_id);
                run_process_sentinel(interp, stream_id, &message, env)?;
            }
        }
    }

    Ok(progressed)
}
