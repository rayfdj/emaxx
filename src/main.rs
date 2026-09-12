#![deny(clippy::unwrap_used)]

use std::io::Write;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::thread;

use emaxx::batch::{self, BatchRunOptions, BatchRunOutcome};
use emaxx::lisp::DaemonState;
use emaxx::tty;

/// emacs.c's `usage_message' for this configuration: HAVE_MODULES and
/// HAVE_PDUMPER lines are present; the SECCOMP_USABLE line only on
/// GNU/Linux.
const USAGE_MESSAGE: &[&str] = &[
    r#"
Run Emacs, the extensible, customizable, self-documenting real-time
display editor.  The recommended way to start Emacs for normal editing
is with no options at all.

Run M-x info RET m emacs RET m emacs invocation RET inside Emacs to
read the main documentation for these command-line arguments.

Initialization options:

"#,
    r#"--batch                     do not do interactive display; implies -q
--chdir DIR                 change to directory DIR
--daemon, --bg-daemon[=NAME] start a (named) server in the background
--fg-daemon[=NAME]          start a (named) server in the foreground
--debug-init                enable Emacs Lisp debugger for init file
--display, -d DISPLAY       use X server DISPLAY
"#,
    r#"--module-assertions         assert behavior of dynamic modules
"#,
    r#"--dump-file FILE            read dumped state from FILE
--fingerprint               output fingerprint and exit
"#,
    #[cfg(target_os = "linux")]
    r#"--seccomp=FILE              read Seccomp BPF filter from FILE
"#,
    r#"--no-build-details          do not add build details such as time stamps
--no-desktop                do not load a saved desktop
--no-init-file, -q          load neither ~/.emacs nor default.el
--no-loadup, -nl            do not load loadup.el into bare Emacs
--no-site-file              do not load site-start.el
--no-x-resources            do not load X resources
--no-site-lisp, -nsl        do not add site-lisp directories to load-path
--no-splash                 do not display a splash screen on startup
--no-window-system, -nw     do not communicate with X, ignoring $DISPLAY
--init-directory=DIR        use DIR when looking for the Emacs init files.
"#,
    r#"--quick, -Q                 equivalent to:
                              -q --no-site-file --no-site-lisp --no-splash
                              --no-x-resources
--script FILE               run FILE as an Emacs Lisp script
-x                          to be used in #!/usr/bin/emacs -x
                              and has approximately the same meaning
			      as -Q --script
--terminal, -t DEVICE       use DEVICE for terminal I/O
--user, -u USER             load ~USER/.emacs instead of your own

"#,
    r#"Action options:

FILE                    visit FILE
+LINE                   go to line LINE in next FILE
+LINE:COLUMN            go to line LINE, column COLUMN, in next FILE
--directory, -L DIR     prepend DIR to load-path (with :DIR, append DIR)
--eval EXPR             evaluate Emacs Lisp expression EXPR
--execute EXPR          evaluate Emacs Lisp expression EXPR
"#,
    r#"--file FILE             visit FILE
--find-file FILE        visit FILE
--funcall, -f FUNC      call Emacs Lisp function FUNC with no arguments
--insert FILE           insert contents of FILE into current buffer
--kill                  exit without asking for confirmation
--load, -l FILE         load Emacs Lisp FILE using the load function
--visit FILE            visit FILE

"#,
    r#"Display options:

--background-color, -bg COLOR   window background color
--basic-display, -D             disable many display features;
                                  used for debugging Emacs
--border-color, -bd COLOR       main border color
--border-width, -bw WIDTH       width of main border
"#,
    r#"--color, --color=MODE           override color mode for character terminals;
                                  MODE defaults to `auto', and
                                  can also be `never', `always',
                                  or a mode name like `ansi8'
--cursor-color, -cr COLOR       color of the Emacs cursor indicating point
--font, -fn FONT                default font; must be fixed-width
--foreground-color, -fg COLOR   window foreground color
"#,
    r#"--fullheight, -fh               make the first frame high as the screen
--fullscreen, -fs               make the first frame fullscreen
--fullwidth, -fw                make the first frame wide as the screen
--maximized, -mm                make the first frame maximized
--geometry, -g GEOMETRY         window geometry
"#,
    r#"--no-bitmap-icon, -nbi          do not use picture of gnu for Emacs icon
--iconic                        start Emacs in iconified state
--internal-border, -ib WIDTH    width between text and main border
--line-spacing, -lsp PIXELS     additional space to put between lines
--mouse-color, -ms COLOR        mouse cursor color in Emacs window
--name NAME                     title for initial Emacs frame
"#,
    r#"--no-blinking-cursor, -nbc      disable blinking cursor
--reverse-video, -r, -rv        switch foreground and background
--title, -T TITLE               title for initial Emacs frame
--vertical-scroll-bars, -vb     enable vertical scroll bars
--xrm XRESOURCES                set additional X resources
--parent-id XID                 set parent window
--help                          display this help and exit
--version                       output version information and exit

"#,
    r#"You can generally also specify long option names with a single -; for
example, -batch as well as --batch.  You can use any unambiguous
abbreviation for a --option.

Various environment variables and window system resources also affect
the operation of Emacs.  See the main documentation.

Report bugs to bug-gnu-emacs@gnu.org.  First, please see the Bugs
section of the Emacs manual or the file BUGS.
"#,
];

fn main() -> ExitCode {
    match try_main() {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

/// term.c:fatal: "emacs: " and the message on stderr, exit status 1.
fn fatal(message: &str) -> ! {
    eprint!("emacs: {message}");
    if !message.ends_with('\n') {
        eprintln!();
    }
    std::process::exit(1)
}

/// exit(): stdio flushes stdout before the process goes away; Rust's
/// buffered stdout does not on `process::exit'.
fn exit_flushing(code: i32) -> ! {
    let _ = std::io::stdout().flush();
    std::process::exit(code)
}

/// strerror(errno) for an OS error: the text alone, as C prints it.
fn strerror(error: &std::io::Error) -> String {
    match error.raw_os_error() {
        // SAFETY: strerror returns a static, NUL-terminated string.
        Some(code) => unsafe { std::ffi::CStr::from_ptr(libc::strerror(code)) }
            .to_string_lossy()
            .into_owned(),
        None => error.to_string(),
    }
}

/// emacs.c:argmatch.  ARGV[SKIP + 1] is the argument examined: an exact
/// match of SSTR, or, with LSTR, an abbreviation of LSTR at least MINLEN
/// long.  With WANT_VALUE the option's value follows `=' or is the next
/// argument.  SKIP advances past what was used.  It is signed because
/// main un-skips two positions after rewriting `--script'; GNU reads
/// argv[0] then, and so does this.
fn argmatch(
    argv: &[String],
    sstr: &str,
    lstr: Option<&str>,
    minlen: usize,
    want_value: bool,
    skip: &mut isize,
) -> Option<Option<String>> {
    let argc = argv.len() as isize;
    // Don't access argv[argc]; give up in advance.
    if argc <= *skip + 1 {
        return None;
    }
    let index = usize::try_from(*skip + 1).ok()?;
    let arg = &argv[index];
    if arg == sstr {
        if want_value {
            let value = argv.get(index + 1).cloned();
            *skip += 2;
            return Some(value);
        }
        *skip += 1;
        return Some(None);
    }
    let equals = if want_value { arg.find('=') } else { None };
    let arglen = equals.unwrap_or(arg.len());
    let lstr = lstr?;
    if arglen < minlen || !lstr.as_bytes().starts_with(&arg.as_bytes()[..arglen]) {
        return None;
    }
    if !want_value {
        *skip += 1;
        return Some(None);
    }
    if let Some(equals) = equals {
        *skip += 1;
        return Some(Some(arg[equals + 1..].to_string()));
    }
    if let Some(next) = argv.get(index + 1) {
        *skip += 2;
        return Some(Some(next.clone()));
    }
    None
}

/// The scan emacs.c runs before sorting for `--temacs', `--dump-file'
/// and `--seccomp': every position up to "--", the first match wins.
fn leading_option_value(argv: &[String], sstr: &str, lstr: &str, minlen: usize) -> Option<String> {
    let argc = argv.len() as isize;
    let mut skip = 0isize;
    while skip < argc - 1 {
        if let Some(value) = argmatch(argv, sstr, Some(lstr), minlen, true, &mut skip) {
            return value;
        }
        if argmatch(argv, "--", None, 2, false, &mut skip).is_some() {
            break;
        }
        skip += 1;
    }
    None
}

fn try_main() -> Result<u8, String> {
    emaxx::tune_allocator();
    let mut argv = std::env::args_os()
        .map(|arg| {
            arg.into_string()
                .map_err(|_| "command-line argument is not valid UTF-8".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;
    // GNU checks for a Seccomp filter at the very beginning of main(),
    // before any other startup work, so the filter protects the whole
    // initialization phase.
    #[cfg(target_os = "linux")]
    seccomp::maybe_load_seccomp(&argv);
    // Latch the harness's trace knob, then scrub it: the compat harness
    // sets EMAXX_TRACE_LOAD_ERRORS only on the measured emaxx runner
    // (never on the GNU oracle), so Lisp `getenv' and any child emacs a
    // test spawns must observe the same clean environment on both
    // runners, while this process keeps its own diagnostics.
    emaxx::lisp::latch_trace_load_errors();
    if std::env::var_os("EMAXX_TRACE_LOAD_ERRORS").is_some() {
        // SAFETY: single-threaded startup, before Lisp or any subprocess.
        unsafe { std::env::remove_var("EMAXX_TRACE_LOAD_ERRORS") };
    }

    // `--temacs=MODE' is looked for first, before any heap allocation in
    // GNU.  Only the portable dumper's modes exist here (no HAVE_UNEXEC).
    let temacs = leading_option_value(&argv, "-temacs", "--temacs", 8);
    let dump_mode = match temacs.as_deref() {
        None => None,
        Some(mode @ ("pdump" | "pbootstrap")) => Some(mode.to_string()),
        Some(other) => fatal(&format!("Invalid temacs mode '{other}'")),
    };
    // load_pdump: an explicitly named image, else the executable's own,
    // attempted only by a process that is not the dumping one.
    let dump_file = if dump_mode.is_none() {
        leading_option_value(&argv, "-dump-file", "--dump-file", 6).map(PathBuf::from)
    } else {
        None
    };

    // Command-line argument processing.  The arguments in argv are sorted
    // in the descending order of their priority as defined in
    // STANDARD_ARGS, then processed from the highest to the lowest
    // priority; each one recognized here advances `skip', removing it
    // from what init_cmdargs hands to startup.el as `command-line-args'.
    sort_args(&mut argv).unwrap_or_else(|message| fatal(&message));
    let mut skip = 0isize;
    let only_version =
        argmatch(&argv, "-version", Some("--version"), 3, false, &mut skip).is_some();

    if argmatch(
        &argv,
        "-fingerprint",
        Some("--fingerprint"),
        4,
        false,
        &mut skip,
    )
    .is_some()
        && !only_version
    {
        if batch::startup_image_loads(dump_file.as_deref()) {
            println!("{}", batch::executable_fingerprint_hex());
            exit_flushing(0);
        }
        eprintln!("Not initialized");
        std::process::exit(1);
    }

    if let Some(directory) = argmatch(&argv, "-chdir", Some("--chdir"), 4, true, &mut skip)
        && !only_version
    {
        let directory = directory.unwrap_or_default();
        if let Err(error) = std::env::set_current_dir(&directory) {
            eprintln!(
                "{}: Can't chdir to {}: {}",
                argv[0],
                directory,
                strerror(&error)
            );
            std::process::exit(1);
        }
    }

    // Handle the -t switch, which specifies filename to use as terminal.
    if !only_version {
        while let Some(term) = argmatch(&argv, "-t", Some("--terminal"), 4, true, &mut skip) {
            use_terminal_device(&argv[0], &term.unwrap_or_default());
        }
    }

    // Command line option --no-windows is deprecated and thus not
    // mentioned in the manual and usage information.  Emaxx has no
    // window system to inhibit; the switch is consumed as in GNU.
    if argmatch(
        &argv,
        "-nw",
        Some("--no-window-system"),
        6,
        false,
        &mut skip,
    )
    .is_none()
    {
        argmatch(&argv, "-nw", Some("--no-windows"), 6, false, &mut skip);
    }

    // Handle the -batch switch, which means don't do interactive display.
    let mut noninteractive =
        argmatch(&argv, "-batch", Some("--batch"), 5, false, &mut skip).is_some() || only_version;
    if argmatch(&argv, "-script", Some("--script"), 3, true, &mut skip).is_some() {
        noninteractive = true;
        // Convert --script to -scriptload, un-skip it, and sort again so
        // that it will be handled in proper sequence.  (GNU's FIXME: with
        // `--script=FILE' this rewrites the argument before the option
        // instead, argv[0] included; identical here.)
        if let Ok(index) = usize::try_from(skip - 1) {
            argv[index] = "-scriptload".to_string();
        }
        skip -= 2;
        sort_args(&mut argv).unwrap_or_else(|message| fatal(&message));
    }

    // Handle the --help option, which gives a usage message.
    if argmatch(&argv, "-help", Some("--help"), 3, false, &mut skip).is_some() && !only_version {
        println!("Usage: {} [OPTION-OR-FILENAME]...", argv[0]);
        for chunk in USAGE_MESSAGE {
            print!("{chunk}");
        }
        exit_flushing(0);
    }

    let mut daemon = None;
    if !only_version {
        if argmatch(
            &argv,
            "-fg-daemon",
            Some("--fg-daemon"),
            10,
            false,
            &mut skip,
        )
        .is_some()
        {
            daemon = Some((false, None));
        } else if let Some(name) = argmatch(
            &argv,
            "-fg-daemon",
            Some("--fg-daemon"),
            10,
            true,
            &mut skip,
        ) {
            daemon = Some((false, name));
        } else if argmatch(&argv, "-daemon", Some("--daemon"), 5, false, &mut skip).is_some() {
            daemon = Some((true, None));
        } else if let Some(name) = argmatch(&argv, "-daemon", Some("--daemon"), 5, true, &mut skip)
        {
            daemon = Some((true, name));
        } else if argmatch(
            &argv,
            "-bg-daemon",
            Some("--bg-daemon"),
            10,
            false,
            &mut skip,
        )
        .is_some()
        {
            daemon = Some((true, None));
        } else if let Some(name) = argmatch(
            &argv,
            "-bg-daemon",
            Some("--bg-daemon"),
            10,
            true,
            &mut skip,
        ) {
            daemon = Some((true, name));
        }
    }

    let no_loadup = argmatch(&argv, "-nl", Some("--no-loadup"), 6, false, &mut skip).is_some();
    let mut no_site_lisp =
        argmatch(&argv, "-nsl", Some("--no-site-lisp"), 11, false, &mut skip).is_some();
    let build_details = argmatch(
        &argv,
        "-no-build-details",
        Some("--no-build-details"),
        7,
        false,
        &mut skip,
    )
    .is_none();
    // HAVE_MODULES: the switch exists; Emaxx's module loader has no
    // assertion mode to enable, so recognizing it is its whole effect.
    let module_assertions = argmatch(
        &argv,
        "-module-assertions",
        Some("--module-assertions"),
        15,
        false,
        &mut skip,
    )
    .is_some();
    if dump_mode.is_some() && module_assertions && !only_version {
        eprintln!("Module assertions are not supported during dumping");
        std::process::exit(1);
    }

    // The -d/--display block exists only in a build with a window system;
    // a `--without-x' GNU leaves those arguments to startup.el, as here.
    // -Q and -x are recognized but not discarded: startup.el sees them.
    {
        let count_before = skip;
        if !no_site_lisp
            && (argmatch(&argv, "-Q", Some("--quick"), 3, false, &mut skip).is_some()
                || argmatch(&argv, "-quick", None, 2, false, &mut skip).is_some())
        {
            no_site_lisp = true;
        }
        if argmatch(&argv, "-x", None, 1, true, &mut skip).is_some() {
            noninteractive = true;
            no_site_lisp = true;
            // This is picked up in startup.el.  (GNU un-skips one
            // position here; `skip' is reset below either way.)
            if let Ok(index) = usize::try_from(skip - 1) {
                argv[index] = "-scripteval".to_string();
            }
            sort_args(&mut argv).unwrap_or_else(|message| fatal(&message));
        }
        // Don't actually discard this arg.
        skip = count_before;
    }
    // argmatch must not be used after here, except when building temacs.

    emaxx::lisp::set_build_details(build_details);
    if let Some((background, name)) = daemon {
        // The daemon serves emacsclient through server.el's network
        // process, which Emaxx does not implement: an interactive daemon
        // session cannot do its job and says so instead of detaching.
        if !noninteractive {
            fatal("daemon mode needs the Emacs server, which this build does not implement");
        }
        emaxx::lisp::set_daemon_state(DaemonState {
            background,
            name,
            pipe_writer: None,
        });
    }

    // init_cmdargs: argv[0] and every argument after the ones consumed.
    let command_line_args = std::iter::once(argv[0].clone())
        .chain(
            argv.iter()
                .enumerate()
                .skip(1)
                .filter(|(index, _)| *index as isize > skip)
                .map(|(_, arg)| arg.clone()),
        )
        .collect::<Vec<_>>();
    // "Handle -l loadup, args passed by Makefile": the next argument, for
    // the process that builds its own state.
    let temacs_load = {
        let mut peek = skip;
        argmatch(&argv, "-l", Some("--load"), 3, true, &mut peek).flatten()
    };

    if only_version {
        match batch::version_banner(dump_file.as_deref()) {
            Ok(report) => {
                print!("{report}");
                exit_flushing(0);
            }
            Err(message) => {
                eprintln!("{message}");
                std::process::exit(1);
            }
        }
    }

    let options = BatchRunOptions {
        no_site_lisp,
        startup_command_line_args: Some(command_line_args.clone()),
        defer_delayed_custom_init: true,
        dump_file,
        dump_mode,
        no_loadup,
        temacs_load,
        ..Default::default()
    };
    if noninteractive {
        let outcome = run_batch_with_large_stack(options)?;
        // exit() and shut_down_emacs's reset_sys_modes flush stdio's stdout
        // before the process goes away or re-executes itself.
        emaxx::lisp::flush_batch_stdout();
        return match outcome {
            BatchRunOutcome::Exit(code) => Ok(code as u8),
            BatchRunOutcome::Restart => restart_current_process(),
        };
    }
    tty::run(&command_line_args, &options).map(|code| code as u8)
}

/// emacs.c:main's `-t DEVICE': the device replaces standard input and
/// output and must be a terminal.
#[cfg(unix)]
fn use_terminal_device(argv0: &str, term: &str) {
    let Ok(path) = std::ffi::CString::new(term) else {
        eprintln!(
            "{argv0}: {term}: {}",
            strerror(&std::io::Error::from_raw_os_error(libc::ENOENT))
        );
        std::process::exit(1);
    };
    // SAFETY: descriptor operations on this process's standard descriptors.
    let (opened, duplicated) = unsafe {
        libc::close(libc::STDIN_FILENO);
        libc::close(libc::STDOUT_FILENO);
        let opened = libc::open(path.as_ptr(), libc::O_RDWR, 0);
        let duplicated = if opened == libc::STDIN_FILENO {
            libc::fcntl(
                libc::STDIN_FILENO,
                libc::F_DUPFD_CLOEXEC,
                libc::STDOUT_FILENO,
            )
        } else {
            -1
        };
        (opened, duplicated)
    };
    if opened != libc::STDIN_FILENO || duplicated != libc::STDOUT_FILENO {
        eprintln!(
            "{argv0}: {term}: {}",
            strerror(&std::io::Error::last_os_error())
        );
        std::process::exit(1);
    }
    // SAFETY: isatty only inspects the descriptor.
    if unsafe { libc::isatty(libc::STDIN_FILENO) } == 0 {
        eprintln!("{argv0}: {term}: not a tty");
        std::process::exit(1);
    }
    eprintln!("Using {term}");
}

#[cfg(not(unix))]
fn use_terminal_device(argv0: &str, term: &str) {
    eprintln!("{argv0}: {term}: not a tty");
    std::process::exit(1);
}

/// emacs.c's `maybe_load_seccomp'/`load_seccomp': read a Secure Computing
/// BPF filter named by `-seccomp'/`--seccomp' and install it with the
/// `seccomp' system call, exiting fatally when the file is unusable.
#[cfg(target_os = "linux")]
mod seccomp {
    use std::io::Read;

    pub fn maybe_load_seccomp(argv: &[String]) {
        let Some(file) = super::leading_option_value(argv, "-seccomp", "--seccomp", 9) else {
            return;
        };
        if !load_seccomp(&file) {
            super::fatal(&format!("cannot enable seccomp filter from {file}"));
        }
    }
    fn load_seccomp(file: &str) -> bool {
        const FILTER_ENTRY_SIZE: u64 = std::mem::size_of::<libc::sock_filter>() as u64;
        let mut handle = match std::fs::File::open(file) {
            Ok(handle) => handle,
            Err(error) => {
                eprintln!("emacs: open: {}", super::strerror(&error));
                return false;
            }
        };
        let metadata = match handle.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                eprintln!("emacs: fstat: {}", super::strerror(&error));
                return false;
            }
        };
        if !metadata.is_file() {
            eprintln!("seccomp file {file} is not regular");
            return false;
        }
        let size = metadata.len();
        if size == 0 || !size.is_multiple_of(FILTER_ENTRY_SIZE) {
            eprintln!("seccomp filter {file} has invalid size {size}");
            return false;
        }
        let count = size / FILTER_ENTRY_SIZE;
        if count > u64::from(u16::MAX) {
            eprintln!("seccomp filter {file} is too big");
            return false;
        }
        // Try reading one more byte to detect file size changes.
        let mut buffer = Vec::with_capacity(size as usize + 1);
        if let Err(error) = handle.by_ref().take(size + 1).read_to_end(&mut buffer) {
            eprintln!("emacs: read: {}", super::strerror(&error));
            return false;
        }
        if buffer.len() as u64 != size {
            eprintln!("seccomp filter {file} changed size while reading");
            return false;
        }
        drop(handle);
        let program = libc::sock_fprog {
            len: count as u16,
            filter: buffer.as_mut_ptr().cast::<libc::sock_filter>(),
        };
        // See the seccomp man page: without no-new-privs the syscall is
        // refused for unprivileged callers.  GNU ignores this call's
        // result deliberately.
        // SAFETY: plain prctl flag set; no memory handed over.
        unsafe {
            libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0);
        }
        // SAFETY: PROGRAM points at a properly sized filter buffer that
        // outlives the call.
        let result = unsafe {
            libc::syscall(
                libc::SYS_seccomp,
                libc::SECCOMP_SET_MODE_FILTER,
                libc::SECCOMP_FILTER_FLAG_TSYNC,
                std::ptr::addr_of!(program),
            )
        };
        if result != 0 {
            eprintln!(
                "emacs: seccomp: {}",
                super::strerror(&std::io::Error::last_os_error())
            );
            return false;
        }
        true
    }
}

/// emacs.c's standard_args for this configuration: the old-fashioned
/// spelling, the long spelling, the priority and the argument count.
/// HAVE_PDUMPER and HAVE_MODULES entries are present; the SECCOMP_USABLE
/// entry only on GNU/Linux; the HAVE_NS entries (the Cocoa build's) are
/// not, as in a `--without-ns' build.
const STANDARD_ARGS: &[(&str, Option<&str>, i32, usize)] = &[
    ("-version", Some("--version"), 150, 0),
    ("-fingerprint", Some("--fingerprint"), 140, 0),
    ("-chdir", Some("--chdir"), 130, 1),
    ("-t", Some("--terminal"), 120, 1),
    ("-nw", Some("--no-window-system"), 110, 0),
    ("-nw", Some("--no-windows"), 110, 0),
    ("-batch", Some("--batch"), 100, 0),
    ("-script", Some("--script"), 100, 1),
    ("-daemon", Some("--daemon"), 99, 0),
    ("-bg-daemon", Some("--bg-daemon"), 99, 0),
    ("-fg-daemon", Some("--fg-daemon"), 99, 0),
    ("-help", Some("--help"), 90, 0),
    ("-nl", Some("--no-loadup"), 70, 0),
    ("-nsl", Some("--no-site-lisp"), 65, 0),
    ("-no-build-details", Some("--no-build-details"), 63, 0),
    ("-module-assertions", Some("--module-assertions"), 62, 0),
    ("-d", Some("--display"), 60, 1),
    ("-display", None, 60, 1),
    ("-Q", Some("--quick"), 55, 0),
    ("-quick", None, 55, 0),
    ("-x", None, 55, 0),
    ("-q", Some("--no-init-file"), 50, 0),
    ("-no-init-file", None, 50, 0),
    ("-init-directory", Some("--init-directory"), 30, 1),
    ("-no-x-resources", Some("--no-x-resources"), 40, 0),
    ("-no-site-file", Some("--no-site-file"), 40, 0),
    ("-no-comp-spawn", Some("--no-comp-spawn"), 60, 0),
    ("-u", Some("--user"), 30, 1),
    ("-user", None, 30, 1),
    ("-debug-init", Some("--debug-init"), 20, 0),
    ("-iconic", Some("--iconic"), 15, 0),
    ("-D", Some("--basic-display"), 12, 0),
    ("-basic-display", None, 12, 0),
    ("-nbc", Some("--no-blinking-cursor"), 12, 0),
    ("-nbi", Some("--no-bitmap-icon"), 10, 0),
    ("-bg", Some("--background-color"), 10, 1),
    ("-background", None, 10, 1),
    ("-fg", Some("--foreground-color"), 10, 1),
    ("-foreground", None, 10, 1),
    ("-bd", Some("--border-color"), 10, 1),
    ("-bw", Some("--border-width"), 10, 1),
    ("-ib", Some("--internal-border"), 10, 1),
    ("-ms", Some("--mouse-color"), 10, 1),
    ("-cr", Some("--cursor-color"), 10, 1),
    ("-fn", Some("--font"), 10, 1),
    ("-font", None, 10, 1),
    ("-fs", Some("--fullscreen"), 10, 0),
    ("-fw", Some("--fullwidth"), 10, 0),
    ("-fh", Some("--fullheight"), 10, 0),
    ("-mm", Some("--maximized"), 10, 0),
    ("-g", Some("--geometry"), 10, 1),
    ("-geometry", None, 10, 1),
    ("-T", Some("--title"), 10, 1),
    ("-title", None, 10, 1),
    ("-name", Some("--name"), 10, 1),
    ("-xrm", Some("--xrm"), 10, 1),
    ("-parent-id", Some("--parent-id"), 10, 1),
    ("-r", Some("--reverse-video"), 5, 0),
    ("-rv", None, 5, 0),
    ("-reverse", None, 5, 0),
    ("-hb", Some("--horizontal-scroll-bars"), 5, 0),
    ("-vb", Some("--vertical-scroll-bars"), 5, 0),
    ("-color", Some("--color"), 5, 0),
    ("-no-splash", Some("--no-splash"), 3, 0),
    ("-no-desktop", Some("--no-desktop"), 3, 0),
    ("-temacs", Some("--temacs"), 1, 1),
    ("-dump-file", Some("--dump-file"), 1, 1),
    #[cfg(target_os = "linux")]
    ("-seccomp", Some("--seccomp"), 1, 1),
    ("-L", Some("--directory"), 0, 1),
    ("-directory", None, 0, 1),
    ("-l", Some("--load"), 0, 1),
    ("-load", None, 0, 1),
    ("-scriptload", None, 0, 1),
    ("-f", Some("--funcall"), 0, 1),
    ("-funcall", None, 0, 1),
    ("-eval", Some("--eval"), 0, 1),
    ("-execute", Some("--execute"), 0, 1),
    ("-find-file", Some("--find-file"), 0, 1),
    ("-visit", Some("--visit"), 0, 1),
    ("-file", Some("--file"), 0, 1),
    ("-insert", Some("--insert"), 0, 1),
    ("-kill", Some("--kill"), -10, 0),
];

/// emacs.c:sort_args.  Reorder ARGV so that the highest-priority options
/// come first, keeping the order of equal priorities, keeping an option
/// with its argument, dropping repeated copies of an argument-less option,
/// and leaving "--" and everything after it at the end.  An option missing
/// its argument is emacs.c's `fatal'.
fn sort_args(argv: &mut Vec<String>) -> Result<(), String> {
    let argc = argv.len();
    // options[i]: 0 for an option without arguments, n for one taking n,
    // -1 for an ordinary argument.
    let mut options = vec![-1i64; argc];
    let mut priority = vec![0i32; argc];
    let mut from = 1;
    while from < argc {
        let arg = argv[from].clone();
        if arg.starts_with('-') {
            if arg == "--" {
                for slot in from..argc {
                    priority[slot] = -100;
                    options[slot] = -1;
                }
                break;
            }
            if let Some(&(_, _, prio, nargs)) =
                STANDARD_ARGS.iter().find(|(name, _, _, _)| *name == arg)
            {
                options[from] = nargs as i64;
                priority[from] = prio;
                if from + nargs >= argc {
                    return Err(format!("Option '{arg}' requires an argument"));
                }
                from += nargs;
            } else if arg.starts_with("--") {
                let equals = arg.find('=');
                let this = &arg[..equals.unwrap_or(arg.len())];
                let mut matched = STANDARD_ARGS
                    .iter()
                    .filter(|(_, long, _, _)| long.is_some_and(|long| long.starts_with(this)));
                match (matched.next(), matched.next()) {
                    (Some(&(_, _, prio, nargs)), None) => {
                        let nargs = if equals.is_some() { 0 } else { nargs };
                        options[from] = nargs as i64;
                        priority[from] = prio;
                        if from + nargs >= argc {
                            return Err(format!("Option '{arg}' requires an argument"));
                        }
                        from += nargs;
                    }
                    (Some(_), Some(_)) => {
                        eprintln!("Option '{arg}' matched multiple standard arguments");
                    }
                    (None, _) => {}
                }
            }
        }
        from += 1;
    }

    let mut taken = vec![false; argc];
    let mut sorted = Vec::with_capacity(argc);
    sorted.push(argv[0].clone());
    let mut incoming_used = 1;
    while incoming_used < argc {
        let mut best = None;
        let mut best_priority = -9999;
        let mut index = 1;
        while index < argc {
            if !taken[index] && priority[index] > best_priority {
                best_priority = priority[index];
                best = Some(index);
            }
            if options[index] > 0 {
                index += options[index] as usize;
            }
            index += 1;
        }
        let best = best.expect("an untaken argument remains");
        let count = options[best].max(0) as usize;
        let duplicate = options[best] == 0
            && sorted
                .last()
                .is_some_and(|previous: &String| previous == &argv[best]);
        if !duplicate {
            sorted.extend(argv[best..=best + count].iter().cloned());
        }
        incoming_used += 1 + count;
        for slot in taken.iter_mut().skip(best).take(count + 1) {
            *slot = true;
        }
    }
    *argv = sorted;
    Ok(())
}

fn run_batch_with_large_stack(options: BatchRunOptions) -> Result<BatchRunOutcome, String> {
    // Dropping an N-element list recurses N deep through the cons chain;
    // upstream tests build 8-million-element lists (Bug#24264), so the
    // batch thread needs stack for the teardown as well as evaluation.
    // The stack is virtual memory: only touched pages ever commit.
    thread::Builder::new()
        .stack_size(8 * 1024 * 1024 * 1024)
        .spawn(move || batch::run_batch(options))
        .map_err(|error| format!("start batch thread: {error}"))?
        .join()
        .map_err(|_| "batch thread panicked".to_string())?
}

#[cfg(unix)]
fn restart_current_process() -> Result<u8, String> {
    let mut args = std::env::args_os();
    let executable = args
        .next()
        .ok_or_else(|| "No command line arguments known; unable to re-execute Emaxx".to_string())?;
    let error = Command::new(executable).args(args).exec();
    Err(format!("Unable to re-execute Emaxx: {error}"))
}

#[cfg(not(unix))]
fn restart_current_process() -> Result<u8, String> {
    let mut args = std::env::args_os();
    let executable = args
        .next()
        .ok_or_else(|| "No command line arguments known; unable to re-execute Emaxx".to_string())?;
    let status = Command::new(executable)
        .args(args)
        .status()
        .map_err(|error| format!("Unable to re-execute Emaxx: {error}"))?;
    Ok(status.code().unwrap_or(1) as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted(args: &[&str]) -> Vec<String> {
        let mut argv = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
        sort_args(&mut argv).expect("the arguments are complete");
        argv
    }

    fn sort_error(args: &[&str]) -> String {
        let mut argv = args.iter().map(|arg| arg.to_string()).collect::<Vec<_>>();
        sort_args(&mut argv).expect_err("an option is missing its argument")
    }

    #[test]
    fn sort_args_orders_by_emacs_c_priority_and_keeps_option_arguments() {
        // -Q (55) and -batch (100) move ahead of --eval (0); --eval keeps
        // its argument; equal priorities keep their order.
        assert_eq!(
            sorted(&["emaxx", "--eval", "(a)", "-Q", "--eval", "(b)", "--batch"]),
            ["emaxx", "--batch", "-Q", "--eval", "(a)", "--eval", "(b)"]
        );
        // A repeated argument-less option is kept once; an --OPTION=VALUE
        // spelling takes no separate argument; an unambiguous long-option
        // prefix is the option; "--" and what follows stay at the end.
        assert_eq!(
            sorted(&[
                "emaxx",
                "--",
                "-Q",
                "-Q",
                "--no-init-fil",
                "--load=x.el",
                "-Q",
                "file"
            ]),
            [
                "emaxx",
                "--",
                "-Q",
                "-Q",
                "--no-init-fil",
                "--load=x.el",
                "-Q",
                "file"
            ]
        );
        assert_eq!(
            sorted(&[
                "emaxx",
                "-Q",
                "--load=x.el",
                "-Q",
                "--no-init-fil",
                "file",
                "-Q"
            ]),
            ["emaxx", "-Q", "--no-init-fil", "--load=x.el", "file"]
        );
        // emacs.c:fatal for an option missing its argument.
        assert_eq!(
            sort_error(&["emaxx", "--batch", "--eval"]),
            "Option '--eval' requires an argument"
        );
        assert_eq!(
            sort_error(&["emaxx", "-l"]),
            "Option '-l' requires an argument"
        );
    }

    fn argv(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    #[test]
    fn argmatch_matches_exactly_or_by_long_abbreviation_and_takes_values() {
        let args = argv(&[
            "emacs",
            "-batch",
            "--no-site-l",
            "--chdir=/tmp",
            "--load",
            "f.el",
        ]);
        let mut skip = 0;
        assert_eq!(
            argmatch(&args, "-batch", Some("--batch"), 5, false, &mut skip),
            Some(None)
        );
        assert_eq!(skip, 1);
        // Too short an abbreviation is not the option.
        assert_eq!(
            argmatch(&args, "-nsl", Some("--no-site-lisp"), 12, false, &mut skip),
            None
        );
        assert_eq!(
            argmatch(&args, "-nsl", Some("--no-site-lisp"), 11, false, &mut skip),
            Some(None)
        );
        assert_eq!(skip, 2);
        assert_eq!(
            argmatch(&args, "-chdir", Some("--chdir"), 4, true, &mut skip),
            Some(Some("/tmp".to_string()))
        );
        assert_eq!(skip, 3);
        assert_eq!(
            argmatch(&args, "-l", Some("--load"), 3, true, &mut skip),
            Some(Some("f.el".to_string()))
        );
        assert_eq!(skip, 5);
        assert_eq!(
            argmatch(&args, "-l", Some("--load"), 3, true, &mut skip),
            None
        );
        // The leading scans stop at "--".
        assert_eq!(
            leading_option_value(
                &argv(&["emacs", "-Q", "--", "--temacs=pdump"]),
                "-temacs",
                "--temacs",
                8
            ),
            None
        );
        assert_eq!(
            leading_option_value(
                &argv(&["emacs", "-Q", "--temacs", "pdump"]),
                "-temacs",
                "--temacs",
                8
            ),
            Some("pdump".to_string())
        );
    }
}
