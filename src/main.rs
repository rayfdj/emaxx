#![deny(clippy::unwrap_used)]

use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, ExitCode};
use std::thread;

use clap::{ArgMatches, CommandFactory, FromArgMatches, Parser};

use emaxx::batch::{self, BatchAction, BatchRunOptions, BatchRunOutcome};
use emaxx::tty;

#[derive(Debug, Parser)]
#[command(name = "emaxx", version, disable_help_subcommand = true)]
struct Cli {
    #[arg(long)]
    batch: bool,
    // GNU resolves `-b' to the no-build-details startup option.  Emaxx does
    // not add build metadata, so parsing the flag is the complete behavior.
    // GNU has no short `-b'; only `--no-build-details' (and the
    // single-dash long spelling) exist.
    #[arg(long = "no-build-details")]
    _no_build_details: bool,
    #[arg(long)]
    no_init_file: bool,
    #[arg(long)]
    no_site_file: bool,
    #[arg(long)]
    no_site_lisp: bool,
    // `emacs.c' recognizes and orders this switch, while startup.el owns its
    // effect on the Elisp compiler state. All batch invocations go through
    // unchanged GNU startup, which interprets this switch itself.
    #[arg(long = "no-comp-spawn")]
    no_comp_spawn: bool,
    // C uses quick to suppress site-lisp paths; GNU startup.el owns its
    // remaining effects and still receives the original argument.
    #[arg(short = 'Q', long = "quick")]
    _quick: bool,
    #[arg(short = 'L', value_name = "DIR")]
    load_path: Vec<PathBuf>,
    #[arg(short = 'l', long = "load", value_name = "FILE")]
    load: Vec<String>,
    #[arg(long = "eval", value_name = "EXPR")]
    eval: Vec<String>,
    #[arg(short = 'f', long = "funcall", value_name = "FUNCTION")]
    funcall: Vec<String>,
    // Parsed for CLI compatibility; the filter itself is installed by
    // `maybe_load_seccomp' before ordinary argument processing, exactly
    // like GNU's main() (emacs.c).
    #[cfg(target_os = "linux")]
    #[arg(long = "seccomp", value_name = "FILE")]
    _seccomp: Option<String>,
    #[arg(value_name = "FILE")]
    file: Vec<PathBuf>,
}

fn main() -> ExitCode {
    match try_main() {
        Ok(code) => ExitCode::from(code),
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

fn try_main() -> Result<u8, String> {
    // GNU checks for a Seccomp filter at the very beginning of main(),
    // before any other startup work, so the filter protects the whole
    // initialization phase.
    #[cfg(target_os = "linux")]
    seccomp::maybe_load_seccomp();
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
    // emacs.c:main sorts argv by option priority before anything reads it,
    // so `-Q' after `--eval' still reaches startup.el's option loop first.
    let original_args = match sort_args(std::env::args_os().collect::<Vec<_>>()) {
        Ok(args) => args,
        Err(message) => {
            // emacs.c:fatal.
            eprintln!("emacs: {message}");
            return Ok(1);
        }
    };
    let args = normalize_gnu_single_dash_long_options(original_args.iter().cloned());
    let matches = Cli::command().get_matches_from(args.clone());
    let startup_args = startup_command_line_args(&original_args)?;
    let actions = ordered_batch_actions(&matches);
    let cli = Cli::from_arg_matches(&matches).map_err(|error| error.to_string())?;
    let no_site_lisp = cli.no_site_lisp || cli._quick;
    if cli.batch {
        let outcome = run_batch_with_large_stack(
            BatchRunOptions {
                no_site_lisp,
                startup_command_line_args: Some(startup_args),
                defer_delayed_custom_init: true,
                ..Default::default()
            },
            actions,
        )?;
        // exit() and shut_down_emacs's reset_sys_modes flush stdio's stdout
        // before the process goes away or re-executes itself.
        emaxx::lisp::flush_batch_stdout();
        return match outcome {
            BatchRunOutcome::Exit(code) => Ok(code as u8),
            BatchRunOutcome::Restart => restart_current_process(),
        };
    }

    run_interactive(&startup_args, no_site_lisp)
}

/// emacs.c's `maybe_load_seccomp'/`load_seccomp': read a Secure Computing
/// BPF filter named by `-seccomp'/`--seccomp' and install it with the
/// `seccomp' system call, exiting fatally when the file is unusable.
#[cfg(target_os = "linux")]
mod seccomp {
    use std::io::Read;

    pub fn maybe_load_seccomp() {
        let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
        let mut file: Option<String> = None;
        let mut index = 1;
        while index < args.len() {
            let arg = args[index].to_string_lossy();
            if arg == "--" {
                break;
            }
            if let Some(value) = arg
                .strip_prefix("--seccomp=")
                .or_else(|| arg.strip_prefix("-seccomp="))
            {
                file = Some(value.to_string());
                break;
            }
            if arg == "--seccomp" || arg == "-seccomp" {
                file = args
                    .get(index + 1)
                    .map(|value| value.to_string_lossy().into_owned());
                break;
            }
            index += 1;
        }
        let Some(file) = file else { return };
        if !load_seccomp(&file) {
            // GNU's fatal(): report and die before any Lisp runs.
            eprintln!("emacs: cannot enable seccomp filter from {file}");
            std::process::exit(1);
        }
    }

    fn load_seccomp(file: &str) -> bool {
        const FILTER_ENTRY_SIZE: u64 = std::mem::size_of::<libc::sock_filter>() as u64;
        let mut handle = match std::fs::File::open(file) {
            Ok(handle) => handle,
            Err(error) => {
                eprintln!("emacs: open: {error}");
                return false;
            }
        };
        let metadata = match handle.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                eprintln!("emacs: fstat: {error}");
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
            eprintln!("emacs: read: {error}");
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
            eprintln!("emacs: seccomp: {}", std::io::Error::last_os_error());
            return false;
        }
        true
    }
}

/// emacs.c's standard_args for this configuration: the old-fashioned
/// spelling, the long spelling, the priority and the argument count.
/// HAVE_PDUMPER, HAVE_MODULES and SECCOMP_USABLE entries are present as in
/// the configured Linux build; the HAVE_NS entries are not.
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
fn sort_args(argv: Vec<OsString>) -> Result<Vec<OsString>, String> {
    let argc = argv.len();
    // options[i]: 0 for an option without arguments, n for one taking n,
    // -1 for an ordinary argument.
    let mut options = vec![-1i64; argc];
    let mut priority = vec![0i32; argc];
    let mut from = 1;
    while from < argc {
        let arg = argv[from].to_string_lossy().into_owned();
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
                .is_some_and(|previous: &OsString| previous == &argv[best]);
        if !duplicate {
            sorted.extend(argv[best..=best + count].iter().cloned());
        }
        incoming_used += 1 + count;
        for slot in taken.iter_mut().skip(best).take(count + 1) {
            *slot = true;
        }
    }
    Ok(sorted)
}

fn normalize_gnu_single_dash_long_options(
    args: impl IntoIterator<Item = OsString>,
) -> Vec<OsString> {
    args.into_iter()
        .map(|arg| {
            // emacs.c:sort_args and startup.el both accept an unambiguous
            // prefix of a long option (`--no-init-fil'); expand it to the
            // spelling Clap knows, keeping any `=VALUE'.
            let Some(text) = arg.to_str() else {
                return arg;
            };
            if !text.starts_with("--") || text == "--" {
                return arg;
            }
            let (prefix, rest) = match text.find('=') {
                Some(index) => (&text[..index], &text[index..]),
                None => (text, ""),
            };
            let mut matched = STANDARD_ARGS
                .iter()
                .filter_map(|(_, long, _, _)| long.filter(|long| long.starts_with(prefix)));
            match (matched.next(), matched.next()) {
                (Some(long), None) if long != prefix => OsString::from(format!("{long}{rest}")),
                _ => arg,
            }
        })
        .map(|arg| match arg.to_str() {
            // GNU accepts the full spelling of long options with one dash.
            // Normalize the subset Emaxx implements before Clap interprets
            // each spelling as a cluster of unrelated short options.
            Some("-batch") => OsString::from("--batch"),
            Some("-eval") => OsString::from("--eval"),
            Some("-funcall") => OsString::from("--funcall"),
            Some("-help") => OsString::from("--help"),
            Some("-load") => OsString::from("--load"),
            Some("-no-build-details") => OsString::from("--no-build-details"),
            Some("-no-init-file") => OsString::from("--no-init-file"),
            Some("-no-site-file") => OsString::from("--no-site-file"),
            Some("-no-site-lisp") => OsString::from("--no-site-lisp"),
            Some("-no-comp-spawn") => OsString::from("--no-comp-spawn"),
            Some("-quick") => OsString::from("--quick"),
            Some("-version") => OsString::from("--version"),
            _ => arg,
        })
        .collect()
}

/// Build the argument list seen by unchanged GNU startup.el after emacs.c has
/// consumed its C-owned startup switches.  All remaining arguments retain
/// their original order and are interpreted by `normal-top-level'.
fn startup_command_line_args(args: &[OsString]) -> Result<Vec<String>, String> {
    args.iter()
        .filter(|arg| {
            !matches!(
                arg.to_str(),
                Some(
                    "-batch"
                        | "--batch"
                        | "-no-build-details"
                        | "--no-build-details"
                        | "-nsl"
                        | "-no-site-lisp"
                        | "--no-site-lisp"
                )
            )
        })
        .map(|arg| {
            arg.to_str()
                .map(str::to_owned)
                .ok_or_else(|| "command-line argument is not valid UTF-8".to_string())
        })
        .collect()
}

fn ordered_batch_actions(matches: &ArgMatches) -> Vec<BatchAction> {
    let mut indexed_actions = Vec::new();
    if let (Some(indices), Some(values)) = (
        matches.indices_of("load"),
        matches.get_many::<String>("load"),
    ) {
        indexed_actions.extend(
            indices
                .zip(values)
                .map(|(index, value)| (index, BatchAction::Load(value.clone()))),
        );
    }
    if let (Some(indices), Some(values)) = (
        matches.indices_of("eval"),
        matches.get_many::<String>("eval"),
    ) {
        indexed_actions.extend(
            indices
                .zip(values)
                .map(|(index, value)| (index, BatchAction::Eval(value.clone()))),
        );
    }
    if let (Some(indices), Some(values)) = (
        matches.indices_of("funcall"),
        matches.get_many::<String>("funcall"),
    ) {
        indexed_actions.extend(
            indices
                .zip(values)
                .map(|(index, value)| (index, BatchAction::Funcall(value.clone()))),
        );
    }
    indexed_actions.sort_by_key(|(index, _)| *index);
    indexed_actions
        .into_iter()
        .map(|(_, action)| action)
        .collect()
}

fn run_batch_with_large_stack(
    options: BatchRunOptions,
    actions: Vec<BatchAction>,
) -> Result<BatchRunOutcome, String> {
    // Dropping an N-element list recurses N deep through the cons chain;
    // upstream tests build 8-million-element lists (Bug#24264), so the
    // batch thread needs stack for the teardown as well as evaluation.
    // The stack is virtual memory: only touched pages ever commit.
    thread::Builder::new()
        .stack_size(8 * 1024 * 1024 * 1024)
        .spawn(move || batch::run_batch_with_actions(options, actions))
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

fn run_interactive(args: &[String], no_site_lisp: bool) -> Result<u8, String> {
    tty::run(args, no_site_lisp).map(|code| code as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sorted(args: &[&str]) -> Vec<String> {
        sort_args(args.iter().map(OsString::from).collect())
            .expect("the arguments are complete")
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn sort_error(args: &[&str]) -> String {
        sort_args(args.iter().map(OsString::from).collect())
            .expect_err("an option is missing its argument")
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

    #[test]
    fn startup_receives_gnu_no_comp_spawn_spelling_unchanged() {
        let original = [
            OsString::from("emaxx"),
            OsString::from("-no-comp-spawn"),
            OsString::from("-Q"),
            OsString::from("--batch"),
            OsString::from("-l"),
            OsString::from("worker.el"),
        ];
        assert_eq!(
            startup_command_line_args(&original).expect("UTF-8 argv"),
            ["emaxx", "-no-comp-spawn", "-Q", "-l", "worker.el"]
        );
    }
}
