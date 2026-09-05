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
    let original_args = std::env::args_os().collect::<Vec<_>>();
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

fn normalize_gnu_single_dash_long_options(
    args: impl IntoIterator<Item = OsString>,
) -> Vec<OsString> {
    args.into_iter()
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
