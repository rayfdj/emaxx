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
    #[arg(short = 'b', long = "no-build-details")]
    _no_build_details: bool,
    #[arg(long)]
    no_init_file: bool,
    #[arg(long)]
    no_site_file: bool,
    #[arg(long)]
    no_site_lisp: bool,
    // Emaxx does not load user/site init files yet, so GNU's -Q/--quick is
    // already the effective startup mode.  Keep the parsed flag for command-
    // line compatibility even though it requires no additional action.
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
    let args = normalize_gnu_single_dash_long_options(std::env::args_os());
    let matches = Cli::command().get_matches_from(args);
    let actions = ordered_batch_actions(&matches);
    let cli = Cli::from_arg_matches(&matches).map_err(|error| error.to_string())?;
    if cli.batch {
        let outcome = run_batch_with_large_stack(
            BatchRunOptions {
                load_path: cli.load_path,
                load: cli.load,
                eval: cli.eval,
                funcall: cli.funcall,
                args_left: cli
                    .file
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect(),
            },
            actions,
        )?;
        return match outcome {
            BatchRunOutcome::Exit(code) => Ok(code as u8),
            BatchRunOutcome::Restart => restart_current_process(),
        };
    }

    if cli.no_init_file
        || cli.no_site_file
        || cli.no_site_lisp
        || !cli.load_path.is_empty()
        || !cli.load.is_empty()
        || !cli.eval.is_empty()
        || !cli.funcall.is_empty()
    {
        return Err(
            "`--no-init-file`, `--no-site-file`, `--no-site-lisp`, `-L`, `-l`, `--eval`, and `-f` are only supported together with `--batch`".into(),
        );
    }

    run_interactive(cli.file.into_iter().next())
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
            Some("-quick") => OsString::from("--quick"),
            Some("-version") => OsString::from("--version"),
            _ => arg,
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

fn run_interactive(file: Option<PathBuf>) -> Result<u8, String> {
    tty::run(file).map(|code| code as u8)
}
