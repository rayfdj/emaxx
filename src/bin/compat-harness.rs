use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::Serialize;

use emaxx::compat::{self, BatchReport, FileStatus, OracleLocalConfig, OracleLock, Scope};

#[derive(Debug, Parser)]
#[command(name = "compat-harness", disable_help_subcommand = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Oracle(OracleArgs),
    Selectors,
    List(ListArgs),
    Run(RunArgs),
}

#[derive(Debug, Args)]
struct OracleArgs {
    #[command(subcommand)]
    command: OracleCommand,
}

#[derive(Debug, Subcommand)]
enum OracleCommand {
    Pin(PinArgs),
}

#[derive(Debug, Args)]
struct PinArgs {
    #[arg(long)]
    emacs: PathBuf,
    #[arg(long)]
    repo: PathBuf,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ScopeArg {
    Src,
    Lisp,
    All,
}

impl From<ScopeArg> for Scope {
    fn from(value: ScopeArg) -> Self {
        match value {
            ScopeArg::Src => Scope::Src,
            ScopeArg::Lisp => Scope::Lisp,
            ScopeArg::All => Scope::All,
        }
    }
}

#[derive(Debug, Args)]
struct ListArgs {
    #[arg(long, value_enum, default_value = "all")]
    scope: ScopeArg,
    #[arg(long, default_value = "default")]
    selector: String,
    #[arg(long)]
    file: Option<String>,
    #[arg(long)]
    name: Option<String>,
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(long, value_enum, default_value = "all")]
    scope: ScopeArg,
    #[arg(long, default_value = "default")]
    selector: String,
    #[arg(long)]
    file: Option<String>,
    #[arg(long)]
    name: Option<String>,
}

#[derive(Debug)]
struct ProcessResult {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
}

#[derive(Debug)]
struct RunnerArtifacts {
    report: BatchReport,
    process: ProcessResult,
}

#[derive(Debug, Serialize)]
struct AggregateReport {
    selector: String,
    scope: String,
    total_files: usize,
    matching_files: usize,
    mismatching_files: usize,
    mismatches: Vec<String>,
}

fn main() -> std::process::ExitCode {
    match try_main() {
        Ok(code) => std::process::ExitCode::from(code),
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::from(2)
        }
    }
}

fn try_main() -> Result<u8, String> {
    match Cli::parse().command {
        Commands::Oracle(oracle) => match oracle.command {
            OracleCommand::Pin(args) => {
                pin_oracle(args)?;
                Ok(0)
            }
        },
        Commands::Selectors => {
            print_selectors()?;
            Ok(0)
        }
        Commands::List(args) => {
            list_tests(args)?;
            Ok(0)
        }
        Commands::Run(args) => run_compat(args),
    }
}

fn pin_oracle(args: PinArgs) -> Result<(), String> {
    let emacs_binary = compat::canonicalize_path(&args.emacs)?;
    let emacs_repo = compat::canonicalize_path(&args.repo)?;
    let runtime = compat::current_emacs_runtime(&emacs_binary)?;
    let commit = compat::current_repo_commit(&emacs_repo)?;
    let lock = OracleLock::current(
        commit,
        runtime.emacs_version.clone(),
        runtime.system_type.clone(),
        runtime.native_compilation,
    );
    let local = OracleLocalConfig::new(emacs_binary.clone(), emacs_repo.clone());
    compat::write_oracle_lock(&lock)?;
    compat::write_oracle_local_config(&local)?;
    println!(
        "Pinned oracle {} at {} against {} ({}, system_type={}, native_compilation={})",
        runtime.emacs_version,
        emacs_binary.display(),
        emacs_repo.display(),
        lock.emacs_repo_commit,
        lock.system_type,
        lock.native_compilation
    );
    Ok(())
}

fn print_selectors() -> Result<(), String> {
    let lock = compat::load_oracle_lock()?;
    let aliases = lock.selector_aliases();
    println!("Pinned oracle selectors:");
    for (alias, expression) in aliases {
        println!("  {alias}: {expression}");
    }
    println!("Literal ERT selector expressions are also accepted via --selector.");
    Ok(())
}

fn list_tests(args: ListArgs) -> Result<(), String> {
    let context = load_context()?;
    let selector = compat::resolve_selector(&context.lock, &args.selector)?;
    let files = selected_files(&context.local.emacs_repo, args.scope.into(), args.file.as_deref())?;
    let name_filter = compat::compile_name_filter(args.name.as_deref())?;
    let artifact_root = make_artifact_root("list")?;

    for file in files {
        let relative = compat::relative_test_path(&context.local.emacs_repo, &file)?;
        let per_file_dir = per_file_artifact_dir(&artifact_root, &relative);
        let oracle = run_oracle(
            &context.local,
            &relative,
            &file,
            &selector,
            &per_file_dir,
            compat::resolve_timeout()?,
        )?;
        let filtered = compat::filter_report_by_name(&oracle.report, name_filter.as_ref());
        match filtered.file_status {
            FileStatus::Loaded => {
                println!(
                    "{}: discovered={} selected={}",
                    filtered.file,
                    filtered.discovered_tests.len(),
                    filtered.selected_tests.len()
                );
                for name in &filtered.selected_tests {
                    println!("  {name}");
                }
            }
            FileStatus::LoadError => {
                println!(
                    "{}: load-error {}",
                    filtered.file,
                    filtered.file_error.unwrap_or_else(|| "unknown load error".into())
                );
            }
        }
    }

    Ok(())
}

fn run_compat(args: RunArgs) -> Result<u8, String> {
    let context = load_context()?;
    let selector = compat::resolve_selector(&context.lock, &args.selector)?;
    let files = selected_files(&context.local.emacs_repo, args.scope.into(), args.file.as_deref())?;
    let timeout = compat::resolve_timeout()?;
    let name_filter = compat::compile_name_filter(args.name.as_deref())?;
    let artifact_root = make_artifact_root("run")?;
    let emaxx_binary = ensure_emaxx_binary()?;

    let mut matching_files = 0usize;
    let mut mismatches = Vec::new();

    for file in files {
        let relative = compat::relative_test_path(&context.local.emacs_repo, &file)?;
        let per_file_dir = per_file_artifact_dir(&artifact_root, &relative);
        fs::create_dir_all(&per_file_dir)
            .map_err(|error| format!("create {}: {error}", per_file_dir.display()))?;

        let oracle = run_oracle(
            &context.local,
            &relative,
            &file,
            &selector,
            &per_file_dir,
            timeout,
        )?;
        let emaxx = run_emaxx(
            &emaxx_binary,
            &context.local.emacs_repo,
            &relative,
            &file,
            &selector,
            &per_file_dir,
            timeout,
        )?;

        let oracle_report = compat::filter_report_by_name(&oracle.report, name_filter.as_ref());
        let emaxx_report = compat::filter_report_by_name(&emaxx.report, name_filter.as_ref());
        let comparison = compat::compare_reports(&oracle_report, &emaxx_report);
        write_json(
            &per_file_dir.join("comparison.json"),
            &comparison,
            "comparison report",
        )?;
        write_raw_log(&per_file_dir.join("oracle.log"), &oracle.process)?;
        write_raw_log(&per_file_dir.join("emaxx.log"), &emaxx.process)?;

        if comparison.matches {
            matching_files += 1;
            println!("PASS {}", relative);
        } else {
            mismatches.push(relative.clone());
            println!("FAIL {}", relative);
            for issue in &comparison.issues {
                println!("  [{}] {}", issue.kind, issue.detail);
            }
        }
    }

    let aggregate = AggregateReport {
        selector: selector.clone(),
        scope: format!("{:?}", args.scope),
        total_files: matching_files + mismatches.len(),
        matching_files,
        mismatching_files: mismatches.len(),
        mismatches,
    };
    write_json(
        &artifact_root.join("summary.json"),
        &aggregate,
        "aggregate summary",
    )?;

    if aggregate.mismatching_files == 0 {
        Ok(0)
    } else {
        Ok(1)
    }
}

struct Context {
    lock: OracleLock,
    local: OracleLocalConfig,
}

fn load_context() -> Result<Context, String> {
    let lock = compat::load_oracle_lock()?;
    let local = compat::load_oracle_local_config()?;
    compat::validate_oracle(&lock, &local)?;
    Ok(Context { lock, local })
}

fn selected_files(repo_root: &Path, scope: Scope, file_filter: Option<&str>) -> Result<Vec<PathBuf>, String> {
    let files = compat::discover_test_files(repo_root, scope)?;
    let filtered = compat::filter_files(&files, repo_root, file_filter)?;
    if file_filter.is_some() && filtered.is_empty() {
        return Err(format!(
            "no test file matched `{}` under {}",
            file_filter.unwrap_or_default(),
            repo_root.display()
        ));
    }
    Ok(filtered)
}

fn make_artifact_root(prefix: &str) -> Result<PathBuf, String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("clock error: {error}"))?
        .as_secs();
    let root = compat::project_root()
        .join("target")
        .join("compat")
        .join(format!("{prefix}-{timestamp}"));
    fs::create_dir_all(&root).map_err(|error| format!("create {}: {error}", root.display()))?;
    Ok(root)
}

fn per_file_artifact_dir(artifact_root: &Path, relative: &str) -> PathBuf {
    artifact_root.join(relative).with_extension("compat")
}

fn run_oracle(
    local: &OracleLocalConfig,
    relative_file: &str,
    file: &Path,
    selector: &str,
    per_file_dir: &Path,
    timeout: Option<Duration>,
) -> Result<RunnerArtifacts, String> {
    fs::create_dir_all(per_file_dir)
        .map_err(|error| format!("create {}: {error}", per_file_dir.display()))?;
    let result_path = per_file_dir.join("oracle.json");
    let helper_path = compat::oracle_helper_path();
    let test_directory = local.emacs_repo.join("test");
    let mut command = Command::new(&local.emacs_binary);
    compat::configure_upstream_like_env(&mut command, &test_directory);
    command.env(compat::BATCH_RESULT_FILE_ENV, &result_path);
    command.env("EMAXX_COMPAT_RELATIVE_FILE", relative_file);
    command.arg("--no-init-file");
    command.arg("--no-site-file");
    command.arg("--no-site-lisp");
    command.arg("--batch");
    command.arg("-L");
    command.arg(&test_directory);
    command.arg("-l");
    command.arg("ert");
    command.arg("-l");
    command.arg(&helper_path);
    command.arg("-l");
    command.arg(file);
    command.arg("--eval");
    command.arg(format!("(emaxx-compat-run (quote {selector}))"));

    let process = run_command(command, timeout)?;
    let report = load_or_synthesize_report(
        &result_path,
        "oracle",
        relative_file,
        selector,
        &process,
    )?;
    Ok(RunnerArtifacts { report, process })
}

fn run_emaxx(
    emaxx_binary: &Path,
    repo_root: &Path,
    relative_file: &str,
    file: &Path,
    selector: &str,
    per_file_dir: &Path,
    timeout: Option<Duration>,
) -> Result<RunnerArtifacts, String> {
    fs::create_dir_all(per_file_dir)
        .map_err(|error| format!("create {}: {error}", per_file_dir.display()))?;
    let result_path = per_file_dir.join("emaxx.json");
    let test_directory = repo_root.join("test");
    let load_paths = compat::emaxx_upstream_load_path(repo_root)?;
    let mut command = Command::new(emaxx_binary);
    compat::configure_upstream_like_env(&mut command, &test_directory);
    command.env(compat::BATCH_RESULT_FILE_ENV, &result_path);
    command.arg("--no-init-file");
    command.arg("--no-site-file");
    command.arg("--no-site-lisp");
    command.arg("--batch");
    for load_path in &load_paths {
        command.arg("-L");
        command.arg(load_path);
    }
    command.arg("-l");
    command.arg("ert");
    command.arg("-l");
    command.arg(file);
    command.arg("--eval");
    command.arg(format!(
        "(ert-run-tests-batch-and-exit (quote {selector}))"
    ));

    let process = run_command(command, timeout)?;
    let report = load_or_synthesize_report(
        &result_path,
        "emaxx",
        relative_file,
        selector,
        &process,
    )?;
    Ok(RunnerArtifacts { report, process })
}

fn load_or_synthesize_report(
    result_path: &Path,
    runner: &str,
    relative_file: &str,
    selector: &str,
    process: &ProcessResult,
) -> Result<BatchReport, String> {
    if result_path.exists() {
        return BatchReport::read_json(result_path);
    }

    let message = if process.timed_out {
        "process timed out".to_string()
    } else if let Some(exit_code) = process.exit_code {
        let detail = if process.stderr.trim().is_empty() {
            process.stdout.trim()
        } else {
            process.stderr.trim()
        };
        format!(
            "process exited {}: {}",
            exit_code,
            if detail.is_empty() {
                "no structured result produced"
            } else {
                detail
            }
        )
    } else {
        "process terminated without a status code".to_string()
    };
    Ok(BatchReport::load_error(runner, relative_file, selector, message))
}

fn run_command(mut command: Command, timeout: Option<Duration>) -> Result<ProcessResult, String> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|error| format!("spawn command: {error}"))?;
    let started = Instant::now();

    loop {
        if let Some(status) = child.try_wait().map_err(|error| format!("wait for command: {error}"))? {
            let stdout = read_pipe(child.stdout.take())?;
            let stderr = read_pipe(child.stderr.take())?;
            return Ok(ProcessResult {
                exit_code: status.code(),
                stdout,
                stderr,
                timed_out: false,
            });
        }

        if timeout.is_some_and(|limit| started.elapsed() > limit) {
            child.kill().map_err(|error| format!("kill timed out command: {error}"))?;
            let status = child.wait().map_err(|error| format!("wait after kill: {error}"))?;
            let stdout = read_pipe(child.stdout.take())?;
            let stderr = read_pipe(child.stderr.take())?;
            return Ok(ProcessResult {
                exit_code: status.code(),
                stdout,
                stderr,
                timed_out: true,
            });
        }

        thread::sleep(Duration::from_millis(50));
    }
}

fn read_pipe(pipe: Option<impl Read>) -> Result<String, String> {
    let Some(mut pipe) = pipe else {
        return Ok(String::new());
    };
    let mut bytes = Vec::new();
    pipe.read_to_end(&mut bytes)
        .map_err(|error| format!("read process output: {error}"))?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn write_raw_log(path: &Path, process: &ProcessResult) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    let mut content = String::new();
    content.push_str(&format!("exit_code={:?}\n", process.exit_code));
    content.push_str(&format!("timed_out={}\n", process.timed_out));
    content.push_str("\n[stdout]\n");
    content.push_str(&process.stdout);
    content.push_str("\n[stderr]\n");
    content.push_str(&process.stderr);
    fs::write(path, content).map_err(|error| format!("write {}: {error}", path.display()))
}

fn write_json(path: &Path, value: &impl Serialize, label: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    let json =
        serde_json::to_string_pretty(value).map_err(|error| format!("serialize {label}: {error}"))?;
    fs::write(path, json).map_err(|error| format!("write {}: {error}", path.display()))
}

fn ensure_emaxx_binary() -> Result<PathBuf, String> {
    let current = env::current_exe().map_err(|error| format!("current exe: {error}"))?;
    let Some(bin_dir) = current.parent() else {
        return Err(format!("cannot locate binary directory for {}", current.display()));
    };
    let candidate = bin_dir.join("emaxx");

    let status = Command::new("cargo")
        .arg("build")
        .arg("--quiet")
        .arg("--bin")
        .arg("emaxx")
        .current_dir(compat::project_root())
        .status()
        .map_err(|error| format!("build emaxx binary: {error}"))?;
    if !status.success() {
        return Err("`cargo build --quiet --bin emaxx` failed".into());
    }
    if !candidate.exists() {
        return Err(format!("expected emaxx binary at {}", candidate.display()));
    }
    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_file_artifact_directory_preserves_tree_shape() {
        let root = PathBuf::from("/tmp/compat");
        let dir = per_file_artifact_dir(&root, "test/src/buffer-tests.el");
        assert_eq!(dir, PathBuf::from("/tmp/compat/test/src/buffer-tests.compat"));
    }
}
