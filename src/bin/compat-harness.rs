use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand, ValueEnum};
use regex::Regex;
use serde::{Deserialize, Serialize};

use emaxx::compat::{self, BatchReport, FileStatus, OracleLocalConfig, OracleLock, Scope};

const ADVANCE_COMPAT_PREFIX: &str = "Advance compatibility for ";
const COMPAT_REGRESSION_MANIFEST_PATH: &str = "compat/compat_regressions.json";

struct CompatRunPlan<'a> {
    mode: &'a str,
    scope: String,
    selector: &'a str,
    files: Vec<PathBuf>,
    name_filter: Option<&'a Regex>,
    artifact_root: &'a Path,
    timeout: Option<Duration>,
    emaxx_binary: &'a Path,
}

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
    Landed(LandedArgs),
    Regressions(RegressionArgs),
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
    LibSrc,
    Misc,
    Automated,
    All,
}

impl From<ScopeArg> for Scope {
    fn from(value: ScopeArg) -> Self {
        match value {
            ScopeArg::Src => Scope::Src,
            ScopeArg::Lisp => Scope::Lisp,
            ScopeArg::LibSrc => Scope::LibSrc,
            ScopeArg::Misc => Scope::Misc,
            ScopeArg::Automated => Scope::Automated,
            ScopeArg::All => Scope::All,
        }
    }
}

#[derive(Debug, Args)]
struct ListArgs {
    #[arg(long, value_enum, default_value = "automated")]
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
    #[arg(long, value_enum, default_value = "automated")]
    scope: ScopeArg,
    #[arg(long, default_value = "default")]
    selector: String,
    #[arg(long)]
    file: Option<String>,
    #[arg(long)]
    name: Option<String>,
}

#[derive(Debug, Args)]
struct LandedArgs {
    #[arg(long, value_enum, default_value = "all")]
    scope: ScopeArg,
    #[arg(long, default_value = "check-all")]
    selector: String,
    #[arg(long)]
    file: Option<String>,
    #[arg(long)]
    name: Option<String>,
}

#[derive(Debug, Args)]
struct RegressionArgs {
    #[command(subcommand)]
    command: RegressionCommand,
}

#[derive(Debug, Subcommand)]
enum RegressionCommand {
    List,
    Run(RegressionRunArgs),
    Add(RegressionAddArgs),
    ImportLanded(RegressionImportLandedArgs),
}

#[derive(Debug, Args)]
struct RegressionRunArgs {
    #[arg(long)]
    file: Option<String>,
    #[arg(long)]
    name: Option<String>,
}

#[derive(Debug, Args)]
struct RegressionAddArgs {
    #[arg(long)]
    file: String,
    #[arg(long, default_value = "check-all")]
    selector: String,
}

#[derive(Debug, Args)]
struct RegressionImportLandedArgs {
    #[arg(long, value_enum, default_value = "all")]
    scope: ScopeArg,
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
    mode: String,
    selector: String,
    scope: String,
    total_files: usize,
    matching_files: usize,
    mismatching_files: usize,
    files: Vec<String>,
    mismatches: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CompatibilityRegressionManifest {
    format_version: u32,
    files: Vec<CompatibilityRegressionEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct CompatibilityRegressionEntry {
    path: String,
    selector: String,
}

impl CompatibilityRegressionManifest {
    fn load_or_default() -> Result<Self, String> {
        let path = compat::compat_path(COMPAT_REGRESSION_MANIFEST_PATH);
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        Self::from_json_str(&data).map_err(|error| format!("parse {}: {error}", path.display()))
    }

    fn from_json_str(data: &str) -> Result<Self, String> {
        let manifest: Self = serde_json::from_str(data).map_err(|error| error.to_string())?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> Result<(), String> {
        if self.format_version != 1 {
            return Err(format!(
                "unsupported compatibility regression format_version {}; expected 1",
                self.format_version
            ));
        }
        let mut seen = BTreeSet::new();
        for entry in &self.files {
            if entry.path.trim().is_empty() {
                return Err("regression file path must not be empty".into());
            }
            if entry.selector.trim().is_empty() {
                return Err(format!(
                    "regression entry `{}` is missing selector",
                    entry.path
                ));
            }
            if !seen.insert((entry.path.clone(), entry.selector.clone())) {
                return Err(format!(
                    "duplicate regression entry `{}` with selector `{}`",
                    entry.path, entry.selector
                ));
            }
        }
        Ok(())
    }

    fn save(&self) -> Result<(), String> {
        let path = compat::compat_path(COMPAT_REGRESSION_MANIFEST_PATH);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create {}: {error}", parent.display()))?;
        }
        let json = serde_json::to_string_pretty(self)
            .map_err(|error| format!("serialize compatibility regressions: {error}"))?;
        fs::write(&path, json).map_err(|error| format!("write {}: {error}", path.display()))
    }

    fn insert(&mut self, entry: CompatibilityRegressionEntry) {
        if self
            .files
            .iter()
            .any(|existing| existing.path == entry.path && existing.selector == entry.selector)
        {
            return;
        }
        self.files.push(entry);
        self.files.sort();
    }
}

impl Default for CompatibilityRegressionManifest {
    fn default() -> Self {
        Self {
            format_version: 1,
            files: Vec::new(),
        }
    }
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
        Commands::Landed(args) => run_landed_compat(args),
        Commands::Regressions(args) => run_regressions(args),
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
    let files = selected_files(
        &context.local.emacs_repo,
        args.scope.into(),
        args.file.as_deref(),
    )?;
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
                    filtered
                        .file_error
                        .unwrap_or_else(|| "unknown load error".into())
                );
            }
        }
    }

    Ok(())
}

fn run_compat(args: RunArgs) -> Result<u8, String> {
    let context = load_context()?;
    let selector = compat::resolve_selector(&context.lock, &args.selector)?;
    let files = selected_files(
        &context.local.emacs_repo,
        args.scope.into(),
        args.file.as_deref(),
    )?;
    let timeout = compat::resolve_timeout()?;
    let name_filter = compat::compile_name_filter(args.name.as_deref())?;
    let artifact_root = make_artifact_root("run")?;
    let emaxx_binary = ensure_emaxx_binary()?;

    run_compat_files(
        &context,
        CompatRunPlan {
            mode: "run",
            scope: format!("{:?}", args.scope),
            selector: &selector,
            files,
            name_filter: name_filter.as_ref(),
            artifact_root: &artifact_root,
            timeout,
            emaxx_binary: &emaxx_binary,
        },
    )
}

fn run_landed_compat(args: LandedArgs) -> Result<u8, String> {
    let context = load_context()?;
    let selector = compat::resolve_selector(&context.lock, &args.selector)?;
    let files = landed_compat_files(
        &compat::project_root(),
        &context.local.emacs_repo,
        args.scope.into(),
    )?;
    let files = compat::filter_files(&files, &context.local.emacs_repo, args.file.as_deref())?;
    if args.file.is_some() && files.is_empty() {
        return Err(format!(
            "no landed compatibility file matched `{}` under {}",
            args.file.unwrap_or_default(),
            context.local.emacs_repo.display()
        ));
    }
    let timeout = compat::resolve_timeout()?;
    let name_filter = compat::compile_name_filter(args.name.as_deref())?;
    let artifact_root = make_artifact_root("landed")?;
    let emaxx_binary = ensure_emaxx_binary()?;

    run_compat_files(
        &context,
        CompatRunPlan {
            mode: "landed",
            scope: format!("{:?}", args.scope),
            selector: &selector,
            files,
            name_filter: name_filter.as_ref(),
            artifact_root: &artifact_root,
            timeout,
            emaxx_binary: &emaxx_binary,
        },
    )
}

fn run_regressions(args: RegressionArgs) -> Result<u8, String> {
    match args.command {
        RegressionCommand::List => list_regressions(),
        RegressionCommand::Run(args) => run_regressions_audit(args),
        RegressionCommand::Add(args) => add_regression(args),
        RegressionCommand::ImportLanded(args) => import_landed_regressions(args),
    }
}

fn list_regressions() -> Result<u8, String> {
    let manifest = CompatibilityRegressionManifest::load_or_default()?;
    for entry in &manifest.files {
        println!("{} [{}]", entry.path, entry.selector);
    }
    println!("total={}", manifest.files.len());
    Ok(0)
}

fn run_regressions_audit(args: RegressionRunArgs) -> Result<u8, String> {
    let context = load_context()?;
    let manifest = CompatibilityRegressionManifest::load_or_default()?;
    if manifest.files.is_empty() {
        return Err(format!(
            "{} is empty; add files with `cargo run --bin compat-harness -- regressions add --file <path>` or bootstrap with `... regressions import-landed`",
            compat::compat_path(COMPAT_REGRESSION_MANIFEST_PATH).display()
        ));
    }

    let name_filter = compat::compile_name_filter(args.name.as_deref())?;
    let timeout = compat::resolve_timeout()?;
    let artifact_root = make_artifact_root("regressions")?;
    let emaxx_binary = ensure_emaxx_binary()?;
    let entries = manifest_entries_for_file_filter(&manifest, args.file.as_deref())?;

    let mut grouped = BTreeMap::<String, Vec<PathBuf>>::new();
    for entry in entries {
        let file = resolve_manifest_entry_path(&context.local.emacs_repo, &entry)?;
        let selector = compat::resolve_selector(&context.lock, &entry.selector)?;
        grouped.entry(selector).or_default().push(file);
    }

    let mut status = 0u8;
    for (selector, files) in grouped {
        let run_status = run_compat_files(
            &context,
            CompatRunPlan {
                mode: "regressions",
                scope: "TrackedRegressions".into(),
                selector: &selector,
                files,
                name_filter: name_filter.as_ref(),
                artifact_root: &artifact_root,
                timeout,
                emaxx_binary: &emaxx_binary,
            },
        )?;
        if run_status != 0 {
            status = run_status;
        }
    }
    Ok(status)
}

fn add_regression(args: RegressionAddArgs) -> Result<u8, String> {
    let context = load_context()?;
    let selector = compat::resolve_selector(&context.lock, &args.selector)?;
    let file = resolve_manifest_path_from_cli(&context.local.emacs_repo, &args.file)?;
    let timeout = compat::resolve_timeout()?;
    let artifact_root = make_artifact_root("regression-add")?;
    let emaxx_binary = ensure_emaxx_binary()?;

    let status = run_compat_files(
        &context,
        CompatRunPlan {
            mode: "regression-add",
            scope: "TrackedRegressions".into(),
            selector: &selector,
            files: vec![file.clone()],
            name_filter: None,
            artifact_root: &artifact_root,
            timeout,
            emaxx_binary: &emaxx_binary,
        },
    )?;
    if status != 0 {
        return Err(format!(
            "refusing to record `{}` because oracle and emaxx do not match yet",
            compat::relative_test_path(&context.local.emacs_repo, &file)?
        ));
    }

    let mut manifest = CompatibilityRegressionManifest::load_or_default()?;
    canonicalize_manifest_selectors(&mut manifest, &context.lock)?;
    manifest.insert(CompatibilityRegressionEntry {
        path: compat::relative_test_path(&context.local.emacs_repo, &file)?,
        selector,
    });
    manifest.save()?;
    println!(
        "Recorded {} in {}",
        compat::relative_test_path(&context.local.emacs_repo, &file)?,
        compat::compat_path(COMPAT_REGRESSION_MANIFEST_PATH).display()
    );
    Ok(0)
}

fn import_landed_regressions(args: RegressionImportLandedArgs) -> Result<u8, String> {
    let context = load_context()?;
    let test_repo_root = &context.local.emacs_repo;
    let subjects = advance_compat_subjects(&compat::project_root())?;
    let discovered_all = compat::discover_test_files(test_repo_root, Scope::All)?;
    let (resolved_all, skipped) =
        resolve_landed_compat_files_best_effort(&subjects, &discovered_all, test_repo_root)?;
    let files = if matches!(args.scope, ScopeArg::All) {
        resolved_all
    } else {
        let allowed = compat::discover_test_files(test_repo_root, args.scope.into())?
            .into_iter()
            .collect::<BTreeSet<_>>();
        resolved_all
            .into_iter()
            .filter(|file| allowed.contains(file))
            .collect::<Vec<_>>()
    };
    let mut manifest = CompatibilityRegressionManifest::load_or_default()?;
    canonicalize_manifest_selectors(&mut manifest, &context.lock)?;
    let before = manifest.files.len();
    let selector = compat::resolve_selector(&context.lock, "check-all")?;
    for file in files {
        manifest.insert(CompatibilityRegressionEntry {
            path: compat::relative_test_path(test_repo_root, &file)?,
            selector: selector.clone(),
        });
    }
    manifest.save()?;
    let added = manifest.files.len().saturating_sub(before);
    for message in &skipped {
        eprintln!("warning: {message}");
    }
    println!(
        "Imported {} landed compatibility files into {} (skipped {})",
        added,
        compat::compat_path(COMPAT_REGRESSION_MANIFEST_PATH).display(),
        skipped.len()
    );
    Ok(0)
}

fn run_compat_files(context: &Context, plan: CompatRunPlan<'_>) -> Result<u8, String> {
    let CompatRunPlan {
        mode,
        scope,
        selector,
        files,
        name_filter,
        artifact_root,
        timeout,
        emaxx_binary,
    } = plan;
    let mut matching_files = 0usize;
    let mut mismatches = Vec::new();
    let mut relative_files = Vec::new();

    for file in files {
        let relative = compat::relative_test_path(&context.local.emacs_repo, &file)?;
        relative_files.push(relative.clone());
        let per_file_dir = per_file_artifact_dir(artifact_root, &relative);
        fs::create_dir_all(&per_file_dir)
            .map_err(|error| format!("create {}: {error}", per_file_dir.display()))?;

        let oracle = run_oracle(
            &context.local,
            &relative,
            &file,
            selector,
            &per_file_dir,
            timeout,
        )?;
        let emaxx = run_emaxx(
            emaxx_binary,
            &context.local.emacs_repo,
            &relative,
            &file,
            selector,
            &per_file_dir,
            timeout,
        )?;

        let oracle_report = compat::filter_report_by_name(&oracle.report, name_filter);
        let emaxx_report = compat::filter_report_by_name(&emaxx.report, name_filter);
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
        mode: mode.to_string(),
        selector: selector.to_string(),
        scope,
        total_files: matching_files + mismatches.len(),
        matching_files,
        mismatching_files: mismatches.len(),
        files: relative_files,
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

fn landed_compat_files(
    git_repo_root: &Path,
    test_repo_root: &Path,
    scope: Scope,
) -> Result<Vec<PathBuf>, String> {
    let subjects = advance_compat_subjects(git_repo_root)?;
    let discovered_all = compat::discover_test_files(test_repo_root, Scope::All)?;
    let resolved_all = resolve_landed_compat_files(&subjects, &discovered_all, test_repo_root)?;
    if matches!(scope, Scope::All) {
        return Ok(resolved_all);
    }

    let allowed = compat::discover_test_files(test_repo_root, scope)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    Ok(resolved_all
        .into_iter()
        .filter(|file| allowed.contains(file))
        .collect())
}

fn manifest_entries_for_file_filter(
    manifest: &CompatibilityRegressionManifest,
    file_filter: Option<&str>,
) -> Result<Vec<CompatibilityRegressionEntry>, String> {
    let entries = match file_filter {
        Some(filter) => manifest
            .files
            .iter()
            .filter(|entry| entry.path == filter)
            .cloned()
            .collect::<Vec<_>>(),
        None => manifest.files.clone(),
    };
    if file_filter.is_some() && entries.is_empty() {
        return Err(format!(
            "no tracked regression matched `{}` in {}",
            file_filter.unwrap_or_default(),
            compat::compat_path(COMPAT_REGRESSION_MANIFEST_PATH).display()
        ));
    }
    Ok(entries)
}

fn advance_compat_subjects(repo_root: &Path) -> Result<Vec<String>, String> {
    let output = Command::new("git")
        .arg("log")
        .arg("--format=%s")
        .arg(format!("--grep=^{ADVANCE_COMPAT_PREFIX}"))
        .current_dir(repo_root)
        .output()
        .map_err(|error| format!("run git log in {}: {error}", repo_root.display()))?;
    if !output.status.success() {
        return Err(format!(
            "git log failed in {}: {}",
            repo_root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let subjects = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if subjects.is_empty() {
        return Err(format!(
            "no commits matched `{ADVANCE_COMPAT_PREFIX}...` in {}",
            repo_root.display()
        ));
    }
    Ok(subjects)
}

fn canonicalize_manifest_selectors(
    manifest: &mut CompatibilityRegressionManifest,
    lock: &OracleLock,
) -> Result<(), String> {
    let entries = manifest
        .files
        .iter()
        .map(|entry| {
            Ok(CompatibilityRegressionEntry {
                path: entry.path.clone(),
                selector: compat::resolve_selector(lock, &entry.selector)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    manifest.files.clear();
    for entry in entries {
        manifest.insert(entry);
    }
    Ok(())
}

fn parse_advance_compat_subject(subject: &str) -> Option<String> {
    subject
        .strip_prefix(ADVANCE_COMPAT_PREFIX)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn resolve_landed_compat_files(
    subjects: &[String],
    discovered: &[PathBuf],
    repo_root: &Path,
) -> Result<Vec<PathBuf>, String> {
    let mut by_basename = BTreeMap::<String, Vec<PathBuf>>::new();
    let mut by_relative = BTreeMap::<String, PathBuf>::new();
    for file in discovered {
        let Some(name) = file.file_name() else {
            continue;
        };
        let basename = name.to_string_lossy().to_string();
        by_basename.entry(basename).or_default().push(file.clone());
        let relative = compat::relative_test_path(repo_root, file)?;
        by_relative.insert(relative, file.clone());
    }
    for matches in by_basename.values_mut() {
        matches.sort();
    }

    let mut seen = BTreeSet::new();
    let mut resolved = Vec::new();
    for subject in subjects {
        let Some(target) = parse_advance_compat_subject(subject) else {
            return Err(format!(
                "unexpected advance compatibility subject `{subject}`"
            ));
        };
        if !seen.insert(target.clone()) {
            continue;
        }
        resolved.push(resolve_landed_compat_target(
            &target,
            &by_relative,
            &by_basename,
            repo_root,
        )?);
    }

    Ok(resolved)
}

fn resolve_landed_compat_files_best_effort(
    subjects: &[String],
    discovered: &[PathBuf],
    repo_root: &Path,
) -> Result<(Vec<PathBuf>, Vec<String>), String> {
    let mut by_basename = BTreeMap::<String, Vec<PathBuf>>::new();
    let mut by_relative = BTreeMap::<String, PathBuf>::new();
    for file in discovered {
        let Some(name) = file.file_name() else {
            continue;
        };
        let basename = name.to_string_lossy().to_string();
        by_basename.entry(basename).or_default().push(file.clone());
        let relative = compat::relative_test_path(repo_root, file)?;
        by_relative.insert(relative, file.clone());
    }
    for matches in by_basename.values_mut() {
        matches.sort();
    }

    let mut seen = BTreeSet::new();
    let mut resolved = Vec::new();
    let mut skipped = Vec::new();
    for subject in subjects {
        let Some(target) = parse_advance_compat_subject(subject) else {
            skipped.push(format!(
                "unexpected advance compatibility subject `{subject}`"
            ));
            continue;
        };
        if !seen.insert(target.clone()) {
            continue;
        }

        match resolve_landed_compat_target(&target, &by_relative, &by_basename, repo_root) {
            Ok(file) => resolved.push(file),
            Err(error) => skipped.push(error),
        }
    }

    Ok((resolved, skipped))
}

fn resolve_landed_compat_target(
    target: &str,
    by_relative: &BTreeMap<String, PathBuf>,
    by_basename: &BTreeMap<String, Vec<PathBuf>>,
    repo_root: &Path,
) -> Result<PathBuf, String> {
    if target.contains('/') {
        let Some(file) = by_relative.get(target) else {
            return Err(format!(
                "commit target `{target}` did not match any discovered test file under {}",
                repo_root.display()
            ));
        };
        return Ok(file.clone());
    }

    match by_basename.get(target) {
        Some(matches) if matches.len() == 1 => Ok(matches[0].clone()),
        Some(matches) => {
            let choices = matches
                .iter()
                .map(|path| compat::relative_test_path(repo_root, path))
                .collect::<Result<Vec<_>, _>>()?;
            Err(format!(
                "commit target `{target}` is ambiguous; matches: {}",
                choices.join(", ")
            ))
        }
        None => Err(format!(
            "commit target `{target}` did not match any discovered test file under {}",
            repo_root.display()
        )),
    }
}

fn resolve_manifest_entry_path(
    repo_root: &Path,
    entry: &CompatibilityRegressionEntry,
) -> Result<PathBuf, String> {
    resolve_manifest_path_from_cli(repo_root, &entry.path)
}

fn resolve_manifest_path_from_cli(repo_root: &Path, path: &str) -> Result<PathBuf, String> {
    let candidate = repo_root.join(path);
    if !candidate.exists() {
        return Err(format!(
            "tracked regression `{path}` does not exist under {}",
            repo_root.display()
        ));
    }
    let files = compat::discover_test_files(repo_root, Scope::All)?;
    let filtered = compat::filter_files(&files, repo_root, Some(path))?;
    match filtered.as_slice() {
        [file] => Ok(file.clone()),
        [] => Err(format!(
            "`{path}` is not a discovered automated test file under {}",
            repo_root.display()
        )),
        _ => Err(format!("`{path}` matched multiple discovered test files")),
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

fn selected_files(
    repo_root: &Path,
    scope: Scope,
    file_filter: Option<&str>,
) -> Result<Vec<PathBuf>, String> {
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
    command.env("EMAXX_COMPAT_SELECTOR", format!("(quote {selector})"));
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
    let report =
        load_or_synthesize_report(&result_path, "oracle", relative_file, selector, &process)?;
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
    command.arg(format!("(ert-run-tests-batch-and-exit (quote {selector}))"));

    let process = run_command(command, timeout)?;
    let report =
        load_or_synthesize_report(&result_path, "emaxx", relative_file, selector, &process)?;
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
    Ok(BatchReport::load_error(
        runner,
        relative_file,
        selector,
        message,
    ))
}

fn run_command(mut command: Command, timeout: Option<Duration>) -> Result<ProcessResult, String> {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|error| format!("spawn command: {error}"))?;
    let started = Instant::now();

    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("wait for command: {error}"))?
        {
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
            child
                .kill()
                .map_err(|error| format!("kill timed out command: {error}"))?;
            let status = child
                .wait()
                .map_err(|error| format!("wait after kill: {error}"))?;
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
    let json = serde_json::to_string_pretty(value)
        .map_err(|error| format!("serialize {label}: {error}"))?;
    fs::write(path, json).map_err(|error| format!("write {}: {error}", path.display()))
}

fn ensure_emaxx_binary() -> Result<PathBuf, String> {
    let current = env::current_exe().map_err(|error| format!("current exe: {error}"))?;
    let Some(bin_dir) = current.parent() else {
        return Err(format!(
            "cannot locate binary directory for {}",
            current.display()
        ));
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
        assert_eq!(
            dir,
            PathBuf::from("/tmp/compat/test/src/buffer-tests.compat")
        );
    }

    #[test]
    fn parses_advance_compat_subjects() {
        assert_eq!(
            parse_advance_compat_subject("Advance compatibility for align-tests.el"),
            Some("align-tests.el".into())
        );
        assert_eq!(
            parse_advance_compat_subject("Advance compatibility for test/lisp/align-tests.el"),
            Some("test/lisp/align-tests.el".into())
        );
        assert_eq!(parse_advance_compat_subject("something else"), None);
        assert_eq!(
            parse_advance_compat_subject("Advance compatibility for "),
            None
        );
    }

    #[test]
    fn resolves_landed_files_by_basename_and_dedupes() {
        let repo_root = Path::new("/repo");
        let subjects = vec![
            "Advance compatibility for align-tests.el".to_string(),
            "Advance compatibility for files-x-tests.el".to_string(),
            "Advance compatibility for align-tests.el".to_string(),
        ];
        let discovered = vec![
            PathBuf::from("/repo/test/lisp/align-tests.el"),
            PathBuf::from("/repo/test/lisp/files-x-tests.el"),
        ];

        let resolved = resolve_landed_compat_files(&subjects, &discovered, repo_root).unwrap();

        assert_eq!(
            resolved,
            vec![
                PathBuf::from("/repo/test/lisp/align-tests.el"),
                PathBuf::from("/repo/test/lisp/files-x-tests.el"),
            ]
        );
    }

    #[test]
    fn resolves_landed_files_by_relative_path() {
        let repo_root = Path::new("/repo");
        let subjects = vec!["Advance compatibility for test/lisp/align-tests.el".to_string()];
        let discovered = vec![PathBuf::from("/repo/test/lisp/align-tests.el")];

        let resolved = resolve_landed_compat_files(&subjects, &discovered, repo_root).unwrap();

        assert_eq!(
            resolved,
            vec![PathBuf::from("/repo/test/lisp/align-tests.el")]
        );
    }

    #[test]
    fn rejects_missing_landed_file_targets() {
        let repo_root = Path::new("/repo");
        let subjects = vec!["Advance compatibility for align-tests.el".to_string()];
        let discovered = vec![PathBuf::from("/repo/test/lisp/files-x-tests.el")];

        let error = resolve_landed_compat_files(&subjects, &discovered, repo_root).unwrap_err();

        assert!(error.contains("align-tests.el"));
    }

    #[test]
    fn rejects_ambiguous_landed_file_targets() {
        let repo_root = Path::new("/repo");
        let subjects = vec!["Advance compatibility for dup-tests.el".to_string()];
        let discovered = vec![
            PathBuf::from("/repo/test/lisp/dup-tests.el"),
            PathBuf::from("/repo/test/src/dup-tests.el"),
        ];

        let error = resolve_landed_compat_files(&subjects, &discovered, repo_root).unwrap_err();

        assert!(error.contains("ambiguous"));
        assert!(error.contains("test/lisp/dup-tests.el"));
        assert!(error.contains("test/src/dup-tests.el"));
    }

    #[test]
    fn best_effort_landed_resolution_skips_ambiguous_targets() {
        let repo_root = Path::new("/repo");
        let subjects = vec![
            "Advance compatibility for dup-tests.el".to_string(),
            "Advance compatibility for align-tests.el".to_string(),
        ];
        let discovered = vec![
            PathBuf::from("/repo/test/lisp/dup-tests.el"),
            PathBuf::from("/repo/test/src/dup-tests.el"),
            PathBuf::from("/repo/test/lisp/align-tests.el"),
        ];

        let (resolved, skipped) =
            resolve_landed_compat_files_best_effort(&subjects, &discovered, repo_root).unwrap();

        assert_eq!(
            resolved,
            vec![PathBuf::from("/repo/test/lisp/align-tests.el")]
        );
        assert_eq!(skipped.len(), 1);
        assert!(skipped[0].contains("dup-tests.el"));
    }

    #[test]
    fn regression_manifest_rejects_duplicates() {
        let error = CompatibilityRegressionManifest {
            format_version: 1,
            files: vec![
                CompatibilityRegressionEntry {
                    path: "test/lisp/align-tests.el".into(),
                    selector: "check-all".into(),
                },
                CompatibilityRegressionEntry {
                    path: "test/lisp/align-tests.el".into(),
                    selector: "check-all".into(),
                },
            ],
        }
        .validate()
        .unwrap_err();

        assert!(error.contains("duplicate regression entry"));
    }

    #[test]
    fn regression_manifest_allows_same_file_with_different_selector() {
        CompatibilityRegressionManifest {
            format_version: 1,
            files: vec![
                CompatibilityRegressionEntry {
                    path: "test/lisp/align-tests.el".into(),
                    selector: "check-all".into(),
                },
                CompatibilityRegressionEntry {
                    path: "test/lisp/align-tests.el".into(),
                    selector: "default".into(),
                },
            ],
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn manifest_insert_dedupes_and_sorts_entries() {
        let mut manifest = CompatibilityRegressionManifest::default();
        manifest.insert(CompatibilityRegressionEntry {
            path: "test/lisp/files-x-tests.el".into(),
            selector: "check-all".into(),
        });
        manifest.insert(CompatibilityRegressionEntry {
            path: "test/lisp/align-tests.el".into(),
            selector: "check-all".into(),
        });
        manifest.insert(CompatibilityRegressionEntry {
            path: "test/lisp/align-tests.el".into(),
            selector: "check-all".into(),
        });

        assert_eq!(
            manifest.files,
            vec![
                CompatibilityRegressionEntry {
                    path: "test/lisp/align-tests.el".into(),
                    selector: "check-all".into(),
                },
                CompatibilityRegressionEntry {
                    path: "test/lisp/files-x-tests.el".into(),
                    selector: "check-all".into(),
                },
            ]
        );
    }

    #[test]
    fn manifest_file_filter_reports_missing_entry() {
        let manifest = CompatibilityRegressionManifest {
            format_version: 1,
            files: vec![CompatibilityRegressionEntry {
                path: "test/lisp/align-tests.el".into(),
                selector: "check-all".into(),
            }],
        };

        let error = manifest_entries_for_file_filter(&manifest, Some("test/lisp/files-x-tests.el"))
            .unwrap_err();

        assert!(error.contains("no tracked regression matched"));
    }
}
