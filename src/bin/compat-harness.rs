use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Args, Parser, Subcommand, ValueEnum};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use emaxx::compat::{
    self, BatchReport, FileStatus, OracleLocalConfig, OracleLock, Scope, TestStatus,
};

const ADVANCE_COMPAT_PREFIX: &str = "Advance compatibility for ";
const COMPAT_REGRESSION_MANIFEST_PATH: &str = "compat/compat_regressions.json";
const FROZEN_COMPAT_MANIFEST_PATH: &str = "compat/oracle_tests_all.txt";
const FROZEN_COMPAT_FILE_COUNT: usize = 515;
const FROZEN_COMPAT_LOAD_ERROR_COUNT: usize = 4;
const FROZEN_COMPAT_OUTCOME_COUNT: usize = 7_595;
/// sha256 of compat/oracle_tests_all.txt.  The three counts above cannot
/// detect a same-count substitution of test names, so the manifest's contents
/// are pinned too; regenerating it is a deliberate constant bump.
const FROZEN_COMPAT_MANIFEST_SHA256: &str =
    "bc1070ec0f4256c929ebf7f7254290ee88049fd1b6495252ee7849cecd7f7758";
const TARGET_OWNER_FILE: &str = ".emaxx-source-root";
const SUBJECT_LOCK_FILE: &str = ".emaxx-compat.lock";
const DEFAULT_TIMEOUT_SECONDS: u64 = 180;

struct CompatRunPlan<'a> {
    mode: &'a str,
    scope: String,
    selector: &'a str,
    files: Vec<PathBuf>,
    name_filter: Option<&'a Regex>,
    name_filter_expression: Option<&'a str>,
    artifact_root: &'a Path,
    timeout: Option<Duration>,
    subject: &'a SubjectBuild,
    provenance: &'a RunProvenance,
    frozen_manifest: Option<&'a FrozenCompatibilityManifest>,
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
    /// Replay exactly the pinned 7,595-outcome compatibility manifest.
    Frozen(FrozenArgs),
    Landed(LandedArgs),
    Regressions(RegressionArgs),
    /// Compare only Emaxx outcomes from two completed artifact directories.
    CompareSubjects(CompareSubjectsArgs),
}

#[derive(Debug, Args)]
struct FrozenArgs {
    /// Source checkout whose Emaxx binary should be built and tested.
    #[arg(long)]
    subject_root: Option<PathBuf>,
    /// Per setup and test phase.  This is recorded in summary provenance.
    #[arg(long)]
    timeout_seconds: Option<u64>,
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
    /// Per setup and test phase (default 180 seconds each).
    #[arg(long)]
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(long, value_enum, default_value = "automated")]
    scope: ScopeArg,
    #[arg(long, default_value = "default")]
    selector: String,
    #[arg(long)]
    file: Option<String>,
    /// Run the canonical sorted prefix ending with this file.
    #[arg(long, conflicts_with = "file")]
    through_file: Option<String>,
    #[arg(long)]
    name: Option<String>,
    /// Source checkout whose Emaxx binary should be built and tested.
    #[arg(long)]
    subject_root: Option<PathBuf>,
    /// Per setup and test phase.  This is recorded in summary provenance.
    #[arg(long)]
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Args)]
struct CompareSubjectsArgs {
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    candidate: PathBuf,
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
    /// Per setup and test phase (default 180 seconds each).
    #[arg(long)]
    timeout_seconds: Option<u64>,
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
    /// Per setup and test phase (default 180 seconds each).
    #[arg(long)]
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Args)]
struct RegressionAddArgs {
    /// Files to validate and record atomically.  Repeat for multiple owners.
    #[arg(long, required = true)]
    file: Vec<String>,
    #[arg(long, default_value = "check-all")]
    selector: String,
    /// Per setup and test phase (default 180 seconds each).
    #[arg(long)]
    timeout_seconds: Option<u64>,
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
    timeout_phase: Option<TimeoutPhase>,
    setup_elapsed: Duration,
    test_started: bool,
    test_elapsed: Duration,
    elapsed: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TimeoutPhase {
    Setup,
    Test,
}

impl TimeoutPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Setup => "setup",
            Self::Test => "test",
        }
    }
}

#[derive(Debug)]
struct RunnerArtifacts {
    report: BatchReport,
    process: ProcessResult,
}

#[derive(Debug)]
struct RunnerTempDirectory {
    path: PathBuf,
}

impl Drop for RunnerTempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

#[derive(Debug)]
struct IsolatedTestCheckout {
    root: PathBuf,
    checkout: PathBuf,
    commit: String,
    support_files: Vec<PathBuf>,
}

impl IsolatedTestCheckout {
    fn clone(source: &Path, commit: &str, runner: &str) -> Result<Self, String> {
        let root = unique_temp_path(&format!("checkout-{runner}"))?;
        fs::create_dir(&root).map_err(|error| {
            format!("create isolated checkout root {}: {error}", root.display())
        })?;
        let checkout = root.join("emacs");
        let support_files = isolated_test_support_inputs(source)?;
        copy_relative_files(source, &root.join("test-support"), &support_files)?;
        let isolated = Self {
            root,
            checkout,
            commit: commit.to_string(),
            support_files,
        };
        let clone = Command::new("git")
            .args([
                "-c",
                "advice.detachedHead=false",
                "clone",
                "--shared",
                "--quiet",
            ])
            .arg(source)
            .arg(&isolated.checkout)
            .status()
            .map_err(|error| format!("clone isolated GNU Emacs checkout: {error}"))?;
        if !clone.success() {
            return Err(format!(
                "clone isolated GNU Emacs checkout from {} failed",
                source.display()
            ));
        }

        isolated.restore()?;
        Ok(isolated)
    }

    fn restore(&self) -> Result<(), String> {
        let reset = Command::new("git")
            .args(["reset", "--hard", "--quiet"])
            .arg(&self.commit)
            .current_dir(&self.checkout)
            .status()
            .map_err(|error| {
                format!(
                    "reset isolated checkout {}: {error}",
                    self.checkout.display()
                )
            })?;
        if !reset.success() {
            return Err(format!(
                "reset isolated checkout {} to {} failed",
                self.checkout.display(),
                self.commit
            ));
        }

        let clean = Command::new("git")
            .args(["clean", "-ffdqx"])
            .current_dir(&self.checkout)
            .status()
            .map_err(|error| {
                format!(
                    "clean isolated checkout {}: {error}",
                    self.checkout.display()
                )
            })?;
        if !clean.success() {
            return Err(format!(
                "clean isolated checkout {} failed",
                self.checkout.display()
            ));
        }
        copy_relative_files(
            &self.root.join("test-support"),
            &self.checkout,
            &self.support_files,
        )?;
        Ok(())
    }

    fn file(&self, relative: &str) -> PathBuf {
        self.checkout.join(relative)
    }
}

impl Drop for IsolatedTestCheckout {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn isolated_test_support_inputs(repo_root: &Path) -> Result<Vec<PathBuf>, String> {
    let output = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--",
            "lisp",
            "lib-src",
            "etc/charsets",
            "etc/DOC",
        ])
        .current_dir(repo_root)
        .output()
        .map_err(|error| {
            format!(
                "list generated test-support inputs in {}: {error}",
                repo_root.display()
            )
        })?;
    if !output.status.success() {
        return Err(format!(
            "list generated test-support inputs in {} failed",
            repo_root.display()
        ));
    }

    let mut files = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| {
            String::from_utf8(path.to_vec())
                .map(PathBuf::from)
                .map_err(|error| format!("generated Lisp path is not UTF-8: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    files.retain(|path| {
        (path.starts_with("lisp") && path.extension().is_some_and(|extension| extension == "el")
            || libexec_test_helper(path)
            || generated_charset_map(path)
            || path == Path::new("etc/DOC"))
            && repo_root.join(path).is_file()
    });
    files.sort();
    Ok(files)
}

fn generated_charset_map(path: &Path) -> bool {
    path.parent() == Some(Path::new("etc/charsets"))
        && path.extension().is_some_and(|extension| extension == "map")
}

fn libexec_test_helper(path: &Path) -> bool {
    const HELPERS: &[&str] = &[
        "ctags",
        "ebrowse",
        "emacsclient",
        "etags",
        "hexl",
        "make-docfile",
        "make-fingerprint",
        "movemail",
    ];
    if path.parent() != Some(Path::new("lib-src")) {
        return false;
    }
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    HELPERS
        .iter()
        .any(|helper| name == *helper || name == format!("{helper}{}", env::consts::EXE_SUFFIX))
}

fn copy_relative_files(source: &Path, destination: &Path, files: &[PathBuf]) -> Result<(), String> {
    for relative in files {
        let source_file = source.join(relative);
        let destination_file = destination.join(relative);
        let parent = destination_file
            .parent()
            .ok_or_else(|| format!("{} has no parent", destination_file.display()))?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
        fs::copy(&source_file, &destination_file).map_err(|error| {
            format!(
                "copy isolated test-support input {} to {}: {error}",
                source_file.display(),
                destination_file.display()
            )
        })?;
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct AggregateReport {
    mode: String,
    selector: String,
    scope: String,
    total_files: usize,
    matching_files: usize,
    mismatching_files: usize,
    /// Per-TEST tallies.  A compatibility claim is a count of tests, so it is
    /// computed here rather than reconstructed by hand from the manifest.
    #[serde(default)]
    matching_outcomes: usize,
    #[serde(default)]
    mismatching_outcomes: usize,
    #[serde(default)]
    total_outcomes: usize,
    files: Vec<String>,
    mismatches: Vec<String>,
    name_filter: Option<String>,
    #[serde(default)]
    timings: Vec<FileTiming>,
    #[serde(default)]
    performance_regressions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    frozen_manifest: Option<FrozenManifestEvidence>,
    provenance: RunProvenance,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FrozenManifestEvidence {
    path: String,
    sha256: String,
    recorded_files: usize,
    executed_files: usize,
    historical_load_errors: usize,
    required_outcomes: usize,
    compared_outcomes: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FrozenCompatibilityManifest {
    path: PathBuf,
    sha256: String,
    entries: BTreeMap<String, Vec<String>>,
    historical_load_errors: Vec<String>,
}

impl FrozenCompatibilityManifest {
    fn load() -> Result<Self, String> {
        let path = compat::compat_path(FROZEN_COMPAT_MANIFEST_PATH);
        let data = fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        Self::parse(
            path,
            sha256_file(&compat::compat_path(FROZEN_COMPAT_MANIFEST_PATH))?,
            &data,
        )
    }

    fn parse(path: PathBuf, sha256: String, data: &str) -> Result<Self, String> {
        let header = Regex::new(r"^([^ ].*): discovered=([0-9]+) selected=([0-9]+)$")
            .map_err(|error| format!("compile frozen manifest header parser: {error}"))?;
        let load_error = Regex::new(r"^([^ ].*): load-error (.+)$")
            .map_err(|error| format!("compile frozen manifest load-error parser: {error}"))?;
        let mut entries = BTreeMap::<String, Vec<String>>::new();
        let mut expected_selected = BTreeMap::<String, usize>::new();
        let mut historical_load_errors = Vec::new();
        let mut current_file = None::<String>;
        let mut seen_outcomes = BTreeSet::new();

        for (index, line) in data.lines().enumerate() {
            if let Some(captures) = header.captures(line) {
                let file = captures[1].to_string();
                let selected = captures[3].parse::<usize>().map_err(|error| {
                    format!(
                        "parse selected count on {}:{}: {error}",
                        path.display(),
                        index + 1
                    )
                })?;
                if entries.insert(file.clone(), Vec::new()).is_some() {
                    return Err(format!(
                        "duplicate file `{file}` in {}:{}",
                        path.display(),
                        index + 1
                    ));
                }
                expected_selected.insert(file.clone(), selected);
                current_file = Some(file);
            } else if let Some(captures) = load_error.captures(line) {
                historical_load_errors.push(captures[1].to_string());
                current_file = None;
            } else if let Some(name) = line.strip_prefix("  ") {
                let file = current_file.as_ref().ok_or_else(|| {
                    format!(
                        "test `{name}` has no file header in {}:{}",
                        path.display(),
                        index + 1
                    )
                })?;
                if name.is_empty() || !seen_outcomes.insert((file.clone(), name.to_string())) {
                    return Err(format!(
                        "empty or duplicate outcome `{file}|{name}` in {}:{}",
                        path.display(),
                        index + 1
                    ));
                }
                entries
                    .get_mut(file)
                    .expect("current frozen manifest entry must exist")
                    .push(name.to_string());
            } else if !line.trim().is_empty() {
                return Err(format!(
                    "unrecognized frozen manifest line {}:{}: {line}",
                    path.display(),
                    index + 1
                ));
            }
        }

        for (file, expected) in expected_selected {
            let actual = entries.get(&file).map_or(0, Vec::len);
            if actual != expected {
                return Err(format!(
                    "frozen manifest `{file}` records selected={expected} but lists {actual} outcomes"
                ));
            }
        }
        let outcome_count = entries.values().map(Vec::len).sum::<usize>();
        if sha256 != FROZEN_COMPAT_MANIFEST_SHA256 {
            return Err(format!(
                "frozen compatibility manifest {} has sha256 {sha256}, expected {FROZEN_COMPAT_MANIFEST_SHA256}; \
                 regenerating the manifest requires bumping FROZEN_COMPAT_MANIFEST_SHA256",
                path.display()
            ));
        }
        if entries.len() != FROZEN_COMPAT_FILE_COUNT
            || historical_load_errors.len() != FROZEN_COMPAT_LOAD_ERROR_COUNT
            || outcome_count != FROZEN_COMPAT_OUTCOME_COUNT
        {
            return Err(format!(
                "frozen compatibility inventory drifted: files={} (expected {}), load_errors={} (expected {}), outcomes={} (expected {})",
                entries.len(),
                FROZEN_COMPAT_FILE_COUNT,
                historical_load_errors.len(),
                FROZEN_COMPAT_LOAD_ERROR_COUNT,
                outcome_count,
                FROZEN_COMPAT_OUTCOME_COUNT
            ));
        }

        Ok(Self {
            path,
            sha256,
            entries,
            historical_load_errors,
        })
    }

    fn executable_files(&self, repo_root: &Path) -> Result<Vec<PathBuf>, String> {
        self.entries
            .iter()
            .filter(|(_, names)| !names.is_empty())
            .map(|(file, _)| resolve_manifest_path_from_cli(repo_root, file))
            .collect()
    }

    fn evidence(&self, compared_outcomes: usize) -> FrozenManifestEvidence {
        FrozenManifestEvidence {
            path: FROZEN_COMPAT_MANIFEST_PATH.into(),
            sha256: self.sha256.clone(),
            recorded_files: self.entries.len(),
            executed_files: self
                .entries
                .values()
                .filter(|names| !names.is_empty())
                .count(),
            historical_load_errors: self.historical_load_errors.len(),
            required_outcomes: self.entries.values().map(Vec::len).sum(),
            compared_outcomes,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FileTiming {
    file: String,
    gnu_emacs_duration_ms: u64,
    emaxx_duration_ms: u64,
    #[serde(default)]
    gnu_emacs_setup_duration_ms: u64,
    #[serde(default)]
    emaxx_setup_duration_ms: u64,
    #[serde(default)]
    gnu_emacs_test_duration_ms: u64,
    #[serde(default)]
    emaxx_test_duration_ms: u64,
    #[serde(default)]
    gnu_emacs_timed_out: bool,
    #[serde(default)]
    emaxx_timed_out: bool,
    #[serde(default)]
    gnu_emacs_timeout_phase: Option<String>,
    #[serde(default)]
    emaxx_timeout_phase: Option<String>,
    emaxx_over_gnu_milli: Option<u64>,
    emaxx_at_least_twice_as_slow: bool,
}

#[derive(Serialize)]
struct TimedComparison<'a> {
    #[serde(flatten)]
    comparison: &'a compat::ComparisonReport,
    timing: &'a FileTiming,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RunProvenance {
    harness_source_root: String,
    harness_compiled_target_dir: String,
    harness_binary: String,
    harness_sha256: String,
    oracle_helper_sha256: String,
    subject_source_root: String,
    subject_source_sha256: String,
    subject_git_head: Option<String>,
    subject_git_dirty: Option<bool>,
    subject_target_dir: String,
    subject_profile: String,
    subject_binary: String,
    subject_sha256: String,
    oracle_binary: String,
    oracle_sha256: String,
    oracle_repo: String,
    oracle_repo_commit: String,
    #[serde(default)]
    oracle_test_support_sha256: String,
    oracle_emacs_version: String,
    oracle_system_type: String,
    oracle_native_compilation: bool,
    timeout_seconds: Option<u64>,
}

#[derive(Debug)]
struct SubjectBuild {
    source_root: PathBuf,
    target_dir: PathBuf,
    profile: String,
    binary: PathBuf,
    source_sha256: String,
    _lock: SubjectLock,
}

#[derive(Debug)]
struct SubjectLock {
    _file: fs::File,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
struct SubjectTransition {
    file: String,
    test: String,
    baseline_status: String,
    baseline_condition: Option<String>,
    candidate_status: String,
    candidate_condition: Option<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct SubjectComparison {
    baseline: String,
    candidate: String,
    incompatible: Vec<String>,
    pass_to_fail: Vec<SubjectTransition>,
    fail_to_pass: Vec<SubjectTransition>,
    changed_failures: Vec<SubjectTransition>,
    missing_from_candidate: Vec<String>,
    added_in_candidate: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NormalizedSubjectOutcome {
    status: TestStatus,
    condition: Option<String>,
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
        Commands::Frozen(args) => run_frozen_compat(args),
        Commands::Landed(args) => run_landed_compat(args),
        Commands::Regressions(args) => run_regressions(args),
        Commands::CompareSubjects(args) => compare_subject_artifacts(args),
    }
}

fn pin_oracle(args: PinArgs) -> Result<(), String> {
    let emacs_binary = compat::canonicalize_path(&args.emacs)?;
    let emacs_repo = compat::canonicalize_path(&args.repo)?;
    let runtime = compat::current_emacs_runtime(&emacs_binary)?;
    let commit = compat::current_repo_commit(&emacs_repo)?;
    let (_, dirty) = git_state(&emacs_repo)?;
    if dirty != Some(false) {
        return Err(format!(
            "refusing to pin dirty GNU Emacs checkout {}; commit identity alone would not describe the oracle sources",
            emacs_repo.display()
        ));
    }
    if runtime.repository_version.as_deref() != Some(commit.as_str()) {
        return Err(format!(
            "refusing to pin GNU Emacs binary {}: checkout is {commit}, binary reports repository version {:?}; rebuild the oracle first",
            emacs_binary.display(),
            runtime.repository_version
        ));
    }
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
        None,
    )?;
    let name_filter = compat::compile_name_filter(args.name.as_deref())?;
    let artifact_root = make_artifact_root("list")?;
    let timeout = resolve_run_timeout(args.timeout_seconds)?;
    let oracle_checkout = IsolatedTestCheckout::clone(
        &context.local.emacs_repo,
        &context.lock.emacs_repo_commit,
        "oracle",
    )?;

    for file in files {
        let relative = compat::relative_test_path(&context.local.emacs_repo, &file)?;
        let per_file_dir = per_file_artifact_dir(&artifact_root, &relative);
        oracle_checkout.restore()?;
        let oracle = run_oracle(
            &context.local,
            &oracle_checkout.checkout,
            &relative,
            &oracle_checkout.file(&relative),
            &selector,
            &per_file_dir,
            timeout,
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

/// A summary from a tree that has not passed the anti-cheat gates is not
/// evidence.  Refuse to run rather than annotate (finding 24).
fn enforce_anti_cheat_gates() -> Result<(), String> {
    eprintln!("enforcing anti-cheat gates before the run...");
    emaxx::anti_cheat::enforce_all().map_err(|violations| {
        format!(
            "anti-cheat gates failed; no measured run can be produced from this tree:\n  {}",
            violations.join("\n  ")
        )
    })
}

fn run_compat(args: RunArgs) -> Result<u8, String> {
    enforce_anti_cheat_gates()?;
    let context = load_context()?;
    let selector = compat::resolve_selector(&context.lock, &args.selector)?;
    let files = selected_files(
        &context.local.emacs_repo,
        args.scope.into(),
        args.file.as_deref(),
        args.through_file.as_deref(),
    )?;
    let timeout = resolve_run_timeout(args.timeout_seconds)?;
    let name_filter = compat::compile_name_filter(args.name.as_deref())?;
    let artifact_root = make_artifact_root("run")?;
    let subject = ensure_emaxx_binary(args.subject_root.as_deref())?;
    let provenance = collect_run_provenance(&context, &subject, timeout)?;

    run_compat_files(
        &context,
        CompatRunPlan {
            mode: "run",
            scope: format!("{:?}", args.scope),
            selector: &selector,
            files,
            name_filter: name_filter.as_ref(),
            name_filter_expression: args.name.as_deref(),
            artifact_root: &artifact_root,
            timeout,
            subject: &subject,
            provenance: &provenance,
            frozen_manifest: None,
        },
    )
}

fn run_frozen_compat(args: FrozenArgs) -> Result<u8, String> {
    enforce_anti_cheat_gates()?;
    let context = load_context()?;
    let manifest = FrozenCompatibilityManifest::load()?;
    let selector = compat::resolve_selector(&context.lock, "default")?;
    let files = manifest.executable_files(&context.local.emacs_repo)?;
    let timeout = resolve_run_timeout(args.timeout_seconds)?;
    let artifact_root = make_artifact_root("frozen-7595")?;
    let subject = ensure_emaxx_binary(args.subject_root.as_deref())?;
    let provenance = collect_run_provenance(&context, &subject, timeout)?;

    run_compat_files(
        &context,
        CompatRunPlan {
            mode: "frozen-7595",
            scope: "Frozen7595".into(),
            selector: &selector,
            files,
            name_filter: None,
            name_filter_expression: None,
            artifact_root: &artifact_root,
            timeout,
            subject: &subject,
            provenance: &provenance,
            frozen_manifest: Some(&manifest),
        },
    )
}

fn compare_subject_artifacts(args: CompareSubjectsArgs) -> Result<u8, String> {
    let baseline_root = compat::canonicalize_path(&args.baseline)?;
    let candidate_root = compat::canonicalize_path(&args.candidate)?;
    let baseline_summary = load_aggregate_report(&baseline_root)?;
    let candidate_summary = load_aggregate_report(&candidate_root)?;
    let baseline = load_subject_outcomes(&baseline_root, &baseline_summary)?;
    let candidate = load_subject_outcomes(&candidate_root, &candidate_summary)?;
    let comparison = compare_subject_outcomes(
        baseline_root.display().to_string(),
        candidate_root.display().to_string(),
        &baseline_summary,
        &candidate_summary,
        &baseline,
        &candidate,
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&comparison)
            .map_err(|error| format!("serialize subject comparison: {error}"))?
    );
    if comparison.incompatible.is_empty()
        && comparison.pass_to_fail.is_empty()
        && comparison.missing_from_candidate.is_empty()
        && comparison.added_in_candidate.is_empty()
    {
        Ok(0)
    } else {
        Ok(1)
    }
}

fn load_aggregate_report(artifact_root: &Path) -> Result<AggregateReport, String> {
    let path = artifact_root.join("summary.json");
    let data = fs::read_to_string(&path)
        .map_err(|error| format!("read required artifact summary {}: {error}", path.display()))?;
    serde_json::from_str(&data).map_err(|error| {
        format!(
            "parse required artifact summary {}: {error}",
            path.display()
        )
    })
}

fn load_subject_outcomes(
    artifact_root: &Path,
    summary: &AggregateReport,
) -> Result<BTreeMap<String, NormalizedSubjectOutcome>, String> {
    let mut outcomes = BTreeMap::new();
    let mut reports = 0usize;
    for entry in WalkDir::new(artifact_root) {
        let entry = entry.map_err(|error| {
            format!(
                "walk subject artifacts under {}: {error}",
                artifact_root.display()
            )
        })?;
        if !entry.file_type().is_file() || entry.file_name() != "emaxx.json" {
            continue;
        }
        reports += 1;
        let data = fs::read_to_string(entry.path())
            .map_err(|error| format!("read {}: {error}", entry.path().display()))?;
        let report: BatchReport = serde_json::from_str(&data)
            .map_err(|error| format!("parse {}: {error}", entry.path().display()))?;
        if !report_selector_matches_summary(&report.selector, &summary.selector) {
            return Err(format!(
                "Emaxx report {} used selector `{}` but summary records `{}`",
                entry.path().display(),
                report.selector,
                summary.selector
            ));
        }
        let file_key = format!("{}|<file-load>", report.file);
        if outcomes
            .insert(
                file_key.clone(),
                NormalizedSubjectOutcome {
                    status: if report.file_status == FileStatus::Loaded {
                        TestStatus::Passed
                    } else {
                        TestStatus::Failed
                    },
                    condition: report.file_error,
                },
            )
            .is_some()
        {
            return Err(format!(
                "duplicate Emaxx outcome `{file_key}` under {}",
                artifact_root.display()
            ));
        }
        for result in report.results {
            let key = format!("{}|{}", report.file, result.name);
            if outcomes
                .insert(
                    key.clone(),
                    NormalizedSubjectOutcome {
                        status: result.status,
                        condition: result.condition_type,
                    },
                )
                .is_some()
            {
                return Err(format!(
                    "duplicate Emaxx outcome `{key}` under {}",
                    artifact_root.display()
                ));
            }
        }
    }
    if reports == 0 {
        return Err(format!(
            "no emaxx.json reports found under {}",
            artifact_root.display()
        ));
    }
    let reported_files = outcomes
        .keys()
        .filter_map(|key| key.strip_suffix("|<file-load>"))
        .collect::<BTreeSet<_>>();
    let expected_files = summary
        .files
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if reported_files != expected_files {
        return Err(format!(
            "Emaxx reports under {} do not exactly cover summary file list: {}",
            artifact_root.display(),
            format_name_diff_sets(&expected_files, &reported_files)
        ));
    }
    if reports != summary.total_files {
        return Err(format!(
            "found {reports} Emaxx reports under {} but summary records {} files",
            artifact_root.display(),
            summary.total_files
        ));
    }
    Ok(outcomes)
}

fn report_selector_matches_summary(report_selector: &str, summary_selector: &str) -> bool {
    report_selector == summary_selector || report_selector == format!("'{summary_selector}")
}

fn format_name_diff_sets(expected: &BTreeSet<&str>, actual: &BTreeSet<&str>) -> String {
    let missing = expected.difference(actual).copied().collect::<Vec<_>>();
    let extra = actual.difference(expected).copied().collect::<Vec<_>>();
    format!("missing={missing:?}, extra={extra:?}")
}

fn compare_subject_outcomes(
    baseline_name: String,
    candidate_name: String,
    baseline_summary: &AggregateReport,
    candidate_summary: &AggregateReport,
    baseline: &BTreeMap<String, NormalizedSubjectOutcome>,
    candidate: &BTreeMap<String, NormalizedSubjectOutcome>,
) -> SubjectComparison {
    let mut comparison = SubjectComparison {
        baseline: baseline_name,
        candidate: candidate_name,
        incompatible: artifact_incompatibilities(baseline_summary, candidate_summary),
        pass_to_fail: Vec::new(),
        fail_to_pass: Vec::new(),
        changed_failures: Vec::new(),
        missing_from_candidate: Vec::new(),
        added_in_candidate: Vec::new(),
    };
    for (key, before) in baseline {
        let Some(after) = candidate.get(key) else {
            comparison.missing_from_candidate.push(key.clone());
            continue;
        };
        let transition = || subject_transition(key, before, after);
        match (&before.status, &after.status) {
            (TestStatus::Passed, status) if *status != TestStatus::Passed => {
                comparison.pass_to_fail.push(transition())
            }
            (status, TestStatus::Passed) if *status != TestStatus::Passed => {
                comparison.fail_to_pass.push(transition())
            }
            (before_status, after_status)
                if *before_status != TestStatus::Passed
                    && *after_status != TestStatus::Passed
                    && (before_status != after_status || before.condition != after.condition) =>
            {
                comparison.changed_failures.push(transition())
            }
            _ => {}
        }
    }
    for key in candidate.keys() {
        if !baseline.contains_key(key) {
            comparison.added_in_candidate.push(key.clone());
        }
    }
    comparison
}

fn artifact_incompatibilities(
    baseline: &AggregateReport,
    candidate: &AggregateReport,
) -> Vec<String> {
    let mut issues = Vec::new();
    macro_rules! require_equal {
        ($label:literal, $left:expr, $right:expr) => {
            if $left != $right {
                issues.push(format!(
                    "{} differs: baseline={:?}, candidate={:?}",
                    $label, $left, $right
                ));
            }
        };
    }
    require_equal!("selector", baseline.selector, candidate.selector);
    require_equal!("files", baseline.files, candidate.files);
    require_equal!("name_filter", baseline.name_filter, candidate.name_filter);
    require_equal!("total_files", baseline.total_files, candidate.total_files);
    require_equal!("scope", baseline.scope, candidate.scope);
    require_equal!("mode", baseline.mode, candidate.mode);
    require_equal!(
        "frozen_manifest",
        baseline.frozen_manifest,
        candidate.frozen_manifest
    );
    require_equal!(
        "provenance.harness_sha256",
        baseline.provenance.harness_sha256,
        candidate.provenance.harness_sha256
    );
    require_equal!(
        "provenance.oracle_helper_sha256",
        baseline.provenance.oracle_helper_sha256,
        candidate.provenance.oracle_helper_sha256
    );
    require_equal!(
        "provenance.subject_profile",
        baseline.provenance.subject_profile,
        candidate.provenance.subject_profile
    );
    require_equal!(
        "provenance.oracle_sha256",
        baseline.provenance.oracle_sha256,
        candidate.provenance.oracle_sha256
    );
    require_equal!(
        "provenance.oracle_repo_commit",
        baseline.provenance.oracle_repo_commit,
        candidate.provenance.oracle_repo_commit
    );
    require_equal!(
        "provenance.oracle_test_support_sha256",
        baseline.provenance.oracle_test_support_sha256,
        candidate.provenance.oracle_test_support_sha256
    );
    require_equal!(
        "provenance.oracle_emacs_version",
        baseline.provenance.oracle_emacs_version,
        candidate.provenance.oracle_emacs_version
    );
    require_equal!(
        "provenance.oracle_system_type",
        baseline.provenance.oracle_system_type,
        candidate.provenance.oracle_system_type
    );
    require_equal!(
        "provenance.oracle_native_compilation",
        baseline.provenance.oracle_native_compilation,
        candidate.provenance.oracle_native_compilation
    );
    require_equal!(
        "provenance.timeout_seconds",
        baseline.provenance.timeout_seconds,
        candidate.provenance.timeout_seconds
    );
    issues
}

fn subject_transition(
    key: &str,
    baseline: &NormalizedSubjectOutcome,
    candidate: &NormalizedSubjectOutcome,
) -> SubjectTransition {
    let (file, test) = key.split_once('|').unwrap_or((key, ""));
    SubjectTransition {
        file: file.to_string(),
        test: test.to_string(),
        baseline_status: test_status_name(&baseline.status).into(),
        baseline_condition: baseline.condition.clone(),
        candidate_status: test_status_name(&candidate.status).into(),
        candidate_condition: candidate.condition.clone(),
    }
}

fn test_status_name(status: &TestStatus) -> &'static str {
    match status {
        TestStatus::Passed => "passed",
        TestStatus::Failed => "failed",
        TestStatus::Skipped => "skipped",
    }
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
    let timeout = resolve_run_timeout(args.timeout_seconds)?;
    let name_filter = compat::compile_name_filter(args.name.as_deref())?;
    let artifact_root = make_artifact_root("landed")?;
    let subject = ensure_emaxx_binary(None)?;
    let provenance = collect_run_provenance(&context, &subject, timeout)?;

    run_compat_files(
        &context,
        CompatRunPlan {
            mode: "landed",
            scope: format!("{:?}", args.scope),
            selector: &selector,
            files,
            name_filter: name_filter.as_ref(),
            name_filter_expression: args.name.as_deref(),
            artifact_root: &artifact_root,
            timeout,
            subject: &subject,
            provenance: &provenance,
            frozen_manifest: None,
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
    let timeout = resolve_run_timeout(args.timeout_seconds)?;
    let artifact_root = make_artifact_root("regressions")?;
    let subject = ensure_emaxx_binary(None)?;
    let provenance = collect_run_provenance(&context, &subject, timeout)?;
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
                name_filter_expression: args.name.as_deref(),
                artifact_root: &artifact_root,
                timeout,
                subject: &subject,
                provenance: &provenance,
                frozen_manifest: None,
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
    let files = args
        .file
        .iter()
        .map(|file| resolve_manifest_path_from_cli(&context.local.emacs_repo, file))
        .collect::<Result<Vec<_>, _>>()?;
    let timeout = resolve_run_timeout(args.timeout_seconds)?;
    let artifact_root = make_artifact_root("regression-add")?;
    let subject = ensure_emaxx_binary(None)?;
    let provenance = collect_run_provenance(&context, &subject, timeout)?;

    let status = run_compat_files(
        &context,
        CompatRunPlan {
            mode: "regression-add",
            scope: "TrackedRegressions".into(),
            selector: &selector,
            files: files.clone(),
            name_filter: None,
            name_filter_expression: None,
            artifact_root: &artifact_root,
            timeout,
            subject: &subject,
            provenance: &provenance,
            frozen_manifest: None,
        },
    )?;
    if status != 0 {
        return Err(
            "refusing to record requested files because oracle and emaxx do not match yet".into(),
        );
    }

    let mut manifest = CompatibilityRegressionManifest::load_or_default()?;
    canonicalize_manifest_selectors(&mut manifest, &context.lock)?;
    let relative_files = files
        .iter()
        .map(|file| compat::relative_test_path(&context.local.emacs_repo, file))
        .collect::<Result<Vec<_>, _>>()?;
    for path in &relative_files {
        manifest.insert(CompatibilityRegressionEntry {
            path: path.clone(),
            selector: selector.clone(),
        });
    }
    manifest.save()?;
    for path in relative_files {
        println!(
            "Recorded {} in {}",
            path,
            compat::compat_path(COMPAT_REGRESSION_MANIFEST_PATH).display()
        );
    }
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
        name_filter_expression,
        artifact_root,
        timeout,
        subject,
        provenance,
        frozen_manifest,
    } = plan;
    let mut matching_files = 0usize;
    let mut matching_outcomes = 0usize;
    let mut mismatching_outcomes = 0usize;
    let mut mismatches = Vec::new();
    let mut timings = Vec::new();
    let mut performance_regressions = Vec::new();
    let mut relative_files = Vec::new();
    let mut compared_outcomes = 0usize;
    let oracle_checkout = IsolatedTestCheckout::clone(
        &context.local.emacs_repo,
        &context.lock.emacs_repo_commit,
        "oracle",
    )?;
    let emaxx_checkout = IsolatedTestCheckout::clone(
        &context.local.emacs_repo,
        &context.lock.emacs_repo_commit,
        "emaxx",
    )?;

    for file in files {
        let relative = compat::relative_test_path(&context.local.emacs_repo, &file)?;
        relative_files.push(relative.clone());
        let per_file_dir = per_file_artifact_dir(artifact_root, &relative);
        fs::create_dir_all(&per_file_dir)
            .map_err(|error| format!("create {}: {error}", per_file_dir.display()))?;

        oracle_checkout.restore()?;
        let oracle = run_oracle(
            &context.local,
            &oracle_checkout.checkout,
            &relative,
            &oracle_checkout.file(&relative),
            selector,
            &per_file_dir,
            timeout,
        )?;
        emaxx_checkout.restore()?;
        let emaxx_file = emaxx_checkout.file(&relative);
        let emaxx = run_emaxx(EmaxxRun {
            binary: &subject.binary,
            load_path_repo: &context.local.emacs_repo,
            test_repo: &emaxx_checkout.checkout,
            relative_file: &relative,
            file: &emaxx_file,
            selector,
            artifact_dir: &per_file_dir,
            timeout,
        })?;

        let (oracle_report, emaxx_report) = if let Some(manifest) = frozen_manifest {
            let required_names = manifest.entries.get(&relative).ok_or_else(|| {
                format!("frozen compatibility manifest has no entry for `{relative}`")
            })?;
            let required_names = required_names.iter().cloned().collect::<BTreeSet<_>>();
            let oracle_report =
                compat::filter_report_by_exact_names(&oracle.report, &required_names);
            let emaxx_report = compat::filter_report_by_exact_names(&emaxx.report, &required_names);
            let oracle_results = oracle_report
                .results
                .iter()
                .map(|result| result.name.as_str())
                .collect::<BTreeSet<_>>();
            let emaxx_results = emaxx_report
                .results
                .iter()
                .map(|result| result.name.as_str())
                .collect::<BTreeSet<_>>();
            let missing_oracle = required_names
                .iter()
                .filter(|name| !oracle_results.contains(name.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            let missing_emaxx = required_names
                .iter()
                .filter(|name| !emaxx_results.contains(name.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            if !missing_oracle.is_empty() || !missing_emaxx.is_empty() {
                return Err(format!(
                    "frozen outcome coverage failed for `{relative}`: missing from GNU Emacs {missing_oracle:?}; missing from Emaxx {missing_emaxx:?}"
                ));
            }
            compared_outcomes += required_names.len();
            (oracle_report, emaxx_report)
        } else {
            (
                compat::filter_report_by_name(&oracle.report, name_filter),
                compat::filter_report_by_name(&emaxx.report, name_filter),
            )
        };
        // Erase only environmental variance from failure messages before
        // equality: each runner's isolated checkout root and the shared
        // temp directory.  Anything else that differs is a real divergence.
        let oracle_root = oracle_checkout.checkout.display().to_string();
        let emaxx_root = emaxx_checkout.checkout.display().to_string();
        let temp_root = std::env::temp_dir().display().to_string();
        let normalize = move |text: &str| {
            text.replace(&oracle_root, "<checkout>")
                .replace(&emaxx_root, "<checkout>")
                .replace(&temp_root, "<tmp>")
        };
        let mut comparison =
            compat::compare_reports_normalized(&oracle_report, &emaxx_report, &normalize);
        invalidate_timed_out_comparison(&mut comparison, "GNU Emacs", &oracle.process);
        invalidate_timed_out_comparison(&mut comparison, "Emaxx", &emaxx.process);
        let timing = compare_runner_timings(&relative, &oracle.process, &emaxx.process);
        write_json(
            &per_file_dir.join("comparison.json"),
            &TimedComparison {
                comparison: &comparison,
                timing: &timing,
            },
            "comparison report",
        )?;
        write_raw_log(&per_file_dir.join("oracle.log"), &oracle.process)?;
        write_raw_log(&per_file_dir.join("emaxx.log"), &emaxx.process)?;

        matching_outcomes += comparison.matching_outcomes;
        mismatching_outcomes += comparison.mismatching_outcomes;
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
        if timing.emaxx_at_least_twice_as_slow {
            performance_regressions.push(relative.clone());
            println!(
                "SLOW {} gnu_test={}ms emaxx_test={}ms ratio={} setup_gnu={}ms setup_emaxx={}ms",
                relative,
                timing.gnu_emacs_test_duration_ms,
                timing.emaxx_test_duration_ms,
                format_ratio(timing.emaxx_over_gnu_milli),
                timing.gnu_emacs_setup_duration_ms,
                timing.emaxx_setup_duration_ms,
            );
        }
        timings.push(timing);
    }

    let aggregate = AggregateReport {
        mode: mode.to_string(),
        selector: selector.to_string(),
        scope,
        total_files: matching_files + mismatches.len(),
        matching_files,
        mismatching_files: mismatches.len(),
        matching_outcomes,
        mismatching_outcomes,
        total_outcomes: matching_outcomes + mismatching_outcomes,
        files: relative_files,
        mismatches,
        name_filter: name_filter_expression.map(ToOwned::to_owned),
        timings,
        performance_regressions,
        frozen_manifest: frozen_manifest.map(|manifest| manifest.evidence(compared_outcomes)),
        provenance: provenance.clone(),
    };
    if frozen_manifest.is_some() && compared_outcomes != FROZEN_COMPAT_OUTCOME_COUNT {
        return Err(format!(
            "frozen replay compared {compared_outcomes} outcomes; expected {FROZEN_COMPAT_OUTCOME_COUNT}"
        ));
    }
    println!(
        "TESTS {}/{} matching ({} mismatching) across {} files",
        aggregate.matching_outcomes,
        aggregate.total_outcomes,
        aggregate.mismatching_outcomes,
        aggregate.total_files,
    );
    verify_run_inputs_unchanged(provenance)?;
    write_json(
        &artifact_root.join("summary.json"),
        &aggregate,
        "aggregate summary",
    )?;

    Ok(compatibility_exit_status(&aggregate))
}

fn compatibility_exit_status(aggregate: &AggregateReport) -> u8 {
    u8::from(aggregate.mismatching_files != 0)
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn invalidate_timed_out_comparison(
    comparison: &mut compat::ComparisonReport,
    runner: &str,
    process: &ProcessResult,
) {
    let Some(phase) = process.timeout_phase else {
        return;
    };
    comparison.matches = false;
    comparison.issues.push(compat::ComparisonIssue {
        kind: "timeout".into(),
        detail: format!(
            "{runner} exceeded the {} timeout after {}ms in that phase; the result is incomplete",
            phase.as_str(),
            match phase {
                TimeoutPhase::Setup => duration_millis(process.setup_elapsed),
                TimeoutPhase::Test => duration_millis(process.test_elapsed),
            }
        ),
    });
}

fn compare_runner_timings(
    file: &str,
    gnu_emacs: &ProcessResult,
    emaxx: &ProcessResult,
) -> FileTiming {
    let gnu_nanos = gnu_emacs.test_elapsed.as_nanos();
    let emaxx_nanos = emaxx.test_elapsed.as_nanos();
    let timings_are_comparable =
        gnu_emacs.test_started && emaxx.test_started && !gnu_emacs.timed_out && !emaxx.timed_out;
    let emaxx_over_gnu_milli = timings_are_comparable.then(|| {
        if gnu_nanos == 0 {
            u64::MAX
        } else {
            emaxx_nanos
                .saturating_mul(1_000)
                .checked_div(gnu_nanos)
                .unwrap_or(u128::MAX)
                .min(u64::MAX as u128) as u64
        }
    });
    // A killed process is a censored observation, not a performance sample.
    // Its correctness comparison is marked incomplete above.
    let emaxx_at_least_twice_as_slow = timings_are_comparable
        && if gnu_nanos == 0 {
            emaxx_nanos > 0
        } else {
            emaxx_nanos >= gnu_nanos.saturating_mul(2)
        };
    FileTiming {
        file: file.to_string(),
        gnu_emacs_duration_ms: duration_millis(gnu_emacs.elapsed),
        emaxx_duration_ms: duration_millis(emaxx.elapsed),
        gnu_emacs_setup_duration_ms: duration_millis(gnu_emacs.setup_elapsed),
        emaxx_setup_duration_ms: duration_millis(emaxx.setup_elapsed),
        gnu_emacs_test_duration_ms: duration_millis(gnu_emacs.test_elapsed),
        emaxx_test_duration_ms: duration_millis(emaxx.test_elapsed),
        gnu_emacs_timed_out: gnu_emacs.timed_out,
        emaxx_timed_out: emaxx.timed_out,
        gnu_emacs_timeout_phase: gnu_emacs
            .timeout_phase
            .map(TimeoutPhase::as_str)
            .map(str::to_string),
        emaxx_timeout_phase: emaxx
            .timeout_phase
            .map(TimeoutPhase::as_str)
            .map(str::to_string),
        emaxx_over_gnu_milli,
        emaxx_at_least_twice_as_slow,
    }
}

fn format_ratio(ratio_milli: Option<u64>) -> String {
    match ratio_milli {
        Some(ratio) => format!("{}.{:03}x", ratio / 1_000, ratio % 1_000),
        None => "infinite".to_string(),
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
    let (head, dirty) = git_state(&local.emacs_repo)?;
    if head.as_deref() != Some(lock.emacs_repo_commit.as_str()) || dirty != Some(false) {
        return Err(format!(
            "GNU Emacs oracle checkout must be clean at pinned commit {}: found head={head:?}, dirty={dirty:?}",
            lock.emacs_repo_commit
        ));
    }
    Ok(Context { lock, local })
}

fn selected_files(
    repo_root: &Path,
    scope: Scope,
    file_filter: Option<&str>,
    through_file: Option<&str>,
) -> Result<Vec<PathBuf>, String> {
    let files = compat::discover_test_files(repo_root, scope)?;
    let mut filtered = compat::filter_files(&files, repo_root, file_filter)?;
    if file_filter.is_some() && filtered.is_empty() {
        return Err(format!(
            "no test file matched `{}` under {}",
            file_filter.unwrap_or_default(),
            repo_root.display()
        ));
    }
    truncate_files_through(&mut filtered, repo_root, through_file)?;
    Ok(filtered)
}

fn truncate_files_through(
    files: &mut Vec<PathBuf>,
    repo_root: &Path,
    through_file: Option<&str>,
) -> Result<(), String> {
    let Some(through_file) = through_file else {
        return Ok(());
    };
    let Some(index) = files.iter().position(|file| {
        compat::relative_test_path(repo_root, file).as_deref() == Ok(through_file)
    }) else {
        return Err(format!(
            "no prefix endpoint matched `{through_file}` under {}",
            repo_root.display()
        ));
    };
    files.truncate(index + 1);
    Ok(())
}

fn resolve_run_timeout(timeout_seconds: Option<u64>) -> Result<Option<Duration>, String> {
    match timeout_seconds {
        Some(0) => Err("--timeout-seconds must be greater than zero".into()),
        Some(seconds) => Ok(Some(Duration::from_secs(seconds))),
        None => {
            Ok(compat::resolve_timeout()?.or(Some(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS))))
        }
    }
}

fn collect_run_provenance(
    context: &Context,
    subject: &SubjectBuild,
    timeout: Option<Duration>,
) -> Result<RunProvenance, String> {
    let harness_binary = env::current_exe().map_err(|error| format!("current exe: {error}"))?;
    let harness_source_root = compat::canonicalize_path(&compat::project_root())?;
    let harness_compiled_target_dir = compat::canonicalize_path(&compiled_target_dir()?)?;
    let (subject_git_head, subject_git_dirty) = git_state(&subject.source_root)?;
    let oracle_runtime = compat::current_emacs_runtime(&context.local.emacs_binary)?;
    Ok(RunProvenance {
        harness_source_root: harness_source_root.display().to_string(),
        harness_compiled_target_dir: harness_compiled_target_dir.display().to_string(),
        harness_binary: harness_binary.display().to_string(),
        harness_sha256: sha256_file(&harness_binary)?,
        oracle_helper_sha256: sha256_file(&compat::oracle_helper_path())?,
        subject_source_root: subject.source_root.display().to_string(),
        subject_source_sha256: subject.source_sha256.clone(),
        subject_git_head,
        subject_git_dirty,
        subject_target_dir: subject.target_dir.display().to_string(),
        subject_profile: subject.profile.clone(),
        subject_binary: subject.binary.display().to_string(),
        subject_sha256: sha256_file(&subject.binary)?,
        oracle_binary: context.local.emacs_binary.display().to_string(),
        oracle_sha256: sha256_file(&context.local.emacs_binary)?,
        oracle_repo: context.local.emacs_repo.display().to_string(),
        oracle_repo_commit: context.lock.emacs_repo_commit.clone(),
        oracle_test_support_sha256: test_support_fingerprint(&context.local.emacs_repo)?,
        oracle_emacs_version: context.lock.emacs_version.clone(),
        oracle_system_type: oracle_runtime.system_type,
        oracle_native_compilation: oracle_runtime.native_compilation,
        timeout_seconds: timeout.map(|duration| duration.as_secs()),
    })
}

fn verify_run_inputs_unchanged(provenance: &RunProvenance) -> Result<(), String> {
    let checks = [
        (
            "compatibility harness binary",
            Path::new(&provenance.harness_binary),
            &provenance.harness_sha256,
        ),
        (
            "oracle helper",
            &compat::oracle_helper_path(),
            &provenance.oracle_helper_sha256,
        ),
        (
            "Emaxx subject binary",
            Path::new(&provenance.subject_binary),
            &provenance.subject_sha256,
        ),
        (
            "GNU Emacs oracle binary",
            Path::new(&provenance.oracle_binary),
            &provenance.oracle_sha256,
        ),
    ];
    for (label, path, expected) in checks {
        let actual = sha256_file(path)?;
        if &actual != expected {
            return Err(format!(
                "{label} changed during compatibility run: {} was {expected}, now {actual}; refusing to write a valid summary",
                path.display()
            ));
        }
    }

    let source_root = Path::new(&provenance.subject_source_root);
    let actual_source = subject_source_fingerprint(source_root)?;
    if actual_source != provenance.subject_source_sha256 {
        return Err(format!(
            "Emaxx subject sources changed during compatibility run: {} was {}, now {}; refusing to write a valid summary",
            source_root.display(),
            provenance.subject_source_sha256,
            actual_source
        ));
    }

    let oracle_repo = Path::new(&provenance.oracle_repo);
    let (head, dirty) = git_state(oracle_repo)?;
    if head.as_deref() != Some(provenance.oracle_repo_commit.as_str()) || dirty != Some(false) {
        return Err(format!(
            "GNU Emacs oracle checkout changed during compatibility run: expected clean {}, found head={head:?}, dirty={dirty:?}; refusing to write a valid summary",
            provenance.oracle_repo_commit
        ));
    }
    let test_support_sha256 = test_support_fingerprint(oracle_repo)?;
    if test_support_sha256 != provenance.oracle_test_support_sha256 {
        return Err(format!(
            "GNU Emacs generated test-support inputs changed during compatibility run: expected {}, found {}; refusing to write a valid summary",
            provenance.oracle_test_support_sha256, test_support_sha256
        ));
    }
    Ok(())
}

/// Everything the oracle's answers depend on that git cannot see.
///
/// The copied support inputs are `.el' only, but GNU resolves `lisp/**/*.elc'
/// from its own tree at run time, so those compiled files are what the oracle
/// actually executes.  They are gitignored, which means `git status' cannot
/// detect an edit to one -- a weakened `subr.elc' would move every oracle
/// result invisibly.  Hash them here even though they are not copied.
fn fingerprint_inputs(repo_root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = isolated_test_support_inputs(repo_root)?;
    let mut stack = vec![repo_root.join("lisp")];
    while let Some(directory) = stack.pop() {
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|extension| extension == "elc")
                && let Ok(relative) = path.strip_prefix(repo_root)
            {
                files.push(relative.to_path_buf());
            }
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn test_support_fingerprint(repo_root: &Path) -> Result<String, String> {
    let mut hasher = Sha256::new();
    for relative in fingerprint_inputs(repo_root)? {
        let path = relative.to_string_lossy();
        hasher.update((path.len() as u64).to_le_bytes());
        hasher.update(path.as_bytes());
        let mut file = fs::File::open(repo_root.join(&relative))
            .map_err(|error| format!("open {} for hashing: {error}", relative.display()))?;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .map_err(|error| format!("read {} for hashing: {error}", relative.display()))?;
            if read == 0 {
                break;
            }
            hasher.update((read as u64).to_le_bytes());
            hasher.update(&buffer[..read]);
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path)
        .map_err(|error| format!("open {} for hashing: {error}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("read {} for hashing: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn git_state(root: &Path) -> Result<(Option<String>, Option<bool>), String> {
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .map_err(|error| format!("inspect Git HEAD in {}: {error}", root.display()))?;
    if !head.status.success() {
        return Ok((None, None));
    }
    let head = String::from_utf8_lossy(&head.stdout).trim().to_string();
    let status = Command::new("git")
        .args(["status", "--porcelain=v1", "--untracked-files=normal"])
        .current_dir(root)
        .output()
        .map_err(|error| format!("inspect Git state in {}: {error}", root.display()))?;
    if !status.status.success() {
        return Err(format!("git status failed in {}", root.display()));
    }
    Ok((Some(head), Some(!status.stdout.is_empty())))
}

fn make_artifact_root(prefix: &str) -> Result<PathBuf, String> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("clock error: {error}"))?
        .as_nanos();
    let base = compat::project_root().join("target").join("compat");
    fs::create_dir_all(&base).map_err(|error| format!("create {}: {error}", base.display()))?;
    let root = base.join(format!("{prefix}-{timestamp}-{}", std::process::id()));
    fs::create_dir(&root).map_err(|error| {
        format!(
            "create unique compatibility artifact directory {}: {error}",
            root.display()
        )
    })?;
    println!("Artifacts: {}", root.display());
    Ok(root)
}

fn unique_temp_path(label: &str) -> Result<PathBuf, String> {
    Ok(env::temp_dir().join(format!(
        "emaxx-compat-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("clock error: {error}"))?
            .as_nanos()
    )))
}

fn per_file_artifact_dir(artifact_root: &Path, relative: &str) -> PathBuf {
    artifact_root.join(relative).with_extension("compat")
}

fn configure_isolated_temp_directory(
    command: &mut Command,
    runner: &str,
) -> Result<RunnerTempDirectory, String> {
    let temp_directory = unique_temp_path(runner)?;
    fs::create_dir(&temp_directory)
        .map_err(|error| format!("create {}: {error}", temp_directory.display()))?;
    // Keep each side independent of the developer's shared temp directory
    // and of artifacts left by a crashed peer run.  Cover Unix and Windows
    // conventions; unused variables are harmless on either platform.
    for variable in ["TMPDIR", "TMP", "TEMP"] {
        command.env(variable, &temp_directory);
    }
    Ok(RunnerTempDirectory {
        path: temp_directory,
    })
}

fn configure_isolated_source_directory(
    command: &mut Command,
    repo_root: &Path,
) -> Result<(), String> {
    let mut directory = repo_root.display().to_string();
    if !directory.ends_with(std::path::MAIN_SEPARATOR) {
        directory.push(std::path::MAIN_SEPARATOR);
    }
    let literal = serde_json::to_string(&directory)
        .map_err(|error| format!("encode isolated source-directory: {error}"))?;
    // GNU's dumped `source-directory' points at the checkout that built the
    // oracle executable.  Override it before loading a test so fixtures under
    // test/data are resolved inside this run's clean checkout instead.
    command.arg("--eval");
    command.arg(format!("(setq source-directory {literal})"));
    Ok(())
}

fn configure_loaded_marker(command: &mut Command, path: &Path) -> Result<(), String> {
    let literal = serde_json::to_string(&path.display().to_string())
        .map_err(|error| format!("encode loaded-marker path: {error}"))?;
    command.arg("--eval");
    command.arg(format!("(with-temp-file {literal} (insert \"loaded\\n\"))"));
    Ok(())
}

fn run_oracle(
    local: &OracleLocalConfig,
    repo_root: &Path,
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
    let test_directory = repo_root.join("test");
    let mut command = Command::new(&local.emacs_binary);
    compat::configure_upstream_like_env(&mut command, &test_directory);
    let _temp_directory = configure_isolated_temp_directory(&mut command, "oracle")?;
    command.env(compat::BATCH_RESULT_FILE_ENV, &result_path);
    command.env("EMAXX_COMPAT_RELATIVE_FILE", relative_file);
    command.env("EMAXX_COMPAT_SELECTOR", format!("(quote {selector})"));
    command.env("EMAXX_COMPAT_RUNNER", "oracle");
    command.arg("--no-init-file");
    command.arg("--no-site-file");
    command.arg("--no-site-lisp");
    command.arg("--batch");
    configure_isolated_source_directory(&mut command, repo_root)?;
    command.arg("-L");
    command.arg(&test_directory);
    command.arg("-l");
    command.arg("ert");
    command.arg("-l");
    command.arg(&helper_path);
    command.arg("-l");
    command.arg(file);
    let loaded_marker = per_file_dir.join("oracle.loaded");
    configure_loaded_marker(&mut command, &loaded_marker)?;
    command.arg("--eval");
    command.arg(format!("(emaxx-compat-run (quote {selector}))"));

    let process = run_command(command, timeout, &loaded_marker, Some(&result_path))?;
    let report =
        load_or_synthesize_report(&result_path, "oracle", relative_file, selector, &process)?;
    Ok(RunnerArtifacts { report, process })
}

struct EmaxxRun<'a> {
    binary: &'a Path,
    load_path_repo: &'a Path,
    test_repo: &'a Path,
    relative_file: &'a str,
    file: &'a Path,
    selector: &'a str,
    artifact_dir: &'a Path,
    timeout: Option<Duration>,
}

fn run_emaxx(request: EmaxxRun<'_>) -> Result<RunnerArtifacts, String> {
    fs::create_dir_all(request.artifact_dir)
        .map_err(|error| format!("create {}: {error}", request.artifact_dir.display()))?;
    let result_path = request.artifact_dir.join("emaxx.json");
    let test_directory = request.test_repo.join("test");
    // Artifact-form parity with the oracle.  GNU resolves library Lisp
    // through its dumped load-path, which points at the pinned checkout, so it
    // executes the 1621 compiled files under lisp/.  Remapping the subject's
    // library path into the disposable clone put it on `.el' source instead,
    // because `git clean -ffdqx' deletes every ignored `.elc' there -- the two
    // runners were executing different forms of the same GNU Lisp.  Read the
    // same tree the oracle reads; the test file still comes from the clone.
    let load_paths = compat::emaxx_upstream_load_path(request.load_path_repo)?;
    let mut command = Command::new(request.binary);
    compat::configure_upstream_like_env(&mut command, &test_directory);
    // GNU's dumped standard-Lisp load path retains the tree that built it,
    // even while this test executes in a disposable checkout.  Emaxx reads an
    // isolated copy, so pass the equivalent observable provenance separately
    // from those physical runtime paths.
    command.env(compat::DUMP_SOURCE_DIRECTORY_ENV, request.load_path_repo);
    let _temp_directory = configure_isolated_temp_directory(&mut command, "emaxx")?;
    command.env(compat::BATCH_RESULT_FILE_ENV, &result_path);
    // Emaxx's Lisp condition is the compatibility result, while this
    // host-side trace preserves the nested file/form that produced opaque
    // conditions such as `(args-out-of-range [] 0)' in the immutable log.
    command.env("EMAXX_TRACE_LOAD_ERRORS", "1");
    command.env("EMAXX_COMPAT_RELATIVE_FILE", request.relative_file);
    command.env(
        "EMAXX_COMPAT_SELECTOR",
        format!("(quote {})", request.selector),
    );
    command.env("EMAXX_COMPAT_RUNNER", "emaxx");
    command.arg("--no-init-file");
    command.arg("--no-site-file");
    command.arg("--no-site-lisp");
    command.arg("--batch");
    configure_isolated_source_directory(&mut command, request.test_repo)?;
    for load_path in &load_paths {
        command.arg("-L");
        command.arg(load_path);
    }
    // The oracle is given -L <checkout>/test; without it the subject cannot
    // resolve a test-support helper that GNU resolves.
    command.arg("-L");
    command.arg(&test_directory);
    command.arg("-l");
    command.arg("ert");
    command.arg("-l");
    command.arg(compat::oracle_helper_path());
    command.arg("-l");
    command.arg(request.file);
    let loaded_marker = request.artifact_dir.join("emaxx.loaded");
    configure_loaded_marker(&mut command, &loaded_marker)?;
    command.arg("--eval");
    command.arg(format!("(emaxx-compat-run (quote {}))", request.selector));

    let process = run_command(command, request.timeout, &loaded_marker, Some(&result_path))?;
    let report = load_or_synthesize_report(
        &result_path,
        "emaxx",
        request.relative_file,
        request.selector,
        &process,
    )?;
    Ok(RunnerArtifacts { report, process })
}

fn remap_load_paths(
    paths: Vec<PathBuf>,
    source_repo: &Path,
    isolated_repo: &Path,
) -> Result<Vec<PathBuf>, String> {
    let source_repo = compat::canonicalize_path(source_repo)?;
    paths
        .into_iter()
        .map(|path| match path.strip_prefix(&source_repo) {
            Ok(relative) => {
                let isolated = isolated_repo.join(relative);
                if isolated.is_dir() {
                    Ok(isolated)
                } else {
                    Err(format!(
                        "isolated load-path directory is missing: {}",
                        isolated.display()
                    ))
                }
            }
            Err(_) => Ok(path),
        })
        .collect()
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

    let message = if let Some(phase) = process.timeout_phase {
        format!("process timed out during {}", phase.as_str())
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
    let report = BatchReport::load_error(runner, relative_file, selector, message);
    // A synthesized timeout/crash report is part of the immutable run
    // artifact, not merely an in-memory summary convenience.  Persist it at
    // the same path the child would have written so aggregate loading and
    // compare-subjects see exactly the file coverage recorded by summary.json.
    report.write_json(result_path)?;
    Ok(report)
}

fn timeout_phase_for_elapsed(
    timeout: Option<Duration>,
    process_elapsed: Duration,
    test_elapsed: Option<Duration>,
) -> Option<TimeoutPhase> {
    timeout.and_then(|limit| match test_elapsed {
        Some(elapsed) if elapsed > limit => Some(TimeoutPhase::Test),
        None if process_elapsed > limit => Some(TimeoutPhase::Setup),
        _ => None,
    })
}

fn run_command(
    mut command: Command,
    timeout: Option<Duration>,
    loaded_marker: &Path,
    completed_marker: Option<&Path>,
) -> Result<ProcessResult, String> {
    // Capture output through temporary files rather than pipes: a pipe fills
    // up while we poll `try_wait' (deadlocking a chatty child), and any
    // grandchild that outlives the child (Tramp's mock shells) would keep a
    // pipe open past the exit we're waiting for.
    let unique = format!(
        "emaxx-harness-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("clock error: {error}"))?
            .as_nanos()
    );
    let stdout_path = env::temp_dir().join(format!("{unique}.out"));
    let stderr_path = env::temp_dir().join(format!("{unique}.err"));
    let stdout_file = fs::File::create(&stdout_path)
        .map_err(|error| format!("create {}: {error}", stdout_path.display()))?;
    let stderr_file = fs::File::create(&stderr_path)
        .map_err(|error| format!("create {}: {error}", stderr_path.display()))?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    let wall_started = SystemTime::now();
    let started = Instant::now();
    let mut child = command
        .spawn()
        .map_err(|error| format!("spawn command: {error}"))?;
    let mut test_started = None;
    let mut test_started_at = None;
    let mut setup_elapsed = None;

    let collect = |timeout_phase: Option<TimeoutPhase>,
                   exit_code: Option<i32>,
                   setup_elapsed: Option<Duration>,
                   test_started: Option<Instant>,
                   test_started_at: Option<SystemTime>|
     -> Result<ProcessResult, String> {
        let elapsed = started.elapsed();
        let stdout = fs::read_to_string(&stdout_path).unwrap_or_default();
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        let _ = fs::remove_file(&stdout_path);
        let _ = fs::remove_file(&stderr_path);
        // The supervisory loop deliberately polls coarsely so hundreds of
        // compatibility children do not busy-wait.  Using the poll instant
        // as a phase boundary therefore adds an arbitrary ~50 ms to short
        // bodies.  Both runners write the loaded marker immediately before
        // ERT and the structured report immediately after it, so their file
        // timestamps provide the precise, runner-symmetric body interval.
        let child_reported_test_elapsed = test_started_at.and_then(|started_at| {
            completed_marker
                .and_then(|path| fs::metadata(path).ok())
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|completed_at| completed_at.duration_since(started_at).ok())
        });
        Ok(ProcessResult {
            exit_code,
            stdout,
            stderr,
            timed_out: timeout_phase.is_some(),
            timeout_phase,
            setup_elapsed: setup_elapsed.unwrap_or(elapsed),
            test_started: test_started.is_some(),
            test_elapsed: child_reported_test_elapsed
                .or_else(|| test_started.map(|test_started| test_started.elapsed()))
                .unwrap_or_default(),
            elapsed,
        })
    };

    loop {
        if test_started.is_none() && loaded_marker.is_file() {
            let now = Instant::now();
            test_started_at = fs::metadata(loaded_marker)
                .ok()
                .and_then(|metadata| metadata.modified().ok());
            setup_elapsed = test_started_at
                .and_then(|loaded_at| loaded_at.duration_since(wall_started).ok())
                .or_else(|| Some(now.duration_since(started)));
            test_started = Some(now);
        }

        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("wait for command: {error}"))?
        {
            return collect(
                None,
                status.code(),
                setup_elapsed,
                test_started,
                test_started_at,
            );
        }

        let timeout_phase = timeout_phase_for_elapsed(
            timeout,
            started.elapsed(),
            test_started.map(|test_started| test_started.elapsed()),
        );
        if let Some(timeout_phase) = timeout_phase {
            child
                .kill()
                .map_err(|error| format!("kill timed out command: {error}"))?;
            let status = child
                .wait()
                .map_err(|error| format!("wait after kill: {error}"))?;
            return collect(
                Some(timeout_phase),
                status.code(),
                setup_elapsed,
                test_started,
                test_started_at,
            );
        }

        thread::sleep(Duration::from_millis(50));
    }
}

fn write_raw_log(path: &Path, process: &ProcessResult) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    let mut content = String::new();
    content.push_str(&format!("exit_code={:?}\n", process.exit_code));
    content.push_str(&format!("timed_out={}\n", process.timed_out));
    content.push_str(&format!(
        "timeout_phase={}\n",
        process
            .timeout_phase
            .map(TimeoutPhase::as_str)
            .unwrap_or("none")
    ));
    content.push_str(&format!(
        "elapsed_ms={}\n",
        duration_millis(process.elapsed)
    ));
    content.push_str(&format!(
        "setup_elapsed_ms={}\n",
        duration_millis(process.setup_elapsed)
    ));
    content.push_str(&format!(
        "test_elapsed_ms={}\n",
        duration_millis(process.test_elapsed)
    ));
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

fn ensure_emaxx_binary(subject_root: Option<&Path>) -> Result<SubjectBuild, String> {
    let current = env::current_exe().map_err(|error| format!("current exe: {error}"))?;
    validate_harness_runtime_location(&current)?;
    let harness_layout = emaxx_build_layout(&current)?;
    let compiled_root = compat::canonicalize_path(&compat::project_root())?;
    let source_root = match subject_root {
        Some(root) => compat::canonicalize_path(root)?,
        None => compiled_root.clone(),
    };
    if !source_root.join("Cargo.toml").is_file() {
        return Err(format!(
            "subject root {} does not contain Cargo.toml",
            source_root.display()
        ));
    }
    let layout = subject_build_layout(&source_root, &harness_layout.profile);
    claim_target_directory(&layout.target_dir, &source_root, false)?;
    let subject_lock = acquire_subject_lock(&layout.target_dir)?;
    let source_sha256 = subject_source_fingerprint(&source_root)?;

    // `cargo run --bin compat-harness' only guarantees that this harness is
    // current; Cargo does not also rebuild its sibling `emaxx' target.  Ask
    // Cargo to validate/build the exact profile whose sibling we are about
    // to execute, so a stale executable can never produce an oracle result.
    let mut build = Command::new("cargo");
    build.args(["build", "--quiet", "--locked", "--bin", "emaxx"]);
    // Pin Cargo to the target directory containing this harness.  Otherwise
    // an inherited CARGO_TARGET_DIR could build a fresh Emaxx elsewhere while
    // the gate continued to execute an old sibling from this directory.
    build.arg("--target-dir").arg(&layout.target_dir);
    match layout.profile.as_str() {
        "dev" => {}
        "release" => {
            build.arg("--release");
        }
        profile => {
            build.args(["--profile", profile]);
        }
    }
    // Do not merely trust that Cargo will replace an existing sibling.  By
    // removing it before the synchronous build, success plus the file check
    // below proves that the executable this run receives was materialized by
    // this build invocation; an old Emaxx executable cannot survive as a
    // fallback.
    remove_existing_emaxx_candidate(&layout.candidate)?;
    let status = build
        .current_dir(&source_root)
        .status()
        .map_err(|error| format!("build emaxx binary: {error}"))?;
    if !status.success() {
        return Err(format!(
            "building emaxx in Cargo profile `{}` failed",
            layout.profile
        ));
    }
    if !layout.candidate.is_file() {
        return Err(format!(
            "expected emaxx binary at {}",
            layout.candidate.display()
        ));
    }
    let source_after_build = subject_source_fingerprint(&source_root)?;
    if source_after_build != source_sha256 {
        return Err(format!(
            "Emaxx subject sources changed while building {}: before={source_sha256}, after={source_after_build}",
            source_root.display()
        ));
    }
    Ok(SubjectBuild {
        source_root,
        target_dir: layout.target_dir,
        profile: layout.profile,
        binary: layout.candidate,
        source_sha256,
        _lock: subject_lock,
    })
}

fn subject_build_layout(source_root: &Path, profile: &str) -> EmaxxBuildLayout {
    let target_dir = source_root.join("target").join("compat-subject");
    let binary_profile = if profile == "dev" { "debug" } else { profile };
    EmaxxBuildLayout {
        profile: profile.to_string(),
        candidate: target_dir
            .join(binary_profile)
            .join(format!("emaxx{}", env::consts::EXE_SUFFIX)),
        target_dir,
    }
}

fn subject_source_fingerprint(source_root: &Path) -> Result<String, String> {
    let mut files = Vec::new();
    for relative in ["Cargo.toml", "Cargo.lock", "build.rs"] {
        let path = source_root.join(relative);
        if path.exists() {
            files.push(path);
        }
    }
    for directory in [".cargo", "src"] {
        let root = source_root.join(directory);
        if !root.exists() {
            continue;
        }
        for entry in WalkDir::new(&root).follow_links(false) {
            let entry = entry.map_err(|error| {
                format!(
                    "walk Emaxx subject inputs under {}: {error}",
                    root.display()
                )
            })?;
            if entry.file_type().is_file() || entry.file_type().is_symlink() {
                files.push(entry.into_path());
            }
        }
    }
    files.sort();

    let mut hasher = Sha256::new();
    for path in files {
        let relative = path.strip_prefix(source_root).map_err(|error| {
            format!(
                "subject input {} is outside {}: {error}",
                path.display(),
                source_root.display()
            )
        })?;
        let name = relative.to_string_lossy();
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        if path.is_symlink() {
            let target = fs::read_link(&path)
                .map_err(|error| format!("read symlink {}: {error}", path.display()))?;
            let target = target.to_string_lossy();
            hasher.update(b"symlink");
            hasher.update((target.len() as u64).to_le_bytes());
            hasher.update(target.as_bytes());
        } else {
            let mut file = fs::File::open(&path)
                .map_err(|error| format!("open subject input {}: {error}", path.display()))?;
            let size = file
                .metadata()
                .map_err(|error| format!("stat subject input {}: {error}", path.display()))?
                .len();
            hasher.update(b"file");
            hasher.update(size.to_le_bytes());
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = file
                    .read(&mut buffer)
                    .map_err(|error| format!("read subject input {}: {error}", path.display()))?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
        }
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(unix)]
fn acquire_subject_lock(target_dir: &Path) -> Result<SubjectLock, String> {
    let path = target_dir.join(SUBJECT_LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| format!("open subject lock {}: {error}", path.display()))?;
    // The lock is deliberately nonblocking: a second gate must fail rather
    // than wait behind (and potentially interfere with) a multi-hour sweep.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(format!(
            "another compatibility run is already using subject target {}; refusing concurrent rebuild/test",
            target_dir.display()
        ));
    }
    Ok(SubjectLock { _file: file })
}

#[cfg(not(unix))]
fn acquire_subject_lock(_target_dir: &Path) -> Result<SubjectLock, String> {
    Err("compatibility subject locking is not implemented on this platform".into())
}

fn claim_target_directory(
    target_dir: &Path,
    source_root: &Path,
    allow_unowned_existing: bool,
) -> Result<(), String> {
    fs::create_dir_all(target_dir)
        .map_err(|error| format!("create target directory {}: {error}", target_dir.display()))?;
    let source_root = compat::canonicalize_path(source_root)?;
    let marker = target_dir.join(TARGET_OWNER_FILE);
    if marker.exists() {
        let owner = fs::read_to_string(&marker)
            .map_err(|error| format!("read target owner {}: {error}", marker.display()))?;
        let owner = PathBuf::from(owner.trim());
        if owner != source_root {
            return Err(format!(
                "refusing to share Cargo target {}: owned by {}, requested by {}",
                target_dir.display(),
                owner.display(),
                source_root.display()
            ));
        }
        return Ok(());
    }
    if !allow_unowned_existing
        && fs::read_dir(target_dir)
            .map_err(|error| format!("read target directory {}: {error}", target_dir.display()))?
            .next()
            .is_some()
    {
        return Err(format!(
            "refusing unowned non-empty Cargo target {}; use an empty subject target",
            target_dir.display()
        ));
    }
    fs::write(&marker, format!("{}\n", source_root.display()))
        .map_err(|error| format!("write target owner {}: {error}", marker.display()))
}

fn compiled_target_dir() -> Result<PathBuf, String> {
    Ok(PathBuf::from(env!("EMAXX_COMPILED_TARGET_DIR")))
}

fn validate_harness_runtime_location(harness_binary: &Path) -> Result<(), String> {
    let runtime_target = emaxx_build_layout(harness_binary)?.target_dir;
    let compiled_target = compiled_target_dir()?;
    let runtime_target = compat::canonicalize_path(&runtime_target)?;
    let compiled_target = compat::canonicalize_path(&compiled_target)?;
    if runtime_target != compiled_target {
        return Err(format!(
            "refusing copied compatibility harness: running from target {}, compiled for {}",
            runtime_target.display(),
            compiled_target.display()
        ));
    }
    Ok(())
}

fn remove_existing_emaxx_candidate(candidate: &Path) -> Result<(), String> {
    match fs::remove_file(candidate) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "remove stale emaxx candidate {}: {error}",
            candidate.display()
        )),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct EmaxxBuildLayout {
    profile: String,
    target_dir: PathBuf,
    candidate: PathBuf,
}

fn emaxx_build_layout(harness_binary: &Path) -> Result<EmaxxBuildLayout, String> {
    let bin_dir = harness_binary.parent().ok_or_else(|| {
        format!(
            "cannot locate binary directory for {}",
            harness_binary.display()
        )
    })?;
    let target_dir = bin_dir.parent().ok_or_else(|| {
        format!(
            "cannot locate Cargo target directory for {}",
            harness_binary.display()
        )
    })?;
    Ok(EmaxxBuildLayout {
        profile: cargo_profile_for_binary_directory(bin_dir)?,
        target_dir: target_dir.to_path_buf(),
        candidate: bin_dir.join(format!("emaxx{}", env::consts::EXE_SUFFIX)),
    })
}

fn cargo_profile_for_binary_directory(bin_dir: &Path) -> Result<String, String> {
    let directory = bin_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("cannot determine Cargo profile from {}", bin_dir.display()))?;
    Ok(if directory == "debug" {
        "dev".into()
    } else {
        directory.into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_frozen_manifest_is_exactly_7595_outcomes() {
        let manifest = FrozenCompatibilityManifest::load().expect("load frozen manifest");

        assert_eq!(manifest.entries.len(), FROZEN_COMPAT_FILE_COUNT);
        assert_eq!(
            manifest.historical_load_errors.len(),
            FROZEN_COMPAT_LOAD_ERROR_COUNT
        );
        assert_eq!(
            manifest.entries.values().map(Vec::len).sum::<usize>(),
            FROZEN_COMPAT_OUTCOME_COUNT
        );
        assert_eq!(
            manifest
                .entries
                .values()
                .filter(|names| !names.is_empty())
                .count(),
            458
        );
    }

    #[test]
    fn frozen_manifest_parser_rejects_duplicate_and_count_drift() {
        let duplicate = "test/a.el: discovered=2 selected=2\n  same\n  same\n";
        assert!(
            FrozenCompatibilityManifest::parse(
                PathBuf::from("manifest.txt"),
                "hash".into(),
                duplicate
            )
            .unwrap_err()
            .contains("duplicate outcome")
        );

        let count_drift = "test/a.el: discovered=2 selected=2\n  one\n";
        assert!(
            FrozenCompatibilityManifest::parse(
                PathBuf::from("manifest.txt"),
                "hash".into(),
                count_drift
            )
            .unwrap_err()
            .contains("records selected=2 but lists 1")
        );
    }

    #[test]
    fn regression_add_accepts_multiple_files() {
        let cli = Cli::try_parse_from([
            "compat-harness",
            "regressions",
            "add",
            "--file",
            "test/lisp/a-tests.el",
            "--file",
            "test/src/b-tests.el",
        ])
        .unwrap();

        let Commands::Regressions(RegressionArgs {
            command: RegressionCommand::Add(args),
        }) = cli.command
        else {
            panic!("expected regressions add command");
        };
        assert_eq!(args.file, ["test/lisp/a-tests.el", "test/src/b-tests.el"]);
    }

    fn git_ok(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(root)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "git {args:?} failed in {}",
            root.display()
        );
    }

    fn process_result(
        setup: Duration,
        test: Duration,
        test_started: bool,
        timeout_phase: Option<TimeoutPhase>,
    ) -> ProcessResult {
        ProcessResult {
            exit_code: timeout_phase.is_none().then_some(0),
            stdout: String::new(),
            stderr: String::new(),
            timed_out: timeout_phase.is_some(),
            timeout_phase,
            setup_elapsed: setup,
            test_started,
            test_elapsed: test,
            elapsed: setup + test,
        }
    }

    fn test_provenance() -> RunProvenance {
        RunProvenance {
            harness_source_root: "/harness".into(),
            harness_compiled_target_dir: "/harness/target".into(),
            harness_binary: "/harness/target/release/compat-harness".into(),
            harness_sha256: "harness".into(),
            oracle_helper_sha256: "helper".into(),
            subject_source_root: "/subject".into(),
            subject_source_sha256: "source".into(),
            subject_git_head: Some("head".into()),
            subject_git_dirty: Some(false),
            subject_target_dir: "/subject/target/compat-subject".into(),
            subject_profile: "release".into(),
            subject_binary: "/subject/target/compat-subject/release/emaxx".into(),
            subject_sha256: "subject".into(),
            oracle_binary: "/oracle/src/emacs".into(),
            oracle_sha256: "oracle".into(),
            oracle_repo: "/oracle".into(),
            oracle_repo_commit: "oracle-head".into(),
            oracle_test_support_sha256: "test-support".into(),
            oracle_emacs_version: "30.2".into(),
            oracle_system_type: "darwin".into(),
            oracle_native_compilation: true,
            timeout_seconds: Some(DEFAULT_TIMEOUT_SECONDS),
        }
    }

    fn test_summary() -> AggregateReport {
        AggregateReport {
            mode: "run".into(),
            selector: "t".into(),
            scope: "All".into(),
            total_files: 1,
            matching_outcomes: 0,
            mismatching_outcomes: 0,
            total_outcomes: 0,
            matching_files: 1,
            mismatching_files: 0,
            files: vec!["a.el".into()],
            mismatches: Vec::new(),
            name_filter: None,
            timings: Vec::new(),
            performance_regressions: Vec::new(),
            frozen_manifest: None,
            provenance: test_provenance(),
        }
    }

    #[test]
    fn performance_warnings_do_not_fail_semantic_compatibility() {
        let mut summary = test_summary();
        summary.performance_regressions.push("a.el".into());

        assert_eq!(compatibility_exit_status(&summary), 0);

        summary.mismatching_files = 1;
        assert_eq!(compatibility_exit_status(&summary), 1);
    }

    #[test]
    fn runner_timing_flags_the_two_x_boundary() {
        let oracle = process_result(
            Duration::from_secs(3),
            Duration::from_millis(100),
            true,
            None,
        );
        let below_subject = process_result(
            Duration::from_secs(30),
            Duration::from_millis(199),
            true,
            None,
        );
        let below = compare_runner_timings("below.el", &oracle, &below_subject);
        assert_eq!(below.emaxx_over_gnu_milli, Some(1_990));
        assert!(!below.emaxx_at_least_twice_as_slow);
        assert_eq!(below.gnu_emacs_setup_duration_ms, 3_000);
        assert_eq!(below.emaxx_setup_duration_ms, 30_000);

        let boundary_subject = process_result(
            Duration::from_secs(1),
            Duration::from_millis(200),
            true,
            None,
        );
        let boundary = compare_runner_timings("boundary.el", &oracle, &boundary_subject);
        assert_eq!(boundary.emaxx_over_gnu_milli, Some(2_000));
        assert!(boundary.emaxx_at_least_twice_as_slow);
    }

    #[test]
    fn paired_timeouts_are_incomplete_not_passing_or_slow() {
        let mut comparison = compat::ComparisonReport {
            file: "timeout.el".into(),
            matches: true,
            issues: Vec::new(),
            matching_outcomes: 0,
            mismatching_outcomes: 0,
        };
        let oracle = process_result(
            Duration::from_secs(180),
            Duration::ZERO,
            false,
            Some(TimeoutPhase::Setup),
        );
        let emaxx = process_result(
            Duration::from_secs(2),
            Duration::from_secs(180),
            true,
            Some(TimeoutPhase::Test),
        );
        invalidate_timed_out_comparison(&mut comparison, "GNU Emacs", &oracle);
        invalidate_timed_out_comparison(&mut comparison, "Emaxx", &emaxx);
        assert!(!comparison.matches);
        assert_eq!(comparison.issues.len(), 2);
        assert!(
            comparison
                .issues
                .iter()
                .all(|issue| issue.kind == "timeout")
        );

        let timing = compare_runner_timings("timeout.el", &oracle, &emaxx);
        assert!(timing.gnu_emacs_timed_out);
        assert!(timing.emaxx_timed_out);
        assert_eq!(timing.gnu_emacs_timeout_phase.as_deref(), Some("setup"));
        assert_eq!(timing.emaxx_timeout_phase.as_deref(), Some("test"));
        assert_eq!(timing.emaxx_over_gnu_milli, None);
        assert!(!timing.emaxx_at_least_twice_as_slow);
    }

    #[test]
    fn command_timeout_resets_when_test_execution_starts() {
        let limit = Some(Duration::from_millis(500));
        assert_eq!(
            timeout_phase_for_elapsed(
                limit,
                Duration::from_millis(900),
                Some(Duration::from_millis(499)),
            ),
            None,
            "setup time must stop consuming the budget after the marker"
        );
        assert_eq!(
            timeout_phase_for_elapsed(
                limit,
                Duration::from_millis(900),
                Some(Duration::from_millis(501)),
            ),
            Some(TimeoutPhase::Test)
        );
    }

    #[cfg(unix)]
    #[test]
    fn command_reports_setup_timeout_before_the_loaded_marker() {
        let marker = unique_temp_path("missing-phase-marker").unwrap();
        let mut command = Command::new("sh");
        command.args(["-c", "sleep 1"]);
        let result = run_command(command, Some(Duration::from_millis(75)), &marker, None).unwrap();
        assert_eq!(result.timeout_phase, Some(TimeoutPhase::Setup));
        assert!(!result.test_started);
        assert_eq!(result.test_elapsed, Duration::ZERO);
    }

    #[cfg(unix)]
    #[test]
    fn command_uses_child_markers_for_sub_poll_body_timing() {
        let loaded = unique_temp_path("phase-loaded").unwrap();
        let completed = unique_temp_path("phase-completed").unwrap();
        let mut command = Command::new("sh");
        command
            .args([
                "-c",
                "sleep 0.1; : > \"$EMAXX_TEST_PHASE_MARKER\"; : > \"$EMAXX_TEST_COMPLETED_MARKER\"; touch -r \"$EMAXX_TEST_PHASE_MARKER\" \"$EMAXX_TEST_COMPLETED_MARKER\"",
            ])
            .env("EMAXX_TEST_PHASE_MARKER", &loaded)
            .env("EMAXX_TEST_COMPLETED_MARKER", &completed);
        let result = run_command(command, None, &loaded, Some(&completed)).unwrap();
        assert!(!result.timed_out);
        assert!(result.test_started);
        assert_eq!(result.test_elapsed, Duration::ZERO);
        let _ = fs::remove_file(loaded);
        let _ = fs::remove_file(completed);
    }

    #[test]
    fn emaxx_build_uses_the_harness_binary_profile() {
        let debug = emaxx_build_layout(Path::new("/repo/target/debug/compat-harness")).unwrap();
        assert_eq!(debug.profile, "dev");
        assert_eq!(debug.target_dir, Path::new("/repo/target"));
        assert_eq!(debug.candidate, Path::new("/repo/target/debug/emaxx"));

        let release = emaxx_build_layout(Path::new("/repo/target/release/compat-harness")).unwrap();
        assert_eq!(release.profile, "release");
        assert_eq!(release.target_dir, Path::new("/repo/target"));
        assert_eq!(release.candidate, Path::new("/repo/target/release/emaxx"));

        let custom =
            emaxx_build_layout(Path::new("/repo/target/compat-ci/compat-harness")).unwrap();
        assert_eq!(custom.profile, "compat-ci");
        assert_eq!(custom.target_dir, Path::new("/repo/target"));
        assert_eq!(custom.candidate, Path::new("/repo/target/compat-ci/emaxx"));
    }

    #[test]
    fn stale_emaxx_candidate_must_be_removed_before_build() {
        let root = env::temp_dir().join(format!(
            "emaxx-stale-candidate-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let candidate = root.join(format!("emaxx{}", env::consts::EXE_SUFFIX));
        fs::write(&candidate, b"stale executable").unwrap();

        remove_existing_emaxx_candidate(&candidate).unwrap();
        assert!(!candidate.exists());
        // Absence is also valid, so first-time builds take the same path.
        remove_existing_emaxx_candidate(&candidate).unwrap();

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn synthesized_runner_failure_is_persisted_as_a_batch_report() {
        let root = env::temp_dir().join(format!(
            "emaxx-synthesized-report-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let result_path = root.join("test/lisp/example.compat/emaxx.json");
        let process = ProcessResult {
            exit_code: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: true,
            timeout_phase: Some(TimeoutPhase::Setup),
            setup_elapsed: Duration::from_secs(180),
            test_started: false,
            test_elapsed: Duration::ZERO,
            elapsed: Duration::from_secs(180),
        };

        let report =
            load_or_synthesize_report(&result_path, "emaxx", "test/lisp/example.el", "t", &process)
                .unwrap();

        assert!(result_path.is_file());
        assert_eq!(BatchReport::read_json(&result_path).unwrap(), report);
        assert_eq!(report.file_status, FileStatus::LoadError);
        assert_eq!(
            report.file_error.as_deref(),
            Some("process timed out during setup")
        );

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn cargo_target_cannot_be_shared_between_source_roots() {
        let root = env::temp_dir().join(format!(
            "emaxx-target-owner-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let target = root.join("target");
        let source_a = root.join("source-a");
        let source_b = root.join("source-b");
        fs::create_dir_all(&source_a).unwrap();
        fs::create_dir_all(&source_b).unwrap();

        claim_target_directory(&target, &source_a, false).unwrap();
        claim_target_directory(&target, &source_a, false).unwrap();
        let error = claim_target_directory(&target, &source_b, false).unwrap_err();
        assert!(error.contains("refusing to share Cargo target"));

        fs::remove_dir_all(&root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn concurrent_subject_use_is_rejected() {
        let root = env::temp_dir().join(format!(
            "emaxx-subject-lock-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();

        let first = acquire_subject_lock(&root).unwrap();
        let error = acquire_subject_lock(&root).unwrap_err();
        assert!(error.contains("another compatibility run"));
        drop(first);
        acquire_subject_lock(&root).unwrap();

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn subject_fingerprint_detects_runtime_or_rust_source_changes() {
        let root = env::temp_dir().join(format!(
            "emaxx-subject-fingerprint-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(root.join("src/lisp")).unwrap();
        fs::write(root.join("Cargo.toml"), "[package]\nname='probe'\n").unwrap();
        fs::write(root.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n").unwrap();
        fs::write(root.join("src/lisp/runtime_probe.el"), "(provide 'probe)\n").unwrap();
        let initial = subject_source_fingerprint(&root).unwrap();

        fs::write(
            root.join("src/lisp/runtime_probe.el"),
            "(provide 'changed)\n",
        )
        .unwrap();
        assert_ne!(subject_source_fingerprint(&root).unwrap(), initial);

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn copied_harness_location_is_rejected() {
        let root = env::temp_dir().join(format!(
            "emaxx-copied-harness-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let bin_dir = root.join("release");
        fs::create_dir_all(&bin_dir).unwrap();
        let error = validate_harness_runtime_location(&bin_dir.join("compat-harness")).unwrap_err();
        assert!(error.contains("refusing copied compatibility harness"));

        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn every_subject_uses_a_dedicated_target_cache() {
        let layout = subject_build_layout(Path::new("/baseline"), "release");
        assert_eq!(
            layout.target_dir,
            Path::new("/baseline/target/compat-subject")
        );
        assert_eq!(
            layout.candidate,
            Path::new("/baseline/target/compat-subject/release/emaxx")
        );
    }

    #[test]
    fn explicit_zero_timeout_is_rejected() {
        assert!(resolve_run_timeout(Some(0)).is_err());
        assert_eq!(
            resolve_run_timeout(Some(17)).unwrap(),
            Some(Duration::from_secs(17))
        );
        assert_eq!(
            resolve_run_timeout(None).unwrap(),
            Some(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS))
        );
    }

    #[test]
    fn subject_comparison_separates_regressions_fixes_and_changed_failures() {
        let baseline = BTreeMap::from([
            (
                "a.el|pass".into(),
                NormalizedSubjectOutcome {
                    status: TestStatus::Passed,
                    condition: None,
                },
            ),
            (
                "a.el|fail".into(),
                NormalizedSubjectOutcome {
                    status: TestStatus::Failed,
                    condition: Some("old-error".into()),
                },
            ),
            (
                "a.el|changed".into(),
                NormalizedSubjectOutcome {
                    status: TestStatus::Failed,
                    condition: Some("first-error".into()),
                },
            ),
        ]);
        let candidate = BTreeMap::from([
            (
                "a.el|pass".into(),
                NormalizedSubjectOutcome {
                    status: TestStatus::Failed,
                    condition: Some("regression".into()),
                },
            ),
            (
                "a.el|fail".into(),
                NormalizedSubjectOutcome {
                    status: TestStatus::Passed,
                    condition: None,
                },
            ),
            (
                "a.el|changed".into(),
                NormalizedSubjectOutcome {
                    status: TestStatus::Failed,
                    condition: Some("later-error".into()),
                },
            ),
        ]);

        let summary = test_summary();
        let comparison = compare_subject_outcomes(
            "baseline".into(),
            "candidate".into(),
            &summary,
            &summary,
            &baseline,
            &candidate,
        );
        assert!(comparison.incompatible.is_empty());
        assert_eq!(comparison.pass_to_fail.len(), 1);
        assert_eq!(comparison.pass_to_fail[0].test, "pass");
        assert_eq!(comparison.fail_to_pass.len(), 1);
        assert_eq!(comparison.fail_to_pass[0].test, "fail");
        assert_eq!(comparison.changed_failures.len(), 1);
        assert_eq!(comparison.changed_failures[0].test, "changed");
        assert!(comparison.missing_from_candidate.is_empty());
        assert!(comparison.added_in_candidate.is_empty());
    }

    #[test]
    fn subject_comparison_rejects_incompatible_runs_and_pass_to_skip() {
        let baseline_summary = test_summary();
        let mut candidate_summary = test_summary();
        candidate_summary.selector = "(not (tag :unstable))".into();
        candidate_summary.provenance.oracle_sha256 = "different-oracle".into();
        let baseline = BTreeMap::from([(
            "a.el|test".into(),
            NormalizedSubjectOutcome {
                status: TestStatus::Passed,
                condition: None,
            },
        )]);
        let candidate = BTreeMap::from([
            (
                "a.el|test".into(),
                NormalizedSubjectOutcome {
                    status: TestStatus::Skipped,
                    condition: Some("ert-test-skipped".into()),
                },
            ),
            (
                "a.el|extra".into(),
                NormalizedSubjectOutcome {
                    status: TestStatus::Passed,
                    condition: None,
                },
            ),
        ]);

        let comparison = compare_subject_outcomes(
            "baseline".into(),
            "candidate".into(),
            &baseline_summary,
            &candidate_summary,
            &baseline,
            &candidate,
        );

        assert_eq!(comparison.incompatible.len(), 2);
        assert_eq!(comparison.pass_to_fail.len(), 1);
        assert_eq!(comparison.pass_to_fail[0].candidate_status, "skipped");
        assert_eq!(comparison.added_in_candidate, vec!["a.el|extra"]);
    }

    #[test]
    fn subject_report_accepts_the_batch_runners_quoted_selector_spelling() {
        assert!(report_selector_matches_summary("t", "t"));
        assert!(report_selector_matches_summary("'t", "t"));
        assert!(report_selector_matches_summary(
            "'(not (tag :unstable))",
            "(not (tag :unstable))"
        ));
        assert!(!report_selector_matches_summary("nil", "t"));
    }

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
    fn through_file_selects_the_canonical_inclusive_prefix() {
        let repo_root = Path::new("/repo");
        let mut files = vec![
            PathBuf::from("/repo/test/lisp/a-tests.el"),
            PathBuf::from("/repo/test/lisp/b-tests.el"),
            PathBuf::from("/repo/test/lisp/c-tests.el"),
        ];

        truncate_files_through(&mut files, repo_root, Some("test/lisp/b-tests.el")).unwrap();

        assert_eq!(
            files,
            vec![
                PathBuf::from("/repo/test/lisp/a-tests.el"),
                PathBuf::from("/repo/test/lisp/b-tests.el"),
            ]
        );
    }

    #[test]
    fn runner_temp_directory_is_isolated_and_exported_portably() {
        let mut command = Command::new("emaxx-test-command");
        let configured = configure_isolated_temp_directory(&mut command, "oracle")
            .expect("configure isolated runner temp directory");
        let configured_path = configured.path.clone();

        assert_eq!(configured_path.parent(), Some(env::temp_dir().as_path()));
        assert!(configured_path.is_dir());
        assert!(
            configured_path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("emaxx-compat-oracle-"))
        );
        let exported = command
            .get_envs()
            .filter_map(|(name, value)| {
                value.map(|value| (name.to_string_lossy().into_owned(), PathBuf::from(value)))
            })
            .collect::<std::collections::HashMap<_, _>>();
        for variable in ["TMPDIR", "TMP", "TEMP"] {
            assert_eq!(exported.get(variable), Some(&configured_path));
        }

        drop(configured);
        assert!(!configured_path.exists());
    }

    #[test]
    fn runner_overrides_dumped_source_directory_with_isolated_checkout() {
        let mut command = Command::new("emacs-test-command");
        let checkout = Path::new("/tmp/emaxx isolated checkout");
        configure_isolated_source_directory(&mut command, checkout)
            .expect("configure isolated source-directory");

        let mut expected_directory = checkout.display().to_string();
        expected_directory.push(std::path::MAIN_SEPARATOR);
        let expected_literal = serde_json::to_string(&expected_directory).unwrap();
        let args = command
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert_eq!(
            args,
            vec![
                "--eval".to_string(),
                format!("(setq source-directory {expected_literal})"),
            ]
        );
    }

    #[test]
    fn isolated_test_checkout_excludes_ignored_state_and_restores_between_files() {
        let source = unique_temp_path("checkout-source-test").unwrap();
        fs::create_dir(&source).unwrap();
        git_ok(&source, &["init", "--quiet"]);
        fs::write(
            source.join(".gitignore"),
            "*.elc\nlisp/loaddefs.el\nlib-src/emacsclient\netc/charsets/*.map\netc/DOC\n",
        )
        .unwrap();
        fs::write(source.join("fixture.el"), "(pristine)\n").unwrap();
        fs::create_dir(source.join("lisp")).unwrap();
        fs::write(source.join("lisp/loaddefs.el"), "(generated-pristine)\n").unwrap();
        fs::create_dir(source.join("lib-src")).unwrap();
        fs::write(source.join("lib-src/emacsclient"), "helper\n").unwrap();
        fs::create_dir_all(source.join("etc/charsets")).unwrap();
        fs::write(source.join("etc/charsets/IBM038.map"), "0x81 0x0061\n").unwrap();
        fs::write(source.join("etc/DOC"), "generated-doc\n").unwrap();
        git_ok(&source, &["add", ".gitignore", "fixture.el"]);
        git_ok(
            &source,
            &[
                "-c",
                "user.name=Emaxx Test",
                "-c",
                "user.email=emaxx@example.invalid",
                "commit",
                "--quiet",
                "-m",
                "fixture",
            ],
        );
        let commit = String::from_utf8(
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(&source)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        fs::write(source.join("stale.elc"), "stale").unwrap();
        let support_fingerprint = test_support_fingerprint(&source).unwrap();

        let checkout = IsolatedTestCheckout::clone(&source, commit.trim(), "test").unwrap();
        assert!(!checkout.file("stale.elc").exists());
        assert_eq!(
            fs::read_to_string(checkout.file("lisp/loaddefs.el")).unwrap(),
            "(generated-pristine)\n"
        );
        assert_eq!(
            fs::read_to_string(checkout.file("lib-src/emacsclient")).unwrap(),
            "helper\n"
        );
        assert_eq!(
            fs::read_to_string(checkout.file("etc/charsets/IBM038.map")).unwrap(),
            "0x81 0x0061\n"
        );
        assert_eq!(
            fs::read_to_string(checkout.file("etc/DOC")).unwrap(),
            "generated-doc\n"
        );
        fs::write(checkout.file("fixture.el"), "(mutated)\n").unwrap();
        fs::write(checkout.file("lisp/loaddefs.el"), "(generated-mutated)\n").unwrap();
        fs::write(checkout.file("lib-src/emacsclient"), "helper-mutated\n").unwrap();
        fs::write(
            checkout.file("etc/charsets/IBM038.map"),
            "generated-mutated\n",
        )
        .unwrap();
        fs::write(checkout.file("etc/DOC"), "generated-doc-mutated\n").unwrap();
        fs::write(checkout.file("generated.elc"), "generated").unwrap();

        checkout.restore().unwrap();
        assert_eq!(
            fs::read_to_string(checkout.file("fixture.el")).unwrap(),
            "(pristine)\n"
        );
        assert_eq!(
            fs::read_to_string(checkout.file("lisp/loaddefs.el")).unwrap(),
            "(generated-pristine)\n"
        );
        assert_eq!(
            fs::read_to_string(checkout.file("lib-src/emacsclient")).unwrap(),
            "helper\n"
        );
        assert_eq!(
            fs::read_to_string(checkout.file("etc/charsets/IBM038.map")).unwrap(),
            "0x81 0x0061\n"
        );
        assert_eq!(
            fs::read_to_string(checkout.file("etc/DOC")).unwrap(),
            "generated-doc\n"
        );
        assert!(!checkout.file("generated.elc").exists());
        fs::write(source.join("lisp/loaddefs.el"), "(generated-changed)\n").unwrap();
        fs::write(source.join("etc/charsets/IBM038.map"), "0x82 0x0061\n").unwrap();
        fs::write(source.join("etc/DOC"), "generated-doc-changed\n").unwrap();
        assert_ne!(
            test_support_fingerprint(&source).unwrap(),
            support_fingerprint
        );

        let checkout_root = checkout.root.clone();
        drop(checkout);
        assert!(!checkout_root.exists());
        fs::remove_dir_all(source).unwrap();
    }

    #[test]
    fn isolated_runner_preserves_the_oracles_load_path_order() {
        let source = unique_temp_path("load-path-source-test").unwrap();
        let isolated = unique_temp_path("load-path-isolated-test").unwrap();
        fs::create_dir_all(source.join("lisp/emacs-lisp")).unwrap();
        fs::create_dir_all(isolated.join("lisp/emacs-lisp")).unwrap();
        let source = source.canonicalize().unwrap();
        let external = PathBuf::from("/external/load-path");

        assert_eq!(
            remap_load_paths(
                vec![
                    source.join("lisp"),
                    source.join("lisp/emacs-lisp"),
                    external.clone()
                ],
                &source,
                &isolated,
            )
            .unwrap(),
            vec![
                isolated.join("lisp"),
                isolated.join("lisp/emacs-lisp"),
                external
            ]
        );

        fs::remove_dir_all(source).unwrap();
        fs::remove_dir_all(isolated).unwrap();
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
