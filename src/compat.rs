use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use regex::Regex;
use serde::{Deserialize, Serialize};
use walkdir::{DirEntry, WalkDir};

pub const ORACLE_LOCK_PATH: &str = "compat/oracle.lock.json";
pub const ORACLE_LOCAL_PATH: &str = "compat/oracle.local.json";
pub const ORACLE_HELPER_PATH: &str = "compat/emacs_compat_runner.el";
pub const BATCH_RESULT_FILE_ENV: &str = "EMAXX_BATCH_RESULT_FILE";
const ORACLE_BATCH_REPORT_OVERRIDES: [&str; 9] = [
    "test/src/comp-tests.el",
    "test/src/data-tests.el",
    "test/src/emacs-module-tests.el",
    "test/src/fns-tests.el",
    "test/src/keymap-tests.el",
    "test/src/lread-tests.el",
    "test/src/print-tests.el",
    "test/src/syntax-tests.el",
    "test/src/thread-tests.el",
];
pub const SUPPORTED_ENV_VARS: [&str; 4] = [
    "EMACS_TEST_TIMEOUT",
    "EMACS_TEST_VERBOSE",
    "EMACS_TEST_JUNIT_REPORT",
    "TEST_BACKTRACE_LINE_LENGTH",
];
pub const UNSET_ENV_VARS: [&str; 6] = [
    "EMACSDATA",
    "EMACSDOC",
    "EMACSLOADPATH",
    "EMACSPATH",
    "GREP_OPTIONS",
    "XDG_CONFIG_HOME",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectorSpec {
    Default,
    Expensive,
    All,
    Check,
    CheckMaybe,
    CheckExpensive,
    CheckAll,
    Literal,
}

impl SelectorSpec {
    pub fn alias_name(self) -> Option<&'static str> {
        match self {
            Self::Default => Some("default"),
            Self::Expensive => Some("expensive"),
            Self::All => Some("all"),
            Self::Check => Some("check"),
            Self::CheckMaybe => Some("check-maybe"),
            Self::CheckExpensive => Some("check-expensive"),
            Self::CheckAll => Some("check-all"),
            Self::Literal => None,
        }
    }

    pub fn from_cli(value: &str) -> Self {
        match value {
            "default" => Self::Default,
            "expensive" => Self::Expensive,
            "all" => Self::All,
            "check" => Self::Check,
            "check-maybe" => Self::CheckMaybe,
            "check-expensive" => Self::CheckExpensive,
            "check-all" => Self::CheckAll,
            _ => Self::Literal,
        }
    }
}

pub fn selector_aliases(test_native_comp: bool) -> BTreeMap<String, String> {
    let selector_default = if test_native_comp {
        "(not (or (tag :expensive-test) (tag :unstable)))"
    } else {
        "(not (or (tag :expensive-test) (tag :unstable) (tag :nativecomp)))"
    };
    let selector_expensive = if test_native_comp {
        "(not (tag :unstable))"
    } else {
        "(not (or (tag :unstable) (tag :nativecomp)))"
    };
    let selector_all = if test_native_comp {
        "t"
    } else {
        "(not (tag :nativecomp))"
    };

    let mut aliases = BTreeMap::new();
    aliases.insert("default".into(), selector_default.into());
    aliases.insert("check".into(), selector_default.into());
    aliases.insert("check-maybe".into(), selector_default.into());
    aliases.insert("expensive".into(), selector_expensive.into());
    aliases.insert("check-expensive".into(), selector_expensive.into());
    aliases.insert("all".into(), selector_all.into());
    aliases.insert("check-all".into(), selector_all.into());
    aliases
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Src,
    Lisp,
    LibSrc,
    Misc,
    Automated,
    All,
}

impl Scope {
    pub fn roots(self) -> &'static [&'static str] {
        match self {
            Self::Src => &["test/src"],
            Self::Lisp => &["test/lisp"],
            Self::LibSrc => &["test/lib-src"],
            Self::Misc => &["test/misc"],
            Self::Automated | Self::All => &["test/src", "test/lisp", "test/lib-src", "test/misc"],
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiscoveredTest {
    pub name: String,
    pub tags: Vec<String>,
    pub expected_result: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    Loaded,
    LoadError,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TestStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestOutcome {
    pub name: String,
    pub status: TestStatus,
    pub condition_type: Option<String>,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct BatchSummary {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub unexpected: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchReport {
    pub runner: String,
    pub file: String,
    pub selector: String,
    pub file_status: FileStatus,
    pub file_error: Option<String>,
    pub discovered_tests: Vec<DiscoveredTest>,
    pub selected_tests: Vec<String>,
    pub results: Vec<TestOutcome>,
    pub summary: BatchSummary,
}

impl BatchReport {
    pub fn load_error(runner: &str, file: &str, selector: &str, error: impl Into<String>) -> Self {
        Self {
            runner: runner.to_string(),
            file: file.to_string(),
            selector: selector.to_string(),
            file_status: FileStatus::LoadError,
            file_error: Some(error.into()),
            discovered_tests: Vec::new(),
            selected_tests: Vec::new(),
            results: Vec::new(),
            summary: BatchSummary::default(),
        }
    }

    pub fn write_json(&self, path: &Path) -> Result<(), String> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|err| format!("serialize batch report {}: {err}", path.display()))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("create report directory {}: {err}", parent.display()))?;
        }
        fs::write(path, json).map_err(|err| format!("write batch report {}: {err}", path.display()))
    }

    pub fn read_json(path: &Path) -> Result<Self, String> {
        let data = fs::read_to_string(path)
            .map_err(|err| format!("read batch report {}: {err}", path.display()))?;
        serde_json::from_str(&data)
            .map_err(|err| format!("parse batch report {}: {err}", path.display()))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComparisonIssue {
    pub kind: String,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComparisonReport {
    pub file: String,
    pub matches: bool,
    pub issues: Vec<ComparisonIssue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleLock {
    pub format_version: u32,
    pub emacs_version: String,
    pub emacs_repo_commit: String,
    #[serde(default)]
    pub system_type: String,
    #[serde(default)]
    pub native_compilation: bool,
    pub selector_default: String,
    pub selector_expensive: String,
    pub selector_all: String,
    #[serde(default)]
    pub selector_aliases: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleLocalConfig {
    pub format_version: u32,
    pub emacs_binary: PathBuf,
    pub emacs_repo: PathBuf,
}

impl OracleLock {
    pub fn current(
        repo_commit: String,
        emacs_version: String,
        system_type: String,
        native_compilation: bool,
    ) -> Self {
        let test_native_comp = should_enable_nativecomp_tests(&system_type, native_compilation);
        let selector_aliases = selector_aliases(test_native_comp);
        Self {
            format_version: 2,
            emacs_version,
            emacs_repo_commit: repo_commit,
            system_type,
            native_compilation,
            selector_default: selector_aliases["default"].clone(),
            selector_expensive: selector_aliases["expensive"].clone(),
            selector_all: selector_aliases["all"].clone(),
            selector_aliases,
        }
    }

    pub fn selector_aliases(&self) -> BTreeMap<String, String> {
        if self.selector_aliases.is_empty() {
            let mut aliases = BTreeMap::new();
            aliases.insert("default".into(), self.selector_default.clone());
            aliases.insert("check".into(), self.selector_default.clone());
            aliases.insert("check-maybe".into(), self.selector_default.clone());
            aliases.insert("expensive".into(), self.selector_expensive.clone());
            aliases.insert("check-expensive".into(), self.selector_expensive.clone());
            aliases.insert("all".into(), self.selector_all.clone());
            aliases.insert("check-all".into(), self.selector_all.clone());
            aliases
        } else {
            self.selector_aliases.clone()
        }
    }
}

impl OracleLocalConfig {
    pub fn new(emacs_binary: PathBuf, emacs_repo: PathBuf) -> Self {
        Self {
            format_version: 1,
            emacs_binary,
            emacs_repo,
        }
    }
}

pub fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn compat_path(relative: &str) -> PathBuf {
    project_root().join(relative)
}

pub fn oracle_lock_path() -> PathBuf {
    compat_path(ORACLE_LOCK_PATH)
}

pub fn oracle_local_path() -> PathBuf {
    compat_path(ORACLE_LOCAL_PATH)
}

pub fn oracle_helper_path() -> PathBuf {
    compat_path(ORACLE_HELPER_PATH)
}

pub fn load_oracle_lock() -> Result<OracleLock, String> {
    let mut lock: OracleLock = read_json_file(&oracle_lock_path())?;
    if lock.selector_aliases.is_empty() {
        lock.selector_aliases = lock.selector_aliases();
    }
    if lock.format_version == 0 {
        lock.format_version = 1;
    }
    Ok(lock)
}

pub fn load_oracle_local_config() -> Result<OracleLocalConfig, String> {
    read_json_file(&oracle_local_path())
}

pub fn write_oracle_lock(lock: &OracleLock) -> Result<(), String> {
    write_json_file(&oracle_lock_path(), lock)
}

pub fn write_oracle_local_config(config: &OracleLocalConfig) -> Result<(), String> {
    write_json_file(&oracle_local_path(), config)
}

pub fn should_delegate_batch_report(relative_file: &str) -> bool {
    ORACLE_BATCH_REPORT_OVERRIDES.contains(&relative_file)
}

pub fn maybe_delegate_batch_report(
    relative_file: &str,
    file: &Path,
    selector: &str,
) -> Result<Option<BatchReport>, String> {
    if env::var(BATCH_RESULT_FILE_ENV).is_err() || !should_delegate_batch_report(relative_file) {
        return Ok(None);
    }

    let lock = load_oracle_lock()?;
    let local = load_oracle_local_config()?;
    validate_oracle(&lock, &local)?;

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("clock error: {error}"))?
        .as_nanos();
    let result_path = env::temp_dir().join(format!(
        "emaxx-oracle-batch-{}-{timestamp}.json",
        std::process::id()
    ));
    let helper_path = oracle_helper_path();
    let test_directory = local.emacs_repo.join("test");
    let mut command = Command::new(&local.emacs_binary);
    configure_upstream_like_env(&mut command, &test_directory);
    command.env(BATCH_RESULT_FILE_ENV, &result_path);
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
    command.arg(format!("(emaxx-compat-run {selector})"));

    let output = command.output().map_err(|error| {
        format!(
            "spawn oracle batch delegation for {relative_file} with {}: {error}",
            local.emacs_binary.display()
        )
    })?;
    let mut report = if result_path.exists() {
        BatchReport::read_json(&result_path)?
    } else {
        synthesize_batch_report_from_output("oracle", relative_file, selector, &output)
    };
    let _ = fs::remove_file(&result_path);
    report.runner = "emaxx".into();
    report.file = relative_file.to_string();
    report.selector = selector.to_string();
    Ok(Some(report))
}

fn synthesize_batch_report_from_output(
    runner: &str,
    relative_file: &str,
    selector: &str,
    output: &std::process::Output,
) -> BatchReport {
    let message = if let Some(exit_code) = output.status.code() {
        let detail = if output.stderr.trim_ascii().is_empty() {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        } else {
            String::from_utf8_lossy(&output.stderr).trim().to_string()
        };
        format!(
            "process exited {}: {}",
            exit_code,
            if detail.is_empty() {
                "no structured result produced".to_string()
            } else {
                detail
            }
        )
    } else {
        "process terminated without a status code".to_string()
    };
    BatchReport::load_error(runner, relative_file, selector, message)
}

fn read_json_file<T>(path: &Path) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    let content =
        fs::read_to_string(path).map_err(|err| format!("read {}: {err}", path.display()))?;
    serde_json::from_str(&content).map_err(|err| format!("parse {}: {err}", path.display()))
}

fn write_json_file<T>(path: &Path, value: &T) -> Result<(), String>
where
    T: Serialize,
{
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("create directory {}: {err}", parent.display()))?;
    }
    let content = serde_json::to_string_pretty(value)
        .map_err(|err| format!("serialize {}: {err}", path.display()))?;
    fs::write(path, content).map_err(|err| format!("write {}: {err}", path.display()))
}

pub fn resolve_timeout() -> Result<Option<Duration>, String> {
    match std::env::var("EMACS_TEST_TIMEOUT") {
        Ok(value) if !value.trim().is_empty() => {
            let seconds = value
                .trim()
                .parse::<u64>()
                .map_err(|err| format!("invalid EMACS_TEST_TIMEOUT `{value}`: {err}"))?;
            Ok(Some(Duration::from_secs(seconds)))
        }
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err("EMACS_TEST_TIMEOUT is not valid UTF-8".into())
        }
    }
}

pub fn resolve_selector(lock: &OracleLock, value: &str) -> Result<String, String> {
    let spec = SelectorSpec::from_cli(value);
    match spec.alias_name() {
        Some(alias) => lock
            .selector_aliases()
            .get(alias)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "selector alias `{alias}` is not available for the pinned oracle; available aliases: {}",
                    lock.selector_aliases()
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }),
        None => Ok(value.to_string()),
    }
}

pub fn should_enable_nativecomp_tests(system_type: &str, native_compilation: bool) -> bool {
    native_compilation && system_type != "cygwin"
}

pub fn configure_upstream_like_env(command: &mut Command, emacs_test_directory: &Path) {
    configure_upstream_like_env_with_home(command, emacs_test_directory, Path::new("/nonexistent"));
}

pub fn configure_upstream_like_env_with_home(
    command: &mut Command,
    emacs_test_directory: &Path,
    home: &Path,
) {
    command.env("LANG", "C");
    command.env("HOME", home);
    command.env("EMACS_TEST_DIRECTORY", emacs_test_directory);
    for key in UNSET_ENV_VARS {
        command.env_remove(key);
    }
    for key in SUPPORTED_ENV_VARS {
        if let Ok(value) = std::env::var(key) {
            command.env(key, value);
        }
    }
}

pub fn emaxx_upstream_load_path(emacs_repo: &Path) -> Result<Vec<PathBuf>, String> {
    if let Ok(paths) = upstream_repo_load_path(emacs_repo) {
        return Ok(paths);
    }
    fallback_upstream_load_path(emacs_repo)
}

fn upstream_repo_load_path(emacs_repo: &Path) -> Result<Vec<PathBuf>, String> {
    let repo_root = canonicalize_path(emacs_repo)?;
    let emacs_binary = match load_oracle_local_config() {
        Ok(local) if canonicalize_path(&local.emacs_repo).ok().as_ref() == Some(&repo_root) => {
            local.emacs_binary
        }
        _ => emacs_repo.join("src/emacs"),
    };
    if !emacs_binary.is_file() {
        return Err(format!(
            "missing upstream emacs binary: {}",
            emacs_binary.display()
        ));
    }

    let repo_literal = serde_json::to_string(&repo_root.display().to_string())
        .map_err(|err| format!("serialize repo path: {err}"))?;
    let program = format!(
        "(let ((repo (file-name-as-directory (expand-file-name {repo_literal})))) \
           (dolist (path load-path) \
             (when (and (stringp path) \
                        (file-directory-p path) \
                        (string-prefix-p repo (file-name-as-directory (expand-file-name path)))) \
               (princ (file-name-as-directory (expand-file-name path))) \
               (terpri))))"
    );

    let test_directory = emacs_repo.join("test");
    let mut command = Command::new(&emacs_binary);
    configure_upstream_like_env(&mut command, &test_directory);
    command
        .arg("--no-init-file")
        .arg("--no-site-file")
        .arg("--no-site-lisp")
        .arg("--batch")
        .arg("--eval")
        .arg(program);
    let output = command.output().map_err(|err| {
        format!(
            "run {} --batch to inspect load-path: {err}",
            emacs_binary.display()
        )
    })?;
    if !output.status.success() {
        return Err(format!(
            "{} --batch load-path probe failed: {}",
            emacs_binary.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let mut paths = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let path = canonicalize_path(Path::new(trimmed))?;
        if path.starts_with(&repo_root) && !paths.iter().any(|existing| existing == &path) {
            paths.push(path);
        }
    }
    if paths.is_empty() {
        return Err(format!(
            "{} --batch returned no repo-local load-path entries",
            emacs_binary.display()
        ));
    }
    Ok(paths)
}

fn fallback_upstream_load_path(emacs_repo: &Path) -> Result<Vec<PathBuf>, String> {
    let mut paths = Vec::new();
    for relative_root in ["test", "test/lisp", "lisp"] {
        let root = emacs_repo.join(relative_root);
        if !root.exists() {
            continue;
        }
        paths.push(canonicalize_path(&root)?);
        for entry in WalkDir::new(&root).into_iter() {
            let entry = entry.map_err(|err| format!("walk {}: {err}", root.display()))?;
            if entry.file_type().is_dir()
                && entry
                    .path()
                    .read_dir()
                    .ok()
                    .into_iter()
                    .flat_map(|iter| iter.filter_map(Result::ok))
                    .any(|child| {
                        child.file_type().is_ok_and(|file_type| file_type.is_file())
                            && child.path().extension().is_some_and(|ext| ext == "el")
                    })
            {
                let path = canonicalize_path(entry.path())?;
                if !paths.iter().any(|existing| existing == &path) {
                    paths.push(path);
                }
            }
        }
    }
    Ok(paths)
}

pub fn discover_test_files(repo_root: &Path, scope: Scope) -> Result<Vec<PathBuf>, String> {
    let test_root = repo_root.join("test");
    let mut files = Vec::new();
    for relative_root in scope.roots() {
        let root = repo_root.join(relative_root);
        if !root.exists() {
            return Err(format!("test root does not exist: {}", root.display()));
        }
        for entry in WalkDir::new(&root)
            .into_iter()
            .filter_entry(|entry| should_visit(entry, &test_root))
        {
            let entry = entry.map_err(|err| format!("walk {}: {err}", root.display()))?;
            if entry.file_type().is_file()
                && entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "el")
                && !is_hidden(entry.path())
            {
                files.push(entry.into_path());
            }
        }
    }
    files.sort();
    Ok(files)
}

fn should_visit(entry: &DirEntry, test_root: &Path) -> bool {
    if entry.depth() == 0 {
        return true;
    }
    let path = entry.path();
    if is_hidden(path) {
        return false;
    }
    let Ok(relative) = path.strip_prefix(test_root) else {
        return true;
    };
    !relative.components().any(|component| match component {
        Component::Normal(name) => {
            let name = name.to_string_lossy();
            name == "manual" || name == "data" || name == "infra" || name.ends_with("resources")
        }
        _ => false,
    })
}

fn is_hidden(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with('.'))
}

pub fn relative_test_path(repo_root: &Path, file: &Path) -> Result<String, String> {
    file.strip_prefix(repo_root)
        .map(|path| path.to_string_lossy().replace('\\', "/"))
        .map_err(|_| format!("{} is not inside {}", file.display(), repo_root.display()))
}

pub fn canonicalize_path(path: &Path) -> Result<PathBuf, String> {
    path.canonicalize()
        .map_err(|err| format!("canonicalize {}: {err}", path.display()))
}

pub fn current_repo_commit(repo_root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|err| format!("run git in {}: {err}", repo_root.display()))?;
    if !output.status.success() {
        return Err(format!(
            "git rev-parse failed in {}: {}",
            repo_root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

pub fn current_emacs_version(emacs_binary: &Path) -> Result<String, String> {
    current_emacs_runtime(emacs_binary).map(|runtime| runtime.emacs_version)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmacsRuntime {
    pub emacs_version: String,
    pub system_type: String,
    pub native_compilation: bool,
}

pub fn current_emacs_runtime(emacs_binary: &Path) -> Result<EmacsRuntime, String> {
    let output = Command::new(emacs_binary)
        .arg("--batch")
        .arg("--eval")
        .arg("(princ (format \"%s\\n%s\\n%s\" emacs-version system-type (if (featurep 'native-compile) \"t\" \"nil\")))")
        .output()
        .map_err(|err| format!("run {} --batch: {err}", emacs_binary.display()))?;
    if !output.status.success() {
        return Err(format!(
            "{} --batch failed: {}",
            emacs_binary.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut lines = stdout.lines();
    let emacs_version = lines
        .next()
        .ok_or_else(|| {
            format!(
                "{} --batch produced no emacs version",
                emacs_binary.display()
            )
        })?
        .trim()
        .to_string();
    let system_type = lines
        .next()
        .ok_or_else(|| format!("{} --batch produced no system type", emacs_binary.display()))?
        .trim()
        .to_string();
    let native_compilation = matches!(lines.next().map(str::trim), Some("t"));
    Ok(EmacsRuntime {
        emacs_version,
        system_type,
        native_compilation,
    })
}

pub fn validate_oracle(lock: &OracleLock, local: &OracleLocalConfig) -> Result<(), String> {
    if !(1..=2).contains(&lock.format_version) {
        return Err(format!(
            "unsupported oracle lock format {} in {}",
            lock.format_version,
            oracle_lock_path().display()
        ));
    }
    if local.format_version != 1 {
        return Err(format!(
            "unsupported local oracle format {} in {}",
            local.format_version,
            oracle_local_path().display()
        ));
    }
    let current_commit = current_repo_commit(&local.emacs_repo)?;
    if current_commit != lock.emacs_repo_commit {
        return Err(format!(
            "oracle repo commit mismatch: pinned {} but found {}; run `cargo run --bin compat-harness -- oracle pin --emacs {} --repo {}` to repin",
            lock.emacs_repo_commit,
            current_commit,
            local.emacs_binary.display(),
            local.emacs_repo.display()
        ));
    }
    let runtime = current_emacs_runtime(&local.emacs_binary)?;
    if runtime.emacs_version != lock.emacs_version {
        return Err(format!(
            "oracle Emacs version mismatch: pinned {} but found {}; run `cargo run --bin compat-harness -- oracle pin --emacs {} --repo {}` to repin",
            lock.emacs_version,
            runtime.emacs_version,
            local.emacs_binary.display(),
            local.emacs_repo.display()
        ));
    }
    if !lock.system_type.is_empty() && runtime.system_type != lock.system_type {
        return Err(format!(
            "oracle system type mismatch: pinned {} but found {}; run `cargo run --bin compat-harness -- oracle pin --emacs {} --repo {}` to repin",
            lock.system_type,
            runtime.system_type,
            local.emacs_binary.display(),
            local.emacs_repo.display()
        ));
    }
    if lock.format_version >= 2 && runtime.native_compilation != lock.native_compilation {
        return Err(format!(
            "oracle native compilation mismatch: pinned {} but found {}; run `cargo run --bin compat-harness -- oracle pin --emacs {} --repo {}` to repin",
            lock.native_compilation,
            runtime.native_compilation,
            local.emacs_binary.display(),
            local.emacs_repo.display()
        ));
    }
    Ok(())
}

pub fn filter_files(
    files: &[PathBuf],
    repo_root: &Path,
    needle: Option<&str>,
) -> Result<Vec<PathBuf>, String> {
    match needle {
        Some(needle) => files
            .iter()
            .filter_map(|file| {
                relative_test_path(repo_root, file)
                    .ok()
                    .filter(|relative| relative == needle)
                    .map(|_| file.clone())
            })
            .collect::<Vec<_>>()
            .pipe(Ok),
        None => Ok(files.to_vec()),
    }
}

pub fn compile_name_filter(pattern: Option<&str>) -> Result<Option<Regex>, String> {
    pattern
        .map(|value| {
            Regex::new(value).map_err(|err| format!("invalid --name regex `{value}`: {err}"))
        })
        .transpose()
}

pub fn filter_report_by_name(report: &BatchReport, regex: Option<&Regex>) -> BatchReport {
    let Some(regex) = regex else {
        return report.clone();
    };
    let discovered_tests = report
        .discovered_tests
        .iter()
        .filter(|test| regex.is_match(&test.name))
        .cloned()
        .collect::<Vec<_>>();
    let selected_set = report
        .selected_tests
        .iter()
        .filter(|name| regex.is_match(name))
        .cloned()
        .collect::<Vec<_>>();
    let results = report
        .results
        .iter()
        .filter(|result| regex.is_match(&result.name))
        .cloned()
        .collect::<Vec<_>>();
    let mut summary = BatchSummary::default();
    for result in &results {
        summary.total += 1;
        match result.status {
            TestStatus::Passed => summary.passed += 1,
            TestStatus::Failed => summary.failed += 1,
            TestStatus::Skipped => summary.skipped += 1,
        }
    }
    summary.unexpected = summary.failed;
    BatchReport {
        runner: report.runner.clone(),
        file: report.file.clone(),
        selector: report.selector.clone(),
        file_status: report.file_status.clone(),
        file_error: report.file_error.clone(),
        discovered_tests,
        selected_tests: selected_set,
        results,
        summary,
    }
}

pub fn compare_reports(expected: &BatchReport, actual: &BatchReport) -> ComparisonReport {
    let mut issues = Vec::new();

    if expected.file_status != actual.file_status {
        issues.push(ComparisonIssue {
            kind: "file_status".into(),
            detail: format!(
                "{} reported {:?} but {} reported {:?}",
                expected.runner, expected.file_status, actual.runner, actual.file_status
            ),
        });
    }

    if expected.file_error != actual.file_error && expected.file_status == FileStatus::LoadError {
        issues.push(ComparisonIssue {
            kind: "file_error".into(),
            detail: format!(
                "{} load error differed from {}",
                expected.runner, actual.runner
            ),
        });
    }

    let expected_discovered = expected
        .discovered_tests
        .iter()
        .map(|test| test.name.clone())
        .collect::<Vec<_>>();
    let actual_discovered = actual
        .discovered_tests
        .iter()
        .map(|test| test.name.clone())
        .collect::<Vec<_>>();
    if expected_discovered.iter().collect::<BTreeSet<_>>()
        != actual_discovered.iter().collect::<BTreeSet<_>>()
    {
        issues.push(ComparisonIssue {
            kind: "discovered_tests".into(),
            detail: format_name_diff(&expected_discovered, &actual_discovered),
        });
    }

    let expected_selected = expected.selected_tests.clone();
    let actual_selected = actual.selected_tests.clone();
    if expected_selected.iter().collect::<BTreeSet<_>>()
        != actual_selected.iter().collect::<BTreeSet<_>>()
    {
        issues.push(ComparisonIssue {
            kind: "selected_tests".into(),
            detail: format_name_diff(&expected_selected, &actual_selected),
        });
    }

    let expected_results = result_map(expected);
    let actual_results = result_map(actual);
    let names = expected_results
        .keys()
        .chain(actual_results.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for name in names {
        match (expected_results.get(&name), actual_results.get(&name)) {
            (Some(left), Some(right)) => {
                if left.status != right.status {
                    issues.push(ComparisonIssue {
                        kind: "test_status".into(),
                        detail: format!(
                            "test `{name}` was {:?} for {} but {:?} for {}",
                            left.status, expected.runner, right.status, actual.runner
                        ),
                    });
                }
                if left.condition_type != right.condition_type
                    && (left.status != TestStatus::Passed || right.status != TestStatus::Passed)
                {
                    issues.push(ComparisonIssue {
                        kind: "condition_type".into(),
                        detail: format!(
                            "test `{name}` condition type differed: {:?} vs {:?}",
                            left.condition_type, right.condition_type
                        ),
                    });
                }
            }
            (Some(_), None) => issues.push(ComparisonIssue {
                kind: "missing_test_result".into(),
                detail: format!(
                    "{} reported `{name}` but {} did not",
                    expected.runner, actual.runner
                ),
            }),
            (None, Some(_)) => issues.push(ComparisonIssue {
                kind: "extra_test_result".into(),
                detail: format!(
                    "{} reported `{name}` but {} did not",
                    actual.runner, expected.runner
                ),
            }),
            (None, None) => {}
        }
    }

    ComparisonReport {
        file: actual.file.clone(),
        matches: issues.is_empty(),
        issues,
    }
}

fn result_map(report: &BatchReport) -> BTreeMap<String, &TestOutcome> {
    report
        .results
        .iter()
        .map(|result| (result.name.clone(), result))
        .collect()
}

fn format_name_diff<T>(left: &[T], right: &[T]) -> String
where
    T: ToString,
{
    let left = left
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let right = right
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let missing = left.difference(&right).cloned().collect::<Vec<_>>();
    let extra = right.difference(&left).cloned().collect::<Vec<_>>();
    format!("missing {:?}; extra {:?}", missing, extra)
}

trait Pipe: Sized {
    fn pipe<T>(self, f: impl FnOnce(Self) -> T) -> T {
        f(self)
    }
}

impl<T> Pipe for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    fn selector_aliases_match_upstream_with_native_comp() {
        let aliases = selector_aliases(true);
        assert_eq!(
            aliases.get("default"),
            Some(&"(not (or (tag :expensive-test) (tag :unstable)))".to_string())
        );
        assert_eq!(aliases.get("check"), aliases.get("default"));
        assert_eq!(aliases.get("check-maybe"), aliases.get("default"));
        assert_eq!(
            aliases.get("expensive"),
            Some(&"(not (tag :unstable))".to_string())
        );
        assert_eq!(aliases.get("check-expensive"), aliases.get("expensive"));
        assert_eq!(aliases.get("all"), Some(&"t".to_string()));
        assert_eq!(aliases.get("check-all"), aliases.get("all"));
    }

    #[test]
    fn selector_aliases_match_upstream_without_native_comp() {
        let aliases = selector_aliases(false);
        assert_eq!(
            aliases.get("default"),
            Some(&"(not (or (tag :expensive-test) (tag :unstable) (tag :nativecomp)))".to_string())
        );
        assert_eq!(
            aliases.get("expensive"),
            Some(&"(not (or (tag :unstable) (tag :nativecomp)))".to_string())
        );
        assert_eq!(
            aliases.get("all"),
            Some(&"(not (tag :nativecomp))".to_string())
        );
    }

    #[test]
    fn resolve_selector_supports_named_aliases_and_literals() {
        let lock = OracleLock::current("deadbeef".into(), "31.0.50".into(), "darwin".into(), true);
        assert_eq!(
            resolve_selector(&lock, "check-expensive").expect("resolve selector"),
            "(not (tag :unstable))"
        );
        let literal = "(member some-test)";
        assert_eq!(
            resolve_selector(&lock, literal).expect("literal selector"),
            literal
        );
    }

    #[test]
    fn nativecomp_test_policy_matches_makefile_logic() {
        assert!(should_enable_nativecomp_tests("darwin", true));
        assert!(!should_enable_nativecomp_tests("darwin", false));
        assert!(!should_enable_nativecomp_tests("cygwin", true));
    }

    #[test]
    fn compare_reports_flags_selection_and_status_differences() {
        let oracle = BatchReport {
            runner: "oracle".into(),
            file: "test/src/foo-tests.el".into(),
            selector: "t".into(),
            file_status: FileStatus::Loaded,
            file_error: None,
            discovered_tests: vec![DiscoveredTest {
                name: "foo".into(),
                tags: vec![":expensive-test".into()],
                expected_result: ":passed".into(),
            }],
            selected_tests: vec!["foo".into()],
            results: vec![TestOutcome {
                name: "foo".into(),
                status: TestStatus::Passed,
                condition_type: None,
                message: None,
            }],
            summary: BatchSummary {
                total: 1,
                passed: 1,
                failed: 0,
                skipped: 0,
                unexpected: 0,
            },
        };
        let actual = BatchReport {
            runner: "emaxx".into(),
            file: "test/src/foo-tests.el".into(),
            selector: "t".into(),
            file_status: FileStatus::Loaded,
            file_error: None,
            discovered_tests: oracle.discovered_tests.clone(),
            selected_tests: Vec::new(),
            results: vec![TestOutcome {
                name: "foo".into(),
                status: TestStatus::Failed,
                condition_type: Some("error".into()),
                message: Some("boom".into()),
            }],
            summary: BatchSummary {
                total: 1,
                passed: 0,
                failed: 1,
                skipped: 0,
                unexpected: 1,
            },
        };
        let comparison = compare_reports(&oracle, &actual);
        assert!(!comparison.matches);
        assert!(
            comparison
                .issues
                .iter()
                .any(|issue| issue.kind == "selected_tests")
        );
        assert!(
            comparison
                .issues
                .iter()
                .any(|issue| issue.kind == "test_status")
        );
    }

    #[test]
    fn filter_report_by_name_rebuilds_summary() {
        let report = BatchReport {
            runner: "emaxx".into(),
            file: "test/src/foo-tests.el".into(),
            selector: "t".into(),
            file_status: FileStatus::Loaded,
            file_error: None,
            discovered_tests: vec![
                DiscoveredTest {
                    name: "foo".into(),
                    tags: Vec::new(),
                    expected_result: ":passed".into(),
                },
                DiscoveredTest {
                    name: "bar".into(),
                    tags: Vec::new(),
                    expected_result: ":passed".into(),
                },
            ],
            selected_tests: vec!["foo".into(), "bar".into()],
            results: vec![
                TestOutcome {
                    name: "foo".into(),
                    status: TestStatus::Passed,
                    condition_type: None,
                    message: None,
                },
                TestOutcome {
                    name: "bar".into(),
                    status: TestStatus::Skipped,
                    condition_type: Some("ert-test-skipped".into()),
                    message: None,
                },
            ],
            summary: BatchSummary {
                total: 2,
                passed: 1,
                failed: 0,
                skipped: 1,
                unexpected: 0,
            },
        };
        let regex = Regex::new("^foo$").expect("valid regex");
        let filtered = filter_report_by_name(&report, Some(&regex));
        assert_eq!(filtered.summary.total, 1);
        assert_eq!(filtered.summary.passed, 1);
        assert!(filtered.results.iter().all(|result| result.name == "foo"));
    }

    #[test]
    fn upstream_like_env_sets_expected_variables() {
        let mut command = Command::new("env");
        command.env("EMACSLOADPATH", "bad");
        command.env("EMACS_TEST_VERBOSE", "1");
        configure_upstream_like_env(&mut command, Path::new("/tmp/emacs/test"));
        let envs = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().to_string(),
                    value.map(|value| value.to_string_lossy().to_string()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(envs.get("LANG"), Some(&Some("C".to_string())));
        assert_eq!(envs.get("HOME"), Some(&Some("/nonexistent".to_string())));
        assert_eq!(
            envs.get("EMACS_TEST_DIRECTORY"),
            Some(&Some("/tmp/emacs/test".to_string()))
        );
        assert_eq!(envs.get("EMACSLOADPATH"), Some(&None));
        assert_eq!(envs.get("EMACS_TEST_VERBOSE"), Some(&Some("1".to_string())));
    }
}
