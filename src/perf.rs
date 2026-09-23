use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::compat;

pub const PERF_SCENARIO_MANIFEST_PATH: &str = "compat/perf_scenarios.json";
pub const PERF_RESULT_FILE_ENV: &str = "EMAXX_PERF_RESULT_FILE";
pub const PERF_HARNESS_LOAD_PREFIX: &str = "harness:";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerfScenarioManifest {
    pub format_version: u32,
    pub scenarios: Vec<PerfScenario>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerfScenario {
    pub id: String,
    pub description: String,
    pub group: String,
    pub tier: PerfTier,
    pub oracle_adapter: String,
    pub emaxx_adapter: Option<String>,
    #[serde(default)]
    pub load_files: Vec<String>,
    #[serde(default)]
    pub params: BTreeMap<String, JsonValue>,
    pub warmup: u32,
    pub samples: u32,
    pub timeout_secs: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PerfTier {
    Comparable,
    Provisional,
    OracleOnly,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PerfRunStatus {
    Completed,
    Unsupported,
    Failed,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PerfCaseStatus {
    Completed,
    Unsupported,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerfRunReport {
    pub runner: String,
    pub scenario_id: String,
    pub tier: PerfTier,
    pub status: PerfRunStatus,
    pub cases: Vec<PerfCaseReport>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerfCaseReport {
    pub case_id: String,
    pub status: PerfCaseStatus,
    pub metric_unit: String,
    pub samples: Vec<f64>,
    pub min: Option<f64>,
    pub median: Option<f64>,
    pub mean: Option<f64>,
    pub p95: Option<f64>,
    pub max: Option<f64>,
    pub gc_count: u64,
    pub gc_seconds: f64,
    pub notes: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PerfComparisonClass {
    Faster,
    Parity,
    Slower,
    Unsupported,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerfCaseComparison {
    pub case_id: String,
    pub class: PerfComparisonClass,
    pub oracle_median: Option<f64>,
    pub emaxx_median: Option<f64>,
    /// Comparable steady-state ratio.  Startup/image construction is never
    /// represented by a `comparable` scenario, so this is eligible for the
    /// compatibility-frontier 2x investigation rule.
    pub emaxx_over_oracle: Option<f64>,
    pub exceeds_two_x: bool,
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PerfComparisonSummary {
    pub comparable_cases: usize,
    pub faster: usize,
    pub parity: usize,
    pub slower: usize,
    pub over_two_x: usize,
    pub unsupported: usize,
    pub failed: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerfComparisonReport {
    pub scenario_id: String,
    pub tier: PerfTier,
    pub case_results: Vec<PerfCaseComparison>,
    pub summary: PerfComparisonSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PerfScenarioTotals {
    pub scenarios: usize,
    pub cases: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PerfRunSummary {
    pub comparable: PerfScenarioTotals,
    pub provisional: PerfScenarioTotals,
    pub oracle_only: PerfScenarioTotals,
    pub faster: usize,
    pub parity: usize,
    pub slower: usize,
    pub over_two_x: usize,
    pub unsupported: usize,
    pub failed: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerfSummaryReport {
    pub timestamp: u64,
    pub oracle_emacs_version: String,
    pub oracle_emacs_repo_commit: String,
    pub emaxx_git_commit: String,
    pub target_profile: String,
    pub os: String,
    pub arch: String,
    pub cpu_model: String,
    pub summary: PerfRunSummary,
    pub scenarios: Vec<PerfScenarioSummary>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct PerfScenarioSummary {
    pub scenario_id: String,
    pub tier: PerfTier,
    pub oracle_status: PerfRunStatus,
    pub emaxx_status: Option<PerfRunStatus>,
    pub comparison: Option<PerfComparisonSummary>,
}

impl PerfScenarioManifest {
    pub fn load() -> Result<Self, String> {
        let path = compat::compat_path(PERF_SCENARIO_MANIFEST_PATH);
        let data = fs::read_to_string(&path)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        Self::from_json_str(&data).map_err(|error| format!("parse {}: {error}", path.display()))
    }

    pub fn from_json_str(data: &str) -> Result<Self, String> {
        let manifest: Self = serde_json::from_str(data).map_err(|error| error.to_string())?;
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != 1 {
            return Err(format!(
                "unsupported perf scenario format_version {}; expected 1",
                self.format_version
            ));
        }
        let mut ids = BTreeSet::new();
        for scenario in &self.scenarios {
            if scenario.id.trim().is_empty() {
                return Err("scenario id must not be empty".into());
            }
            if !ids.insert(scenario.id.clone()) {
                return Err(format!("duplicate scenario id `{}`", scenario.id));
            }
            if scenario.oracle_adapter.trim().is_empty() {
                return Err(format!(
                    "scenario `{}` is missing oracle_adapter",
                    scenario.id
                ));
            }
            if scenario.warmup == 0 {
                return Err(format!("scenario `{}` must use warmup >= 1", scenario.id));
            }
            if scenario.samples == 0 {
                return Err(format!("scenario `{}` must use samples >= 1", scenario.id));
            }
            if scenario.timeout_secs == 0 {
                return Err(format!(
                    "scenario `{}` must use timeout_secs >= 1",
                    scenario.id
                ));
            }
            if scenario.tier == PerfTier::Comparable && scenario.emaxx_adapter.is_none() {
                return Err(format!(
                    "scenario `{}` is comparable but has no emaxx_adapter",
                    scenario.id
                ));
            }
        }
        Ok(())
    }

    pub fn find(&self, id: &str) -> Option<&PerfScenario> {
        self.scenarios.iter().find(|scenario| scenario.id == id)
    }
}

impl PerfScenario {
    pub fn param_u64(&self, key: &str) -> Option<u64> {
        self.params.get(key).and_then(JsonValue::as_u64)
    }

    pub fn param_str(&self, key: &str) -> Option<&str> {
        self.params.get(key).and_then(JsonValue::as_str)
    }
}

impl PerfCaseReport {
    pub fn completed(
        case_id: impl Into<String>,
        metric_unit: impl Into<String>,
        samples: Vec<f64>,
        gc_count: u64,
        gc_seconds: f64,
        notes: Option<String>,
    ) -> Self {
        let summary = SampleSummary::compute(&samples);
        Self {
            case_id: case_id.into(),
            status: PerfCaseStatus::Completed,
            metric_unit: metric_unit.into(),
            samples,
            min: summary.as_ref().map(|value| value.min),
            median: summary.as_ref().map(|value| value.median),
            mean: summary.as_ref().map(|value| value.mean),
            p95: summary.as_ref().map(|value| value.p95),
            max: summary.as_ref().map(|value| value.max),
            gc_count,
            gc_seconds,
            notes,
        }
    }

    pub fn unsupported(case_id: impl Into<String>, notes: impl Into<String>) -> Self {
        Self {
            case_id: case_id.into(),
            status: PerfCaseStatus::Unsupported,
            metric_unit: "seconds".into(),
            samples: Vec::new(),
            min: None,
            median: None,
            mean: None,
            p95: None,
            max: None,
            gc_count: 0,
            gc_seconds: 0.0,
            notes: Some(notes.into()),
        }
    }

    pub fn failed(case_id: impl Into<String>, notes: impl Into<String>) -> Self {
        Self {
            case_id: case_id.into(),
            status: PerfCaseStatus::Failed,
            metric_unit: "seconds".into(),
            samples: Vec::new(),
            min: None,
            median: None,
            mean: None,
            p95: None,
            max: None,
            gc_count: 0,
            gc_seconds: 0.0,
            notes: Some(notes.into()),
        }
    }
}

impl PerfRunReport {
    /// Check the report against the requested workload, rather than trusting
    /// a child's status string or summary fields.  Raw reports are retained
    /// by the harness even when this check rejects them.
    pub fn validate_completed(&self, runner: &str, scenario: &PerfScenario) -> Result<(), String> {
        if self.runner != runner || self.scenario_id != scenario.id || self.tier != scenario.tier {
            return Err("performance report identity does not match the request".into());
        }
        if self.status != PerfRunStatus::Completed || self.cases.is_empty() {
            return Err("performance report did not complete every requested case".into());
        }
        let expected = expand_scenario_cases(scenario)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let actual = self
            .cases
            .iter()
            .map(|case| case.case_id.clone())
            .collect::<BTreeSet<_>>();
        if actual.len() != self.cases.len() {
            return Err("performance report contains duplicate cases".into());
        }
        if actual != expected {
            return Err(format!(
                "performance case inventory mismatch: expected {expected:?}, got {actual:?}"
            ));
        }
        for (key, expected) in [
            ("n", scenario.param_u64("n").unwrap_or(4096)),
            ("warmup", u64::from(scenario.warmup)),
            ("samples", u64::from(scenario.samples)),
        ] {
            if self
                .metadata
                .get(key)
                .and_then(|value| value.parse::<u64>().ok())
                != Some(expected)
            {
                return Err(format!("performance report has the wrong {key}"));
            }
        }
        for case in &self.cases {
            if case.status != PerfCaseStatus::Completed
                || case.metric_unit != "seconds"
                || case.samples.len() != scenario.samples as usize
                || case
                    .samples
                    .iter()
                    .any(|sample| !sample.is_finite() || *sample <= 0.0)
                || !case.gc_seconds.is_finite()
                || case.gc_seconds < 0.0
            {
                return Err(format!(
                    "{} has incomplete or invalid samples",
                    case.case_id
                ));
            }
            let summary = SampleSummary::compute(&case.samples)
                .ok_or_else(|| format!("{} has no samples", case.case_id))?;
            for (reported, computed) in [
                (case.min, summary.min),
                (case.median, summary.median),
                (case.mean, summary.mean),
                (case.p95, summary.p95),
                (case.max, summary.max),
            ] {
                if reported.is_none_or(|value| {
                    !value.is_finite() || (value - computed).abs() > 1e-12 * computed.abs().max(1.0)
                }) {
                    return Err(format!(
                        "{} summary disagrees with its raw samples",
                        case.case_id
                    ));
                }
            }
        }
        Ok(())
    }

    pub fn unsupported(
        runner: &str,
        scenario: &PerfScenario,
        notes: impl Into<String>,
        case_ids: Vec<String>,
    ) -> Self {
        let note = notes.into();
        let cases = if case_ids.is_empty() {
            Vec::new()
        } else {
            case_ids
                .into_iter()
                .map(|case_id| PerfCaseReport::unsupported(case_id, note.clone()))
                .collect()
        };
        Self {
            runner: runner.to_string(),
            scenario_id: scenario.id.clone(),
            tier: scenario.tier,
            status: PerfRunStatus::Unsupported,
            cases,
            metadata: BTreeMap::from([("notes".into(), note)]),
        }
    }

    pub fn failed(runner: &str, scenario: &PerfScenario, notes: impl Into<String>) -> Self {
        let note = notes.into();
        Self {
            runner: runner.to_string(),
            scenario_id: scenario.id.clone(),
            tier: scenario.tier,
            status: PerfRunStatus::Failed,
            cases: Vec::new(),
            metadata: BTreeMap::from([("notes".into(), note)]),
        }
    }

    pub fn write_json(&self, path: &Path) -> Result<(), String> {
        write_json(path, self, "perf run report")
    }

    pub fn read_json(path: &Path) -> Result<Self, String> {
        read_json(path, "perf run report")
    }
}

pub fn write_json(path: &Path, value: &impl Serialize, label: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(value)
        .map_err(|error| format!("serialize {label} {}: {error}", path.display()))?;
    fs::write(path, json).map_err(|error| format!("write {}: {error}", path.display()))
}

pub fn read_json<T>(path: &Path, label: &str) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    let json = fs::read_to_string(path)
        .map_err(|error| format!("read {label} {}: {error}", path.display()))?;
    serde_json::from_str(&json)
        .map_err(|error| format!("parse {label} {}: {error}", path.display()))
}

pub fn make_artifact_root() -> Result<PathBuf, String> {
    let timestamp = current_timestamp_secs()?;
    let root = compat::project_root()
        .join("target")
        .join("perf")
        .join(format!("run-{timestamp}"));
    fs::create_dir_all(&root).map_err(|error| format!("create {}: {error}", root.display()))?;
    Ok(root)
}

pub fn scenario_artifact_dir(root: &Path, scenario_id: &str) -> PathBuf {
    root.join(scenario_id).with_extension("perf")
}

pub fn create_temp_home(scenario_dir: &Path) -> Result<PathBuf, String> {
    let home = scenario_dir.join("home");
    if home.exists() {
        fs::remove_dir_all(&home).map_err(|error| format!("reset {}: {error}", home.display()))?;
    }
    fs::create_dir_all(&home).map_err(|error| format!("create {}: {error}", home.display()))?;
    Ok(home)
}

pub fn current_timestamp_secs() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("clock error: {error}"))
        .map(|duration| duration.as_secs())
}

pub fn best_effort_cpu_model() -> String {
    let candidates = [
        ("sysctl", vec!["-n", "machdep.cpu.brand_string"]),
        ("sysctl", vec!["-n", "hw.model"]),
        ("uname", vec!["-p"]),
    ];
    for (program, args) in candidates {
        let output = Command::new(program).args(args).output();
        if let Ok(output) = output
            && output.status.success()
        {
            let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !value.is_empty() {
                return value;
            }
        }
    }
    "unknown".into()
}

pub fn ensure_release_emaxx_binary(emacs_repo: &Path) -> Result<PathBuf, String> {
    let project_root = compat::project_root();
    let status = Command::new("cargo")
        .arg("build")
        .arg("--quiet")
        .arg("--locked")
        .arg("--release")
        .arg("--bin")
        .arg("emaxx")
        .arg("--bin")
        .arg("make-fingerprint")
        .env("EMAXX_GNU_SOURCE_DIRECTORY", emacs_repo)
        .current_dir(&project_root)
        .status()
        .map_err(|error| format!("build release emaxx binary: {error}"))?;
    if !status.success() {
        return Err("`cargo build --quiet --release --bin emaxx` failed".into());
    }
    let candidate = project_root.join("target").join("release").join("emaxx");
    if !candidate.exists() {
        return Err(format!(
            "expected release emaxx binary at {}",
            candidate.display()
        ));
    }
    let status = Command::new(project_root.join("tools/build-image.sh"))
        .arg(&candidate)
        .env("EMAXX_DUMP_SOURCE_DIRECTORY", emacs_repo)
        .env("EMACS_TEST_DIRECTORY", emacs_repo.join("test"))
        .current_dir(&project_root)
        .status()
        .map_err(|error| format!("build the release binary's own image: {error}"))?;
    if !status.success() {
        return Err("release image construction failed".into());
    }
    Ok(candidate)
}

pub fn compare_reports(
    scenario: &PerfScenario,
    oracle: &PerfRunReport,
    emaxx: Option<&PerfRunReport>,
) -> PerfComparisonReport {
    let mut ids = expand_scenario_cases(scenario)
        .into_iter()
        .collect::<BTreeSet<_>>();
    for case in &oracle.cases {
        ids.insert(case.case_id.clone());
    }
    if let Some(emaxx) = emaxx {
        for case in &emaxx.cases {
            ids.insert(case.case_id.clone());
        }
    }

    let oracle_cases = oracle
        .cases
        .iter()
        .map(|case| (case.case_id.clone(), case))
        .collect::<BTreeMap<_, _>>();
    let emaxx_cases = emaxx
        .map(|report| {
            report
                .cases
                .iter()
                .map(|case| (case.case_id.clone(), case))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let mut summary = PerfComparisonSummary::default();
    let mut case_results = Vec::new();

    for case_id in ids {
        let oracle_case = oracle_cases.get(&case_id).copied();
        let emaxx_case = emaxx_cases.get(&case_id).copied();
        let run_failed = oracle.status != PerfRunStatus::Completed
            || emaxx.is_some_and(|report| report.status == PerfRunStatus::Failed);
        let (class, notes) = if run_failed {
            (
                PerfComparisonClass::Failed,
                Some("an editor did not complete the requested run".into()),
            )
        } else {
            classify_case(oracle_case, emaxx_case)
        };
        let oracle_median = oracle_case.and_then(|case| case.median);
        let emaxx_median = emaxx_case.and_then(|case| case.median);
        let emaxx_over_oracle = (!run_failed)
            .then(|| comparable_ratio(oracle_median, emaxx_median))
            .flatten();
        let exceeds_two_x = scenario.tier == PerfTier::Comparable
            && emaxx_over_oracle.is_some_and(|ratio| ratio >= 2.0);
        match class {
            PerfComparisonClass::Faster => summary.faster += 1,
            PerfComparisonClass::Parity => summary.parity += 1,
            PerfComparisonClass::Slower => summary.slower += 1,
            PerfComparisonClass::Unsupported => summary.unsupported += 1,
            PerfComparisonClass::Failed => summary.failed += 1,
        }
        if scenario.tier == PerfTier::Comparable {
            summary.comparable_cases += 1;
            if exceeds_two_x {
                summary.over_two_x += 1;
            }
        }
        case_results.push(PerfCaseComparison {
            case_id,
            class,
            oracle_median,
            emaxx_median,
            emaxx_over_oracle,
            exceeds_two_x,
            notes,
        });
    }

    PerfComparisonReport {
        scenario_id: scenario.id.clone(),
        tier: scenario.tier,
        case_results,
        summary,
    }
}

fn comparable_ratio(oracle_median: Option<f64>, emaxx_median: Option<f64>) -> Option<f64> {
    let oracle = oracle_median.filter(|median| median.is_finite() && *median > 0.0)?;
    let emaxx = emaxx_median.filter(|median| median.is_finite() && *median >= 0.0)?;
    Some(emaxx / oracle)
}

pub fn expand_scenario_cases(scenario: &PerfScenario) -> Vec<String> {
    match scenario.oracle_adapter.as_str() {
        "interpreter_suite" => interpreted_case_names()
            .into_iter()
            .map(str::to_string)
            .collect(),
        "noverlay_suite" => match scenario.param_str("suite") {
            Some("perf-marker-suite") => vec![
                "perf-insert-before-marker",
                "perf-insert-after-marker",
                "perf-insert-scatter-marker",
                "perf-delete-before-marker",
                "perf-delete-after-marker",
                "perf-delete-scatter-marker",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            Some("perf-insert-delete-suite") => vec![
                "perf-insert-before",
                "perf-insert-after",
                "perf-insert-scatter",
                "perf-delete-before",
                "perf-delete-after",
                "perf-delete-scatter",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            Some("perf-realworld-suite") => vec![
                "perf-realworld-flycheck",
                "perf-realworld-make-lines-invisible",
                "perf-realworld-line-numbering",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            Some("perf-display-suite") => vec![
                "perf-display-sequential/display/scroll",
                "perf-display-sequential/display/random",
                "perf-display-sequential/face/scroll",
                "perf-display-sequential/face/random",
                "perf-display-sequential/invisible/scroll",
                "perf-display-sequential/invisible/random",
                "perf-display-random/display/scroll",
                "perf-display-random/display/random",
                "perf-display-random/face/scroll",
                "perf-display-random/face/random",
                "perf-display-random/invisible/scroll",
                "perf-display-random/invisible/random",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            Some("perf-noc-suite") => vec![
                "perf-noc-hierarchical/forward/linear",
                "perf-noc-hierarchical/forward/backnforth",
                "perf-noc-hierarchical/forward/backnforth#2",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            _ => Vec::new(),
        },
        // test/src/coding-tests.el:test-file-list at the pinned GNU revision.
        "coding_decoder" => ["ascii", "utf-8-r", "utf-8-m"]
            .into_iter()
            .flat_map(|prefix| {
                [
                    "tag-utf-8-unix.unix",
                    "tag-utf-8.unix",
                    "tag-none.unix",
                    "tag-utf-8-dos.dos",
                    "tag-utf-8.dos",
                    "tag-none.dos",
                ]
                .into_iter()
                .flat_map(move |suffix| {
                    ["without-optimization", "with-optimization"]
                        .into_iter()
                        .map(move |mode| format!("{mode}/{prefix}-{suffix}"))
                })
            })
            .collect(),
        _ => Vec::new(),
    }
}

pub fn resolve_scenario_load_file(emacs_repo: &Path, spec: &str) -> Result<PathBuf, String> {
    if let Some(path) = resolve_harness_load_file(spec)? {
        return Ok(path);
    }
    resolve_file_within(emacs_repo, Path::new(spec), "oracle repository")
}

fn resolve_harness_load_file(spec: &str) -> Result<Option<PathBuf>, String> {
    let Some(relative) = spec.strip_prefix(PERF_HARNESS_LOAD_PREFIX) else {
        return Ok(None);
    };
    resolve_file_within(
        &compat::project_root(),
        Path::new(relative),
        "Emaxx performance harness",
    )
    .map(Some)
}

fn resolve_file_within(root: &Path, relative: &Path, owner: &str) -> Result<PathBuf, String> {
    if relative.as_os_str().is_empty() || relative.is_absolute() {
        return Err(format!(
            "performance load path `{}` must be a non-empty relative path inside the {owner}",
            relative.display()
        ));
    }
    let canonical_root = compat::canonicalize_path(root)?;
    let candidate = compat::canonicalize_path(&canonical_root.join(relative))?;
    if !candidate.starts_with(&canonical_root) {
        return Err(format!(
            "performance load path `{}` escapes the {owner} root {}",
            relative.display(),
            canonical_root.display()
        ));
    }
    if !candidate.is_file() {
        return Err(format!(
            "performance load path {} is not a file",
            candidate.display()
        ));
    }
    Ok(candidate)
}

fn interpreted_case_names() -> [&'static str; 3] {
    [
        "emaxx-perf-interpreted-list-walk",
        "emaxx-perf-interpreted-cons-allocation",
        "emaxx-perf-interpreted-function-calls",
    ]
}

fn classify_case(
    oracle_case: Option<&PerfCaseReport>,
    emaxx_case: Option<&PerfCaseReport>,
) -> (PerfComparisonClass, Option<String>) {
    let Some(oracle_case) = oracle_case else {
        return (
            PerfComparisonClass::Failed,
            Some("oracle did not produce a case result".into()),
        );
    };
    if oracle_case.status != PerfCaseStatus::Completed {
        return (
            PerfComparisonClass::Failed,
            Some(
                oracle_case
                    .notes
                    .clone()
                    .unwrap_or_else(|| "oracle case did not complete".into()),
            ),
        );
    }

    let Some(emaxx_case) = emaxx_case else {
        return (
            PerfComparisonClass::Unsupported,
            Some("emaxx did not produce a comparable case result".into()),
        );
    };
    match emaxx_case.status {
        PerfCaseStatus::Unsupported => {
            return (PerfComparisonClass::Unsupported, emaxx_case.notes.clone());
        }
        PerfCaseStatus::Failed => {
            return (PerfComparisonClass::Failed, emaxx_case.notes.clone());
        }
        PerfCaseStatus::Completed => {}
    }

    let Some(oracle_median) = oracle_case.median else {
        return (
            PerfComparisonClass::Failed,
            Some("oracle case has no median".into()),
        );
    };
    let Some(emaxx_median) = emaxx_case.median else {
        return (
            PerfComparisonClass::Failed,
            Some("emaxx case has no median".into()),
        );
    };
    if emaxx_median <= 0.95 * oracle_median {
        (PerfComparisonClass::Faster, None)
    } else if emaxx_median <= 1.05 * oracle_median {
        (PerfComparisonClass::Parity, None)
    } else {
        (PerfComparisonClass::Slower, None)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct SampleSummary {
    min: f64,
    median: f64,
    mean: f64,
    p95: f64,
    max: f64,
}

impl SampleSummary {
    fn compute(samples: &[f64]) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.to_vec();
        sorted.sort_by(|left, right| left.total_cmp(right));
        let min = sorted[0];
        let max = sorted[sorted.len() - 1];
        let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
        let median = percentile(&sorted, 0.5);
        let p95 = percentile(&sorted, 0.95);
        Some(Self {
            min,
            median,
            mean,
            p95,
            max,
        })
    }
}

fn percentile(sorted: &[f64], pct: f64) -> f64 {
    // The shared Lisp runner uses GNU round, whose ties go to even.
    let idx = ((sorted.len() - 1) as f64 * pct).round_ties_even() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn manifest_parses_and_validates() {
        let manifest = PerfScenarioManifest::from_json_str(
            r#"{
              "format_version": 1,
              "scenarios": [
                {
                  "id": "noverlay/perf-marker-suite",
                  "description": "Marker perf",
                  "group": "noverlay",
                  "tier": "comparable",
                  "oracle_adapter": "noverlay_suite",
                  "emaxx_adapter": "noverlay_marker_suite",
                  "load_files": ["test/manual/noverlay/overlay-perf.el"],
                  "params": { "suite": "perf-marker-suite", "n": 4096 },
                  "warmup": 1,
                  "samples": 5,
                  "timeout_secs": 60
                }
              ]
            }"#,
        )
        .unwrap();
        assert_eq!(manifest.scenarios.len(), 1);
        assert_eq!(manifest.scenarios[0].param_u64("n"), Some(4096));
    }

    #[test]
    fn sample_summary_uses_expected_statistics() {
        let summary = SampleSummary::compute(&[4.0, 1.0, 3.0, 2.0, 5.0]).unwrap();
        assert_eq!(summary.min, 1.0);
        assert_eq!(summary.median, 3.0);
        assert_eq!(summary.mean, 3.0);
        assert_eq!(summary.p95, 5.0);
        assert_eq!(summary.max, 5.0);
    }

    #[test]
    fn classification_thresholds_match_policy() {
        let oracle =
            PerfCaseReport::completed("case", "seconds", vec![1.0, 1.0, 1.0], 0, 0.0, None);
        let faster =
            PerfCaseReport::completed("case", "seconds", vec![0.94, 0.95, 0.96], 0, 0.0, None);
        let parity =
            PerfCaseReport::completed("case", "seconds", vec![1.02, 1.0, 1.04], 0, 0.0, None);
        let slower =
            PerfCaseReport::completed("case", "seconds", vec![1.06, 1.08, 1.1], 0, 0.0, None);
        assert_eq!(
            classify_case(Some(&oracle), Some(&faster)).0,
            PerfComparisonClass::Faster
        );
        assert_eq!(
            classify_case(Some(&oracle), Some(&parity)).0,
            PerfComparisonClass::Parity
        );
        assert_eq!(
            classify_case(Some(&oracle), Some(&slower)).0,
            PerfComparisonClass::Slower
        );
    }

    #[test]
    fn comparison_marks_the_inclusive_two_x_frontier_gate() {
        let scenario = PerfScenario {
            id: "interpreter/source-eval-suite".into(),
            description: "Source evaluation".into(),
            group: "interpreter".into(),
            tier: PerfTier::Comparable,
            oracle_adapter: "interpreter_suite".into(),
            emaxx_adapter: Some("interpreter_suite".into()),
            load_files: Vec::new(),
            params: BTreeMap::new(),
            warmup: 1,
            samples: 3,
            timeout_secs: 60,
        };
        let report = |runner: &str, median: f64| PerfRunReport {
            runner: runner.into(),
            scenario_id: scenario.id.clone(),
            tier: PerfTier::Comparable,
            status: PerfRunStatus::Completed,
            cases: vec![PerfCaseReport::completed(
                "case",
                "seconds",
                vec![median; 3],
                0,
                0.0,
                None,
            )],
            metadata: BTreeMap::new(),
        };

        let below = compare_reports(
            &scenario,
            &report("oracle", 1.0),
            Some(&report("emaxx", 1.999)),
        );
        assert!(!below.case_results[0].exceeds_two_x);
        assert_eq!(below.summary.over_two_x, 0);

        let at_gate = compare_reports(
            &scenario,
            &report("oracle", 1.0),
            Some(&report("emaxx", 2.0)),
        );
        assert_eq!(at_gate.case_results[0].emaxx_over_oracle, Some(2.0));
        assert!(at_gate.case_results[0].exceeds_two_x);
        assert_eq!(at_gate.summary.over_two_x, 1);
    }

    #[test]
    fn comparison_ratio_rejects_zero_or_non_finite_medians() {
        assert_eq!(comparable_ratio(Some(0.0), Some(1.0)), None);
        assert_eq!(comparable_ratio(Some(f64::NAN), Some(1.0)), None);
        assert_eq!(comparable_ratio(Some(1.0), Some(f64::INFINITY)), None);
    }

    #[test]
    fn reports_reject_missing_cases_samples_and_fabricated_summaries() {
        let manifest = PerfScenarioManifest::load().unwrap();
        let scenario = manifest.find("interpreter/source-eval-suite").unwrap();
        let report = PerfRunReport {
            runner: "emaxx".into(),
            scenario_id: scenario.id.clone(),
            tier: scenario.tier,
            status: PerfRunStatus::Completed,
            cases: expand_scenario_cases(scenario)
                .into_iter()
                .map(|name| {
                    PerfCaseReport::completed(
                        name,
                        "seconds",
                        vec![0.25; scenario.samples as usize],
                        3,
                        0.02,
                        None,
                    )
                })
                .collect(),
            metadata: BTreeMap::from([
                ("n".into(), "4096".into()),
                ("warmup".into(), scenario.warmup.to_string()),
                ("samples".into(), scenario.samples.to_string()),
            ]),
        };
        report.validate_completed("emaxx", scenario).unwrap();
        for corruption in 0..12 {
            let mut bad = report.clone();
            match corruption {
                0 => bad.runner = "oracle".into(),
                1 => bad.scenario_id = "other/workload".into(),
                2 => bad.status = PerfRunStatus::Failed,
                3 => bad.cases.clear(),
                4 => {
                    bad.cases.pop();
                }
                5 => bad.cases.push(bad.cases[0].clone()),
                6 => bad.cases[0].status = PerfCaseStatus::Unsupported,
                7 => {
                    bad.cases[0].samples.pop();
                }
                8 => bad.cases[0].samples[0] = f64::NAN,
                9 => bad.cases[0].samples[0] = -0.5,
                10 => bad.cases[0].median = Some(0.001),
                11 => {
                    bad.metadata.insert("n".into(), "32".into());
                }
                _ => unreachable!(),
            }
            assert!(
                bad.validate_completed("emaxx", scenario).is_err(),
                "corruption {corruption}"
            );
        }
    }

    #[test]
    fn artifact_directory_preserves_scenario_shape() {
        let root = PathBuf::from("/tmp/perf");
        let dir = scenario_artifact_dir(&root, "noverlay/perf-marker-suite");
        assert_eq!(
            dir,
            PathBuf::from("/tmp/perf/noverlay/perf-marker-suite.perf")
        );
    }

    #[test]
    fn create_temp_home_is_recreated() {
        let root = std::env::temp_dir().join(format!(
            "emaxx-perf-home-{}",
            current_timestamp_secs().unwrap()
        ));
        fs::create_dir_all(&root).unwrap();
        let dir = root.join("scenario.perf");
        fs::create_dir_all(&dir).unwrap();
        let home = create_temp_home(&dir).unwrap();
        assert!(home.exists());
        fs::write(home.join("test.txt"), "hello").unwrap();
        let recreated = create_temp_home(&dir).unwrap();
        assert!(recreated.exists());
        assert!(!recreated.join("test.txt").exists());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn suite_expansion_for_marker_suite_matches_upstream_names() {
        let scenario = PerfScenario {
            id: "noverlay/perf-marker-suite".into(),
            description: "Marker suite".into(),
            group: "noverlay".into(),
            tier: PerfTier::Comparable,
            oracle_adapter: "noverlay_suite".into(),
            emaxx_adapter: Some("noverlay_marker_suite".into()),
            load_files: vec!["test/manual/noverlay/overlay-perf.el".into()],
            params: BTreeMap::from([(
                "suite".into(),
                JsonValue::String("perf-marker-suite".into()),
            )]),
            warmup: 1,
            samples: 5,
            timeout_secs: 60,
        };
        assert_eq!(
            expand_scenario_cases(&scenario),
            vec![
                "perf-insert-before-marker",
                "perf-insert-after-marker",
                "perf-insert-scatter-marker",
                "perf-delete-before-marker",
                "perf-delete-after-marker",
                "perf-delete-scatter-marker",
            ]
        );
    }

    #[test]
    fn shared_interpreter_workload_is_confined_to_the_harness_root() {
        let resolved = resolve_harness_load_file("harness:compat/interpreter_perf.el")
            .unwrap()
            .expect("harness load path");
        assert_eq!(
            resolved,
            compat::compat_path("compat/interpreter_perf.el")
                .canonicalize()
                .unwrap()
        );
        assert!(resolve_harness_load_file("harness:/tmp/outside.el").is_err());
    }

    #[test]
    fn interpreter_workload_validates_all_cases_and_sample_counts() {
        let scenario = PerfScenario {
            id: "interpreter/source-eval-suite".into(),
            description: "Source evaluation".into(),
            group: "interpreter".into(),
            tier: PerfTier::Comparable,
            oracle_adapter: "interpreter_suite".into(),
            emaxx_adapter: Some("interpreter_suite".into()),
            load_files: vec!["harness:compat/interpreter_perf.el".into()],
            params: BTreeMap::from([("n".into(), JsonValue::from(32))]),
            warmup: 1,
            samples: 2,
            timeout_secs: 60,
        };
        with_shared_interpreter_workload(move |interpreter| {
            let output = crate::test_support::eval_lisp(
                interpreter,
                &mut crate::lisp::types::Env::new(),
                "(json-encode (emaxx-perf--interpreter-cases 32 1 2))",
            )
            .expect("shared Lisp runner completes");
            let output = crate::lisp::primitives::string_like(&output).expect("JSON string");
            let cases: Vec<PerfCaseReport> =
                serde_json::from_str(&output.text).expect("case reports");
            let report = PerfRunReport {
                runner: "emaxx".into(),
                scenario_id: scenario.id.clone(),
                tier: scenario.tier,
                status: PerfRunStatus::Completed,
                cases,
                metadata: BTreeMap::from([
                    ("n".into(), "32".into()),
                    ("warmup".into(), "1".into()),
                    ("samples".into(), "2".into()),
                ]),
            };
            report
                .validate_completed("emaxx", &scenario)
                .expect("complete checked samples");
        });
    }

    #[test]
    fn interpreter_sample_is_rejected_when_checksum_validation_does_not_return_t() {
        with_shared_interpreter_workload(|interpreter| {
            for body in ["nil", "(error \"deliberately broken checksum\")"] {
                let program = format!(
                    "(progn
                       (fset 'emaxx-perf-interpreted-list-walk (lambda (_) {body}))
                       (emaxx-perf--interpreter-cases 32 1 2))"
                );
                assert!(
                    crate::test_support::eval_lisp(
                        interpreter,
                        &mut crate::lisp::types::Env::new(),
                        &program
                    )
                    .is_err(),
                    "the shared runner must reject {body}"
                );
            }
        });
    }

    fn with_shared_interpreter_workload(
        test: impl FnOnce(&mut crate::lisp::eval::Interpreter) + Send + 'static,
    ) {
        let permit = crate::test_support::acquire_host_test_permit();
        std::thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(move || {
                let _permit = permit;
                crate::test_support::note_host_permit_moved_to_this_thread();
                let mut interpreter = crate::test_support::initialized_upstream_batch_interpreter();
                for file in ["compat/emacs_perf_runner.el", "compat/interpreter_perf.el"] {
                    let path = compat::compat_path(file);
                    interpreter
                        .load_target(path.to_str().expect("UTF-8 helper path"))
                        .expect("load shared performance source");
                }
                test(&mut interpreter);
            })
            .expect("performance contract test stack")
            .join()
            .expect("shared workload tests");
    }
}
