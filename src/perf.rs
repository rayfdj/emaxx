use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::buffer::Buffer;
use crate::compat;
use crate::lisp::eval::Interpreter;
use crate::lisp::types::Value;
use crate::overlay::Overlay;

pub const PERF_SCENARIO_MANIFEST_PATH: &str = "compat/perf_scenarios.json";
pub const PERF_RESULT_FILE_ENV: &str = "EMAXX_PERF_RESULT_FILE";

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
    pub notes: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct PerfComparisonSummary {
    pub comparable_cases: usize,
    pub faster: usize,
    pub parity: usize,
    pub slower: usize,
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

pub fn ensure_release_emaxx_binary() -> Result<PathBuf, String> {
    let project_root = compat::project_root();
    let status = Command::new("cargo")
        .arg("build")
        .arg("--quiet")
        .arg("--release")
        .arg("--bin")
        .arg("emaxx")
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
    Ok(candidate)
}

pub fn compare_reports(
    scenario: &PerfScenario,
    oracle: &PerfRunReport,
    emaxx: Option<&PerfRunReport>,
) -> PerfComparisonReport {
    let mut ids = BTreeSet::new();
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
        let (class, notes) = classify_case(oracle_case, emaxx_case);
        let oracle_median = oracle_case.and_then(|case| case.median);
        let emaxx_median = emaxx_case.and_then(|case| case.median);
        match class {
            PerfComparisonClass::Faster => summary.faster += 1,
            PerfComparisonClass::Parity => summary.parity += 1,
            PerfComparisonClass::Slower => summary.slower += 1,
            PerfComparisonClass::Unsupported => summary.unsupported += 1,
            PerfComparisonClass::Failed => summary.failed += 1,
        }
        if scenario.tier == PerfTier::Comparable {
            summary.comparable_cases += 1;
        }
        case_results.push(PerfCaseComparison {
            case_id,
            class,
            oracle_median,
            emaxx_median,
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

pub fn run_emaxx_batch_scenario(
    scenario_id: &str,
    n: usize,
    warmup: u32,
    samples: u32,
) -> Result<PerfRunReport, String> {
    let manifest = PerfScenarioManifest::load()?;
    let scenario = manifest
        .find(scenario_id)
        .ok_or_else(|| format!("unknown perf scenario `{scenario_id}`"))?;
    let report = match scenario.emaxx_adapter.as_deref() {
        Some("noverlay_marker_suite") => run_noverlay_marker_suite(scenario, n, warmup, samples),
        Some("noverlay_insert_delete_suite") => {
            run_noverlay_insert_delete_suite(scenario, n, warmup, samples)
        }
        Some(_) | None => PerfRunReport::unsupported(
            "emaxx",
            scenario,
            "emaxx does not yet provide a comparable adapter for this scenario",
            expand_scenario_cases(scenario),
        ),
    };
    if let Ok(result_path) = std::env::var(PERF_RESULT_FILE_ENV) {
        report.write_json(Path::new(&result_path))?;
    }
    Ok(report)
}

pub fn expand_scenario_cases(scenario: &PerfScenario) -> Vec<String> {
    match scenario.oracle_adapter.as_str() {
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
                "perf-display-hierarchical/face/scroll",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            Some("perf-noc-suite") => vec![
                "perf-noc-hierarchical/forward/linear",
                "perf-noc-sequential/forward/linear",
                "perf-noc-random/forward/linear",
                "perf-noc-hierarchical/forward/line-end",
                "perf-noc-sequential/forward/line-end",
                "perf-noc-random/forward/line-end",
                "perf-noc-hierarchical/backward/linear",
                "perf-noc-sequential/backward/linear",
                "perf-noc-random/backward/linear",
                "perf-noc-hierarchical/backward/line-beginning",
                "perf-noc-sequential/backward/line-beginning",
                "perf-noc-random/backward/line-beginning",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            _ => Vec::new(),
        },
        "coding_decoder" => vec!["without-optimization", "with-optimization"]
            .into_iter()
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

fn run_noverlay_marker_suite(
    scenario: &PerfScenario,
    n: usize,
    warmup: u32,
    samples: u32,
) -> PerfRunReport {
    let cases = vec![
        run_case("perf-insert-before-marker", warmup, samples, |sample| {
            let mut interpreter = Interpreter::new();
            insert_perf_text(&mut interpreter.buffer, n);
            seed_markers(&mut interpreter, n, sample as u64 + 1);
            interpreter.buffer.goto_char(interpreter.buffer.point_min());
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    interpreter.insert_current_buffer("X");
                }
            })
        }),
        run_case("perf-insert-after-marker", warmup, samples, |sample| {
            let mut interpreter = Interpreter::new();
            insert_perf_text(&mut interpreter.buffer, n);
            seed_markers(&mut interpreter, n, sample as u64 + 11);
            interpreter.buffer.goto_char(interpreter.buffer.point_max());
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    interpreter.insert_current_buffer("X");
                }
            })
        }),
        run_case("perf-insert-scatter-marker", warmup, samples, |sample| {
            let mut interpreter = Interpreter::new();
            insert_perf_text(&mut interpreter.buffer, n);
            seed_markers(&mut interpreter, n, sample as u64 + 21);
            interpreter.buffer.goto_char(interpreter.buffer.point_min());
            let mut rng = PerfRng::new(0x51_0000 + sample as u64);
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    let point_max = interpreter.buffer.point_max();
                    let pos = rng.emacs_marker_scatter_position(point_max);
                    interpreter.buffer.goto_char(pos);
                    interpreter.insert_current_buffer("X");
                }
            })
        }),
        run_case("perf-delete-before-marker", warmup, samples, |sample| {
            let mut interpreter = Interpreter::new();
            insert_perf_text(&mut interpreter.buffer, n);
            seed_markers(&mut interpreter, n, sample as u64 + 31);
            interpreter.buffer.goto_char(interpreter.buffer.point_min());
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    let _ = interpreter.delete_char_current_buffer(1);
                }
            })
        }),
        run_case("perf-delete-after-marker", warmup, samples, |sample| {
            let mut interpreter = Interpreter::new();
            insert_perf_text(&mut interpreter.buffer, n);
            seed_markers(&mut interpreter, n, sample as u64 + 41);
            interpreter.buffer.goto_char(interpreter.buffer.point_max());
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    let _ = interpreter.delete_char_current_buffer(-1);
                }
            })
        }),
        run_case("perf-delete-scatter-marker", warmup, samples, |sample| {
            let mut interpreter = Interpreter::new();
            insert_perf_text(&mut interpreter.buffer, n);
            seed_markers(&mut interpreter, n, sample as u64 + 51);
            interpreter.buffer.goto_char(interpreter.buffer.point_max());
            let mut rng = PerfRng::new(0x61_0000 + sample as u64);
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    let point_max = interpreter.buffer.point_max();
                    let pos = rng.emacs_marker_scatter_position(point_max);
                    interpreter.buffer.goto_char(pos);
                    let _ = interpreter.delete_char_current_buffer(1);
                }
            })
        }),
    ];
    completed_run_report("emaxx", scenario, n, warmup, samples, cases)
}

fn run_noverlay_insert_delete_suite(
    scenario: &PerfScenario,
    n: usize,
    warmup: u32,
    samples: u32,
) -> PerfRunReport {
    let cases = vec![
        run_case("perf-insert-before", warmup, samples, |sample| {
            let mut buffer = Buffer::new("*perf*");
            insert_perf_text(&mut buffer, n);
            seed_scattered_overlays(&mut buffer, 0, n, sample as u64 + 101);
            buffer.goto_char(1);
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    buffer.insert("X");
                }
            })
        }),
        run_case("perf-insert-after", warmup, samples, |sample| {
            let mut buffer = Buffer::new("*perf*");
            insert_perf_text(&mut buffer, n);
            seed_scattered_overlays(&mut buffer, 0, n, sample as u64 + 111);
            buffer.goto_char(buffer.point_max());
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    buffer.insert("X");
                }
            })
        }),
        run_case("perf-insert-scatter", warmup, samples, |sample| {
            let mut buffer = Buffer::new("*perf*");
            insert_perf_text(&mut buffer, n);
            seed_scattered_overlays(&mut buffer, 0, n, sample as u64 + 121);
            buffer.goto_char(buffer.point_max());
            let mut rng = PerfRng::new(0x71_0000 + sample as u64);
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    let point_max = buffer.point_max();
                    let pos = rng.emacs_insert_scatter_position(point_max);
                    buffer.goto_char(pos);
                    buffer.insert("X");
                }
            })
        }),
        run_case("perf-delete-before", warmup, samples, |sample| {
            let mut buffer = Buffer::new("*perf*");
            insert_perf_text(&mut buffer, n);
            seed_scattered_overlays(&mut buffer, 0, n, sample as u64 + 131);
            buffer.goto_char(1);
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    let _ = buffer.delete_char(1);
                }
            })
        }),
        run_case("perf-delete-after", warmup, samples, |sample| {
            let mut buffer = Buffer::new("*perf*");
            insert_perf_text(&mut buffer, n);
            seed_scattered_overlays(&mut buffer, 0, n, sample as u64 + 141);
            buffer.goto_char(buffer.point_max());
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    let _ = buffer.delete_char(-1);
                }
            })
        }),
        run_case("perf-delete-scatter", warmup, samples, |sample| {
            let mut buffer = Buffer::new("*perf*");
            insert_perf_text(&mut buffer, n);
            seed_scattered_overlays(&mut buffer, 0, n, sample as u64 + 151);
            buffer.goto_char(buffer.point_max());
            let mut rng = PerfRng::new(0x81_0000 + sample as u64);
            timed_operation(|| {
                for _ in 0..(n / 2) {
                    let point_max = buffer.point_max();
                    let pos = rng.emacs_marker_scatter_position(point_max);
                    buffer.goto_char(pos);
                    let _ = buffer.delete_char(1);
                }
            })
        }),
    ];
    completed_run_report("emaxx", scenario, n, warmup, samples, cases)
}

fn completed_run_report(
    runner: &str,
    scenario: &PerfScenario,
    n: usize,
    warmup: u32,
    samples: u32,
    cases: Vec<PerfCaseReport>,
) -> PerfRunReport {
    let mut metadata = BTreeMap::new();
    metadata.insert("group".into(), scenario.group.clone());
    metadata.insert("n".into(), n.to_string());
    metadata.insert("warmup".into(), warmup.to_string());
    metadata.insert("samples".into(), samples.to_string());
    metadata.insert("target_profile".into(), "release".into());
    PerfRunReport {
        runner: runner.into(),
        scenario_id: scenario.id.clone(),
        tier: scenario.tier,
        status: PerfRunStatus::Completed,
        cases,
        metadata,
    }
}

fn run_case<F>(case_id: &str, warmup: u32, samples: u32, mut operation: F) -> PerfCaseReport
where
    F: FnMut(u32) -> f64,
{
    let mut timings = Vec::new();
    for sample in 0..(warmup + samples) {
        let timing = operation(sample);
        if sample >= warmup {
            timings.push(timing);
        }
    }
    PerfCaseReport::completed(case_id, "seconds", timings, 0, 0.0, None)
}

fn timed_operation<F>(operation: F) -> f64
where
    F: FnOnce(),
{
    let started = Instant::now();
    operation();
    started.elapsed().as_secs_f64()
}

fn insert_perf_text(buffer: &mut Buffer, n: usize) {
    let ncols = 68usize;
    for _ in 0..(n / ncols) {
        buffer.insert(&".".repeat(ncols - 1));
        buffer.insert("\n");
    }
    let rem = n % ncols;
    if rem > 0 {
        buffer.insert(&".".repeat(rem.saturating_sub(1)));
        buffer.insert("\n");
    }
    buffer.goto_char(buffer.point_min());
}

fn seed_scattered_overlays(buffer: &mut Buffer, buffer_id: u64, n: usize, seed: u64) {
    let mut rng = PerfRng::new(0x91_0000 + seed);
    for overlay_id in 0..n {
        let begin = rng.emacs_overlay_begin(buffer.point_max());
        let len = rng.inclusive(24);
        let end = begin.saturating_add(len);
        buffer.overlays.push(Overlay::new(
            overlay_id as u64 + 1,
            begin,
            end,
            buffer_id,
            false,
            false,
        ));
    }
}

fn seed_markers(interpreter: &mut Interpreter, n: usize, seed: u64) {
    let mut rng = PerfRng::new(0xA1_0000 + seed);
    let buffer_id = interpreter.current_buffer_id();
    let point_max = interpreter.buffer.point_max();
    for _ in 0..n {
        let marker = interpreter.make_marker();
        let Value::Marker(id) = marker else {
            unreachable!("make_marker must return a marker");
        };
        let position = rng.emacs_overlay_begin(point_max);
        let _ = interpreter.set_marker(id, Some(position), Some(buffer_id));
    }
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
    let idx = ((sorted.len() - 1) as f64 * pct).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[derive(Clone, Debug)]
struct PerfRng {
    state: u64,
}

impl PerfRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state ^= self.state >> 12;
        self.state ^= self.state << 25;
        self.state ^= self.state >> 27;
        self.state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn exclusive(&mut self, upper_exclusive: usize) -> usize {
        if upper_exclusive == 0 {
            0
        } else {
            (self.next_u64() % upper_exclusive as u64) as usize
        }
    }

    fn inclusive(&mut self, upper_inclusive: usize) -> usize {
        self.exclusive(upper_inclusive.saturating_add(1))
    }

    fn emacs_overlay_begin(&mut self, point_max: usize) -> usize {
        self.inclusive(point_max)
    }

    fn emacs_insert_scatter_position(&mut self, point_max: usize) -> usize {
        self.exclusive(point_max).saturating_add(1).max(1)
    }

    fn emacs_marker_scatter_position(&mut self, point_max: usize) -> usize {
        self.exclusive(point_max).max(1)
    }
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
}
