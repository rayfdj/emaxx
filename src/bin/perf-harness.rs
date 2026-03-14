use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};

use emaxx::compat::{self, OracleLocalConfig, OracleLock};
use emaxx::perf::{
    self, PerfComparisonReport, PerfRunReport, PerfScenario, PerfScenarioManifest, PerfScenarioSummary,
    PerfSummaryReport, PerfTier,
};

#[derive(Debug, Parser)]
#[command(name = "perf-harness", disable_help_subcommand = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    List,
    Run(RunArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RunnerArg {
    Oracle,
    Both,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("selection")
        .args(["all", "scenario"])
        .required(true)
))]
struct RunArgs {
    #[arg(long, value_enum, default_value = "both")]
    runner: RunnerArg,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    scenario: Option<String>,
}

#[derive(Debug)]
struct ProcessResult {
    exit_code: Option<i32>,
    stdout: String,
    stderr: String,
    timed_out: bool,
}

#[derive(Debug)]
struct RunArtifacts {
    report: PerfRunReport,
    process: Option<ProcessResult>,
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
        Commands::List => {
            list_scenarios()?;
            Ok(0)
        }
        Commands::Run(args) => run_perf(args),
    }
}

fn list_scenarios() -> Result<(), String> {
    let manifest = PerfScenarioManifest::load()?;
    for scenario in manifest.scenarios {
        println!(
            "{} [{}] oracle={} emaxx={}",
            scenario.id,
            tier_label(scenario.tier),
            scenario.oracle_adapter,
            scenario.emaxx_adapter.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

fn run_perf(args: RunArgs) -> Result<u8, String> {
    let manifest = PerfScenarioManifest::load()?;
    let context = load_context()?;
    let scenarios = select_scenarios(&manifest, args.scenario.as_deref(), args.all)?;
    let artifact_root = perf::make_artifact_root()?;
    let emaxx_binary = match args.runner {
        RunnerArg::Oracle => None,
        RunnerArg::Both => Some(perf::ensure_release_emaxx_binary()?),
    };

    let mut run_summary = perf::PerfRunSummary::default();
    let mut scenario_summaries = Vec::new();
    let mut had_process_failure = false;

    for scenario in scenarios {
        let scenario_dir = perf::scenario_artifact_dir(&artifact_root, &scenario.id);
        fs::create_dir_all(&scenario_dir)
            .map_err(|error| format!("create {}: {error}", scenario_dir.display()))?;
        let home = perf::create_temp_home(&scenario_dir)?;
        let timeout = scenario_timeout(&scenario)?;

        let oracle = run_oracle_scenario(&context.local, &scenario, &scenario_dir, &home, timeout)?;
        write_run_artifacts(&scenario_dir.join("oracle.log"), &oracle.process)?;

        if oracle.process.as_ref().is_some_and(process_failed) {
            had_process_failure = true;
        }

        let emaxx = match args.runner {
            RunnerArg::Oracle => None,
            RunnerArg::Both => {
                let result = match &emaxx_binary {
                    Some(binary) if scenario.emaxx_adapter.is_some() => {
                        run_emaxx_scenario(binary, &context.local.emacs_repo, &scenario, &scenario_dir, &home, timeout)?
                    }
                    _ => RunArtifacts {
                        report: PerfRunReport::unsupported(
                            "emaxx",
                            &scenario,
                            "emaxx does not yet provide a comparable adapter for this scenario",
                            if oracle.report.cases.is_empty() {
                                perf::expand_scenario_cases(&scenario)
                            } else {
                                oracle
                                    .report
                                    .cases
                                    .iter()
                                    .map(|case| case.case_id.clone())
                                    .collect()
                            },
                        ),
                        process: None,
                    },
                };
                if let Some(process) = &result.process
                    && process_failed(process)
                {
                    had_process_failure = true;
                }
                write_run_artifacts(&scenario_dir.join("emaxx.log"), &result.process)?;
                Some(result)
            }
        };

        let comparison = emaxx
            .as_ref()
            .map(|emaxx| perf::compare_reports(&scenario, &oracle.report, Some(&emaxx.report)));
        if let Some(comparison) = &comparison {
            perf::write_json(
                &scenario_dir.join("comparison.json"),
                comparison,
                "perf comparison report",
            )?;
        }

        update_summary(&mut run_summary, &scenario, &oracle.report, comparison.as_ref());
        scenario_summaries.push(PerfScenarioSummary {
            scenario_id: scenario.id.clone(),
            tier: scenario.tier,
            oracle_status: oracle.report.status,
            emaxx_status: emaxx.as_ref().map(|result| result.report.status),
            comparison: comparison.as_ref().map(|result| result.summary.clone()),
        });
    }

    let summary = PerfSummaryReport {
        timestamp: perf::current_timestamp_secs()?,
        oracle_emacs_version: context.lock.emacs_version.clone(),
        oracle_emacs_repo_commit: context.lock.emacs_repo_commit.clone(),
        emaxx_git_commit: compat::current_repo_commit(&compat::project_root())?,
        target_profile: "release".into(),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        cpu_model: perf::best_effort_cpu_model(),
        summary: run_summary,
        scenarios: scenario_summaries,
    };
    perf::write_json(
        &artifact_root.join("summary.json"),
        &summary,
        "perf summary report",
    )?;

    if had_process_failure { Ok(2) } else { Ok(0) }
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

fn select_scenarios(
    manifest: &PerfScenarioManifest,
    scenario: Option<&str>,
    all: bool,
) -> Result<Vec<PerfScenario>, String> {
    if all {
        return Ok(manifest.scenarios.clone());
    }
    let scenario_id = scenario.ok_or_else(|| "either --all or --scenario is required".to_string())?;
    manifest
        .find(scenario_id)
        .cloned()
        .map(|selected| vec![selected])
        .ok_or_else(|| format!("unknown scenario `{scenario_id}`"))
}

fn scenario_timeout(scenario: &PerfScenario) -> Result<Option<Duration>, String> {
    Ok(match compat::resolve_timeout()? {
        Some(timeout) => Some(timeout),
        None => Some(Duration::from_secs(scenario.timeout_secs)),
    })
}

fn run_oracle_scenario(
    local: &OracleLocalConfig,
    scenario: &PerfScenario,
    scenario_dir: &Path,
    home: &Path,
    timeout: Option<Duration>,
) -> Result<RunArtifacts, String> {
    let result_path = scenario_dir.join("oracle.json");
    let helper_path = compat::compat_path("compat/emacs_perf_runner.el");
    let test_directory = local.emacs_repo.join("test");
    let mut command = Command::new(&local.emacs_binary);
    compat::configure_upstream_like_env_with_home(&mut command, &test_directory, home);
    command.env(perf::PERF_RESULT_FILE_ENV, &result_path);
    command.arg("--no-init-file");
    command.arg("--no-site-file");
    command.arg("--no-site-lisp");
    command.arg("--batch");
    command.arg("-L");
    command.arg(&test_directory);
    command.arg("-l");
    command.arg(&helper_path);
    for load_file in &scenario.load_files {
        command.arg("-l");
        command.arg(local.emacs_repo.join(load_file));
    }
    command.arg("--eval");
    command.arg(format!(
        "(emaxx-perf-run-scenario {} {} {} {})",
        lisp_string_literal(&scenario.id),
        scenario.param_u64("n").unwrap_or(4096),
        scenario.warmup,
        scenario.samples
    ));

    let process = run_command(command, timeout)?;
    let report = load_or_synthesize_report("oracle", scenario, &result_path, &process)?;
    Ok(RunArtifacts {
        report,
        process: Some(process),
    })
}

fn run_emaxx_scenario(
    emaxx_binary: &Path,
    emacs_repo: &Path,
    scenario: &PerfScenario,
    scenario_dir: &Path,
    home: &Path,
    timeout: Option<Duration>,
) -> Result<RunArtifacts, String> {
    let result_path = scenario_dir.join("emaxx.json");
    let test_directory = emacs_repo.join("test");
    let mut command = Command::new(emaxx_binary);
    compat::configure_upstream_like_env_with_home(&mut command, &test_directory, home);
    command.env(perf::PERF_RESULT_FILE_ENV, &result_path);
    command.arg("--no-init-file");
    command.arg("--no-site-file");
    command.arg("--no-site-lisp");
    command.arg("--batch");
    command.arg("-L");
    command.arg(&test_directory);
    command.arg("--eval");
    command.arg(format!(
        "(emaxx-perf-run-batch {} {} {} {})",
        lisp_string_literal(&scenario.id),
        scenario.param_u64("n").unwrap_or(4096),
        scenario.warmup,
        scenario.samples
    ));

    let process = run_command(command, timeout)?;
    let report = load_or_synthesize_report("emaxx", scenario, &result_path, &process)?;
    Ok(RunArtifacts {
        report,
        process: Some(process),
    })
}

fn load_or_synthesize_report(
    runner: &str,
    scenario: &PerfScenario,
    result_path: &Path,
    process: &ProcessResult,
) -> Result<PerfRunReport, String> {
    if result_path.exists() {
        return PerfRunReport::read_json(result_path);
    }
    let report = if process.timed_out {
        PerfRunReport::failed(runner, scenario, "process timed out")
    } else {
        let detail = if process.stderr.trim().is_empty() {
            process.stdout.trim()
        } else {
            process.stderr.trim()
        };
        PerfRunReport::failed(
            runner,
            scenario,
            format!(
                "process exited {:?}: {}",
                process.exit_code,
                if detail.is_empty() {
                    "no structured perf result produced"
                } else {
                    detail
                }
            ),
        )
    };
    report.write_json(result_path)?;
    Ok(report)
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

fn write_run_artifacts(path: &Path, process: &Option<ProcessResult>) -> Result<(), String> {
    let Some(process) = process else {
        return Ok(());
    };
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

fn process_failed(process: &ProcessResult) -> bool {
    process.timed_out || process.exit_code != Some(0)
}

fn update_summary(
    summary: &mut perf::PerfRunSummary,
    scenario: &PerfScenario,
    oracle: &PerfRunReport,
    comparison: Option<&PerfComparisonReport>,
) {
    let totals = match scenario.tier {
        PerfTier::Comparable => &mut summary.comparable,
        PerfTier::Provisional => &mut summary.provisional,
        PerfTier::OracleOnly => &mut summary.oracle_only,
    };
    totals.scenarios += 1;
    totals.cases += oracle.cases.len();

    if scenario.tier == PerfTier::Comparable
        && let Some(comparison) = comparison
    {
        summary.faster += comparison.summary.faster;
        summary.parity += comparison.summary.parity;
        summary.slower += comparison.summary.slower;
        summary.unsupported += comparison.summary.unsupported;
        summary.failed += comparison.summary.failed;
    }
}

fn lisp_string_literal(value: &str) -> String {
    format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\""))
}

fn tier_label(tier: PerfTier) -> &'static str {
    match tier {
        PerfTier::Comparable => "comparable",
        PerfTier::Provisional => "provisional",
        PerfTier::OracleOnly => "oracle_only",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn top_line_summary_only_counts_comparable_cases() {
        let scenario = PerfScenario {
            id: "noverlay/perf-display-suite".into(),
            description: "Display suite".into(),
            group: "noverlay".into(),
            tier: PerfTier::OracleOnly,
            oracle_adapter: "noverlay_suite".into(),
            emaxx_adapter: None,
            load_files: Vec::new(),
            params: BTreeMap::new(),
            warmup: 1,
            samples: 3,
            timeout_secs: 180,
        };
        let oracle = PerfRunReport::failed("oracle", &scenario, "timed out");
        let comparison = PerfComparisonReport {
            scenario_id: scenario.id.clone(),
            tier: scenario.tier,
            case_results: Vec::new(),
            summary: perf::PerfComparisonSummary {
                comparable_cases: 0,
                faster: 0,
                parity: 0,
                slower: 0,
                unsupported: 7,
                failed: 9,
            },
        };
        let mut summary = perf::PerfRunSummary::default();
        update_summary(&mut summary, &scenario, &oracle, Some(&comparison));
        assert_eq!(summary.oracle_only.scenarios, 1);
        assert_eq!(summary.oracle_only.cases, 0);
        assert_eq!(summary.unsupported, 0);
        assert_eq!(summary.failed, 0);
    }
}
