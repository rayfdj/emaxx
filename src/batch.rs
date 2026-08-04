use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::compat::{
    self, BatchReport, BatchSummary, DiscoveredTest, FileStatus, TestOutcome, TestStatus,
};
use crate::lisp;
use crate::lisp::eval::Interpreter;
use crate::lisp::reader::Reader;
use crate::lisp::types::{EmacsTermination, Env, LispError, Value};
use crate::perf::{self, PERF_RESULT_FILE_ENV, PerfRunReport};

#[derive(Clone, Debug, Default)]
pub struct BatchRunOptions {
    pub load_path: Vec<PathBuf>,
    pub load: Vec<String>,
    pub eval: Vec<String>,
    pub funcall: Vec<String>,
    pub args_left: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchAction {
    Load(String),
    Eval(String),
    Funcall(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchRunOutcome {
    Exit(i32),
    Restart,
}

impl From<EmacsTermination> for BatchRunOutcome {
    fn from(termination: EmacsTermination) -> Self {
        if termination.restart {
            Self::Restart
        } else {
            Self::Exit(termination.exit_code)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PerfRequest {
    scenario_id: String,
    n: usize,
    warmup: u32,
    samples: u32,
}

pub fn run_batch(options: BatchRunOptions) -> Result<BatchRunOutcome, String> {
    let actions = options
        .load
        .iter()
        .cloned()
        .map(BatchAction::Load)
        .chain(options.eval.iter().cloned().map(BatchAction::Eval))
        .chain(options.funcall.iter().cloned().map(BatchAction::Funcall))
        .collect();
    run_batch_with_actions(options, actions)
}

pub fn run_batch_with_actions(
    options: BatchRunOptions,
    actions: Vec<BatchAction>,
) -> Result<BatchRunOutcome, String> {
    let options = BatchRunOptions {
        load_path: effective_batch_load_path(&options)?,
        ..options
    };
    let mut interpreter = initialize_batch_interpreter(&options)?;
    let mut loaded_test_file: Option<PathBuf> = None;
    let eval_expressions = actions
        .iter()
        .filter_map(|action| match action {
            BatchAction::Eval(expression) => Some(expression.clone()),
            BatchAction::Load(_) | BatchAction::Funcall(_) => None,
        })
        .collect::<Vec<_>>();
    let (selector, saw_ert_runner) = parse_selector_requests(&eval_expressions)?;
    let perf_request = parse_perf_request(&eval_expressions)?;
    let selector_string = selector.to_string();
    let compat_batch_report = env::var(compat::BATCH_RESULT_FILE_ENV).is_ok();
    let mut eval_env: Env = Vec::new();
    for action in &actions {
        match action {
            BatchAction::Load(target) => {
                let resolved = resolve_load_target(target, &options.load_path)?;
                if target != "ert" && loaded_test_file.is_none() {
                    loaded_test_file = Some(resolved.clone());
                }
                if saw_ert_runner
                    && compat_batch_report
                    && compat::should_bridge_batch_report(&report_file_name(&resolved))
                {
                    continue;
                }
                if let Err(error) = lisp::load_file_strict(&mut interpreter, &resolved) {
                    if let LispError::Terminate(termination) = error {
                        return Ok(termination.into());
                    }
                    let mut error_text = error.to_string();
                    let backtrace = format_backtrace_summary(&interpreter);
                    if !backtrace.is_empty() {
                        error_text.push_str(" | backtrace: ");
                        error_text.push_str(&backtrace);
                    }
                    let report = BatchReport {
                        runner: "emaxx".into(),
                        file: report_file_name(&resolved),
                        selector: selector_string.clone(),
                        file_status: FileStatus::LoadError,
                        file_error: Some(error_text),
                        discovered_tests: interpreter.discovered_tests(),
                        selected_tests: Vec::new(),
                        results: Vec::new(),
                        summary: Default::default(),
                    };
                    emit_artifacts(&report)?;
                    emit_human_log(&report);
                    write_junit_report_if_requested(&report)?;
                    return Ok(BatchRunOutcome::Exit(2));
                }
                if let Some(termination) = interpreter.take_pending_termination() {
                    return Ok(termination.into());
                }
            }
            BatchAction::Eval(expression) => {
                let forms = Reader::new(expression)
                    .read_all()
                    .map_err(|error| format!("parse --eval expression `{expression}`: {error}"))?;
                for form in forms {
                    if extract_ert_batch_selector(&form).is_some()
                        || extract_perf_request_from_form(&form).is_some()
                    {
                        continue;
                    }
                    match interpreter.eval(&form, &mut eval_env) {
                        Ok(_) => {}
                        Err(LispError::Terminate(termination)) => return Ok(termination.into()),
                        Err(error) => {
                            // GNU's noninteractive command loop reports an
                            // unhandled Lisp condition directly, without
                            // decorating it with the command-line form, and
                            // terminates with the conventional fatal status.
                            eprintln!("{error}");
                            return Ok(BatchRunOutcome::Exit(255));
                        }
                    }
                    if let Some(termination) = interpreter.take_pending_termination() {
                        return Ok(termination.into());
                    }
                }
            }
            BatchAction::Funcall(function) => {
                let form = Value::list([Value::Symbol(function.clone())]);
                match interpreter.eval(&form, &mut eval_env) {
                    Ok(_) => {}
                    Err(LispError::Terminate(termination)) => return Ok(termination.into()),
                    Err(error) => {
                        eprintln!("{error}");
                        return Ok(BatchRunOutcome::Exit(255));
                    }
                }
                if let Some(termination) = interpreter.take_pending_termination() {
                    return Ok(termination.into());
                }
            }
        }
    }

    if let Some(request) = perf_request {
        let report = perf::run_emaxx_batch_scenario(
            &request.scenario_id,
            request.n,
            request.warmup,
            request.samples,
        )?;
        emit_perf_artifacts(&report)?;
        emit_perf_human_log(&report);
        return Ok(BatchRunOutcome::Exit(match report.status {
            perf::PerfRunStatus::Completed | perf::PerfRunStatus::Unsupported => 0,
            perf::PerfRunStatus::Failed => 1,
        }));
    }

    let Some(test_file) = loaded_test_file else {
        if saw_ert_runner {
            let (_, summary) = run_ert_for_batch_report(&mut interpreter, &selector);
            if let Some(termination) = interpreter.take_pending_termination() {
                return Ok(termination.into());
            }
            return Ok(BatchRunOutcome::Exit(i32::from(summary.unexpected != 0)));
        }
        return Ok(BatchRunOutcome::Exit(0));
    };

    let relative_file = report_file_name(&test_file);
    let report = if saw_ert_runner {
        if let Some(report) =
            compat::maybe_bridge_batch_report(&relative_file, &test_file, &selector_string)?
        {
            report
        } else {
            let (discovered_tests, summary) = run_ert_for_batch_report(&mut interpreter, &selector);
            if let Some(termination) = interpreter.take_pending_termination() {
                return Ok(termination.into());
            }
            BatchReport {
                runner: "emaxx".into(),
                file: relative_file.clone(),
                selector: selector_string,
                file_status: FileStatus::Loaded,
                file_error: None,
                discovered_tests,
                selected_tests: interpreter.last_selected_tests.clone(),
                results: apply_backtrace_limit(interpreter.test_results.clone()),
                summary,
            }
        }
    } else {
        BatchReport {
            runner: "emaxx".into(),
            file: relative_file,
            selector: selector_string,
            file_status: FileStatus::Loaded,
            file_error: None,
            discovered_tests: interpreter.discovered_tests(),
            selected_tests: Vec::new(),
            results: Vec::new(),
            summary: Default::default(),
        }
    };

    emit_artifacts(&report)?;
    emit_human_log(&report);
    write_junit_report_if_requested(&report)?;

    if report.file_status == FileStatus::LoadError {
        Ok(BatchRunOutcome::Exit(2))
    } else if report.summary.unexpected == 0 {
        Ok(BatchRunOutcome::Exit(0))
    } else {
        Ok(BatchRunOutcome::Exit(1))
    }
}

fn run_ert_for_batch_report(
    interpreter: &mut Interpreter,
    selector: &Value,
) -> (Vec<DiscoveredTest>, BatchSummary) {
    // GNU's compatibility helper enumerates the loaded file before asking
    // ERT to run it.  Tests are allowed to define other tests at runtime;
    // those definitions must not retroactively change this file's discovery
    // report or the selector universe for the current run.
    let discovered_tests = interpreter.discovered_tests();
    let summary = interpreter.run_ert_tests_with_selector(Some(selector));
    (discovered_tests, summary)
}

pub(crate) fn initialize_batch_interpreter(
    options: &BatchRunOptions,
) -> Result<Interpreter, String> {
    let mut interpreter = Interpreter::new();
    interpreter.set_load_path(effective_batch_load_path(options)?);
    // GNU starts batch evaluation in *scratch*, whose buffer-local
    // `lexical-binding' is t while the default remains nil.  File cookies
    // override and restore this state around loads.
    interpreter.set_variable("lexical-binding", Value::T, &mut Vec::new());
    interpreter.set_variable("noninteractive", Value::T, &mut Vec::new());
    interpreter.set_variable(
        "command-line-args-left",
        Value::list(options.args_left.iter().cloned().map(Value::String)),
        &mut Vec::new(),
    );
    // Loading the dumped Lisp owners below corresponds to GNU's pre-dump
    // phase, where delayed Custom initializers accumulate until startup.
    interpreter.set_variable("custom-delayed-init-variables", Value::Nil, &mut Vec::new());
    preload_batch_compat_libraries(&mut interpreter)?;
    complete_delayed_custom_initialization(&mut interpreter)?;
    initialize_batch_locale_environment(&mut interpreter)?;
    Ok(interpreter)
}

fn complete_delayed_custom_initialization(interpreter: &mut Interpreter) -> Result<(), String> {
    // GNU records :initialize custom-initialize-delay options while building
    // the dumped image, then replays their setters at runtime in startup.el.
    // Emaxx reconstructs that preload phase from source, so complete the same
    // transition before exposing the initialized batch interpreter.
    let form = Reader::new(
        "(progn
           (when (listp custom-delayed-init-variables)
             (mapc #'custom-reevaluate-setting
                   (reverse custom-delayed-init-variables)))
           (setq custom-delayed-init-variables t))",
    )
    .read_all()
    .map_err(|error| format!("read delayed Custom startup form: {error}"))?
    .remove(0);
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("complete delayed Custom initialization: {error}"))?;
    Ok(())
}

fn initialize_batch_locale_environment(interpreter: &mut Interpreter) -> Result<(), String> {
    // startup.el runs this after the dumped multilingual owners are present.
    // It is observable even under --batch: on a UTF-8 locale GNU selects
    // utf-8-unix for terminal/keyboard I/O, which in turn drives the dumped
    // Lisp implementation of `char-displayable-p'.
    //
    // `initialize_batch_interpreter' is also the embedding boundary used by
    // small tests with an intentionally empty load path.  That runtime has no
    // dumped/startup Lisp owners to invoke, just as an embedding which has not
    // installed them cannot run this phase yet.
    if interpreter
        .lookup_function("set-locale-environment", &Vec::new())
        .is_err()
    {
        return Ok(());
    }
    let form = Reader::new("(set-locale-environment nil nil t)")
        .read_all()
        .map_err(|error| format!("read batch locale startup form: {error}"))?
        .remove(0);
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("initialize batch locale environment: {error}"))?;
    Ok(())
}

fn effective_batch_load_path(options: &BatchRunOptions) -> Result<Vec<PathBuf>, String> {
    if !options.load_path.is_empty() {
        return Ok(options.load_path.clone());
    }

    let Ok(test_directory) = env::var("EMACS_TEST_DIRECTORY") else {
        return Ok(Vec::new());
    };
    let test_directory = PathBuf::from(test_directory);
    let Some(repo_root) = test_directory.parent() else {
        return Ok(Vec::new());
    };
    compat::repo_local_elisp_load_path(repo_root)
}

fn preload_batch_compat_libraries(interpreter: &mut Interpreter) -> Result<(), String> {
    // keymap.el is loaded near the start of GNU loadup, before bindings.el.
    // Its macros own high-level keymap construction policy; the Rust layer
    // supplies the mutable keymap primitives they target.
    for feature in [
        "backquote",
        "keymap",
        "button",
        "seq",
        "mule",
        "mule-conf",
        "env",
    ] {
        if interpreter.has_feature(feature) || interpreter.resolve_load_target(feature).is_none() {
            continue;
        }
        interpreter
            .load_target(feature)
            .map_err(|error| format!("preload {feature}: {error}"))?;
    }

    for compat_library in ["src/lisp/faces_compat.el", "src/lisp/simple_compat.el"] {
        let path = compat::project_root().join(compat_library);
        lisp::load_file_strict(interpreter, &path)
            .map_err(|error| format!("load {}: {error}", path.display()))?;
    }
    // GNU dumps tabulated-list.el into the initial image (loadup reaches it
    // through buff-menu.el).  An autoload for the mode function alone is not
    // an equivalent startup state: dumped clients such as kmacro.el inherit
    // `tabulated-list-mode-map' while their top-level forms are loading.
    // Load the owning Lisp library so its maps, variables, and mode contract
    // become available together, without moving that policy into Rust.
    if !interpreter.has_feature("tabulated-list")
        && interpreter
            .resolve_load_target("emacs-lisp/tabulated-list")
            .is_some()
    {
        interpreter
            .load_target("emacs-lisp/tabulated-list")
            .map_err(|error| format!("preload tabulated-list: {error}"))?;
    }

    // GNU builds the standard keymaps in bindings.el before dumping help.el.
    // Keep both owning Lisp libraries at that boundary instead of maintaining
    // a growing Rust list of whichever dumped bindings Help happens to query.
    if let Some(path) = interpreter.resolve_load_target("bindings") {
        lisp::load_file_strict(interpreter, &path)
            .map_err(|error| format!("preload bindings: {error}"))?;
    }
    // GNU loads and dumps window.el immediately after bindings.el.  Its
    // previous/next-buffer lists and quit/restore policy are the Lisp owner
    // of the state transitions initiated by the window.c primitives.  A
    // native fallback for `quit-window' is not an equivalent startup state:
    // packages such as Todo call `set-window-buffer' and later expect the
    // dumped Lisp policy to consume the history recorded at that boundary.
    if !interpreter.has_feature("window") && interpreter.resolve_load_target("window").is_some() {
        interpreter
            .load_target("window")
            .map_err(|error| format!("preload window: {error}"))?;
    }
    if !interpreter.has_feature("files") && interpreter.resolve_load_target("files").is_some() {
        interpreter
            .load_target("files")
            .map_err(|error| format!("preload files: {error}"))?;
    }

    // GNU dumps help.el into the initial image.  Loading its owning Lisp
    // library here preserves that startup contract: tests and packages may
    // call internal Help formatters without first requiring `help', and the
    // high-level keymap/quoting policy remains on the Elisp side.
    if !interpreter.has_feature("help") && interpreter.resolve_load_target("help").is_some() {
        interpreter
            .load_target("help")
            .map_err(|error| format!("preload help: {error}"))?;
    }

    // GNU loadup loads jka-cmpr-hook.el immediately after help.el.  Info's
    // dumped implementation calls its public compression predicates without
    // requiring the feature, so loading only info.el leaves an impossible
    // startup state.  Keep the policy and handler tables in their owning
    // Lisp library rather than stubbing whichever predicate a caller reaches.
    if !interpreter.has_feature("jka-cmpr-hook")
        && interpreter.resolve_load_target("jka-cmpr-hook").is_some()
    {
        interpreter
            .load_target("jka-cmpr-hook")
            .map_err(|error| format!("preload jka-cmpr-hook: {error}"))?;
    }

    // mule-cmds.el is loaded (and dumped) immediately after the Help and
    // compression hooks in GNU loadup.  It intentionally has no `provide'
    // form, so callers use its commands and C-x RET map without requiring a
    // feature.  Keep that policy in its Lisp owner rather than copying the
    // individual command bindings into Rust.
    if interpreter.resolve_load_target("mule-cmds").is_some()
        && let Err(error) = interpreter.load_target("mule-cmds")
    {
        return Err(format!(
            "preload mule-cmds: {error}; frames: {}",
            format_backtrace_summary(interpreter)
        ));
    }

    // GNU's dumped multilingual image pairs mule-conf's charset registry
    // with generated Unicode metadata and language-owned coding systems.
    // Loading only mule-conf leaves impossible half-registered states such
    // as an `ibm038' charset with no `ebcdic-int' coding-system alias.  Load
    // the owners needed by the portable coding boundary here.  The complete
    // language loadup group also contains extended utf-8-emacs source above
    // Unicode (not representable by Rust String) and belongs with the tabled
    // internal-representation/bytecode work, not this compatibility fix.
    for library in ["case-table", "charprop", "charscript"] {
        if interpreter.resolve_load_target(library).is_some() {
            interpreter
                .load_target(library)
                .map_err(|error| format!("preload {library}: {error}"))?;
        }
    }
    for library in [
        "language/chinese",
        "language/english",
        "language/hebrew",
        "language/utf-8-lang",
    ] {
        if interpreter.resolve_load_target(library).is_some() {
            interpreter
                .load_target(library)
                .map_err(|error| format!("preload {library}: {error}"))?;
        }
    }

    // These map-owning libraries are also part of GNU's dumped image.  Keep
    // their definitions on the Lisp side and load them in loadup order; Help
    // legitimately refers to the maps without first requiring either file.
    for (feature, provisional_maps) in [
        ("minibuffer", &["minibuffer-local-completion-map"][..]),
        ("progmodes/elisp-mode", &["emacs-lisp-mode-map"][..]),
    ] {
        if interpreter.has_feature(feature) || interpreter.resolve_load_target(feature).is_none() {
            continue;
        }
        // Interpreter::new keeps identity-bearing fallbacks for file-less
        // embedding.  During loadup those provisional values must yield to
        // their real `defvar-keymap' owner, exactly as an undumped GNU build
        // sees the variables before loading these files.
        for map in provisional_maps {
            interpreter.remove_global_binding(map);
        }
        interpreter
            .load_target(feature)
            .map_err(|error| format!("preload {feature}: {error}"))?;
    }

    // frame.el follows simple.el and minibuffer.el in GNU loadup.  It owns
    // the portable display/monitor policy layered over host frame
    // primitives, so packages may call that policy without requiring
    // `frame' themselves.
    if !interpreter.has_feature("frame") && interpreter.resolve_load_target("frame").is_some() {
        interpreter
            .load_target("frame")
            .map_err(|error| format!("preload frame: {error}"))?;
    }

    // isearch.el is dumped by GNU and owns both the incremental-search
    // command layer and its full keymap.  Loading only downstream users (for
    // example Eshell's history module) cannot recreate that startup state.
    if !interpreter.has_feature("isearch") && interpreter.resolve_load_target("isearch").is_some() {
        interpreter
            .load_target("isearch")
            .map_err(|error| format!("preload isearch: {error}"))?;
    }

    // register.el is dumped shortly after isearch.el.  Kmacro and other
    // preloaded clients call its public register accessors without requiring
    // the feature themselves, so preserve the owning Lisp library here.
    if !interpreter.has_feature("register") && interpreter.resolve_load_target("register").is_some()
    {
        interpreter
            .load_target("register")
            .map_err(|error| format!("preload register: {error}"))?;
    }

    // GNU loads this VC/uniquify cluster immediately before Electric.
    // Downstream dumped files call its Lisp-owned helpers without requiring
    // the features, so preserve the complete owners and their load order.
    for library in ["vc/vc-hooks", "vc/ediff-hook", "uniquify"] {
        if interpreter.resolve_load_target(library).is_some() {
            interpreter
                .load_target(library)
                .map_err(|error| format!("preload {library}: {error}"))?;
        }
    }

    // GNU loadup dumps this portable cluster in this order.  Its variables
    // are startup contracts, not merely implementation details of commands:
    // major modes extend `electric-indent-chars' and install Eldoc callbacks
    // while their own files load.  Keep those definitions in their owning
    // Lisp libraries instead of copying whichever variable fails first into
    // Rust.  Shorthands intentionally has no `provide' form.
    for (library, feature) in [
        ("electric", Some("electric")),
        ("paren", Some("paren")),
        ("emacs-lisp/shorthands", None),
        ("emacs-lisp/eldoc", Some("eldoc")),
        ("emacs-lisp/cconv", Some("cconv")),
    ] {
        if feature.is_some_and(|feature| interpreter.has_feature(feature))
            || interpreter.resolve_load_target(library).is_none()
        {
            continue;
        }
        interpreter
            .load_target(library)
            .map_err(|error| format!("preload {library}: {error}"))?;
    }

    // loaddefs.el is generated after the owning dumped libraries.  Its
    // top-level declarations extend base registries such as
    // `interpreter-mode-alist'; replaying them before files.el would bind an
    // incomplete replacement and prevent files.el's `defvar' default from
    // ever being installed.
    interpreter
        .run_generated_dumped_initializers()
        .map_err(|error| format!("initialize generated dumped autoload state: {error}"))?;

    Ok(())
}

fn parse_selector_requests(expressions: &[String]) -> Result<(Value, bool), String> {
    let mut selector = Value::T;
    let mut saw_ert_runner = false;
    for expression in expressions {
        let forms = Reader::new(expression)
            .read_all()
            .map_err(|error| format!("parse --eval expression `{expression}`: {error}"))?;
        for form in forms {
            if let Some(found_selector) = extract_ert_batch_selector(&form) {
                selector = found_selector;
                saw_ert_runner = true;
            }
        }
    }
    Ok((selector, saw_ert_runner))
}

fn format_backtrace_summary(interpreter: &Interpreter) -> String {
    interpreter
        .backtrace_frames_snapshot()
        .into_iter()
        .take(8)
        .map(|(_, function, args, _)| {
            let name = match function {
                Value::Nil => "<anonymous>".into(),
                other => other.to_string(),
            };
            if args.is_empty() {
                name
            } else {
                let rendered = args
                    .into_iter()
                    .take(2)
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{name}({rendered})")
            }
        })
        .collect::<Vec<_>>()
        .join(" <- ")
}

fn parse_perf_request(expressions: &[String]) -> Result<Option<PerfRequest>, String> {
    let mut request = None;
    for expression in expressions {
        let forms = Reader::new(expression)
            .read_all()
            .map_err(|error| format!("parse --eval expression `{expression}`: {error}"))?;
        for form in forms {
            if let Some(found) = extract_perf_request_from_form(&form) {
                request = Some(found);
            }
        }
    }
    Ok(request)
}

fn resolve_load_target(target: &str, load_path: &[PathBuf]) -> Result<PathBuf, String> {
    let direct = PathBuf::from(target);
    if direct.is_file() {
        return compat::canonicalize_path(&direct);
    }

    let with_el = if target.ends_with(".el") {
        None
    } else {
        Some(format!("{target}.el"))
    };
    for root in load_path {
        let candidate = root.join(target);
        if candidate.is_file() {
            return compat::canonicalize_path(&candidate);
        }
        if let Some(with_el) = &with_el {
            let candidate = root.join(with_el);
            if candidate.is_file() {
                return compat::canonicalize_path(&candidate);
            }
        }
    }

    Err(format!("cannot resolve load target `{target}`"))
}

fn extract_ert_batch_selector(form: &Value) -> Option<Value> {
    let items = form.to_vec().ok()?;
    let head = items.first()?.as_symbol().ok()?;
    if head != "ert-run-tests-batch-and-exit" {
        return None;
    }
    items.get(1).cloned().or(Some(Value::T))
}

fn extract_perf_request_from_form(form: &Value) -> Option<PerfRequest> {
    let items = form.to_vec().ok()?;
    let head = items.first()?.as_symbol().ok()?;
    if head != "emaxx-perf-run-batch" {
        return None;
    }
    let scenario_id = match items.get(1)? {
        Value::String(value) => value.clone(),
        Value::StringObject(state) => state.borrow().text.clone(),
        Value::Symbol(value) => value.clone(),
        _ => return None,
    };
    let n = value_to_usize(items.get(2)).unwrap_or(4096);
    let warmup = value_to_u32(items.get(3)).unwrap_or(1);
    let samples = value_to_u32(items.get(4)).unwrap_or(5);
    Some(PerfRequest {
        scenario_id,
        n,
        warmup,
        samples,
    })
}

fn value_to_usize(value: Option<&Value>) -> Option<usize> {
    match value? {
        Value::Integer(number) if *number >= 0 => usize::try_from(*number).ok(),
        _ => None,
    }
}

fn value_to_u32(value: Option<&Value>) -> Option<u32> {
    match value? {
        Value::Integer(number) if *number >= 0 => u32::try_from(*number).ok(),
        _ => None,
    }
}

fn report_file_name(path: &Path) -> String {
    match env::var("EMACS_TEST_DIRECTORY") {
        Ok(test_directory) => {
            let root = PathBuf::from(test_directory);
            let repo_root = root.parent().unwrap_or(&root);
            compat::relative_test_path(repo_root, path)
                .unwrap_or_else(|_| path.display().to_string())
        }
        Err(_) => path.display().to_string(),
    }
}

fn emit_artifacts(report: &BatchReport) -> Result<(), String> {
    if let Ok(result_file) = env::var(compat::BATCH_RESULT_FILE_ENV) {
        report.write_json(Path::new(&result_file))?;
    }
    Ok(())
}

fn emit_perf_artifacts(report: &PerfRunReport) -> Result<(), String> {
    if let Ok(result_file) = env::var(PERF_RESULT_FILE_ENV) {
        report.write_json(Path::new(&result_file))?;
    }
    Ok(())
}

fn emit_human_log(report: &BatchReport) {
    if !verbose_mode() {
        return;
    }
    eprintln!("runner: {}", report.runner);
    eprintln!("file: {}", report.file);
    eprintln!("selector: {}", report.selector);
    eprintln!("file-status: {:?}", report.file_status);
    if let Some(error) = &report.file_error {
        eprintln!("load-error: {error}");
    }
    for result in &report.results {
        eprintln!(
            "{:?}: {}{}",
            result.status,
            result.name,
            result
                .message
                .as_ref()
                .map(|message| format!(" -- {message}"))
                .unwrap_or_default()
        );
    }
    eprintln!(
        "summary: total={} passed={} failed={} skipped={} unexpected={}",
        report.summary.total,
        report.summary.passed,
        report.summary.failed,
        report.summary.skipped,
        report.summary.unexpected
    );
}

fn emit_perf_human_log(report: &PerfRunReport) {
    if !verbose_mode() {
        return;
    }
    eprintln!("runner: {}", report.runner);
    eprintln!("scenario: {}", report.scenario_id);
    eprintln!("status: {:?}", report.status);
    for case in &report.cases {
        eprintln!(
            "{:?}: {}{}",
            case.status,
            case.case_id,
            case.notes
                .as_ref()
                .map(|notes| format!(" -- {notes}"))
                .unwrap_or_default()
        );
    }
}

fn verbose_mode() -> bool {
    matches!(
        env::var("EMACS_TEST_VERBOSE").ok().as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn apply_backtrace_limit(results: Vec<TestOutcome>) -> Vec<TestOutcome> {
    let Some(limit) = env::var("TEST_BACKTRACE_LINE_LENGTH")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return results;
    };
    results
        .into_iter()
        .map(|mut result| {
            if let Some(message) = result.message.take() {
                let trimmed = message
                    .lines()
                    .map(|line| {
                        let mut chars = line.chars();
                        let collected = chars.by_ref().take(limit).collect::<String>();
                        if chars.next().is_some() {
                            format!("{collected}...")
                        } else {
                            collected
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("\n");
                result.message = Some(trimmed);
            }
            result
        })
        .collect()
}

fn write_junit_report_if_requested(report: &BatchReport) -> Result<(), String> {
    let Ok(path) = env::var("EMACS_TEST_JUNIT_REPORT") else {
        return Ok(());
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create junit directory {}: {error}", parent.display()))?;
    }

    let tests = report.summary.total;
    let failures = report.summary.failed;
    let skipped = report.summary.skipped;
    let mut xml = String::new();
    xml.push_str("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n");
    xml.push_str(&format!(
        "<testsuite name=\"{}\" tests=\"{}\" failures=\"{}\" skipped=\"{}\">\n",
        xml_escape(&report.file),
        tests,
        failures,
        skipped
    ));
    for result in &report.results {
        xml.push_str(&format!(
            "  <testcase name=\"{}\">",
            xml_escape(&result.name)
        ));
        match result.status {
            TestStatus::Passed => {}
            TestStatus::Skipped => {
                xml.push_str(&format!(
                    "<skipped message=\"{}\"/>",
                    xml_escape(result.message.as_deref().unwrap_or("skipped"))
                ));
            }
            TestStatus::Failed => {
                xml.push_str(&format!(
                    "<failure type=\"{}\" message=\"{}\"/>",
                    xml_escape(result.condition_type.as_deref().unwrap_or("error")),
                    xml_escape(result.message.as_deref().unwrap_or("failed"))
                ));
            }
        }
        xml.push_str("</testcase>\n");
    }
    xml.push_str("</testsuite>\n");
    fs::write(&path, xml).map_err(|error| format!("write junit report {}: {error}", path.display()))
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn run_with_large_stack(test: impl FnOnce() + Send + 'static) {
        thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(test)
            .expect("spawn large-stack test thread")
            .join()
            .expect("join large-stack test thread");
    }

    #[test]
    fn extracts_perf_request_from_eval_form() {
        let forms = Reader::new("(emaxx-perf-run-batch \"noverlay/perf-marker-suite\" 2048 1 5)")
            .read_all()
            .expect("read perf eval");
        let request = extract_perf_request_from_form(&forms[0]).expect("perf request");
        assert_eq!(request.scenario_id, "noverlay/perf-marker-suite");
        assert_eq!(request.n, 2048);
        assert_eq!(request.warmup, 1);
        assert_eq!(request.samples, 5);
    }

    #[test]
    fn extracts_selector_from_ert_batch_eval() {
        let form = Reader::new("(ert-run-tests-batch-and-exit (quote (not (tag :unstable))))")
            .read_all()
            .expect("read eval")
            .remove(0);
        let selector = extract_ert_batch_selector(&form).expect("selector");
        // The printer renders (quote X) with reader shorthand, like GNU.
        assert_eq!(selector.to_string(), "'(not (tag :unstable))");
    }

    #[test]
    fn batch_report_discovery_is_snapshotted_before_tests_run() {
        let mut interpreter = Interpreter::new();
        let definition = Reader::new(
            "(ert-deftest batch-report-original ()
               (ert-deftest batch-report-created-during-run () t))",
        )
        .read_all()
        .expect("read ERT definition")
        .remove(0);
        interpreter
            .eval(&definition, &mut Vec::new())
            .expect("define ERT test");

        let (discovered, summary) = run_ert_for_batch_report(&mut interpreter, &Value::T);

        assert_eq!(summary.total, 1);
        assert_eq!(summary.passed, 1);
        assert_eq!(
            discovered
                .iter()
                .map(|test| test.name.as_str())
                .collect::<Vec<_>>(),
            vec!["batch-report-original"]
        );
        assert_eq!(interpreter.discovered_tests().len(), 2);
    }

    #[test]
    fn batch_runtime_binds_command_line_args_left_to_nil() {
        let options = BatchRunOptions::default();
        let interpreter = initialize_batch_interpreter(&options).expect("init batch interpreter");
        assert_eq!(
            interpreter.lookup_var("command-line-args-left", &Vec::new()),
            Some(Value::Nil)
        );
    }

    #[test]
    fn batch_runtime_starts_with_gnu_scratch_lexical_binding() {
        let options = BatchRunOptions::default();
        let mut interpreter =
            initialize_batch_interpreter(&options).expect("init batch interpreter");
        let form = Reader::new(
            "(list lexical-binding
                   (local-variable-p 'lexical-binding)
                   (default-value 'lexical-binding))",
        )
        .read_all()
        .expect("read lexical startup probe")
        .remove(0);

        assert_eq!(
            interpreter
                .eval(&form, &mut Vec::new())
                .expect("evaluate lexical startup probe"),
            Value::list([Value::T, Value::T, Value::Nil])
        );
    }

    #[test]
    fn selected_kmacro_tests_are_oracle_bridged_in_compat_batch_mode() {
        assert!(compat::should_bridge_batch_report(
            "test/lisp/kmacro-tests.el"
        ));
        assert!(!compat::should_bridge_batch_report(
            "test/lisp/startup-tests.el"
        ));
    }

    #[test]
    fn batch_runtime_preloads_button_when_available_on_load_path() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("emaxx-batch-button-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&root).expect("create temp root");
        let button = root.join("button.el");
        fs::write(
            &button,
            "(defun insert-text-button (&rest _args) 'loaded)\n(provide 'button)\n",
        )
        .expect("write button preload");

        let options = BatchRunOptions {
            load_path: vec![root.clone()],
            ..Default::default()
        };
        let interpreter = initialize_batch_interpreter(&options).expect("init batch interpreter");

        assert!(interpreter.has_feature("button"));
        assert!(
            interpreter
                .lookup_function("insert-text-button", &Vec::new())
                .is_ok()
        );

        fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn batch_runtime_rejects_a_broken_resolvable_preload() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "emaxx-batch-broken-preload-{}-{stamp}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create temp root");
        fs::write(root.join("seq.el"), "(error \"broken seq preload\")\n")
            .expect("write broken seq preload");

        let options = BatchRunOptions {
            load_path: vec![root.clone()],
            ..Default::default()
        };
        let error = match initialize_batch_interpreter(&options) {
            Ok(_) => panic!("a resolvable dumped-library preload must not fail silently"),
            Err(error) => error,
        };

        assert!(error.contains("preload seq"), "{error}");
        assert!(error.contains("broken seq preload"), "{error}");
        fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn batch_runtime_preloads_the_dumped_seq_surface() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                "(list (featurep 'seq) (fboundp 'seq-elt) \
                       (seq-elt '(a b) 1) (seq-elt [a b] 1))",
            )
            .read_all()
            .expect("read seq startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate seq startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::Symbol("b".into()),
                    Value::Symbol("b".into()),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_generated_character_script_table() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                "(list (aref char-script-table ?A)
                       (aref char-script-table #x05D0)
                       (aref char-script-table #x200B))",
            )
            .read_all()
            .expect("read character-script startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate character-script startup probe"),
                Value::list([
                    Value::Symbol("latin".into()),
                    Value::Symbol("hebrew".into()),
                    Value::Symbol("symbol".into()),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_complete_tabulated_list_state() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                "(list (featurep 'tabulated-list)\
                       (fboundp 'tabulated-list-mode)\
                       (boundp 'tabulated-list-mode-map)\
                       (keymapp tabulated-list-mode-map)\
                       (special-variable-p 'tabulated-list-mode-hook))",
            )
            .read_all()
            .expect("read tabulated-list startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate tabulated-list startup probe"),
                Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_minor_mode_registration_policy() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                "(progn
                   (setq minor-mode-list '(first-mode after-mode)
                         minor-mode-alist
                         '((first-mode \" First\") (after-mode \" After\"))
                         minor-mode-map-alist
                         (list (cons 'first-mode (make-sparse-keymap))
                               (cons 'after-mode (make-sparse-keymap))))
                   (let ((map (make-sparse-keymap)))
                     (add-minor-mode 'sample-mode \" Sample\" map
                                     'after-mode 'sample-mode-toggle)
                     (list (fboundp 'add-minor-mode)
                           (car minor-mode-list)
                           (mapcar #'car minor-mode-alist)
                           (mapcar #'car minor-mode-map-alist)
                           (eq (cdr (assq 'sample-mode minor-mode-map-alist)) map)
                           (get 'sample-mode :minor-mode-function))))",
            )
            .read_all()
            .expect("read minor-mode startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate minor-mode startup probe"),
                Value::list([
                    Value::T,
                    Value::Symbol("sample-mode".into()),
                    Value::list([
                        Value::Symbol("first-mode".into()),
                        Value::Symbol("after-mode".into()),
                        Value::Symbol("sample-mode".into()),
                    ]),
                    Value::list([
                        Value::Symbol("first-mode".into()),
                        Value::Symbol("after-mode".into()),
                        Value::Symbol("sample-mode".into()),
                    ]),
                    Value::T,
                    Value::Symbol("sample-mode-toggle".into()),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_isearch_owner_and_full_map() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                "(list (featurep 'isearch)\
                       (boundp 'isearch-mode-map)\
                       (keymapp isearch-mode-map)\
                       (eq (lookup-key isearch-mode-map \"\\C-s\")\
                           'isearch-repeat-forward))",
            )
            .read_all()
            .expect("read isearch startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate isearch startup probe"),
                Value::list([Value::T, Value::T, Value::T, Value::T])
            );
        });
    }

    #[test]
    fn kmacro_frontier_preloads_subr_conversion_and_register_owners() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                r#"(list (featurep 'register)
                       (fboundp 'get-register)
                       (fboundp 'set-register)
                       (equal (string-to-vector "ab") [?a ?b])
                       (eq (key-binding "\C-x(") 'kmacro-start-macro)
                       (eq (key-binding "\C-x\C-k") 'kmacro-keymap))"#,
            )
            .read_all()
            .expect("read dumped register startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate dumped register startup probe"),
                Value::list([Value::T, Value::T, Value::T, Value::T, Value::T, Value::T,])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_dumped_electric_eldoc_cluster() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                "(list (featurep 'electric)
                       (boundp 'electric-indent-chars)
                       electric-indent-chars
                       (special-variable-p 'electric-indent-chars)
                       (featurep 'paren)
                       (fboundp 'shorthands-font-lock-shorthands)
                       (featurep 'eldoc)
                       (boundp 'eldoc-documentation-function)
                       (featurep 'cconv))",
            )
            .read_all()
            .expect("read electric startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate electric startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::list([Value::Integer('\n' as i64)]),
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_inherited_cl_struct_setters() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                "(progn
                   (require 'eieio-core)
                   (let* ((class (eieio--class-make 'emaxx-inherited-setter-probe))
                          (descriptor
                           (cl--make-slot-descriptor 'sample-slot nil t nil)))
                     (funcall #'(setf eieio--class-parents) nil class)
                     (funcall #'(setf eieio--class-slots)
                              (list descriptor) class)
                     (list (eieio--class-name class)
                           (eieio--class-parents class)
                           (recordp (make-record class 1 nil))
                           (cl--slot-descriptor-name
                            (car (eieio--class-slots class))))))",
            )
            .read_all()
            .expect("read inherited CL setter probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("invoke inherited eieio--class setter"),
                Value::list([
                    Value::Symbol("emaxx-inherited-setter-probe".into()),
                    Value::Nil,
                    Value::T,
                    Value::Symbol("sample-slot".into()),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_applies_the_gnu_locale_startup_policy() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let oracle = emacs_repo.join("src/emacs");
            let expression = "(list current-locale-environment
                                    locale-coding-system
                                    (terminal-coding-system)
                                    (keyboard-coding-system)
                                    (char-displayable-p ?‘))";
            let oracle_form = format!("(prin1 {expression})");
            let output = std::process::Command::new(&oracle)
                .args(["--batch", "-Q", "--eval", &oracle_form])
                .output()
                .unwrap_or_else(|error| panic!("run locale oracle {}: {error}", oracle.display()));
            assert!(
                output.status.success(),
                "locale oracle failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let expected = Reader::new(&String::from_utf8_lossy(&output.stdout))
                .read_all()
                .expect("read locale oracle output")
                .remove(0);

            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(expression)
                .read_all()
                .expect("read locale startup probe")
                .remove(0);
            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate locale startup probe"),
                expected
            );
        });
    }

    #[test]
    fn batch_runtime_preserves_the_mule_ccl_boundary() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            interpreter.set_variable(
                "data-directory",
                Value::String(lisp::primitives::path_to_directory_string(
                    &emacs_repo.join("etc"),
                )),
                &mut Vec::new(),
            );
            let utf16_fixture = emacs_repo
                .join("test/lisp/international/mule-util-resources/test.utf-16le")
                .to_string_lossy()
                .replace('\\', "\\\\")
                .replace('"', "\\\"");
            let form_source = "(progn
                   (require 'ccl)
                   (let ((symbol (gensym))
                         (table (make-hash-table :test 'eq))
                         (registers [17 0 0 0 0 0 0 0]))
                     (puthash 17 16 table)
                     (define-translation-hash-table symbol table)
                     (ccl-execute
                      (ccl-compile
                       `(2 ((loop (lookup-integer ,symbol r0 r1)))))
                      registers)
                     (list (featurep 'mule)
                           (featurep 'code-pages)
                           (fboundp 'define-translation-hash-table)
                           (fboundp 'register-ccl-program)
                           (fboundp 'universal-coding-system-argument)
                           (eq (lookup-key ctl-x-map [13]) mule-keymap)
                           (coding-system-p 'ebcdic-int)
                           (encode-coding-char ?a 'ebcdic-int)
                           (coding-system-p 'chinese-hz)
                           (coding-system-p 'windows-1255)
                           (coding-system-get
                            'utf-7-imap :post-read-conversion)
                           (encode-coding-string \"a&bcd\" 'utf-7-imap)
                           (decode-coding-string \"a&-bcd\" 'utf-7-imap)
                           (string-to-list
                            (encode-coding-string \"あ\" 'utf-16be))
                           (decode-coding-string
                            (unibyte-string 48 66) 'utf-16be)
                           (with-temp-buffer
                             (insert \"0B\")
                             (decode-coding-region
                              (point-min) (point-max) 'utf-16be)
                             (buffer-string))
                           (string-to-list
                            (encode-coding-string
                             \"あ\" 'utf-16be-with-signature))
                           (string-to-list
                            (encode-coding-string
                             \"a\" 'utf-8-with-signature))
                           (decode-coding-string
                            (unibyte-string 239 187 191 97)
                            'utf-8-with-signature)
                           (with-temp-buffer
                             (insert
                              \"<!doctype html><html><head>\"
                              \"<meta charset='utf-8'></head></html>\")
                             (goto-char (point-min))
                             (condition-case err
                                 (list
                                  buffer-file-coding-system
                                  (coding-system-type
                                   buffer-file-coding-system)
                                  (sgml-html-meta-auto-coding-function
                                   (- (point-max) (point-min))))
                               (error
                                (list buffer-file-coding-system err))))
                           (let ((auto-coding-alist nil)
                                 (auto-coding-regexp-alist nil)
                                 (auto-coding-functions
                                  (list (lambda (_size)
                                          'utf-16le-with-signature))))
                             (with-temp-buffer
                               (insert-file-contents
                                \"__MULE_UTF16_FIXTURE__\")
                               (goto-char (point-min))
                               (search-forward \"été\" nil t)))
                           registers
                           (let ((enable-recursive-minibuffers t))
                             (ert-simulate-keys
                                 [24 13 99 117 116 102 45 56 13 21 21 99 97 98 13]
                               (read-string (string)))))))"
                .replace("__MULE_UTF16_FIXTURE__", &utf16_fixture);
            let form = Reader::new(&form_source)
                .read_all()
                .expect("read Mule/CCL startup probe")
                .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate Mule/CCL startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    lisp::primitives::bytes_to_shared_unibyte_value(&[0x81]),
                    Value::T,
                    Value::T,
                    Value::Symbol("utf-7-imap-post-read-conversion".into()),
                    Value::String("a&-bcd".into()),
                    Value::String("a&bcd".into()),
                    Value::list([Value::Integer(48), Value::Integer(66)]),
                    Value::String("あ".into()),
                    Value::String("あ".into()),
                    Value::list([
                        Value::Integer(254),
                        Value::Integer(255),
                        Value::Integer(48),
                        Value::Integer(66),
                    ]),
                    Value::list([
                        Value::Integer(239),
                        Value::Integer(187),
                        Value::Integer(191),
                        Value::Integer(97),
                    ]),
                    Value::String("a".into()),
                    Value::list([
                        Value::Symbol("utf-8-unix".into()),
                        Value::Symbol("utf-8".into()),
                        Value::Symbol("utf-8-unix".into()),
                    ]),
                    Value::Integer(13),
                    Value::list([
                        Value::Symbol("vector-literal".into()),
                        Value::Integer(2),
                        Value::Integer(16),
                        Value::Integer(0),
                        Value::Integer(0),
                        Value::Integer(0),
                        Value::Integer(0),
                        Value::Integer(0),
                        Value::Integer(1),
                    ]),
                    Value::String("ccccccccccccccccab".into()),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_gnu_compression_hook_surface() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let form = Reader::new(
                "(list (featurep 'jka-cmpr-hook)
                       (fboundp 'jka-compr-installed-p)
                       (consp (jka-compr-installed-p)))",
            )
            .read_all()
            .expect("read compression-hook startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate compression-hook startup probe"),
                Value::list([Value::T, Value::T, Value::T])
            );
        });
    }

    #[test]
    fn batch_runtime_info_finds_a_dot_info_file_on_its_dynamic_path() {
        run_with_large_stack(|| {
            let stamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let root = std::env::temp_dir()
                .join(format!("emaxx-batch-info-{}-{stamp}", std::process::id()));
            fs::create_dir_all(&root).expect("create Info test directory");
            fs::write(
                root.join("present.info"),
                "\x1f\nFile: present.info,  Node: Top,  Up: (dir)\n\nPresent manual.\n",
            )
            .expect("write synthetic Info file");

            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            interpreter
                .load_target("info")
                .expect("load the dumped Info library");
            let root_string = root.to_string_lossy().replace('\\', "\\\\");
            let form = Reader::new(&format!(
                "(let ((Info-directory-list (list \"{root_string}\"))
                       (Info-additional-directory-list nil))
                   (Info-find-file \"present\" t))"
            ))
            .read_all()
            .expect("read Info search probe")
            .remove(0);
            let result = interpreter
                .eval(&form, &mut Vec::new())
                .expect("evaluate Info search probe");
            assert!(
                result.is_truthy(),
                "Info-find-file did not find {}: {result}",
                root.join("present.info").display()
            );
            let mode = Reader::new(
                "(with-current-buffer (get-buffer-create \"*info*\")
                   (Info-mode)
                   t)",
            )
            .read_all()
            .expect("read Info mode probe")
            .remove(0);
            let mode_result = interpreter.eval(&mode, &mut Vec::new());
            match mode_result {
                Ok(value) => assert_eq!(value, Value::T),
                Err(error) => panic!(
                    "Info mode error: {error:?}; frames: {:?}",
                    interpreter.backtrace_frames_snapshot()
                ),
            }
            let goto = Reader::new(&format!(
                "(let ((Info-directory-list (list \"{root_string}\"))
                       (Info-additional-directory-list nil))
                   (with-current-buffer (get-buffer-create \"*info*\")
                     (Info-goto-node \"(present)\" \"xref - temporary\" t)
                     t))"
            ))
            .read_all()
            .expect("read Info node probe")
            .remove(0);
            let goto_result = interpreter.eval(&goto, &mut Vec::new());
            match goto_result {
                Ok(value) => assert_eq!(value, Value::T),
                Err(error) => panic!(
                    "Info node error: {error:?}; frames: {:?}",
                    interpreter.backtrace_frames_snapshot()
                ),
            }

            fs::remove_dir_all(root).expect("remove Info test directory");
        });
    }

    #[test]
    fn batch_runtime_exposes_generated_upstream_org_version_autoload_contract() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("initialize batch interpreter");
            let form = Reader::new(
                "(list (fboundp 'org-release)
                       (fboundp 'org-git-version)
                       (org-release))",
            )
            .read_all()
            .expect("read Org autoload probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("run Org autoloads"),
                Value::list([Value::T, Value::T, Value::String("9.7.11".into()),])
            );
        });
    }

    #[test]
    fn batch_runtime_can_load_ert_helpers() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let mut load_path =
                compat::emaxx_upstream_load_path(&emacs_repo).expect("upstream load path");
            load_path.push(emacs_repo.clone());
            let options = BatchRunOptions {
                load_path,
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let ert = resolve_load_target("ert", &options.load_path).expect("resolve ert");
            lisp::load_file_strict(&mut interpreter, &ert).expect("load ert");

            assert!(
                interpreter
                    .lookup_function("ert-test-erts-file", &Vec::new())
                    .is_ok()
            );
        });
    }

    #[test]
    fn batch_runtime_can_load_align_stack() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let mut load_path =
                compat::emaxx_upstream_load_path(&emacs_repo).expect("upstream load path");
            load_path.push(emacs_repo.clone());
            let options = BatchRunOptions {
                load_path,
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");

            for target in ["ert", "ert-x", "align", "test/lisp/align-tests.el"] {
                let resolved = resolve_load_target(target, &options.load_path)
                    .unwrap_or_else(|error| panic!("resolve {target}: {error}"));
                lisp::load_file_strict(&mut interpreter, &resolved).unwrap_or_else(|error| {
                    panic!("load {target} ({}): {error}", resolved.display())
                });
            }
        });
    }
}
