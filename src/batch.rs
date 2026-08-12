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
                let resolved = resolve_load_target(
                    target,
                    &options.load_path,
                    interpreter.prefers_compiled_loads(),
                )?;
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
                let form = Value::list([Value::Symbol(function.clone().into())]);
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
        Value::list(
            options
                .args_left
                .iter()
                .cloned()
                .map(|value| Value::String(value.into())),
        ),
        &mut Vec::new(),
    );
    // Loading the dumped Lisp owners below corresponds to GNU's pre-dump
    // phase, where delayed Custom initializers accumulate until startup.
    interpreter.set_variable("custom-delayed-init-variables", Value::Nil, &mut Vec::new());
    configure_batch_source_provenance(&mut interpreter)?;
    preload_batch_compat_libraries(&mut interpreter)?;
    // The reconstructed dumped image still comes from source.  Compiled
    // resolution is enabled only afterward; making the preload itself use
    // `.elc' requires a coherent dumped-image/runtime project of its own.
    interpreter.set_prefer_compiled_loads(lisp::bytecode_vm_enabled());
    initialize_batch_documentation(&mut interpreter)?;
    complete_delayed_custom_initialization(&mut interpreter)?;
    initialize_batch_locale_environment(&mut interpreter)?;
    Ok(interpreter)
}

fn configure_batch_source_provenance(interpreter: &mut Interpreter) -> Result<(), String> {
    let Ok(dump_root) = env::var(compat::DUMP_SOURCE_DIRECTORY_ENV) else {
        return Ok(());
    };
    let test_directory = env::var("EMACS_TEST_DIRECTORY")
        .map_err(|error| format!("resolve runtime source directory: {error}"))?;
    let runtime_test_directory = PathBuf::from(test_directory);
    let runtime_root = runtime_test_directory.parent().ok_or_else(|| {
        format!(
            "runtime test directory has no source-tree parent: {}",
            runtime_test_directory.display()
        )
    })?;
    // The test file itself and test-owned helper libraries stay tied to the
    // disposable checkout.  GNU's standard Lisp load path, by contrast,
    // still names the tree that built the executable; map only that `lisp/'
    // subtree while continuing to read its isolated physical counterpart.
    interpreter.set_load_source_provenance_remap(
        runtime_root.join("lisp"),
        Path::new(&dump_root).join("lisp"),
    );
    Ok(())
}

fn initialize_batch_documentation(interpreter: &mut Interpreter) -> Result<(), String> {
    // dump-mode.el calls `Snarf-documentation' after the native definitions
    // and dumped Lisp owners have been installed.  Reconstruct that phase so
    // variable doc references and C source provenance have the same shape at
    // the batch boundary.  Embedded interpreters with no installed DOC file
    // deliberately remain usable.
    let form = Reader::new(
        "(let ((doc-file (expand-file-name internal-doc-file-name doc-directory)))
           (when (file-readable-p doc-file)
             (Snarf-documentation internal-doc-file-name)))",
    )
    .read_all()
    .map_err(|error| format!("read batch documentation startup form: {error}"))?
    .remove(0);
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("initialize batch documentation: {error}"))?;
    Ok(())
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
    // byte-run.el is the first portable Lisp owner loaded by GNU loadup.  It
    // intentionally has no `provide' form, but installs the declaration
    // expanders used by later libraries (speed, safety, compiler-macro, and
    // friends).  Load the complete owner rather than copying whichever
    // declaration helper a downstream macro happens to call.
    if interpreter
        .resolve_load_target("emacs-lisp/byte-run")
        .is_some()
    {
        interpreter
            .load_target("emacs-lisp/byte-run")
            .map_err(|error| format!("preload emacs-lisp/byte-run: {error}"))?;
    }

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
    // GNU loads the complete subr.el into the dumped startup image.  It has no
    // `provide' form, so feature checks cannot represent that startup fact.
    // Keep simple_compat.el as the file-less/bootstrap substrate needed while
    // reconstructing the image, then load the real Lisp owner so initialized
    // batch execution does not depend on a growing hand-copied subset.
    if interpreter.resolve_load_target("subr").is_some() {
        // Interpreter::new installs an identity-bearing global-map fallback
        // for file-less embeddings.  GNU subr.el owns the initialized map,
        // including its full character table; let its defvar initializer run
        // instead of retaining the provisional map and a parallel Rust list
        // of default bindings.
        interpreter.remove_global_binding("global-map");
        interpreter
            .load_target("subr")
            .map_err(|error| format!("preload subr: {error}"))?;
    }
    // GNU loadup loads the complete widget.el and custom.el owners between
    // keymap/subr bootstrap and face declarations.  Cus-face and loaded theme
    // files call Custom helpers (notably `custom-check-theme') without local
    // requires because those functions are already dumped.  Preserve that
    // Elisp ownership and order instead of recreating theme policy in Rust.
    for feature in ["widget", "custom"] {
        if interpreter.has_feature(feature) || interpreter.resolve_load_target(feature).is_none() {
            continue;
        }
        interpreter
            .load_target(feature)
            .map_err(|error| format!("preload {feature}: {error}"))?;
    }
    // GNU loads and dumps cus-face.el and faces.el after the native face
    // vectors exist.  faces_compat.el remains the file-less/bootstrap
    // substrate above; initialized batch execution must expose the complete
    // Elisp owner rather than pinning its high-level query policy in Rust.
    for feature in ["cus-face", "faces"] {
        if interpreter.has_feature(feature) || interpreter.resolve_load_target(feature).is_none() {
            continue;
        }
        interpreter
            .load_target(feature)
            .map_err(|error| format!("preload {feature}: {error}"))?;
    }
    // GNU loadup dumps the complete terminal-color owner before font-core
    // and font-lock.  faces.el deliberately calls this Elisp API without a
    // local require, so an initialized runtime must reconstruct the same
    // preload fact rather than depend on the bootstrap subset.
    if !interpreter.has_feature("term/tty-colors")
        && interpreter.resolve_load_target("term/tty-colors").is_some()
    {
        interpreter
            .load_target("term/tty-colors")
            .map_err(|error| format!("preload term/tty-colors: {error}"))?;
    }
    // GNU dumps abbrev.el before language modes are loaded.  It owns active
    // table traversal, expansion hooks, case handling, and usage state; the
    // native layer supplies the table/obarray substrate and self-insert's
    // syntax trigger, but should not grow a second abbreviation engine.
    if !interpreter.has_feature("abbrev") && interpreter.resolve_load_target("abbrev").is_some() {
        interpreter
            .load_target("abbrev")
            .map_err(|error| format!("preload abbrev: {error}"))?;
    }
    // Reconstruct GNU loadup's complete Font Lock owner sequence.  The native
    // layer remains the file-less substrate, but initialized execution must
    // use font-core.el's mode lifecycle and syntax.el/font-lock.el/jit-lock.el
    // policy rather than a hand-maintained subset of their dumped state.
    for (feature, library) in [
        ("font-core", "font-core"),
        ("syntax", "emacs-lisp/syntax"),
        ("font-lock", "font-lock"),
        ("jit-lock", "jit-lock"),
    ] {
        if interpreter.has_feature(feature) || interpreter.resolve_load_target(library).is_none() {
            continue;
        }
        interpreter
            .load_target(library)
            .map_err(|error| format!("preload {library}: {error}"))?;
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
        // Interpreter::new keeps a compact auto-mode fallback for file-less
        // embeddings.  GNU files.el owns the initialized registry; its
        // `defvar' must see the cell unbound so the complete base table is in
        // place before generated loaddefs extend it below.
        interpreter.remove_global_binding("auto-mode-alist");
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
    for library in ["case-table", "charprop", "characters", "charscript"] {
        if interpreter.resolve_load_target(library).is_some() {
            interpreter
                .load_target(library)
                .map_err(|error| format!("preload {library}: {error}"))?;
        }
    }
    for library in [
        "language/chinese",
        "language/english",
        "language/european",
        "language/hebrew",
        "language/utf-8-lang",
    ] {
        if interpreter.resolve_load_target(library).is_some() {
            interpreter
                .load_target(library)
                .map_err(|error| format!("preload {library}: {error}"))?;
        }
    }

    // GNU loads indent.el immediately before cl-generic and simple.el.  Its
    // TAB command and indentation orchestration are Lisp policy layered over
    // native buffer primitives; preload the complete owner instead of growing
    // a second, partial command implementation as language modes exercise it.
    if !interpreter.has_feature("indent") && interpreter.resolve_load_target("indent").is_some() {
        interpreter
            .load_target("indent")
            .map_err(|error| format!("preload indent: {error}"))?;
    }

    // GNU dumps simple.el before minibuffer.el.  It owns the completion-list
    // navigation and selection commands used by Minibuffer's M-up/M-down
    // bindings.  Keep those command policies in the standard GNU library
    // instead of maintaining local copies alongside the native minibuffer
    // substrate.
    if !interpreter.has_feature("simple") && interpreter.resolve_load_target("simple").is_some() {
        interpreter
            .load_target("simple")
            .map_err(|error| format!("preload simple: {error}"))?;
    }

    // Minibuffer is also part of GNU's dumped image.  Keep its definitions on
    // the Lisp side; Help legitimately refers to this map without requiring
    // the feature first.
    for (feature, provisional_maps) in [("minibuffer", &["minibuffer-local-completion-map"][..])] {
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

    // GNU loadup dumps easymenu.el after frame.el and before isearch.el.
    // Packages such as EUDC call its menu constructors without requiring the
    // feature, so preserve the complete Lisp-owned menu policy at startup.
    if !interpreter.has_feature("easymenu")
        && interpreter
            .resolve_load_target("emacs-lisp/easymenu")
            .is_some()
    {
        interpreter
            .load_target("emacs-lisp/easymenu")
            .map_err(|error| format!("preload easymenu: {error}"))?;
    }

    // isearch.el is dumped by GNU and owns both the incremental-search
    // command layer and its full keymap.  Loading only downstream users (for
    // example Eshell's history module) cannot recreate that startup state.
    if !interpreter.has_feature("isearch") && interpreter.resolve_load_target("isearch").is_some() {
        interpreter
            .load_target("isearch")
            .map_err(|error| format!("preload isearch: {error}"))?;
    }

    // menu-bar.el follows isearch.el in GNU loadup and is part of the dumped
    // image.  Mode libraries therefore expand its Lisp-owned menu macros
    // without requiring `menu-bar' first.  Load the complete owner here;
    // runtime keymaps retain their Rust identity while exposing GNU's mutable
    // cons-list interface to this library.
    if !interpreter.has_feature("menu-bar") && interpreter.resolve_load_target("menu-bar").is_some()
    {
        interpreter
            .load_target("menu-bar")
            .map_err(|error| format!("preload menu-bar: {error}"))?;
    }

    // GNU loadup loads and dumps tab-bar.el immediately after menu-bar.el.
    // Its frame-local tab data, commands, and undo policy are Elisp-owned
    // startup state: callers may use them without requiring `tab-bar' first.
    // Reconstruct the complete owner here rather than mirroring whichever
    // entry point a test happens to reach in the Rust host.
    if !interpreter.has_feature("tab-bar") && interpreter.resolve_load_target("tab-bar").is_some() {
        interpreter
            .load_target("tab-bar")
            .map_err(|error| format!("preload tab-bar: {error}"))?;
    }

    // GNU dumps emacs-lisp/lisp.el immediately after the menu libraries.
    // Its structural navigation commands (including forward/backward-list)
    // are Lisp policy layered over the native syntax scanner and are called
    // directly by mode libraries such as js.el.
    if !interpreter.has_feature("lisp")
        && interpreter.resolve_load_target("emacs-lisp/lisp").is_some()
    {
        interpreter
            .load_target("emacs-lisp/lisp")
            .map_err(|error| format!("preload lisp: {error}"))?;
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

    // paragraphs.el follows register.el in GNU loadup and intentionally has
    // no `provide' form.  It owns `use-hard-newlines' and the complete
    // paragraph/sentence policy used directly by dumped clients such as So
    // Long, so reconstruct the file itself rather than copying whichever
    // missing entry point a client happens to expose.
    if interpreter
        .resolve_load_target("textmodes/paragraphs")
        .is_some()
    {
        interpreter
            .load_target("textmodes/paragraphs")
            .map_err(|error| format!("preload textmodes/paragraphs: {error}"))?;
    }

    // GNU loadup establishes the programming-mode parent before loading the
    // shared Lisp modes.  Loading lisp-mode first leaves its derived-mode
    // parent pointing at Emaxx's file-less bootstrap fallback, which skips
    // the Elisp-owned reset/hook lifecycle.  Provisional identity-bearing
    // maps must yield before each real `defvar-keymap' owner runs.
    for (library, feature, provisional_maps) in [
        ("progmodes/prog-mode", "prog-mode", &[][..]),
        (
            "emacs-lisp/lisp-mode",
            "lisp-mode",
            &["lisp-mode-shared-map", "lisp-mode-map"][..],
        ),
    ] {
        if interpreter.has_feature(feature) || interpreter.resolve_load_target(library).is_none() {
            continue;
        }
        for map in provisional_maps {
            interpreter.remove_global_binding(map);
        }
        interpreter
            .load_target(library)
            .map_err(|error| format!("preload {library}: {error}"))?;
    }

    // GNU loadup dumps the complete fill.el owner after the standard mode
    // cluster.  The file intentionally has no `provide' form, so reload it
    // after simple_compat.el's file-less no-op substrate and let its real
    // paragraph engine own initialized batch execution.
    if interpreter.resolve_load_target("textmodes/fill").is_some() {
        interpreter
            .load_target("textmodes/fill")
            .map_err(|error| format!("preload textmodes/fill: {error}"))?;
    }

    // GNU preloads newcomment.el immediately before replace.el.  It owns the
    // comment-motion and editing policy used by startup predicates such as So
    // Long's leading-comment scan; preserve that complete Elisp owner rather
    // than treating a swallowed `void-function' as a negative predicate.
    if !interpreter.has_feature("newcomment")
        && interpreter.resolve_load_target("newcomment").is_some()
    {
        interpreter
            .load_target("newcomment")
            .map_err(|error| format!("preload newcomment: {error}"))?;
    }

    // replace.el follows the standard mode/register cluster in GNU loadup
    // and is part of the dumped image.  Its Occur and query-replace engines
    // are Elisp policy; tests and preloaded clients legitimately call them
    // without requiring `replace', so reconstruct that same owner here.
    if !interpreter.has_feature("replace") && interpreter.resolve_load_target("replace").is_some() {
        interpreter
            .load_target("replace")
            .map_err(|error| format!("preload replace: {error}"))?;
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

    // GNU loads and dumps the Emacs Lisp mode after its parent modes and the
    // portable Electric/Eldoc/Cconv cluster.  Keep that final mode policy in
    // its owning Elisp file rather than allowing the native bootstrap arm to
    // remain the visible definition in a full batch runtime.
    if !interpreter.has_feature("elisp-mode")
        && interpreter
            .resolve_load_target("progmodes/elisp-mode")
            .is_some()
    {
        interpreter.remove_global_binding("emacs-lisp-mode-map");
        interpreter
            .load_target("progmodes/elisp-mode")
            .map_err(|error| format!("preload progmodes/elisp-mode: {error}"))?;
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

fn resolve_load_target(
    target: &str,
    load_path: &[PathBuf],
    prefer_compiled: bool,
) -> Result<PathBuf, String> {
    let direct = PathBuf::from(target);
    if direct.is_file() {
        return compat::canonicalize_path(&direct);
    }

    let bare_target = !target.ends_with(".el") && !target.ends_with(".elc");
    let with_el = bare_target.then(|| format!("{target}.el"));
    let with_elc = bare_target.then(|| format!("{target}.elc"));
    for root in load_path {
        let candidate = root.join(target);
        if candidate.is_file() {
            return compat::canonicalize_path(&candidate);
        }
        if prefer_compiled && let Some(with_elc) = &with_elc {
            let candidate = root.join(with_elc);
            if candidate.is_file() {
                return compat::canonicalize_path(&candidate);
            }
        }
        if let Some(with_el) = &with_el {
            let candidate = root.join(with_el);
            if candidate.is_file() {
                return compat::canonicalize_path(&candidate);
            }
        }
        if let Some(with_elc) = &with_elc {
            let candidate = root.join(with_elc);
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
        Value::String(value) => value.to_string(),
        Value::StringObject(state) => state.borrow().text.clone(),
        Value::Symbol(value) => value.to_string(),
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
        let permit = crate::test_support::acquire_host_test_permit();
        thread::Builder::new()
            .stack_size(128 * 1024 * 1024)
            .spawn(move || {
                let _permit = permit;
                test();
            })
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
    fn batch_load_resolution_prefers_elc_only_when_requested() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = env::temp_dir().join(format!("emaxx-batch-load-resolution-{unique}"));
        fs::create_dir_all(&root).expect("create load directory");
        fs::write(root.join("sample.el"), "source").expect("write source");
        fs::write(root.join("sample.elc"), "compiled").expect("write compiled file");

        assert_eq!(
            resolve_load_target("sample", std::slice::from_ref(&root), false)
                .expect("resolve source"),
            root.join("sample.el")
                .canonicalize()
                .expect("canonical source path")
        );
        assert_eq!(
            resolve_load_target("sample", std::slice::from_ref(&root), true)
                .expect("resolve compiled file"),
            root.join("sample.elc")
                .canonicalize()
                .expect("canonical compiled path")
        );

        fs::remove_dir_all(root).expect("remove load directory");
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
    fn batch_documentation_startup_installs_native_provenance_from_doc() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = env::temp_dir().join(format!("emaxx-batch-doc-{unique}"));
        fs::create_dir_all(&root).expect("create DOC directory");
        fs::write(
            root.join("DOC"),
            b"\x1fSbuffer.o\n\x1fSeditfns.o\n\x1fVdefault-directory\nDefault directory.\n",
        )
        .expect("write DOC fixture");

        let mut interpreter = Interpreter::new();
        interpreter.set_variable(
            "doc-directory",
            Value::String(lisp::primitives::path_to_directory_string(&root).into()),
            &mut Vec::new(),
        );
        initialize_batch_documentation(&mut interpreter).expect("snarf batch DOC fixture");

        assert_eq!(
            interpreter.lookup_var("build-files", &Vec::new()),
            Some(Value::list([
                Value::String("buffer.o".into()),
                Value::String("editfns.o".into()),
            ]))
        );
        assert!(matches!(
            interpreter.get_symbol_property("default-directory", "variable-documentation"),
            Some(Value::Integer(_))
        ));

        fs::remove_dir_all(root).expect("remove DOC fixture");
    }

    #[test]
    fn dumped_load_history_keeps_build_tree_provenance() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root = env::temp_dir().join(format!("emaxx-load-provenance-{unique}"));
        let runtime_root = root.join("runtime");
        let dump_root = root.join("build");
        let runtime_lisp = runtime_root.join("lisp/progmodes/xref.el");
        let runtime_test = runtime_root.join("test/xref-probe.el");
        fs::create_dir_all(runtime_lisp.parent().expect("Lisp parent"))
            .expect("create runtime Lisp directory");
        fs::create_dir_all(runtime_test.parent().expect("test parent"))
            .expect("create runtime test directory");
        fs::write(
            &runtime_lisp,
            "(defun xref-find-definitions ())\n(provide 'xref)\n",
        )
        .expect("write standard Lisp fixture");
        fs::write(&runtime_test, "(defun xref-probe-test ())\n").expect("write test Lisp fixture");

        let mut interpreter = Interpreter::new();
        interpreter
            .set_load_source_provenance_remap(runtime_root.join("lisp"), dump_root.join("lisp"));
        lisp::load_file_strict(&mut interpreter, &runtime_lisp)
            .expect("load standard Lisp fixture");
        lisp::load_file_strict(&mut interpreter, &runtime_test).expect("load test Lisp fixture");

        let located = Reader::new(&format!(
            "(locate-file \"xref\" (list {:?}) '(\".el\") nil)",
            runtime_lisp
                .parent()
                .expect("standard Lisp fixture parent")
                .display()
                .to_string()
        ))
        .read_all()
        .expect("read locate-file provenance probe")
        .remove(0);
        assert_eq!(
            interpreter
                .eval(&located, &mut Vec::new())
                .expect("locate standard Lisp fixture"),
            Value::String(
                dump_root
                    .join("lisp/progmodes/xref.el")
                    .display()
                    .to_string()
                    .into()
            )
        );

        let history = interpreter
            .lookup_var("load-history", &Vec::new())
            .expect("load history")
            .to_vec()
            .expect("load history list");
        assert_eq!(
            history[0].car().expect("isolated test filename"),
            Value::String(runtime_test.display().to_string().into())
        );
        assert_eq!(
            history[1].car().expect("remapped standard filename"),
            Value::String(
                dump_root
                    .join("lisp/progmodes/xref.el")
                    .display()
                    .to_string()
                    .into()
            )
        );

        fs::remove_dir_all(root).expect("remove load provenance fixture");
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
    fn batch_runtime_preloads_programming_mode_owners_in_parent_order() {
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
                   (defvar-local emaxx-test-mode-owner-reset nil)
                   (list
                    (featurep 'prog-mode)
                    (featurep 'lisp-mode)
                    (featurep 'elisp-mode)
                    (keymapp prog-mode-map)
                    (keymapp lisp-mode-shared-map)
                    (keymapp lisp-mode-map)
                    (keymapp emacs-lisp-mode-map)
                    (with-temp-buffer
                      (setq-local emaxx-test-mode-owner-reset 'stale)
                      (emacs-lisp-mode)
                      (list major-mode
                            emaxx-test-mode-owner-reset
                            (local-variable-p 'emaxx-test-mode-owner-reset)))))",
            )
            .read_all()
            .expect("read programming-mode startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate programming-mode startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::list([Value::symbol("emacs-lisp-mode"), Value::Nil, Value::Nil,]),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_gnu_paragraph_fill_and_comment_owners() {
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
                "(list
                   (with-temp-buffer
                     (insert \"first\\n\\nsecond\")
                     (use-hard-newlines 1 'always)
                     (list (fboundp 'use-hard-newlines)
                           (get 'use-hard-newlines 'permanent-local)
                           use-hard-newlines
                           (text-property-any
                            (point-min) (point-max) 'hard t)))
                   (with-temp-buffer
                     (emacs-lisp-mode)
                     (insert \";; long\\nx\")
                     (goto-char (point-min))
                     (list (featurep 'newcomment)
                           (comment-forward 1)
                           (point)
                           (char-after)))
                   (with-temp-buffer
                     (setq fill-column 10)
                     (insert \"alpha beta gamma\")
                     (fill-region-as-paragraph (point-min) (point-max))
                     (list
                      (string-suffix-p
                       \"/textmodes/fill.el\"
                       (symbol-file 'fill-region-as-paragraph 'defun))
                      (buffer-string)))
                   (progn
                     (require 'texinfo)
                     (with-temp-buffer
                       (insert
                        \"@defun face-remap-add-relative face &rest specs\n\"
                        \"This function adds the face spec in @var{specs} as relative\n\"
                        \"remappings for face @var{face} in the current buffer.  The remaining\n\"
                        \"arguments, @var{specs}, should form either a list of face names, or a\n\"
                        \"property list of attribute/value pairs.\n\")
                       (goto-char 49)
                       (texinfo-mode)
                       (fill-paragraph)
                       (buffer-string))))",
            )
            .read_all()
            .expect("read paragraphs startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate paragraphs startup probe"),
                Value::list([
                    Value::list([Value::T, Value::T, Value::T, Value::Integer(6)]),
                    Value::list([Value::T, Value::T, Value::Integer(9), Value::Integer(120),]),
                    Value::list([Value::T, Value::String("alpha beta\ngamma".into()),]),
                    Value::String(
                        "@defun face-remap-add-relative face &rest specs\n\
                         This function adds the face spec in @var{specs} as relative remappings\n\
                         for face @var{face} in the current buffer.  The remaining arguments,\n\
                         @var{specs}, should form either a list of face names, or a property\n\
                         list of attribute/value pairs.\n"
                            .into(),
                    ),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_complete_face_and_font_lock_owners() {
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
                "(list (featurep 'cus-face)
                       (featurep 'custom)
                       (not (subr-primitive-p
                             (symbol-function 'custom-check-theme)))
                       (featurep 'faces)
                       (not (subr-primitive-p
                             (symbol-function 'face-background)))
                       (not (subr-primitive-p
                             (symbol-function 'face-set-after-frame-default)))
                       (equal (face-background 'default nil 'default)
                              \"unspecified-bg\")
                       (featurep 'term/tty-colors)
                       (not (subr-primitive-p
                             (symbol-function 'tty-color-translate)))
                       (featurep 'font-core)
                       (boundp 'global-font-lock-mode)
                       (null global-font-lock-mode)
                       (not (subr-primitive-p
                             (symbol-function 'font-lock-mode)))
                       (featurep 'syntax)
                       (not (subr-primitive-p
                             (symbol-function 'syntax-propertize)))
                       (featurep 'font-lock)
                       (not (subr-primitive-p
                             (symbol-function 'font-lock-ensure)))
                       (not (subr-primitive-p
                             (symbol-function 'font-lock-fontify-region)))
                       (featurep 'jit-lock)
                       (not (null (facep 'font-lock-builtin-face)))
                       (face-attr-construct 'font-lock-builtin-face)
                       (fboundp 'copy-to-buffer)
                       (fboundp 'jit-lock-register))",
            )
            .read_all()
            .expect("read Font Lock startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate Font Lock startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::list([
                        Value::Symbol(":weight".into()),
                        Value::Symbol("bold".into())
                    ]),
                    Value::T,
                    Value::T,
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_gnus_complete_character_category_owner() {
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
                r#"(list
                     (category-docstring ?l)
                     (aref (char-category-set ?h) ?a)
                     (aref (char-category-set ?h) ?l)
                     (string-match-p "\\cl" "h")
                     (with-temp-buffer
                       (insert "h")
                       (goto-char (point-min))
                       (looking-at "\\cl")))"#,
            )
            .read_all()
            .expect("read character category startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate character category startup probe"),
                Value::list([
                    Value::String("Latin".into()),
                    Value::T,
                    Value::T,
                    Value::Integer(0),
                    Value::T,
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_lets_the_compile_owner_initialize_its_patterns() {
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
                   (require 'compile)
                   (list (fboundp 'compilation-parse-errors)
                         (> (length compilation-error-regexp-alist-alist) 50)
                         (fboundp 'command-line-normalize-file-name)))",
            )
            .read_all()
            .expect("read Compile startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate Compile startup probe"),
                Value::list([Value::T, Value::T, Value::T])
            );
        });
    }

    #[test]
    fn batch_runtime_uses_the_complete_files_auto_mode_registry() {
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
                r#"(list
                     (featurep 'files)
                     (> (length auto-mode-alist) 100)
                     (eq (cdr (assoc "\\.css\\'" auto-mode-alist))
                         'css-mode)
                     (with-temp-buffer
                       (setq buffer-file-name "/tmp/emaxx-auto-mode.css")
                       (normal-mode)
                       (list (eq major-mode 'css-mode)
                             (eq indent-line-function 'smie-indent-line))))"#,
            )
            .read_all()
            .expect("read files.el startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate files.el startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::list([Value::T, Value::T]),
                ])
            );
        });
    }

    #[test]
    fn batch_font_lock_honors_a_modes_custom_region_function() {
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
                   (require 'cperl-mode)
                   (with-temp-buffer
                     (insert \"printf qq\n{quoted}\nsub sample { return \\\"x\\\"; }\nmy $string = <<HERE;\nbody\nHERE\n\")
                     (cperl-mode)
                     (goto-char (point-min))
                     (search-forward \"{\")
                     (let ((lazy-string-state (nth 3 (syntax-ppss)))
                           (original-point (point)))
                       (font-lock-ensure)
                       (unless (= (point) original-point)
                         (error \"font-lock-ensure moved point\"))
                       (goto-char (point-min))
                       (list
                        lazy-string-state
                        (progn (search-forward \"sub\")
                               (get-text-property (match-beginning 0) 'face))
                        (progn (search-forward \"sample\")
                               (get-text-property (match-beginning 0) 'face))
                        (progn (search-forward \"x\")
                               (get-text-property (match-beginning 0) 'face))
                        (progn (search-forward \"body\")
                               (get-text-property (match-beginning 0) 'face))))))",
            )
            .read_all()
            .expect("read custom Font Lock probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate custom Font Lock probe"),
                Value::list([
                    Value::T,
                    Value::Symbol("font-lock-keyword-face".into()),
                    Value::Symbol("font-lock-function-name-face".into()),
                    Value::Symbol("font-lock-string-face".into()),
                    Value::Symbol("font-lock-string-face".into()),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_gnu_simple_and_event_position_helpers() {
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
                "(list (featurep 'simple)
                       (equal (get 'cconv--interactive-helper
                                   'emaxx-oclosure-slots)
                              '(fun if))
                       (fboundp 'next-completion)
                       (fboundp 'choose-completion)
                       (fboundp 'easy-menu-create-menu)
                       (equal (mapcar #'plistp
                                      '(nil (:a 1) (:a 1 :b 2) (:a) a))
                              '(t t t nil nil))
                       (fboundp 'event-start)
                       (fboundp 'posn-point)
                       (featurep 'replace)
                       (mapcar #'fboundp
                               '(occur perform-replace replace-regexp
                                 query-replace--split-string))
                       (catch 'state
                         (minibuffer-with-setup-hook
                             (lambda ()
                               (throw 'state
                                 (list (minibuffer-depth)
                                       (minibuffer-prompt)
                                       (point)
                                       (minibuffer-contents)
                                       (windowp (active-minibuffer-window))
                                       (eq (current-local-map)
                                           minibuffer-local-completion-map)
                                       (equal minibuffer-completion-table
                                              '(\"a\")))))
                           (let ((executing-kbd-macro t))
                             (completing-read \"Prompt: \" '(\"a\")))))
                       (minibuffer-depth)
                       (active-minibuffer-window))",
            )
            .read_all()
            .expect("read simple startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate simple startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::list([Value::T, Value::T, Value::T, Value::T]),
                    Value::list([
                        Value::Integer(1),
                        Value::String("Prompt: ".into()),
                        Value::Integer(9),
                        Value::String(String::new().into()),
                        Value::T,
                        Value::T,
                        Value::T,
                    ]),
                    Value::Integer(0),
                    Value::Nil,
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
    fn batch_runtime_preloads_the_complete_tab_bar_owner() {
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
                "(list (featurep 'tab-bar)\
                       (fboundp 'tab-bar-tabs-set)\
                       (fboundp 'tab-bar-close-other-tabs)\
                       (fboundp 'tab-new)\
                       (fboundp 'tab-rename)\
                       (fboundp 'tab-undo)\
                       (boundp 'tab-bar-mode)\
                       tab-bar-mode\
                       (boundp 'tab-bar-closed-tabs))",
            )
            .read_all()
            .expect("read tab-bar startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate tab-bar startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::Nil,
                    Value::T,
                ])
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
    fn batch_runtime_preloads_the_complete_subr_owner() {
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
                "(list (fboundp 'buffer-local-boundp)\
                       (fboundp 'global-key-binding)\
                       (fboundp 'list-of-strings-p)\
                       (fboundp 'merge-ordered-lists)\
                       (eq (global-key-binding \"x\") 'self-insert-command)\
                       (eq (xor nil 'truthy) 'truthy))",
            )
            .read_all()
            .expect("read complete subr startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate complete subr startup probe"),
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
            crate::test_support::mark_process_test();
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
                Value::String(
                    lisp::primitives::path_to_directory_string(&emacs_repo.join("etc")).into(),
                ),
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
    fn batch_runtime_installs_generated_autoload_symbol_properties() {
        run_with_large_stack(|| {
            let options = BatchRunOptions::default();
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("initialize batch interpreter");
            let form = Reader::new(
                "(list (get 'http://www.w3.org/2001/XMLSchema-datatypes
                            'rng-dt-compile)
                       (autoloadp (symbol-function 'rng-xsd-compile)))",
            )
            .read_all()
            .expect("read dumped symbol-property probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("inspect dumped symbol properties"),
                Value::list([Value::symbol("rng-xsd-compile"), Value::T])
            );
        });
    }

    #[test]
    fn batch_runtime_installs_generated_builtin_package_versions() {
        run_with_large_stack(|| {
            let options = BatchRunOptions::default();
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("initialize batch interpreter");
            let form = Reader::new(
                "(list (length package--builtin-versions)
                       (cdr (assq 'allout package--builtin-versions))
                       (cdr (assq 'org package--builtin-versions))
                       (cdr (assq 'xref package--builtin-versions))
                       (cdr (assq 'compat package--builtin-versions)))",
            )
            .read_all()
            .expect("read dumped package-version probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("inspect dumped package versions"),
                Value::list([
                    Value::Integer(78),
                    Value::list([Value::Integer(2), Value::Integer(3)]),
                    Value::list([Value::Integer(9), Value::Integer(7), Value::Integer(11),]),
                    Value::list([Value::Integer(1), Value::Integer(7), Value::Integer(0)]),
                    Value::list([Value::Integer(30), Value::Integer(2), Value::Integer(9999),]),
                ])
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
            let ert = resolve_load_target("ert", &options.load_path, false).expect("resolve ert");
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
                let resolved = resolve_load_target(target, &options.load_path, false)
                    .unwrap_or_else(|error| panic!("resolve {target}: {error}"));
                lisp::load_file_strict(&mut interpreter, &resolved).unwrap_or_else(|error| {
                    panic!("load {target} ({}): {error}", resolved.display())
                });
            }
        });
    }
}
