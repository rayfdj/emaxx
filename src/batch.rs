use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::compat::{self, BatchReport, FileStatus, TestStatus};
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
    /// Leave `custom-delayed-init-variables' queued instead of replaying
    /// at the end of the dumped-image reconstruction.  GNU's dump keeps
    /// the queue for startup.el to replay in the real session mode; the
    /// interactive frontend replays it once, under `noninteractive' nil.
    pub defer_delayed_custom_init: bool,
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
    let eval_expressions = actions
        .iter()
        .filter_map(|action| match action {
            BatchAction::Eval(expression) => Some(expression.clone()),
            BatchAction::Load(_) | BatchAction::Funcall(_) => None,
        })
        .collect::<Vec<_>>();
    let perf_request = parse_perf_request(&eval_expressions)?;
    // The compat helper's load-error reports record the harness-provided
    // selector; mirror GNU's `emaxx-compat--selector' environment contract.
    let selector_string =
        env::var("EMAXX_COMPAT_SELECTOR").unwrap_or_else(|_| "(quote t)".to_string());
    let mut eval_env: Env = Vec::new();
    for action in &actions {
        match action {
            BatchAction::Load(target) => {
                let resolved = resolve_load_target(
                    target,
                    &options.load_path,
                    interpreter.prefers_compiled_loads(),
                )?;
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
                    // GNU prints an uncaught load error to stderr and exits
                    // 255; Emaxx used to write only the structured report, so
                    // `emaxx --batch -l broken.el' failed with no diagnostic
                    // at all.  The report is for the harness; the message is
                    // for whoever ran the command.
                    emit_unhandled_batch_error(
                        &mut interpreter,
                        &error,
                        &command_line_bottom_frames(&actions, Some(&resolved)),
                    );
                    return Ok(BatchRunOutcome::Exit(255));
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
                    if extract_perf_request_from_form(&form).is_some() {
                        continue;
                    }
                    // GNU's reader interns as it reads, so `--eval' leaves
                    // every symbol in the form it evaluates in the obarray.
                    // (Not every symbol in the STRING: startup.el:2669 reads
                    // one form and silently discards any trailing ones, which
                    // Emaxx's read_all loop does not -- a separate, older
                    // divergence.)  Walking per form rather than all forms up
                    // front is the faithful choice: an early form asking about
                    // a symbol only a later form mentions answers nil in GNU
                    // too.  The file
                    // loader and `eval-region' replicate that with this walk
                    // (lisp/mod.rs:947, loading.rs:401); `--eval' did not, so
                    // `emacs --batch --eval "(progn 'foo (intern-soft \"foo\"))"'
                    // answered nil where GNU answers foo -- for ordinary
                    // symbols as well as keywords.
                    interpreter.intern_symbols_in_value(&form);
                    match interpreter.eval(&form, &mut eval_env) {
                        Ok(_) => {}
                        Err(LispError::Terminate(termination)) => return Ok(termination.into()),
                        Err(error) => {
                            emit_unhandled_batch_error(
                                &mut interpreter,
                                &error,
                                &command_line_bottom_frames(&actions, None),
                            );
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
                        emit_unhandled_batch_error(
                            &mut interpreter,
                            &error,
                            &command_line_bottom_frames(&actions, None),
                        );
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

    // The measured ERT path is Lisp-driven: real ert.el (or the shared
    // compat reporter) runs the tests, writes any structured artifacts, and
    // exits through `kill-emacs'.  Reaching this point means every action
    // evaluated without a termination request.
    Ok(BatchRunOutcome::Exit(0))
}

/// GNU's batch backtrace bottoms out through the startup frames that ran
/// the failing action: an optional `load' frame for `-l', then
/// command-line-1 with the action arguments, command-line, and
/// normal-top-level.  The argument list is reconstructed with canonical
/// flag spellings, which is what the compat harness passes.
fn command_line_bottom_frames(
    actions: &[BatchAction],
    load_target: Option<&Path>,
) -> Vec<(String, Vec<Value>)> {
    let mut frames = Vec::new();
    if let Some(path) = load_target {
        frames.push((
            "load".to_string(),
            vec![
                Value::String(path.display().to_string().into()),
                Value::Nil,
                Value::T,
            ],
        ));
    }
    let mut cli_args = Vec::new();
    for action in actions {
        match action {
            BatchAction::Load(target) => {
                cli_args.push(Value::String("-l".into()));
                cli_args.push(Value::String(target.clone().into()));
            }
            BatchAction::Eval(expression) => {
                cli_args.push(Value::String("--eval".into()));
                cli_args.push(Value::String(expression.clone().into()));
            }
            BatchAction::Funcall(function) => {
                cli_args.push(Value::String("-f".into()));
                cli_args.push(Value::String(function.clone().into()));
            }
        }
    }
    frames.push(("command-line-1".to_string(), vec![Value::list(cli_args)]));
    frames.push(("command-line".to_string(), Vec::new()));
    frames
}

fn emit_unhandled_batch_error(
    interpreter: &mut Interpreter,
    error: &LispError,
    bottom_frames: &[(String, Vec<Value>)],
) {
    // print_error_message: the same rendering `error-message-string'
    // performs (error-message property through `substitute-command-keys',
    // condition-specific data quoting), falling back to the native text
    // if that primitive cannot run this early.
    let message = lisp::primitives::call(
        interpreter,
        "error-message-string",
        &[lisp::eval::error_condition_value(error)],
        &mut Vec::new(),
    )
    .ok()
    .and_then(|value| lisp::primitives::string_like(&value).map(|text| text.text))
    .unwrap_or_else(|| error.to_string());
    eprintln!("{message}");
    let Some(backtrace) = interpreter.take_batch_error_backtrace() else {
        return;
    };
    if !backtrace.enabled {
        return;
    }
    // debug-early.el's debug--early: "\nError: " + prin1 of the error
    // symbol and data under default print settings, then the frames under
    // debug-early-backtrace's binds (print-escape-newlines,
    // print-escape-control-characters, print-escape-nonascii all t).
    let mut render_env: Env = Vec::new();
    let prin1 = |interpreter: &mut Interpreter, env: &mut Env, value: &Value| {
        lisp::primitives::print::render_prin1(interpreter, value, env)
            .unwrap_or_else(|_| value.to_string())
    };
    let condition = lisp::eval::error_condition_value(error);
    let rendered_condition = match condition.to_vec() {
        Ok(items) if !items.is_empty() => {
            let kind = prin1(interpreter, &mut render_env, &items[0]);
            let data = Value::list(items.into_iter().skip(1));
            let data = prin1(interpreter, &mut render_env, &data);
            format!("{kind} {data}")
        }
        _ => prin1(interpreter, &mut render_env, &condition),
    };
    eprintln!("\nError: {rendered_condition}");
    for name in [
        "print-escape-newlines",
        "print-escape-control-characters",
        "print-escape-nonascii",
    ] {
        lisp::primitives::print::set_env_binding(&mut render_env, name, Value::T);
    }
    for (evald, function, args, _) in backtrace.frames {
        let function = prin1(interpreter, &mut render_env, &function);
        let args = args
            .iter()
            .map(|value| prin1(interpreter, &mut render_env, value))
            .collect::<Vec<_>>()
            .join(" ");
        if evald {
            eprintln!("  {function}({args})");
        } else {
            let separator = if args.is_empty() { "" } else { " " };
            eprintln!("  ({function}{separator}{args})");
        }
    }
    for (function, args) in bottom_frames {
        let args = args
            .iter()
            .map(|value| prin1(interpreter, &mut render_env, value))
            .collect::<Vec<_>>()
            .join(" ");
        eprintln!("  {function}({args})");
    }
    eprintln!("  normal-top-level()");
}

/// Bootstrap the full Lisp runtime for an interactive terminal session.
///
/// The dumped-image reconstruction is identical to batch startup; only the
/// session mode differs, and the caller flips `noninteractive' once the
/// terminal owns the frame.
pub fn initialize_interactive_interpreter() -> Result<Interpreter, String> {
    let mut options = BatchRunOptions::default();
    // GNU's init_lread honors EMACSLOADPATH verbatim in every session
    // mode; interactive commands autoload their dumped Lisp owners
    // (`save-buffer' pulls in files.el) through this path.
    if let Ok(paths) = env::var("EMACSLOADPATH") {
        options.load_path = env::split_paths(&paths).collect();
    }
    // GNU's dump leaves `custom-delayed-init-variables' queued for
    // startup.el to replay in the live session; deferring here keeps the
    // queue open across the whole reconstruction and the preloads below.
    options.defer_delayed_custom_init = true;
    // GNU's interactive session runs from a dumped image built out of
    // compiled Lisp; the live frontend therefore resolves the same GNU
    // owners through their `.elc' representation unconditionally, while
    // batch keeps its configured source/compiled test gating.
    let mut interpreter = initialize_batch_interpreter_with_load_preference(&options, true)?;
    // GNU dumps isearch.el, minibuffer.el, and rfn-eshadow.el into the
    // image; an interactive session must have C-s, the minibuffer's own
    // command set (exit-minibuffer, minibuffer-complete), and file-name
    // shadowing ready before the first keystroke.  A load-path without
    // the real Lisp tree (unit tests) simply leaves them unbound,
    // exactly like the batch runtime.  These preloads run with the
    // delayed-Custom queue still open: a global minor mode's
    // `custom-initialize-delay' defcustom runs before its mode function
    // exists mid-load, so it must queue and replay through startup.el's
    // custom-reevaluate-setting walk — file-name-shadow-mode turns on
    // through that replay, as in GNU's dumped image.
    // cus-start comes after loaddefs, as loadup.el orders it ("Late to
    // reduce customize-rogue (needs loaddefs.el anyway)"): its unbound
    // built-in notes land at dump time in GNU, here at init.
    for target in [
        "isearch",
        "minibuffer",
        "rfn-eshadow",
        "loaddefs",
        "cus-start",
    ] {
        if interpreter.resolve_load_target(target).is_some() {
            interpreter
                .load_target(target)
                .map_err(|error| format!("preload {target}: {error}"))?;
        }
    }
    // The dump ran under `noninteractive' t, but startup.el's single
    // custom-reevaluate-setting walk runs in the live session — a
    // delayed :init-value that consults `noninteractive' (frame.el's
    // blink-cursor-mode) sees nil there, as it does in GNU.  startup.el
    // defvars the -D/--basic-display flags before the walk; delayed
    // forms read them once `noninteractive' stops short-circuiting.
    interpreter.set_variable("noninteractive", Value::Nil, &mut Vec::new());
    interpreter.set_variable("emacs-basic-display", Value::Nil, &mut Vec::new());
    interpreter.set_variable("no-blinking-cursor", Value::Nil, &mut Vec::new());
    // GNU's normal-top-level recomputes `user-emacs-directory' from the
    // XDG-or-homedot policy before delayed Custom forms run
    // (startup.el's setq); the replay below then evaluates delayed
    // standard values such as `auto-save-list-file-prefix' against it.
    // The batch reconstruction already ran this once, but the deferred
    // delayed-Custom queue is only replayed here, under the recomputed
    // directory — the same ordering `command-line' guarantees.
    initialize_batch_user_emacs_directory(&mut interpreter)?;
    complete_delayed_custom_initialization(&mut interpreter)?;
    // startup.el enables transient-mark-mode for interactive sessions
    // (batch keeps the dumped nil default): the region highlights.
    interpreter.set_variable("transient-mark-mode", Value::T, &mut Vec::new());
    // menu-bar-mode's C default is t; startup.el turns it off only when
    // noninteractive.  An interactive session keeps the menu bar (the
    // `menu-bar-lines' frame parameter already defaults to 1).
    interpreter.set_variable("menu-bar-mode", Value::T, &mut Vec::new());
    Ok(interpreter)
}

pub(crate) fn initialize_batch_interpreter(
    options: &BatchRunOptions,
) -> Result<Interpreter, String> {
    initialize_batch_interpreter_with_load_preference(options, lisp::bytecode_vm_enabled())
}

/// Reconstruct the GNU-owned batch Lisp image with an explicit source/bytecode
/// resolver preference.  Normal runtime startup follows its configured mode;
/// ownership-sensitive tests use compiled GNU Lisp to mirror the dumped GNU
/// runtime without mutating process-wide environment variables.
pub(crate) fn initialize_batch_interpreter_with_load_preference(
    options: &BatchRunOptions,
    prefer_compiled_loads: bool,
) -> Result<Interpreter, String> {
    #[cfg(test)]
    let _source_bootstrap_permit =
        (!prefer_compiled_loads).then(crate::test_support::acquire_batch_source_bootstrap_permit);
    // Boot resolves its Lisp tree partly from process environment
    // (EMAXX_DUMP_SOURCE_DIRECTORY, EMACS_TEST_DIRECTORY).  Tests that
    // point those at fixture roots take this lock for WRITE; every boot
    // holds it for READ so a concurrent mutation cannot send this boot to
    // the fixture tree.  The "--test-threads=1" SAFETY comments this
    // replaces were wrong: the gate runs the suite in parallel, and a
    // sibling test's fixture env made random preloads fail with
    // "Cannot open load file".
    let _boot_environment = compat::boot_environment_read_guard();
    let mut interpreter = Interpreter::new();
    let before_init_time =
        lisp::primitives::system_time_list_value(std::time::SystemTime::now())
            .map_err(|error| format!("record batch initialization start: {error}"))?;
    interpreter.define_special_variable("before-init-time", before_init_time);
    interpreter.define_special_variable("after-init-time", Value::Nil);
    interpreter.set_load_path(installation_lisp_load_path()?);
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
    // The VM gate controls resolution for the reconstructed preload itself.
    // Setting it after `preload_batch_compat_libraries' made the flag
    // ineffective for precisely the GNU Elisp image it is meant to execute.
    // Both `.el' and `.elc' remain the same GNU Lisp owner; this selects its
    // compiled representation before any loadup library is resolved.
    interpreter.set_prefer_compiled_loads(prefer_compiled_loads);
    // The reconstruction below is GNU's pre-dump build phase.  Its Loading
    // chatter and cus-start's "Note, built-in variable" messages belong to
    // the build log, never to a running session's stderr OR its *Messages*
    // buffer — a dumped GNU binary starts with an empty one (loaddefs.el's
    // own `(load "theme-loaddefs.el" t)' was leaking a Loading line there).
    let saved_message_log_max = interpreter
        .lookup_var("message-log-max", &Vec::new())
        .unwrap_or(Value::Nil);
    interpreter.set_variable("message-log-max", Value::Nil, &mut Vec::new());
    interpreter.set_variable("inhibit-message", Value::T, &mut Vec::new());
    let reconstruction = (|interpreter: &mut Interpreter| -> Result<(), String> {
        preload_batch_compat_libraries(interpreter)?;
        // The dump boundary.  charset.c's Vcharset_non_preferred_head is
        // not staticpro'd, so the value loadup left (english.el's
        // `set-language-info-alist' re-runs `set-language-environment'
        // for the default "English") does not survive into the dumped
        // image: a fresh GNU session starts with it nil, and only a
        // `set-charset-priority' of the session (a locale that selects a
        // language environment) sets it again.
        interpreter.forget_charset_non_preferred_head();
        initialize_batch_initial_frame_faces(interpreter)?;
        // startup.el's `command-line' computes `user-emacs-directory' (line
        // 597) well before it re-evaluates the custom settings that depend on
        // it (`custom-reevaluate-setting', line 1327).  Running these in the
        // other order left `package-user-dir' and `package-quickstart-file'
        // computed against a nil directory, so they no longer equalled
        // `(locate-user-emacs-file ...)' as they do in GNU.
        initialize_batch_user_emacs_directory(interpreter)?;
        initialize_batch_lisp_directory(interpreter)?;
        if !options.defer_delayed_custom_init {
            complete_delayed_custom_initialization(interpreter)?;
        }
        initialize_batch_scratch_major_mode(interpreter)?;
        initialize_batch_bar_modes(interpreter)?;
        initialize_batch_tty_default_colors(interpreter)?;
        initialize_batch_locale_environment(interpreter)
    })(&mut interpreter);
    // Restore before propagating: a failed reconstruction must not leave the
    // session muted, or its own diagnostics would be swallowed too.
    interpreter.set_variable("inhibit-message", Value::Nil, &mut Vec::new());
    interpreter.set_variable("message-log-max", saved_message_log_max, &mut Vec::new());
    reconstruction?;
    // The image is built; now the session's own `load-path' applies, with the
    // user's `-L' entries ahead of the installation directories.
    interpreter.set_load_path(effective_batch_load_path(options)?);
    let after_init_time = lisp::primitives::system_time_list_value(std::time::SystemTime::now())
        .map_err(|error| format!("record batch initialization end: {error}"))?;
    interpreter.set_global_binding("after-init-time", after_init_time);
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
    // GNU loadup.el calls the native Snarf-documentation primitive here and
    // ignores a missing DOC file in non-dump builds.  Keep the actual loadup
    // form rather than depending on unrelated convenience macros.
    let form = Reader::new(
        "(condition-case nil
             (Snarf-documentation \"DOC\")
           (error nil))",
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
    if !has_configured_lisp_tree(interpreter) {
        return Ok(());
    }
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

fn initialize_batch_bar_modes(interpreter: &mut Interpreter) -> Result<(), String> {
    // startup.el:1453 in `command-line': a noninteractive (or basic-display)
    // session clears the bar modes before frame initialization.  The C
    // defaults are t; batch GNU observably ends nil for all three.
    let source = "(if noninteractive
                     (setq menu-bar-mode nil
                           tab-bar-mode nil
                           tool-bar-mode nil))";
    let form = Reader::new(source)
        .read_all()
        .map_err(|error| format!("read startup bar-mode form: {error}"))?
        .remove(0);
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("initialize batch bar modes: {error}"))?;
    Ok(())
}

fn initialize_batch_tty_default_colors(interpreter: &mut Interpreter) -> Result<(), String> {
    // startup.el:1479 in `command-line', run for batch sessions too: the
    // default TTY color set is registered regardless of whether the terminal
    // supports color, "since users can connect to color-capable terminals".
    // Without it `tty-color-alist' stays empty, so `color-values' answers nil
    // for every named color and color.el's arithmetic on that nil signals
    // (wrong-type-argument number-or-marker-p nil) -- 21 mismatches across
    // color-tests, css-mode-tests and erc-nicks-tests in the 7595 baseline.
    // The whole color database is Lisp-owned (`color-name-rgb-alist' is a
    // defconst in term/tty-colors.el), so this runs GNU's own registration
    // function rather than transcribing any table.
    //
    // `initialize_batch_interpreter' is also the embedding boundary used by
    // small tests with an intentionally empty load path; that runtime has no
    // term/tty-colors.el owner to invoke, so stay quiet there exactly as the
    // locale and frame-face steps do.
    if interpreter
        .lookup_function("tty-register-default-colors", &Vec::new())
        .is_err()
    {
        return if has_configured_lisp_tree(interpreter) {
            Err("GNU term/tty-colors.el did not define tty-register-default-colors".into())
        } else {
            Ok(())
        };
    }
    let source = "(or (eq initial-window-system 'pc)
                     (tty-register-default-colors))";
    let form = Reader::new(source)
        .read_all()
        .map_err(|error| format!("read startup tty-color form: {error}"))?
        .remove(0);
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("register batch tty default colors: {error}"))?;
    Ok(())
}

fn initialize_batch_scratch_major_mode(interpreter: &mut Interpreter) -> Result<(), String> {
    // startup.el:1572 in `command-line', run for batch sessions too: the
    // initial `*scratch*' buffer leaves `fundamental-mode' for
    // `initial-major-mode' before any user code sees it.
    let source = "(if (get-buffer \"*scratch*\")
                     (with-current-buffer \"*scratch*\"
                       (if (eq major-mode 'fundamental-mode)
                           (funcall initial-major-mode))))";
    let form = Reader::new(source)
        .read_all()
        .map_err(|error| format!("read startup *scratch* mode form: {error}"))?
        .remove(0);
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("initialize *scratch* major mode: {error}"))?;
    Ok(())
}

fn initialize_batch_initial_frame_faces(interpreter: &mut Interpreter) -> Result<(), String> {
    // GNU's noninteractive temacs builds its dump with a live initial frame,
    // but does not initialize that frame's display-dependent face parameters.
    // A process restored from the portable dump recreates the frame and
    // dispnew.c invokes this exact GNU Elisp owner from init_display.  Emaxx
    // reconstructs both phases in one process, so perform the same call after
    // loadup rather than fabricating `background-mode', `display-type', or
    // face objects in the Rust host.
    if interpreter
        .lookup_function("tty-set-up-initial-frame-faces", &Vec::new())
        .is_err()
    {
        return if has_configured_lisp_tree(interpreter) {
            Err("GNU faces.el did not define tty-set-up-initial-frame-faces".into())
        } else {
            Ok(())
        };
    }
    let form = Reader::new("(tty-set-up-initial-frame-faces)")
        .read_all()
        .map_err(|error| format!("read initial-frame face startup form: {error}"))?
        .remove(0);
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("initialize initial-frame faces: {error}"))?;
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
        return if has_configured_lisp_tree(interpreter) {
            Err("GNU mule-cmds.el did not define set-locale-environment".into())
        } else {
            Ok(())
        };
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

/// startup.el's `command-line' computes `user-emacs-directory' before any
/// user Lisp runs; subr.el's `defvar' deliberately leaves it nil ("The value
/// does not matter since Emacs sets this at startup").  Batch sessions observe
/// the computed value, so run GNU's own two startup forms rather than
/// inventing a path here.
fn initialize_batch_user_emacs_directory(interpreter: &mut Interpreter) -> Result<(), String> {
    if interpreter
        .lookup_function("startup--xdg-or-homedot", &Vec::new())
        .is_err()
    {
        return if has_configured_lisp_tree(interpreter) {
            Err("GNU startup.el did not define startup--xdg-or-homedot".into())
        } else {
            Ok(())
        };
    }
    for source in [
        "(setq startup--xdg-config-home-emacs
           (let ((xdg-config-home (getenv-internal \"XDG_CONFIG_HOME\")))
             (if xdg-config-home
                 (concat xdg-config-home \"/emacs/\")
               startup--xdg-config-default)))",
        "(setq user-emacs-directory
           (startup--xdg-or-homedot startup--xdg-config-home-emacs nil))",
    ] {
        let form = Reader::new(source)
            .read_all()
            .map_err(|error| format!("read startup user-emacs-directory form: {error}"))?
            .remove(0);
        interpreter
            .eval(&form, &mut Vec::new())
            .map_err(|error| format!("initialize user-emacs-directory: {error}"))?;
    }
    Ok(())
}

/// startup.el:1186 in `command-line': locate `simple' on `load-path' and
/// record its true directory as `lisp-directory'.  doc.c's get_doc_string
/// resolves compiled functions' (FILE . POS) docstring references against
/// this variable; while it stays nil every `.elc' doc reference answers nil
/// and `help-function-arglist' degrades to arity-derived argument names.
fn initialize_batch_lisp_directory(interpreter: &mut Interpreter) -> Result<(), String> {
    if interpreter
        .lookup_function("locate-file", &Vec::new())
        .is_err()
    {
        return if has_configured_lisp_tree(interpreter) {
            Err("GNU files.el did not define locate-file".into())
        } else {
            Ok(())
        };
    }
    let form = Reader::new(
        "(let ((simple-file-name
                (locate-file \"simple\" load-path (get-load-suffixes))))
           (when simple-file-name
             (setq lisp-directory
                   (file-truename (file-name-directory simple-file-name)))))",
    )
    .read_all()
    .map_err(|error| format!("read startup lisp-directory form: {error}"))?
    .remove(0);
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("initialize lisp-directory: {error}"))?;
    Ok(())
}

/// The Lisp tree the dumped image is reconstructed FROM.
///
/// GNU builds its dump from the installation's own `lisp/' directory; a user's
/// `-L' cannot influence what got dumped, so a directory shadowing `button.el'
/// is harmless there.  Emaxx rebuilds that image at startup, so it must
/// likewise reconstruct from the installation tree alone and only afterwards
/// honour `-L' for the session.
fn installation_lisp_load_path() -> Result<Vec<PathBuf>, String> {
    // The image reconstructs from the tree that built the running dump.  The
    // compat harness names that tree explicitly: the pinned oracle repo,
    // whose lisp/**/*.elc are the very bytes the oracle executes.  An
    // `EMACS_TEST_DIRECTORY' checkout is deliberately not consulted here --
    // it is the *test* tree, and as a fresh git checkout it has no compiled
    // Lisp at all, so reconstructing from it would execute different bytes
    // than the oracle dumped (source `.el' where the oracle runs `.elc').
    if let Ok(dump_root) = env::var(compat::DUMP_SOURCE_DIRECTORY_ENV) {
        return compat::emaxx_upstream_load_path(Path::new(&dump_root));
    }
    let sibling = compat::project_root().join("../emacs");
    if sibling.join("lisp").is_dir() {
        return compat::emaxx_upstream_load_path(&sibling);
    }
    Ok(Vec::new())
}

fn effective_batch_load_path(options: &BatchRunOptions) -> Result<Vec<PathBuf>, String> {
    // GNU's `-L' PREPENDS to the dumped `load-path'; it never replaces it, so
    // `emacs -L dir --batch' still has its whole standard library.  Emaxx
    // reconstructs that library from the pinned sibling checkout, so the same
    // composition applies: explicit `-L' entries first, then the test tree
    // when EMACS_TEST_DIRECTORY names one, then the installation's own Lisp
    // directories — the fallback `data-directory' already uses for GNU's DOC
    // database.
    let mut load_path = options.load_path.clone();
    // The installation's own Lisp goes in BEFORE the test tree.  GNU's
    // load-path does not respond to EMACS_TEST_DIRECTORY at all -- it holds
    // the 25 dumped entries either way, and none of them is deeper than one
    // level under lisp/.  Emaxx discovers test-helper directories with a
    // RECURSIVE walk, which adds 35 deeper directories; with those ahead of
    // the standard library, CEDET subdirectories shadowed five core names:
    //
    //     chart comp debug generic map
    //
    // `(require 'map)' then loaded cedet/srecode/map.el, which provides
    // `srecode/map', and failed -- under the exact environment the
    // compatibility harness gives every child it measures (finding 123).
    // Ordering the standard library first keeps helper discovery working
    // while letting core names resolve the way GNU resolves them.
    let sibling = compat::project_root().join("../emacs");
    if sibling.join("lisp").is_dir() {
        for path in compat::emaxx_upstream_load_path(&sibling)? {
            if !load_path.contains(&path) {
                load_path.push(path);
            }
        }
    }
    if let Ok(test_directory) = env::var("EMACS_TEST_DIRECTORY") {
        let test_directory = PathBuf::from(test_directory);
        if let Some(repo_root) = test_directory.parent() {
            for path in compat::repo_local_elisp_load_path(repo_root)? {
                if !load_path.contains(&path) {
                    load_path.push(path);
                }
            }
        }
    }
    Ok(load_path)
}

fn loadup_eval(interpreter: &mut Interpreter, source: &str) -> Result<Value, String> {
    let form = Reader::new(source)
        .read_all()
        .map_err(|error| format!("read loadup form {source}: {error}"))?
        .into_iter()
        .next()
        .ok_or_else(|| format!("empty loadup form: {source}"))?;
    interpreter
        .eval(&form, &mut Vec::new())
        .map_err(|error| format!("evaluate loadup form {source}: {error}"))
}

fn loadup_predicate(interpreter: &mut Interpreter, source: &str) -> Result<bool, String> {
    loadup_eval(interpreter, source).map(|value| !matches!(value, Value::Nil))
}

fn loadup_required_library(interpreter: &mut Interpreter, library: &str) -> Result<(), String> {
    match interpreter.load_target(library) {
        Ok(_) => Ok(()),
        Err(error) => {
            let backtrace = interpreter
                .take_batch_error_backtrace()
                .map(|snapshot| format_backtrace_frames(snapshot.frames))
                .unwrap_or_default();
            if backtrace.is_empty() {
                Err(format!("preload {library}: {error}"))
            } else {
                Err(format!(
                    "preload {library}: {error} | backtrace: {backtrace}"
                ))
            }
        }
    }
}

fn loadup_required_sequence(
    interpreter: &mut Interpreter,
    libraries: &[&str],
) -> Result<(), String> {
    for library in libraries {
        loadup_required_library(interpreter, library)?;
    }
    Ok(())
}

const GNU_OPTIONAL_LOADUP_LIBRARIES: &[&str] = &[
    "international/charprop.el",
    "leim/leim-list.el",
    "site-load",
    "site-init",
];

fn loadup_optional_library(interpreter: &mut Interpreter, library: &str) -> Result<bool, String> {
    if !GNU_OPTIONAL_LOADUP_LIBRARIES.contains(&library) {
        return Err(format!("{library} is not an optional GNU loadup library"));
    }
    if interpreter.resolve_load_target(library).is_none() {
        return Ok(false);
    }
    loadup_required_library(interpreter, library)?;
    Ok(true)
}

fn has_configured_lisp_tree(interpreter: &Interpreter) -> bool {
    interpreter
        .lookup_var("load-path", &Vec::new())
        .and_then(|value| value.to_vec().ok())
        .is_some_and(|paths| !paths.is_empty())
}

fn preload_batch_compat_libraries(interpreter: &mut Interpreter) -> Result<(), String> {
    // Reconstruct the beginning of GNU loadup verbatim.  A bare interpreter
    // supplies only GNU C primitives; every portable definition below comes
    // from its owning GNU Elisp file.  There is no Emaxx compatibility layer
    // between those two boundaries.
    if !has_configured_lisp_tree(interpreter) {
        // A deliberately file-less embedded interpreter exposes the Rust
        // host only; it cannot claim GNU's dumped Elisp surface.  Once any
        // Lisp tree is configured, however, loadup is all-or-error below.
        return Ok(());
    }

    loadup_required_sequence(
        interpreter,
        &[
            "emacs-lisp/debug-early",
            "emacs-lisp/byte-run",
            "emacs-lisp/backquote",
        ],
    )?;

    // subr.el's own `defvar global-map' and `use-global-map' create and
    // install the initial map; the host contributes nothing to it.
    loadup_required_sequence(interpreter, &["subr", "keymap"])?;
    loadup_eval(
        interpreter,
        "(add-hook 'after-load-functions (lambda (_) (garbage-collect)))",
    )?;

    // Keep GNU loadup's early owner sequence contiguous.  Splitting this
    // sequence previously let Emaxx-only native macro fallbacks hide missing
    // dependencies and made the reconstructed image observably impossible.
    loadup_required_sequence(
        interpreter,
        &[
            "version",
            "widget",
            "custom",
            "emacs-lisp/map-ynp",
            "international/mule",
            "international/mule-conf",
            "env",
            "format",
            "bindings",
            "window",
        ],
    )?;

    // These are the two observable loadup.el transitions between window.el
    // and files.el.  Both cells are host primitives, while their values and
    // initialization phase are owned by loadup itself.
    interpreter.set_global_binding("resize-mini-windows", Value::symbol("grow-only"));
    interpreter.set_global_binding(
        "load-source-file-function",
        Value::symbol("load-with-code-conversion"),
    );

    loadup_required_library(interpreter, "files")?;

    // Emaxx reconstructs the dump from source, so follow GNU's interpreted
    // bootstrap arm: load macroexp, then pcase, then macroexp again so pcase
    // uses are expanded by their real Elisp owner.
    loadup_required_library(interpreter, "emacs-lisp/macroexp")?;
    if !loadup_predicate(
        interpreter,
        "(compiled-function-p (symbol-function 'macroexpand-all))",
    )? {
        loadup_eval(
            interpreter,
            "(let ((macroexp--pending-eager-loads '(skip)))
               (load \"emacs-lisp/pcase\"))",
        )?;
        loadup_eval(
            interpreter,
            "(let ((max-lisp-eval-depth (* 2 max-lisp-eval-depth)))
               (load \"emacs-lisp/macroexp\"))",
        )?;
    }

    loadup_required_sequence(interpreter, &["cus-face", "faces"])?;

    // GNU loads the generated loaddefs file here, before button and CL
    // preload.  Execute the actual Elisp owner: a Rust projection of selected
    // forms can silently omit valid top-level definitions (notably macros)
    // and is not an equivalent reconstruction of the dumped image.
    let loaddefs = if interpreter.resolve_load_target("loaddefs").is_some() {
        "loaddefs"
    } else {
        "ldefs-boot"
    };
    interpreter
        .load_target(loaddefs)
        .map_err(|error| format!("preload {loaddefs}: {error}"))?;

    loadup_required_library(interpreter, "button")?;
    if loadup_predicate(
        interpreter,
        "(interpreted-function-p (symbol-function 'add-hook))",
    )? {
        loadup_required_library(interpreter, "emacs-lisp/gv")?;
    }
    loadup_required_sequence(
        interpreter,
        &[
            "emacs-lisp/cl-preloaded",
            "emacs-lisp/oclosure",
            "obarray",
            "abbrev",
            "help",
        ],
    )?;
    // GNU dumps help.el into the initial image.  Loading its owning Lisp
    // library here preserves that startup contract: tests and packages may
    // call internal Help formatters without first requiring `help', and the
    // high-level keymap/quoting policy remains on the Elisp side.
    // GNU loadup loads jka-cmpr-hook.el immediately after help.el.  Info's
    // dumped implementation calls its public compression predicates without
    // requiring the feature, so loading only info.el leaves an impossible
    // startup state.  Keep the policy and handler tables in their owning
    // Lisp library rather than stubbing whichever predicate a caller reaches.
    loadup_required_sequence(interpreter, &["jka-cmpr-hook", "epa-hook"])?;

    // mule-cmds.el is loaded (and dumped) immediately after the Help and
    // compression hooks in GNU loadup.  It intentionally has no `provide'
    // form, so callers use its commands and C-x RET map without requiring a
    // feature.  Keep that policy in its Lisp owner rather than copying the
    // individual command bindings into Rust.
    loadup_required_sequence(interpreter, &["international/mule-cmds", "case-table"])?;

    // Keep the complete multilingual loadup group in GNU's order.  Omitting
    // an owner creates half-registered charset and coding-system state that
    // later files can accidentally paper over.
    // charprop.el is generated and GNU deliberately loads it with NOERROR.
    loadup_optional_library(interpreter, "international/charprop.el")?;
    if interpreter.has_feature("charprop") {
        interpreter.set_global_binding("redisplay--inhibit-bidi", Value::Nil);
    }
    loadup_required_sequence(
        interpreter,
        &[
            "international/characters",
            "composite",
            "language/chinese",
            "language/cyrillic",
            "language/indian",
            "language/sinhala",
            "language/english",
            "language/ethiopic",
            "language/european",
            "language/czech",
            "language/slovak",
            "language/romanian",
            "language/greek",
            "language/hebrew",
            "international/cp51932",
            "international/eucjp-ms",
            "language/japanese",
            "language/korean",
            "language/lao",
            "language/tai-viet",
            "language/thai",
            "language/tibetan",
            "language/vietnamese",
            "language/misc-lang",
            "language/utf-8-lang",
            "language/georgian",
            "language/khmer",
            "language/burmese",
            "language/cham",
            "language/philippine",
            "language/indonesian",
        ],
    )?;

    // GNU loads indent.el immediately before cl-generic and simple.el.  Its
    // TAB command and indentation orchestration are Lisp policy layered over
    // native buffer primitives; preload the complete owner instead of growing
    // a second, partial command implementation as language modes exercise it.
    loadup_required_sequence(interpreter, &["indent", "emacs-lisp/cl-generic", "simple"])?;

    // GNU dumps simple.el before minibuffer.el.  It owns the completion-list
    // navigation and selection commands used by Minibuffer's M-up/M-down
    // bindings.  Keep those command policies in the standard GNU library
    // instead of maintaining local copies alongside the native minibuffer
    // substrate.
    loadup_required_sequence(interpreter, &["emacs-lisp/seq", "emacs-lisp/nadvice"])?;

    // Minibuffer is also part of GNU's dumped image.  Keep its definitions on
    // the Lisp side; Help legitimately refers to this map without requiring
    // the feature first.
    loadup_required_library(interpreter, "minibuffer")?;

    // GNU's bare temacs already has its initial terminal frame while loadup
    // runs.  frame.el adds portable display policy for that existing frame;
    // face creation itself remains owned by the real defface lifecycle.
    loadup_required_library(interpreter, "frame")?;

    // Keep this contiguous: GNU loadup loads startup before the terminal-color
    // and Font Lock cluster.  In particular, syntax.el may call Help functions
    // because help.el was loaded much earlier above.
    loadup_required_sequence(
        interpreter,
        &[
            "startup",
            "term/tty-colors",
            "font-core",
            "emacs-lisp/syntax",
            "font-lock",
            "jit-lock",
            "mouse",
        ],
    )?;

    // GNU loadup dumps mouse.el after frame/font-lock and before select.el.
    // Context-menu construction, mouse translations, and their defcustom
    // declarations are Elisp policy; reconstruct the complete dumped owner
    // rather than relying on the file-less native fallbacks during batch use.
    if loadup_predicate(interpreter, "(boundp 'x-toolkit-scroll-bars)")? {
        loadup_required_library(interpreter, "scroll-bar")?;
    }

    // GNU loadup dumps select.el after frame.el (and the mouse/scroll-bar
    // owners) and before the timer/menu cluster.  Its public
    // GUI selection API is portable Elisp policy over backend primitives;
    // callers such as x-dnd.el legitimately use it without requiring the
    // feature.  Reconstruct the complete owner instead of providing a native
    // gui-set-selection shortcut or teaching individual clients about it.
    loadup_required_sequence(
        interpreter,
        &[
            "select",
            "emacs-lisp/timer",
            "emacs-lisp/easymenu",
            "isearch",
            "rfn-eshadow",
        ],
    )?;

    // timer.el is the next unconditional owner in GNU loadup.  Startup and
    // delayed Custom initialization call its public scheduling helpers
    // without requiring the feature because they are present in the dump.
    // GNU loadup dumps easymenu.el after frame.el and before isearch.el.
    // Packages such as EUDC call its menu constructors without requiring the
    // feature, so preserve the complete Lisp-owned menu policy at startup.
    // isearch.el is dumped by GNU and owns both the incremental-search
    // command layer and its full keymap.  Loading only downstream users (for
    // example Eshell's history module) cannot recreate that startup state.
    // menu-bar.el follows isearch.el in GNU loadup and is part of the dumped
    // image.  Mode libraries therefore expand its Lisp-owned menu macros
    // without requiring `menu-bar' first.  Load the complete owner here;
    // runtime keymaps retain their Rust identity while exposing GNU's mutable
    // cons-list interface to this library.
    // The native bootstrap binds menu-bar-edit-menu as an empty stand-in
    // for sessions without the Lisp tree; unbind it here so menu-bar.el's
    // own defvar populates the real Edit menu exactly as GNU's dumped
    // image carries it.
    interpreter.remove_global_binding("menu-bar-edit-menu");
    loadup_required_sequence(
        interpreter,
        &[
            "menu-bar",
            "tab-bar",
            "emacs-lisp/lisp",
            "textmodes/page",
            "register",
            "textmodes/paragraphs",
            "progmodes/prog-mode",
        ],
    )?;

    // GNU loadup loads and dumps tab-bar.el immediately after menu-bar.el.
    // Its frame-local tab data, commands, and undo policy are Elisp-owned
    // startup state: callers may use them without requiring `tab-bar' first.
    // Reconstruct the complete owner here rather than mirroring whichever
    // entry point a test happens to reach in the Rust host.
    // GNU dumps emacs-lisp/lisp.el immediately after the menu libraries.
    // Its structural navigation commands (including forward/backward-list)
    // are Lisp policy layered over the native syntax scanner and are called
    // directly by mode libraries such as js.el.
    // register.el is dumped shortly after isearch.el.  Kmacro and other
    // preloaded clients call its public register accessors without requiring
    // the feature themselves, so preserve the owning Lisp library here.
    // paragraphs.el follows register.el in GNU loadup and intentionally has
    // no `provide' form.  It owns `use-hard-newlines' and the complete
    // paragraph/sentence policy used directly by dumped clients such as So
    // Long, so reconstruct the file itself rather than copying whichever
    // missing entry point a client happens to expose.
    // GNU loadup establishes the programming-mode parent before loading the
    // shared Lisp modes.  Loading lisp-mode first leaves its derived-mode
    // parent pointing at a mode that never ran the Elisp-owned reset/hook
    // lifecycle.
    loadup_required_sequence(
        interpreter,
        &[
            "emacs-lisp/lisp-mode",
            "textmodes/text-mode",
            "textmodes/fill",
            "newcomment",
            "replace",
            "emacs-lisp/tabulated-list",
            "buff-menu",
        ],
    )?;

    // GNU loadup dumps the complete fill.el owner after the standard mode
    // cluster.  The file intentionally has no `provide' form, so load its
    // real implementation directly.
    // GNU preloads newcomment.el immediately before replace.el.  It owns the
    // comment-motion and editing policy used by startup predicates such as So
    // Long's leading-comment scan; preserve that complete Elisp owner rather
    // than treating a swallowed `void-function' as a negative predicate.
    // replace.el follows the standard mode/register cluster in GNU loadup
    // and is part of the dumped image.  Its Occur and query-replace engines
    // are Elisp policy; tests and preloaded clients legitimately call them
    // without requiring `replace', so reconstruct that same owner here.
    // GNU reaches tabulated-list only after replace.el, immediately before
    // buff-menu.el.  Loading it near the early font cluster fabricated a
    // startup state that GNU never has.
    if loadup_predicate(interpreter, "(fboundp 'x-create-frame)")? {
        loadup_required_sequence(
            interpreter,
            &[
                "fringe",
                "emacs-lisp/regexp-opt",
                "image",
                "international/fontset",
                "dnd",
                "tool-bar",
            ],
        )?;
    }

    if loadup_predicate(interpreter, "(featurep 'dynamic-setting)")? {
        loadup_required_library(interpreter, "dynamic-setting")?;
    }
    if loadup_predicate(interpreter, "(featurep 'x)")? {
        loadup_required_sequence(
            interpreter,
            &["touch-screen", "x-dnd", "term/common-win", "term/x-win"],
        )?;
    }
    if loadup_predicate(interpreter, "(featurep 'haiku)")? {
        loadup_required_sequence(interpreter, &["term/common-win", "term/haiku-win"])?;
    }
    if loadup_predicate(interpreter, "(featurep 'android)")? {
        loadup_required_sequence(
            interpreter,
            &[
                "ls-lisp",
                "touch-screen",
                "term/common-win",
                "term/android-win",
            ],
        )?;
    }
    if loadup_predicate(
        interpreter,
        "(or (eq system-type 'windows-nt) (featurep 'w32))",
    )? {
        loadup_required_sequence(
            interpreter,
            &["term/common-win", "w32-vars", "term/w32-win", "disp-table"],
        )?;
        if loadup_predicate(interpreter, "(eq system-type 'windows-nt)")? {
            loadup_required_sequence(interpreter, &["w32-fns", "ls-lisp", "dos-w32"])?;
        }
        loadup_required_library(interpreter, "touch-screen")?;
    }
    if loadup_predicate(interpreter, "(eq system-type 'ms-dos)")? {
        loadup_required_sequence(
            interpreter,
            &[
                "dos-w32",
                "dos-fns",
                "dos-vars",
                "term/internal",
                "term/pc-win",
                "ls-lisp",
                "disp-table",
            ],
        )?;
    }
    if loadup_predicate(interpreter, "(featurep 'ns)")? {
        loadup_required_library(interpreter, "term/common-win")?;
        if loadup_predicate(interpreter, "(featurep 'charprop)")? {
            loadup_required_sequence(
                interpreter,
                &[
                    "international/mule-util",
                    "international/ucs-normalize",
                    "term/ns-win",
                ],
            )?;
        }
    }
    if loadup_predicate(interpreter, "(featurep 'pgtk)")? {
        loadup_required_sequence(
            interpreter,
            &[
                "pgtk-dnd",
                "touch-screen",
                "term/common-win",
                "term/pgtk-win",
            ],
        )?;
    }
    if loadup_predicate(interpreter, "(fboundp 'x-create-frame)")? {
        loadup_required_library(interpreter, "mwheel")?;
    }

    loadup_required_library(interpreter, "progmodes/elisp-mode")?;
    loadup_required_library(interpreter, "emacs-lisp/float-sup")?;

    loadup_required_sequence(
        interpreter,
        &[
            "vc/vc-hooks",
            "vc/ediff-hook",
            "uniquify",
            "electric",
            "paren",
            "emacs-lisp/shorthands",
            "emacs-lisp/eldoc",
            "emacs-lisp/cconv",
            "cus-start",
        ],
    )?;
    if loadup_predicate(interpreter, "(not (eq system-type 'ms-dos))")? {
        loadup_required_library(interpreter, "tooltip")?;
    }
    loadup_required_sequence(interpreter, &["international/iso-transl", "emacs-lisp/rmc"])?;
    loadup_optional_library(interpreter, "leim/leim-list.el")?;
    loadup_optional_library(interpreter, "site-load")?;

    initialize_batch_documentation(interpreter)?;
    loadup_optional_library(interpreter, "site-init")?;
    loadup_eval(
        interpreter,
        "(progn
           (setq current-load-list nil
                 custom-current-group-alist nil)
           (set-buffer-modified-p nil)
           (remove-hook 'after-load-functions
                        (lambda (_) (garbage-collect)))
           (setq inhibit-load-charset-map nil)
           (clear-charset-maps)
           (garbage-collect)
           (buffer-enable-undo \"*scratch*\")
           (setq purify-flag nil
                 redisplay--inhibit-bidi nil))",
    )?;

    Ok(())
}

fn format_backtrace_summary(interpreter: &Interpreter) -> String {
    format_backtrace_frames(interpreter.backtrace_frames_snapshot())
}

fn format_backtrace_frames(frames: Vec<(bool, Value, Vec<Value>, bool)>) -> String {
    frames
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
                crate::test_support::note_host_permit_moved_to_this_thread();
                test();
            })
            .expect("spawn large-stack test thread")
            .join()
            .expect("join large-stack test thread");
    }

    #[test]
    fn optional_loadup_manifest_matches_gnu_noerror_loads() {
        let source = fs::read_to_string(compat::project_root().join("../emacs/lisp/loadup.el"))
            .expect("read GNU loadup.el");
        let actual = source
            .lines()
            .filter_map(|line| {
                let line = line.trim();
                let library = line.strip_prefix("(load \"")?.strip_suffix("\" t)")?;
                Some(library)
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, GNU_OPTIONAL_LOADUP_LIBRARIES);
    }

    #[test]
    fn optional_loadup_helper_rejects_unconditional_libraries() {
        let error = loadup_optional_library(&mut Interpreter::new(), "subr")
            .expect_err("an unconditional GNU load must not become optional");
        assert!(error.contains("not an optional GNU loadup library"));
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
    fn standard_library_precedes_the_discovered_test_tree() {
        // Finding 123.  `effective_batch_load_path' appended the recursively
        // discovered repo directories BEFORE the installation's Lisp, so 77
        // directories -- 11 of them under `lisp/' itself -- sat ahead of the
        // standard library and shadowed core names.  ELEVEN resolved into
        // CEDET/quail/fixture directories: chart, comp, debug, generic, map,
        // compile, cpp, emoji, etags, grep, python.  `etags' is the vivid one
        // -- it came from test/manual/etags/el-src/.../etags.el, a PARSING
        // FIXTURE, which would have shadowed the real etags.
        //
        // This asserts the ORDER directly rather than through an
        // interpreter.  A previous attempt tested it via
        // `initialized_upstream_batch_interpreter', which is built with
        // `load_path' ALREADY set to GNU's 25 directories (test_support.rs);
        // those sit at the head under either ordering, so that test passed
        // with the fix reverted and pinned nothing.
        let upstream = compat::project_root().join("../emacs");
        if !upstream.join("lisp").is_dir() {
            return;
        }
        let _env_write = compat::lock_boot_environment_for_write();
        let previous = env::var("EMACS_TEST_DIRECTORY").ok();
        // SAFETY: env mutation is serialized against every concurrent boot
        // by the boot-environment write lock held above (the old
        // "--test-threads=1" justification was wrong; the gate is parallel).
        unsafe { env::set_var("EMACS_TEST_DIRECTORY", upstream.join("test")) }
        // An EMPTY `load_path' is what the real CLI passes; supplying one
        // would reintroduce the fixture that made the old test vacuous.
        let resolved = effective_batch_load_path(&BatchRunOptions::default());
        // SAFETY: serialized by the boot-environment write lock above.
        unsafe {
            match previous {
                Some(value) => env::set_var("EMACS_TEST_DIRECTORY", value),
                None => env::remove_var("EMACS_TEST_DIRECTORY"),
            }
        }
        drop(_env_write);
        let resolved = resolved.expect("resolve batch load path");
        // Assert the PREFIX rather than relative indices.  Comparing
        // `position(emacs-lisp) < position(cedet/srecode)' happens to fail
        // under the old order on this filesystem only because `cedet' sorts
        // before `emacs-lisp' in one unsorted WalkDir -- that is readdir
        // order, not a guarantee.  Requiring GNU's entire load-path to be an
        // exact leading prefix cannot hold under the old ordering on any
        // filesystem, and says what the fix actually promises.
        let upstream_paths =
            compat::emaxx_upstream_load_path(&upstream).expect("upstream load path");
        assert!(
            resolved.len() > upstream_paths.len(),
            "the discovered test tree should still be appended"
        );
        assert_eq!(
            &resolved[..upstream_paths.len()],
            upstream_paths.as_slice(),
            "GNU's load-path must be an exact leading prefix of Emaxx's"
        );
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
    fn batch_runtime_records_ordered_initialization_times() {
        let options = BatchRunOptions::default();
        let mut interpreter =
            initialize_batch_interpreter(&options).expect("init batch interpreter");

        for name in ["before-init-time", "after-init-time"] {
            let value = interpreter
                .lookup_var(name, &Vec::new())
                .unwrap_or_else(|| panic!("{name} is bound"));
            assert_eq!(value.to_vec().expect("old-style time list").len(), 4);
        }
        // GNU owns `not` as an alias in subr.el.  This test covers the
        // C-owned time cells and `time-less-p`, not the presence of that
        // Elisp convenience, so keep the probe on the C primitive boundary.
        let ordered = Reader::new("(null (time-less-p after-init-time before-init-time))")
            .read()
            .expect("read time ordering probe")
            .expect("time ordering probe");
        assert_eq!(
            interpreter
                .eval(&ordered, &mut Vec::new())
                .expect("compare startup times"),
            Value::T
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
            // This fixture exercises load-history provenance in a deliberately
            // bare host.  GNU owns `defun' in byte-run.el, whereas `defalias'
            // and `function' are C-owned and available before that Elisp owner
            // loads.
            "(defalias 'xref-find-definitions (function (lambda ())))\n\
             (provide 'xref)\n",
        )
        .expect("write standard Lisp fixture");
        fs::write(
            &runtime_test,
            "(defalias 'xref-probe-test (function (lambda ())))\n",
        )
        .expect("write test Lisp fixture");

        let mut interpreter = Interpreter::new();
        interpreter
            .set_load_source_provenance_remap(runtime_root.join("lisp"), dump_root.join("lisp"));
        lisp::load_file_strict(&mut interpreter, &runtime_lisp)
            .expect("load standard Lisp fixture");
        lisp::load_file_strict(&mut interpreter, &runtime_test).expect("load test Lisp fixture");

        // `locate-file' itself belongs to files.el.  Exercise its C-owned
        // `locate-file-internal' substrate directly because this fixture is
        // intentionally proving bare-host provenance remapping.
        let located = Reader::new(&format!(
            "(locate-file-internal \"xref\" (list {:?}) '(\".el\") nil)",
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
    fn batch_runtime_reconstructs_the_image_despite_a_shadowing_load_path() {
        // GNU's dump is built from the installation's own lisp/ tree, so a
        // user's `-L' cannot disturb it: `emacs -Q -batch -L <dir-with-a-fake
        // button.el>' still has a complete image, and the directory merely
        // sits at the head of `load-path'.  Probed from the pinned oracle:
        //     (t "/tmp/emaxx-partial")
        // Emaxx reconstructs that image at startup and must therefore
        // reconstruct from the installation tree alone, honouring `-L' only
        // for the session that follows.
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("emaxx-batch-button-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&root).expect("create temp root");
        fs::write(
            root.join("button.el"),
            "(defun insert-text-button (&rest _args) 'loaded)\n(provide 'button)\n",
        )
        .expect("write shadowing button preload");

        let options = BatchRunOptions {
            load_path: vec![root.clone()],
            ..Default::default()
        };
        let interpreter = initialize_batch_interpreter(&options)
            .expect("a shadowing -L must not break the image");
        assert!(
            interpreter.lookup_function("when", &Vec::new()).is_ok(),
            "the reconstructed image must be complete despite the shadowing -L"
        );
        assert_eq!(
            interpreter.configured_load_path().first(),
            Some(&root),
            "GNU puts a -L directory at the head of load-path"
        );

        fs::remove_dir_all(root).expect("remove temp root");
    }

    #[test]
    fn batch_runtime_rejects_a_broken_resolvable_preload() {
        // A broken file in the tree the image is reconstructed FROM must fail
        // loudly rather than yield a half-built runtime.  Reconstruction is
        // anchored to EMAXX_DUMP_SOURCE_DIRECTORY -- the tree whose compiled
        // Lisp the oracle executes; EMACS_TEST_DIRECTORY names only the test
        // tree and deliberately cannot choose the image's bytes (finding 63)
        // -- so the fixture is installed as the dump tree.
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "emaxx-batch-broken-preload-{}-{stamp}",
            std::process::id()
        ));
        // The fixture must be a complete dump tree.  Mirror the pinned
        // sibling's lisp/ with real directories (load-path enumeration does
        // not traverse directory symlinks) and per-file symlinks, replacing
        // seq with a broken source and omitting the compiled seq.elc the
        // loader would otherwise prefer.
        let sibling = compat::project_root().join("../emacs");
        let sibling_lisp = sibling
            .join("lisp")
            .canonicalize()
            .expect("canonical sibling lisp tree");
        let fixture_lisp = root.join("lisp");
        let mut pending = vec![sibling_lisp.clone()];
        while let Some(directory) = pending.pop() {
            let relative = directory
                .strip_prefix(&sibling_lisp)
                .expect("fixture mirror stays under lisp/");
            let target = fixture_lisp.join(relative);
            fs::create_dir_all(&target).expect("mirror sibling lisp directory");
            for entry in fs::read_dir(&directory).expect("list sibling lisp directory") {
                let entry = entry.expect("sibling lisp entry");
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                let name = entry.file_name();
                if relative == Path::new("emacs-lisp") && (name == "seq.el" || name == "seq.elc") {
                    continue;
                }
                std::os::unix::fs::symlink(&path, target.join(&name))
                    .expect("shadow sibling lisp file");
            }
        }
        fs::create_dir_all(root.join("test")).expect("create fixture test directory");
        fs::write(
            fixture_lisp.join("emacs-lisp/seq.el"),
            "(error \"broken seq preload\")\n",
        )
        .expect("write broken seq preload");

        let _env_write = compat::lock_boot_environment_for_write();
        let previous_dump = env::var(compat::DUMP_SOURCE_DIRECTORY_ENV).ok();
        let previous_test = env::var("EMACS_TEST_DIRECTORY").ok();
        // SAFETY: env mutation is serialized against every concurrent boot
        // by the boot-environment write lock held above (the old
        // "--test-threads=1" justification was wrong; the gate is parallel).
        unsafe {
            env::set_var(compat::DUMP_SOURCE_DIRECTORY_ENV, &root);
            env::set_var("EMACS_TEST_DIRECTORY", root.join("test"));
        }
        let result = initialize_batch_interpreter(&BatchRunOptions::default());
        // SAFETY: serialized by the boot-environment write lock above.
        unsafe {
            match previous_dump {
                Some(value) => env::set_var(compat::DUMP_SOURCE_DIRECTORY_ENV, value),
                None => env::remove_var(compat::DUMP_SOURCE_DIRECTORY_ENV),
            }
            match previous_test {
                Some(value) => env::set_var("EMACS_TEST_DIRECTORY", value),
                None => env::remove_var("EMACS_TEST_DIRECTORY"),
            }
        }

        let error = match result {
            Ok(_) => panic!("a resolvable dumped-library preload must not fail silently"),
            Err(error) => error,
        };
        assert!(error.contains("preload emacs-lisp/seq"), "{error}");
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
                       \"/textmodes/fill\"
                       (file-name-sans-extension
                        (symbol-file 'fill-region-as-paragraph 'defun)))
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
                       (fboundp 'jit-lock-register)
                       (frame-parameter nil 'background-mode)
                       (frame-parameter nil 'display-type)
                       (and (facep 'show-paren-match) t)
                       (and (facep 'tool-bar) t))",
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
                    Value::Symbol("dark".into()),
                    Value::Symbol("mono".into()),
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
                       (eq (oclosure-type
                            (cconv--interactive-helper
                             (lambda () 'ok) nil))
                           'cconv--interactive-helper)
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
    fn batch_runtime_preloads_the_complete_selection_owner() {
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
                "(list (featurep 'select)\
                       (fboundp 'gui-set-selection)\
                       (not (subrp (symbol-function 'gui-set-selection)))\
                       (boundp 'selection-converter-alist)\
                       (gui-set-selection 'PRIMARY \"payload\")\
                       (condition-case error-data\
                           (gui-set-selection 'PRIMARY '(invalid))\
                         (error error-data)))",
            )
            .read_all()
            .expect("read selection startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate selection startup probe"),
                Value::list([
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::T,
                    Value::String("payload".into()),
                    Value::list([
                        Value::Symbol("error".into()),
                        Value::String("invalid selection".into()),
                        Value::list([Value::Symbol("invalid".into())]),
                    ]),
                ])
            );
        });
    }

    #[test]
    fn batch_runtime_preloads_the_mouse_owner_and_dynamic_context_menu_policy() {
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
                "(list (featurep 'mouse)\
                       (special-variable-p 'context-menu-functions)\
                       (special-variable-p 'context-menu-filter-function)\
                       (not (subrp (symbol-function 'context-menu-map)))\
                       (let ((context-menu-functions nil))\
                         (equal (context-menu-map)\
                                '(keymap \"Context Menu\"))))",
            )
            .read_all()
            .expect("read mouse startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate mouse startup probe"),
                Value::list([Value::T, Value::T, Value::T, Value::T, Value::T])
            );
        });
    }

    #[test]
    fn xt_mouse_read_key_discards_the_unbound_down_event() {
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
                r#"(progn
                     (require 'xt-mouse)
                     (require 'cl-lib)
                     (let ((width (frame-width))
                           (height (frame-height)))
                       (unwind-protect
                           (progn
                             (set-frame-width nil (max width 2000))
                             (set-frame-height nil (max height 2000))
                             (cl-letf (((terminal-parameter nil 'xterm-mouse-x) nil)
                                       ((terminal-parameter nil 'xterm-mouse-y) nil)
                                       ((terminal-parameter nil 'xterm-mouse-last-down) nil)
                                       ((terminal-parameter nil 'xterm-mouse-last-click) nil))
                               (unless xterm-mouse-mode
                                 (cl-letf (((symbol-function 'terminal-name)
                                            (lambda (&optional _) "fake-terminal")))
                                   (xterm-mouse-mode)))
                               (unwind-protect
                                   (let* ((unread-command-events
                                           (append "\e[M%\xD9\x81"
                                                   "\e[M'\xD9\x81" nil))
                                          (key (read-key)))
                                     (list (car key)
                                           (nth 2 (cadr key))
                                           unread-command-events))
                                 (xterm-mouse-mode 0))))
                         (set-frame-width nil width)
                         (set-frame-height nil height))))"#,
            )
            .read_all()
            .expect("read XTerm mouse stage probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("read the translated XTerm mouse event"),
                Value::list([
                    Value::Symbol("S-mouse-2".into()),
                    Value::cons(Value::Integer(184), Value::Integer(95)),
                    Value::Nil,
                ])
            );
        });
    }

    #[test]
    fn batch_reconstruction_registers_the_default_tty_colors() {
        // startup.el:1479 runs `tty-register-default-colors' for batch
        // sessions too.  Without it `tty-color-alist' is empty, so
        // `color-values' answers nil for every named color and color.el's
        // arithmetic on that nil signals wrong-type-argument -- the shape
        // that produced 21 mismatches in the 2026-08-25 frozen baseline.
        // Pin the registered set and one end-to-end color lookup so a
        // loadup or reconstruction reorder cannot silently undo it.
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
                "(list (length (tty-color-alist))\
                       (mapcar #'car (tty-color-alist))\
                       (color-values \"red\")\
                       (color-defined-p \"red\")\
                       (color-name-to-rgb \"red\"))",
            )
            .read_all()
            .expect("read tty color probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut Vec::new())
                    .expect("evaluate tty color probe"),
                Value::list([
                    Value::Integer(8),
                    Value::list([
                        Value::String("black".into()),
                        Value::String("red".into()),
                        Value::String("green".into()),
                        Value::String("yellow".into()),
                        Value::String("blue".into()),
                        Value::String("magenta".into()),
                        Value::String("cyan".into()),
                        Value::String("white".into()),
                    ]),
                    Value::list([Value::Integer(65535), Value::Integer(0), Value::Integer(0),]),
                    Value::T,
                    Value::list([Value::Float(1.0), Value::Float(0.0), Value::Float(0.0)]),
                ])
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
    fn batch_runtime_loads_gnu_inherited_cl_struct_setters() {
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
                     (setf (eieio--class-parents class) nil)
                     (setf (eieio--class-slots class) (list descriptor))
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
            // `?\u{2018}' in the program is destroyed by `--eval' argument
            // decoding under LANG=C; escape it so the argument stays ASCII.
            let oracle_form = crate::test_support::oracle_program_ascii(&oracle_form);
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
                   ;; GNU's mule-tests.el loads this Elisp owner before it
                   ;; uses the test-only input macro.
                   (require 'ert-x)
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
                             ;; Pin the input: under LANG=C the buffer
                             ;; coding defaults to nil/raw-text and the
                             ;; sgml row answers differently per locale
                             ;; (finding 113's hardcoded-expectation
                             ;; class).  With the pin, the oracle answers
                             ;; identically under C and UTF-8 locales.
                             (setq buffer-file-coding-system 'utf-8-unix)
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
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
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
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
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
