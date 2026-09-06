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
    /// emacs.c consumes --no-site-lisp (also implied by -Q) before init_lread.
    pub no_site_lisp: bool,
    pub load: Vec<String>,
    pub eval: Vec<String>,
    pub funcall: Vec<String>,
    pub args_left: Vec<String>,
    /// Complete argv for GNU's unchanged startup, after the C-owned switches
    /// have been consumed. The ordinary CLI always uses this route.
    pub startup_command_line_args: Option<Vec<String>>,
    /// Return before the Lisp top-level runs. GNU startup.el, not the Rust
    /// constructor, owns delayed Custom initialization in the live session.
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
    let mut interpreter = initialize_batch_interpreter(&options)?;
    if let Some(command_line_args) = &options.startup_command_line_args {
        return run_batch_through_normal_top_level(&mut interpreter, command_line_args);
    }
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
                let resolved = PathBuf::from(target);
                if let Err(error) = interpreter.load_target_with_env(target, &eval_env) {
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

pub(crate) fn run_startup_top_level(
    interpreter: &mut Interpreter,
    command_line_args: &[String],
) -> Result<Value, LispError> {
    interpreter.set_global_binding(
        "command-line-args",
        Value::list(
            command_line_args.iter().map(|argument| {
                lisp::primitives::bytes_to_shared_unibyte_value(argument.as_bytes())
            }),
        ),
    );
    // keyboard.c:top_level_2 evaluates the stored form, not a host-authored
    // call to a particular Lisp function. startup.el owns all startup policy.
    let form = interpreter
        .forwarded_c_value("top-level", &Vec::new())
        .unwrap_or(Value::Nil);
    if form.is_nil() {
        let purify = interpreter
            .forwarded_c_value("purify-flag", &Vec::new())
            .unwrap_or(Value::Nil);
        return lisp::primitives::call(
            interpreter,
            "message",
            &[Value::string(if purify.is_nil() {
                "Bare Emacs (standard Lisp code not loaded)"
            } else {
                "Bare impure Emacs (standard Lisp code not loaded)"
            })],
            &mut Vec::new(),
        );
    }
    let handler = interpreter
        .symbol_value_cell("noninteractive")
        .is_ok_and(|value| value.is_truthy())
        .then(|| {
            interpreter.push_handler_bindings(&[(
                vec!["error".into()],
                Value::symbol("debug-early--handler"),
            )])
        });
    let result = lisp::primitives::call(interpreter, "eval", &[form, Value::T], &mut Vec::new());
    if let Some(handler) = handler {
        interpreter.pop_handler_bindings(handler);
    }
    result
}

fn run_batch_through_normal_top_level(
    interpreter: &mut Interpreter,
    command_line_args: &[String],
) -> Result<BatchRunOutcome, String> {
    match run_startup_top_level(interpreter, command_line_args) {
        Ok(_) => {}
        Err(LispError::Terminate(termination)) => return Ok(termination.into()),
        Err(error) => {
            // keyboard.c top_level_1 -> cmd_error -> cmd_error_internal: in
            // batch, print_error_message to stderr and kill-emacs -1.  The
            // backtrace was already printed, to `standard-output', by the
            // `debug-early--handler' that top_level_2 bound around the
            // form (run_batch_toplevel_form), when
            // `backtrace-on-error-noninteractive' asked for it.
            emit_batch_error_message(interpreter, &error);
            return Ok(BatchRunOutcome::Exit(255));
        }
    }
    Ok(interpreter
        .take_pending_termination()
        .map_or(BatchRunOutcome::Exit(0), Into::into))
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

/// cmd_error_internal's batch report: print_error_message to stderr.
fn emit_batch_error_message(interpreter: &mut Interpreter, error: &LispError) {
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
}

fn emit_unhandled_batch_error(
    interpreter: &mut Interpreter,
    error: &LispError,
    bottom_frames: &[(String, Vec<Value>)],
) {
    emit_batch_error_message(interpreter, error);
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

/// In the test build every interpreter these constructors hand out is an
/// in-process fixture: `invocation-name' is the libtest binary, which
/// cannot serve as comp.el's compiler child or as the trampoline
/// compiler `fset' of a primitive asks for (test_support explains the
/// configuration).  The CLI keeps GNU's defaults.
#[cfg(test)]
fn finish_test_fixture(interpreter: Result<Interpreter, String>) -> Result<Interpreter, String> {
    let mut interpreter = interpreter?;
    crate::test_support::configure_embedded_native_compilation(&mut interpreter);
    Ok(interpreter)
}

#[cfg(not(test))]
fn finish_test_fixture(interpreter: Result<Interpreter, String>) -> Result<Interpreter, String> {
    interpreter
}

/// Reconstruct persistent Lisp state and initialize C-owned interactive
/// process state. The terminal must be ready before run_startup_top_level.
pub(crate) fn initialize_interactive_interpreter(
    no_site_lisp: bool,
) -> Result<Interpreter, String> {
    let options = BatchRunOptions {
        no_site_lisp,
        defer_delayed_custom_init: true,
        ..Default::default()
    };
    finish_test_fixture(initialize_interpreter(&options, false))
}

pub(crate) fn initialize_batch_interpreter(
    options: &BatchRunOptions,
) -> Result<Interpreter, String> {
    finish_test_fixture(initialize_interpreter(options, true))
}

/// The batch image exactly as startup left it, before the test fixture's
/// embedded native-compilation settings are applied: for tests that
/// assert what the unchanged GNU Lisp owners themselves established.
#[cfg(test)]
pub(crate) fn initialize_batch_interpreter_as_started(
    options: &BatchRunOptions,
) -> Result<Interpreter, String> {
    initialize_interpreter(options, true)
}

fn initialize_interpreter(
    options: &BatchRunOptions,
    noninteractive: bool,
) -> Result<Interpreter, String> {
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
    let installation_load_path = installation_lisp_load_path()?;
    interpreter.set_load_path(installation_load_path.clone());
    configure_native_load_path_for_dump_reconstruction(&mut interpreter)?;
    // GNU starts batch evaluation in *scratch*, whose buffer-local
    // `lexical-binding' is t while the default remains nil.  File cookies
    // override and restore this state around loads.
    interpreter.set_variable("lexical-binding", Value::T, &mut Vec::new());
    interpreter.set_variable("noninteractive", Value::T, &mut Vec::new());
    // emacs.c handles --batch before syms_of_undo: in an uninitialized
    // builder, syms_of_undo reinstalls 24000000 before loadup runs. Leave
    // that raw initializer intact here; a fresh batch session clears it below.
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
    // font.c:init_font runs after syms_of_font and before loadup.el.  Merely
    // having EMACS_FONT_LOG in the environment enables logging, even when its
    // value is the empty string.
    interpreter.set_variable(
        "font-log",
        if env::var_os("EMACS_FONT_LOG").is_some() {
            Value::Nil
        } else {
            Value::T
        },
        &mut Vec::new(),
    );

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
        // pdumper.c writes Vpurify_flag as nil into the saved image.
        interpreter.set_global_binding("purify-flag", Value::Nil);
        Ok(())
    })(&mut interpreter);
    // Restore before propagating: a failed reconstruction must not leave the
    // session muted, or its own diagnostics would be swallowed too.
    interpreter.set_variable("inhibit-message", Value::Nil, &mut Vec::new());
    interpreter.set_variable("message-log-max", saved_message_log_max, &mut Vec::new());
    reconstruction?;
    // emacs.c uses 0.1 while temacs builds the dump, then raises the value to
    // 1.0 only when an initialized (dump-loaded) process starts in batch mode.
    // The reconstructed image above is the temacs/loadup phase. Apply the
    // new process's C values before live-session initialization and Lisp.
    interpreter.set_variable(
        "noninteractive",
        if noninteractive { Value::T } else { Value::Nil },
        &mut Vec::new(),
    );
    interpreter.set_variable(
        "gc-cons-percentage",
        Value::float(if noninteractive { 1.0 } else { 0.1 }),
        &mut Vec::new(),
    );
    if noninteractive {
        interpreter.set_variable("undo-outer-limit", Value::Nil, &mut Vec::new());
    }
    let dump_path = Value::list(installation_load_path.iter().map(|path| {
        lisp::primitives::bytes_to_shared_unibyte_value(path.as_os_str().as_encoded_bytes())
    }));
    crate::startup::initialize_load_path(&mut interpreter, dump_path, false, options.no_site_lisp)
        .map_err(|error| format!("initialize session load-path: {error}"))?;
    // emacs.c:init_display follows init_lread. Interactive initialization
    // waits for the terminal in tty.rs before invoking the same C-owned call.
    if noninteractive {
        initialize_initial_frame_faces(&mut interpreter)?;
    }
    // The process CLI enters top-level with its complete argv in the caller.
    // Embedders request an initialized session without executing user actions;
    // use the same GNU owner and consume its normal batch termination here.
    if options.startup_command_line_args.is_none()
        && !options.defer_delayed_custom_init
        && has_configured_lisp_tree(&interpreter)
    {
        let mut args = vec![
            env::current_exe()
                .map_err(|error| format!("resolve invocation: {error}"))?
                .display()
                .to_string(),
        ];
        for path in &options.load_path {
            args.extend(["-L".into(), path.display().to_string()]);
        }
        match run_batch_through_normal_top_level(&mut interpreter, &args)? {
            BatchRunOutcome::Exit(0) => {
                interpreter.take_pending_termination();
            }
            other => return Err(format!("GNU batch startup did not complete: {other:?}")),
        }
    }
    Ok(interpreter)
}

fn configure_native_load_path_for_dump_reconstruction(
    interpreter: &mut Interpreter,
) -> Result<(), String> {
    // emacs.c expands the temporary path installed by syms_of_comp against
    // invocation-directory when a non-dumped Emacs is about to load Lisp.
    // For Emaxx this is the writable build-tree cache under target/.
    let load_path = interpreter
        .lookup_var("native-comp-eln-load-path", &Vec::new())
        .ok_or_else(|| "native-comp-eln-load-path is unbound during startup".to_string())?;
    let (initial_directory, _) = load_path
        .cons_values()
        .ok_or_else(|| "native-comp-eln-load-path has no initial entry".to_string())?;
    let initial_directory = lisp::primitives::string_like(&initial_directory)
        .ok_or_else(|| "native-comp-eln-load-path initial entry is not a string".to_string())?
        .text;
    let invocation_directory = interpreter
        .lookup_var("invocation-directory", &Vec::new())
        .and_then(|value| lisp::primitives::string_like(&value).map(|string| string.text))
        .ok_or_else(|| "invocation-directory is not a string during startup".to_string())?;
    let native_directory =
        lisp::primitives::expand_file_name(&initial_directory, Some(&invocation_directory));

    // The image reconstructed immediately below is GNU's dumped Lisp image.
    // Its system .eln files were built beside `source-directory', and GNU
    // keeps that system directory last after adding writable cache entries.
    // Keeping the two directories distinct lets unchanged loadup.el resolve
    // the same native standard-library functions as the dumped GNU binary,
    // while newly compiled files continue to land in Emaxx's build tree.
    let source_directory = interpreter
        .lookup_var("source-directory", &Vec::new())
        .and_then(|value| lisp::primitives::string_like(&value).map(|string| string.text))
        .ok_or_else(|| "source-directory is not a string during startup".to_string())?;
    let system_native_directory =
        lisp::primitives::expand_file_name("native-lisp/", Some(&source_directory));
    interpreter.set_global_binding(
        "native-comp-eln-load-path",
        Value::list([
            Value::String(native_directory.into()),
            Value::String(system_native_directory.into()),
        ]),
    );
    Ok(())
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

pub(crate) fn initialize_initial_frame_faces(interpreter: &mut Interpreter) -> Result<(), String> {
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
    interpreter
        .call_function_value(
            Value::symbol("tty-set-up-initial-frame-faces"),
            None,
            &[],
            &mut Vec::new(),
        )
        .map_err(|error| format!("initialize initial-frame faces: {error}"))?;
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
    // lread.c:load_path_default uses only PATH_DUMPLOADSEARCH while
    // will_dump_p() is true. The expanded path of a running editor belongs
    // to a later startup phase; obtaining it from the oracle both delegates
    // runtime work and changes the initial state observed by loadup.el.
    if let Ok(dump_root) = env::var(compat::DUMP_SOURCE_DIRECTORY_ENV) {
        return compat::canonicalize_path(&Path::new(&dump_root).join("lisp"))
            .map(|path| vec![path]);
    }
    let sibling = compat::project_root().join("../emacs");
    if sibling.join("lisp").is_dir() {
        return compat::canonicalize_path(&sibling.join("lisp")).map(|path| vec![path]);
    }
    Ok(Vec::new())
}

fn has_configured_lisp_tree(interpreter: &Interpreter) -> bool {
    interpreter
        .lookup_var("load-path", &Vec::new())
        .and_then(|value| value.to_vec().ok())
        .is_some_and(|paths| !paths.is_empty())
}

fn preload_batch_compat_libraries(interpreter: &mut Interpreter) -> Result<(), String> {
    // GNU's executable starts from the state produced by loadup.el.  Emaxx
    // reconstructs that image by evaluating the unchanged GNU owner itself
    // in its real portable-dump mode, stopping at the C dumper handoff after
    // every Lisp-owned image-construction form has run.
    if !has_configured_lisp_tree(interpreter) {
        return Ok(());
    }
    interpreter.define_special_variable("dump-mode", Value::string("pdump"));
    interpreter.define_special_variable("purify-flag", Value::T);
    let path = interpreter
        .resolve_load_target("loadup")
        .map_err(|error| format!("preload GNU loadup.el: {error}"))?
        .ok_or_else(|| "preload GNU loadup.el: cannot resolve loadup".to_string())?;
    let stopped = match crate::lisp::load_file_strict_until_or_error(
        interpreter,
        &path,
        |_| false,
        |error| {
            matches!(
                error,
                LispError::Signal(message)
                    if message == crate::lisp::primitives::PORTABLE_DUMPER_UNAVAILABLE
            )
        },
    ) {
        Ok(stopped) => stopped,
        Err(error) => {
            let backtrace = interpreter
                .take_batch_error_backtrace()
                .map(|snapshot| format_backtrace_frames(snapshot.frames))
                .unwrap_or_default();
            let suffix = if backtrace.is_empty() {
                String::new()
            } else {
                format!(" | backtrace: {backtrace}")
            };
            return Err(format!("preload GNU loadup.el: {error}{suffix}"));
        }
    };
    if !stopped {
        return Err("preload GNU loadup.el: portable-dump handoff was not found".to_string());
    }
    // pdumper restores the live process outside the dump-time dynamic
    // binding.  The running image exposes nil, as does GNU after startup.
    interpreter.set_global_binding("dump-mode", Value::Nil);
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

    #[test]
    fn batch_top_level_uses_the_form_installed_by_gnu_startup() {
        let mut interpreter = Interpreter::new();
        interpreter.set_global_binding("top-level", Value::Integer(42));
        assert_eq!(
            run_batch_through_normal_top_level(&mut interpreter, &["emaxx".into()])
                .expect("evaluate the stored self-evaluating top-level form"),
            BatchRunOutcome::Exit(0),
            "keyboard.c evaluates Vtop_level, not a hardcoded normal-top-level call"
        );
    }

    #[test]
    fn startup_process_modes_preserve_gnu_initialization_phases() {
        let raw = Interpreter::new();
        assert_eq!(
            raw.symbol_value_cell("undo-outer-limit")
                .expect("raw undo initializer"),
            Value::Integer(24_000_000)
        );
        assert_eq!(
            raw.symbol_value_cell("gc-cons-percentage")
                .expect("raw GC initializer"),
            Value::float(0.1)
        );
        let interactive =
            initialize_interactive_interpreter(true).expect("prepare interactive session");
        assert_eq!(
            interactive
                .symbol_value_cell("undo-outer-limit")
                .expect("interactive undo value"),
            Value::Integer(24_000_000)
        );
        assert_eq!(
            interactive
                .symbol_value_cell("gc-cons-percentage")
                .expect("interactive GC value"),
            Value::float(0.1)
        );
        assert_eq!(
            interactive
                .symbol_value_cell("noninteractive")
                .expect("interactive mode"),
            Value::Nil
        );
        drop(interactive);
        let options = BatchRunOptions {
            no_site_lisp: true,
            defer_delayed_custom_init: true,
            ..Default::default()
        };
        let batch = initialize_batch_interpreter(&options).expect("prepare batch session");
        assert_eq!(
            batch
                .symbol_value_cell("undo-outer-limit")
                .expect("batch undo value"),
            Value::Nil
        );
        assert_eq!(
            batch
                .symbol_value_cell("gc-cons-percentage")
                .expect("batch GC value"),
            Value::float(1.0)
        );
        assert_eq!(
            batch
                .symbol_value_cell("noninteractive")
                .expect("batch mode"),
            Value::T
        );
        assert_eq!(
            batch
                .symbol_value_cell("command-line-processed")
                .expect("startup remains pending"),
            Value::Nil
        );
    }

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
    fn batch_load_resolution_uses_the_shared_gnu_search() {
        let root = compat::project_root()
            .join("../emacs/lisp/emacs-lisp")
            .canonicalize()
            .expect("unchanged GNU fixture directory");
        let mut interpreter = Interpreter::new();
        interpreter.set_load_path(vec![root.clone()]);
        for suffix in [".elc", ".el"] {
            interpreter.set_variable(
                "load-suffixes",
                Value::list([Value::string(suffix)]),
                &mut Env::new(),
            );
            let expected = root.join(format!("seq{suffix}"));
            assert!(expected.is_file());
            assert_eq!(
                interpreter
                    .resolve_load_target("seq")
                    .expect("shared load search"),
                Some(expected)
            );
        }
    }

    #[test]
    fn dump_build_load_path_starts_at_the_gnu_source_lisp_root() {
        // lread.c:load_path_default returns PATH_DUMPLOADSEARCH while
        // will_dump_p() is true. GNU's later startup.el owner adds subdirs;
        // an already-expanded path queried from a running oracle is not
        // the initial dump-build state.
        let _boot_environment = compat::boot_environment_read_guard();
        let source = env::var(compat::DUMP_SOURCE_DIRECTORY_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|_| compat::project_root().join("../emacs"));
        let expected = compat::canonicalize_path(&source.join("lisp"))
            .expect("configured GNU Lisp directory exists");
        assert_eq!(
            installation_lisp_load_path().expect("initialize dump-build load path"),
            vec![expected],
            "dump construction must start from its build-time Lisp root"
        );
    }

    #[test]
    fn session_path_does_not_discover_the_gnu_test_tree() {
        // lread.c:init_lread does not read EMACS_TEST_DIRECTORY. This
        // replaces the old assertion that a fabricated recursive path was
        // acceptable as long as standard libraries happened to precede it.
        let upstream = compat::project_root().join("../emacs");
        let root = compat::canonicalize_path(&upstream.join("lisp")).expect("GNU Lisp root");
        let _env_write = compat::lock_boot_environment_for_write();
        let previous_test = env::var_os("EMACS_TEST_DIRECTORY");
        let previous_path = env::var_os("EMACSLOADPATH");
        // SAFETY: the shared bootstrap environment write lock is held.
        unsafe {
            env::set_var("EMACS_TEST_DIRECTORY", upstream.join("test"));
            env::remove_var("EMACSLOADPATH");
        }
        let mut interpreter = Interpreter::new();
        let result = crate::startup::initialize_load_path(
            &mut interpreter,
            Value::list([Value::String(root.display().to_string().into())]),
            false,
            true,
        );
        // SAFETY: restore both variables while retaining the same lock.
        unsafe {
            match previous_test {
                Some(value) => env::set_var("EMACS_TEST_DIRECTORY", value),
                None => env::remove_var("EMACS_TEST_DIRECTORY"),
            }
            match previous_path {
                Some(value) => env::set_var("EMACSLOADPATH", value),
                None => env::remove_var("EMACSLOADPATH"),
            }
        }
        result.expect("GNU C path initialization");
        assert_eq!(interpreter.configured_load_path(), [root]);
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
    fn dump_reconstruction_has_cache_then_gnu_system_native_paths() {
        let mut interpreter = Interpreter::new();
        let invocation_directory = interpreter
            .lookup_var("invocation-directory", &Vec::new())
            .and_then(|value| lisp::primitives::string_like(&value))
            .expect("invocation-directory string")
            .text;
        let expected_cache =
            lisp::primitives::expand_file_name("../native-lisp/", Some(&invocation_directory));
        let source_directory = interpreter
            .lookup_var("source-directory", &Vec::new())
            .and_then(|value| lisp::primitives::string_like(&value))
            .expect("source-directory string")
            .text;
        let expected_system =
            lisp::primitives::expand_file_name("native-lisp/", Some(&source_directory));
        configure_native_load_path_for_dump_reconstruction(&mut interpreter)
            .expect("configure native paths for GNU dump reconstruction");
        let load_path = interpreter
            .lookup_var("native-comp-eln-load-path", &Vec::new())
            .expect("native-comp-eln-load-path is C-initialized")
            .to_vec()
            .expect("native-comp-eln-load-path is a proper list");
        assert_eq!(
            load_path,
            vec![
                Value::String(expected_cache.into()),
                Value::String(expected_system.into())
            ]
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

        // lread.c:Flocate_file_internal returns openp's actual found name.
        // Recorded build-tree history does not rewrite a later filesystem
        // search to a nonexistent build-tree file. `locate-file' itself
        // belongs to files.el; keep this probe on its C-owned substrate.
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
            Value::String(runtime_lisp.display().to_string().into())
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
        // The image is reconstructed by GNU's loadup.el itself, so the
        // failure is loadup's, with the broken library in its backtrace.
        assert!(error.contains("preload GNU loadup.el"), "{error}");
        assert!(error.contains("broken seq preload"), "{error}");
        assert!(error.contains("load(\"emacs-lisp/seq\")"), "{error}");
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
    fn load_path_forwarding_supports_unchanged_gnu_startup_splicing() {
        run_with_large_stack(|| {
            let mut interpreter = initialize_batch_interpreter(&BatchRunOptions::default())
                .expect("load unchanged GNU startup definitions");
            let root = compat::canonicalize_path(&compat::project_root().join("../emacs/lisp"))
                .expect("GNU Lisp root");
            interpreter.set_load_path(vec![root.clone()]);
            let mut env = Vec::new();
            interpreter.set_variable(
                "default-directory",
                Value::String(format!("{}/", root.display()).into()),
                &mut env,
            );
            let original = interpreter
                .symbol_value_cell("load-path")
                .expect("initial load-path");
            let function = interpreter
                .lookup_function("normal-top-level-add-to-load-path", &env)
                .expect("startup.el owns subdirectory expansion");
            interpreter
                .call_function_value(
                    function,
                    None,
                    &[Value::list([Value::string("emacs-lisp")])],
                    &mut env,
                )
                .expect("execute unchanged GNU startup function");
            let current = interpreter
                .symbol_value_cell("load-path")
                .expect("spliced load-path");
            assert!(lisp::primitives::values_eq_in_env(
                &interpreter,
                &current,
                &original,
                &env
            ));
            assert_eq!(
                interpreter.configured_load_path(),
                [root.clone(), root.join("emacs-lisp")]
            );
            assert_eq!(
                original
                    .to_vec()
                    .expect("original remains a proper list")
                    .len(),
                2,
                "the original list itself was spliced"
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
    fn batch_reconstruction_reaches_loadup_native_trampoline_transition() {
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            // The test fixture turns trampolines off after startup (tests
            // must not compile them); loadup's own transition is asked for
            // on the image as started.
            let interpreter =
                initialize_batch_interpreter_as_started(&options).expect("init batch interpreter");
            assert_eq!(
                interpreter.lookup_var("native-comp-enable-subr-trampolines", &Env::new()),
                Some(Value::T),
                "unchanged loadup.el enables trampolines before calling the C dumper"
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
                    Value::list([Value::float(1.0), Value::float(0.0), Value::float(0.0)]),
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
            interpreter
                .load_target("ert")
                .expect("load ert through the shared search");

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
                interpreter
                    .load_target(target)
                    .unwrap_or_else(|error| panic!("load {target}: {error}"));
            }
        });
    }
}
