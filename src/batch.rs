use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use crate::compat::{self, BatchReport, FileStatus, TestStatus};
use crate::lisp;
use crate::lisp::eval::Interpreter;
use crate::lisp::reader::Reader;
use crate::lisp::types::{EmacsTermination, Env, Kind, LispError, LispErrorKind, Value};

#[derive(Clone, Debug, Default)]
pub struct BatchRunOptions {
    pub load_path: Vec<PathBuf>,
    /// emacs.c consumes --no-site-lisp (also implied by -Q) before init_lread.
    pub no_site_lisp: bool,
    /// Enable GNU dynamic-module lifetime and API assertions.
    pub module_assertions: bool,
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
    /// emacs.c's `--dump-file FILE': the image the process starts from.
    /// None looks for the executable's own `<name>.pdmp' beside it.
    pub dump_file: Option<PathBuf>,
    /// emacs.c's `--temacs=MODE' (`pdump' or `pbootstrap'): the process
    /// loads no image, builds its state by running loadup.el as its
    /// top-level form, and loadup.el itself dumps the image.
    pub dump_mode: Option<String>,
    /// emacs.c's `-nl'/`--no-loadup': an uninitialized process does not
    /// load loadup.el ("Bare Emacs").  Ignored by a process that starts
    /// from an image, as in GNU.
    pub no_loadup: bool,
    /// emacs.c:main's "Handle -l loadup, args passed by Makefile": the
    /// file a `-l FILE' first among the remaining arguments names, which
    /// an uninitialized process loads as its top-level form (loadup.el
    /// replaces it unless `-nl').
    pub temacs_load: Option<String>,
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

/// Run the batch interpreter on a guarded stack on the calling OS thread.
pub fn run_batch_with_large_stack(options: BatchRunOptions) -> Result<BatchRunOutcome, String> {
    lisp::runtime::with_runtime(|| with_batch_stack(|| run_batch(options)))?
}

/// Run the batch CLI with its interpreter alive through FINISH.
///
/// emacs.c:Fkill_emacs ends shutdown with exit or exec, without walking and
/// freeing the Lisp heap. FINISH owns process termination; if it returns,
/// ordinary Rust Drop still releases the interpreter.
pub fn run_batch_process_with_large_stack(
    options: BatchRunOptions,
    finish: impl FnOnce(BatchRunOutcome) -> Result<u8, String>,
) -> Result<u8, String> {
    lisp::runtime::with_runtime(|| {
        with_batch_stack(|| {
            let _symbols = crate::lisp::types::symbols::EditorSymbolScope::enter();
            let actions = batch_actions(&options);
            let mut interpreter = initialize_batch_interpreter(&options)?;
            let outcome = run_initialized_batch(&mut interpreter, &options, &actions)?;
            interpreter.release_external_resources_for_exit();
            crate::lisp::flush_batch_stdout();
            finish(outcome)
        })
    })?
}

pub(crate) fn with_batch_stack<R>(body: impl FnOnce() -> R) -> Result<R, String> {
    // Cons chains are released iteratively, so list length no longer dictates
    // stack capacity. Use the same guarded reservation as Lisp threads; Rust
    // evaluator frames still need more room than GNU's C frames. Keep GNU's
    // calling OS thread: an extra worker and join would introduce a blocking
    // futex that GNU's seccomp policy deliberately does not permit. Native
    // roots stay on this stack, and only touched pages commit.
    let stack = corosensei::stack::DefaultStack::new(128 * 1024 * 1024)
        .map_err(|error| format!("allocate batch stack: {error}"))?;
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crate::lisp::eval::continuations::on_stack(stack, body)
    }))
    .map_err(|_| "batch runtime panicked".to_string())
}

pub fn run_batch(options: BatchRunOptions) -> Result<BatchRunOutcome, String> {
    let actions = batch_actions(&options);
    run_batch_with_actions(options, actions)
}

fn batch_actions(options: &BatchRunOptions) -> Vec<BatchAction> {
    options
        .load
        .iter()
        .cloned()
        .map(BatchAction::Load)
        .chain(options.eval.iter().cloned().map(BatchAction::Eval))
        .chain(options.funcall.iter().cloned().map(BatchAction::Funcall))
        .collect()
}

pub fn run_batch_with_actions(
    options: BatchRunOptions,
    actions: Vec<BatchAction>,
) -> Result<BatchRunOutcome, String> {
    lisp::runtime::with_runtime(|| {
        let _symbols = crate::lisp::types::symbols::EditorSymbolScope::enter();
        let mut interpreter = initialize_batch_interpreter(&options)?;
        run_initialized_batch(&mut interpreter, &options, &actions)
    })
}

fn run_initialized_batch(
    interpreter: &mut Interpreter,
    options: &BatchRunOptions,
    actions: &[BatchAction],
) -> Result<BatchRunOutcome, String> {
    if let Some(termination) = interpreter.take_pending_termination() {
        return Ok(termination.into());
    }
    if let Some(command_line_args) = &options.startup_command_line_args {
        return run_batch_through_normal_top_level(interpreter, command_line_args);
    }
    // The compat helper's load-error reports record the harness-provided
    // selector; mirror GNU's `emaxx-compat--selector' environment contract.
    let selector_string =
        env::var("EMAXX_COMPAT_SELECTOR").unwrap_or_else(|_| "(quote t)".to_string());
    let mut eval_env: Env = crate::lisp::types::Env::new();
    for action in actions {
        match action {
            BatchAction::Load(target) => {
                let resolved = PathBuf::from(target);
                if let Err(error) = interpreter.load_target_with_env(target, &eval_env) {
                    if let LispErrorKind::Terminate(termination) = error.kind() {
                        return Ok(termination.clone().into());
                    }
                    let mut error_text = error.to_string();
                    let backtrace = format_backtrace_summary(interpreter);
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
                        interpreter,
                        &error,
                        &command_line_bottom_frames(actions, Some(&resolved)),
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
                    match interpreter
                        .materialize_read_object_literals(form, &mut eval_env)
                        .and_then(|form| interpreter.eval(&form, &mut eval_env))
                        .map_err(LispError::into_kind)
                    {
                        Ok(_) => {}
                        Err(LispErrorKind::Terminate(termination)) => return Ok(termination.into()),
                        Err(error) => {
                            emit_unhandled_batch_error(
                                interpreter,
                                &LispError::from(error.clone()),
                                &command_line_bottom_frames(actions, None),
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
                match interpreter
                    .eval(&form, &mut eval_env)
                    .map_err(LispError::into_kind)
                {
                    Ok(_) => {}
                    Err(LispErrorKind::Terminate(termination)) => return Ok(termination.into()),
                    Err(error) => {
                        emit_unhandled_batch_error(
                            interpreter,
                            &LispError::from(error.clone()),
                            &command_line_bottom_frames(actions, None),
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
        .forwarded_c_value("top-level", &crate::lisp::types::Env::new())
        .unwrap_or(Value::Nil);
    if form.is_nil() {
        let purify = interpreter
            .forwarded_c_value("purify-flag", &crate::lisp::types::Env::new())
            .unwrap_or(Value::Nil);
        return lisp::primitives::call(
            interpreter,
            "message",
            &[Value::string(if purify.is_nil() {
                "Bare Emacs (standard Lisp code not loaded)"
            } else {
                "Bare impure Emacs (standard Lisp code not loaded)"
            })],
            &mut crate::lisp::types::Env::new(),
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
    let result = lisp::primitives::call(
        interpreter,
        "eval",
        &[form, Value::T],
        &mut crate::lisp::types::Env::new(),
    );
    if let Some(handler) = handler {
        interpreter.pop_handler_bindings(handler);
    }
    result
}

fn run_batch_through_normal_top_level(
    interpreter: &mut Interpreter,
    command_line_args: &[String],
) -> Result<BatchRunOutcome, String> {
    match run_startup_top_level(interpreter, command_line_args).map_err(LispError::into_kind) {
        Ok(_) => {}
        Err(LispErrorKind::Terminate(termination)) => return Ok(termination.into()),
        Err(error) => {
            // keyboard.c top_level_1 -> cmd_error -> cmd_error_internal: in
            // batch, print_error_message to stderr and kill-emacs -1.  The
            // backtrace was already printed, to `standard-output', by the
            // `debug-early--handler' that top_level_2 bound around the
            // form (run_batch_toplevel_form), when
            // `backtrace-on-error-noninteractive' asked for it.
            emit_batch_error_message(interpreter, &LispError::from(error.clone()));
            // Fkill_emacs (make_fixnum (-1), Qnil): `kill-emacs-hook' and
            // shut_down_emacs run as for any other exit.
            match lisp::primitives::call(
                interpreter,
                "kill-emacs",
                &[Value::Integer(-1)],
                &mut crate::lisp::types::Env::new(),
            )
            .map_err(LispError::into_kind)
            {
                Err(LispErrorKind::Terminate(termination)) => return Ok(termination.into()),
                _ => return Ok(BatchRunOutcome::Exit(255)),
            }
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
    // if that primitive cannot run this early.  cmd_error sets
    // `print-level' and `print-length' to 10 around it, against circular
    // data.
    let message = lisp::primitives::with_cmd_error_print_limits(interpreter, |interpreter| {
        lisp::primitives::call(
            interpreter,
            "error-message-string",
            &[lisp::eval::error_condition_value(error)],
            &mut crate::lisp::types::Env::new(),
        )
    })
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
    let mut render_env: Env = crate::lisp::types::Env::new();
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
    // Dynamic bindings, as debug-early-backtrace's `let' of these special
    // variables makes them: the printer reads them as C reads its DEFVARs.
    let mut restores = Vec::new();
    for name in [
        "print-escape-newlines",
        "print-escape-control-characters",
        "print-escape-nonascii",
    ] {
        if let Ok(restore) = interpreter.bind_special_dynamic(name, Value::T, &mut render_env) {
            restores.push(restore);
        }
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
    for restore in restores.into_iter().rev() {
        let _ = interpreter.restore_special_dynamic(restore, &mut render_env);
    }
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
    options: &BatchRunOptions,
) -> Result<Interpreter, String> {
    finish_test_fixture(initialize_interpreter(options, false))
}

/// emacs.c:main after load_pdump: whether this process starts
/// `initialized', from an image (`--dump-file', or the executable's own
/// `<name>.pdmp' beside it).  A named image that cannot be loaded is
/// fatal there, as in GNU.
pub fn startup_image_loads(dump_file: Option<&Path>) -> bool {
    lisp::runtime::with_runtime(|| {
        let _symbols = crate::lisp::types::symbols::EditorSymbolScope::enter();
        let mut interpreter = Interpreter::new();
        crate::lisp::primitives::pdumper::load_pdump_at_startup(&mut interpreter, dump_file)
            .is_some()
    })
}

/// pdumper.c:dump_fingerprint with an empty label: the executable's
/// fingerprint as lowercase hex.
pub fn executable_fingerprint_hex() -> String {
    crate::lisp::primitives::pdumper::image::executable_fingerprint()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// emacs.c:main's `--version' report, produced after load_pdump and the
/// init_* calls: the dumped process reports its `emacs-version',
/// `emacs-copyright' and repository details; a process without an image
/// (temacs) reports the C constants.  An unusable variable is the
/// message GNU writes to stderr before exiting with status 1.
pub fn version_banner(dump_file: Option<&Path>) -> Result<String, String> {
    lisp::runtime::with_runtime(|| version_banner_owned(dump_file))
}

fn version_banner_owned(dump_file: Option<&Path>) -> Result<String, String> {
    let _symbols = crate::lisp::types::symbols::EditorSymbolScope::enter();
    let mut interpreter = Interpreter::new();
    let initialized =
        crate::lisp::primitives::pdumper::load_pdump_at_startup(&mut interpreter, dump_file)
            .is_some();
    let env: Env = crate::lisp::types::Env::new();
    let variable = |interpreter: &Interpreter, name: &str| {
        interpreter.lookup_var(name, &env).unwrap_or(Value::Nil)
    };
    let string_variable = |interpreter: &Interpreter, name: &str| -> Result<String, String> {
        lisp::primitives::string_like(&variable(interpreter, name))
            .map(|string| string.text)
            .ok_or_else(|| format!("Invalid value of '{name}'"))
    };
    let (version, copyright) = if initialized {
        (
            string_variable(&interpreter, "emacs-version")?,
            string_variable(&interpreter, "emacs-copyright")?,
        )
    } else {
        (
            lisp::primitives::emacs_version_value(),
            lisp::primitives::EMACS_COPYRIGHT.to_string(),
        )
    };
    let mut report = format!("GNU Emacs {version}\n");
    if initialized {
        let rversion = variable(&interpreter, "emacs-repository-version");
        let rbranch = variable(&interpreter, "emacs-repository-branch");
        let rtime = variable(&interpreter, "emacs-build-time");
        if !rversion.is_nil() && !rbranch.is_nil() && !rtime.is_nil() {
            let text = |interpreter: &mut Interpreter, function: &str, args: &[Value]| {
                lisp::primitives::call(
                    interpreter,
                    function,
                    args,
                    &mut crate::lisp::types::Env::new(),
                )
                .map_err(|error| error.to_string())
                .and_then(|value| {
                    lisp::primitives::string_like(&value)
                        .map(|string| string.text)
                        .ok_or_else(|| format!("{function} did not return a string"))
                })
            };
            let short = text(
                &mut interpreter,
                "substring",
                &[rversion, Value::Integer(0), Value::Integer(12)],
            )?;
            let branch = lisp::primitives::string_like(&rbranch)
                .map(|string| string.text)
                .unwrap_or_default();
            let date = text(
                &mut interpreter,
                "format-time-string",
                &[Value::string("%Y-%m-%d"), rtime, Value::Nil],
            )?;
            report.push_str(&format!(
                "Development version {short} on {branch} branch; build date {date}.\n"
            ));
        }
    }
    report.push_str(&format!(
        "{copyright}\n\
         GNU Emacs comes with ABSOLUTELY NO WARRANTY.\n\
         You may redistribute copies of GNU Emacs\n\
         under the terms of the GNU General Public License.\n\
         For more information about these matters, see the file named COPYING.\n"
    ));
    Ok(report)
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

/// A boot that has no image to start from reconstructs the dumped state
/// by running loadup.el, whose dump-mode path runs git
/// (`emacs-repository-get-version'): that crosses the host-process
/// boundary, which the test harness serializes with a permit every
/// process test takes before it boots.  Such a boot takes the permit
/// before the boot-environment guard too, so the two are always taken
/// in that order; whether this boot reconstructs is known only under the
/// guard, so the attempt reports it, the permit is taken, and the boot
/// restarts (taking the permit under the guard deadlocked against a
/// process test waiting to boot).
enum BootAttempt {
    Ready(Box<Interpreter>),
    #[cfg(test)]
    NeedsProcessPermit,
}

fn initialize_interpreter(
    options: &BatchRunOptions,
    noninteractive: bool,
) -> Result<Interpreter, String> {
    #[cfg(test)]
    loop {
        match initialize_interpreter_attempt(options, noninteractive)? {
            BootAttempt::Ready(interpreter) => return Ok(*interpreter),
            BootAttempt::NeedsProcessPermit => crate::test_support::mark_process_test(),
        }
    }
    #[cfg(not(test))]
    match initialize_interpreter_attempt(options, noninteractive)? {
        BootAttempt::Ready(interpreter) => Ok(*interpreter),
    }
}

fn initialize_interpreter_attempt(
    options: &BatchRunOptions,
    noninteractive: bool,
) -> Result<BootAttempt, String> {
    // Boot resolves its Lisp tree partly from process environment
    // (EMAXX_DUMP_SOURCE_DIRECTORY, EMACS_TEST_DIRECTORY).  Tests that
    // point those at fixture roots take this lock for WRITE; every boot
    // holds it for READ so a concurrent mutation cannot send this boot to
    // the fixture tree.  The "--test-threads=1" SAFETY comments this
    // replaces were wrong: the gate runs the suite in parallel, and a
    // sibling test's fixture env made random preloads fail with
    // "Cannot open load file".
    let _boot_environment = compat::boot_environment_read_guard();
    // emacs.c:main: load_pdump precedes every init_* call.  A process
    // with an image starts from it (`initialized') and never runs
    // loadup; without one it is temacs and reconstructs the dumped state
    // in the closure below.  The process values the constructor took
    // once are applied again over the image, as GNU's init_* are.
    // `--temacs=MODE' names the dumping process itself: it attempts no
    // image load (emacs.c's `attempt_load_pdump' stays false).  A process
    // about to load an image leaves out the syms_of_* definitions the
    // image's symbols carry, and starts again from the full state when
    // no image loads.
    let temacs = options.dump_mode.is_some();
    let mut interpreter = if temacs {
        Interpreter::new()
    } else {
        Interpreter::for_image()
    };
    let mut initialized = !temacs
        && crate::lisp::primitives::pdumper::load_pdump_at_startup(
            &mut interpreter,
            options.dump_file.as_deref(),
        )
        .is_some();
    // The harness's shared loadup image (`EMAXX_FIXTURE_IMAGE_DIR'): the
    // `emacs.pdmp' of this build, loaded when present, else dumped by
    // this process once its loadup state is built.
    let mut fixture_to_dump = None;
    if !temacs
        && !initialized
        && !options.no_loadup
        && options.dump_file.is_none()
        && let Some(fixture) = FixtureImage::acquire()?
    {
        use crate::lisp::primitives::pdumper::{PdumperLoadError, QuietDumpMessages, pdumper_load};
        let _quiet = QuietDumpMessages::hold();
        match pdumper_load(&fixture.path, &mut interpreter) {
            Ok(_) => initialized = true,
            Err(
                PdumperLoadError::FileNotFound
                | PdumperLoadError::BadFileType
                | PdumperLoadError::FailedDump
                | PdumperLoadError::VersionMismatch,
            ) => {
                let _ = fs::remove_file(&fixture.path);
                fixture_to_dump = Some(fixture);
            }
            Err(PdumperLoadError::Error(message)) => {
                return Err(format!(
                    "could not load the fixture image \"{}\": {message}",
                    fixture.path.display()
                ));
            }
        }
    }
    #[cfg(test)]
    if !initialized && !crate::test_support::holds_process_test_permit() {
        // The reconstruction runs git (see `BootAttempt'); the fixture
        // directory lock goes with the attempt.
        drop(fixture_to_dump);
        return Ok(BootAttempt::NeedsProcessPermit);
    }
    if !initialized && !temacs {
        interpreter = Interpreter::new();
    }
    if initialized {
        interpreter
            .init_after_pdump_load()
            .map_err(|error| format!("initialize restored process: {error}"))?;
    }
    interpreter.modules.assertions = options.module_assertions;
    let before_init_time =
        lisp::primitives::system_time_list_value(std::time::SystemTime::now())
            .map_err(|error| format!("record batch initialization start: {error}"))?;
    interpreter.define_special_variable("before-init-time", before_init_time);
    interpreter.define_special_variable("after-init-time", Value::Nil);
    let installation_load_path = installation_lisp_load_path()?;
    interpreter.set_load_path(installation_load_path.clone());
    // emacs.c: the eln load path is expanded only in a non-initialized
    // Emacs about to load Lisp (the `if (!initialized)' branch).
    if !initialized {
        configure_native_load_path_for_dump_reconstruction(&mut interpreter)?;
    }
    // GNU starts batch evaluation in *scratch*, whose buffer-local
    // `lexical-binding' is t while the default remains nil.  File cookies
    // override and restore this state around loads.
    interpreter.set_variable(
        "lexical-binding",
        Value::T,
        &mut crate::lisp::types::Env::new(),
    );
    interpreter.set_variable(
        "noninteractive",
        Value::T,
        &mut crate::lisp::types::Env::new(),
    );
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
        &mut crate::lisp::types::Env::new(),
    );
    // Loading the dumped Lisp owners below corresponds to GNU's pre-dump
    // phase, where delayed Custom initializers accumulate until startup;
    // an image carries the accumulated list.
    if !initialized {
        interpreter.set_variable(
            "custom-delayed-init-variables",
            Value::Nil,
            &mut crate::lisp::types::Env::new(),
        );
    }
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
        &mut crate::lisp::types::Env::new(),
    );

    // The reconstruction below is GNU's pre-dump build phase.  Its Loading
    // chatter and cus-start's "Note, built-in variable" messages belong to
    // the build log, never to a running session's stderr OR its *Messages*
    // buffer — a dumped GNU binary starts with an empty one (loaddefs.el's
    // own `(load "theme-loaddefs.el" t)' was leaking a Loading line there).
    let saved_message_log_max = interpreter
        .lookup_var("message-log-max", &crate::lisp::types::Env::new())
        .unwrap_or(Value::Nil);
    interpreter.set_variable(
        "message-log-max",
        Value::Nil,
        &mut crate::lisp::types::Env::new(),
    );
    interpreter.set_variable(
        "inhibit-message",
        Value::T,
        &mut crate::lisp::types::Env::new(),
    );
    let reconstruction = (|interpreter: &mut Interpreter| -> Result<(), String> {
        // An initialized process has the dumped state already.
        if initialized {
            return Ok(());
        }
        // The dumping process (`--temacs'), and the bare one (`-nl'):
        // emacs.c:main's uninitialized branch stores the top-level form
        // that loads loadup.el, which performs the dump itself.
        if temacs || options.no_loadup {
            prepare_temacs_top_level(interpreter, options);
            return Ok(());
        }
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
    interpreter.set_variable(
        "inhibit-message",
        Value::Nil,
        &mut crate::lisp::types::Env::new(),
    );
    interpreter.set_variable(
        "message-log-max",
        saved_message_log_max,
        &mut crate::lisp::types::Env::new(),
    );
    reconstruction?;
    // loadup.el's end: `(dump-emacs-portable "emacs.pdmp")' from the state
    // just built, for the processes and tests that follow.
    if let Some(fixture) = fixture_to_dump {
        fixture.dump(&mut interpreter)?;
    }
    // emacs.c uses 0.1 while temacs builds the dump, then raises the value to
    // 1.0 only when an initialized (dump-loaded) process starts in batch mode.
    // The reconstructed image above is the temacs/loadup phase. Apply the
    // new process's C values before live-session initialization and Lisp.
    interpreter.set_variable(
        "noninteractive",
        if noninteractive { Value::T } else { Value::Nil },
        &mut crate::lisp::types::Env::new(),
    );
    interpreter.set_variable(
        "gc-cons-percentage",
        Value::float(if noninteractive { 1.0 } else { 0.1 }),
        &mut crate::lisp::types::Env::new(),
    );
    // sysdep.c:init_system_name, from init_editfns in every process: this
    // host's name, or nil after `--no-build-details'; the image carries
    // the dumping host's.
    interpreter.set_global_binding("system-name", lisp::primitives::system_name_lisp_value());
    if noninteractive {
        interpreter.set_variable(
            "undo-outer-limit",
            Value::Nil,
            &mut crate::lisp::types::Env::new(),
        );
    }
    let dump_path = Value::list(installation_load_path.iter().map(|path| {
        lisp::primitives::bytes_to_shared_unibyte_value(path.as_os_str().as_encoded_bytes())
    }));
    crate::startup::initialize_load_path(&mut interpreter, dump_path, temacs, options.no_site_lisp)
        .map_err(|error| format!("initialize session load-path: {error}"))?;
    // emacs.c:init_display follows init_lread. Interactive initialization
    // waits for the terminal in tty.rs before invoking the same C-owned call.
    // dispnew.c:init_display makes it only for a process that started from
    // an image (`dumped_with_pdumper_p'); the dumping process and the bare
    // one have no faces.el yet.  The reconstruction above stands for both
    // phases and so makes the call too.
    if noninteractive && !(temacs || options.no_loadup) {
        initialize_initial_frame_faces(&mut interpreter)?;
    }
    // emacs.c:main after `initialized = true': "Allow code to be run
    // (mostly useful after redumping)."  Every process runs it, dumped or
    // not, before the top level. The interactive caller does this after
    // establishing terminal input and display, just as init_display does.
    if noninteractive {
        match safe_run_hooks(&mut interpreter, "after-pdump-load-hook")
            .map_err(LispError::into_kind)
        {
            Ok(()) => {}
            Err(LispErrorKind::Terminate(termination)) => {
                interpreter.request_termination(termination);
                return Ok(BootAttempt::Ready(Box::new(interpreter)));
            }
            Err(error) => return Err(format!("after-pdump-load-hook: {error}")),
        }
        // emacs.c:main, just before Frecursive_edit.  The interactive
        // caller makes the same call after its hook run.
        crate::lisp::primitives::set_initial_minibuffer_mode(&mut interpreter)
            .map_err(|error| format!("initial minibuffer mode: {error}"))?;
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
    Ok(BootAttempt::Ready(Box::new(interpreter)))
}

fn configure_native_load_path_for_dump_reconstruction(
    interpreter: &mut Interpreter,
) -> Result<(), String> {
    // emacs.c expands the temporary path installed by syms_of_comp against
    // invocation-directory when a non-dumped Emacs is about to load Lisp.
    // For Emaxx this is the writable build-tree cache under target/.
    let load_path = interpreter
        .lookup_var("native-comp-eln-load-path", &crate::lisp::types::Env::new())
        .ok_or_else(|| "native-comp-eln-load-path is unbound during startup".to_string())?;
    let (initial_directory, _) = load_path
        .cons_values()
        .ok_or_else(|| "native-comp-eln-load-path has no initial entry".to_string())?;
    let initial_directory = lisp::primitives::string_like(&initial_directory)
        .ok_or_else(|| "native-comp-eln-load-path initial entry is not a string".to_string())?
        .text;
    let invocation_directory = interpreter
        .lookup_var("invocation-directory", &crate::lisp::types::Env::new())
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
        .lookup_var("source-directory", &crate::lisp::types::Env::new())
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

/// keyboard.c:safe_run_hooks: inhibit quits while calling each function,
/// demote signals to messages, and remove the failing function by identity.
/// Throws and process termination still leave the hook.
pub(crate) fn safe_run_hooks(interpreter: &mut Interpreter, hook: &str) -> Result<(), LispError> {
    safe_run_hooks_with_args(interpreter, hook, &[])
}

/// keyboard.c:safe_run_hooks_2 and the like: `safe_run_hooks' calling
/// each function with ARGS.
pub(crate) fn safe_run_hooks_with_args(
    interpreter: &mut Interpreter,
    hook: &str,
    args: &[Value],
) -> Result<(), LispError> {
    let mut env = crate::lisp::types::Env::new();
    let binding = interpreter.bind_special_dynamic("inhibit-quit", Value::T, &mut env)?;
    let result = {
        // GNU's Vrun_hooks is an internal initialization flag, not the
        // value cell of the Lisp run-hooks symbol. This constructor has
        // already installed the primitive definitions before calling us.
        let value = interpreter.lookup_var(hook, &env).unwrap_or(Value::Nil);
        run_safe_hook_value(interpreter, hook, value, false, args, &mut env)
    };
    let restored = interpreter.restore_special_dynamic(binding, &mut env);
    result.and(restored)
}

fn run_safe_hook_value(
    interpreter: &mut Interpreter,
    hook: &str,
    value: Value,
    global: bool,
    args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    if value.is_nil() || matches!(value.kind(), Kind::Unbound) {
        return Ok(());
    }
    let function = !value.is_cons()
        || lisp::primitives::call(interpreter, "functionp", std::slice::from_ref(&value), env)?
            .is_truthy();
    if function {
        return call_safe_hook_function(interpreter, hook, &value, args, env);
    }
    // Follow the original cons cells, including mutations made by a callback.
    // A local t reads the global value at that point, after earlier callbacks.
    let mut tail = value;
    while tail.is_cons() {
        interpreter.with_lisp_stack_roots(&tail, |interpreter| {
            let function = tail.car()?;
            if matches!(function.kind(), Kind::T) {
                if !global {
                    let default = interpreter.default_value(hook).unwrap_or(Value::Nil);
                    run_safe_hook_value(interpreter, hook, default, true, args, env)?;
                }
            } else {
                call_safe_hook_function(interpreter, hook, &function, args, env)?;
            }
            Ok::<_, LispError>(())
        })?;
        tail = tail.cdr()?;
    }
    Ok(())
}

fn call_safe_hook_function(
    interpreter: &mut Interpreter,
    hook: &str,
    function: &Value,
    args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    interpreter.with_lisp_stack_roots(function, |interpreter| {
        // internal_condition_case_n establishes its catch before funcall:
        // outer handler-bind handlers must not observe a handled signal.
        let handlers = interpreter.push_condition_case_handler(vec![Value::T]);
        let depth = env.len();
        let result = interpreter.call_function_value(*function, None, args, env);
        interpreter.pop_handler_bindings(handlers);
        env.truncate(depth);
        let error = match result.map_err(LispError::into_kind) {
            Ok(_) => return Ok(()),
            Err(error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_))) => {
                return Err(LispError::from(error));
            }
            Err(error) => error,
        };
        interpreter.clear_batch_error_backtrace();
        let condition = lisp::eval::error_condition_value(&LispError::from(error.clone()));
        lisp::primitives::call(
            interpreter,
            "message",
            &[
                Value::string("Error in %s (%S): %S"),
                Value::symbol(hook),
                *function,
                condition,
            ],
            env,
        )?;
        // keyboard.c:safe_run_hooks_error searches the current value first,
        // then the default, and uses EQ rather than structural equality.
        let without_function = |interpreter: &Interpreter, value: Value| -> Option<Value> {
            let mut tail = value;
            let mut found = false;
            let mut kept = Vec::new();
            while tail.is_cons() {
                let entry = tail.car_if_list()?;
                if lisp::primitives::values::values_eq_in_env(interpreter, &entry, function, env) {
                    found = true;
                } else {
                    kept.push(entry);
                }
                tail = tail.cdr_if_list()?;
            }
            found.then(|| Value::list(kept))
        };
        let local = interpreter.lookup_var(hook, env).unwrap_or(Value::Nil);
        if let Some(rest) = without_function(interpreter, local) {
            lisp::primitives::call(interpreter, "set", &[Value::symbol(hook), rest], env)?;
        } else {
            let global = interpreter.default_value(hook).unwrap_or(Value::Nil);
            if let Some(rest) = without_function(interpreter, global) {
                lisp::primitives::call(
                    interpreter,
                    "set-default",
                    &[Value::symbol(hook), rest],
                    env,
                )?;
            }
        }
        Ok(())
    })
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
        .lookup_function(
            "tty-set-up-initial-frame-faces",
            &crate::lisp::types::Env::new(),
        )
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
            &mut crate::lisp::types::Env::new(),
        )
        .map_err(|error| format!("initialize initial-frame faces: {error}"))?;
    Ok(())
}

/// The loadup image the test harness shares between its processes and
/// tests (`EMAXX_FIXTURE_IMAGE_DIR'): what GNU's `temacs --batch -l loadup
/// pdump' leaves beside the executable as `emacs.pdmp'.  The first boot
/// that finds none builds the loadup state and dumps it there; every
/// later boot starts from it, 1 s instead of the reconstruction's tens
/// of seconds.  The file is named by this build's fingerprint and the
/// installation Lisp tree the state is built from, the loader's
/// fingerprint check refuses another build's image, and an exclusive
/// lock on the directory serializes the processes that check and build
/// it.  Not GNU's: loadup.el performs the dump itself; here the startup
/// performs it after the reconstruction, only under the harness's
/// variable.
struct FixtureImage {
    path: PathBuf,
    _lock: fs::File,
}

impl FixtureImage {
    fn acquire() -> Result<Option<Self>, String> {
        let Some(directory) = env::var_os("EMAXX_FIXTURE_IMAGE_DIR") else {
            return Ok(None);
        };
        let directory = PathBuf::from(directory);
        fs::create_dir_all(&directory)
            .map_err(|error| format!("create {}: {error}", directory.display()))?;
        let hex = |bytes: &[u8]| {
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let fingerprint =
            hex(&crate::lisp::primitives::pdumper::image::executable_fingerprint()[..4]);
        let tree = {
            use sha2::Digest;
            let mut hasher = sha2::Sha256::new();
            for path in installation_lisp_load_path()? {
                hasher.update(path.as_os_str().as_encoded_bytes());
                hasher.update(b"\0");
            }
            hex(&hasher.finalize()[..4])
        };
        let path = directory.join(format!("loadup-{fingerprint}-{tree}.pdmp"));
        let lock_path = directory.join(format!("loadup-{fingerprint}-{tree}.lock"));
        let lock = fs::File::create(&lock_path)
            .map_err(|error| format!("create {}: {error}", lock_path.display()))?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: a blocking exclusive advisory lock on an open file.
            if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX) } != 0 {
                return Err(format!(
                    "lock {}: {}",
                    lock_path.display(),
                    std::io::Error::last_os_error()
                ));
            }
        }
        Ok(Some(Self { path, _lock: lock }))
    }

    /// `(dump-emacs-portable FILE)' to a temporary name, then the rename:
    /// no process ever sees a partial image under the final name.
    fn dump(self, interpreter: &mut Interpreter) -> Result<(), String> {
        let temporary = self.path.with_extension("pdmp.tmp");
        let _ = fs::remove_file(&temporary);
        let _quiet = lisp::primitives::pdumper::QuietDumpMessages::hold();
        // src/Makefile.in runs loadup with `--bin-dest $(BIN_DESTDIR)
        // --eln-dest $(ELN_DESTDIR)', from which loadup.el sets these two
        // variables so that `load--fixup-all-elns' (called by
        // dump-emacs-portable) records each loaded native unit's file as
        // the pair of paths relative to the executable's directory that
        // the loader resolves again.  Emaxx has no installation layout:
        // the binary's own directory stands for bindir, and the source
        // tree, whose `native-lisp/' holds the preloaded units, for the
        // directory the installed `native-lisp/' would be under.
        let invocation_directory = interpreter
            .lookup_var("invocation-directory", &crate::lisp::types::Env::new())
            .unwrap_or(Value::Nil);
        let source_directory = interpreter
            .lookup_var("source-directory", &crate::lisp::types::Env::new())
            .unwrap_or(Value::Nil);
        interpreter.set_global_binding("load--bin-dest-dir", invocation_directory);
        interpreter.set_global_binding("load--eln-dest-dir", source_directory);
        lisp::primitives::call(
            interpreter,
            "dump-emacs-portable",
            &[Value::String(temporary.display().to_string().into())],
            &mut crate::lisp::types::Env::new(),
        )
        .map_err(|error| format!("dump the fixture image: {error}"))?;
        fs::rename(&temporary, &self.path).map_err(|error| {
            format!(
                "rename {} to {}: {error}",
                temporary.display(),
                self.path.display()
            )
        })
    }
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
    let sibling = compat::configured_gnu_source_root();
    if crate::file_system::is_directory(sibling.join("lisp")) {
        return compat::canonicalize_path(&sibling.join("lisp")).map(|path| vec![path]);
    }
    Ok(Vec::new())
}

fn has_configured_lisp_tree(interpreter: &Interpreter) -> bool {
    interpreter
        .lookup_var("load-path", &crate::lisp::types::Env::new())
        .and_then(|value| value.to_vec().ok())
        .is_some_and(|paths| !paths.is_empty())
}

/// emacs.c:main for a process that is not `initialized': `dump-mode'
/// is the `--temacs' mode, `purify-flag' is t (alloc.c's initial value),
/// and `top-level' is the `-l FILE' load, then the load of loadup.el
/// unless `-nl'.  loadup.el runs as the top-level form: with a dump mode
/// it dumps the image where the Makefile's `--bin-dest' and `--eln-dest'
/// (read from `command-line-args' by loadup.el itself) say, and kills
/// Emacs; without one its end evaluates `top-level' for the session.
fn prepare_temacs_top_level(interpreter: &mut Interpreter, options: &BatchRunOptions) {
    if let Some(mode) = &options.dump_mode {
        interpreter.define_special_variable("dump-mode", Value::string(mode));
    }
    interpreter.define_special_variable("purify-flag", Value::T);
    let mut top_level = Value::Nil;
    if let Some(file) = &options.temacs_load {
        top_level = Value::list([
            Value::symbol("load"),
            lisp::primitives::bytes_to_shared_unibyte_value(file.as_bytes()),
        ]);
    }
    if !options.no_loadup {
        top_level = Value::list([Value::symbol("load"), Value::string("loadup.el")]);
    }
    interpreter.set_global_binding("top-level", top_level);
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
    // temacs writes the image where loadup.el calls dump-emacs-portable;
    // this process, which has no image to load, reconstructs the state by
    // running the same file and hands off at that call instead.
    interpreter.image_reconstruction_handoff = true;
    let loaded = crate::lisp::load_file_strict_until_or_error(
        interpreter,
        &path,
        |_| false,
        |error| {
            matches!(
                error.kind(),
                LispErrorKind::Signal(message)
                    if message == crate::lisp::primitives::PORTABLE_DUMPER_UNAVAILABLE
            )
        },
    );
    interpreter.image_reconstruction_handoff = false;
    let stopped = match loaded {
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
            let name = match function.kind() {
                Kind::Nil => "<anonymous>".into(),
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
    #[test]
    fn batch_stack_preserves_the_calling_thread_and_unwinds_back_to_it() {
        thread_local! {
            static CALLER_VALUE: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
        }
        let thread = std::thread::current().id();
        CALLER_VALUE.set(41);
        let result = super::with_batch_stack(|| {
            assert_eq!(std::thread::current().id(), thread);
            assert_eq!(CALLER_VALUE.get(), 41);
            CALLER_VALUE.set(42);
            "finished"
        })
        .expect("run on the batch stack");
        assert_eq!(result, "finished");
        assert_eq!(CALLER_VALUE.get(), 42);
        assert!(super::with_batch_stack(|| panic!("unwind control")).is_err());
        assert_eq!(std::thread::current().id(), thread);
        assert_eq!(CALLER_VALUE.get(), 42);
    }

    #[test]
    fn safe_startup_hooks_inhibit_quit_and_observe_live_global_values() {
        // keyboard.c:safe_run_hooks specbinds inhibit-quit, and
        // eval.c:run_hook_with_args reads the default only on reaching t.
        let mut interpreter = Interpreter::new();
        let mut env = crate::lisp::types::Env::new();
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            r#"(progn
                 (setq zz-seen nil)
                 (fset 'zz-before #'(lambda ()
                     (setq zz-seen (cons inhibit-quit zz-seen))
                     (set-default 'zz-hook '(zz-after))))
                 (fset 'zz-after #'(lambda () (setq zz-seen (cons 'after zz-seen))))
                 (set-default 'zz-hook '(zz-unused))
                 (make-local-variable 'zz-hook)
                 (setq zz-hook '(zz-before t)))"#,
        )
        .expect("set up the live global hook");
        super::safe_run_hooks(&mut interpreter, "zz-hook").expect("run the hook");
        let result = crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            "(list zz-seen zz-hook (default-value 'zz-hook) inhibit-quit)",
        )
        .expect("observe the hook state");
        assert_eq!(
            result.to_string(),
            "((after t) (zz-before t) (zz-after) nil)"
        );
    }

    #[test]
    fn safe_startup_hooks_preserve_nonlocal_exits_and_restore_inhibit_quit() {
        let mut interpreter = Interpreter::new();
        let mut env = crate::lisp::types::Env::new();
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            "(setq zz-hook (list #'(lambda () (throw 'stop 7))))",
        )
        .expect("a throwing hook");
        // With no active catch GNU signals no-catch, which a safe hook
        // handles like any other signal. Register the enclosing catch so
        // this exercises a nonlocal exit, not that error path.
        interpreter.push_catch_tag(Value::symbol("stop"));
        let result = super::safe_run_hooks(&mut interpreter, "zz-hook");
        interpreter.pop_catch_tag();
        assert!(matches!(result.as_ref().map_err(LispError::kind),
            Err(LispErrorKind::Throw(tag, value)) if *value == Value::Integer(7) && tag.as_symbol().is_ok_and(|name| name == "stop")));
        assert_eq!(
            interpreter.lookup_var("inhibit-quit", &env),
            Some(Value::Nil)
        );
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            "(setq zz-hook (list #'(lambda () (kill-emacs 7))))",
        )
        .expect("a terminating hook");
        assert!(
            matches!(super::safe_run_hooks(&mut interpreter, "zz-hook").map_err(LispError::into_kind),
            Err(LispErrorKind::Terminate(termination)) if termination.exit_code == 7)
        );
        assert_eq!(
            interpreter.lookup_var("inhibit-quit", &env),
            Some(Value::Nil)
        );
    }

    #[test]
    fn safe_run_hooks_reports_and_removes_a_failing_function_as_keyboard_c_does() {
        // keyboard.c:safe_run_hooks: every function runs, a signalling one is
        // reported with `message' and removed from the hook (the local
        // value first, then the global), the rest keep running.
        let mut interpreter = Interpreter::new();
        let mut env = crate::lisp::types::Env::new();
        let program = r#"(progn
             (fset 'zz-ok #'(lambda () (setq zz-ran t)))
             (fset 'zz-bad #'(lambda () (signal 'error '("boom"))))
             (fset 'zz-ok2 #'(lambda () (setq zz-ran2 t)))
             (fset 'zz-outer #'(lambda (_error) (setq zz-outer-ran t)))
             (setq zz-outer-ran nil)
             (setq zz-ran nil zz-ran2 nil)
             (set-default 'zz-hook '(zz-bad zz-ok2))
             (make-local-variable 'zz-hook)
             (setq zz-hook '(zz-ok t)))"#;
        crate::test_support::eval_lisp(&mut interpreter, &mut env, program).expect("set up");
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            "(progn (setq zz-watcher-seen nil) \
             (add-variable-watcher 'zz-hook \
               #'(lambda (_symbol value operation where) \
                 (setq zz-watcher-seen (list value operation where)))))",
        )
        .expect("watch the hook removal");
        let handlers =
            interpreter.push_handler_bindings(&[(vec!["error".into()], Value::symbol("zz-outer"))]);
        super::safe_run_hooks(&mut interpreter, "zz-hook").expect("run the local and global hook");
        interpreter.pop_handler_bindings(handlers);
        let watched = interpreter
            .lookup_var("zz-watcher-seen", &env)
            .expect("watcher observation");
        assert_eq!(watched.to_string(), "((zz-ok2) set nil)");
        assert_eq!(
            interpreter.lookup_var("zz-outer-ran", &env),
            Some(Value::Nil)
        );
        let state = crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            r#"(list zz-ran zz-ran2 zz-hook (default-value 'zz-hook)
                     (save-current-buffer (set-buffer "*Messages*") (buffer-string)))"#,
        )
        .expect("read the state");
        let printed =
            lisp::primitives::call(&mut interpreter, "prin1-to-string", &[state], &mut env)
                .expect("print");
        assert_eq!(
            lisp::primitives::string_like(&printed)
                .expect("printed")
                .text,
            "(t t (zz-ok t) (zz-ok2) \"Error in zz-hook (zz-bad): (error \\\"boom\\\")\n\")"
        );
        // A void hook and a single-function value.
        super::safe_run_hooks(&mut interpreter, "zz-no-such-hook").expect("void hook");
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            "(progn (setq zz-ran nil) (setq zz-hook 'zz-ok))",
        )
        .expect("single function");
        super::safe_run_hooks(&mut interpreter, "zz-hook").expect("single-function hook");
        assert_eq!(interpreter.lookup_var("zz-ran", &env), Some(Value::T));
    }

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
        let interactive = initialize_interactive_interpreter(&BatchRunOptions {
            no_site_lisp: true,
            defer_delayed_custom_init: true,
            ..Default::default()
        })
        .expect("prepare interactive session");
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
    fn batch_evaluates_perf_named_forms_as_ordinary_lisp() {
        // A benchmark's spelling must not select a second evaluator or
        // bypass its Lisp definition, including through the embedded API.
        let mut interpreter = Interpreter::new();
        let actions = vec![
            BatchAction::Eval(
                "(defalias 'emaxx-perf-run-batch
                   #'(lambda (&rest args) (setq perf-call-arguments args)))"
                    .into(),
            ),
            BatchAction::Eval("(emaxx-perf-run-batch \"arbitrary/workload\" 37 2 9)".into()),
        ];
        assert_eq!(
            run_initialized_batch(&mut interpreter, &BatchRunOptions::default(), &actions)
                .expect("ordinary Lisp evaluation"),
            BatchRunOutcome::Exit(0)
        );
        assert_eq!(
            interpreter
                .lookup_var("perf-call-arguments", &Env::new())
                .expect("the function body executed")
                .to_string(),
            "(\"arbitrary/workload\" 37 2 9)"
        );
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
        let upstream = compat::configured_gnu_source_root();
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
            interpreter.lookup_var("command-line-args-left", &crate::lisp::types::Env::new()),
            Some(Value::Nil)
        );
    }

    #[test]
    fn dump_reconstruction_has_cache_then_gnu_system_native_paths() {
        let mut interpreter = Interpreter::new();
        let invocation_directory = interpreter
            .lookup_var("invocation-directory", &crate::lisp::types::Env::new())
            .and_then(|value| lisp::primitives::string_like(&value))
            .expect("invocation-directory string")
            .text;
        let expected_cache =
            lisp::primitives::expand_file_name("../native-lisp/", Some(&invocation_directory));
        let source_directory = interpreter
            .lookup_var("source-directory", &crate::lisp::types::Env::new())
            .and_then(|value| lisp::primitives::string_like(&value))
            .expect("source-directory string")
            .text;
        let expected_system =
            lisp::primitives::expand_file_name("native-lisp/", Some(&source_directory));
        configure_native_load_path_for_dump_reconstruction(&mut interpreter)
            .expect("configure native paths for GNU dump reconstruction");
        let load_path = interpreter
            .lookup_var("native-comp-eln-load-path", &crate::lisp::types::Env::new())
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
                .lookup_var(name, &crate::lisp::types::Env::new())
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
                .eval(&ordered, &mut crate::lisp::types::Env::new())
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
                .eval(&located, &mut crate::lisp::types::Env::new())
                .expect("locate standard Lisp fixture"),
            Value::String(runtime_lisp.display().to_string().into())
        );

        let history = interpreter
            .lookup_var("load-history", &crate::lisp::types::Env::new())
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
                .eval(&form, &mut crate::lisp::types::Env::new())
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
            interpreter
                .lookup_function("when", &crate::lisp::types::Env::new())
                .is_ok(),
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
            let mut env = crate::lisp::types::Env::new();
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
    fn named_character_escapes_read_as_char_from_name_answers() {
        // lread.c character_name_to_code: a name is the code
        // `(char-from-name NAME t)' gives, so the reader takes exactly the
        // names `ucs-names' and the numbered ideographs know, case ignored.
        // The first name builds `ucs-names', which collects while the
        // strings read before it are held by the read alone.
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
                r#"(prin1-to-string
                    (list (read "(\"abc\" (1 . \"x\") ?\\N{GREEK SMALL LETTER BETA} \"def\")")
                          (condition-case err (read "?\\N{CJK UNIFIED IDEOGRAPH-4E2D}")
                            (invalid-read-syntax err))
                          (read "?\\N{greek small letter alpha}")
                          (read "\"\\N{LINE FEED (LF)}\"")
                          (condition-case err (read "?\\N{LINE FEED}")
                            (invalid-read-syntax (cadr err)))
                          (read "?\\N{TANGUT IDEOGRAPH-17000}")
                          (read "?\\N{U+1F600}")))"#,
            )
            .read_all()
            .expect("read named-character probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate named-character probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                "((\"abc\" (1 . \"x\") 946 \"def\") \
                 (invalid-read-syntax \"\\\\N{CJK UNIFIED IDEOGRAPH-4E2D}\") 945 \"\n\" \
                 \"\\\\N{LINE FEED}\" 94208 128512)"
            );
        });
    }

    #[test]
    fn assoc_string_compares_each_string_or_symbol_element_as_minibuf_c() {
        // minibuf.c Fassoc_string: the walk stops at the first tail that
        // is not a cons, passes over elements whose car is neither a string
        // nor a symbol, and asks `compare-strings' of the rest, which
        // upcases only a differing pair of characters.
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
                r#"(prin1-to-string
                    (list (assoc-string 'foo '(bar ("foo" . 1) baz))
                          (assoc-string "FOO" '(1 (2 . 3) "bar" "foo") t)
                          (assoc-string "foo" '("a" "b" . "foo"))
                          (assoc-string "foo" ["foo"])
                          (assoc-string 7 '(1 2))
                          (condition-case err (assoc-string 7 '("a")) (error err))
                          (assoc-string "ß" '("SS" "ẞ") t)
                          (assoc-string "ς" '("Σ") t)
                          (assoc-string "ǅ" '("ǆ") t)
                          (assoc-string "ﬀ" '("FF" "ff") t)
                          (assoc-string "a-b" '("A-B") t)))"#,
            )
            .read_all()
            .expect("read assoc-string probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate assoc-string probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                "((\"foo\" . 1) \"foo\" nil nil nil (wrong-type-argument stringp 7) \
                 \"ẞ\" \"Σ\" \"ǆ\" nil \"A-B\")"
            );
        });
    }

    #[test]
    fn file_name_handlers_and_face_aliases_follow_fileio_c_and_xfaces_c() {
        // fileio.c Ffind_file_name_handler: the latest match among the
        // entries whose handler's `operations' lists the operation (Fmemq,
        // which signals on a dotted list), the inhibited handlers passed
        // over, case never folded.  xfaces.c resolve_face_name: an alias
        // chain that comes round again signals `circular-list'.
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
                r#"(prin1-to-string
                    (progn
                      (defun r101-h1 (&rest _) 'h1)
                      (defun r101-h2 (&rest _) 'h2)
                      (put 'r101-h2 'operations '(file-exists-p load))
                      (put 'r101-c 'face-alias 'r101-d)
                      (put 'r101-d 'face-alias 'r101-c)
                      (let ((file-name-handler-alist
                             '(("\\.foo\\'" . r101-h1) ("foo" . r101-h2) (7 . r101-h1))))
                        (list (find-file-name-handler "/a/b.foo" 'load)
                              (find-file-name-handler "/a/b.foo" 'expand-file-name)
                              (find-file-name-handler "/a/foo.c" 'expand-file-name)
                              (let ((inhibit-file-name-operation 'load)
                                    (inhibit-file-name-handlers '(r101-h2)))
                                (find-file-name-handler "/a/foo.c" 'load))
                              (let ((case-fold-search t))
                                (find-file-name-handler "/A/B.FOO" 'load))
                              (progn (put 'r101-h2 'operations '(load . bad))
                                     (condition-case err
                                         (find-file-name-handler "/a/foo.c" 'expand-file-name)
                                       (error err)))
                              (condition-case err (internal-lisp-face-p 'r101-c)
                                (error err))))))"#,
            )
            .read_all()
            .expect("read file-name-handler probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate file-name-handler probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                "(r101-h2 r101-h1 nil nil nil (wrong-type-argument listp (load . bad)) \
                 (circular-list r101-c))"
            );
        });
    }

    #[test]
    fn bool_vectors_keep_their_bits_and_signal_as_data_c() {
        // lisp.h's Lisp_Bool_Vector: the bits packed in words, a large one
        // across a collection; data.c's checks (Faset's range,
        // wrong_length_argument of the lengths, CHECK_FIXNAT), Fbool_vector_not
        // into its destination, fns.c:bool_vector_cmp, and an image's
        // category set read where the image holds it.
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
                r##"(prin1-to-string
                    (let* ((a (bool-vector t nil t t nil))
                           (d (make-bool-vector 5 nil))
                           (big (make-bool-vector 100001 nil)))
                      (aset big 100000 t)
                      (garbage-collect)
                      (list (condition-case e (aset a 5 t) (error e))
                            (bool-vector-count-consecutive a t 2)
                            (condition-case e (bool-vector-count-consecutive a t 6)
                              (error (car e)))
                            (condition-case e (bool-vector-union a (make-bool-vector 0 nil))
                              (error e))
                            (condition-case e (bool-vector-subsetp (make-bool-vector 3 nil)
                                                                   (make-bool-vector 4 nil))
                              (error e))
                            (progn (bool-vector-not a d) (append d nil))
                            (condition-case e (make-bool-vector -1 nil) (error e))
                            (bool-vector-count-population big)
                            (aref big 100000)
                            (value< (bool-vector t nil t) (bool-vector t))
                            (value< (bool-vector t) (bool-vector t nil t))
                            (category-set-mnemonics (char-category-set ?a))
                            (equal (read "#&5\"\\r\"") a)
                            (type-of a))))"##,
            )
            .read_all()
            .expect("read bool vector probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate bool vector probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                "((args-out-of-range #&5\"\r\" 5) 2 args-out-of-range \
                 (wrong-length-argument 5 0) (wrong-length-argument 3 4 4) \
                 (nil t nil nil t) (wrong-type-argument wholenump -1) 1 t nil t \".Lalr\" t \
                 bool-vector)"
            );
        });
    }

    #[test]
    fn sxhash_is_fns_c_sxhash_obj() {
        // GNU 30.2's values for objects whose hash holds no address:
        // fns.c's hash_string, sxhash_list, sxhash_vector over the size
        // word, sxhash_bool_vector, sxhash_bignum and sxhash_float, the
        // depth and length bounds, hash_interval over a string's
        // intervals (an emptied one included, which `object-intervals'
        // lists), and SXHASH_REDUCE.
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
                r#"(prin1-to-string
                    (list (sxhash-equal "abc") (sxhash-equal "") (sxhash-equal "abcdefghijklmnop")
                          (sxhash-equal "été") (sxhash-equal (string-to-multibyte "\351"))
                          (sxhash-equal 5) (sxhash-equal -5) (sxhash-equal most-positive-fixnum)
                          (sxhash-equal t) (sxhash-eq 5) (sxhash-eq -5) (sxhash-eq t)
                          (sxhash-equal -0.0) (sxhash-equal 1.5) (sxhash-eql 1.5)
                          (sxhash-equal (expt 2 70)) (sxhash-equal (- (expt 2 70)))
                          (sxhash-eql (expt 2 70)) (sxhash-equal (1+ most-positive-fixnum))
                          (sxhash-equal (list 1 2)) (sxhash-equal (make-list 10 1))
                          (sxhash-equal (list 1 (list 2 (list 3 (list 4 (list 5))))))
                          (sxhash-equal (let ((l (list 1 2))) (setcdr (cdr l) l) l))
                          (sxhash-equal [1 2]) (sxhash-equal (make-vector 10 1))
                          (sxhash-equal (vector 1 (vector 2 (vector 3 (vector 4)))))
                          (sxhash-equal (list "a" 1.0 [2 "b"]))
                          (sxhash-equal (record nil 1 2)) (sxhash-equal (make-char-table nil))
                          (sxhash-equal (make-byte-code 0 "" [] 0))
                          (sxhash-equal (bool-vector t)) (sxhash-equal (make-bool-vector 200 t))
                          (sxhash-equal (make-marker))
                          (sxhash-equal-including-properties (propertize "abc" 1 2))
                          (sxhash-equal-including-properties (concat "a" (propertize "bc" 1 2)))
                          (sxhash-equal-including-properties
                           (concat (propertize "a" 1 2 3 4) "b" (propertize "c" 1 2)))
                          (let ((s (propertize "abc" 1 2)))
                            (remove-text-properties 0 3 (list 1 nil) s)
                            (list (object-intervals s) (sxhash-equal-including-properties s)))
                          (sxhash-equal-including-properties (list (propertize "abc" 1 2)))))"#,
            )
            .read_all()
            .expect("read sxhash probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate sxhash probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                "(8059383 0 -1621414601513750798 1046521033479 33689 \
                 4 -1152921504606846971 1729382256910270464 15 3 -1152921504606846974 15 \
                 -2305843009213693952 -1151232654746583040 -1151232654746583040 \
                 80 272 80 -1729382256910270464 \
                 22 357914281 120 1410602000 662 2304070997 2028 54043195528474480 \
                 2920577775894 180144007995719680 27212913135616 21 16126996 0 \
                 33011235922 135214022082942 553836364246106794 (((0 3 nil)) 33011235900) 8059383)"
            );
        });
    }

    #[test]
    fn a_list_headed_by_vector_literal_is_a_list() {
        // The symbol `vector-literal' is Lisp data like any other: a list
        // that starts with it stays a list through the primitives that
        // build lists, and vectors are made and copied as vectors.
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
                "(prin1-to-string
                  (list (copy-sequence (list 'vector-literal 1))
                        (reverse (list 1 'vector-literal))
                        (elt (list 'vector-literal 'a 'b) 1)
                        (nreverse (list 2 'vector-literal))
                        (append (list 'vector-literal) nil)
                        (copy-sequence [1 2]) (reverse [1 2 3]) (sort [3 1 2] #'<)
                        (vconcat (list 1 2) [3]) (make-vector 2 'x)
                        (vector 'vector-literal 1) (remove 1 [1 2 1])
                        (seq-into (list 1 2) 'vector) (mapcar #'identity [4 5])
                        (delete-dups (list 'vector-literal 1 'vector-literal))))",
            )
            .read_all()
            .expect("read vector-literal probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate vector-literal probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                "((vector-literal 1) (vector-literal 1) a (vector-literal 2) (vector-literal) \
                 [1 2] [3 2 1] [1 2 3] [1 2 3] [x x] [vector-literal 1] [2] [1 2] (4 5) \
                 (vector-literal 1))"
            );
        });
    }

    #[test]
    fn lisp_faces_are_xfaces_c_tables_and_checks() {
        // xfaces.c: face--new-frame-defaults and the frame's face table are
        // the faces' only storage (a frame-local face made by setting an
        // attribute takes a global id too), an unspecified value for new
        // frames is :ignore-defface, and Finternal_set_lisp_face_attribute's
        // checks signal with the value; a tty stores no font.
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
                r#"(prin1-to-string
                 (progn
                   (internal-make-lisp-face 'zz)
                   (list
                    (internal-lisp-face-p 'zz-nosuch)
                    (condition-case e (internal-get-lisp-face-attribute 'zz-nosuch :weight) (error e))
                    (let ((n (hash-table-count face--new-frame-defaults)))
                      (internal-set-lisp-face-attribute 'zz-frame-first :weight 'bold (selected-frame))
                      (list (- (hash-table-count face--new-frame-defaults) n)
                            (integerp (get 'zz-frame-first 'face))
                            (internal-get-lisp-face-attribute 'zz-frame-first :weight t)))
                    (progn (internal-set-lisp-face-attribute 'zz :weight 'unspecified t)
                           (list (aref (internal-lisp-face-p 'zz) 5)
                                 (internal-get-lisp-face-attribute 'zz :weight t)))
                    (condition-case e (internal-set-lisp-face-attribute 'zz :underline '(:style bogus) t) (error e))
                    (condition-case e (internal-set-lisp-face-attribute 'zz :box '(:line-width 0) t) (error e))
                    (condition-case e (internal-set-lisp-face-attribute 'zz :height 0.01 t) (error e))
                    (condition-case e (internal-set-lisp-face-attribute 'default :height 1.5 t) (error e))
                    (condition-case e (internal-set-lisp-face-attribute 'zz :foundry "" t) (error e))
                    (condition-case e (internal-set-lisp-face-attribute 'zz :weight 5 t) (error e))
                    (condition-case e (internal-set-lisp-face-attribute 'zz :extend 'foo t) (error e))
                    (condition-case e (internal-set-lisp-face-attribute 'zz :inherit '(bold . italic) t) (error e))
                    (condition-case e (internal-set-lisp-face-attribute 'zz :overline 'foo t) (error e))
                    (progn (internal-set-lisp-face-attribute 'zz :weight 'heavy t)
                           (internal-get-lisp-face-attribute 'zz :weight t))
                    (progn (internal-set-lisp-face-attribute 'zz :font "Mono" t)
                           (internal-get-lisp-face-attribute 'zz :font t))
                    (progn (internal-set-lisp-face-attribute 'zz :height (lambda (h) (* 2 h)) t)
                           (functionp (internal-get-lisp-face-attribute 'zz :height t)))
                    (progn (internal-set-lisp-face-attribute 'zz :foreground nil t)
                           (list (internal-get-lisp-face-attribute 'zz :foreground t)
                                 (and (string-match-p "attribute `:foreground' of face `zz': nil value is invalid"
                                                      (with-current-buffer "*Messages*" (buffer-string)))
                                      t)))
                    (list (face-id 'default) (face-id 'bold) (aref (cdr (gethash 'bold face--new-frame-defaults)) 0))
                    (merge-face-attribute :height 1.5 10)
                    (merge-face-attribute :height :ignore-defface 10)
                    (face-attribute-relative-p :weight :ignore-defface))))"#,
            )
            .read_all()
            .expect("read face probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate face probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                r#"(nil (error "Invalid face" zz-nosuch) (1 t unspecified) (:ignore-defface unspecified) (error "Invalid face underline" :style bogus) (error "Invalid face box" :line-width 0) (error "Face height does not produce a positive integer" 0.01) (error "Default face height not absolute and positive" 1.5) (error "Invalid face foundry" "") (wrong-type-argument symbolp 5) (error "Invalid extend face attribute value" foo) (error "Invalid face inheritance" (bold . italic)) (error "Invalid face overline" foo) heavy unspecified t (unspecified t) (0 1 face) 15 10 t)"#
            );
        });
    }

    #[test]
    fn file_locks_are_filelock_c_symlinks() {
        // filelock.c: a lock is a symbolic link to USER@HOST.PID:BOOT named
        // by `make-lock-file-name'; another host's lock asks
        // `ask-user-about-lock' with USER@HOST (pid PID), which may take
        // it; a dead process's lock on this host is removed; a malformed
        // lock or a directory in its place is report_file_errno's
        // "Testing file lock"; `lock-buffer' and `unlock-buffer' act on a
        // modified buffer's file truename only.
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
                r#"(prin1-to-string
                  (progn
                    (require 'cl-lib)
                    (let* ((dir (file-name-as-directory (make-temp-file "emaxx-lock" t)))
                           (f (expand-file-name "a.txt" dir))
                           (lock (expand-file-name ".#a.txt" dir))
                           (me (format "%s@%s." (user-login-name) (system-name)))
                           (plant (lambda (target)
                                    (when (file-symlink-p lock) (delete-file lock))
                                    (make-symbolic-link target lock))))
                      (unwind-protect
                          (progn
                            (write-region "x" nil f)
                            (list
                             (progn (lock-file f)
                                    (list (file-locked-p f) (string-prefix-p me (file-symlink-p lock))
                                          (and (string-match-p "\\.[0-9]+:[0-9]+\\'" (file-symlink-p lock)) t)))
                             (progn (unlock-file f) (file-symlink-p lock))
                             (progn (funcall plant "someone@elsewhere.42:17") (file-locked-p f))
                             (let (asked)
                               (cl-letf (((symbol-function 'ask-user-about-lock)
                                          (lambda (_file opponent) (setq asked opponent) nil)))
                                 (lock-file f))
                               (list asked (file-symlink-p lock)))
                             (cl-letf (((symbol-function 'ask-user-about-lock) (lambda (_file _opponent) t)))
                               (lock-file f)
                               (list (file-locked-p f) (string-prefix-p me (file-symlink-p lock))))
                             (progn (funcall plant (concat me "999999999"))
                                    (list (file-locked-p f) (file-symlink-p lock)))
                             (progn (funcall plant "u@h.12:x")
                                    (prog1 (condition-case e (file-locked-p f) (file-error (nth 2 e)))
                                      (delete-file lock)))
                             (progn (make-directory lock)
                                    (prog1 (condition-case e (file-locked-p f) (file-error (nth 2 e)))
                                      (delete-directory lock)))
                             (let ((create-lockfiles nil)) (lock-file f) (file-symlink-p lock))
                             (with-current-buffer (find-file-noselect f)
                               (prog1 (list (progn (insert "y") (file-locked-p f))
                                            (progn (unlock-buffer) (file-locked-p f))
                                            (progn (lock-buffer) (file-locked-p f))
                                            (condition-case e (lock-buffer 7) (error e))
                                            (progn (set-buffer-modified-p nil) (file-locked-p f)))
                                 (kill-buffer)))))
                        (delete-directory dir t)))))"#,
            )
            .read_all()
            .expect("read lock probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate lock probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                r#"((t t t) nil "someone" ("someone@elsewhere (pid 42)" "someone@elsewhere.42:17") (t t) (nil nil) "Invalid argument" "Is a directory" nil (t nil t (wrong-type-argument stringp 7) nil))"#
            );
        });
    }

    #[test]
    fn unicode_property_tables_are_chartab_c_and_string_to_number_checks_base() {
        // chartab.c: uniprop_table finds PROP's entry by `eq' and returns
        // the table only when it is a property table with a known
        // decoder; the run-length decoder reads nil as index 0.
        // data.c:Fstring_to_number checks BASE as a fixnum in 2..16.
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let mut env = crate::lisp::types::Env::new();
            let mut forms = Reader::new(
                r#"(defmacro emaxx-probe-or-error (form)
                     `(condition-case e ,form (error (list 'ERR e))))
                   FORM"#
                    .replace("FORM", r##"(prin1-to-string (list (let* ((name (unicode-property-table-internal 'name))
       (gc (unicode-property-table-internal 'general-category))
       (out nil))
  (push (char-table-p name) out)
  (push (unicode-property-table-internal 5) out)
  (push (unicode-property-table-internal "name") out)
  (push (unicode-property-table-internal 'no-such-prop) out)
  (push (get-unicode-property-internal gc ?a) out)
  (push (get-unicode-property-internal gc #x10FFFF) out)
  (push (get-unicode-property-internal gc #x3FFF00) out)
  (push (condition-case e (get-unicode-property-internal (make-char-table 'foo) ?a) (error e)) out)
  (push (condition-case e (get-unicode-property-internal gc -1) (error e)) out)
  (push (list (char-table-extra-slot gc 1) (length (char-table-extra-slot gc 4))) out)
  (let ((tbl (make-char-table 'char-code-property-table)))
    (push (char-table-extra-slot tbl 1) out)
    (set-char-table-extra-slot tbl 1 0)
    (set-char-table-extra-slot tbl 4 [a b c])
    (aset tbl ?x 2) (aset tbl ?y nil) (aset tbl ?z 7) (aset tbl ?w 'sym) (aset tbl ?v -1)
    (push (mapcar (lambda (c) (condition-case e (get-unicode-property-internal tbl c) (error e))) '(?x ?y ?z ?v ?u)) out))
  (push (get-char-code-property ?A 'name) out)
  (push (get-char-code-property ?a 'general-category) out)
  (push (get-char-code-property ?\N{GREEK SMALL LETTER ALPHA} 'uppercase) out)
  (push (upcase "ǆ") out)
  (push (capitalize "ǆa") out)
  (nreverse out)) (list (emaxx-probe-or-error (string-to-number "123")) (emaxx-probe-or-error (string-to-number "-123")) (emaxx-probe-or-error (string-to-number "ff" 16)) (emaxx-probe-or-error (string-to-number "-FF" 16))
             (emaxx-probe-or-error (string-to-number "2305843009213693951")) (emaxx-probe-or-error (string-to-number "2305843009213693952")) (emaxx-probe-or-error (string-to-number "-2305843009213693952"))
             (emaxx-probe-or-error (string-to-number "-2305843009213693953")) (emaxx-probe-or-error (string-to-number "18446744073709551615")) (emaxx-probe-or-error (string-to-number "18446744073709551616"))
             (emaxx-probe-or-error (string-to-number "123456789012345678901234567890")) (emaxx-probe-or-error (string-to-number "-ZZZZZZZZZZZZZZZZ" 36)) (emaxx-probe-or-error (string-to-number "  +7")) (emaxx-probe-or-error (string-to-number "x"))
             (emaxx-probe-or-error (string-to-number "1e3")) (emaxx-probe-or-error (string-to-number "0x10")) (emaxx-probe-or-error (string-to-number "777" 8)) (emaxx-probe-or-error (bignump (string-to-number "2305843009213693952")))
             (emaxx-probe-or-error (string-to-number "12" 1)) (emaxx-probe-or-error (string-to-number "12" 17)) (emaxx-probe-or-error (string-to-number "12" 2.0)) (emaxx-probe-or-error (string-to-number "12" (expt 2 70)))
             (emaxx-probe-or-error (string-to-number "-fffffffffffffffffffff" 16)) (emaxx-probe-or-error (string-to-number "1111111111111111111111111111111111111111111111111111111111111111111" 2))
             #x1fffffffffffffff #x2000000000000000 #o7777777777777777777777 #b101 #24r1k (read "#x-10"))))"##)
                    .as_str(),
            )
            .read_all()
            .expect("read property probe");
            let probe = forms.pop().expect("probe form");
            interpreter
                .eval(&forms[0], &mut env)
                .expect("define the probe's macro");
            let printed = interpreter
                .eval(&probe, &mut env)
                .expect("evaluate property probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                r##"((t nil nil nil Ll Cn Cn (error "Invalid Unicode property table") (wrong-type-argument characterp -1) (0 31) nil (c a 7 -1 a) "LATIN CAPITAL LETTER A" Ll 913 "Ǆ" "ǅa") (123 -123 255 -255 2305843009213693951 2305843009213693952 -2305843009213693952 -2305843009213693953 18446744073709551615 18446744073709551616 123456789012345678901234567890 (ERR (args-out-of-range 36)) 7 0 1000.0 0 511 t (ERR (args-out-of-range 1)) (ERR (args-out-of-range 17)) (ERR (wrong-type-argument fixnump 2.0)) (ERR (wrong-type-argument fixnump 1180591620717411303424)) -19342813113834066795298815 147573952589676412927 2305843009213693951 2305843009213693952 73786976294838206463 5 44 -16))"##
            );
        });
    }

    #[test]
    fn string_aref_aset_and_hash_input_follow_data_c_and_fns_c() {
        // data.c:Faref and Faset on unibyte and multibyte strings, from
        // byte-code (bytecode.c's Baref and Baset) and as functions:
        // CHECK_IMPURE, the index, then CHECK_CHARACTER of NEWELT.
        // fns.c:extract_data_from_object hashes a string's bytes, encoded
        // first when multibyte, between endpoints into those bytes.
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let mut env = crate::lisp::types::Env::new();
            let mut forms = Reader::new(
                r##"(defmacro emaxx-probe-or-error (form)
                     `(condition-case e ,form (error (list 'ERR e))))
FORM"##
                    .replace(
                        "FORM",
                        r##"(let* ((get (byte-compile (lambda (s i) (aref s i))))
       (put (byte-compile (lambda (s i v) (aset s i v))))
       (u (string-to-unibyte "abc\377"))
       (m (string ?a ?é ?b ?\x3fff80))
       (out nil))
  (dolist (fn (list get #'aref))
    (dolist (args (list (list u 0) (list u 3) (list u 4) (list u -1) (list u 1.0)
                        (list m 1) (list m 3) (list m 4) (list "" 0)))
      (push (emaxx-probe-or-error (apply fn args)) out)))
  (dolist (fn (list put #'aset))
    (let ((s (copy-sequence u)))
      (dolist (args '((0 ?z) (1 255) (2 ?é) (4 ?a) (-1 ?a) (0 -1) (0 x) (0 #x400000)))
        (push (emaxx-probe-or-error
               (list (apply fn s args) (string-to-list s) (multibyte-string-p s)))
              out)))
    (let ((s (copy-sequence "abcd")))
      (dolist (args '((1 ?é) (1 ?x) (2 #x3fff80)))
        (push (emaxx-probe-or-error
               (list (apply fn s args) (string-to-list s) (multibyte-string-p s)
                     (string-bytes s)))
              out)))
    (let ((s (copy-sequence m)))
      (dolist (args '((1 ?è) (1 ?z) (0 ?€) (3 ?q)))
        (push (emaxx-probe-or-error
               (list (apply fn s args) (string-to-list s) (string-bytes s)))
              out))))
  (dolist (form '((md5 "abc") (md5 (string-to-unibyte "a\377")) (md5 "aé")
                  (md5 "aé" nil nil 'latin-1) (md5 "aébc" 1 3) (md5 "abcdef" -3)
                  (md5 "abc" 0 4) (md5 "abc" 2 1) (md5 "abc" 'x)
                  (secure-hash 'sha256 "abc")
                  (secure-hash 'sha1 (string-to-unibyte "\200\201") 0 1)
                  (string-to-list (secure-hash 'md5 "é" nil nil t))
                  (secure-hash 'md5 "abcdef" 1 -1) (secure-hash 'sha224 "aé" 1 2)
                  (secure-hash 'sha224 "aé" 1 3) (md5 "x" nil nil 'no-such-coding)
                  (md5 "x" nil nil 'no-such-coding t) (md5 "é" nil nil 'no-such-coding t)
                  (md5 42)))
    (push (emaxx-probe-or-error (eval form t)) out))
  (with-temp-buffer
    (insert "héllo")
    (push (list (md5 (current-buffer)) (md5 (current-buffer) 2 4)
                (secure-hash 'sha1 (current-buffer)) (buffer-hash))
          out))
  (prin1-to-string (nreverse out)))"##,
                    )
                    .as_str(),
            )
            .read_all()
            .expect("read aref probe");
            let probe = forms.pop().expect("probe form");
            interpreter
                .eval(&forms[0], &mut env)
                .expect("define the probe's macro");
            let printed = interpreter
                .eval(&probe, &mut env)
                .expect("evaluate aref probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                r##"(97 255 (ERR (args-out-of-range "abc\377" 4)) (ERR (args-out-of-range "abc\377" -1)) (ERR (wrong-type-argument fixnump 1.0)) 233 128 (ERR (args-out-of-range "aéb" 4)) (ERR (args-out-of-range "" 0)) 97 255 (ERR (args-out-of-range "abc\377" 4)) (ERR (args-out-of-range "abc\377" -1)) (ERR (wrong-type-argument fixnump 1.0)) 233 128 (ERR (args-out-of-range "aéb" 4)) (ERR (args-out-of-range "" 0)) (122 (122 98 99 255) nil) (255 (122 255 99 255) nil) (233 (122 255 233 255) nil) (ERR (args-out-of-range "z\377\351\377" 4)) (ERR (args-out-of-range "z\377\351\377" -1)) (ERR (wrong-type-argument characterp -1)) (ERR (wrong-type-argument characterp x)) (ERR (wrong-type-argument characterp 4194304)) (233 (97 233 99 100) nil 4) (120 (97 120 99 100) nil 4) (4194176 (97 120 4194176 100) t 5) (232 (97 232 98 128) 6) (122 (97 122 98 128) 5) (8364 (8364 122 98 128) 7) (113 (8364 122 98 113) 6) (122 (122 98 99 255) nil) (255 (122 255 99 255) nil) (233 (122 255 233 255) nil) (ERR (args-out-of-range "z\377\351\377" 4)) (ERR (args-out-of-range "z\377\351\377" -1)) (ERR (wrong-type-argument characterp -1)) (ERR (wrong-type-argument characterp x)) (ERR (wrong-type-argument characterp 4194304)) (233 (97 233 99 100) nil 4) (120 (97 120 99 100) nil 4) (4194176 (97 120 4194176 100) t 5) (232 (97 232 98 128) 6) (122 (97 122 98 128) 5) (8364 (8364 122 98 128) 7) (113 (8364 122 98 113) 6) "900150983cd24fb0d6963f7d28e17f72" "cc7bb99ae517c38d9547266ff7157071" "128fda53c8c07c8c66f2a4812187d92f" "784ca25d737cedf39d61866697e6ba7e" "66ddcd97cfdeabb2f6fb8a999b4bc76f" "4ed9407630eb1000c0f6b63842defa7d" (ERR (args-out-of-range "abc" 0 4)) (ERR (args-out-of-range "abc" 2 1)) (ERR (wrong-type-argument integerp x)) "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" "c78ebd3c85a39a596d9f5cfd2b8d240bc1b9c125" (102 221 205 151 207 222 171 178 246 251 138 153 155 75 199 111) "e02cfbe5502b64aa5ae9f2d0d69eaa8d" "d82a2abcec72bf6f5b7085ac1e793c66ddc7102186c519b2fb019c97" "33dd9448e5538d16ddc6df32dd7f16240cac6cb5238c1c22cb33ae66" (ERR (coding-system-error no-such-coding)) "9dd4e461268c8034f5c8564e155c67a6" "66ddcd97cfdeabb2f6fb8a999b4bc76f" (ERR (error "Invalid object argument" 42)) ("be50e8478cf24ff3595bc7307fb91b50" "4fc48b7d17df5bbe6402fd375f558e0c" "35b5ea45c5e41f78b46a937cc74d41dfea920890" "35b5ea45c5e41f78b46a937cc74d41dfea920890"))"##
            );
        });
    }

    #[test]
    fn gnutls_inputs_are_extract_data_from_object_specs() {
        // gnutls.c makes a buffer or string input the spec `list1' gives,
        // CHECK_CONSes the rest and reads every key, IV, input and AEAD
        // datum with fns.c:extract_data_from_object: OBJECT, START, END,
        // CODING-SYSTEM and NOERROR, endpoints into the encoded bytes.
        run_with_large_stack(|| {
            let emacs_repo = compat::project_root().join("../emacs");
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let mut env = crate::lisp::types::Env::new();
            let mut forms = Reader::new(
                r##"(defmacro emaxx-probe-or-error (form)
                     `(condition-case e ,form (error (list 'ERR e))))
FORM"##
                    .replace("FORM", r##"(progn
(require 'hex-util)
(let ((out nil)
      (h (lambda (in) (encode-hex-string (gnutls-hash-digest 'SHA256 in)))))
  (dolist (in (list "abc" "é" (string-to-unibyte "\377") '("aé" 1 2) '("aé" 1 3) '("aé" 1)
                    '("é" nil nil latin-1) '("é" nil nil no-such-coding)
                    '("é" nil nil no-such-coding t) '("abcdef" -2) '("abc" 2 1)
                    '("abc" 0 4) '("abc" x) '(nil) '(42) 42 nil '(iv-auto) '(iv-auto x)))
    (push (emaxx-probe-or-error (funcall h in)) out))
  (with-temp-buffer
    (insert "héllo")
    (dolist (in (list (current-buffer) (list (current-buffer) 2 4) (list (current-buffer) 4 2)
                      (list (current-buffer) 1 99) (list (current-buffer) nil nil 'latin-1)))
      (push (emaxx-probe-or-error (funcall h in)) out)))
  (push (emaxx-probe-or-error (length (gnutls-hash-digest 'SHA256 '(iv-auto 5)))) out)
  (push (emaxx-probe-or-error
         (encode-hex-string (gnutls-hash-mac 'SHA256 (copy-sequence "key") '("aé" 1 3))))
        out)
  (push (emaxx-probe-or-error
         (let ((k (copy-sequence "key")))
           (gnutls-hash-mac 'SHA256 (list k) "x")
           k))
        out)
  (push (emaxx-probe-or-error
         (let* ((c (cdr (assq 'AES-128-CBC (gnutls-ciphers))))
                (k (copy-sequence "0123456789abcdef"))
                (r (gnutls-symmetric-encrypt c k '("0123456789abcdefXX" 0 16) '("aé" nil nil latin-1 t))))
           r))
        out)
  (push (emaxx-probe-or-error
         (let* ((c (cdr (assq 'AES-128-CBC (gnutls-ciphers))))
                (k (copy-sequence "0123456789abcdef"))
                (r (gnutls-symmetric-encrypt c k "0123456789abcdef" "0123456789abcdé")))
           (list (mapcar #'encode-hex-string r) k)))
        out)
  (prin1-to-string (nreverse out))))"##)
                    .as_str(),
            )
            .read_all()
            .expect("read gnutls probe");
            let probe = forms.pop().expect("probe form");
            interpreter
                .eval(&forms[0], &mut env)
                .expect("define the probe's macro");
            let printed = interpreter
                .eval(&probe, &mut env)
                .expect("evaluate gnutls probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                r##"("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" "4a99557e4033c3539de2eb65472017cad5f9557f7a0625a09f1c3f6e2ba69c4c" "a8100ae6aa1940d0b663bb31cd466142ebbdbd5187131b92d93818987832eb89" "ae3f4619b0413d70d3004b9131c3752153074e45725be13b9a148978895e359e" "4a99557e4033c3539de2eb65472017cad5f9557f7a0625a09f1c3f6e2ba69c4c" "4a99557e4033c3539de2eb65472017cad5f9557f7a0625a09f1c3f6e2ba69c4c" "de2e331d891ae267a7009cb45b4e8830f170e0c937288ea2731a1941c7a53b0d" (ERR (coding-system-error no-such-coding)) "4a99557e4033c3539de2eb65472017cad5f9557f7a0625a09f1c3f6e2ba69c4c" "4ca669ac3713d1f4aea07dae8dcc0d1c9867d27ea82a3ba4e6158a42206f959b" (ERR (args-out-of-range "abc" 2 1)) (ERR (args-out-of-range "abc" 0 4)) (ERR (wrong-type-argument integerp x)) (ERR (error "Invalid object argument" "nil")) (ERR (error "Invalid object argument" 42)) (ERR (wrong-type-argument consp 42)) (ERR (wrong-type-argument consp nil)) (ERR (error "Without a length, `iv-auto' can't be used; see ELisp manual")) (ERR (error "Without a length, `iv-auto' can't be used; see ELisp manual")) "3c48591d8d098a4538f5e013dfcf406e948eac4d3277b10bf614e295d6068179" "018576b8030e53d8294d839a689d014d313f0170c07d0817670d2e2d9c3d3e73" "018576b8030e53d8294d839a689d014d313f0170c07d0817670d2e2d9c3d3e73" (ERR (args-out-of-range 1 99)) "c63c19ed1e0ee079f22ddb0d4112cfc799132c88684eb42324c62b9899b0c6c1" 32 "e5eb9b0a697f5df0e9c03ffdd759f4b6edb39204332904d5eb74735b54259255" "   " (ERR (error "GnuTLS cipher AES-128-CBC/encrypt input block length 2 is not a multiple of the required 16")) (("0750400a883b035762997c1160d05967" "30313233343536373839616263646566") "                "))"##
            );
        });
    }

    #[test]
    fn image_symbols_are_the_obarrays_and_the_initial_frame_is_terminal_frame() {
        // pdumper.c maps the dumped symbols where the image holds them:
        // a preloaded symbol is the obarray's, its cells are traced by the
        // collector and Funintern leaves it whole.  window.c's
        // init_window_once_for_pdumper makes the new process's initial
        // frame Vterminal_frame.
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
                r#"(prin1-to-string
                    (list (eq terminal-frame (selected-frame))
                          (eq (intern-soft "cl-incf") 'cl-incf)
                          (car-safe (symbol-function 'cl-incf))
                          (progn (put 'cl-incf 'r103-prop (list 1 2 (make-string 3 ?x)))
                                 (setq cl-incf (list 'a (copy-sequence "b")))
                                 (garbage-collect)
                                 (list (get 'cl-incf 'r103-prop) cl-incf))
                          (let ((old 'cl-incf))
                            (list (unintern "cl-incf" obarray)
                                  (progn (garbage-collect) (intern-soft "cl-incf"))
                                  (eq old (intern "cl-incf"))
                                  (symbol-name old)
                                  (get old 'r103-prop)
                                  (car-safe (symbol-function old))
                                  (intern-soft old)))))"#,
            )
            .read_all()
            .expect("read image symbol probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate image symbol probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                "(t t autoload ((1 2 \"xxx\") (a \"b\")) \
                 (t nil nil \"cl-incf\" (1 2 \"xxx\") autoload nil))"
            );
        });
    }

    #[test]
    fn reader_interns_in_each_symbols_obarray_and_closes_dotted_lists_as_read0() {
        // lread.c read0: each symbol is interned in `check_obarray
        // (Vobarray)' as it is read, so Lisp run in the middle of a read
        // (here `char-from-name', for `\N{NAME}') can redirect the rest;
        // RE_list_dot skips to the next character, signalling at the end,
        // and reads it before "expected )".
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
                r#"(prin1-to-string
                    (let* ((orig obarray)
                           (ob (obarray-make))
                           (switch (lambda (&rest _) (setq obarray ob)))
                           (form (progn
                                   (advice-add 'char-from-name :after switch)
                                   (unwind-protect
                                       (car (read-from-string
                                             "(r102-a ?\\N{LATIN SMALL LETTER A} r102-b . r102-c)"))
                                     (advice-remove 'char-from-name switch)
                                     (setq obarray orig)))))
                      (list (eq (nth 0 form) 'r102-a)
                            (eq (nth 2 form) (intern-soft "r102-b" ob))
                            (intern-soft "r102-c")
                            (nth 1 form)
                            (condition-case e (read "(a . b") (error (car e)))
                            (condition-case e (read "(a . b c)") (error e))
                            (with-temp-buffer
                              (insert "(a\n . b c)")
                              (goto-char (point-min))
                              (condition-case e (read (current-buffer))
                                (error (list e (point))))))))"#,
            )
            .read_all()
            .expect("read obarray probe")
            .remove(0);
            let printed = interpreter
                .eval(&form, &mut crate::lisp::types::Env::new())
                .expect("evaluate obarray probe");
            assert_eq!(
                lisp::primitives::string_like(&printed)
                    .expect("printed")
                    .text,
                "(t t nil 97 end-of-file (invalid-read-syntax \"expected )\") \
                 ((invalid-read-syntax \"expected )\" 2 6) 10))"
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                       (or (not (subrp (symbol-function 'gui-set-selection)))\
                           (subr-native-elisp-p (symbol-function 'gui-set-selection)))\
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                       (or (not (subrp (symbol-function 'context-menu-map)))\
                           (subr-native-elisp-p (symbol-function 'context-menu-map)))\
                       (let ((context-menu-functions nil))\
                         (equal (context-menu-map)\
                                '(keymap \"Context Menu\"))))",
            )
            .read_all()
            .expect("read mouse startup probe")
            .remove(0);

            assert_eq!(
                interpreter
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
            // Ask for loadup's own transition on the image as started,
            // before the embedded fixture configures in-process compilation.
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
            let options = BatchRunOptions {
                load_path: compat::emaxx_upstream_load_path(&emacs_repo)
                    .expect("upstream load path"),
                ..Default::default()
            };
            let mut interpreter =
                initialize_batch_interpreter(&options).expect("init batch interpreter");
            let expected = Reader::new(&String::from_utf8_lossy(&output.stdout))
                .read_all()
                .expect("read locale oracle output")
                .remove(0);
            let form = Reader::new(expression)
                .read_all()
                .expect("read locale startup probe")
                .remove(0);
            assert_eq!(
                interpreter
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                &mut crate::lisp::types::Env::new(),
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    Value::vector([
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                .eval(&form, &mut crate::lisp::types::Env::new())
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
            let mode_result = interpreter.eval(&mode, &mut crate::lisp::types::Env::new());
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
            let goto_result = interpreter.eval(&goto, &mut crate::lisp::types::Env::new());
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .eval(&form, &mut crate::lisp::types::Env::new())
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
                    .lookup_function("ert-test-erts-file", &crate::lisp::types::Env::new())
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
