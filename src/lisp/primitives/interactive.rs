use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

mod key_sequence;
pub(crate) use key_sequence::{KeySequenceReader, read_key_sequence_events};

#[cfg(unix)]
use std::sync::atomic::{AtomicUsize, Ordering as UserSignalOrdering};

#[cfg(unix)]
static PENDING_SIGUSR1: AtomicUsize = AtomicUsize::new(0);
#[cfg(unix)]
static PENDING_SIGUSR2: AtomicUsize = AtomicUsize::new(0);

/// sysdep.c registers SIGUSR1/SIGUSR2 as user-input signals.  The handler may
/// only touch lock-free state; Lisp dispatch happens later from read_char's
/// ordinary event pump.
pub(crate) fn install_user_signal_handlers() {
    #[cfg(unix)]
    {
        static INSTALLED: OnceLock<Result<(), String>> = OnceLock::new();
        let result = INSTALLED.get_or_init(|| {
            // SAFETY: both callbacks perform only an atomic increment, which
            // is async-signal-safe, and signal-hook owns handler chaining.
            unsafe {
                signal_hook::low_level::register(libc::SIGUSR1, || {
                    PENDING_SIGUSR1.fetch_add(1, UserSignalOrdering::Relaxed);
                })
                .map_err(|error| error.to_string())?;
                signal_hook::low_level::register(libc::SIGUSR2, || {
                    PENDING_SIGUSR2.fetch_add(1, UserSignalOrdering::Relaxed);
                })
                .map_err(|error| error.to_string())?;
            }
            Ok(())
        });
        result
            .as_ref()
            .unwrap_or_else(|error| panic!("install SIGUSR1/SIGUSR2 handlers: {error}"));
    }
}

/// The signal sysdep.c:init_signals' process_fatal_action caught (SIGHUP,
/// SIGINT, SIGTERM, SIGQUIT, SIGVTALRM, SIGXCPU or SIGXFSZ) and the next
/// quit check has not yet acted on; -1 once it has.  A fatal signal after
/// that is dropped, as GNU's handler, running terminate_due_to_signal with
/// every signal blocked (its sa_mask is full), never sees one.
#[cfg(unix)]
static PENDING_FATAL_SIGNAL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// emacs.c's fatal_error_in_progress: the orderly shutdown of a fatal
/// signal has begun, so a later one only re-raises.
#[cfg(unix)]
static FATAL_ERROR_IN_PROGRESS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// keyboard.c's DEFVAR_BOOL attempt_orderly_shutdown_on_fatal_signal, as
/// the interpreter last stored it (`set_attempt_orderly_shutdown'), for a
/// fault's handler, which has no interpreter to read it from.
#[cfg(unix)]
static ATTEMPT_ORDERLY_SHUTDOWN: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// The store of `attempt-orderly-shutdown-on-fatal-signal' into its C
/// variable.
pub(crate) fn set_attempt_orderly_shutdown(attempt: bool) {
    #[cfg(unix)]
    ATTEMPT_ORDERLY_SHUTDOWN.store(attempt, UserSignalOrdering::Relaxed);
    #[cfg(not(unix))]
    let _ = attempt;
}

/// The C variable `attempt-orderly-shutdown-on-fatal-signal' forwards to.
pub(crate) fn attempt_orderly_shutdown() -> bool {
    #[cfg(unix)]
    {
        ATTEMPT_ORDERLY_SHUTDOWN.load(UserSignalOrdering::Relaxed)
    }
    #[cfg(not(unix))]
    true
}

/// The signals sysdep.c:init_signals gives process_fatal_action beyond the
/// three maybe_fatal_sig handles: each ends Emacs through
/// terminate_due_to_signal.
#[cfg(unix)]
const PROCESS_FATAL_SIGNALS: [libc::c_int; 4] =
    [libc::SIGQUIT, libc::SIGVTALRM, libc::SIGXCPU, libc::SIGXFSZ];

/// The signals init_signals gives thread_fatal_action (SIGSEGV through
/// init_sigsegv's handler): a fault of the thread they arrive on.
#[cfg(unix)]
const THREAD_FATAL_SIGNALS: [libc::c_int; 7] = [
    libc::SIGILL,
    libc::SIGTRAP,
    libc::SIGFPE,
    libc::SIGABRT,
    libc::SIGBUS,
    libc::SIGSEGV,
    libc::SIGSYS,
];

/// sysdep.c:init_signals and the handlers the rest of startup adds.
///
/// maybe_fatal_sig gives SIGHUP and SIGINT (SIGINT only when
/// NONINTERACTIVE, keyboard.c taking it otherwise) process_fatal_action
/// unless batch inherited them ignored; SIGTERM gets it whatever it
/// inherited, and so do SIGQUIT, SIGVTALRM, SIGXCPU and SIGXFSZ.  GNU's
/// handler runs terminate_due_to_signal at once; this one records the
/// signal for the next quit check (`maybe_quit', the waits' passes),
/// which runs it.  The faults (SIGILL, SIGTRAP, SIGFPE, SIGABRT, SIGBUS,
/// SIGSEGV, SIGSYS) cannot be returned to and end Emacs in their handler,
/// on a stack of their own as init_sigsegv's.  SIGPROF is ignored until a
/// profiler takes it (atimer.c:init_atimer, which runs next, catches
/// SIGALRM).
pub(crate) fn install_fatal_signal_handlers(noninteractive: bool) {
    #[cfg(unix)]
    {
        NONINTERACTIVE.store(noninteractive, UserSignalOrdering::Relaxed);
        for signal in [libc::SIGHUP, libc::SIGINT, libc::SIGTERM] {
            if signal == libc::SIGINT && !noninteractive {
                continue;
            }
            if noninteractive && signal != libc::SIGTERM && inherited_ignored(signal) {
                continue;
            }
            install_process_fatal_handler(signal);
        }
        for signal in PROCESS_FATAL_SIGNALS {
            install_process_fatal_handler(signal);
        }
        install_thread_fatal_handlers(noninteractive);
        // SAFETY: setting a disposition has no other effect.
        unsafe {
            libc::signal(libc::SIGPROF, libc::SIG_IGN);
        }
        // emacs.c:main's emacs_backtrace (-1): `backtrace' loads what it
        // needs now, not in a handler.
        emacs_backtrace(-1);
    }
    #[cfg(not(unix))]
    let _ = noninteractive;
}

/// Whether SIGNAL's disposition, as the process inherited it, is SIG_IGN.
#[cfg(unix)]
fn inherited_ignored(signal: libc::c_int) -> bool {
    // SAFETY: querying the disposition writes only OLD.
    unsafe {
        let mut old: libc::sigaction = std::mem::zeroed();
        libc::sigaction(signal, std::ptr::null(), &mut old) != 0
            || old.sa_sigaction == libc::SIG_IGN
    }
}

/// process_fatal_action for SIGNAL: the signal recorded for the next quit
/// check; a second one while the first still waits takes the signal's
/// default action, so a primitive that never reaches a check (a native
/// walk of a circular list) cannot leave the process running.
#[cfg(unix)]
fn install_process_fatal_handler(signal: libc::c_int) {
    // SAFETY: the callback performs an atomic compare-and-swap and, for a
    // second signal, signal-hook's async-signal-safe default action.
    let registered = unsafe {
        signal_hook::low_level::register(signal, move || {
            match PENDING_FATAL_SIGNAL.compare_exchange(
                0,
                signal,
                UserSignalOrdering::Relaxed,
                UserSignalOrdering::Relaxed,
            ) {
                Ok(_) => super::atimer::note_pending_signal(),
                Err(previous) if previous > 0 => {
                    let _ = signal_hook::low_level::emulate_default_handler(signal);
                }
                Err(_) => {}
            }
        })
    };
    if let Err(error) = registered {
        panic!("install the fatal signal handlers: {error}");
    }
    // GNU's handler acts inside an interrupted system call; this one
    // returns, so a slow call it interrupts must fail with EINTR rather
    // than restart, for emacs_open's, emacs_read_quit's and
    // emacs_write_quit's maybe_quit and the waits' passes to run it.
    // SAFETY: rewrites only the flags of the action just installed.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        if libc::sigaction(signal, std::ptr::null(), &mut action) == 0 {
            action.sa_flags &= !libc::SA_RESTART;
            libc::sigaction(signal, &action, std::ptr::null_mut());
        }
    }
}

/// sysdep.c:emacs_sigaction_flags: restartable calls in batch, whose
/// input is stdio's; interrupted ones interactively, where input is
/// polled.
#[cfg(unix)]
fn emacs_sigaction_flags(noninteractive: bool) -> libc::c_int {
    if noninteractive { libc::SA_RESTART } else { 0 }
}

/// Whether the session is a batch one, as `install_fatal_signal_handlers'
/// was told (batch until then).
#[cfg(unix)]
static NONINTERACTIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

/// emacs_sigaction_flags for a handler installed after startup.
#[cfg(unix)]
pub(crate) fn current_sigaction_flags() -> libc::c_int {
    emacs_sigaction_flags(NONINTERACTIVE.load(UserSignalOrdering::Relaxed))
}

/// init_signals' thread_fatal_action for the faults, with init_sigsegv's
/// alternate stack (64 KiB, its sigsegv_stack) so a handler runs when the
/// fault is the stack's own overflow.
#[cfg(unix)]
fn install_thread_fatal_handlers(noninteractive: bool) {
    const SIGSEGV_STACK_SIZE: usize = 64 * 1024;
    static SIGSEGV_STACK: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    let stack = *SIGSEGV_STACK.get_or_init(|| {
        Box::leak(vec![0u8; SIGSEGV_STACK_SIZE].into_boxed_slice()).as_mut_ptr() as usize
    });
    // SAFETY: the stack lives for the process; the actions name a handler
    // that only makes async-signal-safe calls.
    unsafe {
        let alternate = libc::stack_t {
            ss_sp: stack as *mut libc::c_void,
            ss_flags: 0,
            ss_size: SIGSEGV_STACK_SIZE,
        };
        let on_stack = if libc::sigaltstack(&alternate, std::ptr::null_mut()) == 0 {
            libc::SA_ONSTACK
        } else {
            0
        };
        let mut action: libc::sigaction = std::mem::zeroed();
        libc::sigfillset(&mut action.sa_mask);
        action.sa_sigaction = deliver_fatal_thread_signal as *const () as usize;
        action.sa_flags = libc::SA_SIGINFO | on_stack | emacs_sigaction_flags(noninteractive);
        for signal in THREAD_FATAL_SIGNALS {
            libc::sigaction(signal, &action, std::ptr::null_mut());
        }
    }
}

/// sysdep.c:deliver_fatal_thread_signal's handle_fatal_signal for a fault:
/// terminate_due_to_signal with the 40-frame backtrace, in the handler for
/// a fault the thread made.  Its orderly shutdown's Lisp half (the
/// processes hung up, the auto-saves, the files unlocked) needs the
/// interpreter the fault interrupted, which a handler cannot take, so only
/// the diagnostic and the backtrace remain there.
#[cfg(unix)]
extern "C" fn deliver_fatal_thread_signal(
    signal: libc::c_int,
    info: *mut libc::siginfo_t,
    _context: *mut libc::c_void,
) {
    // SAFETY: the calls below are async-signal-safe but for glibc's
    // backtrace, whose loading emacs_backtrace (-1) did at startup; INFO
    // is the kernel's.
    unsafe {
        // A fault another process or `signal-process' sent (kill, sigqueue)
        // can be returned to, so the next quit check runs the whole
        // terminate_due_to_signal as for a process signal; glibc's own
        // abort (a raise, SI_TKILL from this process) cannot.
        let sent = (*info).si_code <= 0
            && !(signal == libc::SIGABRT
                && (*info).si_code == libc::SI_TKILL
                && (*info).si_pid() == libc::getpid());
        if sent {
            match PENDING_FATAL_SIGNAL.compare_exchange(
                0,
                signal,
                UserSignalOrdering::Relaxed,
                UserSignalOrdering::Relaxed,
            ) {
                Ok(_) => {
                    super::atimer::note_pending_signal();
                    return;
                }
                Err(previous) if previous < 0 => return,
                Err(_) => {}
            }
        }
        libc::signal(signal, libc::SIG_DFL);
        if attempt_orderly_shutdown()
            && !FATAL_ERROR_IN_PROGRESS.swap(true, UserSignalOrdering::Relaxed)
        {
            fatal_error_diagnostic(signal);
            emacs_backtrace(40);
        }
        raise_unblocked(signal);
    }
}

/// emacs.c:shut_down_emacs's "Fatal error NUM: DESC" for SIGNAL (not
/// SIGTERM), written with one write, when Emacs's process group is the
/// foreground of the terminal on standard input.
#[cfg(unix)]
fn fatal_error_diagnostic(signal: libc::c_int) {
    // SAFETY: tcgetpgrp, getpgrp and write are async-signal-safe.
    unsafe {
        let foreground = libc::tcgetpgrp(libc::STDIN_FILENO);
        if foreground == -1 || foreground != libc::getpgrp() || signal == libc::SIGTERM {
            return;
        }
        let mut buffer = [0u8; 256];
        let mut length = 0;
        let mut put = |bytes: &[u8]| {
            let take = bytes.len().min(buffer.len() - length);
            buffer[length..length + take].copy_from_slice(&bytes[..take]);
            length += take;
        };
        put(b"Fatal error ");
        let mut digits = [0u8; 12];
        let mut at = digits.len();
        let mut number = signal.unsigned_abs();
        loop {
            at -= 1;
            digits[at] = b'0' + (number % 10) as u8;
            number /= 10;
            if number == 0 {
                break;
            }
        }
        put(&digits[at..]);
        put(b": ");
        let description = safe_strsignal(signal);
        put(std::ffi::CStr::from_ptr(description).to_bytes());
        put(b"\n");
        libc::write(libc::STDERR_FILENO, buffer.as_ptr().cast(), length);
    }
}

/// sysdep.c:safe_strsignal: sigdescr_np's description, "Unknown signal"
/// for a number it does not know.
#[cfg(unix)]
fn safe_strsignal(signal: libc::c_int) -> *const libc::c_char {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    // SAFETY: sigdescr_np returns a static string or null.
    let description = unsafe {
        unsafe extern "C" {
            fn sigdescr_np(signal: libc::c_int) -> *const libc::c_char;
        }
        sigdescr_np(signal)
    };
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    // SAFETY: strsignal returns a string valid until its next call.
    let description = unsafe { libc::strsignal(signal) }.cast_const();
    if description.is_null() {
        c"Unknown signal".as_ptr()
    } else {
        description
    }
}

/// sysdep.c:emacs_backtrace: "Backtrace:" and up to LIMIT + 1 frames on
/// standard error as backtrace_symbols_fd writes them, then "..." when
/// LIMIT cut them; a negative LIMIT only makes `backtrace' load what it
/// needs.
#[cfg(unix)]
pub(crate) fn emacs_backtrace(limit: i32) {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        const BACKTRACE_LIMIT_MAX: i32 = 500;
        let mut buffer = [std::ptr::null_mut::<libc::c_void>(); BACKTRACE_LIMIT_MAX as usize + 1];
        let bounded = limit.min(BACKTRACE_LIMIT_MAX);
        // SAFETY: BUFFER holds BOUNDED + 1 entries; the writes take owned
        // bytes.
        unsafe {
            if bounded < 0 {
                libc::backtrace(buffer.as_mut_ptr(), 1);
                return;
            }
            let count = libc::backtrace(buffer.as_mut_ptr(), bounded + 1);
            if count > 0 {
                libc::write(libc::STDERR_FILENO, b"Backtrace:\n".as_ptr().cast(), 11);
                libc::backtrace_symbols_fd(buffer.as_ptr(), count, libc::STDERR_FILENO);
                if bounded < count {
                    libc::write(libc::STDERR_FILENO, b"...\n".as_ptr().cast(), 4);
                }
            }
        }
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let _ = limit;
}

/// emacs.c:terminate_due_to_signal's end: SIGNAL unblocked and raised at
/// its default action, which ends the process.
#[cfg(unix)]
fn raise_unblocked(signal: libc::c_int) -> ! {
    // SAFETY: async-signal-safe calls on owned storage.
    unsafe {
        let mut unblocked: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut unblocked);
        libc::sigaddset(&mut unblocked, signal);
        libc::pthread_sigmask(libc::SIG_UNBLOCK, &unblocked, std::ptr::null_mut());
        libc::raise(signal);
        // This shouldn't be executed.
        libc::_exit(1)
    }
}

/// Whether a fatal signal waits for `run_pending_fatal_signal'.
#[inline]
pub(crate) fn fatal_signal_pending() -> bool {
    #[cfg(unix)]
    {
        PENDING_FATAL_SIGNAL.load(UserSignalOrdering::Relaxed) > 0
    }
    #[cfg(not(unix))]
    false
}

/// emacs.c:terminate_due_to_signal for the signal process_fatal_action
/// recorded.  With `attempt-orderly-shutdown-on-fatal-signal', SIGTERM, SIGHUP and SIGINT
/// run Fkill_emacs with the signal number, whatever `inhibit-quit' says,
/// and the others shut_down_emacs (no hook runs from then on; the
/// diagnostic, the processes hung up, every buffer auto-saved, the files
/// unlocked) and write the backtrace.  The signal, at its default action,
/// then ends the process.
#[cold]
pub(crate) fn run_pending_fatal_signal(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    #[cfg(unix)]
    {
        let signal = PENDING_FATAL_SIGNAL.swap(-1, UserSignalOrdering::Relaxed);
        if signal > 0 {
            if attempt_orderly_shutdown()
                && !FATAL_ERROR_IN_PROGRESS.swap(true, UserSignalOrdering::Relaxed)
            {
                if matches!(signal, libc::SIGTERM | libc::SIGHUP | libc::SIGINT) {
                    return call(
                        interp,
                        "kill-emacs",
                        &[Value::Integer(i64::from(signal))],
                        env,
                    )
                    .map(drop);
                }
                shut_down_for_signal(interp, signal, env);
                emacs_backtrace(40);
            }
            // The default action, as terminate_due_to_signal set it first:
            // the handler, which drops a later fatal signal as GNU's full
            // sa_mask blocks it, stays until the raise.  What stdio has
            // buffered for standard output is lost, as in GNU.
            // SAFETY: setting a disposition has no other effect.
            unsafe {
                libc::signal(signal, libc::SIG_DFL);
            }
            raise_unblocked(signal);
        }
        if signal == 0 {
            PENDING_FATAL_SIGNAL.store(0, UserSignalOrdering::Relaxed);
        }
    }
    let _ = (interp, env);
    Ok(())
}

/// emacs.c:shut_down_emacs for SIGNAL: Vrun_hooks cleared, so no hook runs
/// from now on; the diagnostic; kill_buffer_processes' SIGHUP to each
/// subprocess, sentinels inhibited; Fdo_auto_save (t, nil); and
/// unlock_all_files.  An error in a step does not stop the others.
#[cfg(unix)]
fn shut_down_for_signal(interp: &mut Interpreter, signal: libc::c_int, env: &mut Env) {
    interp.hooks_cleared = true;
    fatal_error_diagnostic(signal);
    interp.hang_up_processes_and_reset_terminals();
    let _ = super::file_io::do_auto_save(interp, env, Value::T, Value::Nil);
    let _ = super::filelock::unlock_all_files(interp, env);
}

#[cfg(unix)]
fn take_pending_user_signal() -> Option<&'static str> {
    // GNU prepends each add_user_signal registration.  sysdep.c registers
    // SIGUSR1 and then SIGUSR2, so simultaneous pending events drain in this
    // order, one occurrence at a time.
    for (pending, name) in [(&PENDING_SIGUSR2, "sigusr2"), (&PENDING_SIGUSR1, "sigusr1")] {
        if pending
            .try_update(
                UserSignalOrdering::Relaxed,
                UserSignalOrdering::Relaxed,
                |count| count.checked_sub(1),
            )
            .is_ok()
        {
            return Some(name);
        }
    }
    None
}

#[cfg(not(unix))]
fn take_pending_user_signal() -> Option<&'static str> {
    None
}

/// Convert pending host user signals into GNU keyboard events.  A binding in
/// `special-event-map' executes through `command-execute'; an unbound event is
/// appended to `unread-command-events' for the active input reader to return.
pub(crate) fn run_pending_user_signal_events(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut handled = false;
    loop {
        // User signals first (one occurrence at a time), then the THREAD_EVENTs
        // `thread-signal' stored for the main thread, in arrival order.
        let event = match take_pending_user_signal() {
            Some(name) => Value::Symbol(name.into()),
            None if !interp.pending_thread_events.is_empty() => {
                interp.pending_thread_events.remove(0)
            }
            None => break,
        };
        handled = true;
        let keymap = interp
            .lookup_var_key(cached_symbol!("special-event-map"), env)
            .unwrap_or(Value::Nil);
        // keyboard.c:read_char uses access_keymap on the actual event.
        // Filters may collect after a thread event leaves the pending queue,
        // before last-input-event becomes its Lisp-visible root.
        let mut roots = crate::lisp::alloc::RootedVec::from_vec(vec![event, keymap]);
        let binding = keymap_access_event(interp, keymap, event, false, env)?.unwrap_or(Value::Nil);
        roots.push(binding);
        interp.set_variable_key(cached_symbol!("last-input-event"), event, env);
        if binding.is_nil() {
            let mut unread = unread_command_events(interp, env)?;
            unread.push(event);
            interp.set_variable_key(
                cached_symbol!("unread-command-events"),
                Value::list(unread),
                env,
            );
            // Keep later signals behind this unread event, matching the main
            // keyboard queue's ordering.
            break;
        }

        let keys = Value::vector([event]);
        interp.call_function_value(
            Value::Symbol("command-execute".into()),
            Some("command-execute"),
            &[binding, Value::Nil, keys, Value::T],
            env,
        )?;
    }
    Ok(handled)
}

pub(crate) fn set_command_key_state(
    interp: &mut Interpreter,
    keys: Vec<Value>,
    raw_keys: Vec<Value>,
    env: &mut Env,
) {
    interp.keyboard_input.command_keys = keys;
    interp.keyboard_input.single_command_start = 0;
    interp.keyboard_input.raw_keys = raw_keys;
    // GNU has no `this-single-command-keys' *variable* -- only the
    // keyboard.c function, which reads this native state (finding 66).
    let _ = env;
}

fn dribble_event_bytes(event: &Value) -> Vec<u8> {
    match event.kind() {
        Kind::Integer(code) => u32::try_from(code)
            .ok()
            .and_then(char::from_u32)
            .map(|character| character.to_string().into_bytes())
            .unwrap_or_else(|| format!("<{code}>").into_bytes()),
        Kind::StringObject(state) => state.borrow().text().as_bytes().to_vec(),
        Kind::Symbol(symbol) => format!("<{symbol}>").into_bytes(),
        other => format!("<{other}>").into_bytes(),
    }
}

/// keyboard.c:command_loop_1 enters each command it runs in the lossage
/// as (nil . COMMAND), after the keys that invoked it.
pub(crate) fn record_command_lossage(interp: &mut Interpreter, command: Value) {
    interp
        .keyboard_input
        .recent_keys
        .push(Value::cons(Value::Nil, command));
    let limit = interp.lossage_size.max(0) as usize;
    if interp.keyboard_input.recent_keys.len() > limit {
        let excess = interp.keyboard_input.recent_keys.len() - limit;
        interp.keyboard_input.recent_keys.drain(0..excess);
    }
}

/// command_loop_1 empties this-command-keys once a command is done, after
/// its post-command-hook.
pub(crate) fn clear_command_keys(interp: &mut Interpreter) {
    interp.keyboard_input.command_keys.clear();
    interp.keyboard_input.single_command_start = 0;
}

pub(crate) fn executing_kbd_macro_p(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
        .is_some_and(|value| value.is_truthy())
}

fn record_external_input_event(interp: &mut Interpreter, event: &Value, env: &Env) {
    interp.keyboard_input.num_input_events += 1;
    if executing_kbd_macro_p(interp, env) {
        return;
    }
    // record_char's `num_nonmacro_input_events++', which lets the
    // debugger be entered again (maybe_call_debugger).
    let events = interp
        .global_binding_value("num-nonmacro-input-events")
        .and_then(|value| value.as_integer().ok())
        .unwrap_or(0);
    interp.set_symbol_value_cell(
        "num-nonmacro-input-events",
        Value::Integer(events.saturating_add(1)),
    );
    interp.keyboard_input.recent_keys.push(*event);
    let limit = interp.lossage_size.max(0) as usize;
    if interp.keyboard_input.recent_keys.len() > limit {
        let excess = interp.keyboard_input.recent_keys.len() - limit;
        interp.keyboard_input.recent_keys.drain(0..excess);
    }
    if let Some(path) = &interp.keyboard_input.dribble_file
        && let Ok(mut file) = crate::file_system::OpenOptions::new()
            .append(true)
            .open(path)
    {
        let _ = file.write_all(&dribble_event_bytes(event));
        let _ = file.flush();
    }
    // read_char records terminal events at the point where they are
    // consumed, including events read recursively by an interactive
    // command.  The outer command loop records only that command's key
    // sequence, so a register name, query answer, or other nested input
    // must be appended here to make the macro self-contained on replay.
    if interp
        .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
        .is_some_and(|value| value.is_truthy())
    {
        interp.kbd_macro_definition.push(*event);
    }
}

pub(crate) fn function_documentation(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Option<Value> {
    if let Kind::Symbol(symbol) = value.kind()
        && let Some(documentation) = interp.get_symbol_property_of(
            &symbol,
            &cached_symbol!("function-documentation").with(Clone::clone),
        )
    {
        return Some(documentation);
    }
    let value = match value.kind() {
        Kind::Symbol(symbol) => interp.lookup_function_symbol(&symbol, env).ok()?,
        other => other.value(),
    };
    // doc.c Fdocumentation: a macro's documentation lives on the function
    // inside its (macro . FUNCTION) cons.
    let value = if matches!(value.car().map(|v| v.kind()), Ok(Kind::Symbol(ref name)) if name.is_named("macro"))
    {
        value.cdr_if_list()?
    } else {
        value
    };
    // doc.c Fdocumentation: an autoload's documentation is the third
    // element of its (autoload FILE DOC INTERACTIVE TYPE) form, read
    // without resolving the autoload (GNU does not load the file here).
    if matches!(value.car().map(|v| v.kind()), Ok(Kind::Symbol(ref name)) if name.is_named("autoload"))
    {
        let items = value.to_vec().ok()?;
        return items.get(2).filter(|doc| !doc.is_nil()).cloned();
    }
    if let Kind::Record(id) = value.kind()
        && let Some(record) = interp.find_record(id)
        && record.kind == crate::lisp::eval::RecordKind::ModuleFunction
    {
        return record.slots.first().cloned();
    }
    let Kind::Closure(lambda) = value.kind() else {
        return None;
    };
    lambda.documentation().filter(|documentation| {
        matches!(
            documentation.kind(),
            Kind::StringObject(_) | Kind::Integer(_) | Kind::Cons(_)
        )
    })
}

pub(crate) fn is_vector_like_value(value: &Value) -> bool {
    is_vector_value(value)
        || is_bool_vector_value(value)
        || matches!(value.kind(), Kind::CharTable(_))
}

pub(crate) fn is_vector_value(value: &Value) -> bool {
    matches!(value.kind(), Kind::Vector(_))
}

/// Whether VALUE has GNU cons identity despite Emaxx's internal runtime-keymap
/// projection.  Ordinary vectors have their own object class and never enter
/// this predicate.
pub(crate) fn is_cons_value(_interp: &Interpreter, value: &Value) -> bool {
    matches!(value.kind(), Kind::Cons(_))
}

pub(crate) fn symbol_with_pos_parts(_interp: &Interpreter, value: &Value) -> Option<(Value, i64)> {
    let Kind::SymbolWithPos(object) = value.kind() else {
        return None;
    };
    Some((object.symbol(), object.position().as_fixnum().ok()?))
}

#[cfg(test)]
thread_local! {
    static SYMBOL_WITH_POS_FLAG_READ_COUNT: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

pub(crate) fn symbols_with_pos_enabled(interp: &Interpreter, _env: &Env) -> bool {
    symbols_with_pos_flag(interp)
}

/// `symbols_with_pos_enabled', the C boolean.
#[inline]
pub(crate) fn symbols_with_pos_flag(interp: &Interpreter) -> bool {
    #[cfg(test)]
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(|count| count.set(count.get() + 1));
    // lisp.h:SYMBOLP/EQ read the C boolean, not a lexical environment or
    // even the Lisp value cell after makunbound has detached that symbol.
    interp.symbols_with_positions_enabled()
}

/// lisp.h:SYMBOLP and XSYMBOL: VALUE's bare symbol (nil and t
/// included), a symbol with position counting only while
/// `symbols-with-pos-enabled' is set.
pub(crate) fn lisp_symbol(interp: &Interpreter, value: Value) -> Option<Value> {
    if value.is_symbol() {
        return Some(value);
    }
    if interp.symbols_with_positions_enabled()
        && let Some((symbol, _)) = symbol_with_pos_parts(interp, &value)
        && symbol.is_symbol()
    {
        return Some(symbol);
    }
    None
}

/// `lisp_symbol' as the symbol cell XSYMBOL addresses, nil and t
/// included.
pub(crate) fn lisp_symbol_identity(
    interp: &Interpreter,
    value: Value,
) -> Option<crate::lisp::types::SymbolName> {
    match lisp_symbol(interp, value)?.kind() {
        Kind::Symbol(symbol) => Some(symbol),
        Kind::Nil => Some(crate::lisp::types::symbols::builtin_boolean(false)),
        Kind::T => Some(crate::lisp::types::symbols::builtin_boolean(true)),
        _ => None,
    }
}

/// lisp.h:CHECK_SYMBOL, then XSYMBOL: VALUE's bare symbol, or
/// wrong-type-argument symbolp of VALUE as given.
pub(crate) fn check_symbol(interp: &Interpreter, value: Value) -> Result<Value, LispError> {
    lisp_symbol(interp, value).ok_or_else(|| LispError::WrongTypeArgument("symbolp".into(), value))
}

/// GNU's `CHECK_SYMBOL' accepts a symbol-with-position while
/// `symbols-with-pos-enabled' is dynamically non-nil, and `XSYMBOL' then
/// addresses the underlying bare symbol (`src/lisp.h').  Keep that contract
/// in one place so C-owned symbol primitives do not each invent a subtly
/// different positioned-symbol policy.
pub(crate) fn checked_symbol_name(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<String, LispError> {
    if let Ok(symbol) = value.as_symbol() {
        return Ok(symbol.to_string());
    }
    if symbols_with_pos_enabled(interp, env)
        && let Some((symbol, _)) = symbol_with_pos_parts(interp, value)
        && let Ok(symbol) = symbol.as_symbol()
    {
        return Ok(symbol.to_string());
    }
    Err(LispError::WrongTypeArgument("symbolp".into(), *value))
}

/// The same CHECK_SYMBOL/XSYMBOL boundary when the caller stores the symbol
/// itself, rather than using its name to address legacy host-side tables.
pub(crate) fn checked_symbol_identity(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<crate::lisp::types::SymbolName, LispError> {
    if let Some(symbol) = value.symbol_cell() {
        return Ok(symbol);
    }
    if value.is_nil() {
        return Ok(crate::lisp::types::symbols::builtin_boolean(false));
    }
    if value.word() == Value::T.word() {
        return Ok(crate::lisp::types::symbols::builtin_boolean(true));
    }
    if symbols_with_pos_enabled(interp, env)
        && let Some((symbol, _)) = symbol_with_pos_parts(interp, value)
    {
        match symbol.kind() {
            Kind::Symbol(symbol) => return Ok(symbol),
            Kind::Nil => return Ok(crate::lisp::types::symbols::builtin_boolean(false)),
            Kind::T => return Ok(crate::lisp::types::symbols::builtin_boolean(true)),
            _ => {}
        }
    }
    Err(LispError::WrongTypeArgument("symbolp".into(), *value))
}

#[cfg(test)]
pub(crate) fn reset_symbol_with_pos_flag_read_count() {
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn symbol_with_pos_flag_read_count() -> usize {
    SYMBOL_WITH_POS_FLAG_READ_COUNT.with(std::cell::Cell::get)
}

pub(crate) fn symbol_with_pos_equal_in_env(
    interp: &Interpreter,
    left: &Value,
    right: &Value,
    env: &Env,
) -> Option<bool> {
    let left_with_pos = symbol_with_pos_parts(interp, left);
    let right_with_pos = symbol_with_pos_parts(interp, right);
    if left_with_pos.is_none() && right_with_pos.is_none() {
        return None;
    }

    let enabled = symbols_with_pos_enabled(interp, env);
    if enabled {
        let left_base = left_with_pos
            .as_ref()
            .map(|(symbol, _)| symbol)
            .unwrap_or(left);
        let right_base = right_with_pos
            .as_ref()
            .map(|(symbol, _)| symbol)
            .unwrap_or(right);
        return Some(
            match (plain_symbol_name(left_base), plain_symbol_name(right_base)) {
                (Some(_), Some(_)) => left_base == right_base,
                _ => false,
            },
        );
    }

    match (left_with_pos, right_with_pos) {
        (Some((left_symbol, left_pos)), Some((right_symbol, right_pos))) => {
            Some(left_symbol == right_symbol && left_pos == right_pos)
        }
        _ => None,
    }
}

pub(crate) fn eval_callable_metadata_form(
    interp: &mut Interpreter,
    func: &Value,
    form: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // callint.c:Fcall_interactively passes CLOSURE_CONSTANTS to Feval only
    // when CLOSURE_CODE is a cons: an interpreted closure's body.  A
    // bytecode closure stores its constants vector in that same slot.
    // All other commands evaluate their interactive form with a nil lexical
    // environment, independently of the caller's lexical bindings.
    let lexical = match func.kind() {
        Kind::Closure(closure) if matches!(closure.body().kind(), Kind::Cons(_)) => {
            closure.environment_value()
        }
        _ => Value::Nil,
    };
    eval_impl(interp, &[*form, lexical], env)
}

// The arguments a string interactive spec read, with callint.c's
// per-argument `varies': 1 to 4 name the call -- (point), (mark),
// (region-beginning), (region-end) -- the command history records in
// place of the value, -1 marks an argument found without I/O, and 0 one
// read from the terminal.
pub(crate) struct InteractiveStringArgs {
    pub(crate) values: Vec<Value>,
    pub(crate) varies: Vec<i8>,
    // `*' in a read-only buffer under RECORD-FLAG, when every later
    // character is r, p, P or a newline: the call is recorded and only
    // then refused.
    pub(crate) record_then_fail: bool,
}

pub(crate) fn parse_interactive_string(
    function: &Value,
    spec: &str,
    record_flag: bool,
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<InteractiveStringArgs, LispError> {
    // The events the `e' code walks: KEYS, or the current command's keys,
    // starting at the first with parameters unless
    // `inhibit-mouse-event-check' admits any event.
    let keys = interp
        .lookup_var_key(cached_symbol!("this-command-keys-vector"), env)
        .and_then(|keys| vector_or_list_items(&keys).ok())
        .unwrap_or_default();
    let inhibit_mouse_event_check = interp
        .lookup_var_key(cached_symbol!("inhibit-mouse-event-check"), env)
        .is_some_and(|value| value.is_truthy());
    let mut next_event = 0;
    if !inhibit_mouse_event_check {
        while next_event < keys.len() && !keys[next_event].is_cons() {
            next_event += 1;
        }
    }
    let mut record_then_fail = false;
    // The prefix argument as the call began: reading earlier arguments
    // through the minibuffer runs commands that reset it.
    let prefix_arg = interp
        .lookup_var_key(cached_symbol!("current-prefix-arg"), env)
        .unwrap_or(Value::Nil);
    // callint.c's leading flag characters, handled once at the start of
    // the whole spec: `*' barfs on a read-only buffer, `^' runs the
    // shift-selection protocol, `@' selects the event's window (a mouse
    // affair the keyboard frontend has no window position for), `-' is
    // ignored for Lucid compatibility, and `+' is reserved.
    let mut rest = spec;
    loop {
        match rest.chars().next() {
            Some('+') => {
                return Err(crate::lisp::primitives::values::doprnt_error(
                    interp,
                    "`+' is not used in `interactive' for ordinary commands",
                    &[],
                ));
            }
            Some('*') => {
                if interp
                    .lookup_var_key(cached_symbol!("buffer-read-only"), env)
                    .is_some_and(|value| value.is_truthy())
                {
                    if record_flag {
                        if rest[1..]
                            .chars()
                            .any(|ch| !matches!(ch, 'r' | 'p' | 'P' | '\n'))
                        {
                            super::call(interp, "barf-if-buffer-read-only", &[], env)?;
                        }
                        record_then_fail = true;
                    } else {
                        super::call(interp, "barf-if-buffer-read-only", &[], env)?;
                    }
                }
            }
            Some('^') => {
                // callint.c:410 `call0 (Qhandle_shift_selection)': the
                // handler is simple.el's, reached through the ordinary
                // function cell, and GNU has no missing-function
                // tolerance -- a runtime without simple.el loaded
                // signals void-function exactly as GNU would.
                interp.call_function_value(
                    Value::Symbol("handle-shift-selection".into()),
                    Some("handle-shift-selection"),
                    &[],
                    env,
                )?;
            }
            Some('@') | Some('-') => {}
            _ => break,
        }
        rest = &rest[1..];
    }
    let call1 = |interp: &mut Interpreter, env: &mut Env, name: &str, args: &[Value]| {
        interp.call_function_value(Value::Symbol(name.into()), Some(name), args, env)
    };
    // callint.c's check_mark: the mark must exist in this buffer, and
    // under transient-mark-mode an inactive mark signals `mark-inactive'
    // unless mark-even-if-inactive overrides.
    let check_mark =
        |interp: &mut Interpreter, env: &mut Env, for_region: bool| -> Result<usize, LispError> {
            let mark = interp.buffer.borrow().mark().ok_or_else(|| {
                LispError::Signal(
                    if for_region {
                        "The mark is not set now, so there is no region"
                    } else {
                        "The mark is not set now"
                    }
                    .into(),
                )
            })?;
            let transient = interp
                .lookup_var_key(cached_symbol!("transient-mark-mode"), env)
                .is_some_and(|value| value.is_truthy());
            let even_if_inactive = interp
                .lookup_var_key(cached_symbol!("mark-even-if-inactive"), env)
                .is_some_and(|value| value.is_truthy());
            if transient && !even_if_inactive && !interp.buffer.borrow().mark_active() {
                return Err(LispError::SignalValue(Value::list([Value::Symbol(
                    "mark-inactive".into(),
                )])));
            }
            Ok(mark)
        };
    let mut values = Vec::new();
    let mut varies: Vec<i8> = Vec::new();
    // The up-event a `k' or `K' sequence ending in a down-event left, for
    // a later `U'.
    let mut up_event = Value::Nil;
    // GNU formats each prompt with `format-message' against the
    // arguments read so far (callint.c builds callint_message from the
    // visargs); a later prompt's %s shows the earlier answer.
    let mut visible: Vec<Value> = Vec::new();
    // One argument a line; a final newline ends the spec, while an empty
    // line inside it is a newline control letter.
    let mut lines: Vec<&str> = rest.split('\n').collect();
    if lines.last().is_some_and(|line| line.is_empty()) {
        lines.pop();
    }
    for line in lines {
        let mut chars = line.chars();
        let Some(code) = chars.next() else {
            return Err(invalid_interactive_control_letter(interp, '\n'));
        };
        // Codes that read nothing from the terminal mark their argument.
        let mut vary: i8 = 0;
        let raw_prompt: String = chars.collect();
        // GNU builds every prompt through Fformat_message against the
        // visible arguments read so far; that also converts quote
        // characters per text-quoting-style, exactly as the glass shows.
        let formatted_prompt = |interp: &mut Interpreter,
                                env: &mut Env,
                                visible: &[Value]|
         -> Result<Value, LispError> {
            let mut args = vec![Value::String(raw_prompt.clone().into())];
            args.extend(visible.iter().cloned());
            call1(interp, env, "format-message", &args)
        };
        // Each answer's visible form feeds later prompts' formats; the
        // no-I/O codes leave nil there exactly as GNU's visargs do.
        let mut seen = Value::Nil;
        match code {
            'a' | 'C' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let obarray = interp
                    .lookup_var_key(cached_symbol!("obarray"), env)
                    .unwrap_or(Value::Nil);
                let predicate =
                    Value::Symbol(if code == 'a' { "fboundp" } else { "commandp" }.into());
                let name = call1(
                    interp,
                    env,
                    "completing-read",
                    &[message, obarray, predicate, Value::T],
                )?;
                let text = crate::lisp::primitives::string_text(&name)?;
                seen = Value::String(text.clone().into());
                values.push(Value::Symbol(text.into()));
            }
            'b' | 'B' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let current = call1(interp, env, "current-buffer", &[])?;
                let in_minibuffer = call1(interp, env, "window-minibuffer-p", &[])
                    .map(|flag| flag.is_truthy())
                    .unwrap_or(false);
                let default = if code == 'B' || in_minibuffer {
                    call1(interp, env, "other-buffer", &[current])?
                } else {
                    current
                };
                let require = if code == 'b' { Value::T } else { Value::Nil };
                let name = call1(interp, env, "read-buffer", &[message, default, require])?;
                seen = name;
                values.push(name);
            }
            'c' => {
                let message = formatted_prompt(interp, env, &visible)?;
                // GNU shows the prompt in minibuffer-prompt face while
                // read-char waits on the echo area.
                let message = call1(
                    interp,
                    env,
                    "propertize",
                    &[
                        message,
                        Value::Symbol("face".into()),
                        Value::Symbol("minibuffer-prompt".into()),
                    ],
                )
                .unwrap_or_else(|_| Value::String(raw_prompt.clone().into()));
                let event = call1(interp, env, "read-char", &[message])?;
                crate::lisp::primitives::dispatch::display::clear_message_nolog(interp, env)?;
                if !matches!(event.kind(), Kind::Integer(_)) {
                    return Err(LispError::Signal("Non-character input-event".into()));
                }
                seen = call1(interp, env, "char-to-string", std::slice::from_ref(&event))?;
                values.push(event);
            }
            'd' => {
                values.push(Value::Integer(interp.buffer.borrow().point() as i64));
                vary = 1;
            }
            'D' | 'f' | 'F' | 'G' => {
                let message = formatted_prompt(interp, env, &visible)?;
                // callint.c's read_file_name helper: Fread_file_name
                // (PROMPT, nil, DEFAULT, MUSTMATCH, INITIAL, PREDICATE).
                let (default, mustmatch, initial, predicate) = match code {
                    'D' => (
                        interp
                            .lookup_var_key(cached_symbol!("default-directory"), env)
                            .unwrap_or(Value::Nil),
                        Value::Symbol("lambda".into()),
                        Value::Nil,
                        Value::Symbol("file-directory-p".into()),
                    ),
                    'f' => (
                        Value::Nil,
                        Value::Symbol("lambda".into()),
                        Value::Nil,
                        Value::Nil,
                    ),
                    'G' => (Value::Nil, Value::Nil, Value::String("".into()), Value::Nil),
                    _ => (Value::Nil, Value::Nil, Value::Nil, Value::Nil),
                };
                let name = call1(
                    interp,
                    env,
                    "read-file-name",
                    &[message, Value::Nil, default, mustmatch, initial, predicate],
                )?;
                seen = name;
                values.push(name);
            }
            'e' => {
                // The next invoking event with parameters (a mouse posn);
                // keyboard keys never have them.
                let Some(event) = keys.get(next_event).copied() else {
                    return Err(LispError::Signal(format!(
                        "{} must be bound to an event with parameters",
                        function.symbol_str().unwrap_or("command")
                    )));
                };
                values.push(event);
                vary = -1;
                next_event += 1;
                if !inhibit_mouse_event_check {
                    while next_event < keys.len() && !keys[next_event].is_cons() {
                        next_event += 1;
                    }
                }
            }
            'k' | 'K' => {
                // callint.c: Fread_key_sequence, or for K
                // Fread_key_sequence_vector without downcasing the last
                // event, the prompt in `minibuffer-prompt' face and
                // `cursor-in-echo-area' bound to t.
                let message = formatted_prompt(interp, env, &visible)?;
                let message = call1(
                    interp,
                    env,
                    "propertize",
                    &[
                        message,
                        Value::symbol("face"),
                        Value::symbol("minibuffer-prompt"),
                    ],
                )?;
                let cursor = interp.bind_special_key(
                    cached_symbol!("cursor-in-echo-area"),
                    Value::T,
                    env,
                )?;
                let events = read_key_sequence_events(interp, message, code == 'K', env);
                interp.restore_special_binding(cursor, env)?;
                let events = events?;
                let keys = crate::lisp::primitives::dispatch::event_array(&events, code == 'K');
                seen = call1(interp, env, "key-description", &[keys])?;
                // A sequence ending in a down-event: its up-event follows,
                // read and kept for `U'.
                let last = events.last().copied().unwrap_or(Value::Nil);
                let head = last.car_if_list().unwrap_or(last);
                if let Kind::Symbol(name) = head.kind()
                    && interp
                        .get_symbol_property_of(
                            &name,
                            &cached_symbol!("event-symbol-elements").with(Clone::clone),
                        )
                        .and_then(|elements| elements.cdr().ok())
                        .and_then(|modifiers| modifiers.to_vec().ok())
                        .is_some_and(|modifiers| {
                            modifiers
                                .iter()
                                .any(|modifier| modifier.is_key(cached_symbol!("down")))
                        })
                {
                    up_event = call1(interp, env, "read-event", &[])?;
                }
                values.push(keys);
            }
            'm' => {
                let mark = check_mark(interp, env, false)?;
                values.push(Value::Integer(mark as i64));
                vary = 2;
            }
            'p' => {
                values.push(prefix_numeric_value(&prefix_arg)?);
                vary = -1;
            }
            'P' => {
                values.push(prefix_arg);
                vary = -1;
            }
            // "i": an ignored argument — always nil, no I/O (window.el's
            // commands pass their INTERACTIVE params through it).
            'i' => {
                values.push(Value::Nil);
                vary = -1;
            }
            'n' | 'N' => {
                if code == 'N' && prefix_arg.is_truthy() {
                    values.push(prefix_numeric_value(&prefix_arg)?);
                    vary = -1;
                } else {
                    let message = formatted_prompt(interp, env, &visible)?;
                    let number = call1(interp, env, "read-number", &[message])?;
                    seen = call1(
                        interp,
                        env,
                        "number-to-string",
                        std::slice::from_ref(&number),
                    )
                    .unwrap_or(Value::Nil);
                    values.push(number);
                }
            }
            'r' => {
                let mark = check_mark(interp, env, true)?;
                let point = interp.buffer.borrow().point();
                values.push(Value::Integer(point.min(mark) as i64));
                values.push(Value::Integer(point.max(mark) as i64));
                // Two arguments: the region's beginning, then its end.
                varies.push(3);
                visible.push(Value::Nil);
                vary = 4;
            }
            's' | 'M' => {
                let message = formatted_prompt(interp, env, &visible)?;
                // 'M' inherits the input method; the tty session reads
                // plain strings either way.
                let inherit = if code == 'M' { Value::T } else { Value::Nil };
                let text = call1(
                    interp,
                    env,
                    "read-string",
                    &[message, Value::Nil, Value::Nil, Value::Nil, inherit],
                )?;
                seen = text;
                values.push(text);
            }
            'S' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let text = call1(
                    interp,
                    env,
                    "read-string",
                    &[message, Value::Nil, Value::Nil, Value::Nil, Value::Nil],
                )?;
                seen = text;
                values.push(Value::Symbol(
                    crate::lisp::primitives::string_text(&text)?.into(),
                ));
            }
            'U' => {
                // The up-event a preceding k or K read, as a vector, used
                // once.
                if up_event.is_nil() {
                    values.push(Value::Nil);
                } else {
                    let keys = Value::vector([up_event]);
                    up_event = Value::Nil;
                    seen = call1(interp, env, "key-description", &[keys])?;
                    values.push(keys);
                }
            }
            'v' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let variable = call1(interp, env, "read-variable", &[message])?;
                seen = interp
                    .lookup_var_key(cached_symbol!("minibuffer-history"), env)
                    .and_then(|history| history.car_if_list())
                    .unwrap_or(Value::Nil);
                values.push(variable);
            }
            'x' | 'X' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let reader = if code == 'x' {
                    "read-minibuffer"
                } else {
                    "eval-minibuffer"
                };
                let form = call1(interp, env, reader, &[message])?;
                values.push(form);
            }
            'z' => {
                let message = formatted_prompt(interp, env, &visible)?;
                let coding = call1(interp, env, "read-coding-system", &[message, Value::Nil])?;
                values.push(coding);
            }
            'Z' => {
                if prefix_arg.is_truthy() {
                    let message = formatted_prompt(interp, env, &visible)?;
                    let coding = call1(interp, env, "read-non-nil-coding-system", &[message])?;
                    values.push(coding);
                } else {
                    values.push(Value::Nil);
                    vary = -1;
                }
            }
            _ => return Err(invalid_interactive_control_letter(interp, code)),
        }
        if seen.is_nil()
            && let Some(Kind::StringObject(_)) = values.last().map(|v| v.kind())
        {
            seen = values.last().cloned().unwrap_or(Value::Nil);
        }
        visible.push(seen);
        varies.push(vary);
    }
    Ok(InteractiveStringArgs {
        values,
        varies,
        record_then_fail,
    })
}

// Terminal-driven event input, installed by the tty frontend for the
// duration of an interactive session.  The reader blocks on the terminal
// and returns one event, or `None' when the terminal is gone; without a
// reader the queued-events contract below is unchanged.
thread_local! {
    static TTY_EVENT_READER: std::cell::RefCell<Option<TtyEventReader>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) type TtyEventReader = Box<dyn FnMut(&mut Interpreter) -> Option<Value>>;

// A non-blocking companion to the reader: wait briefly for one event and
// answer None when the terminal stays quiet.  Blocking reads poll
// through it so ripe timers keep firing while a command waits for input
// (GNU's read_char runs timer_check inside its wait) — the minibuffer's
// own reads included.
thread_local! {
    static TTY_EVENT_POLLER: std::cell::RefCell<Option<TtyEventPoller>> =
        const { std::cell::RefCell::new(None) };
    static TTY_CURSOR_IN_ECHO_AREA: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

pub(crate) type TtyEventPoller = Box<dyn FnMut(&mut Interpreter) -> Option<Option<Value>>>;

pub(crate) fn set_tty_event_reader(reader: Option<TtyEventReader>) {
    TTY_EVENT_READER.with_borrow_mut(|slot| *slot = reader);
}

pub(crate) fn set_tty_event_poller(poller: Option<TtyEventPoller>) {
    TTY_EVENT_POLLER.with_borrow_mut(|slot| *slot = poller);
}

fn read_via_tty_event_reader(
    interp: &mut Interpreter,
    cursor_in_echo_area: bool,
) -> Option<Option<Value>> {
    TTY_CURSOR_IN_ECHO_AREA.set(cursor_in_echo_area);
    let result =
        TTY_EVENT_READER.with_borrow_mut(|slot| slot.as_mut().map(|reader| reader(interp)));
    TTY_CURSOR_IN_ECHO_AREA.set(false);
    result
}

fn poll_via_tty_event_poller(
    interp: &mut Interpreter,
    cursor_in_echo_area: bool,
) -> Option<Option<Option<Value>>> {
    TTY_CURSOR_IN_ECHO_AREA.set(cursor_in_echo_area);
    let result =
        TTY_EVENT_POLLER.with_borrow_mut(|slot| slot.as_mut().map(|poller| poller(interp)));
    TTY_CURSOR_IN_ECHO_AREA.set(false);
    result
}

pub(crate) fn tty_cursor_in_echo_area() -> bool {
    TTY_CURSOR_IN_ECHO_AREA.get()
}

/// Whether a terminal frontend is feeding events this session; the
/// minibuffer reads through its own event loop when one is.
pub(crate) fn has_tty_event_reader() -> bool {
    TTY_EVENT_READER.with_borrow(|slot| slot.is_some())
}

/// What a pending key sequence resolves to under the live keymaps.
pub(crate) enum KeyResolution {
    Command(Value),
    Prefix,
    Undefined,
}

fn command_loop_call(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    interp.call_function_value(Value::Symbol(name.into()), None, args, env)
}

// GNU's kboard->echo_string for the frontend: the echo of a key
// sequence that is still pending while its read blocks (the mouse-menu
// popup).  The blocking reader displays it once `echo-keystrokes' idle
// seconds pass, exactly as read_char's echo timer does.
thread_local! {
    static PENDING_KEYSTROKE_ECHO: std::cell::RefCell<
        Option<(String, crate::lisp::primitives::EchoSpans)>,
    > = const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_pending_keystroke_echo(
    echo: Option<(String, crate::lisp::primitives::EchoSpans)>,
) {
    PENDING_KEYSTROKE_ECHO.with_borrow_mut(|slot| *slot = echo);
}

pub(crate) fn take_pending_keystroke_echo() -> Option<(String, crate::lisp::primitives::EchoSpans)>
{
    PENDING_KEYSTROKE_ECHO.with_borrow_mut(|slot| slot.take())
}

/// keyboard.c's echo pipeline for a pending key sequence: echo_add_key
/// renders each event space-separated (a composite event echoes its
/// bare head symbol name, a character its key description), echo_dash
/// appends the trailing dash, and — when `echo-keystrokes-help' is on —
/// help.el's own `help--append-keystrokes-help' decides whether the C-h
/// hint follows, reading `this-single-command-keys' against the active
/// maps and propertizing the key through substitute-command-keys.
pub(crate) fn pending_keystroke_echo(
    interp: &mut Interpreter,
    env: &mut Env,
    pending: &[Value],
) -> (String, crate::lisp::primitives::EchoSpans) {
    let mut text = String::new();
    for event in pending {
        if !text.is_empty() {
            text.push(' ');
        }
        let head = match event.kind() {
            Kind::Cons(_) => event.car().unwrap_or(Value::Nil),
            other => other.value(),
        };
        match head.kind() {
            Kind::Symbol(name) => text.push_str(name.as_str()),
            other => {
                let description = super::call(
                    interp,
                    "single-key-description",
                    std::slice::from_ref(&other.value()),
                    env,
                )
                .ok()
                .and_then(|value| crate::lisp::primitives::string_text(&value).ok());
                match description {
                    Some(description) => text.push_str(&description),
                    None => text.push_str(&format!("{other}")),
                }
            }
        }
    }
    // echo_dash: the temporary trailing dash of an unfinished sequence.
    text.push('-');
    let help_wanted = interp
        .lookup_var_key(cached_symbol!("echo-keystrokes-help"), env)
        .is_none_or(|value| value.is_truthy());
    if help_wanted
        && interp
            .lookup_function_key(cached_symbol!("help--append-keystrokes-help"), env)
            .is_ok()
    {
        // The Lisp side reads this-single-command-keys, exactly what
        // read_key_sequence has accumulated at this point.
        interp.keyboard_input.command_keys = pending.to_vec();
        interp.keyboard_input.single_command_start = 0;
        // help.el's function is interpreted Lisp: route through the
        // full function channel, not the builtin dispatch.
        if let Ok(appended) = interp.call_function_value(
            Value::Symbol("help--append-keystrokes-help".into()),
            Some("help--append-keystrokes-help"),
            &[Value::String(text.clone().into())],
            env,
        ) && let Ok(appended_text) = crate::lisp::primitives::string_text(&appended)
        {
            let spans = crate::lisp::primitives::string_face_spans(&appended);
            return (appended_text, spans);
        }
    }
    (text, Vec::new())
}

/// keyboard.c:EVENT_HEAD_KIND reads the live symbol property. A mouse
/// event need not have "mouse" in its name, including after translation.
fn mouse_event_head(interp: &Interpreter, event: &Value) -> Option<Value> {
    let (head, tail) = event.cons_values()?;
    let kind = interp.get_property_of_symbol_value(head, "event-kind")?;
    if tail.car_if_list()?.is_cons()
        && (crate::lisp::types::eq(kind, Value::symbol("mouse-click"))
            || crate::lisp::types::eq(kind, Value::symbol("touchscreen")))
    {
        Some(head)
    } else {
        None
    }
}

/// Whether a click event's posn sits on the menu-bar area.
fn mouse_event_on_menu_bar(event: &Value) -> bool {
    event
        .to_vec()
        .ok()
        .and_then(|items| items.get(1)?.to_vec().ok())
        .is_some_and(|posn| matches!(posn.get(1).map(|v| v.kind()), Some(Kind::Symbol(area)) if area.is_named("menu-bar")))
}

/// Complete command resolution using the binding already reached by the
/// incremental reader. Re-entering key-binding here would repeat live filters.
fn resolve_read_key_binding(
    interp: &mut Interpreter,
    env: &mut Env,
    binding: Value,
    keys: &[Value],
) -> Result<KeyResolution, LispError> {
    if binding.is_nil() {
        return Ok(KeyResolution::Undefined);
    }
    let mut roots = crate::lisp::alloc::RootedVec::from_vec(vec![binding, Value::Nil]);
    if let Some(map) = keymap_reference_map(interp, &binding, env) {
        roots[0] = map;
        let Some(event) = keys
            .last()
            .filter(|event| mouse_event_head(interp, event).is_some())
        else {
            return Ok(KeyResolution::Prefix);
        };
        if !has_tty_menu_executor() {
            return Ok(KeyResolution::Prefix);
        }
        let echo = pending_keystroke_echo(interp, env, keys);
        set_pending_keystroke_echo(Some(echo));
        roots[1] = super::call(interp, "x-popup-menu", &[*event, map], env)?;
        let path = roots[1].to_vec()?;
        if path.is_empty() {
            return Ok(KeyResolution::Command(Value::symbol("ignore")));
        }
        for event in path {
            roots[0] =
                keymap_access_event(interp, roots[0], event, true, env)?.unwrap_or(Value::Nil);
        }
        if roots[0].is_nil() {
            return Ok(KeyResolution::Command(Value::symbol("ignore")));
        }
    }
    if roots[0].is_symbol() {
        let remapped = command_remapping(interp, &roots[0], None, env)?;
        if !remapped.is_nil() {
            roots[0] = remapped;
        }
    }
    Ok(KeyResolution::Command(roots[0]))
}

/// Execute one resolved command with GNU's full per-command ceremony:
/// undo boundary, echo clearing, key-state publication, prefix handoff,
/// pre/post-command hooks around `call-interactively', and the
/// last-command bookkeeping.  Shared by the frame command loop and the
/// minibuffer's recursive loop.
#[cfg(test)]
pub(crate) fn execute_command_binding(
    interp: &mut Interpreter,
    env: &mut Env,
    binding: Value,
    keys: &[Value],
    last_event: Value,
) -> Result<(), LispError> {
    set_command_key_state(interp, keys.to_vec(), keys.to_vec(), env);
    execute_command_binding_inner(interp, env, binding, binding, keys, last_event, true)
}

/// The terminal reader already published the translated and raw sequences.
/// Preserve that distinction while recording the completed macro command.
pub(crate) fn execute_read_key_command_binding(
    interp: &mut Interpreter,
    env: &mut Env,
    original: Value,
    binding: Value,
    keys: &[Value],
    last_event: Value,
) -> Result<(), LispError> {
    execute_command_binding_inner(interp, env, original, binding, keys, last_event, true)
}

/// Execute a command whose input events were already recorded by a recursive
/// reader.  `read_char' records those events as they arrive; adding the same
/// resolved key sequence again would duplicate minibuffer input in a keyboard
/// macro definition.
pub(crate) fn execute_recorded_input_command_binding(
    interp: &mut Interpreter,
    env: &mut Env,
    original: Value,
    binding: Value,
    keys: &[Value],
    last_event: Value,
) -> Result<(), LispError> {
    execute_command_binding_inner(interp, env, original, binding, keys, last_event, false)
}

/// keyboard.c:1537, as a command is about to run: boundaries for the LAST
/// command's changes through simple.el's own `undo-auto--add-boundary',
/// whose amalgamation policy (fusing runs of self-inserts) native boundary
/// pushes cannot reproduce, then point_before_last_command_or_undo, which
/// is refreshed whether or not a boundary was needed (without it, a motion
/// between two edits leaves record_point the earlier edit's position).  A
/// bare runtime without simple.el keeps the plain native boundary.
pub(crate) fn add_command_undo_boundary(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    if interp
        .lookup_function_key(cached_symbol!("undo-auto--add-boundary"), env)
        .is_ok()
    {
        interp.call_function_value(
            Value::Symbol("undo-auto--add-boundary".into()),
            Some("undo-auto--add-boundary"),
            &[],
            env,
        )?;
    } else {
        interp.undo_boundary(env)?;
    }
    interp.note_point_before_last_command_or_undo();
    Ok(())
}

/// ORIGINAL is the key sequence's binding before command remapping,
/// BINDING the command that runs.
fn execute_command_binding_inner(
    interp: &mut Interpreter,
    env: &mut Env,
    original: Value,
    binding: Value,
    keys: &[Value],
    last_event: Value,
    record_keys: bool,
) -> Result<(), LispError> {
    add_command_undo_boundary(interp, env)?;
    // keyboard.c read_char wipes a lingering message when the next input
    // event arrives: the channel empties before the command runs, but the
    // glass only catches up at the next redisplay — so a command that
    // blocks with redisplay frozen (the F10 menu) keeps the old message
    // visible until an explicit `message' repaints the row.
    crate::lisp::primitives::expire_echo_area_message();
    interp.set_variable_key(cached_symbol!("last-command-event"), last_event, env);
    interp.set_variable_key(cached_symbol!("last-input-event"), last_event, env);
    if !mouse_event_on_menu_bar(&last_event) {
        interp.set_variable_key(cached_symbol!("last-nonmenu-event"), last_event, env);
    }
    // The reader has published translated command keys and the original
    // raw input separately. Commands and hooks must observe both unchanged.
    interp.set_variable_key(
        cached_symbol!("this-command-keys-vector"),
        Value::vector(keys.iter().cloned()),
        env,
    );
    // command_loop_1: Vthis_original_command is the binding before
    // remapping; Vthis_command = cmd; Vreal_this_command = cmd.
    interp.set_variable_key(cached_symbol!("this-original-command"), original, env);
    interp.set_variable_key(cached_symbol!("this-command"), binding, env);
    interp.set_variable_key(cached_symbol!("real-this-command"), binding, env);
    record_command_lossage(interp, binding);
    // read_char records terminal events as they arrive while a keyboard
    // macro is being defined.  The command's end keys are deliberately
    // provisional: `end-kbd-macro' truncates back to the last command
    // boundary, while an ordinary completed command commits them below.
    if record_keys
        && interp
            .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
            .is_some_and(|value| value.is_truthy())
    {
        interp.kbd_macro_definition.extend_from_slice(keys);
    }
    // Timers may have messed with `deactivate-mark'; the command starts
    // with it reset (keyboard.c command_loop_1, a C assignment: no new
    // local in the current buffer).
    interp.set_c_variable("deactivate-mark", Value::Nil);
    let modified_before = (
        interp.current_buffer_id(),
        interp.buffer.borrow().chars_modified_tick(),
    );
    // pre-command-hook may rewrite `this-command' (isearch's exit path
    // does); GNU executes whatever the hook left there, nil (an undefined
    // key's command too) as `undefined'.
    crate::lisp::primitives::safe_run_hooks(
        interp,
        &cached_symbol!("pre-command-hook").symbol(),
        env,
    )?;
    let dispatched = interp
        .lookup_var_key(cached_symbol!("this-command"), env)
        .unwrap_or(Value::Nil);
    // keyboard.c command_loop_1 executes the command through simple.el's
    // `command-execute' (call1 (Qcommand_execute, Vthis_command)): the
    // prefix-arg handoff (current-prefix-arg takes prefix-arg, which
    // clears, with `prefix-command-update' run), the `disabled' property
    // check routing to `disabled-command-function', and keyboard-macro
    // arrays all live there.  A bare runtime without simple.el keeps the
    // native equivalent of the prefix handoff plus call-interactively.
    let result = if dispatched.is_nil() {
        // nil means key is undefined: call0 (Qundefined).
        interp
            .call_function_value(Value::symbol("undefined"), Some("undefined"), &[], env)
            .map(|_| ())
    } else if interp
        .lookup_function_key(cached_symbol!("command-execute"), env)
        .is_ok()
    {
        command_loop_call(
            interp,
            env,
            "command-execute",
            std::slice::from_ref(&dispatched),
        )
        .map(|_| ())
    } else {
        let prefix = interp
            .lookup_var_key(cached_symbol!("prefix-arg"), env)
            .unwrap_or(Value::Nil);
        interp.set_variable_key(cached_symbol!("current-prefix-arg"), prefix, env);
        interp.set_variable_key(cached_symbol!("prefix-arg"), Value::Nil, env);
        command_loop_call(
            interp,
            env,
            "call-interactively",
            std::slice::from_ref(&dispatched),
        )
        .map(|_| ())
    };
    // A command that leaves through `throw' or another nonlocal exit never
    // reaches keyboard.c's command-completion boundary.  In particular,
    // exit-minibuffer must not run the minibuffer's post-command hooks a
    // second time after its initial pre-input boundary.  A signal goes to
    // command_loop_2's handler, cmd_error, and command_loop_1's entry then
    // runs the post-command hooks with `this-command' still the command's
    // (the caller's `cmd_error' and `command_loop_entry').
    let signalled = matches!(&result, Err(error) if !matches!(
        error.kind(),
        LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
    ));
    if result.is_ok() {
        run_post_command_hooks(interp, env)?;
        // After post-command-hook GNU deactivates the mark when the command
        // asked for it (keyboard.c calls the real `deactivate-mark' so its
        // hook runs).  Buffer-modifying primitives are insdel.c's trigger
        // for the same flag; until every native arm publishes it, a changed
        // modification tick stands in for that side of the protocol.
        let deactivate = interp
            .lookup_var_key(cached_symbol!("deactivate-mark"), env)
            .is_some_and(|value| value.is_truthy())
            || (interp.current_buffer_id() == modified_before.0
                && interp.buffer.borrow().chars_modified_tick() != modified_before.1);
        let mark_active = interp
            .lookup_var_key(cached_symbol!("mark-active"), env)
            .is_some_and(|value| value.is_truthy());
        if deactivate && mark_active {
            // `deactivate-mark' is GNU simple.el's; keyboard.c reaches it as
            // an ordinary Lisp call (call0), never through native dispatch.
            interp.call_function_value(
                Value::Symbol("deactivate-mark".into()),
                Some("deactivate-mark"),
                &[],
                env,
            )?;
        }
    }
    // GNU takes last-command from this-command AFTER the command ran: a
    // prefix command (universal-argument) restores the previous value
    // there via prefix-command-preserve-state, keeping last-command
    // stable across the C-u chain.
    // undo.c's run_undoable_change fires on the first change a command
    // makes in a buffer; at command granularity the same set is "every
    // buffer whose text tick moved" -- report each through simple.el's
    // `undo-auto--undoable-change' so the next `undo-auto--add-boundary'
    // sees it.  (Changes made by idle timers between commands are the
    // disclosed gap of this granularity.)
    if interp
        .lookup_function_key(cached_symbol!("undo-auto--undoable-change"), env)
        .is_ok()
    {
        let current_changed = interp.current_buffer_id() == modified_before.0
            && interp.buffer.borrow().chars_modified_tick() != modified_before.1;
        let switched_and_changed = interp.current_buffer_id() != modified_before.0
            && interp
                .get_buffer_by_id(modified_before.0)
                .is_some_and(|buffer| buffer.chars_modified_tick() != modified_before.1);
        if current_changed {
            interp.call_function_value(
                Value::Symbol("undo-auto--undoable-change".into()),
                Some("undo-auto--undoable-change"),
                &[],
                env,
            )?;
        }
        if switched_and_changed {
            let here = interp.current_buffer_id();
            if interp.set_current_buffer_id(modified_before.0).is_ok() {
                let reported = interp.call_function_value(
                    Value::Symbol("undo-auto--undoable-change".into()),
                    Some("undo-auto--undoable-change"),
                    &[],
                    env,
                );
                let _ = interp.set_current_buffer_id(here);
                reported?;
            }
        }
    }
    clear_command_keys(interp);
    if signalled {
        return result;
    }
    // A throw leaves command_loop_1 for its catch here: neither
    // `last-command' nor `this-command' is touched (exit-minibuffer and
    // abort-minibuffers leave themselves in `this-command').
    result?;
    let last_command = interp
        .lookup_var_key(cached_symbol!("this-command"), env)
        .filter(|command| !command.is_nil())
        .unwrap_or(dispatched);
    interp.set_variable_key(cached_symbol!("last-command"), last_command, env);
    // keyboard.c:1585: real-last-command takes real-this-command after
    // the command ran (execute-extended-command sets real-this-command
    // to the invoked function, and its suggestion timer compares
    // against real-last-command).
    let real_last = interp
        .lookup_var_key(cached_symbol!("real-this-command"), env)
        .unwrap_or(Value::Nil);
    interp.set_variable_key(cached_symbol!("real-last-command"), real_last, env);
    // keyboard.c takes last-prefix-arg from current-prefix-arg after the
    // command ran (command-execute moved prefix-arg there).
    let last_prefix = interp
        .lookup_var_key(cached_symbol!("current-prefix-arg"), env)
        .unwrap_or(Value::Nil);
    interp.set_variable_key(cached_symbol!("last-prefix-arg"), last_prefix, env);
    if interp
        .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
        .is_some_and(|value| value.is_truthy())
    {
        interp.kbd_macro_committed_len = interp.kbd_macro_definition.len();
    }
    // keyboard.c:1421 (command_loop_1): before waiting for the next key
    // sequence, `this-command' and its shadows go nil -- Lisp that runs
    // between commands (idle timers; eldoc's `(not this-command)' guard)
    // must see no command executing.
    interp.set_variable_key(cached_symbol!("this-command"), Value::Nil, env);
    interp.set_variable_key(cached_symbol!("real-this-command"), Value::Nil, env);
    interp.set_variable_key(cached_symbol!("this-original-command"), Value::Nil, env);
    Ok(())
}

// keyboard.c's timer_check in miniature: while the command loop waits
// for input, fire the ripe entries of `timer-list' (absolute times) and
// `timer-idle-list' (idle durations, once per idle period) through
// timer.el's own `timer-event-handler'.  The idle clock below backs
// `current-idle-time'; isearch's lazy highlight arrives this way.
thread_local! {
    static TTY_IDLE_START: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

// keyboard.c timer_start_idle: on entering the idle state (and only on
// entering it), every idle timer becomes a candidate again -- GNU calls
// timer.el's `internal-timer-start-idle' to clear the triggered flags,
// and so do we.  The recorded instant backs `current-idle-time'.
pub(crate) fn tty_note_idle_start(interp: &mut Interpreter, env: &mut Env) {
    let already_idle = TTY_IDLE_START.with(|cell| cell.get().is_some());
    if already_idle {
        return;
    }
    TTY_IDLE_START.with(|cell| cell.set(Some(std::time::Instant::now())));
    if interp
        .lookup_function_key(cached_symbol!("internal-timer-start-idle"), env)
        .is_ok()
    {
        let _ = interp.call_function_value(
            Value::Symbol("internal-timer-start-idle".into()),
            Some("internal-timer-start-idle"),
            &[],
            env,
        );
    }
}

// keyboard.c timer_stop_idle: input ends the idle period.
pub(crate) fn tty_note_idle_end() {
    TTY_IDLE_START.with(|cell| cell.set(None));
}

// Fcurrent_idle_time's backing state: the elapsed idle span, when idle.
pub(crate) fn tty_current_idle_duration() -> Option<std::time::Duration> {
    TTY_IDLE_START
        .with(|cell| cell.get())
        .map(|start| start.elapsed())
}

/// keyboard.c's decode_timer: the timer vector's exact timestamp, compared
/// against the C clock.  Going back through the Lisp accessors here is
/// observably wrong: applications are free to advise or redefine
/// `float-time' for presentation, and that must not make every delayed
/// timer immediately ripe.
fn decode_timer(interp: &Interpreter, timer: &Value) -> Option<ExactTimeValue> {
    let slots = vector_items(timer).ok()?;
    // Exactly ten slots, an untriggered timer (vec[0] nil -- on BOTH timer
    // lists), and a fixnum USECS slot; anything else "is not a proper
    // timer" and is skipped.
    if slots.len() != 10 || slots[0].is_truthy() || !matches!(slots[2].kind(), Kind::Integer(_)) {
        return None;
    }
    let _ = interp;
    let time = decode_time_components(
        TimeForm::HiLoUsPs,
        &slots[1],
        &slots[2],
        &slots[3],
        &slots[8],
    )?;
    // lisp_to_timespec: the timer's time floored to whole nanoseconds,
    // and valid.
    let (seconds, nanoseconds) = lisp_to_timespec(&time)?;
    Some(ExactTimeValue::from_timespec(seconds, nanoseconds))
}

/// keyboard.c:timer_check.  Copies of `timer-list' and, while Emacs is
/// idle (IDLE_SECONDS finite), of `timer-idle-list' are walked by
/// timer_check_2 until no timer in them is ripe: each pass runs the one
/// ripe timer that is most overdue across both lists, marked triggered
/// first so the walk passes over it, through `timer-event-handler' with
/// `inhibit-quit' bound and `deactivate-mark' kept.  A timer added by a
/// callback is not in the copies, so it waits for the next check.  How
/// long until the first timer still waiting is ripe is left in
/// `timer_check_delay', which bounds a wait as timer_delay does.
pub(crate) fn run_due_timers(
    interp: &mut Interpreter,
    env: &mut Env,
    idle_seconds: f64,
) -> Result<bool, LispError> {
    let mut ran = false;
    let delay = timer_check(interp, env, idle_seconds, &mut ran);
    interp.timer_check_delay = delay.as_ref().ok().copied().flatten();
    delay.map(|_| ran)
}

fn timer_check(
    interp: &mut Interpreter,
    env: &mut Env,
    idle_seconds: f64,
    ran: &mut bool,
) -> Result<Option<std::time::Duration>, LispError> {
    if interp
        .lookup_function_key(cached_symbol!("timer-event-handler"), env)
        .is_err()
    {
        return Ok(None);
    }
    // keyboard.c:timer_check keeps its copied lists reachable across
    // callbacks. A Rust heap Vec is outside the conservative stack:
    // cancel-timer followed by GC must not free its remaining entries.
    let copy = |interp: &Interpreter, env: &Env, name: &str| {
        crate::lisp::alloc::RootedVec::from_vec(
            interp
                .lookup_var(name, env)
                .and_then(|value| value.to_vec().ok())
                .unwrap_or_default(),
        )
    };
    let timers = copy(interp, env, "timer-list");
    let idle_timers = if idle_seconds.is_finite() {
        copy(interp, env, "timer-idle-list")
    } else {
        crate::lisp::alloc::RootedVec::new()
    };
    let started = std::time::Instant::now();
    loop {
        let Ok(now) = current_time_value() else {
            return Ok(None);
        };
        let idleness_now = decode_float_time(idle_seconds + started.elapsed().as_secs_f64());
        // timer_check_2: the first proper timer left on each list, the
        // ripe one longest overdue (or, none ripe, the soonest) chosen.
        let next = |list: &[Value], clock: &ExactTimeValue| {
            list.iter().find_map(|timer| {
                let time = decode_timer(interp, timer)?;
                let ripe = !exact_time_less(clock, &time);
                let difference = (exact_time_to_f64(clock) - exact_time_to_f64(&time)).abs();
                Some((*timer, ripe, difference))
            })
        };
        let timer = next(&timers, &now);
        let idle = next(&idle_timers, &idleness_now);
        let pick_timer = match (timer, idle) {
            (None, None) => return Ok(None),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some((_, timer_ripe, timer_difference)), Some((_, idle_ripe, idle_difference))) => {
                !idle_ripe && timer_ripe
                    || (idle_ripe == timer_ripe
                        && if timer_ripe {
                            idle_difference < timer_difference
                        } else {
                            timer_difference < idle_difference
                        })
            }
        };
        let (timer, ripe, difference) = if pick_timer {
            timer.expect("chosen ordinary timer")
        } else {
            idle.expect("chosen idle timer")
        };
        // A timer still waiting ends the check.
        if !ripe {
            return Ok(Some(
                std::time::Duration::try_from_secs_f64(difference)
                    .unwrap_or(std::time::Duration::MAX),
            ));
        }
        *ran = true;
        super::call(interp, "aset", &[timer, Value::Integer(0), Value::T], env)?;
        let old_deactivate_mark = interp
            .lookup_var_key(cached_symbol!("deactivate-mark"), env)
            .unwrap_or(Value::Nil);
        let inhibit_quit =
            interp.bind_special_key(cached_symbol!("inhibit-quit"), Value::T, env)?;
        interp.begin_timer_callback();
        let outcome = interp.call_function_value(
            Value::Symbol("timer-event-handler".into()),
            Some("timer-event-handler"),
            std::slice::from_ref(&timer),
            env,
        );
        interp.end_timer_callback();
        if outcome.is_ok() {
            interp.set_c_variable("deactivate-mark", old_deactivate_mark);
        }
        interp.restore_special_binding(inhibit_quit, env)?;
        outcome?;
        // The next timer_check_2 pass runs what the callback deferred.
        interp.run_pending_funcalls(env)?;
    }
}

/// The echo-area text for a command's error, GNU's
/// `error-message-string' rendering ("Quit", "Beginning of buffer", a
/// user-error's own message).
pub(crate) fn command_error_echo_text(
    interp: &mut Interpreter,
    env: &mut Env,
    error: &LispError,
) -> String {
    let text = match error.kind() {
        LispErrorKind::SignalValue(data) => {
            let data = if matches!(data.kind(), Kind::Symbol(_)) {
                Value::list([*data])
            } else {
                *data
            };
            command_loop_call(
                interp,
                env,
                "error-message-string",
                std::slice::from_ref(&data),
            )
            .ok()
            .and_then(|value| string_like(&value).map(|text| text.text))
            .unwrap_or_else(|| format!("{data}"))
        }
        LispErrorKind::Signal(text) => text.clone(),
        other => format!("{other:?}"),
    };
    text.replace(['\n', '\r'], " ").chars().take(200).collect()
}

// Frame repaint, installed alongside the event reader.  Command code
// that runs its own event loop (the interactive minibuffer) calls it so
// window-configuration changes made mid-read — a *Completions* pop-up —
// reach the glass before the next key blocks.
thread_local! {
    static TTY_FRAME_REDRAW: std::cell::RefCell<Option<TtyFrameRedraw>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) type TtyFrameRedraw = Box<dyn FnMut(&mut Interpreter, &mut Env)>;

pub(crate) fn set_tty_frame_redraw(hook: Option<TtyFrameRedraw>) {
    TTY_FRAME_REDRAW.with_borrow_mut(|slot| *slot = hook);
}

/// Run the frontend's frame repaint if one is installed.  The hook is
/// taken out for the call: redisplay evaluates Lisp (mode lines), which
/// must not re-enter the hook cell.
pub(crate) fn run_tty_frame_redraw(interp: &mut Interpreter, env: &mut Env) {
    let hook = TTY_FRAME_REDRAW.with_borrow_mut(std::mem::take);
    if let Some(mut hook) = hook {
        hook(interp, env);
        TTY_FRAME_REDRAW.with_borrow_mut(|slot| {
            if slot.is_none() {
                *slot = Some(hook);
            }
        });
    }
}

// read_char consumes the actual queue spine. Retaining its cdr preserves
// sharing and lets a filter mutate the very next input event in place.
fn pop_unread_event_cell(interp: &mut Interpreter, env: &mut Env) -> Option<Value> {
    let (event, tail) = interp
        .lookup_var_key(cached_symbol!("unread-command-events"), env)?
        .cons_values()?;
    interp.with_lisp_stack_roots(&event, |interp| {
        interp.set_variable_key(cached_symbol!("unread-command-events"), tail, env);
    });
    Some(event)
}

pub(crate) fn pop_pending_input_event_value(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    if let Some(event) = take_unread_command_event(interp, env) {
        record_external_input_event(interp, &event, env);
        return Ok(Some(event));
    }
    run_pending_user_signal_events(interp, env)?;
    if let Some(event) = take_unread_command_event(interp, env) {
        record_external_input_event(interp, &event, env);
        return Ok(Some(event));
    }
    // GNU's input readers consume the executing keyboard macro's
    // remaining events (viper's `F'/`t' read their target char that way).
    // Lisp hooks may have rewound the public index after a speculative
    // read (kmacro's quoted-insert step editor does exactly this), so the
    // next read observes the actual array and public index directly.
    crate::lisp::primitives::dispatch::next_kbd_macro_event(interp, env)
}

/// read_char for `read-event' and its kin: C-g typed while it waits
/// leaves `quit-flag' set, so the read quits.
pub(crate) fn pop_unread_command_event_value(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    read_command_event(interp, env, false)
}

/// read_char under read_key_sequence: handle_interrupt's longjmp makes
/// read_char return C-g as an event, and read_key_sequence clears
/// `quit-flag' after each key it reads, so C-g is looked up like any key
/// (`keyboard-quit', the minibuffer's `abort-minibuffers').
pub(crate) fn read_key_sequence_event(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let event = read_command_event(interp, env, true)?;
    interp.set_c_variable("quit-flag", Value::Nil);
    Ok(event)
}

fn read_command_event(
    interp: &mut Interpreter,
    env: &mut Env,
    quit_char_is_event: bool,
) -> Result<Value, LispError> {
    if let Some(event) = pop_pending_input_event_value(interp, env)? {
        return Ok(event);
    }
    // Poll for the event so ripe timers fire during the wait, as
    // GNU's read_char does; the blocking reader stands in when the
    // frontend installed no poller.
    let mut idle_start: Option<std::time::Instant> = None;
    let cursor_in_echo_area = interp
        .lookup_var_key(cached_symbol!("cursor-in-echo-area"), env)
        .is_some_and(|value| value.is_truthy());
    while let Some(step) = poll_via_tty_event_poller(interp, cursor_in_echo_area) {
        match step {
            None => return Err(LispError::SignalValue(Value::list([Value::symbol("quit")]))),
            Some(Some(event)) => {
                if event == Value::Integer(7) && !quit_char_is_event {
                    return Err(LispError::SignalValue(Value::list([Value::symbol("quit")])));
                }
                interp.keyboard_input.last_event_reread = false;
                record_external_input_event(interp, &event, env);
                return Ok(event);
            }
            Some(None) => {
                let idle = idle_start
                    .get_or_insert_with(std::time::Instant::now)
                    .elapsed();
                let mut process_progress =
                    crate::lisp::primitives::processes::pump_external_process_output(interp, env)?;
                process_progress |=
                    crate::lisp::primitives::processes::pump_connection_processes(interp, env)?;
                if process_progress
                    || interp.service_async_runtime_events(env, true, Some(idle.as_secs_f64()))?
                {
                    // A blocking Lisp reader owns the command thread, so
                    // the outer terminal loop cannot observe asynchronous
                    // work.  Redisplay here, as read_char does after
                    // wait_reading_process_output.
                    run_tty_frame_redraw(interp, env);
                }
                if let Some(event) = take_unread_command_event(interp, env) {
                    record_external_input_event(interp, &event, env);
                    return Ok(event);
                }
            }
        }
    }
    if let Some(read) = read_via_tty_event_reader(interp, cursor_in_echo_area) {
        return match read {
            Some(event) if event != Value::Integer(7) || quit_char_is_event => {
                interp.keyboard_input.last_event_reread = false;
                record_external_input_event(interp, &event, env);
                Ok(event)
            }
            _ => Err(LispError::SignalValue(Value::list([Value::symbol("quit")]))),
        };
    }
    Err(LispError::Signal(
        "No unread-command-events available for interactive input".into(),
    ))
}

pub(crate) fn normalize_input_event_value(event: Value) -> Result<Value, LispError> {
    // lread.c:Fread_event returns the event word without resolving its
    // modifier bits. In particular, a macro string's high bit becomes
    // CHAR_META in read_char and must survive this raw event reader.
    if matches!(event.kind(), Kind::Integer(_)) {
        Ok(event)
    } else if let Some(ch) = unread_event_char(&event) {
        Ok(Value::Integer(ch as i64))
    } else {
        Ok(event)
    }
}

pub(crate) fn unread_command_events(
    interp: &Interpreter,
    env: &Env,
) -> Result<Vec<Value>, LispError> {
    interp
        .lookup_var_key(cached_symbol!("unread-command-events"), env)
        .unwrap_or(Value::Nil)
        .to_vec()
}

pub(crate) fn unread_event_char(value: &Value) -> Option<char> {
    match value.kind() {
        Kind::Integer(code) if code >= 0 => modified_event_code_char(code),
        Kind::Cons(cell) if matches!(cell.car.get().kind(), Kind::T) => {
            match cell.cdr.get().kind() {
                Kind::Integer(code) if code >= 0 => modified_event_code_char(code),
                _ => None,
            }
        }
        Kind::StringObject(state) => state.borrow().text().chars().next(),
        _ => None,
    }
}

/// GNU's default `local-function-key-map' translations.  Raw event readers
/// preserve these symbols, while key sequences, character readers, and
/// minibuffer command loops consume their translated character events.
pub(crate) fn function_key_default_translation(name: &str) -> Option<i64> {
    Some(match name {
        "escape" => 27,
        "tab" => 9,
        "return" => 13,
        "linefeed" => 10,
        "delete" | "backspace" => 127,
        _ => return None,
    })
}

pub(crate) fn translated_unread_event_char(value: &Value) -> Option<char> {
    match value.kind() {
        Kind::Symbol(name) => function_key_default_translation(name.as_str())
            .and_then(|code| char::from_u32(code as u32)),
        _ => unread_event_char(value),
    }
}

fn modified_event_code_char(code: i64) -> Option<char> {
    const ALT: i64 = 1 << 22;
    const SUPER: i64 = 1 << 23;
    const HYPER: i64 = 1 << 24;
    const SHIFT: i64 = 1 << 25;
    const CONTROL: i64 = 1 << 26;
    const META: i64 = 1 << 27;
    const MODIFIERS: i64 = ALT | SUPER | HYPER | SHIFT | CONTROL | META;

    let mut base = code & !MODIFIERS;
    if code & CONTROL != 0 && base != 0 {
        base = match base {
            0x3f => 0x7f,
            n if (b'a' as i64..=b'z' as i64).contains(&n) => (n - b'a' as i64) + 1,
            n if (b'A' as i64..=b'Z' as i64).contains(&n) => (n - b'A' as i64) + 1,
            n => n & 0x1f,
        };
    } else if code & SHIFT != 0
        && let Some(ch) = char::from_u32(base as u32)
    {
        base = ch.to_ascii_uppercase() as i64;
    }
    char::from_u32(base as u32)
}

pub(crate) fn translated_input_events(value: &Value) -> Result<Vec<Value>, LispError> {
    if matches!(value.kind(), Kind::Nil) {
        return Ok(Vec::new());
    }
    if let Ok(items) = vector_items(value) {
        return Ok(items);
    }
    if let Some(string) = string_like(value) {
        return Ok(string
            .character_codes()
            .into_iter()
            .map(Value::Integer)
            .collect());
    }
    Ok(vec![*value])
}

pub(crate) fn is_mouse_down_event(value: &Value) -> bool {
    value
        .to_vec()
        .ok()
        .and_then(|items| items.first().cloned())
        .and_then(|item| item.symbol_str().map(str::to_string))
        .is_some_and(|name| name.contains("down-mouse"))
}

pub(crate) fn is_declare_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(
        |items| matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(name)) if name.is_named("declare")),
    )
}

pub(crate) fn invalid_interactive_control_letter(interp: &Interpreter, ch: char) -> LispError {
    let code = raw_byte_from_regex_char(ch)
        .map(u32::from)
        .unwrap_or(ch as u32);
    let display = char::from_u32(code).unwrap_or(ch);
    crate::lisp::primitives::values::doprnt_error(
        interp,
        "Invalid control letter `{}' (#o{}, #x{}) in interactive calling string",
        &[&display, &format!("{:03o}", code), &format!("{:04x}", code)],
    )
}

#[inline]
pub(crate) fn need_args(name: &str, args: &[Value], n: usize) -> Result<(), LispError> {
    if args.len() < n {
        return Err(wrong_number_of_args(name, args.len()));
    }
    Ok(())
}

#[inline]
pub(crate) fn need_arg_range(
    name: &str,
    args: &[Value],
    min: usize,
    max: usize,
) -> Result<(), LispError> {
    if args.len() < min || args.len() > max {
        return Err(wrong_number_of_args(name, args.len()));
    }
    Ok(())
}

/// The `wrong-number-of-arguments' error of a call of NAME with COUNT
/// arguments, out of the callers' way.
#[cold]
#[inline(never)]
fn wrong_number_of_args(name: &str, count: usize) -> LispError {
    LispError::WrongNumberOfArgs(name.into(), count)
}

/// keyboard.c's menu_bar_items: the menu bar's top-level captions in
/// display order.  Every active keymap's `menu-bar' prefix is scanned
/// lowest-precedence first (global map, then local, then minor modes),
/// so global menus lead the row and higher-precedence maps append or
/// merge into existing entries; the keys named by
/// `menu-bar-final-items' move to the end (Help).
pub(crate) fn menu_bar_row_captions(interp: &mut Interpreter, env: &mut Env) -> Vec<String> {
    menu_bar_row_items(interp, env)
        .into_iter()
        .map(|(caption, _, _)| caption)
        .collect()
}

/// The menu bar's items with their display geometry: (CAPTION, KEY,
/// COLUMN), column being where the caption starts on the row — the
/// coordinates menu.c's menu-bar-menu-at-x-y answers from.
pub(crate) fn menu_bar_row_items(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Vec<(String, Value, usize)> {
    let maps = crate::lisp::alloc::RootedVec::from_vec(
        super::call(interp, "current-active-maps", &[Value::T], env)
            .ok()
            .and_then(|maps| maps.to_vec().ok())
            .unwrap_or_default(),
    );
    let menu_bar_key = Value::vector([Value::symbol("menu-bar")]);
    let mut items = crate::lisp::alloc::RootedVec::<(Value, String)>::new();
    interp.with_lisp_stack_roots(&menu_bar_key, |interp| {
        for map in maps.iter().rev() {
            let Ok(menu) = super::call(interp, "lookup-key", &[*map, menu_bar_key], env) else {
                continue;
            };
            if !super::is_keymap_value(interp, &menu) {
                continue;
            }
            // keyboard.c:menu_bar_items uses map_keymap_canonical, whose
            // safe call to unchanged subr.el resolves included maps,
            // inherited prefixes and duplicate definitions before display.
            let Ok(menu) = interp.call_function_value(
                Value::symbol("keymap-canonicalize"),
                Some("keymap-canonicalize"),
                &[menu],
                env,
            ) else {
                continue;
            };
            let _ = super::map_keymap_own_entries(
                interp,
                menu,
                &mut |interp, key, item, env| {
                    if crate::lisp::types::eq_symbol_named(item, "undefined") {
                        items.retain(|(existing, _)| !crate::lisp::types::eq(*existing, key));
                    } else if let Some(caption) = menu_item_caption(interp, env, &item)
                        && !items
                            .iter()
                            .any(|(existing, _)| crate::lisp::types::eq(*existing, key))
                    {
                        items.push((key, caption));
                    }
                    Ok(())
                },
                env,
            );
        }
    });
    if let Some(final_items) = interp
        .lookup_var_key(cached_symbol!("menu-bar-final-items"), env)
        .and_then(|value| value.to_vec().ok())
    {
        for name in final_items {
            if let Some(position) = items
                .iter()
                .position(|(key, _)| crate::lisp::types::eq(*key, name))
            {
                let item = items.remove(position);
                items.push(item);
            }
        }
    }
    let mut column = 0usize;
    items
        .into_iter()
        .map(|(key, caption)| {
            let start = column;
            column += caption.chars().count() + 1;
            (caption, key, start)
        })
        .collect()
}

/// parse_menu_item for the menu bar: the caption of a live top-level
/// item, or None when the item is invisible, disabled, undefined, or
/// not a menu item at all.
fn menu_item_caption(interp: &mut Interpreter, env: &mut Env, item: &Value) -> Option<String> {
    let (caption, def, enabled) = menu_item_details(interp, env, item)?;
    // The menu bar drops disabled and definition-less items
    // (parse_menu_item's inmenubar rules); a dropdown keeps them greyed.
    (enabled && !def.is_nil()).then_some(caption)
}

/// parse_menu_item for a dropdown pane: (CAPTION, DEF, ENABLED) of a
/// visible item — disabled and unselectable entries stay, drawn in
/// tty-menu-disabled-face.  None only for invisible or non-items.
pub(crate) fn menu_item_details(
    interp: &mut Interpreter,
    env: &mut Env,
    item: &Value,
) -> Option<(String, Value, bool)> {
    menu_item_details_with_button(interp, env, item).map(|(c, d, e, _)| (c, d, e))
}

/// A parsed pane item: (CAPTION, DEF, ENABLED, BUTTON), button being
/// the :button spec's (TYPE-KEYWORD, SELECTED) for toggle/radio items —
/// menu.c's checkbox prefix source.
pub(crate) type MenuItemDetails = (String, Value, bool, Option<(String, bool)>);

/// menu_item_details plus the :button spec.
pub(crate) fn menu_item_details_with_button(
    interp: &mut Interpreter,
    env: &mut Env,
    item: &Value,
) -> Option<MenuItemDetails> {
    if !matches!(item.kind(), Kind::Cons(_)) {
        return None;
    }
    let eval_property = |interp: &mut Interpreter, env: &mut Env, form: &Value| match form.kind() {
        Kind::Symbol(name) if !name.is_named("t") && !name.is_named("nil") => {
            interp.lookup_var_symbol(&name, env).unwrap_or(Value::Nil)
        }
        Kind::Cons(_) => interp.eval(form, env).unwrap_or(Value::Nil),
        other => other.value(),
    };
    let car = item.car_if_list()?;
    if let Ok(name) = crate::lisp::primitives::string_text(&car) {
        // Old format (NAME [HELP-STRING] [CACHE] . DEF).
        let mut def = item.cdr_if_list()?;
        if def.car().is_ok_and(|help| help.is_string()) {
            def = def.cdr_if_list()?;
        }
        // An obsolete key binding cache: a cons whose car is nil or a vector.
        if def.car().is_ok_and(|cache| {
            matches!(
                cache.car().map(|v| v.kind()),
                Ok(Kind::Nil | Kind::Vector(_))
            )
        }) {
            def = def.cdr_if_list()?;
        }
        // Unselectable text (a separator) still draws in the enabled
        // face; only the menu bar drops definition-less items.
        return Some((name, def, true, None));
    }
    if !crate::lisp::types::eq_symbol_named(car, "menu-item") {
        return None;
    }
    // New format (menu-item NAME DEF [CACHE] . PROPS).
    let rest = item.cdr_if_list()?.to_vec().ok()?;
    let name_form = *rest.first()?;
    let mut def = rest.get(1).cloned().unwrap_or(Value::Nil);
    let mut index = 2;
    if matches!(rest.get(2).map(|v| v.kind()), Some(Kind::Cons(_))) {
        index = 3;
    }
    let mut filter = None;
    let mut enabled = true;
    let mut button = None;
    while index + 1 < rest.len() {
        let Kind::Symbol(keyword) = rest[index].kind() else {
            break;
        };
        let value = &rest[index + 1];
        match keyword.as_ref() {
            ":visible" if eval_property(interp, env, value).is_nil() => {
                return None;
            }
            ":enable" if eval_property(interp, env, value).is_nil() => {
                enabled = false;
            }
            ":filter" => filter = Some(*value),
            ":button" => {
                if let (Ok(Kind::Symbol(kind)), Ok(selected)) =
                    (value.car().map(|v| v.kind()), value.cdr())
                {
                    let selected = eval_property(interp, env, &selected).is_truthy();
                    button = Some((kind.to_string(), selected));
                }
            }
            _ => {}
        }
        index += 2;
    }
    if let Some(filter) = filter {
        def = interp
            .call_function_value(filter, None, std::slice::from_ref(&def), env)
            .unwrap_or(Value::Nil);
    }
    let name = eval_property(interp, env, &name_form);
    let caption = crate::lisp::primitives::string_text(&name).ok()?;
    Some((caption, def, enabled, button))
}

/// The menu bar's cheap per-redraw change signal: how many minor-mode
/// maps are live plus whether an overriding map is installed.  Entering
/// isearch (or any minor mode with a map) changes it, which triggers
/// the recompute GNU gets from update_mode_lines — without walking the
/// full active-keymap set on every keystroke.
pub(crate) fn active_keymap_count(interp: &mut Interpreter, env: &mut Env) -> usize {
    let minors = interp
        .lookup_var_key(cached_symbol!("minor-mode-map-alist"), env)
        .and_then(|alist| alist.to_vec().ok())
        .map(|entries| {
            entries
                .iter()
                .filter(|entry| {
                    entry.car().is_ok_and(|mode| {
                        matches!(mode.kind(), Kind::Symbol(name)
                            if interp.lookup_var_symbol(&name, env).is_some_and(|on| on.is_truthy()))
                    })
                })
                .count()
        })
        .unwrap_or(0);
    let overriding = interp
        .lookup_var_key(cached_symbol!("overriding-terminal-local-map"), env)
        .is_some_and(|map| map.is_truthy());
    minors + usize::from(overriding)
}

/// Pop the first `unread-command-events' entry, unwrapping GNU's
/// `(t . EVENT)' requeued-event form — read_char's front of the input
/// stream, consulted before the terminal.
pub(crate) fn take_unread_command_event(interp: &mut Interpreter, env: &mut Env) -> Option<Value> {
    let event = pop_unread_event_cell(interp, env)?;
    match event.kind() {
        Kind::Cons(_) if matches!(event.car().map(|v| v.kind()), Ok(Kind::T)) => {
            interp.keyboard_input.last_event_reread = false;
            event.cdr_if_list()
        }
        _ => {
            interp.keyboard_input.last_event_reread = true;
            Some(event)
        }
    }
}

/// keyboard.c:read_char's add_command_key for an event it returned: the
/// event joins this-command-keys, one reread from `unread-command-events'
/// only as their first.
pub(crate) fn add_read_event_to_command_keys(interp: &mut Interpreter, event: Value) {
    let reread = std::mem::take(&mut interp.keyboard_input.last_event_reread);
    if !reread || interp.keyboard_input.command_keys.is_empty() {
        interp.keyboard_input.command_keys.push(event);
    }
}

// Whether terminal input is ready without consuming it — keyboard.c's
// input_pending, for `input-pending-p' and sit-for's early exit.
thread_local! {
    static TTY_INPUT_PENDING: std::cell::RefCell<Option<Box<dyn Fn() -> bool>>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_tty_input_pending_check(check: Option<Box<dyn Fn() -> bool>>) {
    TTY_INPUT_PENDING.with_borrow_mut(|slot| *slot = check);
}

pub(crate) fn tty_input_pending() -> bool {
    TTY_INPUT_PENDING.with_borrow(|slot| slot.as_ref().is_some_and(|check| check()))
}

// keyboard.c's Fdiscard_input empties the keyboard buffer (kbd_fetch_ptr =
// kbd_store_ptr): the frontend drops the input it has read from the
// terminal.
thread_local! {
    static TTY_INPUT_DISCARD: std::cell::RefCell<Option<Box<dyn Fn()>>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) fn set_tty_input_discard(discard: Option<Box<dyn Fn()>>) {
    TTY_INPUT_DISCARD.with_borrow_mut(|slot| *slot = discard);
}

pub(crate) fn discard_tty_input() {
    TTY_INPUT_DISCARD.with_borrow(|slot| {
        if let Some(discard) = slot.as_ref() {
            discard();
        }
    });
}

pub(crate) fn has_tty_event_poller() -> bool {
    TTY_EVENT_POLLER.with_borrow(|slot| slot.is_some())
}

/// A timed event read on the live terminal: the first event within
/// TIMEOUT, or None when it elapses quietly.  Ripe timers fire between
/// polls, exactly like the untimed read.
pub(crate) fn read_tty_event_with_timeout(
    interp: &mut Interpreter,
    env: &mut Env,
    timeout: std::time::Duration,
) -> Result<Option<Value>, LispError> {
    let deadline = std::time::Instant::now() + timeout;
    let started = std::time::Instant::now();
    loop {
        if let Some(event) = pop_pending_input_event_value(interp, env)? {
            return Ok(Some(event));
        }
        // A timed read is still wait_reading_process_output with keyboard
        // input in the descriptor set.  In particular, subr.el's `sit-for'
        // is the cancel-on-input wait used by jsonrpc/Eglot completion: a
        // subprocess reply and the zero-delay continuation its filter
        // schedules must run even when no key arrives.
        let mut process_progress =
            crate::lisp::primitives::processes::pump_external_process_output(interp, env)?;
        process_progress |=
            crate::lisp::primitives::processes::pump_connection_processes(interp, env)?;
        let async_progress = interp.service_async_runtime_events(
            env,
            true,
            Some(started.elapsed().as_secs_f64()),
        )?;
        if process_progress || async_progress {
            run_tty_frame_redraw(interp, env);
        }
        let cursor_in_echo_area = interp
            .lookup_var_key(cached_symbol!("cursor-in-echo-area"), env)
            .is_some_and(|value| value.is_truthy());
        let Some(step) = poll_via_tty_event_poller(interp, cursor_in_echo_area) else {
            return Ok(None);
        };
        match step {
            None => return Err(LispError::SignalValue(Value::list([Value::symbol("quit")]))),
            Some(Some(event)) => {
                if event == Value::Integer(7) {
                    return Err(LispError::SignalValue(Value::list([Value::symbol("quit")])));
                }
                interp.keyboard_input.last_event_reread = false;
                record_external_input_event(interp, &event, env);
                return Ok(Some(event));
            }
            Some(None) => {
                if std::time::Instant::now() >= deadline {
                    return Ok(None);
                }
            }
        }
    }
}

/// One dropdown pane ready for the glass: term.c's tty_menu built from
/// a menu keymap, item text already NAME-padded-plus-key-hint as
/// tty_menu_show lays it out.
pub(crate) struct TtyMenuPane {
    pub title: String,
    pub items: Vec<TtyMenuPaneItem>,
    /// Pane text width (the widest NAME+DESCRIP); the drawn box adds
    /// the two padding blanks.
    pub width: usize,
    /// tty_menu_activate's KBD_NAVIGATION: the next and previous menu
    /// commands leave the menu (for its caller to open the adjacent
    /// menu-bar menu) only when it was opened from the keyboard.
    pub kbd_navigation: bool,
}

pub(crate) struct TtyMenuPaneItem {
    pub text: String,
    pub enabled: bool,
    pub key: Value,
}

pub(crate) enum TtyMenuOutcome {
    Selected(usize),
    NextMenu,
    PrevMenu,
    /// A separator or disabled item was chosen: no selection, no quit
    /// signal (GNU's TTYM_IA_SELECT).
    NoSelect,
    Quit,
}

// The frontend's modal dropdown executor — term.c's tty_menu_activate.
thread_local! {
    static TTY_MENU_EXECUTOR: std::cell::RefCell<Option<TtyMenuExecutor>> =
        const { std::cell::RefCell::new(None) };
}

pub(crate) type TtyMenuExecutor = Box<
    dyn FnMut(
        &mut Interpreter,
        &mut Env,
        &TtyMenuPane,
        usize,
        usize,
    ) -> Result<TtyMenuOutcome, LispError>,
>;

pub(crate) fn set_tty_menu_executor(executor: Option<TtyMenuExecutor>) {
    TTY_MENU_EXECUTOR.with_borrow_mut(|slot| *slot = executor);
}

pub(crate) fn has_tty_menu_executor() -> bool {
    TTY_MENU_EXECUTOR.with_borrow(|slot| slot.is_some())
}

pub(crate) fn run_tty_menu_executor(
    interp: &mut Interpreter,
    env: &mut Env,
    pane: &TtyMenuPane,
    x: usize,
    y: usize,
) -> Result<Option<TtyMenuOutcome>, LispError> {
    let Some(mut executor) = TTY_MENU_EXECUTOR.with_borrow_mut(|slot| slot.take()) else {
        return Ok(None);
    };
    let outcome = executor(interp, env, pane, x, y);
    TTY_MENU_EXECUTOR.with_borrow_mut(|slot| {
        if slot.is_none() {
            *slot = Some(executor);
        }
    });
    outcome.map(Some)
}

/// tty_menu_show's pane construction: walk MENU's entries in keymap
/// order, item text = NAME padded to the widest name + "  KEY-HINT"
/// (parse_menu_item's equivalent-key description).
pub(crate) fn tty_menu_pane_from_keymap(
    interp: &mut Interpreter,
    env: &mut Env,
    menu: &Value,
    title: &str,
) -> TtyMenuPane {
    let menu = *menu;
    #[allow(clippy::type_complexity)]
    let mut raw: Vec<(Value, String, Value, bool, Option<(String, bool)>)> = Vec::new();
    let mut tail = menu.cdr().unwrap_or(Value::Nil);
    let mut seen: Vec<Value> = Vec::new();
    let same_key = |a: &Value, b: &Value| match (a.kind(), b.kind()) {
        (Kind::Symbol(a), Kind::Symbol(b)) => a == b,
        (Kind::Integer(a), Kind::Integer(b)) => a == b,
        _ => false,
    };
    while let Kind::Cons(_) = tail.kind() {
        let Ok(entry) = tail.car() else { break };
        let next = tail.cdr().unwrap_or(Value::Nil);
        if let Kind::Cons(_) = entry.kind() {
            let key = entry.car().unwrap_or(Value::Nil);
            let item = entry.cdr().unwrap_or(Value::Nil);
            if !seen.iter().any(|earlier| same_key(earlier, &key)) {
                seen.push(key);
                if let Some((caption, def, enabled, button)) =
                    menu_item_details_with_button(interp, env, &item)
                {
                    // A tty submenu item carries GNU's " >" marker,
                    // counted by the pane's width scan.
                    let caption = if super::is_keymap_value(interp, &def)
                        || matches!(def.kind(), Kind::Symbol(name)
                            if interp.lookup_function_symbol(&name, env)
                                .is_ok_and(|f| super::is_keymap_value(interp, &f)))
                    {
                        format!("{caption} >")
                    } else {
                        caption
                    };
                    raw.push((key, caption, def, enabled, button));
                }
            }
        }
        tail = next;
    }
    // menu.c's checkbox column: once any item in the pane is a
    // toggle/radio button, every other item gains a four-blank prefix
    // so captions line up with "[X] " — separators and empty names
    // excepted.  A buttonless pane keeps bare captions (Edit).
    let has_buttons = raw.iter().any(|(_, _, _, _, button)| button.is_some());
    let raw: Vec<(Value, String, Value, bool)> = raw
        .into_iter()
        .map(|(key, caption, def, enabled, button)| {
            let prefixed = match button {
                Some((kind, selected)) => {
                    let mark = match (kind.as_str(), selected) {
                        (":radio", true) => "(*) ",
                        (":radio", false) => "( ) ",
                        (_, true) => "[X] ",
                        (_, false) => "[ ] ",
                    };
                    format!("{mark}{caption}")
                }
                None if has_buttons && !caption.is_empty() && !caption.starts_with('-') => {
                    format!("    {caption}")
                }
                None => caption,
            };
            (key, prefixed, def, enabled)
        })
        .collect();
    // The widest NAME sets the padding column for every key hint.
    let max_name = raw
        .iter()
        .map(|(_, caption, _, _)| caption.chars().count())
        .max()
        .unwrap_or(0);
    let mut items = Vec::new();
    let mut width = 0usize;
    for (key, caption, def, enabled) in raw {
        // keyboard.c:parse_menu_item requests FIRSTONLY=t. That applies
        // the preferred-modifier rules and rejects menu paths; taking
        // the first entry of the complete binding list is different.
        let hint = if matches!(def.kind(), Kind::Symbol(_)) {
            super::call(
                interp,
                "where-is-internal",
                &[def, Value::Nil, Value::T],
                env,
            )
            .ok()
            .filter(|key| !key.is_nil())
            .and_then(|key| {
                super::call(interp, "key-description", &[key], env)
                    .ok()
                    .and_then(|description| crate::lisp::primitives::string_text(&description).ok())
            })
        } else {
            None
        };
        let text = match hint {
            Some(hint) => {
                let mut text = caption.clone();
                for _ in caption.chars().count()..max_name {
                    text.push(' ');
                }
                text.push_str("  ");
                text.push_str(&hint);
                text
            }
            None => caption,
        };
        width = width.max(text.chars().count());
        items.push(TtyMenuPaneItem { text, enabled, key });
    }
    TtyMenuPane {
        title: title.to_string(),
        items,
        width,
        kbd_navigation: true,
    }
}

/// keyboard.c command_loop_1 after a command (and on entry):
/// `post-command-hook', then `delayed-warnings-hook' when warnings wait
/// in `delayed-warnings-list'.
/// safe_run_hooks catches each function's signal; a throw or the end of
/// the session goes on.
pub(crate) fn run_post_command_hooks(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    crate::lisp::primitives::safe_run_hooks(
        interp,
        &cached_symbol!("post-command-hook").symbol(),
        env,
    )?;
    if interp
        .lookup_var_key(cached_symbol!("delayed-warnings-list"), env)
        .is_some_and(|value| value.is_truthy())
    {
        crate::lisp::primitives::safe_run_hooks(
            interp,
            &cached_symbol!("delayed-warnings-hook").symbol(),
            env,
        )?;
    }
    Ok(())
}

/// keyboard.c command_loop_1's entry, each time a command loop starts
/// (the top level, a keyboard macro's iteration, a recursive edit, or
/// after an error threw to top level): the post-command hooks, so they
/// run after commands that exited nonlocally, then `last-command' and
/// its companions from the command that ran last.
pub(crate) fn command_loop_entry(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    run_post_command_hooks(interp, env)?;
    let this_command = interp
        .lookup_var_key(cached_symbol!("this-command"), env)
        .unwrap_or(Value::Nil);
    let real_this_command = interp
        .lookup_var_key(cached_symbol!("real-this-command"), env)
        .unwrap_or(Value::Nil);
    interp.set_variable_key(cached_symbol!("last-command"), this_command, env);
    interp.set_variable_key(cached_symbol!("real-last-command"), real_this_command, env);
    let last_event_is_cons = interp
        .lookup_var_key(cached_symbol!("last-command-event"), env)
        .is_some_and(|event| event.cons_values().is_some());
    if !last_event_is_cons {
        interp.set_variable_key(
            cached_symbol!("last-repeatable-command"),
            real_this_command,
            env,
        );
    }
    Ok(())
}

/// Whether the error DATA's symbol has CONDITION among its
/// `error-conditions' (Fmemq of Fget).
fn error_data_has_condition(interp: &Interpreter, data: &Value, condition: &str) -> bool {
    let Some(symbol) = data.cons_ref().map(|cell| cell.car.get()) else {
        return false;
    };
    let Kind::Symbol(name) = symbol.kind() else {
        return false;
    };
    interp
        .get_symbol_property_of(
            &name,
            &cached_symbol!("error-conditions").with(Clone::clone),
        )
        .and_then(|conditions| conditions.to_vec().ok())
        .is_some_and(|conditions| {
            conditions.iter().any(
                |entry| matches!(entry.kind(), Kind::Symbol(entry) if entry.is_named(condition)),
            )
        })
}

/// keyboard.c cmd_error: command_loop_2's handler for the signal ERROR
/// from a command, before command_loop_1 is entered again.
pub(crate) fn cmd_error(
    interp: &mut Interpreter,
    env: &mut Env,
    error: &LispError,
) -> Result<(), LispError> {
    let data = crate::lisp::eval::error_condition_value(error);
    let macroerror = if interp.executing_kbd_macro {
        if interp.kbd_macro_iterations == 1 {
            "After 1 kbd macro iteration: ".to_string()
        } else {
            format!(
                "After {} kbd macro iterations: ",
                interp.kbd_macro_iterations
            )
        }
    } else {
        String::new()
    };
    if !error_data_has_condition(interp, &data, "minibuffer-quit") {
        interp.set_variable_key(cached_symbol!("executing-kbd-macro"), Value::Nil, env);
        interp.executing_kbd_macro = false;
    } else if interp
        .lookup_var_key(cached_symbol!("defining-kbd-macro"), env)
        .is_some_and(|value| value.is_truthy())
    {
        // An `M-x' command that signals a `minibuffer-quit' condition
        // that's part of a kbd macro: finalize_kbd_macro_chars.
        interp.kbd_macro_committed_len = interp.kbd_macro_definition.len();
    }
    let output = interp.bind_special_dynamic("standard-output", Value::T, env)?;
    let input = match interp.bind_special_dynamic("standard-input", Value::T, env) {
        Ok(input) => input,
        Err(error) => {
            interp.restore_special_dynamic(output, env)?;
            return Err(error);
        }
    };
    interp.set_variable_key(cached_symbol!("prefix-arg"), Value::Nil, env);
    interp.set_variable_key(cached_symbol!("last-prefix-arg"), Value::Nil, env);
    // Avoid unquittable loop if data contains a circular list.
    let reported = interp.with_lisp_stack_roots(&data, |interp| {
        crate::lisp::primitives::with_cmd_error_print_limits(interp, |interp| {
            cmd_error_internal(interp, env, data, &macroerror)
        })
    });
    interp.set_c_variable("quit-flag", Value::Nil);
    interp.set_c_variable("inhibit-quit", Value::Nil);
    interp.restore_special_dynamic(input, env)?;
    interp.restore_special_dynamic(output, env)?;
    reported
}

/// keyboard.c cmd_error_internal: `command-error-function' with DATA,
/// CONTEXT (empty or ASCII) and the function that signalled.
pub(crate) fn cmd_error_internal(
    interp: &mut Interpreter,
    env: &mut Env,
    data: Value,
    context: &str,
) -> Result<(), LispError> {
    // The immediate context is not interesting for Quits, since they are
    // asynchronous (signal_quit_p).
    let quit = data.cons_ref().is_some_and(|cell| {
        let symbol = cell.car.get();
        crate::lisp::types::eq_symbol_named(symbol, "quit")
    }) || error_data_has_condition(interp, &data, "quit");
    if quit {
        interp.signaling_function = Value::Nil;
    }
    interp.set_c_variable("quit-flag", Value::Nil);
    interp.set_c_variable("inhibit-quit", Value::T);
    // Use user's specified output function if any.
    let function = interp
        .lookup_var_key(cached_symbol!("command-error-function"), env)
        .unwrap_or(Value::Nil);
    let result = if function.is_nil() {
        Ok(())
    } else {
        let context = if context.is_empty() {
            crate::lisp::primitives::bytes_to_shared_unibyte_value(b"")
        } else {
            Value::string(context)
        };
        let signal = interp.signaling_function;
        interp
            .call_function_value(function, None, &[data, context, signal], env)
            .map(|_| ())
    };
    interp.signaling_function = Value::Nil;
    result
}

/// keyboard.c command_loop_1 before read_key_sequence: no command is
/// running while the next key sequence is read.
pub(crate) fn clear_this_command(interp: &mut Interpreter, env: &mut Env) {
    for name in [
        "this-command",
        "real-this-command",
        "this-original-command",
        "this-command-keys-shift-translated",
    ] {
        interp.set_variable(name, Value::Nil, env);
    }
}
