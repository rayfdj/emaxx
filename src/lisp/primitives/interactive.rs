use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

mod command_loop;
mod echo;
mod kbd_input;
mod key_sequence;
#[cfg(test)]
pub(crate) use command_loop::select_window_buffer;
pub(crate) use command_loop::{
    CommandLoopHandlers, command_loop_2, recursive_edit, recursive_edit_1,
    take_resize_echo_area_exactly, top_level_1,
};
pub(crate) use echo::{
    cancel_echoing, echo_after_command, echo_after_event, echo_before_wait, echo_dash,
    echo_keystrokes_p, echo_length, echo_now, echo_timer, echo_timer_expired, echo_truncate,
    echo_update, set_echo_prompt,
};
pub(crate) use kbd_input::{
    KbdWait, KeyboardTerminal, clear_keyboard_waiting_thread, clear_waiting_for_input,
    detect_input_pending, detect_input_pending_run_timers, get_input_pending, gobble_input,
    kbd_buffer_get_event_wait, kbd_buffer_store_event, kbd_is_ours, keyboard_wait_descriptors,
    redisplay_preserve_echo_area, set_keyboard_terminal, set_tty_showing_menu,
    set_waiting_for_input, sit_for, swallow_events,
};
#[cfg(unix)]
pub(crate) use kbd_input::{
    ResetState, arm_emergency_reset, disarm_emergency_reset, emergency_reset,
};
pub(crate) use kbd_input::{
    SysModes, TtyInputModes, begin_redisplay, change_tty_input_modes, dev_tty_terminal,
    frame_garbaged, init_all_sys_modes, interrupt_signal_pending, note_tty_erase_char,
    process_interrupt_signal, quit_char, redisplaying_p, refresh_emergency_reset,
    reset_all_sys_modes, set_frame_garbaged, set_quit_char, set_tty_input_modes,
    stuff_buffered_input, sys_suspend, take_frame_garbaged, take_window_change_signal,
    tty_input_modes, unread_command_events_pending, unwind_redisplay,
};
use kbd_input::{quit_throw_to_read_char_outside_signal, waiting_for_input};
#[cfg(test)]
pub(crate) use key_sequence::KeySequenceReader;
pub(crate) use key_sequence::{read_key_sequence, read_key_sequence_vs};

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
            // keyboard.c:init_keyboard gives SIGQUIT, like SIGINT, the
            // interrupt handler in an interactive session.
            if signal == libc::SIGQUIT && !noninteractive {
                continue;
            }
            install_process_fatal_handler(signal);
        }
        if !noninteractive {
            install_interrupt_handlers();
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

/// keyboard.c:init_keyboard's deliver_interrupt_signal for SIGINT and
/// SIGQUIT (C-g on the controlling terminal gives either): the main
/// thread's next quit check runs handle_interrupt_signal.  Calls it
/// interrupts fail with EINTR (emacs_sigaction_flags), so a wait's select
/// ends.
#[cfg(unix)]
fn install_interrupt_handlers() {
    for signal in [libc::SIGINT, libc::SIGQUIT] {
        install_input_signal_handler(signal, kbd_input::note_interrupt_signal);
    }
    // USABLE_SIGIO's deliver_input_available_signal:
    // handle_input_available_signal leaves process_pending_signals to read
    // the input (handle_async_input).
    install_input_signal_handler(libc::SIGIO, super::atimer::note_pending_signal);
}

/// dispnew.c:init_display's deliver_window_change_signal for SIGWINCH,
/// for a session on a terminal: do_pending_window_change takes the size.
pub(crate) fn install_window_change_handler() {
    #[cfg(unix)]
    {
        static INSTALLED: OnceLock<()> = OnceLock::new();
        INSTALLED.get_or_init(|| {
            install_input_signal_handler(libc::SIGWINCH, kbd_input::note_window_change_signal);
        });
    }
}

/// A handler for SIGNAL that runs NOTE (async-signal-safe), installed
/// without SA_RESTART.
#[cfg(unix)]
fn install_input_signal_handler(signal: libc::c_int, note: fn()) {
    // SAFETY: NOTE only stores atomics, which is async-signal-safe.
    if let Err(error) = unsafe { signal_hook::low_level::register(signal, note) } {
        panic!("install the handler of signal {signal}: {error}");
    }
    // SAFETY: rewrites only the flags of the action just installed.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        if libc::sigaction(signal, std::ptr::null(), &mut action) == 0 {
            action.sa_flags &= !libc::SA_RESTART;
            libc::sigaction(signal, &action, std::ptr::null_mut());
        }
    }
}

/// Record SIGNAL as process_fatal_action does, for the next quit check's
/// terminate_due_to_signal; false when one is recorded already.
pub(crate) fn request_fatal_signal(signal: i32) -> bool {
    #[cfg(unix)]
    {
        PENDING_FATAL_SIGNAL
            .compare_exchange(
                0,
                signal,
                UserSignalOrdering::Relaxed,
                UserSignalOrdering::Relaxed,
            )
            .is_ok()
    }
    #[cfg(not(unix))]
    {
        let _ = signal;
        false
    }
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
            // The terminal's reset as published at its last update: the
            // signal may have come mid-allocation or with the terminal's
            // state borrowed, and on any thread.
            fatal_error_diagnostic(signal, &mut || emergency_reset());
            emacs_backtrace(40);
        }
        raise_unblocked(signal);
    }
}

/// emacs.c:shut_down_emacs's terminal reset and "Fatal error NUM: DESC"
/// for SIGNAL (not SIGTERM), written with one write, when Emacs's process
/// group is the foreground of the terminal on standard input:
/// RESET_ALL_SYS_MODES leaves the terminals as Emacs found them first.
#[cfg(unix)]
fn fatal_error_diagnostic(signal: libc::c_int, reset_all_sys_modes: &mut dyn FnMut()) {
    // SAFETY: tcgetpgrp, getpgrp and write are async-signal-safe.
    unsafe {
        let foreground = libc::tcgetpgrp(libc::STDIN_FILENO);
        if foreground == -1 || foreground != libc::getpgrp() {
            return;
        }
        // shut_down_emacs: the terminals as Emacs found them, then the
        // message.
        reset_all_sys_modes();
        if signal == libc::SIGTERM {
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
pub(crate) fn raise_unblocked(signal: libc::c_int) -> ! {
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
                shut_down_for_signal(interp, signal, env)?;
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

/// sysdep.c:deliver_fatal_thread_signal for a fault Emaxx finds itself
/// rather than takes in a handler (a C stack overflow it cannot return
/// from): terminate_due_to_signal (SIGNAL, 40), its orderly shutdown and
/// backtrace run here, where the interpreter is at hand, and then the
/// signal at its default action ends the process.
#[cfg(unix)]
#[cold]
pub(crate) fn deliver_fatal_signal(
    interp: &mut Interpreter,
    signal: libc::c_int,
    env: &mut Env,
) -> Result<(), LispError> {
    PENDING_FATAL_SIGNAL.store(signal, UserSignalOrdering::Relaxed);
    run_pending_fatal_signal(interp, env)
}

/// emacs.c:shut_down_emacs (0, ...), as Fkill_emacs runs it: Vrun_hooks
/// cleared, so no hook runs from now on, and Vinhibit_redisplay t;
/// inhibit_sentinels set and kill_buffer_processes (Qnil), each connection
/// deleted and each subprocess whose channel is open hung up; Fdo_auto_save
/// (Qt, Qnil); and unlock_all_files.  reset_all_sys_modes is the
/// terminals' release as the process exits.
pub(crate) fn shut_down_emacs(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    interp.hooks_cleared = true;
    interp.set_c_variable("inhibit-redisplay", Value::T);
    interp.inhibit_sentinels = true;
    super::processes::kill_buffer_processes(interp, env, None)?;
    super::file_io::do_auto_save(interp, env, Value::T, Value::Nil)?;
    super::filelock::unlock_all_files(interp, env)
}

/// emacs.c:shut_down_emacs for SIGNAL: Vrun_hooks cleared, so no hook runs
/// from now on, and Vinhibit_redisplay t; the diagnostic; the terminals'
/// modes reset; inhibit_sentinels set and kill_buffer_processes (Qnil);
/// Fdo_auto_save (t, nil); and unlock_all_files.  An error in a step does
/// not stop the others, but a `kill-emacs' a filter runs meanwhile (a
/// filter's error in batch is one) ends Emacs then and there, as GNU's
/// exit does.
#[cfg(unix)]
fn shut_down_for_signal(
    interp: &mut Interpreter,
    signal: libc::c_int,
    env: &mut Env,
) -> Result<(), LispError> {
    let terminated = |result: Result<(), LispError>| match result {
        Err(error) if matches!(error.kind(), LispErrorKind::Terminate(_)) => Err(error),
        _ => Ok(()),
    };
    interp.hooks_cleared = true;
    interp.set_c_variable("inhibit-redisplay", Value::T);
    fatal_error_diagnostic(signal, &mut || super::reset_all_sys_modes());
    interp.reset_terminals_for_shutdown();
    interp.inhibit_sentinels = true;
    terminated(super::processes::kill_buffer_processes(interp, env, None))?;
    terminated(super::file_io::do_auto_save(interp, env, Value::T, Value::Nil).map(drop))?;
    terminated(super::filelock::unlock_all_files(interp, env))
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

/// A SIGUSR1 or SIGUSR2 not yet made an event.
#[cfg(unix)]
pub(crate) fn user_signals_pending() -> bool {
    PENDING_SIGUSR1.load(UserSignalOrdering::Relaxed) != 0
        || PENDING_SIGUSR2.load(UserSignalOrdering::Relaxed) != 0
}

#[cfg(not(unix))]
pub(crate) fn user_signals_pending() -> bool {
    false
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
            append_unread_command_events(interp, env, [event]);
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

/// keyboard.c:init_raw_keybuf_count (`raw_keybuf_count = 0'), which
/// command_loop_1, read_key_sequence_vs and read_menu_command do before
/// read_key_sequence: this-single-command-raw-keys starts empty, even
/// for a sequence that ends before reading a key.
pub(crate) fn init_raw_keybuf_count(interp: &mut Interpreter) {
    interp.keyboard_input.raw_keys.clear();
}

pub(crate) fn executing_kbd_macro_p(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("executing-kbd-macro"), env)
        .is_some_and(|value| value.is_truthy())
}

/// keyboard.c:record_char for an event read_char got: the lossage
/// (`recent-keys'), the input counts and the dribble file.
pub(crate) fn record_external_input_event(interp: &mut Interpreter, event: &Value, env: &Env) {
    count_read_event(interp);
    READ_RECORDED.set(true);
    record_char(interp, event, env);
}

/// read_char's `num_input_events++' for an event it returns, recorded or
/// not.
pub(crate) fn count_read_event(interp: &mut Interpreter) {
    interp.keyboard_input.num_input_events += 1;
}

/// read_char past its input method step: an event taken from the unread
/// lists or a macro, not recorded yet (`recorded' false), is recorded now
/// as the input method left it (record_char); a keyboard event was
/// recorded as it was read, and one from `unread-post-input-method-events'
/// or a `(no-record . EVENT)' never is.
pub(crate) fn record_unrecorded_event(interp: &mut Interpreter, event: &Value, env: &Env) {
    if !READ_RECORDED.replace(true) {
        record_char(interp, event, env);
    }
}

/// keyboard.c:record_char: EVENT into `recent-keys', the dribble file and
/// the keyboard macro being defined; nothing while a macro executes.
fn record_char(interp: &mut Interpreter, event: &Value, env: &Env) {
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
    keys: &[Value],
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<InteractiveStringArgs, LispError> {
    // The events the `e' code walks (call-interactively's KEYS, or the
    // command's keys), starting at the first with parameters unless
    // `inhibit-mouse-event-check' admits any event.
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
                let events =
                    read_key_sequence_vs(interp, message, false, code == 'K', false, false, env);
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

/// The keyboard buffer's next event (Some(None) for none yet), or None
/// once the terminal's device is gone.
pub(crate) type TtyEventPoller = Box<dyn FnMut(&mut Interpreter) -> Option<Option<Value>>>;

/// read_avail_input's -2 for the last terminal, its device terminated (a
/// feed's end, for an embedder's or a test's keyboard):
/// terminate_due_to_signal (SIGHUP, 10), whose orderly shutdown is
/// Fkill_emacs (SIGHUP), `kill-emacs-hook' and all.
fn terminal_gone(interp: &mut Interpreter, env: &mut Env) -> LispError {
    const SIGHUP: i64 = 1;
    match call(interp, "kill-emacs", &[Value::Integer(SIGHUP)], env) {
        Err(error) => error,
        Ok(_) => LispError::Terminate(crate::lisp::types::EmacsTermination {
            exit_code: SIGHUP as i32,
            restart: false,
            c_stack_overflow: false,
        }),
    }
}

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
    end_time: Option<std::time::Instant>,
) -> Option<Option<Option<Value>>> {
    TTY_CURSOR_IN_ECHO_AREA.set(cursor_in_echo_area);
    let outer_end_time = TTY_READ_END_TIME.replace(end_time);
    let result =
        TTY_EVENT_POLLER.with_borrow_mut(|slot| slot.as_mut().map(|poller| poller(interp)));
    TTY_READ_END_TIME.set(outer_end_time);
    TTY_CURSOR_IN_ECHO_AREA.set(false);
    result
}

thread_local! {
    /// The END_TIME of the read whose event the poller is fetching.
    static TTY_READ_END_TIME: std::cell::Cell<Option<std::time::Instant>> =
        const { std::cell::Cell::new(None) };
}

/// read_decoded_event_from_main_queue's END_TIME: the read's own, which
/// the rest of an encoded sequence is waited for until.
pub(crate) fn tty_read_end_time() -> Option<std::time::Instant> {
    TTY_READ_END_TIME.get()
}

pub(crate) fn tty_cursor_in_echo_area() -> bool {
    TTY_CURSOR_IN_ECHO_AREA.get()
}

thread_local! {
    static RAW_EVENT_READ: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A read whose read_char gets PREV_EVENT t (read_filtered_event without
/// INHERIT-INPUT-METHOD): read_decoded_event_from_main_queue leaves the
/// terminal's bytes undecoded.  The previous state comes back when the
/// read ends.
pub(crate) struct RawEventRead(bool);

impl RawEventRead {
    pub(crate) fn enter(raw: bool) -> Self {
        Self(RAW_EVENT_READ.replace(raw))
    }
}

impl Drop for RawEventRead {
    fn drop(&mut self) {
        RAW_EVENT_READ.set(self.0);
    }
}

/// Whether the read in progress takes the terminal's bytes undecoded.
pub(crate) fn raw_event_read() -> bool {
    RAW_EVENT_READ.get()
}

/// What a pending key sequence resolves to under the live keymaps.
pub(crate) enum KeyResolution {
    Command(Value),
    Prefix,
    Undefined,
    /// read_char showed a menu and the user rejected it (its t), which
    /// makes read_key_sequence return -1.
    MenuRejected,
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
        // A prefix keymap reads on: after a click, the next read_char
        // shows its menu (read_char_x_menu_prompt).
        let _ = keys;
        return Ok(KeyResolution::Prefix);
    }
    if roots[0].is_symbol() {
        let remapped = command_remapping(interp, &roots[0], None, env)?;
        if !remapped.is_nil() {
            roots[0] = remapped;
        }
    }
    Ok(KeyResolution::Command(roots[0]))
}

/// keyboard.c:1537, as a command is about to run: boundaries for the LAST
/// command's changes through simple.el's own `undo-auto--add-boundary',
/// whose amalgamation policy (fusing runs of self-inserts) native boundary
/// pushes cannot reproduce, then point_before_last_command_or_undo, which
/// is refreshed whether or not a boundary was needed (without it, a motion
/// between two edits leaves record_point the earlier edit's position).
pub(crate) fn add_command_undo_boundary(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    // call0 (Qundo_auto__add_boundary), void without simple.el as GNU's.
    interp.call_function_value(
        Value::Symbol("undo-auto--add-boundary".into()),
        Some("undo-auto--add-boundary"),
        &[],
        env,
    )?;
    // Record point and buffer, so we can put point into the undo
    // information if necessary.
    interp.note_point_before_last_command_or_undo();
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
        count_read_event(interp);
        return Ok(Some(event));
    }
    run_pending_user_signal_events(interp, env)?;
    if let Some(event) = take_unread_command_event(interp, env) {
        count_read_event(interp);
        return Ok(Some(event));
    }
    // GNU's input readers consume the executing keyboard macro's
    // remaining events (viper's `F'/`t' read their target char that way).
    // Lisp hooks may have rewound the public index after a speculative
    // read (kmacro's quoted-insert step editor does exactly this), so the
    // next read observes the actual array and public index directly.
    if let Some(event) = crate::lisp::primitives::dispatch::next_kbd_macro_event(interp, env)? {
        return Ok(Some(event));
    }
    // unread_switch_frame, the switch-frame event read_key_sequence
    // delayed: into this-command-keys and echoed again (not reread), and
    // not recorded again (reread_first).
    if let Some(event) = interp.keyboard_input.unread_switch_frame.take() {
        interp.keyboard_input.last_event_reread = false;
        note_event_source(EventSource::Unread);
        READ_RECORDED.set(true);
        count_read_event(interp);
        return Ok(Some(event));
    }
    Ok(None)
}

/// read_char for `read-event' and its kin: C-g typed while it waits
/// leaves `quit-flag' set, so the read quits.
pub(crate) fn pop_unread_command_event_value(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    read_command_event(interp, env, false, Value::Nil, Value::Nil)
}

/// read_char under read_key_sequence: handle_interrupt's longjmp makes
/// read_char return C-g as an event, and read_key_sequence clears
/// `quit-flag' after each key it reads (KeySequenceReader::read_event,
/// once the input method has had the event), so C-g is looked up like any
/// key (`keyboard-quit', the minibuffer's `abort-minibuffers').
/// PREV_EVENT is read_key_sequence's last_nonmenu_event, the key read
/// before this one.
pub(crate) fn read_key_sequence_event(
    interp: &mut Interpreter,
    env: &mut Env,
    prev_event: Value,
    map: Value,
) -> Result<Value, LispError> {
    read_command_event(interp, env, true, prev_event, map)
}

/// read_char (COMMANDFLAG, Qnil, Qt, 0, NULL) as
/// read_char_minibuf_menu_prompt reads its answer: no keymap, and t for
/// the previous event.
pub(crate) fn read_menu_prompt_char(
    interp: &mut Interpreter,
    env: &mut Env,
    quit_char_is_event: bool,
) -> Result<Value, LispError> {
    read_command_event(interp, env, quit_char_is_event, Value::T, Value::Nil)
}

/// Whether the event read_char returned last came from a mouse menu
/// (its *USED_MOUSE_MENU), cleared by the read.
pub(crate) fn take_read_used_mouse_menu() -> bool {
    READ_USED_MOUSE_MENU.replace(false)
}

/// Whether read_char returned the event read last from a menu prompt,
/// without adding it to this-command-keys; cleared by the read.
pub(crate) fn take_read_menu_exit() -> bool {
    READ_MENU_EXIT.replace(false)
}

/// read_char: MAP is the keymap the key sequence has reached (nil
/// outside one), whose prompt read_char_minibuf_menu_prompt or
/// read_char_x_menu_prompt may show a menu of.
fn read_command_event(
    interp: &mut Interpreter,
    env: &mut Env,
    quit_char_is_event: bool,
    prev_event: Value,
    map: Value,
) -> Result<Value, LispError> {
    READ_USED_MOUSE_MENU.set(false);
    READ_MENU_EXIT.set(false);
    let event = read_command_event_1(interp, env, quit_char_is_event, prev_event, map)?;
    read_char_help_form(interp, env, quit_char_is_event, event)
}

/// read_char's help character: with `help-form' non-nil, a help
/// character read is not the read's; `help-form-show' shows the help and
/// the next event is (a SPC after it reading one more), the window
/// configuration put back as it was (read_char_help_form_unwind) unless a
/// mouse click came.
fn read_char_help_form(
    interp: &mut Interpreter,
    env: &mut Env,
    quit_char_is_event: bool,
    event: Value,
) -> Result<Value, LispError> {
    if !help_form_applies(interp, event, env) {
        return Ok(event);
    }
    // read_char has made the help character part of the current key
    // (add_command_key, echo_update) and `last-input-event' before it
    // processes it.
    add_read_event_to_command_keys(interp, event, env)?;
    interp.set_variable_key(cached_symbol!("last-input-event"), event, env);
    run_help_form(interp, env, &mut |interp, env| {
        read_command_event(interp, env, quit_char_is_event, Value::Nil, Value::Nil)
    })
}

/// Whether read_char processes EVENT as the help character: `help-form'
/// is non-nil and EVENT is `help-char' or in `help-event-list'.
pub(crate) fn help_form_applies(interp: &mut Interpreter, event: Value, env: &mut Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("help-form"), env)
        .is_some_and(|form| !form.is_nil())
        && echo::help_char_p(interp, event, env)
}

/// read_char's processing of the help character it has just recorded:
/// `help-form-show' with the window configuration saved, then READ_NEXT
/// (read_char (0, ...)) for the event the read returns, a SPC after the
/// help reading one more.
pub(crate) fn run_help_form(
    interp: &mut Interpreter,
    env: &mut Env,
    read_next: &mut dyn FnMut(&mut Interpreter, &mut Env) -> Result<Value, LispError>,
) -> Result<Value, LispError> {
    let config = super::call(interp, "current-window-configuration", &[], env)?;
    let mut saved = crate::lisp::alloc::RootedVec::from_vec(vec![config]);
    let read: Result<Value, LispError> = (|| {
        interp.call_function_value(
            Value::symbol("help-form-show"),
            Some("help-form-show"),
            &[],
            env,
        )?;
        cancel_echoing(interp);
        let c = read_next(interp, env)?;
        if mouse_event_head(interp, &c).is_some() {
            saved[0] = Value::Nil;
        }
        Ok(c)
    })();
    // Remove the help from the frame, the event read kept alive.
    let c = read.as_ref().copied().unwrap_or(Value::Nil);
    let roots = [c];
    let restored = interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
        if saved[0].is_nil() {
            Ok(())
        } else {
            super::call(
                interp,
                "set-window-configuration",
                &[saved[0], Value::Nil, Value::Nil],
                env,
            )
            .map(|_| ())
        }
    });
    let c = read?;
    restored?;
    run_tty_frame_redraw(interp, env);
    if matches!(c.kind(), Kind::Integer(0o40)) {
        cancel_echoing(interp);
        return read_next(interp, env);
    }
    Ok(c)
}

fn read_command_event_1(
    interp: &mut Interpreter,
    env: &mut Env,
    quit_char_is_event: bool,
    prev_event: Value,
    map: Value,
) -> Result<Value, LispError> {
    if let Some(event) = pop_pending_input_event_value(interp, env)? {
        return Ok(event);
    }
    // read_char: timer_start_idle, then kbd_buffer_get_event until an
    // event is in the keyboard buffer or `unread-command-events'.
    let cursor_in_echo_area = interp
        .lookup_var_key(cached_symbol!("cursor-in-echo-area"), env)
        .is_some_and(|value| value.is_truthy());
    // Redisplay if no pending input (COMMANDFLAG >= 0): the frontend
    // paints unless input is pending, when redisplay_internal runs only
    // the window change functions.
    run_tty_frame_redraw(interp, env);
    // An embedder's (or a test's) event feed without a keyboard buffer
    // hands over its events one at a time, None ending the feed.
    if !has_tty_event_poller()
        && let Some(read) = read_via_tty_event_reader(interp, cursor_in_echo_area)
    {
        return match read {
            Some(event) if event != Value::Integer(7) || quit_char_is_event => {
                interp.keyboard_input.last_event_reread = false;
                record_external_input_event(interp, &event, env);
                read_char_non_reread(interp, &event, env)?;
                Ok(event)
            }
            Some(_) => Err(LispError::SignalValue(Value::list([Value::symbol("quit")]))),
            // The feed's end is its terminal's device terminating.
            None => Err(terminal_gone(interp, env)),
        };
    }
    // read_char before it waits: the echo area's test, and its echo timer
    // (sit_for `echo-keystrokes', then echo_now) once the idle timers
    // have started.
    echo_before_wait(interp, env)?;
    // Try reading a character via menu prompting in the minibuf.  Try
    // this before the sit-for, because the sit-for would do the wrong
    // thing if we are supposed to do menu prompting.  If
    // EVENT_HAS_PARAMETERS then we are reading after a mouse event so
    // don't try a minibuf menu.
    let interactive = !executing_kbd_macro_p(interp, env)
        && !interp
            .forwarded_c_value("noninteractive", &Env::new())
            .is_some_and(|value| value.is_truthy());
    // EVENT_HAS_PARAMETERS: CONSP.
    let has_parameters = prev_event.is_cons();
    let unread_pending = interp
        .lookup_var_key(cached_symbol!("unread-command-events"), env)
        .is_some_and(|value| value.is_cons());
    if keymapp(interp, map, env)
        && interactive
        && !prev_event.is_nil()
        && !has_parameters
        // Don't bring up a menu if we already have another event.
        && !unread_pending
        && !kbd_input::detect_input_pending_run_timers(interp, env, false)?
    {
        let c = crate::lisp::primitives::menu::read_char_minibuf_menu_prompt(
            interp,
            quit_char_is_event,
            map,
            env,
        )?;
        if !c.is_nil() {
            // goto exit: the menu's own read_char recorded the key and
            // added it to this-command-keys.
            READ_RECORDED.set(true);
            READ_MENU_EXIT.set(true);
            return Ok(c);
        }
    }
    tty_note_idle_start(interp, env);
    // If in middle of key sequence and minibuffer not active, start
    // echoing if enough time elapses -- at once after a mouse event, as a
    // menu is probably about to show.
    let mut echo_deadline = echo_timer(interp, env, prev_event)?
        .and_then(|seconds| std::time::Instant::now().checked_add(seconds));
    // Try reading using an X menu.  This is never confused with reading
    // using the minibuf because the recursive call of read_char in
    // read_char_minibuf_menu_prompt does not pass on any keymaps.
    let head = prev_event.car_if_list().unwrap_or(Value::Nil);
    if keymapp(interp, map, env)
        && interactive
        && !prev_event.is_nil()
        && has_parameters
        && !head.is_key(cached_symbol!("menu-bar"))
        && !head.is_key(cached_symbol!("tab-bar"))
        && !head.is_key(cached_symbol!("tool-bar"))
        // Don't bring up a menu if we already have another event.
        && !unread_pending
    {
        let mut used_mouse_menu = false;
        let c = crate::lisp::primitives::menu::read_char_x_menu_prompt(
            interp,
            map,
            prev_event,
            &mut used_mouse_menu,
            env,
        )?;
        READ_USED_MOUSE_MENU.set(used_mouse_menu);
        // The menu's keys were recorded as record_menu_key records them.
        READ_RECORDED.set(true);
        READ_MENU_EXIT.set(true);
        // Now that we have read an event, Emacs is not idle.
        tty_note_idle_end();
        return Ok(c);
    }
    let result = (|| loop {
        // kbd_buffer_get_event: `unread-command-events' before the
        // keyboard buffer.
        if let Some(event) = take_unread_command_event(interp, env) {
            count_read_event(interp);
            return Ok(event);
        }
        if let Some(step) = poll_via_tty_event_poller(interp, cursor_in_echo_area, None) {
            match step {
                // gobble_input's -2: the controlling terminal's device
                // terminated, which ends Emacs.
                None => return Err(terminal_gone(interp, env)),
                Some(Some(event)) => {
                    interp.keyboard_input.last_event_reread = false;
                    record_external_input_event(interp, &event, env);
                    read_char_non_reread(interp, &event, env)?;
                    return Ok(event);
                }
                Some(None) => {}
            }
        }
        match kbd_input::kbd_buffer_get_event_wait(interp, env, echo_deadline, true)? {
            kbd_input::KbdWait::Quit => {
                if let Some(event) = read_char_quit(interp, env)? {
                    return Ok(event);
                }
            }
            kbd_input::KbdWait::TimedOut if echo_deadline.is_some() => {
                // sit_for's t: no input arrived (none is pending) in
                // `echo-keystrokes' seconds; the echo shows on the glass
                // at once (message3_nolog's echo_area_display).
                echo_deadline = None;
                if !kbd_input::detect_input_pending(interp, env)? {
                    echo_timer_expired(interp, env)?;
                    run_tty_frame_redraw(interp, env);
                }
            }
            kbd_input::KbdWait::Ready | kbd_input::KbdWait::TimedOut => {}
        }
    })();
    // read_char's timer_stop_idle once an event is in hand.
    tty_note_idle_end();
    // The quit character's quit, if `quit-flag' still holds it, comes at
    // the next quit check after the read returns.
    result
}

/// read_char's setjmp for quit_throw_to_read_char: the quit character,
/// unless `quit-flag' holds `throw-on-input', when C stays nil and
/// read_char reads again (non_reread, wrong_kboard): the event that set
/// it is the read's, while-no-input's throw coming at the next quit check
/// after the read returns; and under `inhibit-quit' the quit, reported as
/// this event, is not reported again.
fn read_char_quit(interp: &mut Interpreter, env: &mut Env) -> Result<Option<Value>, LispError> {
    let throw_on_input = interp.throw_on_input_value();
    if crate::lisp::types::eq(interp.quit_flag_value(), throw_on_input) {
        return Ok(None);
    }
    if interp.inhibit_quit_is_truthy() {
        interp.set_c_variable("quit-flag", Value::Nil);
    }
    interp.keyboard_input.last_event_reread = false;
    let event = Value::Integer(kbd_input::quit_char());
    record_external_input_event(interp, &event, env);
    // The quit character goes on as read_char's non_reread event.
    read_char_non_reread(interp, &event, env)?;
    Ok(Some(event))
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

/// EVENTS after `unread-command-events'' events, for a reader to return
/// in that order; a value that is not a list counts as none, as
/// read_char's CONSP test reads it.
pub(crate) fn append_unread_command_events(
    interp: &mut Interpreter,
    env: &mut Env,
    events: impl IntoIterator<Item = Value>,
) {
    let current = interp
        .lookup_var_key(cached_symbol!("unread-command-events"), env)
        .unwrap_or(Value::Nil);
    let mut unread = if current.is_cons() {
        current.to_vec().unwrap_or_default()
    } else {
        Vec::new()
    };
    unread.extend(events);
    interp.set_variable_key(
        cached_symbol!("unread-command-events"),
        Value::list(unread),
        env,
    );
}

/// EVENTS before `unread-command-events'' value, whatever it is:
/// nconc2 (Flist (EVENTS), Vunread_command_events).
pub(crate) fn prepend_unread_command_events(
    interp: &mut Interpreter,
    env: &mut Env,
    events: &[Value],
) {
    let mut list = interp
        .lookup_var_key(cached_symbol!("unread-command-events"), env)
        .unwrap_or(Value::Nil);
    for event in events.iter().rev() {
        list = Value::cons(*event, list);
    }
    interp.set_variable_key(cached_symbol!("unread-command-events"), list, env);
}

#[cfg(test)]
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

/// read_char's events before the keyboard, in its order:
/// `unread-post-input-method-events' (the rest of what an input method
/// returned, not given to it again), `unread-command-events', then
/// `unread-input-method-events', each reread; the source is noted (the
/// keyboard's when none is there).  A one-element list (EVENT) from the
/// input-method lists is EVENT (read_char_x_menu_prompt's unread keys).
pub(crate) fn take_unread_command_event(interp: &mut Interpreter, env: &mut Env) -> Option<Value> {
    let pop = |interp: &mut Interpreter,
               env: &mut Env,
               list: crate::lisp::types::symbols::SymbolKey| {
        let (event, tail) = interp.lookup_var_key(list, env)?.cons_values()?;
        interp.with_lisp_stack_roots(&event, |interp| {
            interp.set_variable_key(list, tail, env);
        });
        Some(match event.cons_values() {
            Some((head, rest))
                if rest.is_nil() && matches!(head.kind(), Kind::Symbol(_) | Kind::Integer(_)) =>
            {
                head
            }
            _ => event,
        })
    };
    // Each read starts with no previous echo area message kept.
    PREVIOUS_ECHO_AREA_MESSAGE.set(None);
    // read_char's `recorded' starts false at each read: a post-input-method
    // event goes past recording (reread_first), a `(no-record . EVENT)'
    // counts as recorded, the others are recorded after the input method.
    if let Some(event) = pop(
        interp,
        env,
        cached_symbol!("unread-post-input-method-events"),
    ) {
        interp.keyboard_input.last_event_reread = true;
        note_event_source(EventSource::PostInputMethod);
        READ_RECORDED.set(true);
        return Some(event);
    }
    if let Some((event, no_record)) = take_from_unread_command_events(interp, env) {
        note_event_source(EventSource::Unread);
        READ_RECORDED.set(no_record);
        return Some(event);
    }
    if let Some(event) = pop(interp, env, cached_symbol!("unread-input-method-events")) {
        interp.keyboard_input.last_event_reread = true;
        note_event_source(EventSource::Unread);
        READ_RECORDED.set(false);
        return Some(event);
    }
    note_event_source(EventSource::Keyboard);
    None
}

/// read_char's `unread-command-events' branch: the first entry popped,
/// `(t . EVENT)' (sit-for's requeued keys inside universal-argument) not
/// reread, anything else reread and `(no-record . EVENT)' already
/// recorded; then read_char_x_menu_prompt's `(EVENT . disabled)' undone
/// (a symbol or fixnum the menu gave), which, like a menu-, tab- or
/// tool-bar event, used the mouse.  The event and whether it counts as
/// recorded.
fn take_from_unread_command_events(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Option<(Value, bool)> {
    let mut c = pop_unread_event_cell(interp, env)?;
    let mut reread = false;
    let mut recorded = false;
    // Undo what sit-for did when it unread additional keys inside
    // universal-argument.
    if let Some((head, tail)) = c.cons_values()
        && crate::lisp::types::eq(head, Value::T)
    {
        c = tail;
    } else {
        if let Some((head, tail)) = c.cons_values()
            && head.is_key(cached_symbol!("no-record"))
        {
            c = tail;
            recorded = true;
        }
        reread = true;
    }
    // Undo what read_char_x_menu_prompt did when it unread additional
    // keys returned by Fx_popup_menu.
    let mut was_disabled = false;
    if let Some((head, tail)) = c.cons_values()
        && tail.is_key(cached_symbol!("disabled"))
        && (head.is_symbol() || matches!(head.kind(), Kind::Integer(_)))
    {
        was_disabled = true;
        c = head;
    }
    // If the queued event is something that used the mouse, set
    // used_mouse_menu accordingly.  Also check was_disabled so
    // last-nonmenu-event won't return a bad value when submenus are
    // involved.
    if c.is_key(cached_symbol!("tool-bar"))
        || c.is_key(cached_symbol!("tab-bar"))
        || c.is_key(cached_symbol!("menu-bar"))
        || was_disabled
    {
        READ_USED_MOUSE_MENU.set(true);
    }
    interp.keyboard_input.last_event_reread = reread;
    Some((c, recorded))
}

thread_local! {
    /// read_char's `recorded' for the event read last: false for one
    /// taken from the unread lists or a macro until it is recorded past
    /// the input method step (record_unrecorded_event).
    static READ_RECORDED: std::cell::Cell<bool> = const { std::cell::Cell::new(true) };
    /// read_char's *USED_MOUSE_MENU for the event read last.
    static READ_USED_MOUSE_MENU: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Whether read_char returned the event read last from a menu prompt
    /// (`goto exit'), past its add_command_key: record_menu_key recorded
    /// what an X menu chose, the minibuffer menu's read_char what it
    /// read.
    static READ_MENU_EXIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// An event taken from the executing keyboard macro: recorded past the
/// input method step, as an unread one is (record_char then records
/// nothing while the macro executes).
pub(crate) fn note_macro_event_read() {
    READ_RECORDED.set(false);
}

/// read_char's PREV_EVENT, as its input method step sees it: nil with a
/// keymap (a key sequence's first event), or nil with none (a
/// `read-event' that inherits the input method, `input-method-use-echo-area'
/// then t for the call).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum InputMethodRead {
    KeySequence,
    NoKeymap,
}

/// Where read_char took the event it is handing out: the keyboard,
/// `unread-command-events' or `unread-input-method-events',
/// `unread-post-input-method-events' (which the input method does not
/// see again), or a keyboard macro.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum EventSource {
    Keyboard,
    Unread,
    PostInputMethod,
    Macro,
}

thread_local! {
    static LAST_EVENT_SOURCE: std::cell::Cell<EventSource> =
        const { std::cell::Cell::new(EventSource::Keyboard) };
}

/// The source of the event read last.
pub(crate) fn last_event_source() -> EventSource {
    LAST_EVENT_SOURCE.get()
}

pub(crate) fn note_event_source(source: EventSource) {
    LAST_EVENT_SOURCE.set(source);
}

/// read_char's input method step for the event read last, read with
/// PREV_EVENT nil as READ: none for an event from
/// `unread-post-input-method-events'.
pub(crate) fn input_method_for_last_event(
    interp: &mut Interpreter,
    env: &mut Env,
    c: Value,
    read: InputMethodRead,
) -> Result<Option<Value>, LispError> {
    match last_event_source() {
        EventSource::PostInputMethod => Ok(Some(c)),
        source => {
            let reread = interp.keyboard_input.last_event_reread;
            let result =
                read_char_input_method(interp, env, c, read, source == EventSource::Keyboard);
            interp.keyboard_input.last_event_reread = reread;
            result
        }
    }
}

/// Whether read_char would give EVENT, the first of a key sequence, to
/// the input method: a character from SPC to 255 but DEL, with a non-nil
/// `input-method-function' (an event from `unread-post-input-method-events'
/// excepted).
pub(crate) fn input_method_takes(interp: &Interpreter, event: &Value, env: &Env) -> bool {
    matches!(event.kind(), Kind::Integer(code) if (0x20..0x100).contains(&code) && code != 0x7f)
        && last_event_source() != EventSource::PostInputMethod
        && interp
            .lookup_var_key(cached_symbol!("input-method-function"), env)
            .is_some_and(|function| !function.is_nil())
}

thread_local! {
    /// read_char's previous_echo_area_message, kept by a reader that wipes
    /// the echo area before the input method step.
    static PREVIOUS_ECHO_AREA_MESSAGE: std::cell::Cell<Option<Value>> =
        const { std::cell::Cell::new(None) };
}

/// command_loop_1 at the top of each cycle in a minibuffer: a message in
/// the echo area over it (the mini window is the echo area's) shows for
/// `minibuffer-message-timeout' seconds, or until input comes, with
/// `inhibit-quit' bound so a C-g is read; then the echo area is cleared
/// (message1 (0)), `echo-area-clear-hook' runs, and a C-g that came is
/// read as input.
pub(crate) fn minibuffer_message_timeout(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    if interp.minibuffer_depth() == 0
        || crate::lisp::primitives::dispatch::display::echo_area_message_value().is_none()
    {
        return Ok(());
    }
    let timeout = interp
        .lookup_var_key(cached_symbol!("minibuffer-message-timeout"), env)
        .unwrap_or(Value::Nil);
    if !matches!(
        timeout.kind(),
        Kind::Integer(_) | Kind::Float(_) | Kind::BigInteger(_)
    ) {
        return Ok(());
    }
    let count = interp.specpdl_index();
    interp.specbind_symbol(&"inhibit-quit".into(), Value::T, env)?;
    let result = (|| {
        sit_for(interp, env, timeout, false, 2)?;
        crate::lisp::primitives::dispatch::display::message3(interp, Value::Nil, None, env)?;
        crate::lisp::primitives::safe_run_hooks(
            interp,
            &cached_symbol!("echo-area-clear-hook").symbol(),
            env,
        )
    })();
    interp.unbind_to(count, env)?;
    result?;
    if !interp.quit_flag_is_nil() {
        interp.set_c_variable("quit-flag", Value::Nil);
        interp.set_variable_key(
            cached_symbol!("unread-command-events"),
            Value::list([Value::Integer(quit_char())]),
            env,
        );
    }
    Ok(())
}

/// read_char's non_reread step for an event from the keyboard buffer: a
/// character an input method could take has the echo area's message kept
/// as `input-method-previous-message', then the echo area is wiped.
fn read_char_non_reread(
    interp: &mut Interpreter,
    event: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if input_method_takes(interp, event, env) {
        save_input_method_previous_message(interp, env);
    }
    read_char_wipe_echo_area(interp, event, env)
}

/// keyboard.c:read_char's wipe of the echo area for an event from the
/// keyboard buffer (a reread, macro or input method event is not one), but
/// for the help-echo, switch-frame and select-window events, which do
/// their own echo area work: with a message, `echo-area-clear-hook' runs
/// and clear_message (1, 0) clears it, the mini window resized over an
/// active minibuffer at the next redisplay; without one, clear_message
/// when `clear-message-function' is a function.
pub(crate) fn read_char_wipe_echo_area(
    interp: &mut Interpreter,
    event: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Some((head, _)) = event.cons_values()
        && (crate::lisp::types::eq_symbol_named(head, "help-echo")
            || crate::lisp::types::eq_symbol_named(head, "switch-frame")
            || crate::lisp::types::eq_symbol_named(head, "select-window"))
    {
        return Ok(());
    }
    if crate::lisp::primitives::dispatch::display::echo_area_message_value().is_some() {
        crate::lisp::primitives::safe_run_hooks(
            interp,
            &cached_symbol!("echo-area-clear-hook").symbol(),
            env,
        )?;
        crate::lisp::primitives::dispatch::display::clear_message(interp, env, true, false)
    } else {
        let function = interp
            .forwarded_c_value("clear-message-function", &Env::new())
            .unwrap_or(Value::Nil);
        if crate::lisp::primitives::function_value_p(interp, &function, env) {
            crate::lisp::primitives::dispatch::display::clear_message(interp, env, true, false)
        } else {
            Ok(())
        }
    }
}

/// read_char for a keyboard character the input method will have:
/// `Fcurrent_message' as `input-method-previous-message' (and its
/// previous_echo_area_message) before the echo area is wiped.
pub(crate) fn save_input_method_previous_message(interp: &mut Interpreter, env: &mut Env) {
    let message =
        crate::lisp::primitives::dispatch::display::echo_area_message_value().unwrap_or(Value::Nil);
    interp.set_variable_key(
        cached_symbol!("input-method-previous-message"),
        message,
        env,
    );
    PREVIOUS_ECHO_AREA_MESSAGE.set(Some(message));
}

/// keyboard.c:read_char's input method step for C, read with PREV_EVENT
/// nil as READ: a character from SPC to 255 but DEL goes to a non-nil
/// `input-method-function'.  The echo and `this-command-keys' state are
/// saved around the call and the echo area wiped first (the message
/// shown, for a C FROM_KEYBOARD, kept as `input-method-previous-message'
/// and shown again when no event comes back); its first event is the
/// read's, the rest go before `unread-post-input-method-events'.  None
/// when it returns none: read_char reads again.
pub(crate) fn read_char_input_method(
    interp: &mut Interpreter,
    env: &mut Env,
    c: Value,
    read: InputMethodRead,
    from_keyboard: bool,
) -> Result<Option<Value>, LispError> {
    let Kind::Integer(code) = c.kind() else {
        return Ok(Some(c));
    };
    if !(0x20..0x100).contains(&code) || code == 0x7f {
        return Ok(Some(c));
    }
    let function = interp
        .lookup_var_key(cached_symbol!("input-method-function"), env)
        .unwrap_or(Value::Nil);
    if function.is_nil() {
        return Ok(Some(c));
    }
    // A keyboard character: the echo area's message before it is wiped
    // (kept by the command loop, which wipes it as the event is read).
    let previous_message = if from_keyboard {
        match PREVIOUS_ECHO_AREA_MESSAGE.take() {
            Some(message) => message,
            None => {
                let message = crate::lisp::primitives::dispatch::display::echo_area_message_value()
                    .unwrap_or(Value::Nil);
                interp.set_variable_key(
                    cached_symbol!("input-method-previous-message"),
                    message,
                    env,
                );
                message
            }
        }
    } else {
        Value::Nil
    };
    // The echo status and this_command_keys, saved.
    let saved_immediate_echo = interp.keyboard_input.immediate_echo;
    let saved_ok_to_echo = interp.keyboard_input.ok_to_echo_at_next_pause;
    let saved_echo_string = interp.keyboard_input.echo_string;
    let saved_echo_prompt = interp.keyboard_input.echo_prompt;
    let saved_keys = std::mem::take(&mut interp.keyboard_input.command_keys);
    let saved_start = std::mem::replace(&mut interp.keyboard_input.single_command_start, 0);
    let roots = [
        saved_echo_string.unwrap_or(Value::Nil),
        saved_echo_prompt.unwrap_or(Value::Nil),
    ];
    let call = interp.with_lisp_stack_roots(&roots.as_slice(), |interp| {
        // Wipe the echo area.
        if crate::lisp::primitives::dispatch::display::echo_area_message_value().is_some() {
            crate::lisp::primitives::safe_run_hooks(
                interp,
                &cached_symbol!("echo-area-clear-hook").symbol(),
                env,
            )?;
        }
        crate::lisp::primitives::dispatch::display::clear_message(interp, env, true, false)?;
        echo_truncate(interp, 0, env)?;
        let count = interp.specpdl_index();
        if read == InputMethodRead::NoKeymap {
            interp.specbind_symbol(&"input-method-use-echo-area".into(), Value::T, env)?;
        }
        // read_char's `recorded' is its own: the reads the input method
        // makes record their events without changing whether this read's
        // event has been.
        let recorded = READ_RECORDED.get();
        let result = interp.call_function_value(function, None, &[c], env);
        READ_RECORDED.set(recorded);
        interp.unbind_to(count, env)?;
        result
    });
    // The echoing and this_command_keys state, restored.
    interp.keyboard_input.command_keys = saved_keys;
    interp.keyboard_input.single_command_start = saved_start;
    cancel_echoing(interp);
    interp.keyboard_input.ok_to_echo_at_next_pause = saved_ok_to_echo;
    interp.keyboard_input.echo_string = saved_echo_string;
    interp.keyboard_input.echo_prompt = saved_echo_prompt;
    if saved_immediate_echo {
        echo_now(interp, env)?;
    }
    let events = call?;
    let Some((first, rest)) = events.cons_values() else {
        // No event: the previous message back, if any.
        if !previous_message.is_nil() {
            crate::lisp::primitives::dispatch::display::message_with_string_nolog(
                interp,
                "%s",
                previous_message,
                env,
            )?;
        }
        return Ok(None);
    };
    // nconc2 (XCDR (tem), Vunread_post_input_method_events).
    if rest.is_cons() {
        let post = interp
            .lookup_var_key(cached_symbol!("unread-post-input-method-events"), env)
            .unwrap_or(Value::Nil);
        let mut last = rest;
        while let Some((_, next)) = last.cons_values() {
            if !next.is_cons() {
                break;
            }
            last = next;
        }
        last.set_cdr(post)?;
        interp.set_variable_key(cached_symbol!("unread-post-input-method-events"), rest, env);
    }
    Ok(Some(first))
}

/// keyboard.c:read_char's add_command_key for an event it returned: the
/// event joins this-command-keys, one reread from `unread-command-events'
/// only as their first.
pub(crate) fn add_read_event_to_command_keys(
    interp: &mut Interpreter,
    event: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let reread = std::mem::take(&mut interp.keyboard_input.last_event_reread);
    if !reread || interp.keyboard_input.command_keys.is_empty() {
        interp.keyboard_input.command_keys.push(event);
        echo_after_event(interp, event, env)?;
    }
    Ok(())
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

/// read_char with an END_TIME (`read-event''s SECONDS, None when past what
/// the clock holds): the first event consumed before it passes (unread, macro, or what
/// kbd_buffer_get_event's wait finds in the keyboard buffer), or None.
pub(crate) fn read_tty_event_with_timeout(
    interp: &mut Interpreter,
    env: &mut Env,
    end_time: Option<std::time::Instant>,
) -> Result<Option<Value>, LispError> {
    let mut before_wait = true;
    loop {
        if let Some(event) = pop_pending_input_event_value(interp, env)? {
            return Ok(Some(event));
        }
        // read_char before it waits: the echo area's test (a read with an
        // end time runs no echo timer).
        if std::mem::take(&mut before_wait) {
            echo_before_wait(interp, env)?;
        }
        let cursor_in_echo_area = interp
            .lookup_var_key(cached_symbol!("cursor-in-echo-area"), env)
            .is_some_and(|value| value.is_truthy());
        if let Some(step) = poll_via_tty_event_poller(interp, cursor_in_echo_area, end_time) {
            match step {
                // gobble_input's -2: the controlling terminal's device
                // terminated, which ends Emacs.
                None => return Err(terminal_gone(interp, env)),
                Some(Some(event)) => {
                    interp.keyboard_input.last_event_reread = false;
                    record_external_input_event(interp, &event, env);
                    read_char_non_reread(interp, &event, env)?;
                    return Ok(Some(event));
                }
                Some(None) => {}
            }
        }
        match kbd_input::kbd_buffer_get_event_wait(interp, env, end_time, true)? {
            kbd_input::KbdWait::TimedOut => return Ok(None),
            kbd_input::KbdWait::Quit => {
                // The quit character; the quit `quit-flag' still holds
                // comes at the next quit check after the read returns.
                if let Some(event) = read_char_quit(interp, env)? {
                    return Ok(Some(event));
                }
            }
            kbd_input::KbdWait::Ready => {}
        }
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
    // Only while `memory-full' is nil; the hook only once Vrun_hooks is.
    if interp
        .lookup_var_key(cached_symbol!("memory-full"), env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(());
    }
    if !interp.hooks_cleared {
        crate::lisp::primitives::safe_run_hooks(
            interp,
            &cached_symbol!("post-command-hook").symbol(),
            env,
        )?;
    }
    // If displaying a message, resize the echo area window to fit that
    // message's size exactly.
    if crate::lisp::primitives::dispatch::display::echo_area_message_value().is_some() {
        command_loop::resize_echo_area_exactly();
    }
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
    // No prefix argument, `deactivate-mark' nil, not waiting for input,
    // and this-command-keys empty.
    interp.set_variable_key(cached_symbol!("prefix-arg"), Value::Nil, env);
    interp.set_variable_key(cached_symbol!("last-prefix-arg"), Value::Nil, env);
    // Vdeactivate_mark = Qnil: the binding in place, never a new local
    // one (the variable is automatically buffer-local).
    interp.set_c_variable("deactivate-mark", Value::Nil);
    clear_waiting_for_input();
    cancel_echoing(interp);
    clear_command_keys(interp);
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
    cancel_echoing(interp);
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
