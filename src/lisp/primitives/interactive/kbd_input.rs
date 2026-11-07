//! keyboard.c's input side as a terminal frontend feeds it: gobble_input
//! over the terminal's read_socket_hook, kbd_buffer_store_event's quit
//! character and `throw-on-input', readable_events and get_input_pending,
//! handle_interrupt's quit request and emergency escape, and the
//! terminal's input modes (sysdep.c's init_sys_modes applies them) with
//! the SIGINT, SIGQUIT and SIGWINCH a terminal session receives.
//!
//! The frontend's keyboard buffer (kbd_fetch_ptr .. kbd_store_ptr) holds
//! what gobble_input stored; on GNU/Linux, where INTERRUPT_INPUT makes
//! `interrupt_input' true, input reaches it when SIGIO's
//! handle_input_available_signal leaves process_pending_signals to call
//! handle_async_input, never from get_input_pending itself.

use super::*;

/// t->read_socket_hook: the ready input read and stored; how many events
/// were read, 0 for none, -2 when the terminal is gone.
pub(crate) type ReadSocketHook = Box<dyn FnMut(&mut Interpreter, &mut Env) -> i32>;

/// The terminal's reset_sys_modes, init_sys_modes and stuffing.
pub(crate) type SysModesHook = Box<dyn FnMut(SysModes)>;

/// The frontend's part of a terminal: t->read_socket_hook
/// (tty_read_avail_input), which reads what the terminal has ready and
/// stores each event with kbd_buffer_store_event, and the test of the
/// keyboard buffer readable_events makes.
pub(crate) struct KeyboardTerminal {
    /// Read the ready input, storing it; how many events were read, 0 for
    /// none, -2 when the terminal is gone.
    pub(crate) read_socket_hook: ReadSocketHook,
    /// kbd_fetch_ptr != kbd_store_ptr.
    pub(crate) buffer_nonempty: Box<dyn Fn() -> bool>,
    /// The descriptor add_keyboard_wait_descriptor gave the select.
    pub(crate) fd: Option<i32>,
}

thread_local! {
    static KEYBOARD_TERMINAL: std::cell::RefCell<Option<KeyboardTerminal>> =
        const { std::cell::RefCell::new(None) };
    /// The keyboard descriptor's fd_callback_info[fd].waiting_thread.
    static KEYBOARD_WAITING_THREAD: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
    /// keyboard.c's force_quit_count.
    static FORCE_QUIT_COUNT: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
    /// keyboard.c's waiting_for_input: wait_reading_process_output's
    /// set_waiting_for_input around its select, READ_KBD < 0 and the
    /// keyboard the waiting thread's.
    static WAITING_FOR_INPUT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// quit_throw_to_read_char's longjmp, which read_char's setjmp
    /// catches: the read returns quit_char.
    static QUIT_THROWN_TO_READ_CHAR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// keyboard.c's quit_char (`set-quit-char').
    static QUIT_CHAR: std::cell::Cell<i64> = const { std::cell::Cell::new(7) };
    /// The controlling terminal's tty_display_info flow_control and
    /// meta_key, None without a terminal session.
    static TTY_INPUT_MODES: std::cell::Cell<Option<TtyInputModes>> =
        const { std::cell::Cell::new(None) };
    /// sysdep.c's reset_sys_modes and init_sys_modes for that terminal.
    static SYS_MODES_HOOK: std::cell::RefCell<Option<SysModesHook>> =
        const { std::cell::RefCell::new(None) };
    /// dispnew.c's frame_garbaged: a size change (SIGWINCH's) the frame
    /// has not been redisplayed for yet.
    static FRAME_GARBAGED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// get_named_terminal (DEV_TTY): the session's terminal is the
    /// controlling one.
    static DEV_TTY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// How many SIGINTs or SIGQUITs arrived (deliver_interrupt_signal) since
/// the main thread's last process_pending_signals, which runs
/// handle_interrupt_signal once for each, as GNU's handler runs it once
/// per signal: a second with `quit-flag' still set is the emergency
/// escape.
static PENDING_INTERRUPTS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// SIGWINCH arrived (handle_window_change_signal); dispnew.c's
/// do_pending_window_change takes it.
static WINDOW_CHANGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The controlling terminal's input modes (tty_display_info's
/// flow_control and meta_key: 0 strips the 8th bit, 1 makes it Meta, 2
/// passes it through, 3 makes it Meta after decoding).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct TtyInputModes {
    pub(crate) flow_control: bool,
    pub(crate) meta_key: i32,
}

/// What the terminal's mode hook is asked to do.
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) enum SysModes {
    /// reset_sys_modes: the terminal as Emacs found it.
    Reset,
    /// init_sys_modes: Emacs's modes, from the current input modes, SIGIO
    /// requested with `interrupt_input'.
    Init { interrupt_input: bool },
    /// keyboard.c:stuff_buffered_input: these bytes, then what was read
    /// ahead and not used, pushed into the terminal's input.
    Stuff(Vec<u8>),
    /// The terminal was updated: publish its reset_sys_modes again for a
    /// fatal signal (arm_emergency_reset).
    Refresh,
}

/// term.c's init_tty for the controlling terminal: its input modes and
/// the hook that applies and undoes them; None at delete_terminal.
pub(crate) fn set_tty_input_modes(
    modes: Option<TtyInputModes>,
    controlling: bool,
    hook: Option<SysModesHook>,
) {
    TTY_INPUT_MODES.set(modes);
    DEV_TTY.set(modes.is_some() && controlling);
    SYS_MODES_HOOK.with_borrow_mut(|slot| *slot = hook);
}

/// The controlling terminal's input modes, None without one.
pub(crate) fn tty_input_modes() -> Option<TtyInputModes> {
    TTY_INPUT_MODES.get()
}

fn run_sys_modes_hook(op: SysModes) {
    let hook = SYS_MODES_HOOK.with_borrow_mut(Option::take);
    if let Some(mut hook) = hook {
        hook(op);
        SYS_MODES_HOOK.with_borrow_mut(|slot| {
            if slot.is_none() {
                *slot = Some(hook);
            }
        });
    }
}

/// sysdep.c:reset_all_sys_modes.
pub(crate) fn reset_all_sys_modes() {
    run_sys_modes_hook(SysModes::Reset);
}

/// The most bytes the emergency reset keeps; reset_sys_modes's output for
/// a terminal is far smaller (a `tty-mode-reset-strings' longer than this
/// is cut short there).
#[cfg(unix)]
const EMERGENCY_RESET_BYTES: usize = 4096;

/// One published copy of the controlling terminal's reset_sys_modes.
#[cfg(unix)]
struct EmergencyResetSlot {
    fd: libc::c_int,
    length: usize,
    bytes: [u8; EMERGENCY_RESET_BYTES],
    old_tty: Option<libc::termios>,
    old_flags: libc::c_int,
    inherited_pgroup: libc::pid_t,
}

/// shut_down_emacs's reset_all_sys_modes for a fatal signal, which can
/// arrive on any thread and in the middle of anything (an allocation, a
/// borrow of the terminal's state): what reset_sys_modes would do as of
/// the terminal's last update, readable with no lock or allocation.  The
/// main thread fills the slot not published, then publishes it, so a
/// signal arriving mid-fill reads the previous copy whole.
#[cfg(unix)]
struct EmergencyReset {
    slots: [std::cell::UnsafeCell<EmergencyResetSlot>; 2],
    /// The slot to read: 0 or 1, or -1 while Emacs's modes are not in
    /// force.
    published: std::sync::atomic::AtomicI32,
}

// SAFETY: only the main thread writes a slot, and never the published
// one; readers take the published slot once (see emergency_reset).
#[cfg(unix)]
unsafe impl Sync for EmergencyReset {}

#[cfg(unix)]
const EMPTY_EMERGENCY_SLOT: EmergencyResetSlot = EmergencyResetSlot {
    fd: -1,
    length: 0,
    bytes: [0; EMERGENCY_RESET_BYTES],
    old_tty: None,
    old_flags: 0,
    inherited_pgroup: 0,
};

#[cfg(unix)]
static EMERGENCY_RESET: EmergencyReset = EmergencyReset {
    slots: [
        std::cell::UnsafeCell::new(EMPTY_EMERGENCY_SLOT),
        std::cell::UnsafeCell::new(EMPTY_EMERGENCY_SLOT),
    ],
    published: std::sync::atomic::AtomicI32::new(-1),
};

/// The controlling terminal's state reset_sys_modes restores: its
/// descriptor FD, the bytes it writes (BYTES), the modes and descriptor
/// flags Emacs found, and init_foreground_group's inherited group (0 when
/// Emacs leads its own).
#[cfg(unix)]
pub(crate) struct ResetState<'a> {
    pub(crate) fd: libc::c_int,
    pub(crate) bytes: &'a [u8],
    pub(crate) old_tty: Option<libc::termios>,
    pub(crate) old_flags: libc::c_int,
    pub(crate) inherited_pgroup: libc::pid_t,
}

/// Publish STATE as what a fatal signal resets the terminal with; called
/// on the main thread when Emacs's modes take effect and at each update.
#[cfg(unix)]
pub(crate) fn arm_emergency_reset(state: &ResetState<'_>) {
    use std::sync::atomic::Ordering;
    let slot = if EMERGENCY_RESET.published.load(Ordering::Acquire) == 0 {
        1
    } else {
        0
    };
    // SAFETY: only the main thread fills slots, and SLOT is not the
    // published one, so no reader holds it.
    let target = unsafe { &mut *EMERGENCY_RESET.slots[slot].get() };
    let length = state.bytes.len().min(EMERGENCY_RESET_BYTES);
    target.bytes[..length].copy_from_slice(&state.bytes[..length]);
    target.length = length;
    target.fd = state.fd;
    target.old_tty = state.old_tty;
    target.old_flags = state.old_flags;
    target.inherited_pgroup = state.inherited_pgroup;
    EMERGENCY_RESET
        .published
        .store(slot as i32, Ordering::Release);
}

/// Emacs's modes are no longer in force (reset_sys_modes ran).
#[cfg(unix)]
pub(crate) fn disarm_emergency_reset() {
    EMERGENCY_RESET
        .published
        .store(-1, std::sync::atomic::Ordering::Release);
}

/// The fatal-signal reset_all_sys_modes: once, from any thread, with
/// async-signal-safe calls only (write, tcdrain, fcntl, tcsetattr,
/// setpgid, tcsetpgrp, pthread_sigmask).
#[cfg(unix)]
pub(crate) fn emergency_reset() {
    let slot = EMERGENCY_RESET
        .published
        .swap(-1, std::sync::atomic::Ordering::AcqRel);
    let Ok(slot) = usize::try_from(slot) else {
        return;
    };
    // SAFETY: the published slot is never written; taking it unpublished
    // makes this the only reset.
    let state = unsafe { &*EMERGENCY_RESET.slots[slot].get() };
    // SAFETY: the calls are async-signal-safe; FD is the terminal's open
    // descriptor and the structures are the slot's own.
    unsafe {
        let mut rest = &state.bytes[..state.length];
        while !rest.is_empty() {
            let written = libc::write(state.fd, rest.as_ptr().cast(), rest.len());
            if written < 0 {
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                break;
            }
            rest = &rest[written as usize..];
        }
        // last_os_error reads errno into a value, allocating nothing.
        let interrupted = || std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR);
        while libc::tcdrain(state.fd) != 0 && interrupted() {}
        libc::fcntl(state.fd, libc::F_SETFL, state.old_flags);
        if let Some(old) = &state.old_tty {
            while libc::tcsetattr(state.fd, libc::TCSADRAIN, old) != 0 && interrupted() {}
        }
        // widen_foreground_group.
        if state.inherited_pgroup != 0 && libc::setpgid(0, state.inherited_pgroup) == 0 {
            let mut blocked: libc::sigset_t = std::mem::zeroed();
            let mut old: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut blocked);
            libc::sigaddset(&mut blocked, libc::SIGTTOU);
            libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, &mut old);
            libc::tcsetpgrp(state.fd, state.inherited_pgroup);
            libc::pthread_sigmask(libc::SIG_SETMASK, &old, std::ptr::null_mut());
        }
    }
}

/// The terminal's latest update: what a fatal signal would reset with is
/// published again (its size and `tty-mode-reset-strings' may have
/// changed).
pub(crate) fn refresh_emergency_reset() {
    run_sys_modes_hook(SysModes::Refresh);
}

thread_local! {
    /// The VERASE the last init_sys_modes found, for `tty-erase-char'.
    static TTY_ERASE_CHAR: std::cell::Cell<Option<u8>> = const { std::cell::Cell::new(None) };
}

/// init_sys_modes's terminal VERASE: ERASE becomes `tty-erase-char'.
pub(crate) fn note_tty_erase_char(erase: Option<u8>) {
    TTY_ERASE_CHAR.set(erase);
}

/// sysdep.c:init_all_sys_modes, SIGIO requested with INTERRUPT_INPUT:
/// init_sys_modes sets `tty-erase-char' to nil, then (with a terminal)
/// to the terminal's VERASE.
pub(crate) fn init_all_sys_modes(interp: &mut Interpreter, interrupt_input: bool) {
    TTY_ERASE_CHAR.set(None);
    run_sys_modes_hook(SysModes::Init { interrupt_input });
    let erase = TTY_ERASE_CHAR
        .take()
        .map_or(Value::Nil, |erase| Value::Integer(i64::from(erase)));
    interp.set_c_variable("tty-erase-char", erase);
}

/// A change of the controlling terminal's input modes, as
/// Fset_output_flow_control and Fset_input_meta_mode make one:
/// reset_sys_modes, the new modes, init_sys_modes.  False without a
/// terminal session.
pub(crate) fn change_tty_input_modes(
    interp: &mut Interpreter,
    interrupt_input: bool,
    change: impl FnOnce(&mut TtyInputModes),
) -> bool {
    let Some(mut modes) = TTY_INPUT_MODES.get() else {
        return false;
    };
    let old = modes;
    change(&mut modes);
    if modes != old {
        reset_all_sys_modes();
        TTY_INPUT_MODES.set(Some(modes));
        init_all_sys_modes(interp, interrupt_input);
    }
    true
}

/// Fset_quit_char's store: QUIT kept in range of the terminal's meta mode,
/// between reset_sys_modes and init_sys_modes.
pub(crate) fn set_quit_char(interp: &mut Interpreter, quit: i64, interrupt_input: bool) {
    let mask = if TTY_INPUT_MODES
        .get()
        .is_some_and(|modes| modes.meta_key == 0)
    {
        0o177
    } else {
        0o377
    };
    reset_all_sys_modes();
    QUIT_CHAR.set(quit & mask);
    init_all_sys_modes(interp, interrupt_input);
}

/// get_named_terminal (DEV_TTY): a terminal session on the controlling
/// terminal, whose C-g is SIGINT.
pub(crate) fn dev_tty_terminal() -> bool {
    DEV_TTY.get()
}

/// deliver_interrupt_signal's handler for SIGINT and SIGQUIT: the main
/// thread's next quit check takes it.  Async-signal-safe.
pub(crate) fn note_interrupt_signal() {
    PENDING_INTERRUPTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    crate::lisp::primitives::atimer::note_pending_signal();
}

/// handle_window_change_signal's handler for SIGWINCH: the size is read
/// when do_pending_window_change takes it.  Async-signal-safe.
pub(crate) fn note_window_change_signal() {
    WINDOW_CHANGED.store(true, std::sync::atomic::Ordering::Relaxed);
    crate::lisp::primitives::atimer::note_pending_signal();
}

/// A SIGWINCH not yet taken (handle_window_change_signal's work, which
/// do_pending_window_change does outside the handler).
pub(crate) fn take_window_change_signal() -> bool {
    WINDOW_CHANGED.swap(false, std::sync::atomic::Ordering::Relaxed)
}

/// dispnew.c's frame_garbaged, taken by the redisplay that clears it.
pub(crate) fn take_frame_garbaged() -> bool {
    FRAME_GARBAGED.replace(false)
}

/// frame_garbaged = true: the next redisplay paints the whole frame.
pub(crate) fn set_frame_garbaged() {
    FRAME_GARBAGED.set(true);
}

/// Whether the frame awaits a whole redisplay.
pub(crate) fn frame_garbaged() -> bool {
    FRAME_GARBAGED.get()
}

/// keyboard.c:stuff_buffered_input: STUFFSTRING's bytes and a newline
/// (when it is a string), then what was read ahead, back as the
/// terminal's input for the shell; the keyboard buffer is emptied.
pub(crate) fn stuff_buffered_input(stuffstring: Option<&[u8]>) {
    let mut stuff = Vec::new();
    if let Some(bytes) = stuffstring {
        stuff.extend_from_slice(bytes);
        stuff.push(b'\n');
    }
    run_sys_modes_hook(SysModes::Stuff(stuff));
}

/// keyboard.c:handle_interrupt_signal for a SIGINT or SIGQUIT the main
/// thread takes: with no frame on the controlling terminal, `quit-flag'
/// `kill-emacs' (so the next quit check kills Emacs); otherwise
/// handle_interrupt.  A Lisp thread other than the main one leaves it
/// pending, as deliver_process_signal hands it to the main thread.
pub(crate) fn process_interrupt_signal(interp: &mut Interpreter, env: &mut Env) {
    if !interp.current_thread_is_main() {
        return;
    }
    let signals = PENDING_INTERRUPTS.swap(0, std::sync::atomic::Ordering::Relaxed);
    for _ in 0..signals {
        if !dev_tty_terminal() {
            interp.set_c_variable("quit-flag", Value::symbol("kill-emacs"));
            continue;
        }
        handle_interrupt(interp, env, true);
    }
}

/// Whether a SIGINT awaits the main thread.
pub(crate) fn interrupt_signal_pending() -> bool {
    PENDING_INTERRUPTS.load(std::sync::atomic::Ordering::Relaxed) > 0
}

/// term.c's init_tty: the terminal's hooks, and add_keyboard_wait_descriptor
/// of its input descriptor; None deletes them (delete_terminal).
pub(crate) fn set_keyboard_terminal(terminal: Option<KeyboardTerminal>) {
    KEYBOARD_TERMINAL.with_borrow_mut(|slot| *slot = terminal);
    KEYBOARD_WAITING_THREAD.set(None);
}

/// The descriptors add_keyboard_wait_descriptor gave the select
/// (FOR_READ | KEYBOARD_FD): each terminal's input descriptor.
fn keyboard_descriptors(interp: &Interpreter) -> Vec<i32> {
    let mut fds: Vec<i32> = KEYBOARD_TERMINAL
        .with_borrow(|slot| slot.as_ref().and_then(|t| t.fd))
        .into_iter()
        .collect();
    #[cfg(unix)]
    fds.extend(interp.terminal_input_fds());
    #[cfg(not(unix))]
    let _ = interp;
    fds
}

/// The keyboard descriptors compute_input_wait_mask puts in a select for
/// THREAD: the terminals' input descriptors, unless another thread is
/// waiting on them, marked as THREAD's when MARK.
pub(crate) fn keyboard_wait_descriptors(interp: &Interpreter, thread: u64, mark: bool) -> Vec<i32> {
    if KEYBOARD_WAITING_THREAD
        .get()
        .is_some_and(|waiter| waiter != thread)
    {
        return Vec::new();
    }
    let fds = keyboard_descriptors(interp);
    if mark && !fds.is_empty() {
        KEYBOARD_WAITING_THREAD.set(Some(thread));
    }
    fds
}

/// clear_waiting_thread_info for the keyboard descriptor.
pub(crate) fn clear_keyboard_waiting_thread(thread: u64) {
    if KEYBOARD_WAITING_THREAD.get() == Some(thread) {
        KEYBOARD_WAITING_THREAD.set(None);
    }
}

/// process.c's kbd_is_ours: a keyboard descriptor is being monitored by
/// THREAD (false when there is none, as in batch).
pub(crate) fn kbd_is_ours(interp: &Interpreter, thread: u64) -> bool {
    !keyboard_descriptors(interp).is_empty() && KEYBOARD_WAITING_THREAD.get() == Some(thread)
}

/// keyboard.c:set_waiting_for_input: handle_interrupt may now throw back
/// to read_char, and a C-g it buffered before is thrown at once
/// (quit_throw_to_read_char (0)).  True when the wait ends so.
pub(crate) fn set_waiting_for_input(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    WAITING_FOR_INPUT.set(true);
    if !interp.quit_flag_is_nil() {
        quit_throw_to_read_char(interp, env, false)?;
        return Ok(true);
    }
    Ok(false)
}

/// keyboard.c:clear_waiting_for_input.
pub(crate) fn clear_waiting_for_input() {
    WAITING_FOR_INPUT.set(false);
}

/// Whether quit_throw_to_read_char was called since read_char last took
/// it: the read returns quit_char.
pub(crate) fn take_quit_thrown_to_read_char() -> bool {
    QUIT_THROWN_TO_READ_CHAR.replace(false)
}

/// keyboard.c:gobble_input: each terminal's read_socket_hook until it
/// reads nothing.  A tty's tty_read_avail_input stores with
/// kbd_buffer_store_event, no HOLD_QUIT, so its quit character is
/// handle_interrupt's as it is read.  Returns how many events were read.
pub(crate) fn gobble_input(interp: &mut Interpreter, env: &mut Env) -> i32 {
    let mut nread = 0;
    if let Some(mut terminal) = KEYBOARD_TERMINAL.with_borrow_mut(Option::take) {
        let nr = loop {
            let nr = (terminal.read_socket_hook)(interp, env);
            if nr <= 0 {
                break nr;
            }
            nread += nr;
        };
        KEYBOARD_TERMINAL.with_borrow_mut(|slot| {
            if slot.is_none() {
                *slot = Some(terminal);
            }
        });
        // -2, a non-transient error: the terminal device terminated, and
        // with it the last terminal, so Emacs ends as for SIGHUP
        // (terminate_due_to_signal (SIGHUP, 10)).
        if nr == -2 && super::request_fatal_signal(libc::SIGHUP) {
            crate::lisp::primitives::atimer::note_pending_signal();
        }
    }
    // The secondary terminals' tty_read_avail_input.
    if interp.service_terminal_input(env).unwrap_or(false) {
        nread += 1;
    }
    nread
}

/// keyboard.c:kbd_buffer_store_event (kbd_buffer_store_event_hold with no
/// HOLD_QUIT) for an event a terminal read: whether it goes into the
/// keyboard buffer.  The quit character is not stored but given to
/// handle_interrupt at once; any other input inside `while-no-input' sets
/// `quit-flag' to `throw-on-input'.
pub(crate) fn kbd_buffer_store_event(
    interp: &mut Interpreter,
    env: &mut Env,
    event: Option<&Value>,
) -> bool {
    if let Some(event) = event
        && *event == Value::Integer(quit_char())
    {
        handle_interrupt(interp, env, false);
        return false;
    }
    let throw_on_input = interp.throw_on_input_value();
    if !throw_on_input.is_nil() && !is_ignored_event(interp, event) {
        interp.set_c_variable("quit-flag", throw_on_input);
    }
    true
}

/// keyboard.c:is_ignored_event for a keyboard or mouse event: its kind
/// names no symbol, so it is ignored only when
/// `while-no-input-ignore-events' holds nil.
fn is_ignored_event(interp: &Interpreter, _event: Option<&Value>) -> bool {
    interp
        .lookup_var_key(cached_symbol!("while-no-input-ignore-events"), &Env::new())
        .and_then(|list| list.to_vec().ok())
        .is_some_and(|items| items.iter().any(Value::is_nil))
}

/// keyboard.c's quit_char (`set-quit-char', `set-input-mode''s QUIT),
/// C-g until changed.
pub(crate) fn quit_char() -> i64 {
    QUIT_CHAR.get()
}

/// keyboard.c:readable_events (no flags): the keyboard buffer holds an
/// event: a terminal's, a THREAD_EVENT `thread-signal' stored for the main
/// thread, or a user signal's (store_user_signal_events's, which
/// gobble_input stores).
pub(crate) fn readable_events(interp: &Interpreter) -> bool {
    terminal_buffer_nonempty() || special_events_pending(interp)
}

/// The terminal's events in the keyboard buffer.
fn terminal_buffer_nonempty() -> bool {
    KEYBOARD_TERMINAL.with_borrow(|slot| slot.as_ref().is_some_and(|t| (t.buffer_nonempty)()))
}

/// The keyboard buffer's events that are no terminal's: a THREAD_EVENT
/// stored for the main thread, or a pending user signal's.
fn special_events_pending(interp: &Interpreter) -> bool {
    !interp.pending_thread_events.is_empty() || super::user_signals_pending()
}

/// keyboard.c:requeued_events_pending_p: an event in
/// `unread-command-events' (CONSP) or a non-nil
/// `unread-post-input-method-events' or `unread-input-method-events'.
pub(crate) fn requeued_events_pending_p(interp: &Interpreter, env: &Env) -> bool {
    unread_command_events_pending(interp, env)
        || [
            cached_symbol!("unread-post-input-method-events"),
            cached_symbol!("unread-input-method-events"),
        ]
        .into_iter()
        .any(|list| {
            interp
                .lookup_var_key(list, env)
                .is_some_and(|value| !value.is_nil())
        })
}

/// CONSP (Vunread_command_events): any value but a cons holds no event.
pub(crate) fn unread_command_events_pending(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("unread-command-events"), env)
        .is_some_and(|events| events.is_cons())
}

/// keyboard.c:get_input_pending: `quit-flag' set or an event in the
/// keyboard buffer, the timers run first with DO_TIMERS_NOW (whether one
/// ran is the second value).  Input read as it arrives (`interrupt_input',
/// SIGIO's) is all there is; otherwise gobble_input reads what is there.
pub(crate) fn get_input_pending(
    interp: &mut Interpreter,
    env: &mut Env,
    do_timers_now: bool,
) -> Result<(bool, bool), LispError> {
    let mut timers_ran = false;
    // First of all, have we already counted some input?  (readable_events
    // with READABLE_EVENTS_DO_TIMERS_NOW runs timer_check first.)
    let mut readable = |interp: &mut Interpreter, env: &mut Env| -> Result<bool, LispError> {
        if !interp.quit_flag_is_nil() {
            return Ok(true);
        }
        if do_timers_now {
            timers_ran |= interp.run_pending_timer_events(env)?;
        }
        Ok(readable_events(interp))
    };
    let mut pending = readable(interp, env)?;
    if !pending && !interp.input_interrupt_mode() {
        gobble_input(interp, env);
        pending = readable(interp, env)?;
    }
    Ok((pending, timers_ran))
}

/// keyboard.c:detect_input_pending.
pub(crate) fn detect_input_pending(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    Ok(get_input_pending(interp, env, false)?.0)
}

/// keyboard.c:detect_input_pending_run_timers: the timers run when no
/// input is pending, a redisplay after those that ran with DO_DISPLAY.
pub(crate) fn detect_input_pending_run_timers(
    interp: &mut Interpreter,
    env: &mut Env,
    do_display: bool,
) -> Result<bool, LispError> {
    if get_input_pending(interp, env, false)?.0 {
        return Ok(true);
    }
    let (pending, timers_ran) = get_input_pending(interp, env, true)?;
    if timers_ran && do_display {
        redisplay_preserve_echo_area(interp, env);
    }
    Ok(pending)
}

/// keyboard.c:swallow_events: process_special_events (the selection
/// requests a terminal frame has none of), then get_input_pending with
/// the timers run, a redisplay after those that ran when no input came.
pub(crate) fn swallow_events(
    interp: &mut Interpreter,
    env: &mut Env,
    do_display: bool,
) -> Result<(), LispError> {
    let (pending, timers_ran) = get_input_pending(interp, env, true)?;
    if !pending && timers_ran && do_display {
        redisplay_preserve_echo_area(interp, env);
    }
    Ok(())
}

/// xdisp.c:redisplay_preserve_echo_area: the frontend's redisplay, with
/// the echo area's message kept (none without a frontend: batch).
pub(crate) fn redisplay_preserve_echo_area(interp: &mut Interpreter, env: &mut Env) {
    super::run_tty_frame_redraw(interp, env);
}

/// keyboard.c:handle_interrupt for a quit character a terminal read
/// (IN_SIGNAL_HANDLER false) or SIGINT's (true): request a quit, the
/// third request in a row giving up `inhibit-quit', and make a waiting
/// read_char return the quit character.
pub(crate) fn handle_interrupt(interp: &mut Interpreter, env: &mut Env, in_signal_handler: bool) {
    // cancel_echoing: the keys echoed so far are echoed no more.
    super::set_pending_keystroke_echo(None);
    if !interp.quit_flag_is_nil() && dev_tty_terminal() {
        // A quit not yet taken: the emergency escape.
        emergency_escape(interp, env, in_signal_handler);
    } else {
        // Request quit when it's safe.
        let count = if interp.quit_flag_is_nil() {
            1
        } else {
            FORCE_QUIT_COUNT.get() + 1
        };
        FORCE_QUIT_COUNT.set(count);
        if count == 3 {
            interp.set_c_variable("inhibit-quit", Value::Nil);
        }
        interp.set_c_variable("quit-flag", Value::T);
    }
    if WAITING_FOR_INPUT.get() {
        // `quit-flag' is t, so no `kill-emacs' to run.
        let _ = quit_throw_to_read_char(interp, env, in_signal_handler);
    }
}

/// handle_interrupt's emergency escape, for a C-g while `quit-flag' is
/// still set on the controlling terminal: the terminal's modes reset, the
/// job stopped (sys_suspend), and on its return the questions on the
/// standard streams, an auto-save and an abort offered; then the modes
/// again.
fn emergency_escape(interp: &mut Interpreter, env: &mut Env, in_signal_handler: bool) {
    #[cfg(unix)]
    {
        // Don't let a SIGINT interrupt the escape (in GNU's handler SIGINT
        // is already blocked; elsewhere stdout is flushed too).
        // SAFETY: the set is local and initialized by the calls.
        unsafe {
            let mut blocked: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut blocked);
            libc::sigaddset(&mut blocked, libc::SIGINT);
            libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, std::ptr::null_mut());
        }
        if !in_signal_handler {
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
        reset_all_sys_modes();
        sys_suspend();
        write_stdout("Emacs is resuming after an emergency escape.\n");
        // A collection is never in progress where Lisp checks for quits.
        write_stdout("Auto-save? (y or n) ");
        let mut c = read_stdin();
        if matches!(c, Some(b'y' | b'Y')) {
            let _ =
                crate::lisp::primitives::file_io::do_auto_save(interp, env, Value::T, Value::Nil);
            write_stdout("Auto-save done\n");
        }
        while c.is_some_and(|c| c != b'\n') {
            c = read_stdin();
        }
        write_stdout("Abort (and dump core)? (y or n) ");
        c = read_stdin();
        if matches!(c, Some(b'y' | b'Y')) {
            std::process::abort();
        }
        while c.is_some_and(|c| c != b'\n') {
            c = read_stdin();
        }
        write_stdout("Continuing...\n");
        let interrupt_input = interp.input_interrupt_mode();
        init_all_sys_modes(interp, interrupt_input);
        // pthread_sigmask (SIG_SETMASK, &empty_mask, 0).
        // SAFETY: the set is local and initialized by the call.
        unsafe {
            let mut empty: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut empty);
            libc::pthread_sigmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut());
        }
    }
    #[cfg(not(unix))]
    let _ = (interp, env, in_signal_handler);
}

/// sysdep.c:sys_suspend: the job stopped, as a shell's C-z stops it.
pub(crate) fn sys_suspend() {
    #[cfg(unix)]
    // SAFETY: kill has no memory preconditions.
    unsafe {
        libc::kill(0, libc::SIGTSTP);
    }
}

/// keyboard.c's write_stdout: MSG on the standard output, unbuffered.
#[cfg(unix)]
fn write_stdout(msg: &str) {
    // SAFETY: MSG is a live buffer of its length.
    unsafe {
        let _ = libc::write(libc::STDOUT_FILENO, msg.as_ptr().cast(), msg.len());
    }
}

/// keyboard.c's read_stdin: a byte of the standard input, unbuffered;
/// None at its end (where GNU's loops would read EOF without end).
#[cfg(unix)]
fn read_stdin() -> Option<u8> {
    let mut c = 0u8;
    // SAFETY: C is one writable byte.
    (unsafe { libc::read(libc::STDIN_FILENO, (&mut c as *mut u8).cast(), 1) } == 1).then_some(c)
}

/// keyboard.c:quit_throw_to_read_char: outside a signal handler a
/// `quit-flag' of `kill-emacs' kills Emacs; then no more waiting, no input
/// pending, `unread-command-events' emptied, and read_char's setjmp taken.
fn quit_throw_to_read_char(
    interp: &mut Interpreter,
    env: &mut Env,
    from_signal: bool,
) -> Result<(), LispError> {
    if !from_signal && crate::lisp::types::eq_symbol_named(interp.quit_flag_value(), "kill-emacs") {
        call(interp, "kill-emacs", &[Value::Nil, Value::Nil], env)?;
    }
    WAITING_FOR_INPUT.set(false);
    interp.set_variable_key(cached_symbol!("unread-command-events"), Value::Nil, env);
    QUIT_THROWN_TO_READ_CHAR.set(true);
    Ok(())
}

/// What kbd_buffer_get_event's wait for input found.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum KbdWait {
    /// An event in the keyboard buffer, or one in `unread-command-events'.
    Ready,
    /// quit_throw_to_read_char: read_char returns the quit character.
    Quit,
    /// END_TIME passed with no input ("Finished waiting").
    TimedOut,
}

/// keyboard.c:kbd_buffer_get_event's loop "until there is input
/// available": an unread command event or a stored one ends it, a set
/// `quit-flag' is quit_throw_to_read_char, gobble_input reads what SIGIO
/// left, and the wait is wait_reading_process_output with READ_KBD -1
/// (DO_DISPLAY false while a tty menu shows) until END_TIME.
pub(crate) fn kbd_buffer_get_event_wait(
    interp: &mut Interpreter,
    env: &mut Env,
    end_time: Option<std::time::Instant>,
    do_display: bool,
) -> Result<KbdWait, LispError> {
    use crate::lisp::primitives::processes::{
        WAIT_READING_MAX, WaitRequest, wait_reading_process_output,
    };
    loop {
        // Break loop if there's an unread command event.
        if unread_command_events_pending(interp, env) {
            return Ok(KbdWait::Ready);
        }
        if terminal_buffer_nonempty() {
            return Ok(KbdWait::Ready);
        }
        // The buffer's user signal and thread events: read_char runs the
        // `special-event-map' binding of each and reads on (goto retry); one
        // the map does not bind is the read's, through
        // `unread-command-events'.
        if special_events_pending(interp) {
            super::run_pending_user_signal_events(interp, env)?;
            continue;
        }
        // If the quit flag is set, then read_char will return quit_char,
        // so that counts as "available input."
        if thrown_to_read_char(interp, env)? {
            return Ok(KbdWait::Quit);
        }
        // One way or another, wait until input is available; then, if
        // interrupt handlers have not read it, read it now (USABLE_SIGIO).
        gobble_input(interp, env);
        if terminal_buffer_nonempty() {
            return Ok(KbdWait::Ready);
        }
        if special_events_pending(interp) {
            continue;
        }
        let request = if let Some(end) = end_time {
            let now = std::time::Instant::now();
            if end <= now {
                return Ok(KbdWait::TimedOut);
            }
            let duration = end - now;
            WaitRequest {
                time_limit: i64::try_from(duration.as_secs()).unwrap_or(WAIT_READING_MAX),
                nsecs: i64::from(duration.subsec_nanos()),
                read_kbd: -1,
                do_display: true,
                wait_proc: None,
                just_wait_proc: 0,
            }
        } else {
            WaitRequest {
                time_limit: 0,
                nsecs: 0,
                read_kbd: -1,
                do_display,
                wait_proc: None,
                just_wait_proc: 0,
            }
        };
        wait_reading_process_output(interp, env, request)?;
        if take_quit_thrown_to_read_char() {
            return Ok(KbdWait::Quit);
        }
        if !interp.input_interrupt_mode() && !readable_events(interp) {
            gobble_input(interp, env);
        }
    }
}

/// read_char's setjmp for kbd_buffer_get_event's test of `quit-flag':
/// whether quit_throw_to_read_char ran, in handle_interrupt during the
/// wait or now (quit_throw_to_read_char (0)) for a set `quit-flag'.
fn thrown_to_read_char(interp: &mut Interpreter, env: &mut Env) -> Result<bool, LispError> {
    if take_quit_thrown_to_read_char() {
        return Ok(true);
    }
    if interp.quit_flag_is_nil() {
        return Ok(false);
    }
    quit_throw_to_read_char(interp, env, false)?;
    Ok(take_quit_thrown_to_read_char())
}
