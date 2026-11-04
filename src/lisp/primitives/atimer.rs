//! atimer.c: timers that run their function when a SIGALRM says they are
//! ripe, at the next pending-signals check (keyboard.c's
//! process_pending_signals, from maybe_quit's probably_quit).
//!
//! The list is kept by expiration on CLOCK_REALTIME; set_alarm arms
//! init_atimer's POSIX timer (and its timerfd, as GNU arms both) for the
//! first.  Emaxx's one client is eval.c's `funcall-with-delayed-message'.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering::Relaxed};

/// keyboard.c's pending_signals: a signal handler asked for
/// process_pending_signals.  Every handler that defers its work to a quit
/// check (atimer.c's, terminate_due_to_signal's) sets it.
static PENDING_SIGNALS: AtomicBool = AtomicBool::new(false);

/// Whether a handler left work for `process_pending_signals'.
#[inline(always)]
pub(crate) fn signals_pending() -> bool {
    PENDING_SIGNALS.load(Relaxed)
}

/// A handler's request for the next quit check.
#[inline(always)]
pub(crate) fn note_pending_signal() {
    PENDING_SIGNALS.store(true, Relaxed);
}

/// The pending flag taken for a check: whether it was set.
pub(crate) fn take_pending_signals() -> bool {
    PENDING_SIGNALS.swap(false, Relaxed)
}

/// atimer.h's enum atimer_type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum AtimerType {
    /// Expires at the time given.
    #[allow(dead_code)]
    Absolute,
    /// Expires the time given from now.
    Relative,
    /// Expires the time given from now, and again each time after.
    #[allow(dead_code)]
    Continuous,
}

/// What an atimer does when it runs (its fn and client_data).
#[derive(Clone, Debug)]
pub(crate) enum AtimerCallback {
    /// eval.c:with_delayed_message_display: message3 of the text.
    DelayedMessage(Value),
}

/// The handle `start_atimer' returns and `cancel_atimer' takes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct AtimerId(u64);

struct Atimer {
    id: AtimerId,
    kind: AtimerType,
    expiration: libc::timespec,
    interval: libc::timespec,
    callback: AtimerCallback,
}

/// atimer.c's `atimers', sorted by expiration.  The interpreter thread's:
/// a callback holds Lisp objects of its interpreter.
struct Atimers {
    active: Vec<Atimer>,
    next_id: u64,
}

thread_local! {
    static ATIMERS: std::cell::RefCell<Atimers> = const {
        std::cell::RefCell::new(Atimers { active: Vec::new(), next_id: 0 })
    };
}

/// init_atimer's timerfd, -1 without one.
static TIMERFD: AtomicI32 = AtomicI32::new(-1);
/// init_atimer's alarm_timer, and whether timer_create made it.
static ALARM_TIMER: AtomicUsize = AtomicUsize::new(0);
static ALARM_TIMER_OK: AtomicBool = AtomicBool::new(false);

/// systime.h:current_timespec, CLOCK_REALTIME.
fn current_timespec() -> libc::timespec {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: NOW is owned.
    unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut now) };
    now
}

fn timespec_cmp(a: &libc::timespec, b: &libc::timespec) -> std::cmp::Ordering {
    (a.tv_sec, a.tv_nsec).cmp(&(b.tv_sec, b.tv_nsec))
}

/// timespec.h:timespec_add, saturating as Gnulib's does.
fn timespec_add(a: libc::timespec, b: libc::timespec) -> libc::timespec {
    let mut sec = a.tv_sec.saturating_add(b.tv_sec);
    let mut nsec = a.tv_nsec + b.tv_nsec;
    if nsec >= 1_000_000_000 {
        nsec -= 1_000_000_000;
        sec = sec.saturating_add(1);
    }
    libc::timespec {
        tv_sec: sec,
        tv_nsec: nsec,
    }
}

fn timespec_sub(a: libc::timespec, b: libc::timespec) -> libc::timespec {
    let mut sec = a.tv_sec.saturating_sub(b.tv_sec);
    let mut nsec = a.tv_nsec - b.tv_nsec;
    if nsec < 0 {
        nsec += 1_000_000_000;
        sec = sec.saturating_sub(1);
    }
    libc::timespec {
        tv_sec: sec,
        tv_nsec: nsec,
    }
}

/// Gnulib's dtotimespec: SEC as a timespec, the seconds truncated toward
/// zero and the nanoseconds rounded up, a value outside time_t's range
/// (or a NaN) clipped to its extremes.
pub(crate) fn dtotimespec(sec: f64) -> libc::timespec {
    use std::cmp::Ordering::Less;
    const HZ: libc::c_long = 1_000_000_000;
    let min = libc::time_t::MIN as f64;
    let max = libc::time_t::MAX as f64;
    // `! (TYPE_MINIMUM (time_t) < sec)', true of a NaN.
    if min.partial_cmp(&sec) != Some(Less) {
        return libc::timespec {
            tv_sec: libc::time_t::MIN,
            tv_nsec: 0,
        };
    }
    // `! (sec < 1.0 + TYPE_MAXIMUM (time_t))'.
    if sec.partial_cmp(&(1.0 + max)) != Some(Less) {
        return libc::timespec {
            tv_sec: libc::time_t::MAX,
            tv_nsec: HZ - 1,
        };
    }
    let mut s = sec as libc::time_t;
    let frac = HZ as f64 * (sec - s as f64);
    let mut ns = frac as libc::c_long;
    if (ns as f64) < frac {
        ns += 1;
    }
    s = s.saturating_add(ns / HZ);
    ns %= HZ;
    if ns < 0 {
        s -= 1;
        ns += HZ;
    }
    libc::timespec {
        tv_sec: s,
        tv_nsec: ns,
    }
}

/// atimer.c:handle_alarm_signal.
extern "C" fn handle_alarm_signal(_signal: libc::c_int) {
    note_pending_signal();
}

static ATIMER_INITIALIZED: std::sync::Once = std::sync::Once::new();

fn ensure_atimer_initialized() {
    ATIMER_INITIALIZED.call_once(init_atimer_once);
}

/// atimer.c:init_atimer: the timerfd (unless EMACS_IGNORE_TIMERFD is set),
/// the POSIX timer that sends SIGALRM, and SIGALRM's handler.
pub(crate) fn init_atimer() {
    ensure_atimer_initialized();
}

fn init_atimer_once() {
    #[cfg(target_os = "linux")]
    {
        if std::env::var_os("EMACS_IGNORE_TIMERFD").is_none() {
            // SAFETY: creates a descriptor this module owns.
            let fd = unsafe {
                libc::timerfd_create(libc::CLOCK_REALTIME, libc::TFD_NONBLOCK | libc::TFD_CLOEXEC)
            };
            TIMERFD.store(fd, Relaxed);
        }
        // "We're starting the alarms even if we have timerfd, because
        // timerfd events do not fire while Emacs Lisp is busy."
        // SAFETY: SIGEV and TIMER are owned; timer_create writes TIMER.
        unsafe {
            let mut sigev: libc::sigevent = std::mem::zeroed();
            sigev.sigev_notify = libc::SIGEV_SIGNAL;
            sigev.sigev_signo = libc::SIGALRM;
            sigev.sigev_value.sival_ptr = ALARM_TIMER.as_ptr().cast();
            let mut timer: libc::timer_t = std::ptr::null_mut();
            if libc::timer_create(libc::CLOCK_REALTIME, &mut sigev, &mut timer) == 0 {
                ALARM_TIMER.store(timer as usize, Relaxed);
                ALARM_TIMER_OK.store(true, Relaxed);
            }
        }
    }
    #[cfg(unix)]
    // SAFETY: the handler only stores an atomic flag.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        libc::sigemptyset(&mut action.sa_mask);
        action.sa_sigaction = handle_alarm_signal as *const () as usize;
        action.sa_flags = super::interactive::current_sigaction_flags();
        libc::sigaction(libc::SIGALRM, &action, std::ptr::null_mut());
    }
}

/// atimer.c:set_alarm: a SIGALRM when the first atimer is ripe.
fn set_alarm(first: Option<libc::timespec>) {
    let Some(expiration) = first else {
        return;
    };
    #[cfg(target_os = "linux")]
    {
        let timerfd = TIMERFD.load(Relaxed);
        let alarm_timer_ok = ALARM_TIMER_OK.load(Relaxed);
        if timerfd >= 0 || alarm_timer_ok {
            let mut exit = false;
            let ispec = libc::itimerspec {
                it_interval: libc::timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                it_value: expiration,
            };
            // SAFETY: the timer and the descriptor are init_atimer's.
            unsafe {
                if alarm_timer_ok
                    && libc::timer_settime(
                        ALARM_TIMER.load(Relaxed) as libc::timer_t,
                        libc::TIMER_ABSTIME,
                        &ispec,
                        std::ptr::null_mut(),
                    ) == 0
                {
                    exit = true;
                }
                if timerfd >= 0
                    && libc::timerfd_settime(
                        timerfd,
                        libc::TFD_TIMER_ABSTIME,
                        &ispec,
                        std::ptr::null_mut(),
                    ) == 0
                {
                    exit = true;
                }
            }
            if exit {
                return;
            }
        }
    }
    // "Determine interval till the next timer is ripe."
    let now = current_timespec();
    if timespec_cmp(&expiration, &now) != std::cmp::Ordering::Greater {
        // "Timer is (over)due -- just trigger the signal right way."
        // SAFETY: raises a signal whose handler only stores a flag.
        unsafe { libc::raise(libc::SIGALRM) };
    } else {
        let interval = timespec_sub(expiration, now);
        // timespec.h:make_timeval rounds up to microseconds.
        let mut sec = interval.tv_sec;
        let mut usec = (interval.tv_nsec + 999) / 1000;
        if usec >= 1_000_000 {
            sec += 1;
            usec -= 1_000_000;
        }
        let it = libc::itimerval {
            it_interval: libc::timeval {
                tv_sec: 0,
                tv_usec: 0,
            },
            it_value: libc::timeval {
                tv_sec: sec,
                tv_usec: usec as libc::suseconds_t,
            },
        };
        // SAFETY: an owned value.
        unsafe { libc::setitimer(libc::ITIMER_REAL, &it, std::ptr::null_mut()) };
    }
}

/// atimer.c:schedule_atimer: T placed before the first atimer ripe after it.
fn schedule_atimer(list: &mut Vec<Atimer>, timer: Atimer) {
    let at = list
        .iter()
        .position(|other| {
            timespec_cmp(&other.expiration, &timer.expiration) != std::cmp::Ordering::Less
        })
        .unwrap_or(list.len());
    list.insert(at, timer);
}

/// atimer.c:start_atimer: an atimer of KIND for TIMESTAMP that runs
/// CALLBACK, and the SIGALRM armed for the first of them.
pub(crate) fn start_atimer(
    kind: AtimerType,
    timestamp: libc::timespec,
    callback: AtimerCallback,
) -> AtimerId {
    // Emacs's startup runs init_atimer; an interpreter made without that
    // startup (a test's) gets it here, before any alarm is armed.
    ensure_atimer_initialized();
    let (id, first) = ATIMERS.with(|atimers| {
        let mut atimers = atimers.borrow_mut();
        atimers.next_id += 1;
        let id = AtimerId(atimers.next_id);
        let zero = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let (expiration, interval) = match kind {
            AtimerType::Absolute => (timestamp, zero),
            AtimerType::Relative => (timespec_add(current_timespec(), timestamp), zero),
            AtimerType::Continuous => (timespec_add(current_timespec(), timestamp), timestamp),
        };
        schedule_atimer(
            &mut atimers.active,
            Atimer {
                id,
                kind,
                expiration,
                interval,
                callback,
            },
        );
        (id, atimers.active.first().map(|timer| timer.expiration))
    });
    set_alarm(first);
    id
}

/// atimer.c:cancel_atimer: TIMER off the list, if it is still there.  "We
/// don't bother to arrange for setting a different alarm time, since a
/// too early one doesn't hurt."
pub(crate) fn cancel_atimer(timer: AtimerId) {
    ATIMERS.with(|atimers| {
        atimers
            .borrow_mut()
            .active
            .retain(|other| other.id != timer);
    });
}

/// The atimers' Lisp objects, roots of a collection.
pub(crate) fn mark_atimers(mark: &mut dyn FnMut(&Value)) {
    ATIMERS.with(|atimers| {
        if let Ok(atimers) = atimers.try_borrow() {
            for timer in &atimers.active {
                match &timer.callback {
                    AtimerCallback::DelayedMessage(message) => mark(message),
                }
            }
        }
    });
}

/// atimer.c:do_pending_atimers' run_timers: each atimer ripe now run, in
/// order (a continuous one put back for its next time), then the alarm
/// set for the first left.
pub(crate) fn do_pending_atimers(interp: &mut Interpreter, env: &mut Env) {
    let now = current_timespec();
    loop {
        let ripe = ATIMERS.with(|atimers| {
            let mut atimers = atimers.borrow_mut();
            let first = atimers.active.first()?;
            if timespec_cmp(&first.expiration, &now) == std::cmp::Ordering::Greater {
                return None;
            }
            let mut timer = atimers.active.remove(0);
            let callback = timer.callback.clone();
            if timer.kind == AtimerType::Continuous {
                timer.expiration = timespec_add(now, timer.interval);
                schedule_atimer(&mut atimers.active, timer);
            }
            Some(callback)
        });
        let Some(callback) = ripe else {
            break;
        };
        match callback {
            AtimerCallback::DelayedMessage(message) => {
                // with_delayed_message_display's message3 (build_string
                // of the copy taken at the start).
                if let Ok(log) = string_text(&message) {
                    let _ = dispatch::display::message3(interp, message, &log, env);
                }
            }
        }
    }
    let first = ATIMERS.with(|atimers| atimers.borrow().active.first().map(|t| t.expiration));
    set_alarm(first);
}

/// keyboard.c:process_pending_signals: the atimers' turn (handle_async_input
/// has nothing to read in Emaxx, whose input is not signal-driven).
pub(crate) fn process_pending_signals(interp: &mut Interpreter, env: &mut Env) {
    let has_atimers = ATIMERS.with(|atimers| !atimers.borrow().active.is_empty());
    if has_atimers {
        do_pending_atimers(interp, env);
    }
}

/// wait_reading_process_output's pending-signal processing at the top of
/// each pass: a fatal signal's terminate_due_to_signal, then
/// process_pending_signals (the quit flag is the wait's own business).
pub(crate) fn process_pending_signals_in_wait(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    let pending = take_pending_signals();
    if fatal_signal_pending() {
        run_pending_fatal_signal(interp, env)?;
    }
    if pending {
        interp.inhibit_garbage_collection();
        process_pending_signals(interp, env);
        interp.allow_garbage_collection();
    }
    Ok(())
}

/// eval.c:Ffuncall_with_delayed_message: FUNCTION called with an atimer
/// that shows MESSAGE once TIMEOUT seconds have passed, cancelled however
/// the call ends.
pub(crate) fn funcall_with_delayed_message(
    interp: &mut Interpreter,
    timeout: Value,
    message: Value,
    function: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    // CHECK_NUMBER (timeout); CHECK_STRING (message); XFLOATINT (timeout).
    let seconds = extract_float(interp, &timeout)?;
    // xstrdup (SSDATA (message)) and with_delayed_message_display's
    // build_string: the bytes as they are now, up to the first NUL, in a
    // string of their own, multibyte as make_string decides.
    let crate::lisp::types::Kind::StringObject(state) = message.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), message));
    };
    let mut bytes = state.borrow().bytes().to_vec();
    if let Some(nul) = bytes.iter().position(|&byte| byte == 0) {
        bytes.truncate(nul);
    }
    let copy =
        super::strings::make_string(bytes).map_err(|error| LispError::Signal(error.to_string()))?;
    let timer = start_atimer(
        AtimerType::Relative,
        dtotimespec(seconds),
        AtimerCallback::DelayedMessage(copy),
    );
    // CALLN (Ffuncall, function), and the unwind's cancel_atimer however
    // it ends.
    let result = resolve_callable(interp, &function, env)
        .and_then(|callable| interp.call_function_value(callable, None, &[], env));
    cancel_atimer(timer);
    result
}

#[cfg(test)]
mod tests {
    use super::dtotimespec;

    fn parts(sec: f64) -> (libc::time_t, libc::c_long) {
        let spec = dtotimespec(sec);
        (spec.tv_sec, spec.tv_nsec)
    }

    #[test]
    fn dtotimespec_is_gnulibs() {
        // Seconds truncated toward zero, nanoseconds rounded up and the
        // pair normalised; time_t's extremes for what lies outside it.
        assert_eq!(parts(0.0), (0, 0));
        assert_eq!(parts(0.05), (0, 50_000_000));
        assert_eq!(parts(1.0000000001), (1, 1));
        assert_eq!(parts(-1.5), (-2, 500_000_000));
        assert_eq!(parts(-0.25), (-1, 750_000_000));
        assert_eq!(parts(f64::NAN), (libc::time_t::MIN, 0));
        assert_eq!(parts(f64::NEG_INFINITY), (libc::time_t::MIN, 0));
        assert_eq!(parts(f64::INFINITY), (libc::time_t::MAX, 999_999_999));
        assert_eq!(parts(1e30), (libc::time_t::MAX, 999_999_999));
        assert_eq!(parts(-1e30), (libc::time_t::MIN, 0));
    }
}
