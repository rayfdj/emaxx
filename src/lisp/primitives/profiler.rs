//! profiler.c: the sampling CPU profiler and the memory profiler.
//!
//! GNU's SIGPROF handler and its allocation probes record the current
//! backtrace on the spot.  Here the handler and the probes, which may run
//! in the middle of anything (a handler cannot borrow the interpreter), only
//! count: the CPU ticks the timer delivered, the bytes allocated.  The
//! interpreter records what they counted at its next backtrace push or pop,
//! before the frame changes, so a sample is filed under the backtrace that
//! was current when it was taken, as GNU's are.  The logs themselves are
//! profiler.c's log_t: a fixed table of SIZE call stacks of DEPTH
//! functions, the lower half by count evicted when it fills.  They are
//! the interpreter thread's, as profiler.c's are the process's: the
//! thread whose CPU clock the timer reads and whose collections mark them.

use super::*;
use crate::lisp::types::Kind;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering::Relaxed};

/// alloc.c's QAutomatic_GC, the function of the frame garbage_collect
/// records.
pub(crate) const AUTOMATIC_GC: &str = "Automatic GC";

/// lisp.h's MOST_POSITIVE_FIXNUM.
const MOST_POSITIVE_FIXNUM: i64 = (1_i64 << 61) - 1;

/// Something waits to be recorded: CPU ticks or allocated bytes.
static SAMPLES_PENDING: AtomicBool = AtomicBool::new(false);
/// The ticks SIGPROF counted since the last record (each signal one, and
/// one more for each timer overrun).
static CPU_TICKS: AtomicI64 = AtomicI64::new(0);
/// profiler_memory_running, as the allocation probe reads it.
static MEMORY_RUNNING: AtomicBool = AtomicBool::new(false);
/// The bytes the probe counted since the last record.
static MEMORY_BYTES: AtomicI64 = AtomicI64::new(0);

/// Whether a sample waits for `InterpreterState::record_profiler_samples'.
#[inline(always)]
pub(crate) fn samples_pending() -> bool {
    SAMPLES_PENDING.load(Relaxed)
}

/// alloc.c's MALLOC_PROBE: an allocation of SIZE bytes, counted while the
/// memory profiler runs.  Called by the global allocator, so it allocates
/// nothing and takes no lock.
#[inline(always)]
pub(crate) fn malloc_probe(size: usize) {
    if MEMORY_RUNNING.load(Relaxed) {
        malloc_probe_running(size);
    }
}

#[cold]
#[inline(never)]
fn malloc_probe_running(size: usize) {
    // profiler.c:malloc_probe's min (size, MOST_POSITIVE_FIXNUM).
    let size =
        i64::try_from(size).map_or(MOST_POSITIVE_FIXNUM, |size| size.min(MOST_POSITIVE_FIXNUM));
    MEMORY_BYTES.fetch_add(size, Relaxed);
    SAMPLES_PENDING.store(true, Relaxed);
}

/// profiler.c:saturated_add.
fn saturated_add(a: i64, b: i64) -> i64 {
    a.saturating_add(b).min(MOST_POSITIVE_FIXNUM)
}

/// fns.c:sxhash_combine.
fn sxhash_combine(x: u64, y: u64) -> u64 {
    (x << 4).wrapping_add(x >> 60).wrapping_add(y)
}

/// A closure's CLOSURE_CODE, which profiler.c compares and hashes in
/// place of the closure.
fn closure_code(function: Value) -> Option<Value> {
    match function.kind() {
        Kind::Closure(closure) => Some(closure.body()),
        _ => None,
    }
}

/// profiler.c:Ffunction_equal's test.
pub(crate) fn function_equal(f1: Value, f2: Value) -> bool {
    f1.eq_value(f2)
        || matches!((closure_code(f1), closure_code(f2)), (Some(c1), Some(c2)) if c1.eq_value(c2))
}

/// profiler.c's log_t: a fully associative cache of SIZE call stacks of
/// DEPTH functions each, mapped to counts (0 marks an unused entry), and
/// struct profiler_log's GC and discarded counts.
pub(crate) struct ProfilerLog {
    size: usize,
    depth: usize,
    /// The working trace, DEPTH functions.
    trace: Vec<Value>,
    /// index_size heads of the hash buckets, -1 for none.
    index: Vec<i32>,
    /// SIZE links: the next entry in a bucket, or on the free list.
    next: Vec<i32>,
    hash: Vec<u64>,
    keys: Vec<Value>,
    counts: Vec<i64>,
    next_free: i32,
    /// Samples taken during GC.
    gc_count: i64,
    /// Samples evicted during table overflow.
    discarded: i64,
}

impl ProfilerLog {
    /// profiler.c:make_log, None when the tables cannot be allocated.
    fn new(size: usize, depth: usize) -> Option<Self> {
        // "The index size is arbitrary but for there to be any point it
        // should be bigger than SIZE."
        let index_size = size.checked_mul(2)?.checked_add(1)?;
        fn filled<T: Copy>(len: usize, value: T) -> Option<Vec<T>> {
            let mut vec = Vec::new();
            vec.try_reserve_exact(len).ok()?;
            vec.resize(len, value);
            Some(vec)
        }
        let mut next = filled(size, -1_i32)?;
        for (i, link) in next.iter_mut().enumerate().take(size.saturating_sub(1)) {
            *link = i32::try_from(i + 1).ok()?;
        }
        Some(Self {
            size,
            depth,
            trace: filled(depth, Value::Nil)?,
            index: filled(index_size, -1)?,
            next,
            hash: filled(size, 0)?,
            keys: filled(size.checked_mul(depth)?, Value::Nil)?,
            counts: filled(size, 0)?,
            // An empty table has no free entry (GNU's make_log writes
            // next[-1] for SIZE 0, and its next sample past the arrays).
            next_free: if size == 0 { -1 } else { 0 },
            gc_count: 0,
            discarded: 0,
        })
    }

    fn key(&self, idx: usize) -> &[Value] {
        &self.keys[idx * self.depth..(idx + 1) * self.depth]
    }

    fn hash_index(&self, hash: u64) -> usize {
        (hash % self.index.len() as u64) as usize
    }

    /// profiler.c:remove_log_entry.
    fn remove_entry(&mut self, idx: usize) {
        let hidx = self.hash_index(self.hash[idx]);
        let target = idx as i32;
        if self.index[hidx] == target {
            self.index[hidx] = self.next[idx];
        } else {
            let mut p = self.index[hidx] as usize;
            while self.next[p] != target {
                p = self.next[p] as usize;
            }
            self.next[p] = self.next[idx];
        }
        self.counts[idx] = 0;
        self.next[idx] = self.next_free;
        self.next_free = target;
    }

    /// profiler.c:approximate_median over the counts of the entries START
    /// to START + SIZE.
    fn approximate_median(&self, start: usize, size: usize) -> i64 {
        if size < 2 {
            return self.counts[start];
        }
        if size < 3 {
            // "Not an actual median, but better for our application than
            // choosing either of the two numbers."
            return (self.counts[start] + self.counts[start + 1]) / 2;
        }
        let newsize = size / 3;
        let start2 = start + newsize;
        let i1 = self.approximate_median(start, newsize);
        let i2 = self.approximate_median(start2, newsize);
        let i3 = self.approximate_median(start2 + newsize, size - 2 * newsize);
        if i1 < i2 {
            if i2 < i3 {
                i2
            } else if i1 < i3 {
                i3
            } else {
                i1
            }
        } else if i1 < i3 {
            i1
        } else if i2 < i3 {
            i3
        } else {
            i2
        }
    }

    /// profiler.c:evict_lower_half: every entry whose count is at most the
    /// approximate median goes, its count to the discarded samples.
    fn evict_lower_half(&mut self) {
        let median = self.approximate_median(0, self.size);
        for i in 0..self.size {
            let count = self.counts[i];
            if count <= median {
                self.discarded = saturated_add(self.discarded, count);
                self.remove_entry(i);
            }
        }
    }

    /// profiler.c:trace_hash of the working trace.
    fn trace_hash(&self) -> u64 {
        self.trace.iter().fold(0, |hash, &function| {
            let word = closure_code(function).unwrap_or(function);
            sxhash_combine(hash, word.word() as u64)
        })
    }

    /// profiler.c:record_backtrace for the trace already in `trace', with
    /// weight COUNT.
    fn record_trace(&mut self, count: i64) {
        if self.size == 0 {
            self.discarded = saturated_add(self.discarded, count);
            return;
        }
        let hash = self.trace_hash();
        let hidx = self.hash_index(hash);
        let mut idx = self.index[hidx];
        while idx >= 0 {
            let entry = idx as usize;
            if self.hash[entry] == hash
                && self
                    .trace
                    .iter()
                    .zip(self.key(entry))
                    .all(|(&f1, &f2)| function_equal(f1, f2))
            {
                // Found existing entry.
                self.counts[entry] = saturated_add(self.counts[entry], count);
                return;
            }
            idx = self.next[entry];
        }
        // Add new entry.
        if self.next_free < 0 {
            self.evict_lower_half();
        }
        let entry = self.next_free as usize;
        self.next_free = self.next[entry];
        self.next[entry] = self.index[hidx];
        self.index[hidx] = entry as i32;
        self.hash[entry] = hash;
        let depth = self.depth;
        self.keys[entry * depth..(entry + 1) * depth].copy_from_slice(&self.trace);
        self.counts[entry] = count;
    }

    /// profiler.c:add_sample: a sample whose backtrace's top is `Automatic
    /// GC' counts toward the GC; any other is recorded under its trace,
    /// which FILL writes (eval.c:get_backtrace).
    fn add_sample(&mut self, count: i64, top_is_gc: bool, fill: impl FnOnce(&mut [Value])) {
        if top_is_gc {
            self.gc_count = saturated_add(self.gc_count, count);
        } else {
            fill(&mut self.trace);
            self.record_trace(count);
        }
    }

    /// profiler.c:mark_log: the keys of the entries in use.
    fn mark(&self, mark: &mut dyn FnMut(&Value)) {
        for (i, &count) in self.counts.iter().enumerate() {
            if count > 0 {
                for value in self.key(i) {
                    mark(value);
                }
            }
        }
    }
}

/// profiler.c's enum profiler_cpu_running: NOT_RUNNING,
/// TIMER_SETTIME_RUNNING and SETITIMER_RUNNING.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CpuRunning {
    Not,
    #[cfg(target_os = "linux")]
    TimerSettime,
    Setitimer,
}

/// profiler.c's state: the two logs, the CPU profiler's status and
/// interval, profiler_memory_running.
struct Profiler {
    cpu: Option<ProfilerLog>,
    memory: Option<ProfilerLog>,
    cpu_running: CpuRunning,
    cpu_interval: i64,
    memory_running: bool,
}

thread_local! {
    static PROFILER: std::cell::RefCell<Profiler> = const {
        std::cell::RefCell::new(Profiler {
            cpu: None,
            memory: None,
            cpu_running: CpuRunning::Not,
            cpu_interval: 0,
            memory_running: false,
        })
    };
}

fn with_profiler<R>(body: impl FnOnce(&mut Profiler) -> R) -> R {
    PROFILER.with(|profiler| body(&mut profiler.borrow_mut()))
}

/// File the pending samples: CPU ticks into the CPU log, allocated bytes
/// into the memory log, under the backtrace FILL writes (TOP_IS_GC when
/// its innermost frame is `Automatic GC').  Counts that arrive with no log
/// to take them go nowhere.
pub(crate) fn record_pending_samples(top_is_gc: bool, fill: impl Fn(&mut [Value])) {
    SAMPLES_PENDING.store(false, Relaxed);
    let ticks = CPU_TICKS.swap(0, Relaxed);
    let bytes = MEMORY_BYTES.swap(0, Relaxed);
    PROFILER.with(|profiler| {
        // A sample due while a log is being exported waits for none.
        let Ok(mut profiler) = profiler.try_borrow_mut() else {
            return;
        };
        if ticks > 0
            && let Some(log) = profiler.cpu.as_mut()
        {
            log.add_sample(ticks.min(MOST_POSITIVE_FIXNUM), top_is_gc, &fill);
        }
        if bytes > 0
            && let Some(log) = profiler.memory.as_mut()
        {
            log.add_sample(bytes.min(MOST_POSITIVE_FIXNUM), top_is_gc, &fill);
        }
    });
}

/// alloc.c's mark_profiler.
pub(crate) fn mark_profiler(mark: &mut dyn FnMut(&Value)) {
    PROFILER.with(|profiler| {
        if let Ok(profiler) = profiler.try_borrow() {
            for log in [&profiler.cpu, &profiler.memory].into_iter().flatten() {
                log.mark(mark);
            }
        }
    });
}

/// alloc.c's gcstat totals as total_bytes_of_live_objects reads them: the
/// live bytes the last collection found.
static LIVE_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// A collection's live total, for the next one's tot_before.
pub(crate) fn note_live_bytes(bytes: usize) {
    LIVE_BYTES.store(bytes, Relaxed);
}

/// garbage_collect's tot_before: the live bytes as of the last collection,
/// taken while the memory profiler runs.
pub(crate) fn collection_tot_before() -> Option<usize> {
    MEMORY_RUNNING
        .load(Relaxed)
        .then(|| LIVE_BYTES.load(Relaxed))
}

/// garbage_collect's report of the bytes a collection freed, from the live
/// totals before and after it.
pub(crate) fn probe_collection(tot_before: Option<usize>, tot_after: usize) {
    if let Some(tot_before) = tot_before
        && tot_after < tot_before
    {
        // alloc.c calls malloc_probe itself, without MALLOC_PROBE's test.
        malloc_probe_running(tot_before - tot_after);
    }
}

/// profiler.c:make_profiler_log: the sizes `profiler-log-size' and
/// `profiler-max-stack-depth' give now.
fn make_profiler_log(interp: &mut Interpreter, env: &Env) -> Result<ProfilerLog, LispError> {
    let read = |interp: &Interpreter, name: &str| {
        interp
            .symbol_value_cell(name)
            .ok()
            .and_then(|value| value.integer_value())
            .unwrap_or(0)
    };
    let size = read(interp, "profiler-log-size").clamp(0, i64::from(i32::MAX)) as usize;
    let depth = read(interp, "profiler-max-stack-depth").clamp(0, i64::from(i32::MAX)) as usize;
    ProfilerLog::new(size, depth).ok_or_else(|| {
        let bytes = size
            .saturating_mul(depth.saturating_add(4))
            .saturating_mul(8);
        super::strings::memory_full(interp, env, bytes)
    })
}

/// profiler.c:export_log: the log as an `equal' hash table of each call
/// stack's vector to its count, then (Automatic GC nil) and (Discarded
/// Samples nil) if they counted; the log is gone afterward.
fn export_log(
    interp: &mut Interpreter,
    log: ProfilerLog,
    env: &mut Env,
) -> Result<Value, LispError> {
    let table = call(
        interp,
        "make-hash-table",
        &[Value::symbol(":test"), Value::symbol("equal")],
        env,
    )?;
    let Kind::HashTable(handle) = table.kind() else {
        unreachable!("make-hash-table makes a hash table")
    };
    for i in 0..log.size {
        // export_log reads each count into an int.
        let count = log.counts[i] as i32;
        if count > 0 {
            let key = Value::vector(log.key(i).iter().copied());
            hash_table_put(interp, handle, key, Value::Integer(i64::from(count)), env)?;
        }
    }
    if log.gc_count != 0 {
        let key = Value::vector([Value::symbol(AUTOMATIC_GC), Value::Nil]);
        hash_table_put(interp, handle, key, Value::Integer(log.gc_count), env)?;
    }
    if log.discarded != 0 {
        let key = Value::vector([Value::symbol("Discarded Samples"), Value::Nil]);
        hash_table_put(interp, handle, key, Value::Integer(log.discarded), env)?;
    }
    Ok(table)
}

#[cfg(unix)]
mod cpu_timer {
    use super::*;

    /// The POSIX timer and whether it was made (profiler_timer and
    /// profiler_timer_ok).
    #[cfg(target_os = "linux")]
    static PROFILER_TIMER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    #[cfg(target_os = "linux")]
    static PROFILER_TIMER_OK: AtomicBool = AtomicBool::new(false);

    /// This thread's errno.
    fn errno_location() -> *mut libc::c_int {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        // SAFETY: returns this thread's errno slot.
        unsafe {
            libc::__errno_location()
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        // SAFETY: returns this thread's errno slot.
        unsafe {
            libc::__error()
        }
    }

    /// profiler.c:handle_profiler_signal: one tick, and one more for each
    /// expiry the timer could not deliver.
    extern "C" fn handle_profiler_signal(_signal: libc::c_int) {
        // SAFETY: errno is this thread's; timer_getoverrun is
        // async-signal-safe.
        unsafe {
            let errno = *errno_location();
            #[allow(unused_mut)]
            let mut count: i64 = 1;
            #[cfg(target_os = "linux")]
            if PROFILER_TIMER_OK.load(Relaxed) {
                let timer = PROFILER_TIMER.load(Relaxed) as libc::timer_t;
                let overruns = libc::timer_getoverrun(timer);
                if overruns > 0 {
                    count += i64::from(overruns);
                }
            }
            CPU_TICKS.fetch_add(count, Relaxed);
            SAMPLES_PENDING.store(true, Relaxed);
            *errno_location() = errno;
        }
    }

    /// profiler.c:setup_cpu_timer: SIGPROF's handler, then a timer of
    /// INTERVAL nanoseconds on the first clock that makes one, else
    /// ITIMER_PROF.  None for an interval that is not a positive fixnum.
    pub(super) fn setup(interval: Value) -> Option<CpuRunning> {
        let interval = interval
            .integer_value()
            .filter(|interval| (1..=MOST_POSITIVE_FIXNUM).contains(interval))?;
        const BILLION: i64 = 1_000_000_000;
        let spec = libc::timespec {
            tv_sec: (interval / BILLION) as libc::time_t,
            tv_nsec: (interval % BILLION) as libc::c_long,
        };
        // SAFETY: the handler only touches atomics and errno.
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            libc::sigemptyset(&mut action.sa_mask);
            action.sa_sigaction = handle_profiler_signal as *const () as usize;
            action.sa_flags = super::super::interactive::current_sigaction_flags();
            libc::sigaction(libc::SIGPROF, &action, std::ptr::null_mut());
        }
        #[cfg(target_os = "linux")]
        {
            if !PROFILER_TIMER_OK.load(Relaxed) {
                // System clocks to try, in decreasing order of desirability.
                let clocks = [
                    libc::CLOCK_THREAD_CPUTIME_ID,
                    libc::CLOCK_PROCESS_CPUTIME_ID,
                    libc::CLOCK_MONOTONIC,
                    libc::CLOCK_REALTIME,
                ];
                // SAFETY: SIGEV and TIMER are owned; timer_create writes
                // only TIMER.
                unsafe {
                    let mut sigev: libc::sigevent = std::mem::zeroed();
                    sigev.sigev_value.sival_ptr = PROFILER_TIMER.as_ptr().cast();
                    sigev.sigev_signo = libc::SIGPROF;
                    sigev.sigev_notify = libc::SIGEV_SIGNAL;
                    for clock in clocks {
                        let mut timer: libc::timer_t = std::ptr::null_mut();
                        if libc::timer_create(clock, &mut sigev, &mut timer) == 0 {
                            PROFILER_TIMER.store(timer as usize, Relaxed);
                            PROFILER_TIMER_OK.store(true, Relaxed);
                            break;
                        }
                    }
                }
            }
            if PROFILER_TIMER_OK.load(Relaxed) {
                let ispec = libc::itimerspec {
                    it_interval: spec,
                    it_value: spec,
                };
                let timer = PROFILER_TIMER.load(Relaxed) as libc::timer_t;
                // SAFETY: the timer timer_create made.
                if unsafe { libc::timer_settime(timer, 0, &ispec, std::ptr::null_mut()) } == 0 {
                    return Some(CpuRunning::TimerSettime);
                }
            }
        }
        // timespec.h:make_timeval rounds the nanoseconds up to microseconds.
        let mut sec = spec.tv_sec;
        let mut usec = (spec.tv_nsec + 999) / 1000;
        if usec >= 1_000_000 {
            sec += 1;
            usec -= 1_000_000;
        }
        let tv = libc::timeval {
            tv_sec: sec,
            tv_usec: usec as libc::suseconds_t,
        };
        let timer = libc::itimerval {
            it_interval: tv,
            it_value: tv,
        };
        // SAFETY: an owned value.
        if unsafe { libc::setitimer(libc::ITIMER_PROF, &timer, std::ptr::null_mut()) } == 0 {
            return Some(CpuRunning::Setitimer);
        }
        Some(CpuRunning::Not)
    }

    /// profiler.c:Fprofiler_cpu_stop's disarming, and SIGPROF ignored.
    pub(super) fn stop(running: CpuRunning) {
        // SAFETY: zeroed timer values disarm; the timer is timer_create's.
        unsafe {
            match running {
                CpuRunning::Not => {}
                #[cfg(target_os = "linux")]
                CpuRunning::TimerSettime => {
                    let disable: libc::itimerspec = std::mem::zeroed();
                    let timer = PROFILER_TIMER.load(Relaxed) as libc::timer_t;
                    libc::timer_settime(timer, 0, &disable, std::ptr::null_mut());
                }
                CpuRunning::Setitimer => {
                    let disable: libc::itimerval = std::mem::zeroed();
                    libc::setitimer(libc::ITIMER_PROF, &disable, std::ptr::null_mut());
                }
            }
            libc::signal(libc::SIGPROF, libc::SIG_IGN);
        }
    }
}

#[cfg(not(unix))]
mod cpu_timer {
    use super::*;

    pub(super) fn setup(interval: Value) -> Option<CpuRunning> {
        interval
            .integer_value()
            .filter(|interval| (1..=MOST_POSITIVE_FIXNUM).contains(interval))?;
        Some(CpuRunning::Not)
    }

    pub(super) fn stop(_running: CpuRunning) {}
}

/// profiler.c:Fprofiler_cpu_start.
pub(crate) fn profiler_cpu_start(
    interp: &mut Interpreter,
    sampling_interval: Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if with_profiler(|profiler| profiler.cpu_running) != CpuRunning::Not {
        return Err(values::doprnt_error(
            interp,
            "CPU profiler is already running",
            &[],
        ));
    }
    if with_profiler(|profiler| profiler.cpu.is_none()) {
        let log = make_profiler_log(interp, env)?;
        with_profiler(|profiler| profiler.cpu = Some(log));
    }
    match cpu_timer::setup(sampling_interval) {
        None => {
            with_profiler(|profiler| profiler.cpu_running = CpuRunning::Not);
            Err(values::doprnt_error(
                interp,
                "Invalid sampling interval",
                &[],
            ))
        }
        Some(status) => {
            with_profiler(|profiler| {
                profiler.cpu_interval = sampling_interval.integer_value().unwrap_or(0);
                profiler.cpu_running = status;
            });
            if status == CpuRunning::Not {
                return Err(values::doprnt_error(
                    interp,
                    "Unable to start profiler timer",
                    &[],
                ));
            }
            Ok(Value::T)
        }
    }
}

/// profiler.c:Fprofiler_cpu_stop.
pub(crate) fn profiler_cpu_stop() -> Value {
    let running = with_profiler(|profiler| profiler.cpu_running);
    if running == CpuRunning::Not {
        return Value::Nil;
    }
    cpu_timer::stop(running);
    with_profiler(|profiler| profiler.cpu_running = CpuRunning::Not);
    Value::T
}

/// profiler.c:Fprofiler_cpu_running_p.
pub(crate) fn profiler_cpu_running_p() -> Value {
    if with_profiler(|profiler| profiler.cpu_running) == CpuRunning::Not {
        Value::Nil
    } else {
        Value::T
    }
}

/// profiler.c:Fprofiler_cpu_log: the profiler stopped, the log exported
/// and the profiler restarted with a fresh one.  With no log (GNU reads
/// through a null one) the answer is nil.
pub(crate) fn profiler_cpu_log(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let prof_cpu = with_profiler(|profiler| profiler.cpu_running) != CpuRunning::Not;
    if prof_cpu {
        profiler_cpu_stop();
    }
    let ret = match with_profiler(|profiler| profiler.cpu.take()) {
        Some(log) => export_log(interp, log, env)?,
        None => Value::Nil,
    };
    if prof_cpu {
        let interval = Value::Integer(with_profiler(|profiler| profiler.cpu_interval));
        profiler_cpu_start(interp, interval, env)?;
    }
    Ok(ret)
}

/// profiler.c:Fprofiler_memory_start.
pub(crate) fn profiler_memory_start(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    if with_profiler(|profiler| profiler.memory_running) {
        return Err(values::doprnt_error(
            interp,
            "Memory profiler is already running",
            &[],
        ));
    }
    if with_profiler(|profiler| profiler.memory.is_none()) {
        let log = make_profiler_log(interp, env)?;
        with_profiler(|profiler| profiler.memory = Some(log));
    }
    with_profiler(|profiler| profiler.memory_running = true);
    MEMORY_RUNNING.store(true, Relaxed);
    Ok(Value::T)
}

/// profiler.c:Fprofiler_memory_stop.
pub(crate) fn profiler_memory_stop() -> Value {
    if !with_profiler(|profiler| profiler.memory_running) {
        return Value::Nil;
    }
    with_profiler(|profiler| profiler.memory_running = false);
    MEMORY_RUNNING.store(false, Relaxed);
    Value::T
}

/// profiler.c:Fprofiler_memory_running_p.
pub(crate) fn profiler_memory_running_p() -> Value {
    if with_profiler(|profiler| profiler.memory_running) {
        Value::T
    } else {
        Value::Nil
    }
}

/// profiler.c:Fprofiler_memory_log, as `profiler_cpu_log'.
pub(crate) fn profiler_memory_log(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<Value, LispError> {
    let prof_mem = with_profiler(|profiler| profiler.memory_running);
    if prof_mem {
        profiler_memory_stop();
    }
    let ret = match with_profiler(|profiler| profiler.memory.take()) {
        Some(log) => export_log(interp, log, env)?,
        None => Value::Nil,
    };
    if prof_mem {
        profiler_memory_start(interp, env)?;
    }
    Ok(ret)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trace_of(log: &mut ProfilerLog, names: &[i64]) {
        for (slot, name) in log.trace.iter_mut().zip(names) {
            *slot = Value::Integer(*name);
        }
    }

    #[test]
    fn a_full_log_evicts_its_lower_half() {
        let mut log = ProfilerLog::new(4, 1).expect("log");
        for (name, count) in [(1, 5), (2, 1), (3, 7), (4, 2)] {
            trace_of(&mut log, &[name]);
            log.record_trace(count);
        }
        assert_eq!(log.next_free, -1);
        // The median of 5, 1, 7, 2 by approximate_median: size 4 splits
        // into 1, 1 and 2 entries: 5, 1 and (7 + 2) / 2 = 4, whose median
        // is 4.  The counts 1 and 2 go.
        trace_of(&mut log, &[9]);
        log.record_trace(3);
        assert_eq!(log.discarded, 3);
        let live: Vec<_> = (0..4)
            .filter(|&i| log.counts[i] > 0)
            .map(|i| (log.key(i)[0].integer_value(), log.counts[i]))
            .collect();
        assert_eq!(live.len(), 3);
        assert!(live.contains(&(Some(1), 5)));
        assert!(live.contains(&(Some(3), 7)));
        assert!(live.contains(&(Some(9), 3)));
        // A repeated trace adds to its entry.
        trace_of(&mut log, &[3]);
        log.record_trace(2);
        assert!((0..4).any(|i| log.counts[i] == 9));
    }

    #[test]
    fn an_empty_log_discards_every_sample() {
        let mut log = ProfilerLog::new(0, 2).expect("log");
        log.record_trace(4);
        assert_eq!(log.discarded, 4);
    }
}
