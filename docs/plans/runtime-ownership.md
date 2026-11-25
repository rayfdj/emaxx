# Runtime ownership and thread access (A2.5, A4.7, C5.1, C5.3, D43): a staged plan

Written on 24 November 2026 before round 388, from a reading of the tree at
round 380. Paths beginning `/home/user/` and `/tmp/` are those of the
container it was written in; line numbers are of that tree. HANDOVER.md
records what each stage did. Stages 0, 1 and 2 are round 388.

Source: `/home/user/emaxx-draft` at `41202f70` (round 380). GNU: the pinned
30.2 mirror in `/home/user/emacs-mirror/emacs/src`. Written 24 November 2026
as a read-only planning task.

**Evidence limits.** Nothing was built or tested. The inventory comes from
reading the source (a script listed every `static` and `thread_local!`; the
classification is by name and type and was spot-checked by hand). Three
small Lisp probes ran on the prebuilt editors (GNU `src/emacs` and Emaxx
`target/prof-r380/release/emaxx`). One disassembly of the r380 binary checked
how an existing thread-local compiles. Every "concrete path" below was traced
in the code. None was run, and the "used to crash" controls in the stages
must be captured on the base commit before they are cited.

## 0. Corrections to the brief's premises

1. **Lisp already runs on the main OS thread (finding 157 / E157).** Batch
   runs on a 128 MB corosensei stack switched in on the calling thread
   (`src/batch.rs:98-111`, `with_batch_stack`; the comment says why: "an
   extra worker and join would introduce a blocking futex that GNU's seccomp
   policy deliberately does not permit"). Interactive `tty::run`
   (`src/tty.rs:542`) runs on the main thread's own stack. Probe
   `(process-attributes (emacs-pid))` → `(state thcount (eq main-thread
   (current-thread)))`: GNU `("R" 1 t)`, Emaxx r380 `("R" 1 t)`. The
   honesty audit row 157 ("OPEN") and the milestone list's E157 (under
   Accepted) give a cause that no longer holds. §1.7 has the details and the
   remaining thread-count differences.
2. **The public boundary already exists.** `src/lisp/runtime.rs` holds the
   GNU-style process lock (`RUNTIME: Mutex<()>`, line 12) and the reentrant
   `with_runtime` (line 37). Every public Lisp entry is wrapped in it, and
   `Value`, `Interpreter` and the heap modules are `pub(crate)`. `Value` is
   `!Send + !Sync` (`types.rs:1816`, `PhantomData<*mut ()>`), and so is every
   heap handle (each wraps a `NonNull`). What remains open: crate-internal
   code (the library tests) reaches the heap without the lock, roots kept in
   thread-locals are not seen by another OS thread's collection, the host
   symbol table is shared without synchronization, and a second lock sits
   under native calls.
3. **The ordinary configuration is 2 test threads, not 1.**
   `.cargo/config.toml:6` sets `RUST_TEST_THREADS = 2`, with a comment that
   "the tests do not exercise shared cross-interpreter concurrency". That
   claim is false (D43; the 21 September audit's finding 5). Only the gate
   scripts and the CI workflows pin 1. `docs/testing.md:74-75` still
   describes two of the gate groups as "2 test threads", but
   `tools/grouped_gate.py:105-106` gives them 1.
4. There is no `static mut`, `LazyLock` or `lazy_static`. There are 10
   `unsafe impl Send/Sync` lines on 8 types (§1.5).

## 1. Inventory

### 1.1 Counts by kind (src/, all targets)

| Kind | Count |
|---|---:|
| `thread_local!` blocks | 69 (declaring 121 statics: 74 `Cell`, 44 `RefCell`, 3 other) |
| process `static` | 193 |
| of which atomics (`Atomic*`, arrays of them) | 104 |
| of which `OnceLock` (45, including `OnceLock<Mutex<_>>` and `OnceLock<RwLock<_>>`) and `Once` (1) | 46 |
| of which `Mutex` | 20 |
| of which built from atomics (`SmallBlocks` ×3, `ImageCells` ×3) | 6 |
| of which `unsafe`-Sync wrappers (`ProcessTable`, `EmptyClosures`, `EmergencyReset`, the `NativeSubr` tables) | 6 |
| of which immutable data, ZSTs, the global allocator | 11 |
| `static mut` / `LazyLock` / `lazy_static` | 0 / 0 / 0 |
| `unsafe impl Send/Sync` | 10 impls on 8 types |
| `extern "C" fn` definitions | 1,388 generated subr shims (x86-64 Linux; 1,378 aarch64 macOS), 156 hand-written helpers in `native_comp/runtime.rs`, 38 module callbacks, about 25 others |
| `extern "C-unwind"` | 0 |

Appendix A lists every static by group with file:line.

### 1.2 The runtime boundary today

- `src/lisp/runtime.rs:12` `RUNTIME: Mutex<()>`, the counterpart of
  thread.c:68 `global_lock`. `:15` `ENTERED` (TLS) marks nesting.
  `with_runtime` (`:37`) takes the lock once per OS thread and registers the
  entry frame as the stack bottom, like emacs.c:1315
  `stack_bottom = &stack_bottom_variable`. A panic that escapes poisons the
  lock.
- Its callers are every public Lisp entry: `batch.rs:71`
  (`run_batch_with_large_stack`), `:80` (`run_batch_process_with_large_stack`),
  `:130` (`run_batch_with_actions`, which `run_batch` `:114` calls), `:444`
  (`startup_image_loads`), `:467` (`version_banner`); `tty.rs:542` (`run`);
  `lisp/mod.rs:1196` (`run_ert_file`); and `anti_cheat.rs:2098`
  (`enforce_all`). Internally there are 15 test uses (`types.rs`,
  `types/ownership_tests.rs`, `eval/roots.rs`, `runtime.rs`).
- The public functions that do not take the lock only touch host state
  under a `Mutex`, an atomic or a `Once`: `install_fatal_signal_handlers`,
  `init_atimer`, `init_process_nofile_limit`, `stderr_write`,
  `write_standard_output`, `close_output_streams`, `set_initial_argv`,
  `set_build_details`, `set_daemon_state`, `tty::set_dev_tty`,
  `executable_fingerprint_hex`. They are sound in Rust's sense.
- `tests/runtime_ownership.rs` already checks that three OS threads making
  twelve public calls serialize. There is a `compile_fail` doctest at
  `lisp/mod.rs:6-10`, but it fails for any reason (`eval` is a private
  module), and no gate stage runs doctests (`tools/grouped_gate.py:808-822`
  runs the lib groups, `--bins` and the integration tests only).

### 1.3 Process statics: owner, reach, and whether two OS threads can reach them at once

"Crate-internal" means safe `pub(crate)` code, which in practice is the
library tests. "Public" means the crate's exported API.

| Group (count) | Owner | Reached through | Two OS threads at once through safe code today? |
|---|---|---|---|
| P0 runtime lock (1) | the process | `with_runtime` | Yes, by design: callers serialize. |
| P1 immutable tables, ZSTs (14) | the binary | subr tables, opcode tables, fingerprints | Yes, harmless: never written. |
| **P2 allocator frontier (21)**: `alloc.rs:357-366` (cons and float free lists and bump regions), `strings.rs:361-374`, `strings/data.rs:47-64` (current string-data blocks), `symbols.rs:817-819`, `vectors.rs:339-341` | "the one Lisp thread that runs" (`alloc.rs:353-356`), which nothing enforces | every allocating constructor (`Value::cons` → `alloc::allocate_cons` `alloc.rs:1711`, about 3,100 runtime call sites and 6,300 in all) | **Yes, crate-internal, and this is the D43 crash.** `lisp::types::tests::deep_cons_chains_allocate_and_release_on_a_small_stack` (`types.rs:4305`) allocates 100,000 conses on a spawned thread with no runtime entry. Under the default 2 test threads it runs next to any other test that allocates. Both threads execute `BUMP_NEXT.load; BUMP_NEXT.store(next + CELL_SIZE)` (`alloc.rs:1716-1720`) or pop the same `FREE_LIST` head, so one cell is handed out twice, and the non-atomic `fill_cons` writes then race. Public: no (every entry holds `RUNTIME`). |
| **P3 host symbols (1)**: `symbols.rs:994` `HOST_SYMBOLS: ProcessTable<Rc<Symbols>>` with `unsafe impl Sync` (`types.rs:355`) | "internal heap operations that do not create an editor" | `with_current` (`symbols.rs:1055`, its arm for "no editor selected"), `SymbolOwner::default` (`:1072`), `host_symbols_with_pos_enabled` (`:1038`), interning and lookup | **Yes, crate-internal.** On a fresh libtest thread, `ACTIVE` is empty, so `Value::symbol("x")` takes `HOST_SYMBOLS.with_borrow(Rc::clone)`. `with_borrow` makes a `&mut` from `&self` (`types.rs:362-366`) and `Rc::clone` bumps a non-atomic count, so two such threads race on initialization and on the count. Public: no. |
| P4 heap registries (17): `BLOCKS` (`alloc.rs:144`, Mutex), `HEAP_ROOTS` (`:735`), `LIVE_STATES` (`:1217`), `FREED_RECORDS` and `ALL_BUFFERS` (`vectors.rs:370,380`), arena `CLASSES` (`arena.rs:31`), heap bounds, `IMAGE_REGION_OPEN`, `ImageCells` ×3, GC epoch and sweep flag (`types.rs:179,190`), `ZERO_VECTOR`, `EMPTY_UNIBYTE/MULTIBYTE` | the heap | collection, `mem_find`, image load, the canonical empty objects | Sound as data structures, but what they name is shared. **`LIVE_STATES` makes every live `InterpreterState` a root that any thread's collection reads** through `registered_gc_view` (`eval.rs:5545-5562`; its SAFETY comment says "parked"). Under parallel tests that is another OS thread's state while it runs: a data race. Public: no. |
| P5 second serializers and guards (5): `NATIVE_EXECUTION_LOCK` and `ACTIVE_NATIVE_HEAP` (`native_comp/runtime.rs:759-760`), `MEMORY_FULL`, `IMAGE_TEMPLATE_ACTIVE` (`eval.rs:6518`), `DUMP_MESSAGES_SUPPRESSED` | native activations; the test template | `with_active_call` (`runtime.rs:838`) on every outermost Rust→native call | The native lock serializes native code between OS threads, a second lock under `RUNTIME` (A2.3). It is taken and released on every outermost native call (`NativeExecutionGuard::enter`, `:779`). |
| P6 signal-shared flags and timers (27) | the signal handlers | handlers, and the Lisp thread's polling | Yes, by design: async-signal-safe atomics (GNU's `volatile sig_atomic_t` globals). |
| P7 counters and serials (46) | the runtime | allocation, collection, `memory-use-counts` | Rust-sound. Under parallel tests, relaxed load/store pairs (for example `LIVE_CONSES`) lose updates: wrong census, not UB. |
| P8 once-initialized host facts (35) | the process | configuration, libraries, regexes | Yes, harmless. |
| P9 test-only (8): `test_support.rs` gates, `TEMPLATE` with `AssertSend<Interpreter>` (`:366-369`) | the test binary | `initialized_upstream_batch_interpreter` | **The template is unsound under parallel use even with its counter.** `ImageTemplateToken` is the first field of `InterpreterState` (`eval.rs:6671`), so its `Drop` brings `IMAGE_TEMPLATE_ACTIVE` to 0 before the clone's Rc graph, which it shares with the template, is dropped. A second thread can then deep-clone, bumping the same non-atomic counts, while the first is still dropping. |
| P10 host I/O state (18): stdout and stderr streams, GnuTLS log, `DEV_TTY`, termcap, process table, daemon | the process | printing, process I/O | Sound (Mutex or atomic). Parallel tests only interleave output and share state. |

### 1.4 Thread-locals

| Group (count) | Examples | Verdict |
|---|---|---|
| T1 per-OS-thread by nature (17) | stack bounds (`continuations.rs:44-52`, `alloc.rs:716,721,2298`), the host allocator's `SMALL_BLOCKS` (`allocator.rs:55`), native lock holding (`native_comp/runtime.rs:766-768`), `ENTERED`, test permits | Correct as TLS. |
| T2 scratch and host-keyed caches (18) | regex `WORK`/`SPARE_STATE`, `TPUTS_OUTPUT`, doc and tz caches | Correct as TLS. |
| T3 heap-identity caches and active-call pointers (17) | bytecode `CACHE`/`VERIFIED`/`CLOSURES` (`bytecode/verify.rs:96-258`, checked against the global `CODE_CHANGES`/`VECTOR_SWEEPS` epochs), `STRING_CHAR_BYTE_CACHE` (`types/string_data.rs:447`, checked against the contents), native `ACTIVE_CALL`/`ACTIVE_GC`, module `ACTIVE_ENVS` | Valid while the runtime stays on the thread. The native and module pointers double as foreign-thread checks: `with_active` aborts on null (`native_comp/runtime.rs:931-936`), and `get_environment` aborts (`modules/mod.rs:371-375`). |
| **T4 editor state holding Lisp objects or heap handles (23)** | `PROFILER` (`profiler.rs:306`; `ProfilerLog.keys: Vec<Value>`), `ATIMERS` (`atimer.rs:74`), `ECHO_AREA_MESSAGE_VALUE` (`dispatch/display.rs:33`), `PREVIOUS_ECHO_AREA_MESSAGE`, `MINIBUFFER_INPUT_METHOD`, `SEMANTIC_CPP_INCLUDE_TAG_CACHE` (`dispatch/misc_keymaps.rs:9`), `PRIMARY_TOP_FRAME` and `CONTROLLING_OUTPUT` (`eval/terminal.rs:1452-1453`), `GL_STATE`, `PRSTACK`/`RDSTACK`, symbols `ACTIVE` | **Unsound across OS threads through the public API.** These roots are marked only from the collecting thread's own TLS (`mark_thread_local_roots`, `dispatch.rs:525-530`; `mark_profiler` and `mark_atimers`, called from `eval.rs:5661-5662`), and nothing clears them when an editor or entry ends. Concrete path: thread A calls `emaxx::batch::run_batch_with_large_stack` with `(profiler-memory-start)` and some allocation, then returns; A's TLS `PROFILER` keeps its log's key vectors. Thread B calls `run_batch_with_large_stack` with `(garbage-collect)`. B marks B's TLS, so A's vectors are swept. Thread A makes a third call with `(profiler-memory-log)`, which reads the freed vectors: use after free. |
| T5 editor or C-global state without Lisp objects (46) | keyboard (`kbd_input.rs:38-89`), tty event state, echo flags, auto-save flags, `RANDOM` | Per-OS-thread copies of GNU's process globals. Correct while one OS thread runs the runtime. When it moves (tests, public calls from several threads) state bleeds or is lost; not UB. |

### 1.5 `unsafe impl Send/Sync`

| Site | Type | Verdict and action |
|---|---|---|
| `types.rs:355` | `ProcessTable<T>: Sync` | Unsound across threads (P3), and `with_borrow` aliases a `&mut` even on one thread if re-entered. **Remove** (Stage 4). |
| `test_support.rs:367` | `AssertSend<T>: Send` | Template. Sound only while uses never overlap. After Stage 2 every use is under the runtime; **keep**, with a SAFETY note pointing there. |
| `bytecode/verify.rs:235` | `EmptyClosures: Sync` | Never written. Sound; keep. |
| `sqlite.rs:57-58` | `SqliteDatabaseState`, `SqliteSetState: Send` | The justification (`SQLITE_OPEN_FULLMUTEX`) is right, but nothing needs `Send` (`InterpreterState` is `!Send`). **Remove**; if the build objects, the error names the bound. |
| `native_comp/abi.rs:76,79` | `NativeSubr: Send + Sync` | Immutable generated tables in statics. Sound; keep. |
| `primitives/processes.rs:401,403` | `ExecImage: Send + Sync` | Required by the `pre_exec` closure; used only in the forked child. Sound; keep. |
| `interactive/kbd_input.rs:196` | `EmergencyReset: Sync` | "Only the main thread writes a slot" is not enforced. **Make it "only the runtime holder"**, with `ensure_held` in the writer (Stage 4). |

(`lib.rs:26` `unsafe impl GlobalAlloc` is not a Send/Sync impl.)

### 1.6 How the tests create interpreters, and what `--test-threads=1` hides

- **Library:** 3,583 `#[test]`s (and 59 in `src/bin`).
  - 781 call `Interpreter::new()` directly, which builds a bare temacs-like
    editor and selects a fresh `SymbolOwner` into TLS `ACTIVE`
    (`eval.rs:7384`). Most do it through helpers such as `eval_str_bare`
    (`eval/tests.rs:132`).
  - The `test_support` fixtures:
    `initialized_gnu_early_lisp_interpreter` (`:200`, 98 sites; loads
    debug-early, byte-run, backquote and subr from `../emacs`), its
    `_with` and window variants, `initialized_upstream_batch_interpreter`
    (`:345`, 314 sites; through `batch::initialize_batch_interpreter`,
    loading the shared fixture image under `EMAXX_FIXTURE_IMAGE_DIR` with
    a `flock`, `batch.rs:1021-1062`), the interactive fixture, and
    `replace_with_gnu_batch_runtime`.
  - 190 sites use `run_with_large_stack` (`test_support.rs:446`, and local
    copies in `batch.rs`, `perf.rs`, `eval/tests.rs` and
    `compat_runtime_tests.rs`). It takes a host permit on the libtest
    thread, then runs the test on a new 128 MB thread.
  - Gates: `HostTestGate` (at most 2 "host-heavy" tests, `:8`), an
    exclusive process-test gate held in TLS until the thread exits
    (`:150-190`), a batch bootstrap mutex (`:142`), and the
    boot-environment `RwLock` (`compat.rs:409-460`).
  - libtest runs every test on a new thread and joins it before reporting.
- **Integration:** 218 tests in 7 binaries. Most spawn the `emaxx` and GNU
  executables, so each has its own heap. `runtime_ownership.rs` and
  `ert_runner.rs` use the public in-process API, which serializes on
  `RUNTIME`.
- **One filter run in the head:** `cargo test --lib lisp::types::` under
  the ordinary configuration (2 threads).
  - Test A, `deep_cons_chains_…`, conses 100,000 cells on a 256 KB thread
    with no entry.
  - Test B, `symbol_ids_resolve_across_serialized_host_threads`
    (`types.rs:4799`), builds an `Interpreter` inside `with_runtime` and runs
    `garbage-collect`. A never takes `RUNTIME`, so B is not excluded.
  - B's collection scans only B's stack (`alloc::mark_all_stacks`,
    `alloc.rs:1312-1314`), so A's chain, rooted on A's stack, is unmarked
    and swept onto `FREE_LIST`. A keeps consing from the same free list and
    bump region.
  - Result: A's `census_live_conses() >= before + 100_000` assertion can
    fail, or A reuses its own swept cells (cycles), or both threads pop the
    same head and one cell is used twice. Larger groups crash in string
    data (HANDOVER's D43 note).
- **What one test thread hides:**
  1. the shared allocator frontier (P2);
  2. collections that sweep objects held only by another OS thread's stack,
     while reading other live states through `LIVE_STATES` (P4);
  3. `HOST_SYMBOLS` aliasing and its non-atomic count (P3);
  4. the image-template counter race (P9);
  5. the native lock's per-heap pointer (`ACTIVE_NATIVE_HEAP`) and
     `dlopen`'s shared relocation words, which belong to one heap
     (`test_support.rs:283-287`);
  6. host-process state: SIGCHLD slots, the alarm timer, stdout and
     stderr, the environment, the current directory. The process-test
     gate and the boot-environment lock already handle these; they are
     host concerns, not runtime ones.

### 1.7 The main-thread question (finding 157)

- **GNU:** emacs.c:1315 records `stack_bottom` in `main`, and emacs.c:2027
  calls `init_threads` (thread.c:1163-1175), which locks `global_lock` on
  the main thread and sets `current_thread = &main_thread.s`. Other Lisp
  threads are pthreads (systhread.c:236 `sys_thread_create`) running
  `run_thread` (thread.c:758), which acquires the lock (`:802`) before any
  Lisp. Process signals are forwarded to the main thread
  (sysdep.c:1739 `deliver_process_signal`, `pthread_kill`).
- **Emaxx now:** no production code spawns an OS thread to run Lisp
  (§0.1). The remaining OS threads:
  1. **libtest and `run_with_large_stack`, in tests only.** Rust's
     evaluator frames are deeper than GNU's C frames, hence 128 MB stacks
     (`.cargo/config.toml:13` sets `RUST_MIN_STACK` for the same reason).
  2. **`process-reaper` helpers** (`primitives/processes.rs:1160`
     `reap_deleted_child`; `eval/threads.rs:406` for `delete-process`).
     GNU reaps these in `handle_child_signal` through `deleted_pid_list`.
     Probe: after `(call-process "sleep" nil 0 nil "2")`, GNU `("R" 1)`,
     Emaxx `("R" 2)`.
  3. **The DNS lookup thread** (`processes.rs:4385`). GNU's `getaddrinfo_a`
     also uses one.
  4. **Lisp threads are not OS threads (finding 84's design).** Probe with
     a live `make-thread` thread: GNU thcount 2, Emaxx 1.
- **Decision:** run Lisp on the main thread, as GNU does. That is already
  true. Make it structural: `main` takes the runtime handle before any
  other work (Stage 7), as `init_threads` does.
  - Correct E157's recorded cause (it is stale).
  - Record the reaper's thread count as a finding made after the scope
    lock. It is not a crash, so by the milestone rules it waits.
  - Leave the Lisp-thread count as finding 84's disclosed design.

### 1.8 Unwinding boundaries a panic or a Lisp exit could cross

- **No `extern "C-unwind"` exists.** On Rust 1.81 and later a panic that
  reaches an `extern "C"` frame aborts the process. That is sound but fatal,
  and the gate profile's overflow checks and debug assertions make it more
  likely. Every callback below is therefore "abort on panic".
- **Entry:** `main.rs:160` `extern "C" fn main` → `linux_startup.rs:51-69`
  `catch_unwind`, then `process::exit`.
- **Native code → Rust:** the generated shims (`generated_native_subrs_*.rs`)
  delegate to `invoke_subr_*` (`native_comp/runtime.rs:5473`), and there are
  156 helpers.
  - A Lisp error inside them is an `Err` until `remember_helper_error`
    (`:5544`) calls `jump_nonlocal` (`:5533`). That `longjmp`s (or
    `emaxx_native_longjmp`, the asm at `:392-404`) out of the shim, through
    native frames, to a gccjit `_setjmp` handler or to the invocation's
    escape record.
  - **Soundness condition:** every Rust frame it leaves must hold nothing
    with a destructor (be a POF, a plain old frame). The fast paths do
    (`invoke_other_subr` `:2742`, `remember_active_helper_error` `:2818`:
    "for a caller that holds none"). That is a convention; nothing checks
    it.
- **Suspended native frames:** `suspension.rs:83-107,271-281`
  `catch_unwind`/`resume_unwind` around an `extern "C"` invoke. A jump must
  land on the stack it starts from; the escape record is per invocation.
  This needs a check (§4).
- **Modules:** 38 `extern "C"` callbacks (`modules/callbacks.rs`).
  `modules/mod.rs:353` wraps the body in `catch_unwind` and turns an error
  into the pending non-local exit (GNU's `module_handle_nonlocal_exit`).
  A foreign OS thread aborts in `get_environment` (`:371`), where GNU checks
  only with `--module-assertions` (emacs-module.c:335).
- **Signal handlers:** `atimer.rs:168`, `profiler.rs:469`,
  `interactive.rs:368` (fatal faults, on the main thread's 64 KB
  `sigaltstack`), the signal-hook closures (`interactive.rs:63,67,223,277`),
  and SIGCHLD (`processes.rs:262` → `eval.rs` `handle_child_signal`, a
  lock-free slot list). They use only atomics and async-signal-safe libc
  and run on whichever thread the kernel picks. There is no forwarding to
  the main thread; the self-pipe and timerfd wake the Lisp thread on Linux.
- **Others:**
  - `vfork_child_main` (`processes.rs:2054`) runs in a vfork child sharing
    the parent's memory and must not allocate or unwind.
  - `qsort` compare (`buffers.rs:145`), GnuTLS (`dispatch/gnutls.rs:124,
    1649,1660,2126`), tputs (`eval/terminal.rs:168`), libattr
    (`system.rs:444`), sound (`sound.rs:490`).
- **Coroutine stacks:** a panic in a Lisp thread's body or in the batch
  stack goes to the resumer (corosensei's unwind support), where it is
  caught: `continuations.rs:298,471` and `batch.rs:108` ("batch runtime
  panicked").
- **Lisp non-local exits inside Rust** are `Result` values, so no unwinding
  is involved. GNU `longjmp`s across C frames (eval.c `unwind_to_catch`).

## 2. Target design

**The idea: one runtime, owned by one OS thread at a time, which is GNU's
`global_lock` expressed in Rust.**

1. **The owner.** The process lock owns what alloc.c's globals are:
   - the allocation frontier: cons, float, string-header and symbol free
     lists and bump regions, the current small-string-data blocks, and the
     vector free lists;
   - the host obarray of code that runs without an editor;
   - the holder's identity (thread id and name, for diagnostics).

   Holding the lock is being `current_thread` (thread.c:118), and
   `runtime::held()` is GNU's `in_current_thread` (thread.c:1155). The
   holder *checks the frontier out* into its own thread-local cells and
   checks it back in on release. `RUNTIME` becomes a `Mutex<RuntimeState>`
   plus a `Condvar`, so a waiter can name the holder; an uncontended
   acquire is one compare-and-swap with no futex, as seccomp needs.
2. **Acquisition.**
   - *Scoped:* `with_runtime` (public entries; nested entries reuse it) and
     the public `emaxx::Runtime` handle (Stage 7). `main` takes the handle
     first, as `init_threads` does.
   - *Implicit, for the thread's life:* the first root construction or cold
     allocation path on a thread that does not hold the runtime acquires it
     until the thread exits. This mirrors `run_thread` acquiring
     `global_lock` before its first Lisp. libtest gives every test a new
     thread and joins it before the next, so every runtime-touching library
     test serializes at the heap, with no edits to the 3,583 tests.
   - Production never reaches the implicit path: every entry is scoped. If
     a helper thread ever did, it would wait, with a diagnostic, rather
     than corrupt the heap.
3. **What becomes unreachable, and how.**
   1. *Allocation:* a thread that does not hold the runtime finds empty
      thread-local frontier cells, so its fast path always falls to the
      cold path. The cold path (new block, large vector or string, the
      collector's entry, image load) calls `runtime::ensure_held()` before
      it reads any shared state, then retries the fast path with the
      checked-out frontier.
   2. *Symbols:* `HOST_SYMBOLS` moves into the runtime state. It is
      installed in a TLS slot while held, so a non-holder's `with_current`
      finds none and takes `ensure_held` first. `ProcessTable` and its
      `unsafe impl Sync` disappear.
   3. *Editors:* `Interpreter::construct`, `for_image`, `Clone` and
      `deep_clone_image` call `ensure_held`.
   4. *Escape:* `with_runtime<R: Send>`. A heap object is `!Send`, so none
      can leave a scoped entry. Build-time `const` assertions (the
      ambiguous-impl trick already in `types.rs:4105-4118`) make `Value`,
      `Interpreter`, `ConsRef`, `StringObjectRef`, `SymbolRef`,
      `VectorRef`, `FloatRef`, `WeakConsRef` and `SymbolOwner` fail the
      build if any of them gains `Send` or `Sync`.
   5. *Roots in thread-locals:* the T4 statics are the runtime's C globals.
      They are reset when the runtime leaves an OS thread. An anti-cheat
      inventory gate forces every `thread_local!` to be classified as
      released with the runtime or per-OS-thread.
   6. *Public:* `emaxx::Runtime` (`!Send + !Sync`) is taken by every public
      function that runs Lisp. `compile_fail` doctests show it cannot be
      sent or shared to another thread, each next to a passing control.
4. **Library tests in parallel.**

   | | process-wide serializer at the true boundary (chosen) | each test its own runtime on its own thread |
   |---|---|---|
   | GNU closeness | GNU has one heap (alloc.c), one obarray, one mapped dump (pdumper), one set of `.eln` relocations per `dlopen`'d unit (comp.c), one SIGCHLD table, all behind `global_lock` | every one of these becomes per-thread state with no GNU counterpart, and native units could not be shared |
   | Hot-path cost | none (below) | every allocator global becomes TLS (similar cost) plus per-thread relocation of native units |
   | Effect | runtime-touching tests run one at a time, host-only tests in parallel, no crash at any `--test-threads` | real parallelism |

   The serializer is closer to GNU and keeps the costs.
5. **Per-call costs.** No new lock or thread check on `Fcons`, intern, the
   byte-code loop, or any fast path.
   - The thread-local frontier costs the same instructions as the statics
     on x86-64 Linux. The package builds with `-C relocation-model=static`
     (`tools/rustc-workspace-wrapper.py`), so a const-initialized
     thread-local with no destructor is local-exec `%fs:` addressing. The
     existing `SMALL_BLOCKS` compiles to `mov %fs:-0x15d0(,%rdx,8),%rax`
     in r380's `HostAllocator::alloc` (0x5c0157), one instruction where a
     static is one `rip`-relative `mov`.
   - **Expected probe effect: 0.0 instructions a unit**, within rounding, on
     `conses`, `conses-nogc`, `native-cons`, `make-symbol`, `intern-new`,
     `read-gensym`, `concat-uni-multi`, `byte-calls`, `lex-rec` and
     `mapc-bc`. This matters most for cons allocation, which sits at 1.000
     [0.937, 1.026] of GNU, with no margin.
   - The lock is taken once per tenure: once a process in the CLI, once a
     test.
   - Removing the native execution lock (Stage 6) *saves* the per-call
     `Mutex` lock and unlock and the `RefCell<Option<MutexGuard>>` store and
     take on every outermost Rust→native call: **an expected 30 to 60
     fewer instructions a call** in the interpreted-to-native and
     bytecode-to-native workloads.
   - macOS pays a TLV call for each thread-local access; cg-table cannot
     measure it (valgrind runs on Linux only). See §4.
6. **Finding 157:** Lisp runs on the main thread; see §1.7.

## 3. Stages

Each stage is behaviour-neutral for the CLI and the suite, builds, and must
pass the gate (the full grouped gate on the stage's tree), plus the
validations listed with it. The line counts estimate hand-written changes;
mechanical ones are listed apart. CI keeps its serial pins until Stage 8,
which says what stays and why.

### Stage 0: align the ordinary configuration (C5.1, interim)

- **Files:**
  - `.cargo/config.toml:1-6`: `RUST_TEST_THREADS` 2 → 1. Replace the
    comment with the real reason: until the runtime enforces ownership, a
    second test thread races the process heap (D43).
  - `docs/testing.md:74-75,90`: the groups run 1 test thread.
  - `tools/grouped_gate.py:101-104`: the comment stays accurate.
  - HANDOVER's D43 note: ordinary `cargo test` is now serial.
- **About 40 lines. Risk:** slower local `cargo test`.
- **Tests:** none (configuration only). R6 flags the cargo configuration
  change for review.
- **CI:** unchanged (already 1).

### Stage 1: runtime tenure primitives

- **Files:** `src/lisp/runtime.rs` (about 220), `src/lisp/types.rs` (the
  assertions, about 30).
- **What moves:**
  - `RUNTIME` becomes `Mutex<RuntimeState> + Condvar`, with holder id and
    name, still without frontier.
  - `ENTERED` splits into `HELD` (depth) and `BOUNDARY` (the stack base is
    registered), so a scoped `with_runtime` nested in an implicit tenure
    still registers and pops its stack base. `eval/roots.rs:404-414`
    asserts exactly that.
  - `ensure_held()`: `#[inline]` test, `#[cold]` acquire, tenure held in a
    TLS guard dropped at thread exit.
  - After 60 s of waiting, one stderr line names the holder.
  - `with_runtime<R: Send>`; the 23 call sites already return `Send` data
    (`identity_ptr` returns `usize`).
  - Poison policy: scoped entries keep refusing a poisoned runtime.
    Implicit tenure is released by the TLS destructor after libtest has
    caught a failing test's panic, so a failed test does not poison.
  - The `const` not-Send/not-Sync assertions for the heap handle types.
- **Risks:**
  - TLS destructor order: the tenure guard may drop before other TLS. The
    reset functions of Stage 5 use `try_with`. The frontier cells (Stage 3)
    have no destructor, so they stay readable.
- **Tests:**
  - `tenure_holds_the_runtime_until_the_thread_exits`: B's `with_runtime`
    does not enter while A's implicit tenure lives, and enters after A is
    joined.
  - `scoped_entry_under_tenure_registers_its_own_stack_base`.
  - `waiting_names_the_holder`, with a short test-only wait.
  - The existing `concurrent_entries_serialize…` (`runtime.rs:69`)
    unchanged.
- **CI:** unchanged. Reason: nothing calls `ensure_held` yet.

### Stage 2: root constructors acquire the runtime; lock order of the test gates

- **Files:** `src/lisp/eval.rs` (`construct` `:7379`, `for_image`,
  `Clone for Interpreter` `:6604`, `deep_clone_image` `:6086`, about 20),
  `src/lisp/types/symbols.rs` (`with_current` and `SymbolOwner::default`,
  about 15), `src/test_support.rs` (about 80), `src/compat.rs` (about 5).
- **What moves:**
  - `ensure_held()` at every editor construction. Every test that builds
    an editor now serializes.
  - Lock order, enforced:
    - `mark_process_test` takes the runtime before its gate, so the order
      is always runtime then process gate, and
      `lock_boot_environment_for_write` (which already calls it,
      `compat.rs:440`) inherits the order runtime → boot-env write. Boots
      take runtime → read.
    - A host permit requested while holding the runtime is `Inherited`: the
      runtime already excludes other Lisp tests.
    - An exclusive permit requested while holding the runtime panics with
      "take the exclusive permit before the first Lisp use". Check its 9 call
      sites; the one at `eval/tests.rs:264` takes it before it spawns.
  - The template's SAFETY comment and the "requires --test-threads=1"
    panic (`test_support.rs:372-378`) are rewritten: clones now live inside
    one tenure, which also closes the drop-order race of §1.3 P9.
- **About 120 hand lines; no mechanical conversion.**
- **Risks:**
  - Deadlock in a test whose libtest thread touched the runtime and then
    joins a spawned thread that also needs it. A search of the 60 spawn
    sites found none (`eval_04.rs:3914-4273`, `eval/tests.rs:82`,
    `types.rs:4312`, `primitives/tests.rs:23889`: the parents do no Lisp).
    The holder diagnostic would name the case.
  - Every gate run is still serial, so outcomes must match the base
    exactly.
- **Tests:**
  - `editors_on_two_os_threads_take_the_runtime_in_turn`: B's
    `Interpreter::new()` does not return while A's editor lives; both
    evaluate and collect correctly.
  - `a_template_clone_lives_inside_one_tenure`.
- **CI:** unchanged. Reason: allocation without an editor still races
  until Stage 3.

### Stage 3: the heap frontier moves with the runtime (C5.3's core; D43)

- **Files:**
  - `src/lisp/alloc.rs` (cons and float frontier `:350-369`; the cold path
    `allocate_cons_in_new_block` `:1787`; `abandon_allocation_frontiers`
    `:339-347`);
  - `alloc/strings.rs:361-377` (and `EMPTY_UNIBYTE/MULTIBYTE` `:332`);
  - `alloc/strings/data.rs:41-64` (the `current` pointers of `SMALL`,
    `PURE_SMALL` and `IMAGE_SMALL`; the oldest-block lists and `LARGE`
    stay static, reached only from cold paths);
  - `alloc/symbols.rs:817-821`;
  - `alloc/vectors.rs:339-341` and `ZERO_VECTOR` `:945`;
  - `runtime.rs` (frontier check-out and check-in).
- **What moves:** the 17 frontier words and arrays become const-initialized
  `thread_local!` cells with no destructor. `.load(Relaxed)` becomes
  `.get()` and `.store(x, Relaxed)` becomes `.set(x)`. Every cold path
  calls `ensure_held()` first and retries once.
- **About 200 mechanical and 250 hand lines.**
- **Risks:**
  1. Performance (measure, below).
  2. A sweep, image adoption or `abandon_allocation_frontiers` that wrote
     the statics now writes the holder's cells. It runs on the holder
     already; review each writer.
  3. glibc places a spawned thread's static TLS inside its stack mapping
     (as `bytecode/verify.rs:246-248` notes), so frontier words can be
     scanned conservatively. `mem_find` accepts only cells whose
     allocated bit is set (`alloc.rs:1924`); the float, string, symbol and
     vector `mem_find` arms need the same check confirmed.
  4. The empty canonical objects become frontier state, so a non-holder
     cannot obtain them without acquiring.
- **Tests (the second-thread proof and the parallel run):**
  - `a_thread_without_the_runtime_waits_at_its_first_allocation`: A holds
    tenure and allocates; B's `Value::cons(Value::Nil, Value::Nil)` has not
    returned after 200 ms (`recv_timeout` is `Err`) while `holder()` names
    A; after A is joined, B completes.
  - `…_waits_at_its_first_intern`, the same for `Value::symbol`.
  - `the_frontier_continues_across_os_threads`: B's first cons lies right
    after A's last in A's block, so it is one heap, as GNU's is.
  - `mem_find_rejects_the_bump_and_free_list_words`.
  - `four_test_threads_share_one_heap_in_turn`, a deterministic stress
    test: four threads each build an editor, allocate 50,000 conses and
    strings, collect and verify. It runs under the serial gate too.
- **The D43 control:**
  `cargo test --profile gate --lib -- --test-threads=4 lisp::types:: lisp::alloc:: lisp::eval::tests::eval_01::`.
  - Capture it three times on the base commit (expected: a crash or a
    census failure; if the base passes 3/3, say the control did not
    reproduce).
  - Then on this stage: identical outcomes to the serial run.
- **Performance:** `tools/perf/cg-table.py base=… new=… gnu=… -- conses
  conses-nogc native-cons:100000:300000 make-symbol:200000:600000
  intern-new:20000:60000 read-gensym:100000:300000
  concat-uni-multi:100000:300000 byte-calls lex-rec:2000:6000
  mapc-bc:200:600`, at HANDOVER's 23 November counts.
  - Expect 0.0 a unit.
  - If a probe moves by 1 or more instructions a unit, disassemble
    `allocate_cons`. If the access is not `%fs:`-direct, stop and fix the
    declaration (const init, no `Drop`).
- **CI:** add the D43 control as a step of `compat-runner.yml`, a cheap job.
  Keep the serial pins elsewhere.

### Stage 4: the host symbol table and the remaining unsafe impls

- **Files:** `src/lisp/types.rs:345-378` (delete `ProcessTable`),
  `types/symbols.rs:994-1075` (host table in `RuntimeState`, installed in a
  TLS slot while held), `runtime.rs`, `sqlite.rs:55-58`,
  `interactive/kbd_input.rs:194-215`.
- **What moves:**
  - `HostSymbols(Rc<Symbols>)` is the only new `unsafe impl Send`.
    SAFETY: it moves only under the runtime lock, and release asserts
    `Rc::strong_count == 1`, so no clone outlives a tenure.
  - Remove `unsafe impl Sync for ProcessTable` and the two SQLite `Send`
    impls.
  - The `EmergencyReset` writer calls `ensure_held`.
- **About 180 lines.**
- **Risks:** `symbol_ids_resolve_across_serialized_host_threads`
  (`types.rs:4799`) must still see one host symbol across OS threads (it
  will: the table moves with the runtime). A clone held across a release
  trips the assertion; that would be a real bug.
- **Tests:**
  - `host_symbols_move_with_the_runtime`.
  - `a_host_symbol_clone_cannot_outlive_its_tenure`: the assertion, in a
    `catch_unwind` on a spawned thread, because H4 forbids
    `#[should_panic]`.
- **CI:** unchanged plus the control.

### Stage 5: thread-local roots are released with the runtime (the A2.5 use-after-free)

- **Files:**
  - `runtime.rs`: `release_thread_state()`, run at every release.
  - About 12 modules gain a `release_*` function of 5 to 15 lines:
    `profiler.rs` (logs dropped, memory running cleared, CPU timer
    disarmed), `atimer.rs` (list cleared, `set_alarm(None)`),
    `dispatch/display.rs`, `interactive.rs`, `completion.rs`,
    `dispatch/misc_keymaps.rs`, `eval/terminal.rs` (frame route and
    controlling output), `reader.rs` and `print/object.rs` (stacks
    emptied), `syntax_scan.rs`, `types/string_data.rs`.
  - `anti_cheat.rs`: a new gate,
    `thread_locals_are_released_with_the_runtime_or_per_os_thread`, about
    120 lines. It lists every `thread_local!` in runtime files and requires
    each to be named in `release_thread_state` or in a `PER_OS_THREAD` table
    with its reason, like `interpreter_value_fields_are_gc_roots_or_documented`
    (`anti_cheat.rs:1608`). It is added to `enforce_all_owned` and gets a
    `#[test]` wrapper.
  - `tests/runtime_ownership.rs`: the public control.
- **About 400 lines.**
- **Behaviour:**
  - The CLI releases only at exit, and each test thread releases only at
    its end, so both are unchanged.
  - Consecutive public calls on one thread no longer carry C-global state.
    That is GNU's fresh-process semantics, and the fix.
- **Risks:**
  - Completeness: the gate makes it checkable.
  - A reset that touches a destroyed TLS key must use `try_with`.
  - Disarming the process-wide alarm must not cancel a scoped caller's
    timers on another thread. There are none: the runtime has one holder.
- **Tests:**
  - `editor_state_left_by_one_os_thread_is_not_read_after_another_collects`
    (public API, three calls, as in §1.4 T4). Each call checks its own
    expectation in Lisp: `(unless (null (profiler-memory-running-p))
    (error …))` and `BatchRunOutcome::Exit(0)`. Base behaviour: running-p
    is t, and the log may name freed vectors.
  - The inventory gate itself.
- **CI:** unchanged plus the control.

### Stage 6: the native execution lock is subsumed by the runtime (A2.3, performance)

- **Files:**
  - `native_comp/runtime.rs:752-805` (delete `NATIVE_EXECUTION_LOCK`,
    `NATIVE_EXECUTION_ACTIVATIONS`, `NATIVE_EXECUTION_LOCK_HELD` and
    `NativeExecutionGuard`; `with_active_call` `:838` keeps its `outermost`
    logic; `ACTIVE_NATIVE_HEAP` stays as the holder's);
  - comments in `runtime/suspension.rs` and `docs/thread-continuation-audit-2026-09-09.md`
    ("Native activation locking remains held across suspended
    activations…");
  - new probes `tools/perf/probes/bc-to-native.el` and
    `interp-to-native.el` (a byte-compiled or interpreted loop calling one
    native-compiled function); the `cg-table.py` docstring.
- **About 120 lines.**
- **Risks:** finding 84 must not regress (suspended native activations
  relied on the lock's hand-over).
- **Validation:**
  - `tests/native_thread_continuations.rs`.
  - The upstream thread-tests selection (36: 34 pass, the shared
    `threads-join-error` fails, 1 skipped).
  - The native suite (178).
  - The lib tests naming `native`, `continuation`, `suspension` and
    `thread`.
  - The September 9 audit's `native-workers.el` pair: GNU and Emaxx print
    `(t 2 (0 0) (1 caught 1) [1 1])`.
- **Tests:** `native_activation_requires_no_second_lock`: two OS threads
  take turns running native code through scoped entries, with no
  deadlock and the right results.
- **Performance:** the new probes plus `native-base` and `native-cons`.
  Expect 30 to 60 fewer instructions a call on the new probes and 0 on
  the others.
- **CI:** unchanged plus the control.

### Stage 7: the public type-level owner and doctests (A4.7's public half)

- **Files:**
  - `src/lib.rs`: `pub use lisp::runtime::Runtime`.
  - `runtime.rs`: `pub struct Runtime`, `!Send + !Sync`, `Runtime::lock()`
    (blocking; reentrant on the holder).
  - Public signatures take `&Runtime`: `batch.rs:71,80,114,130,444,467`,
    `tty.rs:542`, `lisp/mod.rs:1196`, `anti_cheat.rs:2098`.
  - Callers: `main.rs:164` (`let runtime = Runtime::lock()` at the top of
    `main_status`, = `init_threads`), `src/bin/compat-harness.rs:1201`,
    `tests/runtime_ownership.rs`, `tests/ert_runner.rs`.
  - `tools/grouped_gate.py`: a `doc` cargo stage (`cargo test --doc`);
    `tools/test_grouped_gate.py`.
- **About 300 lines.**
- **Tests (the compile-fail proof):**
  - `compile_fail` doctests on `Runtime`:
    `std::thread::spawn(move || drop(runtime))`;
    `std::thread::scope(|s| { s.spawn(|| emaxx::batch::version_banner(&runtime, None)); })`;
    naming `emaxx::lisp::types::Value`.
  - Each comes with a passing doctest of the same shape without the
    offending line (an owned `String` result crosses to another thread),
    so a typo cannot pass as a compile failure.
  - Replace the weak doctest at `lisp/mod.rs:6-10` the same way.
- **Risks:**
  - Seccomp: main's uncontended acquire must not futex-wait.
  - `--version` and `--help` must still work before Lisp.
- **CI:** the grouped gate gains the doc stage (R6).

### Stage 8: runner configuration, controls and records (D43, A4.7, C5.1 closure)

- **Files:** `.cargo/config.toml`, `tools/grouped_gate.py:101-106`,
  `tools/serial_grouped_gate.py` (comment only),
  `.github/workflows/compat-runner.yml`, `frozen-run.yml`,
  `docs/testing.md`, `HANDOVER.md` (D43 entry),
  `docs/honesty-audit-2026-08-18.md` (rows 157 and 84 notes),
  `docs/milestones.md`.
- **About 120 lines.**
- **Decisions (no pin remains for soundness; each remaining pin has a
  resource or measurement reason):**
  - `.cargo/config.toml`: back to 2, comment rewritten to the resource
    reason ("runtime tests serialize on the runtime; the second worker
    overlaps host-only tests; more workers starve child-process deadlines").
  - `grouped_gate.py`: groups keep `test_threads=1`, the reason now being
    scheduling: Lisp tests serialize on the runtime, so a second worker
    only waits and triggers libtest's 60 s warnings.
  - `serial_grouped_gate.py` and `frozen-run.yml`: stay serial, because the
    frozen run times the gate build (D31).
  - `module-eglot.yml`, `openpgp-linux.yml` and
    `loader-timeout-diagnostic.yml` rerun 1 to 3 exact tests: keep, for
    readable output.
  - The D43 control stays in `compat-runner.yml`. Add a second control:
    the whole lightweight group at `--test-threads=4`.
  - The parallel control cannot be a gate group: `classify_inventory`
    refuses a test selected twice (`grouped_gate.py:169-170`), so it is a
    cargo stage after the groups.
- **Records:** mark A4.7, C5.1, C5.3 and D43 done with the control logs.
  A2.5 is done for thread access only; see §4 for unwinding and aliasing.

### Stage 9: unwinding and signals (A2.5's remaining half)

- **Files:** `processes.rs:1160,4385` and `eval/threads.rs:406` (helper
  threads spawned with async signals blocked: block in the parent, spawn,
  restore), `native_comp/runtime.rs` around `:5533-5560`, and
  `anti_cheat.rs`.
- **What moves:**
  - Spawn helpers with signals blocked, so process signals reach the Lisp
    thread as `deliver_process_signal` arranges in GNU (sysdep.c:1739).
    This is neutral on Linux, where the main thread is usually chosen.
  - State the POF rule for non-local exits in one place, `jump_nonlocal`'s
    doc.
  - Add a gate that checks the shape of every caller of
    `remember_helper_error` and `remember_active_helper_error`: the call is
    the tail of an `Err` arm, and the function declares no local of an
    owning type (`Vec`, `SmallVec`, `String`, `Box`, `Rc`, `Ref`/`RefMut`,
    `MutexGuard`).
  - Audit the signal handlers' plain arithmetic (overflow checks are on in
    the gate profile).
- **About 200 lines.**
- **Tests:**
  - `a_signalling_native_many_call_leaks_nothing`: 100,000 calls of a
    native `&rest` subr with more than 8 arguments that signals, so
    `SmallVec` spills. Resident memory must not grow by more than 1 MB.
  - `helper_threads_block_process_signals`: read the helper's
    `/proc/self/task/TID/status` `SigBlk` on Linux.
- **CI:** unchanged.

Order and dependencies: 0 → 1 → 2 → 3 → 4 → 5 are a chain. 6 and 9 need 3.
7 needs 1. 8 comes last.

## 4. Traps and open questions

**Traps.**

- **`tools/round_audit.py`.**
  - H5: no `#[allow]` or `#[expect]`. Deleting `ProcessTable`,
    `NativeExecutionGuard` and the SQLite impls must also delete what they
    leave unused (`PhantomData`, `MutexGuard` imports), never silence it.
    The existing `#[allow(dead_code)]` at `eval.rs:6670` predates this
    work.
  - H4: panics are tested with `catch_unwind` on a spawned thread, never
    `#[should_panic]`.
  - H6: no stage removes a test. The process-test and bootstrap gates
    keep their tests even where the runtime makes them redundant.
  - R4 flags every new static and `thread_local!` (Stage 3 adds about 17,
    Stage 1 about 3). Each round record names them.
  - R6 flags `.cargo`, the workflows, `grouped_gate.py` and `anti_cheat.rs`.
  - R2 flags `catch_unwind` in runtime code (none is added).
- **`src/anti_cheat.rs`.**
  - Its textual anchors must survive: `pub struct InterpreterState {`,
    `pub(crate) fn weak_hash_reachability(`, `pub(crate) fn
    mark_static_roots_into`, `struct ThreadExecutionContext {`,
    `struct ThreadState {`, and `if record.kind == RecordKind::Thread`
    (`anti_cheat.rs:1608-1745`).
  - `enforce_all` runs under `with_runtime`, and after Stage 7 under
    `&Runtime`.
  - New gates need both a `#[test]` wrapper and an `enforce_all_owned`
    entry (`:2102-2190`).
- **The gate profile.** It inherits release with debug assertions and
  overflow checks (`Cargo.toml` `[profile.gate]`), and the frozen run times
  it (D31). A `debug_assert!(held())` on a fast path is therefore a timed
  cost: put checks only on cold paths.
- **Serial runner pins.**
  - `serial_grouped_gate.py` monkeypatches `test_threads=1` and
    `RUST_TEST_THREADS=1`.
  - `grouped_gate.py:263,689` pins the environment and records it in the
    summary.
  - `frozen-run.yml` pins it in about 10 places; `compat-runner.yml:40-54`,
    `module-eglot.yml:107-180`, `openpgp-linux.yml:128-140` and
    `loader-timeout-diagnostic.yml:110-116` also pin it.
  - `docs/testing.md` is stale.
- **The dumper.**
  - GNU loads the dump in `main` before `init_threads`. Emaxx loads it
    under the runtime, on the main thread in the CLI and on the test's
    thread in tests.
  - `IMAGE_REGION_OPEN`, `ImageCells` and image block adoption must stay
    holder-only, and image adoption's frontier writes go to the holder's
    cells (Stage 3).
  - Lock order is runtime → fixture `flock` (`batch.rs:1050-1066`).
  - Dumping with live Lisp threads stays refused.
  - The dumped `main-thread` is the running process's
    (`DUMP_OBJECT_IS_RUNTIME_MAGIC`).
- **Cooperative threads (finding 84).** Lisp threads stay continuations on
  the holder's OS thread. The runtime is never released at a Lisp thread
  switch, unlike GNU's `release_global_lock` in `thread_select` and
  `yield`, and that must stay so. The thread-local frontier is shared by
  all continuations on that OS thread. Validate as in Stage 6.
- **Timers and atimers.** `ATIMERS` lives in the holder's TLS, and
  SIGALRM and the timerfd set only atomics. Stage 5 clears the list and
  disarms the process alarm at release. Lisp `timer-list` timers run on
  the Lisp thread.
- **Process filters.** Filters run on the Lisp thread from
  `wait_reading_process_output`. Descriptors belong to Lisp threads, which
  are cooperative. Helper threads hold only host `Child` handles.
- **SIGCHLD.** The handler polls slots lock-free and wakes the Lisp thread
  through the child-signal pipe. Both a reaper thread and the handler may
  `waitpid` the same child; the `ECHILD` handling needs a look
  (pre-existing).
- **SIGIO** has `F_SETOWN_EX` set to the setting thread
  (`tty/sysdep.rs:338-358`). If the runtime moves between OS threads
  (tests only), SIGIO goes to the old thread. This is not reached in
  production.
- **Implicit tenure deadlocks:** a thread holding the runtime that waits on
  another thread needing it, or a gate taken in the wrong order (Stage 2).
  The 60 s holder diagnostic makes any case visible. libtest's
  new-thread-per-test model is assumed; a runner that reused threads would
  hold tenure across tests, which is coarser but still sound.

**Open questions.**

1. **macOS:** each thread-local access is a TLV thunk call. Accept the
   cost, or keep statics plus cold-path checks on macOS only (weaker: a
   tripwire, not a proof)? Measure with the contract runner's wall time
   on the macOS runner before deciding.
2. **Implicit tenure versus a panic in production.** Recommended: the same
   implicit acquisition everywhere, with the diagnostic. A
   `cfg!(test)`-only switch would make the boundary behave differently in
   tests.
3. **The POF condition** for `longjmp` over Rust shim frames is a
   convention, and Stage 9's gate is textual, not a proof. The structural
   alternative, return-then-jump through an asm thunk, costs a compare on
   every helper return (the `native-*` probes would show it).
4. **Suspended native frames:** confirm that `prepare_nonlocal_exit` only
   returns buffers on the jumping stack (`suspension.rs`).
5. **Per-editor obarrays** (`SymbolOwner::fresh` in every
   `Interpreter::new`) and marking of other live editors
   (`eval.rs:5545-5562`) have no GNU counterpart: they exist because tests
   build several editors in one process. That is outside this group, but
   Stage 2 makes the "parked" claim in `registered_gc_view`'s SAFETY
   comment true for other OS threads.
6. **A2.5's aliasing and lifetime half** is beyond the thread group:
   - `InterpreterStateOwner`'s `DerefMut` from a shared `NonNull`
     (`eval.rs:6567-6574`);
   - `ProcessTable::with_borrow`'s `&mut` from `&self` (removed in
     Stage 4);
   - `registered_gc_view`.

   A full aliasing audit (Miri cannot run this crate's FFI paths) should be
   scheduled separately before A2.5 is marked done.
7. **The reaper threads** (thcount 2 where GNU has 1, with no Lisp thread)
   are a post-lock finding. Replacing them with GNU's `deleted_pid_list`
   reaping in `handle_child_signal` would also remove the waitpid race
   noted under SIGCHLD.

## Appendix A: every static and thread-local by group

Generated from the source at `41202f70` (name:line). The classification is
by name and type and was hand-checked for P2–P5, P9 and T4. Stage 5's
inventory gate replaces it with an exact, enforced list. P groups are
process statics (193), T groups thread-locals (121).

### P0: the runtime lock (GNU global_lock) (1)

- `src/lisp/runtime.rs`: RUNTIME:12

### P1: immutable tables and constants (14)

- `src/lib.rs`: GLOBAL_ALLOCATOR:13, GLOBAL_ALLOCATOR:17
- `src/lisp/alloc/counters.rs`: FORWARDED_COUNTERS:33
- `src/lisp/alloc/vectors/hash_tables.rs`: EMPTY_INDEX:11
- `src/lisp/bytecode.rs`: OPERANDLESS_OPS:738
- `src/lisp/bytecode/verify.rs`: EMPTY_CLOSURES:237
- `src/lisp/native_comp/abi.rs`: C_VARIABLE_WATCHERS:425
- `src/lisp/native_comp/generated_native_subrs_aarch64_apple_darwin.rs`: NATIVE_SUBRS:13
- `src/lisp/native_comp/generated_native_subrs_x86_64_unknown_linux_gnu.rs`: NATIVE_SUBRS:13
- `src/lisp/native_comp/runtime.rs`: SUBR:12256
- `src/lisp/primitives/pdumper/image.rs`: DEFAULT_FINGERPRINT_COMPLEMENT:39, EMAXX_FINGERPRINT:51
- `src/lisp/types.rs`: INTERNED_SYMBOL_NAMES:378
- `src/lisp/types/string_data.rs`: EMPTY_DATA:38

### P2: allocator frontier (free lists, bump regions, string-data blocks) (21)

- `src/lisp/alloc.rs`: FREE_LIST:357, BUMP_NEXT:358, BUMP_END:359, SERIAL:361, FLOAT_FREE_LIST:364, FLOAT_BUMP_NEXT:365, FLOAT_BUMP_END:366
- `src/lisp/alloc/strings.rs`: FREE_LIST:361, BUMP_NEXT:373, BUMP_END:374
- `src/lisp/alloc/strings/data.rs`: SMALL:47, LARGE:48, PURE_SMALL:52, PURE_LARGE:53, IMAGE_SMALL:58, SMALL_ENTRIES_RETIRED:64
- `src/lisp/alloc/symbols.rs`: SYMBOL_FREE_LIST:817, SYMBOL_BUMP_NEXT:818, SYMBOL_BUMP_END:819
- `src/lisp/alloc/vectors.rs`: FREE_LISTS:339, LAST_INSERTED_FREE_INDEX:341

### P3: host symbol table (ProcessTable, unsafe Sync) (1)

- `src/lisp/types/symbols.rs`: HOST_SYMBOLS:994

### P4: heap registries, image region, collector epoch, canonical empty objects (17)

- `src/lisp/alloc.rs`: BLOCKS:144, HEAP_LOW:155, HEAP_HIGH:156, IMAGE_REGION_OPEN:238, IMAGE_STRINGS:289, IMAGE_SYMBOLS:290, IMAGE_VECTORS:291, HEAP_ROOTS:735, LIVE_STATES:1217
- `src/lisp/alloc/arena.rs`: CLASSES:31
- `src/lisp/alloc/strings.rs`: EMPTY_UNIBYTE:332, EMPTY_MULTIBYTE:333
- `src/lisp/alloc/vectors.rs`: FREED_RECORDS:370, ALL_BUFFERS:380, ZERO_VECTOR:945
- `src/lisp/types.rs`: GC_MARK_EPOCH:179, GC_SWEEP_PENDING:190

### P5: second serializers and template/dump guards (5)

- `src/lisp/eval.rs`: IMAGE_TEMPLATE_ACTIVE:6518
- `src/lisp/native_comp/runtime.rs`: NATIVE_EXECUTION_LOCK:759, ACTIVE_NATIVE_HEAP:760, MEMORY_FULL:1076
- `src/lisp/primitives/pdumper/mod.rs`: DUMP_MESSAGES_SUPPRESSED:202

### P6: signal-handler shared flags and timers (async-signal-safe) (27)

- `src/lisp/eval.rs`: CHILD_SLOTS:2434, CHILD_SIGNAL_HANDLED:2440
- `src/lisp/primitives/atimer.rs`: PENDING_SIGNALS:15, TIMERFD:80, ALARM_TIMER:82, ALARM_TIMER_OK:83, ATIMER_INITIALIZED:172
- `src/lisp/primitives/file_io.rs`: STDERR_DUP:1673
- `src/lisp/primitives/interactive.rs`: PENDING_SIGUSR1:48, PENDING_SIGUSR2:50, INSTALLED:58, PENDING_FATAL_SIGNAL:86, FATAL_ERROR_IN_PROGRESS:91, ATTEMPT_ORDERLY_SHUTDOWN:98, INSTALLED:211, NONINTERACTIVE:320, SIGSEGV_STACK:334
- `src/lisp/primitives/interactive/kbd_input.rs`: PENDING_INTERRUPTS:97, WINDOW_CHANGED:101, EMERGENCY_RESET:209
- `src/lisp/primitives/processes.rs`: CHILD_SIGNAL_READ_FD:222
- `src/lisp/primitives/profiler.rs`: SAMPLES_PENDING:27, CPU_TICKS:30, MEMORY_RUNNING:32, MEMORY_BYTES:34, PROFILER_TIMER:449, PROFILER_TIMER_OK:451

### P7: counters and serials (relaxed atomics) (46)

- `src/lisp/alloc.rs`: LIVE_CONSES:350, FREE_CONSES:351, LIVE_FLOATS:368, FREE_FLOATS:369
- `src/lisp/alloc/counters.rs`: CONS_CELLS_CONSED:24, FLOATS_CONSED:25, VECTOR_CELLS_CONSED:26, SYMBOLS_CONSED:27, STRING_CHARS_CONSED:28, INTERVALS_CONSED:29, STRINGS_CONSED:30
- `src/lisp/alloc/strings.rs`: CODE_CHANGES:366, LIVE_STRINGS:375, LIVE_BYTES:376, LIVE_SPANS:377
- `src/lisp/alloc/symbols.rs`: LIVE_SYMBOLS:820, FREE_SYMBOLS:821
- `src/lisp/alloc/vectors.rs`: LIVE_VECTORS:346, LIVE_VECTOR_SLOTS:347, LIVE_CLOSURES:348, VECTOR_SWEEPS:351, LIVE_CLOSURE_SLOTS:358, LIVE_BIGNUMS:359, LIVE_OVERLAYS:360, LIVE_RECORDS:363, LIVE_RECORD_SLOTS:364
- `src/lisp/alloc/vectors/char_tables.rs`: CHAR_TABLE_STORES:19
- `src/lisp/alloc/vectors/hash_tables.rs`: LIVE_ARRAY_BYTES:552, THAWED_BYTES:559
- `src/lisp/eval.rs`: NEXT:1390, PROCESS_TICK:3018, PROCESS_ALIST_REMOVALS:3026
- `src/lisp/eval/core.rs`: PROFILING:31
- `src/lisp/eval/local_cells.rs`: SHAPE:32
- `src/lisp/native_comp/runtime.rs`: LISP_ALLOCATED_BYTES:950
- `src/lisp/primitives.rs`: TEMP_NAME_COUNTER:276, MAKE_SYMBOL_COUNTER:277, FILE_NOTIFY_DESCRIPTOR_COUNTER:284
- `src/lisp/primitives/accessors_random.rs`: COUNTER:189
- `src/lisp/primitives/processes.rs`: SERIAL:940, PROCESS_OUTPUT_DELAY_COUNT:3864
- `src/lisp/primitives/profiler.rs`: LIVE_BYTES:360
- `src/lisp/primitives/tz.rs`: LOCALTIME_OFFSET:1190, GMTIME_OFFSET:1191
- `src/lisp/sqlite.rs`: DB_COUNT:251
- `src/lisp/types.rs`: FRESH_OBARRAY_SYMBOL_SERIAL:2643

### P8: once-initialized host facts and caches (35)

- `src/lisp/alloc.rs`: STRESS:1245, VERIFY:2114
- `src/lisp/bytecode/vm.rs`: PRIM_FACTS:1356
- `src/lisp/eval.rs`: INITIAL:5110
- `src/lisp/eval/core.rs`: PATH:23
- `src/lisp/eval/terminal.rs`: LIBRARY:269
- `src/lisp/mod.rs`: TRACE_LOAD_ERRORS:528
- `src/lisp/native_comp/abi.rs`: BY_NAME:378
- `src/lisp/native_comp/gccjit.rs`: API:548
- `src/lisp/native_comp/runtime.rs`: CAR_SUBR_INDEX:3205, CDR_SUBR_INDEX:3206, TYPE_OF_SUBR_INDEX:3207, SYMBOL_VALUE_SUBR_INDEX:3208, NREVERSE_SUBR_INDEX:3209, FUNCALL_SUBR_INDEX:3210, LENGTH_SUBR_INDEX:3211, FBOUNDP_SUBR_INDEX:3212, AREF_SUBR_INDEX:3213, CHAR_TABLE_EXTRA_SLOT_SUBR_INDEX:3214, ASET_SUBR_INDEX:3215, AUTOLOAD_DO_LOAD_SUBR_INDEX:3216, SYMBOL_FUNCTION_SUBR_INDEX:3217, FUNCALL_SUBR_INDEX:4578, FUNCALL_SUBR_INDEX:4759, FUNCALL_SUBR_INDEX:5252
- `src/lisp/primitives/coding/charset_table.rs`: CACHE:611
- `src/lisp/primitives/dispatch/frames.rs`: GEOMETRY:670
- `src/lisp/primitives/file_io.rs`: CODING_TAG:951
- `src/lisp/primitives/filelock.rs`: BOOT_TIME:63
- `src/lisp/primitives/pdumper/image.rs`: FINGERPRINT:63
- `src/lisp/primitives/processes.rs`: NOFILE_LIMIT:1800
- `src/lisp/primitives/system.rs`: LIBACL:356, LIBATTR:411, FIELDS:637
- `src/lisp/primitives/values.rs`: UTF8:2812

### P9: test-only gates and template (8)

- `src/compat.rs`: LOCK:411
- `src/lisp/primitives/tests.rs`: PROBE_ID:6782
- `src/test_support.rs`: GATE:127, GATE:132, GATE:142, FRESH_REQUIRED:355, TEMPLATE:368, GATE:540

### P10: host I/O and process-wide host state (Mutex/atomic) (18)

- `src/lisp/eval.rs`: GNUTLS_LOG:3088, GNUTLS_LOG_PENDING:3089
- `src/lisp/eval/terminal.rs`: TERMCAP_LOCK:287, DEV_TTY:362
- `src/lisp/primitives/batch_stdout.rs`: STDOUT:49, BUFERR:55, STDERR_FAILED:58, BUFERR_STATE:62, INITIAL_ARGV:92
- `src/lisp/primitives/dispatch/files_process.rs`: ACCOUNT_DATABASE_LOCK:5
- `src/lisp/primitives/dispatch/gnutls.rs`: GNUTLS_LIBRARY:289
- `src/lisp/primitives/dispatch/lists.rs`: BATCH_STDIN:311
- `src/lisp/primitives/processes.rs`: INIT:238, TABLE:1943, PROCESS_OUTPUT_SKIP:3882, LAST_READ_CHANNEL:3885
- `src/lisp/primitives/system.rs`: BUILD_DETAILS:3416, DAEMON:3448

### T1: per-OS-thread by nature (stacks, host allocator, lock holding, test permits) (17)

- `src/allocator.rs`: SMALL_BLOCKS:55, FLUSH_AT_EXIT:63
- `src/batch.rs`: CALLER_VALUE:1367
- `src/compat.rs`: BOOT_ENV_WRITE_HELD:416
- `src/lisp/alloc.rs`: PARKED_STACKS:716, DRIVER_REGIONS:721, STACK_TOP:2298
- `src/lisp/alloc/strings.rs`: LIVE_GUARDS:297
- `src/lisp/eval/continuations.rs`: CURRENT_RESUME:44, CURRENT_STACK_BASE:45, CURRENT_STACK_LIMIT:48, C_STACK_FLOOR:52
- `src/lisp/native_comp/runtime.rs`: NATIVE_EXECUTION_ACTIVATIONS:766, NATIVE_EXECUTION_LOCK_HELD:768
- `src/lisp/runtime.rs`: ENTERED:15
- `src/test_support.rs`: HOST_PERMIT_HELD_BY_THIS_THREAD:92, PROCESS_TEST_PERMIT:153

### T2: scratch buffers and host-keyed caches (no Lisp objects) (18)

- `src/lisp/eval/core.rs`: PROFILE_CHILD_STACK:57, PROFILE_TABLE:59, PROFILE_DUMP_COUNTDOWN:61
- `src/lisp/eval/terminal.rs`: DEFAULT_TTY_COLORS:78, TPUTS_OUTPUT:163
- `src/lisp/eval/threads.rs`: READ_BUFFER:10
- `src/lisp/primitives/dispatch.rs`: NAME_FACTS:398
- `src/lisp/primitives/dispatch/misc.rs`: DOC_FILE_CACHE:2349, LISP_SOURCE_DOC_CACHE:2400
- `src/lisp/primitives/interactive.rs`: SYMBOL_WITH_POS_FLAG_READ_COUNT:1026
- `src/lisp/primitives/print/object.rs`: PRINT_OBJECTS:133
- `src/lisp/primitives/search.rs`: PATTERN_CODES:92, COMPILES:101
- `src/lisp/primitives/tz.rs`: TZFILES:664, ZONES:666
- `src/lisp/reader.rs`: INFILE_LOADS:195
- `src/lisp/regex_emacs/compile.rs`: WORK:692
- `src/lisp/regex_emacs/exec.rs`: SPARE_STATE:113

### T3: heap-identity caches and active-call pointers (valid only during a call or epoch) (17)

- `src/lisp/bytecode/verify.rs`: CACHE:96, VERIFIED:103, CLOSURES:250, CLOSURES_SWEPT:255, CLOSURE_TABLE:258
- `src/lisp/eval.rs`: FRAME_DETAIL_POOL:1984
- `src/lisp/modules/mod.rs`: ACTIVE_ENVS:154, ACTIVE_RUNTIMES:155, MODULE_GC:156
- `src/lisp/native_comp/loader.rs`: ACTIVE_REGISTERED_RUNTIME:155, ACTIVE_COMPILER:157
- `src/lisp/native_comp/runtime.rs`: ACTIVE_CALL:709, ACTIVE_GC:712, ACTIVE_GC_TRIGGER:716
- `src/lisp/primitives/search.rs`: SEARCHBUFS:89, CANON_CACHE:96
- `src/lisp/types/string_data.rs`: STRING_CHAR_BYTE_CACHE:447

### T4: editor state holding Lisp objects or heap handles (roots marked only by the collecting OS thread) (23)

- `src/lisp/eval/terminal.rs`: PRIMARY_TOP_FRAME:1452, CONTROLLING_OUTPUT:1453
- `src/lisp/primitives/atimer.rs`: ATIMERS:74
- `src/lisp/primitives/completion.rs`: MINIBUFFER_INPUT_METHOD:1313
- `src/lisp/primitives/dispatch/display.rs`: ECHO_AREA_MESSAGE_VALUE:33, ECHO_AREA_DISPLAY:477
- `src/lisp/primitives/dispatch/misc_keymaps.rs`: SEMANTIC_CPP_INCLUDE_TAG_CACHE:9
- `src/lisp/primitives/interactive.rs`: TTY_FRAME_REDRAW:2061, PREVIOUS_ECHO_AREA_MESSAGE:2911
- `src/lisp/primitives/interactive/command_loop.rs`: RESIZE_ECHO_AREA_EXACTLY:23
- `src/lisp/primitives/interactive/kbd_input.rs`: KEYBOARD_TERMINAL:38
- `src/lisp/primitives/menu.rs`: TTY_MENU_ACTIVATE:1671
- `src/lisp/primitives/print/object.rs`: PRSTACK:124
- `src/lisp/primitives/profiler.rs`: PROFILER:306
- `src/lisp/primitives/syntax_scan.rs`: GL_STATE:352, FIND_START:929
- `src/lisp/reader.rs`: SAVED_STRINGS:145, RDSTACK:526
- `src/lisp/types/symbols.rs`: ACTIVE:999, ACTIVE_KEYS:1004, ACTIVE_POS_FLAG:1007
- `src/tty.rs`: WALK_LAYOUTS:5235, PAINT_TERMINAL:10447

### T5: editor/C-global state without Lisp objects (keyboard, tty, echo flags, auto-save, random) (46)

- `src/lisp/eval/terminal.rs`: OUTPUT_DEVICE:1450, TTY_ROUTE_ACTIVE:1451
- `src/lisp/primitives/accessors_random.rs`: RANDOM:58
- `src/lisp/primitives/dispatch/display.rs`: ECHO_AREA_MESSAGE:28, ECHO_AREA_LAST_DISPLAYED:41, ECHO_PRESERVE_REDISPLAY:44, ECHO_FROM_PRINT:48, MESSAGE_LOG_NEED_NEWLINE:51, ECHO_MESSAGE_TICK:57
- `src/lisp/primitives/dispatch/display/mode_line.rs`: LINE_NUMBER_DISPLAYED:1801
- `src/lisp/primitives/file_io.rs`: AUTO_SAVING:3384, AUTO_SAVE_MODE_BITS:3386, AUTO_SAVE_ERROR_OCCURRED:3388, AUTO_SAVING_DIR_UMASK:3391
- `src/lisp/primitives/interactive.rs`: TTY_EVENT_READER:1657, TTY_EVENT_POLLER:1669, TTY_CURSOR_IN_ECHO_AREA:1671, TTY_READ_END_TIME:1730, RAW_EVENT_READ:1745, TTY_IDLE_START:1853, TTY_LAST_IDLE_START:1856, READ_CHAR_RETRY:2057, READ_RECORDED:2822, READ_USED_MOUSE_MENU:2824, READ_MENU_EXIT:2829, LAST_EVENT_SOURCE:2862, TTY_INPUT_DISCARD:3163
- `src/lisp/primitives/interactive/kbd_input.rs`: KEYBOARD_WAITING_THREAD:41, FORCE_QUIT_COUNT:44, WAITING_FOR_INPUT:48, QUIT_THROWN_TO_READ_CHAR:51, QUIT_CHAR:53, TTY_INPUT_MODES:56, SYS_MODES_HOOK:59, FRAME_GARBAGED:63, TTY_SHOWING_MENU:67, INPUT_WAS_PENDING:72, REDISPLAY_INPUT_PENDING:77, LAST_AUTO_SAVE:80, LAST_NON_MINIBUF_SIZE:83, REDISPLAYING_P:86, DEV_TTY:89, TTY_ERASE_CHAR:320
- `src/lisp/primitives/window.rs`: INTERACTIVE_WINDOW_METRICS:15, SCROLL_PRESERVE:1505
- `src/tty.rs`: TERMINAL_GONE:851
