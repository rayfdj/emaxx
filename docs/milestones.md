# Milestones and locked scope

The goal's 296 completion items (the goal's requirements, the performance
contract, the September 21 de-cheating findings, HANDOVER's recorded findings
and the honesty audit) were counted on 23 November 2026 with 81 done. The
owner signed off this assignment of the other 215 on that day. The list is
closed: work proceeds one milestone at a time, in the order below, and is
reported as items done of that milestone's list.
A finding that joins a milestone under the first rule below is added to that
milestone's list with the round that found it (D133, round 375's review).

## Rules

- A finding made after 23 November 2026 joins a milestone only if it is a
  regression of the round under review, or a crash, a hang or data loss.
  Anything else is recorded in HANDOVER's findings as found after the scope
  lock and waits until the goal closes.
- A round's independent review answers whether the round cheats, forwards to
  GNU, or breaks something. It does not survey the neighbourhood of the
  change.
- One milestone at a time. A milestone is done when its exit test passes; an
  item that proves larger or already done changes its own status, not the
  list.
- Unfinished work is pushed as a packaged draft (wip/rNNN, source untouched)
  before any wait.

## Decisions of 23 November 2026

- The frozen run goes on timing the checked (gate) build (D31).
- macOS validation runs on a GitHub-hosted macOS runner; it does not wait for
  the owner's machine.
- The items under Accepted are closed as documented and explained, with no
  code change.
- The items under Parked are real differences that only contrived input
  reaches; they are left until the goal closes.

## M0. Secure and baseline (done)

Rounds 364 to 366 merged to main (PR 143, main at 39aa23f); round 367's
unfinished work pushed as a packaged draft (wip/r367).

## M1. Scope lock (done)

This document.

## M2. Structure and performance (30 items; 5 done)

Exit test: the remaining representation, collector and ownership engineering
is done, the recorded per-call excesses are removed where GNU does less, and
all 16 locked workloads have their upper 95% bound at or under 1.03 of GNU in
a diagnostic run of the locked suite on the profile-guided build.
Judged on a noisy host (decided 24 November 2026): each workload's median at
or under 1.00 of GNU in that run, and the bound of 1.03 applied to each
workload that the same session's run of GNU against itself resolves (its
interval within 0.97 and 1.03).

- A2.3: No unnecessary GNU-absent mechanisms (parallel object models, shadow
  state, synchronization layers, identity caches, registries, replacement
  Lisp)
- A2.5: Rust soundness preserved (aliasing, lifetimes, thread access,
  unwinding across incompatible boundaries)
- A3.3: Necessary allocator metadata (e.g. the cons block's 8-byte weak-
  reference serial) accounted for separately and honestly
- A3.4: Interpreter, bytecode and native code observe/mutate the same
  authoritative objects; duplicate payloads, agreement checks, conversions,
  mutation notif…
- A3.5: Ordinary car/cdr, field stores, identity checks and common value
  operations have GNU-comparable direct paths (no mandatory lookup)
- A3.6: Symbols, strings, vectors, records, closures and native handles
  reviewed for the same duplicate-representation problem
- A3.8: Temporary migration adapters have explicit removal criteria and are
  removed before the architecture is declared complete
- A4.1: Allocation, marking, sweeping, root handling and dumped-object
  treatment follow GNUImage symbols and other records are still rebuilt into
  the heap whe…
- A4.2: Duplicate tracing and unnecessary retention from mirrors, registries
  and reconstructed state removed
- A4.3: Honest accounting of allocated bytes, live objects, collection
  triggers, retained memory and collection work
- A4.7: Runtime ownership/serialization enforced at the actual boundary
  (public Rust APIs, process-global state), not by a serial test runner
- A5.3: Redundant decoding, type reconstruction, lookups, frame construction,
  temporary allocations, argument copying and bookkeeping removed where GNU
  does l…
- B4: Workload bytecode-calls: upper 95% bound of Emaxx/GNU needs final-source
  rerun: latest recorded contract-runner result (Round 193 measured
  (--diagnost…
- B10: Workload cons-allocation: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--diagnos…
- B15: Workload upstream-undo: upper 95% bound of Emaxx/GNU Latest recorded
  contract-runner result (Round 193 measured (--diagnostic, no calibration,
  round-1…
- B19: Real allocation counters (all seven memory-use-counts counters)
  recorded in the final run
- C5.1: Finding 5: ordinary test configuration aligned with the single-OS-
  thread runtime assumption
- C5.3: Finding 5: ownership/serialization enforced at the runtime boundary
  (safe public APIs cannot race the allocator/symbol tables)
- D2: tramp-tests body gap: pty read timing, 2.3x string consing (round 327)
- D3: Windows as generic records rather than GNU's struct window pseudovector
  (round 321). In progress (round 369: every access through one handle;
  round 371: the window is its own PVEC_WINDOW object with its slots in it;
  round 372: GNU's contents word and bits, its own number, use time and cursor
  bit; round 373: geometry and redisplay state as its own integers and bits;
  round 375: holders and Rust signatures keep the window, not its id).
  **Done** (round 378: no window ids; window.h's struct window, its size and
  census GNU's).
- D4: Native code's calls into subrs cost ~65 vs 14 instructions (round 321).
  In progress (round 374: sixteen type predicates are their own entries, 78
  against 73 for the probe; round 377: nineteen more subrs, the word-level
  ones and the readers of the interpreter's state; the general entry is
  unchanged).
- D9: Dynamic-binding byte-code call from byte code ~80 instructions over GNU
  (rounds 289, 295)
  **Done** (round 376: 634.0 against 636.2 a level of the recursion).
- D13: looking-at ~250 instructions over GNU around the match (rounds 174-176,
  296)
  In progress (round 381: the current buffer is its object; one liveness
  and one base_buffer, as buffer.h's).
- D14: make-symbol ~690 instructions (round 173)
  **Done** (round 379: 1,280 against 1,965 a call).
- D17: Rust-to-byte-code call ~120 instructions over GNU (rounds 167, 297).
  In progress (round 370: 414 against 289 a `mapc' callback, was 458).
- D22: Building ucs-names costs 1,024M vs 818M instructions (round 162)
- D23: Reading a large file still costs 85M vs 67M instructions (rounds 161,
  175). **Done** (baseline of 23 November 2026: 60.0M against 67.2M).
- D25: Closure entry from Rust 807 vs 639 instructions per mapatoms callback
  (rounds 159, 160). In progress (round 370: 501 against 459, was 548).
- D43: Library tests share one process heap and are not thread-safe (parallel
  runs crash)
- D125: Interpreted code and the call from C to a Lisp function are slower
  than GNU's (measured in round 364): four (ignore X) forms in a source loop
  take 1.3…
  **Done** (round 368: 0.98 and 0.71 of GNU's time on the profile-guided
  build).

## M3. Fix list (55 items; 1 done)

Exit test: every item below is fixed, or verified against its own recorded
cases and closed. D119's panic comes first. Instruction counts of the call and
interpreter loops are measured each round and may not rise without a stated
reason.

- A8.13: Clean results obtained by fixing causes, without introduced lint
  suppressions, broad allowances or exclusions
- C6.2: Finding 6: consing.el's 'conses live' label (actually the object size,
  16)
- C6.3: Finding 6: committed tools/perf timing probes check results (or are
  not used as evidence)
- D10: Fatal-signal handling lacks shut_down_emacs work and C-stack-overflow
  recovery (rounds 115, 285)
- D12: Insertion stickiness reads text-property-default-nonsticky without
  signalling (round 174)
- D45: The errno text follows LC_MESSAGES (round 329): emacs_strerror
  synchronizes the messages locale; Emaxx sets only LC_CTYPERecorded in
  HANDOVER's findin…
- D46: load's read errors (round 329): GNU reads a stream and takes a read
  error for end of file; Emaxx reads the whole file and signals a non-GNU
  "Reading f…
- D50: The errno text's post-read-conversion and autoloaded locale coding
  systems
- D57: The 187 name tests left that may stand for EQ, and bare-only SYMBOLP
  guards before an EQRecorded in HANDOVER's findings by round 331
- D58: == between Lisp values is `equal' where GNU's C uses EQ: a census to
  make
- D59: window_list_1 binds inhibit-quit around its walk, which a variable
  watcher sees; Emaxx's window walks do not
- D60: The echo area buffers are on GNU's buffer list in an interactive
  session; Emaxx's tty session lists neither
- D62: A replaced byte-code constant stays a weak-table key after garbage-
  collect in ~1 of 7 runs (pre-existing)
- D69: TLS over a process that is no network connection: p->infd and p->outfd
  as the transport, the session's reads and writes for every process kind
- D73: The terminal session's remaining keyboard and display differences:
  read_char's menu prompting, secondary terminals' input, modes and sizes,
  sys_subshe…
- D74: A quit a handler-bind handler leaves pending is processed after the
  search, where GNU's find_handler_clause polls for it at each later clause;
  redispl…
- D75: The reader state GNU shares between nested reads (labels, read-from-
  string's index), which Emaxx keeps per reader (round 346)
- D77: callint's `e' code after a key read in the middle of a command reads
  what that read left in this_command_keys in GNU; Emaxx walks the keys as the
  call…
- D80: A tty's glyphless characters as produce_glyphless_glyph shows them, and
  compositions in posn-at-x-y (round 347)
- D81: Deferred native compilation: defalias of a byte-compiled definition
  while a .elc loads queues native--compile-async
  (maybe_defer_native_compilation), …
- D86: The terminal's tab bar: no update_tab_bar, and tab-bar-lines draws
  nothing on a terminal (round 349)
- D87: The mini window's buffer at a minibuffer's exit, which runs window-
  buffer-change-functions in GNU (round 349)
- D89: Long lines' display shortcuts: display_line's chars_to_skip past large-
  hscroll-threshold, line starts sought no further back than the small and
  medium…
- D101: Display lines joining long invisible runs are laid out a character at
  a time: pos-visible-in-window-p in a folded 500 KB org buffer 0.29 s against
  GNU…
- D104: Display lines laid out whole by the planner, the fontification walks
  (again from a start inside the line) and the drawing, where GNU's iterator
  stops …
- D117: Found by round 363's first review outside the round: a user's dump
  made after *scratch* was killed loads with its first window showing a live
  buffer t…
- D119: Found by round 363's second review outside the round: a window of the
  dumped session held by a variable of a user's dump panics window-list-1
  after th…
  **Done** (round 371: a loaded window is a deleted window under an id of
  its own; the dump starts and answers as GNU's).
- D120: noninteractive is two things in GNU and one in Emaxx (round 364's
  first review): GNU's C reads a flag set once from the command line; Emaxx
  reads the …
- D122: The echo area is not a buffer (round 364's reviews): GNU's print to t
  inserts into  *Echo Area 0*, which Lisp can read; Emaxx keeps host text, so
  char…
- D123: The obarray holds fewer symbols than GNU's: # against n=17837 at the
  start of a batch session (the oracle is an X build; how much is that and how
  much…
- D127: DISP_CHAR_VECTOR outside the printer (round 364's second review):
  round 364 gave the char table GNU's two display-table lookups and uses them
  for stdo…
- D128: nil and t are symbols to GNU's evaluator (round 366's review): it
  looks any symbol form up in the lexical environment and evaluates a subr's
  omitted a…
- D131: (when FORM . SPEC) display specifications are not evaluated (round
  366's review, on a terminal): Emaxx shows the specification without asking
  FORM
- D132: A fatal signal waits for the next check (seen in round 366): GNU's
  handler for SIGTERM runs kill-emacs from the signal itself; Emaxx runs it
  when its …
- D133: A block allocation the system refuses aborts (round 375's review):
  GNU's lisp_align_malloc signals memory_full for the blocks conses and the
  other small objects come from; Emaxx's new_block asserts
- E46: Interpreted closure prints #, not #[...] (verify against the audit's
  own cases: a probe on round 366 already answers as GNU)
- E54: Char table prints #, not #^[...] (verify against the audit's own cases:
  a probe on round 366 already answers as GNU)
- E56: Unnamed thread/mutex/condvar print Emaxx identity (verify against the
  audit's own cases: a probe on round 366 already answers as GNU)
- E57: wrong-type-argument data: 104 sites remain (verify against the audit's
  own cases: a probe on round 366 already answers as GNU)
- E61: propertize property order differs in prin1 (verify against the audit's
  own cases: a probe on round 366 already answers as GNU)
- E69: DEFVAR completeness: dump-frozen values
- E71: find-composition string surface (verify against the audit's own cases:
  a probe on round 366 already answers as GNU)
- E76: Probe-found runtime gaps (ppss elt-2/10, charset text prop, WTA tail)
  (verify against the audit's own cases: a probe on round 366 already answers
  as GNU)
- E82: Boxed-float eq identity approximated (verify against the audit's own
  cases: a probe on round 366 already answers as GNU)
- E84: Cooperative thread model cannot suspend mid-body
- E111: network-interface-info nil on non-macOS platforms
- E127: Supra-Unicode characters in strings/buffers (verify against the
  audit's own cases: a probe on round 366 already answers as GNU)
- E160: comp--install-trampoline with a plain subr (verify against the audit's
  own cases: a probe on round 366 already answers as GNU)
- E161: comp--compile-ctxt-to-file0 without a context
- E164: Async pipe process without :stderr gets two pipes (verify against the
  audit's own cases: a probe on round 366 already answers as GNU)
- E165: Backtraces record unevaluated frames only for some special forms
  (verify against the audit's own cases: a probe on round 366 already answers
  as GNU)
- E166: *Messages* buffer-modified-p after startup (verify against the audit's
  own cases: a probe on round 366 already answers as GNU)
- E167: time-convert (TICKS . HZ) reduced (verify against the audit's own
  cases: a probe on round 366 already answers as GNU)
- E168: garbage-collect SIZE columns report GNU layouts (verify against the
  audit's own cases: a probe on round 366 already answers as GNU)
- E169: GC cadence differs from GNU's (verify against the audit's own cases: a
  probe on round 366 already answers as GNU)

## M4. Freeze and certify on Linux (40 items)

Exit test: on the frozen final source: the Linux validation reruns, profiles,
reclamation checks, the final adversarial review and the evidence record. The
six roll-up items (A6.1, A9.6a to A9.6e) close with their groups.

- A1.4: September 21 findings reproduced against the starting revision,
  distinguishing already-fixed from still-present
- A1.6: Corrected baseline established with final measurements and provenance
  so the cost of restored behavior stays visible
- A2.1: Each changed mechanism traced to its actual GNU implementation
  (lisp.h, alloc.c, data.c, eval.c, bytecode.c, native compilation, dumping)
- A2.2: Division between behavior owned by C primitives and by unchanged GNU
  Lisp preserved
- A2.4: Every necessary deviation names its
  Rust/ownership/platform/integration constraint, correctness argument and
  measured cost
- A3.7: Object identity, sharing, cycles, mutation visibility, alignment,
  native/module ABI and dump restoration preserved
- A4.4: Collection-count differences under equivalent settings explained
- A4.5: Gains not obtained by suppressing collection, undercounting
  allocation, retaining dead objects or shifting unreported work outside
  measurement
- A4.6a: Survival and eventual reclamation verified: suspended bytecode stacks
- A4.6b: Survival and eventual reclamation verified: native calls
- A4.6c: Survival and eventual reclamation verified: lexical environments
- A4.6d: Survival and eventual reclamation verified: dynamic bindings
- A4.6e: Survival and eventual reclamation verified: unwind state
- A4.6f: Survival and eventual reclamation verified: weak references
- A4.6g: Survival and eventual reclamation verified: Lisp threads
- A4.6h: Survival and eventual reclamation verified: restored images
- A5.1: Profiles of interpreted execution, bytecode dispatch, primitive/Lisp
  calls, native transitions, variable access and returns taken on the shared
  repres…
- A5.2: Instructions and allocations per operation measured alongside elapsed
  time
- A5.4: Evaluation order, arity checks, mutation, errors, backtraces, quit
  checks, hooks, binding and nonlocal exits preserved
- A5.5: Redefinition, advice, aliases and dynamic behavior that invalidate
  assumptions preserved on optimized paths
- A5.6: Further specialization introduced only with explicit validity
  conditions and adversarial tests
- A6.5: Runtime code, caches, startup, images, native loading, test selection,
  comparison logic and reporting audited for cheats (special cases, canned
  result…
- A6.6: Differential/adversarial GNU-vs-Emaxx tests on the same inputs
  covering changed names/paths, sizes/contents, shared/cyclic structures,
  mutation, redef…
- A7.6: Persistent slowdowns remain visible with profile-based explanations
- A8.1: Linux: cargo fmt --all -- --check clean on the final source
- A8.2: Linux: cargo check --locked --all-targets --all-features clean on the
  final source
- A8.3: Linux: cargo clippy --locked --all-targets --all-features -- -D
  warnings clean on the final source
- A8.4: Linux: git diff --check clean on the final source
- A8.5: Linux: python3 tools/serial_grouped_gate.py --scope full clean on the
  final source
- A8.11: Linux: release-specific paths validated (not only the gate profile),
  zero compiler warnings
- A8.14: Linux: ordinary full frozen comparison against the pinned oracle on
  the final source
- A8.25: Upstream tests, selectors, expected outcomes, timeouts, comparison
  strictness and inventory preserved (no new ignores, expected-failure
  conversions, r…
- A8.26: Separately scoped platform differences explicit in raw results and
  the final report, never counted as passes
- A9.1: Concise record maps each architectural change to its GNU mechanism,
  removed overhead, correctness evidence and performance result
- A9.2: Commands, source/artifact identities, raw logs, profiles, all timing
  samples and reproduction instructions retained
- A9.3: Final adversarial review of the actual final source and complete
  evidence chain
- A9.4: Checks invalidated by later changes rerun on the final source
- A9.5: Audit coverage and remaining limitations reported without claiming
  universal absence of cheating
- C3.5: Finding 3: fresh complete frozen result for the current implementation
  before carrying the compatibility score forward
- E75: Anti-cheat blind-spot catalogue

## M5. External (40 items)

Exit test: macOS validation of the final source on a hosted runner (build,
lint, gate, frozen run, terminal scenarios) and the certified performance run
on a quiet locked host.

The first hosted macOS build check (frozen-run.yml's `macos-build`, run
45096909115 on `394d57de`, macos-15 on Apple silicon, Rust 1.100.0):
formatting passes; `cargo check` stops at 13 errors in the library (15 with
its tests): `libc::getrandom`, `mkostemp` (two sites), `pipe2` (a test),
`O_PATH`, `ESTRPIPE` and `SI_TKILL`, which only Linux has; `MaybeUninit` and
`ManuallyDrop` in native_comp/runtime.rs, imported for Linux only; and a
mismatched type at primitives/file_io.rs:1466. More may follow once these
compile. They are this milestone's work.

- A7.7: Authoritative timing: repeated interleaved runs on one host under
  controlled load, every sample retained,
  distributions/ratios/regressions/aggregates …
- A8.6: macOS: cargo fmt --all -- --check clean on the final source
- A8.7: macOS: cargo check --locked --all-targets --all-features clean on the
  final source
- A8.8: macOS: cargo clippy --locked --all-targets --all-features -- -D
  warnings clean on the final source
- A8.9: macOS: git diff --check clean on the final source
- A8.10: macOS: python3 tools/serial_grouped_gate.py --scope full clean on the
  final source
- A8.12: macOS: release-specific paths validated, zero compiler warnings
- A8.15: macOS: ordinary full frozen comparison against the pinned oracle on
  the final source (including the Tramp observation)
- A8.16: Affected native compilation contracts pass on the final source (Linux
  and macOS)
- A8.17: Affected module contracts pass on the final source (Linux and macOS)
- A8.18: Affected dumping contracts pass on the final source (Linux and macOS)
- A8.19: Affected startup contracts pass on the final source (Linux and macOS)
- A8.20: Affected thread contracts pass on the final source (Linux and macOS)
- A8.21: Affected GC contracts pass on the final source (Linux and macOS)
- A8.22: Affected integration contracts pass on the final source (Linux and
  macOS)
- A8.23: All 226 terminal scenarios pass on the final source
- A8.24: Existing native artifact identity requirements preserved
- B1: Workload interpreted-lexical: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--dia…
- B2: Workload interpreted-dynamic: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--dia…
- B3: Workload interpreted-calls: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--diagn…
- B5: Workload native-execution: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--diagno…
- B6: Workload interpreted-to-native: upper 95% bound of Emaxx/GNU Latest
  recorded contract-runner result (Round 193 measured (--diagnostic, no
  calibration,…
- B7: Workload bytecode-to-native: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--diag…
- B8: Workload native-to-interpreted: upper 95% bound of Emaxx/GNU needs
  final-source rerun: latest recorded contract-runner result (Round 193
  measured (--d…
- B9: Workload native-to-bytecode: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--diag…
- B11: Workload list-traversal: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--diagnost…
- B12: Workload mapcar: upper 95% bound of Emaxx/GNU needs final-source rerun:
  latest recorded contract-runner result (Round 193 measured (--diagnostic, no
  c…
- B13: Workload explicit-gc: upper 95% bound of Emaxx/GNU needs final-source
  rerun: latest recorded contract-runner result (Round 193 measured
  (--diagnostic,…
- B14: Workload upstream-sort: upper 95% bound of Emaxx/GNU needs final-source
  rerun: latest recorded contract-runner result (Round 193 measured
  (--diagnosti…
- B16: Workload upstream-bindat: upper 95% bound of Emaxx/GNU needs final-
  source rerun: latest recorded contract-runner result (Round 193 measured
  (--diagnos…
- B18: Calibration: a matching full GNU self-comparison resolves every
  workload's 95% interval within [1/1.03, 1.03]Calibration unresolved; the
  round-47 self…
- B20: Certified final run: nine rounds, one warmup, three samples,
  rotation/alternation, quiet host, non-diagnostic, frozen oracle identity,
  final source, c…
- B21: Every retained timed body at least 100 ms in the final run
- B22: Final run: valid result and mode checks, complete inventory, no
  unsuccessful/missing process or report, unchanged measurement inputs
- B23: Native preparation: each editor compiles its own unchanged comp-test-
  funcs.el on the final source, hashes retained outside body timing
- C4.2: Finding 4: macOS build check added to the normal validation path
- C4.3: Finding 4: the current source builds on macOSneeds final-source rerun;
  no macOS build recorded after PR87
- D47: The kqueue (macOS) file-notify backend still names its errors with text
  paths; needs a macOS check
- D66: The macOS spawn's copies of descriptors: Rust's Command copies the
  parent's tty and the :stderr pipe's descriptor, and redirects before the
  child open…
- D115: The terminal tests' clock outside Linux: the pseudo-terminal harness
  reads thread states from /proc; without it (macOS) every process counts as
  asleep…

## Accepted: closed as documented (25 items)

Exit test: none; closed by the owner's decision of 23 November 2026.

- D7: Instruction-cache footprint; hot-first link ordering tried and rejected
  (round 305)
- D8: GNU faults found, not fixed (transpose-regions multibyte middle, stale
  undo point); excluded from fuzzes
- D11: Allocations outside the Lisp object makers abort instead of memory_full
  (round 277)
- D15: Gate build's excess over release is its checks (round 172)
- D16: Frozen run's gate-build spending past the release build (round 168)
- D18: comp-tests' time is the instruction rate (round 166)
- D20: undo-tests' million insertions (rounds 165, 175)
- D26: Frozen run's slowest files are mostly the gate profile's (round 160)
- D27: What the gate profile adds to a file's setup (round 157)
- D28: Expensive tests: tramp-test45 race and slow ratios (round 155)
- D29: What the checked build costs (round 131)
- D30: Process descriptors: GLib wakeup eventfd differs, one descriptor lower
  (rounds 119-287)
- D31: The frozen run times the gate profile, not release (round 112)
- D32: Setup's time at equal instructions: I-cache misses, text size (round
  110)
- D33: electric-tests bodies at near-equal instructions; profile-guided build
  (round 150)
- D34: A test file's setup by stage, collections apart (round 106)
- D35: A test file's setup in wall time; relocation table and image size
  (round 102)
- D36: Where a test file's setup goes, mutator and collector apart (round 100)
- D42: GNU 30.2 mapconcat crash when FUNCTION shortens the list (excluded from
  oracle comparisons)
- D44: Interpreter state behind Option costs a test and branch per field
  access (kept)
- D65: A :nowait TLS client left connecting with no socket, its name lookup's
  answer never taken (one run of the m-z group, beside the integration
  targets; n…
- D121: What print.c's undefined cases give is not reproduced (round 364, by
  decision): where GNU reads memory that is no part of the text (its print
  stack re…
- E117: Intermittent gate test under load
- E157: process-attributes of the Emaxx process reports state S / thcount 2
- E163: (featurep 'x) is nil where the Linux oracle provides x

## Parked: after the goal closes (20 items)

Exit test: none; outside the goal by the owner's decision of 23 November 2026.

- D5: No newline region cache; newline-cache-check answers nil (round 320)
- D6: Static compositions laid out per character in the screen-line layouter
  (round 319)
- D61: A frame deleted from an advised get-scratch-buffer-create during kill-
  buffer: GNU re-enters ~130 times, Emaxx deletes once (untraced)
- D83: The glyph matrices' state redisplay decides from: window_end_valid, the
  this_line optimizations, try_cursor_movement and try_window_id; Emaxx keeps
  a …
- D84: Redisplay marking a frame again: GNU's retry_frame, and
  windows_or_buffers_changed 50 with a retry for the selected frame; Emaxx
  displays once, and fr…
- D85: Mode lines displayed to measure: pos_visible_p (pos-visible-in-
  window-p, the scroll commands) displays a window's mode line, evaluating its
  :eval form…
- D93: Point inside an automatic composition after a command:
  composition_adjust_point by the glyph string's glyphs (a Devanagari cluster
  on a terminal) (rou…
- D94: A wide character the window's edge splits: move_it_in_display_line_to
  counts as many of its glyphs as fit on the continued row, so pos-visible-in-
  wind…
- D95: Inside a composition: GNU's cursor, posn-at-point and posn-at-x-y
  follow the composed glyph for a position inside it (e + U+0301); Emaxx's
  stay on the…
- D98: Fontification behind the iterator: GNU fontifies while it walks, so a
  change a function makes to text already passed does not change that walk's
  answe…
- D99: Point moved by Lisp mid-redisplay: GNU restores PT only when
  redisplay_window ends, so the mode line and a second scroll see the moved
  point; Emaxx re…
- D105: Redisplay paths not modelled: try_window_id (which runs wherever bidi-
  paragraph-direction is set, as in prog-mode buffers), redisplay-skip-
  fontificati…
- D108: Where move_it_to (PT) stops at a string at a line start: GNU's walk
  from an earlier line stops before a before-string or display string at a
  line star…
- D110: Strings and the iterator, still apart: force_start's alt_pt in
  invisible text and C-f out of a string taller than the window (round 356
  review probes …
- D112: A display string taller than the window, still apart: a before-string
  "AB\nCD" with a cursor property on B, and so cursor on all of a before-
  string "B…
- D114: Display strings and the text they cover: an after-string at the end of
  text a display string replaces (posn-at-point before it in GNU, after it in
  Ema…
- D116: Stretches and positions at an hscrolled row's edges (round 362
  review): an :align-to space takes its width when the line is built, from a
  column count…
- D126: print-circle numbers a string's shared property values in the order of
  positions; GNU in the order of its interval tree (round 364's second
  review): l…
- D129: After makunbound of a C-defined variable, GNU's C still has its own
  variable (round 366's review): it goes on reading and assigning it, and what
  Lisp …
- D130: Byte code runs under an environment frame GNU does not have (round
  366's review): a byte-compiled function that declares a caller's lexical
  variable s…
