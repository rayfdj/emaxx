# Resume the full compact-runtime goal here

The goal is **active and incomplete**. Read the [complete goal and all completion
requirements](docs/runtime-representation-goal.md) and the
[locked performance contract](docs/core-runtime-performance-contract.md) before
editing. A validated checkpoint, green gate or handover does not complete it.
Current user instructions take precedence over historical notes.

This file holds only the current state and the next steps. Every earlier
checkpoint, failure, diagnosis, draft and evidence link is preserved verbatim in
the [15 October archive](docs/runtime-representation-handover-history-2026-10-15.md)
(and, before that, the [1 October archive](docs/runtime-representation-handover-history-2026-10-01.md)).
The [requirement map](docs/runtime-representation-progress.md) tracks the full
scope; the [accounting continuation](docs/runtime-representation-accounting-draft.md)
records the GNU counter contract.

## Where things stand — 15 October 2026

| State | Contents |
| --- | --- |
| **Main** `94295d0a` | Source324: one-word values, 16-byte cons payloads, shared object work, and symbol value/function/plist/flags on the allocated symbols. |
| **[PR87](https://github.com/rayfdj/emaxx/pull/87)** `35c6cd9b` (`runtime-char-tables`) | Object-state draft17: membership, constants, aliases, same-name replacement, image restoration and native `nil`/`t` identity. 584 runtime inputs. |
| **Task branch** `runtime-performance` (pushed as `claude/awesome-newton-yk6sae` through `c48c5a2`; PR #88 took it into main up to `ba696b3`, PR #89 up to `6242417`, PR #90 up to `8aaccaa`, PR #91 up to `aa0c63f`) | Stacked on PR87: allocation counters and forwarded counter variables, actual header charges, image objects outside the GC census, char-table store counts for regexp caches, and a performance round on undo, sorting and the call path (below). |

**PR87 evidence in this repository:** strict checks with zero warnings, 407
affected controls in each of gate and release, and 44 ordinary GNU comparisons /
88 processes (see the archive's 13 October section). Its three Linux CI runs on
the PR head are green.

**PR87 results reported by the previous session, whose raw evidence is on the
macOS host and not in this repository:** full Linux Rust 3,200 passes and macOS
Rust 3,188 passes (two existing ignores each); Linux frozen all 519 files /
7,928 outcomes; three Linux pilots at about 2.73× GNU (geometric mean) and
1.33% slower than main; macOS frozen 318 files / 4,479 matches before both
Tramp timeouts, with 200 files unrun; the 226 terminal scenarios not yet run.
Record the raw evidence with the merge.

**To merge PR87** (macOS host): the Tramp observation with the established
600-second bound, the exact remaining 200 macOS frozen files, all 226 terminal
scenarios, an audit of the results, and the published evidence. Existing
approval covers task pushes, Linux CI and the main merge.

The previous session also held an unpushed local draft correcting the symbol
header charge (64 bytes charged as 48). The task branch implements that
correction independently (below); the local draft need not be recovered.

## Task branch: allocation counters and forwarded variables

GNU `alloc.c` raises seven process-wide counters at its allocation points and
exposes them as DEFVAR_INT variables. Previously Emaxx bound the seven names to
zero and `memory-use-counts` signalled "unavailable", which also blocked the
performance contract's allocation evidence.

- [`alloc/counters.rs`](src/lisp/alloc/counters.rs) holds the seven words.
  They are raised at the single allocation point of each kind: `allocate_cons`,
  `allocate_float`, `allocate_symbol`, `allocate_vectorlike` (the words asked
  for after the header, before block rounding) and ordinary non-empty
  `allocate_string` (one string plus its bytes). Pure and image-restored
  strings are not counted, as in GNU.
- The symbols forward to the words: `SymbolCell` gains an `INT_FORWARD` state
  whose payload is the counter (GNU's `Lisp_Intfwd`). Reads, stores, `let`
  bindings and image snapshots act on the counter. Stores keep the existing
  FWD_INT checks; `makunbound` makes the symbol plain while the counter keeps
  counting. Ordinary value reads pay one combined state test.
- After loading an image, the counters stand at the image's recorded values
  (pdumper's `dump_fwd_int`), not at those values plus the loader's rebuilding.
- Copying an editor's symbol graph copies the forwarding, not a frozen integer.
- `memory-use-counts` returns the seven counters.
- Symbol and string headers are charged at the actual 64- and 48-byte cells
  (GNU: 48 and 32), in the GC tally, live bytes and the `garbage-collect`
  report. The lower charges undercounted live bytes and delayed collection.

- The counters exposed a real `float` deviation: GNU returns a float argument
  itself, while Emaxx allocated a copy (`(eq f (float f))` was nil). Fixed.

**Explicit differences that remain:** `intervals-consed` is never raised
because text properties are not yet allocated intervals. Vector cells count
Emaxx's actual vector-like sizes, so pseudovectors laid out differently from GNU
(bignums, hash tables and others) differ, and objects Emaxx keeps outside the
Lisp heap are not counted. In particular, **bool vectors** (and other
interpreter records) keep their slots in a host vector, one Lisp value per
bit, so only the record object is counted: `(make-bool-vector 1000 nil)` adds
16 vector cells in GNU and a fixed record size in Emaxx. That representation
(a word per bit where GNU packs bits) is open architecture work. The
`garbage-collect` report shows 64/48 where GNU shows 48/32. String data is
still charged by GNU's `sdata` formula, not measured.

**Validation so far:** cargo check, rustfmt and `git diff --check` pass. In
the gate profile, the five affected oracle-compared controls pass
(`memory_use_counts_exposes_the_allocation_telemetry_boundary_honestly`,
`memory_use_counts_counts_actual_allocations_by_category`,
`allocation_counter_variables_forward_to_the_counters`,
`float_returns_a_float_argument_itself` and
`garbage_collect_reports_the_live_census`). The first run of the category test
failed on `float` and bool vectors, as described above.

**Linux CI on `23d230a`** (counters, float fix, header charges):
- Full Rust gate (`frozen-run.yml`, run 38694907626): **3,203 passed, 0
  failed**, the two existing ignores; strict formatting and lint in CI.
- Frozen (run 38694910744): **7,927 of 7,928 outcomes match across 519
  files**. The one mismatch is GNU's: `eglot-test-rust-completion-exit-function`
  failed in the oracle with `jsonrpc-error` while waiting on the external
  `rust-analyzer` server (the server was then killed); Emaxx passed. The run
  is recorded as failed; a fresh frozen run is pending.
- Three paired pilots against PR87's head (run 38694913972), all 192
  answers/modes verified: body geometric ratio **1.0118** (diagnostic only).
  Every case is within about ±1% except **mapcar, 16.5% slower with 32
  collections instead of 22**. The heaps are identical; the cause is the
  broken GC trigger below, which the larger honest header charges made
  worse. The same pilots show the trigger bug across the suite (Emaxx/GNU
  collections): sort 139/18, interpreted calls 80/27, bindat 54/9,
  interpreted lexical 40/13, undo 23/6.

## Task branch: image objects outside the GC census (`8aafca7`)

The collection trigger was wrong. GNU's `total_bytes_of_live_objects` counts
only objects in swept blocks; pdumper's mapped objects are never counted, and
`gc-cons-percentage` scales the threshold from that live heap. Emaxx rebuilt
the image in ordinary blocks and subtracted everything the loader allocated
(16.67 MB, transient garbage included) from a census of 14–16 MB, so live
bytes saturated to **zero at every collection** (measured with a throwaway
diagnostic build) and Emaxx collected every `gc-cons-threshold` bytes
regardless of heap size.

Now blocks registered while an image loads are image blocks (`alloc::ImageRegion`,
a guard held by `pdumper_load`). Sweeps still clean dead objects there (they
own host storage) and release empty blocks, but leave image objects out of the
census and their freed cells off the free lists, as GNU never reuses dumped
storage. Opening and closing the region move every allocator to new blocks;
the allocation fast path is unchanged. The `image_bytes` subtraction is gone.

Measured locally: a bare `-Q` session's live bytes are 1.18 MB (GNU's
non-dumped heap is about 0.58 MB; Emaxx makes more symbols and strings after
load and charges larger headers), so Emaxx now collects every ~148k conses
where GNU collects every ~73k. Upstream sorting now collects 0 and 1 times
for one and two units, as GNU does (4 and 8 before). The `garbage-collect`
census now has GNU's shape: the heap's own objects.

**Validation:** 331 of the 333 locally selected GC, allocation and image
tests passed on the first run. `escaped_frame_graph_survives_its_owner_and_is_then_reclaimed`
failed from register allocation in the test itself: gdb showed the first
iteration's unhidden terminal word left in the test function's `rbp`, spilled
by `flush_stack_call_func` as a root after the second iteration reused the
cell. `1b14ec4` decodes in an `#[inline(never)]` helper, as the survival check
already did; the assertion is unchanged, gdb finds no hidden word on the stack
or in spilled registers, and it passes normally and under `EMAXX_GC_STRESS`.
`batch_startup_image_round_trip_or_explicit_native_image_limit` fails
identically in this container at `23d230a`, which CI passed, so it is
environmental here (an obarray enumeration order difference). Clippy with
`-D warnings` passes.

CI's full gate on `1b14ec4` (run 38703616708, image template on) then failed
one test, `garbage_collect_reports_the_live_census`: its floors (more than
10,000 conses, at least one buffer) only held while Emaxx counted its image;
GNU's own bare census reports 3,173 conses. `8a75c7f` counts image buffers
again (GNU's `sweep_buffers` counts every live buffer, dumped or not) and
replaces the floors with a growth check GNU and Emaxx both answer
`(t t t t t t 20000 50000 500 2000)`. **CI's full gate on `8a75c7f` passes**
(run 38709432511).

Three paired pilots of `8aafca7` against `23d230a` (run 38702850532), all 192
answers/modes verified, diagnostic only: bodies **19.8% faster** (geometric
mean); the geometric GNU ratio goes from **2.777 to 2.210**. Upstream sort
5.54× → 3.53× GNU with 18 collections (GNU 18, was 139); interpreted calls
2.85× → 1.48× (15 collections, GNU 27); bindat 3.27× → 1.70×; interpreted
lexical 2.64× → 1.60×; mapcar 2.96× → 1.90×. Two cases are slower and need
an explanation: **bytecode-to-native 26% slower** (samples 1.93–1.97 s before,
2.45–2.46 s after; no collections in either) and **explicit-gc 8% slower**.
Locally, callgrind gives the same work per iteration for bytecode-to-native
before and after (1,719.9 vs 1,724.2 instructions) and cachegrind's simulated
caches show essentially no misses in either build, so that slowdown is a
placement effect the simulator does not model (branch aliasing, 4K aliasing
or alignment), not added work; it stays unexplained and is to be rechecked in
later pilots. Explicit GC does 1.6% more instructions per collection (65.5M
vs 66.6M: the image-block checks in the sweeps).

Both full frozen runs (on `23d230a` and `1b14ec4`) match 7,927 of 7,928
outcomes; the mismatch is GNU's own `eglot-test-rust-completion-exit-function`
failing (`jsonrpc-error` from `rust-analyzer`) while Emaxx passes. Runs of that
file alone: PR87's branch passed twice (runs 38713593324, 38715907382); this
branch failed once (38713596938) and passed once (38715910214). The harness
runs the two editors concurrently (here 39 s and 45 s, started 3 s apart), each
with its own `rust-analyzer`, and GNU's test calls itself "pretty brittle"
about server timing. This branch makes Emaxx much faster, which moves its own
rust-analyzer tests relative to GNU's. The evidence fits a concurrency timing
interaction, not a behavior change; it is not proven, and the mismatch is
recorded as such rather than counted as a match.

## Task branch: char-table store count for regexp caches (`4057404`)

The regexp backend compiles syntax, case and category table contents into its
patterns, so every cache hit re-walked and compared the whole reachable table
graph (case-folding search does this on every match). In a startup that loads
one file this was **1.93 of 3.47 billion instructions**; GNU's whole startup is
0.32 billion. Char-table and sub-char-table slots change only through
`set_slot`/`set_extra`, which now count every store; GNU's native compiler
emits calls for `aset` (it inlines only car/cdr/setcar/setcdr and a few
predicates and arithmetic operations) and the module API has no raw access.
While the count is unchanged since a signature last matched in full, a hit
skips the table nodes and compares only conses, vectors, records and strings,
which native code can still change directly. Debug-assertion builds (the gate)
also compare the full graph and require the same answer; that cross-check
caught the one test whose hand-written "native" function poked sub-char-table
memory, a store no native code can make. Its store is now the `aset` subr call
comp.c emits; every assertion is unchanged. **Startup falls from 3.47 to 1.55
billion instructions** (GNU 0.32). 494 of 495 regexp/syntax/case/char-table
tests passed before that test's rewrite; the last passes after it.

`4c13fba` closes a hole in that commit: a new table could take a dead table's
address with its slots written by its constructor, so table creation now
counts as a store too (unit test added). `8352eb5` reuses a recent capture
whose table still matches (an unchanged graph is traversed into the same
nodes) and hashes captures by a digest taken once; debug builds compare every
reuse with a fresh capture. **Startup: 3.47 → 1.55 → 1.25 billion
instructions** (GNU 0.32). 106 char-table/case/syntax/regexp tests pass with
the reuse cross-check.

Pilots of `4057404` against `8a75c7f` (run 38713604238): undo 10% faster (the
most regexp-heavy body); the other cases move within this runner's noise (GNU's
own explicit-GC samples spread from 0.19 to 0.32 s), and body timing excludes
the startup this change mostly speeds up. Diagnostic only.

**Frozen on `4c13fba`** (run 38716001257): **all 7,928 outcomes match across
519 files**, no unsuccessful execution; GNU's eglot rust-analyzer test passed
this time. Median per-file setup in the frozen logs: Emaxx 970 ms, GNU 92 ms
(at `23d230a`: 1,177 ms and 107 ms). **CI's full gate on `8352eb5`** (run
38717322155): **3,204 passed, 0 failed**, the two existing ignores.

## Task branch: performance round on undo, sorting and the call path

Measured with `callgrind` instruction counts on the locked workloads: each
editor runs a case at two sizes and the difference per unit is the body's
cost, startup cancelled (`--divisor` large enough that startup noise of a
few hundred thousand instructions stays below 1%). Diagnostic only; timing
pilots come at the merge. Every change below is GNU-compared or keeps the
recorded behavior, and most exposed a behavior difference that is now fixed.

- `1ca6ab0` **`*Messages*` folds repeated lines** as `message_dolog` does
  ("Undo [1200 times]"), replaces a line matched through "...", trims to
  `message-log-max` through the rope's line index, and restores point and
  the restriction as GNU's markers would. Emaxx logged every "Undo", filled
  the log to 1,000 lines and re-copied the buffer on every message.
  `(message "%s" "")` now logs an empty line, as Fmessage does.
- `fe2bc37` the batch error backtrace was copied (every frame's arguments)
  at every frame an error unwound through, then discarded; the depth is
  compared first now.
- `5714ffd` a buffer switch reloaded the eight forwarded evaluator cells by
  name; it now does so only when either buffer has a local binding of one,
  and the paths that drop a current buffer's locals refresh them
  themselves (GNU-compared test with a buffer-local `max-lisp-eval-depth`).
- `37cc968` `format` read its format string's properties through a full
  decode per conversion; `nthcdr`/`nth` built a bignum and a SipHash table
  per call (GNU walks counts up to 127 directly; so does Emaxx now).
- `2c7c55f` `compare-buffer-substrings` copied both ranges through rope
  slices; it reads them in place now, and signals `args-out-of-range` for
  bounds outside the accessible region where Emaxx clamped them.
- `45929f3` `eval` answers symbols and immediates inline below the top
  level, and special forms are found through the subr's cached facts
  instead of their names; `1fccdb1` the GC check tests the active runtime's
  trigger before the register-spilling trampoline; `fd81bb1` argument lists
  of up to two elements are counted inline (FOR_EACH_TAIL's quit check
  kept) and a function cell that is no symbol is read inline; `b9f7d06`
  backtrace frame details (an interpreted call's recorded parameters) are
  pooled instead of allocated twice per call.
- `63eb047` `mapcar`/`mapc`/`mapcan` read each element when its call comes
  (GNU's `mapcar1`: a function that changes the sequence sees the change, a
  shortened list ends the mapping, a char-table signals `listp`; Emaxx
  copied the sequence first). GNU-compared test.
- `c100767` the bytecode fast loop dispatches through a 256-entry class
  table; `37a0c4f` inlines it into the frame loop, so a bytecode call and
  return no longer leave and re-enter it through a prologue twice
  (bytecode calls 3.18 → 2.87, bytecode-to-native 4.17 → 3.97, undo
  by 7.7%); `898a088` reads a callee's name from its decoded symbol
  (bytecode calls 2.83).
- `77a5be0` native funcall tests the GC trigger before the trampoline too
  (native execution 1.48 → 1.39). Rejected in the same commit: a per-subr
  table of `invoke_subr`'s fast cases in place of its name comparisons made
  native execution 4% slower; its patch is
  `docs/handover/2026-10-15-performance-round/rejected-native-subr-kind-table.patch`.
- Byte-code verification (`src/lisp/bytecode/verify.rs`): GNU trusts the
  compiler (BYTE_CODE_SAFE is off); Emaxx checked stack bounds, constant
  indices and code bounds at every op to stay memory-safe on any closure.
  A function's code is now verified once: from the entry and every branch,
  handler and switch target, each reachable instruction decodes within the
  code, reads a constant within the vector, finds its operands, stays
  within the declared depth and is reached with one operand count.
  Verified code runs in `run_verified`, with the stack top in a local
  pointer, one jump table on the opcode and no bounds checks; other code
  runs the checked loop as before. A result belongs to the code string's
  current bytes: any exclusive borrow of the string retires it (a byte in
  the string cell's padding), its collection drops it, and the fast loop
  is entered only where the verified operand count is the stack's, so a
  mis-modeled slow op costs the fast loop, never safety. Bcall reads a
  byte-code callee's fields directly (setup_frame's reads) instead of
  revalidating them through `closure_program`. Bytecode calls 2.83 →
  2.29, bytecode-to-native 3.97 → 3.39. Undo's byte-code spends about 1%
  more of undo: its code leaves the fast loop every few ops, and each exit
  and entry now crosses a call (inlining `run_verified` into the frame
  loop was worse for both workloads; skipping the loop when its first op
  is slow helped undo by 0.3% and cost bytecode calls 1.7%). GNU-compared
  test of changed, shared and collected code; unit tests of the verifier
  and of result retirement.
- Bcall calls a native-compiled callee directly (funcall_general's
  funcall_subr) instead of through `funcall_body` and
  `call_function_value_inner`; the native trampoline's jump buffer is no
  longer zeroed before `_setjmp` writes it, and the handler reconciliation
  before and after each native call returns at once when the lists agree.
  Bytecode-to-native 3.39 → 2.78, native execution 1.39 → 1.31, undo 2.80
  → 2.79; bytecode calls 2.29 → 2.32 (the callee classification's enum).
  GNU-compared test of Bcall into native functions through the live cell,
  arity errors, aliases and redefinition. Rejected: moving the full
  dispatch into `run_op` so the verified loop runs slow ops, calls and
  returns without leaving it. The new functions' calls and LLVM's lost
  inlining cost more than the exits they saved (bytecode calls 1,070 →
  1,145 instructions per iteration, bytecode-to-native 1,080 → 1,126,
  undo +1.5%, also with the callee helpers forced inline); its patch is
  `docs/handover/2026-10-15-performance-round/rejected-run-op-dispatch.patch`.
- Ffuncall calls a symbol whose live cell is a subr or a native function
  directly (funcall_general's first test) instead of through
  `call_function_body`, `call_function_value_inner` and
  `resolve_symbol_call`; `mapcar` reserves its result array at the
  sequence's length (mapcar1's SAFE_ALLOCA_LISP) instead of growing it;
  `length` counts a list's tails in place instead of copying its elements
  into a vector. mapcar 2.68 → 2.30; the other workloads moved within 1%
  (explicit GC, which none of this touches, moved by as much). GNU-compared
  test of the values and errors.
- The arithmetic and comparison subrs (`+ - * = < > <= >= 1+ 1-`) answer
  two fixnums (one for `1+`/`1-`) before their general paths, as data.c's
  FIXNUMP fast paths do; a backtrace frame that owns nothing (no detail,
  no argument vector of its own) is popped by moving the length back,
  inline for a call's own frame, instead of through `Vec::truncate` and
  the frame's drop glue; lexical lookup compares the binding's car with
  the symbol's word (Fassq's EQ) instead of decoding each car. Instruction
  ratios: interpreted lexical 1.77 → 1.57, dynamic 1.90 → 1.74, calls
  1.82 → 1.63, cons allocation 1.98 → 1.78, list traversal 1.92 → 1.72,
  mapcar 2.30 → 2.08, interpreted-to-native 2.27 → 2.00,
  native-to-interpreted 1.99 → 1.81, native-to-bytecode 2.17 → 1.99,
  native execution 1.31 → 1.23, sort 2.66 → 2.50, bindat 1.67 → 1.54,
  undo 2.81 → 2.76, bytecode-to-native 2.80 → 2.73, bytecode calls 2.33
  → 2.28. Rejected without a patch (no change in any count): replacing
  `kind()` matches in the evaluator's hot paths with single tag tests; the
  instructions attributed to `kind()` were already those tests. GNU-compared
  test of the fast paths' overflow, float, marker and error cases.
- `call_with_facts` is an inline arity check and the subr's function
  pointer, with its general path (special forms, file name handlers,
  unknown names) out of line; a subr's `NameFacts` are read by reference
  from their cache instead of copied per call; `resolve_symbol_call` is
  inlined; the fixnum fast paths build their result inline. Interpreted
  lexical 1.57 → 1.48, dynamic 1.74 → 1.61, calls 1.63 → 1.54, cons
  allocation 1.78 → 1.65, list traversal 1.72 → 1.59, mapcar 2.08 → 1.81.
  On cons allocation the remaining gap is the evaluator itself: GNU's
  whole `eval_sub` costs 2,770 instructions a unit, Emaxx's `eval_call`,
  `eval_sub` and `finish_eval_call` 6,355; lexical lookup is already
  cheaper than GNU's `Fassq`.
- A form calling an interpreted function enters `funcall_interpreted_lambda`
  straight from `finish_eval_call` with its frame already recorded, as
  eval_sub calls funcall_lambda, instead of going through
  `call_function_value_named`, `call_function_body` and the general
  funcall dispatch; `eval_call` and `finish_eval_call` are inlined into
  `eval_sub`. Interpreted calls 1.53 → 1.45, sort 2.50 → 2.37,
  interpreted lexical 1.48 → 1.44. GNU-compared test of the caller and
  callee backtrace frames, `mapbacktrace`, and the arity and argument
  errors through interpreted calls.
- Undo and the byte-code slow path. The buffer keeps its character and
  byte counts beside the rope (GNU's Z and Z_BYTE): ropey stores a text of
  up to about a kilobyte as a single leaf and recounted its characters,
  line breaks and UTF-16 units on every length query and bounds check,
  about 350 instructions each and 642 times per undo iteration; deletion
  takes the byte count from the text it already copies for undo. The batch
  error backtrace is not copied for an error an active `condition-case`
  catches (keyboard.c's toplevel reports through an outermost
  `handler-bind`, which a catching `condition-case` pre-empts). A C
  variable read tests a KBOARD bit set with its C slot before comparing
  the symbol with the 14 keyboard variables. Bvarbind and the native
  specbind bind the constant's own symbol instead of interning its name
  again; the message log stores `cache-long-scans` through the cached
  symbol. Bcall calls a subr found in the live function cell directly
  (funcall_subr), as Ffuncall already did, and the verified loop answers
  Bvarref of a plain bound symbol and Bvarset of a learned plain store
  (bytecode.c's SYMBOL_PLAINVAL cases) without leaving. Undo 2.69 → 2.45
  (3.37 to 3.06 million instructions per unit). GNU-compared test of the
  kept counts through inserts, deletions and erasure, a let-bound
  keyboard variable, an uninterned Bvarbind and a caught error.
- The verified loop's opcode match names every byte instead of ending in
  a default arm, so rustc proves it covers all 256 and LLVM's jump table
  needs one compare fewer per op (it still range-checks: it keeps the 64
  constant opcodes as one range outside the table). Undo 2.45 → 2.37,
  bytecode calls 2.25 → 2.19, bytecode-to-native 2.72 → 2.68.
- `NILP`, `CONSP` and `LISTP` test the word (nil is the word zero) instead
  of decoding the whole kind, which compiled to a jump table or two tests
  at many sites; `eval_sub` and `eval` read their entry and depth tests
  through one borrow of the interpreter state instead of one per field
  (each a parked-shell test); `eval_call` takes the form's head from
  `eval_sub` instead of decoding the form again; Ffuncall (and so `apply`,
  `sort`, `mapcar`) calls an interpreted closure, given as a value or
  found in a symbol's live cell, through `funcall_interpreted_lambda` at
  once, as funcall_general calls funcall_lambda; `let` finds a local
  special declaration by the symbol's word (Flet's Fmemq) instead of
  comparing names; `eval`, `set_backtrace_args` and the vector header's
  tag decoding are always inlined. Interpreted lexical 1.44 → 1.35, calls
  1.45 → 1.38, cons allocation 1.61 → 1.52, sort 2.31 → 2.21,
  native-to-interpreted 1.71 → 1.66, bytecode-to-native 2.68 → 2.64.
  GNU-compared tests of closures called through funcall, apply, sort and
  mapcar (values, optional and rest parameters, an alias, arity errors
  and the recorded frames) and of local special declarations by symbol.
  Rejected: moving the hot special forms out of line so that
  `eval_native_form` is a thin dispatcher (its prologue and epilogue cost
  about 20 instructions a form); each form's own frame cost more
  (lexical +1.8%, cons allocation +2.5%); patch
  `docs/handover/2026-10-15-performance-round/rejected-special-forms-out-of-line.patch`.
  Also rejected: decoding a symbol word with one `< 128` test before the
  nil/t/unbound compares, and `SYMBOLP` as a tag test (lexical +1.3%,
  cons allocation +1.2%, no case better); patch
  `docs/handover/2026-10-15-performance-round/rejected-symbol-tag-tests.patch`.
- `compare-buffer-substrings` compares two ranges of ASCII buffers that
  each lie in one rope chunk as bytes, with no case folding active; a
  character iterator over the rope cost about 67 instructions a
  character there. Sort 2.21 → 2.18. GNU-compared test of short ASCII
  ranges: equal, differing, prefix either way, empty, reversed, another
  buffer, case kept. Measured and dropped: one borrow of the state in
  `begin_funcall` (no case moved).
- Ffuncall's frame (unnamed and named calls, and native code's Ffuncall)
  is written and popped in place, as eval_sub's is, instead of through a
  scope helper with a closure and a drop guard, which cost about 70
  instructions a call. The guard also popped the frame when a caught Rust
  panic unwound through it; no production path continues after a panic
  (the batch driver reports it and ends, the module ABI aborts), and
  eval_sub never had the guard, so the scoped form is kept only for the
  tests that unwind through a borrowed frame. A form's head and `eval`'s
  argument fast path find a symbol by its tag and one bound
  (`Value::symbol_cell`) instead of the whole kind dispatch. mapcar 1.85
  → 1.54, sort 2.18 → 2.14, bindat 1.35 → 1.33, native-to-interpreted
  1.64 → 1.62, native-to-bytecode 1.82 → 1.80, native execution 1.21 →
  1.20, interpreted calls 1.38 → 1.36. Measured and dropped: leaving a
  generic byte op's operands on the byte-code stack for its subr instead
  of rooting a copy (undo moved 0.2%).
- `Fcons`' storage takes a new block in an out-of-line function reached by
  a tail call, so the allocation path keeps no frame (it saved four
  registers and a 184-byte frame on every cons for the block refill's
  sake), sets the cell's serial and bits without a bounds check, and no
  longer decrements the free count (GNU's `total_free_conses` is the last
  sweep's count; Fcons leaves it alone). `Value::cons` is 37 instructions
  where it was 51 (GNU's Fcons 27). mapcar 1.54 → 1.45, list traversal
  1.47 → 1.45, cons allocation 1.51 → 1.50, sort 2.14 → 2.13. Not done:
  moving the per-cell serial to the sweep (four instructions a cons)
  needs a block generation in the serial so that a released and
  reallocated block cannot revive a weak reference; about 1% of mapcar.
- Bcall and Breturn switch byte-code frames inside the verified loop, as
  exec_byte_code's `goto setup_frame' and Breturn do, instead of leaving
  it for the full dispatch and re-entering it twice per call. A call to a
  byte-code closure takes the in-loop frame entry when nothing else
  happens on the way in (`plain_bytecode_callee`: no quit, the depth
  within `max-lisp-eval-depth`, no pending termination, edebug not
  loaded, the callee not `backtrace-eval`, no profiling, maybe_gc would
  not collect, no `&rest` list to build); every other callee (subrs,
  native functions, interpreted closures, arity errors) runs the full
  Bcall sequence as an out-of-line function (`bytecode_call`, shared
  with the slow dispatch) without leaving the loop, which re-reads the
  code string and its verification afterwards, since the callee ran
  Lisp. A return whose frame left no unwind entries pops it in the loop.
  The loop receives the stack's base and top and hands the top back, so
  each frame switch and loop entry costs no extra parked-shell test.
  Bcall's nesting limit now signals bytecode.c's plain `(error "Lisp
  nesting exceeds `max-lisp-eval-depth'")`; Emaxx signalled Ffuncall's
  `excessive-lisp-nesting`, which GNU's byte code does not. Bytecode
  calls 2.19 → 1.87, bytecode-to-native 2.66 → 2.63; native-to-bytecode,
  undo, sort and bindat moved within their allocator noise (the VM code
  itself got a little cheaper in undo and bindat). Measured on the way
  and not kept: the switch alone between runs of the loop (no gain, the
  two re-entries stayed), and a version that re-tested the shell in each
  switch arm (27 instructions a call).
- The verifier gave Bswitch's targets one operand too many: Bswitch pops
  both the jump table and the value, so its targets are reached two below
  the table's push, not one. Every function with a jump table (`pcase`, a
  `cond` on `eq`) failed verification on the disagreeing paths and ran
  in the unverified loop; `primitive-undo` was one. The targets now get the
  right depth (checked against GNU's disassembly of `primitive-undo`; a
  test runs a jump-table function through the verified loop).
- The evaluator tests conses, nil, t and symbols by the word's tag
  (`Value::cons_ref`, `Value::builtin_ref`, `symbol_cell`, `is_symbol`,
  `car`/`cdr`) where it built the whole kind classification: Fassq over
  the lexical environment, the argument count and walk, the function
  cell's subr test, the list iterators, `setq`, `let`'s binding parts and
  the lambda list. The kind dispatches cost 685 instructions an iteration
  of interpreted calls and now cost 62. `eval_sub` reads its entry tests
  (depth, pending termination, quit) through one borrow of the state and
  sends anything unusual to the general entry; a form then takes
  maybe_gc, the depth step and its call path with the cell in hand. A
  variable reference in `eval` reads a lexical binding or a plain value
  cell from the state it already holds (`lookup_symbol_plain`) and goes
  past the lexical environment once for any other symbol
  (`lookup_symbol_global`). `capture_current_backtrace_context` is always
  inlined (out of line it cost sort 12 million instructions a unit).
  Unbinding a variable that was void no longer hashes its name into every
  terminal's keyboard-variable table: only the KBOARD variables have
  terminal values. `Value::symbol` looks a name up before copying it, and
  the keymap code compares with the cached `keymap` symbol (GNU's
  `EQ (head, Qkeymap)`) instead of interning the name on every test, which
  had cost undo 29,000 instructions a unit in the interned-name table.
  Ratios against `29e7780`: interpreted calls 1.32 → 1.25, lexical 1.35
  → 1.26, dynamic 1.47 → 1.37, sort 2.13 → 2.09, interpreted-to-native
  1.73 → 1.64, native-to-interpreted 1.60 → 1.55, native-to-bytecode
  1.78 → 1.73, cons allocation 1.50 → 1.41, list traversal 1.45 → 1.37,
  bindat 1.32 → 1.26, undo 2.37 → 2.35, bytecode calls 1.88 → 1.87; the
  others within 0.4%.
- Bcall into a native-compiled function, or into a subr that evaluates
  its arguments, runs from the verified loop when nothing else happens on
  the way in (`plain_bcall_callee`: a byte-code callee's entry tests;
  maybe_gc would not collect; no error trace is asked for): Ffuncall's
  depth step and frame record, the call, the frame's settling and pop,
  and the value replacing the function and its arguments (`call_plain`).
  The full Bcall arm (`bytecode_call`) is left for every other callee.
- Every op the verified loop has no arm for (variables in their full
  forms, bindings and unwinding, handlers, buffer and primitive ops), and
  each fast op whose operands its arm does not take, runs its full arm in
  place (`exec_op`, shared with the dispatch loop) where it left the loop
  for the dispatch and entered it again: exec_byte_code runs every op in
  one loop. The loop then re-reads the frame's code and verification, as
  after a call. An op that is a whole byte comes from a table built once
  from the decoder. Bsymbolp, Bstringp, Blistp, Bnumberp, Bintegerp,
  Bsetcar, Bsetcdr and Bmemq have arms in the loop: Bmemq walks up to 64
  elements for a fixnum or a symbol, whose words are eq exactly when they
  are equal unless an element might be a symbol with position, and takes
  FOR_EACH_TAIL's first quit check (nothing in the walk can set
  `quit-flag`, so one test stands for all); any other operand runs Fmemq.
  In undo these ops left the loop about 1,160 times a unit, at about 190
  instructions each before the op's own work. `prim` (an op's named
  primitive) finds its dispatch facts in a process-wide open-addressed
  table by the name literal's address; a thread-local hash map of vectors
  cost about 50 instructions a call.
- A buffer switch looked each of the eight forwarded evaluator variables
  (`quit-flag`, `load-path`, `max-lisp-eval-depth`...) up in both buffers'
  local bindings, sixteen hash probes a switch; each buffer's bindings now
  count those variables as they are bound and removed
  (`LocalCells::has_forwarded_eval_binding`). In undo this cost 71,000
  instructions a unit and now costs about 9,000.
  Ratios against `0b0b6dc`: undo 2.35 → 2.11, bytecode-to-native 2.64 →
  2.37, native-to-bytecode 1.73, interpreted-to-native 1.64 → 1.63; the
  others within about 1% (interpreted dynamic 1.37 → 1.39 is `eval_sub`
  layout, 12 instructions an iteration, and allocator noise). Measured and
  left: `prim` still registers its arguments as precise roots (about 45
  instructions a call) though they lie in the caller's frame, which the
  conservative scan reads; a plain subr call still costs about 196
  instructions in `call_plain` over a dozen steps of 3 to 9 each; undo's
  remaining slow ops are buffer primitives (`point`, `goto-char`,
  `point-min`, `point-max`) at about 200 instructions each where GNU's arms
  are 30 to 50.
- The cons and float sweeps read a block's mark bitmap a word (64 cells)
  at a time, as alloc.c's sweep_conses reads its mark words: the live
  cells are counted by population count, the allocation and mark bits of
  the dead ones cleared word by word (a cons and a float have nothing to
  drop), and the free cells linked in address order as before, where each
  cell's bits were extracted and tested one by one. A sort unit's
  collection swept for 27.7 million instructions; sort 2.09 → 2.03,
  explicit GC 1.01 → 0.89 (below GNU's), mapcar, cons allocation and
  interpreted calls about 0.5% lower. Measured and not kept: a shortcut in
  `lookup_var_symbol` for plain cells, for the change-hook variables
  insertion reads (about 250 instructions a lookup, three an insertion):
  those variables are not plain cells, so it never applied.
- The buffers' local-binding table and each buffer's bindings hash their
  keys (a buffer id, a symbol's address) with the identity hasher (one
  multiply) instead of FNV, which ran eight rounds a key byte by byte;
  `case-fold-search`, read through them on every buffer comparison, cost
  sort about 9 million instructions a unit. Sort 2.03 → 2.01.
- `eval` calls a native-compiled function, or a byte-code function, under
  the frame it recorded for the form, as eval_sub calls a native subr
  (funcall_subr) or applies a byte-code function (apply_lambda): the frame
  takes the evaluated arguments and the callee runs at once
  (`eval_call_compiled`). It popped that frame, pushed another and went
  through the general funcall dispatch. Interpreted-to-native 1.63 →
  1.57, native-to-bytecode 1.73 → 1.69, native-to-interpreted 1.55 → 1.50
  (undo read 2.04 once, but nearly all of that was the allocator).
  Measured and not kept: classifying a resolved closure or native
  function by its header before `kind` in `eval_call`'s resolution loop
  (interpreted calls and lexical about 1.5% slower from `eval_sub`'s
  layout, the native cases no faster).
- Ffuncall's first test (`call_subr_directly`, funcall_general's SUBRP
  and CLOSUREP) reads the callee symbol and its function cell by tag and
  header where it built the whole kind classification twice, 22
  instructions a call. Mapcar 1.45 → 1.43. Measured on the way: the same
  test written with `bool::then` and a closure compiled the subr call out
  of line, 30 instructions a call worse. Noted, not changed: mapcar's
  result buffer is zeroed with `memset` (16 instructions an element in
  callgrind, which counts each repetition of `rep stosb`), where GNU's
  `xzalloc` of a large array gets fresh zero pages from `calloc`; in time
  it is well under 1%.
- Value conversions asked only whether they apply (`as_integer().ok()`,
  `as_symbol().ok()`, `car().ok()`, `cdr().ok()`, about 240 sites) built a boxed
  error with a `String` for every miss and dropped it; they now call
  `integer_value`, `symbol_str`, `car_if_list` and `cdr_if_list`, which
  answer the same without building one. A syntax-table entry's matching
  character (`nil` in most entries) was one: undo spent about 25,000
  instructions a unit in these errors' allocation and release.
  Remaining on undo's error path: `handler-bind` dispatch clones the
  active handler list and builds the condition names as `Vec<String>` per
  signal, and conditions match by name where GNU compares symbols (an
  uninterned condition symbol with a registered name would match).
- `signal`'s walk of the handler stack worked on a deep copy of it, made
  on every signal while a `handler-bind` was active (ert's runner keeps
  one): each `handler-bind` entry's condition names (`Vec<String>`) and
  each `condition-case` entry's clause heads were copied. The lists are
  shared (`Rc<[String]>`, `Rc<[Value]>`) now, so the copy counts a
  reference each. Undo about 34,000 instructions a unit lower (2.06 →
  2.03 on one pair of builds).
- `Interpreter`'s `Deref` to its state, which every field access of the
  shell goes through, tested whether the shell was parked (its state
  handed to a continuation) and panicked if so; the compiler cannot carry
  the answer across a call, so the test ran at nearly every access, about
  4% of an interpreted call. The state is read unchecked now, the test
  kept as a debug assertion (the gate profile runs with it): a shell is
  parked only while the thread and continuation drivers and
  `suspend_payload` have its payload away, and nothing reaches the shell
  before the payload is back. Interpreted calls 1.26 → 1.20, lexical
  1.27 → 1.21, dynamic 1.38 → 1.32, mapcar 1.43 → 1.37, sort 2.01 →
  1.95, undo 2.03 → 1.97, cons allocation 1.42 → 1.36, list traversal
  1.38 → 1.32, interpreted-to-native 1.56 → 1.51, native-to-interpreted
  1.51 → 1.44, native-to-bytecode 1.69 → 1.63, bindat 1.26 → 1.20,
  native execution 1.21 → 1.18. The native runtime's shell has the same
  check; its default shells are empty by design, so it stays checked.
  Measured and not kept: a per-closure cache of the decoded byte-code
  object and its verification for the in-loop frame entry (keyed by the
  closure's address, the collection epoch and its first four slots)
  saved 3 of the entry's 303 instructions, the verifier's direct-mapped
  cache being about as cheap; a test now covers a byte-code caller whose
  callee's code string changes between calls.
- An interpreted call's parameters (funcall_lambda's bindings, which
  `backtrace--locals` and the debugger's frame context report) were kept
  in a detail box on its frame, taken from a pool and handed back when
  the frame was popped, so every such frame took the slow pop: about 130
  instructions a call. They are kept on a stack beside the frames now
  (`frame_locals`, each entry naming its frame), and the call removes its
  entries when it returns, its error settled; its frame owns nothing and
  leaves as an ordinary call's does. Interpreted calls 1.20 → 1.17, sort
  1.95 → 1.93. Measured on the way: trimming the entries in every frame
  pop instead gave each pop a test and gained nothing.
- The native runtime's `Deref` to its shared state (heap, thread cell,
  link table), reached several times a native call, tested for a parked
  shell the same way; every runtime is built with the state and only a
  test moves it between shells, so the test is a debug assertion now.
  Bytecode-to-native 2.35 → 2.30, native-to-bytecode 1.63 → 1.61,
  interpreted-to-native 1.50 → 1.49, native execution 1.18 → 1.17.
- Undo's allocations, by `malloc`'s callers: a signal's
  `error-conditions` were copied into a vector of `String`s (now the
  symbols, read in place into an inline `SmallVec`; matching still
  compares names); `featurep` copied `features` into a vector; the
  condition symbol's name was formatted through `Display`, and its `car`
  probe built an error for a non-list datum; the message log's duplicate
  check took a second substring only for its byte length, and the
  `message-log-max` probe built an error for `t`; buffer text was turned
  into strings through ropey's `Display` (`to_string`) instead of its
  chunk copy (`String::from`). Undo 1.99 → 1.93 (2,485,000 → 2,417,000
  instructions a unit, with the allocator's own functions about even).
  Measured and not kept: lending the byte-code frame stack of each VM
  activation from a per-thread pool cost about as much as the vector
  growth it saved, and bytecode calls 0.9% more.
- The native execution guard (the OS-thread lock that serializes native
  activations) counted a thread's activations in a `RefCell` borrowed on
  every native call's entry and exit; the count is a constant-initialized
  thread-local `Cell` now, and the lock's cell is touched only when the
  count leaves or returns to zero. Bytecode-to-native 2.30 → 2.25,
  interpreted-to-native and native-to-interpreted about 0.6% lower.
- On Linux x86-64 the native call trampoline records its own escape
  point instead of calling glibc's `_setjmp` (about 38 instructions a
  call with the PLT and `__sigjmp_save`): the callee-saved registers, the
  stack pointer and the resume address, nine stores, plus a marker word
  at offset 64, where glibc's `_setjmp` always stores
  `__mask_was_saved = 0`. `jump_nonlocal` takes a buffer with the marker
  to `emaxx_native_longjmp`, which restores those registers and resumes
  the trampoline, and any other buffer (a handler's, filled by generated
  code's `_setjmp`) to `longjmp`. macOS keeps `_setjmp`. The saved
  stack and frame pointers are not mangled as glibc's are.
  Bytecode-to-native 2.25 → 2.17, interpreted-to-native 1.49 → 1.47.
- The native call's invocation record is written in place (it was built
  by value and copied whole, its 200-byte jump buffer included) and its
  eight register slots are filled by a fixed-count copy (the
  per-argument loop after zeroing them had become a `memcpy` call);
  `invoke_function`'s dynamic-binding path is out of line, so the
  ordinary path inlines into its caller. Bytecode-to-native 2.17 → 2.10,
  interpreted-to-native 1.47 → 1.46.
- The editing primitives read `inhibit-read-only`, `buffer-read-only`,
  `buffer-file-name`, `combine-after-change-calls` and the change hooks
  as C does, with find_symbol_value (`symbol_value_key`): the alias
  chain, then the buffer-local, dynamic or global value. They went
  through the evaluator's variable lookup, which first walks the
  caller's lexical environment, about 3,400 instructions an insertion in
  sort. That walk was also a difference from GNU: a lexical binding of
  one of these names (a lexical function's parameter) reached `insert`,
  where GNU's C code reads the dynamic value; a test covers it. Sort
  1.92 → 1.86, undo about 1.5% lower.
- Two more of undo's allocations: `push_undo_entry` made each entry's
  Lisp form (a deletion's is a new string) before checking that the
  cached `buffer-undo-list` view it would extend was current, and dropped
  it when the view was stale; it is made only for a view it extends now.
  `decode_bytes` and `encode_text`, between a Lisp string's bytes and
  Rust text, went a character at a time for ASCII too (about 44,000
  instructions a unit); ASCII is copied whole. Undo 1.92 → 1.88.
  Measured and not kept, a second time: the byte-code activation's frame
  and handler vectors lent from a pool in the interpreter state (no
  thread-local, kept only once allocated) saved 0.36% of undo, inside the
  noise, with bytecode calls even.
- `prim`, the VM's call of a primitive for a dedicated op (`Bnth`,
  `Bsubstring`, `Bconcat2`, ...), opened an explicit stack-root scope
  for its arguments (`with_lisp_stack_roots`: an `Rc` clone, a `RefCell`
  borrow and a slot, then the guard's drop, about 120 instructions),
  though they are a small array on the Rust stack, which the conservative
  scan reads as it reads a C function's locals; the explicit scopes
  predate that scan (the honesty audit's item (c)). The arguments are
  passed by value now, and `prim_rooted` keeps the scope for the two ops
  whose arguments are drained into a heap vector (`insert` and `concat`
  of N). Undo 1.88 → 1.86 (about 45,000 instructions a unit outside the
  allocator).
- `sort`'s arguments follow `Fsort`: only the two-argument call
  `(sort SEQ PREDICATE)` sorts in place unless `:in-place` asks; the
  keyword form sorts a copy. Emaxx sorted in place either way, so
  `(sort v :lessp #'<)` changed `v`. Two arguments are always the old
  call (a lone keyword is the predicate), another even count signals
  "Invalid argument list", and an unknown keyword "Invalid keyword
  argument" with the keyword (spliced when it is a list, as
  `signal_error` does); they were wrong-number-of-arguments. Found while
  checking the sort port below against GNU.
- `sort` runs GNU's Timsort (sort.c, CPython's listsort with powersort
  merging; `sequences/timsort.rs`). Rust's `sort_by` wanted a three-way
  answer, so a comparison called the predicate a second time whenever the
  first answered nil, and its merge order is not GNU's: the sort workload
  called its predicate 64,700 times a unit against GNU's 35,600. That was
  the comparison-schedule finding in
  `docs/runtime-representation-progress.md`. The predicate now sees GNU's
  calls in GNU's order: a trace of every call (5.7 MB over lists and
  vectors of 2 to 3,000 entries, `:key`, `:reverse`, copies, ascending,
  descending and interleaved runs that gallop, `value<`, signals at many
  points and the argument forms) is byte-identical to GNU's. A signal
  leaves a vector sorted in place as sort.c leaves its slots: once a merge
  has outgrown sort.c's 256-slot stack array (or from the start when keys
  were allocated, at 128 entries or more) the merge's temporary run is put
  back; before that the run is lost and stale copies stay, as GNU's
  abandoned stack array leaves them. A list is sorted in a copy and keeps
  its order. Oracle tests pin a short sort's calls, checksums of longer
  ones and a vector after a signal. Sort 1.97 → 1.36 at four and eight
  units (below).
- A buffer text of up to a kilobyte is one string with its counts
  (`BufferText`'s small store), and a rope past that, a string again
  under half a kilobyte. Ropey keeps such a text in a single leaf and
  scans the whole leaf for its counts on every bounds check, two or three
  times an insertion or deletion and once a chunk lookup: 133,000
  instructions a unit of undo, whose temporary buffers and message log
  are that small. `text_rope` builds a small text's rope for the regexp,
  syntax and coding scans and keeps it until the next change. A test
  takes a multibyte text through the string, the rope and back. Undo
  1.85 → 1.72 (about 149,000 instructions a unit outside the allocator),
  sort 1.36 → 1.355.
- Every byte-code op dispatches through one jump table. The verified
  loop's match covered all 256 opcodes, but some of its arms were range
  patterns (the constants, the calls, the short variable references),
  and rustc tests a range with comparisons ahead of the switch: LLVM's
  table stopped at 0o266, every op paid a range check first, and
  constants and calls went through further branches. Each byte is its
  own case now (clippy's range-pattern lint is allowed on the function,
  with the reason). Bytecode calls 1.864 → 1.812, bytecode-to-native
  2.109 → 2.056; undo's verified loop about 22,800 instructions a unit
  lower (its ratio moved with the allocator's state).
- A call of a lexical native function of fixed arity, with a count it
  accepts, goes from `native_comp::call_function` to the trampoline
  through one function, `invoke_fixed`: the runtime found once
  (`current_runtime`, the pointer `with_current_runtime` lends), the
  arity read from the function object (`descriptor` and `check_arity`
  for this case), the activation's install inline, and the call frame's
  pop inline on an ordinary return (`finish_call` stays out of line for
  the escape and error paths). Any other callee or count, or no runtime,
  takes the general path, which signals as before. Bytecode-to-native
  2.056 → 1.862 (797 → 723 instructions an iteration), interpreted-to-
  native 1.468 → 1.427. Lending a nested call the installed activation
  when its interpreter, environment and runtime are the same was
  measured and not kept: on the byte-code path the environments differ,
  and the comparisons cost 9 instructions a call (interpreted-to-native
  gained 10).
- Allocation and copying GNU does not do, found by listing each
  workload's calls into the allocator, `memset` and `memcpy` a unit:
  `function`'s test for a `(lambda ...)` form took `Value::car` of its
  argument, which for a symbol (`#'f`) built a wrong-type-argument error
  and dropped it, two allocations and two frees each time (its two tests
  of the body's first form did the same for a docstring); it tests the
  cons now. A native `catch` or `condition-case` boxed a new 304-byte
  handler record and freed it when popped; the runtime keeps up to 16
  popped records for the next pushes, as eval.c keeps popped handlers on
  `nextfree`. `RootedVec`'s pop cleared the vacated slot with a `memset`
  call, its size known only at run time; one slot is cleared by stores of
  its constant size. A new cons block zeroed every cell's weak-reference
  serial (about 11 KB); a serial is written when its cell is allocated
  and read only while it is, so a new block clears only its epoch and
  bitmaps, as Fcons clears only a new block's mark bits. `cond` copied
  each clause into a vector before evaluating it; it reads the clause in
  place, as Fcond does, which also reads the body after the test (a test
  that changed its clause's cdr ran the old body) and runs a dotted body
  as no form. `forward-line` did its arithmetic in bignums, four
  allocations a call; a nil or fixnum count moves by an isize, and the
  argument is checked as CHECK_INTEGER does (a float, symbol or marker
  signaled `number-or-marker-p`, or moved). A function alias chain kept
  the symbols it had seen in a vector to catch a cycle; GNU 30's `fset`
  refuses cycles and `indirect_function` follows the chain with no test,
  and this one keeps a second walker at half speed instead. `ash`,
  `logand`, `logior`, `logxor` and `lognot` computed in bignums; fixnums
  are i64s now (the bitwise operations keep a fixnum's top bits equal; a
  left shift that overflows takes the bignum path), and their arguments
  are checked as data.c checks them (`logand`, `logior` and `logxor`
  signaled `number-or-marker-p` for a float or for anything in first
  position, and `lognot`, `ash` and `logcount` took markers). Tests pin
  `cond`, `forward-line`, the bitwise boundaries and the argument errors
  against GNU. Native-to-bytecode 1.572 → 1.433, native-to-interpreted
  1.42 → 1.27, mapcar 1.353 → 1.324, sort 1.353 → 1.314, bindat 1.190
  → 1.150, undo about 1.73 → 1.67 to 1.70 (the allocator's state),
  interpreted calls 1.164 → 1.150, lexical 1.204 → 1.197.
- An evaluator call and a byte-code call each repeated work. For a
  subr or a function `eval_sub` resolved: `set_backtrace_args` replaced
  the frame's arguments by assignment, which calls the `FrameArgs`
  enum's drop glue out of line, though the frame `eval_sub` recorded
  holds borrowed words (it now writes over words that own nothing); the
  frame's function went through `Value::Symbol`'s test for the built-in
  nil and t, which a symbol with a function never is
  (`Value::symbol_of_cell`); and `call_with_facts` checked the arity
  `eval_call` had just checked against the same descriptor, the facts'
  arity authority (`call_with_facts_arity_checked`). The byte-code Call
  arm read its callee's function cell and `enter_closure_frame` resolved
  it again (`plain_bytecode_callee`, removed: the arm passes the closure
  and the entry runs `plain_bcall_entry`'s tests), and the loop tested
  the entered frame's verification at pc 0, which `verified_code` had
  just established (`FastExit::Entered` skips it). Interpreted calls
  1.150 → 1.131, lexical 1.197 → 1.186, cons allocation 1.354 → 1.322,
  list traversal 1.310 → 1.282, interpreted-to-native 1.437 → 1.398,
  bytecode calls 1.809 → 1.761; bytecode-to-native 1.863 → 1.872 (the
  loop's code around the call arm: 3 instructions an iteration more).
- The verified loop's dispatch and its frame switches. The head read
  the opcode, kept the op's offset in a second register, advanced the
  pc and computed the address of the stack's top before its jump, and
  arms that read the opcode again (a constant's index, a call's count)
  kept it in a third: about ten instructions an op, 14 for a stack-ref,
  where GNU's NEXT is three or four. The pc now stays at the op until
  its arm moves it by the op's length (or jumps), the stack pointer
  addresses the top operand as bytecode.c's `top` does, and each op with
  its operand in the opcode (Bconstant, Bcall0 to 5, Bvarref and Bvarset
  0 to 5, Bsetcar and Bsetcdr) has its own arm: the head is the byte's
  load, the table entry and the jump, and a stack-ref is 8. A call into
  byte code re-read and checked the callee's four slots (`for_call`) and
  looked up its verified code each time; `verify::entry_program` keeps
  a closure's program and verified code (64 entries by identity), valid
  while the closure's four slots hold the words they were read from and
  its code string the bytes verified. A call into byte code, a return
  into a byte-code caller and a plain native or subr call now go on in
  the loop instead of leaving it to enter again; the frame entry and
  Breturn's fast path are inline, as setup_frame and Breturn are in
  exec_byte_code, and the code string's bytes are taken again without
  counting a borrow. A call's few arguments are stored one by one, into
  the frame and into a native call's register slots, which are cleared
  first (an omitted optional argument reads nil, as funcall_subr pads
  it; a version that left them unwritten crashed building the image).
  Inlining the fixed-arity native call into the loop's plain call made
  bytecode-to-native worse (634.7 → 654.2 an iteration) and was
  reverted. Bytecode calls 1.761 → 1.236 (579 instructions an
  iteration, all in the loop), bytecode-to-native 1.872 → 1.633, sort
  1.314 → 1.289, bindat 1.150 → 1.113, native-to-bytecode 1.433 → 1.416,
  undo 1.67 to 1.70 → 1.649; the rest within 0.02 (interpreted dynamic
  1.29, calls 1.124, lexical 1.183).
- **Rust 1.99.1.** CI and the local builds moved to Rust 1.99.1, the
  current stable release (CI pinned 1.97.1; the rounds above were
  measured on 1.97.0). Its Clippy asked for `try_update` for the
  deprecated `fetch_update`, `as_chunks` for a constant chunk size, and
  an error closure passed to `map_err` by value; the generated
  native-subr table is byte for byte the same under its rustc and
  rustfmt. With the same source, 1.99.1 compiled three compares into
  calls to `bcmp` that 1.97 made inline: `invoke_subr` chose a direct
  path by comparing the subr's name with up to 36 names on each call
  from native code; the Bcall entry tested for `backtrace-eval` by name
  (a call for any 14-character callee name); and the entry cache
  compared a closure's four words as an array (17 instructions an
  iteration of bytecode calls in `bcmp`). Native-execution read 1.171 →
  1.255, native-to-bytecode 1.416 → 1.443 and bytecode calls 1.236 →
  1.293. The direct paths go by a kind read from a table by the subr's
  index in the ABI now (`DirectSubr`, built once from the same names,
  which stay inside the source the native fast-path audit reads; a test
  pins its 36 kinds to the table), `backtrace-eval` is compared as an
  array of 14 bytes, and the four words one by one; no `bcmp` or
  `memcmp` call is left in the byte-code loop or `invoke_subr`. Against
  1.97.0 the 1.99.1 build then reads native-execution 1.144,
  native-to-bytecode 1.425 and bytecode calls 1.251 (the loop's code: 7
  instructions an iteration more), bindat 1.113 → 1.126, interpreted
  calls 1.124 → 1.136, bytecode-to-native 1.633 → 1.640, lexical 1.183 →
  1.174, explicit-gc 0.887 → 0.876, native-to-interpreted 1.267 → 1.254;
  the rest within 0.005.
- An evaluator form whose head symbol's function cell holds a C
  primitive takes eval_sub's SUBRP branch directly (`eval_fixed_subr`
  and `eval_special_form`, from `eval_cons`). `eval_call` resolves a
  head through a loop that also handles aliases, autoloads, macros and
  lambdas; it wrapped a subr in a `FunctionResolution`, decoded the
  cell's object a second time to classify it (a special form, which the
  first test leaves out, only there), walked the arguments with branches
  for MANY subrs and lambdas at each one, and matched the resolution
  again to dispatch. For a subr of at most eight fixed arguments the
  frame is recorded, the arguments counted and checked against both
  bounds, the subr's maximum positions evaluated as eval_sub does (Fcar
  before and Fcdr after each, so a position past the list's end
  evaluates nil and one the list grew to is evaluated: a first version
  that evaluated only the counted arguments was wrong, GNU answers "b"
  for a `substring` form grown that way, and
  `eval_call_fixed_subrs_read_cdr_after_evaluation_and_fill_optional_positions`
  pins it), and the subr called through its facts; for a special form
  with a native body the frame is recorded, the minimum checked and the
  body run. The choice reads the subr's raw MAX_ARGS word rather than
  building a `NativeMaxArgs` from it (7 instructions a form: cons
  allocation 5,909 → 5,792 an iteration). A list form of cons allocation
  costs `eval_sub` about 233 instructions now against 302 (GNU's
  `eval_sub` 173, with `list_length` 53 and its `Fassq` lexical lookups
  apart). Measured and not kept: the same direct path for a head holding
  a closure or a native function cost the subr loops about 17
  instructions a form, inline or out of line (cons allocation 5,909 →
  6,290 or 6,181 an iteration), and gained interpreted calls nothing
  (3,607 → 3,623 or 3,627). Cons allocation 1.322 → 1.111, list
  traversal 1.283 → 1.122, interpreted dynamic 1.289 → 1.161, sort 1.291
  → 1.169, lexical 1.174 → 1.071, interpreted-to-native 1.403 → 1.325,
  bindat 1.126 → 1.050, native-to-bytecode 1.425 → 1.362, interpreted
  calls 1.136 → 1.073, native-to-interpreted 1.254 → 1.201, undo 1.646 →
  1.622; the rest within 0.005.
- **Small blocks from per-thread lists.** On Linux the host allocator
  takes a block of at most 256 bytes (aligned to at most 16) from libc in
  a class of 16 bytes and keeps a freed one on its thread's list for the
  class, up to 64 blocks, for the thread's next request in the class; a
  reallocation that stays in its class keeps the block, and the lists go
  back to libc when their thread exits (a destructor arranged before the
  thread's first block is kept; a block freed while it registers, or
  after it ran, goes to libc). Taking a block is 17 instructions and
  keeping one 16, where glibc's `malloc` and `free` took about 160 a pair
  in undo (its per-thread cache holds seven blocks a size, and its `free`
  goes to the arena past that): undo's 1,340 allocations a unit cost
  about 217,000 instructions in the allocator and 85,000 now, nine in ten
  served from the lists. Each entry point is a function of its own, as
  `malloc` is: inlined, the allocator went into every cold allocating
  path of a hot function, and the fixnum `1+` kept two more registers for
  its bignum case (mapcar 1.293 → 1.316). With it, `RootedVec` skips the
  registry test on a push within its capacity and takes a first buffer
  zeroed from the allocator instead of clearing it, and the list walks of
  `visit_list_elements` and of `mapcar`, `mapc` and `mapcan` test the cons
  by its word. Undo 1.622
  → 1.470, mapcar 1.319 → 1.287, bindat 1.050 → 1.022, sort 1.169 →
  1.155, interpreted dynamic 1.161 → 1.150, interpreted-to-native 1.325 →
  1.314; native-to-interpreted 1.201 → 1.211 (`eval_sub`, with no change
  on its path); the rest within 0.005.
- **No locked exchange on a native call; string data compacted only
  when an entry died.** `with_active_call` swapped the process-wide
  native heap pointer, a locked `xchg` on x86 (one instruction, about 20
  cycles) on every native call; every store to that pointer is made
  under the native execution lock, which the call holds from its guard,
  so a load and a store exchange it. An instruction-level scan of all
  sixteen workloads for locked instructions found it once a native call
  (bytecode-to-native, interpreted-to-native, native-to-interpreted,
  native-to-bytecode) and otherwise only `mem_find`'s registry mutex
  (about 5,000 a collection) and ropey's `Arc` counts in sort, both
  under 1% of their time. Bytecode-to-native wall time at 4,000,000
  iterations 0.222 → 0.200 s (GNU 0.109). Each collection walked every
  small string data block twice, the image's strings' data included:
  `pinned` read each entry's owner for a borrow, then the compaction read
  each owner again for its size. Without a dead entry the compaction
  moves nothing (each live entry is copied to where it is), so it runs
  only when an entry was retired since the last one or a borrow held a
  block back, and it tests each block for borrows as its walk reaches
  it, so the owners are in cache when their sizes are read; the held and
  moving chains come out as before. An explicit collection 57.9 → 55.4
  million instructions, L1 data misses 736,000 → 588,000, last-level
  8,600 → 2,100 (GNU's: 66.2 million, 297,000 and none); an otherwise
  empty collection 15.1 → 11.5 ms (GNU 9.8); explicit-gc wall time
  0.145 → 0.133 s (GNU 0.107). Explicit-gc 0.874 → 0.834, the rest within
  their noise (undo read 1.486, `mem_find` 340,000 instructions more in
  the conservative scan's collection, and bindat 1.045: it reads 1.022 to
  1.048 on one build).
- **The byte-code dispatch copied into each arm.** The verified loop's
  opcode match is one jump table at the loop's head (5 indirect jumps in
  `run_verified`, against 125 in GNU's `exec_byte_code`, whose
  computed-goto dispatch ends every op with its own). LLVM copies such a
  dispatch into its predecessors (tail duplication), but since LLVM 18
  only for a block with at most 16 predecessors and 16 successors; this
  one has hundreds. `.cargo/config.toml` raises both limits
  (`-tail-dup-pred-size`, `-tail-dup-succ-size`, 1024): six functions
  change, `run_verified` to 144 indirect jumps (3.8 KB more code), the
  others `direct_assq_family`, `values_equal_recursive`,
  `find_operation_coding_system_value`, `values_eq_plain` and
  `values_eql` by a few dozen bytes; instruction counts within 1% on all
  sixteen. Wall time (five rounds of five samples, the minimum):
  bytecode-to-native 1.95 → 1.68 of GNU's, consistently in three runs;
  the rest moved within ±0.03 except explicit-gc 1.12 → 1.24 and
  interpreted dynamic 1.23 → 1.16, though neither's code changed (the
  collector, `eval_sub` and the native call path are the same bytes):
  moving the code after the loop moves wall time by up to about 10% on
  this host, so a wall-time change without a mechanism behind it is
  layout. The options are LLVM's own (`cl::opt`), not rustc's; a
  toolchain update that drops them fails the build at once, and an
  explicit `RUSTFLAGS` replaces them.
- **A case table's signature checked by its few non-table nodes.** A
  regexp search validates its cached case classes with
  `CaseTableSignature::matches_current`, and when no char-table store
  happened since the last full match, `CharTableChainSignature::matches`
  still walked the whole captured node array to skip the table nodes: a
  case table's chain is hundreds of sub-char-table nodes of about 50
  bytes each, twice (down and up), more than the L1 data cache holds,
  so each check missed on most of it (undo: 3,400 L1 misses a unit for
  2,500 instructions, 15% of the unit's misses). The capture now keeps
  the indices of the nodes that are not tables, in capture order, and the
  fast path reads only those (the debug assertion still compares it with
  the full match). Undo wall time 0.179 → 0.174 s (GNU 0.107);
  instruction counts unchanged. Undo also takes 74,000 L1 instruction-cache misses a unit
  against GNU's 21,000: the byte-code VM's functions alone (`run_verified`
  7,900 a unit, `exec_op`, `run`, `run_frames`, `call_plain`,
  `closure_program`) take about 16,000 against `exec_byte_code`'s 1,700,
  and the rest is spread over `eval_sub` and dozens of large primitive
  dispatchers. At about a dozen cycles each that is a large share of
  undo's time; code layout (hot and cold blocks apart, or
  profile-guided optimization, which would also order the functions and
  place the blocks by their counts) is the lever, and PGO is a build
  change (instrumented build, `llvm-profdata` from `llvm-tools`, a
  profile kept with the source and renewed as the code changes).
- **A count of live string guards spares the collector two walks.**
  Each collection read every string cell's borrow count before marking
  (`borrowed_string_roots`, to root a borrowed string and reject a
  mutable borrow) and, before compacting, every data entry's owner for a
  borrow (`pinned`). The string guards (`borrow`, `borrow_mut` and a
  construction's) now count themselves on their thread as they are made
  and dropped; the collector scans only its own thread's stack and Lisp
  threads switch stacks on it (a parked one's guards counted too), so at
  zero no cell is borrowed and both walks are skipped. Checked builds
  still walk every cell and assert that the count was right. Two
  instructions a guard (sort, the heaviest borrower, takes 30,000 to
  60,000 guards a unit, out of 243 million instructions). An explicit collection: L1 misses 588,000 →
  529,000 and the sweep's 603,000 instructions fewer (marking read
  1.13 million more on that build, the conservative scan finding another
  graph, as `mem_find` did in undo above); explicit-gc wall time 0.119 →
  0.110 s (GNU 0.103), 1.15 → 1.06 of GNU's.
- **The vector sweep's buffers from `all_buffers`.** Before reclaiming
  vector storage the sweep walks each buffer's marker chain (alloc.c's
  `sweep_buffers`), and it found the buffers by reading every vector
  header in every block, the image's included. A buffer's header is now
  listed when it is allocated (`BufferRef::new` and `for_restore`, image
  restoration included, both through `VectorlikeRef::allocate`) and
  unlisted when `cleanup_vector` reclaims it, as GNU's `all_buffers`, and
  the pass follows the list; checked builds still walk every header and
  assert that the list matches it. An explicit collection 55.7 → 54.7
  million instructions, L1 misses 529,000 → 502,000; wall time within
  noise (0.115 and 0.117 s).
- **Byte code entered from Rust as Bcall enters it.** A byte-code
  function called through funcall from native code, `apply` or a hook
  (`execute_closure`) had its closure's four slots decoded afresh
  (`closure_program`), its verification looked up again, and then went
  through the general activation (`run` and `run_frames`: their setup,
  a dispatch closure whose entry stored some forty loop constants, and
  the verified loop): about 550 instructions before its first op, where
  Bcall's frame entry reads the cache `entry_program` keeps. A closure
  that cache holds, called with arguments its packed list takes, is now
  entered the same way (`run_entered`): the frame and the arguments
  pushed, the verified loop run at once, and a return from the entry
  frame finishes there; any other exit (a signal, a quit check, a frame
  or handler still pushed) goes, with the loop's state, to the general
  loop (`run_loop`, the former body of `run_frames`), which takes that
  exit up first. The callback's entry in native-to-bytecode costs about
  290 instructions instead of about 550. Filling the cache from funcall
  showed that its entries, held in static TLS, were read by the
  conservative stack scan on a thread whose stack range covers its TLS:
  glibc places a spawned thread's static TLS at the top of its stack
  mapping, and `os_stack_base` reports the mapping's end (the runtime's
  own entry records a local as the base instead, so the editor's main
  thread was not affected). Two `suspended_bytecode_*` retention tests
  and two census tests failed, the entries keeping a finished thread's
  code and constants alive. The table is now on the heap behind a
  thread-local pointer (an empty static table until the thread's first
  miss), where the scan does not look.
- **Two stores the next load spans.** callgrind does not see a load that
  needs bytes from two stores still in the store buffer: store
  forwarding fails and the load waits for both to reach the cache, about
  a dozen cycles. Each backtrace record's argument field (`FrameArgs`)
  was built in a stack temporary, held across the reservation's growth
  call, and copied in with 16-byte loads that each spanned two of its
  8-byte stores, on every interpreted form, Bcall and native funcall;
  `write_backtrace_frame` now takes the field as a closure it calls once
  the slot is reserved, and the field is stored in place. A byte-code
  return moved the caller's frame out whole (`pop_if`, an Option whose
  None is the argument list's tag byte), so the 48-byte program was
  copied as that byte and three misaligned 16-byte pieces, and the
  loop's next read of the caller's constants spanned two of them; the
  frame is now read in place and released by the length (`BcFrame` has
  no drop glue, which a compile-time assertion keeps so). The
  temporaries' stores and loads went too: bytecode calls 579 → 558
  instructions an iteration, bytecode-to-native 634 → 617, the
  interpreted workloads about 2% fewer, native-to-bytecode 5,160 → 4,818
  with the entry above. Wall time, five rounds of five samples with the
  build before in the same rounds, the minimum: bytecode calls 0.158 →
  0.130 s (1.41 → 1.16 of GNU's), interpreted dynamic 0.106 → 0.097
  (1.19 → 1.09), lexical 0.094 → 0.087 (0.85 → 0.78), native-to-bytecode
  0.147 → 0.134 (1.45 → 1.33); the others within their noise (native
  execution, which runs neither path, read 1.03 and 1.07). Bytecode
  calls lost 18% of its time for 4% of its instructions: look for such
  copies (an aggregate built on the stack, then moved with wider or
  offset loads) on the other hot paths. A scan of the eight main
  workloads' hot code for loads that span younger narrower stores (each
  function's instructions in address order, a store's slot retired when
  its base register is written) found none left that runs more than once
  an iteration, the largest in undo the copy of `run_verified`'s exit in
  `run_entered` (119 a unit).
- **Four smaller cuts on the callback paths and undo.**
  `call_function_value_inner` classified an interpreted closure (itself,
  or a symbol's function) with `kind()` four times before
  `funcall_interpreted_lambda`; a closure now goes there by its tag and
  header, once (native-to-interpreted's callback: 157 → 129 instructions
  in that function). A native `catch` reused a spare handler record and
  zeroed all 304 bytes of it (`memset`); generated code reads only `next`
  and `val`, both set on each push, and its setjmp fills the jump buffer,
  as `push_handler_nosignal` reuses `nextfree` without clearing it. The
  direct-mapped closure entry table had 64 slots, and undo's byte-code
  functions missed it 185 times a unit (conflicts; a miss decodes the
  closure and looks its verification up, about 130 instructions); with
  256 (22 KB a thread, on the heap) 21 times. A dynamic binding's record
  (`SpecialBindingRestore`) held a buffer's whole undo state inline, for
  a binding of `buffer-undo-list`: 144 bytes, moved by `memcpy` into the
  unwind list and out again on every binding; that state is boxed now.
  Undo 1,806,000 → 1,786,000 instructions a unit (at 20 and 40 units: the
  10 and 20 pair read lower, a collector function negative in it),
  native-to-interpreted 4,923 → 4,864 an iteration, native-to-bytecode
  4,818 → 4,788, interpreted dynamic and calls 0.6% fewer. Wall time,
  the same way against the build before: native-to-interpreted 0.971 of
  it, native-to-bytecode 0.958, undo 0.980, the rest 0.93 to 1.03, the
  host's noise (bytecode-to-native and interpreted-to-native read 0.93
  and 0.94 with no change on their paths or in their counts).
- **Profile-guided builds for timing (`tools/pgo-build.sh`).** Undo takes
  three and a half times GNU's L1 instruction-cache misses (see the
  case-table bullet), and code laid out by its execution counts is the
  lever named there. The script builds an instrumented release binary and
  its image, runs GNU's own tests under ERT in batch (`test/src`'s files
  and 26 of `test/lisp`'s, about 2,500 tests in two minutes), merges the
  counts with the toolchain's `llvm-profdata` (component `llvm-tools`),
  and builds again with them. None of the files the locked workloads load
  is in the training (`undo-tests.el`, `sort-tests.el`, `bindat-tests.el`,
  nor `comp-tests.el` and its native resources), nor are the workloads
  themselves; the profile is made from the source being built, for every
  build, and none is kept in the tree. Its flags go through `--config`,
  joining `.cargo/config.toml`'s (the dispatch copy's tail-duplication
  limits are kept: `run_verified` has its indirect jumps as without the
  profile). The mismatch warning is off: only the build scripts' and the
  fingerprint helper's `main` met the editor's `main` in the profile.
  Against the same source without it (five rounds of five samples in the
  same rounds, the minimum), the geometric mean of the sixteen is 0.90 of
  its time: native execution 0.73 (its native code is GNU's, the runtime
  helpers Emaxx's), native-to-interpreted 0.79, native-to-bytecode 0.83,
  bindat 0.84, lexical 0.86, sort 0.87, undo 0.90, dynamic 0.90,
  interpreted-to-native 0.92; the byte-code loop and the collector barely
  move (bytecode calls 0.99, bytecode-to-native 1.02, mapcar 1.03,
  explicit-gc 0.96). Trained on the sixteen workloads instead (an upper
  bound, not a fair build) the mean was 0.88. With the profile, against
  GNU (built as the oracle records, `-O2`, no profile): lexical 0.69,
  calls 0.72, native execution 0.75, bindat 0.81, list traversal 0.94,
  sort 1.01, dynamic 1.02, native-to-interpreted 1.03; above the bound
  bytecode-to-native 1.54, undo 1.35 (1.46), mapcar 1.29, cons allocation
  1.22 (the in-sample collection), interpreted-to-native 1.19, bytecode
  calls 1.10, explicit-gc 1.09, native-to-bytecode 1.06; the geometric
  mean of all sixteen 1.03. Timing pilots and the final run use this
  build from here (the gates keep the gate profile); instruction counts
  stay on the plain release build, whose code the profile does not
  change in kind. The script ran end to end with no warning (about
  fifteen minutes from nothing); seven of GNU's test files run with the
  final binary (777 tests) gave the outcomes of the instrumented run, and
  the sixteen workloads' result checks pass with it.
- **mapcar's list from its results in place (`Value::list_of`).** mapcar
  built its value with `Value::list`, which collected the rooted results
  into a new vector (a copy loop, 13 instructions an element) and then
  read a `vector-literal` head as a vector: `(mapcar #'identity
  '(vector-literal 1 2))` returned `[1 2]` where GNU returns the list.
  `Value::list_of` conses the results from the last, as Flist does over
  mapcar1's array, which stays rooted meanwhile. Mapcar 30.09 → 28.76
  million instructions a unit (1.29 → 1.24 of GNU's); the mapping test
  takes the `vector-literal` case. The same reading remains in `list`,
  `copy-sequence`, `reverse`, `butlast`, `take` and `seq-map` (each
  returns a vector for a list whose first element is the symbol
  `vector-literal`, and `(delq nil (list 'vector-literal nil 1))`
  signals): Emaxx carries vectors through lists headed by that interned
  symbol (`Value::to_vec` of a vector puts it first, `Value::list` turns
  such a list back into a vector, the reader and about sixty producers
  build vectors that way, and some sixteen consumers test for the head
  by name). The fix is a head Lisp cannot make, tested by identity: an
  uninterned symbol kept as a static root, or an internal immediate such
  as the unbound marker, at every producer and consumer (about 120 sites
  with the tests); see the next steps.
- **Native calls: the escape record in the calling frame, and a Bcall
  into native code in one frame.** Three changes, on the paths every
  locked workload takes (batch startup runs `--eval` and `-l` inside
  startup.el's native `command-line-1`, in GNU as here, so their calls
  nest in an active native call):
  - `invoke_fixed_registers`, for a callee whose arguments all travel in
    registers (at most six, the maximum, not the count): the trampoline's
    escape record (callee-saved registers, stack pointer, resume address
    and marker, as `emaxx_native_longjmp` reads them) is written by inline
    assembly in the calling frame, which then calls the target with the
    arguments in registers, zero past them. No Rust frame lies between
    the generated code and the record, as with the trampoline; the
    trampoline's own frame and the invocation record it loaded the
    registers from are gone. A call nested in an active call of the same
    runtime and interpreter (`is_nested_call`) changes only the active
    call's record: every change of stack clears the active call
    (`with_thread_suspended`), so that call is on this stack, holds the
    execution lock and set the heap pointer and the stack's bottom.
  - `call_native_nested`: a Bcall whose callee is such a native function
    and whose call nests in the active call of its interpreter
    (`NestedCall`) runs in one frame: `call_plain`'s and
    `call_fixed_function`'s tests (the arity read unsigned, MANY and
    UNEVALLED exceeding six), Ffuncall's depth and frame, the call frame
    pushed without the thread activation and heap depth the active call
    holds (`invoke_nested_fixed`), and the value written by the loop in
    place of the function and its arguments (the stack's top as
    `replace_call` leaves it). Anything else, or any test failing, takes
    `call_plain` with nothing changed. The call frame is written in its
    slot (`Vec::push` built it on the stack and copied it in by a call
    that was not inlined).
  - `profiling()`: `EMAXX_PROFILE`'s test on every call is one byte, not
    the OnceLock's state and its Option.

  Bytecode-to-native 620 → 493 instructions an iteration (1.60 → 1.27 of
  GNU's; `run_verified` 259, `call_native_nested` 222), interpreted-to-native
  3,121 → 3,069 (the native layer 297 → 272), native-to-bytecode 4,788 →
  4,768, the interpreted loops' `eval_sub` a few instructions lower;
  the other rows move within the allocator's noise (undo at twenty and
  forty units: 1,775,165 before, 1,770,842 after). In wall time (release
  builds, five rounds of five samples alternating, the minimum):
  bytecode-to-native 0.170 → 0.132 s (GNU 0.111: 1.53 → 1.19),
  native-to-interpreted 0.135 → 0.133, native-to-bytecode 0.131 → 0.126,
  interpreted-to-native 0.1025 → 0.1027. With `native_comp::call_function`
  always inline (in the evaluator's calls too) interpreted-to-native ran
  20 instructions an iteration lower and native-to-interpreted 30, but
  interpreted-to-native took 0.110 s, 8% more, in two runs; it stays
  `#[inline]`. Fusing `call_plain`
  with the general native entry instead measured worse (599: the
  register allocator spilled 46 values and outlined `Vec::push`), as the
  earlier inlining of `invoke_fixed` did, and so did skipping the thread
  activation and heap depth in the general nested call (no gain, the
  spills around the assembly call). A new test,
  `bytecode_calls_native_functions_nested_in_native_calls_like_funcall_subr`,
  runs byte code called from native code (`assoc-delete-all`'s TEST)
  whose own native calls return, signal to its `condition-case` and
  throw out of a native callee to its `catch`, against GNU.
- **Undo: a signal's conditions by symbol, and three lookups on its
  edits.**
  - `LispErrorKind::condition_symbol`: the signaled condition is the car
    of a signal's data or the symbol the C core signals, read from its
    fixed slot (`cached_symbol!`, a Qsymbol), and
    `error_condition_names_of` reads its `error-conditions` by the
    property's symbol (`get_symbol_property_of`, eq as in Fget). Every
    signal used to build the condition's name as a `String`, find its
    symbol by hashing the name, and walk the plist comparing each key's
    name. Every handler search takes it: the interpreter's
    `condition-case`, the VM's and native code's handlers,
    `capture_batch_error_backtrace`'s test and
    `dispatch_handler_bindings`. `condition_type` is the symbol's name.
  - `dispatch_handler_bindings` searches the handler stack in place up
    to the first `handler-bind` handler it would call. Only then does it
    copy the stack and build the handlers' argument. Most signals meet
    their `condition-case` first.
  - `is_window_configuration_value` tests the record kind and its two
    frame slots, the only parts `window_configuration_snapshot_from_value`
    requires, without building the snapshot's window lists (about 4,900
    instructions a call, 4 a unit).
  - `adjust_window_point_slots`, run on every insertion and deletion,
    iterates the window ids from a `SmallVec`, not a fresh vector, and
    `create_buffer` sets its two locals by symbol through the
    address-keyed cache.

  Undo 1,770,842 → 1,741,232 instructions a unit at twenty and forty
  units (1.41 → 1.39 of GNU's). The window and buffer lookups account
  for 13,400 and the signals for 16,200: `error_condition_names` went
  from about 400 to 170 a call (59 calls a unit), and
  `dispatch_handler_bindings` from 1,150 to 390. The remaining signal
  cost is mostly Lisp: `error`'s `format-message` through `apply` (about
  24,000 a call, two a unit, against GNU's 19,000).
- **Undo: change hooks without the local hook's copy; the slow ops' fast
  paths measured and left out.**
  - `hook_values` asked `buffer_local_hook` only whether the buffer has
    a Rust-side local hook, which cloned the hook's vector each time;
    `has_buffer_local_hook` tests it in place. A local hook's default is
    read by the hook's symbol (`default_value_symbol`) rather than
    interned again by name. Together that is about 2,300 instructions a
    unit in undo (`buffer_local_hook` 1,520, `hook_values` 800; 1,741,232
    → 1,740,124 at twenty and forty units, with the allocator's noise
    around it). The byte-code workloads are unchanged (bytecode calls
    549.5, bytecode-to-native 489.0).
  - The ops that leave the verified loop for `op_in_place` and
    `exec_op` were tried in eight arrangements. Relative to `4e49cce`
    rebuilt (bytecode calls 550.0, bytecode-to-native 491.5,
    native-to-bytecode 4,751, undo 1,741,232):

    | Variant | Undo | Bytecode calls | b2n | n2b |
    | --- | --- | --- | --- | --- |
    | Q: Bvarbind and Bunbind arms in `run_verified`, calling an out-of-line function and continuing the loop | −8,600 | +0.9 | +2.0 | +6 |
    | R: Q plus arms for Bpoint, Bpoint_max, Bpoint_min and Bgoto_char of a fixnum | −25,100 | +3.3 | 0 | +7 |
    | S1: Bvarbind and Bunbind dispatched by their byte at the head of `op_in_place` | −11,100 | +4.6 | −2.1 | −2.8 |
    | S2: S1 plus Bvarref (not plain) and the point ops | −17,300 | +4.3 | +2.0 | −8.4 |
    | S3: S2 with the bind and unbind shared, not inlined, plus the hooks | −21,200 | +5.3 | 0 | 0 |
    | S5: S3 with the callee's entry depth read again after its frame record | — | +10.7 | +0.5 | — |
    | S6: `op_in_place` as before, a small `exec_op` running those ops | −1,500 | −0.2 | +0.3 | −4.5 |
    | S7: S3's dispatch with `op_in_place`'s general body out of line | — | +5.7 | — | — |

    The n2b column is `run_verified`'s own change for Q and R, whose
    totals include a one-time rehash of the address-keyed symbol cache
    (38.9 an iteration) that fell inside the measured range; for the
    others it is the whole iteration, with no rehash in range.

    In every variant that touches `run_verified` or `op_in_place`, the
    extra cost in bytecode calls and native-to-bytecode sits in
    `run_verified` alone, a function whose source did not change in S1
    to S7. Per-instruction profiles of bytecode calls (P against S2, 452
    against 456 instructions an iteration, each run once) trace it to
    `enter_closure_frame`, inlined into the Call arm. There the callee's
    arity bytes and entry depth stay live across the thread-local read
    and the 88-byte frame record's push; P keeps them in registers, and
    S2 stores three of them on the stack and reloads them for
    `push_arguments`. Re-reading the depth after the push (S5) moved the
    spills and cost more. S6 leaves the loop alone, but the small
    `exec_op` costs about 35 instructions a call (other ops then pay it
    and `exec_op_general`'s frame too), so it gains nothing. Wall time
    agreed: S2 against P on bytecode calls, minimum of 25 and 40
    samples, 0.1257 against 0.1242 s and 0.1272 against 0.1245 s. Undo's
    wall time on this machine varied more than the difference (0.663 to
    0.697 s for P). None of these is kept. Moving those ops (about 360 a
    unit in undo at 60 to 136 instructions each) needs the closure-entry
    path made insensitive to the allocator first: fewer values live
    across the frame record, or that record's push out of line.
- **The evaluator's call into native code nested in the active call.**
  `call_fixed_function`, which the evaluator's calls to native-compiled
  functions take (`eval_call_compiled`, Ffuncall's subr path), sends a
  callee of at most six arguments whose call nests in the active call of
  its interpreter (`NestedCall`) to `invoke_nested_fixed`, as Bcall does:
  the call frame pushed, the active call's record replaced, the frame
  popped. The thread activation and the heap's depth, which the active
  call holds, are left alone. The caller has made maybe_gc's test
  (`eval_sub` at the form, Ffuncall before dispatch), so no collection
  test is added. Interpreted-to-native 3,067 → 3,039 instructions an
  iteration (1.29 → 1.27; wall time 0.1034 → 0.0992 s on the plain
  release build, six rounds of five samples), native-to-interpreted
  4,789 → 4,767 (1.16 → 1.15), native-to-bytecode 4,751 → 4,741, mapcar
  28.58 → 28.47 million (1.229 → 1.225); bytecode calls, bytecode-to-native,
  native execution and undo unchanged.
- **Collections that leave the image's cells unread, and a census without
  the registry walk.** A collection's census found the obarrays by
  walking the whole record registry (`gnu_hash_storage_bytes`: about
  584,000 instructions and 24,000 L1 misses a collection); it reads the
  obarray and abbrev-table types from the type index instead (checked
  builds compare it with the walk). The block registry carries each
  block's image flag, so the five sweeps no longer clone the set of image
  blocks. And the string, symbol and small-vector sweeps leave the
  image's blocks unread when nothing in them died: a string or symbol
  allocated while an image loads carries an image flag (a padding byte;
  the cells stay 48 and 64 bytes), as does a vector block registered
  then, and a mark that first reaches such a cell raises its kind's
  count (`ImageCells`, reset as each mark epoch begins). Only a sweep
  frees an image cell and none is allocated outside a load, so when the
  count equals the image cells the last sweep that read them left, every
  one of them is marked again: the sweep passes the image's blocks by,
  carrying over their live buffers for gcstat; otherwise it reads them all
  and records the new count. Checked builds re-read every block passed by
  and assert that each allocated cell in it is marked. GNU's sweeps never
  read its dump. An explicit collection (100,000 live conses): 54.53 →
  49.31 million instructions (0.82 → 0.74 of GNU's), L1 data misses
  502,000 → 381,000 (GNU 297,000), and misses of a 1 MB cache modelled as
  this host's L2 (`--LL=1048576,16,64`) 408,000 → 318,000 (GNU 236,000),
  the sweeps' 100,000 → 16,500 (GNU's about 9,500). Timed with per-phase
  clocks in an experiment build (best of five rounds of 32 collections
  with a 100,000-cons list live), the vector sweep 994 → 39 µs, strings
  714 → 108, symbols 198 → 29, and a whole collection 11.5–12.0 → 10.3–10.8
  ms against GNU's 10.7–11.0. Wall time on the line-table release builds
  (five rounds of five samples, the minimum, GNU's in parentheses):
  explicit-gc 0.1040 → 0.0917 s (0.0973; 1.07 → 0.94), cons allocation
  0.0917 → 0.0893 (0.0733), mapcar 0.0764 → 0.0710 (0.0629; 1.21 → 1.13),
  undo 0.1673 → 0.1651 (0.1111). Instruction counts of the other fourteen
  are within their noise: undo 1,739,519 → 1,745,355 a unit (the byte-code
  verifier's address-keyed caches collide more on this layout,
  `entry_program_slow` 40 → 65 calls a unit) and mapcar 28.47 → 28.62
  million (`direct_add1`, whose source did not change, gained one
  instruction from an inlining change, 100,000 calls a unit). In mapcar
  and undo the string phase still takes 1.3 to 2.0 ms a collection: heap
  strings die every cycle, and the compaction that follows
  (`data::sweep`) reads the image's string data and its owners as well
  (next).
- **The image's string data out of the compaction.** The small data of
  strings allocated while an image loads goes to a chain of its own
  (`IMAGE_SMALL`), packed apart and never compacted, as pure data already
  was and as GNU's dumped strings keep their data in the dump, which
  `compact_small_strings` never reads. A dead image string's entry is
  retired in place and its block kept. Before, any heap string that died
  made the next compaction walk every small block from the oldest, the
  image's 52,600 strings' entries and their owners' sizes included. The
  string phase of a collection (per-phase clocks, as above): mapcar's
  compaction 1,300–1,465 → 161–179 µs, undo's 1,460–2,005 → 284–379 µs.
  Wall time against `85f3e00` (five rounds of five samples, the minimum,
  GNU's in parentheses): mapcar 0.0700 → 0.0673 s (0.0639; 1.10 → 1.05),
  sort 0.2036 → 0.1977 (0.1807; 1.13 → 1.09), cons allocation 0.0886 →
  0.0874 (0.0727), explicit-gc 0.0859 → 0.0827 (0.0999), undo 0.1630 →
  0.1638 (0.1107; one collection a sample, its gap is the mutator's:
  about 0.145 s outside collection against GNU's 0.093). Instruction
  counts: undo 1,742,444 a unit, mapcar 28.59 million, sort, cons
  allocation and explicit-gc unchanged (their callgrind sizes hold
  almost no compaction).
- **The conservative scan's lookups, and the sweeps' block lists by
  kind.** `mem_find` tests the heap's bounds before the registry
  (`HEAP_LOW` and `HEAP_HIGH`, alloc.c's `min_heap_address` and
  `max_heap_address`, raised as blocks register): most words a stack
  scan reads are return addresses, stack addresses and small numbers. The
  collection's scan of the native stack (about 4,400 words) looks all of
  them up under one lock (`conservative_values`), where each took the
  registry's mutex, its poison test and a B-tree descent, about 228
  instructions a word. The registry keeps each kind's blocks in a map of
  their own beside the one `mem_find` reads, so a sweep's block list is
  its kind's alone, not a filter of every block, six times a collection.
  An explicit collection 49.32 → 48.89 million instructions with the
  bounds and the one lock, 48.55 million with the per-kind lists (0.744 →
  0.733 of GNU's).
- **Undo's window configurations, message log and native `set`.** A
  window configuration keeps each window's saved slots in a vector, as
  window.c's `saved_window` vector does, where it built a list a window
  and read it back by a list walk; restoring one finds a frame's few saved
  windows by a search instead of building a hash map. `message_dolog`
  reads `messages-buffer-name` and `message-log-max` by their cached
  symbols (DEFVAR_LISP variables, never lexically bound), not by name
  through the lexical environment, and builds its line without
  `format!`. A buffer's first undo entry reserves room for sixteen, where
  a temporary buffer's list grew through four allocations. Native code's
  `set_internal` calls the set itself, as GNU's compiled code calls
  `set_internal`, instead of `set` by name through the primitive table;
  the remaining word-level subrs in `invoke_subr` dispatch by one match on
  the kind and arguments. Undo 1,751,807 → 1,702,865 instructions a unit
  (1.40 → 1.36 of GNU's), wall time 0.1653 → 0.1627 s (six rounds of five
  samples, the minimum, on the line-table release builds; GNU's 0.1137).
  The other fifteen are within their noise against the round's base:
  bindat's +6,400 a unit is a collection's stack scan and the verifier's
  address-keyed caches falling differently between the two sizes.
- **Undo's edits and native code's calls.** Two micro-benchmarks (each
  callgrinded at two sizes on both editors) isolate undo's largest gaps:
  an insert and a `delete-region` with undo enabled, 13,207 instructions
  an iteration against GNU's 7,417, and an interpreted closure over a
  non-empty environment, whose creation runs
  `cconv-make-interpreted-closure` and its `macroexpand-all` in native
  code, 68,843 against 34,432. For the edits: a change hook whose current
  value is void or nil runs nothing, whatever the buffer's own binding, so
  `hook_values` answers at once (signal_before_change's NILP test) where it
  read the buffer-local binding and the local-hook mirror by name;
  `run_change_hooks` compares the hook's name once; `related_buffer_ids`
  answers the buffer alone, in a SmallVec, while no buffer is indirect; a
  buffer without overlays skips the overlay search; and Bpoint,
  Bpoint_max, Bpoint_min and Bcurrent_buffer push their values in
  `exec_op`, where they called the primitives by name. For native code:
  `funcall`, `list`, `memq` and `assq` are direct targets of the generated
  subr table (`tools/generate_native_subrs.rs`, as `car-safe` and `get`
  were), so their calls skip `invoke_subr`'s dispatch on the subr's kind;
  a funcall's target is classified by the tag and the header (SUBRP,
  CLOSUREP), not by the whole `Kind` twice; and native code's specbind,
  like the VM's Bvarbind of a symbol, leaves its record on the
  interpreter's binding stack alone and unbinds it by its id, where both
  cloned the record into their unwind lists and searched for it again on
  the way out (the interpreter's record was already the canonical one,
  and collection already skipped the copies). The unwind lists box their
  rare records, so each entry stays a few words. The edits 13,207 →
  12,139 an iteration, the closure 68,843 → 62,664; undo 1,711,192 →
  1,657,377 instructions a unit (1.363 → 1.320 of GNU's), native
  execution 524.3 → 482.9 million (1.133 → 1.043; its recursive calls go
  through `funcall`), bindat −2.4%, sort −1.9%, native-to-interpreted
  −1.0%, the rest within their noise (mapcar's +0.4% is the allocator's
  consolidation and a stack scan falling between the two sizes). Wall time
  on the line-table release builds against `b206a4a` (five rounds of five
  samples, the minimum, GNU's in parentheses): native execution 0.3572 →
  0.2958 s (0.3397; 1.05 → 0.87), undo 0.1616 → 0.1583 (0.1126), sort
  0.2001 → 0.1991 (0.1822), bindat 0.0910 → 0.0913 (0.0965).
- **Temporary buffers and `format`.** Undo's native-compiled Lisp
  functions run GNU's own `.eln` code in both editors, so each `F…`
  function's own instructions are the same and its inclusive cost
  measures the runtime it calls: against GNU's, a unit spends +63,000 in
  `cconv-make-interpreted-closure`, +18,000 in `generate-new-buffer`
  (`with-temp-buffer`), +9,500 in `error` and +7,000 in `user-error`
  (`format-message` and `signal` under them), +6,300 in
  `substitute-command-keys` and +3,700 in `menu-bar-update-yank-menu`
  (scratch script `fincl.py`). `generate-new-buffer-name` returns its
  argument when no buffer has that name, as buffer.c returns NAME itself,
  where it allocated a copy; a new buffer reads `default-directory` and
  sets its two locals by their held symbols, where it looked up the
  directory by name through an empty environment and the locals through
  the interned-name cache. `format`'s loop keeps the result's length in
  characters as it grows, where it counted the whole result again for
  every literal character and every conversion (quadratic in the
  message), and skips the format string's text properties when it has
  none. A `with-temp-buffer` 12,235 → 10,623 instructions (GNU 17,991), an
  `error` and a `user-error` caught by `condition-case` 26,494 → 23,808
  (GNU 22,509); undo 1,654,414 → 1,645,316 a unit at twenty and forty
  units (1.311 of GNU's), the other fifteen within their noise
  (interpreted-to-native's +1.3% is a hash table's one rehash falling in
  the larger run). The signal itself still computes the condition's
  names about six times an error (handler-bind dispatch, the batch
  backtrace's catch test, the VM's handler search, the native error
  record), where signal_or_quit reads them once; a cache across those
  layers would have to see every change to the symbol's plist, which
  `plist-put` on the list itself makes without `put`.
- **Where undo's wall time goes beyond its instructions** (measured, no
  change kept). The profile-guided build runs undo in 1,473,768
  instructions a unit, 1.18 of GNU's, where its wall time is 1.24 of
  GNU's. Callgrind's cache and branch simulation (`--cache-sim=yes
  --branch-sim=yes`, a 32 KB L1i and a 1 MB last level as this host's L2)
  gives per unit: instruction-cache misses 47,000 against GNU's 21,000,
  L1 data misses 20,700 against 11,300, mispredicted conditional branches
  21,700 against 13,400, indirect ones about the same (9,700 against
  9,400). At ten to fifteen cycles each the instruction-cache misses
  weigh more than the extra instructions; they are spread over the large
  hot functions (`run_verified` 4,900 and 800 a unit, its code 33 KB, the
  size of the L1i; `exec_op`, `eval_sub`, `eval_native_form` and
  `call_function_value` about 1,500 to 2,200 each), where GNU's
  `exec_byte_code` takes 1,500 and `eval_sub` 600. Two layout
  experiments on `df06b81`'s profile: LLVM's machine function splitting
  (`-Cllvm-args=-enable-split-machine-functions`, 3,296 cold parts split
  off) left the simulated misses (472,700 against 470,500 per ten units)
  and the wall time as they were; ext-TSP block placement
  (`-Cllvm-args=-enable-ext-tsp-block-placement`, ten rounds of five
  samples against the default layout, the same binary otherwise) gave
  undo 1.242 → 1.207 (median ratio) but bytecode calls 1.072 → 1.083,
  mapcar 1.096 → 1.113 and cons allocation 1.159 → 1.171, so neither is
  kept. The hot code itself is larger than GNU's: fewer misses need
  smaller hot functions, not a different order of the same ones.
- **The six cases at the contract's size**, on `df06b81`'s profile build
  (five rounds of three samples, minimum and median ratio): mapcar 1.015
  and 1.057, bytecode calls 1.058 and 1.070, undo 1.154 and 1.119, cons
  allocation 1.112 and 1.154, interpreted-to-native 1.186 and 1.183,
  bytecode-to-native 1.195 and 1.240. In GNU's bytecode calls (468
  instructions an iteration) a call and return cost about 110 (the frame
  set up in the byte-code stack 40, the arguments pushed 25, the
  backtrace record 12, the depth 8, the return 25) and the ops about what
  Emaxx's cost; Emaxx's call and return are about twice that (the entry
  cache's four-word test and the code string's state, about 33; the
  program object copied into and out of the frame record). The entry
  cache's word test guards a closure address reused after a collection,
  in tables kept per thread; dropping it would need an epoch shared by
  all threads, for about 8 instructions a call.
- **A cons's weak-reference serial is given when a weak reference asks
  for it.** Fcons wrote an 8-byte serial into the block's metadata for
  every cell, a write stream beside the cell's own that GNU's Fcons does
  not have (at sixteen mapcar units a sample with a 1 MB last level,
  12,600 of Emaxx's 83,700 last-level write misses a unit, against GNU's
  67,500 in all). A cell's serial is now given at its first `downgrade`
  (the keymap and syntax views' `ConsMutationSnapshot` and tests are the
  only askers) and marked in a third bitmap of the block, which the sweep
  clears with the cell's other bits: an allocation finds its bit clear,
  so a reference to an earlier allocation of the slot answers nothing,
  and the slot's next serial is a new one. Three bits a cell leave 1,344
  cells a 32 KB block (1,350 before). The funcall forwarder mapcar's loop
  called (`hooks_overlays::call_function_value`, 7 instructions an
  element of its own) is inline, and `RootedVec::push` tests the capacity
  once. Mapcar's last-level write misses 83,700 → 69,900 a unit (GNU
  67,500), its level-one write misses 96,800 → 84,300. Instructions a
  unit against the build the full-size table used (`df06b81` with part of
  `f001516`): mapcar 28.60 → 28.22 million (1.230 → 1.214 of GNU's), undo
  1,668,563 → 1,595,058 (1.335 → 1.276, with `f001516`'s `format` count),
  cons allocation 5,674 → 5,654, sort 220.9 → 220.1 million, the other
  measured cases within 0.4%. Wall time, profile-use builds of `f001516`
  and of this round with `df06b81`'s profile (eight rounds of three
  samples, minimum and median, GNU's in parentheses): mapcar 0.0682 and
  0.0717 → 0.0668 and 0.0707 s (0.0614, 0.0661), cons allocation 0.0865
  and 0.0892 → 0.0851 and 0.0918 (0.0731, 0.0764), undo 0.1381 and 0.1437
  → 0.1292 and 0.1449 (0.1124, 0.1243), list traversal 0.1274 and 0.1341 →
  0.1280 and 0.1330 (0.1362, 0.1435). The experiment without any serial
  (the collection-overheads bullet above) gave mapcar about 1.6%; this
  keeps weak references exact for about that. A test takes a swept cell's
  slot again (`garbage-collect`'s own value is consed from the head of the
  free list, often the cell just swept) and checks that the earlier
  reference answers nothing and the new cell has a serial of its own.
  Validation (gate profile, one test thread and CI's 128 MB stack): the
  43 weak-reference, allocation, verifier, mapping and collection tests
  and the new one pass; clippy `-D warnings` and rustfmt pass. With more
  than one test thread the native runtime's tests corrupt the shared
  heap (a free-list word of 0x1, then a segfault), on the binary before
  this round too: the allocator presumes one Lisp thread, as CI runs it.
- **Cons allocation's collection, measured again.** Every timed sample
  runs no collection in GNU and one in Emaxx (0.0145 s of its 0.0885 on
  the `df06b81` profile build), so the case's whole gap is that
  collection: GNU's start-of-sample `garbage-collect` keeps the previous
  sample's list (the conservative stack scan), which at
  `gc-cons-percentage` 1.0 puts its threshold above one sample's consing.
  Emaxx's threshold follows `bump_consing_until_gc` from its census, as
  GNU's does; matching GNU's count would mean keeping garbage alive.
- **Where the byte-code call and return go, by instruction** (callgrind
  `--dump-instr=yes`, bytecode calls on `df06b81`'s line-table build):
  Emaxx's Bcall arm, frame entry and Breturn run about 98, 131 and 123
  instructions an iteration, about 350 against GNU's 195 or so (its
  Bcall about 80 with the backtrace record and maybe_gc, the frame set up
  in the byte-code stack with the argument copy about 75, the return
  about 40); Emaxx's ops are the cheaper by about 75, so the net is the
  80 an iteration of the table. The pieces: the callee's cell and kind
  tests, `plain_bcall_entry` and the entry cache's lookup (its hash, the
  four closure words and two state flags, about 45) in the arm; the
  entry's program tests, the byte-code stack's frame header, the
  backtrace record, the 48-byte program swapped into `vm.program` through
  a temporary and the 88-byte `BcFrame` pushed in the entry; the frame's
  pop, the program copied back (about 25) and the caller's code and
  verification tested again (`bytes_unguarded`'s state and
  `VerifiedCode::admits`, about 25) on the return. For bytecode-to-native
  the same profile gives `call_native_nested` about 210 a call: the
  prologue and epilogue 16, the entry tests about 50, the backtrace
  record 18, the handler reconciliation twice about 16, the native call
  frame's push and pop about 45, the active call 8, the argument by the
  count 13 and the escape record's call 18.
- **The byte-code entry cache by identity, and a list's cycle test by a
  count.** `entry_program`'s hit compared the closure's four slot words
  with the entry's (a closure address reused after a collection), and the
  program's code string state and verification (`Current`, `is_current`):
  about 45 instructions a call with the hash. A closure's slots never
  change once it is published, so its address names the program the
  entry read until the closure is freed, and only the vector sweep frees
  one: the sweep now forgets each freed closure's entry in its own
  thread's table (`forget_closure`), and a table whose thread did not run
  the last sweep is emptied at its next miss (`CLOSURES_SWEPT` against
  `vector_sweeps`). A string verified as code turns `Stale` only by
  `borrow_mut`, which now counts the change (`code_changes`); an entry
  holds the count it was filled at. A hit is the identity and the two
  counts. The table has 1,024 slots where it had 256: with the words
  gone a hit costs no slot loads, and undo's working set of closures had
  collided in 256 (its refills ran 41 to 340 a unit depending on where
  the heap put the closures; 38 with 1,024). Bcall's frame entry copies
  the caller's program into the frame's slot before the callee's goes in
  (`mem::replace` moved it through a temporary). `CycleGuard::step`
  counts down to the tortoise's move, out of line, where it compared,
  stepped and shifted branch-free every step (16 instructions an element
  of `length` against FOR_EACH_TAIL's 9). Instructions: bytecode calls
  547.8 → 521.9 an iteration (1.170 → 1.115 of GNU's), native-to-bytecode
  4,673 → 4,647, mapcar 28.22 → 27.78 million a unit (1.214 → 1.195);
  undo's own code about 8,000 a unit cheaper, its total moved by glibc's
  `malloc_consolidate` and `unlink_chunk` (between -19,000 and +11,000 a
  unit from build to build, with the heap's layout). A test frees called
  closures among kept ones and checks that the closures taking their
  addresses run their own constants.
  Validation (gate profile, one test thread, CI's stack): 132 byte-code,
  verifier and closure tests, 103 allocation, weak-reference and
  collection tests, 145 mapping, length, snapshot, keymap and syntax
  tests, 180 native-runtime tests and the evaluation suite's 389
  tests pass.
- **Profile-guided wall time on `3c034cc`** (`tools/pgo-build.sh` afresh;
  pilot sizes, five rounds of five samples, the minimum and median ratio
  to GNU, `df06b81`'s build alongside in parentheses): bytecode calls
  1.046 and 1.052 (1.084, 1.086), mapcar 1.061 and 1.011 (1.100, 1.050),
  cons allocation 1.152 and 1.111 (1.192, 1.202), undo 1.235 and 1.217
  (1.260, 1.309), interpreted-to-native 1.176 and 1.184 (1.169, 1.214),
  bytecode-to-native 1.233 and 1.302 (1.219, 1.231); the other ten at or
  below 1.0 (native-to-bytecode 0.996 and 1.017, dynamic 0.951 and
  0.961). A first run of the script lost its profile: one training
  process died writing the merged `.profraw` (a test past its 300-second
  limit while a build loaded the machine), leaving it truncated, and
  `llvm-profdata` merged nothing; with the machine otherwise idle the
  rerun was clean. Run the script alone.
- **The register calls by a mask, and a verified return by the code
  count.** A native function keeps the argument counts a call may pass
  in registers alone (a target, lexical binding, the count within an
  arity of at most six) as a bit each, in `Lisp_Subr`'s padding after the
  arity (offset 12, which no C or generated code reads), refreshed by
  every setter of those fields; `call_native_nested` and
  `call_fixed_function` test the bit where they tested the arity's two
  words, the lambda list and the target. A frame the verified loop
  enters records `code_changes`, and its Breturn resumes without testing
  the caller's code string and verification while the count is the
  same (`usize::MAX` for frames entered elsewhere). Instructions:
  interpreted-to-native `call_function` 156 → 143 a call,
  `call_native_nested` 222 → 220, bytecode calls 521.9 → 519.8.
  Validation (gate profile, `--test-threads=1`): the 201 native
  compilation tests, the 132 byte-code, verifier and closure tests and
  445 of the 446 funcall, native and subr tests pass (the other the
  image round-trip test below, which fails locally since `4008098` and
  passes in CI); clippy `-D warnings` and rustfmt pass.
- **The mark phase marks as it queues.** `LispReachability` pushed every
  field that was not an immediate and tested the mark when it popped
  the object (about 825,000 pops a collection, of which about 256,000
  were symbols already marked), each pop dispatching on the kind twice.
  An object is now marked as it is queued (alloc.c's `mark_object` tests
  the mark before it looks inside); a float, a bignum, a marker and a
  string without text properties are marked alone and never queued
  (`has_properties_unguarded`: no guard is alive during a collection);
  the drain traces each popped object inline (`trace_fields` had become
  a call with a six-register prologue for each of about 346,000 objects
  a collection). `trace_pending` still answers whether anything new was
  reached, for the weak tables' fixed point: the enqueues record it.
  Explicit collections on the line-table build: 49.0 → 39.8 million
  instructions each (GNU's `Fgarbage_collect` about 66 million); a
  collection of a heap of 600,000 cells with 100,000 live (`gcper2`):
  10.9 → 10.1 ms. A ring of eight prefetched objects between the mark
  stack and the trace was slower (11.2 ms) and is not kept. Validation:
  the 220 collection, weak-table, finalizer, reachability, mark, sweep
  and trace tests pass.
- **Cons allocation's gap is its one collection.** GNU's census at a
  sample's start holds 517,547 conses, the previous sample's list kept
  by a stale slot of its interpreted `let*` frame in the C stack, so
  its threshold (`gc-cons-percentage` 1.0 of about 9 MB) is above a
  sample's 8 MB of consing; Emaxx's census holds 15,790 and its
  threshold about 1.45 MB, so one collection runs in each sample.
  Keeping that list alive would be keeping garbage. The rest is already
  ahead: the timed loop alone (`core-perf-conses`, no collection) runs
  2,321 instructions an iteration against GNU's 2,101 and its wall time
  without the collection is below GNU's sample (0.071 against 0.079 s on
  `3c034cc`'s profile build). The in-sample collection takes 14.5 ms
  where back-to-back collections of a like heap take 9.6 (GNU 11.3):
  the caches are cold after the loop, and the trace walks the image's
  objects every time, as GNU's walks its dump. The case reaches the
  bound when that collection costs about 11 ms.
- **Profile-guided wall time on `ba9f640`** (`tools/pgo-build.sh` with
  the machine otherwise idle; pilot sizes, five rounds of five samples,
  the minimum and median ratio to GNU, `3c034cc`'s build alongside in
  parentheses): undo 1.214 and 1.262 (1.200, 1.221), bytecode-to-native
  1.206 and 1.206 (1.234, 1.263), interpreted-to-native 1.151 and 1.144
  (1.190, 1.216), cons allocation 1.131 and 1.106 (1.149, 1.165),
  bytecode calls 1.064 and 1.151 (1.045, 1.028), eight rounds of it
  again 1.070 and 1.042 (1.050, 1.041) with 511.7 instructions an
  iteration on the profile-guided build against 519.0 (its loop is
  sensitive to the layout the profile gives); mapcar 0.963 and 0.946
  (1.093, 1.074), explicit-gc 0.720 and 0.766 (0.959, 0.991): the mark
  phase's change is worth a quarter of a collection on this build; the
  other nine at or below 0.95 (interpreted calls 0.60, native execution
  0.63, bindat 0.71, sort 0.91, native-to-bytecode 0.92).
- **Undo's ops outside the verified loop.** Per unit undo leaves the
  verified loop 622 times for an op it runs in place (`op_in_place`):
  about 55 instructions of `op_in_place` and 25 of `exec_op`'s prologue,
  match and epilogue before the op's own work, and 198 of them decoded
  again by `decode_instr` (63 instructions each) for a variable or
  unbind family's operand. `exec_op`'s body is now inline in
  `op_in_place` (`exec_op_inline`; the full dispatch keeps the out-of-line
  copy), and `fetch_family_instruction` reads those families' operand
  and checks the constant index as `fetch_instruction` does (a test
  compares the two on every opcode and operand width). Undo's
  `line_start_at` ran about 384 instructions for each of 38 calls a unit
  (`forward-line` and the message log's line count) converting char
  indexes to byte indexes in a chunk; a text whose char and byte counts
  agree is ASCII, whose indexes are the same, and `newline_before` and
  `newline_at_or_after` skip the conversion for it. Undo: 1,614,623 →
  1,574,485 instructions a unit (10 against 20 units; glibc's
  `malloc_consolidate` and `unlink_chunk` add about 18,000 of noise from
  the heap's layout). The ops still call their primitives by name
  (`prim` → `prim_facts`' probe, about 20 instructions); the decoded
  op table behind a `OnceLock` costs about 8 a slow op. Validation (gate
  profile, `--test-threads=1`): the 97 byte-code, decoder and VM tests,
  the 25 line-motion tests and 461 of the 462 buffer, undo, search,
  insertion, region, column, indentation and message tests pass (the
  other the as-root save-buffer test below); clippy `-D warnings` and
  rustfmt pass.
- **Activation vectors kept, and the operandless table built at compile
  time.** Each activation (`run_entered`, `Activation::new`; 119 a unit
  in undo) began with empty frame and handler vectors, so the first calls
  of one that called grew a fresh vector (an allocation, then a
  reallocation and a copy at each doubling, about 10,000 instructions a
  unit with glibc's consolidation) and freed it at its end. They are now
  `Pooled`: empty until a push finds no room (`push_pooled`), when the
  cold `grow_pooled` takes a spare from a per-thread list first; the drop
  gives a vector with capacity back cleared (`give_back`, at most eight
  spares of at most 1,024 elements). A frame is no root (its program is
  its closure's, which the byte-code stack's header holds). The table of
  one-byte ops `op_in_place` reads sat behind a `OnceLock` (about 8
  instructions a slow op); `decode_instr`'s operand-free arms are now a
  `const fn plain_op`, and `OPERANDLESS_OPS` is built from it at compile
  time with the stack-ref, call and unbind members whose operand is in
  the opcode (a test compares it with the decoder's answer for every
  byte). Undo: 1,574,485 → 1,556,807 instructions a unit; bytecode calls
  525.0 → 521.5 an iteration on the line-table build (round 4 had moved
  the verified loop's register allocation by 4 instructions, this round
  back by 3.5: the loop is that sensitive to code beside it). Validation:
  the 97 byte-code, decoder and VM tests and the 93 condition-case,
  catch, unwind, handler, recursion, nesting, depth and throw tests pass;
  clippy `-D warnings` and rustfmt pass.
- **Strings read in place, and a native funcall's target from its cell.**
  Per unit undo made about 1,015 Rust allocations and as many frees (17
  instructions each in the host allocator, besides the copies). GNU's
  undo makes almost none outside its Lisp heap. Most were copies made to
  read something. `format` copied its format string three times and
  each `%s` argument once, to learn whether it was multibyte; `get-buffer`
  copied the name, and `find_buffer` cloned the buffer's own;
  `combine_insert_args` copied a sole string twice; a Lisp string made
  from Rust text was copied into a `String`, then by `encode_text` into a
  byte vector, then into its data. `with_string_text` now reads a
  string's text in place: its stored bytes are that text when they are
  ASCII, or valid UTF-8 in a multibyte string; only raw bytes and
  characters beyond Unicode are decoded. `string_is_multibyte` and
  `string_without_properties` read the flags. ASCII text goes straight
  into string data (`from_text`, `from_host_str`). `apply`, `list`,
  `&rest` parameters (byte-code and interpreted), `signal` and the
  interpreted `condition-case`'s clause heads no longer copy a list into
  a vector first. `Value::list` collected into a vector and turned a list
  headed by `vector-literal` into a vector, so `(list 'vector-literal 1)`
  and such a `&rest` list came back as vectors; they now cons as
  `Flist` does. `signal` copied a proper DATA (and listed a vector's
  elements after `vector-literal`); it now conses the error symbol onto
  DATA itself, as `signal_or_quit` does. `set-buffer-multibyte nil`
  allocated a vector for each character and recounted its result's
  characters after each; `text_range_string` (the deleted text undo
  records) converts no indexes for an ASCII text; the word-boundary
  lookup behind `forward-word` looked up the function `nil` (and
  allocated the void-function error it discarded) for each empty
  char-table entry. In the native call
  layer, `cconv-make-interpreted-closure` (the closure each interpreted
  `lambda` makes) cost 128,000 instructions a unit against GNU's 67,000.
  Each funcall from native code ran about 250 instructions in
  `direct_native_funcall`, `invoke_native_funcall`, `direct_funcall_target`
  and `call_word_target` (GNU's `Ffuncall`, `funcall_general` and
  `funcall_subr` about 70). A function cell holding a native function
  whose register counts admit the argument count is now called from the
  cell (`RegisteredNative`, nil for omitted optionals), and a byte-code
  callee's class is read from the same read of the cell.
  `mapcar` called `invoke_subr`'s whole dispatch for each element; it now
  syncs the handlers and calls `invoke_native_funcall`, and its result
  vector is on the stack up to 32 elements. A native funcall's arguments
  reaching a byte-code or interpreted callee were checked word by word
  through `mem_find` (220 to 730 instructions for a vector-like word,
  depending on the heap's layout); they are the caller's live words, read
  as `XUNTAG` reads them (`invoke_subr`'s general path already did).
  Undo: 1,556,807 → 1,504,349 instructions a unit (GNU 1,248,561: 1.25 →
  1.20), host allocations 1,015 → about 820 a unit; native-execution
  483.2 → 415.1 million instructions (0.90 of GNU's);
  native-to-interpreted 4,758 → 4,725 and bytecode-to-native 493.0 →
  489.4 an iteration; native-to-bytecode, interpreted-to-native, mapcar
  and bytecode calls unchanged. Validation (gate profile,
  `--test-threads=1`): the 539 `format`, buffer-name, `apply`, `signal`, `condition-case`,
  message, insertion, multibyte, undo, `delete-region`, funcall, `mapcar`,
  `&rest`, native-runtime, byte-code, word and text-property tests pass,
  among them three new ones (`signal`'s DATA, a leading `vector-literal`
  in `list` and `&rest` lists, the unibyte conversion against GNU's
  answer); clippy `-D warnings` and
  rustfmt pass.
- **Stores to C variables without a name check, `eq` by the word, and
  three subrs from native code without the general entry.** `specbind`
  and its unbind store a special variable's global value through
  `set_global_binding_resolved`. For a variable with a C slot (most of
  those undo's macro expansion binds, such as
  `inhibit-modification-hooks`), that compared the name against the
  evaluator's own cells, `text-conversion-style`, the terminal's KBOARD
  variables and `features`, and hashed it into the detached-variable
  table, though only ten of them do more than store the cell. A
  `STORE_EFFECT` bit, set with the C slot by name
  (`STORE_EFFECT_VARIABLES`), says which; any other C variable's store is
  the cell's alone. The detached-variable table, empty unless a forwarded
  variable was `makunbound`, is asked whether it is empty before the
  name is hashed (`forwarded_variable_detached`). `eq` (in `memq`,
  `delq`, hash-table lookups, `eq` itself and the byte-code op) classified
  both objects before comparing them, 38 and 29 instructions a call in
  `values_eq_plain` and `values_eq_in_env`; it compares the words first,
  and only two vectorlike words (a symbol with position, or a kind
  compared by id) go on to the kinds, as `lisp.h`'s `EQ` needs nothing
  more for the rest. Native code's `symbolp`, `symbol-function` and
  `fboundp` went through `invoke_subr`'s handler sync and kind dispatch
  (82, 169 and 172 instructions a call against GNU's 6, 6 and 9); a word
  that is not vectorlike answers `symbolp` by its tag, and a symbol
  cell's word answers the other two from its function cell, before the
  general entry. Each new buffer looked up `text-conversion-style` by
  name; it reads the symbol held interned. The byte-code `list` ops
  (`Blist1` to `Blist4`, `BlistN`) built their result through
  `Value::list`, which turns a list headed by `vector-literal` into a
  vector, as `list` and `&rest` did before the last round; they cons
  their operands from the stack. Undo: 1,504,349 → 1,470,853 for the
  stores, → 1,444,629 instructions a unit with the rest (GNU 1,248,561:
  1.20 → 1.16); bytecode calls 522.0 → 518.8 and bytecode-to-native
  490.6 → 487.2 an iteration, interpreted-to-native (3,044.4, 3,042.1),
  native-execution (415.3 million) and mapcar (27.84 million) unchanged.
  Validation (gate profile, `--test-threads=1`): the 443 variable,
  binding, keyboard, feature, quit, watcher, buffer-local, undo and
  special-variable tests pass (the KBOARD input contract test alone, as
  it takes 98 seconds on this build and the previous one), and the 370
  `eq`, member, `delete`, hash-table, byte-code and native-runtime tests,
  among them two new ones: the byte-code list ops, which `6242417`'s
  build answers with vectors (`([1] [1])`), and native code's `symbolp`,
  `symbol-function` and `fboundp` on each kind of word; clippy
  `-D warnings` and rustfmt pass.
- **Name lookups by one multiply, `format`'s copies, native `length`
  of a string, `min` and `max` of two fixnums.** `lookup_var` by name
  (61 a unit in undo: `print_options` reads eight variables a print,
  `message` four) finds the symbol through an address-keyed cache, whose
  FNV hash of the text's address and length ran a byte at a time over 16
  bytes (about 120 of 175 instructions a lookup). The key is now one word
  (the address, the length in the 16 bits a user-space address leaves
  clear) under `IdentityHasher`'s multiply; the text check after the hit
  still rejects any colliding entry. `format` collected its format string
  into a heap vector of characters, grew its result from empty, counted
  each conversion's characters twice and scanned an ASCII result for
  multibyte characters; the characters are on the stack up to 64 (an
  ASCII format string's by its bytes), the result starts at the format
  string's length, and an ASCII result skips the scan. Native code's
  `length` of a string took `invoke_subr`'s general entry (decode, call
  by name, encode); it reads the character count as `Flength`'s `SCHARS`
  does. `min` and `max` of two fixnums return the winning argument, as
  `minmax_driver` does. Undo: 1,444,629 → 1,437,808 instructions a unit
  (a second build with the `format` change finished at 1,442,070, of
  which a collection that fell inside the measured range is 5,198);
  bytecode calls 518.5, bytecode-to-native 486.9, interpreted-to-native
  3,054.1 (the change from 3,042.1 is glibc's `malloc_consolidate` and
  `unlink_chunk`, not the interpreter's functions).
  **`81fda48`'s CI failed on the anti-cheat audit**
  `native_comp_fast_paths_are_audited_against_gnu_c`: each primitive with
  a native fast path must be in `EXACT_NATIVE_C_FAST_PATHS` (its GNU
  owner and contract test) or an approved deviation, and `symbol-function`
  and `fboundp` were not (the frozen run enforces the same gates first
  and stopped there). Every other group of the Rust gate passed (run
  39138334869: eval_01 to eval_05 1,626 tests, primitives 728,
  compat_runtime 86, tty 62, batch 50; lightweight 596 of 597). This
  round lists the four new paths (`fboundp`, `symbol-function`, `min`,
  `max`) with their GNU owners and contract tests. The local validation
  of each round now runs the grouped gate's whole lightweight group (every
  test outside the eval, primitives, compat_runtime, tty and batch
  prefixes, the anti-cheat audits among them; about 40 seconds), which
  the per-change filters missed. Validation (gate profile,
  `--test-threads=1`): the lightweight group's 598 tests pass (with a new
  one for native `length` of a string and the two-fixnum `min` and
  `max`), and 197 `format`, `length`, interning, `string-match`,
  message, `prin1`, obarray and `max` tests; clippy `-D warnings` and
  rustfmt pass.
- **Slow ops: an opaque call and a family dispatch, measured and left
  out.** Undo leaves the verified loop 768 times a unit for
  `op_in_place`, about 103 instructions of its own each (decode, the
  combined function's prologue and epilogue, the match and the stack's
  bookkeeping) besides the op's work, some 60,000 a unit. Relative to
  `7cee893`'s build (bytecode calls 518.5, bytecode-to-native 486.9):
  A, `run_verified` calling `op_in_place` through `black_box`'s function
  pointer (so that nothing inferred of its body reaches the loop): 520.5
  and 488.5, undo unchanged (1,441,625). B, A plus the reference,
  binding and unbinding families dispatched by their byte at the head of
  `op_in_place` to small functions of their own, the rest in an
  out-of-line `op_in_place_general`: 522.4 and 487.8, undo 1,462,669
  (+21,000): the dispatcher alone cost 47 instructions an op (its
  frame, and the general body's `FastExit` returned through memory), and
  the general body 96 for the other 526. Neither is kept; a cheaper
  slow op needs the ops' arms reached without a second function's frame
  (an arm in `run_verified` itself moves its register allocation, see
  the eight variants above).
- **One handler search a signal.** An error unwinding through frames
  is asked at each frame boundary whether a `handler-bind` handler runs
  for it (`dispatch_handler_bindings`) and whether a `condition-case`
  catches it (`capture_batch_error_backtrace` then takes no backtrace
  copy), and each question recomputed the condition's `error-conditions`
  and every handler entry's match: 52 searches a unit in undo for four
  signals, about 20,000 instructions. GNU's `signal_or_quit` searches
  once, at the signal. The search (`HandlerSearch`: the innermost module
  boundary or matching entry, and the innermost matching `condition-case`)
  is kept for the error's box and condition symbol under a count of the
  pushes onto the handler stack and of the thread switches that exchange
  it, and the collection epoch; while those hold the stack has only lost
  entries, so the answer stands unless an entry it stopped at is gone.
  Undo: 1,442,069 → 1,430,253 instructions a unit (the same profile's
  `7cee893` reading; `error_condition_names_of` 59 → 11 calls a unit);
  bytecode calls 518.4, bytecode-to-native 486.6. Validation (gate
  profile, `--test-threads=1`): the lightweight group's 598 tests and
  172 handler, `condition-case`, signal, backtrace, thread, debug,
  unwind, `catch` and `throw` tests pass, with a new one: the same
  condition signaled again under the same handlers after the inner
  `condition-case` that caught the first exits reaches the
  `handler-bind` (GNU answers the same); clippy `-D warnings` and
  rustfmt pass.
- **Watched and buffer-local variables by flags on the symbol.** Every store and
  binding outside the plain-symbol fast path asked whether a variable
  watcher is registered by scanning the watcher list (21 entries after
  startup, the display variables `frame.el` watches with xdisp.c's
  `set-buffer-redisplay`) for the symbol; in undo that is 49 `specbind`s, their 120
  restores and the stores a unit, a loop of 22 compares each. GNU reads
  `trapped_write` on the symbol. The cell's state now carries
  `TRAPPED_WRITE`, set wherever a watcher entry is made (adding a
  watcher, the image's watchers, the image copier) and cleared when the
  symbol's entry goes; the list is searched only for a flagged symbol.
  Undo: 1,430,254 → 1,411,986 instructions a unit
  (`specbind_symbol_record` 136.8 → 91.9 a call, `apply_special_restore`
  96.8 → 51.3, `memcmp` 5,355 fewer). A variable read or binding outside
  the fast paths also looked the symbol up in the current buffer's local
  cells (two hash probes, 76 instructions, 139 times a unit) whatever
  the symbol; every buffer-local cell is made after its symbol is flagged
  `LOCALIZED` (as `make-local-variable` makes it `SYMBOL_LOCALIZED`), so
  a symbol without the flag answers no local at once: 1,411,986 →
  1,408,804. Bytecode calls 519.0 (`run_verified` 518 a call, as
  before), bytecode-to-native 486.4, interpreted-to-native 3,044.1.
  Validation (gate profile, `--test-threads=1`): the lightweight group's
  598 tests, 166 of 167 watcher, image, dump, clone, alias and
  uninterned-symbol tests (the one failure is this container's
  `batch_startup_image_round_trip_or_explicit_native_image_limit`, the
  restored image's first native function in `mapatoms` order, as on
  `4008098`), and 98 buffer-local, `kill-all-local-variables`,
  `make-variable-buffer-local` and watcher tests, with a new test: a variable's watcher
  notifies while registered, not after its removal, and again once
  re-added (GNU answers the same); clippy `-D warnings` and rustfmt
  pass.
- **The undo list's Lisp view kept across marker adjustments.**
  `buffer-undo-list` reads a cached Lisp list that each new record is
  consed onto. A deletion that moves markers splices their adjustments in
  below it, and that splice dropped the cache, as did enabling undo
  (`buffer-enable-undo` in a temporary buffer) and clearing the history;
  the next read rebuilt the whole list from the native records: 8
  rebuilds a unit in undo, about 110 records converted (tracing each
  drop showed 7 a unit at the splice and 6 at `disable_undo`, which only
  matters once undo is enabled again). The splice now gives the cache a
  new head over the adjustments and its old tail (no Lisp has read the
  newest record between the deletion and the splice; the list after
  marker-moving deletions is GNU's, adjustments and their order
  included), and an emptied or newly enabled history starts with the
  cache nil, which the records then extend. Undo: 1,408,804 → 1,401,532
  (rebuilds 8 → 4 a unit). A fresh buffer's cache is still made at its
  first read: making it at its first record left one rebuild a unit but
  measured 1,407,168, the records' Lisp forms made for lists never read
  costing what the rebuilds saved (and glibc's `malloc_consolidate`
  moving by 6,297), so it is not kept. Neither is native `symbolp`
  answering a vectorlike argument other than a symbol with position
  before the general entry (192 instructions a unit). Bytecode calls and
  bytecode-to-native run no undo records and were not re-measured.
  Validation (gate profile, `--test-threads=1`): the lightweight group's
  598 tests and 48 undo, marker, `delete-region` and amalgamation tests
  pass; clippy `-D warnings` and rustfmt pass.
- **A store's watchers looked for by the flag first.** `set_internal`'s
  notification found the store's buffer (`assignment_scope_symbol`, a
  buffer-local probe) before asking whether the symbol has a watcher;
  data.c notifies only a `SYMBOL_TRAPPED_WRITE` symbol, and the flag now
  answers first. Undo: 1,401,532 → 1,401,728 instructions a unit, the
  saving (`assignment_scope_symbol` 2,274 and `notify_assignment_symbol`
  371 fewer) under a collection that fell into the measured range on
  this build (`collect_below_stack_top` +2,724) and longer `memcmp`s
  (+4,018, 20 more calls a unit from the same callers). The binding
  paths, measured against GNU: Emaxx makes about 120 dynamic bindings a
  unit (51 from native code at 54 instructions a `runtime_specbind`
  plus 121 in `specbind_symbol_record`), GNU 399 at 64 each, its
  `unbind_to` 404 at 74; the binding stack is not where undo's gap is.
  Validation (gate profile, `--test-threads=1`): the lightweight group's
  598 tests and 28 watcher, `setq`, `set-default`, forwarded-variable and
  `text-conversion-style` tests pass; clippy `-D warnings` and rustfmt
  pass.
- **Print, message and word-boundary variables by their symbols.**
  `lookup_var` by name was called 61 times a unit in undo, each finding
  the symbol through the address cache and a text compare (about 120
  instructions before the read): `print_options` reads eight `print-*`
  variables a print, `message` reads `noninteractive`,
  `inhibit-message`, `set-message-function` and
  `clear-message-function`, word motion
  `find-word-boundary-function-table`. These now read their symbols
  through `cached_symbol!` (the thirteen names joined `KEY_NAMES`). Undo:
  1,401,728 → 1,394,287 instructions a unit (`interned_cached` 65 → 23
  calls a unit). Validation (gate profile, `--test-threads=1`): the
  lightweight group's 598 tests and 155 of 156 print, `message`,
  `prin1`, word-motion and `noninteractive` tests pass (the other is this
  container's `print_prunes_charset_properties_like_print_prune_string_charset`,
  whose GNU build disagrees with the expected text, as recorded below);
  clippy `-D warnings` and rustfmt pass.
- **Profile-guided wall time on `a2494c7`, against `6242417` on the
  same host.** `tools/pgo-build.sh` afresh (its training and merge clean),
  pilot sizes, five rounds of five samples, minimum and median ratio to
  GNU, `6242417` in parentheses; the first full table, then the closest
  six again: undo 0.900 and 0.845 (1.079, 1.054), again 0.938 and 0.919
  (1.115, 1.068); bytecode-to-native 1.090 and 1.134 (1.070, 1.130),
  again 1.049 and 0.979 (1.069, 1.033); bytecode calls 1.047 and 1.311
  (1.006, 1.030), again 1.039 and 1.026 (1.008, 1.024); cons allocation
  1.010 and 0.951 (1.015, 1.004), again 1.005 and 1.014; interpreted-to-
  native 0.989 and 1.005 (0.971, 0.995), again 1.005 and 1.014;
  native-to-bytecode 0.799 (0.770); the other ten at or below 0.86 by
  minimum. Undo is now below GNU; bytecode-to-native and bytecode calls
  are the two above the bound. Native execution is 0.512 and 0.517 by
  minimum against `6242417`'s 0.453 and 0.445 (0.2014 and 0.2029 seconds
  against 0.1780 and 0.1746, both runs): slower by about 14% while its
  instructions fell, not yet explained (the build's layout, or the
  rounds since; still about half of GNU's time).
- **Profile-guided wall time on `6242417`, against `ba9f640` on the
  same host.** The container came back from a restart on a host whose
  timings differ from the earlier tables' (an Intel Xeon at 2.10 GHz with
  a 260 MB last-level cache; GNU and `ba9f640`'s build both moved, by up
  to 30% on the collector's cases, and medians here lie further above
  minimums), so only the two builds side by side compare. Pilot sizes,
  five rounds of five samples, minimum and median ratio to GNU,
  `ba9f640` in parentheses: undo 1.111 and 1.151 (1.216, 1.307),
  bytecode-to-native 1.069 and 0.973 (1.065, 0.971), bytecode calls
  1.038 and 1.062 (1.063, 1.054), interpreted-to-native 1.007 and 1.019
  (1.009, 0.979), cons allocation 0.997 and 0.972 (1.005, 1.000), mapcar
  0.959 and 0.932 (0.907, 0.881; its instructions are unchanged, 27.95
  million on both builds), native execution 0.444 and 0.462 (0.497,
  0.530), and the other nine at or below 0.85 by minimum. The first
  attempt at this build failed: every training process crashed at exit
  merging into the shared raw profile, which an earlier process had left
  unreadable (`llvm-profdata`: "too much profile data"); the same
  processes ran cleanly on a fresh file, and the build run again
  succeeded.
- **Native execution's slower wall time on `a2494c7` is its layout.**
  Plain release builds of `6242417` and `5658349` (line tables, no
  profile) run native execution in 415,311,498 and 415,302,186
  instructions a unit, and in 0.2355 and 0.2541 seconds by minimum (again
  0.2368 and 0.2483; GNU 0.38): the same work, laid out differently, as
  in the profile-guided pair. Bytecode-to-native, the other workload
  through the native call boundary, measures the same on both (0.1169 and
  0.1186 at 4,000,000), so nothing in the rounds since slowed native
  calls; native execution stays near half of GNU's time.
- **A Bcall into native code by its count, inline in the loop.** The
  nested native call (`call_native_nested`, a Bcall of a native function
  inside an active native call) spent 220 instructions a call against
  GNU's Bcall arm and `funcall_subr` (about 40 for the call itself). It is
  now one copy per count for Bcall0 to Bcall3, whose count is a constant
  in its arm (the registers are the arguments' loads and zeros with no
  jump table on the count; Bcall4 and up keep one copy taking the count),
  inline in `run_verified`'s arm; the arguments are read below the
  loop's top without the frame's base; the native frame records the
  Ffuncall depth and backtrace count `enter_plain_funcall_at` just set
  instead of reading them back; and the callee's environment is written
  into the active call's record for the call (`ActiveCall::environment`
  is a `Cell`, the helpers' `with_active` handing out a shared reference)
  instead of a new record swapped into the thread-local and back.
  Bytecode-to-native: 486 → 444 instructions an iteration (the call path
  220 → 202 by the count, 198 with the arguments and the environment;
  inlining took 17 more, the depths 2); bytecode calls 520 → 512 (the
  loop's registers moved); interpreted-to-native unchanged (3,038 and
  3,039, its native calls go through `call_fixed_function`). Wall time,
  plain line-table builds, pilot sizes, five rounds of five samples,
  minimum ratio to GNU, the same changes without the inlining in
  parentheses: bytecode-to-native 1.019 and 0.996 in two runs (1.080,
  1.060; `5658349` measured 1.10), bytecode calls 1.022 and 1.034 (1.032,
  1.041); at the full 16,000,000, three rounds of five, 1.032 (1.058).
  What is left of the call: the escape record (nine stores) and the
  native frame's push and pop with its balance checks (about 40), the
  handler list's reconciliation before and after (about 14), the entry
  tests, and the loop's check that its code is still current after the
  call (about 16). GNU's `exec_byte_code` runs 339 instructions an
  iteration of this loop, Emaxx's loop without the call about 160.
  Validation (gate profile, `--test-threads=1`): the lightweight group's
  598 tests, including the native runtime's and the byte-code VM's, and
  the seven Bcall, funcall and verified-code tests among them
  `bytecode_calls_native_functions_nested_in_native_calls_like_funcall_subr`
  (a signal and a throw out of a nested native callee) pass; clippy
  `-D warnings` and rustfmt pass. Of the 286 evaluator and primitive
  tests whose names mention native code, byte code, funcall, signals or
  backtraces (finished after the commit), 284 pass and the other two are
  this container's known failures (the as-root
  `byte_compile_file_reports_an_unwritable_target_like_gnu` and
  `batch_startup_image_round_trip_or_explicit_native_image_limit`).
- **Profile-guided wall time on `a0034c1`, against `a2494c7` on the
  same host.** Pilot sizes, five rounds of five samples, minimum ratio to
  GNU, `a2494c7` in parentheses: bytecode calls 1.023 (1.021),
  bytecode-to-native 1.019 (1.081), cons allocation 1.006 (1.006),
  interpreted-to-native 0.989 (1.010), undo 0.934 (0.990), mapcar 0.841
  (0.815), list traversal 0.838 (0.837), native-to-bytecode 0.793
  (0.783), sort 0.762 (0.759), interpreted-dynamic 0.753 (0.752),
  native-to-interpreted 0.699 (0.719), explicit-gc 0.684 (0.689),
  bindat 0.671 (0.652), interpreted-calls 0.576 (0.555),
  interpreted-lexical 0.573 (0.569), native execution 0.474 (0.513).
  Medians on this host lie up to a quarter above minimums
  (interpreted-to-native 1.28 by median against 0.99 by minimum), so the
  four near 1.0 are short of the contract's bound: its median-of-three
  per process, nine paired rounds and bootstrapped upper 95% at most 1.03
  leave room for about 0.98 at most. The first attempt at this build
  failed as `6242417`'s first had: from the nineteenth training process
  on every merge into the raw profile reported it invalid; the training
  and the final build run again on the same instrumented build succeeded.
- **maybe_gc's test on a mirror of the active call's trigger.** An
  evaluated form (`eval_sub`), a byte-code frame's entry and a plain
  Bcall each asked whether a collection could be due through the active
  call's record: the thread-local, its runtime, the runtime's shared
  heap, the trigger, about eight instructions where lisp.h's maybe_gc
  reads one global. The trigger of the active call's collector is now
  mirrored in a thread-local (`ACTIVE_GC_TRIGGER`, 0 with no active call,
  which takes the full test), set with the active call by
  `with_active_call` and the suspension, re-read from its collector when
  the call it was saved for resumes, and written by `publish_trigger`
  (the trigger's one setter) when its collector is the active one; below
  it a collection cannot be due, as the active runtime's own test
  answers. `eval_call_compiled` tests a native callee before a closure
  and makes the usual native call inline (`call_function_inline`), and
  `eval_sub` compares the depth with `max-lisp-eval-depth` without a
  saturating conversion. Instructions a unit: cons allocation 5,623 →
  5,494 (5,542 by the mirror alone), interpreted-to-native 3,038 → 2,957
  (2,998 by the mirror, 2,977 with the inline call), bytecode calls 512
  → 504, bytecode-to-native 444 → 445 (its nested call reads its
  runtime's trigger directly already). GNU runs cons allocation's unit in
  5,215 and interpreted-to-native's in 2,385. Validation (gate profile,
  `--test-threads=1`): the lightweight group's 598 tests and 240 of 242
  evaluator and primitive tests whose names mention collection,
  thresholds, threads, suspension or native code pass; the other two are
  `batch_startup_image_round_trip_or_explicit_native_image_limit` (this
  container's) and
  `process_tls_state_survives_collection_before_and_after_async_handshake`,
  whose GNU oracle answered `connect` for the asynchronous handshake while
  four test processes shared the machine and which passes run alone;
  clippy `-D warnings` and rustfmt pass.
- **The contract's comparison, not the pilot minimum, on four cases.**
  A script following the contract (`compat/core_runtime_perf.json`: full
  sizes, nine rounds, one warmup and three samples a process, the
  editors' order alternating, the median within each process, paired
  round ratios and the bootstrapped 95% interval of their median with
  the locked seed) on the profile-guided builds: on `a0034c1` bytecode
  calls 1.043 [1.019, 1.153], bytecode-to-native 1.044 [1.009, 1.106],
  cons allocation 0.964 [0.926, 1.025], interpreted-to-native 0.954
  [0.919, 1.002]; on `aa0c63f` 1.045 [0.984, 1.120], 1.036 [0.979,
  1.079], 0.940 [0.882, 1.001] and 0.996 [0.898, 1.097]. So the pilot
  minimums (about 1.02 for the two byte-code cases) understate them:
  bytecode calls runs 0.45 to 0.51 seconds a process against GNU's 0.42
  to 0.46 at 12,000,000 (ten processes each), with address randomization
  or without it, about 5 to 9% slower by median. Interpreted-to-native's
  processes spread from 0.319 to 0.388 (one at 0.55) against GNU's 0.351
  to 0.421; with randomization off (`setarch -R`) Emaxx's fall within
  0.327 to 0.362, the median the same: part of that case's interval is
  the address layout, which the contract does not fix for either
  editor. The memory of a byte-code run does not grow with its size (88
  MB at 3, 12 and 24 million calls).
- **A running frame's program in three words.** A byte-code call saved
  the caller's whole program (`ByteCodeObject`, six words, two of them
  the argument template) in its frame, copied the callee's into the
  loop's, and a return copied the caller's back. A running frame reads
  only its code, constants and verifier result (bytecode.c re-reads
  `bytestr_data` and `vectorp` from the function at a return), so frames,
  the loops and the activation now hold those three (`Running`); the
  argument template and stack depth are read at the entry from the whole
  program, as before. The frames were never collector roots (their
  functions on the byte-code stack are, and `run` roots a program run
  without one). Bytecode calls: 504 → 491 instructions an iteration;
  bytecode-to-native 445, native-to-bytecode 4,584 → 4,582, sort and
  mapcar unchanged. Writing the frame record into its slot after the
  capacity test (so the caller's program is not held across the growth
  call) made the loop's registers worse instead (504 → 550) and is not
  kept. Validation (gate profile, `--test-threads=1`): the lightweight
  group's 598 tests and 168 of 169 evaluator and primitive tests whose
  names mention byte code, compilation, funcall, signals, closures,
  verification, unwinding or handlers pass (the other is the as-root
  `byte_compile_file_reports_an_unwritable_target_like_gnu`); clippy
  `-D warnings` and rustfmt pass.
- **A position-dependent executable, and a frame entry's counts in one
  bit.** On x86-64 Linux the package's own crates compile with
  `-C relocation-model=static` (`tools/rustc-workspace-wrapper.py`,
  Cargo's workspace wrapper in `.cargo/config.toml`, a Python script
  since a POSIX shell drops `CARGO_BIN_EXE_make-fingerprint` from the
  environment of the integration tests' compiles; dependencies keep
  the default, since the procedural macros among them are shared objects
  such code cannot be linked into, and Cargo has no stable per-package or
  host flags). A jump table in position-independent code holds 32-bit
  offsets from the table, so each byte-code dispatch was the opcode's
  load, the offset's load, an add of the table's address (which held a
  register through the loop) and the jump; with absolute entries it is
  the load and `jmp *TABLE(,%rax,8)`, where GNU's computed goto (its
  build is a PIE) takes four or five. The frame-entry cache
  (`verify::EntryProgram`) now holds the callee's code, constants,
  verified code, depth and first slots with the argument counts a plain
  entry takes as a 64-bit mask, so setup_frame's count test and the
  frame's room are one bit test, and the verified loop holds its code
  without the length (the slow op reads its own). Instructions a unit
  at a four-hundredth of the contract sizes against `c1ae4e3`, plain
  builds: bytecode calls 495.5 → 439.2 (GNU 468), bytecode-to-native
  450.0 → 411.8 (GNU 388), explicit GC −10.1%, interpreted-dynamic −3.1%,
  native execution −1.5%, the others −0.03 to −1.5% but native-to-bytecode
  (+0.05%), and undo 1,384,033 → 1,347,374 at 10 and 20 units (its
  +1.4% at 2 and 4 is collection noise). The mask and the code without
  its length alone, still position-independent: bytecode calls 491 → 481
  at pilot size. Plain-build wall time at pilot sizes (seven rounds of
  five samples, minimum and median against GNU's): bytecode calls 0.910
  and 0.901 (`c1ae4e3` 1.005 and 0.972), bytecode-to-native 0.983 and
  1.061 (1.028 and 1.057). Validation (gate profile,
  `--test-threads=1`): the lightweight group's 598 tests (197 of them
  native-code tests, 30 image and 33 dump tests), the 168 of 169 byte-code,
  compilation, funcall, signal and closure tests as above (the as-root
  one) and the 50 batch tests pass; clippy `-D warnings` and rustfmt
  pass. The batch tests run two at a time in one process abort on
  `c1ae4e3` as well (a collection meets a string another test's
  interpreter holds mutably borrowed, "cannot collect while a string is
  mutably borrowed", or the C heap's own abort): the string heap is the
  process's, and the gate runs them one at a time.
- **The contract's comparison on all sixteen, on `3f7bff2`'s
  profile-guided build** (the script above: full sizes, nine rounds, one
  warmup and three samples a process, alternating order, the locked
  bootstrap), median and 95% interval of the paired ratios:
  interpreted-lexical 0.602 [0.532, 0.682], interpreted-dynamic 0.768
  [0.737, 0.844], interpreted-calls 0.562 [0.484, 0.611], bytecode calls
  0.943 [0.910, 1.049], native execution 0.507 [0.463, 0.540],
  interpreted-to-native 0.942 [0.910, 1.026], bytecode-to-native 1.023
  [0.968, 1.064], native-to-interpreted 0.708 [0.602, 0.843],
  native-to-bytecode 0.760 [0.703, 0.924], cons allocation 0.929 [0.871,
  1.043], list traversal 0.813 [0.747, 0.833], mapcar 0.812 [0.755,
  0.883], explicit GC 0.688 [0.638, 0.732], sort 0.845 [0.754, 0.943],
  undo 0.991 [0.944, 1.109], bindat 0.673 [0.623, 0.734]. Twelve meet
  the 1.03 bound; bytecode-to-native, undo, bytecode calls and cons
  allocation do not. A round's ratio ranges over about ±10% on this
  machine (undo's from 0.819 to 1.153), so the interval's upper end lies
  well above the median. `tools/pgo-build.sh`'s training now writes a
  profile a process and merges the ones that read: the one file the
  default name merges every process into came out unreadable ("too much
  profile data") on both attempts for this build and on `a0034c1`'s
  first, losing the whole profile, while each process's own file reads
  (one of this build's was unreadable and is skipped).
- **A form whose head holds a MANY subr evaluates its arguments
  without the general call.** eval_sub's direct path took a C primitive
  of at most eight fixed arguments and a special form, while `<`, `+`
  and the other subrs of any number of arguments went through
  `eval_call`'s resolution loop and full classification of the function
  cell. `eval_many_subr` takes them as eval_sub's MANY branch does: the
  frame recorded with the unevaluated form, the count and the minimum
  checked, each argument's Fcdr taken before its evaluation (a list an
  earlier argument cut short ends the arguments there), and more than
  eight left to the rooted general call. Instructions a unit against
  `3f7bff2`, plain builds: interpreted-to-native 2,934 → 2,711 (GNU
  2,387), interpreted-calls 3,441 → 3,205, interpreted-lexical 3,867 →
  3,650, list traversal 286,523 → 273,865, cons allocation 5,460 →
  5,411. GNU and Emaxx print the same for nine edge forms (more than
  eight arguments, `(<)`, an argument that signals, an improper argument
  list, a list an argument's evaluation shortens, a `throw` out of an
  argument, `(max)`). Validation (gate profile, `--test-threads=1`): the
  lightweight group's 598 tests, 196 evaluator and primitive tests whose
  names mention arity, arguments, backtraces, signals, `setq`, `apply`,
  `funcall`, list lengths or subrs, and the seven integration targets'
  42 tests pass; clippy `-D warnings` and rustfmt pass.
- **Where a byte-code iteration's time goes.** One iteration of each
  editor's loop, traced instruction by instruction (gdb attached to a
  running workload, stepping from an instruction the loop runs once
  around to it) and given to `llvm-mca -mcpu=sapphirerapids` (this host
  is an Emerald Rapids Xeon, model 207): bytecode calls is 442
  instructions, 487 µops, 28 taken branches and 26 stack-slot accesses
  on Emaxx's plain build against GNU's 468, 481, 40 and 68, and the
  model puts Emaxx at 5.8 µops a cycle against a dispatch width of 6
  (bound by µops issued) and GNU at 4.4 (bound by dependencies). On the
  profile-guided build the same loop is 441 instructions but 497 µops,
  19 taken branches and 35 stack-slot accesses: its layout takes fewer
  branches, its register allocation spills more, and it times slower
  than the plain build of the same source (bytecode calls, minimum and
  median against GNU's at pilot size: plain 0.91 and 0.94, profile-guided
  0.93 and 0.99; bytecode-to-native 1.01 and 1.03 against 1.02 and 1.05).
  A µop removed is worth a sixth of a cycle there, spills included.
  Bytecode-to-native: 411 instructions and 464 µops against GNU's 388
  and 392, 58 stores against 44; split at the dispatch jumps, its call op
  is 275 instructions against GNU's 115, its other 13 ops 136 against
  GNU's 273. The call op's parts: the callee's type tests (about 41),
  `plain_bcall_entry` (about 20), Ffuncall's depth and backtrace record
  (about 20, 9 stores), the native frame's push and pop with the two
  handler syncs (about 40), the escape buffer and the call (about 26, 9
  stores), and the settling, the record's pop and the loop's resumption
  (about 35), with 12 spills. Taking the fixed-count native call out of
  line made it 7.5 instructions longer and 10.6 loads lighter for the
  same wall time (not kept). Holding the interpreter and environment in
  locals for the call, testing a collection on the active call's trigger
  mirror first (`collection_cannot_be_due`'s fast test is now inline,
  its rest out of line), comparing the depth in `plain_bcall_entry`
  without a saturating conversion and building the entry's count mask
  without a loop: bytecode calls 442.8 → 438.8, bytecode-to-native 411.0
  → 409.7. Validation (gate profile, `--test-threads=1`): the lightweight
  group's 598 tests and 325 of the 326 byte-code, native-call, evaluator
  and argument tests of this round and the last (the as-root one fails
  as before); clippy `-D warnings` and rustfmt pass.
- **A nested native call's frame popped by its length.** The call op of
  bytecode-to-native traced again on `152e76f`: 263 instructions around
  the callee's 10. Five of its steps are shorter now. The callee's subr
  test is one masked compare of the header (`vectors::is_subr`, which
  `native_function_ref` and `builtin_ref` share) where the tag's decoding
  branched on the flag first. After a call into native code or a subr
  the loop goes on in its bytes when `code_changes` reads what it read
  before the call (taking a mutable guard on current code counts a
  change), and tests the string's state and the verification's currency
  only otherwise. A nested call's native frame is popped by the length
  when the callee left everything as it found it (`finish_nested_frame`:
  `sync_handlers` would change nothing, no handler, unwind entry or root
  range lies above the frame's, no error is pending); `finish_frame`
  does the whole of it otherwise. The escape's resume label lies out of
  line (a `.pushsection` stub) and leaves the call's word as tag 1, which
  no Lisp object has, so an ordinary return goes on from the call with
  no flag of its own. `leave_plain_funcall` decrements Ffuncall's depth
  without the checked subtraction that `end_funcall` makes. Bytecode-to-
  native 409.7 → 393.0 instructions an iteration (GNU 388; the call op
  245 around the callee), bytecode calls 438.8 → 435.7 (the byte-code
  return's decrement), undo 1,348,880 → 1,330,595 a unit (the resumption
  after a subr's call), interpreted-to-native 2,711 → 2,699, cons
  allocation unchanged at 5,414. Two further changes measured worse and
  were not kept, both for the register allocation of the loop around
  them: the native call's signal settled out of line with the resumption's
  slow test behind `std::hint::cold_path` (394.3, two taken branches
  fewer but seven stack accesses more, and 1% more cycles in
  `llvm-mca`'s model), and the cold test alone with the frame's whole
  finish out of line (397.7).
  Validation (gate profile, `--test-threads=1`): the lightweight group's
  598 tests and 777 of the 778 byte-code, native, backtrace, funcall,
  signal, condition, unwind, subr and verification tests with the last
  round's (the as-root one fails as before, and
  `bytecode_code_mutation_during_active_call_matches_gnu` takes the
  resumption's other way); clippy `-D warnings` and rustfmt pass.
- **The evaluator's forms and subr calls.** `eval` tests the depth and
  goes to `eval_nested`, which sends a cons straight to `eval_sub` and
  gives a symbol or an immediate its fast way; before, every form went
  through the symbol and immediate tests first. A subr's arguments
  (`eval_fixed_subr`, `eval_many_subr`) are evaluated with
  `eval_nested`, the depth being counted by the form, into slots left
  uninitialized past those written (eval_sub's own `argvals[8]` is
  uninitialized; the stack's scan is conservative), and the subr is
  called through `NativeSubr::direct_entry`, its facts' direct entry
  kept in one word once known (0 before, 1 for none), where
  `call_with_facts_arity_checked` read the facts' `OnceLock` and three
  of their fields on every call. `eval_cons` makes the head's value (a
  head that is no symbol) only on the general call's way, where it was
  made and spilled before the subr arms. Instructions a unit against
  `152e76f`'s plain build: cons allocation 5,422 → 5,047 (GNU 5,215),
  interpreted-to-native 2,711 → 2,541 (GNU 2,387), interpreted-lexical
  3,657 → 3,459, -dynamic 3,353 → 3,195, -calls 3,219 → 3,104,
  list-traversal 273,926 → 258,882, native-to-interpreted 4,396 →
  4,216, native-to-bytecode 4,333 → 4,161, bindat 908,788 → 880,580,
  sort 227.6M → 219.6M; undo 1,330,595 → 1,325,896 against the last
  round. Validation (gate profile, `--test-threads=1`): all 1,677 of
  the evaluator's tests (`lisp::eval::`) with the last round's 104
  primitive tests, 1,781 in all, and the lightweight group's 598 pass;
  clippy `-D warnings` and rustfmt pass.
- **The host changed under the session (wall ratios before and after
  are not comparable).** The container restarted on an Intel Xeon of
  model 85 (Skylake/Cascade Lake, 2.8 GHz, four cores) where the earlier
  figures came from an Emerald Rapids (model 207). On it, the contract's
  comparison of `aa68c68`'s profile-guided build, the host otherwise
  idle: bytecode-to-native 1.018 [0.995, 1.109], undo 1.108 [1.043,
  1.205], bytecode calls 0.998 [0.968, 1.071], cons allocation 1.096
  [1.048, 1.237], interpreted-to-native 1.219 [1.122, 1.298]; `152e76f`'s
  build on the same host, interpreted-to-native 1.250 and cons 1.107, so
  it is the host and not the round. GNU's own rounds range over ±25%
  there. `llvm-mca -mcpu=cascadelake` on one traced cons-allocation
  iteration puts Emaxx at 0.88 of GNU's cycles (0.83 for Sapphire
  Rapids), against the wall's 1.10: the excess is outside the core's
  model (Emaxx's iteration touches 88 code lines to GNU's 58, and fewer
  stack slots and calls than GNU's). Instruction counts are what carry
  over between hosts; on this one a workload needs its count well below
  GNU's to pass.
- **Cons allocation's timed samples collect in Emaxx and not in GNU.**
  `core-perf-run` calls `garbage-collect` before each sample. GNU's
  collection there still finds the previous sample's 2,000,000-cons
  list (`heap_before` reports 2,017,551 conses; its `Flet` evaluates the
  first binding, `(garbage-collect)`, with the next bindings' stack
  slots still holding the last iteration's values, and the stack scan is
  conservative), so with `gc-cons-percentage` 1.0 its next threshold is
  about 32 MB and the 32 MB sample never collects. Emaxx's collection
  frees the list (15,806 live conses), its threshold is small, and each
  sample collects twice, about 0.04 s of 0.5 (8%). The rest of the
  sample is at parity on this host; the collector itself is no slower
  (a first sample from a fresh start: Emaxx 2 collections in 0.049 to
  0.073 s, GNU 3 in 0.074 to 0.086). Emulating the stale-slot retention
  is no fix; the margin has to come from the evaluator.
- **Round 22: the buffer-position ops in the verified loop.** `Bpoint`,
  `Bpoint_max`, `Bpoint_min` and `Bcurrent_buffer` (0o140, 0o144, 0o145,
  0o160) left `run_verified` as slow ops, through `op_in_place` (about
  120 instructions each, with a second decode and dispatch). They are
  arms of the loop now, pushing the current buffer's PT, ZV or BEGV, or
  the buffer, through its `RefCell` borrow; nothing there runs Lisp.
  Instructions: undo 1,325,896 -> 1,300,268 a unit; bytecode-to-native
  393.0 -> 391.6 an iteration and bytecode calls 435.7 -> 437.8, inside
  the ±2 an iteration that bytecode calls moves between runs of one
  build (undo's unit moves about ±5K). Two variants measured worse:
  reading the buffer with no borrow-flag test (undo 1,305,620, bytecode
  calls 438.6), and `Bvarref` of a variable that is not plain
  (buffer-local, forwarded, aliased) resolved by `find_symbol_value` out
  of line instead of through `op_in_place`, with its `void-variable`
  raised as the full arm's (undo 1,296,236, 4K below the round, but
  bytecode-to-native 395.9 and bytecode calls 439.4 and 441.1 in two
  builds: `run_verified`'s register allocation again; patch
  `docs/handover/2026-10-15-performance-round/rejected-varref-in-place.patch`).
  Validation (gate profile, `--test-threads=1`): 195 tests naming
  point, narrowing, `current-buffer` or byte code and the lightweight
  group's 598 pass; clippy `-D warnings` and rustfmt pass.
- **Round 23: a native-compiled function of fixed arity is eval_sub's
  subr.** A form whose head's function cell holds a native-compiled
  function of at most six arguments in registers (`register_counts`,
  lexical binding) is called as eval_sub's SUBRP branch calls one:
  - the arity is checked before any argument is evaluated, the error
    naming the form's symbol;
  - the positions up to the maximum are evaluated into slots (a missing
    optional is nil);
  - the frame shows the arguments given;
  - the call is `call_function_inline`'s.
  It took `eval_call`'s general way before, which resolved the cell again
  and evaluated the arguments first. It also signaled `(wrong-number-of-arguments
  native-compiled-function N)`, two differences from GNU that the new
  oracle test pins (`source_calls_of_fixed_native_functions_are_eval_sub_subr_calls`).
  The path, with `eval_call` for every other head that is no C primitive,
  lies in `eval_cons_call`, out of eval_sub. Two placements inside
  eval_sub (the native test after the subr arms, then sharing their
  function-cell load) cost cons allocation 84 to 112 instructions a unit
  through eval_sub's register allocation, all of it in eval_sub (2.2 to
  5.5 a call). With the profiler (`EMAXX_PROFILE`) on, the call goes the
  general way as before. A pending-termination test before the call,
  which `call_function_value_named` makes, cost 8 a unit and was dropped:
  termination comes due only inside an argument's evaluation, which then
  returns its error, and the C-primitive path has no such test either.
  Instructions a unit against round 22's build (interpreted-to-native
  and cons allocation on the final code, the rest on the variant without
  the profiler test): interpreted-to-native
  2,542.6 -> 2,428.9 (GNU 2,387), native-to-interpreted 4,223.9 ->
  4,097.2, cons allocation 5,046.1 -> 5,055.7, undo 1,306,776 ->
  1,301,605 (inside its noise). The general call is one function further
  away, so: interpreted calls 3,074 -> 3,161, interpreted-lexical 3,460 ->
  3,480, -dynamic 3,147 -> 3,172, list traversal 255,581 -> 256,101,
  bindat 871,879 -> 891,245 (+2.2%). All of these run at 0.56 to 0.81
  of GNU's wall time. Validation (gate profile, `--test-threads=1`):
  2,053 tests (all 1,677 of `lisp::eval::`, and every test naming
  native, backtrace, funcall, arity, lambda or closure) pass but the two
  known container-only failures below
  (`byte_compile_file_reports_an_unwritable_target_like_gnu` as root,
  `batch_startup_image_round_trip_or_explicit_native_image_limit`), and
  the lightweight group's 598 pass. Clippy `-D warnings` and rustfmt
  pass.
- **Simulated branch mispredictions (cachegrind `--branch-sim=yes`, a
  per-address two-bit predictor with no global history; no PMU on this
  host).** Per unit, against GNU:
  - interpreted-to-native: instructions 2,350 (PGO) and 2,420 (plain),
    GNU 2,422; mispredictions 17.4 (PGO) and 9.8 (plain), GNU 9.0.
  - cons allocation: instructions 4,926 (PGO) and 5,059 (plain), GNU
    5,295; mispredictions 36.9 (PGO) and 28.9 (plain), GNU 24.0.
  Fewer loads and stores than GNU in both. In the model:
  - The variable lookup (`lookup_symbol_plain`, the alist walk GNU's
    `Fassq` makes too) misses 5 times a unit in the plain build and 14
    in the PGO build.
  - eval_cons's subr dispatch (`maximum @ 0..=8`) misses about 10 times
    in 16, because the loop body alternates fixed-arity subrs and
    special forms.
  A predictor with global history may learn both, so the model may
  overstate them. The wall comparison of the two builds is what decides.
- **Round 24: native arity errors as GNU signals them.** The loader's
  arity test (`check_arity`) and the binding of a dynamic-binding native's
  lambda list signaled `(wrong-number-of-arguments native-compiled-function
  N)`. GNU's funcall_subr and funcall_lambda signal with the function
  object, `#<subr NAME>`, and the loader does too now
  (`wrong_number_of_arguments`). A form whose head reaches a lexical
  native other than through the round 23 path (through an alias, with
  more than six arguments, a MANY native, or the profiler on) had its
  arguments evaluated before that test. `eval_call` now checks the
  arity first, as for a C primitive (MANY's minimum included), naming the
  form's symbol. A dynamic-binding native stays apply_lambda's: arguments
  first, then the subr in the error. The new oracle test
  `native_arity_errors_name_the_callee_as_gnu_does` covers funcall,
  apply, an alias, MANY and a dynamic native, and the loader's own test
  now expects the function object. Instructions a unit: interpreted
  calls 3,162.8 -> 3,173.8 (`eval_call`'s native test for every closure
  call); interpreted-to-native 2,420.5, bytecode-to-native 392.3 and
  native-to-interpreted 4,093.2, unchanged within their noise.
  Validation (gate profile, `--test-threads=1`): 618 tests naming
  native, arity, funcall, apply, alias, subr, lambda, closure or calls
  (the loader's and the native runtime's among them) pass but the
  known container-only image round trip, and the lightweight group's
  598 pass. Clippy `-D warnings` and rustfmt pass.
- **Wall pilots of the build kinds on this host: too noisy to rank
  them.** Three pilots of the five tight workloads (`pilot3.py`-style:
  the contract's sizes, one warmup and three samples a process, five or
  seven rounds, order rotating), on rounds 23 and 24's source, compared:
  - the profile-guided build;
  - the same build with `run_verified` dropped from the profile
    (`llvm-profdata merge --no-function=2vm12run_verified`, so the
    byte-code loop is compiled as in the plain build);
  - the plain build.
  Median ratio to GNU, pilot by pilot:
  - interpreted-to-native: PGO 0.970, 0.946, 1.121; plain 1.014, 1.065,
    1.090.
  - cons allocation: PGO 1.011, 0.943, 1.074; plain 1.103, 1.115, 1.119.
  - undo: PGO 1.005, 1.141, 1.072; plain 1.217, 1.193, 1.231.
  - bytecode-to-native: PGO 1.085, 1.050, 0.989; plain 1.014, 0.887,
    0.981.
  - bytecode calls: PGO 0.997, 0.954, 1.043; plain 0.942, 1.015, 0.973.
  - without `run_verified`'s profile (second and third pilots):
    bytecode-to-native 0.921 and 0.996, bytecode calls 0.952 and 1.007,
    interpreted-to-native 0.774 and 1.121, cons allocation 1.132 and
    1.093, undo 1.089 and 1.073.
  Single rounds ranged from 0.66 to 1.54, and GNU's own medians moved by
  up to 40% between pilots (interpreted-to-native 0.50 to 0.70 s).
  Each process's CPU time (`getrusage`, the third pilot) varied about as
  much, so the variance comes from the host, not from stolen time
  (steal is about 3% over the uptime). The profile-guided build is
  consistently ahead on undo and cons allocation, the plain one roughly
  even or ahead on the two byte-code workloads. Dropping
  `run_verified`'s profile showed no consistent gain, and
  `tools/pgo-build.sh` is unchanged. On this host a few percent between
  builds needs far more rounds than a pilot. Instruction counts remain
  the guide.
- **Where the profile-guided build stands, by cachegrind's
  simulation** (rounds 23 and 24's source; per unit, against GNU).
  - Instructions: undo 1,185,265 (GNU 1,259,204, 0.94),
    bytecode-to-native 382.6 (388.0), bytecode calls 437.9 (468.0),
    interpreted-to-native 2,350 (2,422), cons allocation 4,926 (5,295).
    The timed build runs fewer instructions than GNU in all five.
  - Undo's L1 instruction-cache misses: 39,711 against GNU's 21,140
    (the plain build 59,810). Its data misses are 13,451 reads and 3,911
    writes against 9,799 and 1,702, and its conditional mispredictions
    17,146 against 13,389.
  - The byte-code VM's functions alone miss about 8,170 times a unit
    against GNU's `exec_byte_code`'s 1,669: `run_verified` 4,712,
    `op_in_place` 1,985, the rest in `run_entered`, `call_plain`,
    `bytecode_call` and `run_loop`. `run_verified` is 46 KB in the plain
    build (49 KB profile-guided) where `exec_byte_code` is 12 KB. About
    18 KB of it is the inlined native-call path (`invoke_nested_fixed`
    and the frame record around it, once per argument-count
    specialization), which undo never runs; the profile places it cold.
    eval_sub is 14 KB, plus `eval_cons_call` 15 KB, against GNU's
    2.8 KB.
  - Machine function splitting (`-Cllvm-args=-enable-split-machine-functions`
    on the same profile, 3,295 functions split) left undo's misses at
    38,989. They come from the breadth of hot code undo executes, not
    from cold blocks inside it.
  Undo's remaining wall excess is most plausibly this footprint, the
  amount of distinct code an iteration touches. Narrowing the hot paths
  of the VM and the evaluator is the lever, not the instruction count.
- **The contract's comparison of the five tight workloads on
  `e053900`'s profile-guided build** (this host, `contractlike.py`:
  full sizes, nine rounds, the locked bootstrap), median and 95% interval
  of the paired ratios, with `aa68c68`'s on the same host in
  parentheses:
  - interpreted-to-native 1.033 [0.954, 1.201] (1.219);
  - cons allocation 1.072 [0.987, 1.105] (1.096);
  - undo 1.021 [1.008, 1.053] (1.108);
  - bytecode-to-native 1.010 [0.912, 1.148] (1.018);
  - bytecode calls 0.982 [0.958, 1.059] (0.998).
  All five are still over the 1.03 bound. With this host's spread a
  median near 0.93 is what passes. About 8 points of cons allocation's
  excess are its two collections a sample (bullet "Cons allocation's
  timed samples collect in Emaxx and not in GNU").
- **Round 25: jumps padded clear of 32-byte boundaries.** This host's
  cores (Intel model 85, Cascade Lake) carry Intel's jump conditional
  code erratum: with its microcode fix, a jump (or a fused compare and
  jump) that ends on or crosses a 32-byte boundary keeps those 32 bytes
  out of the decoded-instruction cache, so the front end decodes them
  again each time. That fits what was left unexplained above: the
  profile-guided build ran fewer instructions, loads and stores than GNU
  and the core's model put it at 0.88 of GNU's cycles, yet the wall
  stood above. `.cargo/config.toml`'s `build.rustflags` now adds LLVM's
  padding for it, `-x86-branches-within-32B-boundaries`. It goes there
  rather than in the workspace wrapper because Cargo fingerprints those
  flags and not the wrapper's (a wrapper change rebuilt nothing,
  locally or in CI's cached targets). A target-specific table would
  replace `build.rustflags`, the profile-guided script's among them.
  The option is the x86 backend's, and rustc's LLVM carries every
  backend: aarch64 Linux and Darwin objects build with it.
  - The contract's comparison of the profile-guided build with the
    padding, the same source and profile as the bullet above:
    interpreted-to-native 0.927 [0.866, 0.998] and cons allocation 0.821
    [0.735, 0.946] pass the bound; bytecode calls 0.928 [0.819, 1.062]
    and bytecode-to-native 0.953 [0.898, 1.108] are over only by their
    spread; undo 1.042 [0.876, 1.110] is unchanged within it.
  - A pilot the same day: padded over unpadded CPU time 0.934, 0.843,
    1.006, 0.939 and 0.975 for those five in the same order.
  - The binary grows by 2% (15.56 MB against 15.25). The padding is
    segment-override prefixes and NOPs in front of jumps, so instruction
    counts from here on include the executed padding. The plain build's
    per unit against the round before: interpreted-to-native 2,420.5 ->
    2,477.4, cons allocation 5,056 -> 5,158, bytecode-to-native 392.3 ->
    406.5, bytecode calls 437.8 -> 449.9, undo 1,301,605 -> 1,341,618
    (2 to 3.6% more). Compare later rounds against these, and GNU's
    counts with that in mind.
  On cores without the erratum (the earlier Emerald Rapids host, AMD)
  the padding only costs the code it adds.
- **The other eleven workloads on the padded profile-guided build**
  (`contractlike.py`, this host, the same binary as the bullet above):
  interpreted-lexical 0.688 [0.541, 0.822], interpreted-calls 0.627
  [0.595, 0.722], native execution 0.635 [0.518, 0.700],
  native-to-interpreted 0.810 [0.709, 0.903], native-to-bytecode 0.781
  [0.714, 0.937], list traversal 0.822 [0.683, 0.970], mapcar 0.865
  [0.716, 1.001], explicit GC 0.721 [0.698, 0.776], sort 0.867 [0.724,
  0.962] and bindat 0.715 [0.682, 0.771] pass; interpreted-dynamic
  0.881 [0.813, 1.037] is over through one round (1.256), with both
  programs' own rounds ranging from 0.41 to 0.75 s. Over the bound:
  undo, bytecode-to-native, bytecode calls, interpreted-dynamic.
  Instructions a unit of this build (callgrind, two sizes), the
  baseline for later profile-guided comparisons: undo 1,225,870,
  bytecode calls 453.8, bytecode-to-native 390.4, native-to-bytecode
  3,831.5, interpreted-lexical 3,349.8, interpreted-dynamic 3,094.8,
  interpreted-calls 3,170.6, interpreted-to-native 2,453.6, cons
  allocation 5,153.8, list traversal 258,100, mapcar 28,327,564, sort
  216,160,065, bindat 819,220, native-to-interpreted 3,908.8, native
  execution 371,239,340, explicit GC 33,232,400.
- **Undo's time, split.** The harness's own report gives each sample's
  collections. GNU collects 4 times a sample (0.11 to 0.17 s of a 0.42
  to 0.66 s sample), Emaxx 3 times (0.08 to 0.11 s). So Emaxx's
  collector costs less, and the rest of the sample is where it loses:
  GNU 0.30 to 0.50 s, Emaxx 0.37 to 0.60 s, medians 1.15 to 1.4 times
  GNU's over three processes of nine samples. Callgrind's per-unit
  counts (10 against 20 units) fall between collections and so measure
  that rest alone: the profile-guided build runs 1,225,870 instructions
  a unit against GNU's 1,259,204. The difference is per instruction.
  - Footprint: the instructions run at least once a unit are 55,352 in
    4,932 64-byte lines (308 KB) over 381 4-KB pages, against GNU's
    27,838 in 2,853 lines (178 KB) over 216 pages. Each distinct
    instruction runs about 22 times a unit against GNU's 45.
  - A cycle model on cachegrind's counts (Ir/3 + 10 I1mr + 10 D1mr +
    5 D1mw + 15 Bcm + 15 Bim) gives 1,345K against 1,098K, 1.22, the
    measured ratio. The byte-code VM is over half of the excess:
    `run_verified` 257K against `exec_byte_code`'s 210K, `op_in_place`
    80K (no counterpart), `call_plain` 36K, `run_entered` 20K.
  - Inlined standard-library code is 28% of the hot instructions in the
    plain build: `Vec`/`RawVec` 3,436 (most in the primitives, the text
    property spans and `run_verified`), pointers, slice iterators,
    `Option`, `RefCell` borrows. `lisp/types.rs`'s value helpers are
    another 4,250.
  - The ops that leave the verified loop for `op_in_place`, 622 a unit:
    Bvarref of a variable that is not plain 98, Bunbind 77, Bvarbind 65,
    Bmemq of an element neither fixnum nor symbol 39, Bgoto_char 38,
    Bswitch 31, Bget 26, Bsave_current_buffer 24, Bset_buffer 24, Baset
    24, then Bunwind_protect, Bdelete_region and Baref 16 each.
  - Tried, none kept (the patch of the second is in the scratch
    history only):
    - One arm for Bcall0 to Bcall5 (one copy of the call sequence):
      undo -4,400 instructions and -98 hot lines, but bytecode calls
      +20.2 and bytecode-to-native +8.9 an iteration (plain build).
    - Arms in `run_verified` calling the full arms' code out of line,
      resuming as after a call, for Bvarref and Bvarset of other
      variables, Bvarbind, Bunbind, Bget, Bgoto_char, Bset_buffer and
      Bsave_current_buffer. Plain build: undo -26,700, bytecode calls
      +11.9; with only the bind ops and primitives, undo -8,200 and
      bytecode calls +18.3. Profile-guided (trained afresh): undo
      1,225,870 -> 1,196,343, bytecode calls +4.8, bytecode-to-native
      +1.8, native-to-bytecode +5.8, but 88 more hot lines
      (`run_verified` 276 -> 324) and 770 more instruction-cache misses
      there: the model's cycles came to 1,343K against 1,333K. Fewer
      instructions bought with more code do not pay here.
    - Compiler options on the same profile: `-hot-callsite-threshold=600`
      changed nothing; `-pgso-lwss-only=false` cut the text by 3% and
      undo's hot lines by 3%; `-inline-threshold=100` with
      `-inlinehint-threshold=150` cut the text by 5% but not undo's hot
      lines (+1% instructions; bytecode calls 453.8 -> 444.0,
      bytecode-to-native 390.4 -> 385.8, and of the other twelve
      interpreted-calls +1.5%, native-to-interpreted +1.7%, native
      execution +2.6%, mapcar -1.1%, sort +1.1%, the rest within 1%);
      linking with
      `-z keep-text-section-prefix` gave a 2.75 MB `.text.hot`, but undo's
      working set straddles it and `.text` (the training is GNU's tests),
      397 pages against 381.
    - The text's 2-MB-aligned interior on anonymous huge pages (a test
      preload, not the build): undo's medians without collections
      0.411, 0.454 and 0.422 s against 0.418, 0.502 and 0.444: the
      instruction TLB is a few percent at most.
    - A profile that also trained on undo itself (a diagnostic only:
      the timed workloads stay out of the real training): hot lines
      4,932 -> 4,832, pages 381 -> 360, instruction-cache misses 40,354
      -> 39,170. Layout is worth 3% of the misses at most.
    - Each heavier special form out of line (`#[inline(never)]` on
      `let`, `let*`, `catch`, `unwind-protect`, `condition-case`, the
      `save-` forms, `defvar`, `defconst`): `eval_native_form`'s frame
      448 -> 144 bytes in the plain build, but 0.1 to 1.4% more
      instructions in the interpreted workloads and no gain in undo's
      misses that the run-to-run variation (one-off regex compilations
      and symbol-cache work landing in one measured range) does not
      cover.
  - The frames are larger than GNU's throughout (profile-guided build,
    bytes including saved registers): `eval_sub` 400 (GNU 192),
    `eval_native_form` 576 (`Flet` 112), `eval_cons_call` 464,
    `run_verified` and `run_entered` 464 each (`exec_byte_code` 160),
    `op_in_place` 1,152, `call_function_value` 496, `bytecode_call` 432,
    `run_loop` 608. A Lisp call level stacks about 2.3 KB against GNU's
    0.5 KB, which deep recursion (`cconv`, `macroexpand-all`) carries
    past the 32 KB data cache; undo's L1 data misses are 13,895 reads
    and 4,140 writes a unit against 9,799 and 1,702.
  - Closure conversion: `cconv-make-interpreted-closure`, the same
    native code in both, takes 107,141 instructions a unit inclusive
    against GNU's 66,879. The difference is the runtime entries its
    code calls: `invoke_native_funcall` 157 calls at 173 instructions of
    its own (GNU's `Ffuncall` and `funcall_subr` about 100 together),
    `invoke_subr` 146 at 95 (`symbolp` 49 of them, `symbol-function`
    10, `delq` 9), `mapcar` 18 at 480, `list` 43 at 200 (about 35 a
    cons). No single step dominates any of them.
  Undo's excess is the amount of distinct code an iteration runs, spread
  over the VM, the evaluator and the primitives. Measure changes for it
  by the hot-line count and the model, not by instructions alone.
- **Round 26: every kind of jump padded, with prefixes.** Round 25's
  `-x86-branches-within-32B-boundaries` pads conditional and direct
  jumps and fused compare-and-jumps. The erratum covers indirect jumps,
  calls and returns too, and the byte-code dispatch's indirect jump at
  the head of the loop ended on a boundary. The padding was also NOP
  instructions in front of a jump, which execute; GNU's assembler pads
  by adding prefixes to the instructions before it.
  `-x86-align-branch=fused+jcc+jmp+indirect+call+ret` adds the other
  kinds and `-x86-pad-max-prefix-size=5` lets the assembler pad with up
  to five prefixes on an instruction. Profile-guided builds of round
  25's source and profile, against round 25's padded one:
  - Instructions a unit (callgrind): bytecode calls 453.8 -> 443.3,
    bytecode-to-native 390.4 -> 389.1, interpreted-to-native 2,453.6
    -> 2,435.0, cons allocation 5,153.8 -> 5,086.5, undo 1,225,870 ->
    1,206,561. With the prefixes alone (the jump kinds as before):
    444.7, 387.7, 2,403.5, 5,060.8 and 1,194,007.
  - Wall (`pilot3.py`: each round's median of three samples a process,
    the paired median over fifteen rounds and the range of the rounds'
    ratios): bytecode-to-native 0.910 [0.789, 1.031] against 0.984
    [0.863, 1.283], bytecode calls 0.916 [0.762, 1.077] against 0.948
    [0.816, 1.129], undo 1.110 [0.815, 1.275] against 1.113 [0.926,
    1.432] (the prefixes alone: 0.930, 0.930, 1.107); fifteen more
    rounds of the other two: interpreted-to-native 0.891 [0.786, 1.102]
    against 0.924 [0.776, 1.107], cons allocation 0.823 [0.702, 1.427]
    against 0.872 [0.786, 1.354]. A three-round pilot before them had
    cons allocation the other way (0.837 against 0.774).
  - The plain build's undo: 1,341,618 -> 1,335,042 a unit.
  The options are the x86 backend's: aarch64 Linux and Darwin objects
  (and x86-64 Windows ones) build with them (a `no_core` crate, Rust
  1.99.1). Validation (gate profile, `--test-threads=1`): the 618
  native, arity, funcall, apply, alias, subr, lambda, closure and call
  tests of round 24 pass but the known container-only image round
  trip, and the lightweight group's 598 pass; clippy `-D warnings` and
  rustfmt pass.
- **What undo's comparable work costs.** By the inclusive costs of the
  natively compiled Lisp both run (`fincl2.py`, profile-guided build),
  GNU spends about 150,000 instructions a unit in window-size Lisp that
  Emaxx does not run: `frame-windows-min-size` 100,452 (under
  `set-window-configuration`'s frame resize), `window-size-fixed-p`
  28,588, `replace-buffer-in-windows` 18,288 (under `kill-buffer`),
  and smaller ones. GNU's `Fset_window_configuration` is 140,244
  inclusive against Emaxx's `restore_window_configuration` 5,680, its
  `Fkill_buffer` 66,726 against `kill_buffer_id` 12,453. Without them
  Emaxx runs about 14% more instructions than GNU for the same work,
  where the totals read 0.96. A more faithful window layer would add
  that work back. Where the rest goes (self costs a unit, calls and
  instructions a call):
  - The byte-code VM and the native runtime's glue: `run_verified`
    177,500 (123 entries, 1,443 each; GNU's `exec_byte_code` 198,462 in
    128 at 1,550), `op_in_place` 87,178 (622 at 140, the ops GNU runs
    inline), `call_plain` 59,214 (318 at 186), `invoke_subr` 32,574 (79
    at 412, the profile inlines callees into it; plain build 146 at 95,
    where GNU's compiled code calls the C function), `run_entered`
    21,628, `direct_native_funcall` 11,169: about 125,000 over GNU's
    `exec_byte_code`, `funcall_subr`, `Ffuncall` and `funcall_general`.
  - The evaluator: `eval_sub` 575 calls at 198 against GNU's 862 at
    104, `eval_native_form` 277 at 182, `call_function_value` 85 at
    245, `eval_cons_call` 50 at 328, variable lookups
    (`lookup_symbol_plain`, `symbol_value_key`, `lookup_var_key`) 73
    to 125 a call against `find_symbol_value`'s 17.5. GNU's buffer-local
    swapping (`set_buffer_internal_2`, `store_symval_forwarding`,
    `swap_in_symval_forwarding`, `reset_buffer_local_variables`) and
    `assq` on the lexical environment cost it more than Emaxx's
    counterparts, so the evaluator as a whole is not the excess.
  - `delete-region` 4,211 a call against GNU's 2,825 (18 a unit):
    the change hooks 894 a call each way, `Buffer::delete_region` 1,691
    against `del_range_2`'s 1,083.
- **Round 27: undo's slow ops resume in the loop, and by-name lookups
  trimmed.** Instructions a unit in the plain build (round 26's in
  parentheses): undo 1,298,608 (1,335,042), bytecode calls 451.9 (450.1),
  bytecode-to-native 403.4 (401.5), interpreted-to-native 2,455.8
  (2,474.8), native-to-bytecode 4,098.8 (4,117.0).
  - An op `run_verified` leaves to `op_in_place` (622 a unit in undo)
    that goes on to the next instruction and stored into no code string
    now answers `FastExit::Continued`, and the loop resumes at it with
    the bytes read again, as after a call (`code_changes` unchanged),
    instead of through the frame-switch exit and its verification check:
    undo -10,100. Resuming inside each slow arm instead (the `slow!`
    sites continuing the loop themselves) saved 14,300 but cost bytecode
    calls 5.0 and bytecode-to-native 2.5 an iteration (the stack base
    lost its register); returning the stack pointer from `op_in_place`
    made that worse (8.4 and 5.6). The exit-path form costs bytecode
    calls 1.8 and bytecode-to-native 0.8, register allocation alone.
  - Bmemq runs in the loop for any element but a symbol with position
    (strings, floats and conses compare by the word, as `eq` does). Undo's
    39 slow Bmemqs a unit are not of that kind: its count did not move.
  - Bvarref of a buffer-local or forwarded variable
    (`symbol_value_cell_symbol`, 55 a unit) reads
    `find_symbol_value_resolved` directly instead of a lexical lookup in
    an empty environment; `has_feature` (`featurep`, `require`) stops at
    the first match, where it compared every element's name (5,214 a call
    to about 2,200); `buffer-string` and `buffer-substring` copy a small
    text's bytes directly, where a rope slice of them counted their chars
    and lines first (327 a call, 17 a unit); `is_forwarded_eval_cell`
    (16 a unit, 237 a call, under `set_buffer_local_value_symbol`)
    compares the names before reading the editor's key table;
    `set-buffer-multibyte`'s undo entry names its two symbols through
    the key table. Together about undo -20,000.
  - Comparing `featurep`'s symbol by its word failed
    `startup_features_own_their_capability_metadata_in_one_manifest`:
    the `features` list can hold another symbol object of the name
    `SymbolName::find` returns, so names are compared still.
  Validation (gate profile, `--test-threads=1`): 851 tests: round 26's 618
  and every test naming byte code, `memq`, features, `require`,
  `buffer-substring`, `buffer-string`, `symbol-value`, undo or Bvarref
  pass but the known container-only image round trip and the as-root
  `byte_compile_file_reports_an_unwritable_target_like_gnu`; the 52
  feature, `memq`, forwarded-variable and buffer-text tests again after
  the name comparison, with the new oracle test for `featurep` of an
  uninterned symbol; the lightweight group's 598. Clippy `-D warnings`
  and rustfmt pass.
- **Round 28: round 27's Bmemq arm undone.** Round 27's profile-guided
  build ran bytecode-to-native slower than round 26's with about the same
  instructions (389.0 an iteration against 387.6): two twelve-round
  pilots read 1.068 and 1.104 of GNU's time against round 26's 0.855
  and 0.923, and the contract-like comparison 1.075 [0.946, 1.159].
  Cachegrind's branch model finds one more conditional mispredict an
  iteration in `run_verified`, at the empty-handler test of
  `finish_nested_frame` (inlined into it), a branch taken every time.
  Round 27 with only the Bmemq arm's element test put back (a fixnum or a
  symbol in the loop, anything else to the full arm, instead of anything
  but a symbol with position) reads 0.954 in the same pilot, 386.4
  instructions and no extra mispredict: the arm's change moved the
  profile's layout of the loop, not its work. Bmemq takes fixnums and
  symbols in the loop again; undo's slow Bmemqs had not moved with the
  change (bullet "Round 27"), and the memq test's cons found by identity
  now goes through the full arm. Plain build, instructions a unit
  (round 27's in parentheses): undo 1,312,240 (1,298,608; its rounds
  vary by about 1% with where collections fall), bytecode calls 448.7
  (451.9), bytecode-to-native 403.7 (403.4); interpreted-to-native
  2,477.8 and native-to-bytecode 4,108.0 have round 27's costs function
  by function.
  Validation (gate profile, `--test-threads=1`, `RUST_MIN_STACK` at
  128 MiB as the config sets it for cargo): the tests naming `memq`,
  byte code, features, `buffer-substring`, `buffer-string`,
  `symbol-value` or undo (142 runs; a test can match two of the names),
  and the lightweight group's 598. Clippy `-D warnings` and rustfmt
  pass.
- **Where undo's wall time goes, round 28's profile-guided build.** The
  contract-like comparison (nine rounds): bytecode-to-native 0.936
  [0.887, 0.962], interpreted-to-native 0.907 [0.816, 0.978] and cons
  allocation 0.841 [0.760, 0.884] pass; bytecode calls 0.973 [0.893,
  1.062] and undo 1.136 [1.028, 1.218] are over. A twelve-round pilot of
  undo with rounds 26 to 28's builds side by side reads 1.063, 1.123 and
  1.036, so the contract-like 1.136 is the host's drift between runs, not
  the round. At the contract's size (n = 1000, five rounds of three
  samples, medians) undo takes GNU 0.614 s with four collections of
  0.124 s in all, Emaxx 0.669 s with three of 0.082 s: Emaxx's
  collections cost less, and the excess is the code between them, 0.585 s
  against 0.490 s (1.19), for fewer instructions (1,191,948 a unit
  against GNU's 1,259,204 in cachegrind). Per unit, Emaxx misses the
  instruction cache 39,616 times against GNU's 21,140, the first-level
  data cache 14,310 and 4,157 (reads, writes) against 9,799 and 1,702,
  and mispredicts 17,297 conditional branches against 13,389. The code
  an undo unit runs at least once (half a time a unit or more) is 4,985
  64-byte lines and 54,765 distinct instructions against GNU's 2,861 and
  27,956, on 355 four-KB pages against 161: twice the distinct code, on
  a tail of functions none of which holds more than 6% of it
  (`run_verified` 279 lines against `exec_byte_code`'s 143, `op_in_place`
  165, `eval_native_form` 149, then `invoke_subr`, `prin1`, the text
  properties, `format`, `eval_cons_call`, `eval_sub`, ...). By how often
  an instruction runs, the excess is warm code: run 2 to 100 times a
  unit, Emaxx executes 766,000 instructions on 4,464 lines against GNU's
  388,000 on 2,615, while GNU runs more of its work in code run a
  hundred times or more (883,000 instructions on 243 lines against
  458,000 on 188, its window-size Lisp among them). Of the warm lines,
  the byte-code VM holds about 300 more than `exec_byte_code`
  (`run_verified` 276, `op_in_place` 156), the evaluator about 210 more
  than GNU's (`eval_native_form`, `eval_sub`, `eval_cons_call`,
  `call_function_value` against `eval_sub`, `Flet`, `apply_lambda`,
  `unbind_to`), and the native code's calls into subrs (`invoke_subr`,
  `direct_native_funcall`, `runtime_specbind`, `runtime_unbind_n`) about
  130 more than `funcall_subr`, `specbind` and `unbind_to`: Emaxx's
  specialized paths for each kind of call and form, each taken a few
  times a unit, where GNU's generic ones run the same instructions every
  time and stay cached. The primitives themselves are not the excess
  (inclusive a unit: `insert` 43,000 against `Finsert`'s 99,000,
  `kill-buffer` 23,800 against 66,700, the message log 37,000 against
  46,700). Code-generation options do not move it: on round 28's profile, LLVM's hot call-site
  inlining threshold at 1,000 instead of 3,000 left the misses at 40,318
  (text 24 KB smaller), and ext-TSP block placement at 39,107 with a
  twelve-round pilot of undo 1.064 -> 1.044, bytecode calls 0.905 ->
  0.924, bytecode-to-native 0.917 -> 0.874, interpreted-to-native 0.886
  -> 0.917, cons allocation 0.862 -> 0.857 (within the host's noise both
  ways, as on `df06b81`). Opt-level 2 (text 1.5% smaller) left them at
  40,906 for 1.7% more instructions. Opt-level `s` (text 15% smaller)
  loses everywhere in a ten-round pilot: undo 1.072 -> 1.276,
  interpreted-to-native 0.884 -> 1.029, bytecode calls 0.905 -> 0.972,
  bytecode-to-native 0.917 -> 0.931, cons allocation 0.832 -> 0.879 (its
  string moves carry the padding's segment prefix, which valgrind cannot
  decode, so it has no cachegrind count). The distinct code has to shrink
  without more instructions: undo's hot paths themselves, not the
  compiler's settings.
- **Round 29: `quote` and `function` of one argument without their
  frame, and the printer's variables by key.** Of the 277 special forms
  undo evaluates a unit, 60 are `quote` and 13 `function` (10 of them of
  a symbol), each through eval_sub's special-form path: the frame record
  pushed and popped, the arguments counted, `eval_native_form`'s
  prologue and match, for a body that returns its argument. Neither runs
  anything a frame could be seen from (Fquote, and Ffunction of no lambda
  list, return XCAR (args)), so `eval_special_form` returns the argument
  before recording the frame; the depth, quit and collection tests of
  eval_sub come before it as they did. `eval_native_form` 277 -> 207
  calls a unit, `eval_sub` and it about 4,000 instructions a unit fewer.
  The form's kind is tested first, so other special forms pay one
  compare: interpreted-calls 3,239.5 -> 3,230.2, interpreted-lexical
  3,553.7 -> 3,566.3, interpreted-dynamic 3,246.4 -> 3,251.4 a unit.
  `function`'s two variables (`lexical-binding`,
  `internal-make-interpreted-closure-function`) and six of the
  printer's are read by symbol key instead of by name (undo's lookups by
  name 19 -> 14 a unit). Undo's total, 1,311,122 a unit against round
  28's 1,312,240, moves within its run-to-run variation. New oracle
  test: one-argument `quote` and `function`, the quoted object's
  identity, and the argument-count errors of the full path.
  Validation (gate profile, `--test-threads=1`, 128 MiB stacks): the
  tests naming `quote` (64), `function` (131), lexical (55), closure
  (54), print (41), backtrace (16), debug (14) and special forms (8) pass
  but `print_prunes_charset_properties_like_print_prune_string_charset`,
  which fails in this container on round 28's binary as well (see the
  local failures with the CI record); the `eval::` module's run was
  stopped at fifty minutes. The lightweight group's 598 pass; clippy
  `-D warnings` and rustfmt pass.
- **Round 30: `features` by the symbol, the window points without a
  copy, and the lexical lookup without a compare.** undo-test1's
  `(require 'facemenu)` runs once a unit. `require` and `featurep` looked
  for the feature's name in `features` by text, a string compare an
  element (`has_feature` 3,980 instructions, 4,790 with its callees),
  interned the name again for the return value, and copied
  `current-load-list` into a vector outside any load. They now take the
  symbol (`checked_symbol_identity`) and test each element's word, as
  Fmemq does in Ffeaturep and Frequire (2,218 instructions: the list is
  long, and GNU walks the same one); `current-load-list` is read by key,
  and an empty one ends the load-history record. By the word,
  `(featurep nil)` is t when `features` holds nil, as in GNU (it was
  nil), and an uninterned symbol of a provided feature's name is not
  that feature. `has_feature` by name stays for the Rust callers that
  hold no symbol: a test's `Interpreter::new()` leaves its caller's
  obarray current, where the editor's symbols are not. Every insertion
  and deletion adjusts the windows' point slots, and the window ids were
  copied out of the type index into a SmallVec first (108 instructions,
  39 times a unit); the index is walked in place. The lexical lookup of
  a symbol under its own name compared the name with itself (36 memcmp
  calls a unit); the same slice is tested first. Undo a unit, in the
  functions changed: about 3,500 instructions fewer for `require`, 9,000
  for the window points and the lexical lookup (memcmp 342.5 -> 316.4
  calls); the total, 1,294,849 against round 29's 1,311,122, moves within
  its variation (a timer's run lands inside one of the two measured runs
  and not the other). Interpreted-calls and interpreted-lexical are
  unchanged function by function (3,251.8 and 3,562.2 a unit, the
  difference glibc's `malloc_consolidate`). An allocator experiment, small
  blocks up to 1 KB on the per-thread lists (16 deep above 256 bytes),
  cut libc malloc's calls 48 -> 35 a unit but raised `_int_malloc`'s 34 ->
  38 (fewer frees reach libc's own cache): no gain, not kept. Found on
  the way, not changed: GNU makes `features` non-special
  (`Fmake_var_non_special (Qfeatures)` in fns.c), so under lexical
  binding `(let ((features ...)) (featurep ...))` binds a lexical
  variable the primitive does not read; Emaxx binds it dynamically
  (`(let* ((s (make-symbol "x")) (features (list s))) (featurep s))` is
  t, GNU's nil). New oracle test: `featurep` and `require` by the
  symbol, nil as a feature, uninterned names, the wrong-type errors, with
  `features` set and restored rather than bound.
  Validation (gate profile, `--test-threads=1`, 128 MiB stacks):
  the tests naming `feature` (15), `require` (26),
  `provide` (12), `undo` (16), `marker` (35), `window` (92), lexical (55),
  closure (54), `delete_region` (2), `insert` (42) and `load_history` (4)
  pass; the lightweight group's 598 pass; clippy `-D warnings` and
  rustfmt pass.
- **Round 31: a buffer named with a space keeps no undo, as in GNU.**
  buffer.c:Fget_buffer_create sets `buffer-undo-list` to t in a buffer
  whose name starts with a space. Emaxx's were nil, so every
  `with-temp-buffer` buffer, the printer's ` *string-output*`, `load`'s
  ` *load*` and the coding detector's ` *auto-coding-work*` recorded undo
  GNU does not keep, and `(with-temp-buffer buffer-undo-list)` was nil
  where GNU's is t. `create_buffer` now disables undo for such a name.
  `buffer-enable-undo` ignored its argument (it enabled the current
  buffer's undo whatever BUFFER was); it takes BUFFER through
  Fget_buffer as GNU's C does, with nsberror's "No buffer named NAME" and
  the stringp type error. `buffer-local-value` of `buffer-undo-list` read
  the locals' alist, not BUFFER's history (nil for a buffer keeping
  none); it reads the buffer's own list, BVAR (b, undo_list). Found while
  tracing undo's rebuilds of the Lisp view of the list (gdb, then a
  temporary diagnostic build): each test's fresh temporary buffer pushed
  its first 5 to 16 records with no view, rebuilt at the next read of
  `buffer-undo-list`, 35 records a unit. A view made for an empty history
  at creation moved that work into the pushes instead (undo 1,311,931 a
  unit against round 30's 1,294,849, the difference glibc's
  `malloc_consolidate` and the byte-code entry cache inside one of the
  two measured runs) and would make every buffer that never reads its
  list convert each record: not kept. Undo's instructions do not move
  with the round, its tests' buffers enabling undo themselves. New oracle
  test: undo in buffers named with a space, `generate-new-buffer`,
  `buffer-enable-undo` of another buffer and by name with its two
  errors, `buffer-local-value` of the list.
  Round 30's profile-guided build against round 28's, ten rotated rounds
  at the contract's sizes (median ratio to GNU and range): undo 1.050
  [0.972, 1.196] against 1.016 [0.953, 1.180], bytecode calls 0.925
  [0.846, 1.053] against 0.916 [0.872, 1.038], within the host's
  variation. Undo at n = 1000, three rounds: GNU 0.580 s with four
  collections taking 0.112 s, Emaxx 0.601 s with three taking 0.077 s;
  the code between collections 0.524 s against 0.469 (1.12), a collection
  26 ms against GNU's 28, 85% of it tracing (`trace_pending`). A
  Lisp-only undo list (GNU's representation) would remove about 22,700
  instructions and 99 of undo's 5,605 lines run at least half a time a
  unit: not attempted for that.
  Validation (gate profile, `--test-threads=1`, 128 MiB stacks):
  the tests naming `undo` (17), `buffer` (325), `temp`
  (27), `insert` (42), `delete` (28), `change` (44), `kill` (22),
  `erase` (1), `marker` (35), `propert` (126) and `format` (33) pass but
  `save_buffer_skips_unmodified_and_unchanged_files` (in `buffer` and
  `change`) and `print_prunes_charset_properties_like_print_prune_string_charset`
  (in `propert`), this container's two known failures (see the local
  failures with the CI record); the lightweight group's 598 pass;
  clippy `-D warnings` and rustfmt pass.
- **Round 32: three compares by name off undo's paths.**
  `run_change_hooks`, run on every insertion and deletion (78 times a
  unit), compared its hook's name with `before-change-functions` by
  text; its callers' literals are the same constants in the release
  build (fat LTO, one codegen unit), so the pointer is tested first and
  the text only otherwise. The store into a forwarded variable compared
  the variable's name with the three whose value it converts
  (`display-hourglass`, `scroll-up-aggressively`,
  `vertical-scroll-bar`) whenever the lengths matched
  (`inhibit-read-only` is `display-hourglass`'s length); their first
  letters are tested first. An undo record of a property change interned
  the property's name through the obarray's table each time; the
  address-keyed cache finds it. Undo a unit: memcmp 311.2 -> 207.2
  calls, `intern_str`'s table path 16 -> 9, about 2,500 instructions
  fewer net (the hooks' pointer test adds 12 a call); the total
  1,296,530 against round 31's 1,311,931, inside the run-to-run
  variation.
  Validation (gate profile, `--test-threads=1`, 128 MiB stacks):
  the tests naming `hook` (49), `change`
  (44), `undo` (17), `propert` (126), `forward` (51), `scroll` (10),
  `overwrite` (2) and `assign` (7) pass but
  `save_buffer_skips_unmodified_and_unchanged_files` (in `change`) and
  `print_prunes_charset_properties_like_print_prune_string_charset` (in
  `propert`), this container's two known failures; the lightweight
  group's 598 pass; clippy `-D warnings` and rustfmt pass.
- **Round 33: the printer's setup as print.c's.** `format`'s `%S`
  conversion (twice a unit in undo) cost 7,229 instructions a call
  before the object was printed, most of it setup: the nine
  `print_options` variables, the four string-escape flags and the
  escape binding of `print_prepare` were read through the evaluator's
  environment, three of them by name (`lookup_var`, interning each);
  `print` stored nil into `print-number-table` through `set_variable`
  by name (1,050 instructions) when it was nil already; and the
  objects being printed were kept in a SipHash map, inserted and
  removed around every list (780 a call). print.c reads these as C
  variables and stores the table's nil directly, which runs no
  watcher, and its `being_printed' is an array scanned from the
  outermost. Now the flags and options are read as `symbol_value_key`
  reads by the symbols interned once (all of them special in both
  editors, so no lexical binding can shadow them), the nil store is
  made only when the table is not nil, and the objects being printed
  are a stack. `effective_text_quoting_style` reads
  `internal--text-quoting-flag` and `standard-display-table` the same
  way. Undo a unit: the `%S` path 14,458 -> 10,243 instructions, the
  quoting style 1,773 -> 1,392, about 4,600 fewer; the total 1,294,897
  against round 32's 1,296,530, inside the run-to-run variation.
  bytecode-to-native's steady state is unchanged (`run_verified` 393
  and the callee 10 an iteration in both rounds; the per-unit totals,
  401.5 to 415.8 over four runs, differ in one-time costs).
  Round 32's profile-guided build, the contract's comparison over all
  sixteen twice (median ratio to GNU and interval): bytecode calls
  1.019 [0.995, 1.105] and 1.004 [0.943, 1.051], bytecode-to-native
  0.983 [0.935, 1.063] and 1.030 [1.009, 1.050], undo 0.961 [0.910,
  1.053] and 0.978 [0.897, 1.056], cons allocation 0.871 [0.816, 0.993]
  and 0.876 [0.786, 1.054]; the other twelve pass both times, at most
  0.90 (mapcar 0.898, 0.893). Undo's median is below round 28's 1.034.
  Validation (gate profile, `--test-threads=1`, 128 MiB stacks):
  the tests naming `print` (41),
  `prin1` (16), `format` (33), `quot` (70), `escape` (30), `circle` (5),
  `numbering` (2), `preprocess` (5) and `string` (241) pass but
  `print_prunes_charset_properties_like_print_prune_string_charset` (in
  `print` and `string`), this container's known failure; the
  lightweight group's 598 pass; clippy `-D warnings` and rustfmt pass.
- **Round 34: a C variable's read answered from its cell.** Undo reads
  variables as C reads them (`symbol_value_key`, `lookup_var_symbol`'s
  global half, `symbol_value_cell_symbol`) 453 times a unit, all through
  `find_symbol_value_resolved` at 48 instructions a call: its prologue
  saved six registers, then the C-slot test, the names
  `text-conversion-style` and `buffer-undo-list` compared, the localized
  flag and the KBOARD test, before the cell's value. 427 of the 453 are of
  C variables, 312 of them neither localized nor one of those two, so
  their value is the cell's, as GNU's `find_symbol_value` finds it under
  SYMBOL_FORWARDED to an object slot. `find_symbol_value_resolved` now
  answers from the cell inline when the symbol is no alias, not
  localized, no counter or KBOARD variable and its name is not 16 or 21
  characters long (the two names' lengths), and calls the full redirect
  switch (`find_symbol_value_redirected`, out of line) otherwise: 251 of
  `symbol_value_symbol`'s 300 calls a unit end there. Undo 1,294,897 ->
  1,281,123 instructions a unit (the reads and their callers about 8,700
  fewer, `memcmp` 2,000), bytecode calls 447.3 -> 443.1;
  bytecode-to-native (405.6), native-to-bytecode (4,017.2 -> 4,015.0) and
  interpreted-to-native (steady state 2,474 an iteration in both builds)
  do not move.
  Round 33's frozen run failed on it: `test-prin1-overrides` and
  `test-prin1-to-string-overrides` (print-tests.el) failed in Emaxx,
  because `prin1`'s and `prin1-to-string`'s OVERRIDES were bound in a
  copy of the evaluator's environment, which the printer's C-style reads
  no longer see. print.c:print_bind_overrides `specbind's them; now
  `with_print_overrides` binds each setting's variable dynamically for
  the print and undoes the bindings after it, as `unbind_to` does, and
  the walk follows GNU's: `print--variable-mapping`'s keys (GNU spells
  the `print-unreadable-function` key `unreadeable-function`), the value
  a setting's cdr (`(length 2)` binds `print-length` to `(2)`), `t`
  binding all fifteen variables to their defaults (`print-quoted` t, the
  rest nil; Emaxx had reset only the length and level), and GNU's errors
  (`(wrong-type-argument . consp)` for a non-list or a setting that is no
  cons, `(wrong-type-argument symbolp nil)` for an unknown key, where
  Emaxx signalled "invalid print overrides"). `prin1-to-string` binds
  them with NOESCAPE too, as Fprin1_to_string does (Emaxx ignored them
  there: `(prin1-to-string '(1 2 3) t '((length . 1)))` gave `"(1 2 3)"`
  where GNU's is `"(1 ...)"`). The batch error backtrace's three escape
  variables (debug-early-backtrace's `let`) are dynamic bindings now for
  the same reason. GNU's print-tests.el passes 46 of 46 in Emaxx, as do
  cl-print-tests, pp-tests, backtrace-tests, ert-tests and fns-tests as
  in GNU. New oracle test: the overrides, their errors and NOESCAPE.
  Validation (gate profile, `--test-threads=1`, 128 MiB stacks):
  the tests naming `variable` (92),
  `local` (92), `special` (54), `bind` (156), `alias` (55), `forward` (51),
  `default` (129), `unbound` (9), `boundp` (4), `symbol_value` (5),
  `keyboard` (26), `kboard` (1), `terminal` (26), `conversion` (21), `undo`
  (17) and `watch` (13) pass on the reads' change; with the overrides',
  `print_overrides` (1), `print` (42), `prin1` (16), `format` (33),
  `escape` (30), `backtrace` (16), `batch` (105) and `unhandled` (1) pass
  but `print_prunes_charset_properties_like_print_prune_string_charset`
  and `batch_startup_image_round_trip_or_explicit_native_image_limit`,
  this container's known failures; the lightweight group's 598 pass both
  times; clippy `-D warnings` and rustfmt pass.

- **Round 35: a key's symbol without the table's borrow, and a C
  variable's `let`, `set` and byte-code read from its cell.**
  `SymbolKey::with` (every `cached_symbol!` use) borrowed the thread's
  `ACTIVE` table (`RefCell<Option<Rc<Symbols>>>`: the borrow count, the
  Option, the Rc) to read one key slot, 24 instructions a call standalone
  and as much again inlined into `symbol_value_key` and `lookup_var_key`.
  A thread-local pointer to the selected table's key slots
  (`ACTIVE_KEYS`) is now stored with `ACTIVE` at each of its four stores
  and cleared by a table's Drop, so a non-null pointer always names a
  live table; a key already resolved is read through it, the borrow kept
  for the first resolution and the host table. `symbol_value_symbol` is
  inline with round 34's fast path in front of the alias check
  (`unlocalized_value` excludes an alias) and its rest out of line
  (`symbol_value_redirected`, 49 of its 300 calls a unit).
  Round 34's test of the two names whose value the buffer holds by their
  lengths (16 and 21 characters) is a cell bit now, `BUFFER_SLOT`, set
  with `C_SLOT` on `buffer-undo-list` and `text-conversion-style` (as
  `KBOARD_FORWARD` and `STORE_EFFECT` are on theirs), and a second bit,
  `STORE_CONVERTS`, marks the three C variables whose store converts the
  value beyond its slot kind's conversion (`display-hourglass`,
  `scroll-up-aggressively`, `vertical-scroll-bar`). With them
  `SymbolCell::is_plain_c_slot` names a C variable whose value is its
  cell's and whose store changes nothing else: eval.c:specbind's
  SYMBOL_FORWARDED let of it is SPECPDL_LET, a store into the slot and
  its restore, which `specbind_symbol_record`, `apply_special_restore` and
  `assign_plain_global` now take as they take a plain symbol's (a
  DEFVAR_BOOL's value stored as t or nil, and none of it while a C
  variable is detached from its slot). Undo's 120 `let`s a unit of them
  went the full specbind (`set_global_binding_resolved`, the forwarded
  checks and the restore record by name). Bvarref answers from
  `unlocalized_value` too, a C variable whose cell holds its value
  included, where it answered a plain symbol's alone.
  Undo a unit: `symbol_value_key` 6,380 and `symbol_value_symbol` 8,352
  -> 5,675 and 1,073, `SymbolKey::with` 3,480 -> 1,885, `lookup_var_key`
  2,376 -> 1,518, `specbind_symbol_record` 11,318 -> 8,279,
  `apply_special_restore` 6,202 -> 5,610, the slow byte ops 622 -> 600
  calls; the total 1,281,123 -> 1,269,473. Bytecode calls 443.1 ->
  434.3 and bytecode-to-native 405.6 -> 402.1 an iteration, both from
  `run_verified`'s register allocation (neither reads a C variable in its
  loop); native-to-bytecode 4,015.0 -> 4,017.7.
  Round 34's profile-guided build against round 32's, ten rotated rounds
  (median ratio to GNU and range): undo 0.893 [0.809, 1.063] against
  0.979 [0.818, 1.079], bytecode calls 0.978 [0.851, 1.406] against 0.987
  [0.889, 1.142], bytecode-to-native 0.982 [0.820, 1.132] against 1.001
  [0.858, 1.112]. The contract's comparison of it over all sixteen, on a
  noisier hour of the host (single rounds 0.78 to 1.40 for
  bytecode-to-native): thirteen pass, bytecode calls 1.024 [0.895,
  1.097], bytecode-to-native 1.019 [0.905, 1.334] and undo 1.005 [0.803,
  1.038] over.
  Validation (gate profile, `--test-threads=1`, 128 MiB stacks):
  the tests naming `symbol` (193), `intern` (78), `obarray` (21), `editor`
  (6), `scope` (14), `thread` (17), `nested` (55), `variable` (93),
  `special` (54), `alias` (55), `forward` (51), `local` (92), `default`
  (129), `bind` (157), `unbound` (9), `boundp` (4), `keyboard` (26),
  `terminal` (26), `conversion` (21), `watch` (13), `undo` (17),
  `constant` (18), `bool` (10), `print_overrides` (1), `byte_code` (11)
  and `bytecode` (84) pass, and `image`'s 41 but
  `batch_startup_image_round_trip_or_explicit_native_image_limit`, this
  container's known failure; the lightweight group's 598 pass; clippy
  `-D warnings` and rustfmt pass. One run of the `symbol` group failed
  `symbol_alias_redirects_preserve_handover_fields_and_reclaim_replaced_targets`
  (two collections left one of three replaced values and one of three
  old alias targets in their weak tables, `((1 1 3 3) ...)` against
  `((0 0 3 3) ...)`); the same binary passed the group's 193 on the next
  run and the test alone, and round 34's passed it once: a conservative
  root that comes and goes between runs, as the test's own note on the
  stack's old argument slots allows for its last check but not for this
  one.

- **Round 36: a native call's record in its caller's frame, its depths
  taken at its first pushes.** Every call into generated code from Rust
  pushed a `NativeCallFrame` of seven fields on the runtime's `calls`
  vector (the handler, unwind and root stacks' depths, Ffuncall's depth,
  the backtrace depth, the pending error, the escape buffer), reconciled
  the handler registrations with the native list before the call and
  again after it, and compared the frame's depths with the stacks' before
  the pop: in bytecode-to-native's call op the pre-call sync, the push
  and the pop with its tests were 61 instructions around the callee's 10
  (round 35's build without any of them: 339.4 an iteration against
  402.1). GNU has no such record: a C frame can be left by longjmp, and
  each handler (its `pdlcount`, its `lisp_eval_depth`) and specpdl entry
  carries what an exit to it restores. Generated code can be left only to
  the Rust frame that called it, so a record stays, but it is now the
  calling frame's own: `NativeCallFrame` lives in the Rust frame of the
  call (`invoke`, `invoke_fixed`, `invoke_fixed_registers`,
  `invoke_nested_fixed`), linked to the call it nests in
  (`NativeRuntime::call`, the innermost), and a call's first push on the
  handler, unwind or root stack records that stack's depth
  (`note_call_push`, from `runtime_push_handler` after its own
  reconciliation, `record_unwind` and `push_ephemeral_root_range`);
  until then the stack is as the call found it, its own length the depth
  (`NativeCallFrame::depth`). A state byte says which depths are
  recorded and whether the call has a pending error, so a call that
  pushed on none of the stacks ends by its unlinking and one test of that
  byte (`finish_frame`), the rest out of line (`settle_call_frame`). The
  reconciliation before a call is gone: it was there for the depth taken
  at the call's start, and a handler's push still reconciles before it
  records one; generated code pops only the handlers its own activation
  pushed, so a call that pushes none leaves the native list and the
  registrations as it found them, a popped registration of an outer call
  included, which the next push, signal or settling removes as before.
  The collector and a suspended thread trace the pending errors along the
  links (`CallFrames`); the threads' stacks are corosensei's, never
  copied, so a suspended call's record stays where it was. New test: a
  call's handler depth below a popped registration of the call it nests
  in, its signal caught by its handler and then escaping, and the outer
  call balanced at its recorded depth.
  Per iteration: bytecode-to-native 402.1 -> 343.3 (GNU 388),
  interpreted-to-native 2,472.2 -> 2,436.0, native-to-interpreted 4,101.0
  -> 4,087.4, native-to-bytecode 4,017.7 -> 4,015.0; bytecode calls'
  loop is unchanged (`run_verified` 448 and 449 an iteration). Undo read
  1,269,473 -> 1,255,812 a unit (GNU 1,252,071), but that is the spread
  of one build's runs, not the round: round 35 rebuilt and run beside
  round 36 reads 1,254,268 against 1,253,825, and runs of one build
  differ by up to 15,000 a unit (the allocator's `malloc_consolidate`
  and `unlink_chunk` and the `memcmp`s of hash probes follow each
  process's hash seeds), so the round's effect on undo is under 1,000;
  compare undo's builds by the functions a change touches, or by several
  runs each. Validation (gate profile,
  `--test-threads=1`, 128 MiB stacks): the tests naming `native` (405), `handler` (34),
  `escape` (30), `signal` (40), `condition` (30), `unwind` (17), `gc`
  (58), `collect` (46), `callback` (33), `suspen` (9), `continuation`
  (7), `nonlocal` (11), `funcall` (26), `catch` (7), `throw` (5) and
  `weak` (14) pass but
  `batch_startup_image_round_trip_or_explicit_native_image_limit`, this
  container's known failure; the lightweight group's 599 pass; clippy
  `-D warnings` and rustfmt pass.
  Round 36's profile-guided build against round 34's, nine rotated
  rounds (median ratio to GNU): bytecode calls 0.914 against 1.014,
  bytecode-to-native 0.920 against 0.972, undo 1.001 against 0.931. Undo
  ran 27,000 fewer instructions a unit in round 36's build; its swing the
  other way is the build's layout and the host.
- **A byte-code call's saved state in the stack's frame footer, measured
  and left out.** A call the verified loop makes pushes a `BcFrame` (88
  bytes: the caller's program, pc, depth and watermarks) on a vector of
  its own; bytecode.c keeps that state in the frame's `bc_frame` on the
  byte-code stack. Folding it into the frame's footer on Emaxx's byte-code
  stack (the footer written at the call, read in place at the return and
  on an error) took bytecode calls from 449 to 441 instructions an
  iteration. The footer grows from GNU's four words to thirteen, and the
  stack's capacity is GNU's in words: the oracle test
  `bytecode-stack-layout` signals at GNU's depth, which thirteen-word
  footers reach sooner. Charging a frame GNU's four words against the
  limit while it occupies thirteen cost about the eight instructions it
  saved, so the fold is not kept.
- **Round 37: undo's slow byte ops that run no Lisp, in the verified
  loop.** Undo left `run_verified` 600 times a unit for `op_in_place`,
  about 87 instructions each of decode, frame and bookkeeping besides the
  op's work. A census of the ops it ran (the byte at `op_in_place`'s
  entry, counted at ten and twenty units) puts 430 of them in ops whose
  usual case reads or changes the interpreter's state but runs no Lisp,
  collects nothing and stores into no string: Bvarref of a variable whose
  cell does not hold its value (buffer-local or an alias, 78), Bvarset of
  a C variable (21), Bvarbind (66) and Bunbind 1 (67), Bmemq (39), Bget
  (26), Blist1 to Blist4 (15), Bsave_current_buffer (24), Bset_buffer
  (24), Bgoto_char (38) and Bswitch (31). Each now has an arm in the loop
  that calls a small out-of-line helper for that case and goes on with
  the next byte, the other cases left to the op's full arm from its
  start, signals included: `varref_redirected` (data.c's
  find_symbol_value past the cell), `varset_plain` (`assign_plain_global`,
  the C variable whose store is its cell's), `varbind_plain`
  (`Interpreter::specbind_plain`, the plain store `specbind_symbol_record`
  starts with, and its record on the unwind list), `unbind_plain` (a plain
  restore, `is_plain_restore`, or a saved current buffer), `get_plain`
  (Fget of two bare symbols with no overriding plist environment),
  `set_buffer_object` (a live buffer object), `goto_char_fixnum` and
  `switch_destination` (a table with eq, eql or equal, whose hashing
  runs no Lisp; a hit leaves through the frame-switch exit, which admits
  the destination). Bmemq compares each element as lisp.h:EQ does while
  `symbols-with-pos-enabled` is nil (`values_eq_plain`), where it took
  only a fixnum or a symbol and stopped at a vectorlike element; Blist1
  to Blist4 cons in the loop. That left 190 slow ops a unit, 40 of them
  Baref and Baset, every one on a record (ERT's `cl-defstruct` objects:
  `ert-test`, `ert--test-execution-info`, `ert-test-passed`), which
  bytecode.c's fast case takes as it takes a vector (RECORDP, PVSIZE
  counting the type slot) and Emaxx's (`vector_aref_fast`,
  `vector_aset_fast`) did not; they take a record's slots now. Undo's
  slow ops 600 -> 150 a unit (`op_in_place`'s calls, its own
  instructions 85,262 -> 22,772); what is left is mostly Bunwind_protect,
  Bdelete_region, Bset_marker, Binsert and the handler ops. Instructions a
  unit in undo, plain build: 1,253,825 -> 1,193,255 and 1,205,555 (two
  runs of one build) without the records, 1,169,473 with them (one run)
  (`op_in_place` 28,172 -> 22,772, `aref` and `aset` 7,978 -> 3,074,
  `prim` 4,018 -> 1,418); profile-guided, without the records: 1,151,371
  -> 1,090,242 (GNU 1,252,071).
  In the plain build the new arms move `run_verified`'s register
  allocation (the code pointer and two other values stored at the loop's
  head, reloaded after calls): bytecode calls 449.3 -> 462.3 and
  bytecode-to-native 344.3 -> 350.3 an iteration in `run_verified`,
  native-to-bytecode 80.0 -> 83.0. The profile-guided build places those
  spills off the hot paths: `run_verified` 442.2 -> 443.2 in bytecode
  calls, 338.2 -> 341.2 in bytecode-to-native; the records' case leaves
  both workloads' `run_verified` as it was. New tests: GNU 30.2's byte
  code for Baref and Baset of a record's slots, its type and an index
  past it, and for a function running each of the other ops' usual and
  other cases (a watched variable's let, set and unlet, a buffer-local let,
  set-buffer of a name, goto-char of a marker, a void variable, switch
  hits and misses on eq and equal tables), with GNU's answer and its
  watcher's log. Validation (gate profile, `--test-threads=1`, 128 MiB
  stacks): the tests naming
  `bytecode` (84), `special` (54), `switch` (13), `memq` (5), `unbind`
  (1), `handler` (34), `unwind` (17), `list` (157), `set_buffer` (3),
  `goto` (1), `symbols_with_pos` (1), `variable` (93), `local` (92),
  `bind` (157), `watch` (13), `default` (129), `undo` (17) and `forward`
  (51) pass, and `buffer`'s 325, `get`'s 51 and `alias`'s 55 but one
  each: the as-root `save_buffer_skips_unmodified_and_unchanged_files`
  and `byte_compile_file_reports_an_unwritable_target_like_gnu`, this
  container's known failures, and
  `symbol_alias_redirects_preserve_handover_fields_and_reclaim_replaced_targets`,
  round 35's intermittent one (`((1 1 3 3) ...)`), which passed alone
  three times after. With the records' case: `aref` (7), `aset` (9),
  `record` (98), `vector` (86), `struct` (63), `bytecode` (86, the two
  new tests among them) and `anti_cheat` (24) pass, and the lightweight
  group's 601; clippy `-D warnings` (all targets and features), rustfmt
  and `git diff --check` pass. The lightweight group caught the first
  draft's tests naming their Lisp symbols in the project-private
  `emaxx--` namespace, which the anti-cheat gate forbids in any runtime
  source; they are `vm-light-...` and `vm-record-slots` now.
  Round 37's profile-guided build against round 36's, nine rotated rounds
  (median ratio to GNU and range): undo 0.931 [0.762, 1.011] against
  1.030 [0.843, 1.082], bytecode-to-native 0.866 [0.712, 1.129] against
  0.883 [0.754, 1.188], bytecode calls 1.037 [0.893, 1.295] against 0.986
  [0.873, 1.309]; bytecode calls again, 21 rounds, 0.883 against 0.850
  and then 0.997 against 1.011, the same instructions in both builds
  (`run_verified` 442.2 and 443.2 an iteration): the host's noise. The
  contract's comparison of round 37's profile-guided build over all
  sixteen: thirteen pass; over are bytecode calls 0.954 [0.873, 1.097],
  bytecode-to-native 0.914 [0.740, 1.266] and undo 0.994 [0.907, 1.044],
  the other thirteen at most 0.92 (interpreted ones 0.60 to 0.69,
  native-execution 0.548, mapcar 0.920 [0.726, 0.972]).
- **Where undo's cycles go now, and round 39: one Bcall arm for counts
  three to seven.** With round 37 undo runs fewer instructions than GNU
  and as much wall time. Cachegrind on the profile-guided build, a unit
  (GNU's in parentheses): 1,068,944 instructions (1,259,024), 37,692
  first-level instruction-cache misses (21,209), 9,482 data read misses
  (7,484) and 3,174 write misses (1,400), 15,375 mispredicted conditional
  branches (13,448) and 8,320 indirect ones (9,361). The instruction-cache
  misses are the difference: the code a unit runs at least once a unit
  spans 4,944 distinct 64-byte lines, 309 KB (GNU 3,142, 196 KB), against
  a 32 KB first-level cache. The byte-code VM's functions miss about
  8,100 times a unit (`run_verified` 5,263 over its 314 hot lines,
  `op_in_place` 883, `run_entered` 655, `call_plain`, `bytecode_call`,
  `run_loop`) against `exec_byte_code`'s 1,672 (143 hot lines) and `funcall_subr`'s
  175; the evaluator's (`eval_native_form` 1,584,
  `eval_sub` 1,254, `call_function_value` 1,084, `eval_cons_call` 851)
  about 4,800 against GNU's `eval_sub`, `funcall_lambda`, `Flet` and
  `apply_lambda`'s 1,300, and those functions are 28, 16, 23 and 15 KB of
  code against GNU's 2.8 KB `eval_sub` and 1.3 KB `funcall_lambda`; the
  primitives' dispatch functions (`buffer_edit`'s 153 hot lines,
  `strings`' 140) come next. One part of that was the call op itself:
  each of Bcall to Bcall7 had its own copy of the call's path, the count a
  constant in it, eight copies making 25 KB of `run_verified`'s 47 KB in
  the profile-guided build (Bcall6 6.4 KB, Bcall7 6.1 KB, Bcall and Bcall3
  3.5 KB each), and undo calls with every count from none to six (151,
  197, 93, 27, 18, 12 and 4 calls a unit). One arm for all eight counts,
  as exec_byte_code's `dofuncall`, the count read at run time, halved the
  function (46,317 to 23,098 bytes in the plain build) but cost 20 to 22
  instructions a call (the count's selection, the arguments' copy by a
  run-time length, the arity mask), bytecode calls 462.3 -> 484.3 an
  iteration in `run_verified`. Bcall, Bcall1 and Bcall2 (441 of undo's
  502 calls a unit, and the only counts the locked call workloads use)
  keep their constant copies; Bcall3 to Bcall7 share one arm. The plain
  build's `run_verified` is 32,826 bytes; instructions are unchanged
  (bytecode calls 460.3, bytecode-to-native 348.3 an iteration in
  `run_verified`, undo within its noise). Profile-guided, against round
  37's build with line tables (two builds of one source differ by about
  ten instructions an iteration here, the training's counts not being
  identical): undo's instruction-cache misses 37,694 -> 36,943 a unit and
  `run_verified`'s hot lines 314 -> 295, bytecode calls 454.2 -> 444.2
  and bytecode-to-native 345.2 -> 340.2 an iteration. New test: GNU
  30.2's byte code calling byte code (optional and rest arguments, an
  arity error) and a subr with three to seven arguments, through Bcall3
  to Bcall5 and Bcall6's operand, and a Bcall7 of 300 arguments, against
  GNU's answers. A nine-round pilot of the three profile-guided builds
  on a noisy hour of the host (single rounds 0.71 to 1.57 of GNU's time)
  reads undo 1.094, 0.899 and 0.947 for rounds 36, 37 (with line tables)
  and 39, bytecode calls 0.902, 0.964 and 0.848, bytecode-to-native 0.843,
  1.004 and 0.985: within its noise, so the round stands on the counts
  above. Validation (gate profile, `--test-threads=1`, 128 MiB stacks):
  the tests naming `bytecode` (86), `byte_code` (11), `call` (229),
  `funcall` (26), `arity` (16), `optional` (16), `rest` (67), `apply`
  (10) and `anti_cheat` (24) pass, the three `verified_loop_` tests among
  them, and `native`'s 406 but
  `batch_startup_image_round_trip_or_explicit_native_image_limit`, this
  container's known failure; the lightweight group's 601 pass; clippy
  `-D warnings` (all targets and features), rustfmt and `git diff
  --check` pass.
- **Round 40: a four-word backtrace record, one gate for a call's entry
  tests, and the byte-code return without its re-checks.** Every call
  path pushes and pops a backtrace record (record_in_backtrace), and the
  record was eight words: the function as a two-variant enum, the
  arguments as a four-variant one whose owned variants were vectors, two
  flags and the detail. It is four now, as GNU's specbinding: the
  function word (eval_sub's unevaluated form in it, a cons being its own
  word), the arguments' address, their count with the native-words,
  evaluated and debug-on-exit flags above it, and the detail, which now
  holds the arguments of a frame that owns them (one retained past its
  activation, or a copy of an interpreter), the address then pointing
  into it; a frame without a detail owns nothing. A push is four stores
  (seven), the pop's test one compare fewer. Separately, a plain call's
  entry (`plain_bcall_entry`: Bcall into byte code, a native function or
  a subr) tested the quit flag, a pending termination, edebug's special
  flag, the depth against max-lisp-eval-depth, the profiler and the
  callee's name; eval_sub's entry tested the first two. The quit flag,
  the termination, the profiler and the bound now make one word,
  `call_depth_gate`: max-lisp-eval-depth, or below every depth while a
  quit or termination is pending or calls are profiled, refreshed by
  each store of those (the forwarded store, process_quit_flag, the
  image's install, the termination's request and take, the bump to
  100); a call compares its depth with it. eval_sub keeps its check after
  the increment, as maybe_gc can run Lisp (post-gc-hook). Breturn into a
  byte-code caller tested the frame list three times, `leave_frame` and
  `replace_call` asserted what the call's own entry established, and the
  frame list's truncate compared its length; one test of the last frame
  remains, the rest are debug assertions, as is the argument copy's at a
  verified entry (`EntryProgram::takes` and the entered frame's depth
  imply its bounds). Plain build, `run_verified` an iteration: bytecode
  calls 460.3 -> 430.3, bytecode-to-native 348.3 -> 333.3; undo's
  evaluator and call paths about 10,000 instructions a unit fewer
  (`run_verified` -3.2K, `eval_sub` -2.6K, `call_plain` -2.2K, native
  funcall -0.9K, `eval_cons_call` -0.6K), its total within its
  allocator noise. Profile-guided, against round 39's build: bytecode
  calls 444.2 -> 419.2 an iteration in `run_verified` (GNU's whole loop
  468), bytecode-to-native 340.2 -> 325.2 (with the callee's 10, GNU
  388); undo's evaluator and call paths about 16,000 instructions a unit
  fewer (`eval_sub` -5.9K, `call_plain` -4.7K, `run_verified` -3.9K),
  1,088,017 -> 1,083,518 in all with the allocator's run-to-run
  difference, and its instruction-cache misses unchanged (36,943 ->
  36,875). New test: GNU 30.2's byte code recursing into byte
  code under a let of max-lisp-eval-depth (GNU's Ffuncall error), and
  stores of quit-flag before a call into byte code, a subr and an
  interpreted form, each signalling quit, and calls running again after,
  against GNU's answers (Emaxx's answer was GNU's before the change).
  Validation (gate profile, one test thread a process, 128 MiB stacks):
  the whole library suite (3,165 tests) passes but for this container's
  known failures (the image round trip and the three as-root file tests)
  and `char_charset_family_follows_charset_c` and
  `print_prunes_charset_properties_like_print_prune_string_charset`,
  whose GNU oracle answers differently in this container's locale (both
  fail likewise on round 37's build);
  `subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`
  failed once among four parallel processes and passes alone, three times
  of three. The suite's binary preceded the new test, which passes with
  the 54 `lisp::bytecode` and 24 `anti_cheat` tests; clippy `-D warnings`
  (all targets and features), rustfmt and `git diff --check` pass.

- **Round 41: a native caller's words are the subr's arguments; where
  undo's remaining time is not.** Native code calls a C subr through its
  generated wrapper and `invoke_subr`. For a subr with no word-level path
  of its own there (`DirectSubr::Other`: `delq`, `equal`, `nconc`,
  `nthcdr`, `set`, `get-buffer-create` and most others) the call went
  through every word-level test first, copied the argument words into a
  vector of values, and looked the name's facts up to call through them.
  Such a subr is now called at once through `NativeSubr::call`:
  funcall_subr's usual case read from the subr's own arity words and
  direct entry, the facts only otherwise, with the caller's words
  themselves as the arguments (`live_values`: a `Value` is the word, as
  `decode_live` reads it), as funcall_subr passes C its argument vector.
  The evaluator's and Bcall's subr calls (`call_subr`) use the same
  reading, and native funcall's byte-code and general targets take the
  words in place too. Bcall's plain call reads the function cell once
  (the loop's read is passed on). Plain build, undo: `invoke_subr`
  14,015 -> 9,798 instructions a unit, `call_plain` 53,498 -> 49,286.
  Profile-guided, each build trained afresh (`tools/pgo-build.sh`),
  against round 40's: undo 1,083,503 -> 1,061,700 instructions a unit
  (GNU 1,252,071), instruction-cache misses 36,874 -> 36,479, data read
  misses 10,538 -> 9,654; bytecode calls 418.3 -> 416.7 an iteration,
  bytecode-to-native 337.7 -> 338.8. A build of round 41's source with
  round 40's profile ran undo at 1,154,590 instructions and 50,849
  misses: a function whose code changed loses its profile, so every
  comparison takes a build trained on its own source.
  Two measurements of undo's wall time against its instruction count
  (about 0.93 of GNU's time for 0.85 of its instructions), neither kept:
  (a) callgrind's branch simulation: 24.7K mispredictions a unit, GNU
  22.8K. (b) The code's pages: undo runs code on about 375 4 KiB pages of
  the executable (4,734 hot 64-byte lines, 300 KB), GNU on 217, since the
  profile-guided layout spreads undo's hot functions over 6 MB of text
  (lld's `-z keep-text-section-prefix` gathers the training's 2.6 MB hot
  set, but undo's code is spread through it: 390 pages). Moving the text
  segment onto transparent huge pages at startup (an anonymous copy
  advised MADV_HUGEPAGE, moved over the file's mapping at the same
  addresses, with a 2 MiB maximum page size at link time so that no other
  segment shares the text's regions: six 2 MiB pages) worked on this
  host, but the same binary with and without it, alternating over 11
  rounds with the timing held to two CPUs: undo 0.925 against 0.932 of
  GNU's time, bytecode calls 0.907 against 0.904, bytecode-to-native 0.813
  against 0.823, CPU time without/with 1.005, 0.993 and 0.978. Without a
  measurable gain it is not in the tree.
  Validation (gate profile, one test thread a process, 128 MiB stacks,
  four processes at a time): the whole library suite (3,166 tests) passes
  but for this container's known failures (the image round trip, the
  three as-root file tests, and the two charset tests whose GNU oracle
  answers differently in this container's locale);
  `subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`
  and `process_tls_state_survives_collection_before_and_after_async_handshake`
  failed among the four parallel processes and pass alone, three times of
  three each. Clippy `-D warnings` (all targets and features), rustfmt and
  `git diff --check` pass. The contract-like comparison of round 41's
  profile-guided build (nine rounds, the paired median ratio's bootstrapped
  95% interval): 15 of the 16 workloads within 1.03 of GNU's wall time;
  mapcar's interval reaches over from two slow rounds (median 0.861,
  [0.721, 1.088]), and undo is the tightest of the others (0.956,
  [0.845, 0.995]).
- **Round 42: a text property's name is the Lisp object; undo records an
  edit as insdel.c does.** An interval's property names were Rust strings:
  every `put-text-property`, lookup and undo record compared names as text,
  a name that was not a symbol signalled `wrong-type-argument symbolp`
  (`(put-text-property 1 2 1 'x)`, a string or list name kept by identity,
  `(propertize "ab" 7 'seven)`: GNU takes any object), and an uninterned
  symbol's name went through an encoded-name adapter. A name is now a
  `PropertyName`, the object itself compared with EQ (textprop.c's
  `EQ (XCAR (tail), prop)`); a Rust caller that asks for `"face"` names the
  interned symbol (`PropertyKey`). The Lisp primitives take any object as
  a name (`get-text-property`, `get-char-property`, `put-text-property`,
  `add-`/`set-`/`remove-text-properties`, `remove-list-of-text-properties`,
  `text-property-any`, the `*-property-change` scans, `propertize`, the
  `#("ab" 0 1 (1 one))` read syntax, `overlay-get`), category and
  `char-property-alias-alist` lookups compare with EQ, and the names are
  traced wherever the values are: the collector, a buffer's intervals and
  undo payload, the reader's placeholders and obarray interning, the image.
  An undo record of a property change names the object (no symbol lookup
  a record). Three older divergences from GNU in the same records, now as
  insdel.c has them: (a) a deletion's record held its text alone, so
  `primitive-undo` reinserted the text without its properties; the string
  is now `make_buffer_string`'s, with the deleted intervals and multibyte
  as the buffer is (`set-buffer-multibyte` keeps those already recorded).
  (b) Inserting a propertized string recorded a property change after the
  insertion (`(nil face nil 1 . 3)` after `(1 . 3)`), as did
  `replace-match`, `replace-buffer-contents`, `transpose-regions`,
  translation, decoding, the minibuffer's prompt and native undo's
  reinsertion; the inserted text's intervals are now grafted unrecorded and
  uncounted (graft_intervals_into_buffer), in their own order. (c) An
  insertion, deletion or in-place replacement raised the modification tick
  by one; it is raised as lisp.h's modiff_incr raises it, by one more than
  the binary logarithm of the change's length. Profile-guided builds, each
  trained on its own source: undo 1,057,434 -> 1,048,999 instructions a
  unit (GNU 1,252,071), bytecode calls 417.7 -> 418.5 an iteration.
  `text_property_names_are_any_object_compared_with_eq` and
  `undo_records_inserted_and_deleted_text_as_insdel_does` check both
  against the GNU oracle. Validation as round 41's: the whole library suite
  (3,168 tests) passes but for the same known failures;
  `process_tls_state_survives_collection_before_and_after_async_handshake`
  failed among the four parallel processes and passes alone, three times of
  three. Clippy `-D warnings`, rustfmt and `git diff --check` pass.
- **Round 43: read-only text and property changes as textprop.c checks and
  signals them.** Older divergences, found by probing GNU: (a) a deletion
  or replacement read each character's own `read-only` alone, so a nil
  value counted as read-only, `category` was ignored, a non-list
  `inhibit-read-only` (`5`) was not honoured and a list one matched with
  `equal`. The check now walks the intervals as
  verify_interval_modification does (`textget` through `category` and
  `char-property-alias-alist`, INTERVAL_GENERALLY_WRITABLE_P and
  INTERVAL_EXPRESSLY_WRITABLE_P, `memq` of the list), after
  Fbarf_if_buffer_read_only's look at the first character's
  `inhibit-read-only`. (b) An insertion checked the buffer's flag alone:
  text went in after read-only text that is not rear-nonsticky and before
  front-sticky read-only text. The insertion case's stickiness rules (TMEM
  over `front-sticky` and `rear-nonsticky`, a `category`'s read-only when
  the interval has none of its own) now refuse it. (c) The property
  primitives on a buffer (`put-text-property`, `add-`, `set-` and
  `remove-text-properties`, `remove-list-of-text-properties`,
  `add-face-text-property`) checked nothing and ran no change hooks, and
  given a buffer OBJECT they changed the current buffer. They now act as
  modify_text_properties: only when something would change
  (add_text_properties_1's all-properties test with EQ values, the remove
  scans, set's intervals), with the object buffer current for the
  read-only checks, the overlays' modification hooks, the lock and
  `before-change-functions`, then the change and `after-change-functions`
  in the caller's buffer, one tick counted. The minibuffer prompt's face
  is added without hooks, as read_minibuf binds
  `inhibit-modification-hooks`. (d) GNU's buffers and strings keep their
  intervals when the properties go, and `set-text-properties` modifies a
  region with intervals (returns t, signals, counts a tick and records a
  first change) though no plist changes. A buffer now carries that state
  (dropped when a deletion takes its whole text, exchanged by
  `buffer-swap-text`), a string an empty interval container (copied by
  `copy-sequence`, not by `substring` or `concat`, dropped by
  `set-text-properties` of the whole string to nil). Still not as GNU: the
  `modification-hooks`, `insert-in-front-hooks` and `insert-behind-hooks`
  text properties (only the overlays' run), and a string's empty intervals
  grafted into a buffer by its insertion.
  `read_only_text_follows_textprop_intervals_and_stickiness`,
  `text_property_changes_modify_the_object_buffer` and
  `intervals_outlive_their_properties_until_the_text_goes` check them
  against the GNU oracle. Profile-guided builds, each trained on its own
  source: undo 1,048,999 -> 1,081,140 instructions a unit (GNU 1,252,071),
  the property changes running the change hooks GNU runs and the read-only
  walk's `textget` reading `char-property-alias-alist` by name (about
  5,400 a unit); bytecode calls 418.5 -> 418.6 an iteration. Validation as
  round 42's: the whole library suite (3,171 tests) passes but for the
  same known failures. Clippy `-D warnings`, rustfmt and
  `git diff --check` pass. The contract-like comparison of round 42's
  profile-guided build, run twice: all 16 workloads within 1.03 of GNU's
  wall time both times (the closest: bytecode calls 0.926 [0.868, 1.015]
  and undo 0.912 [0.887, 0.992]; then undo 0.920 [0.802, 1.026], with one
  1.229 round, and cons allocation 0.918 [0.767, 1.018]).
- **Round 44: every change runs insdel.c's protocol.** Each edit path had
  assembled its own hooks, and probing every edit primitive against GNU
  with all the hooks logged found them apart: an internal counter stood in
  for `inhibit-modification-hooks` (binding or setting the variable
  inhibited nothing), the overlays' hooks ran before
  `before-change-functions`, `first-change-hook` and the text's own
  `modification-hooks`, `insert-in-front-hooks` and `insert-behind-hooks`
  never ran, `deactivate-mark` was never set, the file was locked after
  the change, and `erase-buffer`, `delete-char`, `replace-match`,
  `subst-char-in-region`, `transpose-regions` and `delete-field` checked
  no read-only text (`transpose-regions`, the base64 region primitives and
  `compose-region` ran no hooks at all). One protocol (`insdel.rs`) now
  serves them: prepare_to_modify_buffer's read-only buffer check,
  undoable-change notice and interval walk (read-only text, the text's
  `modification-hooks` run unbound, an insertion's neighbours' insert
  hooks recorded), then unless the hooks are inhibited the first change's
  file lock and, with `inhibit-modification-hooks` bound,
  `first-change-hook`, `before-change-functions` and the overlays' hooks
  (the bounds following the earlier hooks' changes), then
  `deactivate-mark`; and signal_after_change's `after-change-functions`,
  overlays' hooks and the recorded insert hooks, or its deferral under
  `combine-after-change-calls` (whole-buffer offsets,
  `syntax-ppss-flush-cache` alone allowed). The insert hooks persist as
  textprop.c's globals do: an insertion in a buffer without intervals runs
  the last ones found until another check replaces them, and the image
  carries them as pdumper does a staticpro'd root. Each primitive signals
  the change GNU does: `replace-match` one (its start preserved, the
  casing a change of its own inside it), `subst-char-in-region` and
  `translate-region` from the first changed character (translation a
  change a character, string tables now accepted and the text's properties
  kept), casing the changed span, `transpose-regions` one over both
  regions, `indent-to` its tabs and spaces apart, the base64 regions an
  insertion then a deletion, the default process filter and sentinel their
  insertions (the buffer's read-only flag cleared). Also as GNU now:
  `move-to-column` turns an overshooting tab into spaces only and extends
  a short line only for FORCE `t`, `indent-to` reads an insane `tab-width`
  as 8, `narrow-to-region` signals bounds outside the buffer, and
  `delete-and-extract-region` of nothing is the empty unibyte string. The
  probe's one remaining difference is GNU printing adjacent intervals with
  equal plists apart where Emaxx merges them.
  `change_hooks_run_in_insdel_order` and
  `every_edit_primitive_checks_read_only_text` check them against the GNU
  oracle. Profile-guided builds, each trained on its own source: undo
  1,081,140 -> 1,086,993 instructions a unit (GNU 1,252,071), the protocol
  costing what the hook paths it replaces did and the insertion checks
  reaching `textget`'s by-name read of `char-property-alias-alist` (27 a
  unit, about 9,600 instructions with their string compares; round 45
  reads it as a C variable); bytecode calls 418.6 -> 417.7 an iteration.
  Validation as round 43's: the whole library suite (3,173 tests) passes
  but for the same known failures: 89 of its 98 groups on the round's
  first build, then, after the `replace-match` casing and the insert
  hooks' image slots were fixed, the other 9 and every group those fixes
  touch (anti-cheat, the image writer and loader, the hooks, replacement
  and regexp tests). Clippy `-D warnings`, rustfmt and `git diff --check`
  pass. The seven GNU tests round 43's frozen run failed pass, run as
  GNU's own test files.
- **Round 45: text properties read and searched as textprop.c does.**
  Chasing round 43's undo cost found `textget` reading
  `char-property-alias-alist` by name as a buffer-local value (a let-bound
  or global alias list never seen, hundreds of instructions a lookup),
  taking an alternative's value through its category, stopping at a nil
  category or alias value, and ignoring `default-text-properties`, so text
  the defaults made read-only could be deleted. `textget` is now
  intervals.c:lookup_char_property: the plist's value, else the category
  symbol's non-nil value, else the first non-nil value of an alternative,
  else the defaults, both variables read as C variables; an empty interval
  and the end of the text read the defaults too, and a syntax scan sees
  them only in a buffer with intervals (interval_of finds none otherwise).
  `text-property-any`, `text-property-not-all` and the single-property
  change scans read each character's raw property, compared values with
  equal-like rules and clamped bad positions; they now walk the intervals
  from find_interval (`PropertyIntervals` over a buffer's or string's
  spans), compare `textget` values with EQ, answer for an object without
  intervals as if every value were nil, coerce limits and markers and
  signal args-out-of-range outside the accessible text. The string case
  of the single-char-property scans is the single-property scan's (the
  backward one read a string as a buffer name).
  `property_change_helpers_accept_markers` had pinned a backward scan's 1
  where GNU returns nil (no interval before the first).
  `text_property_searches_walk_intervals_with_textget` checks the lookups
  and searches against the GNU oracle.
  Round 44's frozen run failed six erc tests with `text-read-only`:
  `read-passwd`'s own cleanup erases the minibuffer, which in GNU
  read_minibuf_unwind has already erased with `inhibit-read-only` bound,
  while Emaxx left the read-only prompt there (round 44's read-only checks
  in `erase-buffer` exposed it). Probing a read against GNU found the
  minibuffer's whole life apart, and it now follows minibuf.c:
  get_minibuffer makes the buffer with undo enabled (it was disabled, the
  name starting with a space) and reuses one with its overlays deleted;
  each read runs `minibuffer-mode` (the level-0 minibuffer
  `minibuffer-inactive-mode`, as emacs.c sets it before the top level),
  clears `truncate-lines`, erases the buffer and inserts the prompt through
  `erase-buffer`, `insert`, `put-text-property` and
  `add-face-text-property` with `inhibit-read-only` and
  `inhibit-modification-hooks` bound (recording undo; the stickiness and
  field controls first, then `minibuffer-prompt-properties`, so the
  prompt's plist prints in GNU's order), inserts the initial input as an
  ordinary insertion, and empties the undo list after the setup hook.
  Every exit runs `minibuffer-exit-hook` (it ran only for a live
  terminal's reads, there after the window restore and before the
  contents were taken; the contents are now taken with the minibuffer
  current, as read_minibuf's `Fset_buffer` before
  `Fminibuffer_contents`, since an exiting command may have run in
  `*Completions*`), then erases the expired minibuffer the same way,
  keeping `deactivate-mark`, and runs `minibuffer-inactive-mode`. Both
  recursive edits (the minibuffer's and `recursive-edit`'s) bind
  `inhibit-redisplay` and `undo-auto--undoably-changed-buffers` as
  recursive_edit_1 does, so a command inside one puts no undo boundary in
  a buffer changed before it (Bug#23632), and the keyboard-macro command
  loop takes its boundary through `undo-auto--add-boundary` before the
  command, as command_loop_1 does, so typed characters amalgamate. A batch
  read from stdin activates no minibuffer (read_minibuf_noninteractive
  returns before choosing one). `erase-buffer` widens first and resets
  `buffer-saved-size`, as Ferase_buffer. `deactivate-mark` and the twelve
  other C variables keyboard.c, casefiddle.c and xdisp.c make
  buffer-local when set (`case-symbols-as-words`, `wrap-prefix`,
  `display-line-numbers` and the rest) now are, and the command loops'
  reset of `deactivate-mark` is a C assignment that makes no local; a
  store into a buffer-local variable whose binding in the current buffer
  exists is set_internal's store into the loaded binding and nothing else
  (`image_round_trips_buffers_markers_finalizers_and_nilled_frames` now
  expects the buffer's `deactivate-mark` local, as GNU makes it).
  `minibuffer_reads_unwind_like_read_minibuf` checks the read's setup
  view, the expired minibuffer (its undo list included), the level-0
  minibuffer, `read-passwd`, `erase-buffer` and the variables against the
  GNU oracle; erc-sasl-tests, erc-tests, erc-scenarios-auth-source,
  auth-source-tests, minibuffer-tests, minibuf-tests,
  completion-preview-tests, rmc-tests, isearch-tests, comint-tests,
  simple-tests, undo-tests, buffer-tests, replace-tests, subr-tests,
  files-tests, data-tests, xdisp-tests, ert-x-tests, keyboard-tests,
  edebug-tests and kmacro-tests fail exactly GNU's tests when run as
  GNU's own files.
  Profile-guided builds, each trained on its own source: undo 1,086,993 ->
  1,087,600 instructions a unit (GNU 1,252,071). The text-property half
  took it to 1,072,834, the by-name reads and their string compares gone;
  the minibuffer half added 14.8K, nearly all of it insdel.c's `Fset
  (Qdeactivate_mark, Qt)` in each `prepare_to_modify_buffer` now that
  `deactivate-mark` is buffer-local (`set_internal_symbol` 646 -> 13,260 a
  unit inclusive: two hash lookups a store, the buffer's locals and then
  the binding, and the full path for a buffer's first store, which makes
  the local), where GNU stores through the symbol's loaded binding (blv) in
  a few instructions. Bytecode calls 417.7 -> 418.2 an iteration.
  Validation as round 44's: the whole library suite (3,175 tests, two of
  them new) passes but for the same known failures, all 98 groups on the
  round's final build; clippy `-D warnings`, rustfmt and `git diff --check`
  pass. The six erc tests round 44's frozen run failed pass, run as GNU's
  own test files. Still not as GNU: `get-text-property` at a narrowed
  buffer's end reads no interval (GNU reads the one there), and the buffer
  case of the single-char-property scans compares overlay and text values
  per character rather than through `next-char-property-change`.
- **Round 46: buffer-local variables read and stored through their loaded
  binding.** Round 45's `deactivate-mark`, buffer-local as in GNU, cost
  each modification's `Fset (Qdeactivate_mark, Qt)` two hash lookups (the
  buffer's local bindings, then the variable's) and a buffer's first store
  the whole assignment path; the per-buffer variables (`buffer-read-only`,
  `parse-sexp-lookup-properties`, `default-directory` and the rest, 116
  reads a unit in undo) paid the same lookups on every read. data.c keeps
  a localized variable's loaded binding in its blv (`where`, `valcell`);
  Emaxx now keeps one per symbol slot in each interpreter
  (`LoadedBindings`, 256 direct-mapped slots by the cell's address, beside
  the symbols that the interpreter states of a process share), naming the
  binding's value in a `Cell` whose node lasts as long as the binding.
  With the current buffer's binding loaded, a read through
  `find_symbol_value_redirected` or `buffer_local_binding_symbol` is a
  compare and a load (a variable whose value the buffer or the evaluator
  holds never loads), and so is a plain buffer-local variable's store. A
  store with no binding here makes one when the variable is local when set
  and no `let` of its default is in effect (let_shadows_buffer_binding_p),
  and loads it. Making or killing a binding unloads its symbol, as data.c
  swaps the loaded binding out; dropping or copying a buffer's bindings
  wholesale (a buffer's death, `kill-all-local-variables`, an indirect
  clone, an image load) advances a shape that retires every loaded
  binding. `is_forwarded_eval_cell`, which every new binding and load
  asked, compared the name with eight others (about 100 instructions); it
  now rejects by length first.
  `loaded_buffer_local_bindings_follow_data_c` checks reads and stores
  across buffers, lets, kills, watchers added after a load, byte-code and
  an indirect clone against the GNU oracle. Release builds: undo 1,187,477
  -> 1,169,255 instructions a unit (`buffer_local_binding_symbol` 16.8K ->
  6.7K a unit inclusive, `find_symbol_value_redirected` 24.7K -> 19.9K,
  `set_internal_symbol` 13.5K -> 9.9K). Profile-guided builds, each
  trained on its own source: undo 1,087,600 -> 1,084,787 instructions a
  unit (`find_symbol_value_redirected` 28.5K -> 18.1K inclusive,
  `set_internal_symbol` 13.3K -> 9.5K; the byte-code verifier's slow
  paths, which vary from build to build, +4.9K); bytecode calls 418.2 ->
  417.7 an iteration. Validation: 48 of the library suite's 98 groups ran
  on this round's own build and failed only the known failures (the 49th
  outlasted the background limit); the whole suite (3,177 tests) then ran
  on round 47's build, which adds only that round's allocator layout.
  Clippy `-D warnings`, rustfmt and `git diff --check` pass.
- **Round 47: a cons or float block's address names no cell.**
  `suspended_bytecode_retains_operand_and_unwind_roots` failed now and
  then under load (4 in 300 runs on rounds 44 and 45, 2 in 300 on a
  diagnostic build of round 46's source): after the worker joined, the
  weak table's key survived the collection. A diagnostic build printing,
  at each collection, the root that first reached the key found it held by
  a word of a live frame of the main thread (an `eval_cons_call` frame's
  `argvals` slot past the subr's arity, never written on the lambda path),
  and the word was the key's cons block's address with the key the block's
  first cell. A hardware write watchpoint on that slot (a
  `perf_event_open` breakpoint whose signal handler logged each write's
  instruction) showed the writer in passing runs too:
  `funcall_interpreted_lambda`, walking its lambda list, spilled the block
  address that `ConsRef`'s checked dereference forms (`cell & !0x7fff`) to
  reach the block's allocation bits; the key was later allocated from the
  same block, and the run failed when it took cell 0. Fcons and make_float
  form a cell's block address the same way on every allocation
  (`fill_cons`, `allocate_float`). GNU's cons and float blocks start with
  their cells too, but outside checking builds only its collector forms a
  block's address; in Emaxx every allocation does, so the bookkeeping
  (epoch, bitmaps, serials) now starts each cons and float block and the
  cells follow it (`cons_cell`, `float_cell`), and a block's address names
  metadata, which `mem_find` rejects. The cons cells start on a bitmap
  word's span (64 cells), so a cons's mark is still found by its slot from
  the block's address and Fcons's bit arithmetic is unchanged (the first
  version, indexing from the first cell, put a subtraction in every
  allocation). Vector blocks keep GNU's layout: their allocation never
  forms a block's address. `a_block_address_names_no_cons_or_float` checks
  that a word equal to a block's address keeps neither its first cons nor
  its first float. Release builds: cons allocation 5,087 -> 5,085
  instructions an iteration, mapcar 33.68M -> 33.51M a unit; undo 1,169.2K
  -> 1,179.0K a unit at 20 and 40 units, the difference all in collection
  and malloc-trimming steps whose slope round 46's run measured negative
  (the mutator's functions equal or lower). Profile-guided builds, each
  trained on its own source: undo 1,084,787 -> 1,078,785 instructions a
  unit (GNU 1,252,071), bytecode calls 417.7 -> 410.6 an iteration. The
  flaky test passed 300 runs of 300 beside the gate's four test processes.
  Validation: the whole library suite (3,178 tests) passes but for the
  known failures, all 98 groups on the round's final build. Clippy
  `-D warnings`, rustfmt and `git diff --check` pass.
- **Round 47 measured** (its profile-guided build, idle host). The
  contract-like comparison, run twice: 15 of the 16 workloads within 1.03
  of GNU's wall time, then all 16; the first run's bytecode calls 0.863
  [0.748, 1.034] had two of its nine rounds at 1.034 and 1.066 while GNU's
  own processes ranged 0.376 to 0.478 s (0.857 [0.724, 0.915] in the
  second run), and the closest otherwise were mapcar 0.873 [0.863, 1.013]
  and undo 0.892 [0.803, 1.010]. The locked runner itself
  (`tools/core_runtime_perf.py` prepare, calibrate and run, all
  `--diagnostic` because this host's GNU differs from the platform lock)
  completed every process, and every case's upper 95% bound is at or below
  0.996: mapcar 0.915 [0.849, 0.996], cons allocation 0.898 [0.869,
  0.955], bytecode-to-native 0.888 [0.842, 0.953], undo 0.883 [0.774,
  0.952], bytecode calls 0.853 [0.757, 0.933], the other medians 0.774 and
  below with upper bounds at most 0.850 (geometric mean 0.722). Its GNU
  self-comparison resolved only list traversal (the other intervals as
  wide as interpreted-to-native's [0.887, 1.084]), so the run certifies
  nothing: the criterion still needs a quiet host and the locked GNU
  build. Mapcar's instructions a unit are Emaxx 32.9 million to GNU's 46.4
  million; its wall ratio near 0.9 is memory traffic, 100,000 new conses a
  unit, not instructions.
- **Beyond the sixteen workloads** (round 47's profile-guided build;
  GNU's own ERT files, each run in both editors at once). Of the 230
  files under `test/src`, `test/lisp` and `test/lisp/emacs-lisp`, 226
  ran in both: their tests took 166.1 s in GNU and 239.6 s in Emaxx
  (1.44; 1.51 with process start). Of the 70 files whose tests take
  0.3 s or more in GNU, the median ratio is 1.63, 12 run faster in Emaxx
  and 18 at least twice as slow: keymap-tests 7.36
  (`describe-buffer-bindings` takes 0.72 s a call against 0.026 s,
  spread over the keymap code's lookups of symbols by name and its
  `RootedVec` records), cl-seq-tests 4.21 (`nthcdr`, round 48),
  ls-lisp 3.75, dired-aux 3.43, regex-emacs 3.33, help-fns 3.30, dabbrev
  3.05 and simple 2.70 (two `shell-command` tests, 11.2 s against
  4.4 s). In callgrind, 70% of dired-aux-tests is `beginning-of-line`
  and copies: each `beginning-of-line` scans every text-property span
  for a `field` (265,000 instructions a call), a search after an edit
  or over another range copies the searched text into a fresh string
  (`buffer_regexp_haystack`: 7,419 copies, 168 KB on average) and an
  edit shifts the vector of spans; 35% of help-fns-tests
  is fancy-regex's backtracking VM, lookaround patterns over NEWS files.
  info-xref-tests had not finished after 400 s (GNU 49 s);
  autorevert-tests timed out once at 300 s and ran in 17 s the next time,
  its remote tests failing; comp-tests timed out in GNU. Starting
  `-Q --batch` takes 0.19 s against 0.08 s: 508 million instructions
  against 166 million, about 260 million of them loading the image and
  140 million compiling regexps (73 regex-automata builds and one
  fancy-regex build of 41 million, a lookaround under Unicode case
  folding). A regexp compiled afresh costs about 42 µs against GNU's
  1 µs (2,000 distinct `string-match` patterns: 0.084 s against
  0.002 s). In a 5 MB buffer plain searches run 3 to 30 times faster
  than GNU's, but `\_<...\_>` runs 10 times slower, a case-folded `\b` 9
  times and a back-reference 1.5 times. Building an 8-million-cell list
  takes 2.2 times GNU's time and 1.8 times its memory.
- **Round 48: `nthcdr` counts down as fns.c does.** Above 127, `nthcdr`
  (and `nth`, list `elt` and `last`, which walk through it) kept every
  cell it passed in a `HashMap` and counted down in a `BigInt`, about
  400 ns a cell: 3.2 s for an 8-million-cell list against GNU's 0.044 s,
  and `cl-seq-test-bug24264`, whose `last` calls walk such lists, took
  25 s against 10.7 s. It is now Fnthcdr: an `i64` count (a positive
  bignum counts down from `i64::MAX`, and the cycle reduction adds back
  the difference), FOR_EACH_TAIL_SAFE's tortoise counted as lisp.h counts
  `li.q` and `li.n`, the cycle's length taken from where the tortoise
  last moved, and `rarely_quit` each 2^16 cells. `proper-list-p` counts
  the tails in place (it copied the list into a vector), and list `elt`
  reads the head before it checks for a `vector-literal` list (it copied
  every list first). On 8 million cells, against GNU: `nthcdr` 0.75,
  `nth` 0.78, `last` 0.90 and `proper-list-p` 0.92 (73, 80, 54 and 3.3
  before); cl-seq-tests 3.7 s against round 47's 16.1 s (GNU about 9 s).
  `nthcdr_large_counts_match_a_plain_walk_past_a_prefix` checks fixnum
  and bignum counts on a cycle reached after a prefix against a plain
  walk (GNU gives the same answers) and a count that ends at a dotted
  tail. Validation: 387 list, keymap, sequence, `cl-`, `apply`,
  membership and circular-list tests pass (gate profile, one thread a
  process), the new one with them; GNU's fns, subr, cl-seq, seq, cl-lib,
  data and subr-x test files pass on the release build. Clippy
  `-D warnings`, rustfmt and `git diff --check` pass; the full Rust gate
  runs in CI.
- **Round 49: line motion looks for a field only where it moves.**
  `beginning-of-line`, `line-beginning-position` and `line-end-position`
  asked `constrain-to-field` for their result only when the buffer had a
  `field` property anywhere, found by scanning every text-property span
  and comparing its names: 265,000 instructions a `beginning-of-line` in
  dired-aux-tests, whose listings put properties on every line. They now
  test the gate Fconstrain_to_field opens with, a `field` at or just
  before the new or the old position (`field_near`, four lookups, which
  `constrain-to-field` itself now uses); past a failed gate the call
  returned the new position anyway. dired-aux-tests 2.50 s -> 1.75 s
  (GNU 0.88 s), ls-lisp-tests 2.38 s -> 1.43 s (GNU 0.66 s), round 47's
  profile-guided build against this round's release build. Validation:
  109 field, line-motion, minibuffer and comint tests pass (gate
  profile, one thread a process); GNU's editfns, simple, minibuffer,
  comint, subr and subr-x test files give the results they gave before.
  Clippy `-D warnings`, rustfmt and `git diff --check` pass.
- **Round 50: regex-emacs.c and search.c, ported; casetab.c's case
  tables.** Elisp regexps were translated to `fancy-regex`/`regex`
  syntax, with syntax classes, categories and case folding rendered into
  the translated pattern and the haystack copied out of the buffer for
  each search; a syntax-table or `syntax-table` property change
  re-rendered both. Font-lock over subr.el took 24.7 s (GNU 0.27 s), a
  new pattern 38 µs to compile (GNU 2.5 µs), `\_<defun\_>` over a 5 MB
  buffer 0.32 s (GNU 0.020 s). `src/lisp/regex_emacs/` is now
  regex-emacs.c: `regex_compile` into GNU's opcodes (exactn in the
  buffer's storage form, charset bitmaps with range tables, `succeed_n`
  counters, `on_failure_jump_smart` rewritten by `mutually_exclusive_p`),
  the fastmap by stored lead byte (`analyze_first`), and
  `re_match_2_internal` with GNU's failure stack (40,000 × 20 items,
  "Stack overflow in regexp matcher") reading the rope's chunks in place,
  with the syntax class, category and case translation of each character
  asked of the buffer or string at that position (`syntax-table`
  properties and `syntax-propertize` included). `src/lisp/primitives/
  search.rs` is search.c's side: the 20-entry pattern cache keyed as
  `compile_pattern` keys it (the pattern, its translate table, posix,
  `search-spaces-regexp`, and the syntax table only for a pattern that
  read one while compiling, `[[:space:]]` and `[[:word:]]`), cleared of
  those by `modify-syntax-entry` (`clear_regexp_cache`), `looking-at`,
  `string-match`, the four searches, `re--describe-compiled` (RAW gives
  GNU's bytecode) and the `fast_*` entry points the C code uses (file
  name handlers, `directory-files`, completion, compositions,
  tree-sitter predicates, coding-system lookups). The old translator
  and its caches (6,200 lines, `fancy-regex` and `regex-automata` with
  them) are gone. Case folding needed casetab.c: `set-case-table` and
  `set-standard-case-table` now complete a table (the upcase table by
  `set_identity` and `shuffle`, the canonicalization DOWN[UP[DOWN[c]]],
  the equivalences) and store all four tables in the buffer's slots,
  as buffer.h keeps them; a buffer keeps the four it was given when the
  standard ones change, `kill-all-local-variables` and a new buffer
  take the standard ones (completing an invalidated standard table
  first), `upcase`/`downcase` reinstall a table whose canonicalization
  was cleared, and the image carries the four per buffer and the
  standard four. A folded search keys its pattern on the buffer's
  canonicalization, as GNU's translate table, so a store into another
  char-table (`regexp-opt`'s charset table) compiles nothing again.
  `marker_byte_position` counted the bytes before a position by walking
  the text (O(n) per call in a non-ASCII buffer, which made the new
  matcher's match data cost 58 times GNU's fontification); the text now
  keeps a count of its raw-byte markers, so a byte position is the rope's
  byte offset adjusted by that count and the side table. A unibyte
  buffer stores the bytes 0x80 to 0xFF as U+0080 to U+00FF where strings
  keep raw-byte markers; the matcher reads them as the raw-byte
  characters (a unibyte buffer's `\220` had matched nothing). Against
  GNU, over the 5 MB elisp.info (GNU / Emaxx / round 47's profile-guided
  build): `\*note` 0.0140 / 0.0065 / 0.0036 s, node separators 0.0150 /
  0.0039 / 0.0026, `[ \t]+$` 0.170 / 0.127 / 0.0097, `\_<defun\_>`
  0.0199 / 0.0157 / 0.324, folded `\bfunction\b` 0.0425 / 0.120 /
  0.860, `\(\w+\) \1` 0.714 / 0.700 / 0.936, folded xref 0.0345 /
  0.0552 / 0.0107, 200,000 `looking-at` 0.201 / 0.350 / 0.202, 200,000
  `string-match` 0.402 / 0.449 / 0.319, 2,000 distinct patterns 0.0050
  / 0.0030 / 0.0757. Font-lock: subr.el 0.265 / 0.500 / 24.7 s, forty
  edits each refontified 0.086 / 1.19 / 14.9 s; xdisp.c 10.9 / 22.3 s
  (GNU / Emaxx); two hundred edits refontified in subr.el 0.36 / 5.2 s,
  simple.el 0.33 / 7.2 s, python.el 0.32 / 4.6 s, xdisp.c 4.4 / 10.6 s.
  The refontification gap is syntax scanning (`syntax-ppss`,
  `parse-partial-sexp`), the next round's. Tests: the old engine's
  count tests (renderings, encodings, compiles) are GNU results now,
  with a test-only compile counter asserting compile_pattern's reuse,
  `clear_regexp_cache`, and that a folded pattern survives stores into
  other tables and into the downcase table;
  `set_case_table_completes_a_table_and_buffers_keep_the_tables_they_were_given`
  checks casetab.c's tables and a buffer's kept canonicalization
  against the oracle; `src/lisp/regex_emacs/tests.rs` has fourteen
  matcher tests with GNU's answers. Two old expectations were not
  GNU's: `[\-[:alnum:]]` is `\-[` (an empty range) plus `:alnum:` and
  closes before a literal `]` (it matched `--tofu-policy=`), and a
  lambda parameter named `case-fold-search` is lexical in the batch
  image (the test let-binds it now). Syntax lookups read each entry's
  class from the entry, as SYNTAX does, so a descriptor changed in place
  is seen at once. A POSIX pattern ends without a final `succeed` (GNU
  reads past its buffer when a loop's tail runs to the end); the
  first-character walk takes the end as matching the empty string.
  Validation: the regex, search, case, syntax, completion, marker,
  match, dump, image, case-table, compile-count, POSIX and anti-cheat
  filters pass (gate profile, one thread a process,
  `RUST_MIN_STACK=134217728` as CI sets it), but for
  `batch_startup_image_round_trip_or_explicit_native_image_limit`, which
  fails in this container as before (below); thirty GNU test files
  (regex-emacs, search, rx, regexp-opt, syntax, casefiddle, subr,
  subr-x, simple, replace, thingatpt, isearch, minibuffer, minibuf,
  files, dired, fill, font-lock, elisp-mode, python, ruby, css, cc-mode,
  buffer, editfns, fns, character, coding, mule-util, lisp) pass on the
  release build. Clippy `-D warnings`, rustfmt and `git diff --check`
  pass.
- **Round 51: syntax.c's scanners, ported.** After round 50,
  refontifying after edits was 12 to 15 times GNU's, and the cause was a
  divergence, not a slow loop: `scan-lists`, `scan-sexps` and
  `forward-comment` called `syntax-propertize` to `point-max` before
  scanning, where GNU propertizes just in time as a scan reaches
  `syntax-propertize--done`. Through
  `lisp-font-lock-syntactic-face-function`'s `forward-sexp` that made
  `elisp-mode-syntax-propertize` run `syntax-ppss` over the rest of the
  buffer at each refontification: 1,517 `parse-partial-sexp` calls over
  4,545,728 characters against GNU's 941 over 540,267 for ten edits of
  subr.el. The scanners also read the buffer through a decoded
  `Vec<char>` chunk and re-decoded every syntax descriptor, and
  `parse-partial-sexp` returned a private element 10 (the state of its
  own parser) where GNU returns the previous character's syntax.
  `src/lisp/primitives/syntax_scan.rs` is now syntax.c's scanning core:
  gl_state (`SETUP_SYNTAX_TABLE`, `update_syntax_table` over the
  buffer's intervals, merging up to ten with an `eq` property,
  `update_syntax_table_forward`, `parse_sexp_propertize` calling
  `internal--syntax-propertize` with one past the position about to be
  read, and `e_property_truncated`), `char_quoted`, `find_defun_start`
  with its cache, `prev_char_comend_first`, `back_comment` with its
  lossage path, `forw_comment`, `Fforward_comment`, `scan_lists`,
  `Fbackward_prefix_chars`, `skip_syntaxes`, `in_2char_comment_start`,
  `scan_sexps_forward`, `internalize_parse_state` and
  `Fparse_partial_sexp`. Positions travel as GNU's (charpos, bytepos)
  pairs over a snapshot of the rope read in place a chunk at a time
  (bytes count Emaxx's UTF-8 and never leave the module); the snapshot
  holds through the Lisp a scan calls because GNU signals when
  `internal--syntax-propertize` or `syntax-ppss` changes the text.
  SYNTAX reads the descriptor's car as GNU does (whitespace for a
  non-cons); ASCII entries are remembered per table and forgotten after
  any Lisp call, the only way an entry can change mid-scan. The C
  variables (`parse-sexp-lookup-properties`, `syntax-propertize--done`,
  `parse-sexp-ignore-comments`, `comment-end-can-be-escaped`,
  `multibyte-syntax-as-symbol`, `open-paren-in-column-0-is-defun-start`,
  `comment-use-syntax-ppss`) are read when first needed and again after
  each Lisp call, which is when GNU's live reads could see them change.
  A unibyte buffer's bytes read as raw-byte characters for the scans and
  as themselves for `skip-syntax-*`, as FETCH_CHAR_AS_MULTIBYTE and
  skip_syntaxes's `*p` do (Emaxx stores such a byte either as U+0080..
  U+00FF or as a raw-byte marker). A runtime without syntax.el treats
  `comment-use-syntax-ppss` as nil and has nothing to propertize with.
  The old scanners (2,260 lines of syntax.rs) are gone. Against GNU (GNU /
  Emaxx; round 50's Emaxx after the slash): font-lock over subr.el 0.268
  / 0.273 s (0.500), two hundred edits refontified 0.403 / 0.397 s (5.2);
  simple.el 0.338 / 0.375 and 0.318 / 0.428 s (7.2); python.el 0.175 /
  0.236 and 0.251 / 0.380 s (4.6); xdisp.c in C mode 9.29 / 14.4 and 3.29
  / 6.20 s (22.3 and 10.6). Ten edits of subr.el refontified: 0.016 /
  0.026 s. What is left in C mode is cc-engine's Lisp, not scanning. A
  differential probe of every position of buffers in Emacs Lisp, C,
  Python, a Haskell-like table (nested `{- -}`, `--` comments, comment
  and string fences, math, prefix flags, `comment-end-can-be-escaped`,
  `parse-sexp-ignore-comments` nil), `syntax-table` properties (cons
  and char table), narrowing, multibyte text with and without
  `multibyte-syntax-as-symbol`, a unibyte buffer and unbalanced text
  with `comment-use-syntax-ppss` on and off (8,130 results of
  `parse-partial-sexp` from 1 and in `syntax-table` steps, `scan-sexps`
  both ways, `scan-lists` up, `forward-comment` 1, -1 and to either end,
  `skip-syntax-*`, `backward-prefix-chars` and `syntax-ppss`) is
  identical to GNU's. New tests with the oracle's answers:
  `parse_partial_sexp_state_carries_the_syntax_a_two_char_construct_started_with`
  (element 10 and resuming inside `#|`), `scans_propertize_only_as_far_as_they_read`
  (the propertizer's calls for `scan-sexps`, `forward-comment` and
  `parse-partial-sexp`) and
  `scans_read_a_unibyte_buffer_as_raw_bytes_and_skip_syntax_as_bytes`;
  `syntax_ppss_reports_negative_depth_for_extra_closing_parens` expected
  the old private element 10 and now expects GNU's nil. Validation: the
  133 tests that call the scanners or the modes built on them pass
  (gate profile, one thread a process, `RUST_MIN_STACK=134217728`), but
  for the image round trip that fails in this container (below), as do
  the syntax, comment, paren, ppss, prefix, sexp, skip and scan-list
  filters but for the as-root `save_buffer_skips_unmodified_and_unchanged_files`
  (below); thirty-three GNU test files (syntax and emacs-lisp/syntax,
  elisp-mode, lisp-mode, lisp, python, cc-mode, ruby, js, css, sh-script,
  perl, cperl, octave, f90, sql, scheme, opascal, pascal, tcl, ps-mode,
  bat-mode, sgml, paragraphs, fill, newcomment, electric, hideshow,
  thingatpt, simple, grep, xref, package) give GNU's results on the
  release build, and the probe and fifteen of them on the gate build
  (overflow checks on), but for package-tests' three asynchronous archive
  tests, which fail the same way on round 50's build in this container
  (its HTTP proxy settings meet the tests' local server) and passed in
  round 50's CI. Round 50's frozen run (below) stopped at
  `src/undo-tests.el`: `BufferText::check_counts`, a debug-build check
  CI's gate profile runs, recounted the raw-byte markers of the whole
  text at every edit since round 50, which made undo-test4's million
  insertions quadratic (over 180 s, killed). It now recounts a small
  text's only (at most 1 KB); on the gate build undo-tests take 2.2 s
  and the three files after them (xdisp, xfaces, xml) give GNU's
  results. Clippy `-D warnings`, rustfmt and `git diff --check` pass.
  Round 51's CI then found the loader's `eval-region` and `eval-buffer`
  skipping the whitespace between forms with `forward-comment` of
  `i64::MAX / 2`, a bignum here, which the ported count check rejects as
  CHECK_FIXNUM does; the error was dropped, so edebug's and testcover's
  reading went wrong. They pass the buffer's size plus one, a fixnum, and
  cl-generic, cl-macs, edebug, gv and testcover tests give GNU's results
  again, as do the 13 eval-region, eval-buffer and load tests.
- **Round 52: the matcher's syntax lookups on gl_state, `scan_words`,
  and a matcher call's fixed cost.** The matcher still asked syntax
  classes of round 50's `SyntaxScan`, which refreshed per interval,
  decoded integer and string `syntax-table` values GNU reads as
  whitespace, looked at buffers only and ran `internal--syntax-propertize`
  before every lookup. It now does as regex-emacs.c does:
  `RE_SETUP_SYNTAX_TABLE_FOR_OBJECT` when `re_search_2` or `re_match_2`
  starts, for the current buffer (GNU's nil), a string with its own text
  properties, or text without an object (GNU's t, the `&str` callers);
  `UPDATE_SYNTAX_TABLE` before each `SYNTAX` of the text; and
  propertizing only where `update_syntax_table_forward` does, when a
  lookup in the buffer crosses `e_property` past
  `syntax-propertize--done`. `syntax_scan.rs`'s `GlState` serves both:
  `update_syntax_table` walks a buffer's or a string's intervals, and
  `SETUP_SYNTAX_TABLE`, `update_syntax_table_forward` and
  `parse_sexp_propertize` moved into it from the scanner (which reads its
  own state again after the propertizer runs). `BUFFER_SYNTAX` reads the
  entry's car, whitespace for a non-cons. `forward-word` and the `-word`
  case commands are syntax.c's `scan_words` (with
  `words-include-escapes`, a `find-word-boundary-function-table`
  function checked with `fboundp`, so a non-symbol signals, and
  category.c's `word_boundary_p` over `char-script-table` and the
  separating and combining categories), `Fforward_word` (a fixnum count,
  BEGV or ZV when the text ends first, `constrain-to-field`, t only when
  the count was reached) and casefiddle.c's `casify_word` (point at the
  cased region's end for either sign; `case-symbols-as-words` is not
  word motion's). The old `forward_word`, which had none of these, and
  `case_word_region` are gone, as are `SyntaxScan` and syntax.rs's
  one-shot property lookups. A matcher call's fixed cost: the pattern
  cache moved whole entries (pattern, fastmap and all) to its front at
  every call and now moves pointers; the 128 ASCII syntax entries copied
  into and out of every call are gone (a char-table read is cheap);
  variables C reads are read as C reads them (`symbol_value_key`, no
  lexical walk); an ASCII buffer's snapshot needs no rope walk and is
  sliced once; the failure stack and registers are reused across calls;
  and a case-folded fastmap scan tests ASCII bytes against a verdict
  table the pattern keeps per char-table store count instead of decoding
  and translating each character. Instructions (GNU's in brackets): five
  case-folded `\bbar\b` passes over 4,000 lines, `re-search-forward`
  328.5M before, 111.3M after [106.3M]; 200,000 `looking-at` calls
  1,337.6M, 420.1M [152.1M]; 20,000 `(forward-word 10)` 347M of
  `scan_words`, 269M [110M]. Wall on an idle host, GNU / Emaxx: twenty
  case-folded `\bbar\b` passes 0.040 / 0.041 s, `\_<foo-[0-9]+\_>`
  0.044 / 0.052, `\<defun\>` 0.042 / 0.043; a million `looking-at` 0.54
  / 0.58 s and `string-match` 0.69 / 0.69 to 0.73; a million
  `looking-at-p` 0.53 / 0.66 (the call into byte code); 100,000
  `(forward-word 10)` 0.117 / 0.139 (`word_boundary_p`'s four char-table
  reads a character pair). Differential probes identical to GNU's: 776
  results of forward and backward searches, `looking-at` at every
  position and `string-match` with and without
  `parse-sexp-lookup-properties` over text with cons and char-table
  `syntax-table` properties, `forward-word` by 1, 2, -1 and -3 at every
  position with and without `words-include-escapes`, a boundary function,
  fields and the case commands with seven counts at five positions; and
  24 with the propertizer's calls logged for searches both ways,
  `looking-at` and word motion over text with `syntax-table` intervals,
  and propertizers that change the text or never move. New tests with
  the oracle's answers: `regexps_read_syntax_table_properties_of_strings_and_buffers`,
  `regexp_searches_propertize_only_past_their_property_bounds` and
  `word_motion_and_case_commands_follow_scan_words`. Twenty GNU test
  files (syntax, regex-emacs, search, casefiddle, subword, thingatpt, rx,
  simple, python, ruby, cperl, sh-script, css, lisp-mode, elisp-mode,
  js, fill, octave, sgml, nxml) give GNU's results.
- **Round 53: keymap.c's `describe_vector`, ported.** Emaxx's
  `help--describe-vector` expanded a char-table into its characters
  (`char_table_effective_ranges`, then every code of every range), looked
  each one up in ENTIRE-MAP and SHADOW with full key lookups, and merged
  neighbours afterwards: `describe-buffer-bindings` of the global map ran
  4.19 million lookups for the 4 million characters bound to
  `self-insert-command`. It is now keymap.c's loop: `char_table_ref_and_range`
  (`CharTable::ref_and_range`, a run of `eq` values within the bounds GNU
  passes, with `range_value` its first value) gives each run once, split at
  `MAX_5_BYTE_CHAR` as GNU's STOP; the run's first character alone is
  checked against SHADOW (`shadow_lookup`, defaults accepted, no
  remapping, a binding `eq` to the definition not shadowing it) and
  ENTIRE-MAP (`Flookup_key` with defaults, `eq`); `get_keyelt` without
  autoloading; a vector's following elements are merged while `equal`;
  `describe-bindings-check-shadowing-in-ranges` (with `ignore-self-insert`)
  shortens a char-table run at the first character whose shadowing
  binding differs, C's live `j <= i` included; keys are
  `describe_key_maybe_fontify` (`key-description` with the prefix, then
  `font-lock-face help-key-binding` over the whole description); the
  shadow note goes before the describer's newline; and a char-table's
  default is described last. `standard-output` is bound to the current
  buffer for the whole call, and a non-array signals
  `(wrong-type-argument vector-or-char-table-p ...)`. Wall (GNU / Emaxx
  before / after): `describe-buffer-bindings` three times 0.12 / 2.03 /
  0.13 s; keymap-tests 0.36 / 3.13 / 0.53 s. Differential probe
  (`describe-buffer-bindings` of the global map, emacs-lisp-mode and the
  `C-x` prefix, and `help--describe-vector` over a char-table with
  ranges, a default, shadowing with and without MENTION-SHADOW, a plain
  vector with a prefix and `describe-bindings-check-shadowing-in-ranges`):
  identical to GNU's except where they already differed. Those are (a)
  `key-description` of codes past the Unicode range, which Emaxx prints
  as `[4194175]` where GNU prints the raw-byte character (Emaxx keeps such
  characters as a placeholder plus a side table), and the column widths
  that follow from it; (b) touch-screen and `XF86WakeUp` bindings, which
  this container's GNU has because it was built with X and loadup loads
  touch-screen.el under `(featurep 'x)`; and (c) prefix commands
  (`C-c`, `C-h`, `C-x`, `ESC`, `<f1>`, `<f2>`, `<help>`) that Emaxx lists
  and help.el's `describe-map` drops, because `keymapp` answers nil for a
  symbol whose function is a keymap (fixed in round 54). New test:
  `help_describe_vector_describes_each_run_of_a_char_table_once`. 239
  keymap, key-description, menu, help and `where-is` Rust tests pass, and
  six GNU test files (keymap, help, help-fns, help-mode, kmacro, subr)
  give GNU's results, apart from two help-fns tests that this container's
  GNU itself fails.
- **Round 54: keymap.c's accessors and mutators, ported.** `keymapp`
  answered nil for a symbol whose function is a keymap (every prefix
  command: `Control-X-prefix`, `mode-specific-command-prefix`), so help.el
  listed prefix commands that GNU drops (bullet "Round 53"), and no
  primitive followed a keymap autoload: `lookup-key`, `keymap-parent`,
  `map-keymap`, `use-local-map` and others signalled for an autoloaded
  map, or answered nil, where GNU loads it (20 of 24 probes differed).
  The cause was a reimplementation that matched bindings through key
  descriptions and approximated parts. `primitives/keymap.rs` now ports
  keymap.c: `get_keymap` (a list, a symbol's function indirection, an
  autoload of TYPE `keymap` loaded when asked and otherwise itself, so
  KEYMAPP holds for it), `access_keymap_1` (Qunbound as `Value::Unbound`,
  T_OK, NOINHERIT and AUTOLOAD threaded through, the meta-prefix-char
  mapping, composed submaps, vectors and char-tables), `get_keyelt` (a
  menu item's `:filter` evaluated as the form `(FILTER 'DEFN)` under an
  error handler, so `#'f` gives nil as in GNU), `lookup_key_1`,
  `Fdefine_key` with `store_in_keymap` and `define_as_prefix` (Lucid
  event lists, ranges as leaves, the meta bit by key type, `[keymap]`
  reserved, `message_with_string`'s CHECK_STRING for invalid events,
  "Key sequence %s starts with non-prefix key %s%s", removal from a
  char-table storing nil, character ranges into a new char-table instead
  of the old code's broken spine), `silly_event_symbol_error`,
  `keymap_parent`, `Fset_keymap_parent` (the parent not loaded, cycles
  checked through `keymap_memberp`), `map_keymap`, `Fkeymap_prompt`,
  `Fuse_local_map` and `Fuse_global_map` (storing the resolved map),
  `Faccessible_keymaps` with `accessible_keymaps_1` (breadth first, cycles
  by prefix, metized sequences right after their parent), and
  `Fwhere_is_internal` with `where_is_internal`, `where_is_internal_1`,
  `shadow_lookup` and `preferred_sequence_p`, including keymap.c's
  `where_is_cache` for menu-less lookups (two interpreter fields, rooted,
  flushed by `store_in_keymap` and `set-keymap-parent`, reset in an image
  copy). keyboard.c's `reorder_modifiers` keeps `parse_modifiers`'
  `event-symbol-element-mask` and `apply_modifiers`' `modifier-cache`
  as GNU does, and a wheel event is a click unless double or triple, as
  `parse_modifiers_uncached` says. The bootstrap `special-event-map` now
  has keyboard.c's `keys_of_keyboard` entries (`ns-put-working-text`,
  `focus-in`, `focus-out`, `move-frame`, `file-notify`; not the
  commented-out `select-window`), and `visual-line-mode-map` has no
  prompt string. The old helpers (`keymap_define_binding_with_placement`,
  exact-parts lookups, the where-is collector) are gone. Wall, GNU /
  Emaxx before / after: 60,000 `define-key` 0.376 / 0.818 / 0.461 s;
  60,000 `lookup-key` 0.038 / 0.090 / 0.045 s; 200 `where-is-internal`
  of `find-file` 0.035 / 0.164 / 0.067 s (one `get_keymap` a binding cost
  137 instructions until the list case stopped going through the symbol
  key table and `==` on values stopped being `equal`). Probes identical
  to GNU's: autoloaded keymaps through 24 primitives, 17 `define-key`
  cases, `keymap--get-keyelt` with filters, and `describe-buffer-bindings`
  apart from the X build's bindings and raw-byte key descriptions. New
  test: `keymaps_resolve_through_symbols_and_autoloads_as_get_keymap_does`.
  Twenty-three GNU test files (keymap, keyboard, cmds, minibuf, help,
  help-fns, help-mode, kmacro, subr, simple, minibuffer, repeat, tab-bar,
  isearch, edmacro, which-key, xt-mouse, mouse, easy-mmode, use-package,
  electric, buff-menu, dired) give GNU's results. Locally (gate profile,
  `--test-threads=1` in four shards) 779 keymap, key, menu, where-is,
  help, minibuffer, event, image, dump and collection tests pass, apart
  from the image round trip that fails in this container and
  `subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`,
  whose child exited before `process-live-p` under the shards' load and
  which passes alone three times out of three. Two tests that call
  `where-is-internal` with a keymap in a bare interpreter now install a
  global map first: keymap.c's `list2 (keymap, current_global_map)`
  meets nil there, which GNU (whose `global-map` comes from subr.el)
  signals for too. The anti-cheat tests pass single-threaded, as CI runs
  them; run with several threads they can panic in the shared vector
  allocator while interpreters are built in parallel.
- **Round 55: search.c's plain-string search and decode_coding_gap's
  ASCII optimization, ported.** `describe-function` was 2.4 times GNU's
  (five calls 0.51 s against 0.216): `help-fns--first-release` reads
  every etc/NEWS* file, and Emaxx took 0.31 s for what GNU reads in
  0.07. Three causes. `search-forward` was no port of search.c: it
  copied the buffer in 4,096-character windows and lowercased each with
  Rust's `to_lowercase` (300 million instructions for
  `sgml-html-meta-auto-coding-function`'s case-folded `<html` search
  over the auto-coding work buffers), ignored
  `inhibit-changing-match-data`, folded with Unicode's mapping instead of
  the buffer's case tables, signalled with a copy of the string and
  accepted a bound on the wrong side. `primitives/search/non_re.rs` now
  ports `search_buffer_non_re` (the pattern converted to the buffer's
  multibyteness, translated through `case-canon-table`, and
  `boyer_moore_ok` decided from `case-eqv-table` as GNU does, so `ſ` or
  the KELVIN SIGN sends a pattern to `simple_search`), `simple_search`
  and `boyer_moore` (the stride table, `simple_translate` and the
  last-byte translation test from `char_base`'s leading bytes, the
  20,000-byte clumps with a quit check each, the short-clump loop, and
  match data set at each occurrence, so a count that runs out leaves the
  last one found, while `simple_search` leaves the match data alone), and
  `search_command`/`search_buffer` are shared with the regexp commands:
  a trivial regexp with no `search-spaces-regexp` is searched as its
  string, as GNU does. The comparisons are on the rope's stored form,
  which is GNU's internal form for every character to U+10FFFF; a
  pattern character with no stored form of its own, or a raw byte where
  the text has a side table, makes the search compare characters. A
  unibyte buffer's bytes are read a chunk at a time. Second, the NEWS
  file that is pure ASCII declares `us-ascii`, a charset coding: each
  byte asked the charset's dimension and code space by name through its
  plist (44 ms a read against GNU's 0.3). `decode_coding_gap`'s ASCII
  optimization is ported for `insert-file-contents` (an ASCII-compatible
  coding with no post-read conversion and no decoding translation
  table, `disable-ascii-optimization` nil, and only ASCII bytes: the
  bytes are the text). It also removes a divergence: a pure-ASCII file
  read as `iso-latin-1`, `koi8-r` or `windows-1252` got a `charset` text
  property GNU never gives it. Third, the raw-text copy of a file for
  the auto-coding functions went byte by byte, and inserting into a
  buffer with markers walked every character to compute GNU's byte
  length; both now copy ASCII runs or scan for the one lead byte
  (0xEE) whose characters differ, with the same results. Wall, GNU /
  Emaxx before / after: five `describe-function` calls 0.216 / 0.51 /
  0.25 to 0.29 s; reading every etc/NEWS* five times 0.065 to 0.078 /
  0.31 to 0.36 / 0.083 to 0.099 s. A differential probe of
  `search-forward` and `search-backward` (four texts, multibyte and
  unibyte, 28 patterns including raw bytes, case folding on and off,
  counts nil, 2, -1 and 3: 3,603 lines) gives GNU's output, as do the
  bound, noerror, empty-string, zero-count, inhibited-match-data,
  narrowing and trivial-regexp cases and the file-reading probe of
  codings, `charset` properties and `last-coding-system-used`. New test:
  `plain_string_searches_are_search_c_simple_and_boyer_moore`;
  `case_folded_patterns_ignore_writes_to_tables_that_are_not_case_tables`
  now searches `fo+`, since `foo`, a trivial regexp, is no longer
  compiled. Fifteen GNU test files (search, coding, fileio, help-fns,
  replace, isearch, mule, mule-util, casefiddle, subr, files, subr-x,
  fill, elisp-mode, thingatpt) give GNU's results (this container's GNU
  fails two help-fns tests for want of subr.el, which Emaxx passes);
  locally (gate profile, `--test-threads=1`, four shards) 420 search,
  coding, file, keymap, marker and anti-cheat tests pass but the two
  known container failures. Still to do: the charset functions read their attributes by name from plists
  (`decode_charset_code` scans a map charset's whole table for each
  code), which wants charset.c's `struct charset` table.
- **Round 56: charset.c's charset table and coding.c's coding-system
  table.** Decoding a 20,001-byte string three times took 0.08 to 0.13 s
  through any map charset (iso-latin-2, koi8-r, cp437, iso-8859-7,
  windows-1252) against GNU's 0.001: every code asked the charset's
  attributes by name through its plist and searched the map's pairs in
  order, cloning them. `primitives/coding/charset_table.rs` is the
  `struct charset` table: each charset's attributes (dimension, code
  space and bounds, `char_index_offset`, the offset, map, subset and
  superset methods, unification, `:ascii-compatible-p`,
  `:emacs-mule-id`) read once from its plist, the map's decoder and
  encoder built as hash tables the first time it is used
  (`load_charset_map`, first entry winning), a map file parsed once a
  process. It is emptied when a charset, an alias, a plist or a
  unification changes. Encoding was as slow for another reason: each
  coding-system lookup scanned the thousand definitions by name and
  cloned the one found, several times a character. The coding systems
  and the charsets now have name indexes (GNU's
  `Vcoding_system_hash_table` and `Vcharset_hash_table`) and lookups
  return the definition in place; `coding-system-get` and the charset
  table read their plists as pairs, as `plist-get` does (they matched a
  key in a value's position too). The ISO-2022 encoder resolves its
  charset list and the use-roman and use-oldjis charsets once
  (`setup_coding_system`), and the unencodable-character scan builds
  the ISO-2022 attributes and the charset list once a text instead of
  once a character; the emacs-mule encoder takes `:emacs-mule-id` from
  the table. `encode_coding_charset`'s loop is ported: a raw byte is
  written as itself (it was read as the Unicode character of its value,
  so iso-latin-3 wrote a space for the raw byte A5), a code is written
  in as many bytes as the charset's dimension, and an unencodable
  character is the default character's low byte. Two divergences found
  on the way are fixed: the five charsets charset.c defines now carry
  `define_charset_internal`'s plist (`charset-dimension` of `ascii` was
  nil and `charset-iso-final-char` -1, GNU's 1 and 66), and an
  undecided eol is settled from the decoded text by `decode_eol`'s rule
  (CRLF with stray CRs is dos, CR alone mac, any other mixture unix,
  converting nothing; Emaxx named the coding `-unix` and still turned
  every lone CR into a newline). Wall, GNU / Emaxx before / after, three
  conversions of 20,001 characters: decoding iso-latin-2 0.001 / 0.126 /
  0.002 s, koi8-r 0.001 / 0.121 / 0.002, cp437 0.010 to 0.016 / 0.134 /
  0.002 to 0.003; encoding iso-latin-1 0.0006 / 0.063 / 0.0015, iso-2022-jp
  0.0032 / 0.101 / 0.0025, gb2312 0.0033 / 0.061 / 0.0030, euc-kr
  0.0042 / 0.066 / 0.0037, emacs-mule 0.017 / 0.064 / 0.026. A probe
  decoding all 256 bytes, the 9,216 two-byte pairs above 0xA0 and 1,900
  SJIS pairs through 42 codings and encoding them back, plus
  `decode-char`, `encode-char` and `make-char` over 26 charsets, went
  from 6.3 s to 0.97 s (GNU 0.29) with the same output as before but for
  the fixes above; its decodes and its charset-level answers are GNU's.
  New test: `charset_codings_go_through_the_charset_table_like_charset_c`.
  Thirteen GNU test files (character, charset, chartab, coding, ccl, mule,
  mule-util, textsec, fileio, files, process, fns, editfns) give GNU's
  results; ucs-normalize-tests fails `ucs-normalize-part1` as it did
  before the round, on U+E080..U+E0FF, the private-use range the text
  representation spells raw bytes with. Still to do, the encode side of
  coding.c: `consume_chars` reads a unibyte source as internal multibyte
  sequences for every encoder but raw-text and CCL (`C0 A0` is the raw
  byte A0, a valid sequence its character), where Emaxx takes each byte
  as a raw byte, so encoding a unibyte string with bytes above 0x7F
  differs; Emaxx scans for unencodable characters by coding kind before
  encoding, substitutes per kind and can signal "Character cannot be
  encoded", which GNU's encoders never do; and windows-1251,
  windows-1252 and koi8-r go through encoding_rs instead of their
  charset maps. That wants `encode_coding_object` and the encoders
  ported whole.
- **Round 57: `equal`'s EQ test and `get_doc_string`'s block read.**
  `substitute-command-keys` was 24 times GNU's (fifty rounds of twelve
  calls, 0.47 s against 0.019): `where_is_internal` checks its cache
  with `Fequal (keymaps, where_is_cache_keymaps)`, as GNU does, on a fresh
  list of the same keymaps, and both of Emaxx's `equal` walks lacked
  `internal_equal`'s first test, `BASE_EQ (o1, o2)`, so each check
  compared the global map with itself element by element through a
  SipHash table of visited pairs (2 million instructions a call). Both
  walks now answer t for the same object before anything else, as fns.c
  does. Raw `documentation` was 20 times GNU's (57 against 2.7
  microseconds a call): every docstring read the whole of etc/DOC or the
  `.elc` file. The read is now `get_doc_string`'s: seek to the disk
  block before the position (at least 1,024 bytes ahead of it for the
  consistency checks) and read 8 KB blocks to the terminating 037; the
  checks and the decoding are unchanged. Wall, GNU / Emaxx before /
  after: `substitute-command-keys` 0.019 / 0.47 / 0.014 s, raw
  `documentation` 0.0016 / 0.034 / 0.0020 s, `documentation` with
  substitution 0.17 / 0.53 / 0.28 to 0.36 s; five `describe-function`
  calls 0.200 to 0.210 / 0.235 to 0.240 / 0.232 to 0.235 s and nine
  `describe-key` 0.36 to 0.40 / 0.47 to 0.54 / 0.43 to 0.46 s. The
  printed docstrings are GNU's (one hash over twelve functions), and
  fns, doc, help, help-fns, keymap, data, subr, cl-lib, seq, coding,
  charset and mule tests give GNU's results.
- **Rounds 58 and 59: minibuf.c's completion functions and buffer.c's
  `generate-new-buffer-name`.** `execute-extended-command--shorter` was
  2.9 times GNU's (0.55 s against 0.19) and `all-completions` 2.6 times
  on a list and 4 times on the obarray. `try-completion`,
  `all-completions` and `test-completion` collected every candidate's
  name into Rust strings before matching, compared them with
  `eq_ignore_ascii_case`, and kept a fresh name per symbol. They are now
  ports of `Ftry_completion`, `Fall_completions` and `Ftest_completion`:
  minibuf.c's table type (hash table, obarray with `check_obarray` on a
  vector, nil or a cons that is no function as a list, anything else
  called as a function, so `t` is void and 5 invalid), one walk over the
  list, obarray or hash table with its elements rooted across the
  predicate's calls, the prefix test and the best match's common prefix
  through `Fcompare_strings` (`compare_string_ranges`, split out of
  `compare-strings`), `match_regexps` through `fast_string_match` with
  the canonical case table, `hide-spaces`, the user's case kept on an
  exact match, and `test-completion` through `assoc-string`, the
  obarray lookup or the hash lookup and then a `compare-strings` loop. A
  circular list signals `circular-list` where GNU would loop. The
  evaluator, `funcall` and `functionp` now read `t`'s function cell like
  any symbol's (`(funcall t)` is `(void-function t)`, and `fset` on `t`
  works), where Emaxx had said `invalid-function`.
  `generate-new-buffer-name` tries `NAME-NNNNNN` with a random number
  first for a name that starts with a space, as buffer.c does. Wall, GNU
  / Emaxx before / after: `all-completions` with a regexp over an
  18,000-string list ten times 0.023 / 0.080 / 0.018 s, over the obarray
  with `commandp` 0.013 / 0.057 / 0.016 s,
  `execute-extended-command--shorter` three times 0.22 / 0.55 / 0.29 s
  (the rest is `string-match` with a lazy `.*?`, 1.4 to 3 times GNU's).
  A 97-line probe of the three functions over lists, alists, obarrays,
  hash tables, predicates, regexps, case folding, function tables and
  errors gives GNU's output, as do minibuf, minibuffer, completion,
  completion-preview, simple, buffer, eval, data, subr, help-fns and
  files tests. Two new tests check the completion functions and
  `generate-new-buffer-name` against the oracle's answers.
- **Round 60: `string-match`'s setup.** After round 59,
  `execute-extended-command--shorter` spent its time in `string-match`
  with a lazy `.*?`: 6,185 instructions a call against GNU's 2,750, the
  matcher itself (`re_match_2_internal`, 2,400) at GNU's cost. The rest
  was setup GNU does not do. Each call built two `RopeSlice`s of the
  string (one for the matcher's text, one for position conversions;
  ropey counts the characters and line breaks of the whole string to
  make one, 460 instructions for 25 bytes): the matcher now reads a
  string's bytes in place, as `re_search_2` reads SDATA (`TextSource`),
  and the conversions use `str_utils` on the string. And with
  `parse-sexp-lookup-properties` set (as in every Lisp-mode buffer,
  `*scratch*` included) RE_SETUP_SYNTAX_TABLE_FOR_OBJECT built a fresh
  700-byte gl_state, cleared its 128 cached ASCII entries, copied it
  four times into a new box and read `parse-sexp-lookup-properties`
  twice and `syntax-propertize--done` once (1,250 instructions): one
  gl_state is now kept per thread and reset in place, as GNU keeps its
  one `gl_state`, and `syntax-propertize--done` is read for a buffer
  alone. `string-match` 6,185 to 4,170 instructions a call; 100,000
  matches of ``\`d.*?-`` 0.066 to 0.137 s before, 0.052 to 0.056 after
  (GNU 0.044 to 0.067); `execute-extended-command--shorter` three times
  0.26 to 0.39 s (GNU 0.21 to 0.25) on a host loaded by a test run. What
  is left is the general cost of reading a buffer-local variable (about
  100 instructions for `case-fold-search` or
  `parse-sexp-lookup-properties` where GNU reads a C variable), the
  matcher's pooled state (250) and `compile_pattern`'s cache lookup. A
  string-match probe (non-ASCII positions and starts, case folding,
  classes and categories, strings with `syntax-table` properties, a
  `syntax-propertize-function` that matches strings inside a buffer
  search, unibyte and raw-byte strings) gives GNU's output, as do
  regex-emacs, search, syntax, fns, casefiddle, editfns, rx, subr-x,
  subr, replace, isearch, thingatpt, minibuffer, simple and files tests.
- **Round 61: code_convert_string's buffer destination, eight-bit
  storage, byte positions.** A probe of the encoders against GNU found:
  `encode-coding-string` and `decode-coding-string` with a BUFFER returned
  the string where code_convert_string returns the characters produced,
  `decode-coding-string` checked the coding system before the string, and
  neither checked the destination's type. Both now follow
  code_convert_string: CHECK_STRING, the coding system, nil or t for a
  string and else a buffer (`bufferp`), a nil coding system with a buffer
  encoding with no-conversion. The text goes in as insert_from_gap puts
  it, `insert_text_before_point`: before-change at point, point and the
  markers at it left before the text, after-change for
  `coding->produced_char` characters; decoded into a unibyte buffer each
  character is written in its multibyte form, as produce_chars does, and
  still counts as one. `string-to-unibyte` returned a copy of a unibyte
  string, kept the properties and signalled "Character cannot be
  encoded"; it is fns.c's now (the string itself, no properties, "Cannot
  convert character at index N to unibyte"). The UTF-16 encoders wrote an
  eight-bit character's byte where encode_coding_utf_16 writes the
  default character. The stored eight-bit markers are U+E080..U+E0FF,
  but `raw_byte_from_regex_char`, `raw_case_byte` and the reader's,
  JSON's and `decode-char`'s tests took all of U+E000..U+E0FF, so a
  private-use U+E000..U+E07F (mac-roman's 0xF0 decodes to U+E01E) read
  back as a character #x3FFF00 + N in buffers, `upcase` and `format`;
  #x3FFF00..#x3FFF7F, ordinary five-byte characters, were taken for raw
  bytes by `insert`, `char_from_integer` and `multibyte-char-to-unibyte`.
  Both ranges are 0x80..0xFF now (`RAW_BYTE_MARKERS`, `RAW_BYTE8_CHARS`)
  and a marker is never made for an ASCII byte. `position-bytes` and
  `byte-to-position` counted Emaxx's UTF-8 storage (three bytes for a raw
  byte, three for a character beyond Unicode); they count GNU's bytes
  through `marker_byte_position`, and tree-sitter and unibyte overlay
  positions, which index the stored text, use the new
  `storage_byte_position` and `storage_byte_to_position`. Encoder,
  destination, private-use and byte-position probes give GNU's output
  (GNU itself crashes on `(encode-coding-string "é" 'utf-8 nil t)` and on
  a killed destination buffer, where Emaxx returns a string and signals),
  as do coding, fns, character, charset, casefiddle, buffer, editfns,
  data, print, lread, search, regex-emacs, syntax, json, marker, mule,
  mule-util, textsec, subr, subr-x, simple, files, bytecomp, fill, replace
  and rx tests. Still open on the encode side: GNU's encoders take
  consume_chars' character codes, emit an eight-bit character as its byte
  and substitute the default character inline; Emaxx's take the stored
  text, so a character beyond Unicode reaches them as its placeholder
  U+F8FF, and the substitution pass before encoding (charset and us-ascii
  codings) replaces an eight-bit character too when the text holds an
  unencodable one.
- **Round 62: consume_chars and the encoders on GNU character codes.**
  Every encoder reads what coding.c's `consume_chars` hands it: a
  multibyte source's characters as GNU numbers (an eight-bit one as
  #x3FFF80..#x3FFFFF, one beyond Unicode from the side table, a real
  U+E080..U+E0FF as itself); a unibyte source's bytes for the raw-text
  encoder and, for every other one, each internal multibyte sequence the
  bytes spell as its character (`multibyte_length` with eight-bit
  allowed, so `"\303\251"` encodes in latin-1 as 0xE9 and `"\300\200"`
  in utf-8 as 0x80) and the other bytes as eight-bit characters; the dos
  and mac newline conversions; a CR read as a newline for write-region in
  a buffer whose `selective-display` is t; and the encode translation
  tables (`get_translation_table`, sequence entries through
  `get_translation`). The encoder is the coding type's, as
  setup_coding_system chooses it: utf-8 and raw-text write the internal
  spelling with an eight-bit character as its byte, utf-16 the default
  character above MAX_UNICODE_CHAR, and charset, shift-jis, big5,
  emacs-mule and iso-2022 their charset lists with the default character
  substituted inline. shift-jis and big5 go through CODING_CHAR_CHARSET
  over the coding's own list, so japanese-shift-jis-2004's fourth charset
  encodes and decodes, where a shift-jis coding other than
  japanese-shift-jis fell to raw text both ways. japanese-iso-8bit is an
  iso-2022 coding again (the bootstrap's separate EUC-JP codec routing is
  gone), and the ISO-2022 decoder closes its last charset run before it
  flushes an incomplete trailing sequence, as no_more_source does. The
  pass that substituted unencodable characters by coding kind before
  encoding, the "Character cannot be encoded" error GNU's encoders never
  signal, the hand-written EUC-JP, Latin-1 and ASCII encoders and
  encoding_rs (windows-1251, windows-1252 and koi8-r are charset codings
  over their charset maps) are gone. `encode-coding-region` encodes the
  region with its characters beyond Unicode and its `charset`
  properties. `unencodable-char-position` and
  `check-coding-systems-region` are coding.c's: the coding's charset list
  (CODING_ATTR_CHARSET_LIST, `iso-2022` and `emacs-mule` standing for
  those lists) after the encode translation, a raw-text coding answering
  nil in the first and every non-ASCII position in the second,
  validate_region (swapped bounds, narrowing) for the first and BEG..Z
  for the second, the coding as given in the result; the terminal's
  glyphless test is term.c's char_charset over coding_charset_list. A
  probe of 37 codings over 19 strings and two regions, the
  encodability probe (20 codings) and a decode probe of eight Japanese
  codings give GNU's output, except euc-jis-2004's two characters of
  japanese-jisx0213-2 (#x150000 and up): every decoder still builds Rust
  text, which cannot hold a character beyond Unicode, so such a code
  comes out as its bytes. Porting the decode side the same way
  (`produce_chars` from GNU codes, the decoders writing character codes
  rather than a `String`) is the next coding step. 20,000 encodings of a
  100-character string: utf-8 0.076 s (GNU 0.122), latin-1 0.082
  (0.084), euc-jp 0.195 (0.180), utf-16le 0.071 (0.051) on a loaded host.
- **Round 63: the decoders on GNU character codes, detect_coding first.**
  Every decoder writes coding.c's character codes into a `DecodedText`
  (its text, the `charset` runs and the side table of characters beyond
  Unicode, as strings and buffers keep them) through `push_code`: an
  eight-bit character becomes its stored marker, a real U+E080..U+E0FF
  stays a character, and a code beyond Unicode, or a surrogate, goes to
  the side table. Before, the decoders built Rust text, so such codes came
  out as their bytes, and a real U+E080 decoded as a raw byte. The utf-8
  decoder is decode_coding_utf_8's, with 4-octet sequences up to #x1FFFFF
  and 5-octet ones up to MAX_CHAR; the utf-16 one is decode_coding_utf_16's:
  a lone low surrogate decodes to its own character, a high one followed by
  anything else comes out as its two bytes, and since
  CODING_UTF_16_SURROGATE is cleared only by the next surrogate, every unit
  after it until then repeats those bytes. An auto-BOM coding (`utf-16`,
  `utf-8-auto`) is settled by detect_coding before anything decodes, from
  detect.rs's port (a multibyte source's characters through the new
  `DetectSource::from_codes`, the UTF-8 signature tested on the raw
  source as detect_coding_utf_8 tests it: a multibyte buffer's three
  eight-bit characters EF BB BF are no signature), with the requested eol
  type carried over (`adjust_coding_eol_type`); the settled coding decodes
  and is the one `last-coding-system-used` records
  (`utf-16le-with-signature`), and one detection could not settle decodes
  without a BOM in its own byte order, as utf_detect_bom does. A string
  decoded by a coding detection settles on `raw-text` is unibyte, as
  decode_coding_object sets `dst_multibyte` after detect_coding. The
  decoded side table reaches strings, regions (Replace, Return and a
  buffer destination), `insert-file-contents` (including REPLACE, which
  compares character codes) and `call-process` output. A decode probe of
  27 codings over 20 inputs plus 30 multibyte-buffer region decodes went
  from 49 differing lines to one, and the round 62 encode probes give
  GNU's output. A naming probe of 36 codings over 18 strings, with
  `inhibit-eol-conversion` and as regions (2,312 lines), differs on 360:
  `decode-coding-string` with NOCOPY returns the source string itself
  whenever the decoded text reads the same, where GNU returns it only on
  code_convert_string's ASCII fast path (so `(decode-coding-string
  "\244\242\n" 'utf-8 t)` gives back the unibyte source); with
  `inhibit-eol-conversion` the name still gains an eol subsidiary; and a
  region decode of ASCII text keeps the requested name where decode_eol
  names the subsidiary (`utf-8-unix`). Compare probe output with `diff -a`:
  these probes hold raw bytes, and plain `diff` reports only "Binary files
  differ". Still open: (1) the one
  differing line is ISO-2022's CODING_DECODE_CHAR failure on a multibyte
  source, where GNU emits the internal bytes of the invalid sequence one by
  one; Emaxx splits a multibyte source into runs of bytes and decodes each
  run on its own, so decoder state also does not carry across a non-byte
  character. The faithful form is the decoders reading the source with
  ONE_MORE_BYTE's multibytep semantics. (2) `insert-file-contents`
  converts end-of-lines in the raw bytes before decoding, where
  decode_coding_gap runs decode_eol on the decoded text, so a UTF-16 file
  with CR LF keeps its CR and one whose code units contain 0x0D or 0x0A
  bytes is corrupted; its naming (`last-coding-system-used` against
  `buffer-file-coding-system`, `utf-8-auto` files) is heuristic and differs
  from GNU on 45 lines of a BOM probe. (3) process output: `call-process`
  converts end-of-lines in the bytes before decoding as files do, and a
  process filter's output with an undecided coding is read as UTF-8 with
  U+FFFD for an invalid byte (`decode_process_output_bytes`), its decoded
  text without the side table. All three want one decode_coding_object
  (detect_coding, the decoder over the source with multibytep, decode_eol
  on the produced text) behind strings, regions, files and processes.
- **Round 64: one decode_coding_object for strings, regions, files and
  processes.** `decode_coding_object` (strings, regions, `call-process`
  output) and `decode_coding_gap` (`insert-file-contents`) share one
  `decode_coding`, coding.c's: the name starts as the coding as given (an
  alias keeps its own), detect_coding runs for `undecided` and the
  auto-BOM codings with the specified eol carried over
  (`adjust_coding_eol_type`, the subsidiary taken from the canonical base's
  eol vector, so `euc-jp` settles as `japanese-iso-8bit-unix`), the settled
  coding decodes, and decode_eol then works on the decoded text: it counts
  LF, CR LF as a pair and lone CR (CR LF with lone CR but no LF counts as
  CR LF, any other mixture as LF), names the subsidiary from what it saw
  (LF before CR LF before CR), turns CR into LF for `mac` and drops each
  CR before LF for `dos`, and is skipped for `unix` or under
  `inhibit-eol-conversion`. A file's detection runs without
  CODING_MODE_LAST_BLOCK (an odd-length UTF-16 file or a truncated UTF-8
  tail is not rejected), with decode_coding_gap's ASCII optimization (an
  ASCII-compatible coding with no post-read conversion, translation table
  or `disable-ascii-optimization`, over all-ASCII bytes or UTF-8 whose
  detection counted every byte) and its eol from the detection's head scan.
  Finsert_file_contents picks the coding as fileio.c does
  (`coding-system-for-read`, `auto-save-coding` as `utf-8-emacs-unix`,
  `set-auto-coding-function` on a non-empty file, the car of
  find-operation-coding-system called with the file name as given, VISIT,
  BEG, END and REPLACE, else `undecided`; a unibyte buffer's
  raw_text_coding_system), makes a visited buffer unibyte for a coding
  `:for-unibyte`, does not decode an empty file, records the decided name
  in `last-coding-system-used` (`after-insert-file-set-coding` sets
  `buffer-file-coding-system` from it, as in GNU, in place of the old
  heuristic), and runs the post-read conversion with point at the inserted
  text and undo off, the inserted count growing by its change of size.
  `decode-coding-string` takes code_convert_string's fast path only for an
  ASCII string and an ASCII-compatible coding with nothing to convert at
  eol, which alone returns the source under NOCOPY, decodes into a
  multibyte string unless the settled coding is for unibyte text, runs the
  settled coding's post-read conversion and then records it; a region
  decode's destination is the buffer it writes. Raw text copies a
  multibyte source's characters into a multibyte destination and the low
  bytes ONE_MORE_BYTE reads into a unibyte one. `call-process` decodes as
  callproc.c chooses (`coding-system-for-read`, else the car of
  find-operation-coding-system, else of `default-process-coding-system`,
  raw text with the eol type in a unibyte buffer), records
  `last-coding-system-used` even when the program writes nothing, and
  reads DESTINATION as callproc.c does: `(REAL-BUFFER STDERR-FILE)` with
  REAL-BUFFER possibly `(:file FILE)`, both files expanded against
  `default-directory` and truncated even when nothing is written, and a
  dotted `(BUFFER . X)` mixing standard error into the output. File reads
  cost 2.82 to 2.12 billion instructions on a probe reading 60 Lisp files
  ten times (GNU 1.52): detect.rs's head scan skips printable ASCII runs,
  decode_eol finds CR and LF with `memchr2`, raw text copies its ASCII
  prefix a word at a time, the inserted text is not counted twice, and
  `search`'s unibyte loader copies an ASCII chunk at once. The naming
  probe (2,312 lines) went from 360 differing lines to none and the BOM
  file probe (337) from 45 to none; the file-read (1,080), alias,
  decode, encode and charset probes give GNU's output but for the round 63
  ISO-2022 line. GNU's arc-mode, bytecomp, files, ccl, mule, mule-util,
  textsec, qp, rfc2045, rfc2047, simple, subr, buffer, casefiddle,
  character, charset, coding, data, editfns, fileio, fns, json, lread,
  print, process, search, jit-lock, tramp-archive, compile and esh-proc
  tests give GNU's results. Still open: (1) the ISO-2022 line, as in round
  63; (2) `call-process` sends a unibyte argument's raw bytes as the UTF-8
  of their markers and fails on a NUL where GNU truncates the argument
  (60 of 162 lines of a process probe), and `call-process-region` sends
  the region's Rust text rather than encoding it; (3) a process filter
  still reads `undecided` output as UTF-8, and `call-process` decodes its
  whole output at once where callproc.c decodes each 16 to 64 KB block,
  detection settling on the first; (4) the two `bookmark-tests`
  non-ASCII tests that fail on `9ba4dee` (below): their gb18030
  character, #x22484C, now decodes to GNU's code beyond Unicode, and
  `read` from a buffer, `format`, `message`, case conversion, `char-width`,
  regexp search and several editing commands lose such characters (or
  real U+E000..U+E0FF ones) through the Rust text view; (5) with
  `:expensive-test`s, `package-test-update-archives-async` fails
  (`url-http-response-status` void in its filter) and leaves two later
  package tests failing, and `ucs-normalize-part1` fails on U+E0FF, as on
  `9ba4dee`; the frozen run does not include them.
- **Round 65: characters beyond Unicode and real U+E000..U+E0FF through
  the Rust text paths.** Lisp strings and buffers keep GNU's internal
  bytes, but many primitives work on the Rust text view, where a raw byte
  is a U+E080..U+E0FF marker and a character the view cannot carry (beyond
  Unicode, a surrogate, a real U+E000..U+E0FF such as the Powerline glyphs
  U+E0A0..U+E0D4) needs its side-table entry. Every path below now carries
  it or works on the internal bytes. `read` and `read-positioning-symbols`
  on a buffer or marker, and `eval-buffer`/`eval-region` (so `load` of a
  source file), read the region's internal bytes when it holds such a
  character: a loaded `.el` file with a literal U+E0B0 in a string came
  back with the raw byte 176. `format` (`%c` checks the code against
  MAX_CHAR, "Invalid character: N"; `%s`/`%S` keep the side table through
  padding and precision), `message` and its batch stderr output
  (xdisp.c message_to_stderr: the internal bytes, or encoded with
  `coding-system-for-write`/`locale-coding-system`), `write-char`,
  `char-to-string`, case conversion of strings and regions (each character
  cased by its code through the case table, words by its syntax),
  `delete-and-extract-region`, `kill-region`/`yank`, `transpose-chars` and
  `decode-coding-region`. `replace-match` is search.c's Freplace_match
  (the literal fast path, `\&`, `\N`, `\\`, `\?` an error in the buffer
  case, case conversion of the replacement from the matched text,
  "replace-match subexpression does not exist", args-out-of-range on the
  bounds, string pieces via concat3 keeping properties) over insdel.c's
  replace_range (insertion recorded before the deletion in the undo list,
  markers and overlays adjusted as adjust_markers_for_replace and
  adjust_overlays_for_insert/delete do, point moved as replace_range moves
  it, match data updated by update_search_regs, one after-change call);
  `subst-char-in-region` is editfns.c's (byte-length check in a multibyte
  buffer, low byte in a unibyte one); `regexp-quote` works on the internal
  bytes and returns a copy when nothing needs quoting;
  `string-to-multibyte`, `string-as-multibyte` and `string-make-unibyte`
  are fns.c's byte conversions (STRING itself when nothing changes, no
  properties otherwise); `char-width` and `string-width` are character.c's
  (char-width-table, which now also gives raw bytes width 4, display
  tables, `ctl-arrow`, tab width). keymap.c's `key-description`,
  `single-key-description` (push_key_description: a multibyte result, any
  valid character as itself, `[N]` past MAX_CHAR, map-char-table intervals
  as `A..B`, Lucid event lists through `event-convert-list`, EVENT_HEAD of
  other conses, the modifier prefix of a symbol scanned while `i < len -
  3`, "KEY must be an integer, cons, symbol, or string") and
  `text-char-description` (`characterp`, a unibyte result for ASCII);
  `char-syntax` gives word syntax for characters beyond Unicode;
  base64 encoding takes a multibyte source's eight-bit characters as bytes
  and rejects any other non-ASCII character ("Multibyte character in data
  for base64 encoding"); `json-serialize` checks strings as json.c's
  json_out_string does over the internal bytes (`(wrong-type-argument
  json-value-p STRING)` for a raw byte, a surrogate or a character beyond
  Unicode) and requires string hash keys, and `json-insert` keeps a real
  U+E000..U+E0FF; `file-name-directory`, `file-name-nondirectory`,
  `file-name-as-directory`, `directory-file-name` and `directory-name-p`
  work on the internal bytes and keep the name's multibyteness. A reader
  error in a buffer source is lread.c's invalid_syntax: point stays where
  reading stopped and the data gains the line and column (`Invalid read
  syntax: ")", 77, 4`), a stray closer is read before the error and named
  `)` or `]`, and readevalloop's native path puts a `read` frame on the
  backtrace while it reads each form, as GNU's call of
  `load-read-function` does; `eval-buffer` restores point however its
  forms end. A key-description probe went from 160 differing lines of 160
  to 6, the replace-match probe (29 lines), the regexp probe and the width
  probe (22) give GNU's output, and the two 84-line character probes
  (#x22484C and U+E0B0 through 80 primitives) from 23 and 21 differing
  lines to 2 each. The bookmark, simple, subr, fns, editfns, search,
  casefiddle, character, print, lread, buffer, undo, textprop, replace,
  files, mule, coding, json, keymap, help, bytecomp, fileio, files-x,
  kmacro, syntax, edebug, elisp-mode, data, minibuf, ls-lisp and dired
  tests give GNU's results. Still open: (1) a symbol's name keeps no
  multibyteness: GNU's reader gives a symbol first read from a multibyte
  source a multibyte name, so `single-key-description` with NO-ANGLES
  returns a multibyte name there (the 6 key-description lines); (2) buffer
  names are Rust strings, so `buffer-name` builds a new string on each
  call (never `eq` to the last, and losing such characters), and
  `expand-file-name` still works on the text view (the 2 character-probe
  lines); (3) the reader's escape sequences and `#` syntaxes still signal
  Emaxx's own conditions and messages where lread.c signals `error` or
  other text (`\M`, `\C` without `-`, `\x` without a digit, malformed
  `\u`/`\U`, `\N` without `{`, `#s`, `#&`, `#@`, radix errors, the
  char-table size): 56 of 117 lines of a reader-error probe over strings,
  buffers and markers differ, and some end-of-file errors lack
  `load-true-file-name`; (4) the round 64 items (1) to (3) and (5).
- **Round 66: lread.c's reader; callproc.c's environment, arguments,
  input and output.** The reader is lread.c's over character codes:
  read_char_escape for strings and character literals (the control,
  meta, shift, hyper, alt and super modifiers with `Invalid escape char
  syntax: \C not followed by -` and its kin, `\x` with any number of hex
  digits and "Hex character out of range", `\u`/`\U` with exactly four and
  eight and "Malformed Unicode escape" or "Non-Unicode character",
  `\N{NAME}` and `\N{U+X}` with `Expected opening brace after \N`,
  "Empty character name", "Character name too long" and "Invalid
  character U+X in character name", a backslash-newline an error in a
  character literal), read_char_literal (modifier bits on any code, so
  `?\x8000041` reads; an unescaped `?(`, `?)`, `?[`, `?]`, `?"` or `?;`
  consed onto `lread--unescaped-character-literals` for the warning),
  read_string_literal (each character entered as push_string_character
  does, the string's multibyteness from what it holds, "Invalid modifier
  in string"),
  `#(` through string_props_from_rev_list ("Invalid string property
  list"), read_integer for `#b`/`#o`/`#x`/`#NrDIGITS` ("integer, radix N"),
  bool vectors, `#s(...)`, char-tables and sub-char-tables ("Invalid size
  char-table", "Invalid depth in sub-char-table", "Invalid minimum
  character in sub-char-table", "Invalid size in sub-char-table"), `#[`
  ("Invalid byte-code object"), `#@N` (in a buffer or string skipping to
  the next `\037`), `#&`, `#:`, `#_`, `##`, `#'` and `#N=`/`#N#` (which
  stay readable whatever `read-circle` says, where GNU rejects `#1=` with
  it nil; `#$` as before), with INVALID_SYNTAX_WITH_BUFFER's line and
  column for a buffer source, and end_of_file_error naming
  `load-true-file-name` while loading. The 230-line escape and `#` probe
  and the 117-line reader-error probe (56 differing lines on round 65)
  give GNU's output, as do the lread and print tests.
  `getenv-internal` is callproc.c's over the entries' internal bytes (a
  non-string entry passed over, an improper tail ending the walk; a list
  ENV searched instead, its negative entry t; any other ENV the frame
  whose `display` parameter answers DISPLAY ahead of
  `initial-environment`), its value make_string's, so valid multibyte
  bytes come back multibyte; egetenv's HOME reads go through it. A
  child's environment is make_environment_block's, handed to execve in
  its order: PWD, when the Lisp environment has one, naming the child's
  directory without trailing slashes, DISPLAY from the frame or
  `initial-environment` when `process-environment` lacks it, then
  `process-environment`'s strings up to its first non-string as their
  internal bytes cut at a NUL, each left out when an earlier entry names
  its variable (a lone name keeps later definitions out), the lone names
  dropped last. Before, the entries reached the child as Rust text (a raw
  byte as its marker's UTF-8) applied last-first, and a lone name had no
  effect. `call-process` output goes in as callproc.c inserts a
  bufferful: prepare_to_modify_buffer at point (the before-change
  functions), then the bytes as they are for a unibyte buffer whose
  coding needs no decoding (insert_1_both) or decode_coding_c_string with
  `inhibit-modification-hooks` bound (produce_charset's `charset`
  properties, one insertion in the undo list, the post-read conversion
  through safe_calln with undo off, its errors muted into `*Messages*`),
  point moved over the characters produced and signal_after_change for
  them; before, no change hook ran and no `charset` property was put. An
  error out of the insertion has call_process_cleanup announce "Waiting
  for process to die...(type C-g again to kill it instantly)" and
  "...done" (xdisp.c message1) first. Its arguments are callproc.c's:
  checked as strings, the coding `coding-system-for-write`, else for a
  multibyte argument the cdr of find-operation-coding-system's answer,
  else raw-text, completed by coding.c's complement_process_encoding_system
  (the cdr of `default-process-coding-system`, then the preferred coding
  system) and made raw-text when not ASCII compatible; the
  encode_coding_string macro encodes a multibyte argument and passes a
  unibyte one as its bytes, and the child reads each up to its first NUL
  (before: the UTF-8 of the Rust text, and a "nul byte found" error).
  `call-process-region` is Fcall_process_region: START a string, nil
  (the whole buffer, empty when BEG = Z) or a region checked as
  validate_region checks it (`args-out-of-range` with the buffer), the
  input written as create_temp_file writes it (write_region's coding
  choice with `coding-system-for-write` bound to the completed
  `coding-system-for-write`, raw-text for a unibyte buffer or the cdr of
  find-operation-coding-system's answer for `call-process-region`, and
  recorded in `last-coding-system-used`), the region deleted before the
  child runs, the null device as stdin for an empty input, and the
  output read as call_process reads it for `call-process` with INFILE in
  DELETE's place. `define-coding-system-internal` checks its translation
  tables (a char-table, a list or a symbol) and conversions (symbols) as
  coding.c does and returns nil. `tramp-test42-utf8`, failing on
  `9ba4dee` and before, passes. The round 64 process probe (60 differing
  lines of 162) gives GNU's output but for the temporary file names, as
  do a 15-form environment probe, a change-hook probe, a 22-form argument
  and region probe and a post-read probe but for one value: GNU leaves
  point past the end of the buffer (8 in a 5-character one) when a
  post-read conversion inserts before point, where Emaxx's buffer keeps
  it in range. The callproc, process, env, files, eshell (proc, var, cmd,
  unix, eshell), shell, term, compile, simple, tramp-archive, vc, vc-git,
  dired, dired-aux, ls-lisp, grep, dbus, fileio, coding, mule, epg and man
  tests give GNU's results. Still open: (1) `make-process` takes its
  codings from `:coding` or `default-process-coding-system` only (not
  `coding-system-for-read`/`-write` or find-operation-coding-system for
  `start-process`), passes its arguments as the UTF-8 of their Rust text
  where GNU encodes a multibyte one with the completed encoding, and its
  filter reads `undecided` output as UTF-8; (2) `call-process` takes the
  child's whole output as one bufferful, where GNU reads 16 to 64 KB
  blocks at the sizes the pipe gives, with a change-hook pair for each
  and detection settled on the first (carryover keeps the text the
  same); (3) post-read conversions of strings and regions propagate their
  errors where decode_coding_object's safe_calln mutes them; (4) the
  program, its directory and file names still go through the Rust text
  (ENCODE_FILE); (5) the round 65 items (1) and (2) and the round 64
  items (1) and (5).
- **Round 67: process.c's codings for subprocesses, pipes, sockets and
  serial ports, its argument encoding, and the decoding of their
  output.** `make-process` chooses its codings as Fmake_process does
  (`:coding`, its car and cdr when a cons; else `coding-system-for-read`
  and `-write`; else for a process with a program what
  find-operation-coding-system answers for `start-process` with the name,
  buffer and command; else `default-process-coding-system`'s), reads each
  keyword's first value as plist_get does, and has
  setup_process_coding_systems check them before the child runs
  (`(coding-system-error X)`); a multibyte argument is encoded by the
  encoding coding completed by complement_process_encoding_system, a
  unibyte one passed as its bytes. `make-pipe-process` always has a
  buffer (`:buffer`, else the one its name names, created when missing)
  and takes Fmake_pipe_process's codings; network streams take
  set_network_socket_coding_system's (find-operation-coding-system for
  `open-network-stream` with the name, buffer, host and service) and
  serial ports Fmake_serial_process's, each with process.c's "no decoding
  for unibyte text" rule (a unibyte process buffer, or the default
  `enable-multibyte-characters` nil without one; a unibyte current buffer
  for the encoding); once their channels exist, connections, servers,
  accepted clients and serial ports have setup_process_coding_systems
  check their codings too, an undefined one signalling with the process
  left registered, as GNU leaves it. `set-process-coding-system` checks
  both codings and
  completes the encoding's eol type (coding_inherit_eol_type: nil becomes
  `raw-text-unix`). A
  process's output is decoded as read_process_output decodes it:
  decode_coding_c_string into a string with the process's decoding coding
  (raw_text_coding_system's form of it for the default filter into a
  unibyte buffer), produce_charset's `charset` properties on it, then
  read_process_output_set_last_coding_system: the settled coding is
  `last-coding-system-used`, becomes the process's decoding coding when
  it differs, and an undecided encoding of a live process takes it with
  its eol completed. The filter receives that string, properties and all;
  the default filter makes its multibyteness the buffer's
  (string-as-unibyte or string-to-multibyte) and inserts it with its
  properties before markers. Before, `undecided` output was read as UTF-8
  with U+FFFD replacements, other codings without properties, and a
  network stream without `:coding` had no coding at all. `default-value`
  of `enable-multibyte-characters` and `buffer-undo-list` reads
  buffer.c's buffer_defaults (t and nil) rather than the current buffer's
  value. `default-process-coding-system` starts nil, as coding.c leaves it
until mule-cmds.el's `set-default-coding-systems` (the bare interpreter
had `(utf-8-unix . utf-8-unix)`, naming codings it never defines, which
setup_process_coding_systems's check now refuses). A 13-form
`make-process` probe (16 differing lines on round 63)
  and a 13-form pipe, network and serial probe give GNU's output; the
  process, callproc, eshell, shell, term, compile, vc-git, epg, comint,
  erc, jsonrpc, eglot, network-stream and server tests give GNU's results
  (jsonrpc's needs the network streams' `undecided-unix` from
  `default-process-coding-system`: plain `undecided` converts the CR LF
  its headers end with). Still open: (1) output is decoded as one block
  per poll with no carryover: GNU reads `read-process-output-max` bytes at
  a time, calling the filter for each, and keeps an incomplete trailing
  sequence (and the coding state) for the next read, which needs every
  decoder to report where it stopped -- the same machinery `call-process`
  blocks need; (2) `default-value` of `default-directory` and `mode-name`
  still gives the startup directory and "Fundamental" (GNU: nil), Emaxx's
  global bindings of them standing in for buffer_defaults; (3) the round
  66 items (2) to (5).
- **Round 68: buffer.c's always-local slots.** Every DEFVAR_PER_BUFFER
  slot whose buffer_local_flags index is -1 is each buffer's own from the
  start, as reset_buffer and reset_buffer_local_variables make it: a new
  buffer gets `default-directory` (the creating buffer's, a `let` of it
  included), `buffer-backed-up`, `buffer-saved-size`,
  `buffer-auto-save-file-name`, `buffer-read-only`, `major-mode`
  (`fundamental-mode`), `local-minor-modes`, `mode-name` (QSFundamental,
  one string every buffer shares, a new dumped root as GNU staticpros it),
  `buffer-file-format`, `buffer-auto-save-file-format` (t),
  `point-before-scroll`, `buffer-invisibility-spec` (t),
  `buffer-display-count` and `buffer-display-time`, and the first buffer
  gets them with the working directory (init_buffer). The slots the
  buffer record holds (`buffer-file-name`, `buffer-file-truename`,
  `mark-active`, `enable-multibyte-characters`) answer
  `buffer-local-value`, `local-variable-p`, `buffer-local-variables` and
  `let` for the buffer named, as `text-conversion-style` did: a `let`
  binds the buffer's slot and restores it there, where it bound the
  default (`default-value` of `mark-active` showed the `let`'s value).
  `mark-active` holds any Lisp object (it kept t or nil, and nil whenever
  there was no mark), a new pdumper word. The global cells are
  buffer_defaults': `default-value` of `mode-name` and
  `default-directory` is nil, `setq-default` reaches no buffer.
  `kill-local-variable` of a DEFVAR_PER_BUFFER slot tells no watcher and
  returns VARIABLE as given, an index -1 slot staying as it is;
  `kill-all-local-variables` keeps every index -1 slot but the three
  reset_buffer_local_variables assigns (`major-mode`, `mode-name`,
  `buffer-invisibility-spec`), returns indexed ones to their defaults
  unless buffer_permanent_local_flags keeps them (`truncate-lines`,
  `buffer-file-coding-system`), and tells watchers only of the alist
  cells; `buffer-local-variables` lists `buffer-undo-list`, the slots in
  struct buffer's order reversed, then the ordinary locals oldest first.
  `make-indirect-buffer` gives a non-clone a fresh mark and `mark-active`
  nil (it set the base's mark and activated it) and a clone the base's
  mark and `mark-active`, then resets its display count, backup and
  auto-save state and minor modes, sets `buffer-save-without-query` and
  `buffer-file-number` and kills a local `buffer-stale-function` as
  Fmake_indirect_buffer does. Before, an always-local slot without a
  local cell read the global binding, so `setq-default mode-name` renamed
  every buffer's mode, `kill-local-variable` and
  `kill-all-local-variables` dropped `local-minor-modes` and
  `mark-active`, and another buffer's `buffer-file-name` read nil. A
  49-form probe of defaults, kills, lets, `setq-default`, other buffers'
  values, `buffer-local-variables` (its order included) and indirect
  buffers gives GNU's output but for `makunbound`; the dired, derived,
  easy-mmode, edebug, ert-x, generator, subr-x, files, files-x, hi-lock,
  minibuffer, elisp-mode, simple, subr, fill, buffer, data, editfns,
  fileio, minibuf, textprop and undo tests give GNU's results. Still
  open: (1) `makunbound` of a DEFVAR_PER_BUFFER slot: GNU's set_internal
  makes the symbol plain and void in every buffer (`default-boundp` nil,
  `local-variable-if-set-p` nil), its C slots kept for C readers; Emaxx
  voids the current buffer's cell -- porting it needs Emaxx's Rust
  readers of these slots to stop going through the symbol; (2) the round
  67 items (1) and (3).
  A temporary buffer costs 19.8K instructions (`with-temp-buffer` with
  one insertion, 20,000 times under callgrind; 17.7K on `183603d`, GNU's
  wall time three times Emaxx's): its 14 cells go into a fresh table at
  once. Measuring it found the sweep reclaiming each dead buffer with a
  linear search of the all-buffers list, quadratic in the buffers one
  collection frees (39.4K a buffer once the cells made collections
  rarer); the list is an address set now.
- **Round 69: process.c's reads and sends by the process's codings,
  callproc.c's blocks, print.c's debugging output.** read_process_output
  reads at most p->readmax bytes at a time (`read-process-output-max`
  when the process is made, clipped to 1..INT_MAX) and decodes them with
  the carryover the last read left (p->decoding_buf, up to 64 bytes); the
  end of file is a read of nothing of its own, which decodes the carryover
  as the last block (CODING_MODE_LAST_BLOCK), and a read that returned
  carried bytes is not yet the end. A network peer's end of file
  deactivates the connection (status `closed`, exit 256) with "connection
  broken by remote peer\n"; GnuTLS reads one record of up to readmax.
  `accept-process-output` for a process keeps reading while a pass finds
  output, a process deleted or stopped meanwhile keeping the rest. Before,
  each poll drained a pipe into one string decoded whole, so a character
  split across reads came out as raw bytes, a dos CR ending a read became
  a newline, and a filter saw more than `read-process-output-max` at once.
  coding.c's decoders without CODING_MODE_LAST_BLOCK stop before an
  incomplete character and leave it as the carryover: UTF-8 (by its lead
  byte's count, 5 after a non-leader), UTF-16 (the surrogate kept in the
  coding struct, GNU's stale-surrogate quirk included, a dos CR waiting
  for two more bytes), Shift-JIS, Big5, charset, emacs-mule, raw-text's
  dos CR, and ISO-2022 with its designations, shifts and composition kept
  across blocks; the BOM is looked for in the first block only, and the
  last block flushes what remains as raw bytes. emacs-mule's invalid bytes
  carry no charset annotation but a lone 0x9E or 0x9F (eight-bit).
  `call-process` and the other callproc.c readers read 16K blocks (64K once
  32 have been read) less 1024 bytes for the carryover, and each block is
  its own prepare_to_modify_buffer, decode and signal_after_change, so the
  change hooks run per block as in GNU. send_process encodes by the
  process's coding struct, which persists from send to send:
  `last-coding-system-used` is its coding; multibyte text complements
  p->encode_coding_system and sets the struct up again when it differs;
  unibyte text sets up raw-text of the coding last used (an ISO-2022 or CCL
  coding's flush first, send_process of t, which encodes and sends
  nothing); text is encoded only when CODING_REQUIRE_ENCODING; no send is
  a last block, so ISO-2022 designations and a UTF-16 BOM written stay
  written for the next send. `process-send-eof` flushes, and
  `set-process-coding-system` makes fresh coding structs keeping the
  decoding carryover. A send to a dead process signals "Process NAME not
  running: STATUS"; `process-send-string` checks its string first;
  `process-send-region` sends the region encoded as its buffer's
  multibyteness says. Before, every send went out as UTF-8 whatever the
  process's coding. `external-debugging-output` takes a fixnum and writes
  it to stderr as printchar_to_stream does (a `standard-display-table`
  vector expanded, other non-ASCII encoded by `coding-system-for-write` or
  `locale-coding-system`); it wrote into a buffer. `redirect-debugging-output`
  dup2s stderr to the file (appending or truncating), nil restoring the
  saved stderr, and returns nil. Also fixed: round 68's two red gate tests
  (below): `buffer-local-variables` reads the `text-conversion-style` slot
  itself, as FOR_EACH_PER_BUFFER_OBJECT_AT does, so a variable `makunbound`
  left plain still lists its slot, and an ordinary local made after that
  is listed besides. Probes of decoding tails, carryover across process
  reads (UTF-8, UTF-16, dos CRs, ISO-2022-JP), `call-process`'s per-block
  hooks, emacs-mule annotations and 31 sends across 18 codings
  (ISO-2022 designations and the UTF-16 and UTF-8 BOMs across sends,
  unibyte sends, dos and mac eols, `inhibit-eol-conversion`, a 70,000
  character send, a network connection, errors) give GNU's output but
  for an out-of-range `process-send-region` (item (2) below); the
  process, callproc, coding, print, editfns, fileio, comint, shell, term,
  erc, jsonrpc, eglot, network-stream, socks, package (all 38; three
  failed before), mule, mule-util, server, vc-git, 23 eshell and 10 url
  test files give GNU's results. Still open: (1) round 68's item (1);
  (2) primitives that clamp an out-of-range region where validate_region
  signals `args-out-of-range`: `buffer-substring` and its
  `-no-properties`, `delete-region`, `delete-and-extract-region`,
  `put-text-property`, `upcase-region`, `insert-buffer-substring`,
  `subst-char-in-region`, `translate-region` and `encode-coding-region`
  among 16 probed (`process-send-region` with them); (3) a character
  U+E080..U+E0FF inserted as an integer (`insert`, `insert-char`,
  `insert-and-inherit`, `translate-region`) is stored as a raw byte, the
  text view's marker for one -- strings carry such characters in their
  side table, these paths do not -- which fails ucs-normalize-tests'
  `ucs-normalize-part1` (on round 66's build too).
- **Round 70: buffer.c's validate_region at its callers, characters as
  GNU numbers in the text views, cmds.c's self-insert.** validate_region
  (fix_position for each bound: a marker pointing nowhere "Marker does not
  point anywhere", a bignum the most positive or negative fixnum, else
  `integer-or-marker-p`; then the bounds in order, within BEGV..ZV, else
  `(args-out-of-range BUFFER B E)` with the bounds as given) is what
  `buffer-substring` and its `-no-properties`, `delete-region`,
  `delete-and-extract-region`, `subst-char-in-region`, `translate-region`
  (a char-table not a translation table "Not a translation table", other
  non-strings `stringp`), `transpose-regions` (each region; characters
  outside Unicode's scalars carried across with the text),
  `upcase-region` and the other casify_region commands (a region list
  walked as FOR_EACH_TAIL walks it, a cycle `circular-list`),
  `find-charset-region`, `detect-coding-region`,
  `unencodable-char-position`, `encode-coding-region` and
  `decode-coding-region` (a destination other than nil and t
  `bufferp` first), `compose-region-internal` (a unibyte buffer
  checked first), `composition-get-gstring`,
  `zlib-decompress-region`, `write-region`, the base64 region
  functions, `process-send-region`, `parse-partial-sexp`,
  `bidi-find-overridden-directionality` and `libxml-parse-*-region`
  (nil bounds point-min and point-max) call; they clamped an out-of-range
  region before (round 69's item (2)). Characters are GNU's numbers
  through the text views: a buffer or string holds a raw byte as its
  private-use stand-in and any other character not a Unicode scalar, or
  U+E080..U+E0FF itself, in the extended side table (push_character_code),
  so `insert`, `insert-char`, `insert-and-inherit`, `insert-before-markers`,
  `insert-byte`, `translate-region`, `subst-char-in-region`,
  `lookup-key`'s keyboard translation and the region replacements store
  U+E080..U+E0FF as the character it is (round 69's item (3);
  ucs-normalize-tests' `ucs-normalize-part1` passes). editfns.c's
  general_insert_function inserts each argument by itself with its own
  change hooks, a character in a unibyte buffer as CHAR_TO_BYTE8's byte,
  an empty string with no change at all, anything else
  `char-or-string-p`; `insert-byte` checks 0..255 and makes a byte from
  128 the raw-byte character in a multibyte buffer. The charset
  functions take characters as GNU numbers (`eight-bit` 0x3FFF80 and up):
  `charset-after`, `find-charset-string` and `find-charset-region`
  (through the translation table, a unibyte text's bytes `ascii` or
  `eight-bit`), `print`'s charset-property pruning. `delete` on a string
  drops the characters ELT names (none when ELT is not a character) and
  returns the string itself when nothing goes, a vector too;
  `string-to-char` reads a unibyte string's byte; `string-to-unibyte`
  keeps raw bytes and signals for other non-ASCII. cmds.c's
  Fself_insert_command and internal_self_insert are ported: N checked,
  the character through `translation-table-for-input`, overwrite mode
  replacing the character after point with the columns kept (a tab kept
  before point, spaces filling), abbrev expansion under GNU's conditions
  (a `no-self-insert` hook ending the insertion), auto-fill after a
  space or newline, `post-self-insert-hook`, the amalgamating undo
  boundary, and the 0/1/2 return value; a non-character is
  bitch_at_user: in batch a BEL on stdout, while a keyboard macro runs
  the user-error that ends it, else `ring-bell-function` (bound to nil
  while it runs) or the bell. `ding` with ARG rings without ending a
  macro. `bidi-find-overridden-directionality` takes a string (FROM and
  TO as validate_subarray's) or a buffer, a unibyte one or one without
  `bidi-display-reordering` finding nothing, and starts its paragraph
  after the newline before FROM. Probes of 57 private-use and raw-byte
  insertions and conversions, 25 insertion cases and 15 charset lookups
  give GNU's output, as do 71 of 98 region-validation cases; the other
  27 are item (1) below. The GNU cmds, editfns,
  buffer, textprop, casefiddle (11 tests; the circular region list hung
  before), coding, charset, character, fns, decompress, undo, xml,
  simple, abbrev, electric, kmacro, mule, ucs-normalize, mule-util, ccl,
  textsec, subr, fill, subr-x, seq, cl-seq, sort, thingatpt, replace,
  minibuffer, elisp-mode, files, ert-x, python, css-mode, process,
  callproc, esh-proc, shell and comint test files give GNU's results.
  Still open: (1) textprop.c's validate_interval_range: the property
  primitives (`put-text-property`, `set-text-properties`,
  `add-text-properties`, `remove-text-properties`,
  `remove-list-of-text-properties`, `add-face-text-property`,
  `text-properties-at`, `get-text-property`, the property-change
  scanners, on buffers and strings) return nil or t for an out-of-range
  range where GNU signals `(args-out-of-range BEGIN END)`, and
  `get-char-property`, `get-pos-property`, `field-string`,
  `constrain-to-field` and `char-property-alias`'s lookup where GNU
  signals `(args-out-of-range POS)`; `insert-buffer-substring` and its
  `-no-properties` clamp where GNU signals; `secure-hash` of a buffer
  clamps the region (`md5` signals already); an overlay made or moved
  in a buffer since killed prints its id, GNU `#<overlay in no buffer>`;
  (2) `intern` of a name with a raw byte gives a symbol `eq` to the one
  named by the private-use stand-in: symbol names do not carry the side
  table; (3) bidi.c is not ported: the overridden-directionality search
  uses unicode-bidi's levels, which agree with GNU on textsec's and
  bidi's cases; (4) `ring_bell` ignores `visible-bell`; (5) round 68's
  item (1).
- **Round 71: textprop.c's lookups, scans and changes, editfns.c's
  fields.** validate_interval_range is ported: the object nil the current
  buffer or CHECK_STRING_OR_BUFFER, each bound CHECK_FIXNUM_COERCE_MARKER
  (fix_position: a bignum the most positive or negative fixnum), the same
  position twice nothing for a primitive over a range, then the bounds in
  BEGV..ZV or 0..length, else `(args-out-of-range BEGIN END)` as given;
  no text and (soft) no intervals are GNU's NULL. Over it, as textprop.c
  writes them: `text-properties-at` and `get-text-property` (the
  interval's plist, nil at the object's very end, so narrowing does not
  hide ZV's properties), `next-property-change` and
  `previous-property-change` (intervals_equal over the interval walk,
  LIMIT t the next interval's start), get_char_property_and_overlay
  (`get-char-property`, `get-char-property-and-overlay`: fix_position, a
  live window's buffer with only its overlays, `(args-out-of-range POS)`
  outside BEGV..ZV), `get-pos-property` (overlays by their ends' advance
  types, else text_property_stickiness: `text-property-default-nonsticky`,
  `rear-nonsticky` before, `front-sticky` after), `next-char-property-
  change` and `previous-char-property-change` (bounded by
  Fnext_overlay_change), and the single-char-property scans, which now
  step from char-property change to change with OBJECT's buffer current
  instead of testing every character. `add-text-properties`,
  `put-text-property`, `set-text-properties`, `remove-text-properties`,
  `remove-list-of-text-properties` and `add-face-text-property` follow
  their GNU control flow: validate_plist (an odd list "Odd length text
  property list", a non-list that property with nil), the range checks,
  nil without intervals to remove from, `set-text-properties` t whenever
  there is something to set, and a buffer OBJECT current for the whole
  change, its hooks included (bug 36190; before, only
  prepare_to_modify_buffer ran there). `add-face-text-property` is
  add_properties' prepend or append per interval: an interval whose face
  is FACE already unchanged, a list face (not an anonymous one) taking
  FACE at its head or, copied, at its end, a single value (nil too)
  becoming a list of the two, in one walk over the range's intervals (a
  face over 2,000 intervals took 0.213 s, now 0.080 s; GNU 0.024 s; the
  single-char-property scans over them now beat GNU's time). Foverlay_get is lookup_char_property: a nil
  value is no value (a higher overlay's explicit nil no longer hides a
  lower one's), and `char-property-alias-alist` applies to overlays.
  editfns.c's find_field, `field-beginning`, `field-end`, `field-string`,
  `field-string-no-properties`, `delete-field` and `constrain-to-field`
  are ported (fields from overlays too; ONLY-IN-LINE and
  INHIBIT-CAPTURE-PROPERTY), and `line-beginning-position`,
  `line-end-position`, `beginning-of-line` and `end-of-line` constrain
  through it as GNU's do (`end-of-line` ignored fields before).
  `insert-buffer-substring` checks its range (`(args-out-of-range START
  END)`) and runs update_buffer_properties, as do `buffer-substring` and
  `buffer-string` (`buffer-access-fontify-functions`); `secure-hash` reads
  its object through extract_data_from_object as `md5` does (its range
  checked, a multibyte buffer encoded); `buffer-hash` hashes the whole
  buffer, narrowing aside; an overlay prints as print.c's `#<overlay from
  S to E in NAME>` or `#<overlay in no buffer>`. Also fixed: a `#N=` label
  inside a string's property list was left as a reader placeholder (its
  materialization never looked into strings), which failed six
  erc-fill-tests. The six `secure-hash`, `buffer-hash` and gnutls digest
  tests that hash a unibyte string run under the batch runtime now: GNU
  encodes it as `raw-text`, which mule-conf.el defines and the early-Lisp
  test environment lacks. Probes: the 98 region-validation cases of round 70 and
  64 property, field, overlay and hash cases give GNU's output. The GNU textprop,
  editfns, buffer, fns, print, lread, simple, text-property-search,
  comint, faces, font-lock, jit-lock, button, shr, erc, erc-fill,
  erc-stamp, eshell, em-prompt, em-hist, minibuffer, isearch, fill,
  subr-x, minibuf, help, elisp-mode, subr, undo, hi-lock, ert-x, python,
  casefiddle, xdisp, wid-edit, pp, diff-mode, cperl-mode, ses, eww, term,
  css-mode and package test files give GNU's results (help-fns-tests
  fails two tests under this checkout's GNU, for its source layout, and
  none under Emaxx). Still open: (1) intervals: a string's or buffer's
  adjacent spans with EQ plists are one span, so `object-intervals` shows
  fewer intervals than GNU where GNU keeps them apart; (2) deletions do
  not run update_buffer_properties (del_range_2 makes the deleted text a
  string with properties); (3) round 70's items (2) to (5).
- **Round 72: print.c's print, print_preprocess and print_object.** The
  printer walked an object by host recursion, each level returning its
  text for the level above to copy, so printing N nested levels cost
  O(N^2) (`print-deeply-nested`'s shape at 16,000 levels took 1.63 s,
  GNU 0.009 s) and 100,000 levels overflowed the stack (GNU prints
  400,000). It is now print.c's: one output, print_object's explicit
  stack (PE_list with Brent's tortoise, PE_rbrac, PE_vector, PE_hash),
  print_depth, being_printed and new_backquote_output as the shared
  printer state, and the C recursion only where print.c recurses
  (backquote and comma, string intervals, hash-table test and weakness,
  symbol-with-pos, native compilation units). 16,000 levels print in
  0.013 s, 400,000 in 0.54 s (GNU 0.55 s), release builds; ASCII
  characters skip the multibyte decode and encode, fixnums and symbol
  names go to the output without host strings, a result's character
  count is its length when it is ASCII, and the escape variables are read
  once a print and again after `print-unreadable-function' runs, so a flat
  150,000-element list prints in 0.020 s (GNU 0.036 s; 0.052 s before)
  and with `print-circle' in 0.175 s (GNU 0.155 s; 0.223 s). `print-circle` labels are
  `print-number-table`'s, as print.c keeps them: print_preprocess fills
  the table (t, then the negated next `print_number_index`), print()
  drops the entries seen once, print_object reads and writes the table by
  Fgethash/Fputhash (a string value printed in place), and the table is
  left in the variable after the print; before, a private count table
  stood in for it, so a print left `print-number-table` nil, continuous
  numbering did not number the table itself, and labels were looked up
  by a scan of all entries. `print-level` applies before `print-quoted`'s
  shorthands (`'b` at a too-deep level is `...`), backquote and comma
  print their operand one level deeper, string interval positions and
  plists print through print_object (`print-length` limits the plist, not
  the intervals; `print-integers-as-characters` applies), a sub
  char-table's slots count from SUB_CHAR_TABLE_OFFSET under
  `print-length`, and a list's cycle detection runs under `print-circle`
  too. `print-unreadable-function` runs for every unreadable object
  (windows, frames, processes, threads, symbols with positions, ...) but
  not closures, with ESCAPEFLAG as its second argument and itself bound to
  nil, any non-nil value handling the object; before, it ran only for
  closures, subrs, buffers, markers and overlays, with t, and recursed on
  itself. Windows print as `#<window N on BUFFER>` with window.c's
  sequence numbers (they printed their record slots),
  window configurations as `#<window-configuration>`, a marker that
  advances and has no buffer as GNU writes it, an empty-named gensym as
  `#:`, and a symbol of a private obarray with `#:` under
  `print-gensym`. keyboard.c's cmd_error sets `print-level` and
  `print-length` to 10 around the batch error report and
  `command-error-function`. Probes: 100 printer cases (circle, continuous
  numbering, gensyms, `print--preprocess`, hash tables, char tables,
  records, closures, strings with properties, print-level/length,
  quoting, unreadable objects) give GNU's output; print-tests,
  cl-print-tests, lread-tests, pp-tests, fns-tests, data-tests,
  subr-tests, bytecomp-tests and editfns-tests give GNU's results; the
  local gate passes (3,160 tests, the as-root and image exceptions below
  skipped; `subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`
  failed once under a parallel release build, its child exiting before
  the first `process-live-p`, and passes alone). Still open: fonts,
  tree-sitter objects and sqlite handles still print as their record
  slots, not as print_vectorlike_unreadable writes them. Note on the CI
  timings: the frozen run builds the gate profile (debug assertions,
  overflow checks, thin LTO), which puts nearly every file at 2.0 to 2.3
  times GNU; release-build comparisons are made locally.
- **Round 73: the image loader's tables, string data and heap blocks as
  pdumper.c and alloc.c keep them.** Startup (`-Q --batch --eval '(princ
  "x")'`, release) took 0.12 s against GNU's 0.05 s: 399M instructions
  (GNU 166M) and 17,747 minor page faults (GNU 5,642), at about 2.2 us a
  fault in this container, so the faults alone were some 26 ms. Where they
  came from: (1) the loader's dense tables, 5.5 MB of relocation kinds
  written in full, an 11 MB object index and 4.6 MB of `Option<Value>`;
  (2) a copy of every string's data, through a temporary vector per
  string; (3) heap blocks from glibc's aligned malloc, which writes its
  chunk header in the page before each block, so a 4 KB symbol, string or
  vector block took two pages; (4) the 22 MB image itself was cheap
  (file pages, 16 a fault). Now: the object-start and relocation tables
  are read where the image maps them, as pdumper.c reads its tables; the
  early relocations are walked in order alongside the fields being read
  (dump_do_all_dump_reloc_for_phase applies them in order), runs of
  fields read in one pass, with a rank index (a bit per word, counts per
  64 words) only for a read out of order; each relocation entry names its
  target's object number (`RELOC_ENTRY_LEN` 12: the writer resolves it
  from the sorted object starts), so a field naming an object is one index
  into the object vector, whose slots the kernel zeroes. A dumped string's
  data stays in the image, as GNU's stays in the mapped dump: the writer
  lays each cold string out as the sdata entry the loaded string keeps
  (owner word, bytes, NUL, padding to `string_data_size`), the image is
  mapped private and writable and never unmapped (pdumper.c maps every
  section read-write and keeps the dump), and the string's header points
  at its entry, which no block lists, so compaction never moves it and
  retiring or resizing it within its slop writes the entry like any other
  (a page copied on write); ASCII text is counted without decoding. Heap
  blocks (cons, float, symbol, string, vector) come from an arena of 4 MB
  anonymous mappings carved in address order, released blocks kept on
  their size's free list as `free` keeps a chunk; symbol and string blocks
  are their cells (21 symbols, 1,344 bytes; 85 strings) at cache-line
  alignment instead of a rounded, page-aligned 4 KB. A copied subr record
  is resolved once per record through a name index of the subr table (the
  linear search cost 4,700 instructions a reference). `install_image`
  finds a symbol's record by the symbol's identity and installs its
  function cell, plist and watchers on that symbol: by name, an uninterned
  symbol's cells went to the interned symbol of its name, or were skipped
  when a same-named record had been installed. Native-subr records start
  without names (two allocations each before), and the unit file name is
  built only for an error. Result: 0.07 to 0.08 s, 8,545 faults, 298M
  instructions, RSS 90 MB to 75 MB. What remains of the 0.02 to 0.03 s:
  rebuilding the 285,638 objects (phase 2 about 17 ms, mostly symbols,
  conses and sub char-tables), `Interpreter::new` (20M instructions:
  temacs's syms_of_* work, which the image then replaces; GNU skips it
  when a dump loads), native-code relocation as GNU does it (dlopen of 87
  units, dlsym of 5,901 functions), and the record registry indexed by id
  (record ids reach 190,000 for 800 records, 1.5 MB of slots). Probes:
  `aset` on a dumped string in place and across a width change, then
  collections, give GNU's results; fns, data, alloc, print, subr, lread,
  editfns, casefiddle, chartab and cl-lib tests give GNU's results; the
  pdumper tests and the local gate pass (3,202 tests, the as-root and
  image exceptions below skipped).
- **Round 74: the image's tables read at a fixed stride, image names as
  C strings, a process that loads an image skips what the image defines.**
  Startup (`-Q --batch --eval `(princ "x")'`) was 298M instructions (GNU
  166M) and 0.076 s median against GNU's 0.056 s. Phases measured with
  per-phase counters (callgrind dumps at each boundary): the table reads
  28.5M, `Interpreter::new` 20.3M, building the image`s strings 31.3M,
  its symbols, cons placeholders and vectors 45.6M, relocating fields
  64M, native code 27M. Changes: (1) `ImageTable` is typed by its entry
  length (`[[u8; 12]]` relocations, `[[u8; 8]]` object starts), so an
  entry's word is one bounds check at a constant offset, not a multiply
  by a run-time stride behind an assertion; the order check is one
  branch-free pass with the failure located after; the rank index (a bit
  a word, counts a block) is gone, a read past the cursor searching
  outward from it in doubling steps; (2) a cons's two fields and a symbol
  record's three are read by a const-length `field_array`, inlined, with
  `immediate_value` inline and its error cold; (3) a dumped string is
  made by `allocate_restored_string` (alloc.c`s allocate_string for a
  header complete at birth, no initializer or construction guard), its
  four header words read at once; (4) the 5,901 native subrs' names are
  written as dump_cold_native_subr writes them: NUL-terminated bytes in
  the cold section, the record's two words raw dump pointers to them
  (they were two Lisp strings each, 11,802 objects), and the loaded
  `Lisp_Subr` points at them (`set_image_names`, never freed), as a
  dumped subr's `symbol_name` and `native_c_name` point into the dump; a
  first version pointed at the old name strings' data, which a collection
  of those unreferenced strings retires (the entry's first bytes take the
  size), and the image round-trip tests caught the garbled name;
  RELOC_NATIVE_SUBR's dlsym takes that C string and calls Fgethash and
  Fputhash directly instead of by name;
  (5) char-table and sub-char-table slots are stored at once with one
  store count, and the 177,573 cons placeholders are made at once
  (`allocate_nil_conses`: Fcons of nil and nil from the newest block's
  untouched cells, a run at a time, allocation bits set a word at a
  time); (6) the record registry is a `BTreeMap` by id (ids reach
  190,000 for 800 records: the vector was 1.5 MB, zeroed); (7) emacs.c
  runs syms_of_* only `if (!initialized)`: `Interpreter::for_image`
  leaves out the oracle's printed C defaults read through the reader,
  the mark-all-globals-special pass, the DEFSUBR loop and the DEFSYM
  interning, all of which an image's symbols carry whole (value, flags,
  function cell, plist); a process whose image does not load starts again
  from `Interpreter::new`. A dump of all 18,003 symbols (flags, boundp,
  fboundp, function type, plist length, value) is identical between the
  two constructors in one binary except the allocation counters (2
  conses, 2 strings) and timing values. Result: 221M instructions
  (`Interpreter` construction 7.8M, tables 5.2M, native 24.4M), 0.068 s
  median against GNU's 0.057 s (40 interleaved runs), 7,438 page faults
  (GNU 5,614), RSS 72 MB (GNU 55 MB). The local gate passes (3,202
  tests).
- **Round 75: process.c's get_process, non-blocking connects, deletion
  and EOF on connections, timers bounding the wait.** A survey of the 519
  GNU test files (each under GNU and Emaxx, `-l ert -f
  ert-run-tests-batch-and-exit`) took 1,616 s against GNU's 1,842 s; what
  stood out was correctness: 21 `erc-scenarios-*` files, outside the
  frozen selection (`:expensive-test`, local servers), failed under Emaxx
  and passed under GNU. Startup was looked at again first: a linear probe
  of the relocation cursor gained nothing (137.17M against 137.13M
  instructions in the loader) and is not kept; skipping the dumped
  strings' character recount saves 2.9M instructions and no page faults
  (the faults are the 11 MB of objects the loader makes, 2,956 of them
  in the symbol and container phase), so the check stays. Changes: (1)
  process.c's `get_process` (a process, a process's or buffer's name, a
  buffer, nil for the current buffer's process; "Process NAME does not
  exist", "Attempt to get process for a dead buffer", "Buffer NAME has no
  process", else CHECK_PROCESS) for `delete-process` (now 0 to 1
  arguments, its interactive `message` reporting "Deleted process"),
  `process-status` (a name finds only a process, none being nil),
  `process-type`, `process-send-string`, `process-send-region`,
  `process-send-eof`, `process-running-child-p`, the signal functions and
  `internal-default-signal-process`; they took a process object only, or
  answered `processp` for every designator; (2) Fget_buffer_process: nil
  has no process, a buffer object is taken as it is, killed or not, a
  name finds its live buffer, anything else is CHECK_STRING's error; it
  signaled "Selecting deleted buffer" for a killed buffer, which every
  ERC test's cleanup asks about, and read nil as the current buffer (the
  mode line's `%s` and `process-send-eof` now pass the current buffer);
  (3) a `:nowait t` client connects as connect_network_socket's
  non-blocking client: a socket per address, O_NONBLOCK and FD_CLOEXEC,
  connect returning EINPROGRESS, the process `connect` with the addresses
  left (status (connect . ADDRINFOS)), `:remote` the address dialed; the
  event loop's wait finds the socket writable and reads SO_ERROR: zero
  is `open` (after any TLS), an error goes on to the next address, and
  with none left the status is (failed ERRNO) and the sentinel hears
  "failed with code 111"; a socket still connecting is not read, and a
  connect that cannot start on any address leaves the process failed;
  the client connected synchronously and signaled `file-error` at once,
  so ERC's reconnect timers and connectivity probe never ran; (4)
  Fdelete_process: a connection's status becomes (exit 0) whatever is
  left of its socket, a child still running is killed, and status_notify
  reports `status_message` of the new status to a process still on the
  process alist only; a connection whose peer closed leaves the alist
  (`delete-exited-processes`) before its "connection broken by remote
  peer" sentinel, so a later `delete-process` notifies no one; the sentinel
  heard "killed" for a closed connection; (5) server_accept_connection's
  HOST: "open from 127.0.0.1" and "accept from 127.0.0.1\n" (the port was
  in both and the log line had no newline), IPv6 children named "NAME
  <[HOST]:PORT>" with HOST_FORMAT_IN6; (6) Fprocess_send_eof on a
  connection shuts down its sending half and points output at the null
  device (`output_to_null`), as for a pipe or a non-pty child; a serial
  port drains (tcdrain); it did nothing for a connection, so a server
  never read the end; (7) timer_check's delay: the wait
  (`wait_pumping_processes`) is bounded by the earliest timer on
  `timer-list`; it napped in 10 ms steps, so a 1 s timer fired up to
  10 ms late (erc-d's "Lingered for 1.00 seconds" read "1.01"); a 1 s,
  0.25 s and 0.3 s timer now fire at 1.0003, 0.2503 and 0.3003 s (GNU
  1.0004, 0.2503, 0.3006); (8) send_process's wait_while_connecting:
  `process-send-string`, `process-send-region` and `process-send-eof` on
  a connection still connecting wait (running timers, reading other
  processes) until it is open or failed; they wrote to the socket still
  connecting; (9) buffer.c's Frename_buffer: UNIQUE asks
  `generate-new-buffer-name` with the old name as IGNORE, so `a<2>`
  renamed uniquely to `a` stays `a<2>` (it became `a<4>` with `a<3>`
  present, and ERC, renaming a reconnected channel's buffer, took another
  buffer's name), a non-unique rename to the buffer's own name returns
  early, an empty name signals "Empty string is invalid as a buffer
  name", and a buffer visiting no file with an auto-save file name calls
  `rename-auto-save-file` through its function cell. Of the 21 failing
  files, 19 pass. The other two run their test in a child Emacs under
  `term`: status-sidebar's nickbar test needs window.c's tree operations
  (its `delete-other-windows` deletes the speedbar's side window, which
  carries `no-delete-other-windows`, because `delete-other-windows-internal`
  ignores ROOT; the speedbar then turns itself off and kills its buffer
  while the test waits on a marker in it); keep-place-indicator's follow
  test times out after 21 s.
- **Round 76: window.c's tree operations and window change functions.**
  The last two `erc-scenarios-*` failures pass (all 21 now). (1)
  window.c's Fsplit_window_internal, Fdelete_window_internal and
  Fdelete_other_windows_internal with window_resize_check,
  window_resize_apply, replace_window, make_parent_window,
  recombine_windows, delete_all_child_windows and unshow_buffer: a split
  reuses its parent combination unless `window-combination-limit' is t,
  OLD has no parent or the parent combines the other way, and applies
  the sizes and normal sizes `split-window' staged (three `split-window'
  calls give one combination of three with normal sizes 0.25, 0.25 and
  0.5, window numbers as GNU's; Emaxx nested binary parents, every normal
  size 1.0); deletion applies the staged sibling sizes, a parent left
  with one child is replaced by it and merged into a same-direction
  grandparent, and "Deletion failed" when the sizes do not add up;
  `delete-other-windows-internal' replaces ROOT (default the frame's root)
  by WINDOW and leaves windows outside ROOT, so `delete-other-windows'
  keeps a side window marked `no-delete-other-windows' (it flattened the
  whole frame, which turned nickbar off), selects a live WINDOW, resizes
  an internal one through `window--resize-root-window' and signals
  "Specified root is not an ancestor of specified window"; the unstaged
  calls GNU rejects ("Resizing old window failed", "Sum of sizes of old
  and new window don't fit") are rejected; `window-resize-apply' applies
  FRAME's staged tree only when the root's new size equals its size (it
  applied any staging on the selected frame); the tty mini window's
  growth commits the root through grow_mini_window's direct apply; (2)
  `set-window-parameter' and `window-parameter' by `assq': a new entry
  goes on the front and nil stays an entry (entries were appended,
  removed for nil and found by `equal'); (3) window.c's
  run_window_change_functions from redisplay (never in batch, as GNU's
  initial-frame redisplay): `window-buffer-change-functions',
  `window-size-change-functions', `window-selection-change-functions' and
  `window-state-change-functions' buffer-locally per window, then per
  frame, `window-configuration-change-hook' and `window-state-change-hook',
  calls muted as safe_funcall ("Error muted by safe_call"), then
  window_change_record's stamps, old buffers, old pixel and body sizes,
  frame old selected windows and the old selected window and frame; none
  of these ran, `window-old-buffer' was always nil and the old sizes 0;
  `old-selected-frame' changes only there (a frame switch set it); the
  initial frame counts as made (make_initial_frame); a redraw that yields
  to pending input still runs them, as redisplay_internal does before any
  update. A `-nw' probe of split, switch-to-buffer and other-window logs
  the same hook calls and old selected windows as GNU. Known batch
  difference: GNU's `window-resize-apply-total' puts the root at the
  frame's top margin (the initial frame's one menu-bar line), so after
  `set-window-configuration' GNU's batch root is 23 lines; Emaxx keeps one
  coordinate set and stays at 24. Tests that called the internal split and
  delete primitives without staging now use `split-window' and
  `delete-window', or stage as `split-window' does. (4)
  wait_reading_process_output's READ_OUTPUT_DELAY_INCREMENT: a wait for
  any process (`accept-process-output' with nil) returns once output has
  been quiet for 10 ms; the grace was re-armed on every pass after the
  first output (the delivery count compared with the wait's start), so
  such a wait always lasted its full timeout and ERC's `erc-d-t-wait-for'
  missed state that lasts under 0.1 s (join-display-context's requested
  channels, which passed before round 75 by timing). Newly exposed and
  fixed in round 77: `erc-scenarios-scrolltobottom--relaxed' (an `:expensive-test' in a
  `term' child, outside the frozen selection) fails its "New message does
  not trigger a scroll when at prompt" sustain check: with the wait now
  returning on output, its loop redisplays while messages arrive, and
  Emaxx's redisplay moves the window start of a window whose point is at
  the prompt (GNU's keeps it); the next round starts there. Checked: the
  buffer-local `window-configuration-change-hook' (scrolltobottom's
  recenter to the bottom) never runs in that test; the open hypothesis is
  that redisplay re-anchors the window start that relaxed scrolltobottom
  restores with `recenter' (fill-wrap's visual lines), so
  `count-screen-lines' from the start to the prompt changes. Of 42 ERC and
  window-related GNU test files (the 21 former failures and every file
  using `split-window', `delete-window', `display-buffer' or
  `pop-to-buffer'), all others match GNU's results.

- **Round 77: xdisp.c display_line's row breaking on terminal frames.**
  `erc-scenarios-scrolltobottom--relaxed' passes. Its failure was not the
  window start's anchoring itself: Emaxx's tty redisplay ignored
  `line-prefix', `wrap-prefix' and `word-wrap', while `vertical-motion'
  (which `recenter' and `count-screen-lines' walk) used the batch vmotion
  model, so the window start relaxed scrolltobottom restored with
  `recenter' on ERC's fill-wrap lines was not a row start of the
  redisplay's layout and the redraw moved it to the line's start. (1)
  `src/tty/layout.rs' lays out a display line as display_line does on a
  tty: the last column holds the continuation glyph; under `word-wrap'
  (and `word-wrap-by-category', its kinsoku categories) a row backs up to
  its last wrap point (the first non-whitespace after whitespace) with no
  `\' glyph, so a word ending at the edge or a space run past it moves
  down, and only a word with no wrap point breaks mid-word with `\';
  `line-prefix' starts a line's first row and `wrap-prefix' each
  continued row, from the char property at the row's first position
  (overlays for that window included, handle_line_prefix) else the
  buffer-local variable, the first real glyph after a pushed prefix always
  produced; TAB stops are Emacs 30's term.c: on a line's first row
  current_x (prefix cells included), on a continued row current_x plus
  continuation_lines_width (every earlier row's full x, the
  `display-line-numbers' gutter included) less the wrap-prefix's width,
  the gutter left out, to the next multiple of the buffer's sane
  `tab-width' — so `ab1<TAB>' after a 2-cell line-prefix stops at column
  8, and a TAB split at the edge resumes on the next row after a
  wrap-prefix or gutter exactly where GNU's does. Columns live in that x
  space, which
  keeps the cursor, face spans, glyphless restarts and hscroll one model;
  truncated rows carry `line-prefix' too. (2) Prefix values: a non-empty
  string shows its characters with its own `face' properties; `(space
  ...)' is produce_stretch_glyph's width from calc_pixel_width_or_height
  (`:width', `:relative-width', `:align-to', symbols read buffer-locally,
  `(+ ...)', `(- ...)', `(NUM)', `(NUM . UNIT)'), drawn in the face of the
  row's first character; a face with `:extend' running on past a
  word-wrapped row fills that row to the edge (back_to_wrap's
  extend_face_to_end_of_line), others stop at the row's last glyph (they
  ran to the window edge). (3) Interactive `vertical-motion' (with WINDOW
  lending its geometry, the goal column of `(COLS . LINES)' measured from
  the text area), `posn-at-point' (X counting the line-number gutter, a
  position past a truncated edge at the last column), `recenter' and
  scrolling walk the same layout; batch keeps vmotion. move_it_by_lines'
  reseat past a truncated last line ending at ZV is one more screen line
  (GNU's `count-screen-lines' counts it). (4) Auto-hscroll measures the
  cursor's row with the same layout, and counts point as at the end of
  the line only on a newline (ITERATOR_AT_END_OF_LINE_P): at ZV of a
  truncated line it centers the cursor as GNU does (it kept it 4 columns
  from the edge). Probes in an 80x34 tmux pane
  (plain word-wrap with words and spaces at the edge, TABs, text-property
  and overlay prefixes of both kinds, a variable `wrap-prefix', a
  truncated buffer with prefixes, `tab-width' 4, line numbers) give GNU's
  screen, cursor, `vertical-motion' row starts, `count-screen-lines',
  `window-end' and `posn-at-point' columns, and so do four probes of
  TABs on continued rows (a variable `wrap-prefix', line numbers, both,
  and `word-wrap' with `tab-width' 5). (5) `write-region''s closing
  message is fileio.c's: through message_with_string (no `message'
  advice, `%' in a file name kept), "Wrote", "Added to" (APPEND non-nil)
  or "Updated" (APPEND a number), naming the expanded VISIT or FILENAME
  (it said "Wrote" with the unexpanded argument). Local validation: the
  full gate, 3,204 passed across its ten groups with the documented
  local skips (before the TAB and message changes; the tty group, 66,
  and the `write_region'/`append' tests passed after them), and 82 GNU
  test files (dired, edebug, every ERC file, flymake, python, replace,
  simple, so-long and others) as GNU (python-tests'
  `python-nav-end-of-statement--infloop' fails in GNU too). Not covered
  then (round 78 covers both): double-width characters took one cell; a
  TAB inside a `line-prefix' or `wrap-prefix' string took one cell (GNU
  expands it from the row's start).

- **Round 78: get_next_display_element's character translation and
  term.c's glyph widths on terminal frames.** (1) Display text is now
  one character a terminal cell: `translate' (window text, overlay and
  display strings, prefixes, mode, header and tab lines, the echo area)
  emits a wide character followed by PADDING_GLYPH cells (append_glyph's
  padding_p glyphs; `char-width-table', sanitized, gives the width) and a
  zero-width character as ATTACHED_GLYPH plus the character, drawn in the
  cell before it (the terminal composition compose-gstring-for-terminal
  makes; with no base it follows a space, and with
  `auto-composition-mode' off it produces no glyph). PaintRow keeps each
  cell's attached characters, never prints padding, and a blit that cuts
  a wide character blanks its remains. The order is GNU's: a display-table
  vector, an explicit `glyphless-char-display' method, the escape
  translations, then the no-font method for a character outside the
  terminal's charset list. (2) Escape translations: a control character
  shows as `^C' under the buffer's `ctl-arrow' (else `\003'), DEL as `^?',
  a newline only in mode lines; a raw byte as `\200' (`\x80' under
  `display-raw-bytes-as-hex'); a character outside `printable-chars' (C1
  controls) as its octal code; under `nobreak-char-display' t a non-ASCII
  space (Zs) or soft/non-breaking hyphen keeps its glyph (or ` '/`-' under
  `nobreak-char-ascii-display') in `nobreak-space'/`nobreak-hyphen', and
  under another non-nil value shows the escape glyph and ` ' or `-'.
  Escape glyphs take `escape-glyph' or the display table's escape/control
  glyph (extra slots 2 and 3) with its face; each glyph is its own
  element, so `^A^B^' can end a row and `C' start the next as GNU does.
  These variables are read buffer-locally, as redisplay sees them. (3)
  Layout: an element is a glyph with its padding and attached
  characters; a padding glyph that does not fit fills the rest of the
  row with `\' and moves the character down, before any word-wrap point
  (GNU's word-wrapped CJK row ends `\\'), while vertical motion,
  `posn-at-point' and recentering walk move_it_in_display_line_to's rows,
  which go back to the wrap point instead; a truncated row's `$' replaces
  from the first cell of the character in the last column (`$$' for a
  wide one); hscroll skips cells; the echo area wraps a wide character
  whole. A prefix string goes through the same translation, its TABs
  measured from the row start and its faces moved to cells. (4) term.c's
  charset list: a terminal's t->charset_list is nil (every character
  drawn) until `set-terminal-coding-system-internal' sets it to the
  coding's charsets, or ASCII when that coding converts nothing, and
  output writes `?' for each cell (and composed character) the output
  coding (the terminal's, else the safe coding) cannot encode: with LANG=C
  GNU shows `??' for a CJK character, which Emaxx had drawn as a hex-code
  glyphless escape. (5) Mode lines: CURRENT_MODE_LINE_ACTIVE_FACE_ID, so
  a non-selected window's mode line is `mode-line-inactive' (all were
  `mode-line'), the selected window's and, while the minibuffer is
  selected, the window it was entered from `mode-line-active', all of
  them when `mode-line-in-non-selected-windows' is nil; a wide character
  cut at the edge leaves pad_mode_line's `-' (a space in header and tab
  lines) in the row's base face. Probes in 80x34 tmux panes, UTF-8 and
  LANG=C: wide characters at the continuation edge, after 79 and 77
  columns, under `word-wrap' (CJK words and Japanese with no spaces), a
  line of 30 CJK characters, TABs after them, combining marks after a
  base, a CJK character and at line start, truncated lines with CJK at
  the edge and `$$', hscroll into CJK text, a split frame with a CJK
  buffer name cut in the mode line, a CJK `line-prefix' with a TAB, faces
  over CJK, an echo message wrapping CJK, and every control, raw-byte,
  C1, non-ASCII space and hyphen, and zero-width case under both
  `ctl-arrow' values and `nobreak-char-display' t and 1, give GNU's
  screen (with SGR attributes), cursor, `vertical-motion' rows,
  `count-screen-lines', `window-end' and `posn-at-point'; round 77's wrap,
  prefix, truncation, motion and TAB probes are unchanged. Not covered:
  `posn-at-point' of a position after a wide character whose padding
  ends exactly at a truncated row's last column reports GNU's (80 . 1)
  only through posn-at-x-y's coordinate classification, which Emaxx
  clamps to (79 . 0); compositions beyond base plus zero-width marks
  (composition-function-table rules, grapheme clusters, point
  adjustment) are not consulted; `format-mode-line' still ignores FACE,
  WINDOW and BUFFER (GNU formats in WINDOW's buffer and keeps faces;
  round 79 ports it).
  Local validation: the full gate, 3,207 passed across its ten groups
  with the documented local skips (the tty group, 68, includes the new
  continuation-TAB, translation and padding-fill tests), and 81 GNU test
  files (dired, edebug, every ERC file, flymake, python, replace,
  simple, so-long and others) as GNU.

- **Round 79: xdisp.c's mode-line formatting (display_mode_element,
  decode_mode_spec, Fformat_mode_line, display_mode_line).** A new module,
  `src/lisp/primitives/dispatch/display/mode_line.rs`, replaces the old
  renderer. (1) One engine with GNU's targets: `format-mode-line' builds
  MODE_LINE_STRING's propertized pieces (store_mode_line_string: a FACE
  argument merged under each piece's own face, `t' stored as is, an
  integer FACE giving MODE_LINE_NOPROP's plain text), and a window's mode,
  header and tab lines lay out MODE_LINE_DISPLAY's terminal row as
  display_string does: the frontend measures each glyph's cells (display
  table, escapes, `char-width-table'), a TAB goes to the buffer's next
  tab stop, the row stops at its right edge and is filled with spaces
  (a custom format no longer ends in dashes). (2) display_mode_element:
  a symbol without a `risky-local-variable' property makes its value
  risky (strings lose their properties, :eval and :propertize are
  ignored); a symbol's string value shows literally; (SYMBOL THEN ELSE)
  tests the symbol's dynamic value, nil, t and keywords included, and
  any malformed element is "*invalid*"; (WIDTH ...) pads and truncates
  in columns, padding in the base face and only after a list's last
  element; :propertize's props replace the outer ones and merge over a
  string's own (those at its start) across the whole copy; a %-construct
  takes the properties of the character after its `%'; depth over 100
  gives "*too-deep*"; a NUL ends a string; :eval is dsafe_eval (`(eval
  FORM t)' with quitting inhibited, an error logged as "Error during
  redisplay: (eval FORM t) signaled ..."). (3) decode_mode_spec: %l from
  the window's start through w->base_line_number (a window slot), "??"
  over `line-number-display-limit' or when lines are longer than
  `line-number-display-limit-width' (GNU's backward scan), carriage
  returns counted under `selective-display'; %c %C %i right-justified in
  their field (pint2str) and %I as pint2hrstr; %o %p %P %q from the
  window's start and the end its last redisplay recorded
  (w->window_end_pos, a window slot); %m only for a string `mode-name';
  %M processes `global-mode-string' as a construct; %- is "--" in strings
  and FRAME_MESSAGE_BUF_SIZE dashes on the row; %[ %] the recursive-edit
  depth; %e, %F (the title, else the name), %z and %Z with the
  end-of-line mnemonic (a string, a character or "(*invalid*)"); an
  unknown spec is "". (4) format-mode-line decodes WINDOW and BUFFER
  (wrong-type-argument windowp or bufferp, the dead-buffer error) before
  the batch no-op, answers "" for a nil FORMAT, and formats with WINDOW
  selected and BUFFER current, then restores the selection, the old
  window's point and the buffer, so Buffer-menu's Mode column shows each
  buffer's mode. (5) display_mode_line: the window's
  `mode-line-format', `header-line-format' or `tab-line-format' parameter
  overrides the buffer's, match data is saved around the formatting,
  `mode-line-compact' (and `long') squeezes runs of spaces, and the row
  handles a string's properties as display_string's iterator does:
  `invisible' text is hidden under the buffer's invisibility spec, a
  display string replaces its run, `(space :width ...)' and `(space
  :align-to ...)' stretch (align-to from the text area's left edge), a
  `min-width' sequence is padded when the row moves on to a string
  without it (one column when the sequence is already wider), `(when
  FORM . SPEC)' tests FORM; mode-line strings use `standard-display-table'
  when it is a display table. tmux probes (35 format-mode-line cases with
  faces, risky symbols, widths, every %-construct, errors; screens of
  custom, min-width, align-to, right-aligned, compact, header and tab
  lines, a window parameter, invisible text, scrolled, narrowed and
  read-only buffers, side-by-side windows, `list-buffers') give GNU's
  results; 2,000 `format-mode-line' calls of the default format take
  0.17 s against GNU's 0.35 s and redisplay costs the same. Not covered:
  GNU's interval tree keeps adjacent intervals with equal properties
  apart where Emaxx's string properties merge them, so a result built
  from equally propertized pieces (any `concat' of them too) prints
  fewer intervals (#("ab" 0 2 (face bold)) for GNU's #("ab" 0 1 (face
  bold) 1 2 (face bold))); the base-line cache's validity uses the
  buffer's modiff and accessible region for BEG_UNCHANGED; a window
  whose `mode-line-format' is nil still gets a blank mode line row
  (window_wants_mode_line); a non-selected window keeps its start when
  its point is off-screen, where GNU's redisplay_window scrolls it, so
  its %l and %p (and `window-end') can differ; the terminal draws no
  italic, strike-through or dim face attribute. Local validation: the
  full gate, 3,208 passed across its ten groups with the documented local
  skips (primitives, 751, includes the new 35-case format-mode-line
  test), and 14 GNU test files (buff-menu, ibuffer, tab-bar, so-long,
  simple, help-fns, package, eshell, which-func, erc, erc statusmsg,
  xdisp, buffer, textprop) as GNU.

- **Round 80: redisplay_window's window start for every window, and
  the window commands that steer it (window.c, xdisp.c).** (1) Window
  state as GNU keeps it: a window's start is a marker (an insertion
  above it moves it with the text, like the window points);
  `window-start' reads it unclamped; force_start, optional_new_start
  and start_at_line_beg are window slots; the frontend records each
  window's window_end_pos, so `window-end' answers Z less it for any
  window (Z in batch, before any redisplay), and with UPDATE displays
  the window's body from its start. (2) Fset_window_start keeps the
  exact position (clamped to the accessible region, no longer snapped
  to its line), sets force_start unless NOFORCE, and returns POS;
  Fset_window_point moves the buffer's point for the selected window and
  clamps the window's own point marker for another; Frecenter is
  ported (the current-buffer error, ARG nil and C-u centering, ARG
  clipped to the scroll margins, the initial frame's vmotion,
  optional_new_start, `recenter-redisplay'); scroll-up and scroll-down
  are window_scroll_line_based (the start from pos-visible-in-window-p,
  force_start, point kept out of the scroll margins,
  `scroll-preserve-screen-position' with window_scroll_preserve_vpos,
  window-old-point following); Fmove_to_window_line is ported
  (displayed_window_lines, a start outside the accessible region
  recomputed with force_start); set-window-buffer is unshow_buffer
  (the buffer's last_window_start, the window's point back into a
  buffer no other selected window shows) plus set_window_buffer (start
  from last_window_start, point, hscroll and the flags reset, unless
  KEEP-MARGINS on the same buffer; `window-scroll-functions' run);
  delete-other-windows-internal moves the start up by the window's old
  top line so the text stays in place (optional_new_start). (3)
  windows_or_buffers_changed: an interpreter flag set by wset_redisplay
  of a window other than the selected one (set-window-start,
  set-window-point, set-window-buffer, an hscroll change), by
  select-window, the window-tree primitives, set-window-configuration,
  force-window-update and set-buffer-multibyte; the frontend adds a
  resize, a face change, changed window edges and bset_redisplay's rule
  (a buffer modified since the last redisplay shown in a non-selected
  window or in two windows). (4) `src/tty/redisplay_window.rs':
  redisplay_window's decision for every window, selected or not, on
  the window's own point (a non-selected window's point clamped and
  written back): compute_window_start_on_continuation_line under
  windows_or_buffers_changed (the row start nearest the start, not
  after the buffer's point); optional_new_start becoming force_start
  when point shows; force_start keeping the start and, when point does
  not show, moving it to the row half the window reaches (header and
  tab rows counted, as MATRIX_ROW_BOTTOM_Y does) and into the window's
  point or the buffer's; the scroll margin (window_scroll_margin with
  `maximum-scroll-margin'); try_cursor_movement's no-motion success;
  start_at_line_beg's recenter; the same start under try_window's
  CHECK_MARGINS; try_scrolling (`scroll-conservatively', `scroll-step',
  `scroll-up-aggressively', `scroll-down-aggressively', the limit of
  100) and recentering (centering at half the window, or under
  conservative or aggressive scrolling just outside the margin);
  `window-scroll-functions' run (safe_run_hooks_2) at GNU's three
  points with redisplay continuing from the start they leave;
  `make-window-start-visible'; round 79's noted gap (a non-selected
  window keeping its start with its point off screen) is gone. (5)
  `window-configuration-equal-p' is compare_window_configurations: the
  frame's size, selected frame and current buffer, then every saved
  window in save_window_save's order (the window tree depth first, the
  minibuffer window last) by whether it is the current window, its
  buffer, pixel edges, display table and its parent's and previous
  sibling's indices; Emaxx compared only the selected window's slots, so
  a split, a buffer change in another window or a different selected
  window went unnoticed, and the window's point and the new start flags
  were compared. (6) Fsplit_window_internal ends with set_window_buffer
  (new, r->contents, true, true): the new window takes the reference
  window's margins and, having had no buffer, the buffer's
  last_window_start and point (Emaxx gave it the old window's start and
  no point, which redisplay then clamped to 1), and
  `window-scroll-functions' run from there (and once more from
  split-window). (7) handle_single_display_spec's `((margin LOCATION)
  VALUE)' on buffer text: under `left-margin' or `right-margin' the
  covered text leaves the text area and VALUE is drawn in that margin on
  the row where the run displays, after the row's earlier margin
  glyphs; `(margin nil)' displays VALUE in the text area (only overlay
  strings were handled; ERC's right-margin timestamps were laid out
  inline and wrapped its rows). (8) Display never looks outside the
  accessible region: a line the narrowing cuts lays out from BEGV and
  stops at ZV (Buffer::display_line_start_of), so `vertical-motion' and
  `count-screen-lines' in a buffer narrowed mid-line find GNU's rows.
  (9) `buffer-text-pixel-size' measures BUFFER-OR-NAME and, like
  unwind_with_echo_area_buffer, gives WINDOW back its points and start
  through set_marker_restricted_both (inside the accessible region: ERC's
  fill-wrap measures in a narrowed buffer and so moves the start to
  BEGV); a WINDOW that is not live signals (wrong-type-argument
  window-live-p OBJ). (10) keyboard.c command_loop_1's entry, at the top
  level and for each iteration of `execute-kbd-macro': the post-command
  hooks, then `last-command', `real-last-command' and
  `last-repeatable-command' from the command that ran; after each
  command `delayed-warnings-hook' runs while `delayed-warnings-list' is
  non-nil (never before); `this-command', `real-this-command',
  `this-original-command' and `this-command-keys-shift-translated' are
  nil while a key sequence is read. With these,
  erc-scenarios-scrolltobottom--relaxed passes again (round 80's
  redisplay had recentered its lower window by Emaxx's narrower row
  layout) and its traced window starts and points match GNU's. tmux probes of GNU 30.2 — non-selected
  windows with point off screen, forced and optional starts in both
  kinds of window, mid-line starts with and without
  windows_or_buffers_changed, scroll-margin, scroll-conservatively 101
  and 5, scroll-step, both aggressive options, every recenter argument
  with and without margins, scroll-up and scroll-down with margins and
  preserve-screen-position, move-to-window-line, the end of the buffer,
  an insertion above a non-selected window's start,
  `window-scroll-functions' calls, `window-end' with UPDATE,
  last_window_start, delete-other-windows, conservative scrolling over
  wrapped lines and a header line — give GNU's window-start,
  window-point, window-end, hook calls, screens and cursor. Not covered:
  a start in the middle of a screen row is shown from that row's start
  (GNU's start_display shows the row from the start itself), so such a
  start's first row and window-end differ; the try_window_id path's
  continuation-line snap (BEG_UNCHANGED/END_UNCHANGED) is not modeled;
  try_scrolling measures the distance from point's row, where GNU
  starts a fresh row at a point inside a continued row; face changes
  count as windows_or_buffers_changed only through the face generation;
  frozen window starts (`frozen-window-start-p') are not consulted.
  Found, not fixed: GNU keeps a window's character geometry (top_line,
  left_col, total_lines, total_cols) apart from its pixel geometry, and
  in batch, where the initial frame counts one menu-bar line in
  FRAME_TOP_MARGIN but places its root window at pixel row 0, the first
  `window-resize-apply-total' (any split or delete) moves the root's
  top_line to 1: after `split-window' GNU's `window-edges' are (0 1 80
  13) and (0 13 80 25) while the pixel edges stay (0 0 80 12) and (0 12
  80 24), and a configuration saved before the split no longer equals
  the same single window after `delete-window'. Emaxx derives both from
  the pixel slots, so it answers (0 0 80 12) and t there; the
  window-configuration test leaves that comparison out. Also found:
  move_it_by_lines with a negative count reseats with handle_stop, so a
  window start or `vertical-motion' result that is the first row of a
  buffer line skips leading invisible text and `display' "" runs
  (GNU's (recenter 1) and (vertical-motion -1) land 9 characters past
  the line start when the line begins with such a 9-character prefix;
  move_it_vertically_backward and vertical-motion from a line start do
  not), where Emaxx answers the row's first position: the screens are
  the same, the positions differ. And `menu-bar-mode' changes the frame
  size only at the next redisplay in GNU (change_frame_size with
  delay), where Emaxx resizes the windows at once.
  Local validation: the full gate, 3,214 passed across its ten groups
  with the documented local skips (eval_03, 324, includes the new
  window-configuration, split-window, `buffer-text-pixel-size' and
  keyboard-macro tests; tty, 71, the redisplay_window probe, margin
  display and narrowing tests), and 88 GNU test files (every ERC
  scenario file, edebug, kmacro, keyboard, minibuffer, simple, xdisp,
  scroll, isearch, repeat, term, python, so-long and others; 1,782
  tests) as GNU.
- **Round 81: window character geometry, a text frame's size and its
  menu and tab bars, as window.c, frame.c and dispnew.c keep them.**
  (1) A window keeps left_col, top_line, total_cols and total_lines in
  slots of their own, apart from its pixel geometry, set where window.c
  sets them: window_resize_apply (the total from the new pixel size,
  each child's column or line from the edge), split-window-internal (the
  orthogonal ones copied from the old window; the new window's
  `window-new-total' from the parent's total, not its pixel size, and
  the new parent's from the old window's), replace_window,
  delete-other-windows-internal (the root's top line or left column),
  resize_mini_window_apply (now ported:
  `resize-mini-window-internal' checks the staged root and mini heights
  against their current sum and applies them through window_resize_apply
  on the root, and grow_mini_window's commit gives the mini window the
  lines the root gave up), resize_frame_windows, and
  `window-resize-apply-total', which was a no-op: it now puts the root at
  column 0 below the bars (FRAME_TOP_MARGIN), applies every window's
  `window-new-total' and places the mini window below the root.
  `window-top-line', `window-left-column', `window-total-height' and
  `window-total-width' read them (with ROUND `floor' or `ceiling' the
  pixel size), so `window-edges' answers GNU's (0 1 80 13) and (0 13 80
  25) after a batch `split-window' whose pixel edges stay (0 0 80 12)
  and (0 12 80 24). (2) A window's margins are window state
  (left_margin_cols and right_margin_cols slots) instead of a table
  beside the windows, so a window configuration saves and restores them;
  set_window_margins is ported (a dimension out of 0..INT_MAX signals
  args-out-of-range, a non-integer wrong-type-argument; nil when the
  margins stay or would leave under two text columns, t otherwise; the
  window must be live), for `set-window-margins' and set-window-buffer's
  reset from `left-margin-width' and `right-margin-width'.
  compare_window_configurations also compares the character geometry and
  the margins. (3) A text frame keeps frame.h's menu_bar_lines,
  tab_bar_lines, text_lines and native size, and new_width, new_height
  and new_size_p for a size change_frame_size has queued.
  set_menu_bar_lines and set_tab_bar_lines take any fixnum (0 for other
  values; Emaxx clamped them to 1 and ignored them in batch) and queue
  the frame's native size; do_pending_window_change applies what is
  queued, after each wait in wait_reading_process_output (`sleep-for',
  `accept-process-output', batch `sit-for') and at the start of every
  redisplay; change_frame_size and adjust_frame_size are ported for a
  text frame (the minimum sizes from `frame-windows-min-size', or the
  frame's `min-width' and `min-height', at least the bars, a mode line,
  a text line and the echo area on a terminal; INHIBIT 1 for the
  set-frame-size family, 4 for set-window-configuration's fit, 5 for a
  terminal's size), and resize_frame_windows resizes the window tree
  below the bars through `window--resize-root-window' and
  `window--pixel-to-total', the mini window back to one line. So the
  batch frame's initial menu-bar line (make_initial_frame's
  set_menu_bar_lines (f, 1)) moves its root to pixel row 1 at the first
  wait, as in GNU; `frame-parameters' reports `height' and `width' from a
  queued size and the bar lines from the frame; `set-frame-height' on a
  terminal frame makes it smaller than the terminal, as GNU does, where
  the frontend used to resize it back to the terminal at the next
  redisplay. (4) The terminal frontend gives the frame the terminal's
  size when it starts (init_display_interactive's immediate
  change_frame_size) and queues a new size only when the terminal itself
  changes size (handle_window_change_signal), paints the frame's own
  size, clears the terminal before a full repaint (clear_garbaged_frames'
  redraw_frame) and puts the menu and tab bars on the frame's top-margin
  rows. tmux probes of GNU 30.2 (`menu-bar-mode' off and on with and
  without a redisplay between, `menu-bar-lines' 3, a split, deletions,
  side-by-side windows, `set-frame-height' 20 on a 34-row terminal) and
  batch probes (the queued menu bar line before and after `sleep-for',
  splits and deletions on both sides of it, `menu-bar-lines' 3, 'foo
  and `tab-bar-lines' 2, `set-frame-height', margins, the
  window-configuration sequence of round 80) give GNU's values and
  screens. Tests that wait or resize a frame now run on loadup's owners
  up to window.el (test_support's initialized_gnu_window_lisp_interpreter):
  adjust_frame_size calls window.el's functions, as every running GNU
  has them preloaded; GNU itself signals (void-function
  frame-windows-min-size) at such a wait when that function is unbound.
  Not covered: GNU's terminal frame matrices take their origin from the
  top margin at the last adjust_frame_glyphs, so after a `menu-bar-mode'
  toggle and window commands in one command GNU draws the window one row
  off (its menu bar over the first text row, the echo area twice) until
  the next resize; Emaxx draws from the windows' pixel edges. (The
  move_it_by_lines reseat skip found in round 80 is round 82's.)
  Local validation: the full gate, 3,215 passed across its ten groups
  with the documented local skips (eval_03, 326, includes the new
  bar-lines and margin tests), and 89 GNU test files (every ERC scenario
  file, edebug, kmacro, keyboard, minibuffer, simple, xdisp, scroll,
  xt-mouse, isearch, repeat, term, python, so-long and others; 1,785
  tests) as GNU.
- **Round 82: the display iterator's motion between screen lines on a
  live display (xdisp.c move_it_by_lines, indent.c Fvertical_motion),
  and indent.c's column scan.** (1) src/lisp/primitives/move_it.rs
  ports what the iterator does that the terminal's screen-line layouts
  do not show by themselves: reseat's handle_stop at a position (a
  replacing `display' spec first: an empty string is popped at once,
  past the text the property covers, while a string that shows, a
  stretch or an image stops there; then invisible text, skipped with the
  properties after it handled anew unless an ellipsis shows); where
  move_it_to (MOVE_TO_POS) stops for a position (at a screen line it
  begins there from the buffer, which move_it_to checks before
  handle_stop runs, else past the display replacement or invisible text
  over it); back_to_previous_visible_line_start;
  move_it_vertically_backward (IT, 0), whose start of a screen line that
  begins inside a multi-line display string is the buffer text after the
  string; and move_it_by_lines (forward to ZV when the lines run out and
  past a display string a line begins inside; backward -DVPOS buffer
  lines within -DVPOS full screen lines, reseated there, forward again
  when continuation lines took it too far, back to the reseated start
  when that moved nowhere, and the pass without the limit). Interactive
  `vertical-motion' is Fvertical_motion over them (start_display's reseat
  at point, the scan from the reseated start of point's line, the stop
  before a display string at point, the overshoot backtracking, one line
  less from a line start a display string covers, the forward cases), so
  `scroll-up' and `scroll-down' (window_scroll_line_based),
  `move-to-window-line' and `line-move-visual' follow it; `recenter' with
  a non-negative ARG is move_it_by_lines (IT, 0) then (IT, -ARG); and
  compute_window_start_on_continuation_line starts from the reseated
  line start, so a start reseated past a prefix stays. (recenter 1) and
  (vertical-motion -1) now land past a 9-character invisible or
  empty-display prefix at a line start, the round 80 finding, while
  (vertical-motion 0), (recenter 0), a negative ARG and a display string
  that shows keep the line start, and `scroll-down' from a line start a
  display string covers lands on the previous line's start. A position's
  screen line among a display line's row starts is the first beginning
  at it (a multi-line display string begins several rows at its
  position) else the last beginning before it (row_index_of, also for
  `posn-at-point''s walk). (2) indent.c scan_for_column is ported for
  `current-column', `move-to-column' and `indent-to': invisible text is
  skipped, though not text an ellipsis stands for, before the goal is
  tested (so (move-to-column 0) on a line beginning with invisible text
  goes past it); check_display_width counts a `display' string's
  `string-width' and a (space ...) spec's :width, :relative-width or
  :align-to over the text the property or its overlay covers (a float
  :width is ignored, as GNU's `||' leaves it); a display table's glyph
  vectors count their glyphs; `selective-display' t ends the line at a
  CR; and Fmove_to_column converts an overshooting tab from the scan's
  previous position. Emaxx counted the raw characters under a display
  string, so `previous-line' from text after a display string set the
  wrong goal column. buffer_display_table also wants DISP_TABLE_P's six
  extra slots now, for `string-width' and the mode line too. (3) An
  invisibility spec list holding t hides text whose `invisible' is t
  (invisibility_atom_eq never matched t). tmux probes of GNU 30.2 (40
  lines whose prefixes are in turn an empty display string, invisible
  and "X": `recenter' 0 to 5 and -1 to -3 from line ends, line starts and
  inside a prefix, `vertical-motion' -1 and 0, `scroll-down' twice from
  line 20, `set-window-start' with and without NOFORCE; 30 lines adding
  overlay prefixes, wrapping lines and a prefix displayed as "AB\nCD":
  `vertical-motion' -3 to 2 from line ends, starts and middles,
  `recenter' 1 to 3, a cons goal column, `previous-line',
  `scroll-down', `move-to-window-line' and motion from inside the
  two-line string) give GNU's values and screens; batch probes of the
  columns of tabs, controls with and without `ctl-arrow', wide
  characters, display strings, space specs, ellipses, a CR under
  `selective-display', display tables and `move-to-column' FORCE give
  GNU's. An instrumented copy of GNU's build (fprintf in Fvertical_motion
  and move_it_by_lines) settled the multi-line display string case:
  move_it_to stops at a line start that is its target before the string
  there is pushed.
  Local validation: the full gate, 3,219 passed across its ten groups
  with the documented local skips (eval_04, 255, includes the column
  test; primitives, 753, the two motion tests), and the same 89 GNU test
  files (1,785 tests) as GNU.
- **Round 83: character and byte positions in strings, the reader's
  inputs and `load''s read loop.** A survey of 515 GNU test files
  (survey82) put Emaxx at 0.71 of GNU's total but small files at 1.6 to
  2.4 times; profiles of them showed per-call costs that grow with the
  string or text (quadratic loops) and a reader doing extra passes.
  (1) fns.c string_char_to_byte and string_byte_to_char with GNU's
  one-entry cache (the string and the character and byte positions last
  converted) replace a scan from the start of a multibyte string for each
  character index: `aref', `substring', `compare-strings', Faset (which
  leaves the cache at the stored character, as GNU's does), `string-match'
  and `looking-at' on strings convert their START and the match registers
  through it, and the matcher's text-property lookups on a string read in
  place too. On a 100,001-character string holding one non-ASCII
  character, 100,001 `aref's took 11.8 s (GNU 0.037 s) and now 0.023 s;
  10,000 `substring's 2.4 s, now 0.014 s (GNU 0.019); 50,000 `string-match'
  calls with START 3.8 s, now 0.019 s (GNU 0.029). The cache entry also
  remembers whether the string's bytes are the matcher's stored form, so
  `string-match' no longer validates the whole string on every call.
  (2) `read-from-string' converts only START (read_internal_start), so
  reading a 2,782-form text form by form went from 11.0 s to 0.15 s (GNU
  0.095). (3) fns.c Fstring_search is ported: memmem over the strings'
  bytes from START's byte, the needle converted to the haystack's
  representation as GNU converts it (a unibyte non-ASCII needle to
  eight-bit characters, a raw-byte needle to unibyte), the out-of-range
  error `(args-out-of-range START)' as GNU signals it. (4) `read' from a
  buffer or marker reads the buffer's rope in place instead of copying the
  text from point to the end for every call. (5) A string's text
  properties are edited only over the spans the change meets, with their
  equal neighbours merged; the old code cloned every span's plist on each
  `put-text-property' (128 times GNU on a string of 2,000 intervals, now
  12). (6) `load' of a file is lread.c's readevalloop: each form is read
  with its symbols interned in the obarray current then, then evaluated
  before the next is read, so a read error comes after the forms before it
  ran and the separate whole-form interning walk is gone; the
  unescaped-character-literal warning is computed after the forms
  (Fload's load_warn_unescaped_character_literals). (7) The reader skips
  ASCII whitespace a byte at a time, comments with memchr, and checks for
  NO-BREAK SPACE only at its lead byte; a token is tried as a number only
  when it starts with a sign, digit or period (string_to_number), which
  also stops `infe+INF' reading as a float.
  Known differences kept: Emaxx merges a string's adjacent intervals with
  equal plists and drops a string's intervals whose plists are all empty
  (GNU's `object-intervals' shows (8 9 ...) (9 10 ...) and ((0 3 nil))).
  Local validation: the full gate, 3,223 passed across its ten groups
  with the documented local skips (eval_05, 361, includes four new tests
  of the position cache, `string-search', string property edits and
  buffer reads, whose values are GNU 30.2's); clippy `-D warnings',
  rustfmt and `git diff --check' pass.
- **Round 84: literal objects built as the reader reads them (lread.c
  read0), and an intern that looks a name up once.** (1) Records, closures
  (bytecode_from_rev_list), bool vectors, hash tables
  (hash_table_from_plist) and char-tables are now made when their syntax
  closes, from fields that are already the objects they read as, through a
  read context (`LispReadContext': the obarray current at the read, the
  interpreter that builds the object) that `read', `read-from-string',
  `load''s readevalloop and positioned reads share; the placeholder and
  the interpreter's walk over every read form that replaced placeholders
  afterwards remain only for a form that defines a `#N=' label, which
  can be referred to inside a literal before the literal is complete (the
  case read0 itself defers to its substitution pass). Loading 13 `.elc'
  files spent 62M instructions in that walk; now 5.6M. (2) An empty
  `#s()' is invalid syntax "#s" (RE_record), not an error of Emaxx's
  wording. (3) The reader reads a text or encoded input's bytes from the
  slice directly instead of through the input enum on every byte. (4)
  `intern' in the standard obarray looks the name up once: a hit returns
  the symbol found, a miss interns it then (the old path looked up, then
  interned, which looked up again). The 13-file load went from 541.6M
  instructions to 479.8M (GNU 390.9M, of which Emaxx's startup is 239M
  and GNU's 173M); the reads inside it from 146.3M to 102.7M including the
  objects now built there. The remaining load-specific gap is the
  decoding pass over a `.elc''s bytes and the lazy-docstring rewriting
  (35M together), which GNU does not have: it reads the file's bytes with
  readchar and handles `#@' and `#$' in read0 (next round). Local
  validation: the full gate, 3,223 passed across its ten groups with the
  documented local skips, and the new eval_05 test of the literals read
  builds (its values are GNU 30.2's) on its own; clippy `-D warnings',
  rustfmt and `git diff --check' pass.
- **Round 85: `load' reads a compiled file's bytes directly, as lread.c
  readevalloop on Qget_file_char does.** Emaxx decoded each `.elc' into
  a Rust string first (decode_utf8_emacs_source, re-spelling raw bytes and
  characters past Unicode as `\x' escapes) and then rewrote it twice
  (preprocess_lazy_doc_source cut the `#@' doc strings out,
  rewrite_lazy_doc_refs replaced `#$' and (#$ . N) with the file name
  as a literal); the three passes and their 426 lines are gone. (1) A file
  named `.elc' or starting with `;ELC' is read from its bytes, each
  character as readchar's read_multibyte takes it: a lead byte and its
  trailing codes, else the lead byte as a raw byte (BYTE8_TO_CHAR), so a
  string of raw byte-code octets reads unibyte and one of characters past
  Unicode multibyte, as GNU's do. `utf-8-emacs' source that is not UTF-8
  reads the same way. (2) skip_lazy_string: from a file `#@N' skips N
  bytes after the digits, the character just read among them (not up to
  the next ^_, which a doc string can hold), with `load-force-doc-strings'
  saving them as lread.c's two saved_strings, freed when the load ends;
  `#@00' ends the input; elsewhere (a buffer, a string) the skip still
  runs to the ^_. (3) `#$' reads as the value of `load-file-name' in any
  read (it read as the symbol `#$' outside a load), so a compiled
  function's (#$ . N) doc reference holds the load's file name object.
  (4) read0's RE_list_dot hack: with `load-force-doc-strings' a (#$ . N)
  whose car is `load-file-name' reads as get_lazy_string's unibyte
  string, from the saved strings or else the file (doc.c get_doc_string
  (VAL, true)); the old rewrite made it a multibyte literal.
  bytecode_from_rev_list's reading of lazily loaded byte-code from a file
  (the `byte-compile-dynamic' of Emacs before 30) is ported too: a
  (FILE . N) code slot is read at once, and code with nil constants is
  the printed (BYTECODE . CONSTANTS) read from it. (5) Fload of a compiled
  file binds no `read-symbol-shorthands' and scans no file-local
  variables; its `lexical-binding' comes from the head lines. (6) A
  function stream's reader (`read' with a function) builds literal
  objects as it reads too: the stream is the read's context. (7) The
  reader makes a vector literal a vector directly, not a list headed by
  `vector-literal' that `Value::list' turned into one. Loading the 13
  `.elc' files took 479.8M instructions and now 425.0M (GNU 390.9M; the
  loads themselves, without startup, 186M against GNU's 218M), and their
  wall time is within noise of GNU's (0.033 to 0.051 s against 0.037 to
  0.040). A probe writing compiled files with lazy doc strings, `#$',
  `#@00', raw bytes, a character past Unicode and a two-byte symbol
  character, loaded with and without `load-force-doc-strings', and `#$'
  and `#@' read from a string and a buffer, gives GNU 30.2's values (an
  eval_05 test holds them).
  Finding recorded, not fixed: `Value::list' still turns any list whose
  first element is the symbol `vector-literal' into a vector, and
  `Value::to_vec' of a vector still prepends that symbol; 63 sites rely on
  the pair, and each `Value::list' call compares a leading symbol's name.
  A list a Lisp program builds with that symbol first through a primitive
  that uses `Value::list' would come back a vector.
  Local validation: the full gate, 3,225 passed across its ten groups
  with the documented local skips (eval_05, 363, includes the new test);
  clippy `-D warnings', rustfmt and `git diff --check' pass. survey84
  (round 84's build): 514 of the 515 files give GNU's `Ran' line and exit
  code; network-stream-tests differs only in GNU's own skip count, which
  moves between runs (5, 4 or 3 skipped) while Emaxx's stays 3.
- **Round 86: buffer position conversions from the nearest known
  position, as marker.c buf_charpos_to_bytepos does.** A rope's char to
  byte conversion walked its tree every time (350 to 700 instructions);
  GNU scans from the closest of PT, GPT, BEGV, ZV, its cached last
  conversion and the markers. (1) BufferText keeps the chunk of the rope
  its last walk reached, with the char and byte offsets it starts at, and
  the last four conversions (shifted by an insertion or deletion before
  them, dropped by one over them): a conversion that was made since the
  last edit before it is answered at once, and one inside the chunk is
  scanned from the nearest known position there, so walks happen only
  when a lookup leaves the chunk. The ends of the text, an ASCII text and
  position 1 need no lookup at all. The chunk's characters are counted
  only when a later lookup by character needs them (a deletion's single
  lookup would otherwise count a whole chunk). The view is forgotten by
  every edit, and a cloned rope shares its leaves, so the chunk it
  addresses stays as it was. (2) The regexp matcher's snapshot takes
  point's byte (PT_BYTE) and the accessible region's bytes from those
  conversions, and maps point, BEGV and ZV back and forth without walking.
  (3) `delete-region' reads the deleted text and its two byte offsets in
  one walk instead of three (text, then each end's marker byte position).
  (4) The syntax scanner's CHAR_TO_BYTE answers the text's ends without a
  walk. (5) `undo-auto-amalgamate' (called by `delete-char' and
  `self-insert-command') and the `field' property name are looked up by
  their symbols, not by interning their names on every call. Per call, on
  a 20,000-line multibyte buffer (base subtracted): `delete-char' in the
  middle 8,980 instructions to 6,212 (GNU 2,686); `looking-at' at
  successive positions 5,780 to 3,285 (GNU 1,197); `forward-word' 9,388
  to 7,959 (GNU 2,864); `forward-sexp' 7,422 to 6,155 (GNU 3,678);
  `re-search-forward' 7,033 to 6,681 (GNU 3,400); `search-forward' 7,251
  to 6,899 (GNU 3,997); `buffer-substring' 5,794 (GNU 5,022) and
  `char-after' 373 (GNU 301) unchanged. What remains of these gaps:
  ropey's own costs (each public call checks its index against a length
  summed over the root's children, `Rope::len_bytes' and `len_chars' do
  the same and rescan a one-leaf rope, `RopeSlice' creation sums them
  five times, and a one-character removal costs about 1,650 instructions
  of tree work against GNU's gap move); scanners that walk their own
  snapshot's rope instead of sharing the buffer's conversions (the syntax
  scanner, the matcher's reader, the non-regexp search); and per-call
  costs outside the text (`forward-word''s field and overlay lookups,
  char-table reads and variable lookups are most of its gap). A probe of
  `looking-at', `re-search-forward' and `delete-char' at, before and after
  point, at narrowing bounds and after edits gives GNU 30.2's values (an
  eval_05 test holds them), and a buffer test checks conversions against
  a model across 400 random insertions and deletions. Local validation:
  the full gate, 3,227 passed across its ten groups with the documented
  local skips (eval_05, 364, and the buffer tests include the new ones);
  clippy `-D warnings', rustfmt and `git diff --check' pass.
- **Round 87: `#N=' labels as lread.c read0 reads them.** Emaxx read a
  label as a wrapper form (`#N=' around its object, `#N#' as a reference
  form), and once a top-level read had a label every later literal
  (record, closure, hash table, char-table) stayed a placeholder too; the
  read's caller then walked the whole result up to six times (finding the
  forms, resolving labels, then records, hash tables and char-tables),
  each walk keeping its visited objects in SipHash sets. Native-compiled
  units' static data is printed with `print-circle', so every `.eln' load
  paid for this over its whole constant vector. Now `#N=' makes read0's
  placeholder cons, in read_objects_map, before the object is read, and
  `#N#' reads as whatever the map holds (an unknown number is invalid
  syntax). When the object is complete (RE_numbered): a cons takes over
  the placeholder (its car and cdr copied in, so the `#N#' inside it are
  already right); any other object is recorded in read_objects_completed
  and put everywhere the placeholder is inside it by
  substitute_object_recurse (conses, vectors, closures, records,
  char-tables, string property lists and, for a read without a context,
  the open literal forms; GNU 30's walk leaves a hash table's entries
  alone, as its hash tables have no Lisp slots in their header). The maps
  are emptied after each top-level read, as readevalloop and
  read_internal_start do, so a later form cannot name an earlier label.
  Literals are built as their syntax closes whenever the read has a
  context. The wrapper forms, the materializer behind them (`#N=' handling
  in `materialize_read_object_literals') and their cases in the copier,
  the collector, the interning walk and `json-serialize' are gone. Two
  reads that signalled errors now give GNU's values:
  `#1=#s(hash-table data (k #1#))' is a table whose value is the
  placeholder `(nil)', and `#1=#2=(a #1# #2#)' is
  `#1=(a . #2=(#1# (a . #2#)))'. Loading `json' (and with it the byte
  compiler's native unit) took 198.3M instructions and now 165.5M (GNU
  109.6M); a test file's setup in the frozen runner (startup, `ert', the
  runner, the file) 802.6M and now 767.2M (GNU 518.3M). What remains of
  that setup gap, by stage: startup 222M against 152M (the image loader
  deserializes object by object where GNU maps its dump); loading `ert'
  281M against 235M, though GNU spends 105M of its 235M in two
  collections and Emaxx 25M in one, so outside collection the reader is
  97M against GNU's 57M, the matcher 34M against 18M and MD5 of the
  source files for the native-code lookup 16M against 8M (the `md5'
  crate is 2.5 times gnulib's cost a byte; both hash the same files).
  A probe reading labels in conses, vectors, records, closures, hash
  tables and string properties, from a string, a buffer and a loaded
  file, gives GNU 30.2's values (an eval_05 test holds them). Local
  validation: the full gate, 3,228 passed across its ten groups with the
  documented local skips (eval_05, 365, includes the new test; the reader
  tests check the placeholder, the substitution and the per-read maps);
  clippy `-D warnings', rustfmt and `git diff --check' pass.
- **Round 88: the reader's common cases read in place.** Each character
  the reader took from an encoded string or an `.elc' file's bytes was
  decoded twice (once to look, once to step past it), each atom was
  built a character at a time into a fresh `String', whitespace was
  skipped through the general peek, a string literal's characters were
  pushed one at a time, and a symbol's recorded position was found by
  decoding every character since the last one. Now an atom of ASCII
  without escapes (nearly every symbol and number in Lisp source) is
  scanned in place up to read0's terminators (whitespace, `()[]";' and,
  after its first character, `#'`,') and goes straight to the number
  parse and the obarray; a run of ASCII whitespace and a run of string
  text other than `"' and `\' are taken from the bytes at once; an
  encoded character is decoded once; the shorthand rewrite is skipped
  when the file has no shorthands; and positions in a well-formed string
  count its lead bytes. make_string's classification of a native unit's
  static data (alloc.c: multibyte only when every byte is part of a
  valid character) steps over ASCII runs instead of decoding and
  re-encoding each byte. Loading `json' took 165.5M instructions and now
  125.3M (GNU 109.6M, of which about 53M is its collector against
  Emaxx's 33M); loading `ert' 281.2M and now 230.2M (GNU 235.1M); a test
  file's setup in the frozen runner 767.2M and now 710.4M (GNU 518.3M).
  Outside collection, Emaxx's reader in the `ert' load now costs less
  than GNU's readchar/read0. What remains of the `ert' load gap: the
  matcher, 28M against GNU's 18M, where each `re_search' call scans the
  256-entry fastmap for a single lead byte (about 1,100 instructions a
  call, 11.5K calls); MD5 of the source files, 15.7M against 7.6M (the
  `md5' crate copies each byte into its block buffer); and interning,
  13.2M against GNU's 1.3M `oblookup'. A probe of tokens next to every
  terminator, escapes, numbers, NO-BREAK SPACE and other non-ASCII
  characters, unterminated atoms and strings, string escapes, positions
  after comments and multibyte text, unibyte and encoded sources and a
  function stream gives GNU 30.2's values (an eval_05 test holds them).
  Local validation: the full gate, 3,229 passed across its ten
  groups with the documented local skips (eval_05, 366, includes
  the new test); clippy `-D warnings', rustfmt and `git diff --check'
  pass.
- **Round 89: gnulib's MD5, the fastmap's lone lead byte, one obarray
  probe.** Three costs of every file `load' finds or reads. (1) The `md5'
  crate copied each input byte into its block buffer and rebuilt the
  words from it, about twice gnulib's cost a byte; the native-code lookup
  hashes each candidate source file (comp-el-to-eln-filename), so loading
  `ert' spent 15.7M instructions there against GNU's 7.6M. Now
  src/lisp/md5.rs is gnulib md5.c (md5_init_ctx, md5_process_bytes with
  whole blocks processed where they lie, md5_process_block,
  md5_finish_ctx, md5_buffer) and serves `md5', `secure-hash' and the
  native compiler's file and ABI hashes; the crate is gone. (2) Each
  `re_search' call found out whether the fastmap admits exactly one byte
  (for its memchr skip) by scanning all 256 entries, about 1,100
  instructions a call; the count is now taken once, when
  re_compile_fastmap fills the fastmap. (3) Reading a symbol in the
  initial obarray hashed its name twice (a membership test, then the
  intern) and scanned it twice for the internal markers; now one scan
  and one probe return the symbol, as oblookup does. Loading `ert' took
  230.2M instructions and now 203.6M (GNU 235.1M); loading `json'
  125.3M and now 113.6M (GNU 109.6M); a test file's setup in the frozen
  runner 710.4M and now 673.3M (GNU 518.3M); startup 221.3M and now
  219.3M (GNU 151.6M). The setup gap is now mostly after `ert' is
  loaded: loading the runner and running the file cost 250.4M against
  GNU's 131.6M, where 41K `string-match' calls cost about 1,950
  instructions each with everything they call against GNU's 1,060: a
  search's `gl_state' setup for `parse-sexp-lookup-properties' costs 361
  against GNU's 113 and its pooled matcher state 236, and each call's
  string text view 106 and case-fold variable reads 110. A probe of `md5' and
  `secure-hash' over strings of every block boundary, multibyte and
  unibyte strings and coding systems, substrings, buffers and regions,
  and of `string-match' and buffer searches whose patterns one byte,
  several or none can start (with and without case folding, multibyte
  and unibyte text) gives GNU 30.2's values; the md5 module's test
  checks RFC 1321's suite and every split of a message. Local
  validation: the full gate, 3,230 passed across its ten groups
  with the documented local skips; clippy `-D warnings', rustfmt and
  `git diff --check' pass.

- **Round 90: search.c's searchbufs, a search's matcher state and
  syntax setup as GNU's.** A test file's setup in the frozen runner
  makes 41K `string-match' calls from byte code, each about 1,950
  instructions with everything it calls against GNU's 1,060. Where the excess was: (1) the pattern cache was
  a vector, the pattern found removed from it and inserted again at the
  front on release; it is now searchbufs: REGEXP_CACHE_SIZE cells
  linked most recently used first, the cell found or compiled into
  moved to the front and busy while its search runs (freeze_pattern),
  the first empty cell or else the least recently used one that is not
  busy taken for a new pattern, `Too much matching reentrancy' when
  every cell is busy, a failed compile leaving its cell empty
  (compile_pattern_1), clear_regexp_cache skipping busy cells; the cell
  is released however the search ends, as the unwind-protect does; (2)
  each search took its matcher arrays from a pool vector and cleared
  all four register arrays; now the last finished search's state is
  kept whole and reused, its best registers written before they are
  read, as re_match_2_internal's are (13.1M to 3.4M); (3) a search in a
  string set up gl_state for `parse-sexp-lookup-properties' whatever
  the string held (361 against GNU's 113 a call); now a string without
  intervals leaves the buffer's table, since update_syntax_table's
  interval_of finds no `syntax-table' property there and returns, and
  only a string with text properties gets gl_state (20.2M to 2.3M); (4)
  a unibyte string's view checks its bytes for ASCII directly instead
  of through the conversion cache.
  `string-match' now costs about 1,410 instructions a call (re_search
  478 against GNU's 579; compile_pattern and the release 293 against
  GNU's 289 with its unwind records); what remains above GNU is the
  `case-fold-search' read (82: a LOCALIZED variable's loaded binding
  through the general lookup, where C reads Vcase_fold_search), the
  string view (74), the canon table lookup (56) and the match data
  copy (59). A test file's setup took 673.3M instructions and now 644.7M
  (GNU 518.3M); loading `ert' 203.6M and now 198.8M (GNU 235.1M);
  loading `json' 113.6M and now 111.9M (GNU 109.6M). Of the setup gap
  now: startup 219.1M against GNU's 151.6M, where the image loader
  costs 176.5M against pdumper_load's 53.4M (GNU's startup also
  collects once, 52.6M, which Emaxx's does not); collections overall
  70M in Emaxx against GNU's 163.8M, so outside collection Emaxx's
  setup is 574.7M against GNU's 354.5M. A probe of more than 20
  patterns in rotation, many-group patterns followed by one-group
  ones, POSIX and backtracking matches, invalid patterns, case folding
  (ASCII, non-ASCII, unibyte), `search-spaces-regexp', syntax classes
  across `modify-syntax-entry' and `with-syntax-table', strings with
  `syntax-table' and other text properties with and without
  `parse-sexp-lookup-properties', buffer searches over properties,
  searches nested through `replace-regexp-in-string' and a
  `syntax-propertize-function', 30 nested `string-match' calls and the
  `-p' variants gives GNU 30.2's values, as do the earlier regexp,
  reader and md5 probes. Local validation: the full gate, 3,230
  passed across its ten groups with the documented local skips; clippy
  `-D warnings', rustfmt and `git diff --check' pass.

- **Round 91: fns.c internal_equal, one walk for `equal' and
  `equal-including-properties'.** Emaxx had three walks: a non-signaling
  one for Rust callers that kept a SipHash set of every cons pair it
  passed (an insertion and allocation a cons), a signaling one for
  `equal' that handed every non-cons leaf back to the first, and a third
  for `equal-including-properties' that compared vectors' and records'
  elements without their strings' properties. Against GNU 30.2:
  250-deep vectors compared t where GNU signals "Stack overflow in
  equal", `(equal-including-properties [#("a" 0 1 (p 1))] ["a"])' was t,
  and a cdr cycle's `circular-list' carried the original list where GNU
  gives the tail at which FOR_EACH_TAIL's tortoise is met. Now
  src/lisp/primitives/values.rs has internal_equal with fns.c's
  equal_kind (EQUAL_PLAIN, EQUAL_INCLUDING_PROPERTIES): past depth 10 a
  table of the objects O2 each cons or vectorlike O1 was compared with
  (made the first time), a pair met again counting as equal; past 200
  `Stack overflow in equal'; the cdrs walked by lisp.h's FOR_EACH_TAIL
  (struct for_each_tail_internal: Q, N and MAX, the tortoise moved to
  the tail after MAX steps and MAX doubled), the shared-tail EQ exit,
  and `circular-list' with the tail in hand; the last cdr and an
  overlay's plist compared one level down by going round again
  (tail_recurse); symbols with position stripped while
  `symbols-with-pos-enabled' is set; strings by characters, bytes and
  data, and with EQUAL_INCLUDING_PROPERTIES by compare_string_intervals
  at every level, property values by `equal'; bignums, markers, bool
  vectors, tree-sitter nodes and symbols with position by value;
  vectors, closures, char-tables, sub-char-tables and records by their
  slots; other pseudovectors only when eq. `equal' signals wherever GNU's
  Fequal does: `values_equal' returns a Result and its 34 callers
  propagate it (member, assoc, rassoc, delete, remove, hash tables'
  equal test and the runtime's equal-table lookups and puts, purecopy's
  hash-consing, `find-buffer', `featurep' subfeatures, keymaps' where-is
  and describe paths, composition ids, face vectors, font matching,
  autoload checks, the load nesting check), where the non-signaling walk
  answered t for isomorphic cycles. Loading `ert' took 198.8M
  instructions and now 194.5M; a test file's setup in the frozen runner
  644.7M and now 640.3M (GNU 518.3M). A probe of cdr cycles of ten
  shapes (the reported tail), cycles against proper lists both ways,
  `member', `assoc', `rassoc', `delete' and `remove' on cycles, car and
  vector nesting at depths 9 to 12 and 150 to 250, a 100,000-cell cdr
  chain, car-, vector- and record-circular structures, overlays with
  each other on their plists, live and deleted, properties inside
  strings, vectors, lists, cdrs, records and alists under both
  functions, property values that differ only in their own properties
  or are circular, symbols with position in and out of lists and
  vectors, floats, bignums, bool vectors, char-tables, hash tables,
  subrs, buffers, markers, closures and byte code, `equal' hash tables
  with circular and 250-deep keys, `sxhash-equal' of a cycle,
  `delete-dups' and `seq-uniq' gives GNU 30.2's values, as do the
  earlier `equal', regexp, reader and md5 probes; an eval_05 test holds
  the cycle, depth and property cases. Local validation: the full gate,
  3,230 passed across its ten groups with the documented local skips,
  and eval_05 again with the new test (367); clippy `-D warnings',
  rustfmt and `git diff --check' pass.

- **Round 92: the empty pattern's cache comparison, the matcher's
  registers in a held buffer, no group registers without groups.** Both
  frozen runners call `(ert-select-tests t t)' twice for every test file,
  once when the runner loads and once when it runs, and each is an
  `apropos-internal' over all 19.8K symbols: `string-match' of `""'
  against each name and the predicate. In wall time (best of 30 inside
  one process) a call took 10.0 ms against GNU's 4.4 ms although its
  instruction count was only 1.4 times GNU's; `string-match ""' over the
  names took 8.5 ms against GNU's 3.2 while `string-match "a"', which
  scans every name, took 5.3 against 4.3. Timing string_match_1's stages
  with the time-stamp counter put the excess in compile_pattern's cache
  walk, about 570 cycles a call for `""' against 100 for `"x*"' or
  `"a"': the empty pattern's saved bytes are an empty vector, whose
  pointer is a placeholder (0x1) rather than an address, Rust's slice
  equality hands both pointers to memcmp even at length 0, and on this
  Xeon such a call costs some 200 to 270 cycles where one through real
  addresses costs 5 (a standalone test shows it); each lookup compared
  twice. Now (1) the cache compares the lengths first and two empty
  patterns are equal without memcmp, as Fstring_equal's SBYTES test
  makes them; (2) re_search_2 and re_match_2 fill the registers they are
  given (GNU's REGS argument) and answer the match's start or end:
  `string-match' and `looking-at' pass a register buffer the interpreter
  keeps between searches when the match data may change (search_regs
  before its conversion to positions) and none otherwise, and a buffer
  search passes it either way (search_regs_1 when the match data stays),
  where each match allocated a register vector and freed it; (3)
  re_match_2_internal sets the group registers unset only when the
  pattern has groups, as GNU does not "bother to initialize all the
  register variables", where each attempt filled both arrays through two
  memset calls; (4) `string-match' makes last_thing_searched t whenever
  the match data may change, matched or not, so after a failed
  `string-match' an earlier buffer search's match data reads as
  integers, as in GNU (Emaxx kept the markers). `string-match ""' over
  the names now takes 4.5 ms (GNU 3.2) and `apropos-internal' 6.2 ms
  (GNU 4.4); in instructions, three `apropos-internal' calls 181.0M and
  now 174.4M (GNU 128.4M), a test file's setup in the frozen runner
  640.3M and now 634.8M (GNU 518.3M), and its test phase 99.8M and now
  97.4M (GNU 63.9M). An LD_PRELOAD count of zero-length memcmp and bcmp
  calls through a placeholder pointer over a trivial test file and six
  real ones (fns, subr, subr-x, search, files and simple tests) found
  the rest rare: about 600 in completion's obarray matching, 140 in one
  other primitive and four elsewhere. A probe of the match data after successful and failed
  `string-match', `string-match-p', `looking-at', `looking-at-p',
  `looking-back', `posix-string-match', `search-forward' and repeated
  and backward regexp searches, with and without groups and
  `inhibit-changing-match-data', of the empty pattern from the start
  and from inside a string, on empty, unibyte and multibyte strings,
  folded and not, of
  `save-match-data', invalid patterns, `replace-regexp-in-string' and
  `split-string' with `""' gives GNU 30.2's values; an eval_05 test
  holds the failed-search and empty-pattern cases. Local validation:
  the full gate, 3,231 passed across its ten groups with the documented
  local skips, and eval_05 again with the new test (368); clippy `-D
  warnings', rustfmt and `git diff --check' pass.

- **Round 93: byte-code frames and handlers on thread-wide lists.** Each
  byte-code activation entered from Rust (a `funcall' from a primitive,
  `mapatoms', `mapcar', `sort's predicate, a hook) kept its suspended
  frames and its catch and condition-case handlers in vectors of its
  own, taken from a thread-local spare list on the first push and given
  back at its end: in `apropos-internal's predicate calls the spare
  list's borrow, pop and push cost about 95 instructions an activation.
  Now the thread holds one list of suspended frames and one of handlers
  (bytecode.c keeps its frames on the thread's byte-code stack and
  eval.c its handlers on the thread's handlerlist), each activation's
  entries lying above the lengths it began at, to which its end returns
  them however it ends; a thread switch exchanges both with the
  byte-code stack, and the spare lists are gone. Three
  `apropos-internal' calls (both frozen runners make two for every test
  file through `ert-select-tests') took 174.4M instructions and now
  166.8M (GNU 128.4M); a test file's setup in the frozen runner 634.8M
  and now 630.0M (GNU 518.3M); the locked workloads per unit:
  native-to-bytecode 3,943 and now 3,914, upstream-undo 1,573.9K and now
  1,563.6K, bytecode-calls 435.5 and now 434.0, bytecode-to-native 345.6
  and now 343.2. A probe of byte-code `condition-case' and `catch'
  around `mapcar', `mapc', `apply' and `sort' whose byte-code callees
  signal, throw, catch for themselves, hold non-matching handlers or
  `unwind-protect', of `handler-bind', a 30-deep recursion through
  `apply' with a catch at every level, and of two Lisp threads yielding
  inside their handlers gives GNU 30.2's values. Local validation: the
  full gate, 3,232 passed across its ten groups with the documented
  local skips; clippy `-D warnings', rustfmt and `git diff --check'
  pass.
- **Round 94: the obarray's hash and the image loader's walks.** The
  name table behind the obarray hashed each name by FNV, a multiply a
  byte; it now takes fns.c's `hash_string', oblookup's hash (the length,
  then a word at a time -- at most eight steps across a long name and
  its last word -- or a short name's bytes as one word, each folded in
  by lisp.h's `sxhash_combine'), spread over the word for the table's
  probe filter. `intern_with_lisp_name' scans a name once for both the
  uninterned and the private-obarray markers, where it searched for each
  in turn. The loader's pass over the object types also lists the
  objects by the phase that builds them (the conses, the objects of
  their own bytes, the rest), so each phase walks its own objects where
  all three walked all 273.8K; phase 3 relocates the 177.6K conses' cars
  and cdrs in one ascending walk that merges the relocations with the
  fields, as pdumper.c applies its sorted relocations; the native subr
  and unit records' relocation checks and the native functions' name
  lookups search on from where the last one stopped instead of bisecting
  the whole table; and a vector's placeholder is written in place
  (alloc.c's `make_vector') rather than built as a host vector and
  moved. Startup (`--batch -l subr-x') took 225.9M instructions and now
  212.9M (GNU 156.4M, of which 52.6M is the collection GNU runs during
  startup); `Loader::load' 137.2M and now 124.5M, where GNU's whole
  `pdumper_load' is 53.4M, 24.2M of it reopening the native units and
  resolving their subrs (ours 24.4M); loading ert 408.4M and now 393.7M;
  a frozen-run test file's setup 630.0M and now 614.8M (GNU 518.3M). In
  wall time, best of 40 interleaved runs, startup took 71.5 ms and now
  68.4 ms (GNU 60.5 ms). What remains of the startup gap is the
  loader's design: GNU maps the image and relocates its words in place,
  where this loader builds every cons, string, symbol and vector anew.
  `hash_string' is tested against the values GNU's own function gives;
  an interning probe (names of every length to 89 differing in one byte
  at each position, multibyte, unibyte and empty names, `obarray-make',
  `unintern', `mapatoms' counts) gives GNU 30.2's output byte for byte;
  every symbol's value, function and plist after startup, printed with
  `print-circle', is round 93's but for timestamps, paths, the consing
  counters the image carries from its build, and the entry order of
  `comp-subr-arities-h', which loadup fills through `mapatoms' and so
  follows the name table's order (unspecified in GNU, and not GNU's
  before this round either). Local validation: the full gate, 3,233
  passed across its ten groups with the documented local skips; clippy
  `-D warnings', rustfmt and `git diff --check' pass.
- **Round 95: the image's conses mapped in place.** The frozen run's
  largest gap is a test file's setup, 1.39 times GNU's in CI (98.5 s
  against 70.6 s over 519 files, where the tests themselves take 1.03
  times), and most of a setup's excess instructions were the image
  loader's, which built every object anew where pdumper.c maps the dump
  and relocates its words where they lie. The conses, 177.6K of the
  image's 273.8K objects, now take GNU's way. The dumper writes them as
  this allocator's cons blocks (each block's metadata and 1,344 cells,
  the runtime form, as GNU's dump holds its objects in theirs) at a
  block-aligned offset at the end of the hot section, which a new
  header locator gives with their count (the header is 108 bytes); a
  cons's offset is provisional while the other objects are dumped and
  every remembered one is translated once the blocks are placed. A
  cell's immediates are written as the heap's words, its references as
  relocations. The loader maps the image at a block's alignment
  (address space reserved and the file mapped over its first aligned
  address), makes the blocks image blocks where they lie (their bits
  set as an allocation sets them, nothing counted or allocated; the
  collector never gives their storage back, as GNU never frees its
  dump), and applies the 289.5K relocations that fall in them in order,
  pdumper.c's RELOC_DUMP_TO_DUMP_LV for a cons being the mapped image's
  address plus the word's offset. The conses are no longer among the
  object starts, so the loader neither types nor numbers them.
  Startup (`--batch -l subr-x') took 212.9M instructions and now 193.4M;
  `Loader::load' 124.5M and now 106.3M; loading ert 393.7M and now
  374.3M; a frozen-run test file's setup 614.8M and now 595.5M (GNU
  518.3M). Wall time, best of interleaved runs: startup 66.1 ms at round
  93 and now 61.5 ms (GNU 58.1 ms); the frozen runner's setup of
  xml-tests.el with CI's load path 137.2 ms at round 93 and now 124.2
  ms (GNU 112.7 ms). Of CI's setup ratio the gate profile (debug
  assertions, overflow checks, thin LTO) accounts for about a tenth:
  locally the gate build's setup is 1.25 times GNU's where the release
  build's is 1.21. Every symbol's value, function and plist after
  startup is round 94's but for timestamps, paths and the image's
  consing counters; a probe that mutates image conses and collects,
  drops an image list's tail while holding it, keys a weak table by
  image conses, holds 20,000 heap conses pointing at one and reaches one
  only from the stack across collections gives GNU 30.2's output byte
  for byte, as does round 94's interning probe. Local validation: the
  full gate, 3,233 passed across its ten groups with the documented
  local skips, and the new test with the other 29 pdumper tests; clippy
  `-D warnings', rustfmt and `git diff --check' pass.
- **Round 96: lread.c's obarray, keyed by each symbol's current name.**
  The obarray was a hash set of the host text each symbol cell owned
  beside its Lisp name, and a private obarray a record holding a list of
  its symbols. Both are now GNU 30's `struct Lisp_Obarray': 2^size_bits
  buckets, each empty or the first of the symbols whose names
  obarray_index (fns.c's hash_string reduced by knuth_hash) files there,
  the rest chained newest first through a new `next' field of the symbol
  cell (Lisp_Symbol.u.s.next); oblookup compares each chained symbol's
  current name, bytes and characters, intern_sym files a new symbol and
  doubles the buckets past one symbol a bucket (grow_obarray filing each
  again by its current name), Funintern unlinks by the symbol's own name,
  Fobarray_clear resets to the default eight buckets, `obarray-make' sizes
  by elogb, a legacy vector gets make_obarray (0), and the collector
  follows `next' as mark_object does. The initial obarray starts at 2^15
  buckets (init_obarray_once); a private obarray's buckets are its
  record's slots, so the image carries them and each of its symbols'
  `next'. GNU-visible results that differed and now match GNU 30.2 byte
  for byte: `internal--obarray-buckets' (it gave one symbol a bucket),
  `mapatoms' order over a private obarray, the bucket count of
  `obarray-make', and the lookups after a symbol's name string is changed
  in place (`(aset (symbol-name 'string-trim-left) 0 ?x)' left the symbol
  found by its old name; GNU finds it by neither name, and `intern' of
  the old name makes another symbol). The host text a symbol keeps for
  this implementation's own name-keyed code is now two words, a box the
  cell owns or text of the permanent image, and the dumper writes each
  symbol's text after its record, so a loaded symbol keeps the image's
  text where it lies instead of a copy decoded from its name; the loader
  files each initial-obarray symbol by its Lisp name. A name of
  printable ASCII (nearly all) is recognised eight bytes a step, which
  stands for both the marker scan and the character count, and
  hash_string and the name comparison read short names as overlapping
  words. Startup (`--batch --eval nil') took 186.2M instructions and now
  179.4M (GNU 166.1M, 52.6M of it collecting); `Loader::load' 106.3M and
  now 99.3M, its symbols 15.1M and now 7.3M; loading `ert' 372.6M and
  now 367.4M; the frozen runner's setup of xml-tests.el 505.4M and now
  501.7M (GNU 469.5M; outside collection 434.5M and now 429.5M against
  GNU's 306.4M). The collector spends about 1.3M more a setup walking the
  32K buckets, as GNU's does. Every symbol's value, function and plist
  after startup is round 95's but for timestamps, paths and consing
  counters, and `comp-subr-arities-h', which a `mapatoms' fills, now
  begins in GNU's order. Not done: the reader names a symbol with a
  unibyte string where GNU's read_symbol gives a name read from a file or
  a multibyte source a multibyte one (GNU 30.2 has 5,884 such names
  after startup, Emaxx 1). Local validation: the full gate, 3,239
  passed across its ten groups with the documented local skips (one
  completion test expected Emaxx's former insertion order of a private
  obarray and now expects GNU's, `("abba" "abc")'); clippy `-D
  warnings', rustfmt and `git diff --check' pass.
- **Round 97: the image's strings mapped in place.** The image's 56,992
  strings (but the two empty singletons) take the conses' way: the
  dumper writes each as the next cell of this allocator's string blocks
  (85 cells of 48 bytes in a 4,096-byte block, lisp.h's four words a
  cell, the properties and data words the image offsets of what they
  name) at a block-aligned offset after the cons blocks, given with their
  count by a second header locator (the header is now 116 bytes); a
  string's offset is provisional while the other objects are dumped,
  above the conses' provisional range, and every remembered one is
  translated when the blocks are placed. The loader makes each cell a
  string of an image block where it lies (its data the sdata entry the
  image holds after it, checked to be one inside the image and to hold
  the cell's characters; its mark the current epoch; nothing allocated
  or counted, as for the conses), returns the 19 that carry text
  properties to have their records read, and registers the 671 blocks as
  the image's own bytes, which the collector never gives back. A string
  reference is the mapped image's address plus the word's offset
  (RELOC_DUMP_TO_DUMP_LV of a string), so the strings are no longer
  among the object starts, and their data words need no relocation
  entries (602.0K hot relocations where there were 654.5K). Adopting the
  strings resets the image strings' live count itself, so a load with no
  image region open (the pdumper tests load images directly) can no
  longer leave a sweep passing a block that holds a dead string unread,
  which the checked build's assertion caught. The character count of a
  multibyte string steps over ASCII bytes without decoding them (1,402
  image strings, 1.6M instructions and now 1.05M). Startup (`--batch
  --eval nil') took 179.4M instructions and now 170.8M (GNU 166.1M, 52.6M
  of it collecting); `Loader::load' 99.3M and now 91.5M; loading `ert'
  367.4M and now 358.4M (GNU 396.6M); the frozen runner's setup of
  xml-tests.el 501.7M and now 492.7M (GNU 469.5M). Of that setup's gap,
  outside collection Emaxx's startup is 57.3M over GNU's, loading `ert'
  33.1M and the runner and test file 23.6M, while its two collections
  (72.2M) cost less than GNU's three (163.0M). Every symbol's value,
  function and plist after startup is round 96's but for timestamps,
  paths and the image's consing counters, and round 96's interning
  probes, round 95's cons probe and a new probe that stores into image
  strings (one of them through a multibyte character), keys a weak table
  by them and drops one across collections give GNU 30.2's output byte
  for byte. Local validation: the full gate, 3,239 passed across its ten
  groups with the documented local skips, and the one other test,
  `just_this_one_wait_still_notifies_an_exited_distractor', whose 0.15 s
  window is host scheduling, failed once while the host ran profiles
  beside the gate and passed five times of five alone; the new test with
  the other 30 pdumper tests; clippy `-D warnings', rustfmt and `git diff
  --check' pass.
- **Round 98: the image's vectors mapped in place.** The image's
  ordinary vectors (but the empty one, alloc.c's `zero_vector', which
  stays an object start of its own), closures, records, char tables and
  sub-char-tables take the conses' and strings' way: the dumper writes
  each in its runtime form (the size word the allocator gave it, then
  its words, a subtable's packed depth and first character among them,
  the immediates as the heap holds them) into this allocator's vector
  blocks, each vector in the first block with room for it below
  `VECTOR_BLOCK_BYTES' (dump order alone left 0.66 MB of block tails
  unused; first fit leaves 0.20 MB), a block's unused bytes one free
  vector and its marks zero; a vector past `VBLOCK_BYTES_MAX' follows
  the blocks on its own with its mark's word. Two more header locators
  give the blocks and the large vectors with their counts (the header is
  now 132 bytes); a vector's offset is provisional while the other
  objects are dumped, above the strings' range, and every remembered one
  (the hash-table list's included) is translated when the blocks are
  placed. The loader checks each vector's size word against the kinds
  the image holds (a free vector only at a block's end; a subtable's
  depth, slot count and first character; a closure's three slots at
  least), sets each block's marks and each large vector's mark to the
  current epoch, and registers the 870 blocks and 35 large vectors (the
  image's 13,932 vectors, 3.76 MB) as the image's own bytes; nothing is
  allocated or counted. A vector reference is the mapped image's
  address plus the word's offset (RELOC_DUMP_TO_DUMP_LV of the vector's
  kind), checked against a bitmap of the vectors' starts; the 210K
  relocations in the vector area are applied in place, each checked to
  name a Lisp word of a vector, and the closures' code is checked after
  them. The collector never gives a mapped vector block back (a block
  with nothing live stays one free vector, on no free list) nor frees a
  mapped large vector, which leaves the registry. A relocation naming an
  object of the starts is resolved before the cell and vector kinds are
  tried (166K of the area's 210K name symbols or bool vectors), which
  brought the area's relocation from 55 instructions a relocation to 38.
  Startup (`--batch --eval nil') took 170.8M instructions and now 162.3M
  (GNU 166.1M, 52.6M of it collecting); `Loader::load' 91.5M and now
  83.0M; loading `ert' 358.4M and now 349.6M (GNU 396.6M); the frozen
  runner's setup of xml-tests.el 492.7M and now 483.7M (GNU 469.5M). The
  image is 25.5 MB. Every symbol's value, function and plist after
  startup is round 97's but for timestamps, paths, the image's consing
  counters and the order of `comp-loaded-comp-units-h''s load-time
  entries (dump-queue order, which the hot section's size moves; GNU's
  differs from both). The cons, interning and string probes give GNU
  30.2's output byte for byte, and a new probe that stores into image
  char tables, keymaps and records, keys a weak table by them and drops
  one across collections gives round 97's output; its one difference
  from GNU's, a replaced `char-script-table' still keyed after three
  collections, is conservative stack residue (with one collection GNU
  keeps a fresh unreachable char table and Emaxx keeps neither), and no
  Lisp or host root holds the old table. Local validation: the full
  gate, 3,240 passed across its ten groups with the documented local
  skips (the pdumper tests 32, one new), and the one other test,
  `just_this_one_wait_still_notifies_an_exited_distractor', failed once
  while the host ran profiles beside the gate, as in round 97, and passed
  five times of five alone; clippy `-D warnings', rustfmt and `git diff
  --check' pass.
- **Round 99: `\N{NAME}' read as lread.c reads it.** lread.c's
  character_name_to_code takes "U+" and hex digits itself and asks
  `(char-from-name NAME t)' (mule-cmds.el) for any other name, where the
  reader looked the name up in Unicode's own table: a name `ucs-names'
  does not hold (Unicode's "CJK UNIFIED IDEOGRAPH-4E2D", which GNU reads
  only as `\N{U+4E2D}') is now `invalid-read-syntax' as in GNU, a name
  matches ignoring case, and the numbered ideographs ("TANGUT
  IDEOGRAPH-17000") read as their codes. In an image built before
  mule-cmds.el is loaded `char-from-name' is void and a named escape
  signals, as it does in GNU; the pdumper tests' probes now spell their
  Greek alpha `\N{U+3B1}'. A read with no Lisp context (the host's own)
  still looks names up in Unicode's table. Lisp now runs in the middle
  of any read, and the first name builds `ucs-names' (loading
  uni-name.el), which collects: the reader rooted what it had read so
  far only for a function stream, so the strings read before a
  `\N{NAME}' in the same form were freed and their cells reused (the
  gate's quoting-style test read uni-name.el's word lists in their
  place). The root stack is now one per thread, lread.c's rdstack as
  mark_lread marks it, for every input: each completed datum is rooted
  until its parent replaces it, each reader works above the depth it
  found, and a whole datum leaves the stack at its base as read0 does.
  It costs 1.2M instructions loading ert (353.0M) and 1.3M in the
  xml-tests setup (488.0M); a stack per reader cost twice that, from
  registering a buffer per read. A new batch test reads a list of
  strings around the first name, then six escapes, and prints GNU 30.2's
  output byte for byte. Measured and not
  kept: resolving a relocation by its target's number first (an entry
  numbering no object then trying the cell and vector kinds) left
  startup at 162.29M instructions against 162.26M. Local validation:
  the full gate, every group passed but primitives (752 of 753: the
  timing-sensitive just_this_one_wait test, failing while profiles ran
  beside the gate as in rounds 97 and 98, passed three times of three
  alone); clippy `-D warnings', rustfmt and `git
  diff --check' pass.
- **Round 100: `assoc-string' as minibuf.c's Fassoc_string.** The walk
  is GNU's: a symbol KEY is its name, the list is followed while its
  tail is a cons, an element (or its car) that is a symbol stands for
  its name, one that is neither a string nor a symbol is passed over,
  and each remaining one is asked of `compare-strings' over both whole
  lengths with CASE-FOLD, which upcases only a differing pair of
  characters; `maybe_quit' runs between elements. The old arm copied
  the list first (a vector LIST read as `vector-literal' and its slots,
  so `(assoc-string "foo" ["foo"])' found "foo", and a dotted or non-list
  tail signalled where GNU stops), signalled for a non-string KEY even
  when no element is a string, and folded both whole strings through
  the casing machinery with a word-boundary rule of its own. That rule
  read `syntax_word_chars', a list of codes `modify-syntax-entry' kept
  beside the syntax table, one code at a time over a whole range, for
  this alone; the list, its image root slot (41) and the three folding
  helpers are gone. A 32-line probe of keys, symbols, dotted tails,
  non-string elements and keys, the characters whose case mappings
  differ (ß and ẞ, the Turkish dotless i, the Greek sigmas, ǅ, the ﬀ
  ligature) and multibyte text gives GNU 30.2's output byte for byte,
  and a new batch test pins eleven of its cases. The frozen runner's
  setup of cal-french-tests.el (runner command line, `-L' GNU's whole
  load path) took 572.9M instructions before round 99 and now 564.0M
  (GNU 508.1M): the folding's 11.9M is gone and round 99's root stack
  costs 3.0M here. Local validation: the full gate, 3,243 passed across
  its ten groups with the documented local skips (the batch tests 52,
  one new); clippy `-D warnings', rustfmt and `git diff --check' pass.
- **Round 101: file-name handlers, face aliases, source hashes and
  load-history as fileio.c, xfaces.c, comp.c and lread.c.**
  `find_file_name_handler' is Ffind_file_name_handler: the three
  variables are read as C reads them, a symbol handler's `operations'
  property through `get' and the operation through `memq', each regexp
  matched with fast_string_match (no case folding) on the file name,
  the entry matching latest kept, a handler `inhibit-file-name-handlers'
  names passed over only while OPERATION is
  `inhibit-file-name-operation' (identity, both lists unvalidated until
  reached), `maybe_quit' between entries; a nil handler that matched
  latest still leaves the caller on its no-handler path. The old walk
  copied the alist and compiled each regexp afresh through the general
  matcher. `resolve_face_name' is xfaces.c's: a string names the symbol
  it interns, the `face-alias' chain is followed through `get' with the
  hare two links to the tortoise's one, and a chain that comes round
  again signals `(circular-list FACE)' where the old walk stopped at the
  first repeat. comp.c's source hash streams the file through
  md5-stream.c's md5_stream 32,768 bytes at a time (a .gz file's
  uncompressed bytes) instead of reading it whole. `map-keymap' walks a
  keymap's cells as keymap.c does, with no set of visited cells.
  `current-load-list' is read and stored as Vcurrent_load_list, by its
  symbol, and the `require' and `defun' entries' heads are their
  symbols, not names interned again. A 26-line probe of handlers (an
  operations list, inhibition, a dotted entry, a later-matching nil
  handler, a non-list entry) and face aliases (strings, chains, a
  two-face cycle) gives GNU 30.2's output byte for byte; a batch test
  pins seven of its cases. With the runner's command line, startup's
  mutator (instructions less collections) went from 162.1M to 158.2M
  and the cal-french-tests.el setup's from 491.1M to 470.7M (GNU
  343.8M); its total is 542.6M (GNU 508.1M). Local validation: the
  full gate across its ten groups with the documented local skips;
  clippy `-D warnings', rustfmt and `git diff --check' pass.
- **The frozen run's timings at round 97, and where the per-file cost
  goes.** Round 97's frozen run (gate profile; summary.json's
  `timings') puts the 519 files' summed wall time at 1.052 times GNU's
  (294.5 s against 279.9 s): setup 1.270 (58.1 s against 45.7 s, about
  24 ms a file), the test bodies 1.010. The 269 files whose GNU body
  takes 8 ms or less take a median 13 ms against 5 ms. The largest body
  excesses are comp-tests (+2.8 s), tramp-tests (+2.2 s),
  package-tests (+1.9 s), semantic-utest-ia (1.93 times), mule-tests
  (2.23), emacs-module-tests, mml-sec-tests, edebug-tests (1.65),
  electric-tests (2.15) and undo-tests (1.61). With the runner's own
  command line (tools: scratchpad trivial.sh), cal-french-tests.el takes
  569.7M instructions to its loaded marker against GNU's 508.1M and
  92.9M for its body against 64.6M. The body is nearly all
  `ert-select-tests' calling `apropos-internal' with "" over every
  symbol (twice a file: the runner's list of tests loaded before the
  file is the other), 55.6M a call against 42.8M (2,794 instructions a
  symbol against 2,143): the byte-code lambda's `string-match' costs
  about 1,400 against 1,000 (the matcher itself 221 against 348, the
  setup around it the rest: `string_match_1' 345 against 166,
  `compile_pattern' 180 against 132, `case-fold-search' through the
  buffer-local lookup about 150 where GNU reads a BVAR, the StringText,
  Canon and register adapters), and the calls into and out of byte-code
  about 1,420 against 960 (`call_plain' 161 a subr call where GNU's
  Bcall and funcall_subr take about 70). Loading the runner and the
  test file (both source) takes 220M against 111.5M: 37M of it
  collecting where GNU collects in the earlier stages (its setup
  collects 163.8M in all, Emaxx 72M), byte-code 38.9M against 21.5M,
  matching 39.4M against 29.2M, the reader 20.3M against 18.1M, host
  allocation 13.7M against 1.8M, and `assoc-string' 8.8M against
  nothing: it folds both sides with a full downcase whose word test
  scans `syntax_word_chars', a list kept beside the syntax table, where
  minibuf.c's Fassoc_string calls `compare-strings' (upcasing a pair of
  characters only where they differ) on each element of the list.
  Writing a file with `with-temp-file' locks it: GNU's filelock.c makes
  the lock a symlink to "USER@HOST.PID:BOOT" with the login name it
  holds, and reads no account file; Emaxx writes a regular file and
  reads /etc/passwd twice a lock (system calls are otherwise even, 475
  in the body against GNU's 502).

**Local validation of this round** (gate profile, `--test-threads=1`): the
36 message tests, 114 backtrace/forwarded-variable/`format`/`nthcdr` tests,
25 sorting/case-folding/substring tests plus the comparison tests, and 95
mapping and bytecode tests, 208 native-runtime/funcall/GC tests and 189
bytecode/reclamation/finalizer/weak-table tests pass; clippy `-D warnings`, rustfmt and
`git diff --check` pass. For the in-loop frame switch: 681 of 683
byte-code, backtrace, native, funcall, unwind, handler, condition, catch,
throw, debug, nesting and compiled-code tests pass; the two others are
the as-root and image round-trip failures below, which pass in CI. For
the evaluator's tag tests and the verifier fix: 349 evaluation, `let`,
lambda, funcall, `setq`, keyboard, backtrace, binding, quit and nesting
tests and 188 keymap, interning, verification, undo and jump-table tests
pass, with the three new tests. A broad local run over the GC, special-form, `eval_`
and native filters was stopped after 89 CPU minutes, with one failure:
`byte_compile_file_reports_an_unwritable_target_like_gnu`, which cannot
pass as root (this container's user writes into a mode-555 directory) and
passes in CI. For the in-place ops, plain native and subr calls and the
forwarded-variable count: 625 of 626 native, byte-code, funcall,
backtrace, arity, buffer-local, forwarded-variable, `memq`, undo,
symbol-with-position, unwind, handler, catch, quit and nesting tests pass
with the new one (predicates and `memq` in the loop against GNU); the
other is the image round trip below. For the compiled callees under
eval's frame and the probes: 2,148 of the 2,150 evaluator tests pass, the
two others being the as-root and image round-trip failures; for the
shared handler lists, 267 of 267 handler, condition and signal tests
pass; for the unchecked shell, 154 of 154 thread, continuation,
funcall, binding, unwind and backtrace tests and 179 of 180 byte-code,
closure and verifier tests with the new one (the other is the as-root
failure); for the parameters beside the frames, 335 of 335 backtrace,
debugger, Edebug, ert, lambda, funcall, thread, root, copy, collection,
handler and signal tests; for the native runtime's unchecked shell, 397
of 398 native tests (the other is the image round trip below); for
undo's allocations, 418 of 419 handler, condition, signal, feature,
message, substring, multibyte, coding and dump tests (the other is the
image round trip); for the activation counter, 446 of 447 native and
thread tests (the other is the image round trip); for the trampoline's
escape record, 672 of 673 native, thread, handler, condition and signal
tests (the other is the image round trip), and errors, `catch`/`throw`
and handlers through the native test functions answer as before on the
release build; for the in-place invocation record, 446 of 447 native
and thread tests (the other is the image round trip); for the C-style
variable reads, 177 of 178 insertion, hook, read-only, undo and lexical
tests with the new one (the other is the save-buffer test below); for
undo's lazy entries and the ASCII copies, 558 of 559 undo, string-coding,
multibyte, unibyte, handler, message, insertion and dump tests (the other
is the print-charset test below); for the unrooted op arguments, the
sort arguments and the sort port, 583 of 583 sort, byte-code, VM, root,
collection, weak-table, undo, string-coding and handler tests (on the
build before the signal handling and argument changes) and 13 of 13 sort
tests on the final build, with the five new ones. Under
`EMAXX_GC_STRESS` (a collection at every `maybe_gc`) the 47 VM unit
tests pass; the 49 byte-code tests that boot a full batch interpreter
take hours each under it and were not run to the end. For the small
buffer texts, 1,272 of 1,272 buffer, insertion, deletion, region,
search, regexp, syntax, line, marker, undo, text-property, narrowing,
coding, case, message and sexp tests, and 250 of 250 editing tests on
the final build with the new one. For the jump table, 653 of 653 VM,
byte-code, native, funcall, catch, condition, unwind and handler tests;
for the fixed-arity native calls, 540 of 541 native, funcall, thread,
handler, condition, signal, escape, catch, throw, unwind and suspension
tests (the other is the image round trip below). For the allocation
sweep: 711 of 712 native, catch, handler, condition, closure, lambda,
function, documentation and unwind tests (the other is the image round
trip), 641 of 641 collection, weak-reference, cons, root, census, `let`
and environment tests, 759 of 759 `cond`, line, function, alias, closure, documentation and `eval_01` tests with the new ones, 126 of 126 numeric, integer, bignum and bindat tests on the final build, and scripts of `cond` forms
(dotted, non-list and self-modifying clauses), `forward-line` counts and
arguments, alias chains with a cycle, `function` forms with
documentation and interactive specs, every `ash` shift of fixnum and
bignum values and the bitwise operations' argument errors, identical to
GNU's output. For the call trims: 1,301 of the 1,302 evaluator,
backtrace and frame tests run by the commit (of 1,732; the rest were
still running, and the other is the as-root byte-compile test below),
and 653 of 653 VM, byte-code, native, funcall, catch, condition, unwind
and handler tests on the build before the entry took the closure from
the arm, and 131 of 132 byte-code and compile tests on the final build
(the other is the as-root byte-compile test). The call trims' evaluator
run was stopped at its time limit: 1,479 of the 1,480 tests it finished
passed (the other is the as-root byte-compile test), and the 252 it had
not reached pass on the next round's build. For the dispatch round, the
continuation test and Rust 1.99.1: 654 of 654 VM, byte-code, native,
funcall, catch, condition, unwind and handler tests with the new one,
the 190 native-runtime tests and the new kind test, and the lightweight
group's 590 tests in each of 10 runs pass on the final build, and 26 of
26 backtrace and debugger tests on the build before the entry cache's
word compare; the 654 and the lightweight group passed on the dispatch
round's own build (Rust 1.97.0) too. On 1.97.1 the changed continuation
test passed 40 of 40 runs of the lightweight group, where the old one
failed about one run in six. Clippy `-D warnings` and rustfmt pass under
1.99.1. For eval_sub's direct path: 3,134 of the 3,138 unit tests pass,
run in four parallel parts; the others are the as-root byte-compile and
save-buffer tests and
`file_writable_p_is_nil_for_missing_files_in_unwritable_directories`
(root may write where the test expects no access), all three failing on
the build before too, and
`process_tls_state_survives_collection_before_and_after_async_handshake`,
whose GNU run disagreed under that load and which passes alone, 3 of 3.
For the buffer list: the same 3,134 of 3,138 and four failures, the
checked build's comparison of the list with every buffer header holding
at every collection. For the guard count: 3,134 of the 3,138 with 2 ignored, the as-root three
and the load-sensitive TLS test failing; the checked build's assertion
that no cell is borrowed when the count is zero held at every collection
of the suite. For the case-table signature: the 137 char-table, case-table, case-fold,
regexp, search and signature tests pass (run one at a time; at two
threads four of them fail the same way on the build before, the tests
sharing one process heap), and the full 3,138 as below with the as-root
three and the load-sensitive TLS test. For the dispatch copy: the same,
with `subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`
failing in that load and passing alone. For the exchange and the
compaction: 3,133 of the 3,138 pass with 2
ignored, the three failing being the as-root ones above, and the four
allocator tests; the compaction's held-block path is pinned by
`string_sblocks_compact_unborrowed_data_and_hold_borrowed_blocks` (the
borrow released, the next collection moves the held string). For the
small-block lists: the same 3,134 of 3,138 with the same four
failures, less
`subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`,
which failed in that load on this round's builds (its child had exited
and been reaped before the first `process-live-p`): under twelve
concurrent runs it fails 12 of 60 times on the eval_sub round's build
and 5 of 60 on this one, and passes alone, 3 of 3; and the four
allocator tests, three of them new (reuse, alignment and zeroing past
the lists' depth, reallocation within and across classes, and blocks
freed by another thread that then exits).
For the byte-code entry and the two stores: 3,135 of the 3,141 pass
with 2 ignored (the allocator's three tests are in the parts now), the
as-root three and the load-sensitive TLS test failing (it passes alone,
3 of 3); the round's first build also failed
`suspended_bytecode_retains_operand_and_unwind_roots`,
`suspended_bytecode_observes_live_constant_vector_mutation`,
`native_vector_and_closure_census_matches_gnu_word_layout` and
`native_pseudovector_census_matches_gnu_word_layout` (the cache in TLS,
above), which pass on the final build and passed on the one before.
For the four smaller cuts: 3,134 of the 3,141 with 2 ignored, the as-root
three failing and the two load-sensitive tests
(`subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`
and `process_tls_state_survives_collection_before_and_after_async_handshake`)
failing in that load and passing alone, 3 of 3.
For mapcar's list: 3,134 of the 3,141 with 2 ignored and the same
five failures (the as-root three, both load-sensitive tests), with the
mapping test's new `vector-literal` case. A first run of that suite
failed about 960 tests at startup because of how it was run, not the
change: a test run from the cargo target directory had dumped the
shared fixture image (`EMAXX_FIXTURE_IMAGE_DIR`), whose native units are
recorded relative to the dumping executable's directory, and the copy of
the same binary run from another directory (same fingerprint, so the
same image name) resolved them to a missing path. Run the suite's
binary from one place per fixture directory, or give each place its own.
For the native calls: 3,134 of the 3,141 with 2 ignored on the final
build before the new test (the as-root three and both load-sensitive
tests failing), and on the build with `call_function` always inline
3,135 of them and the new test, the as-root three and
`process_tls_state_survives_collection_before_and_after_async_handshake`
failing; on the final source 600 of 600 native, byte-code, funcall,
catch, throw, condition, handler, signal, escape, suspension, thread and
unwind tests pass with the new one, and clippy `-D warnings` and rustfmt
pass; the
round's earlier builds gave the same with the other load-sensitive tests
(`subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`,
and once `just_this_one_wait_still_notifies_an_exited_distractor`, whose
GNU run had not yet read the distractor's output under that load; it
passes alone, 3 of 3). The new test's program gives GNU's output on the
release build too, and under callgrind it enters `call_native_nested`
138 times and leaves it by the escape record 5 times.
For the signals' conditions by symbol: 3,135 of the 3,142 with 2
ignored, the as-root three and two load-sensitive tests failing
(`subprocess_exit_is_event_driven_and_notifies_newest_process_first_once`
and `process_tls_state_survives_collection_before_and_after_async_handshake`,
both passing alone), with clippy `-D warnings` and rustfmt clean. Its
release build leaves bytecode calls (551.0 instructions an iteration),
bytecode-to-native (491.5) and native-to-bytecode (4,751) where they
were. For the change hooks: 256 of the 257 hook, change, local, undo,
insert, deletion and overlay tests pass, with the as-root
`save_buffer_skips_unmodified_and_unchanged_files` the one failure;
clippy `-D warnings` and rustfmt are clean. For the nested evaluator
call: 925 of the 928 native, byte-code, funcall, catch, throw,
condition, handler, signal, escape, suspension, thread, unwind and
nested-call tests pass, the as-root three failing; clippy and rustfmt
clean. For the census by type: 135 of the 136 obarray, abbrev, GC and
collection tests pass with the checked comparison, the other being the
load-sensitive TLS test, which passes alone (2 of 2). For the image cells
left unread: 1,110 of the 1,112 GC, image, dump, weak, finalizer, obarray,
interning, symbol, string, vector, record, buffer, allocation and mark
tests pass in three parallel parts with the checked assertions, the
others being the as-root save-buffer test and the TLS test, which passes
alone; clippy `-D warnings` and rustfmt clean. For the image's string
data: 676 of 676 string, data, compaction, relocation, borrow, multibyte,
unibyte, coding, substring, concatenation, format, collection, image,
dump, `aset` and text-property tests pass in three parallel parts; clippy
and rustfmt clean. For the scan's bounds and the per-kind lists: 592 of
the 593 conservative-scan, stack, native, collection, weak, finalizer,
sweep, block, heap, root, allocation and vector tests pass in four
parallel parts, the other being the load-sensitive TLS test, which passes
alone (2 of 2); clippy and rustfmt clean. For undo's window configurations,
message log and native `set`: 870 of the 871 window, configuration,
message, undo, native, subr, `set`, `setq`, buffer and hook tests pass in
four parallel parts, the other being the as-root save-buffer test; clippy
`-D warnings` and rustfmt clean. For undo's edits and native code's calls:
1,392 of the 1,393 hook, change, overlay, insert, delete, undo, buffer,
window, native, funcall, binding, list, closure, byte-code, thread,
continuation, unwind, catch, condition and signal tests pass in four
parallel parts (the other the as-root save-buffer test), and with the
VM's bindings by id 487 of 487 byte-code, binding, unwind, handler,
catch, signal, thread and buffer-local tests; the regenerated x86_64
subr table differs from the generator's output only by the four direct
targets (the aarch64 table, which needs that host's GNU build to
regenerate, has the same four entries changed by hand); clippy `-D
warnings` and rustfmt clean. For temporary buffers and `format`: 775 of
the 776 format, buffer, directory, read-only, message, error, signal,
condition and string tests pass in four parallel parts, the other the
as-root save-buffer test; clippy `-D warnings` and rustfmt clean.
CI's full gate and frozen runs are the merge evidence.

Instruction ratios (Emaxx/GNU per unit) at the start of this round and now:

| Case | Before | Now |
| --- | --- | --- |
| upstream-undo (10 units) | 5.88 (4.33 at 2 units: the log grew) | 1.39 |
| upstream-sort (4 units) | 3.26 (at 1 unit) | 1.14 |
| interpreted-calls | 2.10 | 1.06 |
| interpreted-lexical | 2.04 | 1.05 |
| interpreted-dynamic | 2.14 | 1.12 |
| mapcar | 2.95 | 1.22 |
| bytecode-calls | 3.30 | 1.17 |
| bytecode-to-native | — | 1.27 |
| interpreted-to-native | — | 1.27 |
| native-to-interpreted / -bytecode | — | 1.15 / 1.26 |
| cons-allocation / list-traversal | 2.24 / — | 1.09 / 1.10 |
| native-execution | — | 1.13 |
| bindat | — | 1.02 |
| explicit-gc | — | 0.74 |

The table is one full run of all sixteen on the native calls' build
(mapcar's on the round before, 1.24 there; undo's on the signals'
round, 1.41 on the native calls' build; mapcar, interpreted-to-native
and native-to-interpreted/-bytecode on the nested evaluator call's
round, 1.23, 1.29 and 1.16/1.27 on its base), undo at twenty and forty
units (1.45 at ten and twenty on the same build, the closure entry
table's misses and `malloc_consolidate` in the difference), compiled by
Rust 1.99.1 as the rounds since the Rust 1.99.1 bullet were (the rounds
above those were measured on 1.97.0); interpreted dynamic read 1.13 on
the round's build with `call_function` always inline, `eval_sub`
unchanged (one `malloc_consolidate` falling in the difference); on the
byte-code entry round's build bytecode calls read 1.19,
bytecode-to-native 1.59, interpreted-to-native 1.30 and bindat 1.02,
and mapcar 1.30 there and 1.29 on the round before. Undo read 1.47 to 1.49 and
bindat 1.02 to 1.05 on the last three builds (the conservative scan's
`mem_find` calls in a collection in the difference),
native-to-interpreted 1.20 to 1.21 on one build. Undo read 1.67 to 1.70 across the
allocation sweep's builds (the allocator's state), and a collection's
stack scan or a hash table's growth falling in the difference moves the
interpreted rows by about 0.01.
Sort at one and two units read 1.84 before the port and 1.12 after it,
but after it Emaxx's two collections fell at both sizes, out of the
difference, while one of GNU's fell in it; at four and eight units it
reads 1.97 before (two collections each in the difference) and 1.36
after (one of Emaxx's, two of GNU's; 1.44 without collections, an
Emaxx collection costing about twice GNU's). Count each editor's
collections in a difference before reading a ratio.
Interpreted calls and lexical read 1.24 to 1.27 across this round's
builds before the unchecked shell with no change on their path
(`eval_sub` layout); undo read 2.03 to 2.06 on the same code (the
allocator's state, below; it read 1.97 and 1.99 on two builds before
the allocation fixes and 1.93 to 1.97 after them). Earlier, undo
moved between 2.37 and 2.42 from one build to the next with no code
change on its path: the
difference sits in glibc's `_int_malloc`, `malloc_consolidate` and
`unlink_chunk` (the allocator's free-list state), not in Emaxx code.
Undo's earlier 2.34 at ten units was an artifact: in that pair of runs the
collector worked more at ten units than at twenty (`mem_find` and the
stack scan came out negative in the difference), hiding about 0.57 million
instructions per unit. The like-for-like figure before byte-code
verification was about 2.80 as well. A pair with negative collector
functions in its difference is not a measurement; repeat it.

**CI on `2c7c55f`:** full Rust gate passed (run 38737706552, every group;
3,104 tests in the counted groups plus the bins and integration groups, the
two existing ignores), frozen **7,928 of 7,928 outcomes match across 519
files** with no unsuccessful execution (run 38737719924).

**CI on `352ea91`** (everything through `c100767`): full Rust gate passed
(run 38751776827; 3,105 tests in the counted groups plus bins and
integration, the two existing ignores), frozen **7,928 of 7,928 outcomes
match across 519 files**, no unsuccessful execution (run 38751781622).

**CI on `4008098`** (everything through `898a088`): full Rust gate passed
(run 38761724660; 3,105 tests in the counted groups plus bins and
integration, the two existing ignores), frozen **7,928 of 7,928 outcomes
match across 519 files**, no unsuccessful execution (run 38761729296).

**CI on `cf18c9d`** (byte-code verification): full Rust gate passed (run
38783494477; 3,111 tests in the counted groups plus bins and integration),
frozen **7,928 of 7,928 outcomes match across 519 files** (run
38783499025). **CI on `bfb9c1a`** (Bcall into native functions): full
Rust gate passed (run 38789815645; 3,112 tests), frozen 7,928 of 7,928
(run 38789820093). **CI on `36f1a67`** (fixnum fast paths, in-place
frame pops): full Rust gate passed (run 38803520645; 3,114 tests in the
counted groups plus bins and integration, the two existing ignores),
frozen 7,928 of 7,928 across 519 files, no unsuccessful execution (run
38803524575). **CI on `7172429`** (eval_sub's lambda calls, kept buffer
counts, Bcall subrs, in-loop varref/varset): full Rust gate passed (run
38819256699; 3,116 tests in the counted groups plus bins and
integration), frozen 7,928 of 7,928 across 519 files, no unsuccessful
execution (run 38819259588). **CI on `41be443`** (covering `6761153`
too: word tests, closures from funcall, the opcode match): full Rust gate
passed (run 38828973274; 3,118 tests in the counted groups plus bins and
integration), frozen 7,928 of 7,928 across 519 files, no unsuccessful
execution (run 38828975757). **CI on `b31d6d9`** (in-place funcall
frames): full Rust gate passed (run 38835660142; 3,119 tests in the
counted groups, 2 ignored, plus bins and integration), frozen 7,928 of
7,928 across 519 files, no unsuccessful execution (run 38835662192).
**CI on `dee8061`** (frameless Fcons): full Rust gate passed (run
38838677038; 3,119 tests, 2 ignored), frozen 7,928 of 7,928 across 519
files, no unsuccessful execution (run 38838679538). **CI on `29e7780`**
(in-loop byte-code frames): full Rust gate passed (run 38852469382; 3,120
tests, 2 ignored), frozen 7,928 of 7,928 across 519 files, no
unsuccessful execution (run 38852471256). **CI on `0b0b6dc`** (evaluator
tag tests, jump-table verification): full Rust gate passed (run
38864048354; 3,122 tests, 2 ignored, plus bins and integration), frozen
7,928 of 7,928 across 519 files, no unsuccessful execution (run
38864051065). **CI on `d20ca14`** (ops in place, plain subr and native
calls): full Rust gate passed (run 38874710028; 3,123 tests, 2 ignored,
plus bins and integration), frozen 7,928 of 7,928 across 519 files, no
unsuccessful execution (run 38874712632). **CI on `40de0bc`** (bitmap
sweep): full Rust gate passed (run 38881670816; 3,123 tests, 2 ignored,
plus bins and integration), frozen 7,928 of 7,928 across 519 files, no
unsuccessful execution (run 38881676646). **CI on `b22d7ba`**
(identity-hashed local bindings): full Rust gate passed (run 38888654939;
3,123 tests, 2 ignored, plus bins and integration), frozen 7,928 of 7,928
across 519 files, no unsuccessful execution (run 38888657799). **CI on
`e8740a5`** (compiled callees under eval's frame, the tag-tested
funcall, the error-free probes) and **on `6118781`** (shared handler
lists): full Rust gate passed on each (runs 38904376618 and
38906537195; 3,124 tests, 2 ignored, plus bins and integration), frozen
7,928 of 7,928 across 519 files, no unsuccessful execution (runs
38904379904 and 38906541122). **CI on `c1c555e`** (the unchecked
interpreter shell): full Rust gate passed (run 38916491905; 3,125 tests,
2 ignored, plus bins and integration), frozen 7,928 of 7,928 across 519
files, no unsuccessful execution (run 38916498890). **CI on `62986d1`**
(the parameters beside the frames, the unchecked native shell): full
Rust gate passed (run 38922630018; 3,125 tests, 2 ignored, plus bins
and integration), frozen 7,928 of 7,928 across 519 files, no
unsuccessful execution (run 38922634093). **CI on `4cae2ef`** (undo's
allocations): full Rust gate passed (run 38928340091; 3,125 tests, 2
ignored, plus bins and integration), frozen 7,928 of 7,928 across 519
files, no unsuccessful execution (run 38928344036). **On `8845616`** (the
activation counter): full Rust gate passed (run 38932458858; same
counts), frozen passed (run 38932463661). **On `9eb7217`** (with
`abb9239`: the trampoline's escape record and the in-place invocation
record): full Rust gate passed (run 38940592177; 3,125 tests in the
counted groups, the two ignores, plus bins and integration), frozen
7,928 of 7,928 across 519 files, no unsuccessful execution (run
38940597467). **On `3f61e10`** (the C-style variable reads): full Rust
gate passed (run 38941429841; 3,126 tests with the new one), frozen
7,928 of 7,928 across 519 files, no unsuccessful execution (run
38941434201). **On `2529c2e`** (the sort port and argument forms): the
Rust gate failed in its first group (run 38959692157):
`eval_string_ops` expected `(sort '(3 1 2) #'< :in-place t)` to sort,
which GNU rejects with "Invalid argument list" (a positional predicate
only in the two-argument call); the next commit corrects the test.
Frozen on `2529c2e`: 7,928 of 7,928 across 519 files, no unsuccessful
execution (run 38959695796). **On `3716780`** (the corrected test): full
Rust gate passed (run 38964025272; 3,131 tests in the counted groups,
the two ignores, plus bins and integration). **On `0c6564e`** (the small
buffer texts): full Rust gate passed (run 38970736660; 3,132 tests, the
two ignores, plus bins and integration), frozen 7,928 of 7,928 across 519
files, no unsuccessful execution (run 38970739812). **On `e50b127`** (the
jump table) and **on `5dc69d1`** (the fixed-arity native calls): full
Rust gate passed on each (runs 38975258598 and 38977491857; 3,132 tests,
the two ignores, plus bins and integration), frozen 7,928 of 7,928 across
519 files, no unsuccessful execution (runs 38975261428 and
38977494311). **On `961b207`** (the allocation sweep): full Rust gate
passed (run 38986010893; 3,134 tests with the two new ones, the two
ignores, plus bins and integration), frozen 7,928 of 7,928 across 519
files, no unsuccessful execution, on its second attempt (run
38986012909); the first attempt matched 7,927, the other,
`eglot-test-rust-completion-exit-function`, ending in GNU's run with a
`jsonrpc-error` from the language server while Emaxx's passed. **On
`ef33383`** (the call trims): the full Rust gate stopped in its
lightweight group (run 38995229528):
`native_continuations_can_return_in_non_lifo_order` found the first
probe's key still in its weak table after the first continuation had
returned. The test fails so here about one run in six on Rust 1.97.1, on
`961b207` too (which passed CI): the two probes shared one table, so the
second probe's `puthash` read the first key, and a copy of it that a
returned callee left in the second stack, below a frame still live when
that stack parked, kept it through the conservative scan of the parked
stack. `89dc6a3` gives each probe its own table (40 of 40 runs of the
lightweight group pass on 1.97.1). Frozen on `ef33383`: 7,928 of 7,928
across 519 files, no unsuccessful execution, on its second attempt (run
38995231316); the first matched 7,927, GNU's run of
`eglot-test-rust-completion-exit-function` failing again after
rust-analyzer's cargo watcher failed to start. **On `d4f6b94`** (the
first CI on Rust 1.99.1): full Rust gate passed (run 39003317809; 3,136
tests in the counted groups, the two ignores, plus bins and
integration), frozen 7,928 of 7,928 across 519 files, no unsuccessful
execution, on its first attempt (run 39003319679). **On `200e450`**
(eval_sub's direct path): full Rust gate passed (run 39007442705; 3,136
tests), frozen 7,928 of 7,928, no unsuccessful execution, on its first
attempt (run 39007444124). **On `c722291`** (the small-block lists): full
Rust gate passed (run 39011047978; 3,139 tests in the counted groups,
the two ignores, plus bins and integration), frozen 7,928 of 7,928, no
unsuccessful execution, on its first attempt (run 39011049487). **On
`c65fc87`** (the exchange and the compaction): full Rust gate passed (run
39015057290; 3,139 tests); frozen's first attempt matched 7,927 of 7,928
(run 39015058719), the one mismatch GNU's again: the oracle's
`eglot-test-rust-completion-exit-function` failed with a `jsonrpc-error`
and Emaxx's passed, as on `ef33383`; its second attempt matched 7,928 of
7,928 with no unsuccessful execution. **On `34b7f48`** (the dispatch
copy): full Rust gate passed (run 39018268679; 3,139 tests), frozen
7,928 of 7,928, no unsuccessful execution, on its first attempt (run
39018270078). **On `2d491d2`** (the case-table signature): full Rust gate
passed (run 39020019119; 3,139 tests), frozen 7,928 of 7,928, no
unsuccessful execution, on its first attempt (run 39020021023). **On
`e655fb2`** (the guard count): full Rust gate passed (run 39022801774;
3,139 tests), frozen 7,928 of 7,928, no unsuccessful execution, on its
first attempt (run 39022803610). **On `0c448c6`** (the buffer list):
full Rust gate passed (run 39024108508; 3,139 tests), frozen 7,928 of
7,928 across 519 files, no unsuccessful execution, on its first attempt
(run 39024110358). **On `d9a3c9a`** (the byte-code entry and the two
stores): full Rust gate passed (run 39032410374; 3,139 tests), frozen
7,928 of 7,928 across 519 files, no unsuccessful execution, on its first
attempt (run 39032411872). **On `d80e451`** (the four smaller cuts): full
Rust gate passed (run 39038064118; 3,139 tests), frozen 7,928 of 7,928
across 519 files, no unsuccessful execution, on its first attempt (run
39038065785). **On `d588d70`** (mapcar's list): full Rust gate passed
(run 39044968844; 3,139 tests), frozen 7,928 of 7,928 across 519 files,
no unsuccessful execution, on its first attempt (run 39044970894). **On
`ccdf45e`** (native calls from the calling frame): full Rust gate passed
(run 39055982318; 3,140 tests). Frozen matched 7,927 of 7,928 across 519
files (run 39055984062). The one mismatch is GNU's
`eglot-test-rust-completion-exit-function` failing in the oracle while
Emaxx passed, the rust-analyzer timing recorded above. Its failed job was
re-run once (attempt 2) and matched 7,928 of 7,928, with no unsuccessful
execution. **On `4e49cce`** (signals' conditions by symbol): full Rust
gate passed (run 39061192531; 3,140 tests), frozen passed on its first
attempt (run 39061194462). **On `a56f990`** (change hooks, slow-op
trials recorded): full Rust gate passed (run 39066922265; 3,140 tests),
frozen passed on its first attempt (run 39066924074). **On `5147b2e`**
(the evaluator's nested native call): full Rust gate passed (run
39069860003; 3,140 tests), frozen passed on its first attempt (run
39069862192). **On `85f3e00`** (the image's cells left unread): full
Rust gate passed (run 39074166122; 3,140 tests), frozen passed on its
first attempt (run 39074167800). **On `3e1cced`** (the image's string
data): full Rust gate passed (run 39075363648), frozen passed on its first
attempt (run 39075365786). **On `ba696b3`** (the scan's bounds and the
per-kind lists): full Rust gate passed (run 39078342040), frozen passed on
its first attempt (run 39078343825). **On `b206a4a`** (undo's window
configurations, message log and native `set`): full Rust gate passed (run
39080606435), frozen passed on its first attempt (run 39080608068). **On
`df06b81`** (the change hooks, native funcall and bindings by id): full
Rust gate passed (run 39085349381). Frozen matched 7,927 of 7,928 across
519 files (run 39085350916): GNU's `eglot-test-rust-completion-exit-function`
failed in the oracle with `jsonrpc-error` while Emaxx passed, the
rust-analyzer timing recorded above; its failed job, re-run once (attempt
2), passed. **On `f001516`** (buffer names and `format`'s count): full
Rust gate passed (run 39087539788), frozen passed on its first attempt
(run 39087541542). **On `3c034cc`** (with `093899a` beneath it: lazy cons
serials, the entry cache by identity): full Rust gate passed (run
39102085132). Frozen matched 7,927 of 7,928 (run 39102086693): GNU's
`eglot-test-rust-completion-exit-function` failed in the oracle with
`jsonrpc-error` again while Emaxx passed (0 unexpected outcomes for
Emaxx, 1 for the oracle); its failed job, re-run once (attempt 2),
passed. **On `ba9f640`** (with `71cc79f` beneath it: the register counts
and the verified return, the mark phase): full Rust gate passed (run
39108333438). Frozen matched 7,927 of 7,928 (run 39108334830), the same
oracle failure of `eglot-test-rust-completion-exit-function` (third run
in a row, each time on the GNU side only); its failed job, re-run once
(attempt 2), passed. **On `d7b02ff`** (undo's slow ops, the ASCII
newline search): full Rust gate passed (run 39115079701), frozen passed
on its first attempt (run 39115081583). **On `c48c5a2`** (activation
vectors kept, the operandless table at compile time): full Rust gate
passed (run 39118270646), frozen passed on its first attempt (run
39118272517). **On `6242417`** (strings read in place, a native
funcall's target from its cell): full Rust gate passed (run
39128542991), frozen passed on its first attempt (run 39128544870).
**On `81fda48`** (C-variable stores, `eq` by the word): both runs failed
on the anti-cheat audit of native fast paths (runs 39138334869 and
39138337192; the next round's bullet has the cause and the fix); the
Rust gate's other groups passed. **On `7cee893`** (name lookups by one
multiply, `format`'s copies, the fast-path inventory): full Rust gate
passed (run 39142224150). Frozen matched 7,927 of 7,928 (run
39142226322): the oracle failed `eglot-test-rust-completion-exit-function`
again (a `jsonrpc-error` on the GNU side; Emaxx passed it); its failed
job, re-run once (attempt 2), passed. **On `e5852af`** (one handler
search a signal): full Rust gate passed (run 39146151002), frozen passed
on its first attempt (run 39146153666). **On `f50ce28`** (watched and
buffer-local variables by flags): full Rust gate passed (run
39149792527), frozen passed on its first attempt (run 39149794338).
**On `8aaccaa`** (the undo list's view kept across marker adjustments):
full Rust gate passed (run 39153644820), frozen passed on its first
attempt (run 39153646470); PR #90 merged it into main. **On `5658349`**
(with `a2494c7`: the print, message and word-boundary variables by their
symbols, the watcher flag first): full Rust gate passed (run
39158088826), frozen passed on its first attempt (run 39158090668).
**On `a0034c1`** (a Bcall into native code by its count): full Rust gate
passed (run 39164077964), frozen passed on its first attempt (run
39164080135). **On `aa0c63f`** (maybe_gc on a mirror of the trigger):
full Rust gate passed (run 39167951029), frozen passed on its first
attempt (run 39167952851); PR #91 merged it into main (`bafedf4`). **On
`c1ae4e3`** (a running frame's program in three words): full Rust gate
passed (run 39172012232), frozen passed on its first attempt (run
39172014125). **On `3f7bff2`** (the position-dependent executable): the
Rust gate's library groups and binaries passed, but its integration
targets did not build (run 39175583455): the wrapper was a `/bin/sh`
script, and dash drops the environment variables whose names are no
identifiers, among them the `CARGO_BIN_EXE_make-fingerprint` that
`tests/cli.rs` reads. It is a Python script since (the next commit),
which passes the environment on, and the seven integration targets'
42 tests pass locally with it. Frozen matched 7,927 of 7,928 with the
one mismatch GNU's (run 39175585643, "oracle: 1 unexpected"); its
re-run passed (attempt 2). **On `3e749c3`** (the MANY subr path and the
Python wrapper): full Rust gate passed, the integration targets with it
(run 39180683216), frozen passed on its first attempt (run 39180685580).
**On `152e76f`** (a nested native call's tests): full Rust gate passed
(run 39185224902), frozen passed on its first attempt (run 39185226944);
PR #92 merged it into main (`cbe5eed`). **On `aa68c68`** (a nested
native call's frame popped by its length): full Rust gate passed (run
39191231079); frozen matched 7,927 of 7,928 with the one mismatch GNU's
("oracle: 1 unexpected", Emaxx none), and its re-run passed (run
39191232834, attempt 2). **On `286c624`** (the evaluator's subr calls):
full Rust gate passed (run 39217028701), frozen passed on its first
attempt (run 39217031778). **On `1e3f78e`** (the buffer-position ops in
the verified loop): full Rust gate passed (run 39222288525), frozen
passed on its first attempt (run 39222299548); PR #93 merged it into
main (`769bd10`). **On `3f71b54`** (the evaluator's native subr path):
full Rust gate passed (run 39246860015), frozen passed on its first
attempt (run 39246863841). **On `e053900`** (native arity errors naming
the function): full Rust gate passed (run 39253369363); frozen matched
7,927 of 7,928 with the one mismatch GNU's ("oracle: 1 unexpected",
Emaxx none), and its re-run passed (run 39253372487, attempt 2). **On
`e571f20`** (the jump padding): full Rust gate passed (run
39262083083); frozen matched 7,927 of 7,928, the one mismatch again
GNU's, and its re-run passed (run 39262088787, attempt 2); PR #94
merged it into main (`c769d5b`). **On `65ed19b`** (every kind of jump
padded, with prefixes): full Rust gate passed (run 39295048045), frozen
passed on its first attempt (run 39295051895). **On `07b0947`** (undo's
slow ops resumed in the loop): full Rust gate passed (run 39307717277),
frozen passed on its first attempt (run 39307721612). **On `8085c84`**
(round 27's Bmemq arm undone): full Rust gate passed (run 39322364977),
frozen passed on its first attempt (run 39322368855); PR #95 merged
rounds 26 to 28 into main (`40c3eb0`). **On `c4b94cf`** (one-argument
`quote` and `function` without their frame): full Rust gate passed (run
39350248947), frozen passed on its first attempt (run 39350251689).
**On `b614c70`** (`features` by the symbol, the window index in place):
full Rust gate passed (run 39357843966), frozen passed on its first
attempt (run 39357846766). **On `8c32f50`** (no undo in a buffer named with a
space): full Rust gate passed (run 39368532824); frozen matched 7,927 of
7,928, the one mismatch GNU's (oracle 1 unexpected, Emaxx none), and
its re-run passed (run 39368535177, attempt 2). **On `55cf084`** (three
compares by name off undo's paths): full Rust gate passed (run
39372480369), frozen passed on its first attempt (run 39372482254); PR
#96 merged rounds 29 to 32 into main (`327ecf4`). **On `a29cbef`** (the
printer's setup): full Rust gate passed (run 39377514989); frozen failed
(run 39377517895), 7,926 of 7,928 matching, Emaxx 2 unexpected: print-tests.el's
`test-prin1-overrides` and `test-prin1-to-string-overrides`, the
overrides bound in an environment copy the C-style reads miss (fixed in
round 34, bullet "Round 34"). **On `0378010`** (a C variable's read from
its cell, the overrides bound dynamically): full Rust gate passed (run
39389072258), frozen passed on its first attempt (run 39389075289), the
two print-tests overrides tests with it. **On `bdba9fa`** (round 35):
full Rust gate passed (run 39401137778), frozen passed on its first
attempt (run 39401142276). **On `4a46555`** (round 36): full Rust gate
passed (run 39402406056), frozen passed on its first attempt (run
39402409500); PR #97 merged rounds 33 to 36 into main (`87e8d32`).
**On `e6fb00c`** (round 37): full Rust gate passed (run 39420951276);
frozen passed on its first attempt (run 39420955510). **On `1f69927`**
(round 39): full Rust gate passed (run 39434615987), frozen passed on its
first attempt (run 39434621069); PR #98 merged rounds 37 to 39 into main
(`fe4e253`). **On `680498f`** (round 40): full Rust gate passed (run 39484877632),
frozen passed on its first attempt (run 39484880717). **On `fbc0ed7`**
(round 41): full Rust gate passed (run 39512630692), frozen passed on its
first attempt (run 39512635191); PR #99 merged rounds 40 and 41 into main
(`df82614`). **On `4bea9e1`** (round 42): full Rust gate passed (run
39534620573), frozen passed on its first attempt (run 39534622994).
**On `b0d0a63`** (round 43): full Rust gate passed (run 39554785723);
frozen failed seven GNU tests (run 39554788795): jit-lock's three,
python-mode's two string font-lock tests and eglot's column and
multi-line eldoc tests, all from its property changes running the
change hooks under `with-silent-modifications`, whose let-bound
`inhibit-modification-hooks` it ignored; round 44 passes all seven
here (eglot's with clangd installed). **On `61e3e66`** (round 44): full
Rust gate passed (run 39573615433); frozen failed six erc tests with
`text-read-only` (run 39573619718), `read-passwd`'s cleanup erasing the
minibuffer GNU's read_minibuf_unwind had already erased; round 45 passes
them. **On `626d1eb`** (round 45): full Rust gate passed (run
39639732639), frozen passed on its first attempt (run 39639736946).
**On `eb8e009`** (rounds 46 and 47, pushed together): full Rust gate
passed (run 39682876524), frozen passed on its first attempt (run
39682879871). **On `445cc7d`** (round 48): full Rust gate passed (run
39986757981); frozen matched 7,927 of 7,928 with the one unexpected
outcome on GNU's side (run 39986763116), and its re-run passed. **On
`1695c05`** (round 49): full Rust gate passed (run 39989914863),
frozen passed on its first attempt (run 39989919866). **On `8e2b9f8`**
(round 50): full Rust gate passed (run 40021014339); frozen (run
40021018468) stopped at `src/undo-tests.el`, which the gate build took
over 180 s to run (bullet "Round 51"), after every earlier file had
given its expected results. **On `d658dad`** (round 51): the Rust gate
(run 40034219662) failed `eval_region_preserves_point_and_uses_the_supplied_reader`
in its `primitives` group, and frozen (run 40034222838) failed 77 of
7,928 outcomes in cl-generic, cl-macs, edebug, gv and testcover tests,
all from the loader's oversized `forward-comment` count (bullet "Round
51"). **On `2ff27af`** (the fix): full Rust gate passed (run
40053413052), frozen passed on its first attempt (run 40053415013).
**On `1b61c83`** (round 52): full Rust gate passed (run 40056463837),
frozen passed on its first attempt (run 40056465430). **On `d1a3230`**
(round 53): full Rust gate passed (run 40058055258), frozen passed on its
first attempt (run 40058057152). **On `d3b2ee3`** (round 54): full Rust
gate passed (run 40063298332), frozen passed on its first attempt (run
40063300298). **On `0148bb1`** (round 55): full Rust gate passed (run
40066473882), frozen passed on its first attempt (run 40066475799).
**On `71b5174`** (rounds 56 and 57): full Rust gate passed (run
40073624210), frozen passed on its first attempt (run 40073625996).
**On `5d2bdea`** (rounds 58 and 59): full Rust gate passed (run
40076991774), frozen passed on its first attempt (run 40076993546).
**On `440d81c`** (round 60): full Rust gate passed (run 40078381958),
frozen passed on its first attempt (run 40078383462); rounds 55 to 60 were
merged into main by PR #102 (`df6c293`). **On `8c95f91`** (rounds 61 and
62): full Rust gate passed (run 40087156789); frozen matched 7,927 of 7,928
tests on its first attempt, the miss GNU's eglot flake, and passed on the
second (run 40087158866). **On `9ba4dee`** (round 63): full Rust gate
passed (run 40090524898); frozen matched 7,926 of 7,928 tests (run
40090526570): `bookmark-tests-save/non-ascii-annotation` and
`bookmark-tests-save/non-ascii-bookmark-name` fail, as the round 64
bullet's item (4) describes. **On `d99d85c`** (round 64): full Rust gate
passed (run 40097399568); frozen matched 7,926 of 7,928 tests (run
40097401438), the same two bookmark tests, which pass after round 65.
**On `6002362`** (rounds 65 and 66): full Rust gate passed (run
40113387970), frozen passed on its first attempt (run 40113390272).
**On `183603d`** (round 67): full Rust gate passed (run 40117891134),
frozen passed on its first attempt (run 40117893169). **On `1610718`** (round 68): the Rust
gate failed `text_conversion_detached_state_matches_gnu` and
`text_conversion_native_store_matches_gnu_without_localizing` in its
primitives group (run 40123867987; the groups after it did not run):
round 68's `buffer-local-variables` read the `text-conversion-style` slot
through the symbol, which `makunbound` had made plain; fixed in round 69.
Frozen passed on its first attempt (run 40123869891). **On `13dfb41`**
(round 69): full Rust gate passed (run 40133162719), frozen passed on its
first attempt (run 40133164494). **On `896ba25`** (round 70): full Rust
gate passed (run 40134441249), frozen passed on its first attempt (run
40134442777); PR #103 merged rounds 61 to 70 into main (`7b960e7`).
**On `e3bc9ef`** (round 71): full Rust gate passed (run 40139903681),
frozen passed on its first attempt (run 40139905885).
**On `cae8d39`** (round 72): full Rust gate passed (run 40146208215),
frozen passed on its first attempt (run 40146209717).
**On `caa70a2`** (round 73): full Rust gate passed (run 40154530346),
frozen passed on its first attempt (run 40154532135).
**On `8ec8a70`** (round 74): full Rust gate passed (run 40162780501),
frozen passed on its first attempt (run 40162782011); PR #104 merged
rounds 71 to 74 into main (`a6640b6`).
**On `15acff5`** (round 75): full Rust gate passed (run 40171629179);
frozen failed only GNU's known `eglot-test-rust-completion-exit-function`
oracle flake (7,669 passed, 1 unexpected), and its rerun passed (run
40171630795, attempt 2).
**On `b8c4e84`** (round 76): full Rust gate passed (run 40177432296),
frozen passed on its first attempt (run 40177433903).
**On `0cde30d`** (round 77): full Rust gate passed (run 40188636495),
frozen passed on its first attempt (run 40188638449). Locally the gate
(all groups, gate profile, `LANG=C LC_ALL=C`, the known as-root and
image tests skipped) passed 3,202 of 3,202 before the
`accept-process-output` grace fix, after which the 101 process tests and
78 accept tests pass. On round 75's commit the local gate passed 3,201 of
3,202; the failure was the anti-cheat scan catching
`rename-auto-save-file` called through the native dispatcher, now called
through its function cell, after which the anti-cheat tests pass. Run
the local gate with CI's `LANG=C LC_ALL=C`: the oracle tests start GNU
with the inherited environment and their answers were recorded under that
locale. With none set here, `char_charset_family_follows_charset_c` and
`print_prunes_charset_properties_like_print_prune_string_charset` see
GNU put `iso-8859-1` ahead of the non-preferred head; under `C.UTF-8`,
`new_file_coding_lifecycle_matches_gnu_after_save_and_revisit` sees GNU
pick `utf-8-unix` for a new file. Locally,
`batch_startup_image_round_trip_or_explicit_native_image_limit`
fails in this container on `4008098` already (the restored image's first
native function in `mapatoms` order differs) and passes in CI, like the
as-root `byte_compile_file_reports_an_unwritable_target_like_gnu`,
`save_buffer_skips_unmodified_and_unchanged_files` (root writes the
read-only file instead of being asked) and
`file_writable_p_is_nil_for_missing_files_in_unwritable_directories`, and
`print_prunes_charset_properties_like_print_prune_string_charset`, where
this container's GNU build itself disagrees with the expected text (under
`default` it prints the Latin-1 string without its `charset` property);
`char_charset_family_follows_charset_c` fails the same way on its GNU side
(this build answers `iso-8859-1` where the expected text, CI's, has
`unicode`), and
`process_tls_state_survives_collection_before_and_after_async_handshake`,
whose asynchronous handshake this container's GNU leaves in `connect`
(CI's completes it). `suspended_bytecode_retains_operand_and_unwind_roots`
failed now and then under load until round 47: a cons block's address,
left in a stack slot by the allocator's bookkeeping, kept the block's
first cell, which the weak key sometimes was (see round 47).
**On `fe2b73e`** (round 78): full Rust gate passed (run 40193461329);
frozen failed only GNU's known `eglot-test-rust-completion-exit-function`
oracle flake (7,669 passed, 1 unexpected), and its rerun passed (run
40193463671, attempt 2).
**On `d5e95f9`** (round 79): full Rust gate passed (run 40199436854),
frozen passed on its first attempt (run 40199438907); PR #106 merged
rounds 77 to 79 into main (`4fc869e`).
**On `0a98371`** (round 80): full Rust gate passed (run 40213825220),
frozen passed on its first attempt (run 40213827546).
**On `1d47612`** (round 81): full Rust gate passed (run 40221555021),
frozen passed on its first attempt (run 40221557268); PR #107 merged
rounds 80 and 81 into main (`e5a16c0`).
**On `b2c9e9d`** (round 82): full Rust gate passed (run 40230698235),
frozen passed on its first attempt (run 40230700102).
**On `cf7b8ef`** (round 83): full Rust gate passed (run 40238054634),
frozen passed on its first attempt (run 40238056589). survey83 (the 515
GNU test files under round 83's build): each file gives GNU's `Ran' line
and exit code.
**On `457fa3e`** (round 84): full Rust gate passed (run 40240886483),
frozen passed on its first attempt (run 40240888419).
**On `ed0631e`** (round 85): full Rust gate passed (run 40243227941);
frozen failed only GNU's known oracle flake (Emaxx 7,670 passed with 0
unexpected, GNU 7,669 passed with 1 unexpected), and its failed job was
rerun passed (run 40243230215, attempt 2).
**On `20204fe`** (round 86): full Rust gate passed (run 40246689049),
frozen passed on its first attempt (run 40246690701).
**On `6f55171`** (round 87): full Rust gate passed (run 40250303048);
frozen failed only GNU's known oracle flake (Emaxx 7,670 passed with 0
unexpected, GNU 7,669 passed with 1 unexpected), and its failed job was
rerun passed (run 40250305113, attempt 2).
**On `b69436b`** (round 88): full Rust gate passed (run 40252770662),
frozen passed on its first attempt (run 40252773076).
**On `2c64adc`** (round 89): full Rust gate passed (run 40255536669);
frozen failed only GNU's known oracle flake (Emaxx 7,670 passed with 0
unexpected, GNU 7,669 passed with 1 unexpected), and its failed job was
rerun passed (run 40255538925, attempt 2).
**On `8dd2e58`** (round 90): full Rust gate passed (run 40259071504),
frozen passed on its first attempt (run 40259074201); PR #108 merged
rounds 82 to 90 into main (`44f1c76`).
**On `171d0c0`** (round 91): full Rust gate passed (run 40265340023),
frozen passed on its first attempt (run 40265342433).
**On `1e7203d`** (round 92): full Rust gate passed (run 40273030577),
frozen passed on its first attempt (run 40273033064).
**On `827b8eb`** (round 93): full Rust gate passed (run 40278885996),
frozen passed on its first attempt (run 40278888554).
**On `198c173`** (round 94): full Rust gate passed (run 40285385018),
frozen passed on its first attempt (run 40285394292).
**On `a79bf68`** (round 95): the Rust gate's first attempt lost its
runner in the integration stage after the library groups and binaries
had passed, and the re-run passed (run 40294000763, attempt 2); frozen
passed on its first attempt (run 40294012283).
**On `c75f77f`** (round 96): frozen passed on its first attempt (run
40304554801); the Rust gate passed (run 40304550655).
**On `c2ebbb7`** (round 97): frozen failed only GNU's known oracle flake
(`eglot-test-rust-completion-exit-function` failed in GNU; Emaxx 7,670
passed with 0 unexpected, GNU 7,669 with 1 unexpected), and its failed
job's rerun passed (run 40313513267, attempt 2); the Rust gate passed
(run 40313509629).
**On `2a230c6`** (round 98): full Rust gate passed (run 40322773968),
frozen passed on its first attempt (run 40322787666); PR #109 merged
rounds 91 to 98 into main (`2b01388`).
**On `7130092`** (round 99): full Rust gate passed (run 40415810583),
frozen passed on its first attempt (run 40415814171).
**On `2d968ce`** (round 100): full Rust gate passed (run 40421848778),
frozen passed on its first attempt (run 40421854122).

**Findings recorded, not fixed:**
- **Where a test file's setup goes, mutator and collector apart (round
  100).** With the frozen runner's command line, each stage's
  instructions less its collections (`garbage_collect' in GNU,
  `emaxx_native_gc_collect' here):

  | stage | Emaxx | GNU | gap |
  |---|---|---|---|
  | startup (`--batch --eval nil') | 162.1M | 98.5M | +63.6M |
  | loading ert | +156.2M | +123.0M | +33.2M |
  | loading json | +9.0M | +7.6M | +1.4M |
  | loading the runner (203 lines of .el) | +85.0M | +62.5M | +22.5M |
  | loading cal-french-tests.el | +78.8M | +52.2M | +26.6M |

  The totals hide this: GNU collects three times in that setup for
  163.8M (one in startup itself, 52.6M, below init_faces_initial), Emaxx
  twice for 71.7M, so the setup's totals are 507.6M against 562.8M while
  the mutators are 343.8M against 491.1M. Both allocate alike (the
  runner's load adds 12.3K conses here against 14.9K, 5,795 vector
  cells against 5,815, 68.7K string bytes against 68.6K); only where the
  collections fall differs. Startup is the largest gap. GNU's 98.5M is
  31.7M of dynamic linking and 53.4M of `pdumper_load' (597,762
  relocations at 37 instructions, 7.9M opening 92 .eln files, 7.0M for
  6,056 `dynlib_sym's), about 13M of Lisp. Here `pdumper_load' is
  120.3M: `Loader::load' 82.8M (the cons, string and vector areas' 666K
  relocations at about 70 instructions with their checks;
  `symbol_of_record' 7.3M for 18,172 symbols; `value_at' 5.3M, most of
  it the symbols' three fields; `install_root_group' 4.9M;
  `image_c_string' 4.6M for 11,802 native subr names), `load_dumped_code'
  24.5M, `install_symbol' 5.0M (its `install_watchers' 1.8M, a search of
  the watcher list per symbol). The symbols are the next part to map in
  place (their cells in the allocator's symbol blocks, each word a
  relocation, the initial obarray's buckets restored), about 17M of the
  gap; the relocation loop itself is the next.
- `features`, `values` and `top-level` are special variables in Emaxx
  (`special-variable-p` answers t) where GNU unmarks them after their
  DEFVARs (`Fmake_var_non_special (Qfeatures)` in fns.c, `values` in
  lread.c, `top-level` in keyboard.c: "Let people use lexically scoped
  vars named `features'"). So a lexical `(let ((values ...)) ...)` binds
  dynamically in Emaxx and lexically in GNU, and
  `(let ((features '(foo))) (featurep 'foo))` is t in Emaxx, nil in GNU
  under lexical binding. `GNU_LREAD_SPECIAL_VARIABLES` in eval.rs lists
  `values` with the specials; the fix is GNU's unmarking after
  definition, checked with the image.
- A condition symbol with no `error-conditions` (never `define-error`ed):
  GNU's handlers do not catch it except through `t`, so
  `(condition-case e (signal 'my-err '(3)) (my-err ...))` leaves it
  uncaught ("peculiar error: 3"); Emaxx's `condition_symbol_matches`
  matches a handler named exactly like the signal when the list is empty,
  contrary to its own comment. The same holds through native code.
- `backtrace--locals` on the frame of a `let` inside an interpreted
  lexical function: GNU reports the lexical environment there (the
  function's parameters and the `let`'s variables, `((a . 1) (b . 2) (z .
  5))`), Emaxx reports nil; and frame 0 without BASE signals
  `(wrong-type-argument wholenump -1)` in GNU where Emaxx answers. Emaxx
  reports a call's parameters on the call's own frame, as before the
  parameters moved beside the frames.
- GNU's `Fapply` with a one-element spread list stores that element into
  its own argument array (`args[nargs - 1] = XCAR (spread_arg)`), which is
  the `apply` frame's argument vector: a backtrace taken inside shows
  `(apply f 3)` for `(apply f '(3))`. Emaxx's frame keeps `(3)`.
- Loading prints "Loading FILE..." through Lisp `message`, so in batch it is
  also logged in `*Messages*` and obeys `inhibit-message`; GNU's
  `message_with_string` writes it only to stderr in batch. Switching needs a
  separate quiet mode for `batch.rs`'s image reconstruction, which relies on
  `inhibit-message` to keep that chatter out of stderr.
- GNU 30.2's `mapconcat` crashes (segfault) when FUNCTION shortens the list
  it maps (`eassert (nmapped == leni)` is off in release builds and the
  uninitialized tail is concatenated); Emaxx concatenates what was mapped.
  Exclude that case from oracle comparisons.
- Library tests share one process heap and are not thread-safe: run the gate
  locally with `--test-threads=1`, as CI does; parallel runs crash in string
  data.
- Keeping the interpreter state behind `Option` costs a test and branch per
  field access (about 18 of the 108 instructions `eval_sub` spends per list
  form). Removing the check would make a parked shell's use undefined
  behavior instead of a panic, so it stays.

**Where the remaining gap is** (instruction counts per unit; the native
and byte-code calls on the Rust 1.99.1 round's final build, the rest on
`41be443`):
- **Wall time** parts from the counts, and the criterion is wall time. A
  native pilot of all sixteen on the small-block build with the exchange
  removed (a quarter of each contract n, two rounds of three samples
  after a warmup, the minimum; `core-perf-run` in each editor without
  valgrind) read, Emaxx/GNU: lexical 0.84,
  calls 0.86, bindat 0.98, native-execution 1.08, list traversal 1.13,
  sort 1.16, dynamic 1.18, native-to-interpreted 1.31, mapcar 1.33,
  explicit-gc 1.34, interpreted-to-native 1.41, bytecode calls 1.43,
  cons allocation 1.43, undo 1.49, native-to-bytecode 1.50,
  bytecode-to-native 1.76. The calls through native code read about 0.1
  above their counts. On the buffer-list round's build (`0c448c6`), five
  rounds of five samples, the minimum (the median in parentheses where
  it differs by more than 0.05): bytecode-to-native 1.61 (1.78), undo
  1.52 (1.65), native-to-bytecode 1.45, bytecode calls 1.39,
  interpreted-to-native 1.36, cons allocation 1.32 (1.19),
  native-to-interpreted 1.29, mapcar 1.28, interpreted dynamic 1.19,
  sort 1.16 (1.09), explicit-gc 1.10 (1.17), list traversal 1.09 (1.00),
  native-execution 1.03, bindat 1.01, lexical 0.84, calls 0.79. On the
  byte-code entry round's build, the same way: bytecode-to-native 1.59,
  undo 1.57 (1.65), interpreted-to-native 1.35, native-to-bytecode 1.33
  (1.47), cons allocation 1.30, native-to-interpreted 1.29, mapcar 1.19
  (1.26), bytecode calls 1.16, sort 1.16 (1.13), dynamic 1.09,
  native-execution 1.07 (1.19), list traversal 1.06, explicit-gc 1.05
  (1.13), bindat 0.97, calls 0.79, lexical 0.78. On `a56f990`'s plain
  release build (not the profile-guided one), five rounds of five
  samples, the minimum (median where it differs by more than 0.05): undo
  1.50 (1.58), interpreted-to-native 1.31, explicit-gc 1.24 (1.14), cons
  allocation 1.24 (1.19), bytecode-to-native 1.24, native-to-bytecode
  1.22, native-to-interpreted 1.18, mapcar 1.16, sort 1.16 (1.21),
  bytecode calls 1.10, dynamic 1.06, native execution 1.03, list
  traversal 1.01, bindat 0.95, calls 0.79, lexical 0.78. On `3e1cced`
  (the image's cells and string data left out of the sweeps), the same
  way: on its plain release build undo 1.46, interpreted-to-native 1.31,
  cons allocation 1.22, bytecode-to-native 1.19, native-to-bytecode 1.19,
  native-to-interpreted 1.17, bytecode calls 1.13, dynamic 1.11, mapcar
  1.09, sort 1.08, native execution 1.05, list traversal 1.01, bindat
  0.93, explicit-gc 0.91, lexical 0.79, calls 0.73; with the profile
  (`tools/pgo-build.sh`, the timing build) undo 1.26, bytecode-to-native
  1.23, cons allocation 1.22, interpreted-to-native 1.14, bytecode calls
  1.07, mapcar 1.06, native-to-bytecode 1.02, dynamic 1.00,
  native-to-interpreted 0.97, sort 0.96, list traversal 0.95, explicit-gc
  0.93, bindat 0.76, native execution 0.74, calls 0.70, lexical 0.66: six
  of the sixteen above the bound. On `df06b81` with the profile, the same
  way: undo 1.24, bytecode-to-native 1.24, cons allocation 1.18,
  interpreted-to-native 1.16, mapcar 1.11 (1.09 in eight rounds of five
  against `b206a4a`'s profile build, the same, 0.0683 and 0.0685 s
  against GNU's 0.0627), bytecode calls 1.08, native-to-bytecode 1.02,
  dynamic 1.00, list traversal 0.94, explicit-gc 0.94, native-to-interpreted
  0.94, sort 0.95, bindat 0.76, calls 0.66, lexical 0.66, native execution
  0.63: six above the bound. Collections cost Emaxx more time than GNU at fewer
  instructions: 19 to 22 ms each in the workloads against GNU's 12 to 16,
  an otherwise empty one 15.0 ms against 9.8 (11.5 after the compaction
  change), though marking a long list costs less a cell (6.6 ns against
  9.6). The difference is memory traffic: L1 data misses 736,000 against
  297,000 (callgrind `--cache-sim=yes`), nearly half in the sweeps, which
  read every string, vector and symbol cell, the image's included,
  where GNU's sweeps never touch its dump region and its cons sweep reads
  mark bitmaps; what is left after the compaction and guard-count changes
  is the string sweep's mark reads (58,000), vectors (about 19,000 since
  the buffers are listed) and symbols (19,000), each reading the image's
  cells as well, which GNU's sweeps never do, and marking (301,000 against
  GNU's 281,000). Since the sweeps leave the image's unchanged blocks
  unread (above), a collection's L1 misses are 381,000 and an explicit
  collection is GNU's time or less; marking (302,000 L1 misses, 297,000 of
  a modelled 1 MB L2 against GNU's 225,000) and, where heap strings die,
  the string data compaction over the image's data are what remain.
  Cons allocation's samples collect once each in
  Emaxx (0.021 s of 0.11) and never in GNU: the harness's
  `(garbage-collect)` before each sample still finds the previous
  sample's 500,000-cell list through GNU's conservative stack scan, so
  with `gc-cons-percentage` 1.0 GNU's next threshold is about 8 MB;
  Emaxx frees the list and its threshold stays at 800,000 bytes. At the
  contract's size (2,000,000 cells a sample, the profile build, two
  rounds of three samples) GNU's samples after the warmup collect never
  (the warmup three times, about 19.5 ms each) and Emaxx's twice each
  (about 17 ms each): 0.3363 s against 0.3021, 1.11, all of it the two
  collections. Emaxx's collections are the cheaper; the count is GNU's
  conservative stack scan finding the previous sample's list, so this
  case's gap is the retention, which a precise collector does not
  reproduce. Emaxx
  collects less often than GNU in lexical, calls, sort, bindat and the
  native-to-* cases (its census of live bytes is larger). Mapcar
  (collection about half of each sample in both editors) misses the
  last-level cache where GNU does not: at sixteen units a sample
  (callgrind's 34 MB last-level cache) Emaxx takes about 66,000
  last-level misses a unit against GNU's 800, in marking (16,700 reads a
  unit), the sweep (7,700 reads, 16,100 writes) and the mutator's own
  conses (17,800 writes, about half of a result list's lines). Each unit
  allocates about 71 new 32 KB cons blocks (no collection between most
  units; the dead results' blocks go back to `free` at the next one),
  and an Emaxx cons costs 24 bytes (16 of payload and an 8-byte
  weak-reference serial in the block's metadata) against GNU's 16 and a
  mark bit, so a cycle's churn is about half again GNU's; page faults
  are not it (past startup, 8,300 against GNU's 6,800 a run). Those
  misses are mostly sequential streams, which the hardware prefetches:
  an experiment build without the serial (2,000 cells a block instead of
  1,350, weak upgrades not checked) ran mapcar 0.0827 → 0.0814 s, cons
  allocation 0.0963 → 0.0961 and list traversal 0.145 → 0.154 (layout),
  so the serial's cost is not worth a redesign of weak host references,
  and callgrind's last-level figures over-predict sequential traffic.
  Measure wall time with the counts for any change to collection,
  allocation or the native call path.
- **Native calls** (bytecode-to-native 1.27): 493 instructions an
  iteration against GNU's 388, the byte-code loop below GNU's (259
  against `exec_byte_code`'s 339 with its call setup). A Bcall into
  native code nested in an active call costs 222 in `call_native_nested`
  where GNU's Bcall and `funcall_subr` spend about 100: the prologue and
  epilogue (16), the entry and arity tests (about 25), the nested call's
  and the collection's tests (13), Ffuncall's frame record, its pop and
  the depth (about 35), the native call frame's push and pop with its
  balance and pending-error tests (about 30), the handler lists' two
  reconciliations (14), the escape record and the call (17), the
  argument registers by the count (about 12) and the active call's
  record (8). What is left beyond GNU's is the bookkeeping the Rust
  callers of native code require (the call frame, the escape record,
  the active call and the handler reconciliation), about 80, and the
  arm's own tests in the loop.
- **Byte-code calls** (bytecode calls 1.19): 558 instructions an
  iteration against GNU's 468 (586 on the dispatch copy's build), all in the verified loop
  now that the frame entry and the return are inline: its ops at GNU's
  cost or below, and a call and return about 210 against GNU's 100 or so
  (the plain-entry tests, the entry cache's check of the closure's four
  words, the 64-byte backtrace record and its pop, the 88-byte `BcFrame`
  and the 48-byte `ByteCodeObject` copied into and out of it, the code
  string read again on each switch). GNU's frame lives in the byte-code
  stack and its running state in registers.
- **Source interpreter** (lexical 1.07, calls 1.07, cons allocation
  1.11, sort 1.17): a list form whose head holds a C primitive costs
  `eval_sub` about 233 instructions (cons allocation; GNU's 173 with
  `list_length`'s 53 apart): the entry tests, `maybe_gc` and the depth
  (about 30), the head's cell and the subr's arity (about 20), the frame
  record, its argument update and its pop (about 35), the argument
  count, and for each argument its inline evaluation. An interpreted
  call (interpreted calls, 3,561 instructions an iteration against GNU's
  3,315) still goes through `eval_call`'s resolution and its
  `funcall_interpreted_lambda` (300), and
  `maybe_gc` at each list form (two thread-local reads and the owning
  runtime's trigger, where GNU tests one global counter). Sort is mostly this path too
  (`eval_sub` is 44% of it since the port, 118 million instructions a
  unit at one and two units); before the port
  `eval_sub` spent about 400 instructions of its own on each of 473,000
  list forms a unit, where GNU's spends 111 on each of 548,000 forms
  (symbols included); per form the backtrace record costs about 65 (the
  push 20, the argument update 27, the pop 12, the debugger-context test
  6), the function cell's resolution 32, the argument count 16 and the
  subr dispatch 19, and the rest is spread over the entry tests and
  moves. Sort's buffer side
  (rope chunk lookups, `compare-buffer-substrings`, insertion and its
  change hooks) is about 25 million instructions a unit
  against GNU's 10 million.
- **Ffuncall into a subr** (mapcar 1.45): about 200 per call against
  GNU's 147 across Ffuncall, funcall_general and funcall_subr;
  `Value::cons` is 37 against `Fcons`' 27 (the per-cell serial, the
  allocated and mark bits, and a live count GNU does not keep).
- Undo's remainder (1.39 on the signals' round, 1.74 million
  instructions a unit; the figures below are from 1.47, 1.84 million
  against GNU's 1.25 million, on the small-block round's build) is
  broad. Buffer edits from byte code cost about twice GNU's:
  `delete-region` about 5,900 instructions a call against
  `Fdelete_region`'s 2,844 (18 a unit), `insert` about 6,080 against
  `Finsert`'s 4,730 (21 a unit), and `run_change_hooks` 125,000 a unit,
  mostly the hook functions' Lisp. Native code's calls also cost about
  twice GNU's for the same Lisp:
  `cconv-make-interpreted-closure`, run for each interpreted closure,
  takes 146,000 a unit against GNU's 67,000, made up of its `funcall`s
  (101,000 against 48,000), `mapcar` (26,000 against 12,500), `delq`
  (about 660 a call against 211) and `specbind` (793 a call against
  300). Ops run in place: 622 a unit leave the verified loop through
  `op_in_place` and `exec_op` (Bvarref of a symbol that is not plain 92,
  Bvarbind 97 with its 144-byte record pushed by `memcpy`, Bunbind 91,
  and buffer primitives: `goto-char`, `point-min`, `point-max`,
  `gethash`, `get`, `set`, `memq`), each about 190 instructions before
  its own work (the op decoded again, `exec_op`'s frame and match, and
  the loop's re-entry); GNU runs them in `exec_byte_code`'s loop.
  Allocation is about 1,330 blocks a unit, 85,000 instructions in the
  allocator (217,000 before the lists), from copies GNU does not make:
  `string_like` decoding a string to read a flag or compare a name (124
  a unit: `format` five times a call, `get-buffer`, `insert`), `apply`
  copying its arguments twice, `condition-case` copying each clause to
  read its head, a change hook's list copied to test it for nil. At 1.67
  it was about 3,000 calls into the allocator, `memset` and `memcpy` a
  unit, 230,000 to 290,000 instructions in malloc and free as their state
  varied (vectors' growth 530 calls, string coding 390, special bindings'
  records 240, errors' condition types 106). `eval_sub` is 140,000 a
  unit against GNU's 90,000 (181,000 at 1.67). Measured at 1.67: the
  window configuration snapshot copies its lists on each restore (about
  19,000), `ert-set-test` grows a vector each run (about 15,000), plain
  subr calls (`call_plain` about 196 a call, 340 a unit), byte-code frame
  entry (290 a call, 165 a unit) and native code's subr calls
  (`invoke_subr`, `invoke_native_funcall` 65,000).

## How the work is split

- **Linux (cloud sessions):** build GNU 30.2 at `636f166c` locally with the
  configure options from
  `src/lisp/native_comp/generated_native_subrs_x86_64_unknown_linux_gnu.rs`
  (as `frozen-run.yml` does) and place it at `../emacs`. The native ABI table
  must match exactly. A local build's binary hash differs from the CI lock, so
  authoritative Linux Rust/frozen/timing results come from `frozen-run.yml`.
- **macOS host:** PR87's closing items, and macOS validation at milestones.

## Validation tiers (agreed 15 October 2026)

1. **Each draft:** `cargo fmt --all -- --check`, `cargo check --locked
   --all-targets --all-features`, `cargo clippy --locked --all-targets
   --all-features -- -D warnings`, `git diff --check`, the affected controls in
   the gate profile (and release when the change is release-sensitive), and the
   affected ordinary GNU comparisons.
2. **Each checkpoint merge:** full Linux Rust and frozen runs through CI, plus
   Linux timing pilots.
3. **macOS frozen, terminal and Tramp:** at milestones, at the end, or when a
   change touches platform-specific code.

Rejected experiments get a short note and their patch; full evidence packages
are for merges, and raw samples may live in CI artifacts. Unfavorable results
and failures are still recorded.

## Next steps, in the agreed order

1. Merge PR87 after its macOS closing items; then validate and merge the
   counter branch.
2. **Performance, worst ratios first.** Outside the locked sixteen
   (bullet "Beyond the sixteen workloads") the gaps are larger than any
   workload's margin, so they come first now: syntax scanning under
   font-lock is at GNU's time for subr.el since round 51 ported
   syntax.c's scanners, 1.1 to 1.5 times in simple.el and python.el,
   1.6 to 1.9 times in C mode (cc-engine's Lisp: the interpreter, the
   byte-code VM and the matcher); round 52 put the matcher's syntax
   lookups on the same gl_state and brought case-folded `\b` searches
   and `looking-at` to GNU's time (its instructions are still 2.8 times
   GNU's under an interpreter loop that dominates both: rope slicing and
   chunk lookups, the buffer-local `parse-sexp-lookup-properties` read);
   `forward-word` is 1.2 times. Round 51's frozen run (gate profile)
   puts the slowest files where GNU takes 100 ms or more at keymap-tests
   11.6 times (1.5 since round 53 ported `describe_vector`), data-tests
   3.6, marker-tests 3.2, help-fns-tests 3.1 (round 55 took
   `describe-function` from 2.4 to about 1.2 times GNU's, round 57 raw
   `documentation` from 20 to 1.25 times and `substitute-command-keys`
   below GNU's; `documentation` with substitution is still 1.6 to 2.1
   times, the byte-code of help.el's substitution loop) and simple-tests 2.75,
   summed test time at 1.21 times and setup at 2.30; then process start
   (0.11 s against 0.05 s on this host; `shell-command` tests that start
   Emacs fifty times are 2.5 times slower for it: 400 million
   instructions against GNU's 166 million, 259 million of them in
   `pdumper::Loader::load`, which rebuilds every image object where GNU
   maps the dump and relocates it; the cost is spread over the kinds,
   strings 28 million, symbols 16, `value_at` about 50, with no single
   hot spot, so it wants the image mapped in place rather than another
   trim; round 95 maps the conses in place, round 96 keeps the symbols'
   host text in the image and files them in lread.c's obarray, and round
   97 maps the strings in place and round 98 the vectors, closures,
   records and char tables; the symbols and the other records are still
   rebuilt,
   and the initial obarray's buckets are filled again as the symbols are
   read rather than restored); the encode side of coding.c (bullet "Round 56"); and text
   properties kept in one flat vector of spans (each edit shifts it; GNU
   keeps intervals).
   The host is a different machine
   since `aa68c68` (bullet "The host changed under the session"). On it,
   with round 25's jump padding, the contract's comparison of the
   profile-guided build reads: undo 1.042 [0.876, 1.110],
   bytecode-to-native 0.953 [0.898, 1.108], bytecode calls 0.928
   [0.819, 1.062], interpreted-to-native 0.927 [0.866, 0.998] and cons
   allocation 0.821 [0.735, 0.946], the last two passing (bullet "Round
   25"). GNU's own rounds range over ±25%, so the medians have to come
   to about 0.90 for the intervals to clear 1.03. Instructions a unit in
   the plain build before the padding, against GNU: interpreted-to-native
   2,429 (2,387), undo 1,300,268 (1,252,071), cons allocation 5,056
   (5,215), bytecode-to-native 391.6 an iteration (388), bytecode calls
   437.8 (468). With the padding they are 2,477, 1,341,618, 5,158, 406.5
   and 449.9. Round 26 pads every kind of jump with prefixes (bullet
   "Round 26"): a fifteen-round pilot of the profile-guided build reads
   bytecode-to-native 0.910, bytecode calls 0.916, interpreted-to-native
   0.891, cons allocation 0.823 and undo 1.110. Undo is now the worst
   by far. Round 27 took undo's plain-build count to 1,298,608 a unit
   (bullet "Round 27"); round 28 undid its Bmemq arm, which had moved the
   profile-guided layout of the loop and cost bytecode-to-native about
   15% (bullet "Round 28"). The contract-like comparison of round 28's
   profile-guided build over all sixteen workloads passes thirteen; over
   are undo 1.034 [0.966, 1.132], bytecode calls 0.975 [0.897, 1.066]
   and interpreted-to-native 0.917 [0.860, 1.215] (two of its nine
   rounds at 1.39 and 1.22, the host's). Undo's excess is the code
   between collections, not the collections, and it is breadth: twice
   GNU's distinct code a unit, a third of it in the byte-code VM
   (`run_verified` and `op_in_place`), the evaluator and the native
   code's calls into subrs, the rest spread over the primitives (bullet
   "Where undo's wall time goes"). Compiler settings do not move it;
   each of those paths has to touch less code. Rounds 29 to 32 took
   about 18,500 instructions a unit out of undo's warm paths (special
   forms' frames, lookups and string compares by name, a copied window
   index). A compatibility difference found in round 30 is open: GNU's
   `features` is not special (fns.c's `Fmake_var_non_special`), Emaxx's
   is, so a lexical `let` of it changes what `featurep` reads. For the work both editors do, Emaxx runs about 14% more
   instructions than GNU (GNU's totals include 150,000 a unit of
   window-size Lisp Emaxx does not run), most of it in the byte-code
   VM's slow ops (`op_in_place`, 140 a slow op) and the native runtime's
   calls into subrs (bullet "What undo's comparable work costs"); its
   excess is also code footprint
   (bullets "Where the profile-guided build stands" and "Undo's time,
   split": the same instructions as GNU, twice the distinct code, a cycle
   model that matches the wall). Shrinking the distinct code an
   iteration runs is the lever; arms that save instructions by adding
   code to `run_verified` did not pay in the plain build's counts, but
   judge them in the profile-guided build too, which places their spills
   off the hot paths (round 37's cost bytecode calls 13 instructions an
   iteration in the plain build and 1 in the profile-guided one). Interpreted-dynamic (0.881) is
   over on the host's noise alone. Earlier, undo's next step was its
   slow byte ops' way out of the verified loop:
   767 a unit through `op_in_place`
   at 120 instructions each (a second decode and dispatch, a large
   prologue, the frame's `admits` test on the way back), where GNU runs
   them inline. On the earlier host, by the contract's comparison on
   `3f7bff2`'s profile-guided build (the bullet "The contract's comparison
   on all sixteen"): bytecode-to-native 1.023 [0.968, 1.064], undo 0.991
   [0.944, 1.109], bytecode calls 0.943 [0.910, 1.049], interpreted-to-
   native 0.942 [0.910, 1.026], cons allocation 0.929 [0.871, 1.043], the
   other eleven at most 0.85. A round's ratio ranges over about ±10% on
   this host, so a median near 0.92 or below is what brings the
   interval's upper end to 1.03. Since then the MANY subr path took
   interpreted-to-native from 2,934 to 2,711 instructions a unit (GNU
   2,387). The byte-code loop is µop-bound (bullet "Where a byte-code
   iteration's time goes"): bytecode calls runs fewer instructions, taken
   branches and stack accesses than GNU's loop, and the profile-guided
   build's register allocation, with more spills, is what makes it slower
   there than the plain build; bytecode-to-native's whole excess is its
   call op, 275 instructions against GNU's 115 (its other ops take half
   of GNU's). Since then bytecode-to-native is 393.0 instructions an
   iteration against GNU's 388 (its call op 245 around the callee, bullet
   "A nested native call's frame popped by its length"). Undo runs
   1,330,595 instructions a unit against GNU's 1,252,071, spread over the
   evaluator, the slow byte ops (767 a unit), the allocator and 73
   entry-cache misses (closures made afresh). Round 37 runs undo's slow ops
   that run no Lisp in the loop (bullet "Round 37"): 150 a unit leave it
   now, and undo runs about 1,170,000 instructions a unit in the plain
   build, below GNU's. What is left of its time is the instruction
   cache: 309 KB of code run each unit against GNU's 196 KB, and 78% more
   first-level misses (bullet "Where undo's cycles go now"), most of them
   in the VM's and the evaluator's large functions.
   (Older wall figures, before these rounds: bytecode-to-native 1.54,
   undo 1.35, mapcar 1.29, cons allocation 1.22, interpreted-to-native
   1.19, bytecode calls 1.10, explicit-gc 1.09.) The profile helps the interpreter and
   the native callbacks most and the byte-code loop and the collector
   least, so those (and undo's in-place ops) are where code changes now
   count most. A wall ratio
   well above its instruction ratio points at stalls callgrind does not
   count, such as the store-forwarding failures of the last round. Per
   iteration bytecode calls runs 65 conditional branches against GNU's 28
   and bytecode-to-native 70 against 28 (callgrind `--branch-sim=yes`;
   indirect ones 17 against 17 since the dispatch copy), the excess in
   the call path's tests. The remaining costs are listed
   under "Where the remaining gap is": the native call layer needs
   structural work (a lighter native boundary: inlining `invoke_fixed` into
   `call_plain` measured 20 instructions a call worse), and the byte-code
   call less now (frames kept in the byte-code stack, as GNU's are, instead
   of a `BcFrame` vector holding copies of the decoded object; a cache of
   the decoded closure saves only when it hands the program out by
   reference, as `entry_program` does); in the byte-code loop, an arm that
   reads its opcode again or keeps a second copy of the pc costs every op
   (see the dispatch bullet above). A compiler update can turn a compare
   into a library call (see the Rust 1.99.1 bullet): look for `memcmp` and
   `bcmp` in the profiles after one. The interpreter and funcall paths are
   spread over many small steps, each worth about 1%. The source
   interpreter's subr calls take their own path now (about 233 instructions
   a list form against GNU's 173 plus `list_length`'s 53); what is left
   there is the frame record (about 35 a form against record_in_backtrace's
   15 or so), the entry tests and `maybe_gc`, and an interpreted call's way
   through `eval_call` (a direct path for it measured worse, see the
   eval_sub bullet).
   Changes to `eval_sub`'s inlined body move its register allocation;
   measure interpreted calls and lexical with each. Micro-changes measure within
   about 1% of noise: keep only what moves the counts, record the rest.
   Iterate with instruction counts (callgrind) on the locked workloads for
   both editors; timing pilots at merges. Remaining adapters that show up in
   profiles are fixed as performance work.
3. **Remaining architecture and ownership:** the `vector-literal` head that
   carries vectors through lists, which Lisp data can forge (the mapcar
   bullet: `list`, `copy-sequence`, `reverse`, `butlast` and `take` turn a
   list headed by that symbol into a vector); symbol host-name, buffer-local and
   function adapters; text-property intervals (also needed for
   `intervals-consed`); pure storage (check what the pinned GNU revision
   actually does first); public Rust ownership, multiple editors, descriptor
   lifetimes and reclamation; the September 21 audit findings.
4. **Final validation and calibrated performance** once, on the final source:
   full Linux/macOS gates and the native, module, dump, startup, thread and GC
   contracts; calibration (the GNU self-comparison must resolve) and the
   nine-round run on a quiet host; the final adversarial review.

## Remaining full-goal requirements

Shared compact object authority without remaining adapters; honest physical
accounting (string data, allocator overhead and capacity, intervals); preserved
survival and reclamation with sound public ownership; profile-driven VM/call
work; the final adversarial review; zero-warning Linux/macOS validation and
pinned compatibility; and the **locked 16-workload GNU performance criterion:
every workload's upper 95% bound at or below 1.03× GNU**, with calibration,
nine rounds and real allocation counters. No GNU-parity claim exists.
