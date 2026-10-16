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
| **Task branch** `claude/awesome-newton-yk6sae` | Stacked on PR87: allocation counters and forwarded counter variables, actual header charges, image objects outside the GC census, char-table store counts for regexp caches, and a performance round on undo, sorting and the call path (below). |

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
passes in CI. CI's full gate and frozen runs are the merge evidence.

Instruction ratios (Emaxx/GNU per unit) at the start of this round and now:

| Case | Before | Now |
| --- | --- | --- |
| upstream-undo (10 units) | 5.88 (4.33 at 2 units: the log grew) | 2.35 |
| upstream-sort | 3.26 | 2.09 |
| interpreted-calls | 2.10 | 1.25 |
| interpreted-lexical | 2.04 | 1.26 |
| interpreted-dynamic | 2.14 | 1.37 |
| mapcar | 2.95 | 1.46 |
| bytecode-calls | 3.30 | 1.87 |
| bytecode-to-native | — | 2.64 |
| interpreted-to-native | — | 1.64 |
| native-to-interpreted / -bytecode | — | 1.55 / 1.73 |
| cons-allocation / list-traversal | 2.24 / — | 1.41 / 1.37 |
| native-execution | — | 1.20 |
| bindat | — | 1.26 |
| explicit-gc | — | 1.01 |

The table is the full run of all sixteen on this round's last commit
(undo, bindat and interpreted calls measured again after its last
change). Undo moves between 2.37 and
2.42 from one build to the next with no code change on its path: the
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
unsuccessful execution (run 38852471256). Locally, `batch_startup_image_round_trip_or_explicit_native_image_limit`
fails in this container on `4008098` already (the restored image's first
native function in `mapatoms` order differs) and passes in CI, like the
as-root `byte_compile_file_reports_an_unwritable_target_like_gnu` and
`save_buffer_skips_unmodified_and_unchanged_files` (root writes the
read-only file instead of being asked), and
`print_prunes_charset_properties_like_print_prune_string_charset`, where
this container's GNU build itself disagrees with the expected text (under
`default` it prints the Latin-1 string without its `charset` property).

**Findings recorded, not fixed:**
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

**Where the remaining gap is** (instruction counts per unit on `41be443`):
- **Native calls** (bytecode-to-native 2.63): a Bcall into native code
  costs about 400 instructions in the native layer (`invoke` 142, the
  active-runtime lookup and arity check about 60, `finish_call` 48,
  `setjmp` about 38, the execution guard 17, the trampoline 28) where
  GNU's `funcall_subr` spends about 50, plus about 300 in `bytecode_call`
  and the code string's re-check after it. What is left is the design: thread-local swaps,
  the guard, the call-frame record and the per-call `setjmp` that Rust
  frames between the native code and its caller require.
- **Byte-code calls** (bytecode calls 1.87): 863 instructions an
  iteration against GNU's 468: the verified loop 440 (its ops about as
  GNU's, plus about 30 a switch to re-read the code string and check its
  verification), the in-loop frame entry 302 (the plain-entry tests,
  `for_call`'s field checks, the verification cache, the 64-byte
  backtrace record, the 88-byte `BcFrame` and the copies of the
  48-byte `ByteCodeObject` in and out of it) and the return 122. GNU's
  setup_frame and Breturn together are about 100: its frame lives in the
  byte-code stack and its running state in registers.
- **Source interpreter** (lexical 1.26, calls 1.25, sort 2.09): an
  iteration of interpreted calls is 4,022 instructions against GNU's
  3,315. What is left is spread: parked-shell tests (about 300 an
  iteration; each step that takes the shell after a call tests it again),
  the frame record and its argument update (about 23 and 15 a call),
  `resolve_symbol_call` and the argument count (about 27 and 25 a form,
  close to their minimum), the subr dispatch through `call_with_facts`,
  and an interpreted call's `funcall_interpreted_lambda` (340) with the
  frame detail that records its locals. Sort is mostly this path too
  (`eval_sub` 3.2 times GNU's self cost), plus the buffer comparisons.
- **Ffuncall into a subr** (mapcar 1.45): about 200 per call against
  GNU's 147 across Ffuncall, funcall_general and funcall_subr;
  `Value::cons` is 37 against `Fcons`' 27 (the per-cell serial, the
  allocated and mark bits, and a live count GNU does not keep).
- Undo's remainder is broad: the byte-code slow path (ops the verified
  loop leaves for, Bswitch's `gethash` by name among them), `prim`'s name
  lookup and argument rooting (about 190 a call, 675 calls a unit), the
  string adapters (`string_like` decoding), error matching through
  `Vec<String>` condition names, and allocation (about 400,000
  instructions a unit in malloc and free, the rope's `from_str`
  included).

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
2. **Performance, worst ratios first** (instruction ratios in the section
   above; worst now bytecode-to-native 2.64, undo 2.35, sort 2.09,
   bytecode calls 1.87). The remaining costs are listed under "Where the
   remaining gap is": the native call layer and the byte-code call and
   return need structural work (a lighter native boundary; byte-code
   frames kept in the byte-code stack, as GNU's are, instead of a
   `BcFrame` vector holding copies of the decoded object; slow ops without
   leaving the verified loop); the interpreter and funcall paths are spread over
   many small steps, each worth about 1%. Micro-changes measure within
   about 1% of noise: keep only what moves the counts, record the rest.
   Iterate with instruction counts (callgrind) on the locked workloads for
   both editors; timing pilots at merges. Remaining adapters that show up in
   profiles are fixed as performance work.
3. **Remaining architecture and ownership:** symbol host-name, buffer-local and
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
