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
Emaxx, 1 for the oracle); its failed job is re-run once (attempt 2).
Locally,
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
`unicode`).

**Findings recorded, not fixed:**
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
2. **Performance, worst ratios first.** By wall time on `3c034cc`'s
   profile-guided build (pilot sizes, minimum ratio; the bullet "Profile-
   guided wall time on `3c034cc`" has the medians): bytecode-to-native
   1.23, undo 1.24, interpreted-to-native 1.18, cons allocation 1.15
   (GNU's start-of-sample collection keeps the previous list, see "Cons
   allocation's collection, measured again"), mapcar 1.06, bytecode calls
   1.05, and the other ten at or below 1.0. By instructions on the
   line-table build: undo about 1.30, interpreted-to-native 1.29,
   bytecode-to-native 1.26, native-to-bytecode 1.24, mapcar 1.19,
   bytecode calls 1.11. Where each case's instructions go is under
   "Where the byte-code call and return go" and the native call bullets.
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
