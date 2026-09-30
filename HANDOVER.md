# Resume the compact runtime goal here

**Current task-branch candidate: source204's direct cons fields and GNU cons-root validation.**
Read the [cons continuation](docs/runtime-representation-cons-roots-draft.md)
after the complete goal, shared-reader handover and allocator continuation.
Its [audited selected results](docs/handover/2026-09-30-shared-reader-draft/source204-cons-selected-validation-manifest.json)
pass strict zero-warning checks, **596 gate / 596 release tests**, seven focused
controls in each profile, **39 ordinary batch comparisons and one ordinary
terminal fixture**. All 410 source inputs match the task worktree and frozen
`cons-roots/emaxx` checkout. The original purecopy batch attempt remains failed:
both editors produced identical results but no interactive callbacks. The same
fixture and original expectations pass with collecting callbacks in both editors'
real terminal sessions. Two incomplete auditor invocations are also preserved.

[Complete macOS validation has started](docs/handover/2026-09-30-shared-reader-draft/source204-complete-validation-start-manifest.json):
Rust supervisor **43352** and terminal supervisor **43354**. Keep the candidate
checkout, helpers and artifacts frozen while their children run. Source204 is [published as `d186d40b`](docs/handover/2026-09-30-shared-reader-draft/source204-publication-manifest.json).
[Complete Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36764911748) and
[Linux frozen comparison](https://github.com/rayfdj/emaxx/actions/runs/36764918696)
are running on that exact head. Main remains source174; PR #79 remains draft.
The [accounting continuation](docs/runtime-representation-accounting-draft.md)
records a new same-input negative baseline: all seven allocation counters remain
unimplemented, and `memory-use-counts` errors. It explains the symbol, string and
property-storage work that must precede an honest final accounting result.

The preceding source201 allocator now has complete audited results:
**3,056 macOS / 3,068 Linux Rust passes** (two existing ignores each),
**226 terminal scenarios / 686 comparisons**, and **519 Linux frozen files /
7,928 matching outcomes / 1,038 successful processes**. See its
[macOS Rust receipts](docs/handover/2026-09-30-shared-reader-draft/source201-macos-complete-rust-manifest.json),
[Linux Rust receipts](docs/handover/2026-09-30-shared-reader-draft/source201-linux-complete-rust-manifest.json),
[terminal receipts](docs/handover/2026-09-30-shared-reader-draft/source201-complete-terminal-manifest.json)
and [Linux frozen receipts](docs/handover/2026-09-30-shared-reader-draft/source201-linux-frozen-manifest.json).
The frozen comparison preserves **7,670 passes, 47 expected failures and 211
skips** per editor; the latter are not counted as passes. Native artifact identity
and both original reclamation assertions pass in both full Rust gates. These are
source201 results, not complete certification of the new cons repair. Runtime201
was [published as `320d0a2e`](docs/handover/2026-09-30-shared-reader-draft/source201-publication-manifest.json).

Source198's [complete terminal audit](docs/handover/2026-09-30-shared-reader-draft/source198-complete-terminal-manifest.json)
now passes **226 scenarios / 686 comparisons**, and its
[complete Linux frozen audit](docs/handover/2026-09-30-shared-reader-draft/source198-linux-frozen-manifest.json)
matches **519 files / 7,928 outcomes / 1,038 successful processes**. Its full
Linux Rust failure remains open. The [exact retained replay](docs/handover/2026-09-30-shared-reader-draft/source198-exact-retained-failure-manifest.json)
reproduces that failure with the original executable and startup image. The
separate debugger process passes with 498 address-read overflow errors, so the
cause remains unestablished. The [instrumented trace](docs/handover/2026-09-30-shared-reader-draft/source198-instrumented-root-trace-manifest.json)
also passes with different artifacts and cannot clear the failure.
The [new retained-artifact diagnosis](https://github.com/rayfdj/emaxx/actions/runs/36756891923)
uses source201's diagnostic tools with source198's original executable/image.
It records rejected name reads, preserves five separate debugger outcomes and
checks restored image hashes after each process. Its
[audited result](docs/handover/2026-09-30-shared-reader-draft/source198-five-process-exact-diagnosis-manifest.json)
reproduces the ordinary failure; all five debugger children pass without callback
exceptions. The subsequent [environment-controlled diagnosis](https://github.com/rayfdj/emaxx/actions/runs/36758535929)
now reproduces the failure in all five debugger children. Its
[audited traces](docs/handover/2026-09-30-shared-reader-draft/source198-exact-environment-root-diagnosis-manifest.json)
identify a malformed cons pointer from the one-byte cdr `ConsSlot` selector in
`eval_call`. GNU rejects this offset; Emaxx currently accepts it. Adding only
the observer environment variable makes the direct child pass, explaining the
earlier passing debugger layout. The source201 passes do not establish a repair.
Read the new [cons field and root continuation](docs/runtime-representation-cons-roots-draft.md)
next. Source202's two new controls fail on unchanged runtime201; source203's
compiler failure is preserved. Source204 corrects the import and has completed the selected validation described
above. Its original selected supervisor **41205** has exited. The
[focused archive](docs/handover/2026-09-30-shared-reader-draft/source204-cons-field-root-focused-manifest.json)
retains the complete replay-verified patch and all 410 input hashes; the newer
selected archive adds both complete selected profiles and the ordinary comparisons.
Full candidate runs now use the same frozen `cons-roots/emaxx` checkout.

Source200's [453-pass / one-failure gate](docs/handover/2026-09-30-shared-reader-draft/source200-vector-selected-failure-manifest.json)
is preserved. Source201 keeps its runtime and corrects the old record word-count
expectations against GNU, preserving all original sizes and assertions.
Complete candidate validation, the reclamation repair, object/symbol authority,
physical allocation accounting, final audit and GNU performance parity remain open.

**Latest validated checkpoint: source174**, merged into main at `21d20f0e` in
[PR #78](https://github.com/rayfdj/emaxx/pull/78), with the validated tree verified
unchanged from `2a156a5d` (runtime source `178fdf37`). Read the
[complete validation checkpoint](docs/runtime-representation-call-validation-complete.md)
after the complete goal: **3,023 macOS / 3,035 Linux Rust passes**, native
artifact identity on both platforms, **519 Linux frozen files / 7,928 matching
outcomes / 1,038 successful processes**, and all **223 terminal scenarios / 679
screen and filesystem comparisons**. The original full-run reclamation
assertion passes on both platforms. All prior failures and evidence limits
remain preserved in the [portable receipts](docs/handover/2026-09-30-call-validation-complete/manifest.json).

**Reader stage preceding source201: source198's shared command reader and macro state**, described in
the [current draft handover](docs/runtime-representation-shared-reader-draft.md).
It extends the [source178 decoder](docs/runtime-representation-input-decoder-draft.md)
to frame, menu, macro and both live/simulated minibuffer loops. Source185 passes
**424 gate / 424 release controls** and **15 ordinary exact GNU comparisons**.
Source188 removes the remaining simulated reader adapters and passes strict
checks, **426 gate / 426 release controls** and **17 ordinary GNU comparisons**.
Its selected terminal run fails the real menu-bar click scenario and an invalid
new prompt presence assertion; fifteen other scenarios match. Source189 fixes
only that assertion. Source190 removes the duplicate Rust mouse-link translator,
preserves raw event heads and reads live event-kind properties. Its strict
checks pass, but its gate fails: **428 pass / 1 unchanged XTerm failure**.
That failure exposes the nil `posn-at-x-y` stub. Source192 replaces it with
shared display-motion traversal and real window-coordinate lookup, following
source191's preserved compiler failure. Source192 passes strict checks,
**506 gate / 506 release controls**, **21 ordinary GNU comparisons** and
**17 terminal scenarios / 34 screen comparisons**, including the previously
failing menu click. The [closed source192 receipts](docs/handover/2026-09-30-shared-reader-draft/source192-validation-manifest.json)
verify the raw inventories and unchanged inputs.
Source195 removes the copied macro event queue and reads the actual Lisp array
and index, preserves nested state across collection, and clears active macro
execution when an image starts in a new process. Source193 and source194's
compiler failures are preserved. Source195 passes strict checks, **535 gate /
535 release controls**, **25 selected ordinary GNU comparisons** and **17
terminal scenarios / 34 screen comparisons**. An additional unread-wrapper
comparison fails on both source185 and source195; the
[closed source195 receipts](docs/handover/2026-09-30-shared-reader-draft/source195-validation-manifest.json)
preserve that failure alongside the passes.
Source196 reuses `(t . EVENT)` unwrapping in the common pending reader before
a terminal wait. It adds the ordinary GNU fixture and a Rust control with a
terminal poller installed. Strict checks and **536 gate / 536 release controls**
pass, but its ordinary build reused the exact source195 executable and failed
the wrapper comparison. The [stale-build receipts](docs/handover/2026-09-30-shared-reader-draft/source196-stale-validation-manifest.json)
preserve all 26 comparisons, the failed result and the timestamp diagnosis.
The snapshot copier now refreshes changed-file timestamps. A separately named
`source196-fresh` run cleans the Emaxx package and repeats validation on the
identical 401 inputs. Its [audited selected results](docs/handover/2026-09-30-shared-reader-draft/source196-fresh-selected-manifest.json)
pass all strict checks, **536 gate / 536 release controls** and **26 ordinary
GNU comparisons**, including the wrapper fixture. All **17 selected terminal
scenarios / 34 screen comparisons** pass with raw inventories and input hashes
audited. The [publication receipts](docs/handover/2026-09-30-shared-reader-draft/source196-publication-manifest.json)
record published `ece02f95` and [draft PR #79](https://github.com/rayfdj/emaxx/pull/79).
Complete macOS Rust now fails: **2,221 passes / one keyboard-input failure**;
four later groups and both Cargo stages do not run. The
[closed failure and diagnosis](docs/handover/2026-09-30-shared-reader-draft/source196-macos-gate-failure-manifest.json)
retain the exact-binary failure replay and ten matching ordinary GNU/Emaxx
contracts. The failing Rust fixture loads only early Lisp, omitting mouse.el's
global binding; the previous reader concealed that omission with a hardcoded
fallback. [Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36741202335)
also fails that same fixture: **2,229 passes / one failure**, with the same
remaining stages unexecuted. Its [audited raw receipts](docs/handover/2026-09-30-shared-reader-draft/source196-linux-rust-failure-manifest.json)
verify the clean published head and retained binary/image identities.
[Linux frozen](https://github.com/rayfdj/emaxx/actions/runs/36741207263) now
matches **519 files / 7,928 outcomes**, with all **1,038 processes successful**.
The [audited frozen receipts](docs/handover/2026-09-30-shared-reader-draft/source196-linux-frozen-manifest.json)
retain the 47 expected failures and 211 skips separately from 7,670 passes.
Source196's [complete terminal run](docs/handover/2026-09-30-shared-reader-draft/source196-complete-terminal-manifest.json)
now passes all **226 scenarios / 686 comparisons** (658 screens and 28 filesystem
checks), with every raw checkpoint and all 401 source inputs audited. Its full
supervisors have exited; the failed Rust results remain failed.
Complete source195 runs were deferred before launch to use this candidate.
Its complete patch replays all 401 recorded inputs. The candidate is applied in the local
task worktree and is published on `runtime-char-tables` for complete validation.
Main remains source174. Keep the PR in draft until complete results are audited.
A separately packaged source197 draft removes the macro saved-state host
vector and global root registration using GNU's two-cons state and existing
scoped roots. Its [selected runtime results](docs/handover/2026-09-30-shared-reader-draft/source197-selected-validation-manifest.json)
pass **32 gate / 32 release controls and eight ordinary GNU comparisons**,
including normal/error reclamation. The original gate's two failures from a
missing sibling GNU fixture path remain preserved; restoring that path makes
the identical binary and inventory pass. Source198 retains source197's runtime
and is frozen in `macro-saved-state/emaxx`. Its
[separate draft](docs/handover/2026-09-30-shared-reader-draft/source198-startup-fixture-draft-manifest.json)
uses actual batch startup for all ten unchanged keyboard-input assertions and
adds a GNU-verified live bound/unbound mouse-down control. Its 405-input patch
replays exactly. Its [strict checks and three focused gate controls](docs/handover/2026-09-30-shared-reader-draft/source198-focused-manifest.json)
pass, including all ten unchanged keyboard-input assertions, live binding
mutation and macro reclamation. All **542 gate / 542 release controls and 28
ordinary GNU comparisons** now pass, with [raw inventories and artifact identities audited](docs/handover/2026-09-30-shared-reader-draft/source198-selected-validation-manifest.json).
Supervisor 20015 has exited. The exact 405 inputs are now applied to the task
worktree and [published as `7e45db47`](docs/handover/2026-09-30-shared-reader-draft/source198-publication-manifest.json)
on `runtime-char-tables` in draft PR #79. Its
[complete validation launch](docs/handover/2026-09-30-shared-reader-draft/source198-complete-validation-start-manifest.json)
records queue 22463 and full Rust supervisor 22464. The
[selected terminal run](docs/handover/2026-09-30-shared-reader-draft/source198-selected-terminal-manifest.json)
passes all **17 scenarios / 34 screen comparisons**, with raw checkpoints and
input hashes audited. Full terminal supervisor 24260 now passes all 226
scenarios and has exited; its complete audited evidence is linked above.
The [complete macOS Rust audit](docs/handover/2026-09-30-shared-reader-draft/source198-macos-complete-rust-manifest.json)
now verifies **3,051 passes**, two existing terminal ignores and native artifact
identity, with all 405 inputs matching published `7e45db47`.
[Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36748039991) fails with
**2,231 passes / one suspended-bytecode reclamation failure**; its
[raw audit](docs/handover/2026-09-30-shared-reader-draft/source198-linux-rust-failure-manifest.json)
retains the failed child and four unexecuted groups plus both Cargo stages.
A [standalone replay](https://github.com/rayfdj/emaxx/actions/runs/36751309990)
passes with the same executable hash but a newly created, different startup
image. It does not clear the full failure. A
[separate root trace](https://github.com/rayfdj/emaxx/actions/runs/36751314826)
passes with different artifacts. The exact retained replay now reproduces the
failure; its separate debugger pass has the read errors described above.
[Linux frozen](https://github.com/rayfdj/emaxx/actions/runs/36748047488) now passes
the complete audited inventory. Keep PR #79 in draft.
The [vector allocation review](docs/handover/2026-09-30-shared-reader-draft/vector-allocation-rounding-review-manifest.json)
identifies excess 16-byte rounding for eight-byte-aligned payloads. Its ordinary
GNU census comparison already matches, so changing Lisp statistics to include
that padding would introduce a difference. The [source201 allocator candidate](docs/runtime-representation-vector-allocation-draft.md)
removes that padding while preserving the original 40-byte bitmap by indexing
the minimum object footprint. Its selected results and source200's preserved
broad-gate failure are recorded above. It fixes separate GNU pseudovector census
differences and accounts for the larger free-list head table as a remaining cost.
The source199 baseline assertions, failed ordinary comparison and strict Clippy
failure remain preserved. Source201 now occupies the task branch.
Broader reader contracts, symbol authority, allocation accounting, final audit,
complete final-source validation and locked performance parity remain open.
**The full goal remains active; this checkpoint is not completion.**

**Previous main checkpoint, source161:** [PR #77](https://github.com/rayfdj/emaxx/pull/77)
merged at `e9bbd598e14003d82ff23be4abcf05b0489164ca`. It passes complete Rust gates:
**3,013 macOS / 3,025 Linux**, with two existing terminal ignores each and native
artifact identity passing. Its [Linux frozen comparison](https://github.com/rayfdj/emaxx/actions/runs/36596290294)
matches **519 files / 7,928 outcomes**, with all 1,038 processes successful.
All **223 terminal scenarios, 651 screen comparisons and 28 filesystem
comparisons** match source/ABI-matched GNU. The [complete receipts](docs/handover/2026-09-30-compatibility-complete/manifest.json)
verify raw inventories, outcomes and artifact hashes. All 328 compiled/test
inputs match published `d9caac1e`, documentation checkpoint `72b487b1` and the
macOS worktree. Read the [compatibility checkpoint](docs/runtime-representation-compatibility-checkpoint.md)
after the complete goal.

Source161 repairs Table/Org captions by calling unchanged GNU
`keymap-canonicalize`, with collecting callback roots preserved. Strict checks,
72 debug and 72 release controls and fifteen ordinary GNU comparisons pass.
The initial restricted-sandbox full run failed twelve socket tests; the complete
host-permitted rerun passes using the identical source and test binary.
That failure, source155's 17 terminal divergences and source153's native
reclamation failure/GNU Eglot timeout remain preserved. Later passes do not
establish the causes of those older source153 failures.

**Previous selected continuation:** read the
[combined call-metadata checkpoint](docs/runtime-representation-call-metadata-checkpoint.md).
Then read the [live input-decoder draft](docs/runtime-representation-input-decoder-draft.md)
for the exact unfinished patch, preserved failures, current validation handles
and additional compiler-dependency provenance. This draft is packaged separately
and is not applied to production.
Source174 combines source172's independent mutable TLS reports and source173's
direct subr classification/descriptor reads. All 345 compiled/test inputs match
the isolated checkout. Strict checks, **395 gate and 395 release controls**,
and **12 ordinary exact GNU comparisons** pass. Complete macOS Rust and terminal
runs have started; complete Linux Rust and frozen compatibility remain required.
The [portable receipts](docs/handover/2026-09-30-combined-call-repair/manifest.json)
also verify the source170 Eglot replay's 52 matching outcomes and source172's
two TLS Rust passes plus 27 matching network-stream outcomes. The original
full frozen GNU Eglot timeout remains a failed run. This candidate is not yet
ready for main. Input decoding is a separate unfinished draft; all goal-wide
requirements below remain open.

**Previous task-branch continuation:** read the
[independent TLS report checkpoint](docs/runtime-representation-tls-report-checkpoint.md).
Source172 removes the process-owned report cache and returns fresh mutable
Lisp strings from the live TLS session. Strict checks, **414 debug and 414
release controls**, and two exact ordinary GNU comparisons pass. All 345
compiled/test inputs match the isolated checkout. Its
[portable receipts](docs/handover/2026-09-30-tls-peer-reports/manifest.json)
preserve source171's failed string-mutation probe and both helper invocation
failures. Complete source172 validation remains required. Source173's
call-metadata repair is still isolated: **226 gate and 226 release controls**
and all strict checks pass. Its [portable unfinished draft](docs/handover/2026-09-30-call-metadata-draft/manifest.json)
contains the exact patch, source hashes and closed receipts; ordinary and
combined-source complete validation remain required. Input decoding,
post-deinit TLS behavior and the full goal remain unfinished.

**Previous task-branch candidate:** read the
[event and TLS root checkpoint](docs/runtime-representation-event-roots-checkpoint.md).
Source170 extends source166 with rooted notification batches, traced/cloned
TLS Lisp state and live special-event lookup. All strict checks, **482 debug
and 482 release controls**, and **27 ordinary GNU comparisons** pass. The full
macOS Rust gate now **fails**: 2,195 tests pass, then the unchanged suspended
bytecode reclamation assertion fails; four later groups and both Cargo stages
do not execute. Full Linux Rust passes **3,034 tests**, including native
artifact identity; all **223 terminal scenarios / 679 checkpoints** match.
The full Linux frozen comparison has **7,927/7,928 matching outcomes**:
Emaxx has no unexpected outcomes, while GNU times out in one Eglot JSON-RPC
test. The comparison remains failed. Both earlier autorevert and network-stream
files now match. The [complete source170 receipts](docs/handover/2026-09-30-event-root-full-validation/manifest.json)
retain all successes and failures. The
[new failure and debugger evidence](docs/handover/2026-09-30-gc-retention-diagnosis/manifest.json)
reproduces the original source166 Linux failure using its exact executable.
Further exact-binary diagnosis locates the retaining word in `eval_call`:
a one-byte flag store leaves stale upper pointer bytes at stack offset `0xa8`.
Isolated source173 removes copied optional call metadata and repeated function
decoding; its validation is pending. Source172 separately removes the cached
TLS report and allocates mutable report strings; validation is also pending.
An unchanged source170 macOS binary passes a standalone replay, which does not
clear its full-gate failure or certify either repair. The earlier
[portable evidence](docs/handover/2026-09-30-event-roots/manifest.json)
preserves source168's compiler and source169's new-test failures, negative
source166 comparisons and an unresolved post-deinit TLS difference. This does
not clear the macOS reclamation failure and is not ready for main.

The recovered source166 macOS gate passes **3,019 tests**, including native
artifact identity, with two existing terminal ignores. Its second terminal
run was interrupted during **112/223**, without a final result. The first
instrumented Linux reclamation diagnosis passes with a different binary and
loses its captured child trace; it cannot clear the ordinary failure.
These receipts and limitations are retained in the event-root archive.

**Previous task-branch candidate:** source166 is now applied on top of that main
checkpoint. Read the [live keymap checkpoint](docs/runtime-representation-live-keymap-checkpoint.md)
and its [portable draft evidence](docs/handover/2026-09-30-keymap-live-authority-draft/manifest.json).
It
replaces private record keymaps, reverse indexes, snapshots, binding caches and
permanent roots with actual cons/char-table storage. It integrates source161's
menu repair, reads live input events through collecting callbacks, shares the
actual lookup path with `key-binding`/remapping and removes hardcoded prefix
decisions and duplicate filter calls. Strict checks, **462 debug and 462 release
controls** and all **25 ordinary GNU comparisons** pass. Its portable patch
replays all 339 inputs from published `d9caac1e`.

Source166 was separate continuation material in PR #77 and is now the next
production candidate in [PR #78](https://github.com/rayfdj/emaxx/pull/78), published
at `3bada057`. The first full macOS runs were interrupted: Rust received SIGTERM
in primitives after five completed groups; the terminal log ends during scenario
33/223. Their [raw evidence](docs/handover/2026-09-30-keymap-interrupted-runs/manifest.json)
is preserved. Former temporary worktrees and GNU source are absent. Recovery uses
persistent `target/runtime-goal/recovered-2026-09-30` checkouts and a fresh GNU
candidate; the GNU rebuild and native ABI check now pass. The recovered macOS
outcomes are recorded above. [Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36647925886)
**fails** the unchanged suspended-bytecode reclamation assertion: 2,201 tests
pass, one fails, and four later groups plus both Cargo stages do not execute.
[Linux frozen](https://github.com/rayfdj/emaxx/actions/runs/36647928962) executes
all 519 files / 7,928 outcomes, with **7,926 matching and two mismatches** in
autorevert and network-stream. All 1,038 processes exit successfully, but two
Emaxx outcomes are unexpected failures. The
[complete failure receipts](docs/handover/2026-09-30-keymap-linux-failures/manifest.json)
also retain three diagnostic replays: the same exact Rust binary repeats the
reclamation failure; both ordinary files abort on a freed-cons assertion and
produce no complete Emaxx outcomes. These failures remain unresolved.
The source162/163/165 negative comparisons, source164 compiler failure and
retained-main callback/dispatch failures remain in the linked draft history.
Source166's selected passes do not substitute for complete validation.

The compact representation, symbol authority, physical allocation accounting,
final audit, complete final-source compatibility and locked performance goal
remain unfinished. A validated merge checkpoint does not complete that goal.

**Newest task-branch checkpoint:** read the [shared hash-table allocation](docs/runtime-representation-hash-allocation-checkpoint.md)
after the complete goal. Source 133 replaces the host record and four side maps
with one GNU-compatible allocation and owned arrays shared by all execution
engines, GC and image loading. Both complete gates exposed an obsolete dump
counter assertion; source 135 requires exact restoration of every remembered
scalar. Both fresh gates then exposed missing unordered hash-table comparison
handling. Source 139 repairs that production dispatch without changing the test:
154 debug and 154 release controls, strict checks and nine ordinary exact GNU
comparisons pass. Complete macOS and [Linux gates](https://github.com/rayfdj/emaxx/actions/runs/36530994726)
now pass on PR #76, integration `75b69738`: 2,999 and 3,011 tests respectively,
with two existing terminal ignores each. Native artifact identity passes.
[Complete receipts](docs/handover/2026-09-29-hash-complete/manifest.json),
[original failures](docs/handover/2026-09-29-hash-gate-repair/manifest.json) and
[ordering repair evidence](docs/handover/2026-09-29-hash-ordering-repair/manifest.json)
remain preserved. Separate keymap callback/error diagnostics also fail on the
retained previous main executable; compatibility repairs continue independently.
This validated checkpoint does not complete the full goal.

**Current task-branch follow-up:** the [native-call and window traversal checkpoint](docs/runtime-representation-call-window-checkpoint.md)
combines two measured ordinary-path improvements, with 232 debug and 232 release
passes, strict static checks and all 16 workload result/mode checks. Complete
gates for this newer source now pass: 2,993 tests on macOS and 3,005 on Linux,
with two existing ignores each. PR #75 integrates this follow-up at `49d0c25b`.
Its fresh full terminal run passes all 223 scenarios against source-matched GNU.
The Linux frozen comparison fails after 500 of 519 files: two differ, then the
keymap report's invalid UTF-8 stops parsing; 18 later files never run. Complete
raw evidence and limits are linked from the checkpoint. The previous checkpoint is already
merged into `main` at `bf483660` through PR #74. Full goal completion remains open.

**Newest continuation:** read the [complete Rust gates and main checkpoint](docs/runtime-representation-main-candidate.md)
after the complete goal. Source 115 now passes the complete macOS gate with
2,993 passes and the complete Linux gate with 3,005 passes; each retains the
two existing terminal-test ignores. Native artifact identity and the original
GC contracts pass. Earlier Linux reclamation failures remain preserved and
their cause is not established. Full pinned compatibility, allocation accounting,
the final audit and performance parity remain unfinished. The
[portable evidence](docs/handover/2026-09-29-main-candidate/manifest.json)
records the complete results. The full goal remains open.

The [regexp and VM checkpoint](docs/runtime-representation-regexp-checkpoint.md)
also records 152 debug and 152 release passes, strict static checks, four
ordinary GNU comparisons and all 11 original package installation checkpoints.
All 16 diagnostic workloads match results and execution modes but remain
substantially slower than GNU. Its earlier failures and measurements remain
in their original portable bundle.

**Previous results:** source 109 completes the entire macOS gate: 2,890 library
passes, two existing ignores, 60 binary passes and 39 integration passes,
including unchanged GNU native artifact identity. Linux completes with 2,174
passes and one suspended-bytecode weak-key reclamation failure; its later
groups and Cargo stages remain unexecuted. The [hash semantics continuation](docs/runtime-representation-hash-checkpoint.md)
repairs copying of mutated keys and capture of custom test functions. Its
three ordinary GNU comparisons pass, while the separately preserved native
hash-layout probe still fails. Read that continuation after the complete goal
and combined-source checkpoint. The full goal remains open.

**Latest continuation:** read the
[combined records, GC repair and profiling checkpoint](docs/runtime-representation-integration-checkpoint.md)
after the complete goal. Inline records are combined with the function-cell and
text-conversion repairs. A release-only eager-expansion GC failure is reproduced
against ordinary GNU and repaired; pending Lisp forms now remain reachable.
Source 108 passes 147 selected debug and 663 release tests, strict static checks
and ordinary comparisons. Full macOS and Linux validation subsequently failed;
the [completed full-run evidence](docs/runtime-representation-integration-checkpoint.md#completed-full-runs)
records their failures and unexecuted stages. The first 186 terminal scenarios
match; scenario 187 fails startup, leaving the run incomplete. The regexp
profile identifies and removes unnecessary scalar hashing, while measured pilot
Lisp bodies remain several times slower than GNU. Full goal completion is open.

**Earlier checkpoints:** the [function-cell checkpoint](docs/runtime-representation-function-cell-checkpoint.md)
removes two duplicate function payload tables and their name-position index.
Lookup, static tracing, cycle validation and image cloning use one existing
symbol cell. Source 89 passes 322 selected debug and 447 release tests, all strict
static checks, and 22 ordinary exact GNU comparisons. Allocated-symbol authority
and final-source validation remain incomplete. Generic inline records are now combined in the latest continuation above.

The callable reader now consumes bounded input,
creates authoritative positioned symbols, preserves callback/encoding semantics
and fixes marker-relative positions. Printer byte output and buffer hooks are
repaired too. Read the
[latest reader continuation](docs/runtime-representation-reader-checkpoint.md)
for 359 selected release passes, 19 ordinary exact comparisons, preserved prior
failures and remaining requirements. The earlier full gate failed at native
artifact identity after its library and binary stages passed. Its local GNU
ABI configuration differs from the committed target. The separately built pristine
GNU candidate now matches that configuration, and all nine unchanged native
artifact fixtures pass on the function-cell source; see the
[native configuration checkpoint](docs/runtime-representation-native-configuration-checkpoint.md).
Its executable still differs from the frozen pin. The approved older Linux run
also failed; the new [Linux run on c3204ce5](https://github.com/rayfdj/emaxx/actions/runs/36381574831)
finishes with 999 passes and one failure in the first three groups:
`set-text-conversion-style` still has no dispatch. The char-table key-range
failure is repaired. Later groups and Cargo stages did not execute; the
[raw Linux evidence](docs/handover/2026-09-28-linux-function-cells/manifest.json)
retains the failed result. None of these results is complete final-source validation.
The integrated [GC continuation](docs/runtime-representation-gc-checkpoint.md)
adds migrated-kind verifier coverage and removes inactive error payload storage
that retained a dead thread key. Its separate checkpoint passes 186 debug and
209 release controls plus cached-image and positioned-error checks. The combined source plus the [weak symbol ownership repair](docs/runtime-representation-symbol-ownership-checkpoint.md)
passes 189 selected debug and 389 release tests, three public-runtime integration
tests, all strict static checks and nineteen ordinary exact comparisons. The weak
uninterned-symbol book now follows its process-wide heap across serialized host
entries. Full final-source validation remains required. The newer function-cell
checkpoint preserves those repairs and its own failed baseline controls.
Do not reapply the old patch on this branch. The full goal remains incomplete.

**Active goal:** implement one compact, authoritative Lisp object representation
shared by interpreter, bytecode VM and native code, then reduce instruction/call
cost toward GNU C performance. Preserve GNU semantics, complete the adversarial
de-cheating audit, and require clean rustfmt/compiler/Clippy and full validation.
The goal is **not complete**.

Read these in order:

1. [Complete goal and completion requirements](docs/runtime-representation-goal.md).
   Then read the [current shared-reader draft](docs/runtime-representation-shared-reader-draft.md).
   Then read the [allocator continuation](docs/runtime-representation-vector-allocation-draft.md)
   and [cons field/root repair draft](docs/runtime-representation-cons-roots-draft.md).
   Then read the [shared hash-table allocation checkpoint](docs/runtime-representation-hash-allocation-checkpoint.md).
   Then read the [current task-branch follow-up](docs/runtime-representation-call-window-checkpoint.md).
   Then read the [complete Rust gates and main checkpoint](docs/runtime-representation-main-candidate.md).
   Then read the [newest regexp, VM and completed Linux continuation](docs/runtime-representation-regexp-checkpoint.md).
2. [Latest combined-source continuation and profiling limits](docs/runtime-representation-integration-checkpoint.md).
   Then read the [completed validation and hash semantics continuation](docs/runtime-representation-hash-checkpoint.md).
3. [Inline generic records and preserved failures](docs/runtime-representation-record-checkpoint.md).
4. [Text-conversion repair and Linux continuation](docs/runtime-representation-text-conversion-checkpoint.md).
5. [Function-cell payload consolidation and selected validation](docs/runtime-representation-function-cell-checkpoint.md).
6. [Host-thread symbol ownership and combined-source validation](docs/runtime-representation-symbol-ownership-checkpoint.md).
7. [Bounded reader, printing repair and completed gate failures](docs/runtime-representation-reader-checkpoint.md).
8. [Cons-edge checker, evaluator stack repair and saved-image evidence](docs/runtime-representation-gc-checkpoint.md).
9. [Authoritative positioned symbols and current failures](docs/runtime-representation-positioned-symbol-checkpoint.md).
10. [Authoritative cons payload and failed full gate](docs/runtime-representation-cons-checkpoint.md).
11. [Allocated frames and earlier cons failure](docs/runtime-representation-frame-checkpoint.md).
12. [Ordering repair and completed full-gate failure](docs/runtime-representation-ordering-checkpoint.md).
13. [Input repair and completed older terminal results](docs/runtime-representation-input-checkpoint.md).
14. [Keymap continuation and pending full validation](docs/runtime-representation-keymap-checkpoint.md).
15. [Char-table implementation and earlier evidence](docs/runtime-representation-char-table-checkpoint.md).
16. [27 September handover: original baseline and goal-wide obligations](docs/runtime-representation-handover.md).
17. [Original portable draft/evidence manifest](docs/handover/2026-09-27/manifest.json).

The original main checkpoint packaged its unfinished work as
[char-table-wip.patch](docs/handover/2026-09-27/char-table-wip.patch). That patch is
historical evidence on this branch: its allocator is now wired and the migration
is under validation. Commit `803e4326a16b7bfe13bb0b757ee85c8b02ac3998` contains
the first implementation. Rebuild on another machine; do not claim this host's
executable/image identity or test results as local.

Suggested instruction for the next instance:

> Read HANDOVER.md and the complete linked goal. Continue from the documented
> state without reapplying the historical patch. Preserve failures and all
> validation requirements. Use the repository's existing CI for Linux. The goal
> remains active; a milestone or a handover is not completion.
