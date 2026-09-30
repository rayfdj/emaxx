# Shared command reader draft — 30 September 2026

**Source198 is an unfinished candidate, not production.** Its complete
patch is based on main `21d20f0eec08f3c013d5d3eb3bd6e9cfc16b3199`, the verified
merge of [PR #78](https://github.com/rayfdj/emaxx/pull/78). Production remains
the [fully validated source174 checkpoint](runtime-representation-call-validation-complete.md).
The preceding source196 is published as `ece02f9522d4e0d6d7a6a0cf6648dd109885780b` in
[draft PR #79](https://github.com/rayfdj/emaxx/pull/79). Source198 now occupies
the task worktree with selected validation complete, ready for publication.
The source196 complete macOS Rust gate fails with 2,221 passes and one keyboard-input
failure. [Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36741202335)
also fails the same fixture after 2,229 passes. Its [audited receipts](handover/2026-09-30-shared-reader-draft/source196-linux-rust-failure-manifest.json)
verify raw names/verdicts and retained binary/image hashes on the clean
published head; four later groups and both Cargo stages did not run.
Source196's [Linux frozen comparison](https://github.com/rayfdj/emaxx/actions/runs/36741207263)
now matches 519 files and 7,928 outcomes, with all 1,038 processes successful.
Its [audited receipts](handover/2026-09-30-shared-reader-draft/source196-linux-frozen-manifest.json)
retain 7,670 passes, 47 expected failures and 211 skips for each editor.
Full source196 terminal validation is still running. The
[closed failure receipts](handover/2026-09-30-shared-reader-draft/source196-macos-gate-failure-manifest.json)
verify every executed test name/verdict, the same-binary failure replay and all
ten original contracts matching in ordinary GNU/Emaxx batch execution. Four
later Rust groups and both Cargo stages did not execute. The
[publication receipts](handover/2026-09-30-shared-reader-draft/source196-publication-manifest.json)
verify the remote head, audited terminal results and exact-head Linux dispatches.
The [portable draft bundle](handover/2026-09-30-shared-reader-draft/manifest.json)
retains the earlier patches, failed runs, negative comparisons and closed
validation receipts. The [closed source192 validation](handover/2026-09-30-shared-reader-draft/source192-validation-manifest.json)
adds the complete selected reader results. This continues the
[source178 decoder](runtime-representation-input-decoder-draft.md).
The [source195 macro draft bundle](handover/2026-09-30-shared-reader-draft/source195-draft-manifest.json)
contains its complete patch, 399-input manifest, exact replay audit, strict
checks and all seven raw focused gate verdicts. It preserves both new compiler
failures and all four earlier macro comparisons. Broader running logs are excluded.
The [closed source195 results](handover/2026-09-30-shared-reader-draft/source195-validation-manifest.json)
subsequently record all selected Rust, ordinary and terminal results, including
the additional failed wrapper comparison. Source196 is the correction described below.

The frame, popup-menu, live minibuffer, keyboard-macro and simulated minibuffer
loops now use one incremental `KeySequenceReader`. They retain reached keymaps
and actual event words across reads and collecting callbacks. The loops no
longer format and reparse each key, repeat lookup of an unchanged prefix, or
remap an already resolved command a second time. Translated command keys and
raw keys remain distinct. Popup errors propagate after restoring the screen
and dynamic bindings.

The reader follows GNU `keyboard.c:read_key_sequence`, `keyremap_step` and
`test_undefined`: it restarts after an unbound prefix loses all translations,
autoloads prefix maps before classification, and consults live command remapping
at both function-key translation decision points. GNU's initial
`local-function-key-map` is one stored map with the actual `function-key-map`
parent; removing the lookup fallback also removes repeated fresh-map allocation.
C-created maps have GNU's nil prompt instead of invented name strings.

The two simulated minibuffer loops now consume the actual unread list spine
and share translation state when input switches from that list to a macro.
RET and completion dispatch through the active map. The old queue copy,
hardcoded function-key conversion, special RET handling, duplicate self-insert
body and three unused text/prefix lookup adapters are removed. An unbound
printable macro event now dispatches unchanged GNU Lisp `undefined`; the
independent `ding` primitive gap remains open.

Source195 also removes `KbdMacroExecutionState`, its copied event vector and
cursor, per-read resynchronization, duplicate GC tracing and image-clone work.
Following `keyboard.c:read_char` and `macros.c:at_end_of_macro_p`, readers use
the actual `executing-kbd-macro` array and public index. A store of t stops the
array immediately. Direct bindings, replacement, rewinding and mutation share
that same path. Raw reads retain GNU's string-to-meta conversion and macro
event frame; timed reads consume available input before waiting and propagate
inhibition and bounds errors.

The original array, loop function and saved outer state remain rooted across
nested commands and collecting termination hooks. The pending error is rooted
while those hooks run. Image startup clears the executing array as GNU
`init_macros` does, while retaining the dumped index. The existing test that
loads an actual image in a new OS process now checks both behaviors, preserving
its original identity and environment assertions. Vector reads use the actual
slots without copying the sequence; string length still scans characters, so
this is not a claim of constant-time string reads or measured speed parity.
The five saved values currently occupy a `RootedVec`: 40 bytes of host payload
on the supported 64-bit targets, plus allocator and root-registry overhead.
Registration and removal acquire the heap-root registry mutex. Those host
allocations are not charged by `note_lisp_allocation`. GNU instead saves the
previous macro/index/command in two conses and keeps its other live values on
the C stack. The fixed Rust saved-state set should use the existing scoped
stack-root mechanism in a subsequent candidate; its current host allocation,
registry work and GC-accounting difference remain open costs, not a speedup
or complete allocation-accounting result.
The [archived saved-state follow-up patch](handover/2026-09-30-shared-reader-draft/macro-saved-state-original-manifest.json)
implements GNU's two ordinary saved-state conses plus a three-value borrowed
stack-root scope. It removes that host vector and its global registration while
restoring the actual cons allocation charge. It was first packaged as an
uncompiled draft, with only syntax formatting and patch applicability checked.
Its [original record](handover/2026-09-30-shared-reader-draft/macro-saved-state-draft.json)
preserves those evidence limits.
The [separate source197 candidate](handover/2026-09-30-shared-reader-draft/source197-saved-state-draft-manifest.json)
now applies that patch and the reclamation test only in
`target/runtime-goal/recovered-2026-09-30/macro-saved-state/emaxx`. Its complete
patch replays all 403 inputs against the same main base. Its [strict checks](handover/2026-09-30-shared-reader-draft/source197-strict-manifest.json)
pass with zero warnings. The [closed selected results](handover/2026-09-30-shared-reader-draft/source197-selected-validation-manifest.json)
now pass 32 gate tests, 32 release tests and eight ordinary GNU comparisons.
The first gate passed 30 tests and failed two before their macro assertions:
the checkout lacked the sibling `../emacs` path required by the early-Lisp
fixture. Restoring that symlink makes the identical gate executable, inventory
and environment pass; no source or assertion changes were made for that replay.
Both original failures and the failed audit-helper invocation remain preserved.
Supervisors 10334 and 15213 have exited. Source198 now includes these changes
in the task worktree.
The scoped root mechanism still has per-interpreter registration work and can
grow its storage at a new nesting depth. Removing the host vector and global
mutex does not claim zero root-handling cost or complete allocation accounting.
The [separate reclamation control](handover/2026-09-30-shared-reader-draft/macro-saved-state-reclamation-manifest.json)
now matches ordinary GNU and retained source195: the original macro survives
collection after its executing variable is replaced, remains live through the
termination hook, and is reclaimed after either completion or an explicit
error. This started as positive baseline evidence; the saved-state candidate
now passes its own identical ordinary comparison and Rust gate/release controls.
The first two probe versions and a relocated-image load failure
are preserved. `memory-use-counts` currently signals unsupported in Emaxx, so
the proposed comparison through that primitive cannot establish allocation
accounting; implementing real counters and measuring physical costs remain open.

Source198 combines that selected saved-state candidate with a correction to
the keyboard-input test's startup fixture. GNU's ordinary dumped runtime loads
mouse.el, which binds `down-mouse-1`; the Rust fixture had loaded only early
Lisp. The old reader invented that binding as a bootstrap fallback. The new
reader drops the unbound event and then exhausts the fixture's queue.
All ten unchanged programs and expectations match in ordinary source196/GNU
batch execution. The fixture now uses the actual batch runtime, preserving
every assertion. An additional ordinary control matches GNU/source197 when the
mouse-down binding is absent, installed and removed again, preventing a return
of the hardcoded fallback.
The [source198 draft](handover/2026-09-30-shared-reader-draft/source198-startup-fixture-draft-manifest.json)
contains its complete 405-input patch, exact replay audit, baseline control and
validation helpers. Runtime code is unchanged from source197. It is frozen in
`macro-saved-state/emaxx`, with all 405 inputs also verified in the task
worktree. Supervisor 20015 has finished: the
[closed selected results](handover/2026-09-30-shared-reader-draft/source198-selected-validation-manifest.json)
pass all strict checks, 542 gate controls, 542 release controls and 28 ordinary
GNU comparisons. Every raw inventory, outcome and artifact identity was audited.
The [closed focused receipts](handover/2026-09-30-shared-reader-draft/source198-focused-manifest.json)
verify all strict passes and all three focused gate tests, including every
original keyboard-input assertion, live binding mutation and macro reclamation.
The [broader gate results](handover/2026-09-30-shared-reader-draft/source198-selected-gate-manifest.json)
also pass all 542 selected tests, with every raw name/verdict and the retained
binary identity audited; the newer bundle above adds release and ordinary results.
Source197's ordinary executable/image were retained before this transition.
Those focused passes do not clear source196's failed full runs or replace
complete source198 validation.
The [complete validation launch](handover/2026-09-30-shared-reader-draft/source198-complete-validation-start-manifest.json)
records full queue 22463 and Rust supervisor 22464. Complete Rust starts after
the audited selected Rust/ordinary prerequisites. The queue separately waits
for source196's actual terminal process to exit, runs source198's 17 selected
terminal scenarios, audits their raw checkpoints, then starts all 226 scenarios.
Both checkouts remain frozen while their processes run. Source198 needs its
own complete Linux validation after publication; source196's frozen pass does
not certify the macro saved-state change.

The raw evidence establishes the following selected results, not complete
new-source validation:

| Source | Result |
| --- | --- |
| 179 | Compilation fails: fallible menu code used an infallible closure. |
| 180 | Compilation fails: the success menu outcome was incorrectly included in Lisp root tracing. |
| 181 | Strict checks pass; 402 gate tests pass and the new direct-reader test fails before reaching the reader because its interpreter lacks GNU startup. |
| 182 | Strict checks pass; 418 gate tests pass and the old shifted-tab test fails. |
| 183 | Strict checks pass; 419 gate tests pass and two shifted-tab checks fail, exposing the missing initial function-key parent. |
| 184 | Strict checks pass; 420 gate tests pass and the same two checks fail. The new live-remapping callback-count check passes. |
| 185 | Strict checks, 424 gate tests, 424 release tests and 15 ordinary exact GNU comparisons pass. Its original 17-scenario terminal run fails before the first scenario. |
| 186 | Removing the simulated readers leaves four compiler warnings; strict Clippy rejects the unused import and three obsolete adapters. No tests execute. |
| 187 | The adapters are removed, but a helper path typo stops validation before Cargo. No compiler or test result is claimed. |
| 188 | Strict checks, 426 gate tests, 426 release tests and 17 ordinary exact GNU comparisons pass. Selected terminal validation fails two of seventeen scenarios. |
| 189 | Corrects only the new prompt presence assertion and records two previously omitted terminal fixture inputs. Inherits the real menu click regression; no new runtime pass is claimed. |
| 190 | Removes the duplicate mouse translator, preserves raw event spines and consults live event-kind properties. Strict checks pass; the gate has 428 passes and one failure in the unchanged XTerm test. Its three new mouse controls pass. Release, ordinary and terminal stages do not run. |
| 191 | Replaces the nil coordinate-position stub with shared display-motion traversal and live window lookup. Compilation fails on a private helper and a buffer-position integer type; no runtime tests run. |
| 192 | Corrects those compiler errors without changing the original XTerm assertion or GNU fixture. Strict checks, 506 gate tests, 506 release tests, 21 ordinary GNU comparisons and all 17 terminal scenarios / 34 screen comparisons pass. |
| 193 | Removes duplicate macro state and adds the live-array, nesting and image-startup controls. Compilation fails because the pending `Result<(), LispError>` does not implement the scoped root trait. No runtime tests run. |
| 194 | Roots a `Result<Value, LispError>` instead. Test compilation fails because the new image assertions expected Option from a Result-returning API. No runtime tests run. |
| 195 | Corrects the new assertions without changing their expected behavior or existing coverage. Strict checks, 535 gate tests, 535 release tests, all 25 selected ordinary GNU comparisons and 17 terminal scenarios / 34 screen comparisons pass. An additional unread-wrapper comparison fails, and complete runs are deferred for its correction. |
| 196 | Reuses the existing unread-event unwrapping in the common pending reader and adds the ordinary GNU fixture plus a Rust control with a terminal poller installed. Its complete patch replays all 401 inputs. The first ordinary run fails using a stale source195 executable. A separately recorded clean rebuild passes strict checks, 536 gate / 536 release controls, all 26 ordinary GNU comparisons and 17 terminal scenarios / 34 screen comparisons. The complete macOS Rust run fails with 2,221 passes and one keyboard-input fixture failure; four later groups and both Cargo stages do not run. Full terminal and Linux validation remain running. |
| 197 | Removes the macro saved-state host vector and global root registration. Strict checks pass. The first gate has 30 passes and two missing-fixture-path failures; restoring the sibling GNU path makes the identical 32-test gate pass. All 32 release controls and eight ordinary comparisons pass. Complete validation remains required. |
| 198 | Preserves source197 runtime and changes the keyboard-input fixture to actual batch startup, retaining every assertion. Adds a GNU-verified control for live bound/unbound mouse-down input. Its 405-input patch replays exactly; strict checks, three focused controls in both profiles, all 542 gate / 542 release tests and 28 ordinary GNU comparisons pass. Complete Rust is running and terminal validation is queued. |

Source196 passes all strict checks and its eight focused gate and release tests. The
[focused receipts](handover/2026-09-30-shared-reader-draft/source196-focused-manifest.json)
include every raw verdict, the verified immutable test executable hash, the
exact complete patch and all 401 input hashes. The new wrapper contract passes
with a terminal poller installed. Both broader selected Rust inventories pass
all 536 tests, including the original GC survival/reclamation controls.

The [closed stale-build receipts](handover/2026-09-30-shared-reader-draft/source196-stale-validation-manifest.json)
preserve the subsequent ordinary failure: 25 comparisons match, but the wrapper
comparison still differs. The executable hash is identical to source195's.
The snapshot helper used `copy2`, preserving an edited file's modification time
from before the preceding library build. Cargo consequently reported the old
ordinary executable as fresh. Unchanged source hashes did not establish compiled
artifact freshness. That failed run cannot certify source196, and neither its
terminal nor complete validation launched.

The copier now refreshes timestamps when copying changed inputs. The separately
named [source196-fresh selected receipts](handover/2026-09-30-shared-reader-draft/source196-fresh-selected-manifest.json)
pass strict checks, 536 gate tests, 536 release tests and all 26 ordinary GNU
comparisons after explicitly cleaning only the Emaxx Cargo package in each
profile. Every raw inventory, verdict, stdout/stderr, process status and input
hash was audited; the auditor rejects five corrupt-log controls. The new
ordinary executable differs from the stale source195 executable, and the
unchanged wrapper fixture now matches GNU. Selected terminal validation also
passes all 17 scenarios and 34 screen comparisons, with exact raw inventories
and unchanged input hashes verified. The source manifest and complete patch are
byte-identical to source196.
An initial `source196-rebuilt` launch cleaned the package but failed
before compilation because a helper parsed its qualified source tag as an
integer; those failed receipts are retained too. Complete macOS runs remain
conditional on every selected prerequisite passing. No failed receipt is
overwritten, and no performance or full-goal completion is claimed.
The [clean-rebuild start bundle](handover/2026-09-30-shared-reader-draft/source196-clean-rebuild-start-manifest.json)
contains the corrected helpers, identical source snapshot, supervisor handles,
fresh strict-check passes and both helper failures. Running test logs are excluded.

Source185's terminal startup failure exposed a harness mismatch: GNU received
`-Q`, while Emaxx loaded the host's Doom configuration and displayed an init-hook
error. Source188 passes `-Q` to both editors without changing HOME. The Doom
error itself is not claimed repaired. A supplemental source185 run using that
harness matches the ordinary typing control, the collecting prefix-filter
scenario and the translation/raw-key scenario. Its new minibuffer scenario
then fails a presence assertion on GNU because visible lines are trimmed and
the assertion expected a trailing space in `Read: `. A separately preserved
one-line correction to that new assertion passes all three minibuffer screen
comparisons. The original failed runs remain failed; no selector, timeout,
existing screen comparison or submitted-value assertion is relaxed.

Source188's terminal run has finished with both failures retained. Its
`mouse-bar-click` scenario leaves the ordinary buffer visible where GNU opens
the Edit menu. The corrected prompt assertion was applied in source189 only
after that run ended. Source190 retains the correction. Do not edit source or
replace binaries while any associated validation process is running.

Source190's unchanged XTerm control fails because `posn-at-x-y` returned nil
for every buffer position. GNU's reader treats a symbolic position, including
nil, as a non-text prefix; removing the old reader's nil exception exposes
that primitive gap. Source192 shares the existing display-motion traversal
to locate actual buffer positions, stops on the glyph containing a coordinate,
and locates the live window in the frame tree. The new ordinary GNU fixture
covers tabs, two text lines, positions past end of buffer and unchanged point.
It fails on source185's retained executable. Broader display contracts,
including realized glyph metadata, display objects and non-text areas, still
need differential validation; this draft is not certification of those paths.

The menu regression is reproduced by an ordinary program supplying mouse-click
events with GNU's actual event-kind property. Source188 calls `mouse-on-link-p`
with nil instead of the event position and signals an error. That Rust policy
copy is removed: unchanged `mouse.el` owns link-click translation through the
real key-translation-map. Following `keyboard.c:read_key_sequence`, source190
shallow-copies each raw event spine before a translator can mutate its head;
the position remains shared. Event classification reads the live symbol
property instead of searching its name for `mouse-`. The terminal frontend
sets that property as GNU's `xt-mouse.el` does. Non-mouse parameterized menu
events retain GNU's distinct external-input-only prefix and shared-position
mutation behavior.

Three new GNU comparisons cover shared/distinct menu positions, event-head
mutation during collection, names without `mouse-`, and generic menu events.
All fail on the retained source188 executable. An earlier menu probe had an
extra closing parenthesis and failed before reading input in both editors;
that invalid probe and its corrected successor are preserved separately.

New ordinary GNU controls cover all three translation stages, macro/minibuffer
input, collecting prefix autoloads, live remap callback counts, shifted character
versus function-key events, initial map identity/inheritance, mutation of the
shared unread spine and a custom RET command. The source185 negative minibuffer
comparisons produce empty input or lose queue mutation; source188's new gate
checks and ordinary executable comparisons pass the same programs. The newer
mouse changes still require their own runtime and terminal results.

The old shifted-tab local test used `(kbd "S-TAB")`, an integer character,
while asserting the behavior of the symbolic `(kbd "S-<tab>")`. Only that
input is corrected; its backtab and selected-window assertions remain. A new
GNU fixture covers both event kinds. The attempt to run the entire old window
fixture in GNU failed earlier on batch frame resizing and is preserved; it
does not establish the whole old fixture's behavior. Earlier source178 test
premise corrections and the valid raw XTerm failure remain in its handover.

All source179–192 complete patches replay against the recorded main commit,
including untracked modules/fixtures and Git executable modes. Source188 records
381 input hashes; source192 records 391. The completed selected Rust inventories through source188
were checked against every raw test name and verdict, with binary hashes and
both original GC survival/reclamation controls retained. All 17 source188 ordinary
comparisons were checked against raw stdout/stderr, exit status and unchanged
artifacts. The inventory auditor rejects five deliberately corrupt log controls.
Failed archive-audit helper attempts remain preserved too.

The source190 failed gate was separately checked against all 429 raw test
names/verdicts and its immutable binary: 428 passes, the original XTerm failure,
and no ignores. Both original GC survival/reclamation controls and all three
new mouse controls pass in that failed run. Source192 runs the original XTerm
and new position control first, then the broader selection, additionally
including motion and window tests. The portable bundle contains closed
receipts through source191 and source192's exact patch, manifest and helpers;
its running validation logs are deliberately excluded.
The [source192 focused receipts](handover/2026-09-30-shared-reader-draft/source192-focused-manifest.json)
separately retain the closed strict checks, test build and both raw focused
test verdicts with the verified binary hash. The unchanged XTerm contract and
new ordinary GNU position contract both pass. These two checks do not replace
the broader selected run or any full validation requirement.

The two Python terminal fixture files were absent from the source manifests.
A supplemental audit matches their current bytes to the recorded base and both
checkouts; the corrected prompt replay records both before execution. That is
not a retroactive pre-run capture. GNU matches source/native ABI but is not the
frozen Darwin executable. Concurrent correctness runs supply no performance
measurement.

To reconstruct the validated reader candidate, create a checkout at
`21d20f0eec08f3c013d5d3eb3bd6e9cfc16b3199`, extract the original bundle and apply
`source192-complete.patch`. Verify every entry in `source192-manifest.json` before
building. For the newer source195 candidate, apply `source195-complete.patch`
from its separate macro bundle to that same main base, then verify all 399
entries in `source195-manifest.json`. The frozen validation checkout is
`target/runtime-goal/recovered-2026-09-30/macro-reader/emaxx`; receipts are in
`target/runtime-goal/resume-2026-09-28`. Use `check-reader195.py`,
`static-reader195.py`, `test-reader195.py`, `build-reader195.py` and its queue
scripts after explicitly relocating paths on another host. The broken source187
helpers are historical evidence. Recorded PIDs are local observation handles;
poll them and terminal receipts before taking the next action.
The complete macOS Rust and terminal runs were queued under
`queue-source195-full-validation.py` (supervisor 5109), then deferred before
either child launched. Additional review found that the new shared pending
reader bypasses `(t . EVENT)` unwrapping previously used by timed terminal
reads. The older batch reader already leaks a symbolic wrapper and loses meta
bits, as exact GNU comparisons on both source185 and source195 demonstrate. The local
worktree reuses the existing unwrapping helper and adds the same-input contract
with a terminal poller installed; this follow-up is frozen as source196 and
awaits validation.
Its [separate portable draft](handover/2026-09-30-shared-reader-draft/source195-unread-followup-manifest.json)
contains a complete patch from the same main base, all 401 current input hashes,
both failed wrapper comparisons and the record deferring complete validation.
The [separate replay audit](handover/2026-09-30-shared-reader-draft/source195-unread-followup-patch-replay-and-modes-audit.json)
verifies those 401 input hashes and executable modes after applying the patch
to the recorded base; this is not compiler or runtime validation.
Selected source195 validation has closed successfully, with the supplemental
wrapper failure retained separately. Source196 now occupies the isolated
checkout; its patch and manifest are byte-identical to the portable follow-up
above. Use `check-reader196.py`, `static-reader196.py`, `test-reader196.py`,
`build-reader196.py` and the source196 queue scripts. The selected supervisor is
6540, selected terminal supervisor 6541, and complete-run queue 6542; all have
exited after the stale ordinary failure, without starting complete runs.
The current package-clean retry uses `source196-fresh` helpers and receipts:
selected supervisor 8612 and selected terminal supervisor 8613 have finished
successfully. Complete-run queue 8614 launched macOS Rust supervisor 12611 and
terminal supervisor 12612 after those passes. Rust supervisor 12611 has exited
after the failed gate recorded above; terminal supervisor 12612 and queue 8614
remain live. The full terminal inventory is 226 scenarios. [Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36741202335)
has finished with the same fixture failure, while
[Linux frozen](https://github.com/rayfdj/emaxx/actions/runs/36741207263) has passed
on the published `ece02f95` head. Keep the isolated checkout and its
artifacts frozen while any of these supervisors or their children run.
For a subsequent snapshot, use
`196-fresh` as the previous tag so its full supervisors are checked as well.
The snapshot helper now requires any
queued complete supervisor and its children to have terminal records and exited
before the checkout can be reused.

Remaining reader work includes mouse fallbacks and buffer/frame transitions,
reader arguments/input methods, unread recording and recent-key semantics.
Three negative comparisons on source185 show an extra queued command after
`executing-kbd-macro` becomes t, missing directly bound macro input and swallowed
timed-read errors. A fourth nested-state/collecting-hook control already passes
on source185 and remains positive coverage. These failures motivated the newer
macro draft; its validation is still required. `ding` currently omits GNU's BEL output and
interactive callback/macro-termination behavior. GNU's actual C `noninteractive`
flag differs from the mutable Lisp `noninteractive1` variable: the saved batch
probe does not establish interactive bell behavior merely by rebinding Lisp
`noninteractive` to nil. The raw XTerm terminal-control output gap also remains.

The complete goal still requires symbol/object authority, truthful physical
allocation and collection accounting, post-deinit TLS behavior, final adversarial
review, complete Linux/macOS validation of the eventual final source and the
locked performance criterion. No current GNU performance parity is established.
The draft and the validated main checkpoint do not complete that goal.
