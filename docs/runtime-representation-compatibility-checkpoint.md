# Runtime entry and compatibility repairs — 29 September 2026

The shared hash-table checkpoint is merged into main through
[PR #76](https://github.com/rayfdj/emaxx/pull/76), at `1d1e8cdc`, with complete
macOS and Linux Rust gates passing. This follow-up repairs the runtime-entry,
keymap, casing and error-lifetime paths exposed by its compatibility work.
The [complete goal](runtime-representation-goal.md) remains open.

## Latest complete runs and follow-up — sources 153–155

Source153's complete macOS Rust gate passes **3,011 tests**: 2,912 library,
60 binary and 39 integration tests, with two existing terminal ignores.
The unchanged native artifact identity contract passes. Production `82fcb6c9`
and published `900a6b7f` have the same 326 compiled/test inputs.

The [complete Linux Rust gate](https://github.com/rayfdj/emaxx/actions/runs/36544217147)
passes nine library groups (2,387 tests), then aborts in lightweight on
`native_continuations_can_return_in_non_lifo_order`: the second collection
finds two weak keys instead of one. Lightweight has no completed result
summary; both Cargo stages remain unexecuted. The original test passes in
[an isolated ordinary replay](https://github.com/rayfdj/emaxx/actions/runs/36588607032)
and [a debugger replay](https://github.com/rayfdj/emaxx/actions/runs/36588611905).
These narrower observations do not clear the full-sequence failure.

The [full Linux frozen comparison](https://github.com/rayfdj/emaxx/actions/runs/36544220911)
executes all 519 files and 7,928 outcomes: 7,927 match, one differs. Emaxx passes
`eglot-test-rust-completion-exit-function`; GNU fails with a JSON-RPC request
id 2 timeout. The original repeat-command mismatch now matches. The complete
run remains failed; no timeout, selector or expectation is changed. The
[completed-run receipts](handover/2026-09-29-compatibility-completed-runs/manifest.json)
retain both Linux failures and the complete macOS pass, with locally verified
Rust executable/image identities. Frozen Linux binaries are identified by
runner provenance and are not included in the downloaded reports.

Source155 uses the existing test-only `clobber_stack` helper before the native
reclamation collections. This removes words in dead driver stack space while
leaving live caller frames and suspended coroutine stacks intact. Original
2-to-1-to-0 counts, native result checks and the single-continuation zero count
remain unchanged. Exact root attribution for the original Linux failure is
not established; fresh complete validation is required.

Review also found a concrete source153 lookup mismatch: a redefined
`key-parse` returning nil, an integer or a symbol bypasses GNU's array check.
The ordinary same-input negative control and full outputs are preserved.
Source155 applies the GNU type check before checking sequence length and
adds a collecting-callback differential control covering invalid types, empty
arrays and valid strings/vectors. Compiler, formatting, warnings-denied Clippy
and diff checks pass. Source155 also passes six focused debug and six release
controls, and all fourteen fresh ordinary exact GNU comparisons. The
[230 repair receipts](handover/2026-09-29-compatibility-final-repair/manifest.json)
retain its inputs, commands, identities and outcomes. Full macOS Rust and
terminal runs are active on production `72d98598`. Published `f237bfcf` has
identical compiled/test inputs and runs [Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36590575463)
and [Linux frozen](https://github.com/rayfdj/emaxx/actions/runs/36590580818).
These complete runs are pending; PR #77 remains a draft. Source154 passed 382 controls
in each mode but failed to read the new fixture because of an extra trailing
parenthesis. Source155 corrects that typo; the complete outputs and expected
GNU behavior are unchanged. Both failed source154 runs remain preserved.

Source153's earlier **382 debug and 382 release controls** pass, zero failures
or ignores, including the original parameterized-character control omitted
from the earlier selected set. All thirteen ordinary GNU comparisons pass.
The unchanged upstream repeat file passes all three tests, keymap passes all
46, casing matches ten passes and one existing locale skip, and generalized
variables match six passes and two existing expected failures, including full
messages. The [5,618 earlier receipts](handover/2026-09-29-compatibility-gate-repair/manifest.json)
preserve those inputs, outcomes and original failures. The
[Linux reclamation replay](https://github.com/rayfdj/emaxx/actions/runs/36544224404)
passes its original named control, zero failures or ignores.

The original source148 complete Rust gates fail: macOS 2,185 passes/one failure,
Linux 2,192 passes/two failures. Four later library groups and both Cargo stages
remain unexecuted. Both platforms expose unbound Meta events returning zero
instead of GNU nil. `lookup_key_1` must advance by original events while
`access_keymap_1` handles Meta within one event. Source153 uses that division,
retains original tests, and delegates textual-vector translation to unchanged
GNU `key-valid-p` and `key-parse`.

The second Linux assertion fails in GNU itself, before Emaxx runs: the native
payload is still retained while its enclosing caller is active. The fixture now
returns from all creating/calling frames before collecting. All live-payload
checks, actual execution-mode predicates and the required final zero count in
every mode remain. Three ordinary Darwin GNU runs and the fresh Linux replay
pass; the complete primitive groups now pass on both platforms as well.
No ignore or expected failure was added.

The [source148 Linux frozen comparison](https://github.com/rayfdj/emaxx/actions/runs/36539124065)
finishes all 519 files and compares all 7,928 required outcomes: 7,927 match and
one differs, `repeat-tests-check-key`. This clears its predecessor's message,
stdout and malformed-keymap-report failures, while retaining the repeat failure.
The failed Rust runs are [Linux](https://github.com/rayfdj/emaxx/actions/runs/36539120621)
and the retained local macOS run. All retained Rust executables/images match
their recorded hashes. The original results are not converted to passes.

Source152 passes 381 debug tests and fails the existing textual-vector key
contract; thirteen ordinary comparisons pass. Its superseded full gate is
cancelled during compilation and establishes no full-test result. Source153
restores the GNU translation step and all three original failed assertions pass.
The separate [keymap representation draft149/150](handover/2026-09-29-keymap-authority-draft/manifest.json)
remains parked. Its source150 patch reproduces every manifest input from
published `c2d328ab`. It has one compiler warning, no executed tests, and
requires explicit integration of the newer source153 lookup repair. It is
packaged as unfinished continuation material, not production implementation.

## Changes and GNU reference

The outer serialized Rust runtime entry now installs the existing stack-base
guard, following `emacs.c:main`. Nested entries preserve the outer bound and
restore it after a panic. Ordinary batch startup no longer needs the Linux
stack-bound query that failed inside the upstream stdout-test sandbox. The
existing operating-system fallback remains available outside a runtime entry.
Original Linux stdout controls still require fresh validation of this source.

Menu lookup follows `keymap.c:Flookup_key`: exact bindings precede the vector
menu-bar fallback, Unicode casing precedes buffer-local casing, and event
symbols preserve the original input vector. The captured Unicode table has an
explicit process and image root, matching GNU's static root. Unibyte casing
uses raw-byte character codes and the actual case table; converting an already
multibyte or ASCII-only string preserves its identity and properties.

Binding descriptions select the supplied buffer's maps in GNU order and call
unchanged `help--describe-map-tree`. Character ranges preserve both their text
and `help-key-binding` endpoint properties. Filters retain the source-buffer
context while text goes to the output buffer.

Key lookup reads one event at a time from the actual Lisp map, following
`access_keymap_1` and `lookup_key_1`. It composes inherited prefixes before
advancing, preserves nil/default precedence, and invokes each traversed menu
filter once. `keyboard.c:menu_item_eval_property` supplies the error contract:
ordinary filter errors mean no binding; quit and other nonlocal exits propagate.
The handler is installed before invoking Lisp, so outer error handlers do not
observe errors GNU absorbs. No comparison output or upstream test is relaxed.

Interpreter `unwind-protect` and native nonlocal exits now keep pending Lisp
payloads rooted while cleanup runs. This uses the existing scoped root mechanism,
following `eval.c:unbind_to` and `unwind_to_catch`; cleanup errors still replace
the older exit. Verified interpreted, bytecode and native controls cover return
values, signals, throws, replacement errors, survival and eventual reclamation.

## Validation and preserved failures

Combined source 148 passes strict formatting, compiler, warnings-denied Clippy
and diff checks, and **367 debug and 367 release tests**, with zero failures or
ignores. A fresh ordinary executable and image pass all twelve exact GNU
comparisons. All 46 original keymap tests pass; casing matches ten passes and
one existing locale skip, and generalized variables match six passes and two
existing expected failures, including their complete messages. The
[226 checkpoint receipts](handover/2026-09-29-compatibility/manifest.json)
verify the inventories, executable and image identities and unchanged inputs.

Source148 was published at `7cba6e52` through
[draft PR #77](https://github.com/rayfdj/emaxx/pull/77). Its complete runs finished
with the failures recorded above; none is counted as a full pass.

Separate source 144 passes 193 debug and 193 release controls and all 46 upstream
keymap tests. The casing report matches GNU's ten passes and one existing locale
skip; the generalized-variable report matches six passes and two existing
expected failures, including their messages. Source 145 passes 184 debug and
184 release controls and four exact ordinary GNU root comparisons. The original
pending-error reproducer that exited -9 on source 139 now succeeds. The separate
menu-error probe reports a valid error and exit 255 instead of crashing. Combined
source 148's menu handler now produces GNU's exact output, exit 0 and empty
stderr on that same original probe.

The [history bundle](handover/2026-09-29-compatibility-history/manifest.json)
retains 699 receipts, original failed compiler/tests, exact source and executable
identities, raw outputs and reproduction drivers. Source 147's combined run
passed 365 tests and failed the existing native keymap-state assertion. Source
148 restores the production adapter refresh, leaving that test unchanged; its
367-test run includes the new reclamation regression.

These ordinary macOS comparisons use the source-matched GNU candidate, whose
native ABI matches. It is not the frozen Darwin executable. Source148's complete Linux frozen run clears the earlier message, stdout and
invalid-UTF-8 mismatches but still fails the repeat-command test, as recorded above. No speedup or parity claim is
made for these repairs.

## Remaining representation and completion requirements

Private keymap records, ownership indexes, snapshots and the binding cache remain
temporary adapters. An additional ordinary probe retains live-map assertions,
returns from the creating frame, and then collects: GNU reports zero anonymous
maps remaining; both retained main source 139 and source 145 report two. The
runtime explicitly roots every keymap facade for process lifetime. This existing
retention failure is still open. Removing the facade and all its consumers is
required; merely omitting its refresh does not complete that migration.

Full final-source Linux/macOS and pinned compatibility validation, remaining
representation and ownership work, complete physical allocation accounting,
the final adversarial audit and the locked 16-workload performance criterion
remain mandatory. Earlier pilot slowdowns remain preserved.
