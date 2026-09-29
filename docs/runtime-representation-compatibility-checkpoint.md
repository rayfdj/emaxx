# Runtime entry and compatibility repairs — 29 September 2026

The shared hash-table checkpoint is merged into main through
[PR #76](https://github.com/rayfdj/emaxx/pull/76), at `1d1e8cdc`, with complete
macOS and Linux Rust gates passing. This follow-up repairs the runtime-entry,
keymap, casing and error-lifetime paths exposed by its compatibility work.
The [complete goal](runtime-representation-goal.md) remains open.

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
and diff checks, and **367 selected debug tests**, with zero failures or ignores.
Its release, ordinary executable and complete platform gates are pending at this
publication checkpoint. These pending stages are not counted as passes.

Separate source 144 passes 193 debug and 193 release controls and all 46 upstream
keymap tests. The casing report matches GNU's ten passes and one existing locale
skip; the generalized-variable report matches six passes and two existing
expected failures, including their messages. Source 145 passes 184 debug and
184 release controls and four exact ordinary GNU root comparisons. The original
pending-error reproducer that exited -9 on source 139 now succeeds. The separate
menu-error probe reports a valid error and exit 255 instead of crashing; its
remaining difference from GNU's exit 0 is addressed by the combined menu handler.

The [history bundle](handover/2026-09-29-compatibility-history/manifest.json)
retains 699 receipts, original failed compiler/tests, exact source and executable
identities, raw outputs and reproduction drivers. Source 147's combined run
passed 365 tests and failed the existing native keymap-state assertion. Source
148 restores the production adapter refresh, leaving that test unchanged; its
367-test run includes the new reclamation regression.

These ordinary macOS comparisons use the source-matched GNU candidate, whose
native ABI matches. It is not the frozen Darwin executable. The earlier Linux
frozen comparison's message, stdout and invalid-UTF-8 failures remain failures
until a complete fresh run establishes otherwise. No speedup or parity claim is
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
