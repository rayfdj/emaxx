# Keymap authority continuation — 28 September 2026

The [complete runtime goal](runtime-representation-goal.md) remains incomplete.
This continues the [char-table checkpoint](runtime-representation-char-table-checkpoint.md);
its implementation and earlier failures still apply. The software snapshot here
is source 36, following the terminal-menu fix in `64bfa71fc5ebeed253ddc023975cec1beb7e8ebd`.
It was developed separately while source 25 (`878588f3a7ee1a101dcd9139de6a9cb6e54b7481`)
remained frozen for its full gate and terminal comparison. Those older runs do
not validate this source.

## Changes and remaining adapters

Full-keymap character definitions now live only in the actual char table.
Removed the duplicate sparse navigation entries, table/sparse reconciliation,
EQUAL-based range merging, and where-is traversal's sequence-length sorting.
Point and inherited-prefix lookup read table slots directly. GNU's explicit
unbound `t` slot, removal to `nil`, sparse entries behind a table, and range
updates/removals remain distinct operations.

Enumeration walks the public list cells and the shared live char-table walker.
It preserves EQ-distinct values, copies the walker's reused range cons, and
observes later raw sparse/table writes made during callbacks. Transient Rust
roots retain cursors, values and queued paths across Lisp collection. The table
walker decodes Unicode values only for Lisp map-char-table callbacks, matching
GNU's distinction between Lisp and C callbacks.

Accessible maps use GNU's breadth-first queue, ancestor-cycle check (shared maps
are still visited by each noncyclic path), inherited entries, Meta insertion
order and literal PREFIX boundary. Where-is searches these walks in active-map
order. Terminal menu hints request GNU's preferred FIRSTONLY binding directly.
The C references are `chartab.c:map_sub_char_table/map_char_table`,
`keymap.c:store_in_keymap/map_keymap_internal/accessible_keymaps_1/where_is_internal`,
and `keyboard.c:parse_menu_item` in the unchanged GNU source.

This does **not** remove the keymap record/public-list adapter, sparse binding
cache, display-name metadata or mutation notifications. Other sparse API writes
still rebuild public cells; arbitrary public list layouts and mutation/identity
contracts need further audit. Frames still have native handles. Cons cells still
occupy 80 bytes and contain duplicate mutable words and synchronization state.
The 16-byte authoritative cons requirement, ownership/serialization work, original
audit findings, VM/call optimization and performance objective remain open.
These changes have no measured performance claim.

## Evidence and failures

[Additional portable receipts](handover/2026-09-28-keymaps/manifest.json) preserve
commands, software manifests/patches, GNU outputs and raw logs, including failures.
Binaries are local provenance only; rebuild on another host.

- Source 28 fixes the Redo hint selection. A first regression insertion order
  (source 26) did not reproduce the defect; source 27's reversed order did.
  Source 28 passes the revised regression and all required static checks.
- Source 29 reproduces missing raw table bindings in where-is/accessibility,
  incorrect nil storage and merging of EQ-distinct bindings.
- Sources 30 and 31 fail to compile (root trait for an array, then closure error
  type inference). Source 32 passes authority but its live-callback fixture uses
  the unloaded `when` macro in a bare interpreter. The fixture is corrected to
  `if`/`progn` with an identical separately checked GNU result.
- Source 33 passes authority and live callbacks, then fails the new GNU traversal
  fixture on aliases, parents, Meta order, explicit PREFIX and active-map order.
- Source 34 passes those three groups and all static checks. Its broader release
  run is **84 passed, 1 failed**, zero ignored: removing duplicate table bindings
  exposed inherited Meta lookup's remaining dependence on the sparse projection.
  The original GNU golden is unchanged.
- Source 35 reproduces public sparse-tail loss/stale callback reads and incorrect
  range removal/update behavior. Both newly added fixture tests fail.
- Source 36 passes all ten focused keymap tests (174.68 seconds) and all 87
  selected release tests (126.44 seconds), zero ignored. Rustfmt, all-target and
  all-feature compiler check, strict Clippy and diff checks pass. Each tested
  software manifest and immutable test executable is retained; source hashes
  remained unchanged during each run. These are selected tests, not the full gate.

Source 36's ordinary release and fingerprinted image build successfully. The
first image invocation failed because an extra EMAXX_DUMP_SOURCE_DIRECTORY
override enabled the test-provenance path without EMACS_TEST_DIRECTORY. The
normal default resolves the same pristine GNU tree through the sibling symlink
and passes; both attempts are retained. Executable SHA-256 is
`e2288502e8d71576f2c8a8c1032cb2f06ee1316e262144a68b3614539df397ef`;
image SHA-256 is
`c1af4785ee0d5a4ecf87a11acedf001174728a587d2b511b572b83e3cd0a13ee`.

The generated case uses seed 928036, 96 operations and twelve snapshots, with
shared/cyclic prefixes, raw/API/range stores, removal, inheritance and forced
collection. The same Lisp through the normal GNU and source-36 command lines
produces identical 3,317-byte output, SHA-256
`284082fd4af88c9c80c4b32b8af1187cf24e9dfafdf79d15035fca60ba9acb9c`.
Its uncontrolled timings are diagnostics only. This finite case and selected
release checks do not certify general semantics, frozen compatibility or speed.

## Older full validation remains incomplete

The source-19 terminal run was stopped by the assistant after discovering that
its relative EMACSLOADPATH stopped finding GNU libraries after visiting temporary
files. It started 109 scenarios: 89 MATCH and 19 DIVERGE lines, with the last
scenario unfinished. This is an interrupted, confounded diagnostic, not a pass.
An earlier progress claim that its first 37 scenarios matched was incorrect;
the full raw log and explicit correction are retained.

Source 25 has its own fingerprinted ordinary release and image and passes eleven
same-input ordinary CLI comparisons, including the earlier 512-operation table
case. Its corrected full terminal run uses absolute Lisp/executable paths and
isolated HOME. The 223-scenario inventory and all timeouts/comparisons are unchanged.
So far it retains differences in f10-cycle, mouse-bar-click, mouse-cmenu,
compile-run, compile-parse, compile-next-error, help-key-then-quit and
help-function-then-quit, bookmark-set-and-jump and revert-buffer-confirm.
The labelled partial terminal log records the capture boundary. The menu hints are addressed
by source 28; mouse and compilation failures still need diagnosis and exact-source
terminal validation. Do not run a second ttydiff concurrently: fixed fixtures
are shared. The wrapper runs the ordinary TTY smoke afterward.

The source-25 full serial gate **failed**. All five evaluation groups pass
(385 + 295 + 320 + 254 + 351 = 1,605 tests); primitives has 508 passes and
12 failures, all reporting socket permission errors under the sandbox. The
runner stops there, so later groups are unexecuted. An exact-binary rerun of
those twelve failures outside the sandbox passes all twelve with the original
gate fixture-image/template environment. A first diagnostic omitted those two
variables: eleven pass, while the HTTP server deadline expires during fresh
interpreter startup. Both reruns and the original full failure are retained. No
timeout, selector or expectation changed, and reruns do not turn the failed full
run into a pass. The overall inventory is 2,813 scheduled tests plus
the unchanged two opt-in TTY ignores; those ignores are not passes. Final source
requires a fresh complete gate with the necessary local socket permissions.

Local GNU is pristine revision `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`, native
capable, executable SHA-256
`7d8944fe2b2bdbd2856cfd4f47dbd5c80db90089ac20be641c10a348bf217e82`.
It does not match the locked Darwin binary. All local GNU comparisons here are
source-matched diagnostics, not frozen certification; the lock is unchanged.
The earlier pinned preflight failed on that hash. Linux publication/CI has not
run: automatic approval review rejected the earlier push to the external
repository without explicit user permission. Do not bypass that rejection.

Finish exact final-source full validation on both supported platforms, the
remaining representation and ownership work, the adversarial audit, and the
locked 16-workload performance suite with its unchanged 3% criterion. A selected
checkpoint is not completion of the goal.
