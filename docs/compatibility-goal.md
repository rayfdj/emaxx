# Compatibility Goal

This file records the active long-running goal so interrupted sessions can resume
from the same concrete state.

## Objective

Make `emaxx` 100% compatible with GNU Emacs at the Elisp API boundary by passing
the harness-selected GNU Emacs compatibility tests from the sibling checkout at
`../emacs`.

The canonical ordered manifest is:

- `compat/oracle_tests_all.txt`
- `compat/oracle_tests_all.md`

The denominator is 7080 selected tests. Do not use source-tree `ert-deftest`
counts as the progress denominator.

Post-7080 performance follow-up: track **"Optimize Dired listings for large
directories after 7080 compatibility"**.  Two Dired tests exceeded 20 seconds
only when enumerating the host temp directory with roughly 1,000 entries and
passed against an isolated empty temp directory, demonstrating a real Emaxx
large-directory performance gap rather than a Dired correctness or process-I/O
failure.  GitHub issue creation is currently blocked by missing issue-write
authentication (integration HTTP 403 and invalid local `gh` token); the full
handoff and retry instruction are in
`docs/!!!AI_CONTINUATION_INSTRUCTIONS_DO_NOT_SKIP.md`.

## Current State

- The 2026-08-01 native GnuTLS host-catalog checkpoint advances the exact GNU
  C primitive inventory to 1,390/1,420, leaving 30 missing.  Four new
  primitives use the installed GnuTLS through the existing `libloading`
  boundary rather than copying its version-specific tables into Rust.
  `gnutls-ciphers` and `gnutls-macs` expose the real host algorithm order,
  IDs, sizes, AEAD tags, and nonces in GNU's exact plist shapes, with GNU's
  older-library zero-nonce fallback.  `gnutls-error-fatalp` and
  `gnutls-error-string` preserve numeric `gnutls-code` symbol-property
  resolution and validation while delegating fatality and text to GnuTLS.  A
  direct sibling-GNU oracle pins catalog boundaries, representative
  descriptors, and exact diagnostic behavior.  This does not claim a TLS
  transport; `gnutls-available-p` remains nil until `gnutls-boot` is real.
  Exact inventory fingerprints are mirrored
  `(1_390, 654_403_392_030_036_411)` and missing
  `(30, 8_416_811_313_467_602_167)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,639 library tests (1,635 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  The 30-item remainder is: `alloc.c` 3, `bytecode.c` 2, `comp.c` 9,
  `module-load` 1, `gnutls.c` 6, display connections 2, menus 3, portable
  dumper 2, `re--describe-compiled` 1, and drag-and-drop 1.  Finish every
  non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, keep
  those four visibly unresolved and switch immediately back to the ordered
  7,080-selector frontier.  Return to bytecode afterward or sooner only if it
  blocks that frontier.
- The 2026-08-01 native headless GUI-action checkpoint advances the exact GNU
  C primitive inventory to 1,386/1,420, leaving 34 missing.  Four safely
  probeable graphical entry points are now native: `x-create-frame`,
  `x-show-tip`, `x-file-dialog`, and `x-select-font`.  They preserve GNU's
  arities, proper-list, string, and live-frame validation, including the
  primitive-specific ordering of validation against the display-backend
  check, before reaching Emaxx's honest catchable headless errors.  A direct
  sibling-GNU oracle pins every reachable pre-backend path.  Real display
  connection management, menu selection, and drag remain unclaimed:
  `x-open-connection` reaches the host display backend and can block, so it was
  not disguised as a headless stub.  Exact inventory fingerprints are mirrored
  `(1_386, 17_559_018_464_045_209_336)` and missing
  `(34, 1_222_455_694_860_570_834)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,638 library tests (1,634 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  The 34-item remainder is: `alloc.c` 3, `bytecode.c` 2, `comp.c` 9,
  `module-load` 1, `gnutls.c` 10, display connections 2, menus 3, portable
  dumper 2, `re--describe-compiled` 1, and drag-and-drop 1.  Finish every
  non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, keep
  those four visibly unresolved and switch immediately back to the ordered
  7,080-selector frontier.  Return to bytecode afterward or sooner only if it
  blocks that frontier.
- The 2026-08-01 native font-backend checkpoint advances the exact GNU C
  primitive inventory to 1,382/1,420, leaving 38 missing and no remaining
  `font.c` entry.  The final eight primitives preserve GNU's exact
  font/entity/object, character, frame, glyph-string structure, cached-string
  identity, and validation order.  Because Emaxx has no graphical font
  entities or objects, real backend access stops at an explicit headless
  boundary.  GNU 30.2 aborts its batch process when its frame-first calls or a
  valid font spec reach a tty-only backend; Emaxx uses the established
  catchable "Window system frame should be used" error.  A direct sibling-GNU
  oracle pins all reachable pre-backend behavior, including the coding-system
  glyph-string fast path, while an Emaxx-only assertion pins the safe
  replacement for GNU's abort.  Exact inventory fingerprints are mirrored
  `(1_382, 9_974_182_275_177_395_014)` and missing
  `(38, 5_533_127_793_467_509_550)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,637 library tests (1,633 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  The 38-item remainder is: `alloc.c` 3, GUI frame/tip creation 2,
  `bytecode.c` 2, `comp.c` 9, `module-load` 1, `gnutls.c` 10, display
  connections 2, `x-select-font` 1, menus/dialogs 3, portable dumper 2, file
  dialog 1, `re--describe-compiled` 1, and drag-and-drop 1.  Finish every
  non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, leave
  those four explicitly tracked and resume the ordered 7,080-selector
  frontier.  Return to bytecode afterward or sooner only if it blocks that
  frontier.
- The 2026-08-01 native GnuTLS process-state checkpoint advances the exact GNU
  C primitive inventory to 1,374/1,420, leaving 46 missing.  Every
  subprocess, network, pipe, and serial process now owns private GnuTLS boot,
  stage, and activity state.  Six newly native primitives cover asynchronous
  boot-parameter storage, initial-stage reads, safe inactive deinitialization,
  pre-READY peer status, all 16 certificate-warning descriptions, and GNU's
  broad error predicate.  The future boot parameters remain correctly
  invisible to `process-plist`, and this pre-session theme does not pretend a
  TLS transport is active.  A direct sibling-GNU oracle pins state transitions,
  every warning string, arbitrary error inputs, and validation failures.
  Exact inventory fingerprints are mirrored
  `(1_374, 3_007_806_732_422_836_430)` and missing
  `(46, 4_602_497_257_345_269_984)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,636 library tests (1,632 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  The 46-item remainder is: `alloc.c` 3, GUI frame/tip creation 2,
  `bytecode.c` 2, `comp.c` 9, `module-load` 1, `font.c` 8, `gnutls.c` 10,
  display connections 2, `x-select-font` 1, menus/dialogs 3, portable dumper
  2, file dialog 1, `re--describe-compiled` 1, and drag-and-drop 1.  Finish
  every non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, leave
  those four explicitly tracked and resume the ordered 7,080-selector
  frontier.  Return to bytecode afterward or sooner only if it blocks that
  frontier.
- The 2026-08-01 native GnuTLS digest checkpoint advances the exact GNU C
  primitive inventory to 1,368/1,420, leaving 52 missing.  The new
  `gnutls-digests` surface reproduces GNU 30.2's exact nine-entry descriptor
  order, IDs, and lengths; `gnutls-hash-digest` accepts symbol, string,
  descriptor-plist, and numeric selectors, supports GNU's direct and sliced
  string/buffer inputs, and returns real unibyte digest bytes.  The
  implementation builds on the established RustCrypto MD5/SHA crates plus
  pinned digest-0.10-compatible `streebog` 0.10.2 and `gost94` 0.10.4 rather
  than locally implementing cryptography.  A direct sibling-GNU oracle pins
  all nine algorithms, selectors, slices, and validation errors.  Although
  that sibling build does have GnuTLS enabled, this digest-only theme does not
  claim a TLS session backend; `gnutls-available-p` remains nil until one
  exists.  Exact inventory fingerprints are mirrored
  `(1_368, 7_429_719_598_662_435_112)` and missing
  `(52, 11_972_645_001_314_988)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,635 library tests (1,631 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  The 52-item remainder is: `alloc.c` 3, GUI frame/tip creation 2,
  `bytecode.c` 2, `comp.c` 9, `module-load` 1, `font.c` 8, `gnutls.c` 16,
  display connections 2, `x-select-font` 1, menus/dialogs 3, portable dumper
  2, file dialog 1, `re--describe-compiled` 1, and drag-and-drop 1.  Finish
  every non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, leave
  those four explicitly tracked and resume the ordered 7,080-selector
  frontier.  Return to bytecode afterward or sooner only if it blocks that
  frontier.
- The 2026-08-01 native X-faces checkpoint advances the exact GNU C primitive
  inventory to 1,366/1,420, leaving 54 missing.  The selected frame now owns a
  stable, real `eq` table returned by `frame--face-hash-table`; it contains the
  frame-local face vectors, preserves their identity, and is synchronized when
  later frame-local faces are created.  A direct GNU oracle pins stable table
  identity/test, global-versus-local visibility, shared vector identity,
  mutation, and validation errors.  `internal-face-x-get-resource` preserves
  GNU's string and live-frame validation, then reports Emaxx's established
  catchable headless window-system error.  This intentionally avoids GNU's
  internal batch-process abort for an otherwise valid call on a tty-only
  frame.  Exact inventory fingerprints are mirrored
  `(1_366, 18_079_045_798_471_271_648)` and missing
  `(54, 5_655_136_854_411_230_528)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,634 library tests (1,630 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  The 54-item remainder is: `alloc.c` 3, GUI frame/tip creation 2,
  `bytecode.c` 2, `comp.c` 9, `module-load` 1, `font.c` 8, `gnutls.c` 18,
  display connections 2, `x-select-font` 1, menus/dialogs 3, portable dumper
  2, file dialog 1, `re--describe-compiled` 1, and drag-and-drop 1.  Finish
  every non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, leave
  those four explicitly tracked and resume the ordered 7,080-selector
  frontier.  Return to bytecode afterward or sooner only if it blocks that
  frontier.
- The 2026-08-01 system-trash checkpoint advances the exact GNU C primitive
  inventory to 1,364/1,420, leaving 56 missing.  The native
  `system-move-file-to-trash` uses the established cross-platform `trash`
  5.2.6 crate for Finder Trash, Windows Recycle Bin, and freedesktop.org Trash
  semantics rather than recreating platform behavior.  The macOS path selects
  the crate's non-interactive native `NSFileManager` backend instead of its
  AppleScript default.  Shared file-name expansion, validation, watcher
  invalidation, deletion notification, and GNU's exact structured
  missing-file result remain in the Emaxx runtime.  A direct Emaxx smoke test
  moved a unique empty file through the real macOS Trash, verified it there,
  restored it, and cleaned the temporary artifact.  Exact inventory
  fingerprints are mirrored `(1_364, 4_594_607_034_609_466_038)` and missing
  `(56, 3_864_648_990_304_937_516)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,633 library tests (1,629 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  Per the latest user sequencing instruction, finish all non-bytecode native
  primitives first.  When only `byte-code`, `internal-stack-stats`,
  `make-byte-code`, and `make-closure` remain, leave that four-entry VM cluster
  explicitly tracked and resume the ordered 7,080-selector frontier; return to
  bytecode afterward or sooner if it directly blocks the selector frontier.
- The 2026-08-01 Tree-sitter query/traversal checkpoint completes the
  Tree-sitter native frontier and advances the exact GNU C primitive inventory
  to 1,363/1,420, leaving 57 missing.  The final ten primitives use the
  official `tree-sitter` 0.26.11 `Query`, `QueryCursor`, node range, and tree
  APIs for eager/lazy compiled queries, GNU sexp expansion, captures and
  `equal`/`match`/`pred` predicates, range filtering, child/descendant lookup,
  named predicate settings, forward/backward searches, sparse-tree induction,
  and subtree statistics.  A narrow same-width syntax bridge adapts GNU's
  unpunctuated predicate spellings to the official runtime while preserving
  GNU query-error byte offsets; it is not a query or parser implementation.
  The real `tree-sitter-json` regression exercises every new primitive.
  Direct GNU probes against the same grammar module match captures, custom
  predicates, traversal order, sparse trees, and statistics; a committed
  oracle assertion pins pattern/query expansion and string escaping.  Exact
  inventory fingerprints are mirrored
  `(1_363, 787_443_652_193_165_785)` and missing
  `(57, 9_533_698_609_109_745_145)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,632 library tests (1,628 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  No Tree-sitter primitive remains; NEXT is the next coherent family in the
  57-item native remainder.
- The 2026-08-01 Tree-sitter loader/parser checkpoint advances the native GNU
  C primitive frontier to 1,353/1,420, leaving 67 missing.  Twenty-five
  formerly missing primitives now provide GNU-compatible grammar discovery,
  parser reuse/list/delete/tag/buffer/range/notifier lifecycle, real parse
  trees, and safe node identity/traversal/introspection.  Dynamic grammar
  modules are loaded in GNU's extra-path, user-directory, then system order
  through the established `libloading` crate; their module handles stay alive
  behind official `tree-sitter` `Language`, `Parser`, and `Tree` values.
  Nodes resolve safely against their owning tree and become explicitly
  outdated after reparsing.  The official `tree-sitter-json` grammar is a
  test-only fixture proving real parse output, positions, field/sibling/parent
  traversal, included ranges, edit invalidation, and deletion semantics.
  GNU comparison also pins unavailable-grammar parser creation and load-error
  detail.  Only ten higher-level Tree-sitter primitives remain: sparse-tree
  induction, two range/node match helpers, pattern/query expansion and capture,
  two searches, and subtree statistics.  The complete publication gate is
  green: rustfmt, strict Clippy, and diff checks; all 1,631 library tests
  (1,627 in the restricted sandbox plus the four exact localhost socket tests
  with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).  NEXT is the remaining ten-operation Tree-sitter
  query/search theme on the same official runtime.
- The 2026-08-01 Tree-sitter runtime checkpoint advances the native GNU C
  primitive frontier to 1,328/1,420, leaving 92 missing.  Emaxx now uses the
  official MIT-licensed `tree-sitter` Rust crate, pinned at 0.26.11, rather
  than implementing a parser runtime.  Nine newly mirrored primitives expose
  GNU's exact runtime ABI boundary (latest 15, minimum compatible 13),
  parser/node/compiled-query predicates, node/query ownership errors, and
  identity-preserving lazy query compilation.  Language probes still report
  unavailable grammars honestly; an eager compile reports
  `treesit-load-language-error`, while a non-eager query remains a real
  opaque object ready for later compilation.  The full matrix caught and
  repaired the loaded GNU Semantic mode path that begins using lazy queries
  once `treesit-available-p` truthfully becomes non-nil.  Thirty-five
  Tree-sitter primitives remain for grammar loading, parser/tree/node state,
  ranges, queries, and searches.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,630 library tests (1,626 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).  NEXT is the real grammar-loader and parser-state
  slice on the same official runtime.
- The 2026-08-01 native-audit checkpoint advances the GNU C primitive
  frontier to 1,319/1,420, leaving 101 missing.  Nineteen display primitives
  now expose the honest headless backend boundary: `x-display-list` and
  `x-hide-tip` return nil, while the display/server capability queries and
  `xw-color-*` lookups accept GNU's exact arities and signal that no window
  system is initialized.  GUI creation, connection, dialogs, drag-and-drop,
  and font-selection actions remain deliberately unclaimed until real
  backends exist.  Tree-sitter is also still unclaimed; when that theme
  starts, use the established official Rust `tree-sitter` ecosystem rather
  than reimplementing its parser runtime.  The complete publication gate is
  green: rustfmt, strict Clippy, and diff checks; all 1,629 library tests
  (1,625 in the restricted sandbox plus the four exact localhost socket tests
  with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).  NEXT is another coherent native theme; commit and
  push each theme, then return immediately to the remaining inventory until
  all 1,420 are honest native surface contracts.
- Fresh 2026-07-29 current-code frontier is 2,332/7,080.  The GV batch
  repaired four shared subprocess boundaries: GNU single-dash long options
  (including `-batch`) now parse without stealing `-b` from
  `--no-build-details`; `--eval` and `--load` actions execute in their
  original command-line order; top-level `gv-define-setter` declarations
  update the byte compiler's expansion environment before later forms; and
  noninteractive `message`/printer output reaches stderr/stdout.  Unhandled
  batch Lisp conditions now use GNU's raw diagnostic and exit 255, including
  readable escaping for compound function symbols such as
  `\(setf\ gv-test-foo\)`.  The complete eight-outcome GV replay is green
  (six passes plus the same two expected failures) in
  `target/compat/run-1785410977097230000-12713`.
  Current-code revalidation then passed hierarchy 68/68
  (`target/compat/run-1785411070250562000-12998`), icons 2/2
  (`target/compat/run-1785411094145466000-13125`), let-alist 7/7
  (`target/compat/run-1785411113663745000-13246`), lisp-mnt 3/3
  (`target/compat/run-1785411133415310000-13364`), and lisp-mode 21/21
  (`target/compat/run-1785411565689624000-14556`).  That last replay also
  found and repaired a later scanner regression: loaded Lisp modes delegate
  to native `prog-mode`, which must install GNU's buffer-local
  `parse-sexp-ignore-comments` setting.  The grouped `lisp-tests.el` artifact
  `target/compat/run-1785411624225543000-14797` matches 35/37 outcomes and
  proves the contiguous prefix through selector 2,332.  NEXT is selector
  2,333, `lisp-forward-sexp-python-triple-quoted-string`; it and the adjacent
  triple-quotes case currently fail with `void-function syntax-class`.  The
  complete publication gate is green: rustfmt, strict Clippy, and diff checks;
  1,628 library tests (1,624 in the restricted sandbox plus the four exact
  localhost socket tests with networking allowed); 28 compatibility-harness
  tests, 1 performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus
  the zero-test main binary).
- The 2026-07-29 native-audit checkpoint resumes the host primitive
  frontier after publishing `702d1ae`: 1,300/1,420 configured GNU C
  primitives now have native Rust surface contracts, leaving 120 missing.
  The pure native-comp introspection boundary now supplies exact
  `comp--subr-signature` behavior plus the three capability queries whose
  correct no-backend result is nil.  The implementable portable-dumper
  boundary supplies the ordinary relocation offset comparator and reports
  nil statistics because Emaxx was not restored from a `.pdmp`.  Actual
  native compilation, `.eln` loading, dump creation, and copied-object
  address sorting remain deliberately unclaimed until their real backends
  exist.  Fast Rust tests compare the pure helpers with GNU, pin the
  capability/runtime-state contracts, and keep the complete sorted native
  inventory fingerprinted.  The complete publication gate is green: rustfmt,
  strict Clippy, and diff checks; 1,626 library tests (1,622 in the restricted
  sandbox plus the four exact localhost socket tests with networking allowed);
  28 compatibility-harness tests, 1 performance-harness test, 5 CLI tests,
  and 3 ERT-runner tests (plus the zero-test main binary).
- Fresh 2026-07-29 current-code frontier is 2,199/7,080.  The repaired
  `ert-font-lock-tests.el` matches all 40 GNU outcomes in
  `target/compat/run-1785290189192211000-29635`; the subsequent green grouped
  replays are ERT-X 28/28
  (`target/compat/run-1785290232431202000-29821`), Faceup 15/15 and 1/1
  (`target/compat/run-1785290260618644000-29994` and
  `target/compat/run-1785290274721823000-30198`), Find Function 6/6
  (`target/compat/run-1785292073893687000-32147`), Float-Sup 1/1
  (`target/compat/run-1785292201770742000-32661`), and Generator 92/92
  (`target/compat/run-1785292226029589000-32836`).  NEXT is selector 2,200,
  `gv-define-expander-in-file`, in `test/lisp/emacs-lisp/gv-tests.el`.  Its
  grouped replay `target/compat/run-1785292257313614000-33008` has four
  mismatching outcomes rooted in subprocesses passing GNU's `-b` batch
  shorthand, which the Emaxx CLI currently rejects.
  This batch repairs shared boundaries rather than those individual tests.
  The native ERT runner now installs GNU's dynamic `ert--pass` catch while a
  test body runs.  The dumped-Lisp façade supplies the complete preloaded
  `find-tag-default*` family.  Simulated minibuffer TAB delegates to the loaded
  Lisp completion-style engine, preserving programmed tables' completion
  bases, while bare embedded interpreters retain the native fallback.
  `file-name-all-completions` now opens the directory before producing dot
  entries and signals GNU's `file-missing` contract for a missing directory.
  Finally, generated `etc/DOC` is copied into each clean compatibility
  checkout and included in the support fingerprint, so source lookup cannot
  silently use a missing or stale documentation index.  Fast Rust regressions
  cover the three real ERT success paths, the native missing-directory
  contract, all six upstream Find Function outcomes, and DOC copy/restore/
  fingerprint behavior.  The in-process upstream helper now sets GNU source,
  data, and documentation directories explicitly instead of accidentally
  inspecting the Emaxx working directory.  Final gates are green: rustfmt,
  strict Clippy, diff check, 1,624 library tests, 28 compatibility-harness
  tests, 1 performance-harness test, 5 CLI tests, and 3 ERT-runner tests,
  plus the zero-test main binary.  The preceding EIEIO checkpoint was
  committed and pushed as `2c6bec1`.  Per the current priority, publish this
  coherent checkpoint and then resume the 126-missing native primitive
  inventory before fixing the next GV oracle mismatch.
- Fresh 2026-07-28 current-code frontier is 1,957/7,080.  The re-sweep reached
  1,911 through `easy-mmode-tests.el`; all 46 selected Edebug outcomes then
  matched GNU in
  `target/compat/run-1785255202704658000-17314` (45 passes and the same one
  expected failure).  Next is the eight selected tests in
  `test/lisp/emacs-lisp/eieio-tests/eieio-test-methodinvoke.el`.  The older
  2,879 frontier remains historical evidence, not the current-code position.
  The Edebug repair is thematic: recursive command loops report errors and
  permit post-command macro resumption like GNU; modifier-bearing keyboard
  events normalize before minibuffer editing; native and loaded-Elisp timer
  representations share one event-pump boundary; entire-file evaluations
  replace same-file load history; and the Rust-backed generic facade performs
  exact primary/qualifier method replacement and unload without retaining
  stale closures or metadata.  Fast Rust tests cover the six order-sensitive
  Edebug cases, recursive-edit timers and nonlocal exits, repeated generic
  load/reload/unload, and final `cl-no-applicable-method` state.  The release
  gate additionally found and fixed a pre-existing optimized-build mutex bug:
  recursive lock restoration had incorrectly lived inside `debug_assert!`.
  Rustfmt, strict Clippy, and diff checks are green.  The publication gate
  passes 1,618/1,618 library tests (1,614 in the socket-restricted sandbox and
  four exact localhost tests with permission), 28 compatibility-harness, 1
  performance-harness, 5 CLI, and 3 ERT-runner tests, plus the zero-test main
  binary.
- Fresh 2026-07-28 post-native-audit revalidation is green through
  1,807/7,080 selected tests, ending with all 13 selected
  `cl-seq-tests.el` cases.  Next is the 93-test
  `test/lisp/emacs-lisp/comp-cstr-tests.el` manifest entry.  The older
  measured frontier remains 2,879 and should not be discarded; 1,807 is the
  current-code re-sweep checkpoint that must catch up to it.  This batch
  fixed five shared contracts: indexed GNU preload ownership, general
  structural buckets for `equal` hash tables, a fast ordinary-symbol
  `eq`/`memq` path that avoids irrelevant symbol-with-position policy reads,
  GNU Lisp ownership of the interactive `undo` command, and GNU-compatible
  dynamic `macroexp--dynvars` scoping in native `macroexpand-all`.  The last
  item repairs `cl-macs--labels` at the common expansion layer and keeps
  sequential `defvar` declarations visible without leaking nested scopes.
  Fast Rust tests pin each contract, including scaled hash/memq cases and the
  combined upstream CL selectors.  Electric passes 874/874 and `cl-macs`
  passes 61/61 in exact replay.  Rustfmt and strict Clippy are green.  The
  full library gate passed all 1,611 sandbox-compatible tests; the four
  localhost tests rejected only by sandbox policy all pass under their exact
  fully-qualified selectors with socket permission, for 1,615/1,615 semantic
  passes.  The other publication targets pass 28 compatibility-harness, 1
  performance-harness, 5 CLI, and 3 ERT-runner tests (plus the zero-test
  binary target).  Compatibility runs remain source-fingerprinted and
  isolated so they cannot test a stale Emaxx binary.
- The 2026-07-28 native-audit checkpoint completes `frame.c`, bringing
  the exact inventory to 1,294 mirrored / 126 missing.  Frame and terminal
  objects are no longer synthetic Lisp symbols: distinct opaque host values
  and typed interpreter state now back all 35 formerly missing `frame.c`
  primitives, the already claimed frame family, and all eight `terminal.c`
  primitives.  The shared geometry model separates frame parameters, native
  total size, text size, root-window size, and the minibuffer line.  Direct
  GNU comparison also fixed window-configuration restore: recorded dimensions
  participate in configuration equality but do not rewind a later live-frame
  resize.  Five fast Rust/GNU regressions cover frame and terminal identity,
  traversal, parameters, geometry, focus/mouse/headless errors, and
  configuration restore, while the existing terminal regressions cover
  liveness, deletion hooks, parameters, and TTY controls.  The broad suite
  caught and corrected one stale Todo/window assertion that assumed the
  24-line initial root window equaled its 25-line frame.  Rustfmt, strict
  Clippy, and diff checks are green.  The publication gate passed 1,610
  library, 28 compatibility-harness, 1 performance-harness, 5 CLI, and 3
  ERT-runner tests with zero failures.  The exact 1..N/7080 replay remains
  pending.
- The 2026-07-28 native-audit checkpoint completes the pure/headless
  `font.c` core and all six `fontset.c` primitives, bringing the exact
  inventory to 1,259 mirrored / 161 missing.  The shallow name-only
  `font-spec` facade has been replaced by one mutable normalized 13-slot font
  record shared by construction, lookup, mutation, matching, XLFD generation,
  and headless lookup/cache behavior.  A typed fontset registry owns
  default/custom sets, character/range/script/fallback mappings,
  prepend/append precedence, recreation semantics, ASCII protection, and the
  headless error boundary.  Four direct GNU family regressions cover all 19
  claimed font/fontset names, including exact validation and ordering.
  The eight remaining `font.c` names require real font entities/objects or a
  GUI font driver and remain deliberately unclaimed rather than nil-stubbed.
  Font/face-targeted tests, rustfmt, strict Clippy, and diff checks are green.
  The complete publication gate passed 1,605 library, 28
  compatibility-harness, 1 performance-harness, 5 CLI, and 3 ERT-runner
  tests with zero failures.  The exact 1..N/7080 replay remains pending.
- The 2026-07-28 native-audit checkpoint completes the 25-function
  headless semantic core of `xfaces.c`, bringing the exact inventory to 1,243
  mirrored / 177 missing.  One interpreter-owned mutable 20-slot face-vector
  registry now replaces the former split between synthetic attribute
  properties and a separate inheritance list.  Native creation, copy,
  mutation, lookup, equality/emptiness, global/selected-frame state,
  inheritance, relative-height merging, resource conversion, bitmap/color
  queries, font-selection state, and color-file parsing all share it, and the
  existing high-level face/theme paths have been integrated with the same
  source of truth.  A fast family regression executes all 25 claimed names
  against GNU, including vector identity and external `aset` mutation.
  `frame--face-hash-table` remains deferred to the proper multi-frame model;
  `internal-face-x-get-resource` remains deferred to a real GUI resource
  backend because GNU aborts when it is called on the batch terminal.  The
  targeted face suite, rustfmt, strict Clippy, and diff checks are green.
  The publication gate passed 1,601 library, 28 compatibility-harness, 1
  performance-harness, 5 CLI, and 3 ERT-runner tests.  The first sandboxed
  run's four localhost-socket permission failures all passed when the same
  gate was rerun with socket permission.  The exact 1..N/7080 replay remains
  pending.
- Post-`26e68a4` native audit progress completes `doc.c`, bringing the exact
  inventory to 1,218 mirrored / 202 missing.  Emaxx now uses one native DOC
  index for `F`/`V`/`S` records, signed user-variable offsets, static/lazy
  reference resolution, variable aliases, and stale offset zero across
  `Snarf-documentation`, `internal-subr-documentation`, `documentation`, and
  `documentation-property`.  This replaces a partial facade that rejected
  GNU's optional `RAW` argument and exposed unresolved offsets.  The producer
  fix also restores the dumped global `C-f` / `forward-char` binding and
  `help-key-binding` text properties from `substitute-command-keys`.  A
  synthetic-DOC GNU oracle regression covers the complete shared contract.
  The publication gate passed 1,600 library, 28 compatibility-harness, 1
  performance-harness, 5 CLI, and 3 ERT-runner tests.  The exact 1..N/7080
  replay remains pending.
- Post-`4bb5a43` native audit progress completes `composite.c`, bringing the
  exact inventory to 1,216 mirrored / 204 missing.  The prior
  `find-composition-internal` grapheme approximation is replaced by a native
  family model for static buffer/string composition, registration, detail
  output, headless terminal glyph strings, automatic combining clusters, rule
  sorting, and cache reset.  The shared interval layer now uses GNU `eq`
  identity for both adjacent-property merging and single-property change
  scans, so distinct but structurally equal composition descriptors retain
  their boundary.  A direct GNU family regression covers all six primitives,
  all composition methods, reversed buffer bounds, search/clamping, glyph
  metrics, exact errors, and the shared identity invariant.  The complete
  publication gate passed 1,599 library, 28 compatibility-harness, 1
  performance-harness, 5 CLI, and 3 ERT-runner tests.  The exact 1..N/7080
  replay remains pending.
- Post-`b3c90e2` native audit progress completes `fringe.c`, bringing the
  exact inventory to 1,211 mirrored / 209 missing.  The former
  `define-fringe-bitmap` nil stub and its superficial test are replaced by an
  interpreter-owned native registry with GNU-visible standard/user bitmap
  IDs, replacement/destruction, face overrides, exact validation, and
  headless row-query behavior.  A family-level Rust oracle test covers the
  full contract.  The publication gate passed 1,598 library, 28
  compatibility-harness, 1 performance-harness, 5 CLI, and 3 ERT-runner
  tests.  The exact 1..N/7080 replay remains pending.  `menu.c` remains
  missing because GNU
  `menu-bar-menu-at-x-y` aborts the initial batch frame; do not replace that
  unknown live-frame contract with a nil shim.
- Post-checkpoint native audit progress on 2026-07-27 completes `indent.c` and
  the remaining `xdisp.c` surface, advancing the exact inventory to 1,208
  mirrored / 212 missing.  Family-level GNU-oracle Rust regressions cover
  display motion, line-number width, continuation boundaries, bidi paragraph
  boundaries and RTL visual motion, headless display queries, and image-map
  geometry.  Native metadata generation now reads the authoritative GNU
  C-source manifest instead of scraping arbitrary Rust string literals; an
  exhaustive fast test checks every known-arity C primitive and prevents
  dumped Lisp names from leaking across the ownership boundary.  The
  follow-up full fast gate is green: 1,597 library, 28 compatibility-harness,
  1 performance-harness, 5 CLI, and 3 ERT-runner tests passed.  The exact
  1..N/7080 replay remains pending.
- Authoritative 2026-07-27 native-audit checkpoint: a generated manifest reads
  GNU 30.2 C `DEFUN` declarations and intersects them with the configured
  oracle build.  Of 1,685 source names, 1,420 are host-available; Emaxx has
  exact native Rust surface contracts for 1,197 and 223 remain missing.  A
  fast test fingerprints both complete sorted sets and checks every claimed
  mirror's arity, command/special-form metadata, and Rust dispatch route.
  This is exhaustive surface coverage, not a claim that all deep primitive
  semantics are already exhaustive.
- This batch completed libxml-backed XML/HTML parsing and coherent
  `window.c`, `emacs.c` termination, `data.c`, `terminal.c`, and `dispnew.c`
  families.  It also fixed process-sentinel reentrancy at the process-state
  abstraction: status notification is claimed before invoking a sentinel, so
  a sentinel can delete its own process exactly once, matching GNU.  Direct
  fast Rust/GNU probes cover the repaired contracts.
- The 2026-07-27 checkpoint gate is fully green:
  `cargo test --all-targets --all-features` passed 1,594 library, 28 harness,
  1 performance-harness, 5 CLI, and 3 ERT-runner tests.  Rustfmt,
  `git diff --check`, and Clippy with `-D warnings` also passed.  The full test
  command requires localhost socket permission for real process/network
  tests.  The exact 1..N/7080 oracle replay was not rerun during this final
  native-audit batch; the 2026-07-25 Delimited Columns frontier below remains
  the latest measured compatibility position.
- Authoritative 2026-07-25 frontier: all nine selected tests in
  `test/lisp/delim-col-tests.el` pass together against GNU in
  `target/compat/run-1784977186186812000-88703`.  Immediately before it,
  Dabbrev is 16/16 in `target/compat/run-1784976862207828000-88306`, Custom is
  9/9 in `target/compat/run-1784973964022350000-85646`, and Completion Preview
  is 11/11 in `target/compat/run-1784973333950428000-84528`.  NEXT is the
  three selected tests in `test/lisp/descr-text-tests.el`.
- The latest shared native repairs preserve the GNU ownership boundary.
  Rust now implements `modify-frame-parameters`, selected/old-selected-window
  state, and window use-time ordering; the real Custom and Dabbrev Lisp
  remains authoritative above those primitives.  GNU's C keyboard-macro loop
  catches only its explicit `minibuffer-quit` condition, so Emaxx no longer
  swallows an ordinary `user-error` merely because `command-error-function`
  is customized.  Fast Rust tests cover the direct command-loop contract and
  the formerly failing killed-buffer/cross-buffer Dabbrev paths.
- Completion Preview's failure was the complementary preload theme, not a
  native gap: GNU dumps the Lisp-owned `forward-whitespace`, `forward-symbol`,
  and `forward-same-syntax` helpers from `subr.el`.  Emaxx now preloads those
  same policy functions in Lisp, with a fast full-file Completion Preview
  regression.  Do not move them into Rust.
- The Todo repair is a subsystem-level `window.c` pass, not Todo-specific
  behavior.  Emaxx now owns GNU's native window variable/geometry/resize
  families in Rust, initializes all native window slots at construction,
  distinguishes valid internal windows from live leaf windows, and maintains
  real split/delete parent-sibling topology.  In particular,
  `window-buffer` correctly returns nil for an internal window.  The real
  preloaded `window.el` remains responsible for higher-level policy.  Fast Rust
  coverage exercises the native defaults, geometry, resize state, topology,
  hooks, deletion, and the complete previously failing Todo path.
- The earlier mechanical native-surface audit has been superseded by the
  generated 2026-07-27 function snapshot above.  Its separate variable audit
  found 684 active variables declared from GNU C, of which 338 were unbound in
  Emaxx; that variable list has not yet received the same generated exact
  checkpoint.  These are inventory gaps, not proven bugs: GUI/platform/compiler
  features may be outside the batch target, and bound names still require
  behavioral contract probes.  The recurring gap is nevertheless thematic.
  Audit native families by GNU source subsystem, preserve the C-to-Rust and
  Lisp-to-Lisp boundary, and add table-driven fast Rust contracts rather than
  waiting for large oracle failures.  This pass reduced missing `window.c`
  function bindings from 45/117 to 19.
- Trustworthy 2026-07-24 cumulative baseline:
  `target/compat/run-1784899759213881000-79075` covers the canonical 225-file
  prefix through `files-x-tests.el`, with 173 exact file matches and 52 known
  mismatches.  The newer clean canonical prefix through
  `test/lisp/calendar/icalendar-tests.el` is 22/22 in
  `target/compat/run-1784938521267523000-23325`; this folds the Emacsclient,
  Archive, Auto-Revert, Bookmark, and iCalendar repairs into ordered execution
  with no early regression.  iCalendar itself is 41/41 in that artifact.  A
  new 225-file cumulative replay is still pending.  NEXT is
  `test/lisp/calendar/iso8601-tests.el`.  Do not use the polluted
  shared-tree 170/225 artifact or present the deliberately tabled `.elc.gz`
  VM selector as newly verified strict progress.
- The gate now uses two ephemeral pinned/clean GNU checkouts and restores only
  fingerprinted generated Lisp and explicit `lib-src` test helpers between
  files.  It excludes ignored prior Emaxx output, never copies GNU's
  `src/emacs`, remaps only load-path directories, and verifies the support
  fingerprint at completion.  Both runners also override GNU's dumped
  `source-directory` before loading each test, so tracked fixtures resolve
  inside the clean checkout rather than the writable sibling tree.  Fast
  harness tests make stale code and mutable shared test resources
  regression-tested invariants.
- Auto-Revert now matches all 7 selected GNU tests.  The repair preserves the
  high-level Lisp state machine: file replacement never performs its own
  confirmation, and the atomic delete+insert dynamically hides
  `buffer-file-name`, exactly where GNU suppresses the generic stale-file edit
  guard.  Bookmark is 47/47 after completing the native
  `find-coding-systems-region-internal` position/EXCLUDE contract and the real
  UID/GID primitive family.  Fast Rust tests cover each host boundary.
- iCalendar is 41/41 after fixing a shared time boundary rather than its Lisp:
  runtime `setenv("TZ", ...)` installs a mutable/property-capable Lisp string,
  while local civil-time encoding recognized only Emaxx's compact string
  representation.  Both local encode/decode paths now parse POSIX timezone
  rules through the common string accessor.  A fast Rust regression exercises
  the real upstream preload path, including winter, DST, and explicit UTC.
- Latest thematic fixes: GNU-compatible successful empty batch invocation;
  the missing `discard-input` host primitive; suffix-stripped auto-coding
  policy after transparent decompression; and real `undecided` coding
  detection with precise `last-coding-system-used`, correct multibyte/unibyte
  destinations, and correct coding-region return lengths.  These shared
  contracts make the full Emacsclient and Archive files pass and have direct
  fast Rust regressions.
- Active uncommitted batch: strict ordered progress is 3554/7080.  The four
  canonical automated filenotify selectors pass together in
  `target/compat/run-1784640263759277000-3945`.  The files slice is independently
  proven at 115/116 in `target/compat/run-1784639865227053000-3224`; its only
  mismatch is the explicitly deferred honest bytecode-VM work in
  `files-load-elc-gz-file`, which is selector 3555 and therefore blocks the
  strict prefix.  NEXT non-VM work is all seven selectors in
  `test/lisp/files-x-tests.el` (3670..3676).  Final rustfmt/Clippy/full-Rust
  gates, commit, and push are still pending.
- The latest thematic fixes keep GNU files.el responsible for high-level
  save/revert state machines and add the missing native
  `replace-buffer-contents` boundary with bounded non-destructive diffing.
  Dumped save-variable defaults, prompt dispatch, write-region VISIT state,
  handler-visible visited names, and missing-file visits now share GNU's
  contracts.  Bug#18141 was fixed at two native manifests: buffer.c's complete
  permanent-local slot set and the complete Unix/DOS/Mac variant families for
  all built-in text codings.  Focused Rust regressions cover every contract,
  including compressed visit/save and fine-grained revert end to end.
- The post-3125 thematic regression batch is ready to commit.  Final
  cumulative artifact `target/compat/run-1784589326956761000-16406` covers all
  201 canonical files through `em-glob-tests.el`; the Emaxx-only comparison
  with pre-fix artifact `target/compat/run-1784558452909597000-74441` has zero
  pass-to-fail, six fail-to-pass, no changed failures, and no missing/added
  results.  It repairs dynamic nil-env eval/progv, symbolic GV expanders,
  nested backquote depth, preloaded condition ancestry, the public
  `with-temp-buffer` macro contract, and lexical macro-expander invocation.
  The latter stores macros as callable expander closures rather than raw
  parameter/body tuples and clears `edebug-tests-cl-macrolet` both alone and
  in cumulative order.  Synthesized runner failures are now persisted, and
  source-owned locked builds plus provenance hashes prevent stale Emaxx gates.
  The full gate passes 1297 library tests, 25 harness tests, the perf test, and
  all three ERT integrations; rustfmt, Clippy with `-D warnings`, and diff
  checks are clean.  NEXT is selector 3126,
  `em-hist-test/add-to-history/allow-dups` in `em-hist-tests.el`, after this
  batch is committed.
- Verified through selector 3125/7080: all 27 selected
  `test/lisp/eshell/em-glob-tests.el' cases match GNU together.  The final
  remote-home case was a mock-Tramp localname contract, not Eshell policy:
  GNU resolves a mock remote `~/file.txt' to the remote home once file access
  establishes the connection, while Emaxx stripped the remote prefix but left
  the tilde for Eshell to misinterpret as a glob operator.  Mock localnames now
  use the existing host-home resolver consistently in `file-local-name',
  `file-remote-p' localname queries, and native file operations; other remote
  methods keep their parsed localname.  The Elisp/host boundary is unchanged.
  Focused Rust tests cover both that producer contract and the upstream Eshell
  case end to end.  The full gate passes 1208 library tests, 11
  compatibility-harness tests, the perf-harness test, and all three integration
  ERT runners; rustfmt, clippy with `-D warnings', and `git diff --check' are
  clean.  NEXT is selector 3126, `em-hist-test/add-to-history/allow-dups' in
  `test/lisp/eshell/em-hist-tests.el'.
- Verified through selector 3098/7080: all 17 selected
  `test/lisp/eshell/em-extpipe-tests.el' cases match GNU together.  The
  thematic repairs are below Eshell: `unwind-protect' now propagates a cleanup
  form's newer error/nonlocal exit instead of silently discarding it; forward
  regexp search treats nested `\\=' as an assertion at the original search
  point; and subprocess state changes are owned by the event pump so a fast
  child cannot appear dead before its output and sentinels are delivered.
  Linked stderr is drained/notified before the primary sentinel and terminal
  sentinels fire once.  The native `process.c' surface also supplies GNU's
  dumped defaults, default `utf-8-unix' coding pair, `:sentinel' and `:stderr'
  make-process options, and `process-command', `process-exit-status', and
  pipe-backed `process-tty-name'.  GNU Eshell policy remains in the upstream
  Elisp; the Rust changes stay at the evaluator, regexp, and process host
  boundaries.  Focused Rust tests cover every contract, including both
  end-to-end external pipeline forms and immediate redirected output.  The
  full gate passes 1206 library tests, 11 compatibility-harness tests, the
  perf-harness test, and all three integration ERT runners.  NEXT is selector
  3099, `em-glob-test/convert/absolute-start-directory' in
  `test/lisp/eshell/em-glob-tests.el'.
- Verified through selector 3081/7080: all 11 selected
  `test/lisp/eshell/em-dirs-tests.el' cases match GNU in the grouped replay
  and pass together in the fast native runner.  The thematic repair is at the
  producer boundary: `directory-files' now returns mutable, property-bearing
  strings like GNU's `directory_files_internal', so Eshell can decorate names
  without losing text properties.  Fast startup coverage also caught gaps
  masked by the oracle harness loading `ert.el': the shared batch initializer
  supplies GNU's dumped `seq' contract, preload errors are no longer ignored,
  and the complete public `pp.el' autoload surface is present with matching
  interactive flags.  The Eshell test fixture now uses that initializer rather
  than duplicating only part of startup.  A targeted forward replay restored
  GNU's `customize-set-value' autoload to the existing Elisp `cus-edit.el'
  implementation, making all four selected `em-ls-tests.el' cases pass without
  moving the Elisp/Rust boundary.  The apparent directory-ring order failure
  was disproved: only list-valued expansion failed, specifically because
  `pp-to-string' was absent.  Focused Rust tests cover each contract plus the
  end-to-end `cd' metadata behavior and all 11 directory-module tests in one
  interpreter.  Grouped replays pass for `em-dirs' (11), `em-cmpl' (27),
  `dired-tests' (16), and `em-ls' (4).  The full gate passes 1196 library
  tests, 11 compatibility-harness tests, the perf-harness test, and all three
  integration ERT runners; rustfmt, clippy with `-D warnings', and
  `git diff --check' are clean.  NEXT is selector 3082, `em-extpipe-test-1' in
  `test/lisp/eshell/em-extpipe-tests.el'.
- Verified through selector 3070/7080: all 27 selected
  `test/lisp/eshell/em-cmpl-tests.el' cases match GNU in the grouped replay.
  The fixes are thematic: correct local/default hook composition; nested
  lexical `let*' scopes and identity-safe closure cells without breaking
  dynamic callbacks; one consistent per-buffer/special variable resolver;
  GNU-style new-buffer initialization for `default-directory' versus
  `buffer-read-only'; dumped/autoload symbol discovery through the standard
  obarray; pcomplete command-family and Elisp-completion preload contracts;
  host-derived `system-name'; Lisp completion-table combinators; and native
  wildcard/ambiguous candidate behavior.  Completion-table policy remains in
  the Lisp compatibility layer and the host completion driver remains Rust.
  Every repaired contract has a fast Rust regression, including end-to-end
  Eshell completion cases.  The full gate passes 1190 library tests, 11
  compatibility-harness tests, the perf-harness test, and all three integration
  ERT runners; rustfmt and clippy with `-D warnings' are clean.  NEXT is
  selector 3071, `em-dirs-test/cd' in
  `test/lisp/eshell/em-dirs-tests.el'.
- Verified through selector 3038/7080: the remaining selected ERC core and
  track selectors (3001..3030) pass, followed by all eight selected
  `em-alias-tests.el' selectors (3031..3038).  File-wide `check-all' matches
  GNU for `erc-tests.el', `erc-track-tests.el', and `em-alias-tests.el'.
  The repairs are thematic rather than test-specific: current-buffer versus
  displayed-window state, complete display action parsing, GNU macro identity
  for iteration/control forms consumed by generator.el, lexical file-load and
  eval/macroexpansion context, preload/default contracts, final subprocess
  pipe draining, ellipsis width reservation, nested lexical message capture,
  and exact stored text-property plist order.  Focused Rust regressions cover
  each behavior.  The final gate passes 1168 library tests and every auxiliary
  target; rustfmt is clean and clippy passes all targets with `-D warnings'.
  NEXT is selector 3039, `em-basic-test/umask/print-numeric' in
  `test/lisp/eshell/em-basic-tests.el'.
- Verified through selector 2929/7080: all 17 manifest-selected
  `erc-services-tests.el' tests and all 12 selected
  `erc-stamp-tests.el' tests pass their default oracle comparisons.
  The services plstore cleanup exposed GNU's `(kill-buffer nil)'
  semantics: nil or an omitted argument means the current buffer.
  The stamp date-dedup test exposed three independent runtime gaps:
  `add-hook' ignored numeric depth (so ERC stamped at depth 70 before
  filling at depth 60 and fill wrapped the right stamp onto a new line),
  `format-spec' rendered a buffer with prin1 instead of princ semantics,
  and native `ert-deftest' stored conditional `:tags' forms unevaluated.
  Hook depth metadata, stable global/local ordering, and the local `t'
  default-hook splice now follow GNU; buffers in `format-spec' become
  their names; ERT metadata expressions are evaluated at definition time.
  The selector-2897 socket scenario remains green and the non-manifest
  `erc-d-run-no-block' speed race passes three consecutive runs.  All 1133
  Rust library tests and the auxiliary binary/integration suites pass.
- Verified through selector 2900/7080: all three manifest-selected
  `erc-scenarios-stamp.el' tests pass.  Selector 2898 needed real
  nonblocking-connect semantics: `make-network-process :nowait t' now
  reports `connect' initially, transitions to `open' on the next event
  pump, and only then calls the newly installed sentinel.  ERC therefore
  inserts its "Opening connection" status before logging in, as GNU does.
  A focused socket test covers the status and sentinel sequence.
  The former non-manifest `erc-d-run-no-block' speed race is also resolved:
  `move-to-column' is now single-pass instead of quadratic, compiled-regexp
  cache hits are constant-time and skip redundant validation, and dev builds
  optimize only the local `emaxx' crate so upstream wall-clock deadlines do
  not measure unoptimized interpreter overhead (debug assertions remain).
  `erc-d-run-no-block' passes repeated runs, selector 2897 remains green,
  and all 1128 Rust library tests plus auxiliary suites pass.
- Verified through selector 2897/7080:
  `erc-scenarios-misc-commands--AMSG-GMSG-AME-GME' passes.  The final
  ACTION-8 timeout was not an early/stale expiry and not a ring-loss bug:
  ACTION-7's timer was canceled, ACTION-8 stayed queued throughout the
  metered reply, and ACTION-8's own timer fired on time after 10 seconds.
  The real loss was in `run_pending_timers': an ERT timeout callback used a
  nonlocal `throw' while the runtime had all due timers detached in a local
  batch, so returning early silently discarded the later
  `erc-d--on-request' continuation.  `restore_unfired_timer_batch' now puts
  that tail back ahead of timers scheduled by callbacks.  A focused unit
  test reproduces the throw/catch sequence and failed before the fix.
  A prerequisite network fix now honors `:family ipv4' when resolving a
  listening host; on this machine `localhost' resolves to `::1' first while
  ERC connects to `127.0.0.1', which previously caused an earlier
  connection-refused failure.  Selector 2897, the default internal suite,
  scenarios-match check-all, and all 1127 Rust library tests pass.
- Verified through selector 2896/7080: `erc-scenarios-match.el'
  passes check-all (2895..2896; the intervening join/log scenario
  files select 0).  Root cause was `goto-char' RETURN VALUE: GNU
  `Fgoto_char' returns its POSITION argument UNCHANGED (a marker stays
  a marker), while emaxx returned the clamped integer point.
  `erc-display-msg' does `(marker-position (goto-char erc-insert-marker))'
  — with the integer return, `marker-position' got an integer and
  signalled `wrong-type-argument'.  Supporting compat additions the
  same scenario needed (all oracle-validated in isolation):
  - `coding-system-change-eol-conversion' native (buffer_meta.rs):
    returns the eol variant (unix/dos/mac/0/1/2), the base for nil
    eol-type, or the system itself when unchanged; nil designator maps
    to no-conversion/eol 0.  Added `undecided-{unix,dos,mac}' variants
    to the coding-system bootstrap so `undecided' has a full eol set.
  - `filepos-to-bufferpos' / `bufferpos-to-filepos' /
    `filepos-to-bufferpos--dos' ported verbatim from GNU mule-util.el
    into simple_compat.el (`erc--split-line' splits outgoing lines at
    encoded byte boundaries).
  - `find-composition' subset in simple_compat.el: emaxx has no
    automatic-composition engine, so it consults only the
    `composition' text property (erc avoids splitting a composed run).
  NOTE: `goto-char' is heavily used — full 141-file sweep + 1125 unit
  tests confirm the return-value change regresses nothing.
- Verified through selector 2894/7080: `erc-scenarios-internal.el'
  passes check-all (all erc-d-tests.el selectors, including the three
  timer-teardown tests and the unix-socket test).  Two fronts:
  - GNU process semantics (the "timer race" was really a MISSING
    SENTINEL): `delete-process' on a network process runs the
    process's own sentinel SYNCHRONOUSLY with "deleted\n"
    (Fdelete_process sets status (exit 0) and calls status_notify
    in-line; a process whose death was already notified is out of the
    process list, so the sentinel never fires twice —
    `delete_process_notifying' in processes.rs notifies while the
    network runtime is still attached, demoting sentinel errors like
    exec_sentinel unless debug-on-error).  erc-d's teardown chain
    rides on that sentinel: the first `erc-d--expire' finalizer
    deletes its client, the "deleted" event reaches
    `erc-d--process-sentinel', and THAT calls `erc-d--teardown' —
    deterministically at ~1.03s like the oracle, no timer race at all.
  - `process-contact' fidelity: the full keyword contact plist is now
    stored per process (GNU p->childp) with :service resolved and
    :local/:remote sockaddr vectors ([127 0 0 1 PORT]) appended;
    accepted children get the server's plist with :server nil, :host
    peer-ip, :service peer-port; KEY t returns the plist, any KEY on a
    real child returns t; children are named "NAME <HOST:PORT>" (with
    the space, like GNU server_accept_connection).
    `erc-d-run-nonstandard-messages' needed exactly this (its log id
    is `(aref (plist-get (process-contact P t) :remote) 4)').
  - Unix domain sockets (`:family local', erc-d-unix-socket-direct):
    NetworkRuntime::UnixListener/UnixStream; :service is the socket
    PATH; contact :local is the path (server) / "" (client), children
    are "NAME <N>" (GNU connect_counter) with :host t :remote "";
    delete-process leaves the socket file for the test to delete.
    Two-argument `featurep' checks (get FEATURE 'subfeatures), and
    make-network-process is provided with GNU's subfeature list.
  - Interpreter throughput (erc-d-run-no-block is a pure client-speed
    race: the fuzzy ~join-bar exchange expires 1.5s after creation,
    and erc must send JOIN #bar through flood control and have the
    in-process dumb server match it first).  Message handling went
    ~205ms -> ~90ms per PRIVMSG:
    - Per-callsite MACRO-EXPANSION CACHE (the big one): compiled GNU
      code expands each macro call once, while emaxx re-expanded
      pcase/rx/when-let machinery on every evaluation
      (internal--build-bindings alone was 320 calls/message).  Cached
      by the form's car-cell identity — the entry pins the form so
      the address can't be reused — and validated against a
      definition generation bumped on every function/macro/advice
      (re)definition, cl-macrolet push/drain, and gv-expander/
      gv-setter/setf-method/cl-deftype-handler property writes.
    - A not-a-macro name verdict cache (same generation) skips the
      whole macro probe for plain function calls; verdicts influenced
      by cl-flet frame shadowing are never cached, and frame shadowing
      can only make a name LESS of a macro so cached verdicts stay
      correct under any frames.
    - `name_facts' memo (dispatch.rs): is_builtin/special-form/
      prefer-override/dispatch-module routing were giant linear
      `matches!' chains consulted per form; now one hash lookup.
    - Macro-position resolution scans only FUNCTION_FRAME_MARKER
      frames (GNU: value bindings never shadow macros in function
      cells); marker frames carry the marker as their FIRST entry.
    - `sf_if' runs an allocation-free setcdr pre-scan before engaging
      the self-mutating-form tail-alias machinery; variable lookup
      fast-paths non-aliased names (Cow, no per-lookup String).
    - EMAXX_PROFILE=<path> (dev-only): flat per-name call/self-time
      profiler in call_function_value, dumped periodically.
- NEXT = selector 2901, the first of 17 selected tests in
  erc-services-tests.el (2901..2917), followed by erc-stamp-tests and
  erc-tests.  ALWAYS consult
  `selected=' in compat/oracle_tests_all.txt before working a
  check-all failure — most scenario-file check-all failures are on
  NON-selected tests and don't gate the frontier.  Sweep gate =
  /tmp/probes/sweepH.sh over prefix-files20.txt + znc = 142 runs on
  frozen /tmp/probes/bin binaries.
- Previous milestone — verified through selector 2881/7080:
  `erc-scenarios-base-upstream-recon-znc.el' (2881) passes check-all
  (both `--znc/severed' and the :expensive two-network `--znc' test).
  The decisive fix was a NATIVE `format-spec' (prefer_builtin_override
  in primitives.rs; the interpreted format-spec.el cost tens of ms per
  call and erc updates the mode line via `format-spec' on every
  message, so the two-network burst ran 5-10x too slow and reply
  timers missed their expect windows).  The native version is
  property-aware (result inherits FORMAT's text props like GNU's
  insert-and-inherit buffer build) and passes format-spec-tests.el
  against the oracle.  Supporting changes:
  - Call-frame perf: the empty-closure and advice-transparent branches
    of `call_function_value' (core.rs) now run the body directly on the
    caller's env chain instead of cloning the whole chain per call
    (deep call stacks were quadratic).  (A `raw_function_binding'
    marker-first skip was tried and reverted — it broke `named-let',
    which relies on a letrec value binding resolving in the function
    position; see the continuation doc.)
  - `forward-list' signals `scan-error' at buffer end (GNU's C
    `Fforward_list' behavior) instead of returning nil like the
    `scan-lists' Lisp wrapper; `erc-d-u--read-dialog' reads dialog
    hunks with `forward-list' and catches the end-of-buffer scan-error.
  - `version<' / `version<=' / `version=' added to simple_compat.el
    (erc-d proxy scenarios call `version<').
- Regression-fix follow-up to the 2880 batch (same frontier), found by
  a clean 139-file sweep after the first sweep was invalidated by a
  mid-sweep oracle.lock revert and a leaked CPU-hogging emaxx child:
  - `buffer-local-variables' now honors its BUFFER argument (it always
    reported the current buffer's locals; erc-open's second `erc-open'
    for an existing target buffer restored FOONET's markers into
    "#chan", tripping `(cl-assert (= (field-end erc-insert-marker)
    erc-input-marker))' — znc/severed now passes).
  - `cancel-timer' no longer falls back to function-only matching when
    no timer matches function+args: GNU cancel-timer on an already
    fired timer is a no-op, and the fallback was cancelling the OTHER
    network's pending `erc-server-send-queue' drain timer.
  - Native forms that model GNU `let'-expanding macros now bind
    special names dynamically instead of pushing lexical env frames
    (invisible to callees since the special-reference floor):
    `with-output-to-string' (`standard-output'),
    `ert-with-temp-directory'/`ert-with-temp-file' (custom-tests,
    eieio persistence via `eieio-object-write-to-string'),
    `dolist'/`dotimes' loop variables, and the native `newline''s
    `last-command-event' rebinding (electric-tests layout/reindent).
  - `macroexp--dynamic-variable-p' is now faithful (was a nil stub):
    checks `lexical-binding', special/soft/dlet names, and
    `macroexp--dynvars'; locally-special declarations (non-top-level
    one-arg `defvar') are pushed onto `macroexp--dynvars' like GNU's
    load-time expansion records them, and `local_special_active' is
    floor-scoped rather than activation-stamped so closures created in
    the declaring scope keep the declaration (cl-macs tail-call
    elimination must NOT treat a tail call under a dynamic binding as
    eliminable — cl-macs--labels).
  - One-arg `(eval FORM)' no longer installs a global lambda-capture
    override while evaluating: it leaked into lambdas created inside
    called library functions (seq-reduce, cl-equalp internals broke in
    shortdoc examples).  The empty environment + fresh activation
    already provide GNU's nil-lexenv semantics.
- Verified through selector 2880/7080: erc-scenarios-base-statusmsg
  (2880) passes — the first live erc-d network scenario end-to-end
  (real TCP client/server handshake, status-prefixed messages, /me
  round trip); 139-file prefix sweep on frozen binaries is the gate.
  Key semantics (full detail in the continuation doc):
  - GNU scoping for `defvar': a one-arg `defvar' NOT at top level no
    longer sets the global special flag (`special-variable-p' stays
    nil, matching the oracle); it records an activation-scoped local
    marker so `let's in the SAME scope bind dynamically while other
    functions' same-named arguments and lets stay lexical
    (erc-send-input's obsolete dynamic `str' interface).
  - GNU reference resolution for special variables (bug#47552): when
    a function body runs on the caller's env chain, references and
    `setq's of a GLOBALLY special name no longer resolve through a
    caller's same-named lexical argument frame — they read/write the
    dynamic binding, exactly like the oracle (a callee's `str' read
    could previously see erc-send-action's argument).
  - Function arguments always bind lexically (oracle-confirmed, even
    for special names); internal frames that must be dynamic
    (delay-mode-hooks, with-silent-modifications, overlay
    modification hooks) now use real dynamic bindings.  A top-level
    one-arg `defvar' is "soft special": dynamic `let's and dynamic
    references without the `special-variable-p' flag, exposed via
    `macroexp--dynvars' so cl-macs aliases same-named arguments
    (bug#47552).  `dlet' is a real special form whose bindings and
    names are dynamic for the body's duration (calendar/diary sexp
    machinery), and the native cl-defun &key lowering binds its
    arguments through an always-lexical internal let*.
  - The reader ends symbols at unescaped `,'/`''/`` ` `` like GNU
    read0, so erc-backend's 352 handler pattern `,flags, hop-real'
    reads as two unquotes instead of a symbol named "flags,".
  - `kill-buffer' only asks "Buffer modified; kill anyway?" for
    file-VISITING buffers (erc-d's .eld dialog buffers die silently
    under `inhibit-interaction' like GNU).
  - run-at-time/run-with-timer honor their delay (due instants;
    repeating timers reschedule; cancel-timer matches function+args;
    run-at-time returns the 10-slot timer vector), and the batch
    waits (sleep-for/sit-for/accept-process-output) pump process +
    network output to quiescence and fire due timers throughout the
    wait, so an in-process IRC handshake completes at full speed.
  - Interpreter hot paths are indexed: O(1) function/global/alias/
    special lookups, fast negative macro checks, cycle detection in
    list traversal deferred past 64 nodes, and `quote' returns
    marker-free templates as-is (GNU structure sharing) with a
    per-template verdict cache.  erc message processing dropped from
    ~250ms to low-ms per line, fitting erc-d dialog timeouts.
- NEXT: erc-scenarios-internal (2882..2894) — 3 remaining check-all
  failures (down from 6; the manifest selectors are erc-d unit tests,
  but check-all also compares the :expensive erc-d-run-* live tests).
  FIXED this batch: erc-d-run-unexpected-depleted (forward-list
  scan-error at EOF), erc-d-run-proxy-direct-subprocess{,-lib}
  (locate-library now honors its PATH arg), erc-d-t-with-cleanup
  (start-process/make-process now name the process from the NAME arg,
  not the program — the test reads `(process-name echo)').  STILL
  FAILING, a deep timer-coordination cluster: erc-d-run-linger-direct
  (:unstable; oracle passes deterministically in ~1.03s), no-block,
  nonstandard-messages.  The cancel-timer args-by-identity fix (was
  matching sibling dialog records by structural `equal', cross-
  cancelling the wrong linger timer) helped but did not close them —
  the residual is that emaxx's eager-pump timer model doesn't
  reproduce real Emacs's coordination of two near-simultaneous
  `erc-d--expire' timers with intervening `finalize-dialog' /
  `delete-process' (only one dialog's teardown-this-dialog-at-least
  reaches `erc-d--teardown', so the server never dies).  This needs
  the pump to fire due timers and interleave process events the way
  the C event loop does; it is NOT a one-liner.  Then match
  2895..2896, misc-commands 2897, stamp 2898..2900, erc-services
  (2901..2917, plstore cluster), erc-stamp (2918..2929), erc-tests
  (2930..3023).
- Verified through selector 2879/7080: erc-networks (2812..2854,
  43/43), erc-nicks (2855..2870, 16/16), erc-sasl (2871..2879, 9/9
  selected; the unstable ecdsa placeholder now SKIPS like GNU);
  139-file prefix sweep (prefix-files19.txt) on frozen binaries is
  the gate.  Key semantics (full detail in the continuation doc):
  - `with-current-buffer' saves the current buffer BEFORE evaluating
    the buffer form (macro expands to save-current-buffer +
    set-buffer), so a form that switches buffers no longer leaks;
    same for with-current-buffer-window.
  - GNU buffer-list recency: set-buffer never reorders; record_buffer
    (switch-to-buffer / pop-to-buffer variants / select-window, each
    honoring NORECORD) moves to front; bury-buffer to the end.
  - cl-generic &context methods that differ only in the context
    expression are distinct methods (the expression fingerprints the
    identity key), and stored methods carry the context expr so
    another method's (not <cond>) guard re-evaluates the context test
    (erc-networks--id-create's erc-rename-buffers/erc-reuse-buffers
    compat methods).
  - `should' returns the value of FORM; ert-skip's ert-test-skipped
    signal maps to a Skipped result.
  - save-restriction on a wide buffer just re-widens on exit (GNU
    save-restriction-save), instead of marker-tracking old bounds
    that insert-before-markers at BEGV would push (this silently
    re-narrowed erc-networks--transplant-buffer-content's insert).
  - with-silent-modifications binds inhibit-read-only and
    inhibit-modification-hooks; delete-process accepts nil/buffer/
    name designators; custom-set-variables sets already-defined
    options immediately (NOW only forces undefined ones).
  - GNU --batch color model: verbatim term/tty-colors.el port
    (color-name-rgb-alist + 8-color tty approximation) with faces.el
    color-values/readable-foreground-color/color-dark-p;
    frame-parameter returns unspecified-bg/-fg/background-mode dark.
  - faces UI: face-spec-set (native, over the defface property
    model), list-faces-display + describe-face flows (help-make-xrefs
    + point-min in the with-help-window shim; [back] via help-xref
    stacks; set-window-point on the selected window moves point);
    custom-declare-face + custom.el keyword handlers ported verbatim;
    text-quoting-style honors the variable.
  - hex-util + rfc2104 are compat-preloaded native primitives
    (decode/encode-hex-string, rfc2104-hash HMAC), turning erc-sasl's
    4096-iteration PBKDF2 from a ~25-minute wall into milliseconds.
  - read-string/read-from-minibuffer consume unread-command-events up
    to RET (ert-simulate-keys); read-passwd works: minibuffer-with-
    setup-hook runs the hook inside the minibuffer buffer with
    active-minibuffer-window non-nil, define-minor-mode maintains
    GNU's `local-minor-modes', and `read-hide-char' is defined.
- NEXT: selector 3001, `erc-hide-prompt', in the 94-selector
  `test/lisp/erc/erc-tests.el' block (2930..3023).  Selectors 2930..3000
  and all preceding ERC scenario, services, and stamp blocks are verified.
  The file-wide default comparison has only three failures left.
- Verified through selector 2811/7080: erc-button (2766..2770),
  erc-dcc (2771..2780), erc-fill (2781), erc-goodies (2782..2796),
  erc-join (2797..2806), erc-match (2807..2811) all pass; 136-file
  prefix sweep (prefix-files18.txt) on frozen binaries is the gate
  (erc-join and erc-match passed without further code changes).  Key semantics (full detail in the continuation doc):
  - condition-case `t' handler; signal-time handler-bind dispatch walks
    a unified handler stack so an inner matching condition-case
    suppresses outer handler-bind functions (ert's should-error).
  - GNU field motion: `pos-bol'/`pos-eol' ignore fields;
    `line-beginning-position'/`line-end-position' constrain with
    ONLY-IN-LINE (and ESCAPE-FROM-EDGE after line motion); field-
    beginning/field-end take ESCAPE-FROM-EDGE + LIMIT; constrain-to-
    field implements GNU's near-field gate and other-side check.
  - indent-rigidly edits leading whitespace in place (keeps props).
  - format-time-string %a %A %b %B %c %C %D %e %I %j %l %p %P %r %s
    %u %w %x %X %y and friends; current-time-zone accepts ZONE.
  - visual-line vertical-motion (batch wraps at frame-width-1, ignores
    word-wrap and cons goal columns, like GNU's vmotion); beginning/
    end-of-visual-line + kill-visual-line + posn-at-point ports.
  - GNU kill ring (kill-new/append/current-kill/kill-region/yank/
    yank-pop + subr.el yank helpers); C-y/M-y/C-w/M-w default bindings;
    kbd-macro dispatch applies command remapping; raw "\C-c\C-j" key
    strings no longer misparse the newline as a separator.
  - capf-driven completion-at-point (try/test/all-completion + exit
    functions + *Completions* or "Next char not unique"); minibuffer.el
    quoted completion tables ported verbatim.
  - local hooks mirror into a buffer-local "(fns... t)" value (member/
    local-variable-p see them); remove-hook LOCAL is arg 3 and kills
    the local when empty; global add-hook writes the default when a
    mirror exists; define-minor-mode runs MODE-hook on every toggle.
  - buffer-local-value falls back to the default value, never another
    buffer's local; (with-current-buffer BUF) returns the buffer.
  - native ert runner wraps each test in a temp buffer and binds
    ert--running-tests; timers are GNU 10-slot vectors with a working
    timer-event-handler; print-circle labels resolve inside propertized
    string reads; equal-including-properties compares positions.
- NEXT: erc-networks-tests.el (2812..2854): 19/43 pass. Then
  erc-nicks (13/16), erc-sasl (crashes the runner — investigate
  first). Milestone 3000 sits inside the erc series.
- Verified through selector 2765/7080: viper (2747..2751, 5/5), env
  (2752..2754), epg-config (2755..2758), epg (2759..2765, 7/7) all
  pass; 130-file prefix sweep (prefix-files15.txt) is the gate.
  Highlights (full detail in the continuation doc): emulation-mode-map-
  alists in the active keymaps; GNU undo-list model (Insert = (BEG .
  END), primitive-undo/undo-more ports, buffer-undo-list setq rebuilds
  the native list, per-command boundaries in the kbd macro loop);
  search COUNT; process pipe tail-draining + sleep-for pumping +
  raw-byte process-send-string encoding (gpg); shell-command output
  capture.
- NEXT: erc series (2766+; erc-button needs `emacs-build-time` at
  load). Milestone 3000 sits inside the erc series.
- Verified through selector 2746/7080: testcover (2670..2700, 31/31),
  text-property-search (2701..2720), thunk (2721..2729), timer
  (2730..2734), track-changes (2735), unsafep (2736..2740), vtable
  (2741..2742), warnings (2743..2746) all pass; 126-file prefix sweep
  (prefix-files14.txt) on frozen binaries is the gate.  Key semantics
  (full detail in the continuation doc):
  - Unnamed `:type list` cl-defstructs are plain lists (GNU), with
    accessor reads/writes and setf on the list cells.
  - `equal` on closures compares body-referenced captured bindings
    (nadvice equality preserved, testcover 1value divergence detected).
  - `function-get` follows defalias chains; `not` aliases `null`.
  - GNU time_arith tick/hz arithmetic; flooring time-convert; sit-for
    NODISP; timer-next-integral-multiple-of-time port.
  - insert-file-contents fires change hooks (REPLACE included); the
    supersession check respects a let-bound nil `buffer-file-name`
    (auto-revert tail handler).
  - local-variable-p → t for always-buffer-local DEFVAR_PER_BUFFER vars.
  - recent-keys/display-color-p/frame-parameters batch stubs; a dozen
    preloaded simple.el/subr.el/mule-cmds.el defuns+defvars in
    simple_compat.el (see continuation doc list).
- NEXT: `viper-tests.el` (2747..2751): 1/5 passes (viper-test-fix);
  viper-test-undo-1..4 exercise vi undo grouping via execute-kbd-macro.
- Verified through selector 2669/7080: `shortdoc-tests.el` (2613..2617,
  5/5), `subr-x-tests.el` (2618..2664, 47/47), `syntax-tests.el` (2665),
  `tabulated-list-tests.el` (2666..2669, 4/4) all pass the harness.
  The 118-file prefix sweep (prefix-files13.txt) on frozen binaries is
  the gate; autorevert-tests is a known flake, retry it standalone.
  Batch summary (see the continuation doc for the full list):
  - Native define-short-documentation-group gated behind
    !has_macro_binding → real shortdoc.el owns `shortdoc--groups`.
  - `documentation` falls back to etc/DOC (C primitives) and then to a
    lazy docstring scan of the version's lisp/ sources
    (natively-implemented elisp functions).  Thread-local caches in
    dispatch/misc.rs.
  - `help-function-arglist` resolves macro-table macros to their
    arglist instead of returning `t` (shortdoc dolists over it).
  - rx-let-eval autoload; native ucs-normalize-NFC/NFD-string
    (unicode-normalization crate); buffer-text-pixel-size.
  - ~35 shortdoc group functions ported verbatim from GNU
    subr.el/files.el/simple.el into simple_compat.el, plus honest
    degraded stubs for OS features (ACL/SELinux/xattr nil,
    add-name-to-file signals file-error, vc-responsible-backend nil).
- NEXT: `testcover-tests.el` (2670..2700) is the next wall
  (text-property-search 2701..2720 and thunk 2721..2729 already pass
  behind it; timer-tests 2730..2734 also fails).
- Verified through selector 2612/7080: `rx-tests.el` (2524..2559,
  36/36), `seq-tests.el` (2560..2611, 52/52), `shadow-tests.el`
  (2612) all pass the harness.  The 117-file prefix sweep
  (prefix-files12.txt) on frozen binaries is the gate; autorevert-tests
  is a known flake, retry it standalone.  This batch completed the rx
  groundwork below and added:
  - Reader `\xNNNN` string hex-escape maps the #x3FFF00..#x3FFFFF
    raw-byte range to emaxx's internal raw-byte char (0xE000+byte),
    so rx's constructed regexp strings round-trip as the oracle's
    unibyte bytes (rx-char-any-raw-byte, rx-charset-or).
  - `macroexpand-all` evaluates `eval-and-compile` bodies at expansion
    time and keeps the forms (rx-define's `put 'rx-definition` side
    effect is visible to a later `rx` in the same rx-let — rx-let-define).
  - `char-to-string`/`string`/`unibyte-char-to-multibyte` map the
    #x3FFF00 range to 0xE000+byte; `find-composition-internal` added
    via the unicode-segmentation crate.
  - subr-x is the real GNU file now (removed from
    is_compat_preloaded_feature); `mapconcat` treats a nil separator as
    "" and `string-join` delegates through it; `let`/`let*` signal
    setting-constant when binding nil/t/keywords (and-let*).
  - utf-16/-le/-be coding systems (utf-16 = big-endian, BOM FE FF);
    `dir-locals-file` builtin var (shadow-tests).
  - seq-tests passed with no code change (already supported).
- NEXT WALL: `shortdoc-tests.el` (2613..2617).  Currently 3/5 pass
  VACUOUSLY because emaxx routes `define-short-documentation-group`
  to sf_defgroup (custom defgroup), leaving `shortdoc--groups` empty.
  To reach 5/5: guard the native form behind
  `!has_macro_binding("define-short-documentation-group")` so the real
  shortdoc.el macro populates the groups; make `documentation` fall
  back to the version's etc/DOC file (compat_data_directory()/DOC,
  name-keyed `\x1fF<name>\n<doc>` entries) for C builtins that carry
  no native docstring; add `make-separator-line`; and define the ~36
  group functions that are not yet fboundp (shortdoc-all-functions-fboundp
  checks every listed function).  Non-fboundp functions are SKIPPED in
  display, so the 17 with `:eval` examples must eval without error once
  defined (values are NOT checked), while the 21 `:no-eval` ones only
  need fboundp.  `buffer-text-pixel-size` is needed by the string
  group's string-pixel-width `:eval` example.
- rx groundwork detail (folded into the 2612 batch above):
  - `ensure_gnu_rx_loaded' (like ensure_gnu_pcase_loaded): the first
    rx/rx-let/rx-define/rx-let-eval form loads GNU rx.el when the
    load-path resolves it; native sf_rx* stay the no-file fallback,
    gated by has_macro_binding("rx").  rx-to-string delegates to the
    loaded elisp (its native override is dropped).
  - `macroexpand-all' now binds `macroexpand-all-environment' for
    environments carrying `:rx-locals' (not just cl-flet `function'
    expanders) so rx-let's nested rx forms read their local defs.
  - `define-obsolete-function-alias' ACTUALLY installs the alias now
    (evals a (defalias 'OLD 'NEW DOC) form + make-obsolete); it was a
    nil no-op (rx-submatch-n and many other GNU aliases were void).
  - `regexp-opt' loads GNU regexp-opt.el on demand and delegates (the
    trie/common-prefix optimization); the native plain-alternation
    output is the no-file fallback.
  - `char-to-string'/`string' accept the raw-byte codepoint range
    (#x3FFF00..#x3FFFFF), mapping to the internal private-use marker;
    `unibyte-char-to-multibyte' maps bytes 0x80..0xFF to those raw-byte
    codepoints (GNU eight-bit chars).
  - REMAINING 3 (all deep raw-byte string-model issues, for the next
    agent): rx-char-any-raw-byte and rx-charset-or need emaxx's
    internal raw-byte char (0xE000+byte) to round-trip as the oracle's
    unibyte byte in constructed/compared regexp strings; rx-let-define
    is an rx-let/rx-define shadowing-precedence case.
- Tests through 2523/7080 are verified: `pp-tests.el` (2488..2491),
  `range-tests.el` (2492), `regexp-opt-tests.el` (2493..2494),
  `ring-tests.el` (2495..2518, free) and `rmc-tests.el` (2519..2523).
  The batch:
  - `prin1' escapes only `"' and `\' by default; newlines/tabs/control
    chars print RAW unless `print-escape-newlines' is non-nil (Rust's
    {:?} always-escapes was wrong — pp's docstring roundtrip and the
    code-formats erts depend on raw newlines).
  - `looking-back' prefers the latest-starting NON-EMPTY match ending
    at point (a zero-length match only counts when nothing else does),
    and its match data is based at the haystack origin, not the match
    start (pp-fill's "#[sf]?" unbreakable probe).
  - `insert-buffer-substring' treats nil START/END as the accessible
    bounds (pp-emacs-lisp-code copies its temp buffer via
    insert-into-buffer).
  - `regexp-quote' is GNU-exact: only [ * . \ ? + ^ $ get a
    backslash; ( ) { } | ] stay literal (the native `regexp-opt'
    override is dropped, so the real elisp file runs).
  - `lambda' carries `doc-string-elt' = 2 (GNU function-put; pp's code
    formatter keeps pre-docstring elements on the first line) — merged
    into lambda's existing edebug-form-spec entry, not a second entry
    that the wholesale per-symbol replace would shadow.
  - simple_compat ports: tabify.el `untabify', subr.el
    `use-dialog-box-p' (+ from--tty-menu-p / use-dialog-box-override
    defvars).  Native `window-frame' (single frame) and
    `display-supports-face-attributes-p' (nil: batch/TTY has no
    face-attribute display; rmc.el underlines shortcut keys only on
    graphical terminals).
- Tests through 2487/7080 are verified: `pcase-tests.el` (2475..2487,
  all 13 selectors).  The batch hands the pcase family to GNU pcase.el:
  - `ensure_gnu_pcase_loaded': the first evaluation or macroexpansion of
    a pcase-family form loads GNU pcase.el when the load-path can
    resolve it; the native special forms remain the no-file fallback
    (unit tests).  After the load, the verbatim cl-macs.el integration
    is installed (the `cl-type' pcase pattern and the
    `cl--pcase-mutually-exclusive-p' advice; its struct-predicate
    branches are fboundp-guarded because cl--struct-class-p has no
    native counterpart).
  - The READER now always encodes quote shorthands with the raw
    `\``/`\,'/`\,@' symbols (GNU behavior; pcase.el registers its
    backquote pattern expander under `\`').  Print still renders both
    spellings identically, and the evaluator accepts both names.
  - `eval_backquote_with_depth' preserves the original head symbols
    when rebuilding nested backquote/unquote forms (patterns passing
    through templates keep the reader's raw symbols).
  - Macro calls signal `wrong-number-of-arguments' when required
    parameters are missing (GNU; `(pcase-setq a)' must error).
  - byte-opt.el's `side-effect-free'/`pure' function property tables
    are installed verbatim from simple_compat.el (GNU gets them when
    byte-opt loads, which real sessions do early; pcase--split-pred
    folds predicate calls over quoted values with them and prunes
    shadowed branches — the byte-opt file itself must NOT load, it
    would drag bytecomp over the native compiler).  replace.el's
    `how-many'/`count-matches' and isearch.el's `search-upper-case'
    are ported verbatim for the quote-optimization test.
  - Native `cl-typep' handles GNU range types ((integer LOW HIGH),
    float/number/real, `*' unbounded, (N) exclusive) and signals
    "Unknown type %S" for type names it cannot resolve to a class,
    deftype, satisfies-predicate, oclosure type (root types register
    `emaxx-oclosure-slots'; kmacro.el's `(cl-typep x 'kmacro)' must
    stay nil before matching) or struct type (`emaxx-struct-slots').
  - Sweep regressions fixed in-batch: the cl-macs integration must NOT
    use `advice-add' (it autoloads nadvice.el and the blob can run in
    the middle of nadvice's own load, re-entering it and clobbering
    its cl-print-object method — plain wrapper redefinition of
    `pcase--mutually-exclusive-p' instead); `cl-struct-sequence-type'
    and the `cl-struct' pcase pattern are defined over emaxx struct
    metadata (`emaxx-struct-sequence-type' property, list/vector/nil)
    because the cl-loaddefs autoload stubs would drag cl-macs.el over
    the native cl machinery mid-indent (lisp-mode's indent code
    pcase-matches the lisp-indent-state struct); subr.el
    `insert-into-buffer' is ported (pp-emacs-lisp-code).
- Tests through 2474/7080 are verified: `package-tests.el`
  (2438..2474, all 37 selected — and the harness check-all scope also
  matches the 38th, `package-test-update-archives-async`).  The batch
  is broad cross-cutting GNU semantics:
  - READER string/character modified escapes: modifiers chain ONLY
    through another backslash escape (`"\C-\M-a"`); a bare `C-'/`M-'/
    `^' after the first modifier is the TARGET character, so `"\C-^"`
    reads as control-^ (char 30) instead of desyncing the whole load
    stream (outline.el's docstring made every following form
    misparse).  Control folds GNU's exact set (`@'..`_' and a-z fold
    to 0..31, `?' becomes DEL; anything else keeps the control
    modifier bit, which only `\C-SPC' -> NUL survives inside a
    string; elsewhere it signals "Invalid modifier in string").
    Character literals keep modifier bits (`?\C-\C-a' =
    67108865, `?\C-1' = 67108913).
  - `replace-regexp-in-string': an empty match past the scan position
    (anchors like `$'/`\''`) resumes AFTER the match; previously the
    scan reset behind it and duplicated the tail with a one-char slide
    (lm-commentary output).
  - `search-forward'/`search-backward' honor `case-fold-search'
    (per-character folding keeps char counts aligned).
  - cl-defstruct explicit constructors: in emaxx-struct-make the
    `&rest' variable captures the remaining args WITHOUT consuming
    them positionally (a following `&key' reads the same tail; a rest
    variable naming a slot gets the tail list).  Constructors with
    `&aux' now pass slots as PURE KEYWORDS computed from the
    parameter/aux let* bindings whose names match slot names (GNU
    fills slots only from those bindings) — raw call args can no
    longer leak into slot positions (`package-desc-from-define' with
    requirements produced name=nil records).
  - `let-alist' binds each `.key' to the exact cdr (a single-element
    list cdr stays a list; the unwrap hack broke
    package-menu--partition-transaction).
  - `truncate-string-to-width': a non-string non-nil ELLIPSIS means
    the `truncate-string-ellipsis' default ("…"), GNU-style
    (tabulated-list columns pass t; the verilog-mode version row
    aborted the whole package menu print, leaving point at
    point-max).
  - Native `special-mode' (and generated derived modes without a
    parent) call `kill-all-local-variables' first, running
    change-major-mode-hook: re-entering `tar-mode' unswaps its data
    buffer (GNU's `(delay-mode-hooks (,(or PARENT
    'kill-all-local-variables)) ...)' expansion).
  - The GNU lisp-data-mode-syntax-table is a real shared char-table
    (id 3) exposed through `emacs-lisp-mode-syntax-table'/
    `lisp-mode-syntax-table'/`lisp-data-mode-syntax-table', and native
    emacs-lisp-mode parents its per-buffer table to it.
    `copy-syntax-table' callers (ietf-drums.el) now see `.' as a
    symbol constituent, so `mail-header-parse-addresses-lax' keeps
    "J. R. Hacker" dots (package desc :authors extras).
  - `default-directory' is special (DEFVAR_PER_BUFFER): `let' goes
    through the special-binding machinery recording the binding
    buffer; a `setq' from ANOTHER buffer creates that buffer's own
    local instead of mutating the binding, and reads prefer the
    current buffer's local over a foreign global let (the leak sent
    package-install-file's second install into the previous package's
    directory).
  - load: the load-path resolver tries `NAME.elc' when `NAME.el' is
    missing (gzipped sources with compiled artifacts), keeping the
    empty-.el-stub-prefers-elc rule; nested loads' `load-history'
    entries survive the outer load's completion (the entry list is
    re-read instead of consing onto a stale snapshot).
  - Coding: `file-coding-system-alist' carries the GNU default table
    (simple_compat `setq' — the builtin default is nil);
    `find-operation-coding-system' returns `(REGEXP DECODING .
    ENCODING)' pairs verbatim; `decode-coding-region'/`decode-coding-
    string' DETECT the EOL convention for codings with an unspecified
    eol type (everything except no-conversion/binary), so
    `package-install-from-buffer''s bug#48137 decode path works for
    literally-read dos/mac buffers.
  - `call-process': an unreadable INFILE signals `file-error' (GNU
    report_file_error), which epg's `(condition-case nil (call-process
    "tty" "/dev/fd/0" t) (file-error))' probe catches in batch.
  - Processes: `process-send-eof' (closes stdin, drains until exit,
    delivers to filters/buffers); `accept-process-output' takes
    SECONDS from the second argument (nil PROCESS is not a wait),
    pumps live external process pipes into process buffers, delivers
    completed url retrievals, and returns nil on timeout.
  - Native async HTTP: `url-retrieve' spawns a worker thread
    (http_fetch_raw, HTTP/1.0 GET) and the wait loops deliver the raw
    response into the ` *http URL*' buffer and run the callback there;
    non-2xx responses set `(:error (error http CODE))' in the status
    plist (a 404 .sig download must NOT be treated as a signature).
    `url-retrieve-synchronously', `url-http-file-exists-p' and
    `url-insert' (raw header/body split; url-handlers' mm-dissect
    version is builtin-overridden) are native; features `url' and
    `url-http' are builtin-provided so the GNU files cannot shadow
    them with make-network-process transports; simple_compat defines
    the `url-http'/`url-http-expand-file-name' surface url-methods.el
    introspects, and the native `url-scheme-get-property' fallback
    table serves expand-file-name/file-exists-p for http(s) while
    delegating to a loaded elisp definition.
  - simple_compat ports (verbatim): help.el `substitute-quotes',
    warnings.el `lwarn', lisp-mode.el `lisp-outline-level' (+
    lisp-mode-autoload-regexp), a minimal outline.el surface
    (outline-regexp/-heading-end-regexp/-level machinery + `(provide
    'outline)' — the real file builds menus by walking keymaps as raw
    lists, impossible with record-backed keymaps), and
    `with-help-window' now mirrors GNU `help--window-setup' (help-mode
    + buffer-read-only t + body under inhibit-read-only with
    standard-output bound: describe-package writes into the
    test's read-only fake help buffer).  `mail-fetch-field' is
    autoloaded from mail-utils.el.  Native emacs-lisp-mode sets the
    GNU lisp-mode-variables `outline-regexp'/`outline-level' locals
    (lm-section-end depends on them).
- Tests through 2437/7080 are verified: `oclosure-tests.el`
  (2433..2437, all 5 selectors) passes.  The batch:
  - 'oclosure is a builtin-provided FEATURE (GNU preloads oclosure.el;
    the native implementation must not be shadowed by loading it).
  - The `:closure-oclosure'/transparent call branch now DEDUPES
    captured frames whose IDENTITY is live in the caller env: the
    caller's frame is current (captures are snapshots), so oclosures
    created in a scope see later mutations of captured variables
    (oclosure-test's `(funcall ocl1)' after `cl-incf i') and the
    refreshed frames are written back to the closure's stored env.
  - Default `(:copier NAME)' generates GNU's KEYWORD copier (only
    provided :slot keys are replaced); explicit-arglist copiers stay
    positional.  Accessors carry docstrings and register setf places
    (emaxx-gv-setter) through `emaxx--oclosure-set-slot', which
    enforces `:mutable' (setting-constant otherwise, walking parent
    types).  `eieio-oref'/`slot-value'/`eieio-oset' read and write
    oclosure slots like GNU's eieio integration.
  - `interactive-form'/commandp/call-interactively order: a body
    (interactive ...) form outranks the `oclosure-interactive-form'
    generic; the generic is the fallback (and commandp consults it).
  - cl-typep: every oclosure matches the abstract root type
    `oclosure'.
  - Duplicate-slot validation at macroexpansion time (oclosure-define
    "Duplicate slot name: X" incl. inherited slots; oclosure-lambda
    "Duplicate slot: X"), and byte-compile signals "Slot X should not
    be mutated" when compiled code setqs a slot not declared
    :mutable (GNU cconv integration), scanning nested lambdas.
- Tests through 2432/7080 are verified: `nadvice-tests.el` (2420..2432,
  all 13 selectors including the two the oracle itself fails as
  expected) passes.  The batch is deep GNU-advice semantics:
  - OClosures became TRANSPARENT plumbing: the old
    `:closure-isolated-current-env' body marker is replaced by an inert
    `:closure-oclosure' identification marker plus an IDENTITY-STAMPED
    slot frame (frame-merge only unifies identity frames with
    themselves, so two advice objects' look-alike car/cdr slot frames
    can no longer alias and self-recurse), and oclosure bodies run on
    the caller's env chain (`:closure-transparent-env' branch) so
    lexical mutations (`setq' in a hook lambda) and dynamic bindings
    made outside an advice chain survive through it.  `advice--copy'
    restamps the copy's slot frame.
  - advice-add/advice-remove/advice-member-p now AUTOLOAD nadvice.el
    (the native registry remains the no-file unit-test fallback; the
    primitive arms delegate to the elisp definitions when loaded).
  - `symbol-function' of a native-table macro MATERIALIZES the
    synthesized (macro . EXPANDER) cell into the function binding so
    nadvice's setcdr-based macro advice mutates the real cell.
  - GNU function-cell semantics: `fset'/`defalias'/`defun' of a plain
    function over a macro name ERASES the macro (macro-table entries
    are shadow-renamed, not removed — cl-macrolet drains index ranges);
    `fmakunbound' does too.
  - `raw_function_binding's env scan no longer lets a plain `let' of a
    variable named `car' hijack the function position of a builtin:
    only cl-flet/cl-labels frames (now marked with
    `--emaxx-function-frame--') may shadow builtins.
  - Lambdas now signal wrong-number-of-arguments on EXCESS args (GNU
    always did; nadvice-tests' :filter-args #'list case depends on it).
  - `called-interactively-p' walks the native backtrace like GNU's
    subr.el + advice--called-interactively-skip: apply/funcall and
    advice objects are skipped, an :around advice's user lambda is
    skipped only when its `apply' dispatched to another ADVICE object
    (the innermost :around wrapping the plain original is GNU's
    documented broken case and now correctly reports nil), and
    `call-interactively' leaves a synthetic funcall-interactively
    frame for the walk to stop at.
  - `special-form-p' returns GNU's fixed C set only (emaxx's broad
    native-form list must not leak; nadvice refuses to advise special
    forms and `call-interactively' is not one).
  - `interactive-form' also reads raw `(lambda ...)' LIST expressions
    (advice.el probes stored advice bodies that way), and
    call-interactively's arg collection consults
    `oclosure-interactive-form' for advice objects (composed specs).
  - cl-prin1 dispatches oclosures through the `cl-print-object'
    generic; simple_compat.el defines the generic + default method
    (cl-print.el is not loaded) and nadvice's method prints
    "#f(advice ...)".
  - `help-function-arglist' returns t for nil/unbound (advice.el feeds
    it nil for autoloaded functions); cl-macs.el's def-edebug-elem-spec
    set (cl-lambda-list etc.) and gv.el's gv-place are registered as
    builtin `edebug-elem-spec' properties so edebug can instrument
    cl-defun-family specs with nadvice loaded (the add-function
    autoload no longer breaks edebug-tests.el).
  - GNU keyboard.c/xdisp.c defvar defaults (pre-redisplay-function,
    function-key-map, key-translation-map, input-decode-map,
    local-function-key-map) so oracle simple.el now LOADS fully.
  - `subrp' is nil for native emaxx builtins that GNU defines in
    PRELOADED LISP (mark-sexp, zap-to-char... — memoized scan of the
    loadup.el file set): find-func's subr-primitive-p check must fall
    through to `symbol-file' for them, which resolves via the same
    preloaded-sources scan (find-func-tests' advised-symbol lookup).
- Tests through 2419/7080 are verified: `memory-report-tests.el`
  (2412..2414) and `multisession-tests.el` (2415..2419) pass.  The
  batch: GNU-shaped `garbage-collect' output (fixed 64-bit layout size
  constants; counts approximate), `cl-typep' recognizes every
  cl-defstruct record as `cl-structure-object' (memory-report's struct
  sizing dispatch), native `sqlite-pragma', and verbatim
  simple_compat ports of `file-size-human-readable' (files.el) and
  `readablep' (subr.el).  It also carries the nadvice groundwork the
  next file (nadvice-tests.el) needs: REAL oclosures
  (`oclosure-define' registers slots/predicate/accessors/copiers,
  `oclosure-lambda' builds a closure whose slot frame is a marker
  frame in its captured env; bodies carry
  `:closure-isolated-current-env' — body_has_marker only inspects the
  FIRST body form, so exactly one marker), GNU nadvice.el/advice.el
  load and run (autoloads for add-function/defadvice families, gv-ref/
  gv-deref), the macro↔function-cell bridge (`symbol-function' of a
  macro synthesizes (macro . EXPANDER), a (macro . FN) function cell
  wins over the native macro table in macroexpansion, nil for unbound
  cells), defun/defalias/defmacro honor `defalias-fset-function'
  (nadvice pending advice + redefinition-preserves-advice), `defalias'
  evaluates a bare-symbol name argument (it is a function in GNU;
  Bug#61179's uninterned symbols), `equal' compares lambdas
  structurally (params+body; emaxx lambdas over-capture so environments
  are ignored), function-position lookup skips oclosure slot frames
  (`(car x)' inside an advice body must not resolve the `car' SLOT),
  `interactive-form' returns nil for unbound symbols, consults
  `oclosure-interactive-form' for oclosures, and cl-typep dispatches
  oclosure types through their parent chain.  nadvice-tests.el itself
  is NOT yet passing (7 of 13 remain: interactive-form composition
  through ad-Advice-* assembled definitions, call-interactively spec
  composition, cl-print of advice objects, called-interactively-p, and
  cross-test contamination in the full-file run).  Regressions the
  groundwork exposed in the verified prefix are fixed alongside:
  `add-function'/`remove-function' stay native (nadvice's gv-letplace
  output breaks edebug instrumentation), eieio--defmethod's
  pass-through-primary gate compares `indirect-function' against
  `(symbol-function 'ignore)' (a BuiltinFunc is never `eq' the SYMBOL
  `ignore'; the old gate only "worked" because the old defalias bug
  left the generic unbound), the native insert-directory free-space
  line and a real `get-free-disk-space' port funcall GNU's
  `byte-count-to-string-function' (default
  `file-size-human-readable-iec', "10 B" not "10"), and `fmakunbound'
  purges ALL stacked function-cell entries so stale definitions cannot
  resurface (cl-generic-tests' fmakunbound-between-tests pattern).
- Tests through 2411/7080 are verified: `map-tests.el` (2350..2411, all
  62 selected) passes.  The batch: quoted `#s(hash-table ...)' literals
  MATERIALIZE into real hash-table records at `quote' time (GNU's
  reader builds them at read time; the marker list leaked into GNU
  map.el as an alist), pcase gained GNU `app' patterns (`_' in a call
  form stands for the object, otherwise it is appended as the last
  argument) and a `pcase-macroexpander' fallback so pcase-defmacro
  extensions like map.el's `(map ...)' work, `cl-typep' no longer
  classifies reader vector/hash markers as `list' (cl-generic must not
  dispatch list methods on vectors), an exhausted generic dispatch
  signals the real `cl-no-applicable-method'/`cl-no-next-method'
  conditions, `should-error' matches :type through the signaled
  condition's `error-conditions' (args-out-of-range IS an `error'),
  `(setf (alist-get K PLACE nil t) nil)' splices the entry out with
  `setcdr' so the list stays `eq' (map-delete's contract), and
  caar/cadr/.../cddddr work as setf places ((setf (cddr last) ...) in
  map.el's plist delete).
- Tests through 2349/7080 are verified: `macroexp-tests.el` (2346..2349,
  all 4 selected) passes.  The batch is in two parts.  Scan rewrite:
  `scan-lists'/`scan-sexps' are now a verbatim port of GNU syntax.c
  scan_lists (depth/min-depth tracking, `last_good' obstacle positions,
  forw_comment/back_comment, math delimiters, string and comment
  fences), `forward-sexp'/`backward-sexp' are GNU's default function
  ((goto-char (or (scan-sexps (point) arg) (buffer-end arg))) plus
  `backward-prefix-chars' and the `forward-sexp-function' funcall), and
  the fallback syntax classes for characters a table leaves unset now
  match GNU's standard table (`&*+-/<=>|_' symbol constituents, `$%'
  word) — the old ad-hoc scanners split symbols like `gv-test-foo' at
  hyphens, which broke pp-fill once the 196-entry lisp-indent table
  landed, and returned nil where GNU returns EOB (unterminated comment
  at depth 0) or scan-error (unterminated string).  macroexp batch:
  `(declare (obsolete NEW WHEN))' on defun/defmacro stores
  `byte-obsolete-info' and macroexpand-all warns through `message' via
  macroexp-warn-and-return (cl-letf-interceptable); `eval-buffer' does
  GNU readevalloop's EAGER top-level macroexpansion (macros in defun
  bodies expand while `current-load-list' still names the buffer's
  file; expansion failures fall back to the unexpanded form; note:
  `load' does NOT eager-expand yet); `provide' PREPENDS its entry to
  `current-load-list' like LOADHIST_ATTACH (the file name must stay
  last for `macroexp-file-name'); `eval-defun' binds `current-load-list'
  to (buffer-file-name); `byte-compile-file' binds
  `byte-compile-current-file' to the source and `current-load-list' to
  (nil) so files compiled from another file's load resolve their own
  name.  Because entries now cons onto the front, recording into
  `load-history' nreverses the list like GNU build_load_history (the
  cl-defmethod recorder prepends too) — cl--generic-method-files and
  edebug's load-history readers expect (FILE . ENTRIES).  Regressions
  the full-prefix sweep caught in the uncommitted 2345 scan work (all
  fixed by the rewrite): lisp-mode-tests indent-sexp-in-string,
  faceup-directory, checkdoc cl-defun scans, semantic-utest-ia load,
  gv-dont-define-expander-other-file (pp-fill symbol splitting).
- Tests through 2345/7080 are verified: `lisp-tests.el` (2309..2345,
  all 37 selected) passes its grouped replay.  The batch ported GNU
  lisp.el (`up-list' with escape-strings/no-syntax-crossing,
  `backward-up-list', `delete-pair', `mark-defun',
  `beginning-of-defun-comments'), paragraphs.el (forward/
  backward-paragraph, `sentence-end'), indent.el
  (move-to-left-margin/current-left-margin) and python triple-quote
  syntax-propertize into simple_compat, with the native blank-line
  forward-paragraph and simple up-list arms gated behind
  has_lisp_function.  Primitive repairs: `scan-lists' supports
  (POS ±1 DEPTH) depth-crossing without moving point; forward/
  backward-sexp follow the scan-sexps contract one sexp per step
  (buffer end → nil per Bug#13994; obstacles signal scan-error with
  GNU positions); `syntax-ppss' MOVES POINT to POS like GNU; symbol
  runs cover word/symbol/quote only (punctuation skips, `$' paired
  delimiters scan to their match); text-mode gets a real syntax table
  (`"' punctuation, Bug#15014) and derived modes install
  MODE-syntax-table; `beginning-of-line'/`end-of-line' honor their
  COUNT argument ((end-of-line 0) = previous line's end);
  `char-category-set' returns a 128-slot bool-vector;
  `constrain-to-field' accepts GNU's 5-arg form; parse-partial-sexp
  honors syntax-table TEXT PROPERTIES and generic string fences
  (nth 3 = t, fence closes at the next fence-classed char), and
  forward-sexp runs syntax-propertize first like GNU's scan
  primitives.
- Tests through 2308/7080 are verified: `lisp-mode-tests.el` (2288..2308,
  all 21 selected) passes its grouped replay.  The batch ported GNU's
  lisp indentation stack end to end: `indent-region` dispatches to the
  buffer-local `indent-region-function` (`lisp-indent-region`), every
  symbol carrying a preloaded `(declare (indent N))` property at oracle
  startup is registered natively (196 entries; `when`, `defun`,
  `with-eval-after-load`...), `indent-according-to-mode` funcalls the
  buffer's `indent-line-function`, the native emacs-lisp-mode sets
  lisp-mode-variables' comment settings (`comment-indent-function',
  `comment-start-skip', `comment-column' 40) and `indent-line-to'/
  line-motion primitives constrain to fields like GNU (read-only prompt
  prefixes are not indentation; Bug#32014).  Deep primitive repairs the
  suite exposed: forward regexp searches converted the char-position
  start to a BYTE offset for `captures_from_pos' — with multibyte text
  before point, `re-search-forward' matched BEFORE point and elisp
  skip-and-retry loops (lisp--match-confusable-symbol-character)
  inflooped; font-lock FACENAME lists `(face FACE PROP VAL ...)' apply
  the extra plist as text properties like GNU; `font-lock-fontify-region'
  joined `prefer_builtin_override' with a native arm (loading prolog.el
  pulled GNU font-lock.el over it, which chokes on the native defaults
  sentinel) delegating compat extras to
  `emaxx--font-lock-fontify-region-extras'; inserting propertized
  strings grafts the plist verbatim (the interval-plist prepend rule is
  for property ADDITION, not fresh text); help-uni-confusables ported
  from help.el; `up-list' supports negative COUNT; newcomment/prolog/
  cl-indent autoload like GNU's preloads.
- Tests through 2287/7080 are verified: `icons-tests.el` (2276..2277),
  `let-alist-tests.el` (2278..2284) and `lisp-mnt-tests.el` (2285..2287)
  pass their grouped replays.  `pcase-let`/`pcase-let*` now destructure
  like GNU — membership tests on literal symbols inside backquote
  patterns (icons.el's `(,parent ,spec _ _)) are dropped, not checked;
  the `let-alist` native form defers to GNU let-alist.el's macro once it
  loads (nested `.sublist.foo` fields, `..outer` escapes, exact
  macroexpansion), keeping the native fallback for file-less runs; and
  `gnutls-available-p` exists returning nil like a GNU build configured
  without GnuTLS (package.el evaluates it at load).
- Tests through 2275/7080 are verified: `hierarchy-tests.el` (2208..2275,
  all 68 selected) passes its grouped replay.  Three honest ports:
  `require 'map` now loads the real GNU map.el whenever it is on the
  load-path (GNU does not preload map.el; its cl-generic definitions like
  `map-put!` and the `map-elt` gv-expander only exist after the real
  library loads), with the native compat subset kept as the fallback;
  `cl-defgeneric` processes `(declare (gv-expander (lambda (do) ...)))`
  like `gv--defun-declaration` (DO plus the generic's lambda list);
  `setf` of `alist-get` mutates a found pair with `setcdr` so the list
  stays `eq` (map-put! detects in-place updates that way) and only
  assigns the place when prepending or removing.  Text properties now
  follow GNU's interval-plist order: `add-text-properties` and
  `put-text-property` replace existing entries in place and cons NEW
  properties onto the front (later additions list first in
  `text-properties-at`); `tabulated-list-mode` is autoloadable like
  GNU's buff-menu preload chain.
- Tests through 2207/7080 are verified: `gv-tests.el` (2200..2207, all 8
  selected) passes its grouped replay.  The fix ports GNU's generalized
  variable protocol honestly: the `gv-define-setter` special-form
  shortcut was REMOVED (it shadowed gv.el's real macro, so loading gv.el
  never registered `gv-expander` properties and `cl-callf cdr (car
  cursor)` in edebug could not expand to `setcar`, breaking the cons
  identity that edebug's `&name` spec matcher asserts on).  Instead
  `resolve_setf_place` gained a last-resort arm consuming the standard
  `gv-expander` property via gv-get's DO protocol: the getter form comes
  from calling the expander with a DO returning its getter argument, and
  the setter is a function of the new value that re-invokes the expander
  and evaluates the produced store form.  `setf` of `plist-get`/`cl-getf`
  prepends missing keys (with optional TESTFN), and `(setf (get S P) V)`
  routes to `put`.
- Tests through 2100/7080 are verified locally against the current canonical
  manifest: `faceup-test-basics.el` (selectors 2085..2099) and
  `faceup-test-files.el` (selector 2100) pass their grouped `check-all`
  replays.
- The full 87-file verified-prefix sweep (now including both faceup
  files) is GREEN on the current tree (2026-07-05).
- Tests through 2199/7080 are verified: `float-sup-tests.el` (2107)
  passed as-is and `generator-tests.el` (2108..2199, all 92) passes its
  grouped replay.  The generator batches taught `macroexpand-all' the
  GNU shapes the CPS transformer consumes: `cl-macrolet' expands its
  body with local macros in effect, `cl-symbol-macrolet' substitutes
  variable references (shadowing-aware), `setf' with symbol places
  becomes `setq', `push'/`pop'/`cl-incf'/`cl-decf'/`prog2' expand like
  the GNU macros, and backquote templates become list/append
  constructor code (with `',x' unquote-inside-quote handled and pcase
  clause PATTERNS kept verbatim while clause bodies expand).  Runtime
  repairs: `(signal SYM NON-LIST)' produces GNU's dotted condition value
  and `condition-case' consults `error-conditions' properties; native
  `append' reuses the last argument verbatim as the tail (GNU:
  (append '(2) "b") => (2 . "b")); simple_compat adds the `inline'
  macro and edebug autoloads.
- Previous milestone: tests through 2106/7080 — `find-func-tests.el` passes its
  grouped replay.  The finishing batch built a simulated-minibuffer
  completion engine (`completing-read' key loop over
  `unread-command-events' with TAB longest-common-prefix, trailing-slash
  and component-wise partial-completion expansion; FUNCTION completion
  tables in the native matcher; `locate-file-completion-table' ported to
  simple_compat.el).
- find-func groundwork (previous batch): simple_compat
  ports of the ppss struct accessors, `goto-line',
  `delete-indentation'/`join-line', `function-called-at-point', and a
  minimal `help-C-file-name' reading the oracle DOC file
  (`internal-doc-file-name' defaults to "DOC"); native `symbol-file'
  falls back to scanning GNU's preloaded lisp sources; and
  `macroexpand-all' of `cl-defstruct'/`define-derived-mode' emits
  GNU-shaped `defalias'/`defvar' stubs so find-func's macro-expanding
  search locates generated symbols.
- The latest completed batch is
  `Compat 2100/7080: pass the faceup test files` —
  sexp scanning and syntax-propertize repairs:
  - `forward-sexp'/`scan-sexps' honor `parse-sexp-ignore-comments'
    (skipping comments before and inside sexps like GNU's
    scan_sexps_forward); native `emacs-lisp-mode' sets it buffer-locally
    like GNU lisp-mode-variables.  With nothing but ignorable text
    between point and the buffer end, `forward-sexp' moves there instead
    of signaling (GNU `(or (scan-sexps ...) (buffer-end arg))'), which
    fixes the `eval-defun'+`forward-sexp' walk over a file with header
    and trailing comments (faceup-directory).
  - `syntax-propertize' is ported from syntax.el (high-water mark in the
    now auto-buffer-local `syntax-propertize--done', property cleanup,
    then `syntax-propertize-function'); the native fontification engine
    runs it before its passes like GNU's syntax-ppss-driven
    fontification, so `syntax-propertize-rules' modes stamp
    `syntax-table' text properties that faceup can compare (faceup-files
    checks face + syntax-table + help-echo properties byte-for-byte).
- The previous batch was `Compat 2084/7080: pass ert-x-tests.el` — the
  ert-x helper set plus the runtime repairs the suite exposed:
  - simple_compat.el ports of the preloaded-ert-x helpers:
    `ert-with-buffer-selected'/`ert-with-test-buffer-selected',
    `ert-call-with-buffer-renamed'/`ert-with-buffer-renamed',
    `ert-buffer-string-reindented', `ert-filter-string',
    `ert-propertized-string', `ert--with-temp-file-generate-suffix',
    `ert--force-message-log-buffer-truncation', plus `messages-buffer',
    `indent-region' (drives the buffer's `indent-line-function'),
    `with-help-window', `fill-region-as-paragraph' (no-op),
    `font-lock-default-function', and a `message-log-max' defvar
    (GNU default 1000, dynamically rebindable).
  - `message' implements message_dolog(): nothing is logged for an empty
    message or `message-log-max' nil, and a fixnum truncates *Messages*
    to that many lines; `ert--test-buffers' registration/kill semantics
    and GNU's `*Test buffer (TEST): NAME*' naming in the native
    `ert-with-test-buffer' (the native runner records the top-level test
    name for `ert-running-test'-style naming).
  - `cl-defstruct' honors unnamed `(:type vector)' (plain-vector storage
    with raw `aref'/`aset' access — GNU ewoc nodes and timers are such
    vectors; `timerp' accepts 10-slot vectors); `handler-bind' accepts a
    LIST of condition names per handler; `ert-info' dynamically binds
    `ert--infos' so failure results carry the infos; `symbol-file' with
    type `ert--test' resolves from the native test registry.
  - The pcase family is shielded from macro shadowing (loading GNU
    pcase.el registers the backquote pattern under `\`' while the native
    reader encodes patterns as `backquote'); `equal-including-properties'
    compares interval plists order-insensitively like intervals_equal;
    `indent-rigidly' leaves a partial first line and empty lines alone;
    `indent-line-to' is a no-op at the target column; native
    `emacs-lisp-mode' installs `lisp-indent-line'; `font-lock-mode' runs
    the buffer's `font-lock-function' (ERT's results buffer redraws its
    ewoc there); `ert-with-temp-directory' rejects `:text';
    `inhibit-modification-hooks' defaults to nil.
- The previous batch was `Compat 2056/7080: pass ert-font-lock-tests.el` —
  a font-lock-defaults-driven fontification engine plus the runtime
  repairs it exposed:
  - `font-lock-ensure' fontifies natively: a lazy installer equips the
    native major modes (emacs-lisp/lisp-interaction, lisp, js, c-family)
    with their GNU `font-lock-defaults', loading the defining library
    (lisp-mode.el, js.el) for its keyword variables; a syntactic pass
    fontifies comments/strings from `syntax-ppss', and a keyword pass
    runs the full MATCHER/HIGHLIGHT shapes (regexp and function matchers,
    subexp highlights with OVERRIDE/LAXMATCH, anchored highlighters,
    FACENAME expressions).
  - The standard font-lock face variables (`font-lock-keyword-face' &c)
    are self-quoting defvars like GNU font-core/font-lock, so FACENAME
    expressions such as lisp-mode's `(let ((type ...)) (cond ...))'
    evaluate.
  - `font-lock-defaults' is automatically buffer-local (font-core.el
    declares it with `defvar-local'); sh-mode's plain `setq' no longer
    leaks a global value that suppressed fontification everywhere else.
  - `font-lock-ensure' and the native major modes (`c-mode', `c++-mode',
    `java-mode', `js-mode', `javascript-mode') prefer their builtins even
    after GNU font-lock.el/cc-mode.el/js.el load and would shadow them
    with redisplay-dependent elisp; `javascript-mode' is a `js-mode'
    defalias like GNU js.el.
  - `ert-pass' terminates a test by throwing GNU's `ert--pass' tag (the
    native runner treats it as success) and `ert-fail' signals
    `ert-test-failed' instead of a generic error; `ert-set-test' registers
    tests built by `ert-font-lock-deftest'/`ert-font-lock-deftest-file'.
  - `\s<'/`\s>' regexp atoms resolve from the current syntax table's
    explicit comment-class entries (GNU's standard table maps no
    character to the comment classes), and `regexp-opt' honors PAREN
    (nil/shy, t/capturing, `words', `symbols', literal string).
- The previous batch was
  `Compat 2016/7080: finish eieio-tests.el` — the remaining six mismatches
  after the groundwork batch:
  - `cl-call-next-method' with no next method now dispatches GNU's
    `cl-no-next-method' hook, and a call that matches no method dispatches
    `cl-no-applicable-method': the dispatch chain keeps the `ignore'
    sentinel at its bottom (later registrations still splice by replacing
    that binding), and a new runtime helper
    (`emaxx--cl-generic-apply-next') applies a real next method or routes
    the sentinel to the hook.  A single-method generic now checks its
    specializers like GNU instead of accepting every argument
    (non-matching calls reach `cl-no-applicable-method').
    `simple_compat.el' defines the two hooks with GNU's default erroring
    methods and lowers obsolete `(defmethod no-next-method ...)'/
    `(defmethod no-applicable-method ...)' onto them with eieio-compat.el's
    argument shuffling (tests 08/09).
  - Re-registering a method with the same qualifiers and specializers now
    REPLACES the stored method body in place (GNU semantics) instead of
    splicing a duplicate wrapper.  The duplicate made the two
    same-condition wrappers point at each other, so any dispatch where
    neither matched looped forever — eieio-test-29 crashed with a stack
    overflow once test-18's `slot-unbound' redefinition was reachable
    (tests 18/29).
  - Obsolete `defgeneric' over an existing non-generic function signals
    like GNU's `cl-generic-ensure-function', following defalias chains
    (old EIEIO's `constructor' aliases `make-instance') and exempting
    `no-next-method'/`no-applicable-method'; `generic-p' is defined
    (test 03).
  - `same-class-p' is native (exact class match), the generated `NAME-p'
    predicate matches the exact class per GNU's
    `eieio-make-class-predicate' while the also-generated
    `NAME--eieio-childp' accepts subclasses (test 23).
  - `eieio--class-children' returns child class NAMES (symbols) like GNU's
    class records; `eieio-build-class-alist' recurses over them (test 36).
  - `eieio-oref-default'/`eieio-oset-default' accept instances (GNU
    resolves them to their class), and a class-allocated `oref-default' of
    an unbound cell returns the `eieio--unbound' marker without signaling
    (eieio-base's singleton constructor compares against it).
  - Two follow-up repairs the 83-file sweep demanded (the new specializer
    checking made previously-unconditional dispatch honest): the
    `(subclass CLASS)' condition resolves autoload-stub classes first
    (GNU's subclass generalizer forces `autoload-do-load' through
    `eieio--full-class-object'; semanticdb-project-database-file only
    exists as an `eieio-defclass-autoload' stub until dispatched on), and
    slot descriptors now merge per GNU's storage model — each class's own
    redeclarations override slots inherited from its OWN ancestors, and a
    subclass copies each parent's already-merged view first-parent-wins
    (the old flat recursion lost semanticdb-project-database's
    `tracking-symbol' initform override inside semanticdb's class-allocated
    storage, breaking five cedet files).
- The batch before it was
  `Compat 1975/7080: return GNU (HIGH LOW USEC PSEC) times from
  file-attributes` — a repair for the eieio-tests groundwork batch: the new
  GNU-faithful typed `oset' check exposed that emaxx's `file-attributes'
  returned bare epoch-second integers for the three time fields where GNU
  returns `(HIGH LOW USEC PSEC)' lists, so srecode's
  `(filedate :type cons)' slot signaled `invalid-slot-type' during template
  table construction (srecode-utest-getset.el, srecode-utest-template.el,
  srecode/document-tests.el regressed).  `file-attributes' (and therefore
  `file-attribute-modification-time'/`-access-time'/`-status-change-time')
  now build GNU 4-list times; emaxx's native time functions already accept
  that format.  Also adds `EMAXX_DEBUG_EIEIO'-gated traces to the two
  `invalid-slot-type' signal sites.
- The batch before it was
  `Compat 1975/7080: ground eieio-tests.el on GNU slot semantics` — the
  eieio-tests.el frontier groundwork (28 failing selectors down to ~12):
  parentless `defclass' classes adopt `eieio-default-superclass' implicitly,
  which activates eieio.el's real `make-instance'/`initialize-instance'/
  `shared-initialize'/`clone'/`slot-missing'/`slot-unbound' generic methods
  for every class; slot declarations merge with GNU's
  `eieio--add-new-slot'/`eieio--slot-override' rules (parents first without
  override, type/protection redeclaration errors, initform kept when the
  redeclaration has none, `:group' union) and are validated at defclass time
  (`invalid-slot-type' for constant initforms that miss the slot :type);
  every class stores a defaults-filled default-object cache; class-allocated
  slots live in shared per-class storage evaluated at defclass time and are
  reachable through class symbols in `slot-value'/`slot-boundp'/
  `slot-makeunbound'; `oset' type-checks against the slot :type
  (`eieio-skip-typecheck' honored) and cl-defstruct `:read-only' slots
  signal `eieio-read-only'; `aset' works on records including retagging the
  type slot (symbol tag clears the class-object tag, class record sets it);
  `slot-missing'/`slot-unbound' dispatch from the native `oref'/`oset'
  paths; accessors from `:accessor'/`:reader'/`:writer' are cl-generic
  methods with `(setf ACC)' methods and a `(subclass CLASS)' class-slot
  reader; native `slot-makeunbound'/`slot-exists-p'/`eieio--class-parents'/
  `eieio--class-children'/`eieio--class-options'/
  `eieio--class-default-object-cache'; `clone' left the prefer-builtin list
  so eieio-base's instance-inheritor/named clone methods dispatch;
  `current-message' is nil in batch like GNU; `cl-typep' knows `hash-table'
  and `function' accepts fbound symbols.
- The batch before that was
  `Compat 1975/7080: port the eieio slot descriptor protocol and GNU object
  print/read semantics for eieio-persistent`.
- An earlier batch was
  `Compat 1965/7080: repair verified-prefix regressions across cl-lib, dired,
  bookmark and friends` — a repair batch (no frontier advance) that fixed the
  verified-prefix files an 81-file sweep found failing: arc-mode, cl-seq,
  cl-macs (from 43 failing selectors to one), cl-lib, cconv, bookmark (fully
  green), dired, bytecomp, char-fold (one selector remains) and
  todo-mode/srecode (order-dependent, pass solo). Major pieces: GNU's cl-loop
  engine, cl-flet/cl-labels/cl-function/cl--transform-lambda, letrec and
  macroexp--fgrep ported verbatim into `src/lisp/simple_compat.el`; the
  standard error-condition hierarchy and GNU `define-error'; cl-loaddefs
  autoload registration on the intercepted `(require 'cl-lib)`; GNU
  `autoload' no-clobber semantics; string-literal evaluation allocating
  fresh mutable strings so `eq' identity works; `(setf (elt LIST i) v)'
  structure mutation; marker-based `with-restriction' exit bounds;
  buffer-local `default-directory'; `map-keymap' parent traversal; `--dired'
  marker cleanup in native `insert-directory'; a `(pred (lambda ...))' pcase
  fix; and `subr-arity' fallbacks. The batch also repaired what those
  changes broke and what the sweep still showed red: native fast paths for
  `cl-position'/`cl-remove'/`cl-substitute'/`cl-replace'/`cl-fill' (the
  interpreted cl-seq engines were far too slow for the 8-million-element
  lists in `cl-seq-test-bug24264'); `macroexpand-all' now walks backquote
  templates natively and expands only the unquoted expressions (the oracle
  backquote macro cannot see the reader's `comma' markers, so expanding
  through it dropped every unquote — this broke `` `(,fn ...) `` inside
  cl-flet/cl-labels bodies); macro-environment expanders run in a fresh
  environment so caller locals cannot shadow their captured bindings
  (cl-labels' own `var' shadowed the expander's `var', rewriting local
  calls to the wrong function); `cl--find-class' as a native alias of
  `cl-find-class' (comp-cstr/comp-tests); a cl-lib dependency preload when
  oracle cl-macs.el loads into an environment without cl-lib's Lisp helpers
  (bare-interpreter unit tests); `insert-char' honoring its INHERIT
  argument (dired-align-file alignment spaces must inherit invisibility for
  bug27899); `(:documentation ...)' in `cl-defgeneric' accepting mutable
  strings; byte-compile scanner support for the generated
  `cl-defsubst'/`cl-defstruct' docstrings, the non-top-level macro-autoload
  warning, and `no-byte-compile: t' deleting a stale target. Follow-on
  repairs surfaced by the post-fix sweeps: native `cl-defstruct' now also
  defines the GNU `(setf ACCESSOR)' functions so gv's
  `(funcall #'(setf ACC) V X)' fallback works (comp-cstr unions);
  `\{,N\}' regexps translate to an explicit zero lower bound and the regex
  delegate gets a larger compiled-size budget (cc-mode's
  `[...]\{,1000\}' symbol matcher in semantic/format-tests).
- A follow-up repair pass fixed most of the remaining mismatches with four
  deep interpreter corrections plus targeted ports:
  - Lexical binding frames now carry hidden identity markers
    (`--emaxx-frame-id--'); closure-environment alignment and
    captured-cell sharing compare frame identity instead of name-shape or
    content, so a caller's same-named `let' can no longer shadow a
    closure's captured variable, and content-identical captures from
    different `let's no longer share one mutable cell (bug#51695's
    interpreted lambda, cl-labels expander corruption, the cedet
    `semantic-scope-cache' slot mix-up).
  - Non-local exits now unwind lexical frames at their boundaries like
    GNU's unbind_to: `catch', `condition-case', and every function-call
    boundary truncate the environment back to entry depth, and the
    binding forms that looped with `?' early-returns (dolist, dotimes,
    pcase-dolist) rebalance on error (this fixed edebug's backtracking
    matcher and `cl-flet/edebug').
  - Macro-environment expanders run in a fresh environment, `eval' opens a
    fresh activation, `map-char-table' iterates the effective mapping of
    the append-only char-table log (char-fold exclusions under
    `char-fold-symmetric'), and kill-region/kill-whole-line record
    `this-command'/`last-command' like GNU simple.el (dabbrev's
    order-dependent minibuffer expansions).
  - The native C tag parser gained a lexical preprocessor
    (`expand_cpp_spp_macros'): `#define' object and function-like macros,
    `##' pasting, hide-set recursion prevention, continuations, and the
    builtin symbol map with the G++/VC++ namespace hacks
    (semantic-utest-c's testsppreplace.c now parses identically to its
    hand-expanded counterpart), and function parameter lists end at the
    first balanced close.
- A third repair pass (2026-07-04) fixed the srecode trio and most of
  autorevert/semantic-ia:
  - File notifications are queued and delivered from the idle pump
    (sleep-for/sit-for/read-event/accept-process-output) like GNU's
    asynchronous notify events, filtered per watch to the watched path or
    directory entries; `kill-local-variable' discards buffer-local hooks
    with the local binding; deleting a watched file invalidates its kqueue
    descriptors (auto-revert-test02-auto-revert-deleted-file and the
    remote selectors 00/01/02/03/07 now match).
  - `ert-deftest' also registers the test as an `ert-test' struct under
    the `ert--test' symbol property (`ert-get-test'/`ert-test-body' work);
    batch runs execute in alphabetical order like GNU's
    `apropos-internal' enumeration (srecode-utest-project depended on it);
    requiring `ert-x' after `tramp' registers the `mock' method.
  - cl-generic: the first `cl-defmethod' registers an implicit generic
    lambda list so sibling methods with different parameter names stop
    shadowing each other's dispatch conditions; `cl-typep' understands
    `(satisfies PRED)'; `#'cl-call-next-method' inside `apply' rewrites to
    the previous-method variable (eieio-named constructors).
  - The c-family `indent-according-to-mode' adjusts leading whitespace
    through the marker-safe edit primitives (srecode template inserters
    keep point markers across indentation); `~Class()' parses with
    `:destructor-flag'; namespace members get buffer-absolute bounds;
    `write-file' runs `after-set-visited-file-name-hook' and renames the
    buffer; `semantic-current-tag-of-class',
    `semantic-find-tag-by-overlay-prev'/`-next' are native over the parsed
    tag tree, `semantic-fetch-tags' returns cached `eq'-stable tags per
    buffer fingerprint, and `semantic-go-to-tag' follows `:filename'
    annotations into other files' buffers.
- A fourth repair pass (2026-07-04) finished the last two files;
  the 81-file verified-prefix sweep is fully passing:
  - `semantic-utest-ia.el': stored cl-defmethod-style `:parent' on
    qualified C++ definitions and enclosing-class parents during
    reference collection disambiguate same-named methods across classes;
    prototype/implementation search results dedup ignoring the
    properties slot; `semantic-current-tag' prefers the parsed tree's
    innermost containing tag (position separates overloads) with the
    line parser as fallback; namespace members get buffer-absolute
    bounds; block comments inside prototype parameter lists are
    stripped; the SPP preprocessor blanks consumed `#define' lines with
    equal-length whitespace so positions stay stable.
  - `autorevert-tests.el': buffers visited (or written) through a
    remote name record `emaxx--visited-remote-prefix';
    `verify-visited-file-modtime'/`buffer-stale--default-function'
    compare remote modification times at Tramp's one-second resolution,
    remote watches model gio monitors (deletion does not invalidate),
    remote dired stale checks read Tramp's file-name cache, and
    `make-temp-file' expands relative prefixes against
    `temporary-file-directory' and keeps the remote prefix in the
    returned name.  `kill-buffer' runs the kill hooks with the dying
    buffer current, and the compat harness captures runner output in
    temporary files so chatty children or surviving Tramp shells cannot
    deadlock the pipe reader.
- No known remaining verified-prefix mismatches (all 82 files pass the
  grouped `check-all' replay; sweep 2026-07-04 after the persist batch).
- Selector 1966 and the other nine persistence selectors in
  `test/lisp/emacs-lisp/eieio-tests/eieio-test-persist.el` PASS the grouped
  `check-all` replay (2026-07-04).  The batch ports the EIEIO slot
  descriptor protocol and the GNU object print/read semantics the persist
  machinery depends on:
  - Native `eieio--class-slots'/`eieio--class-class-slots' build vectors of
    `cl-slot-descriptor' records (name, raw initform quoted per GNU's
    `macroexp-const-p'/`eieio--eval-default-p' rule, type, props alist from
    `:documentation'/`:custom'/`:label'/`:group'/`:printer'/`:protection')
    from the stored slot specs, parents first, with subclass re-declarations
    overriding in place; `eieio--class-initarg-tuples' returns the
    `(:initarg . slot)' alist; `cl--slot-descriptor-name'/`-initform'/
    `-type'/`-props' read those records.  All are native because the
    eieio-core.el defstruct accessors mis-index emaxx's class records.
  - `equal' compares records element-wise like GNU (type and slots,
    recursively, with a seen-pair guard); previously two structurally-equal
    eieio objects compared `nil'.
  - `eieio-object-p' requires the record's type to name a registered class
    (hash tables and plain records are not objects), so
    `eieio-override-prin1' takes its hash-table branch for hash slots.
  - `read'/`read-from-string' materialize `#s(hash-table ...)' literals
    into real hash tables like GNU's reader (`eieio-persistent-read' treats
    the result as data, so eval-time conversion never runs).
  - GNU tags eieio objects with the class OBJECT unless `make-instance'
    downgrades the tag to the class symbol under
    `eieio-backward-compatibility'; emaxx models this with class-object-
    tagged records: every `defclass' now stores a default-object cache (an
    all-unbound instance tagged with the class object) on the class record,
    instances created with `eieio-backward-compatibility' nil are tagged
    (clones inherit the tag), and prin1 renders tagged records with the
    class expanded in place of the type symbol.  The cache inside the class
    then hits the printer's active-set guard and prints as a circular `#N'
    marker, which `read' rejects with `invalid-read-syntax' — exactly GNU's
    bug#29220 behavior, so both `-no-backward-compatibility' selectors fail
    identically to the oracle (expected failures).
- The next observed frontier is `test/lisp/emacs-lisp/eieio-tests/
  eieio-tests.el`: the file loads and most selectors run, but the grouped
  `check-all` replay fails ~31 selectors (slot protection/virtual slots,
  class-allocated slot semantics, `slot-makeunbound', typed slot checking,
  named/singleton objects, `eieio-build-class-alist', ...).
- Selectors 1958..1965 (`eieio-test-methodinvoke.el`) passed in a batch that
  runs the obsolete EIEIO `defmethod'/`defgeneric' API on the native
  cl-generic machinery and repairs the dispatch-chain construction bugs the
  file exposed. `simple_compat.el' replaces eieio-compat's runtime helpers
  (`eieio--defgeneric-init-form', `eieio--defmethod') with lowerings onto
  `cl-defmethod': qualifiers normalize case, `call-next-method'/
  `next-method-p' rename to their cl-generic forms inside primary bodies,
  :static methods register both a `(subclass CLASS)' and a `CLASS' method,
  :before/:after methods get a pass-through primary when none exists, and
  old EIEIO's `constructor' aliases `make-instance'. `defclass' constructors
  now construct through the `make-instance' generic (methods participate;
  the builtin constructs when no method matches; `make-instance' left the
  prefer-builtin override list). `(subclass CLASS)' specializers work end to
  end (`cl-typep', dispatch conditions, ranking). Dispatch-chain repairs:
  a method registered below an existing :around splices in with the
  around's old next chain instead of the current top (the top made the
  chain cyclic: `cl-generic-test-02-struct' eval-depth failure);
  a class-`t' specializer ranks like an unspecialized argument (second-
  argument `eql' dispatch, `cl-generic-test-01-eql'); sibling classes order
  through a common subclass's precedence list; :before/:after wrappers form
  one stack ordered most-specific-outermost with unique per-specializer
  capture names, and primary methods always splice below that stack;
  registrations through an alias use the canonical generic name so later
  splices can reconstruct wrapper symbols; `fmakunbound' clears the stored
  cl-defmethod specializer metadata so a rebuilt generic does not rank
  against the destroyed chain. This also fixed the previously-failing
  grouped replay of `cl-generic-tests.el' (01-eql, 02-struct, 06..12): the
  file now PASSES. Exact replays run for this batch: grouped `check-all'
  for `eieio-tests/eieio-test-methodinvoke.el' (PASS),
  `cl-generic-tests.el' (PASS), `edebug-tests.el' (PASS),
  `cl-print-tests.el' (PASS), `derived-tests.el' (PASS),
  `easy-mmode-tests.el' (PASS); `cl-lib-tests.el' failure set is
  byte-identical to the parent commit (pre-existing environment
  discrepancies); `eieio-test-persist.el' failures identical to parent;
  `eieio-tests.el' improved from load-error to loading with test failures;
  raw whole-file `seq-tests.el' timeout identical to parent. Full
  `cargo test' (1119 lib tests) green.
- Environment note: this container verifies against a locally rebuilt
  GNU Emacs 30.2 oracle (gnu/linux, native-compilation, built from the
  Ubuntu `emacs_30.2+1.orig` source snapshot at `/home/user/emacs`); the
  `compat/oracle.lock.json` repin for that oracle is intentionally left
  uncommitted so the darwin pin in git history stays canonical.
- Selector 1917, `edebug-tests-break-in-lambda-out-of-defining-context`,
  passed after making `(eval-defun t)` run real Edebug instrumentation and
  making Edebug's stop/step machinery work in batch: `beginning-of-defun`/
  `end-of-defun` follow GNU lisp.el semantics instead of jumping to the
  buffer limits, `def-edebug-spec`/`def-edebug-elem-spec`/`defmacro (declare
  (debug ...))` store their spec properties, builtin macros carry the specs
  GNU declares in preloaded Lisp, `cl-letf` binds special variables
  dynamically like `let`, nested advice wrappers use unique capture names so
  an inner wrapper cannot resolve an outer wrapper's original function,
  `eval` enforces `max-lisp-eval-depth` (scaled for this evaluator's
  per-subform recursion) instead of overflowing the Rust stack,
  `load-read-function` exists/defaults to `read` and the preloaded
  `eval-defun` reads through it so `edebug--read` can wrap forms,
  a `simple_compat.el` prelude provides `eval-expression`,
  `eval-expression-print-format`, `prin1-char`, `values--store-value`,
  `event-modifiers`, `event-basic-type`, `eval-sexp-add-defvars`,
  `syntax-ppss-toplevel-pos`, and `with-timeout-suspend`/`-unsuspend`,
  `eventp` accepts integer/symbol/list events, special forms resolve through
  function cells for `indirect-function`/`fboundp`/`macrop`, key lookup
  honors `overriding-local-map`, `minor-mode-overriding-map-alist` and the
  buffer local map in GNU order, keyboard macros maintain
  `executing-kbd-macro`/`executing-kbd-macro-index` on a shared cursor, and
  `recursive-edit`/`exit-recursive-edit`/`abort-recursive-edit`/
  `recursion-depth` consume the innermost executing macro until `exit` is
  thrown, with command hooks demoted through GNU's safe_run_hooks behavior,
  batch window-configuration save/restore (`current-window-configuration`,
  `set-window-configuration`, `window-configuration-p`, `select-window`,
  `window-live-p`, `set-window-hscroll`), `read-kbd-macro`, and `setf`
  support for Edebug's `edebug-after` generalized place. Selectors
  1918..1956 passed in the same batch; grouped `check-all` file replay for
  `test/lisp/emacs-lisp/edebug-tests.el` shows selector 1957 as the only
  remaining mismatch. Exact replays run for this batch: selector
  `edebug-tests-break-in-lambda-out-of-defining-context`, grouped
  `check-all` replays for `edebug-tests.el`, `derived-tests.el`,
  `easy-mmode-tests.el`, `cl-print-tests.el`; focused Rust regressions:
  `cargo test defun_navigation_defaults_bracket_the_current_top_level_form`,
  `cargo test defun_navigation_delegates_to_bound_mode_functions`,
  `cargo test cl_letf_binds_special_variables_dynamically`,
  `cargo test special_forms_resolve_through_function_cells`,
  `cargo test temporary_file_directory_names_a_directory_with_trailing_separator`,
  and `cargo test cl_defmethod_updates_generic_under_around_advice`.
- Selector 1957 (`edebug-tests-writable-buffer-state-is-preserved`) passed as
  part of a batch that also resolved the earlier honesty caveat: the
  `edebug-tests-prepare-macro` `cl-loop` shapes now run natively, the
  interleaved keyboard-macro assertions in `edebug-tests-run-kbd-macro`
  execute for real, and all 35 edebug selectors that had passed vacuously now
  genuinely pass the grouped `check-all` replay of
  `test/lisp/emacs-lisp/edebug-tests.el`. The batch spans: live
  `ert-with-message-capture` via dynamic binding; keyboard-macro-driven
  minibuffer reads (C-a/C-e/C-k/DEL editing, `read`-arg support);
  `digit-argument` prefix accumulation; real `backtrace-eval` over frame
  locals; case-sensitive key lookup; `command-error-function` routing;
  activation-scoped closure-env sharing; `throw` passing through
  `condition-case`/`ignore-errors`; interactive specs through advice
  wrappers; prefix-key detection; `gensym-counter`; vector literals excluded
  from `consp`/`listp`/`atom`/`nlistp`; negative `down-list`;
  `forward-comment` leaving point after skipped whitespace on failure; GNU
  eval frames for special forms and in-progress calls; `eval-buffer`
  recording `load-history` under `buffer-file-name` (including `provide` and
  `cl-defmethod` entries, with buffer-stream reads through a non-`read`
  `load-read-function`); a native `unload-feature` that undefines the
  feature's functions/methods and purges every `load-history` entry for the
  feature's file; `cl--generic-search-method` +
  `find-function-regexp-alist` registration so
  `edebug-instrument-function` can find generic methods; GNU edebug specs
  for `cl-defmethod`, `cl-defgeneric` (with `:method` naming), and
  `cl-macrolet` (with `&interpose`, `cl--generic-edebug-make-name`,
  `cl--generic-edebug-remember-name`, `cl--generic-split-args`, and the
  `cl-macro-list` element specs ported to `simple_compat.el`);
  `cl_defmethod_advice_original_binding` only treating a function that IS an
  advice wrapper as advice (a dispatch wrapper whose closure captured an
  unrelated advice activation previously misrouted re-registration so
  freshly instrumented methods never reached the top of the dispatch
  chain); `:closure-transparent-env` dispatch wrappers and method lambdas
  for methods defined with no surrounding lexical bindings, so lexical
  mutations inside top-level method bodies (edebug's spec-matching methods)
  reach the calling scope; eager `cl-macrolet` macroexpansion inside
  edebug-instrumented `defun` bodies (matching GNU's eager top-level
  macroexpansion, Bug#29919); and `%s`/`princ` printing buffers as their
  names. Oracle-verified unit-test corrections: plain evaluation of
  `cl-defmethod`/`cl-defgeneric` forms does not call
  `edebug-new-definition-function`, and `&context` specializers observe
  dynamic bindings (`(text base)`), matching the GNU oracle. Exact replays
  run for this batch: grouped `check-all` for `edebug-tests.el` (PASS) and
  selector `edebug-tests-writable-buffer-state-is-preserved`; regression
  comparisons showing failure sets identical to the parent commit for
  `cl-generic-tests.el`, `cl-macs-tests.el`, `cl-lib-tests.el`, and a PASS
  for `cl-print-tests.el`; full `cargo test` (1119 lib tests) green.
- Observed pre-existing single-selector discrepancies in this Linux
  environment (each fails identically on the parent commit's build, so they
  are not regressions of this batch, but they contradict the recorded
  verified-prefix state and deserve investigation):
  `cl-macs-loop-until` (selector 1782) collects past its `until` clause,
  `cl-generic-test-01-eql` fails its second-argument `eql` dispatch when run
  as a single selector, and `cconv-tests-cl-defun-:documentation` fails in
  the grouped `cconv-tests.el` replay.
- Current verification cadence: for each batch, exact-replay the selectors
  touched by the batch and run impacted unit/regression tests; run full
  `cargo test`, `cargo clippy --all-targets --all-features -- -D warnings`,
  `cargo fmt --check`, and `git diff --check` before pushing. Full selected
  prefix replays should be used strategically at larger milestones rather than
  after every small commit.
- Selectors 1908..1909 in `test/lisp/emacs-lisp/derived-tests.el` passed after
  making `define-derived-mode` delay parent mode hooks, run `:after-hook`
  bodies after mode hooks with the captured lexical environment, treating
  single function hook values as hook functions, preserving isolated closure
  bindings, and making `font-lock-add-keywords` maintain GNU-compatible raw and
  compiled keyword entries. Exact replays run for this batch: selector
  `derived-tests-after-hook-lexical`, selector `test-add-font-lock`, and grouped
  file replay for `test/lisp/emacs-lisp/derived-tests.el`; focused Rust
  regressions: `cargo test define_derived_mode_delays_parent_hooks_and_runs_after_hooks -- --nocapture`,
  `cargo test font_lock_add_keywords_accumulates_derived_mode_keywords -- --nocapture`,
  and `cargo test lexical_closures_ -- --nocapture`; exploratory grouped replay
  for `test/lisp/emacs-lisp/easy-mmode-tests.el` identified selector 1910 as
  the next frontier.
- Selectors 1910..1911 in `test/lisp/emacs-lisp/easy-mmode-tests.el` passed
  after adding the preloaded `define-mail-user-agent` helper used by generated
  mail autoloads and seeding `text-mode-abbrev-table` at interpreter startup so
  `message` can load. Exact replay run for this batch: grouped file replay for
  `test/lisp/emacs-lisp/easy-mmode-tests.el`; focused Rust regressions:
  `cargo test define_mail_user_agent_records_mail_properties -- --nocapture`
  and `cargo test abbrev_require_seeds_standard_table_name_list -- --nocapture`;
  exploratory grouped replay for `test/lisp/emacs-lisp/edebug-tests.el`
  identified selector 1912 as the next frontier.
- Selectors 1912..1913 in `test/lisp/emacs-lisp/edebug-tests.el` passed after
  allowing empty `cl-defmethod` bodies to notify edebug with
  GNU-compatible method names and making runtime `read`/`read-from-string`
  return raw backquote/comma reader symbols, including the GNU dotted `.,`
  shape inside backquoted lists. Exact replays run for this batch: selectors
  `edebug-cl-defmethod-qualifier` and `edebug-test-dot-reader`; focused Rust
  regressions:
  `cargo test cl_defmethod_allows_empty_body_and_notifies_edebug_methods -- --nocapture`,
  `cargo test backquote_dot_comma_reads_as_dotted_comma_tail -- --nocapture`,
  and
  `cargo test runtime_read_returns_raw_backquote_symbols_and_accepts_dot_comma -- --nocapture`;
  exploratory selector `edebug-tests--&rest-behavior` identified selector 1914
  as the next frontier.
- Selector 1914, `edebug-tests--&rest-behavior` in
  `test/lisp/emacs-lisp/edebug-tests.el`, passed after making generated
  `cl-defstruct (:type list)` accessors accept nil and proper list objects,
  matching GNU behavior used by Edebug form-data entries. Exact replay run for
  this batch: selector `edebug-tests--&rest-behavior`; focused Rust regression:
  `cargo test cl_defstruct_type_list_accessors_accept_nil_and_lists -- --nocapture`;
  exploratory selector `edebug-tests--conflicting-internal-names` identified
  selector 1915 as the next frontier.
- Selector 1915, `edebug-tests--conflicting-internal-names` in
  `test/lisp/emacs-lisp/edebug-tests.el`, passed after extending `cl-loop` for
  the Edebug setup forms (`initially` before iteration clauses, top-level
  `until`, `collect ... do ...`, and `do ... collect ...`), preserving original
  names when symbol-valued callables are invoked, preloading a scoped
  `eval-defun` implementation for current-buffer definitions, and adding the
  batch default for `eval-expression-debug-on-error`. Exact replay run for this
  batch: selector `edebug-tests--conflicting-internal-names`; focused Rust
  regressions:
  `cargo test cl_loop_until_collect_do_runs_body_after_collecting -- --nocapture`,
  `cargo test cl_loop_initially_before_while_for_do_collect -- --nocapture`,
  `cargo test builtin_autoloads_cover_saveplace_dependencies -- --nocapture`,
  `cargo test autoloaded_handler_function_quote_resolves_on_dispatch -- --nocapture`,
  `cargo test preloaded_eval_defun_evaluates_current_definition -- --nocapture`,
  and `cargo test debug_on_error_defaults_to_nil_in_batch -- --nocapture`;
  selector `edebug-tests-backtrace-goto-source` is the next frontier.
- Selector 1916, `edebug-tests-backtrace-goto-source` in
  `test/lisp/emacs-lisp/edebug-tests.el`, passed after extending `cl-loop` for
  the Edebug keyboard macro preparation form (`vconcat ... into ... append ...
  into ... finally return ...`), exposing the current command key vector during
  keyboard macro execution, and adding `call-last-kbd-macro` replay support for
  dynamic `last-kbd-macro` bindings. Exact replay run for this batch: selector
  `edebug-tests-backtrace-goto-source`; focused Rust regressions:
  `cargo test cl_loop_vconcat_into_append_into_finally_return -- --nocapture`,
  `cargo test execute_kbd_macro_exposes_this_single_command_keys --
  --nocapture`, and
  `cargo test call_last_kbd_macro_replays_dynamic_last_kbd_macro --
  --nocapture`; selector
  `edebug-tests-break-in-lambda-out-of-defining-context` is the next frontier.
- Selector 1786, `cl-macs-loop-with` in
  `test/lisp/emacs-lisp/cl-macs-tests.el`, passed after adding `cl-loop`
  support for sequential `with` initialization, parallel `with ... and ...`
  initialization against surrounding bindings, default `nil` initialization
  for bare `with` variables, and splitting `do ... finally FORM` so final
  forms are not executed as loop body forms. Exact replay run for this batch:
  selector `cl-macs-loop-with`; exploratory selector
  `cl-macs-test--symbol-macrolet`, which identified selector 1787 as the next
  frontier.
- Selector 1787, `cl-macs-test--symbol-macrolet` in
  `test/lisp/emacs-lisp/cl-macs-tests.el`, passed after making
  `gv-synthetic-place` resolve as a generalized place whose getter value is
  preserved for readback and whose setter function is called to produce the
  setter form evaluated in the live lexical context. Exact replay run for this
  batch: selector `cl-macs-test--symbol-macrolet`; targeted replays:
  `cl-lib-symbol-macrolet`, `cl-lib-symbol-macrolet-2`, and
  `cl-lib-symbol-macrolet-hide` in
  `test/lisp/emacs-lisp/cl-lib-tests.el`; focused Rust regressions:
  `cargo test cl_symbol_macrolet_ -- --nocapture` and
  `cargo test cl_letf_supports_gv_synthetic_place_restore -- --nocapture`;
  exploratory selector `cl-struct-define/builtin-type`, which identified
  selector 1788 as the next frontier.
- Selector 1788, `cl-struct-define/builtin-type` in
  `test/lisp/emacs-lisp/cl-preloaded-tests.el`, passed after making the
  low-level `cl-struct-define` primitive reject built-in class names with the
  GNU-compatible `wrong-type-argument cl--struct-name-p NAME name` error and
  after registering `hash-table` as a built-in class. Exact replay run for this
  batch: selector `cl-struct-define/builtin-type`; focused Rust regressions:
  `cargo test cl_struct_define_rejects_builtin_type_names -- --nocapture` and
  `cargo test cl_find_class_prefers_builtin_runtime_for_builtin_classes --
  --nocapture`; exploratory selector `cl-print-tests-ellipsis-circular`, which
  identified selector 1789 as the next frontier.
- Selectors 1789..1794 in `test/lisp/emacs-lisp/cl-print-tests.el` and
  selectors 1795..1807 in `test/lisp/emacs-lisp/cl-seq-tests.el` passed after
  adding CL printer ellipsis text properties and expansion support, honoring
  `cl-print-string-length`, limiting string property intervals and CL struct
  slots correctly, adding `cl-print--expand-ellipsis`, supporting `setf`
  places for `nthcdr` and `elt`, and making `eql` stop comparing distinct
  strings by contents. Exact replays run for this batch: selectors
  `cl-print-tests-ellipsis-circular`, `cl-print-tests-ellipsis-cons`,
  `cl-print-tests-ellipsis-string`, `cl-print-tests-ellipsis-struct`,
  `cl-print-tests-ellipsis-vector`,
  `cl-print-tests-print-to-string-with-limit`, grouped file replay for
  `test/lisp/emacs-lisp/cl-print-tests.el`, selectors `cl-seq-bignum-eql`,
  `cl-seq-count-test`, `cl-seq-delete-test`, `cl-seq-fill-test`,
  `cl-seq-mismatch-test`, `cl-seq-nsubstitute-test`,
  `cl-seq-position-test`, `cl-seq-remove-duplicates-test`,
  `cl-seq-remove-test`, `cl-seq-replace-test`, `cl-seq-search-test`,
  `cl-seq-substitute-test`, and grouped file replay for
  `test/lisp/emacs-lisp/cl-seq-tests.el`; focused Rust regressions:
  `cargo test cl_prin1_to_string_marks_circular_ellipsis -- --nocapture`,
  `cargo test cl_prin1_to_string_marks_cons_ellipsis -- --nocapture`,
  `cargo test cl_prin1_to_string_marks_string_ellipsis -- --nocapture`,
  `cargo test cl_prin1_to_string_marks_struct_ellipsis -- --nocapture`,
  `cargo test eql_does_not_compare_distinct_strings_by_contents --
  --nocapture`, `cargo test cl_mismatch_key_uses_eql_for_default_test --
  --nocapture`, and
  `cargo test cl_substitute_updates_list_copy_through_setf_elt --
  --nocapture`; exploratory selector `comp-cstr-test-1`, which identified
  selector 1808 as the next frontier.
- Selectors 1808..1823 in `test/lisp/emacs-lisp/comp-cstr-tests.el` passed
  after making `cl-defstruct` named constructors keep the default constructor,
  allowing constructor `&aux` bindings to reference constructor arguments,
  separating true vector literals from ordinary `(vector ...)` lists, expanding
  local `cl-macrolet` macros in `setf` places, adding `cl-defun` named block
  returns, supporting `cl-loop` forms used by comp-cstr, and completing
  built-in class parent/name metadata for comp-cstr type normalization. Exact
  replays run for this batch: selectors `comp-cstr-test-1`,
  `comp-cstr-test-10`, `comp-cstr-test-11`, `comp-cstr-test-12`,
  `comp-cstr-test-13`, `comp-cstr-test-14`, `comp-cstr-test-15`,
  `comp-cstr-test-16`, `comp-cstr-test-17`, `comp-cstr-test-18`,
  `comp-cstr-test-19`, `comp-cstr-test-2`, `comp-cstr-test-20`,
  `comp-cstr-test-21`, `comp-cstr-test-22`, and `comp-cstr-test-23`;
  grouped replay for `test/lisp/emacs-lisp/comp-cstr-tests.el`, which
  confirmed later failures remain; focused Rust regressions:
  `cargo test cl_defstruct_constructor_aux_can_reference_constructor_args --
  --nocapture`,
  `cargo test cl_defstruct_named_constructors_keep_default_constructor --
  --nocapture`,
  `cargo test vectorp_recognizes_vector_literals -- --nocapture`,
  `cargo test remove_filters_lists_vectors_and_strings -- --nocapture`,
  `cargo test cl_loop_if_do_else_do_supports_finally_return -- --nocapture`,
  `cargo test cl_loop_named_catches_return_from_do_body -- --nocapture`,
  `cargo test cl_defun_ -- --nocapture`,
  `cargo test cl_macrolet_expands_setf_places -- --nocapture`, and
  `cargo test cl_find_class_prefers_builtin_runtime_for_builtin_classes --
  --nocapture`; exploratory selector `comp-cstr-test-24`, which identified
  selector 1824 as the next frontier.
- Selectors 1824..1900 completed the remaining selected tests in
  `test/lisp/emacs-lisp/comp-cstr-tests.el` after extending `cl-loop` support
  for comp-cstr normalization forms (`initially`, sequential `when` clauses,
  `if ... collect ... into ... else collect ... into ...`, `unless ... do`
  final forms, repeated `do` handling, and unconditional follow-up `do`
  clauses) and canonicalizing built-in class name/parent values so `t` is the
  real Lisp `t` object for `eq`/`memq` subtype checks. Exact replays run for
  this batch: selectors `comp-cstr-test-24` through `comp-cstr-test-93`,
  plus the lexicographic manifest selectors `comp-cstr-test-3` through
  `comp-cstr-test-9`; grouped replay for
  `test/lisp/emacs-lisp/comp-cstr-tests.el` passed. Focused Rust regressions:
  `cargo test cl_loop_ -- --nocapture` and
  `cargo test cl_find_class_prefers_builtin_runtime_for_builtin_classes --
  --nocapture`; exploratory selector `test-native-compile-prune-cache`, which
  identified selector 1901 as the next frontier.
- Selectors 1901..1903 in `test/lisp/emacs-lisp/comp-tests.el` passed after
  binding startup time variables used while loading `comp.el` and exposing the
  `native-compile` feature so the upstream cache-pruning implementation runs,
  while keeping native compilation availability and native-function probes
  false in the headless runtime. Exact replays run for this batch: selector
  `test-native-compile-prune-cache` and grouped replay for
  `test/lisp/emacs-lisp/comp-tests.el`; focused Rust regressions:
  `cargo test native_comp_capability_probes_are_honest -- --nocapture` and
  `cargo test startup_time_variables_are_bound_in_batch_runtime --
  --nocapture`; exploratory selector `test-copyright-update`, which identified
  selector 1904 as the next frontier.
- Selectors 1904..1907 in `test/lisp/emacs-lisp/copyright-tests.el` passed
  after adding standard autoloads for `define-skeleton` and `fill-region`,
  binding fill/runtime defaults (`char-script-table`, `use-hard-newlines`),
  and making `re-search-backward` clamp below-min integer bounds for short
  buffers so `copyright-at-end-flag` can search from the end. Exact replays run
  for this batch: selector `test-copyright-update`, selector
  `text-copyright-fix-years`, and grouped replay for
  `test/lisp/emacs-lisp/copyright-tests.el`; focused Rust regressions:
  `cargo test builtin_autoloads_cover_saveplace_dependencies -- --nocapture`,
  `cargo test adaptive_fill_defaults_are_bound -- --nocapture`,
  `cargo test char_script_table_is_bound_for_text_fill_runtime --
  --nocapture`, `cargo test re_search_backward_clamps_below_min_bound --
  --nocapture`, and
  `cargo test copyright_update_updates_last_notice_when_searching_from_end --
  --nocapture`; exploratory grouped replay for
  `test/lisp/emacs-lisp/derived-tests.el`, which identified selector 1908 as
  the next frontier.
- Selectors 1553..1563 in `test/lisp/emacs-lisp/bytecomp-tests.el` passed
  after adding byte-compile diagnostics for malformed `interactive` forms,
  `make-process` keyword arguments, versioned obsolete function/variable
  warnings, and obsolete hook-variable references.
- Selectors 1564..1572 in `test/lisp/emacs-lisp/bytecomp-tests.el` passed
  after adding byte-compile diagnostics for function/macro redefinitions,
  `save-excursion` around `set-buffer`, constant/nonvariable `let` bindings,
  constant/nonvariable `setq` targets, and odd `setq` argument counts.
- Selectors 1573..1589 in `test/lisp/emacs-lisp/bytecomp-tests.el` passed
  after adding byte-compile diagnostics for wide docstrings in definition
  forms, including file-local docstring width overrides and ignored
  substitution/signature lines.
- Selectors 1590..1597 in `test/lisp/emacs-lisp/bytecomp-tests.el` passed
  after making `byte-compile-file` write macro-expanded top-level forms into
  its `.elc` stub, making later macro definitions shadow earlier ones, and
  expanding `defun`/`defsubst` bodies while `cl-macrolet` local macros are
  active.
- Selectors 1598..1604 in `test/lisp/emacs-lisp/bytecomp-tests.el` and
  selectors 1605..1606 in `test/lisp/emacs-lisp/cconv-tests.el` passed after
  restoring load-time `lexical-binding` state, reporting `identity` arity,
  making `pcase` treat keyword symbols as constants, making backquoted comma
  forms match nested pcase patterns, canonicalizing `intern`/`intern-soft` for
  `nil` and `t`, and accepting function-quoted lambdas in `byte-compile`.
- Selector 1607, `cconv-safe-for-space` in
  `test/lisp/emacs-lisp/cconv-tests.el`, passed after trimming unused lexical
  closure frames for evaluated lambdas, honoring
  `:closure-dont-trim-context`, and printing retained closure environments
  through `prin1`.
- Selectors 1608..1622 in `test/lisp/emacs-lisp/cconv-tests.el` passed after
  evaluating and recording leading `(:documentation FORM)` metadata for
  lambdas, `defun`/`defsubst`, `cl-defun`/`cl-defsubst`,
  `cl-function`, and generic/method definitions, returning generic/method docs
  from `describe-function`, preserving interactive-form closure mutations, and
  making `called-interactively-p` reflect `call-interactively` calls. Selector
  1623, `check-declare-tests-locate`, also passed without further changes.
- Selectors 1624..1628 in `test/lisp/emacs-lisp/check-declare-tests.el`
  passed after exposing the standard `hack-local-variables` entry point used
  when scanning or verifying Elisp files and making `display-warning` honor
  warning prefix callbacks and explicit warning buffer arguments.
- Selectors 1696..1700 in `test/lisp/emacs-lisp/checkdoc-tests.el` passed
  after preloading `completion-table-dynamic`, dispatching programmed
  completion collections through `try-completion`/`all-completions`/
  `test-completion`, and adding callable `prog-mode` with its standard
  `fundamental-mode` parent.
- Selectors 1701..1705 in `test/lisp/emacs-lisp/checkdoc-tests.el` passed
  after adding enough Lisp list/defun navigation for checkdoc to find
  docstrings, exposing `ppss-depth`, and defining the standard
  `sentence-end-double-space` default. Exact replays run for this batch:
  selector `checkdoc-cl-defun-with-allow-other-keys-ok`; selector
  `checkdoc-docstring-avoid-false-positive-ok`; grouped replay
  `checkdoc-cl-defun-with-(key|allow-other-keys|default-optional-value|destructuring)-ok`
  plus `checkdoc-tests--next-docstring`, which confirmed selectors 1701..1705
  passed and selector 1706 remained the next frontier.
- Selectors 1706..1719 in `test/lisp/emacs-lisp/checkdoc-tests.el` and
  selector 1721, `cl-concatenate`, passed after seeding the standard
  `doc-string-elt` symbol properties used by checkdoc and making unknown
  string escapes read like GNU Emacs by dropping the quoting backslash. Exact
  replays run for this batch: selectors `checkdoc-tests--bug-24998`,
  `checkdoc-tests--next-docstring`, grouped checkdoc error/fix tests, grouped
  checkdoc abbreviation tests, full `cl-extra-tests.el` to identify the next
  failure, and standalone selector `cl-concatenate`.
- Selectors 1722..1728 in `test/lisp/emacs-lisp/cl-extra-tests.el` passed
  after making `cl-defstruct` honor explicit `:predicate` names, which is
  required by `cl-make-random-state`'s private random-state structure. Exact
  replays run for this batch: selector `cl-extra-test-cl-make-random-state`
  and grouped replay for the map/mapc/mapcar/mapl/maplist plus `cl-get`
  selectors.
- Selector 1729, `cl-getf` in `test/lisp/emacs-lisp/cl-extra-tests.el`,
  passed after resolving `cl-getf` as a generalized variable place and using
  `cl--set-getf` semantics for odd property lists. Exact replays run for this
  batch: selector `cl-getf`, full `cl-extra-tests.el`, and full
  `cl-generic-tests.el` to identify the next load-time frontier.
- Selectors 1731..1733 in `test/lisp/emacs-lisp/cl-generic-tests.el` passed
  after preloading standard Emacs Lisp mode keymaps needed by `edebug`, making
  load-time advice on not-yet-defined targets a tolerated no-op, and notifying
  Edebug definition hooks for `cl-defgeneric` inline `:method` forms. Exact
  replays run for this batch: full `cl-generic-tests.el` to expose the load
  blocker, selector `cl-defgeneric/edebug/method`, selector
  `cl-generic-test-00`, selector `cl-generic-test-01-eql`, and exploratory
  grouped replay for `cl-generic-test-01-eql` through
  `cl-generic-test-05-alias`, which identified selector 1734 as the next
  frontier.
- Selectors 1734..1735 in `test/lisp/emacs-lisp/cl-generic-tests.el` passed
  after registering `cl-defstruct :include` parentage for class dispatch,
  rewriting `cl-call-next-method` forms against explicit captured previous
  methods so nested `:around` wrappers stay distinct, and dispatching
  generalized `(setf (generic ...))` places through `(setf generic)` after
  evaluating place subforms. Exact replays run for this batch: selector
  `cl-generic-test-02-struct`, selector `cl-generic-test-03-setf`, and
  exploratory selector `cl-generic-test-04-overlapping-tagcodes`, which
  identified selector 1736 as the next frontier.
- Selectors 1736..1738 in `test/lisp/emacs-lisp/cl-generic-tests.el` passed
  after adding built-in numeric class parentage for `cl-typep`/generic
  specificity, tracking `cl-defmethod` specializers structurally including
  `eql` specializers, splicing broader methods into more-specific
  `cl-call-next-method` continuations, and canonicalizing generic method
  installation through function aliases. Exact replays run for this batch:
  selector `cl-generic-test-04-overlapping-tagcodes`, selector
  `cl-generic-test-05-alias`, selector
  `cl-generic-test-06-multiple-dispatch`, and exploratory selector
  `cl-generic-test-07-apo`, which identified selector 1739 as the next
  frontier.
- Selector 1739, `cl-generic-test-07-apo` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after honoring
  `cl-defgeneric :argument-precedence-order`, mapping method specializers to
  generic argument positions, and giving each stored method a full hidden key
  so same-class specializers on different arguments do not collide. Exact
  replays run for this batch: selector `cl-generic-test-07-apo`; exploratory
  selector `cl-generic-test-08-after/before`, which identified selector 1740
  as the next frontier.
- Selector 1740, `cl-generic-test-08-after/before` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after installing
  `cl-defmethod :before` and `:after` qualifiers as generic wrappers that run
  side effects around the primary method result while preserving live lexical
  frames shared with primary dispatch. Exact replays run for this batch:
  selector `cl-generic-test-08-after/before`; exploratory selector
  `cl-generic-test-09-advice`, which identified selector 1741 as the next
  frontier.
- Selector 1741, `cl-generic-test-09-advice` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after making
  single-list `apply` calls support the advice idiom `(apply args)` and
  teaching `cl-defmethod` to update the generic implementation underneath an
  active advice wrapper so `advice-remove` reveals the updated generic.
  Exact replays run for this batch: selector `cl-generic-test-09-advice`;
  exploratory selector `cl-generic-test-10-weird`, which identified selector
  1742 as the next frontier.
- Selector 1742, `cl-generic-test-10-weird` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after making generic
  dispatch evaluate method specializers on extra fixed arguments from the
  generic rest list and avoiding duplicate rest parameters in generated
  dispatch wrappers. Exact replays run for this batch: selector
  `cl-generic-test-10-weird`; exploratory selector
  `cl-generic-test-11-next-method-p`, which identified selector 1743 as the
  next frontier.
- Selector 1743, `cl-generic-test-11-next-method-p` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after rewriting obsolete
  `cl-next-method-p` forms inside methods to the availability of the captured
  next-method continuation, including `nil` for directly installed base
  methods. Exact replays run for this batch: selector
  `cl-generic-test-11-next-method-p`; exploratory selector
  `cl-generic-test-12-context`, which identified selector 1744 as the next
  frontier.
- Selector 1744, `cl-generic-test-12-context` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after preserving
  `&context` specializers as dynamic dispatch conditions, keeping explicit
  `eql` metadata distinct from class metadata, installing context-only methods
  as wrappers even over an empty generic, and allowing `overwrite-mode` to hold
  the dynamic values used by the upstream test. Exact replays run for this
  batch: selector `cl-generic-test-12-context`; exploratory selector
  `cl-generic-test-13-head`, which identified selector 1745 as the next
  frontier.
- Selector 1745, `cl-generic-test-13-head` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after preserving
  `(head VALUE)` method specializers as structural dispatch metadata and
  checking them as guarded `consp`/`car` equality conditions so non-list
  arguments fall through to broader methods. Exact replays run for this batch:
  selector `cl-generic-test-13-head`; exploratory selector
  `cl-generic-tests--advertised-calling-convention-bug58563`, which identified
  selector 1746 as the next frontier.
- Selector 1746, `cl-generic-tests--advertised-calling-convention-bug58563` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after recording
  `advertised-calling-convention` declarations on `cl-defgeneric` symbols and
  making the byte-compiler report method-body `declare` forms as stray, so
  `byte-compile-error-on-warn` raises the expected warning error. Exact replays
  run for this batch: selector
  `cl-generic-tests--advertised-calling-convention-bug58563`; exploratory
  selector `cl-generic-tests--method-files--finds-methods`, which identified
  selector 1747 as the next frontier.
- Selectors 1747..1748,
  `cl-generic-tests--method-files--finds-methods` and
  `cl-generic-tests--method-files--nonexistent-methods` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, passed after preserving each
  loaded file's full `current-load-list` into `load-history`, recording
  `cl-defmethod` load-history entries with method/specializer metadata, and
  exposing `cl--generic-method-files` over those entries. Exact replays run for
  this batch: selectors `cl-generic-tests--method-files--finds-methods` and
  `cl-generic-tests--method-files--nonexistent-methods`; exploratory selector
  `cl-generic-tests--print-quoted`, which identified selector 1749 as the next
  frontier.
- Selectors 1749..1753 passed after exposing a minimal
  `cl--generic-describe` implementation that renders recorded `cl-defmethod`
  load-history metadata into the current help buffer with normal `prin1`
  quoting, preserving `(eql '4)` without turning it into function-quote syntax.
  Exact replays run for this batch: selector
  `cl-generic-tests--print-quoted` in
  `test/lisp/emacs-lisp/cl-generic-tests.el`, plus selectors `cl-constantly`,
  `cl-digit-char-p`, `cl-flet-test`, and `cl-lib-adjoin-test` in
  `test/lisp/emacs-lisp/cl-lib-tests.el`; exploratory selector
  `cl-lib-arglist-performance`, which identified selector 1754 as the next
  frontier.
- Selectors 1754..1764 passed after recording public lambda lists for
  generated `cl-defstruct` constructors and exposing a minimal
  `help-function-arglist` primitive that returns that metadata before falling
  back to interpreted lambda parameters. This keeps constructors with only
  `&aux` bindings from advertising the internal `&rest args` wrapper. Exact
  replays run for this batch: selectors `cl-lib-arglist-performance`,
  `cl-the`, `cl-lib-test-incf`, `cl-lib-test-decf`, `cl-lib-test-plusp`,
  `cl-lib-test-minusp`, `cl-lib-test-oddp`, `cl-lib-test-evenp`,
  `cl-lib-test-first`, `cl-lib-test-second`, and `cl-lib-test-third` in
  `test/lisp/emacs-lisp/cl-lib-tests.el`; exploratory selector
  `cl-lib-test-fourth`, which identified selector 1765 as the next frontier.
- Selectors 1765..1771 passed after extending the existing `cl-first` through
  `cl-third` positional accessor primitive to the rest of GNU `cl-lib`'s
  one-based accessor family, `cl-fourth` through `cl-tenth`, preserving the
  same list traversal and wrong-type behavior as the earlier accessors. Exact
  replays run for this batch: selectors `cl-lib-test-fourth`,
  `cl-lib-test-fifth`, `cl-lib-test-sixth`, `cl-lib-test-seventh`,
  `cl-lib-test-eighth`, `cl-lib-test-ninth`, and `cl-lib-test-tenth` in
  `test/lisp/emacs-lisp/cl-lib-tests.el`; exploratory selector
  `cl-lib-test-endp`, which identified selector 1772 as the next frontier.
- Selector 1772 passed after adding an honest `cl-endp` primitive that accepts
  only nil or cons cells, rejects vector-like values as non-lists, and is
  protected from `cl-loaddefs` autoload shadowing via the builtin override
  table. Exact replay run for this batch: selector `cl-lib-test-endp` in
  `test/lisp/emacs-lisp/cl-lib-tests.el`; exploratory selector
  `cl-lib-test-nth-value`, which identified selector 1773 as the next frontier.
- Selectors 1773..1775 passed after exposing GNU `cl-lib`'s documented
  multiple-value aliases directly in the list primitive layer: `cl-values`
  behaves as `list`, and `cl-nth-value` behaves as `nth`. This preserves the
  upstream semantics even when `cl--defalias` side effects have not installed
  those aliases before use. Exact replays run for this batch: selectors
  `cl-lib-test-nth-value`, `cl-lib-nth-value-test-multiple-values`, and
  `cl-test-ldiff` in `test/lisp/emacs-lisp/cl-lib-tests.el`; exploratory
  selector `cl-lib-test-typep`, which identified selector 1778 as the next
  frontier.
- Selector 1778 passed after recording `cl-deftype` handlers as callable Lisp
  metadata and expanding them inside `cl-typep`, including GNU `cl-lib`'s rule
  that omitted optional deftype arguments default to `*`. The same matcher now
  evaluates expanded `member`, `and`, `or`, and `not` type specs recursively.
  Exact replay run for this batch: selector `cl-lib-test-typep` in
  `test/lisp/emacs-lisp/cl-lib-tests.el`; exploratory selector
  `cl-lib-symbol-macrolet`, which identified selector 1779 as the next
  frontier.
- Selectors 1779..1782 passed after making `cl-symbol-macrolet` rewrite
  variable references without rewriting ordinary function-call operators, and
  after teaching `cl-letf` symbol-macro substitution to rewrite its place while
  keeping the macro visible in the body. Exact replays run for this batch:
  selectors `cl-lib-symbol-macrolet`, `cl-lib-symbol-macrolet-2`,
  `cl-lib-symbol-macrolet-hide`, and `cl-lib-defstruct-record` in
  `test/lisp/emacs-lisp/cl-lib-tests.el`; exploratory selector `old-struct`,
  which identified selector 1783 as the next frontier.
- Selector 1783 passed after adding `cl-loop` `vconcat` accumulation, which
  evaluates a vector-producing expression each iteration and returns a vector
  containing the concatenated elements. This batch also added honest legacy
  CL struct compatibility support for `cl-old-struct-compat-mode`,
  `cl--struct-get-class`, `cl-struct-define`, and old `cl-struct-*` tagged
  vectors in `type-of`, covering earlier out-of-order selectors
  `old-struct` and `cl-lib-old-struct`. Exact replays run for this batch:
  selectors `cl-macs-loop-vconcat` in `test/lisp/emacs-lisp/cl-macs-tests.el`
  and `old-struct`, `cl-lib-old-struct`, and `cl-constantly` in
  `test/lisp/emacs-lisp/cl-lib-tests.el`; exploratory selector
  `cl-macs-loop-when`, which identified selector 1784 as the next frontier.
- Selector 1784 passed after binding the CL loop anaphoric `it` variable to
  the truthy `when` condition value for `return`, `collect`, `append`, and
  `collect ... into` actions, and after accepting simple `when ... return`
  clauses without a trailing `finally return`. The loop parser also recognizes
  the nested `when`/`else` collect-into shape used by the upstream selector.
  Exact replay run for this batch: selector `cl-macs-loop-when` in
  `test/lisp/emacs-lisp/cl-macs-tests.el`; exploratory selector
  `cl-macs-loop-while`, which identified selector 1785 as the next frontier.
- Selector 1785 passed after teaching `cl-loop` `for VAR = INIT then STEP`
  assignment clauses to evaluate `INIT` on the first iteration and `STEP` on
  later iterations, and after checking loop `while` conditions before those
  per-iteration assignment updates. Exact replay run for this batch: selector
  `cl-macs-loop-while` in `test/lisp/emacs-lisp/cl-macs-tests.el`;
  exploratory selector `cl-macs-loop-with`, which identified selector 1786 as
  the next frontier.
- The 1..378 exact selected-test prefix was replayed after the
  `primitives.rs`/`eval.rs` split, after the SRecode/Semantic fixes, and again
  after the char-fold/regexp changes; all 378 passed. The same exact 1..378
  prefix was replayed again after the Completion Preview changes in the
  457/7080 batch; all 378 passed.
- Selectors 379..396 passed as a grouped Semantic IA replay after the
  char-fold/regexp changes. Selectors 397..407 passed as individual literal
  manifest selectors because `test/lisp/cedet/semantic-utest.el` is
  order-sensitive as a grouped run.
- Selectors 408..414 in `test/lisp/char-fold-tests.el` and selector 415
  `color-tests-cie-de2000` passed individually after adding Unicode
  decomposition support for char folding and the `time-to-seconds` time
  primitive.
- Selectors 416..446 passed individually after adding `color-values` and named
  color parsing for the existing color conversion path.
- Selectors 447..457 in `test/lisp/completion-preview-tests.el` passed
  individually after adding symbol bounds, `while-no-input`, pcase `seq`
  matching, mutable completion strings, and the completion metadata helpers
  needed by Completion Preview mode.
- Selectors 458..461 in `test/lisp/completion-tests.el` passed individually
  after adding the standard backup-retention defaults needed by
  `completion.el` and correcting `setcdr` to return the new cdr value.
- Selectors 462..463 in `test/lisp/completion-tests.el` passed individually
  after correcting regexp syntax-class translation for `\s ` and `\s_`.
- Selector 464, `cus-edit-test-bug63290` in `test/lisp/cus-edit-tests.el`,
  passed after loading real `cus-edit`, adding minimal widget accessors needed
  by `wid-edit`, and accepting marker positions in overlay range primitives.
- Selectors 465..470 in `test/lisp/cus-edit-tests.el` passed after adding
  standard obarray enumeration, `defconst` reinitialization, Custom group and
  version metadata, obsolete-variable metadata, basic batch display/window
  helpers, `dolist-with-progress-reporter`, `cl-letf` support for symbol
  property places, `setopt` type warnings, and `*Warnings*` buffer recording.
- Selectors 471..473 in `test/lisp/custom-tests.el` passed after restoring the
  built-in `user`/`changed` Custom themes, adding batch-safe frame/theme helper
  primitives, and honoring `defcustom :local` including permanent locals.
- Selector 474, `custom-test-no-saved-value-after-customizing-option` in
  `test/lisp/custom-tests.el`, passed after exposing runtime keymaps through
  their Lisp keymap-list view during sequence iteration, preserving preferred
  builtin toolbar stubs across loaded Lisp `defun`s, and defining the standard
  dynamic `inhibit-read-only` variable.
- The full 1..474 selected-test prefix was replayed after the 474 fix; all 474
  passed.
- After selector 474, the requested modularization pass moved evaluator
  bootstrap/static data into `src/lisp/eval/bootstrap.rs` and primitive
  window/scroll helpers into `src/lisp/primitives/window.rs`; the full gates
  and 1..474 compatibility prefix passed before advancing.
- Selectors 475..479 in `test/lisp/custom-tests.el` passed after adding the
  standard mark-ring Custom defaults, `make-empty-file`, dynamic
  `with-temp-file` writes, explicit-target `require` provide checks, and
  source-stub `.elc` fallback for `require-theme` support files.
- Selectors 480..481 in `test/lisp/dabbrev-tests.el` passed after adding a
  batch `execute-kbd-macro` path for parsed `kbd` vectors, dabbrev key
  bindings, interactive `*P` parsing, and the `dabbrev-capf`
  `completion-at-point` path needed by dabbrev completion.
- Selectors 482..491 in `test/lisp/dabbrev-tests.el` passed after adding
  standard minibuffer/window predicates and minibuffer contents helpers,
  formatted `user-error`, command-loop state tracking for keyboard macros,
  failed `looking-at` match-data preservation, lightweight minibuffer window
  selection, multi-key keyboard macro dispatch for search/mark/narrow commands,
  MRU `buffer-list` ordering, and `.el` auto-mode selection for dabbrev's
  same-major-mode buffer filter.
- Selectors 492..495 in `test/lisp/dabbrev-tests.el` passed after marking
  unwritable visited files read-only and enforcing `buffer-read-only` during
  insertion.
- Selectors 496..504 in `test/lisp/delim-col-tests.el` passed after matching
  GNU search `NOERROR` movement semantics and adding real window parameter
  storage for the rectangle helpers used by `delim-col`.
- Selectors 505..507 in `test/lisp/descr-text-tests.el`, 508..512 in
  `test/lisp/desktop-tests.el`, and selector 513
  `dired-guess-default` passed with the same batch.
- Selector 514, `dired-test-bug27496`, passed after adding `cl-callf`,
  keyword-aware `cl-member`, and routing `read-char-choice` through
  `read-char-from-minibuffer` when appropriate.
- Selectors 515..517 in `test/lisp/dired-aux-tests.el` passed after adding
  `rename-file`, dired destination directory coverage, minimal window buffer
  history/list helpers, `file-in-directory-p`, and property-preserving
  `split-string`.
- Selectors 518..520 in `test/lisp/dired-tests.el` passed after adding
  batch-compatible `delete-other-windows`, `switch-to-buffer-other-window`,
  `read-file-name`, and page motion helpers needed by Dired buffer setup.
  These selectors were verified individually because the local grouped
  `dired-tests.el` run is order-sensitive around Dired buffer/window state.
- Selectors 521..523 in `test/lisp/dired-tests.el` passed individually
  after routing directory visits through native Dired buffers, advertising
  native Dired buffers in `dired-buffers`, using parseable ls-style Dired
  listings, refreshing current Dired buffers after directory/file writes,
  giving native Dired buffers a native revert function, adding
  `file-name-sans-versions`, preserving `ert-with-temp-directory`'s trailing
  slash, and supporting wildcard `find-file` over directory entries.
- Selector 524, `dired-test-bug27631`, passed after adding wildcard
  directory recognition, wildcard expansion for `insert-directory`, shell
  `process-file` execution from dynamic `default-directory`, and the minimal
  Dired/window helpers needed by the wildcard listing path.
- Selector 525, `dired-test-bug27940`, passed after adding standard
  `read-answer`, Dired deletion prompt, no-dot directory matcher, and dead
  buffer cleanup semantics, plus GNU-compatible optional deletion arities.
- Selector 526, `dired-test-bug27968`, passed after making Dired buffer
  refresh on `make-directory` conditional on `dired-auto-revert-buffer` and
  preserving native Dired filename/position helpers across loaded Lisp.
- Selectors 527..528 passed after adding standard logical `line-move`
  behavior and related line-move defaults needed by Dired navigation over
  hidden detail lines.
- Selectors 529..530 passed after adding the callable
  `temporary-file-directory` helper and `directory-empty-p` over the native
  filesystem directory primitives.
- Selectors 531..535 passed after making `insert-directory` report
  `dired-free-space` for the target directory via the active
  `file-system-info` binding, independent of `default-directory`.
- Selector 536, `dnd-tests-begin-drag-files`, passed after loading real
  `ert-x`, supporting mock TRAMP local copies, fixing plain-vector/string
  predicates, and filling DND selection metadata helpers. Selector 537 is next.
- Selectors 537..542 in `test/lisp/dnd-tests.el` passed after preserving
  `dolist` binding identity for string list elements, keeping the `dolist`
  binding frame stable across nested empty lexical frames, adding `framep`,
  adding the `ascii` coding alias, and making `encode-coding-string` return
  unibyte encoded data with GNU-compatible `ascii`/`iso-8859-1` substitution.
- Selectors 543..568 in `test/lisp/dom-tests.el` passed after covering
  `cl-loop` append/collect forms used by DOM traversal, `setf` places rooted at
  `nthcdr`, HTML entity escaping, and destructive `delq` list edits.
- Selector 569 in `test/lisp/edmacro-tests.el` passed with the existing
  `edmacro-parse-keys` support.
- Selector 570, `electric-layout-control-reindentation`, passed after enabling
  electric local mode backing variables, self insertion hooks, recursive
  newline hooks, electric hook ordering, and the C-style indentation needed by
  electric layout.
- Selectors 571..580 in `test/lisp/electric-tests.el` passed after adding the
  standard RET binding for `newline` and minimal cc-mode brace layout helpers
  (`c-point-syntax`/`c-brace-newlines`) used by electric layout in C-derived
  modes.
- Selectors 581..600 in `test/lisp/electric-tests.el` passed after exposing
  the standard `syntax-ppss-flush-cache` helper used by `elec-pair.el` while
  checking string/comment syntax.
- Selectors 601..631 in `test/lisp/electric-tests.el` passed after making
  `syntax-ppss` report string starts and installing hash-comment syntax tables
  for Python/Ruby-style modes so electric-pair can apply text syntax tables in
  strings and comments.
- Selectors 632..675 in `test/lisp/electric-tests.el` passed after adding
  `mark-sexp` and making generic sexp scanning stop at string quote
  boundaries, which lets electric-pair autowrap active regions inside strings
  and comments.
- Selectors 676..735 in `test/lisp/electric-tests.el` passed after making
  hook-aware deletion accept reversed bounds and making backward generic sexp
  scanning respect word/symbol syntax boundaries, which covers electric-pair
  autowrapping from closing delimiters and from the end of regions.
- Selectors 736..762 in `test/lisp/electric-tests.el` passed after adding
  minimal `tex-mode` quote insertion for active-region wrapping, a
  `backward-delete-char-untabify` deletion alias for electric-pair backspacing,
  and the balanced-autoskipping cases.
- Selectors 763..783 in `test/lisp/electric-tests.el` passed after making
  `atomic-change-group` roll back current-buffer edits on nonlocal exits, which
  electric-pair uses while probing whether auto-pairing preserves balance.
- Selectors 784..795 in `test/lisp/electric-tests.el` passed after making
  `scan-sexps` report premature-close scan errors and exposing the open-paren
  stack in `syntax-ppss`, which lets electric-pair identify mixed-delimiter
  unbalance without auto-pairing to hide it.
- Selectors 796..818 in `test/lisp/electric-tests.el` passed after making
  `replace-match` preserve live marker positions across whole-region
  replacements, which lets `save-excursion` restore point after electric quote
  replacement.
- Selectors 819..831 in `test/lisp/electric-tests.el` passed after making
  C-family and Emacs Lisp modes expose syntax-aware comment/string contexts for
  electric quote replacement.
- Selectors 832..880 in `test/lisp/electric-tests.el` passed after making
  `char-after` and `char-before` treat a nil position like an omitted position,
  which upstream electric-pair uses while inspecting mixed delimiter contexts.
- Selectors 881..954 in `test/lisp/electric-tests.el` passed after adding the
  JS mode electric layout rules, electric indent characters, and 4-space
  indentation used by brace layout.
- Selectors 955..1100 in `test/lisp/electric-tests.el` passed after making
  `scan-sexps` treat Lisp prefix characters as part of the following
  expression, report GNU-compatible premature-end positions for mixed
  delimiters, and drop mismatched openers from the active `syntax-ppss` stack.
- Selectors 1101..1133 in `test/lisp/electric-tests.el` passed after making
  Ruby mode install its string quote syntax for single quotes, double quotes,
  and backticks, and after making generic string scanning honor the current
  delimiter instead of assuming double quotes.
- Selectors 1134..1440 in `test/lisp/electric-tests.el` passed after adding
  C-family `c-toggle-comment-style` support for line/block comments and making
  `text-mode` use GNU-compatible text syntax for quote characters.
- Selectors 1441..1458 in `test/lisp/elide-head-tests.el` passed after making
  `normal-mode` run `kill-all-local-variables` and making
  `kill-all-local-variables` run `change-major-mode-hook` before clearing
  buffer-local variables.
- Selectors 1459..1460 in `test/lisp/emacs-lisp/backquote-tests.el` passed
  after making `eval` accept explicit lexical alists and making backquote
  vector splicing omit the internal vector marker.
- Selector 1461, `backtrace-tests--backward-frame` in
  `test/lisp/emacs-lisp/backtrace-tests.el`, passed after adding the backtrace
  frame primitives needed by upstream `backtrace.el`, preserving `cl-prin1`
  builtins when `cl-print` loads, honoring `filter-buffer-substring`, and
  recording unevaluated `setq` frames for `mapbacktrace`.
- Selectors 1462..1464 in `test/lisp/emacs-lisp/backtrace-tests.el` passed
  after recording per-frame lambda locals for `backtrace--locals` and adding
  batch-safe backtrace ellipsis expansion for both direct `push-button` and
  frame-level expansion.
- Selectors 1465..1469 in `test/lisp/emacs-lisp/backtrace-tests.el` passed
  after making `%s` formatting honor `print-circle`/`print-gensym` for
  non-strings, supporting direct `car`/`cdr` `setf` places, and adding the
  `indent-line-to` buffer primitive used by backtrace pretty-print expansion.
- Selector 1470, `benchmark-tests` in
  `test/lisp/emacs-lisp/benchmark-tests.el`, passed after exposing the standard
  `gcs-done` and `gc-elapsed` benchmark variables.
- Selector 1471, `bindat-test--pack-val` in
  `test/lisp/emacs-lisp/bindat-tests.el`, passed after honoring
  `macroexpand-all` local macro environments, adding the EQL-specializer
  dispatch needed by Bindat type generation, and making `multibyte-string-p`
  return nil for non-strings.
- Selectors 1472..1473 in `test/lisp/emacs-lisp/bindat-tests.el` passed after
  preserving `letrec` through `macroexpand-all`, evaluating recursive lexical
  bindings with self-referential lambda captures, and allowing `logior` to
  operate on bignum integers produced by wide Bindat unsigned unpacking.
- Selectors 1494..1502 in `test/lisp/emacs-lisp/bindat-tests.el` passed after
  preserving `let`/`let*` binding names through `macroexpand-all`, adding
  string-object mutation for Bindat preallocated string packing, supporting
  vector and bool-vector `substring`, and adding the small network-address
  formatter needed by Bindat IP checks.
- Selectors 1503..1504 in `test/lisp/emacs-lisp/byte-run-tests.el` passed
  after making `make-obsolete` and `make-obsolete-variable` reject nil and t
  obsolete names.
- Selector 1505, `byte-compile-file/no-byte-compile` in
  `test/lisp/emacs-lisp/bytecomp-tests.el`, passed after adding the coding and
  warning defaults used by byte compilation, preserving native mode-hook
  dispatch, adding default file-mode helpers, and applying the
  `no-byte-compile` file-local header in `normal-mode`.
- Selector 1506, `bytecomp--byte-op-error-backtrace` in
  `test/lisp/emacs-lisp/bytecomp-tests.el`, passed after preserving builtin
  call frames through handler dispatch, treating `throw` as a non-evaluating
  special form, and matching byte-op error payloads for list, cons, vector,
  record, and string accessors.
- Selectors 1507..1518 in `test/lisp/emacs-lisp/bytecomp-tests.el` passed
  after adding copy-tree support, byte-compile function metadata preservation,
  byte-switch decompile metadata, byte-compile-file warning handling, and
  `with-suppressed-warnings` suppression for structural byte-compile
  diagnostics.
- Selector 1519, `bytecomp-test-defcustom-type` in
  `test/lisp/emacs-lisp/bytecomp-tests.el`, passed after validating malformed
  `defcustom :type` specs in the byte-compile diagnostic scanner and preserving
  the compile-log buffer point while appending warnings.
- Selectors 1520..1521 in `test/lisp/emacs-lisp/bytecomp-tests.el` passed
  after routing `byte-compile-from-buffer` through the structural byte-compile
  diagnostic scanner, warning for unresolved calls outside known feature
  guards, treating `featurep 'emacs` as true, and reading the accessible buffer
  region used by the byte compiler.
- Selectors 1522..1525 in `test/lisp/emacs-lisp/bytecomp-tests.el` passed
  after warning when `byte-compile-file` sees no first-line `lexical-binding`
  directive, preserving the existing file-output error behavior, and adding a
  compact `define-advice` special form that installs named advice through the
  existing advice wrappers without loading the full `nadvice.el` runtime.
- Selector 1526, `bytecomp-tests--unescaped-char-literals`, passed after
  routing byte compilation through the existing unescaped character literal
  scanner and honoring `byte-compile-error-on-warn` for compile warnings.
- Selector 1527, `bytecomp-tests--warnings`, passed after the byte-compile
  diagnostic scanner started tracking earlier function calls and warning when
  a later `defmacro` redefines that callee as a macro, while treating
  `eval-and-compile` definitions as compile-time knowledge.
- Selector 1528,
  `bytecomp-tests-byte-compile--wide-docstring-p/func-arg-list`, passed after
  implementing `byte-compile--wide-docstring-p` as a protected byte-compile
  primitive that ignores function argument-list docstring lines, URLs, and
  fixed-width command-key substitutions.
- Selector 1529, `bytecomp-tests-dynbind`, passed after making compiled lambda
  forms honor the current `lexical-binding` mode, propagating dynamic binding
  into nested lambdas, and preserving dynamic `condition-case` handler
  variables captured by returned lambdas.
- Selectors 1530..1531, `bytecomp-tests-function-put` and
  `bytecomp-tests-lexbind`, passed after defaulting `print-quoted` to non-nil
  so printed source forms preserve backquote/comma syntax for byte compilation.
- Selector 1532, `bytecomp-warn--ignore`, passed after adding byte-compile
  diagnostics for unused lambda arguments and ignored `assq` return values,
  while treating `ignore` as an explicit use.
- Selector 1533, `bytecomp-warn-dodgy-args-eq`, passed after warning when
  `eq`/`eql` compare literal values whose identity-oriented semantics may
  never match, preserving `eql` numeric literal exceptions.
- Selector 1534, `bytecomp-warn-dodgy-args-memq`, passed after extending the
  same literal diagnostics to identity-based member functions and quoted
  list/alist elements, and after making `cl-labels` local functions visible to
  each other.
- Selector 1535, `bytecomp-warn-quoted-condition`, passed after warning when
  `condition-case` handlers and `ignore-error` condition arguments quote their
  condition names.
- Selectors 1536..1541, the lexical-variable hook warning resource files,
  passed without additional code changes.
- Selector 1544, `bytecomp/warn-callargs-defsubst.el`, passed after retaining a
  single byte-compile diagnostics pass across all forms in `byte-compile-file`,
  teaching the scanner that `defsubst` establishes callable arity, matching the
  GNU warning text for too many arguments, and falling back to a temporary
  output file when the default `.elc` destination is unwritable.
- Selectors 1545..1547 passed after adding byte-compile diagnostics for
  `defcustom` forms that omit `:group` or `:type`; selector 1547,
  `bytecomp/warn-defvar-lacks-prefix.el`, was confirmed in the same forward
  probe and required no additional code changes.
  A forward probe of nearby warning-file selectors observed selector 1548,
  `bytecomp/warn-format.el`, as the next mismatch.

## Workflow

1. Start from the next unverified ordered test in `compat/oracle_tests_all.txt`.
2. Continue forward until the first compatibility mismatch that requires a code
   fix.
3. Fix the behavior honestly in Rust. Do not hardcode test answers, delegate to
   oracle Emacs, or add compatibility shortcuts that only recognize test data.
4. Before committing for test N, run targeted regression coverage for tests
   1..N-1 that the change could affect. Broaden the coverage when the change
   touches shared evaluator, primitive, reader, buffer, process, or file I/O
   behavior.
5. Every code-change batch must pass:
   - `cargo fmt --check`
   - `cargo clippy --all-targets --all-features -- -D warnings`
   - `cargo test`
   - `git diff --check`
   - relevant `compat-harness` runs
6. Commit and push each coherent passing batch before moving on.

## Notes From The 378 Batch

- `src/lisp/eval.rs` and `src/lisp/primitives.rs` were split into modules.
- All Rust files under `src/lisp/eval*` and `src/lisp/primitives*` are below
  3000 lines after the split.
- `test/lisp/calendar/todo-mode-tests.el` is order-sensitive as a full-file
  run. For the 1..378 verification, its 42 selected tests were verified as
  individual literal ERT selectors.
- `test/lisp/calc/calc-tests.el` passed with a longer timeout than the default
  short sweep timeout.
- `test/lisp/dabbrev-tests.el` is order-sensitive as a grouped full-file run;
  verify its selected tests as individual literal selectors when replaying the
  prefix.
