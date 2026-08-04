# AI Continuation Instructions - Do Not Skip

This document is the handoff for any AI agent continuing the GNU Emacs
compatibility goal in this repository. Read it before making changes.

## Active Objective

Make `emaxx` match GNU Emacs at the Elisp API boundary by passing the selected
compatibility tests from the sibling GNU Emacs checkout at `../emacs`.

The canonical ordered manifest is:

- `compat/oracle_tests_all.txt`
- `compat/oracle_tests_all.md`

The denominator is 7080 selected tests. Do not use source-tree `ert-deftest`
counts as the progress denominator.

## Current Resume Point

- 2026-08-04 ERC EVENT-LOOP CHECKPOINT: the fresh contiguous ordered frontier
  is 3,030/7,080, leaving 4,050 selectors.  The 160-outcome ERC batch after
  2,870 completes SASL, StatusMsg, ZNC reconnection, scenario internals,
  Match, Misc Commands, scenario Stamp, Services, Stamp, core ERC, and Track.
  Native `keyboard.c' timer queues are special variables, so dynamically
  isolated test queues remain visible to timer.el and event waits; this fixes
  StatusMsg without advice or polling workarounds.  `move-end-of-line' crosses
  leading timestamp fields like GNU, recursive `keymap-unset' descends through
  prefix maps, simulated minibuffers preserve the caller's prefix argument,
  and undefined-key reports use the ordinary advised `message' call path.
  Load-bearing artifacts: StatusMsg
  `target/compat/run-1785810395046978000-40219`; final Stamp 12/12
  `target/compat/run-1785810964264012000-41569`; core ERC 94/94
  `target/compat/run-1785829193123249000-49380`; Track 7/7
  `target/compat/run-1785829305709710000-49703`.  The publication gate is
  green: 1,663 unrestricted library tests pass in the restricted run, and all
  six localhost/TLS/UDP tests pass when rerun with network access.  NEXT is
  selector 3,031,
  `em-alias-test/alias-all-args-var', in
  `test/lisp/eshell/em-alias-tests.el' (8 selected outcomes).  Keep the four
  separately tracked bytecode/VM primitives deferred: native remains
  1,416/1,420.
- 2026-08-04 ERT/WINDOW-ISOLATION CHECKPOINT: the fresh contiguous ordered
  frontier is 2,870/7,080, leaving 4,210 selectors.  The 201-outcome batch
  after 2,669 verifies Testcover, Text Property Search, Thunk, Timer, Track
  Changes, Unsafep, Vtable, Warnings, Viper, Env, EPG Config, EPG, and ERC
  Button/DCC/Fill/Goodies/Join/Match/Networks/Nicks.  Key lookup ranks the
  shortest character sequence before longer prefixes and function-key
  aliases when GNU asks for the first binding.  Only `defcustom', not
  `defvar' or `defvar-local', installs fallback `standard-value' metadata;
  this keeps ERC's buffer-local module modes out of the global Custom path.
  The native Rust ERT runner now includes GNU's per-test
  `save-window-excursion', backed by complete window-tree snapshots that
  restore the root, every preexisting window record, selection, displayed
  buffer state, and current buffer while retiring windows created by the
  test.  Load-bearing artifacts: ERC Networks 43/43
  `target/compat/run-1785794082967853000-33255`; final ERC Nicks 16/16
  `target/compat/run-1785805313626999000-35587`.  NEXT is selector 2,871,
  `erc-sasl--mechanism-offered-p', in `test/lisp/erc/erc-sasl-tests.el' (9
  selected outcomes).  Keep the four separately tracked bytecode/VM
  primitives deferred: native remains 1,416/1,420.  The complete publication
  gate is green: rustfmt, diff checks, all-target check, strict
  all-feature/all-target Clippy, all 1,658 non-network library tests in the
  restricted sandbox, the six exact localhost/TLS/UDP tests with networking
  allowed, focused regressions, and grouped ERC Nicks.
- 2026-08-03 CALLABLE-INTROSPECTION CHECKPOINT: the fresh contiguous ordered
  frontier is 2,669/7,080, leaving 4,411 selectors.  Seq (52/52), Shadow
  (1/1), Shortdoc (5/5), Subr-X (47/47), Syntax (1/1), and Tabulated List
  (4/4) are green after the 2,559 checkpoint.  Preloaded Lisp callables expose
  the same observable byte-code-function façade through `symbol-function' and
  `indirect-function'; its slot-zero argument descriptor, `func-arity', docs,
  and help arglists are derived from GNU DOC/source metadata on the configured
  load path.  Lisp-source docstrings are decoded by the real reader and gain
  `(fn ...)' usage from their source arglists when absent.  `defmacro' replaces
  an existing autoload cell when that macro is loaded, so `rx', `rx-define',
  and their siblings stop advertising stale stubs; `let-alist' likewise loads
  GNU's macro before using the native file-less fallback.  Load-bearing
  artifacts: exact Shortdoc
  `target/compat/run-1785772693841335000-28237`; grouped Shortdoc
  `target/compat/run-1785772785877356000-28399`; Subr-X
  `target/compat/run-1785772832293102000-28531`; Syntax
  `target/compat/run-1785772893382820000-28804`; Tabulated List
  `target/compat/run-1785772868478180000-28670`.  NEXT is selector 2,670 in
  `test/lisp/emacs-lisp/testcover-tests.el' (31 selectors).  Continue with its
  earliest exact mismatch, then group the file.  Keep the four separately
  tracked bytecode/VM primitives deferred: native remains 1,416/1,420.  The
  proportional checkpoint gate is green: rustfmt, all-target check, strict
  all-feature/all-target Clippy, diff checks, focused callable regressions,
  and all 110 selectors from Seq through Tabulated List.
- 2026-08-03 GENERIC-DISPATCH/LITERAL-FILE CHECKPOINT: the fresh contiguous
  ordered frontier is 2,559/7,080, leaving 4,521 selectors.  The published
  batch starts after 2,411 and verifies nadvice, oclosure, package, pcase, pp,
  range, regexp-opt, ring, rmc, and rx.  Cold specialized `cl-defmethod'
  registrations use a replaceable terminal even before a generic exists;
  later `cl-defgeneric' definitions replace only that terminal, concrete
  oclosure types precede interpreted/byte-code representation specializers,
  and generated oclosure accessors expose canonical `(setf ACCESSOR)'
  functions.  `set-visited-file-name' clears the recorded timestamp like GNU,
  an unknown timestamp verifies true and suppresses supersession prompts, and
  explicit `no-conversion' file reads preserve raw CRLF/CR bytes.  Load-bearing
  artifacts: nadvice grouped
  `target/compat/run-1785672235282253000-14202`; oclosure grouped
  `target/compat/run-1785672794209128000-14840`; exact package regressions
  `target/compat/run-1785679049720973000-16394` and
  `target/compat/run-1785684197511587000-17342`; package grouped
  `target/compat/run-1785684248896863000-17474`; final grouped rx
  `target/compat/run-1785684434559994000-18408`.  NEXT is selector 2,560,
  `test-difference-with-nil`, in `test/lisp/emacs-lisp/seq-tests.el`.  Use
  exact selectors to diagnose the first mismatch, grouped files to bank
  unchanged runs, one release subject build per source change, and the full
  publication gate only every roughly 100--150 selectors or after a high-risk
  shared-runtime change.  Keep the four separately tracked bytecode/VM
  primitives deferred: native remains 1,416/1,420.  The publication gate is
  green: rustfmt, strict all-target Clippy, and diff checks; 1,652 restricted
  library tests plus the six exact localhost tests with networking allowed;
  28 compatibility-harness tests, 1 performance-harness test, 9 CLI tests,
  and 3 ERT-runner tests (plus the zero-test main binary).
- 2026-08-02 SYNTAX-DESCRIPTOR CHECKPOINT: the fresh contiguous ordered
  frontier is 2,345/7,080, completing all 37 selected `lisp-tests.el`
  outcomes.  `syntax-after` and `syntax-class` are GNU-preloaded Lisp policy;
  Emaxx now defines them in `simple_compat.el` over its native syntax-table
  and text-property substrate.  The primitive `syntax.c` switches
  `parse-sexp-ignore-comments` and `parse-sexp-lookup-properties` are bound
  and dynamically special in the Rust interpreter, so lexical callers can
  bind them across separately defined helpers.  A focused Rust regression
  pins normal delimiters, syntax text-property override, bounds, nil, and
  dynamic binding.  Exact selectors 2,333 and 2,334 pass in
  `target/compat/run-1785653718769724000-1819` and
  `target/compat/run-1785653769021843000-2107`; the grouped 37/37 artifact is
  `target/compat/run-1785653802030950000-2250`, proving the contiguous file
  boundary through selector 2,345.  NEXT is 2,346,
  `macroexp--test-obsolete-macro`; 4,735 selectors remain.  Advance by grouped
  manifest files or coherent blocks, diagnose the earliest mismatch, and run
  the full publication gate once per meaningful grouped checkpoint.  Keep
  the four separately tracked bytecode/VM primitives deferred: native remains
  1,416/1,420.  The publication gate is green: rustfmt, all-target check,
  strict Clippy, and diff checks; all 1,653 library tests (1,647 restricted
  plus the six exact localhost tests with networking allowed); 28
  compatibility-harness tests, 1 performance-harness test, 8 CLI tests, and
  3 ERT-runner tests (plus the zero-test main binary).
- 2026-08-02 GNUTLS PROCESS-SESSION CHECKPOINT: the exact generated GNU C
  inventory is 1,416/1,420 mirrored, with only the four explicitly deferred
  VM primitives left: `byte-code`, `internal-stack-stats`, `make-byte-code`,
  and `make-closure`.  `gnutls-boot` now creates a real client session through
  the existing host-GnuTLS `libloading` boundary, owns the session,
  credentials, and library together with RAII, and routes the process's
  nonblocking reads and writes through `gnutls_record_recv` and
  `gnutls_record_send`.  Anonymous and X.509 negotiation, priority/SNI,
  system and explicit trust, CRLs, encrypted client keys and flags,
  verification policy, minimum DH bits, logging, negotiated peer metadata,
  `gnutls-bye`, deinit, EOF, and process deletion all use that same live
  session.  Direct sibling-GNU probes pin arities, validation conditions, and
  the four standard `gnutls-code` symbol properties; live `gnutls-serv`
  regressions prove anonymous encrypted I/O plus explicit-trust X.509 success,
  encrypted private-key loading, and hostname rejection.  Unix connected
  stream descriptors are implemented; non-Unix builds report the explicit
  catchable transport boundary until their platform callback adapter exists.
  Exact fingerprints are mirrored `(1_416, 10_665_204_901_044_147_906)` and
  missing `(4, 11_801_919_205_790_401_648)`.  SEQUENCING OVERRIDE: keep those
  four VM entries visibly unresolved and resume the ordered 7,080-selector
  frontier at the last published compatibility checkpoint, 2,332/7,080; NEXT
  is selector 2,333.  Do not treat the stale deeper 3,554 note below as a
  published strict frontier.  The complete publication gate is green:
  rustfmt, all-target check, strict Clippy, and diff checks; all 1,652 library
  tests (1,646 in the restricted sandbox plus the six exact localhost tests
  with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).
- 2026-08-02 DYNAMIC-MODULE CHECKPOINT: the exact generated GNU C inventory is
  1,414/1,420 mirrored, with six missing and no `emacs-module.c` entry left.
  `module-load` uses the established mature `libloading` crate for real
  shared-library opening and symbol inspection.  It preserves GNU's
  structured open/GPL-marker/init-symbol failures and their
  `module-load-failed` hierarchy.  A library exporting both required symbols
  reaches Emaxx's explicit catchable value-ABI boundary; Emaxx never calls
  `emacs_module_init` with an invented runtime.  GNU's versioned callback
  table, opaque value handles, nonlocal exits, module functions, and user
  pointer/finalizer lifetime model do not yet exist here.  A compiled probe
  pins that safety property, while a direct sibling-GNU oracle pins arity and
  validation conditions.  Exact fingerprints are mirrored
  `(1_414, 916_376_608_050_879_346)` and missing
  `(6, 462_599_485_011_907_078)`.  Only `gnutls-boot` and `gnutls-bye` remain
  outside the four deferred VM primitives.  Finish that pair, then switch
  immediately back to the ordered 7,080-selector frontier.  The complete
  publication gate is green: rustfmt, strict Clippy, and diff checks; all
  1,649 library tests (1,645 in the restricted sandbox plus the four exact
  localhost socket tests with networking allowed); 28 compatibility-harness
  tests, 1 performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus
  the zero-test main binary).
- 2026-08-02 ALLOCATION-TELEMETRY CHECKPOINT: the exact generated GNU C
  inventory is 1,413/1,420 mirrored, with seven missing and no non-VM
  `alloc.c` entry left.  `memory-use-counts` preserves GNU's zero-argument
  contract and stops at Emaxx's explicit catchable allocation-telemetry
  boundary.  GNU's seven cumulative counters are incremented at type-specific
  C GC-arena allocation sites and never decrease after GC; Emaxx's Rust
  ownership model has no equivalent category accounting.  Allocator bytes,
  live-object scans, and constant zeros would all be dishonest replacements.
  A direct sibling-GNU oracle pins the arity and seven-integer result shape.
  Exact fingerprints are mirrored `(1_413, 9_702_192_709_240_017_211)` and
  missing `(7, 17_013_614_379_476_872_707)`.  The remaining non-bytecode
  entries are `module-load`, `gnutls-boot`, and `gnutls-bye`; finish them
  before returning to the ordered 7,080-selector frontier with the four VM
  primitives visibly deferred.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,648 library tests (1,644 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).
- 2026-08-02 COMPILED-REGEXP CHECKPOINT: the exact generated GNU C inventory
  is 1,412/1,420 mirrored, with eight missing and no `search.c` entry left.
  `re--describe-compiled` preserves GNU's arity, current-buffer compilation
  context, and invalid-regexp signaling by using Emaxx's established mature
  `fancy-regex`/`regex-automata` backend.  Valid calls then stop at the
  explicit catchable compiled-introspection boundary: GNU exposes private
  bytecode from its own engine, while the Rust backend deliberately hides its
  VM program and delegated automata behind its stable API.  Emaxx does not
  pretend that a translated source pattern is bytecode.  A direct sibling-GNU
  oracle pins the public contract, invalid-regexp condition, and a known raw
  bytecode result.  Exact fingerprints are mirrored
  `(1_412, 15_109_526_171_507_659_563)` and missing
  `(8, 1_306_524_394_756_610_835)`.  The remaining non-bytecode entries are
  `memory-use-counts`, `module-load`, `gnutls-boot`, and `gnutls-bye`;
  complete them before switching back to the ordered 7,080-selector frontier
  with the four VM entries visibly deferred.  The complete publication gate
  is green: rustfmt, strict Clippy, and diff checks; all 1,647 library tests
  (1,643 in the restricted sandbox plus the four exact localhost socket tests
  with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).
- 2026-08-02 PORTABLE-DUMPER CHECKPOINT: the exact generated GNU C inventory is
  1,411/1,420 mirrored, with nine missing and no `pdumper.c` entry left.
  `dump-emacs-portable` preserves GNU's arity and filename validation before
  reporting Emaxx's explicit catchable "Portable dumper backend is
  unavailable" boundary.  GNU's real implementation serializes and relocates
  the live C heap; Emaxx has no compatible image writer/loader and does not
  create a corrupt lookalike file.  The copied-object sort predicate reaches
  the same boundary because it orders raw addresses from GNU's static C image,
  an identity Rust values do not possess outside a real dump.  A direct
  sibling-GNU oracle pins both contracts and the safe type-error path.  Exact
  fingerprints are mirrored `(1_411, 4_334_947_006_904_824_013)` and missing
  `(9, 12_180_328_675_483_838_565)`.  The nine-item remainder is:
  `memory-use-counts`, `module-load`, `gnutls-boot`, `gnutls-bye`,
  `re--describe-compiled`, and the four explicit VM entries.  SEQUENCING
  OVERRIDE remains authoritative: finish those five non-bytecode primitives;
  when only `byte-code`, `internal-stack-stats`, `make-byte-code`, and
  `make-closure` remain, keep them visibly unresolved and switch immediately
  back to the ordered 7,080-selector frontier.  The complete publication gate
  is green: rustfmt, strict Clippy, and diff checks; all 1,646 library tests
  (1,642 in the restricted sandbox plus the four exact localhost socket tests
  with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).
- 2026-08-02 NATIVE DISPLAY-CONNECTION CHECKPOINT: the exact generated GNU C
  inventory is 1,409/1,420 mirrored, with 11 missing and no display-connection
  entry left.  `x-open-connection` preserves exact arity and display-string
  validation, including Nextstep's deliberate ignoring of the two optional
  arguments, then stops at Emaxx's catchable "Window system is not in use or
  not initialized" boundary.  `x-close-connection` accepts the selected
  terminal, live frames, the live terminal object, and string designators,
  preserves GNU Nextstep's safely probeable `frame-live-p` rejection, and
  reaches the same explicit boundary.  Emaxx does not launch AppKit from its
  terminal runtime, claim an X11 connection with no graphical event loop, or
  terminate the process to simulate close.  A direct sibling-GNU oracle pins
  the safe validation paths; Emaxx-only assertions cover every valid
  designator because GNU's valid Nextstep open path launches platform GUI
  state and can block, while close can terminate the application.  Exact
  fingerprints are mirrored `(1_409, 16_993_417_039_166_424_446)` and missing
  `(11, 886_131_030_095_800_352)`.  The 11-item remainder is: `alloc.c` 3,
  `bytecode.c` 2, `module-load` 1, `gnutls.c` 2, portable dumper 2, and
  `re--describe-compiled` 1.  Of those, seven are non-bytecode and four are
  the explicit VM cluster.  The complete publication gate is green: rustfmt,
  strict Clippy, and diff checks; all 1,646 library tests (1,642 in the
  restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
  SEQUENCING OVERRIDE remains authoritative: finish the seven non-bytecode
  primitives; when only `byte-code`, `internal-stack-stats`, `make-byte-code`,
  and `make-closure` remain, keep those four visibly unresolved and switch
  immediately back to the ordered 7,080-selector frontier.  Return to the
  bytecode VM cluster afterward, or earlier only if it becomes the concrete
  blocker.  NEXT: commit and push this theme, then immediately select the next
  non-bytecode native family.
- 2026-08-01 NATIVE HEADLESS MENU/DRAG CHECKPOINT: the exact generated GNU C
  inventory is 1,407/1,420 mirrored, with 13 missing and no `menu.c` or drag
  entry left.  `x-popup-menu` preserves GNU's obsolete nil-position no-op,
  parses runtime keymaps, keymap lists, and legacy pane menus, and reproduces
  the safely probeable list/string/window validation before returning nil on
  Emaxx's hookless initial frame.  `x-popup-dialog` likewise validates its
  position and dialog title before the honest no-selection result.
  `menu-bar-menu-at-x-y` validates frame and fixnum inputs and returns nil
  because Emaxx's terminal frame retains no native-toolkit or redisplay-time
  menu geometry.  `x-begin-drag` preserves arity/frame validation and stops at
  the established catchable "Window system frame should be used" boundary;
  it does not fabricate a drag target or action.  A direct sibling-GNU oracle
  pins every safe menu parsing/error path, while Emaxx-only assertions cover
  the catchable menu-bar/drag replacements for GNU batch paths that can abort
  or depend on platform selection state.  Exact fingerprints are mirrored
  `(1_407, 2_037_779_427_493_928_198)` and missing
  `(13, 2_153_934_993_855_764_762)`.  The 13-item remainder is: `alloc.c` 3,
  `bytecode.c` 2, `module-load` 1, `gnutls.c` 2, display connections 2,
  portable dumper 2, and `re--describe-compiled` 1.  Of those, nine are
  non-bytecode and four are the explicit VM cluster.  The complete
  publication gate is green: rustfmt, strict Clippy, and diff checks; all
  1,645 library tests (1,641 in the restricted sandbox plus the four exact
  localhost socket tests with networking allowed); 28 compatibility-harness
  tests, 1 performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus
  the zero-test main binary).  SEQUENCING OVERRIDE remains authoritative:
  finish the nine non-bytecode primitives; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, keep
  those four visibly unresolved and switch immediately back to the ordered
  7,080-selector frontier.  Return to the bytecode VM cluster afterward, or
  earlier only if it becomes the concrete blocker.  NEXT: commit and push this
  theme, then immediately select the next non-bytecode native family.
- 2026-08-01 NATIVE GNUTLS X.509 CHECKPOINT: the exact generated GNU C
  inventory is 1,403/1,420 mirrored, with 17 missing.  The newly native
  `gnutls-format-certificate` loads the host GnuTLS X.509 API through the
  established `libloading` boundary, imports PEM certificates, requests
  GnuTLS's full certificate rendering, and releases both the opaque
  certificate and host-allocated result through the matching library
  lifecycle.  It preserves GNU's string validation, first-NUL input behavior,
  and `gnutls-format-certificate error: ...` failures.  A focused regression
  uses the sibling GNU certificate fixture and pins the real 2,863-byte output
  to SHA-256
  `2354c81d5fca4d5d2259514652d1254626f8722b6f682178cc9fce21b094fb26`,
  along with the exact prefix and error paths.  Only `gnutls-boot` and
  `gnutls-bye` remain in `gnutls.c`; `gnutls-available-p` remains nil and no
  TLS transport is claimed.  Exact fingerprints are mirrored
  `(1_403, 16_366_176_615_574_778_632)` and missing
  `(17, 3_193_031_023_882_488_446)`.  The 17-item remainder is: `alloc.c` 3,
  `bytecode.c` 2, `module-load` 1, `gnutls.c` 2, display connections 2, menus
  3, portable dumper 2, `re--describe-compiled` 1, and drag-and-drop 1.  Of
  those, 13 are non-bytecode and four are the explicit VM cluster.  The
  complete publication gate is green: rustfmt, strict Clippy, and diff checks;
  all 1,644 library tests (1,640 in the restricted sandbox plus the four exact
  localhost socket tests with networking allowed); 28 compatibility-harness
  tests, 1 performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus
  the zero-test main binary).  SEQUENCING OVERRIDE remains authoritative:
  finish the 13 non-bytecode primitives; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, keep
  those four visibly unresolved and switch immediately back to the ordered
  7,080-selector frontier.  Return to the bytecode VM cluster afterward, or
  earlier only if it becomes the concrete blocker.  NEXT: commit and push this
  theme, then immediately select the next non-bytecode native family.
- 2026-08-01 NATIVE COMPILER-BOUNDARY CHECKPOINT: the exact generated GNU C
  inventory is 1,402/1,420 mirrored, with 18 missing and no `comp.c` entry
  left.  `comp-el-to-eln-rel-filename` now computes GNU's real canonical-path
  and source-content MD5 prefixes, resolves symlinks, hashes decompressed gzip
  contents, deliberately removes `.gz` before the path hash, and preserves
  exact type and missing-file conditions.  The already mirrored
  `comp-el-to-eln-filename` now builds on that real relative name and honors a
  dynamically bound native version directory.  `comp--release-ctxt` preserves
  GNU's idempotent `t` result.  The context compilation, trampoline,
  registration, and existing-file `.eln` load entry points are native but stop
  at one explicit "Native compiler backend is unavailable" boundary:
  `native-comp-available-p` remains nil, and Emaxx neither pretends to run
  libgccjit nor claims it can load GNU-ABI native code.  `native-elisp-load`
  still preserves GNU's exact type and missing-file signals before that
  boundary.  Direct sibling-GNU oracles pin raw/gzip naming, versioned paths,
  validation, missing files, and safe release behavior; Emaxx-only regressions
  pin every unavailable mutation/load path.  Exact fingerprints are mirrored
  `(1_402, 18_222_472_919_885_261_439)` and missing
  `(18, 14_706_921_403_225_780_709)`.  The 18-item remainder is: `alloc.c` 3,
  `bytecode.c` 2, `module-load` 1, `gnutls.c` 3, display connections 2, menus
  3, portable dumper 2, `re--describe-compiled` 1, and drag-and-drop 1.  Of
  those, 14 are non-bytecode and four are the explicit VM cluster.  The
  complete publication gate is green: rustfmt, strict Clippy, and diff checks;
  all 1,643 library tests (1,639 in the restricted sandbox plus the four exact
  localhost socket tests with networking allowed); 28 compatibility-harness
  tests, 1 performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus
  the zero-test main binary).  SEQUENCING OVERRIDE remains authoritative:
  finish the 14 non-bytecode primitives; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, keep
  those four visibly unresolved and switch immediately back to the ordered
  7,080-selector frontier.  Return to the bytecode VM cluster afterward, or
  earlier only if it becomes the concrete blocker.  NEXT: commit and push this
  theme, then immediately select the next non-bytecode native family.
- 2026-08-01 NATIVE GNUTLS HOST-CRYPTO CHECKPOINT: the exact generated GNU C
  inventory is 1,393/1,420 mirrored, with 27 missing.  Three more primitives
  now use the installed GnuTLS library through the established `libloading`
  boundary: `gnutls-hash-mac` streams real host HMAC, while
  `gnutls-symmetric-encrypt` and `gnutls-symmetric-decrypt` use the host block
  and AEAD cipher APIs, including authenticated decrypt, explicit and
  `(iv-auto N)` nonces, and GNU's exact unibyte results.  All three accept
  GNU's symbol, string, descriptor-plist, and numeric selectors and sliced
  string/buffer inputs where applicable; mutable caller key strings are
  cleared on GNU's paths.  Extracted Rust key buffers use the established
  pinned RustCrypto `zeroize` 1.9.0 crate rather than a local wiping routine.
  The shared FFI declarations also now use GnuTLS's exact C unsigned and
  `size_t` return types for algorithm sizes.  Direct sibling-GNU oracles pin
  HMAC results, AES-128-CBC ciphertext, AES-128-GCM ciphertext and
  authentication, selector/slice behavior, automatic IVs, key clearing, and
  validation errors.  Only session boot, shutdown, and certificate formatting
  remain in `gnutls.c`; `gnutls-available-p` remains nil and no TLS transport
  is claimed.  Exact fingerprints are mirrored
  `(1_393, 4_948_632_708_221_859_943)` and missing
  `(27, 3_849_663_693_217_878_483)`.  The 27-item remainder is: `alloc.c` 3,
  `bytecode.c` 2, `comp.c` 9, `module-load` 1, `gnutls.c` 3, display
  connections 2, menus 3, portable dumper 2, `re--describe-compiled` 1, and
  drag-and-drop 1.  Of those, 23 are non-bytecode and four are the explicit
  VM cluster.  The complete publication gate is green: rustfmt, strict Clippy,
  and diff checks; all 1,641 library tests (1,637 in the restricted sandbox
  plus the four exact localhost socket tests with networking allowed); 28
  compatibility-harness tests, 1 performance-harness test, 8 CLI tests, and 3
  ERT-runner tests (plus the zero-test main binary).  SEQUENCING OVERRIDE
  remains authoritative: finish the 23 non-bytecode primitives; when only
  `byte-code`, `internal-stack-stats`, `make-byte-code`, and `make-closure`
  remain, keep those four visibly unresolved and switch immediately back to
  the ordered 7,080-selector frontier.  Return to the bytecode VM cluster
  afterward, or earlier only if it becomes the concrete blocker.  NEXT:
  commit and push this theme, then immediately select the next non-bytecode
  native family.
- 2026-08-01 NATIVE GNUTLS HOST-CATALOG CHECKPOINT: the exact generated GNU C
  inventory is 1,390/1,420 mirrored, with 30 missing.  Four more GnuTLS
  primitives now use the installed library through the already established
  `libloading` boundary rather than hard-coded or locally reimplemented
  tables.  `gnutls-ciphers` and `gnutls-macs` expose the host library's real
  algorithm order, IDs, sizes, AEAD tags, and nonces in GNU's exact plist
  shapes; the nonce-size symbol remains optional to match GNU's older-library
  fallback.  `gnutls-error-fatalp` and `gnutls-error-string` resolve numeric
  `gnutls-code` symbol properties, preserve GNU's validation/error policy, and
  delegate classification and text to that same library.  A direct sibling-GNU
  oracle pins the 44-cipher and 21-MAC catalogs' boundaries and representative
  descriptors plus exact fatality and diagnostic results.  This catalog/error
  work does not claim a TLS transport: `gnutls-available-p` remains nil until
  `gnutls-boot` is real.  Exact fingerprints are mirrored
  `(1_390, 654_403_392_030_036_411)` and missing
  `(30, 8_416_811_313_467_602_167)`.  The 30-item remainder is: `alloc.c` 3,
  `bytecode.c` 2, `comp.c` 9, `module-load` 1, `gnutls.c` 6, display
  connections 2, menus 3, portable dumper 2, `re--describe-compiled` 1, and
  drag-and-drop 1.  The complete publication gate is green: rustfmt, strict
  Clippy, and diff checks; all 1,639 library tests (1,635 in the restricted
  sandbox plus the four exact localhost socket tests with networking allowed);
  28 compatibility-harness tests, 1 performance-harness test, 8 CLI tests, and
  3 ERT-runner tests (plus the zero-test main binary).  SEQUENCING OVERRIDE
  remains authoritative: finish every non-bytecode primitive first; when only
  `byte-code`, `internal-stack-stats`, `make-byte-code`, and `make-closure`
  remain, keep those four visibly unresolved and switch immediately back to
  the ordered 7,080-selector frontier.  Return to the bytecode VM cluster
  afterward, or earlier only if it becomes the concrete blocker.  NEXT:
  commit and push this theme, then immediately select the next non-bytecode
  native family.
- 2026-08-01 NATIVE HEADLESS GUI-ACTION CHECKPOINT: the exact generated GNU C
  inventory is 1,386/1,420 mirrored, with 34 missing.  Four safely probeable
  graphical entry points are now native: `x-create-frame`, `x-show-tip`,
  `x-file-dialog`, and `x-select-font`.  Their dispatcher preserves GNU's
  arities, proper-list, string, and live-frame validation and, critically,
  each primitive's validation-versus-backend-check order before reaching
  Emaxx's honest catchable headless errors.  A direct sibling-GNU oracle pins
  every reachable pre-backend path; the valid headless paths deliberately use
  Emaxx's safe error boundary where GNU can terminate a tty-only batch.
  Display connection management, menu selection, and drag remain explicitly
  unclaimed: probing `x-open-connection` reaches the real host display backend
  and can block, so it was not replaced with a fake headless stub.  Exact
  fingerprints are mirrored `(1_386, 17_559_018_464_045_209_336)` and missing
  `(34, 1_222_455_694_860_570_834)`.  The 34-item remainder is: `alloc.c` 3,
  `bytecode.c` 2, `comp.c` 9, `module-load` 1, `gnutls.c` 10, display
  connections 2, menus 3, portable dumper 2, `re--describe-compiled` 1, and
  drag-and-drop 1.  The complete publication gate is green: rustfmt, strict
  Clippy, and diff checks; all 1,638 library tests (1,634 in the restricted
  sandbox plus the four exact localhost socket tests with networking allowed);
  28 compatibility-harness tests, 1 performance-harness test, 8 CLI tests, and
  3 ERT-runner tests (plus the zero-test main binary).  SEQUENCING OVERRIDE
  remains authoritative: finish every non-bytecode primitive first; when only
  `byte-code`, `internal-stack-stats`, `make-byte-code`, and `make-closure`
  remain, keep those four visibly unresolved and switch immediately back to
  the ordered 7,080-selector frontier.  Return to the bytecode VM cluster
  afterward, or earlier only if it becomes the concrete blocker.  NEXT:
  commit and push this theme, then immediately select the next non-bytecode
  native family.
- 2026-08-01 NATIVE FONT-BACKEND CHECKPOINT: the exact generated GNU C
  inventory is 1,382/1,420 mirrored, with 38 missing; no `font.c` primitive
  remains.  The final eight are `font-face-attributes`,
  `font-shape-gstring`, `font-variation-glyphs`, `open-font`, `close-font`,
  `query-font`, `font-has-char-p`, and `font-get-glyphs`.  Their native
  dispatcher preserves GNU's exact font/entity/object, character, frame,
  glyph-string structure, cached-gstring identity, and validation order.
  Emaxx has no graphical font entities or font objects, so all backend access
  terminates at the honest headless boundary.  GNU 30.2 aborts its whole batch
  process when the frame-first calls or a valid font spec reach a tty-only
  backend; Emaxx deliberately uses its established catchable "Window system
  frame should be used" error instead.  A direct sibling-GNU oracle pins every
  reachable pre-backend path, including the real coding-system glyph-string
  fast path, and an Emaxx-only regression pins the catchable replacement for
  GNU's abort.  Exact fingerprints are mirrored
  `(1_382, 9_974_182_275_177_395_014)` and missing
  `(38, 5_533_127_793_467_509_550)`.  The 38-item remainder is: `alloc.c` 3,
  GUI frame/tip creation 2, `bytecode.c` 2, `comp.c` 9, `module-load` 1,
  `gnutls.c` 10, display connections 2, `x-select-font` 1, menus/dialogs 3,
  portable dumper 2, file dialog 1, `re--describe-compiled` 1, and
  drag-and-drop 1.  The complete publication gate is green: rustfmt, strict
  Clippy, and diff checks; all 1,637 library tests (1,633 in the restricted
  sandbox plus the four exact localhost socket tests with networking allowed);
  28 compatibility-harness tests, 1 performance-harness test, 8 CLI tests, and
  3 ERT-runner tests (plus the zero-test main binary).  SEQUENCING OVERRIDE
  remains authoritative: finish every non-bytecode primitive first; when only
  `byte-code`, `internal-stack-stats`, `make-byte-code`, and `make-closure`
  remain, leave those four explicitly unresolved and switch back to the
  ordered 7,080-selector frontier.  Return to the bytecode VM cluster
  afterward, or earlier only if it becomes the concrete blocker.  NEXT:
  commit and push this theme, then immediately select the next non-bytecode
  native family.
- 2026-08-01 NATIVE GNUTLS PROCESS-STATE CHECKPOINT: the exact generated GNU
  C inventory is 1,374/1,420 mirrored, with 46 missing.  Six GnuTLS control
  and status primitives now share private state on every subprocess, network,
  pipe, and serial process record.  `gnutls-asynchronous-parameters` stores
  the future boot parameters without leaking them through `process-plist`;
  `gnutls-get-initstage` and `gnutls-deinit` expose GNU's initial-stage and
  inactive-session contracts; `gnutls-peer-status` returns GNU's honest
  pre-READY nil; `gnutls-peer-status-warning-describe` implements all 16
  certificate-warning descriptions; and `gnutls-errorp` preserves GNU's
  deliberately broad predicate, including its two non-error exceptions.
  A direct sibling-GNU oracle pins the process-state transitions, private
  plist boundary, every warning string, arbitrary error values, and type
  errors.  This remains a pre-session implementation: no primitive advances a
  process to READY or claims a TLS transport yet.  Exact fingerprints are
  mirrored `(1_374, 3_007_806_732_422_836_430)` and missing
  `(46, 4_602_497_257_345_269_984)`.  The 46-item remainder is: `alloc.c` 3,
  GUI frame/tip creation 2, `bytecode.c` 2, `comp.c` 9, `module-load` 1,
  `font.c` 8, `gnutls.c` 10, display connections 2, `x-select-font` 1,
  menus/dialogs 3, portable dumper 2, file dialog 1,
  `re--describe-compiled` 1, and drag-and-drop 1.  The complete publication
  gate is green: rustfmt, strict Clippy, and diff checks; all 1,636 library
  tests (1,632 in the restricted sandbox plus the four exact localhost socket
  tests with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).  SEQUENCING OVERRIDE remains authoritative: finish
  every non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, leave
  those four explicitly unresolved and switch back to the ordered
  7,080-selector frontier.  Return to the bytecode VM cluster afterward, or
  earlier only if it becomes the concrete blocker.  NEXT: commit and push this
  theme, then immediately select the next non-bytecode native family.
- 2026-08-01 NATIVE GNUTLS DIGEST CHECKPOINT: the exact generated GNU C
  inventory is 1,368/1,420 mirrored, with 52 missing.  `gnutls-digests`
  exposes GNU 30.2's exact stable descriptor order, numeric IDs, and lengths
  for STREEBOG-512, STREEBOG-256, GOSTR341194, MD5, SHA224, SHA512, SHA384,
  SHA256, and SHA1.  `gnutls-hash-digest` accepts GNU's symbol, string,
  descriptor-plist, and numeric selectors plus direct or sliced string/buffer
  input, and returns real unibyte digest bytes with GNU's validation errors.
  The backend reuses the established RustCrypto MD5/SHA implementations and
  pins its digest-0.10-compatible `streebog` 0.10.2 and `gost94` 0.10.4
  crates; it does not implement cryptography locally.  The sibling GNU build
  does have GnuTLS enabled despite an older note that said otherwise, and a
  direct oracle pins every descriptor, algorithm result, selector, slice, and
  error.  This digest surface does not claim a TLS session backend:
  `gnutls-available-p` remains nil until one exists.  Exact fingerprints are
  mirrored `(1_368, 7_429_719_598_662_435_112)` and missing
  `(52, 11_972_645_001_314_988)`.  The 52-item remainder is: `alloc.c` 3,
  GUI frame/tip creation 2, `bytecode.c` 2, `comp.c` 9, `module-load` 1,
  `font.c` 8, `gnutls.c` 16, display connections 2, `x-select-font` 1,
  menus/dialogs 3, portable dumper 2, file dialog 1,
  `re--describe-compiled` 1, and drag-and-drop 1.  The complete publication
  gate is green: rustfmt, strict Clippy, and diff checks; all 1,635 library
  tests (1,631 in the restricted sandbox plus the four exact localhost socket
  tests with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).  SEQUENCING OVERRIDE remains authoritative: finish
  every non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, leave
  those four explicitly unresolved and switch back to the ordered
  7,080-selector frontier.  Return to the bytecode VM cluster afterward, or
  earlier only if it becomes the concrete blocker.  NEXT: commit and push this
  theme, then immediately select the next non-bytecode native family.
- 2026-08-01 NATIVE X-FACES CHECKPOINT: the exact generated GNU C inventory is
  1,366/1,420 mirrored, with 54 missing.  `frame--face-hash-table` now returns
  the selected frame's stable, real `eq` hash table of frame-local face
  vectors; faces created after the table is materialized are synchronized
  into that same table without changing its identity.  The direct GNU oracle
  pins table identity/test, the global-versus-frame-local boundary, shared face
  vector identity, live mutation, and type/frame errors.
  `internal-face-x-get-resource` preserves GNU's string and live-frame
  validation, then uses Emaxx's established catchable headless
  window-system-unavailable boundary; GNU's internal accessor aborts the whole
  batch process when called with otherwise valid strings on a tty-only frame,
  which Emaxx deliberately does not reproduce.  Exact fingerprints are
  mirrored `(1_366, 18_079_045_798_471_271_648)` and missing
  `(54, 5_655_136_854_411_230_528)`.  The 54-item remainder is: `alloc.c` 3,
  GUI frame/tip creation 2, `bytecode.c` 2, `comp.c` 9, `module-load` 1,
  `font.c` 8, `gnutls.c` 18, display connections 2, `x-select-font` 1,
  menus/dialogs 3, portable dumper 2, file dialog 1,
  `re--describe-compiled` 1, and drag-and-drop 1.  The complete publication
  gate is green: rustfmt, strict Clippy, and diff checks; all 1,634 library
  tests (1,630 in the restricted sandbox plus the four exact localhost socket
  tests with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).  SEQUENCING OVERRIDE remains authoritative: finish
  every non-bytecode primitive first; when only `byte-code`,
  `internal-stack-stats`, `make-byte-code`, and `make-closure` remain, leave
  those four explicitly unresolved and switch back to the ordered
  7,080-selector frontier.  Return to the bytecode VM cluster afterward, or
  earlier only if it becomes the concrete blocker.  NEXT: commit and push this
  theme, then immediately select the next non-bytecode native family.
- 2026-08-01 NATIVE SYSTEM-TRASH CHECKPOINT: the exact generated GNU C
  inventory is 1,364/1,420 mirrored, with 56 missing.  The newly native
  `system-move-file-to-trash` expands and validates names through the shared
  file runtime, preserves GNU's structured `file-missing` contract, updates
  file-watch state, and delegates real desktop recycle-bin behavior to the
  mature permissively licensed `trash` 5.2.6 crate.  On macOS it deliberately
  uses the crate's native `NSFileManager` backend rather than its AppleScript
  Finder default, avoiding automation prompts and subprocess policy inside a
  Lisp primitive; Windows and freedesktop.org hosts use the crate's established
  platform backends.  The focused regression exercises the exact GNU
  missing-file result without littering the host Trash; a direct Emaxx smoke
  test also moved a unique empty file through the real macOS Trash, verified
  its destination, restored it, and removed the temporary artifact.  Exact
  fingerprints are mirrored `(1_364, 4_594_607_034_609_466_038)` and missing
  `(56, 3_864_648_990_304_937_516)`.  The 56-item remainder is: `alloc.c` 3,
  GUI frame/tip creation 2, `bytecode.c` 2, `comp.c` 9, `module-load` 1,
  `font.c` 8, `gnutls.c` 18, display connections 2, `x-select-font` 1,
  menus/dialogs 3, portable dumper 2, file dialog 1,
  `re--describe-compiled` 1, `xfaces.c` 2, and drag-and-drop 1.  The complete
  publication gate is green: rustfmt, strict Clippy, and diff checks; all
  1,633 library tests (1,629 in the restricted sandbox plus the four exact
  localhost socket tests with networking allowed); 28 compatibility-harness
  tests, 1 performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus
  the zero-test main binary).  SEQUENCING OVERRIDE: finish every non-bytecode
  primitive first; when only `byte-code`, `internal-stack-stats`,
  `make-byte-code`, and `make-closure` remain, keep those four explicitly
  unresolved and switch back to the ordered 7,080-selector frontier.  Return
  to the bytecode VM cluster after that frontier, or earlier if it becomes the
  concrete blocker.  This later user instruction supersedes older bullets
  below that say not to switch before 1,420/1,420.  NEXT: commit and push this
  theme, then immediately select the next non-bytecode native family.
- 2026-08-01 TREE-SITTER QUERY/TRAVERSAL CHECKPOINT: the exact generated GNU C
  inventory is 1,363/1,420 mirrored, with 57 missing, and the Tree-sitter
  family is complete.  The final ten primitives are
  `treesit-induce-sparse-tree`, `treesit-node-descendant-for-range`,
  `treesit-node-first-child-for-pos`, `treesit-node-match-p`,
  `treesit-pattern-expand`, `treesit-query-capture`, `treesit-query-expand`,
  `treesit-search-forward`, `treesit-search-subtree`, and
  `treesit-subtree-stat`.  They use official `tree-sitter` 0.26.11 `Query`,
  `QueryCursor`, byte-range, descendant, and tree APIs.  Compiled queries cache
  the official query object without extending grammar-library lifetimes
  unsafely; uncompiled queries remain temporary official queries.  GNU sexp
  expansion, string escaping, capture predicates, region filtering, named
  `treesit-thing-settings`, function predicates, both traversal directions,
  sparse-tree processing, and exact subtree statistics are native.  Official
  0.26 requires predicate punctuation, so a narrow same-width spelling bridge
  maps GNU's `#equal`/`#match`/`#pred` forms internally while preserving query
  error byte offsets; do not replace it or the crate with a local query/parser
  engine.  `native_treesit_queries_and_traversal_use_official_runtime` uses
  `tree-sitter-json` to exercise every new primitive, while direct GNU probes
  against the same temporary grammar module matched captures, predicates,
  traversal, sparse trees, and stats.  Pattern/query expansion and control
  character escaping are pinned by a committed GNU oracle assertion.  Exact
  fingerprints are mirrored `(1_363, 787_443_652_193_165_785)` and missing
  `(57, 9_533_698_609_109_745_145)`.  The 57-item remainder is:
  `alloc.c` 3, GUI frame/tip creation 2, `bytecode.c` 2, `comp.c` 9,
  `module-load` 1, `font.c` 8, `gnutls.c` 18, display connections 2,
  `x-select-font` 1, menus/dialogs 3, portable dumper 2, file dialog 1,
  `re--describe-compiled` 1, trash 1, `xfaces.c` 2, and drag-and-drop 1.
  The complete publication gate is green: rustfmt, strict Clippy, and diff
  checks; all 1,632 library tests (1,628 in the restricted sandbox plus the
  four exact localhost socket tests with networking allowed); 28
  compatibility-harness tests, 1 performance-harness test, 8 CLI tests, and
  3 ERT-runner tests (plus the zero-test main binary).  NEXT: commit and push
  this coherent theme, then immediately select and implement the next native
  family; do not return to the 7,080-selector frontier.
- 2026-08-01 TREE-SITTER LOADER/PARSER CHECKPOINT: the exact generated GNU C
  inventory is 1,353/1,420 mirrored, with 67 missing.  Twenty-five newly
  mirrored primitives implement real grammar discovery, parser lifecycle,
  parse trees, and safe node state while continuing to use the official
  `tree-sitter` runtime.  Grammar libraries are searched in GNU's
  `treesit-extra-load-path`, user Tree-sitter directory, then system order;
  the narrow established `libloading` crate owns the unavoidable dynamic
  module boundary and library handles remain alive behind all cloned
  languages/parsers.  Parser reuse, no-reuse, base-buffer lists, deletion,
  accessors, included ranges, notifiers, and edit-triggered reparsing have
  native state.  Node type/start/end/string, child/parent/field/sibling
  traversal, equality, predicates, and outdated generations are backed by
  actual official parse trees without forging `'static` node lifetimes.
  `native_treesit_parser_lifecycle_and_real_json_nodes_use_official_runtime`
  registers the official `tree-sitter-json` grammar as a test-only fixture and
  proves the complete real lifecycle; the GNU-comparison test additionally
  pins unavailable-grammar parser creation.  Only ten Tree-sitter primitives
  remain: `treesit-induce-sparse-tree`,
  `treesit-node-descendant-for-range`, `treesit-node-first-child-for-pos`,
  `treesit-node-match-p`, `treesit-pattern-expand`, `treesit-query-capture`,
  `treesit-query-expand`, `treesit-search-forward`,
  `treesit-search-subtree`, and `treesit-subtree-stat`.  The exact inventory
  fingerprints are mirrored `(1_353, 882_768_658_706_860_184)` and missing
  `(67, 6_580_794_097_960_062_402)`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; all 1,631 library tests (1,627 in
  the restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).  NEXT: commit and push this coherent theme, then
  implement the final ten Tree-sitter query/search operations before moving
  to the next native family.
- 2026-08-01 TREE-SITTER RUNTIME/QUERY CHECKPOINT: the exact generated GNU C
  inventory is 1,328/1,420 mirrored, with 92 missing.  The implementation
  deliberately uses the official MIT-licensed `tree-sitter` Rust crate pinned
  at 0.26.11; do not replace it with a local parser runtime.  Nine formerly
  missing primitives now cover ABI introspection (15 latest, 13 minimum),
  parser/node/query predicates, node/query ownership checks, and lazy
  `treesit-query-compile`.  Lazy compiled queries have stable opaque identity
  and retain their language/source; eager compilation against an unavailable
  grammar signals `treesit-load-language-error`.  Grammar availability and
  language ABI still return the honest no-grammar result.  Changing
  `treesit-available-p` to its truthful non-nil result exposed a loaded GNU
  Semantic mode call to the previously missing query compiler; the real lazy
  query boundary fixes that integration, and
  `loaded_gnu_file_modes_run_semantic_parser_setup` is green both alone and
  in the full matrix.  The GNU-comparison regression
  `native_treesit_runtime_capabilities_and_query_predicates_match_gnu` pins
  ABI, availability detail, all predicates, lazy identity/language, eager
  failure, and type errors.  Thirty-five Tree-sitter primitives remain.  The
  complete publication gate is green: rustfmt, strict Clippy, and diff
  checks; all 1,630 library tests (1,626 in the restricted sandbox plus the
  four exact localhost socket tests with networking allowed); 28
  compatibility-harness tests, 1 performance-harness test, 8 CLI tests, and
  3 ERT-runner tests (plus the zero-test main binary).  NEXT: publish this
  theme, then add grammar loading and real parser/tree/node state on the same
  official runtime.
- 2026-08-01 NATIVE DISPLAY-QUERY CHECKPOINT: the exact generated GNU C
  inventory is now 1,319/1,420 mirrored, with 101 missing.  This theme adds 19
  honest headless display contracts.  `x-display-list` and `x-hide-tip`
  return nil; the 15 optional display/server queries and the two required
  `xw-color-*` queries preserve GNU's arities and signal that the window
  system is unavailable.  Do not claim `x-create-frame`, `x-show-tip`,
  connection management, dialogs, drag-and-drop, or font selection without
  their real stateful backends.  The exhaustive sorted inventory fingerprint
  and `native_x_display_queries_observe_the_headless_backend_boundary` pin the
  addition, with the latter comparing the complete result against GNU.
  Tree-sitter remains unclaimed: use the mature official Rust `tree-sitter`
  crate ecosystem when beginning that theme; never implement a replacement
  parser runtime here.  The complete publication gate is green: rustfmt,
  strict Clippy, and diff checks; all 1,629 library tests (1,625 in the
  restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 8 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).  NEXT: publish this coherent theme, then immediately
  select the next implementable native family and repeat until 1,420/1,420.
- 2026-07-29 GV/BATCH FRONTIER: the current source tree has a fresh
  contiguous 2,332/7,080 green prefix.  `gv-tests.el` (2200..2207) matches
  all eight GNU outcomes in
  `target/compat/run-1785410977097230000-12713` (six passes and the same two
  expected failures).  The repair is shared infrastructure, not test-name
  branching: normalize GNU's supported single-dash long CLI spellings while
  preserving `-b` as `--no-build-details`; merge `--eval` and `--load` by
  original argument index; apply top-level `function-put` declarations such
  as `gv-define-setter` to the byte compiler environment before expanding the
  next form; send batch `message` to stderr and a `t` print stream to stdout;
  and report unhandled Lisp conditions directly with exit 255 and readable
  compound-symbol escaping.  Fast regressions cover CLI spelling/output/
  action order/fatal errors, fresh-interpreter loading of a compiled GV
  setter, and ordinary/compound void-function diagnostics.
  Current-code grouped replays are additionally green for hierarchy 68/68
  (`target/compat/run-1785411070250562000-12998`), icons 2/2
  (`target/compat/run-1785411094145466000-13125`), let-alist 7/7
  (`target/compat/run-1785411113663745000-13246`), lisp-mnt 3/3
  (`target/compat/run-1785411133415310000-13364`), and lisp-mode 21/21
  (`target/compat/run-1785411565689624000-14556`).  `lisp-mode` initially
  exposed a scanner regression because loaded derived modes delegate to the
  native `prog-mode`; GNU's buffer-local `parse-sexp-ignore-comments = t`
  setting is now installed by that parent and pinned by a Rust test.
  `target/compat/run-1785411624225543000-14797` then matched 35/37
  `lisp-tests.el` outcomes.  NEXT is selector 2,333,
  `lisp-forward-sexp-python-triple-quoted-string`, followed by the adjacent
  triple-quotes case; both currently fail at the honest missing boundary
  `void-function syntax-class`.  The complete publication gate is green:
  rustfmt, strict Clippy, and diff checks; 1,628 library tests (1,624 in the
  restricted sandbox plus the four exact localhost socket tests with
  networking allowed); 28 compatibility-harness tests, 1 performance-harness
  test, 8 CLI tests, and 3 ERT-runner tests (plus the zero-test main binary).
- 2026-07-29 NATIVE-AUDIT CHECKPOINT: after publishing the semantic
  checkpoint as `702d1ae`, the priority returned to the host primitive
  frontier.  The exact generated inventory is now 1,300/1,420 mirrored, with
  120 missing.  Six newly claimed primitives form two honest backend
  boundaries: `comp--subr-signature` matches GNU exactly, while
  `comp-native-driver-options-effective-p`,
  `comp-native-compiler-options-effective-p`, and
  `comp-libgccjit-version` return nil consistently with Emaxx's existing
  `native-comp-available-p` result; `dump-emacs-portable--sort-predicate`
  matches GNU's ordinary relocation-offset ordering, and `pdumper-stats`
  returns nil because Emaxx was not restored from a portable dump.  Do not
  claim actual native compilation, `.eln` loading, dump creation, or
  copied-object address sorting: those still lack real backends.  Fast Rust
  regressions
  `native_comp_pure_introspection_family_matches_gnu_and_the_backend_boundary`
  and `portable_dump_pure_introspection_observes_real_runtime_state` are
  green, as is the exhaustive generated native inventory test.  The complete
  publication gate is green: rustfmt, strict Clippy, and diff checks; 1,626
  library tests (1,622 in the restricted sandbox plus the four exact localhost
  socket tests with networking allowed); 28 compatibility-harness tests, 1
  performance-harness test, 5 CLI tests, and 3 ERT-runner tests (plus the
  zero-test main binary).
- 2026-07-29 ERT/COMPLETION FRONTIER: the current source tree has a fresh
  contiguous 2,199/7,080 green prefix.  `ert-font-lock-tests.el` matches all
  40 GNU outcomes in `target/compat/run-1785290189192211000-29635`; subsequent
  green grouped artifacts are ERT-X 28/28
  (`target/compat/run-1785290232431202000-29821`), Faceup 15/15 and 1/1
  (`target/compat/run-1785290260618644000-29994` and
  `target/compat/run-1785290274721823000-30198`), Find Function 6/6
  (`target/compat/run-1785292073893687000-32147`), Float-Sup 1/1
  (`target/compat/run-1785292201770742000-32661`), and Generator 92/92
  (`target/compat/run-1785292226029589000-32836`).  NEXT is selector 2,200,
  `gv-define-expander-in-file`, in `test/lisp/emacs-lisp/gv-tests.el`.  The
  grouped artifact `target/compat/run-1785292257313614000-33008` has four
  mismatching outcomes whose subprocesses all pass GNU's `-b` batch shorthand;
  Emaxx's CLI currently rejects that option before GV logic runs.
  The thematic repairs in the pending checkpoint are: install GNU's dynamic
  `ert--pass` catch while the native runner invokes each test body; restore the
  preloaded Lisp `find-tag-default*` family; route simulated minibuffer TAB
  through the loaded Lisp completion-style engine so programmed completion
  bases survive; and make native `file-name-all-completions` signal
  `file-missing` before inventing `./` or `../` for an unopened directory.
  The clean-checkout gate now copies generated `etc/DOC` and includes it in
  the test-support fingerprint, fixing native-source lookup without allowing
  stale or developer-tree state.  The fast in-process upstream helper sets
  GNU's source/data/doc directories explicitly.  Rust regressions are
  `ert_font_lock_success_paths_share_the_runners_pass_catch`,
  `file_name_completion_rejects_a_missing_directory_before_adding_dot_entries`,
  `find_function_suite_uses_preloaded_tag_helpers_and_the_upstream_doc_index`,
  and the expanded
  `isolated_test_checkout_excludes_ignored_state_and_restores_between_files`
  harness test.  Final gates are green: rustfmt, strict Clippy, diff check,
  1,624 library tests, 28 compatibility-harness tests, 1
  performance-harness test, 5 CLI tests, and 3 ERT-runner tests, plus the
  zero-test main binary.  The preceding EIEIO checkpoint is committed and
  pushed as `2c6bec1`.  Per the current priority, publish this coherent
  checkpoint and then resume the 126-missing native primitive inventory
  before fixing the next GV oracle mismatch.
- 2026-07-28 EDEBUG FRONTIER: the current source tree has a fresh contiguous
  1,957/7,080 green prefix.  The prefix through `easy-mmode-tests.el` was
  replayed through 1,911, and all 46 selected outcomes in
  `test/lisp/emacs-lisp/edebug-tests.el` match GNU in the final
  source-fingerprinted artifact
  `target/compat/run-1785255202704658000-17314` (45 passes plus the same
  expected `edebug-tests-step-into-macro-error` failure).  NEXT is the eight
  selected tests in
  `test/lisp/emacs-lisp/eieio-tests/eieio-test-methodinvoke.el`.  The older
  2,879 historical frontier remains useful evidence but is not a substitute
  for this current-code re-sweep.
  This batch repaired shared command-loop and lifetime contracts rather than
  Edebug test names.  Recursive edits now use GNU's error-report/resume
  command-loop behavior, keyboard-macro minibuffer events normalize modifier
  bits, and every waiting loop pumps both the bootstrap native timer queue and
  GNU `timer.el`'s `timer-list`.  Whole-file source loads and `eval-buffer`
  now replace same-file `load-history` entries like GNU
  `build_load_history`; the Rust-backed `cl-defgeneric` facade uses stable
  qualifier wrappers, exact method replacement, and a scoped
  `loadhist-unload-element` adapter so repeated loads and `unload-feature`
  cannot leave stale primary/before/after closures or metadata.  Do not
  broadly preload `cl-generic.el`: that creates a second dispatch engine and
  exposes its unrelated method-combination bootstrap requirements.
  Fast Rust regressions cover the six order-sensitive Edebug cases plus
  post-unload state, recursive-edit Elisp timers/nonlocal exits, and repeated
  whole-file generic load/reload/unload.  The release gate also exposed a
  pre-existing mutex bug: recursive ownership restoration had been performed
  inside `debug_assert!`, so optimized builds omitted the state transition.
  Restoration is now unconditional, the existing GNU-comparison regression
  passes in release, and a repository-wide assertion scan found no other
  stateful debug assertions.  Final gates are green: rustfmt, strict Clippy,
  diff check, 1,614 sandbox-compatible library tests plus the four exact
  localhost tests with socket permission (1,618/1,618 semantic passes), 28
  compatibility-harness tests, 1 performance-harness test, 5 CLI tests, and
  3 ERT-runner tests (plus the zero-test binary target).
- 2026-07-28 POST-NATIVE-AUDIT ORACLE REVALIDATION: the current source tree
  has been replayed from the start of `compat/oracle_tests_all.txt` through
  the 13 selected `cl-seq-tests.el` cases, giving a fresh contiguous
  1,807/7,080 green prefix.  The next manifest entry is the 93 selected tests
  in `test/lisp/emacs-lisp/comp-cstr-tests.el`.  This is a fresh validation
  checkpoint, not a replacement for the older 2,879-test historical frontier;
  retain that historical result until the present re-sweep reaches it.
  Five thematic repairs made this prefix reliable and fast enough to replay:
  one per-source-tree index now resolves GNU preloaded Lisp ownership instead
  of rescanning every preload for every missing native name; `equal` hash
  tables now use `sxhash`-partitioned structural buckets while preserving
  conservative buckets for cyclic or representation-polymorphic keys;
  ordinary `eq`/`memq` symbol comparisons bypass the symbol-with-position
  dynamic policy unless an operand can actually be such a record; GNU's
  Lisp-owned interactive `undo` policy is again preloaded above the native
  undo-list machinery; and native `macroexpand-all` now scopes
  `macroexp--dynvars` like GNU, including sequential `defvar` declarations
  and private nested function scopes.  The last repair fixes
  `cl-macs--labels` at the macroexpansion abstraction rather than special
  casing the test.  Fast Rust regressions cover preload index reuse and
  ordering, 5,000 structured hash keys plus numeric representation equality,
  524,288 ordinary `memq` comparisons with zero position-policy reads, the
  real preloaded `undo` command boundary, and the combined upstream
  `cl-macs--labels` / `cl-macs--progv` / symbol-macrolet selectors.
  Electric is 874/874 and the full `cl-macs` file is 61/61 in the exact
  replay.  Rustfmt and strict Clippy are green.  The publication library gate
  passed 1,611 non-network tests in the restricted sandbox; its only four
  failures were localhost binds rejected with `Operation not permitted`, and
  all four exact fully-qualified socket regressions passed immediately with
  local networking allowed, establishing 1,615/1,615 semantic passes.  The
  remaining publication targets passed 28 compatibility-harness tests, 1
  performance-harness test, 5 CLI tests, and 3 ERT-runner tests (plus the
  zero-test binary target).  The source-fingerprinted isolated compatibility
  target remains mandatory, so a gate cannot silently exercise a stale Emaxx
  binary.
- 2026-07-28 NATIVE-AUDIT CHECKPOINT: `frame.c` is complete, advancing
  the exact inventory to 1,294 mirrored / 126 missing.  Emaxx previously
  represented both the selected frame and terminal as ordinary interned
  symbols.  They are now distinct opaque host values (`frame` and `terminal`
  at the Lisp boundary), backed by typed interpreter state and coherent
  frame/terminal dispatch modules.  All 35 formerly missing `frame.c`
  primitives now share that state with the already claimed frame family, and
  the eight `terminal.c` primitives use the same terminal identity and
  liveness.  The frame model distinguishes high-level width/height parameters,
  native total geometry, text geometry, root-window geometry, and the
  minibuffer line as GNU does on the initial TTY.  Direct GNU probes also
  exposed and fixed the adjacent rule that `set-window-configuration` records
  frame dimensions for equality but does not rewind a later frame resize.
  Five fast Rust/GNU regressions cover opaque identity, traversal, parameters,
  native resizing, focus/mouse/headless errors, terminal identity, and window
  configuration restore; the existing terminal family tests cover deletion,
  hooks, parameters, and TTY controls.  The first broad library run correctly
  caught one stale Todo/window assertion that equated the 24-line root window
  with its 25-line frame; a direct GNU probe established `(t nil t nil)` for
  the width/height window/frame comparisons, the assertion was corrected, and
  the complete suite then passed in order.  Rustfmt, strict Clippy, and
  `git diff --check` are green.  The publication gate passed 1,610 library
  tests, 28 compatibility-harness tests, 1 performance-harness test, 5 CLI
  tests, and 3 ERT-runner tests (plus the zero-test binary target).  The exact
  1..N/7080 replay remains pending; do not report this native inventory
  checkpoint as 7080 frontier progress.
- 2026-07-28 NATIVE-AUDIT CHECKPOINT: the pure/headless `font.c`
  core and all of `fontset.c` are complete, advancing the exact inventory to
  1,259 mirrored / 161 missing.  The former `font-spec` record stored only
  `:name` and reparsed a handful of computed fields; it could not preserve
  arbitrary properties or GNU's mutable normalized property contract.  One
  13-slot native font record now owns fixed and extra properties, ordered
  name parsing, validation, `font-put` mutation, matching, XLFD generation,
  and headless lookup/cache behavior.  One interpreter-owned fontset registry
  now owns the default/custom fontsets, character/range/script/fallback
  mappings, prepend/append precedence, recreation semantics, ASCII
  protection, and headless query errors.  Four fast Rust oracle regressions
  compare all 19 claimed `font.c`/`fontset.c` names (including the already
  native `font-spec`, `font-get`, and `fontp`) with GNU and cover state,
  validation, exact errors, window/buffer constraints, and ordering.
  Eight `font.c` names remain deliberately missing because they require real
  font entities/objects or a GUI font driver: `open-font`, `close-font`,
  `query-font`, `font-face-attributes`, `font-has-char-p`,
  `font-get-glyphs`, `font-shape-gstring`, and `font-variation-glyphs`.
  Do not replace them with nil stubs.  Font/face-targeted suites, rustfmt,
  strict Clippy, and `git diff --check` are green.  The complete publication
  gate is green: `cargo test --all-targets --all-features` passed 1,605
  library tests, 28 compatibility-harness tests, 1 performance-harness test,
  5 CLI tests, and 3 ERT-runner tests (plus the zero-test binary target).
  The exact 1..N/7080 replay remains pending.
- 2026-07-28 NATIVE-AUDIT CHECKPOINT: the headless semantic core of
  `xfaces.c` is now complete, advancing the exact inventory to 1,243 mirrored
  / 177 missing.  The old model split face attributes across synthetic symbol
  properties and a separate inheritance list; it could not represent GNU's
  fundamental contract that a Lisp face is one mutable 20-slot vector.  A
  new interpreter-owned registry is authoritative for global and selected-
  frame vectors, identity-preserving creation/copy/mutation, inheritance,
  equality/emptiness, relative-height merging, resource-value conversion,
  font-selection state, bitmap/color queries, and color-file parsing.
  Existing `defface`, theme, `face-attribute`, and `set-face-attribute` paths
  now use that same registry rather than a second cache.  The family-level
  fast Rust regression compares every one of the 25 claimed primitives
  directly with GNU, including external `aset` mutation of the returned
  vector and exact error families.  Two `xfaces.c` names remain deliberately
  missing: `frame--face-hash-table` depends on the proper multi-frame registry
  already identified by the `frame.c` audit, and
  `internal-face-x-get-resource` needs a real GUI resource backend (GNU's
  batch terminal aborts when it is called directly).  Do not replace either
  with a nil stub.  Face-targeted tests, rustfmt, strict Clippy, and
  `git diff --check` are green.  The complete publication gate is green:
  `cargo test --all-targets --all-features` passed 1,601 library tests, 28
  compatibility-harness tests, 1 performance-harness test, 5 CLI tests, and
  3 ERT-runner tests (plus the zero-test binary target).  The first sandboxed
  run passed all non-network tests but could not open localhost sockets; the
  exact same gate passed with the required socket permission.  The exact
  1..N/7080 replay remains pending.
- POST-`26e68a4` 2026-07-27 NATIVE-AUDIT PROGRESS: `doc.c` is now complete,
  advancing the exact inventory to 1,218 mirrored / 202 missing.  The former
  partial documentation facade rejected GNU's optional `RAW` argument and
  returned unresolved DOC offsets from `documentation-property`.  One shared
  native DOC index now parses `F`/`V`/`S` records, installs positive function
  and signed user-variable offsets, resolves static and lazy references,
  handles variable aliases and stale offset zero, and backs
  `Snarf-documentation`, `internal-subr-documentation`, `documentation`, and
  `documentation-property`.  The same audit exposed a producer contract in
  dumped key bindings: the initial global map now owns GNU's canonical `C-f`
  `forward-char` binding, and `substitute-command-keys` preserves
  `help-key-binding` text properties on rendered keys.  A synthetic-DOC Rust
  oracle regression covers offsets, raw/substituted strings, text properties,
  aliases, stale references, and exact errors.  The complete publication gate
  is green: `cargo test --all-targets --all-features` passed 1,600 library
  tests, 28 compatibility-harness tests, 1 performance-harness test, 5 CLI
  tests, and 3 ERT-runner tests (plus the zero-test binary target).  The exact
  1..N/7080 replay remains pending.
- POST-`4bb5a43` 2026-07-27 NATIVE-AUDIT PROGRESS: `composite.c` is now
  complete, advancing the exact inventory to 1,216 mirrored / 204 missing.
  The former `find-composition-internal` was a superficial Unicode-grapheme
  approximation that ignored GNU's `composition` text property.  A dedicated
  native composition module now owns static buffer/string composition,
  registration and detail output, headless terminal glyph strings, automatic
  combining clusters, rule sorting, and cache reset.  The shared text-property
  interval layer now uses GNU `eq` identity—not structural equality—both when
  merging adjacent intervals and when scanning single-property changes; this
  keeps separately allocated but equal composition descriptors distinct.  A
  fast family-level GNU oracle regression covers all six `composite.c`
  primitives, every composition method, reverse buffer bounds, search
  direction and limit clamping, terminal glyph metrics, exact errors, and the
  shared interval-identity invariant.  The complete publication gate is
  green: `cargo test --all-targets --all-features` passed 1,599 library
  tests, 28 compatibility-harness tests, 1 performance-harness test, 5 CLI
  tests, and 3 ERT-runner tests (plus the zero-test binary target).  The exact
  1..N/7080 replay is still pending.
- POST-`b3c90e2` 2026-07-27 NATIVE-AUDIT PROGRESS: `fringe.c` is now complete,
  advancing the exact inventory to 1,211 mirrored / 209 missing.  The former
  `define-fringe-bitmap` implementation was an unconditional nil stub and an
  older fast test incorrectly encoded that stub as expected behavior.  Rust
  now owns the standard/user bitmap registry, replacement and destruction,
  face overrides, exact C validation, and headless glyph-matrix query
  contract.  Native bitmap data stays in interpreter state; only GNU's real
  `fringe-bitmaps` variable and `fringe` symbol property cross into Lisp.
  The family-level Rust regression compares registry mutations, errors, and
  row queries directly with GNU.  The complete publication gate is green:
  `cargo test --all-targets --all-features` passed 1,598 library tests, 28
  compatibility-harness tests, 1 performance-harness test, 5 CLI tests, and 3
  ERT-runner tests (plus the zero-test binary target).  The exact 1..N/7080
  oracle gate remains pending; do not confuse this native-audit checkpoint
  with 7080 frontier progress.
- `menu.c` was audited but deliberately remains missing: GNU's own
  `menu-bar-menu-at-x-y` aborts the initial batch oracle frame (exit 134), so
  a headless nil shim cannot be established as a compatibility contract.
  Do not claim this family until it can be tested on a suitable live terminal
  or graphical-frame harness.
- POST-CHECKPOINT 2026-07-27 NATIVE-AUDIT PROGRESS: the complete `indent.c`
  and remaining `xdisp.c` families are now mirrored, moving the exact
  inventory from 1,197/223 to 1,208 mirrored / 212 missing.  The shared
  display-motion layer now owns `compute-motion`, line-number display width,
  continuation queries, bidi paragraph direction and visual point motion,
  headless bar/pixel queries, and image-map geometry.  Direct Rust tests
  compare family-level results with GNU, including continuation boundaries,
  tabs/control/display/invisible text, blank-line bidi paragraph boundaries,
  RTL motion, and rectangle/circle/polygon image maps.
- The builtin-metadata generator no longer scrapes arbitrary string literals
  from Rust dispatcher files.  It consumes the generated GNU C-source
  manifest, so condition names and Lisp helper names cannot cross the
  Lisp/native ownership boundary merely by being mentioned in Rust.  A fast
  exhaustive test checks arity and command identity for every known-arity
  GNU C primitive and proves dumped-Lisp `beginning-of-buffer` /
  `end-of-buffer` remain absent from native metadata.
- The full fast gate for this follow-up is green:
  `cargo test --all-targets --all-features` passed 1,597 library tests, 28
  compatibility-harness tests, 1 performance-harness test, 5 CLI tests, and 3
  ERT-runner tests (plus the zero-test binary target).  The exact 1..N/7080
  oracle gate still has not been rerun; do not report the native inventory as
  7080 frontier progress.
- AUTHORITATIVE 2026-07-27 NATIVE-AUDIT CHECKPOINT: the generated GNU 30.2
  source/runtime manifest contains 1,685 source-level `DEFUN`s, 1,420 of which
  are available in the configured oracle build.  Emaxx now has exact native
  Rust surface contracts for 1,197 of those and 223 remain missing.  The fast
  inventory test pins both sorted name sets by count and fingerprint and
  verifies arity, command/special-form metadata, and an actual Rust dispatch
  route for every claimed mirror.  This is an inventory, not a claim that
  every mirrored primitive's deep semantics are complete.
- This checkpoint completed coherent native families rather than adding
  selector-specific shims: libxml-backed XML/HTML parsing, the remaining
  `window.c` surface, `kill-emacs` termination/restart semantics, `data.c`
  including exhaustive module-free `user-ptrp`, `terminal.c`, and `dispnew.c`.
  A full-file Eshell regression also exposed reentrant process-sentinel
  notification: GNU claims a terminal status before calling the sentinel, so a
  sentinel may delete its own process without recursively notifying itself.
  Emaxx now makes the same state transition, with a direct GNU-oracle Rust
  regression.
- The final fast gate for this checkpoint is
  `cargo test --all-targets --all-features`: 1,594 library tests, 28
  compatibility-harness tests, 1 performance-harness test, 5 CLI tests, and 3
  ERT-runner tests all passed (plus the zero-test binary target).  It was run
  with localhost socket permission because four real process/network tests
  cannot run in a socket-denying sandbox.  Rustfmt, `git diff --check`, and
  Clippy with `-D warnings` also passed.  Scheduler-sensitive debug tests use
  generous outer test deadlines; production wait semantics were not changed.
  The exact 1..N/7080 oracle gate has NOT been rerun after this native-audit
  batch, so the compatibility frontier below remains the last measured one.
  After committing this checkpoint, continue the native family audit (good
  next candidates are the remaining `indent.c` primitives) or begin the fresh
  exact oracle replay; do not infer 7080 progress from the native inventory.
- AUTHORITATIVE 2026-07-25 FRONTIER: Delimited Columns is 9/9 in
  `target/compat/run-1784977186186812000-88703`.  Dabbrev is 16/16 immediately
  before it in `target/compat/run-1784976862207828000-88306`, Custom is 9/9 in
  `target/compat/run-1784973964022350000-85646`, and Completion Preview is
  11/11 in `target/compat/run-1784973333950428000-84528`.  NEXT is
  `test/lisp/descr-text-tests.el` (three selected tests).
- The newest thematic contracts are native frame/window and command-loop
  behavior: real Rust `modify-frame-parameters`, selected/old-selected-window
  state, use-time ordering, and GNU's deliberately narrow
  `execute-kbd-macro` catch set.  A customized `command-error-function` does
  not catch ordinary `user-error`; only `minibuffer-quit` is handled at that
  loop boundary.  Direct fast Rust coverage and the killed-buffer/cross-buffer
  Dabbrev selectors prevent regression.  Higher Custom and Dabbrev policy
  stays in upstream Lisp.
- Completion Preview exposed the parallel dumped-Lisp inventory theme:
  `forward-whitespace`, `forward-symbol`, and `forward-same-syntax` are
  Lisp-owned GNU `subr.el` preloads, so Emaxx now preloads them in Lisp and
  covers all 11 upstream Completion Preview tests in the fast suite.
- The latest thematic repair is the native `window.c` boundary.  Rust now
  supplies the dumped variable family, batch geometry, resize/new-size state,
  scroll hooks, fringes/scrollbars/cursor/display-table contracts, and actual
  split/delete topology.  Window records initialize every native slot when
  constructed, so later access cannot retroactively fill earlier state with
  nil.  Internal windows are valid but not live and have no buffer, matching
  GNU; this was the shared contract behind the final Todo failure.  Higher
  window policy remains in real preloaded `window.el`.  Keep the focused Rust
  assertions for defaults, topology, deletion, hooks, and Todo itself.
- The first whole-native-surface audit is an inventory, not a bug count.
  Its superseding reproducible snapshot is the 2026-07-27 generated manifest
  above: 1,420 host-available GNU C primitives, 1,197 exact native Emaxx
  surfaces, and 223 missing.  Do not silently compare it with a bare
  `emacs -Q --batch` `subr-primitive-p` enumeration; regenerate from GNU C
  `DEFUN` declarations plus the configured oracle runtime contract.  The
  earlier variable audit found 684 active variables declared from GNU C, of
  which Emaxx left 338 unbound; that variable inventory has not yet received
  the same generated checkpoint treatment.
  Some are intentionally irrelevant GUI/platform/compiler surfaces, and a
  binding does not establish correct semantics.  Use the list to audit one
  GNU source family at a time: read GNU C for the observable contract, keep
  native C behavior native in idiomatic Rust, keep Lisp policy in Lisp, add
  table-driven fast Rust probes, then use the oracle for ambiguous/deep
  behavior.  Do not bulk-add no-op stubs.  The window family demonstrates the
  payoff: its missing function surface fell from 45/117 to 19 while repairing
  a cluster rather than one selector.
- 2026-07-24 trustworthy cumulative baseline:
  `target/compat/run-1784899759213881000-79075` is the clean isolated
  canonical prefix through `test/lisp/files-x-tests.el`: 173/225 files match
  exactly and 52 mismatch.  The newer clean canonical prefix through
  `test/lisp/calendar/icalendar-tests.el` is 22/22 in
  `target/compat/run-1784938521267523000-23325`.  It folds Emacsclient,
  Archive, Auto-Revert, Bookmark, and iCalendar into ordered execution with no
  early regression; iCalendar is 41/41 in that artifact, focused Auto-Revert is
  7/7 in
  `target/compat/run-1784907269175094000-12521`, Bookmark is 47/47 in
  `target/compat/run-1784922448544839000-15902`, and Archive passes under the
  corrected isolated-resource gate in
  `target/compat/run-1784931683709926000-16475`.  A new 225-file replay is
  still pending.  NEXT is `test/lisp/calendar/iso8601-tests.el`.  Do not
  quote the older 3554/7080 prefix as a fresh gate: the VM-only `.elc.gz`
  selector remains deliberately tabled.
- The compatibility harness now makes stale/cross-run test state impossible
  by construction.  Every run creates separate `git clone --shared`
  checkouts for GNU and Emaxx, pins both to the configured GNU commit, and
  runs `git clean -ffdqx` before each test file.  It restores only an explicit
  fingerprinted test-support allowlist (generated ignored Lisp plus named
  `lib-src` helpers); it never copies `src/emacs`.  GNU load-path directory
  order is remapped into the isolated Emaxx checkout.  The support fingerprint
  is recorded and rechecked in provenance, and 28 harness unit tests cover
  ignored-output exclusion, per-file restoration, support mutation detection,
  load-path ordering, and overriding the oracle executable's dumped
  `source-directory` before each test.  The latter prevents `test/data`
  fixture paths from escaping into the original sibling checkout.  Never
  weaken this isolation or run a cumulative gate directly against the
  writable sibling checkout.
- Auto-Revert's timer and notification machinery was healthy; the shared file
  edit boundary was wrong.  `insert-file-contents` with REPLACE no longer asks
  a second supersession question, and its atomic delete+insert dynamically
  binds `buffer-file-name` nil as GNU fileio.c specifies.  Bookmark then
  exposed the full C-boundary contract for
  `find-coding-systems-region-internal`: string or `(START END)` input, ASCII's
  `t` sentinel, and EXCLUDE filtering.  The complete real/effective UID/GID
  family is present, using direct POSIX identity calls on Unix.  Fast Rust
  regressions cover all of these contracts.
- iCalendar's seven two-hour shifts were one host-value abstraction leak:
  GNU's preloaded Lisp `setenv` passes a mutable/property-capable string to
  `set-time-zone-rule`, but Emaxx local civil-time encoding matched only its
  compact string representation and silently fell back to wall time.  Both
  local encode/decode paths now parse POSIX timezone rules through the common
  string accessor.  The fast Rust regression uses the real upstream preload
  stack and covers winter, DST, and explicit UTC.
- The newest thematic repairs are at shared host boundaries.  Empty
  `emaxx --batch` now exits successfully like GNU, which lets the real
  `lib-src/emacsclient` fixture launch Emaxx as `ALTERNATE_EDITOR`.
  `discard-input` implements GNU keyboard.c's batch-visible contract by
  clearing pending command events and ending keyboard-macro definition.
  Compressed file decoding presents the suffix-stripped payload name to
  auto-coding policy, matching `jka-compr-byte-compiler-base-file-name`.
  Most importantly, the `undecided` coding system now performs actual byte
  detection, records the precise detected coding, preserves unibyte
  destination bytes, and `encode/decode-coding-region` return GNU-compatible
  lengths.  Fast Rust regressions cover every one of these behaviors,
  including the exact UTF-8 snowflake path that repaired both nested Archive
  extraction failures.
- ACTIVE UNCOMMITTED THEMATIC FILE/PROCESS BATCH.  The strict canonical
  prefix is now 3554/7080: the four automated `filenotify-tests.el' selectors
  pass together in `target/compat/run-1784640263759277000-3945`, and the first
  `files-tests.el' selector passes.  The second files selector,
  `files-load-elc-gz-file`, remains the explicitly tabled GNU bytecode-VM gap;
  do not fake it by forwarding to GNU or decoding `.elc' as source.  Beyond
  that blocked strict-prefix point, the complete 116-test files replay is
  115/116 in `target/compat/run-1784639865227053000-3224`; bytecode is its only
  mismatch.  NEXT non-VM file is `test/lisp/files-x-tests.el' (selectors
  3670..3676).  No final full Rust gate, commit, or push has happened yet.
- The final files repairs are thematic host/preload contracts.  GNU files.el
  now owns high-level save/revert policy through lazy dumped autoloads, while
  native Rust implements lower-level file I/O and the newly added C-boundary
  `replace-buffer-contents'.  That primitive uses a bounded LCS edit plan so
  matched text, point/markers, properties, overlays, and narrowing survive;
  `revert-buffer-function' carries GNU's public `revert-buffer--default'
  spelling, with native behavior only as a file-less fallback.  VISIT writes
  mark buffers saved, read-file-name honors its dynamic provider, files.el's
  dumped save variables exist before first assignment, and missing-file visits
  finish visiting state before signaling.  File-handler-visible names stay at
  the Lisp boundary while unquoted host paths are used only for I/O.
- Bug#18141 exposed two shared native manifests rather than an ISO fixture
  special case.  GNU buffer.c's complete native permanent-local table is
  `truncate-lines' plus `buffer-file-coding-system'; both now survive major
  mode resets.  GNU 30.2 was probed directly and confirmed that all 18 ordinary
  built-in text coding bases expose Unix/DOS/Mac variants, so bootstrap now
  constructs the complete family centrally instead of maintaining a partial
  hand-written list.  Fast Rust regressions cover native permanence, the full
  coding registry, compressed visit/save, `replace-buffer-contents', and
  fine-grained revert end to end.  The generated root files ` *temp*',
  ` *temp*~', ` *temp*<2>', and ` *temp*<2>~' are stale artifacts from the
  formerly ignored read-file-name provider and must be removed before commit.
- ACTIVE UNCOMMITTED THEMATIC REGRESSION BATCH AFTER 3125 IS READY TO COMMIT.
  The final cumulative artifact is
  `target/compat/run-1784589326956761000-16406` (201 files through
  `test/lisp/eshell/em-glob-tests.el`: 169 exact file matches and 32 known
  historical/oracle-environment mismatches).  Its Emaxx-only comparison
  against the pre-fix artifact
  `target/compat/run-1784558452909597000-74441` reports ZERO pass-to-fail,
  six fail-to-pass, no changed failures, and complete result coverage.  The
  repaired tests are `cl-macs--progv`, `cl-macs-test--symbol-macrolet`,
  `core-elisp-tests-3-backquote`, `test-map-into`,
  `test-map-merge-empty`, and
  `test-with-buffer-unmodified-if-unchanged`.  The comparison correctly notes
  different harness hashes because this batch also fixed artifact provenance;
  do not pretend the artifacts are provenance-identical.
- The six final repairs are shared contracts, not selector patches.  Nil-env
  `(eval FORM)` now gives directly evaluated forms dynamic binding while
  lexical function boundaries retain their definition-time semantics;
  symbolic `gv-expander` declarations install the named function; nested
  backquotes track quotation depth; core and cl-generic condition ancestry is
  available before Lisp preloads; and GNU's preloaded `with-temp-buffer`
  remains publicly visible as a macro rather than only an evaluator shortcut.
  Macro bindings now store callable expander closures instead of raw
  `(params, body)` tuples, so `cl-macrolet` expander arguments remain lexical
  even inside dynamic eval.  This last abstraction repair removed the one
  transient self-introduced regression, `edebug-tests-cl-macrolet`; the full
  Edebug oracle file passes in
  `target/compat/run-1784588888832318000-14442` and in cumulative order.
  The rejected broad approach was to let dynamic eval mode govern the body of
  a separately invoked macro expander; GNU probes prove the eval mode governs
  FORM, not the expander function's lexical parameter scope.
- The harness now writes synthesized load-failure reports to their advertised
  JSON paths, so completed artifact coverage cannot silently omit failed
  runners.  Subject builds remain source-owned, synchronously rebuilt, locked
  against concurrent gates, and checked by source/binary/harness hashes; a
  gate cannot test stale Emaxx code.  The final fast gate passes 1297 library
  tests, 25 harness tests, one perf test, and three integration tests.  Clippy
  passes all targets with `-D warnings`; rustfmt and `git diff --check` are
  clean.  Fast regressions include the exact upstream files plus direct
  semantic assertions for dynamic eval, condition ancestry, nested
  backquote, the public `with-temp-buffer` macro surface, synthesized report
  persistence, and lexical `cl-macrolet` expansion.  NEXT remains selector
  3126, `em-hist-test/add-to-history/allow-dups` in
  `test/lisp/eshell/em-hist-tests.el`, after committing this batch.
- Verified through selector 3125/7080.  All 27 selected tests in
  `test/lisp/eshell/em-glob-tests.el' pass the grouped GNU oracle replay.
  Selector 3125's remote `~/file.txt' case exposed a shared mock-Tramp
  boundary, not an Eshell glob defect.  GNU initially reports `~/file.txt' as
  the localname, then resolves it to the remote home after the test's
  accessibility probe establishes the mock connection; `eshell-glob-convert'
  consequently sees an absolute literal path and returns the original remote
  string even when unmatched globs are errors.  Emaxx's native mock transport
  stripped the remote prefix but left `~' unresolved, so Eshell misread it as
  its exclusion glob operator.  Mock remote localnames now pass through the
  existing host-home resolver consistently for `file-local-name',
  `file-remote-p' localname queries, and native file operations.  Ordinary
  remote methods retain their parsed localname.  This keeps GNU's Eshell in
  Elisp and repairs the existing mock file boundary in Rust.  Fast Rust
  regressions cover the primitive localname/file-operation contract and the
  upstream Eshell scenario end to end.  Full gate: 1208 library tests, 11
  compatibility-harness tests, one perf-harness test, and three integration
  ERT runners pass; rustfmt, clippy with `-D warnings', and `git diff --check'
  are clean.  NEXT = selector 3126,
  `em-hist-test/add-to-history/allow-dups' in
  `test/lisp/eshell/em-hist-tests.el'.
- Verified through selector 3098/7080.  All 17 selected tests in
  `test/lisp/eshell/em-extpipe-tests.el' pass the grouped GNU oracle replay.
  Selector 3082 first exposed missing dumped `process.c' state and accessors,
  but the decisive failure was deeper: Emaxx's `unwind-protect' discarded
  errors and nonlocal exits from cleanup forms.  Eshell intentionally rethrows
  `eshell-defer' from a cleanup; losing that throw cleared the foreground
  command while its fast child was still running.  Cleanup exits now supersede
  the protected result and stop later cleanup forms, matching GNU.  Process
  completion is event-loop-driven: `process-live-p' no longer reaps a fast
  child ahead of output delivery; the pump drains stdout/stderr, closes a
  linked stderr pipe before the primary process, and invokes each terminal
  sentinel once.  `make-process' now retains `:sentinel' and `:stderr';
  `process-command', `process-exit-status', pipe-backed `process-tty-name',
  GNU's dumped process variables, and the default `utf-8-unix' coding pair are
  present.  Selector 3091 (`em-extpipe-test-2' in manifest order) then exposed
  a shared regexp bug: forward search honored `\\=' only when it began the
  pattern, although GNU asserts the original search point wherever `\\='
  occurs.  Nested point assertions now use the search point as the delegate
  haystack origin, so Eshell correctly separates an ordinary `|' before a
  later `*>'.  No Eshell policy moved into Rust: GNU's Eshell remains Elisp;
  Rust implements the existing `process.c', search, and evaluator boundaries.
  Fast Rust regressions cover cleanup precedence, nested `\\=' match data,
  dumped process state/accessors, event-driven exit and exact stderr/primary
  sentinel order, both external-pipeline shapes, and immediate redirected file
  contents.  Full gate: 1206 library tests, 11 compatibility-harness tests, one
  perf-harness test, and three integration ERT runners pass.  NEXT = selector
  3099, `em-glob-test/convert/absolute-start-directory' in
  `test/lisp/eshell/em-glob-tests.el'.
- Verified through selector 3081/7080.  All 11 selected tests in
  `test/lisp/eshell/em-dirs-tests.el' pass both the grouped oracle replay and
  the fast native ERT runner.  Selector 3072 exposed a shared representation
  bug: GNU `directory-files' creates ordinary mutable Lisp strings, and
  Eshell immediately adds display properties to those names; Emaxx returned
  immutable `Value::String' values.  `directory_files' now creates shared,
  property-bearing strings with the correct multibyte flag.  The tempting
  symptom fix--making `add-text-properties' silently accept immutable
  strings--was rejected because it would discard observable GNU properties.
  The native full-file replay then exposed two startup contracts that the
  heavy harness's explicit load of `ert.el' had masked: GNU dumps `seq.el'
  and autoloads the complete public `pp.el' surface (`pp-to-string' is needed
  for multi-index Eshell expansion).  Fast fixtures now use the real shared
  batch initializer instead of reconstructing startup ad hoc, resolvable
  preloads fail loudly instead of having their errors discarded, and the
  `pp.el' autoloads retain their GNU interactive flags.  A forward `em-ls'
  replay also found the omitted GNU `customize-set-value' autoload; it now
  loads the existing Elisp `cus-edit.el' implementation, preserving the
  Elisp/host boundary, and all four selected `em-ls-tests.el' cases pass.
  IMPORTANT DIAGNOSIS: the apparent directory-ring order failures were not
  state leakage--single-index output was already correct, while only list
  output failed with `void-variable pp-to-string'.  Inspect the exact nested
  error/output before changing state isolation.  Fast Rust coverage includes
  mutable returned filenames, strict/dumped startup behavior, the `seq' and
  `pp'/`cus-edit' contracts, the end-to-end `cd' metadata case, and all 11
  `em-dirs' tests in one interpreter.  Affected grouped oracle replays pass
  for `em-dirs' (11), `em-cmpl' (27), `dired-tests' (16), and `em-ls' (4).
  Full gate: 1196 library tests, 11 compatibility-harness tests, one
  perf-harness test, and three integration ERT runners pass; rustfmt, clippy
  with `-D warnings', and `git diff --check' are clean.  NEXT = selector 3082,
  `em-extpipe-test-1' in `test/lisp/eshell/em-extpipe-tests.el'.
- Verified through selector 3070/7080.  All 27 selected tests in
  `test/lisp/eshell/em-cmpl-tests.el' pass their grouped oracle replay.  This
  batch fixed shared contracts rather than individual Eshell cases: local hook
  lists are authoritative and splice the default hook at `t'; lexical `let*'
  uses nested binding scopes with stable frame identities; trimmed/empty
  lexical closures cannot alias same-shaped caller frames or see later
  bindings; dynamic empty closures still see their caller; per-buffer special
  values are read through one resolver; new buffers inherit
  `default-directory' but reset `buffer-read-only'; the standard obarray makes
  dumped/autoload symbols visible through `intern-soft'; the complete pcomplete
  command families, `elisp-completion-at-point', and host-derived `system-name'
  variable are available at startup; and the native completion driver handles
  partial-completion wildcards plus second-attempt candidate display.  The
  Lisp completion-table combinators and file-name table remain in
  `simple_compat.el`; no Lisp/Rust boundary was moved.
  Fast Rust regressions cover each contract and end-to-end Eshell glob,
  ambiguous, Lisp-function, and Lisp-symbol completion.  Two broad attempts
  were rejected by the full suite: making every empty closure lexical broke
  dynamic callbacks, and hiding every cross-buffer per-buffer binding broke
  inherited `default-directory'.  The final implementation keys the former on
  lexical source context and models the latter during buffer creation.  Full
  gate: 1190 library tests, 11 compatibility-harness tests, one perf-harness
  test, and three integration ERT runners pass; rustfmt and clippy with
  `-D warnings' are clean.  NEXT = selector 3071, `em-dirs-test/cd' in
  `test/lisp/eshell/em-dirs-tests.el'.
- Verified through selector 3038/7080.  Selectors 3001..3030 finish the
  selected ERC core/track batch, and all eight `em-alias-tests.el'
  selectors (3031..3038) pass; both ERC files and the Eshell alias file also
  pass file-wide `check-all' comparisons.  NEXT = selector 3039,
  `em-basic-test/umask/print-numeric' in `em-basic-tests.el'.
  This batch fixed contracts at their shared abstraction boundaries:
  current-buffer scopes now use set-buffer semantics without displaying the
  buffer; `switch-to-buffer' still displays an already-current hidden target;
  display action alists retain a bare first entry; preloaded iteration/control
  forms have GNU macro identity so generator.el can transform Eshell's
  `dolist'/`when' bodies; lexical-binding file cookies are scoped and restored,
  including compact modelines and nested loads; and `(eval FORM LEXICAL)'
  supplies the lexical mode to macro expanders without changing ordinary
  `lexical-binding' variable lookup.  Preload/default repairs cover
  `widget-convert', character-property aliases, mode/header/tab line values,
  `remote-shell-program', display-comint, file UID/GID/exec-path contracts,
  and the built-in CL type universe.  The broader ERC replay additionally
  exposed and drove general fixes for ellipsis-width reservation,
  nested lexical `ert-with-message-capture', and stored text-property plist
  order.  Fast subprocess polling now drains once after observed exit, while
  its contention test uses a generous deadline but separately asserts exact
  seconds parsing.  Every behavior change has a focused Rust regression.
  Final gate: 1168 Rust library tests plus all auxiliary targets pass,
  rustfmt is clean, and clippy passes all targets with `-D warnings'.
- Verified through selector 2881/7080:
  `erc-scenarios-base-upstream-recon-znc.el' passes check-all.  The
  decisive lever was a NATIVE `format-spec' (primitives.rs
  prefer_builtin_override + dispatch/strings.rs): erc rebuilds the mode
  line via `format-spec' on EVERY message, and the interpreted
  format-spec.el cost ~50ms/call, so the two-network burst ran too slow
  and erc-d's chained reply timers missed their expect windows.  The
  native version is PROPERTY-AWARE — it tracks, per output char, the
  FORMAT char its text props derive from (literals keep their own,
  a %-spec replacement inherits the spec text's props like GNU's
  insert-and-inherit, a collapsed %% keeps the first %'s) and returns a
  StringObject when FORMAT carried props; passes format-spec-tests.el
  against the oracle.  THREE fidelity rules the erc-fill snapshot tests
  enforce (erc-fill-wrap--merge-action compares `object-intervals'
  fragmentation against .eld snapshots saved under GNU): (1) a
  replacement's OWN text properties survive into the output, mapped
  through width/precision/padding via apply_format_spec_flags_indexed
  (GNU splices with insert-and-inherit; erc's message catalog passes
  propertized speaker strings); (2) own props take precedence,
  inherited FORMAT props only fill missing keys, own-first in the
  plist; (3) output property spans merge by SOURCE INTERVAL IDENTITY,
  never by value equality — adjacent `equal' but not `eq' values stay
  separate intervals and splice boundaries never coalesce.  Also in
  this batch:
  - Call-frame perf (core.rs call_function_value): the empty-closure
    and advice-transparent branches run the body DIRECTLY on the
    caller's `env' (push arg frame, truncate after) instead of
    `env.clone()' per call — deep call stacks (erc's two networks) were
    quadratic.  IMPORTANT NEGATIVE RESULT: two further micro-opts were
    TRIED and REVERTED because they broke `named-let'/bindat:
    (1) making `raw_function_binding' require FUNCTION_FRAME_MARKER as
    the frame's FIRST entry (O(1) skip of non-function frames), and
    (2) a hard `lexical_scan_floor' hiding all caller lexicals below the
    call boundary.  emaxx's `named-let' expands to
    `(letrec ((NAME (lambda ...))) (NAME . INITS))' and RELIES on
    `(NAME ...)' resolving the letrec VALUE binding in the function
    position (a Lisp-1 leak GNU doesn't have but emaxx allows); the
    marker-first change skipped the letrec frame, leaving NAME
    void-variable, which crashed loading bindat-tests.el (its LEB128
    type + every pcase, which expands through named-let).  Do NOT
    reintroduce marker-first or lexical_scan_floor without first
    reworking `named-let' to bind NAME as a real function (cl-labels) —
    and note the cl-labels expansion was ~10x SLOWER per call, blowing
    the bindat-test--sint timeout (3200 iters x pcase x named-let).
  - `forward-list' (buffer_edit.rs) signals `scan-error' at buffer end
    like GNU's C Fforward_list, instead of `.as_integer()'-ing the nil
    that the `scan-lists' Lisp wrapper returns (was a
    `wrong-type-argument integer nil').  erc-d-u--read-dialog catches
    the end-of-buffer scan-error to detect dialog EOF.
  - `version<'/`version<='/`version=' added (simple_compat.el).
  - LESSON: erc's per-message throughput matters for the erc-d live
    scenarios; profile with the interpreter but ALSO check whether a
    hot library function (format-spec here) should be native.  Native
    ports must be validated against the library's own -tests.el file
    (format-spec-tests.el) before adoption.
- REGRESSION-FIX FOLLOW-UP (same 2880 frontier; commit after the 2880
  batch): the first post-batch sweep was invalid — reverting
  compat/oracle.lock.json WHILE a sweep runs makes every remaining
  file report TIMEOUT/NONE (the harness fast-fails on the oracle
  fingerprint mismatch; NEVER revert the lock mid-sweep), and a
  leaked emaxx child from a harness run (todo-mode-tests) pinned a
  core for 1.5h skewing all timing runs (`pgrep -x emaxx' and kill
  leftovers before ANY timing-sensitive validation).  A clean sweep
  then exposed six real regressions, all fixed and re-validated
  (whole 139-file sweep green, cargo test 1140/0, fmt/clippy clean,
  10-probe oracle matrix exact):
  - buffer-local-variables ignored its BUFFER argument
    (buffer_meta.rs) — erc-open restored the SERVER buffer's markers
    into "#chan" on re-join, tripping the field-end cl-assert
    (upstream-recon-znc/severed now passes; also fixes the second
    `--znc' assert).
  - cancel-timer's function-only FALLBACK removed (threads.rs
    unschedule_timer_by_function_and_args): GNU cancel-timer of an
    already-fired timer is a no-op; the fallback cancelled the other
    network's pending erc-server-send-queue drain (cross-network
    flood-queue starvation).
  - Native GNU-`let'-expanding forms bound special names LEXICALLY
    (invisible to callees since the floor) — now dynamic when the
    name is special/soft/locally-declared: with-output-to-string
    (standard-output; eieio-object-write-to-string returned "" →
    eieio-persist end-of-file), ert-with-temp-directory /
    ert-with-temp-file (custom-theme--load-path), dolist / dotimes
    loop vars (fresh per-iteration binding; RESULT form evaluates
    with VAR unbound in the lexical expansion), native `newline''s
    last-command-event=?\n rebinding around post-self-insert-hook
    (electric-layout/pair/indent interplay).
  - macroexp--dynamic-variable-p was a nil STUB (misc.rs) → now
    faithful: (or (not lexical-binding) special-p soft/dlet/local
    memq-macroexp--dynvars).  Non-top-level one-arg `defvar' now
    ALSO pushes onto macroexp--dynvars (GNU's load-time eager
    expansion records dynvars for the rest of the form — the elisp
    macroexp.el definition shadows the native arm once loaded, so
    the LIST must be correct, not just the native predicate), and
    local_special_active is FLOOR-scoped, not activation-stamped:
    closures created inside the declaring scope must keep the
    declaration when invoked later (GNU captures (defvar . VAR) in
    the closure's interpreter env).  Without these, cl-macs
    tail-call elimination optimized `(let ((dyn-var 'b)) (f ...))'
    tail calls it must NOT touch (cl-macs--labels i-case: expect 'b).
  - One-arg `(eval FORM)' dropped its push_lambda_eval_context(false,
    false): that override is GLOBAL while the eval runs and leaked
    into lambdas created inside CALLED library functions (seq-reduce
    → wrong-number-of-arguments; cl-equalp → void-variable `case';
    shortdoc examples).  Empty env + fresh activation suffice.
  - Diagnosis pattern that worked: emaxx batch swallows stdout/princ;
    write probes with write-region crumbs to /tmp/probes/*.out, set
    EMAXX_BATCH_RESULT_FILE to read file_error/results, and ALWAYS
    set EMACS_TEST_DIRECTORY=/home/user/emacs/test or `documentation'
    returns nil for every builtin (that red herring cost an hour).
- Verified through selector 2880/7080:
  `erc-scenarios-base-statusmsg.el' (2880) passes check-all — the
  first LIVE erc-d network scenario end-to-end (TCP client+server in
  one process, full registration, status-prefixed PRIVMSG/ACTION
  send+receive, /me round trip, clean teardown).  139-file prefix
  sweep on frozen binaries is the gate.  This batch is mostly CORE
  INTERPRETER semantics + speed; every semantic change was validated
  against the oracle with minimal probes before adoption:
  - GNU `defvar' scoping (sf_defvar, definitions.rs): a one-arg
    `(defvar v)' NOT at top level (env non-empty) does NOT set the
    global special flag — `special-variable-p' stays nil, matching
    the oracle — and instead records an activation-stamped local
    marker (`--emaxx-local-special--v' in the innermost env frame;
    variables.rs push_local_special_marker/local_special_active).
    sf_let/sf_letstar treat a name as dynamic when globally special
    OR locally marked in the CURRENT activation.  Oracle behavior:
    inside a function, (defvar str) then (let ((str ...))) binds
    dynamically (callees see symbol-value), yet special-variable-p
    is nil and OTHER functions' `str' args/lets stay lexical.  GNU
    erc-send-input relies on ALL of this for its obsolete dynamic
    `str' (erc-send-pre-hook) interface.
  - Special-reference floor (bug#47552; eval.rs special_scan_floor,
    core.rs call_function_value, bindings.rs lookup/lookup_var/
    set_variable): when a function body runs on the CALLER's env
    chain (empty-closure and transparent-env branches), references
    and setqs of a GLOBALLY special name skip caller frames below
    the boundary and resolve dynamically.  Oracle behavior: a callee
    referencing special `pv2' sees the GLOBAL value even while the
    caller holds a lexical `pv2' argument; the caller's own body
    still sees its lexical argument.  Without this, erc-send-action's
    `str' ARG leaked into erc--server-send's queue push ("ready"
    instead of the full PRIVMSG line — the statusmsg /me bug).
    Fresh-env branches (isolated/real closures) reset the floor to 0.
  - Function arguments ALWAYS bind lexically, even for special names
    (oracle-confirmed; a dynamic-binding attempt was tried and
    REVERTED — cl-&key-arguments and callee-reference probes both
    match GNU only with lexical args).
  - A TOP-LEVEL one-arg `defvar' in a lexical-binding file is a
    THIRD tier, "soft special" (mark_soft_special/
    is_dynamic_binding_name): `let's bind dynamically and the
    reference floor applies, but `special-variable-p' stays nil,
    matching the oracle (cal-iso.el's top-level `(defvar date)'
    previously went globally special and broke icalendar).  Soft
    names are also pushed onto the GLOBAL `macroexp--dynvars' list
    so GNU cl-macs' cl--do-arglist gives same-named &key/&optional
    arguments lexical aliases (bug#47552) exactly as during a file
    compile — cl-&key-arguments needs `(funcall f :cl--test-a 'lex)'
    to bind the key lexically while `symbol-value' still sees the
    surrounding dynamic 'dyn.
  - `dlet' is now its OWN special form (sf_dlet), not an alias of
    `let': every binding is dynamic (bind_special_variable), and the
    bound NAMES are registered dynamic for the DURATION of the body
    (dlet_active_names counters) so `mapconcat #'eval' over
    calendar-date-display-form resolves `day'/`month'/`year'
    dynamically instead of finding a caller's same-named lexical
    frame (calendar-date-string; icalendar sexp enumeration).
  - The generated &key bindings of the NATIVE cl-defun lowering bind
    through `--emaxx-lexical-let*' (sf_letstar_forced_lexical), which
    always binds lexically like GNU's aliased arguments.
  - `(eval FORM)' with no LEXICAL argument now evaluates with an
    EMPTY lexical environment like GNU (all references dynamic):
    solar-time-string's `mapconcat #'eval' over
    calendar-time-display-form must read the dlet-bound `24-hours'/
    `minutes' strings, not a same-named outer lexical integer
    (lunar/cal-julian tests).  The nested-backquote unit test now
    passes its variable through eval's LEXICAL alist, matching the
    oracle (the old form signals void-variable in GNU too).
  - Internal frames that GNU binds dynamically now use
    bind_special_variable + restore: the `delay-mode-hooks' special
    form (core.rs — mode bodies are callees and must see it),
    with-silent-modifications (resource_forms.rs), and overlay
    modification-hook runs (hooks_overlays.rs via new pub(crate)
    bind_special_dynamic/restore_special_dynamic wrappers).
  - Reader (reader.rs read_atom): unescaped `,' `'' and backtick
    end a symbol like GNU read0's terminator set — `(,flags, hop-real)'
    in erc-backend's 352 pcase-let now reads as two unquotes; before,
    `hop-real' stayed a plain symbol and the handler died with
    void-variable hop-real (znc scenarios).
  - kill-buffer (files_process.rs) asks "Buffer modified; kill
    anyway?" only when the buffer VISITS a file (GNU Fkill_buffer
    checks BVAR filename): erc-d's insert-file-contents .eld dialog
    buffers die silently under inhibit-interaction.
  - TIMERS (threads.rs ScheduledTimer {due, repeat}, misc.rs arms):
    run-at-time/run-with-timer honor their delay (due Instant; nil/0
    fire at next pump), repeating timers reschedule after firing,
    cancel-timer matches function AND args (multiple erc-d exchange
    timers share erc-d--expire), run-at-time returns the 10-slot
    timer vector.  next_timer_due() lets waits sleep until the next
    due timer.  The earlier char-fold/srecode "timers must fire
    immediately" revert warning is OBSOLETE: with eager pumping (next
    bullet) due-based timers pass the whole sweep.
  - EAGER PUMPS (processes.rs wait_pumping_processes, display.rs
    arms): sleep-for/sit-for/accept-process-output loop until their
    deadline pumping external process output + network I/O + url
    retrievals, firing due timers each iteration, re-pumping
    IMMEDIATELY while progress is made (quiescence pumping — an
    in-process client/server handshake completes inside one wait),
    napping min(10ms, next-timer-due) when idle.
    accept-process-output still returns as soon as anything arrived.
  - INTERPRETER SPEED (the erc scenario timeout killer — message
    processing was ~250ms/line, now low-ms):
    functions_index (HashMap, last-wins) makes function lookup O(1)
    — mutations go through push_function_binding /
    reindex_function_binding etc (bindings.rs); macros_name_counts
    gives fast negative macro lookups (resolve_macro_binding,
    has_lisp_macro, macro_binding_as_function; cl-macrolet ranges via
    push_local_macros/drain_local_macros; shadow_macro_binding
    updates counts); globals_index makes global variable reads O(1)
    (global_value/set_global_binding/remove_global_binding/
    special-binding paths all keep it in sync — grep globals_index
    before touching `globals\` directly!); variable_aliases_index +
    special_variables_index do the same for alias resolution and
    specialness; Value::to_vec defers its cycle-detection HashSet
    until 64 nodes; sf_quote returns marker-free templates AS-IS
    (GNU shares quoted structure) with a per-template verdict cache
    (plain_quote_templates keyed by car-cell address, capped 1<<20).
  - LESSON: /tmp probe files that shadow library names break load
    (`load' checks cwd-relative names first): a stale /tmp/probes/
    fill.el turned an erc test run into an infinite autoload loop.
    Keep probe basenames un-library-like.
- FRONTIER NOW = 3001 (`erc-hide-prompt' in
  test/lisp/erc/erc-tests.el); selectors 2930..3000 pass exactly.
  CRUCIAL: the frontier counts MANIFEST-SELECTED selectors, NOT all
  check-all tests (compat/oracle_tests_all.txt: `selected=N' per file).
  misc-commands.el selects ONLY AMSG-GMSG-AME-GME (MOTD/SQUERY/etc. are
  discovered but NOT selected — do NOT chase them); misc.el selects 0
  (base-flood/kill-server-track/dcc are NON-selectors — ignore); after
  AMSG and stamp.el (2898..2900), erc-services-tests (2901..2917,
  17 selected), and erc-stamp-tests (2918..2929, 12 selected) now pass;
  the active block is erc-tests (2930..3023, 94 selected).  ALWAYS check
  `selected=' before burning time on a check-all failure — MOTD was a
  multi-hour detour on a non-selector.  To run one selector:
  `compat-harness run --scope all --selector <name> --file <f>' or
  emaxx `-l ert -l <proxy> --eval (ert-run-tests-batch-and-exit "<name>")'.
- MILESTONE 2929 — default oracle comparisons PASS for all 17 selected
  erc-services-tests.el tests and all 12 selected erc-stamp-tests.el tests.
  Services' three plstore failures came from cleanup calling
  `(kill-buffer (get-file-buffer FILE))' when get-file-buffer returned nil:
  GNU treats omitted/nil kill-buffer arguments as the current buffer, while
  emaxx rejected nil.  The stamp dedupe failure was a general add-hook bug:
  ERC registers fill at depth 60 and stamp at 70; emaxx ignored numeric
  depth, prepended both, and therefore filled AFTER stamping, folding every
  right stamp onto a physical line.  add-hook now maintains GNU-style
  hook--depth-alist metadata, stable numeric ordering, and a depth-zero local
  `t' sentinel that splices the default hook between negative and positive
  local depths; remove-hook cleans the metadata.  Once layout matched, the
  same test exposed format-spec using prin1 for non-string replacements
  (`#<buffer NAME>') instead of GNU's `%s'/princ rendering (`NAME').  Finally,
  native ert-deftest now evaluates :tags/:expected-result expressions when
  defining a test, so conditional `(:unstable)' metadata matches the oracle
  and default selection excludes erc-echo-timestamp.  Focused Rust tests cover
  all four behaviors.  Selector 2897 still passes, and erc-d-run-no-block
  passes three consecutive local-socket runs; all 1133 Rust library tests and
  auxiliary suites pass, and diagnostic probes were removed.
- MILESTONE 2900 — all three selected erc-scenarios-stamp.el tests PASS.
  Selector 2898 exposed ignored `:nowait' semantics: emaxx's blocking
  TCP connect returned an already-`open' process, so ERC skipped its
  "Opening connection" insertion and registered/login immediately.  GNU
  returns `connect' for `make-network-process :nowait t', reports `open'
  from the next event-loop turn, and invokes the sentinel only after the
  caller has installed it.  Emaxx now mirrors that observable sequence
  while retaining its already-connected OS socket; a focused socket test
  covers initial status, deferred transition, and the `open\n' sentinel.
  The non-manifest erc-d-run-no-block debug speed race is resolved too:
  move-to-column is O(n) rather than O(n^2), compiled-regexp cache hits use
  an O(1) LRU index and validate only on misses, and Cargo optimizes only
  the local emaxx crate in dev builds (debug assertions remain).  Repeated
  no-block runs PASS; selector 2897 and all 1128 Rust tests remain green.
- MILESTONE 2897 — AMSG-GMSG-AME-GME PASSES.  Diagnosed with temporary,
  environment-gated Rust traces (removed before commit).
  ROOT CAUSE 1 (FIXED — the "double-send") = `str' locally-special
  lookup.  `erc--run-send-hooks' does `(defvar str)' (bare, locally
  special) then `(let* ((str ...)))'; the NESTED invocation (via
  `erc--send-message-nested' during /amsg) must read its OWN dynamic
  `str' ("1 foonet only"), but emaxx's `lookup'/`lookup_var' only broke
  at the special-scan floor for `is_dynamic_binding_name' names, NOT for
  locally-special ones — so it fell through the floor to
  `erc-send-current-line's LEXICAL `str' ("/amsg 1 foonet only") and
  re-sent the raw command.  FIX (src/lisp/eval/bindings.rs, both lookup
  sites): the floor-break also fires for `local_special_active(name,env)'
  (marker above the floor => the name is dynamic in THIS scope, so a
  caller's same-named lexical binding is invisible).
  ROOT CAUSE 2 (FIXED — the disconnect hang) = ERC client processes had
  NO sentinel.  simple_compat.el shadowed the Rust `set-process-sentinel'
  primitive with a no-op stub `(defun set-process-sentinel (_process
  sentinel) sentinel)', so `(set-process-sentinel proc #'erc-process-
  sentinel)' (erc-backend.el:783) never stored it; when the barnet
  connection dropped after /QUIT the pump fired "connection broken" with
  `has_sentinel=false' => no `erc-process-sentinel' => "ERC finished"
  never displayed => the test hung on that expect.  FIX: removed the
  Lisp stub AND guarded the Rust `set-process-sentinel' dispatch
  (files_process.rs) to store the sentinel ONLY for network processes
  (`is_network_process') — subprocess/tramp/gpg sentinels stay inert
  exactly as before (the pump only dispatches NETWORK sentinels), so the
  flaky :unstable autorevert-remote (tramp "mock" subprocess) is
  unaffected.  Verified: all 12 erc sweep files PASS (incl. znc,
  scenarios-internal, scenarios-match); autorevert flakiness is
  identical to str-only (pre-existing bug#32645, NOT a regression).
  ROOT CAUSE 3 (FIXED — dropped timer-batch tail) = after the barnet /QUIT,
  the last two foonet messages `/gmsg 7 all live nets' and
  `/gme 8 all live nets' arrive at the erc-d server COALESCED in one
  read ("PRIVMSG #foo :7...\r\nPRIVMSG #foo :\1ACTION 8...\1\r\n"), so
  erc-d--filter queues BOTH.  on-request matches "7", meters its 0.1s
  reply ("alice: Excellent workman"), enters `sending' state; during
  that 0.1s the queued ACTION 8 gets ring-remove'd + ring-insert-at-
  beginning'ed by erc-d--on-request every tick (busy loop).  After the
  reply, the dialog advances to the ACTION 8 exchange, but ACTION 8 was
  never re-matched.  Exact identity/timing traces disproved both earlier
  suspects: ACTION-7's exchange timer was successfully canceled, and the
  newly created ACTION-8 timer fired only after its full 10 seconds.
  Queue traces also showed ACTION 8's parsed record remaining at length 1
  throughout `sending', disproving ring loss.  The actual cause was an ERT
  negative-expect timeout lambda performing a nonlocal `throw' while
  `run_pending_timers' held all due timers in a detached local Vec.  The
  function returned immediately and dropped the unfired tail, including
  the self-rescheduled `erc-d--on-request'.  Fix (threads.rs): iterate the
  detached batch explicitly and, before propagating a throw/debug error,
  prepend its unfired tail back onto `pending_timers'.  Unit regression:
  two due timers, first throws to a surrounding catch, second must remain
  active and fire at the next pump (failed before, passes after).
  PREREQUISITE FIX: make-network-process now honors `:family ipv4' for
  listener resolution.  Rust otherwise bound `localhost' to `[::1]' on
  this host while ERC connected to `127.0.0.1', failing before any dialog.
  Focused IPv4 listener test added.  Verified: selector 2897 PASS; default
  scenarios-internal PASS; scenarios-match check-all PASS; 1127 Rust
  library tests + auxiliary binaries PASS.  (The former non-manifest
  erc-d-run-no-block speed race was resolved in milestone 2900.)
- MILESTONE 2896: erc-scenarios-match.el PASSES check-all (2895..2896;
  the join-*/log scenario files between internal and match select 0).
  ROOT CAUSE was `goto-char' RETURN VALUE.  GNU `Fgoto_char' returns
  its POSITION argument UNCHANGED — `(goto-char MARKER)' returns the
  MARKER, `(goto-char 5)' returns 5 — while emaxx returned the clamped
  integer point.  `erc-display-msg' does `(marker-position (goto-char
  erc-insert-marker))'; the integer return made `marker-position' see
  an integer and signal `wrong-type-argument'.  Fix in buffer_edit.rs:
  `Ok(args[0].clone())`.  DEBUG LESSON: marker builtins (set-marker,
  marker-position, set-marker-insertion-type, ...) BYPASS advice in
  emaxx — `advice-add` on them never fires — so a Lisp breadcrumb
  probe caught nothing.  A temporary Rust trace in
  `marker_id_from_value`'s callers dumping
  `interp.backtrace_frames_snapshot()` (gated on an env var, removed
  before commit) pinpointed the caller.  Same-scenario compat
  additions (all oracle-validated in isolation via /tmp/probes/pc2.el
  style probes): `coding-system-change-eol-conversion' native
  (buffer_meta.rs) + `undecided-{unix,dos,mac}' coding variants;
  `filepos-to-bufferpos'/`bufferpos-to-filepos'/
  `filepos-to-bufferpos--dos' ported VERBATIM from mule-util.el into
  simple_compat.el; `find-composition' subset (composition text
  property only — no auto-composition engine) in simple_compat.el.
- MILESTONE 2894: erc-scenarios-internal.el PASSES check-all
  (2882..2894).  The "timer-coordination heisenbug" was NOT an event
  loop ordering problem — GNU `delete-process' on a network process
  runs the sentinel SYNCHRONOUSLY with "deleted\n" (Fdelete_process:
  pset_status (exit 0) + status_notify inline; the process leaves the
  process list when its death is notified, so a second delete never
  re-fires).  erc-d--teardown is REACHED THROUGH THAT SENTINEL: the
  first erc-d--expire finalizer deletes its client-connection, the
  sentinel sees a non-DROP exchange pending and calls erc-d--teardown
  directly (oracle's deterministic ~1.03s in linger-direct).
  Implemented as delete_process_notifying (processes.rs), notify
  condition = network runtime still attached, sentinel errors demoted
  unless debug-on-error (exec_sentinel).  Oracle-validated via
  /tmp/probes/pc1.el (inet) and pc2.el (unix); emaxx matches
  byte-for-byte modulo closure printing.
  - process-contact: full contact plist stored per process
    (create_network_process 12th arg; GNU p->childp): original
    keyword args with :service resolved + :local/:remote sockaddr
    vectors appended; accepted children = server plist with :server
    nil :host peer-ip :service peer-port :local server-addr :remote
    peer-addr; child names "NAME <HOST:PORT>" WITH SPACE; KEY t ->
    plist, real child -> t for ANY key, KEY nil -> (HOST SERVICE).
    nonstandard-messages' log id = (aref (plist-get (process-contact
    P t) :remote) 4).
  - Unix domain sockets: NetworkRuntime::UnixListener/UnixStream
    (:family local, :service PATH; drain_nonblocking/send_all generic
    helpers in threads.rs); server :local = PATH, client :remote =
    PATH :local = "", child :host t :remote "" named "NAME <N>"
    (interp.network_connect_counter = GNU connect_counter);
    delete-process leaves the socket file.  featurep 2-arg =
    member SUBFEATURE (get FEATURE 'subfeatures); make-network-process
    provided at init with GNU's subfeature list (eval.rs new()).
  - PERF (no-block was a pure speed race; PRIVMSG ~205ms -> ~90ms,
    bench /tmp/probes/privbench2.el; flat profiler via env var
    EMAXX_PROFILE=<path> — core.rs profile_enter/leave, dumps
    per-name call counts + self/total ms periodically):
    (1) MACRO-EXPANSION CACHE per callsite (eval_inner): keyed by the
    form's car-cell address, entry stores the ORIGINAL form (strong
    Rc pins the cell -> no address reuse), validated against
    definition_generation, which bumps on push/reindex/remove
    function bindings, note_macro_added/removed (covers cl-macrolet
    push/drain), and gv-expander/gv-setter/setf-method/
    cl-deftype-handler symbol-property puts (setf expansions read
    them).  Compiled GNU expands once; emaxx re-expanded
    pcase/rx/when-let machinery EVERY eval (internal--build-bindings
    was 320 calls/message).
    (2) not_macro_names verdict cache (same generation): skips the
    macro probe for plain function calls; only GLOBAL verdicts are
    cached (macro_position_function returns a from-frame flag;
    cl-flet shadowing can only make a name LESS of a macro, so cached
    not-macro verdicts stay correct under any frames).
    (3) name_facts memo (dispatch.rs NameFacts): is_builtin /
    is_special_form_name / prefer_builtin_override / undo-reset /
    dispatch-module routing — pure name predicates that were giant
    linear matches! chains per form eval; dispatch::call routes by
    the cached module id.
    (4) macro_position_binding (bindings.rs): macro lookup scans ONLY
    frames whose FIRST entry is FUNCTION_FRAME_MARKER (GNU: value
    bindings never shadow function cells) — cl-flet/cl-labels
    (resource_forms.rs) and oclosure frames insert their marker at
    index 0; raw_function_binding's marker/oclosure checks use the
    same first-entry invariant.  The autoload probe in
    try_macroexpand uses macro_position_function (global state only).
    (5) sf_if: allocation-free form_mentions_setcdr pre-scan (budget
    512; exhausted budget falls back to the cycle-safe slow path)
    gates the tail-alias machinery; is_record_literal_reader_form
    probes the car before to_vec; resolve_variable_name / lookup_var
    fast-path non-aliased names (Cow, no per-lookup String).
  - Remaining interpreter walls if more speed is ever needed:
    erc-update-undo-list ~30ms/call at 600-insert bench scale (walks
    the materialized buffer-undo-list per insert; GNU truncates undo
    at GC — emaxx never does); put/remove/add-text-properties
    ~1-2ms/call; beginning-of-line / move-to-column ~1ms/call.
  - Earlier fixes in this arc (still current): locate-library honors
    its PATH arg (files_process.rs — proxy-direct-subprocess{,-lib});
  start-process /
  make-process name the process from the NAME arg not the program
  (threads.rs create_process, processes.rs parse_make_process_args,
  files_process.rs — erc-d-t-with-cleanup reads `(process-name echo)');
  cancel-timer matches ARGS BY IDENTITY (values_eq_in_env) not `equal'
  (threads.rs unschedule_timer_by_function_and_args — sibling erc-d
  dialog RECORDS are structurally `equal' so deep matching cancelled
  the wrong linger timer).  SUPERSEDED HISTORY — a deep timer-coordination
  heisenbug: erc-d-run-linger-direct (:unstable; oracle passes
  deterministically ~1.03s), no-block, nonstandard-messages.  Traces
  (zdiag17..zdiag22 under /tmp/probes) show two near-simultaneous
  `erc-d--expire' timers whose finalizers race with `finalize-dialog'/
  `delete-process'; only one dialog's teardown-this-dialog-at-least
  reaches `erc-d--teardown' so the dumb-server never dies.  The eager
  pump (processes.rs wait_pumping_processes + threads.rs
  run_pending_timers) fires due timers in a batch, but the ORDERING
  and interleaving with process-event delivery differs from the C
  event loop.  This is the real work item and NOT a one-liner — study
  how GNU orders timer firing vs process filters/sentinels during
  accept-process-output and mirror it.  zdiag5/6/7 trace
  send/drain/expect; zdiag12..16 trace the erc-d command state
  machine.  Then match/misc-commands/stamp scenario files, erc-services
  (plstore), erc-stamp (right-stamp rendering), erc-tests (2930..3023).
- IN PROGRESS — erc-scenarios-misc-commands--MOTD (not yet passing):
  FIXED here the buffer-local HOOK ORDERING (hooks_overlays.rs
  `hook_values'): GNU runs a buffer-local hook by walking its value
  `(local-fns... t)' and splicing the GLOBAL handlers at the `t'
  sentinel, so LOCAL handlers run BEFORE global.  emaxx had it
  backwards ([global, local]); now [local, global].  This is what let
  `erc-once-with-server-event's local one-shot (added at depth -95, `t)
  pre-empt the global `erc-networks-on-MOTD-end' and return `t' to stop
  the `run-hook-with-args-until-success' chain — killing the spurious
  "Unexpected state detected" warning on a re-requested MOTD.  Verified
  vs oracle with /tmp/probes/hooktest.el (buffer-local depth -95 + `t'
  returning t → only the local runs; global skipped).  NOTE: emaxx's
  add-hook still IGNORES a numeric DEPTH arg (treats any truthy 3rd arg
  except :local as "append"); harmless when the local list has one
  entry (MOTD) but a real gap — fix add-hook to sort by depth if a
  later test needs it.  STILL FAILING past that: a DEEPER timer bug —
  `erc-d--on-request' (the erc-d server's self-rescheduling 0-delay
  `run-at-time nil nil' request pump) stops firing after the connect
  burst.  On the re-MOTD, the client's "MOTD irc1.foonet.org" reaches
  the server socket AND `erc-d--filter' queues it (traced at 0.83s),
  but `on-request' never fires again to drain the queue, so the server
  expires the exchange at 10s (erc-d-linger).  Traces: /tmp/probes/
  zdiag3[0-6].el (30=warning path, 32=send timing, 33=server match,
  34=proc-send, 35=filter-recv vs on-request, 36=reschedule).  Root
  cause is in the native pending-timer pump (threads.rs
  run_pending_timers / processes.rs wait_pumping_processes): a pending
  0-delay `on-request' scheduled during one pump isn't fired by a
  later accept-process-output pump after the intervening SYNCHRONOUS
  test Lisp (erc-scenarios-common-say inserts + erc-send-current-line).
  This is the next real work item for MOTD; the erc-scenarios-internal
  multi-exchange tests pass because they don't interleave synchronous
  prompt input between exchanges the same way.
- NEXT (frontier order, each fails for its OWN reason — diagnose
  individually with the fresh binaries): erc-scenarios-misc-commands.el
  (AMSG-GMSG-AME-GME, MOTD — MOTD exercises /MOTD with targets and the
  erc-server-402/376/422-functions local-var cleanup),
  erc-scenarios-stamp.el (date-mode/left-and-right, date-mode/reconnect,
  left/display-margin-mode — the date-stamp splice-insertion machinery:
  erc-stamp--date-marker, erc--with-spliced-insertion,
  field-at-pos/erc--msg checks), erc-scenarios-misc.el (base-flood,
  kill-server-track, dcc-chat-accept, handle-irc-url,
  networks-announced-missing); then erc-services (plstore), erc-stamp,
  erc-tests (2930..3023).  Sweep gate = /tmp/probes/sweepH.sh over
  prefix-files20.txt (prefix-files19 + erc-scenarios-internal +
  erc-scenarios-match) plus the znc file = 142 runs, on frozen
  /tmp/probes/bin binaries.  The macro-expansion cache makes EVERYTHING
  faster — timing-sensitive tests that used to flake may now pass;
  re-verify old assumptions before working around slowness.
- Verified through selector 2879/7080: `erc-networks-tests.el'
  (2812..2854, 43/43), `erc-nicks-tests.el' (2855..2870, 16/16),
  `erc-sasl-tests.el' (2871..2879, 9/9 selected; the :unstable ecdsa
  placeholder SKIPS via ert-skip like GNU); 139-file prefix sweep
  (prefix-files19.txt) on frozen binaries is the gate.  Load-bearing:
  - sf_with_current_buffer (resource_forms.rs) saves the current
    buffer BEFORE evaluating the buffer form and restores it if the
    form or buffer resolution errors; sf_with_current_buffer_window
    likewise.  A buffer form that switches buffers (erc--open-target)
    must not leak.
  - Buffer-list recency (runtime.rs): switch_to_buffer_id never
    reorders buffer_list; record_buffer_front(id) is called by the
    switch-to-buffer / pop-to-buffer / pop-to-buffer-same-window /
    switch-to-buffer-other-window arms (files_process.rs) and
    select-window (display.rs), each honoring its NORECORD argument
    position (pop-to-buffer's is arg 3).
  - cl-generic &context: expression-context specializers embed a
    fingerprint of the context expr in the generated variable name
    (eval.rs cl_defmethod_specializers), so two methods differing only
    in context expr get distinct method identity keys; and
    ClDefmethodStoredMethod stores (variable, specializer,
    Option<context-expr>) triples (metadata_value emits 3-element
    entries) so condition() can re-evaluate ANOTHER method's context
    test inside (not <cond>) guards.
  - sf_should returns the evaluated FORM value (GNU `should').  The
    native ert runner also maps a SignalValue whose car is
    ert-test-skipped to TestStatus::Skipped (`ert-skip').
  - sf_save_restriction: wide buffer at entry -> plain re-widen on
    exit (GNU save-restriction-save); marker tracking of a wide
    buffer's bounds re-narrowed after insert-before-markers at BEGV
    (broke erc-networks--transplant-buffer-content).
  - sf_with_silent_modifications pushes an env frame binding
    inhibit-read-only and inhibit-modification-hooks (GNU macro).
  - delete-process accepts nil (current buffer's process), a buffer,
    or a buffer name (files_process.rs).  custom-set-variables sets
    an option immediately when default_toplevel_value exists (misc.rs).
  - --batch colors: simple_compat.el carries verbatim
    term/tty-colors.el (color-name-rgb-alist, tty-standard-colors,
    tty-color-alist/canonicalize/24bit/off-gray-diag/approximate/
    standard-values/values/desc + tty-defined-color-alist init) and
    faces.el color-values, readable-foreground-color, color-dark-p,
    color-luminance-dark-limit, face-attribute-name-alist,
    face-spec-choose, face-spec-set-match-display, face-attr-match-p,
    face-spec-match-p, face-default-spec, face-user-default-spec,
    face-documentation, set-face-documentation, list-faces-display
    (+ list-faces-sample-text), and custom.el
    custom-handle-all-keywords/custom-handle-keyword/custom-add-*/
    custom-fix-face-spec, cus-face.el custom-declare-face, doc.c
    documentation-stringp, elisp-mode.el lisp-interaction-mode
    (define-derived-mode; do NOT autoload "lisp-mode" for it — that
    breaks the native mode arm), plus autoloads for customize-face /
    customize-face-other-window / describe-face.
  - frame-parameter singular arm returns background-color
    "unspecified-bg", foreground-color "unspecified-fg",
    background-mode `dark, display-type `mono, name "F1", font "tty",
    modeline/minibuffer t (display.rs) — matching the plural arm.
  - face-spec-set is a native arm (display.rs) storing SPEC under the
    requested spec-type property and applying default-display
    attributes via record_defface_runtime_attributes (now pub(crate)).
  - with-help-window shim (simple_compat.el) runs help-make-xrefs and
    goto-char point-min after BODY (GNU help--window-setup): [back]
    buttons appear and help-xref-go-back's position restore works via
    the new set-window-point arm (selected window moves point).
  - text-quoting-style arm reads the variable (curve/straight;
    default grave in batch).
  - hex-util + rfc2104 are in is_compat_preloaded_feature (eval.rs)
    with native arms (misc.rs): decode-hex-string / encode-hex-string
    (hex-util semantics incl. the invalid-digit error) and
    rfc2104-hash (HMAC; known algorithm symbols hash natively via
    secure_hash_digest, wrapper functions are funcalled and their hex
    output re-parsed; a longer-than-block key becomes the HASH's hex
    string itself, exactly like the elisp).  Rationale: interpreted
    hex-util/rfc2104 made erc-sasl's 4096-iteration PBKDF2 take ~25
    minutes; native is milliseconds and byte-identical.
  - read-string/read-from-minibuffer/read-no-blanks-input consume
    unread-command-events up to RET before consulting kbd macros
    (lists.rs) — ert-simulate-keys works.  minibuffer-with-setup-hook
    (core.rs) runs the hook with the " *Minibuf-0*" buffer current and
    emaxx--active-minibuffer bound (active-minibuffer-window returns
    the selected window while set), so auth-source.el read-passwd
    (read-passwd-mode -> read-passwd-toggle-visibility) works.
    define-minor-mode's generated body maintains GNU's
    `local-minor-modes' (buffer-local list of enabled modes; the
    variable is registered natively for bare interpreters and via
    defvar-local in simple_compat.el).  `read-hide-char' defvar added.
- Milestone status: verified frontier 2879 (committed f982967, pushed
  to main by the user).  A FOUNDATION commit 646ca07 sits on top: it
  does NOT bank a new manifest file yet, so the verified frontier is
  still 2879.  What 646ca07 adds (all gated, non-regressing):
  - decode-coding-string decodes utf-8 family codings byte-exactly
    (raw-byte unibyte -> decoded multibyte, undecodable bytes kept)
    via utf8_text_from_bytes_keeping_raw in coding.rs.  Banks the 4
    erc-d-i parse tests.
  - with-timeout macro (verbatim GNU timer.el) in simple_compat.el.
  - cursor-sensor.el subset (cursor-sensor-inhibit + cursor-intangible-
    mode / cursor-sensor-mode) — fixes erc-timestamp-intangible--left.
  - A network process subsystem: NetworkRuntime {Listener,Stream} on
    ProcessState; ProcessStatus Open/Closed/Listen; create_network_
    process/accept_network_connection/poll_network_stream/network_
    stream_send in threads.rs; make-network-process/open-network-
    stream/set-process-sentinel/process-sentinel/process-name/get-
    process/process-contact/set-process-buffer arms; pump_network_
    processes in processes.rs wired into sleep-for/sit-for/accept-
    process-output (accepts server conns -> child process inheriting
    filter/sentinel/log/plist, runs "open from PEER\n"; delivers
    stream input to filters; "connection broken" on close).
  - Follow-on UNCOMMITTED work (3 clean fixes on top of 646ca07, all
    gated + sweep-validated, kept because correct/general):
    (a) processes.rs pump copies the server's plist per accepted
    connection (GNU server_accept_connection Fcopy_sequence) so a
    child's `process-put' can't clobber the server's `:dialog-dialogs';
    (b) misc.rs add-hook wraps a bare-function-symbol hook value into a
    one-element list before adding (GNU), so erc's `422' hook keeps
    `erc-server-376' when the networks module adds
    `erc-networks-on-MOTD-end'; (c) syntax.rs down_list_impl (forward)
    now routes through scan_lists_gnu(from, 1, -1) so
    `parse-sexp-ignore-comments' is honored — a `(' inside a comment no
    longer counts (erc-d-u--read-dialog navigates .eld hunks with
    down-list/forward-list over comments containing parens).
  - erc-d-run-basic now completes the ENTIRE IRC handshake with these
    fixes (PASS/NICK/USER -> welcome burst -> MODE +i -> JOIN #chan ->
    #chan buffer created), but still FAILS on the last exchange:
    emaxx's pump is too SLOW (registration alone takes ~4.8s of pump
    time; each accept-process-output sleeps 0.05s and the flow needs
    many round-trips), so the client's `MODE #chan' arrives after the
    mode-chan exchange's 1.2s timeout expires.  Banking
    erc-scenarios-internal needs PUMP-PERFORMANCE work, not more
    protocol fixes: process pending input/timers eagerly (don't sleep
    a fixed 0.05s when there is deliverable input or a due timer),
    and/or make accept-process-output return as soon as input arrives.
    IMPORTANT: making run-at-time timers respect their delay (a
    ScheduledTimer.due partition in run_pending_timers) was TRIED and
    REVERTED — it regressed char-fold/srecode (they rely on timers
    firing immediately during pumps) AND did not fix erc-d (the pump
    is the real bottleneck).  Do not re-add delay-respecting timers
    without also solving the pump-eagerness problem.
  - Other single-file gaps: erc-services 2901..2917 (3 plstore fails —
    need a plstore.el auth-source backend; get-file-buffer is nil);
    erc-stamp 2918..2929 (1 fail: erc-stamp--dedupe-date-stamps-from-
    target-buffer date-stamp merge — deep); erc-tests 2930..3023
    (57/99, 42 non-network unit fails, listed in
    /tmp/probes/result-erct.json).  Highest yield: erc-stamp (1 fix)
    or erc-services (plstore).  Notes: /tmp/probes/NEXT-BATCH-NOTES.md.
- Previous entry (2765/7080): `viper-tests.el' (2747..2751,
  5/5), `env-tests.el' (2752..2754), `epg-config-tests.el' (2755..2758),
  `epg-tests.el' (2759..2765, 7/7) all pass; 130-file prefix sweep
  (prefix-files15.txt) on frozen binaries is the gate.  Load-bearing:
  - active_command_keymaps consults `emulation-mode-map-alists'
    (symbols resolving to (VAR . KEYMAP) alists — viper's modal maps).
  - kbd macro loop: function-key symbol events described as "<escape>"
    (angle brackets) and translated to their ASCII equivalents via GNU's
    local-function-key-map defaults when unbound; per-command undo
    boundaries (self-insert runs amalgamated); `last-command' takes the
    post-command value of `this-command' (viper-undo-more rewrites it);
    interactive readers consume kbd-macro events when
    unread-command-events is empty (viper's `F' target char).
  - GNU undo-list model: Insert entries render/parse as (BEG . END);
    marker riders live inline in the undo list (push_undo_meta appends
    Opaque entries; replay skips (MARKER . N)/(t . TIME) riders); setq
    of buffer-undo-list REBUILDS the native list (round-trip);
    primitive-undo + undo-more are GNU simple.el ports in
    simple_compat.el, as are prepare/activate/accept/cancel-change-group;
    undo-amalgamate-change-group is length-based (the exposed list is
    rebuilt per read, so GNU's eq/setcar structure-sharing tricks
    cannot work — see the docstring).
  - search-forward/backward honor COUNT (negative = opposite direction,
    viper-find-char).
  - Process machinery: refresh_process_state drains exiting children's
    pipes into pending buffers delivered by the next pump (gpg's final
    status lines); sleep-for pumps process output (GNU waits do);
    deliver_process_output drops output for killed buffers;
    process-send-string encodes via encode_utf8_bytes so raw-byte chars
    are single bytes (binary signatures to gpg's stdin).
  - shell-command captures stdout into OUTPUT-BUFFER (t = current);
    shell-command-to-string port (epg's gnupg-version skip check —
    gpg 2.4.4 is "buggy" upstream so epg-roundtrip-1/2 SKIP to match
    the oracle).
  - IN PROGRESS: erc series groundwork (UNCOMMITTED VALIDATION —
    sweep25 pending; frontier stays 2765 until an erc file passes).
    Landed so far:
    - format supports %i (= %d) and %e/%g (erc-backend's
      define-erc-response-handler uses "%03i" — it rendered "0%i").
    - emacs-build-time (nil = not recorded) in builtin vars.
    - simple_compat: while-let, ascii-case-table (mule.el port),
      with-case-table, custom-load-symbol + custom-load-recursion,
      custom-variable-p.
    - cl-generic-define-context-rewriter WORKS now: stored as a macro
      named cl-generic--context-rewriter--NAME; sf_cl_defmethod
      pre-expands &context entries with registered heads
      (expand_generic_context_rewriters); ClDefmethodSpecializer grew
      context_expr (expression contexts evaluated at dispatch) —
      erc-networks' erc-obsolete-var rewriter.
    - require honors NOERROR (returns nil on file-missing/file-error)
      — erc--find-mode's module fallback.
    - erc/erc-button/erc-loaddefs load cleanly; module autoloads
      registered via erc.el's (load "erc-loaddefs").
    - field-beginning/field-end implement GNU's BOUNDARY rule now
      (buffer_meta.rs): with differing before/after `field' props,
      front-sticky on the after char claims POS, else the (default)
      rear-sticky before char does, else POS is a zero-length field.
      Verified against the oracle; erc's prep-for-insertion assert and
      a plain erc-display-message flow both pass standalone.
    erc-open SESSION BRING-UP NOW WORKS end-to-end (server buffer +
    erc--open-target + erc-display-message in #chan).  The chain of
    fixes, each found by instrumenting erc-open:
    - add-to/remove-from-invisibility-spec, display-buffer-overriding-action
      ('(nil)), custom-initialize-default/set/reset/changed/delay ports
      in simple_compat.el.
    - defcustom records `standard-value' (list of the UNEVALUATED
      default) and honors :initialize/:get — GNU's custom-declare-variable
      contract.  erc-modules' :initialize lambda stamps `erc--module' on
      every built-in module symbol (kills the bogus aberrant-modules
      warning), and :global define-minor-mode records standard-value so
      custom-variable-p routes global modes through erc--update-modules'
      immediate funcall (they were wrongly deferred as local modules).
    - Zero-specializer :around cl-defmethods now wrap the generic's
      current definition with cl-call-next-method chaining (they
      previously CLOBBERED it via plain cl-defun — erc-stamp--current-time).
    - special-variable-p returns t for t/nil/keywords like GNU —
      erc-button-setup's FORM check ((special-variable-p t)) otherwise
      routed every default erc-button-alist entry into a deprecation
      warn that inserted into a marker-less process buffer
      (the "wrong-type-argument marker nil").
    NEWER (this batch, uncommitted-sweep): three verified-vs-oracle
    primitive fixes unlocked buttonizing:
    - subst-char-in-region now substitutes IN PLACE (buffer.rs
      replace_region_in_place): text properties and markers survive.
      fill-region's newline pass was DELETING+REINSERTING the whole
      message, stripping erc-button's props (the erc-data mystery).
    - newcomment.el autoloaded defvars (comment-start[-skip] etc.) in
      simple_compat — fill.el errored void comment-start-skip.
    - substitute-command-keys \\[CMD] consults the ACTIVE keymaps
      (active_command_keymaps then global-map) unless \\<MAPVAR>
      pins one — erc-mode's C-a for erc-bol now resolves.
    RESULT: erc-button-alist--url PASSES alone.  Full-file failures are
    cross-test pollution from the first test.  Remaining:
    - erc-button--display-error-notice-with-keys: string compare passes
      now; fails later at (search-forward "erc-bol") — the notice's
      buttonized key names; check erc-button--display-error-notice-
      with-keys' buttonize pass over its own inserted notice.
    - erc-button-alist--function-as-form: expects form-call positions
      (53 55 ...) — off-by-something in match positions handed to the
      FORM lambda; compare erc-button-add-buttons-1's bounds.
    - Fix those two, then the file should go green (url passes alone).
    CURRENT erc-button-tests state (2766..2770): all 5 reach REAL
    buttonize assertions now:
    - erc-button-alist--url (alone): erc-data text property missing.
      ROOT CAUSE FOUND (probe /tmp/probes/btn.el): message text inserted
      at erc-insert-marker comes out carrying THE PROMPT'S properties
      (rear-nonsticky t front-sticky t read-only t) — emaxx's
      insert-before-markers INHERITS neighboring text properties, but
      GNU's plain insert/insert-before-markers NEVER inherit (only the
      -and-inherit variants do).  Fix
      insert_current_buffer_before_markers (and check the plain insert
      path) in eval/buffers.rs, then the read-only/sticky bleed and the
      missing erc-data/mouse-face should both resolve.  The 3 field-end
      failures in a full-file run are POLLUTION from the first failure;
      fix this first, rerun the file.
    - erc-button--display-error-notice-with-keys:
      substitute-command-keys \\[erc-bol] → "C-a" mismatch.
    NEXT: fix erc-button-add-buttons buttonizing, then erc-dcc/
    erc-goodies/erc-networks (6/43 already passing).  3000 sits inside
    the erc series.
    VALIDATION DEBT: sweep26 caught 10 regressions from this batch,
    all fixed in-place (internal--define-uninitialized-variable accepts
    1 arg for cus-start.el; :initialize only invokes bespoke lambda
    initializers — the standard custom-initialize-* symbols reduce to
    the plain default assignment and re-running them clobbered
    package-tests).  All 10 files re-verified PASS individually, but a
    FULL fresh sweep (sweep27 from sweep26.sh, freeze binaries first)
    is REQUIRED before cutting the next patch.
- Verified through selector 2746/7080: `testcover-tests.el' (2670..2700,
  31/31), `text-property-search-tests.el' (2701..2720),
  `thunk-tests.el' (2721..2729), `timer-tests.el' (2730..2734),
  `track-changes-tests.el' (2735), `unsafep-tests.el' (2736..2740),
  `vtable-tests.el' (2741..2742), `warnings-tests.el' (2743..2746) all
  pass; 126-file prefix sweep (prefix-files14.txt) on frozen binaries
  is the gate.  Load-bearing for this batch:
  - cl-defstruct `:type list' (unnamed) constructs PLAIN LISTS like GNU
    (emaxx-struct-make 'list mode; setf writes the nth car in place,
    gated on the struct's emaxx-struct-sequence-type) — edebug's
    edebug--form-data entries are walked with `car' by testcover.
  - `equal' on closures now compares the captured bindings the body
    references (collect_free_symbol_candidates + lookup_captured_binding
    in values.rs): textually identical lambdas stay equal (nadvice)
    while closures over different values differ (testcover 1value).
  - `function-get' follows defalias chains ((function-get 'not
    'side-effect-free) reads null's — unsafep); `(defalias 'not #'null)'
    and `(put 'format-alist 'risky-local-variable t)' mirror the GNU
    preloads.
  - time-add/time-subtract use GNU's time_arith reduction (same hz →
    ticks combine, hz kept; different hz → LO/HI reduced only by
    gcd(LO, da2*db2)); time-convert with integer HZ floors instead of
    signaling; sit-for accepts (SECONDS NODISP);
    timer-next-integral-multiple-of-time ported (timer-tests bug#33071).
  - insert-file-contents runs before/after-change-functions (both the
    REPLACE wipe and the insertion) — track-changes' revert sync.
    Because inserts now go through insert_text_with_hooks, the
    supersession check respects a let-bound nil `buffer-file-name'
    (GNU gates the conflict prompt on the Lisp value; auto-revert's
    tail handler binds it to nil while appending).  autorevert-tests
    REGRESSES to a hard FAIL without that guard.
  - local-variable-p returns t for GNU's always-buffer-local
    DEFVAR_PER_BUFFER set (is_always_buffer_local_builtin) — unsafep
    treats setq of buffer-display-count/mark-active as safe.
  - recent-keys (empty vector), display-color-p/grayscale-p (nil),
    display-color-cells (0), frame-parameters (terminal alist).
  - simple_compat: 1value/noreturn macros, backtrace-frames,
    risky-local-variable-p, load-library, add-to-ordered-list,
    add-to-history, deactivate-input-method + input-method state vars,
    next-line-add-newlines, scroll-step/-conservatively/-margin,
    global-mode-string, mark-even-if-inactive, emulation-mode-map-alists,
    initial-major-mode, abbrev-mode, auto-fill-function, beep alias.
  - NEXT WALL: `viper-tests.el' (2747..2751) — loads now and
    viper-test-fix passes; the 4 undo tests (viper-test-undo-1..4)
    fail: they drive vi-style editing via execute-kbd-macro and check
    undo grouping semantics.  After viper: env/epg-config already pass;
    erc series follows.
- Verified through selector 2669/7080: `shortdoc-tests.el' (2613..2617,
  5/5), `subr-x-tests.el' (2618..2664, 47/47), `syntax-tests.el' (2665),
  `tabulated-list-tests.el' (2666..2669, 4/4) all pass; 118-file prefix
  sweep (prefix-files13.txt) on frozen binaries is the gate.
  Load-bearing for this batch:
  - Native define-short-documentation-group (sf_defgroup) is now gated
    behind !has_macro_binding so the real shortdoc.el macro populates
    `shortdoc--groups' (it was silently routing to the custom defgroup
    handler, leaving the groups empty and 3/5 tests vacuously passing).
  - `documentation' falls back to (1) the version's etc/DOC file
    (compat_data_directory()/DOC, `\x1fF<name>\n<doc>' entries — C
    primitives) and then (2) a lazy scan of the version's lisp/ tree for
    top-level defun/defmacro/defsubst docstrings (natively-implemented
    subr.el/files.el functions have no lambda body to read from).  Both
    caches are thread-local and keyed by path.
  - `help-function-arglist' returns the macro's arglist for macro-table
    macros instead of `t' (shortdoc iterates the arglist with dolist;
    `t' faulted with wrong-type-argument — rx-let display).
  - `rx-let-eval' has a loaddefs-style autoload in simple_compat.el
    (fboundp before rx.el loads) plus a native eval trigger that loads
    GNU rx.el; ucs-normalize-NFC/NFD-string are native
    (unicode-normalization crate); buffer-text-pixel-size added
    (char-count model, same as window-text-pixel-size).
  - simple_compat.el gained the shortdoc group functions: verbatim GNU
    ports (assoc-default, char-uppercase-p, split-string-and-unquote,
    file-name-with-extension/-parent-directory/-quote/-quoted-p/-unquote,
    file-modes-* family, match-substitute-replacement,
    replace-{regexp,string}-in-region, locate-dominating-file,
    file-equal-p, file-newer-than-file-p, file-chase-links,
    string-or-null-p, string-greaterp, make-separator-line,
    next/previous-property-change, get-char-property-and-overlay,
    string-glyph-compose/decompose, copy-directory,
    split-string-shell-command) and honest degraded stubs for
    OS-level features (file-acl/selinux/xattr return nil/unsupported,
    add-name-to-file signals file-error, vc-responsible-backend nil,
    kill-process via delete-process, set-process-sentinel no-op,
    make-nearby-temp-file = make-temp-file).
  - NEXT WALL: `testcover-tests.el' (2670..2700, 31 selectors) FAILs;
    `text-property-search-tests.el' (2701..2720) and `thunk-tests.el'
    (2721..2729) already PASS behind it; `timer-tests.el' (2730..2734)
    FAILs.  Crack testcover then timer to extend the frontier to 2734+.
- Verified through selector 2612/7080: `rx-tests.el' (2524..2559,
  36/36), `seq-tests.el' (2560..2611, 52/52), `shadow-tests.el' (2612)
  all pass; 117-file prefix sweep (prefix-files12.txt) on frozen
  binaries is the gate (autorevert-tests.el is a known flake — retry
  it standalone).  Load-bearing for this batch:
  - GNU rx.el fully adopted via ensure_gnu_rx_loaded; the reader's
    `\xNNNN' string hex-escape maps the #x3FFF00..#x3FFFFF raw-byte
    range to emaxx's internal raw-byte char (0xE000+byte) so
    constructed regexp strings round-trip as the oracle's unibyte
    bytes (rx-char-any-raw-byte, rx-charset-or).
  - macroexpand-all now evaluates `eval-and-compile' bodies at
    expansion time AND keeps the forms, so rx-define's
    `(put ... 'rx-definition ...)' side effect is visible to a later
    `(rx ...)' in the same rx-let (rx-let-define).
  - char-to-string/`string'/unibyte-char-to-multibyte map the
    #x3FFF00 range consistently to 0xE000+byte; find-composition-internal
    added (unicode-segmentation grapheme clusters).
  - subr-x adopted as the real GNU file (dropped from
    is_compat_preloaded_feature); mapconcat treats a nil separator as
    "" and string-join delegates through it; let/let* signal
    setting-constant when binding nil/t/keywords (and-let*).
  - utf-16/-le/-be coding systems (BOM = FE FF big-endian).
  - dir-locals-file builtin var (shadow-tests).
  - NEXT WALL: `shortdoc-tests.el' (2613..2617) — needs the real
    shortdoc.el groups populated (guard native
    define-short-documentation-group behind has_macro_binding),
    `documentation' to fall back to the version's etc/DOC file for
    C builtins, make-separator-line, and ~36 group functions to be
    fboundp (17 with :eval examples must eval without error; the rest
    are :no-eval and only need fboundp).  Non-fboundp functions are
    SKIPPED in display, so all 36 must be defined for
    shortdoc-all-functions-fboundp.  See docs/compatibility-goal.md.
- Verified through selector 2523/7080: `pp-tests.el' (2488..2491),
  `range-tests.el' (2492), `regexp-opt-tests.el' (2493..2494),
  `ring-tests.el' (2495..2518), `rmc-tests.el' (2519..2523) all pass;
  see docs/compatibility-goal.md 2523 entry.  Load-bearing:
  prin1 escapes only " and \ by default (raw newlines/tabs);
  looking-back prefers latest non-empty match, match-data based at
  haystack origin; insert-buffer-substring nil bounds; GNU-exact
  regexp-quote (native regexp-opt override dropped); lambda
  doc-string-elt 2; simple_compat untabify/use-dialog-box-p; native
  window-frame + display-supports-face-attributes-p (nil).
- IN PROGRESS: `rx-tests.el' (2524..2559) — 33/36 on emaxx
  (UNCOMMITTED groundwork; frontier stays 2523 until all 36 pass).
  GNU rx.el is now adopted via ensure_gnu_rx_loaded (native sf_rx* are
  the fallback); macroexpand-all binds macroexpand-all-environment for
  :rx-locals; define-obsolete-function-alias actually installs aliases;
  regexp-opt delegates to the trie elisp; char-to-string/string accept
  raw-byte codepoints and unibyte-char-to-multibyte maps 0x80..0xFF to
  eight-bit chars.  REMAINING 3 need emaxx's internal raw-byte char
  (0xE000+byte) to round-trip as the oracle's unibyte byte in
  constructed regexp strings (rx-char-any-raw-byte, rx-charset-or) and
  an rx-let/rx-define shadowing fix (rx-let-define).  See
  docs/compatibility-goal.md rx-tests IN PROGRESS entry.
- Verified through selector 2487/7080: `pcase-tests.el' (2475..2487)
  passes; 105-file prefix sweep (prefix-files10.txt) on frozen binaries
  is the gate.  Batch details in docs/compatibility-goal.md 2487 entry
  — READ IT before touching pcase, the reader's quote symbols,
  backquote evaluation, macro arity, or cl-typep; load-bearing:
  - GNU pcase.el now OWNS the pcase family whenever the load-path can
    resolve it (ensure_gnu_pcase_loaded, lazily on first use); native
    sf_pcase* are the no-file fallback, gated by has_macro_binding.
  - The reader always emits raw \`/\,/\,@ quote symbols (GNU).
  - Nested backquote rebuilds preserve original head symbols.
  - Macro calls missing required params signal
    wrong-number-of-arguments.
  - byte-opt.el side-effect-free/pure property tables live in
    simple_compat.el (do NOT load byte-opt.el itself).
  - cl-typep: GNU range types + "Unknown type" signaling.
- NEXT: continue down compat/oracle_tests_all.txt from selector 2488
  (`cargo run --release --bin compat-harness -- run --scope all
  --selector check-all --file FILE` per file) toward 3000.
- Verified through selector 2474/7080: `package-tests.el' (2438..2474,
  all 37 selected; harness check-all also matches
  package-test-update-archives-async) passes; 104-file prefix sweep
  (prefix-files9.txt) on frozen binaries is the gate.  Batch details in
  docs/compatibility-goal.md 2474 entry — READ IT before touching the
  reader's string/char escapes, replace-regexp-in-string,
  cl-defstruct constructors, let-alist, default-directory scoping,
  load resolution/load-history, coding-system EOL detection, process
  pumping, or the native url machinery; several are load-bearing:
  - Reader: escape modifiers chain only via backslash ("\C-^" is
    control-^); GNU ctrl fold set with "Invalid modifier in string"
    for leftovers ("\C-SPC" -> NUL in strings).
  - replace-regexp-in-string empty-match-past-scan fix; searches fold
    case per case-fold-search.
  - emaxx-struct-make: &rest consumes nothing positionally; &aux
    constructors pass slots as pure keywords from let* bindings.
  - let-alist binds the exact cdr.
  - default-directory is special + DEFVAR_PER_BUFFER (foreign-buffer
    lets don't capture setq; reads prefer the buffer's own local).
  - special-mode/parentless derived modes run kill-all-local-variables
    (change-major-mode-hook: tar-mode re-entry unswap).
  - Shared lisp-data syntax table char-table id 3 behind
    emacs-lisp-mode-syntax-table (ietf-drums keeps "J. R." dots).
  - load falls back to NAME.elc; nested load-history entries survive.
  - file-coding-system-alist GNU defaults; EOL detection for
    unspecified-eol codings in decode (bug#48137 path).
  - call-process INFILE errors are file-error (epg tty probe).
  - process-send-eof; accept-process-output pumps process pipes + url
    retrievals, SECONDS is arg 2, returns nil on timeout.
  - Native url-retrieve (worker thread + status plist with
    (:error (error http CODE)) for non-2xx), url-retrieve-synchronously,
    url-http-file-exists-p, url-insert (builtin-overridden);
    features url/url-http builtin-provided; simple_compat url-http
    surface; url-scheme-get-property delegates to loaded elisp.
  - simple_compat: substitute-quotes, lwarn, lisp-outline-level,
    outline surface + (provide 'outline), with-help-window mirrors
    help--window-setup; mail-fetch-field autoload; emacs-lisp-mode
    sets outline-regexp/outline-level locals.
- NEXT: `pcase-tests.el' (2475..2487, 13 selectors) — run
  `cargo run --release --bin compat-harness -- run --scope all
  --selector check-all --file test/lisp/emacs-lisp/pcase-tests.el`
  to see the current state, then continue down
  compat/oracle_tests_all.txt toward 3000.
- Verified through selector 2432/7080: `nadvice-tests.el' (2420..2432)
  passes the harness with ALL 13 selectors matching the oracle,
  including the two the ORACLE fails as :expected-result :failed
  (called-interactively-p-around/-filter-args — emaxx REPRODUCES those
  failures: the strict excess-args lambda arity and the
  called-interactively-p backtrace walk are what encode them).  The
  102-file verified-prefix sweep (prefix-files7.txt) on frozen
  binaries is the gate (2026-07-10 evening).  Batch details in
  `docs/compatibility-goal.md` 2432 entry — READ IT before touching
  oclosures, advice, macro/function cells, arity, or
  called-interactively-p; several of these are load-bearing semantics
  (identity-stamped slot frames, transparent oclosure env, macro
  shadow-renaming, function-frame markers for cl-flet).
- NEXT: `oclosure-tests.el' (2433..2437) — currently LoadError:
  "(wrong-type-argument string symbol)" while loading the file.
  sf_oclosure_define must handle the docstring + slot options
  ((name :mutable t)), keyword-arg copiers ((oclosure-test-copy ocl1
  :fst 7) — GNU copiers take &key when the :copier has no arglist),
  positional copiers with explicit arglists, accessor `documentation',
  and cl-defmethod dispatch on compiled-function/interpreted-function/
  oclosure/oclosure-test type hierarchy.  After that: package-tests
  (2438..2474).
- Groundwork already banked for FUTURE files: oracle simple.el now
  LOADS end-to-end (pre-redisplay-function + keyboard.c keymap defvar
  defaults were the blockers) — simple-tests.el/subr-tests.el (far
  beyond the current frontier, NOT in the verified prefix) get most of
  their functions from it under the harness loadpath.
- Verified through selector 2411/7080: `map-tests.el' (2350..2411)
  passes; the 99-file verified-prefix sweep on the frozen post-batch
  binaries is the gate (2026-07-10).  Batch details in
  `docs/compatibility-goal.md` (hash-table literal materialization at
  quote time, pcase `app'/pcase-macroexpander support, cl-typep vs
  reader markers, cl-no-applicable-method conditions, should-error
  error-conditions matching, eq-preserving alist-get removal, cXr setf
  places).
- Verified through 2419 pending sweep6 (2026-07-10): memory-report +
  multisession pass; big nadvice groundwork included (see
  docs/compatibility-goal.md 2419 entry for the full list: real
  oclosures, GNU nadvice.el/advice.el loading, macro↔function-cell
  bridge, defalias-fset-function protocol, structural lambda `equal',
  oclosure-frame-skipping function lookup).  nadvice-tests.el itself
  still has 7 mismatches — resume there: (a) `interactive-form' of the
  ad-Advice-* ASSEMBLED definitions returns nil (the (interactive "P")
  sits deeper in the assembled body than the native scanner looks) —
  fix that and interactive/preactivate/bug61179 likely follow;
  (b) call-interactively must evaluate the COMPOSED advice interactive
  spec (advice-eval-interactive-spec); (c) cl-print of advice objects
  "#f(advice car :after cdr)" via nadvice's cl-print-object method
  (needs cl-prin1 dispatch on oclosure types — cl-typep side is done);
  (d) advice-tests-advice/nadvice fail ONLY in the full-file run
  (cross-test contamination; use the member-prefix bisect);
  (e) called-interactively-p: oracle FAILS the -around and -filter-args
  variants — emaxx must MATCH those failures, not fix them.
  KEY GOTCHAS learned: body_has_marker inspects only the FIRST body
  form (stack exactly ONE closure marker); eval's frame-merge machinery
  unifies identically-shaped frames across closures (why oclosure
  bodies need :closure-isolated-current-env); the native
  add-function/advice-add arms are gated behind GNU nadvice once
  loaded; try_macroexpand honors preloaded macro autoload stubs that a
  native recognizer would shadow (add-function's stub).
- SWEEP7 REGRESSIONS — ALL RESOLVED (2026-07-10 afternoon):
  (1) edebug-tests.el = the add-function/remove-function MACRO
  AUTOLOADS to nadvice; they stay on the native arms (permanent,
  commented in preload.rs builtin_autoload_function) until edebug can
  instrument nadvice's gv-letplace output.
  (2) eieio-test-methodinvoke.el + eieio-tests.el = simple_compat.el's
  eieio--defmethod pass-through-primary gate compared
  `(eq (symbol-function method) #'ignore)` — a BuiltinFunc never eq a
  Symbol; it only ever "worked" because the old sf_defalias bug left
  the generic UNBOUND (fboundp nil).  Once the WIP defalias fix bound
  the generic to #<builtin ignore>, the gate failed, no pass-through
  primary was created, and the first :before wrapper was built WITHOUT
  its __emaxx_before_method_* qualifier frame — later registrations
  then grafted primaries ABOVE it (before/after ran out of order).
  Fix: gate on `(eq (indirect-function method) (symbol-function
  'ignore))`.  NOTE the oclosure graft in
  cl_defmethod_advice_original_binding was exonerated and is RESTORED.
  (3) dired-tests.el free-space tests = adding the verbatim elisp
  file-size-human-readable made the native insert-directory free-space
  line call it 1-arg (flavor nil → "10", no unit) instead of GNU's
  byte-count-to-string-function default file-size-human-readable-iec
  ("10 B").  Fix: simple_compat.el now defines
  file-size-human-readable-iec + byte-count-to-string-function, and the
  native free-space/get-free-disk-space paths funcall the variable like
  GNU files.el (get-free-disk-space also ported for real, formatting
  (nth 2 (file-system-info dir))).
  (4) cl-generic-tests.el test-11 (contaminated by test-09) =
  fmakunbound only removed the NEWEST functions-list entry; repeated
  defuns push duplicates, so the advice/method churn of test-09 left a
  stale dispatch lambda that resurfaced after fmakunbound.  Fix:
  fmakunbound now purges ALL entries (remove_all_function_bindings),
  like GNU voiding the function cell.
  DEBUGGING TECHNIQUE that cracked it: temporary eprintln! in the
  cl_defmethod qualifier walk + EMAXX_DBG_QUAL env-gated dump of the
  first closure frame names — decode the actual chain shape instead of
  bisecting blind.
- SWEEP HYGIENE (learned 2026-07-10): freeze the binaries before a
  long sweep (`cp target/release/{emaxx,compat-harness} /tmp/probes/bin/`
  and run the frozen harness — it resolves the emaxx binary as a
  SIBLING of its own path), because rebuilding mid-sweep swaps the
  binary under the harness.  The harness still runs `cargo build
  --quiet --bin emaxx` per file AS DEV; a concurrent root build takes
  the cargo lock and shows up as spurious `TIMEOUT/NONE` sweep lines —
  re-run those files individually before treating them as failures.
  Do NOT edit `src/lisp/*.el` while a sweep runs either: even a frozen
  binary reads simple_compat.el from the source tree at runtime.
- ORACLE TREE HYGIENE (cost the macroexp arc an hour): never let root
  write into /home/user/emacs — a root-owned
  `macroexp-resources/vk.elc` (from a direct root oracle probe) made
  the dev-user oracle's `batch-byte-compile` exit 1 under the harness,
  which read as "oracle fails this test".  Probe the oracle as dev, and
  when in doubt run `find /home/user/emacs -user root` and delete the
  turds.
- `eval-buffer' now performs GNU readevalloop EAGER top-level
  macroexpansion (macroexpand → top-level progn recursion →
  macroexpand-all → eval, falling back to the unexpanded form on
  expansion errors).  `load'/`load_file_strict' do NOT eager-expand
  yet — if a future test needs `macroexp-file-name' correct for macros
  expanded from a `load'ed defun body, port the same shape there.
- `provide' and the cl-defmethod recorder PREPEND their entries to
  `current-load-list' (GNU LOADHIST_ATTACH conses; the load-file name
  must stay LAST — `macroexp-file-name' reads
  `(car (last current-load-list))`, and preloaded-shim features like
  ert-x provide into the OUTER file's list).  Because of that,
  recording into `load-history' NREVERSES the list first (GNU
  build_load_history), keeping each history element (FILE . ENTRIES) —
  cl--generic-method-files and edebug read it that way.  If you add a
  new definition recorder, cons onto the front, never append.
- The `Compat 2100/7080` batch: `forward-sexp'/`scan-sexps' honor
  `parse-sexp-ignore-comments' (native emacs-lisp-mode sets it), reach
  buffer end instead of signaling over trailing comments, and
  `syntax-propertize' is ported (auto-buffer-local
  `syntax-propertize--done'; the fontification engine runs it first, so
  `syntax-propertize-rules' modes get their `syntax-table' text
  properties).  Details in `docs/compatibility-goal.md`.
- The `Compat 2084/7080` batch (see `docs/compatibility-goal.md` for the
  full list): simple_compat ports of the ert-x helpers (ert-x is a
  preloaded feature; its macros were void), GNU message_dolog()
  semantics for `message'/`message-log-max', `ert--test-buffers' +
  GNU test-buffer naming in native `ert-with-test-buffer',
  unnamed `(:type vector)' cl-defstructs stored as plain vectors (ewoc
  nodes, timers — `timerp' accepts 10-slot vectors), handler-bind
  condition lists, `ert-info' dynamic `ert--infos' binding, `symbol-file'
  ert--test type, a macro-shadowing shield for the pcase family (GNU
  pcase.el registers its backquote pattern under `\`' but the native
  reader produces `backquote'), order-insensitive
  `equal-including-properties', GNU `indent-rigidly'/`indent-line-to'
  edge cases, `lisp-indent-line' as native elisp-mode's
  `indent-line-function', and `font-lock-mode' running the buffer's
  `font-lock-function'.
- IMPORTANT probe-environment fact (cost an hour): the harness passes the
  FULL oracle load-path (`emaxx_upstream_load_path`) plus `-l ert`, which
  LOADS GNU ert.el/ert-x deps (pcase.el, ewoc.el...) on top of the native
  preloads.  Probes must replicate that: build /tmp/probes/loadpath.txt
  from the ORACLE's `load-path` (batch `--eval` printing `load-path`,
  plus test/lisp dirs), not just the test directories, or `-l ert`
  fails to resolve and probe behavior diverges from harness behavior.
- The `Compat 2056/7080` batch built the native fontification engine and
  the repairs it exposed (see `docs/compatibility-goal.md` for the full
  list): `font-lock-ensure' runs a syntactic pass (comments/strings from
  `syntax-ppss') plus a full keyword pass (regexp/function matchers,
  subexp highlights with OVERRIDE/LAXMATCH, anchored highlighters,
  FACENAME expressions) over `font-lock-defaults' installed lazily for the
  native modes (requiring lisp-mode.el/js.el for their keyword variables);
  standard font-lock face variables are self-quoting defvars;
  `font-lock-defaults' is auto-buffer-local (font-core.el defvar-local) —
  sh-mode's plain `setq' used to leak globally and kill fontification
  everywhere; `font-lock-ensure' + the native major modes (c/c++/java/js/
  javascript) are in `prefer_builtin_override' because loading GNU
  font-lock.el/cc-mode.el/js.el shadows them with redisplay-dependent
  elisp (cc-mode's elisp `c-mode' dies on void `backtrace-frame');
  `javascript-mode' delegates to `js-mode' (GNU defalias); `ert-pass'
  throws `ert--pass' (native runner counts it a pass), `ert-fail' signals
  `ert-test-failed', `ert-set-test' registers the ert-font-lock deftest
  macros' tests; `\s<'/`\s>' resolve from the syntax table's explicit
  comment-class entries; `regexp-opt' honors PAREN (a shy-group PAREN bug
  silently killed every js keyword subexp match).
- Fontification-debugging leverage for future frontiers:
  `EMAXX_DEBUG_FONTLOCK=1' traces installer decisions, matcher attempts,
  and highlight applications; cross-test contamination in a suite is
  bisected fast with `(ert-run-tests-batch-and-exit '(member TEST-A ...
  TARGET))' — alphabetical run order, so test the failing target behind
  successive prefixes of its predecessors.
- The `Compat 2016/7080` batch finished eieio-tests.el on top of the
  groundwork+persist batches: `cl-no-next-method'/`cl-no-applicable-method'
  hooks (runtime helper `emaxx--cl-generic-apply-next' routes the dispatch
  chain's `ignore' sentinel to the hooks; single-method generics check
  their specializers; simple_compat lowers obsolete `no-next-method'/
  `no-applicable-method' defmethods per eieio-compat.el), in-place method
  REPLACEMENT for same-qualifier+specializer re-registration (splicing a
  duplicate wrapper made same-condition wrappers point at each other —
  infinite condition loop / Rust stack overflow once nothing matched),
  `defgeneric' over an existing non-generic errors (follows defalias
  chains; `generic-p' defined), native exact `same-class-p' + GNU `NAME-p'
  (exact) / `NAME--eieio-childp' (subclass) predicates,
  `eieio--class-children' returns symbols, `oref-default'/`oset-default'
  accept instances, class-allocated `oref-default' returns the
  `eieio--unbound' marker unsignaled.  Sweep-demanded repairs: the
  `(subclass CLASS)' dispatch condition resolves `eieio-defclass-autoload'
  stubs via `autoload-do-load' like GNU's subclass generalizer, and slot
  descriptors merge per GNU's storage model (each class's merged view is
  copied parent-by-parent, so a parent's own redeclarations of ancestor
  slots survive into subclasses — five cedet files depended on
  semanticdb's `tracking-symbol' initform).  Details in
  `docs/compatibility-goal.md`.
- Verified through selector 2106/7080: `find-func-tests.el` passes its
  grouped `check-all` replay.  The `Compat 2106/7080` batch finished the
  file with a simulated-minibuffer completion engine: `completing-read'
  consumes queued `unread-command-events' (ert-simulate-keys) as a key
  loop — self-inserting chars, RET submits, TAB completes via
  longest-common-prefix over the table, a trailing-slash retry (completes
  the component before a final "/"), and a component-wise
  partial-completion expander for "o/org"-style patterns;
  `filtered_completion_matches' accepts FUNCTION completion tables
  (calling (TABLE STRING PRED t)); `locate-file-completion-table' is
  ported to simple_compat.el (candidates carry the directory part of
  STRING so plain prefix matching reproduces GNU's boundaries behavior).
- KNOWN ENVIRONMENT FLAKE (do not chase):
  `auto-revert-test02-auto-revert-deleted-file-remote'
  (autorevert-tests.el) flips between PASS and FAIL depending on
  container state — it failed 3/3 on a PRISTINE parent-commit tree in the
  same session where it had passed an earlier sweep.  Compare against the
  parent commit before attributing it to your diff (an hour was lost
  bisecting simple_compat.el for it; a nonsense stub "reproduced" the
  failure because the test is simply unstable).  todo-mode-tests.el
  remains the other known retry-flake.
- Verified through selector 2199/7080: `float-sup-tests.el' (2107)
  passed as-is and `generator-tests.el' (2108..2199) passes all 92.
  Backquote templates now ALWAYS expand to list/append constructor code
  under macroexpand-all (backquote_template_code).  Two regressions that
  gate change caused and their fixes, for the record:
  - `',(f)' reads as (quote (comma (f))); quoted forms are only opaque
    to the converter when NO unquote appears inside
    (template_tree_unquotes) — otherwise the unquote silently became a
    literal `comma' form (bytecomp's dodgy-args warnings vanished
    because the compiled args stopped being literals).
  - Dotted unquote tails emit (append (list ...) TAIL); emaxx's `append'
    flattened a STRING as the last argument instead of reusing it
    verbatim as the tail — GNU: (append '(2) "b") => (2 . "b"); fixed in
    the native append (last arg now verbatim even for strings/vectors).
  - pcase forms keep their backquote PATTERNS: macroexpand-all walks
    only pcase clause bodies (patterns/bindings verbatim) — full opacity
    breaks cl-labels rewriting inside pcase bodies (ert-x-tests caught
    both mistakes).  The
  generator batch, beyond the cl-macrolet groundwork:
  - `macroexpand-all' now yields GNU shapes for the whole macro family
    the CPS transformer consumes: `cl-symbol-macrolet' substitutes
    variable references (shadowing-aware; that fixed the entire
    void-lexical-var cluster — the "doubled gensym" theory was wrong,
    the binding rename simply never applied), `setf' with symbol places
    → `setq', `push'/`pop'/`cl-incf'/`cl-decf', `prog2' →
    progn+prog1, and BACKQUOTE TEMPLATES now expand to list/append
    constructor code (backquote_template_code in macros.rs; both the
    try_macroexpand shield and macroexpand-all's template walk convert).
    The last is the most cross-cutting macro change yet — sweep suspect
    #1 for any regression.
  - `(signal SYM NON-LIST-DATA)' now produces the dotted condition value
    (SYM . DATA) like GNU (generator's iter-end-of-sequence carries the
    final value that way); LispError::condition_type reads the car of
    dotted values.
  - `condition-case' handler matching consults the signaled symbol's
    `error-conditions' property when present (define-error hierarchies;
    generator tests define cps-test-error with a custom condition list).
  - simple_compat: `inline' macro (byte-run.el progn marker), edebug
    autoloads (`edebug-defun'/`edebug-eval-top-level-form').
  - WARNING repeated from this batch's false trail: an EMPTY result JSON
    (load_error) makes "notpassed 0" look like success — always check
    the TOTAL, not just the failure count.
- Verified through selector 2207/7080: `gv-tests.el' (2200..2207, all 8
  selected) passes its grouped replay.  Lessons from this batch:
  - NEVER shadow a loadable GNU macro with a Rust special-form arm.  A
    `gv-define-setter' special-form shortcut looked harmless but it also
    intercepted gv.el's OWN internal registrations when gv.el loaded
    (gv-define-simple-setter expands to gv-define-setter), so `car'/`cdr'
    never got their `gv-expander' properties.  Downstream, GNU cl-macs'
    `cl-callf cdr (car cursor)' in `edebug-move-cursor' could not expand
    to `setcar' and silently fell back to a copying path — breaking the
    cons IDENTITY (`eq') that edebug's `&name' spec matcher asserts on
    (`gv-setter-edebug' failed in-suite with cl-assertion-failed only
    after anything loaded gv.el).  The fix: remove the shortcut and give
    `resolve_setf_place' a LAST-RESORT arm consuming the standard
    `gv-expander' property via gv-get's DO protocol (loops.rs); preload.rs
    already autoloads gv.el for the gv-define-* macros.  Place the arm
    after all native place arms — gv.el registers expanders for car/cdr/
    get/... that natives must outrank.
  - `plist-get' and `cl-getf' have DIFFERENT third arguments: plist-get
    takes a PREDICATE, cl-getf takes a DEFAULT (the setter evaluates but
    ignores it).  Passing cl-getf's default as a testfn funcalls an
    integer (caught by cargo test, not the compat suite).
  - GNU's gv expander for plist-get/cl-getf PREPENDS missing keys
    ((setf (plist-get l :d) v) => (:d v . l)); plist-put appends.  A
    stale Rust unit test asserting append order had to be updated.
  - `(setf (get S P) V)' routes to `put' (sf_setf arm).
- Verified through selector 2275/7080: `hierarchy-tests.el' (2208..2275)
  passes all 68.  Lessons from this batch:
  - 56 of 57 initial failures shared ONE root cause: `(require 'map)'
    no-oped because "map" sat in `is_compat_preloaded_feature', so GNU
    map.el (cl-generic `map-put!'/`map-insert', the `map-elt'
    gv-expander) never loaded.  GNU does NOT preload map.el (under `-l
    ert' the oracle has it loaded via the dependency chain).  Fix:
    require now prefers the real file when it resolves on the load-path
    and keeps the compat shim as the no-file fallback.  When a compat
    feature's tests fail with void-SOMETHING, check whether the shim is
    eclipsing a real GNU library before porting functions one by one.
  - `cl-defgeneric' now processes `(declare (gv-expander (lambda (do)
    ...)))' like `gv--defun-declaration': the expander takes DO plus the
    generic's own lambda list (map-elt's setf machinery needs it).
  - `setf' of `alist-get' must mutate a FOUND pair with `setcdr' — the
    alist stays `eq' and map-put! uses exactly that to detect in-place
    updates — and only assign the place when prepending a missing key
    (GNU prepends) or removing.
  - GNU text-property order: `add-text-properties'/`put-text-property'
    replace existing entries IN PLACE and cons NEW properties onto the
    interval-plist head, so `text-properties-at' lists later additions
    first ((add a b) then (add c) reads (c b a)); `propertize' and
    `set-text-properties' preserve the given plist order verbatim.
    hierarchy's make-text-button test asserts (car properties) eq
    'action because of this.  Cross-cutting: swept clean.
  - `tabulated-list-mode' autoloads tabulated-list.el (GNU preloads it
    through buff-menu.el).
  - `(kill-emacs 0)' is NOT defined in emaxx — probes using it as a
    final `--eval' exit 2 after the load completes; harmless for probe
    outputs written during load, but do not read that exit code as a
    load failure.
- Verified through selector 2287/7080: `icons-tests.el' (2276..2277),
  `let-alist-tests.el' (2278..2284), `lisp-mnt-tests.el' (2285..2287).
  Lessons:
  - GNU `pcase-let'/`pcase-let*' DESTRUCTURE ONLY — they never test.
    icons.el binds with `(,parent ,spec _ _): inside a backquote pattern
    a bare symbol (including `_') is a LITERAL eq-test in real pcase,
    but pcase-let drops all such membership tests.  emaxx's lenient
    binder (pcase_pattern_bindings_inner, eval.rs) now skips literal
    symbol/keyword comparisons inside backquote when lenient.
  - `let-alist' was ANOTHER special-form-shadows-GNU-macro case (same
    disease as gv-define-setter): the native form can't do nested
    `.sublist.foo' or `..outer' escapes and fails the exact
    macroexpansion test.  The core.rs arm now defers to a Lisp macro
    named let-alist when one is defined (the test file requires
    let-alist), falling back to sf_let_alist for file-less runs (cargo
    unit tests use it).  PATTERN: when adding a special-form arm for
    something GNU implements in loadable elisp, gate it on the Lisp
    definition being absent.
  - `gnutls-available-p' returns nil (GNU --without-gnutls build);
    package.el evaluates it at LOAD time so it must exist
    (lisp-mnt's lm-package-requires requires package).
- The next frontier is `test/lisp/emacs-lisp/lisp-mode-tests.el`
  (21 selected; manifest line 2384).  In-progress groundwork took it
  from 13 failing to 7: parse-partial-sexp now tracks element 2 (start
  of last complete sexp, per-level; token STARTS record it), supports
  STOPBEFORE (stop before any sexp start; open parens included —
  lisp-indent-specform needs it), stops on TARGETDEPTH crossings in
  BOTH directions, and honors COMMENTSTOP='syntax-table (stop after
  string/comment boundaries — lisp-indent-calc-next crosses multi-line
  strings with it); `backward-prefix-chars' is native; the native
  emacs-lisp-mode installs GNU lisp-data syntax entries (non-alnum
  ASCII = symbol constituent, `'``,#' prefix, `@' "_ p", `.' symbol)
  plus comment-start-skip ";+ *", comment-indent-function
  `lisp-comment-indent', comment-column 40; autoloads: newcomment.el
  (comment-indent/indent-for-comment), prolog-mode, cl-indent
  (common-lisp-indent-function); `up-list' accepts negative COUNT.
  FINISHED (selector 2308/7080; all 21 pass the grouped replay).  The
  closing batch's lessons:
  - `indent-region' must dispatch to the buffer-local
    `indent-region-function' (lisp-indent-region); the native
    emacs-lisp-mode sets it.
  - GNU's PRELOADED `(declare (indent N))' properties (196 symbols:
    when 1, defun 2, with-eval-after-load 1, ...) are registered
    natively at startup — enumerate them from the oracle with mapatoms
    over `lisp-indent-function' props, do not hand-pick.
  - `indent-according-to-mode' funcalls the buffer's
    `indent-line-function' (newcomment's comment-indent delegates
    comment-only lines through it).
  - `comment-column' is 40 in lisp modes (lisp-mode-variables), not the
    global 32.
  - REGRESSION CLASS TO REMEMBER: forward regexp search converted the
    char START position into `captures_from_pos''s BYTE offset — with
    multibyte text before point `re-search-forward' returned matches
    BEFORE point, and GNU skip-and-retry matcher loops
    (lisp--match-confusable-symbol-character) inflooped.  If an elisp
    while/re-search loop hangs, CHECK THE SEARCH-START OFFSET UNITS
    FIRST.
  - `(face FACE PROP VAL ...)' font-lock FACENAMEs add the extra plist
    as text properties (GNU font-lock-apply-highlight) then fontify
    with FACE.
  - prolog.el's load chain pulls GNU font-lock.el over the compat
    `font-lock-fontify-region'; it is now in prefer_builtin_override
    with a native arm (ensure + `emaxx--font-lock-fontify-region-extras'
    from faces_compat.el).  When a previously-passing fontification
    test breaks after a NEW MODE's library loads, suspect font-lock.el
    shadowing before anything else.
  - Field-constrained line motion: beginning-of-line/
    line-beginning-position/back-to-indentation/indent-line-to
    constrain to the current field when the buffer has any `field'
    property (Bug#32014's read-only prompt).  The check is gated on
    buffer_has_field_property to keep field-less buffers on the fast
    path.
  - Inserting a PROPERTIZED STRING grafts its plist verbatim
    (set_text_properties); the interval prepend rule applies only to
    add-text-properties/put-text-property on EXISTING text.
- The next frontier per the manifest is whatever follows
  `lisp-tests.el` (check compat/oracle_tests_all.txt after line 2444).
- lisp-tests.el FINISHED (selector 2345/7080; all 37 pass the grouped
  replay and the harness).  Final round of fixes beyond the earlier
  notes: `(end-of-line 0)'/`(beginning-of-line N)' honor COUNT (the
  ported forward-paragraph's backward loop does (end-of-line 0));
  python triple-quote fences via `emaxx--python-syntax-propertize'
  (native python-mode sets syntax-propertize-function; parse_forward
  honors syntax-table TEXT PROPERTIES; GenericStringDelimiter opens/
  closes fence strings with nth 3 = t; the sexp scanner's
  lisp-prefix shortcut must EXCLUDE fence-classed `'''` quotes);
  forward-sexp runs syntax-propertize like GNU scan primitives.
  HISTORICAL (in-progress notes, superseded):
  MAJOR FIXES SINCE THE 22/37 NOTE (all uncommitted):
  - scan-lists depth-crossing NEVER MOVES POINT (parse_forward does;
    save/restore around it — up-list's scan-error handler reads
    syntax-ppss AT POINT, so a moved point broke string escape).
  - forward-sexp/backward-sexp rebuilt on the scan-sexps contract,
    ONE SEXP PER STEP: Ok(Some)→move; on None/Err find the obstacle
    (next/previous_code_position helpers in syntax.rs): no obstacle →
    buffer-end + nil (Bug#13994); obstacle → scan-error
    ("Containing expression ends prematurely" LEFT RIGHT) — up-list's
    forward-sexp-function path needs (nth 3 err).
  - syntax-ppss MOVES POINT to POS like GNU (not excursion-saving!) —
    beginning-of-defun-comments depends on it; fixed all mark-defun
    tests.
  - scan_one_sexp_forward: GNU symbol runs are Word|Symbol|Quote (+
    Escape/CharQuote consuming 2); PUNCTUATION never joins a run and
    never starts a sexp (skip+recurse); PairedDelimiter ($) scans to
    the matching character like a string.
  - text-mode-syntax-table is a REAL table now (char-table id 2 at
    interpreter init, parent standard): `"' and `\' punctuation, `''
    "w p" (Bug#15014); define-derived-mode's expansion now does
    (set-syntax-table MODE-syntax-table) when bound.
  - simple_compat: +paragraph-start/paragraph-separate defvars,
    +move-to-left-margin/current-left-margin (indent.el ports).
  REMAINING 3 FAILURES (updated):
  - lisp-fill-paragraph-colon: REAL FILLING NOW WORKS at the
    fill-region-as-paragraph level (verified: fill-column 10 wraps
    correctly).  Fixes so far: simple_compat gained fill-prefix/
    left-margin/sentence-end*/colon-double-space defvars, paragraphs.el
    `sentence-end' defun + sentence-end-base port, indent.el
    move-to-left-margin/current-left-margin ports; native
    char-category-set returns a 128-slot BOOL-VECTOR (was "" — fill.el
    does (aref (char-category-set next) ?|)); native emacs-lisp-mode
    sets fill-paragraph-function = lisp-fill-paragraph.  REMAINING
    PROBLEM: lisp-fill-paragraph's docstring branch reaches the
    narrowed-docstring fill but nothing changes; ppss accessors are
    correct (probe fp10), fill-comment-paragraph correctly nil (fp9).
    DONE SINCE: paragraphs.el forward/backward-paragraph PORTED
    verbatim to simple_compat (+use-hard-newlines/
    paragraph-ignore-fill-prefix defvars); the native blank-line
    forward-paragraph arm is gated on
    !interp.has_lisp_function("forward-paragraph") (new helper in
    runtime.rs); constrain-to-field accepts GNU's 5-arg form.  RESULT:
    the second test block (docstring keywords, Bug#7751) fills
    EXACTLY like the oracle now (probe fp1).  REMAINING: the FIRST
    block (defcustom with keywords below, Bug#24622) signals "End of
    buffer": trace (probe fp12) shows (forward-paragraph -1) →
    (forward-char 1) at eob inside the ported backward loop — some
    primitive divergence (suspects: re-search-backward "^\n" within
    the docstring narrowing, or looking-at with the let-bound
    paragraph regexps using \s- atoms) puts point at point-max where
    GNU does not.  Compare each step of forward-paragraph -1 in the
    narrowed docstring against the oracle.
    ALSO: fill machinery groundwork landed: char-category-set returns
    a 128-slot bool-vector (was ""), sentence-end defun +
    sentence-end-base/without-space ports, fill-prefix/left-margin/
    colon-double-space/sentence-end* defvars,
    fill-paragraph-function=lisp-fill-paragraph in native
    emacs-lisp-mode.  fill-region-as-paragraph verified filling
    correctly at fill-column 10.
  - lisp-forward-sexp-python-triple-quoted/quotes-string: forward-sexp
    over python """...""" needs python syntax-propertize fences;
    check whether syntax_entry_at_buffer_position honors syntax-table
    TEXT PROPERTIES and whether native python-mode sets
    syntax-propertize-function; GNU's scan primitives run
    syntax-propertize implicitly.
  ORIGINAL note (kept for context):
  - simple_compat.el: verbatim GNU lisp.el ports of `up-list' (full
    escape-strings/no-syntax-crossing signature), `backward-up-list',
    `delete-pair', `mark-defun', `beginning-of-defun-comments', plus
    `forward-sexp-function'/`insert-pair-alist'/`delete-pair-blink-delay'
    defvars, simple.el `activate-mark', subr-x
    `with-buffer-unmodified-if-unchanged'.  READER GOTCHAS that cost an
    hour: emaxx's reader rejects `?\`'-style char literals (use
    integer codes) and a truncated defcustom→defvar conversion left an
    UNBALANCED form — emaxx then fails at STARTUP with "End of file
    during parsing" on every invocation (simple_compat.el is read from
    the source path at runtime; no rebuild needed for elisp edits, and
    the oracle's reader accepts the file, so bisect with emaxx itself
    form-by-form).
  - The native `up-list' dispatch arm was REMOVED (buffer_edit.rs +
    dispatch.rs name lists) so the elisp port takes effect; `down-list'
    still native.  SWEEP REQUIRED before committing.
  - Native `frame-selected-window' (= selected-window) in display.rs;
    `fill-paragraph' autoloads fill.el in preload.rs.
  REMAINING FAILURES and leads:
  - DONE since first note: scan_lists_impl supports (scan-lists POS ±1
    DEPTH) depth-crossing (forward via parse_forward with
    target_depth=-DEPTH — the unmatched-close and mismatched-close
    branches must ALSO check target_depth; backward via the enclosing
    open-paren stack scan).  up-list-basic, up-list-no-cross-string and
    backward-up-list-basic pass now.
  - up-list-cross-string / up-list-out-of-string: "Unbalanced
    parentheses" — point starts INSIDE a string; the forward parse
    starts with a fresh state so the string's closing quote reads as a
    string START (regions inverted).  The elisp up-list exits strings
    first only with escape-strings; for the crossing variants GNU
    scan-lists itself tolerates starting mid-string.  Probe GNU
    scan-lists semantics from inside strings before coding
    (/tmp/probes/ul1.el pattern; compare oracle).
  - mark-defun-*: point should be RESTORED on the (= point before)
    checks — likely push-mark/save-excursion or
    beginning-of-defun-comments interplay.
  - lisp-forward/backward-sexp-2-*: expect scan-error signaling at
    eobp/bobp with COUNT 2 (check GNU forward-sexp error contract).
  - lisp-fill-paragraph-colon: `paragraph-start'/`paragraph-separate'
    defvars missing (C defvars in GNU; add to bindings or
    simple_compat).
  - python triple-quoted forward-sexp: needs python-mode string
    scanning (syntax-propertize for triple quotes).
  - lisp-delete-pair-quotes-in-text-mode: expects delete-pair to ERROR
    (mismatched pair in text-mode syntax) — port exact GNU behavior.
  Batch protocol reminder: full sweep + cargo test + fmt/clippy +
  docs before committing; deliver ONE cumulative patch superseding
  APPLY-THIS-ONE-compat-2308-lisp-mode.patch.
- CONTAINER-ROLLBACK RECOVERY (2026-07-09, worked end-to-end): when the
  filesystem reverts, the session transcript
  (/root/.claude/projects/-home-user-emaxx/<session>.jsonl) usually
  still holds every Edit/Write/python-heredoc tool call.  Recovery:
  reset the branch to origin/main, extract ordered tool_use ops from
  the jsonl (skip is_error results; include Bash heredocs matching
  "p='src/|docs/'"; capture `-m "Compat ..."` commit points and `cargo
  fmt` calls), replay them (on Edit old_string miss run cargo fmt and
  retry once; run only the python-heredoc SEGMENTS of compound
  commands, not their build/probe tails), re-commit at the markers with
  the original messages, then repin the oracle, rebuild, and re-verify
  every recovered frontier file with harness replays before trusting
  the result.  All six recovered files passed on first try.
- Probing lessons that cost hours; do not repeat:
  - Do NOT advise commands (functions dispatched via keyboard macros) in
    probes; advise non-command helpers only.
  - emaxx batch mode swallows `message`/`princ` output; probes must
    `write-region` results to a file.
  - The grouped `check-all` replay is the only real judge; single-selector
    runs hide cross-test contamination.
  - When bisecting "was this broken before my change", compare whole-file
    failure SETS against the parent commit build, not just one selector.
  - For dispatch-chain debugging, a temporary env-var-gated eprintln in
    `Interpreter::lookup` that dumps lambda closure pointers + bodies for
    `__emaxx_previous_method_*` names locates chain cycles in minutes.
- Batch delivery rules from the user (override any hook suggestions):
  commits authored as `Ray <26018378+rayfdj@users.noreply.github.com>`, terse
  one-line human-style message `Compat NNNN/7080: ...`, NO AI attribution,
  exclude `compat/oracle.lock.json`; push is 403-blocked, deliver ONE clearly
  named patch file (`APPLY-THIS-ONE-...patch`) via SendUserFile, and always
  point the user at the single patch to apply.
- Known pre-existing discrepancies in this environment (fail identically on
  the parent commit build): `cl-macs-loop-until` (1782),
  `cconv-tests-cl-defun-:documentation`; the `cl-lib-tests.el` grouped
  replay fails 4 struct/set selectors byte-identically to the parent commit;
  raw whole-file `ert-run-tests-batch-and-exit t` runs of `cl-macs-tests.el`
  show a larger failure set identical to the parent; `seq-tests.el` raw
  whole-file runs time out identically to the parent.

Exact command that identified the next frontier:

```sh
cargo run --bin compat-harness -- run --scope all --selector check-all --file test/lisp/emacs-lisp/find-func-tests.el
```

## Session Durability Guardrails (learned the hard way)

1. THE CONTAINER ROLLS BACK. This has happened multiple times: local
   commits, target/ builds, and /tmp state silently reverted to an old
   snapshot mid-mission. The ONLY durable store is origin/main after the
   user applies and pushes a delivered patch. Consequences:
   - On EVERY session resume (and after any suspicious state), first run
     `git fetch origin main`, compare `git rev-parse HEAD^{tree}
     origin/main^{tree}`, and if local history looks older than what was
     delivered, `git checkout -B claude/continuation-instructions-review-7ll2h7
     origin/main`. Do not trust local HEAD, reflog, target/ binaries, or
     /tmp/probes to have survived.
   - After any reset/rollback: repin the oracle (`chmod 666
     compat/oracle.lock.json`, then as dev `./target/release/compat-harness
     oracle pin --emacs /home/user/emacs/src/emacs --repo /home/user/emacs`),
     rebuild `cargo build --release`, and recreate /tmp/probes
     (prefix-files.txt regenerates from compat/oracle_tests_all.txt by
     taking every `^test/...: discovered=` line through the last verified
     file; sweep.sh loops compat-harness check-all over it as dev).
   - Commit and DELIVER the patch as soon as a batch is green. Unpushed
     work is one rollback away from oblivion; the delivered
     APPLY-THIS-ONE patch is the real backup.
2. When several commits accumulate before the user applies anything,
   regenerate ONE cumulative patch (`git format-patch origin/main..HEAD
   --stdout`) and tell the user explicitly that it SUPERSEDES earlier
   patch files. Never leave two live patches ambiguous.
3. The stop hook complaining about uncommitted changes when `git status`
   shows ONLY `compat/oracle.lock.json` is a false positive: that file is
   the container-local repin and must never be committed.
4. Cross-cutting changes (cl-generic dispatch, reader, printer, `equal',
   records, file-attributes, time formats) REQUIRE a full prefix sweep
   before committing, no matter how local they feel: the typed-oset work
   regressed three srecode files through `file-attributes' time shapes,
   and an eieio--object-class tweak broke persist. If the sweep is not
   green, the batch is not done.
5. Probe/debug hygiene that repeatedly wasted cycles:
   - `cmd | head; echo exit=$?` reports HEAD's exit code. Redirect to a
     file and echo the real code, then read the file.
   - DELETE stale probe artifacts (result JSONs, error-capture files)
     before reruns; a stale file has misdirected debugging twice.
   - emaxx exiting 2 with an EMPTY log is a LOAD error whose message was
     swallowed by batch; capture it with `(condition-case err (load ...)
     (error (write-region (format "%S" err) ...)))`.
   - Exit 134/SIGABRT with "stack overflow" during dispatch-heavy code is
     almost always a cl-generic wrapper chain cycle; an env-gated
     eprintln in the ERT runner loop (EMAXX_DEBUG_ERT) finds the test.
   - Run emaxx probes as dev via `bash /home/user/asdev.sh "..."`, never
     as root (root-owned /tmp lock files poison later oracle runs).

## Oracle Setup In A Fresh Container

If the sibling `../emacs` checkout is missing (fresh cloud container), rebuild
the oracle before anything else. GNU/GitHub sources are blocked by the network
policy, but `archive.ubuntu.com` is reachable:

1. `curl -O http://archive.ubuntu.com/ubuntu/pool/universe/e/emacs/emacs_30.2+1.orig.tar.xz`,
   extract to `../emacs`, `git init` + commit it.
2. The Ubuntu `+1` repack strips GFDL docs and `admin/unidata/IVD_Sequences.txt`;
   stub `doc/{emacs,lispintro,lispref,misc}/Makefile.in` with no-op targets
   (include a `.SUFFIXES:` override so `org.texi` matches the catch-all rule)
   plus minimal `emacs.texi`/`emacs-lisp-intro.texi`/`elisp.texi` files that
   carry a `@direntry`, and `touch admin/unidata/IVD_Sequences.txt`.
3. `apt-get install build-essential pkg-config texinfo autoconf libgnutls28-dev
   libncurses-dev libgccjit-13-dev libgmp-dev libxml2-dev libsqlite3-dev
   zlib1g-dev`, then `./autogen.sh && ./configure --with-native-compilation
   --without-x && make -j$(nproc)`.
4. Repin: `cargo run --bin compat-harness -- oracle pin --emacs
   ../emacs/src/emacs --repo ../emacs`. Leave the resulting
   `compat/oracle.lock.json` change uncommitted; the darwin pin in git history
   stays canonical.
5. Run the Rust test suite as a non-root user; several tests assert
   unwritable-file behavior that is vacuous under root.

## Non-Negotiable Rules

1. Preserve identical observable behavior to GNU Emacs for the selected tests.
2. Fix behavior honestly in Rust. Do not hardcode test answers, delegate to the
   oracle at runtime, or add special cases that only recognize test data.
3. Keep formatting clean. Run `cargo fmt`; final verification must include
   `cargo fmt --check`.
4. Keep clippy clean. Final verification must include
   `cargo clippy --all-targets --all-features -- -D warnings`.
5. Keep tests clean. Final verification must include `cargo test`.
6. Keep diffs clean. Final verification must include `git diff --check`.
7. Do not revert unrelated user or generated changes. If the worktree is dirty,
   inspect it and work with relevant changes.
8. Use focused, strategic patches that follow existing code patterns.
9. Add regression tests for behavioral changes when there is a local Rust test
   surface for the behavior.
10. Commit and push each coherent passing compatibility batch before moving to
    the next frontier.
11. Preserve the current Elisp/host-language boundary.  Use GNU source to learn
    the observable contract, then implement it through Emaxx's existing Lisp
    compatibility layer or Rust runtime as appropriate; do not move behavior
    across that boundary merely to make a selector pass.

## Working Method: Thematic Fixes With Controlled Refactoring

The ordered oracle is a discovery tool, not a request to patch 7080 symptoms
one at a time.  For every mismatch:

1. Reproduce the smallest observable difference and inspect the GNU call path
   far enough to identify the semantic contract being exercised.  Read GNU
   source for intent and confirm ambiguous details with probes; do not copy its
   implementation or abandon Emaxx's idiomatic Rust architecture.
2. Search nearby failures and existing implementation shortcuts for the same
   contract.  Preload state, lexical/dynamic binding, object identity, buffer
   versus window state, event ordering, and completion tables are recurring
   themes that often explain a whole cluster.
3. Add a focused Rust regression that is red before the fix and fast enough to
   run routinely.  Include a negative/order-dependent case whenever the
   tempting implementation could synchronize or mutate too broadly.
4. Fix the shared Rust abstraction boundary.  Do not encode a selector name,
   fixture value, timing coincidence, or one library's private spelling.
5. Replay the exact selector, then its grouped file when practical, plus
   nearby oracle files affected by the shared boundary.  The Rust suite catches
   local regressions quickly; only the oracle establishes GNU compatibility.

`--selector check-all` resolves to ERT selector `t` and therefore runs tagged
expensive/remote tests too; `--scope automated` filters files, not tests inside
a file.  For the canonical 7080 denominator, use the manifest selectors (or
`--selector default` where the manifest selected precisely the untagged set).
Use `check-all` deliberately as the broader file stress test, with a timeout
appropriate for its expensive tests, and do not misreport its larger test set
as the canonical denominator.

Opportunistic cleanup is encouraged only inside the code directly implicated
by the failure.  Refactor convoluted, non-idiomatic, duplicated, or
wrong-abstraction-level Rust when doing so makes the semantic fix smaller and
more testable.  Keep that cleanup in the same bounded subsystem, preserve
behavior with focused tests, and defer unrelated architecture work so frontier
progress remains reviewable.  If a broad first design causes unrelated tests
to fail, narrow it using real identity/lifetime/order information instead of
adding exceptions.

Record both the theme and the rejected overly broad approach in this handoff.
That prevents future continuations from rediscovering the same traps and keeps
the project moving by semantic clusters rather than oracle whack-a-mole.

## Deferred Post-7080 Performance Work

- Create/track GitHub issue **"Optimize Dired listings for large directories
  after 7080 compatibility"**.  During the 3125+ cumulative replay,
  `dired-test-bug27496` and `dired-test-bug30624` exceeded a 20-second probe
  when the host `$TMPDIR` contained roughly 1,000 entries; both passed when run
  against a clean isolated temp directory.  This is not an Eshell or
  `process-send-string` correctness failure: the tests enumerate the system
  temp directory, and Emaxx's Dired/file path scales much worse than GNU's on
  a large directory.  Defer optimization until all 7080 selected compatibility
  tests are green, then profile the shared directory enumeration/stat/Dired
  path and optimize the producer boundary with correctness regressions intact.
- The issue was prepared on 2026-07-19, but both the GitHub integration and the
  local `gh` credential lacked issue-write access (`github_create_issue`
  returned HTTP 403; `gh auth status` reported an invalid token).  Retry issue
  creation once GitHub issue-write authentication is restored; until an issue
  URL is recorded here, this section is the durable tracking record.

## Compatibility Harness Usage

Use the harness to compare emaxx against the sibling GNU Emacs checkout. The
normal exact replay shape is:

```sh
cargo run --bin compat-harness -- run --scope all --selector SELECTOR --file PATH/TO/TEST.el
```

Do not compare revisions by copying `compat-harness`, copying `emaxx`, or
sharing `CARGO_TARGET_DIR`.  The harness embeds its compile-time source/target
identity and rejects execution after being copied.  Use `--subject-root` to
run an archived baseline; every subject is synchronously built with `--locked`
in its own source-owned `target/compat-subject` cache.  The exact source tree,
subject binary, harness, oracle helper, and GNU binary are hashed, recorded in
`summary.json`, and rechecked before the summary is accepted.  A nonblocking
subject lock rejects concurrent gates against the same build target.

For revision comparisons, run the same current release harness once with
`--subject-root BASELINE` and once without it, using the same explicit
`--timeout-seconds`.  Save the two artifact paths printed by the harness, then
run:

```sh
cargo run --release --bin compat-harness -- compare-subjects --baseline BASELINE_ARTIFACT --candidate CANDIDATE_ARTIFACT
```

`compare-subjects` deliberately rejects legacy/incomplete summaries and
artifacts that differ in harness, oracle executable/helper, selector, exact
file list, name filter, profile, or timeout.  Pass-to-skip, missing results,
and extra results are not silently treated as success.  Do not weaken these
checks to reuse an old artifact: regenerate both sides with the current
harness.  The default timeout is 120 seconds; CLI values take precedence over
`EMACS_TEST_TIMEOUT`.  Artifact directory names use nanosecond time plus PID
and are created exclusively, so a run cannot append to an older run.

For the next known frontier, run:

```sh
cargo run --release --bin compat-harness -- run --scope all --selector test-font-lock-test-file--correct --file test/lisp/emacs-lisp/ert-font-lock-tests.el
```

The grouped reproduction for all three currently known failures uses
`--selector default` on the same file.  After fixing a selector, exact-replay
that selector. Then probe the next canonical selector from
`compat/oracle_tests_all.txt` and record the result in
`docs/compatibility-goal.md`.

## Prefix Replay Policy

Do not mechanically run the full selected prefix `1..N-1` before every selector
`N`. That is too expensive for small batches and has not been the working
cadence.

Required before committing a batch:

- Exact-replay every selector fixed by the batch.
- Run focused Rust regression tests for the changed behavior.
- Run targeted compatibility replays for nearby or impacted selectors when the
  change touches shared evaluator, reader, primitive, buffer, file, process, or
  byte-compiler behavior.
- Broaden to grouped file or prefix replays when the implementation risk is
  broad enough to justify it.
- Use full selected prefix replays strategically at larger milestones, after
  high-risk shared changes, or when a smaller targeted replay cannot establish
  confidence.

## Final Gates Before Commit

Run these commands before every pushed code-change commit:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
git diff --check
```

Also run the exact `compat-harness` replay commands for the selectors touched
by the batch and any targeted compatibility replays justified by the change.

If `cargo fmt --check` fails, run:

```sh
cargo fmt
```

Then rerun the final gates.

## Documentation Requirements

Update `docs/compatibility-goal.md` before committing:

- Current verified progress, for example `1786/7080`.
- Latest batch title.
- Next frontier selector, file, and observed mismatch.
- Exact replay commands run for the fixed selector.
- Exploratory command that identified the next frontier.
- Any important notes about targeted or broadened regression coverage.

Commit messages must include:

- The compatibility progress count.
- What behavior was fixed.
- Exact replay commands.
- Exploratory frontier command and result.
- Targeted regression tests.
- Final gate commands.

## Current Batch Context

The `Compat 2879/7080` run cleared erc-networks (43/43), erc-nicks
(16/16) and erc-sasl (9/9 selected).  The 139-file verified-prefix
sweep (prefix-files19.txt) on frozen binaries is the gate
(autorevert-tests is a known flake under load; `pkill -f "sleep"`
leftovers from erc probes before retrying it standalone).  The next
agent continues with the selected erc-scenarios tests (statusmsg
2880, upstream-recon-znc 2881, internal 2882..2894, match 2895..2896,
misc-commands 2897, stamp 2898..2900) — these need the erc-d
fake-server machinery under test/lisp/erc/resources (network process
simulation; expect process/timer work) — then erc-services
(2901..2917, plstore cluster fails), erc-stamp (2918..2929, 2 fails)
and erc-tests (2930..3023, 57/99).  Milestone 3000 sits inside
erc-tests.  The Current Resume Point section lists this batch's
load-bearing semantics; the 2811 notes below still apply.

The `Compat 2811/7080` run cleared erc-button, erc-dcc, erc-fill,
erc-goodies, erc-join and erc-match (selectors 2766..2811).  The
136-file verified-prefix sweep (prefix-files18.txt) on frozen
binaries is the gate
(autorevert-tests is a known flake under load; also `pkill -f "sleep"`
leftovers from erc probes before retrying it standalone).  The next
agent continues with `test/lisp/erc/erc-networks-tests.el'
(selectors 2812..2854, 19/43 passing), then erc-nicks (13/16) and
erc-sasl (crashes the native runner — investigate the crash first) —
milestone 3000 sits inside the erc series.

Interpreter-level semantics this batch introduced (watch for their
regressions when touching nearby code):

- `condition-case' accepts the `t' catch-all handler, and handler-bind
  dispatch happens at signal time against a unified stack of active
  condition-case clause heads and handler-bind entries
  (`Interpreter::active_handlers`, `ActiveHandler` in eval.rs): an
  inner MATCHING condition-case stops outer handler-bind functions
  (this is how GNU ert's `should-error' suppresses the test-runner
  debugger).  `sf_should_error', `ignore-errors' and `ignore-error'
  push Case entries too.  A handler-bind function that signals sets a
  precise suspend count so only the condition-cases inside the
  handler-bind frame pass the new error through.
- GNU field motion: `pos-bol'/`pos-eol' ignore fields entirely;
  `line-beginning-position'/`line-end-position' constrain via
  constrain-to-field with ONLY-IN-LINE=t (and ESCAPE-FROM-EDGE only
  after actual line motion); `field-beginning'/`field-end' accept
  ESCAPE-FROM-EDGE (merge-at-boundary) and LIMIT; constrain-to-field
  implements the near-field gate (checks POS-1 too) and the
  "other side" check.
- `indent-rigidly' replaces each line's leading whitespace in place
  (was a destructive delete+reinsert that wiped text props).
- format-time-string gained the common strftime directives;
  current-time-zone takes the optional ZONE argument.
- vertical-motion models GNU's batch display: wrap at frame-width
  minus one (continuation column), honoring line-prefix/wrap-prefix
  display widths, IGNORING word-wrap and the cons goal column (GNU's
  batch vmotion does the same — verified against the oracle).
  beginning/end-of-visual-line, kill-visual-line, posn-at-point,
  count-screen-lines, kill-line and the whole kill-ring/yank subsystem
  are verbatim simple.el/subr.el ports in simple_compat.el.
- Keyboard macros: command remapping applies at dispatch
  (this-original-command keeps the pre-remap binding); raw key strings
  like "\C-c\C-j" parse as two events (a raw newline is C-j, not a
  textual separator); C-y/M-y/C-w/M-w have default global bindings.
- completion-at-point consumes completion-at-point-functions specs
  (BEG END TABLE :predicate :exit-function), performing try/test/
  all-completion with exit-function statuses, the *Completions*
  listing, and the "Next char not unique" message when
  completion-auto-help is nil.  minibuffer.el's quoted completion
  tables (completion-boundaries, complete-with-action,
  completion-table-subvert, completion-table-with-quoting,
  completion--twq-try/all) are verbatim ports; the native
  completion-table-case-fold fallback builds the same closure.
- Local hooks mirror their exact depth-sorted buffer-local value, including
  the `t' default-hook sentinel at depth zero, so Lisp reads and
  local-variable-p see GNU's ordering.  hook_values splices the default at
  that sentinel (negative local depths before it, positive depths after it).
  remove-hook's LOCAL is its THIRD argument, removes auxiliary depth metadata,
  and kills the local binding when only the sentinel remains; global
  add/remove-hook write through set-default when a mirror exists.
  define-minor-mode runs MODE-hook (and MODE-on/off-hook) on every toggle.
- buffer-local-value falls back to the DEFAULT value (never another
  buffer's local or the last-set global cell);
  (with-current-buffer BUF) with an empty body returns the buffer.
- The native ert runner wraps each test body in its own " *temp*"
  buffer (GNU ert--run-test-internal does) and binds
  ert--running-tests so ert-running-test works.  run-with(-idle)-timer
  return GNU 10-slot timer vectors; timer-event-handler fires and
  unschedules them; cancel-timer unschedules by function.
- The reader resolves #N= / #N# labels inside propertized string
  literals; equal-including-properties compares interval contents
  position-wise (segmentation-independent).
- buffer-text-pixel-size honors display string/margin replacements.
- window-margins/set-window-margins track per-window margins;
  window-fringes/set-window-fringes report the batch frame's (0 0 nil
  nil); pre/post-command-hook, window-*-change-functions,
  truncate-lines, word-wrap, left/right-margin-width defvars exist.

The `Compat 2523/7080` run cleared pp-tests, range-tests,
regexp-opt-tests, ring-tests and rmc-tests: prin1 default escaping
(raw newlines), looking-back non-empty-match preference + match-data
base fix, insert-buffer-substring nil bounds, GNU-exact regexp-quote
(dropping the native regexp-opt override), lambda doc-string-elt, and
simple_compat untabify/use-dialog-box-p plus native window-frame and
display-supports-face-attributes-p.  The 111-file verified-prefix
sweep (prefix-files11.txt) is the gate.  The next agent continues with
`test/lisp/emacs-lisp/rx-tests.el' (selectors 2524..2559) — the native
rx macro needs many GNU atoms; consider adopting GNU rx.el.
