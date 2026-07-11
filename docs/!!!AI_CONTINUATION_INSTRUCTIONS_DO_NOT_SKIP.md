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

## Compatibility Harness Usage

Use the harness to compare emaxx against the sibling GNU Emacs checkout. The
normal exact replay shape is:

```sh
cargo run --bin compat-harness -- run --scope all --selector SELECTOR --file PATH/TO/TEST.el
```

For the next known frontier, run:

```sh
cargo run --bin compat-harness -- run --scope all --selector check-all --file test/lisp/emacs-lisp/find-func-tests.el
```

After fixing a selector, exact-replay that selector. Then probe the next
canonical selector from `compat/oracle_tests_all.txt` and record the result in
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
