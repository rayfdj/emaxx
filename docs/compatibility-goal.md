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

## Current State

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
