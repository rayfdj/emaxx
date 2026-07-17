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

- Verified through selector 2960/7080.  Exact oracle runs pass for the
  first 31 manifest selectors in `test/lisp/erc/erc-tests.el'
  (2930..2960), and the file-wide default run is now 76/94 passing.
  The batch fixed syntax-table-aware case word boundaries, preloaded
  `view-mode-enter' plus `custom-group-of-mode', preserved text properties
  through `delete-and-extract-region', and repaired
  `accept-process-output' parsing of `(PROCESS SECONDS)' plus its erroneous
  one-second wait cap.  The unrestricted test suite is green: 1137 library
  tests and all auxiliary suites.  `erc-d-run-no-block' passes three fresh
  runs and selector 2897 still passes.  NEXT = selector 2961,
  `erc--interactive': `url-generic-parse-url' is currently read as a void
  variable.  Do not fold that unproven fix into the completed 2960 batch.
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
- FRONTIER NOW = 2961 (`erc--interactive' in
  test/lisp/erc/erc-tests.el); selectors 2930..2960 pass exactly.
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
