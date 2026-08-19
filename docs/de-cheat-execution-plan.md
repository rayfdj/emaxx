# Execution plan after the harness-measurement gate

Written 2026-08-19.  Ordering rationale: the fabrications still in the tree
*inflate* the score, and the comparison itself is still too lenient (failure
messages are not compared).  So every measurement is deferred until both are
fixed — otherwise the run produces exactly the kind of flattering artifact
this audit was called to stop.

Each step ends with a full serial gate (primitives, compat_runtime,
anti_cheat, tty, perf, compat-harness bin, batch, cli) and a commit + push.
Nothing is claimed without evidence from the pinned oracle at `../emacs`.

---

## Step 1 — land the harness-measurement round  (in flight)

Already applied, gate running:

- `ComparisonReport`/`AggregateReport` tally per-TEST outcomes; `summary.json`
  carries `matching_outcomes`/`mismatching_outcomes`/`total_outcomes` and the
  run prints `TESTS n/m matching`.  Before this, no code in the project
  counted tests.
- `configure_upstream_like_env` strips every `EMAXX_*` from measured children.
- `lisp/**/*.elc` — the bytes the oracle actually executes — are fingerprinted.
- The frozen manifest is pinned by sha256, not only by its three counts.

Exit: gate green, commit, push.

## Step 2 — artifact-form parity: execute what GNU executes

GNU's `load-suffixes` is `(".so" ".dylib" ".elc" ".el")` with
`load-prefer-newer` nil, so a `.elc` wins whenever one exists.  In the pinned
checkout all 1621 files under `lisp/` are compiled and only 58 under `test/`
are — so GNU runs library Lisp as bytecode and reads the upstream test file as
source.  Emaxx must resolve identically.

1. Subject reads library Lisp from the same live pinned tree the oracle reads
   (the oracle's dumped `load-path` already points there; only `-L
   <clone>/test` is added).  Today `remap_load_paths` rewrites the subject's
   library path into the isolated clone, where `git clean -ffdqx` has deleted
   every `.elc` — which is why the subject is stuck on source loads.
   *Fallback if the subject's stronger isolation must be kept:* stage the 1621
   `.elc` (68 MB) and restore them by **hardlink** per file; copying would be
   ~70 GB across a full run.
2. Prefer compiled loads in the measured subject.  `bytecode_vm_enabled()`
   feeds nothing but `set_prefer_compiled_loads` (two call sites), so this is
   purely resolution, not a VM switch.  GNU's default prefers `.elc`, so that
   becomes Emaxx's default; the env var stays an override.
3. Verify by probe, not assumption: both binaries must resolve `subr` and
   `ert` to `.elc` and an uncompiled test file to `.el`; subject setup should
   fall from ~148 s to ~10 s.

Expect outcomes to move in both directions — bytecode is a genuinely
different execution path, and that is the point.

Exit: gate green, commit, push.

## Step 3 — plumbing smoke run (not a quotable number)

A ~12-file run to prove the numerator and `.elc` parity hold at scale before
either is trusted.  Explicitly not a baseline and not an artifact to cite.

## Step 4 — finish the measuring instrument

These change what any later number *means*, so they precede measurement.

1. **Compare failure messages** (finding 22).  `compare_reports` checks status
   and condition type only, so any Emaxx assertion failure matches any GNU
   assertion failure on the same test.  At minimum compare messages for
   skips, and record a `message_differs` issue for failures.
2. **Make `anti_cheat` structural** (finding 24).  It is `#[cfg(test)]` and
   largely a denylist of past incidents' literal spellings over an allow-listed
   file set; a rename or a new top-level module walks past it.  Derive the file
   set from all of `src/`, and gate a run on it rather than trusting that
   someone ran `cargo test`.
3. Fix `compat/oracle_tests_all.md`'s selector documentation (finding 23): it
   says `Selector: all` while the recorded command used the default selector,
   which excludes `:expensive-test` and `:unstable`.

Exit: gate green, commit, push.

## Step 5 — remove the remaining fabrications

Ordered by how much each inflates the score.

1. **Missing features that report success** (findings 11-13):
   `yes-or-no-p` returns **t** with no input where GNU signals `end-of-file`;
   `hooks_overlays`'s blanket `Err(_) => Ok(Value::T)` across ~15 sites (which
   makes `write-region` MUSTBENEW clobber a file GNU refuses to touch); native
   `completing-read` inventing an answer (initial input, else default, else the
   *first candidate*); `kqueue-add-watch` that never watches and never fails;
   `set-network-process-option` returning `t` unvalidated; process output never
   decoded with the process coding system.
2. **The 98 fabricated variable defaults** (finding 9).  79 are read by nothing
   in Emaxx and go outright; 19 are consulted by native code and need
   per-case judgement — the native reader must tolerate the variable being
   void, as GNU does before the owning file loads.  `this-single-command-keys`
   is not a variable in GNU at all, only a keyboard.c function.
3. **The false disclosures in `src/tty.rs`** (finding 14) and the native
   `command-execute`/prefix-argument displacement they cover for.  Probe of the
   real image: all four commands are defined by preloaded `simple.el`.
4. **Keymap representation** (finding 36): `type-of` answers `cons` for a
   record that prints as `#s(keymap ...)` where GNU prints `(keymap ...)`.
   Either represent keymaps as real cons lists, or print the list form the
   record already carries — but `type-of` and `prin1` must stop disagreeing.

Exit: gate green after each sub-step, commit, push.

## Step 6 — correct the assertions that contradict the oracle

About thirteen, each already probed: the five `value<` large-int/float cases;
`charset-priority-list`/`charset-list` (GNU 179/203 entries, Emaxx claims 3 and
aliases the two concepts together); `(length CHAR-TABLE)` off by one;
`define-key` on a full keymap; `-b` is not a GNU option; `find-composition` in
batch; `comp-el-to-eln-filename`'s version subdirectory; the three native-comp
`subrp` assertions that should use `subr-primitive-p`; `key-binding [127]`;
`emacs-version` reporting `"30.2.0"` where GNU says `"30.2"`, hidden behind a
non-empty check; `max-lisp-eval-depth` scaled x384 with no test; `require`'s
failure message dropping GNU's curly quotes.  Also re-host the five deleted
compat_runtime tests that turned out to have no upstream coverage (jka-compr
sniffing, skeleton, `special-mode`, `member-ignore-case`, `display-buffer`).

## Step 7 — hygiene

Dead `generated_autoloads.rs` (4199 lines, nothing consumes it and anti-cheat
bans its use); add a regeneration gate for `generated_builtin_arities.rs`
matching the one that protects the C manifest; delete the dead
`ComposedAccessor` route (`caar`..`cddddr`, subr.el names) that the ownership
test cannot see; delete `incf`/`decf`, which do not exist in GNU 30.2; teach
`compat/generate_dumped_autoloads.el` to whitelist `function-put`.

## Step 8 — the baseline measurement

Only now.  At a **clean tree**, full manifest, artifact committed beside the
claim.  Per the audit, zero of 2824 stored runs were ever measured at a clean
tree, and no tool computed a numerator — so this is the first honest
`X/7595` the project will have.  At ~10 s setup it is roughly 1.5 h rather
than 23 h, which makes it repeatable rather than ceremonial.

## Step 9 — decide on issue #11 with real numbers

With setup at ~10 s, a dumped image saves perhaps 80 minutes per full run:
worth doing, no longer blocking.  Do it *after* the baseline so it can be
validated by requiring it to reproduce that baseline outcome-for-outcome — a
dump layer is exactly where a subtle, undetected divergence would hide.

## Tracked gaps (not scheduled here)

- `(let ((executing-kbd-macro t)) (read-command "C: " 'foo))` hangs where GNU
  returns at once.
- Issue #14, string identity/mutability.
- NS/native-comp build divergence: the pinned oracle is an NS + native-comp
  build, Emaxx models neither.  Costs `ucs-normalize-tests.el` (5 selectors),
  the `<home>`/`<end>` keymap rows, and the subr-identity assertions.
  Re-pinning a plain oracle would make those exact; deferred because it resets
  the denominator and does not block anything.
