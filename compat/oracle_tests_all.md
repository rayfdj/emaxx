# Oracle Test Inventory

Regenerated on 2026-08-26 with:

```sh
cargo run --quiet --bin compat-harness -- list --scope all | tail -n +2 > compat/oracle_tests_all.txt
```

The `tail -n +2` is load-bearing, not tidiness: `list` prints an `Artifacts:
<path>` line to STDOUT before the inventory, so a plain redirect captures it
as line 1 and the resulting file fails the frozen loader's sha check.  The
awk verification below still passes on such a file -- it counts only
`discovered=` and indented lines -- so the trap is silent until the sha
rejects it.

Note the absence of `EMACS_TEST_TIMEOUT`.  Every earlier revision of this file
was generated with `EMACS_TEST_TIMEOUT=20`, a cap nine times tighter than the
180-second default the harness itself applies -- and the frozen procedure runs
with `--timeout-seconds 3600`.  That cap, not any property of the tests,
produced three of the four recorded load errors (honesty finding 98).  Running
the old command will NOT reproduce the checked-in manifest: it yields the
retired 515/4/7595 file, whose sha256 the frozen loader now rejects.

Oracle pin:

- Emacs version: 30.2
- Emacs repo commit: 636f166cfc86aa90d63f592fd99f3fdd9ef95ebd
- Scope: `all` (src + lisp + lib-src + misc test trees)
- Selector: the pinned default, `(not (or (tag :expensive-test) (tag
  :unstable)))` -- the file's own rows prove it (autorevert-tests.el:
  `discovered=16 selected=7`; the nine excluded tests carry those tags).
  An earlier revision of this document said "Selector: `all`", conflating
  the *scope* flag with the ERT selector; the 7,595 denominator has always
  been the default selector's selection (finding 23).
- System type: darwin
- Native compilation: true

Counts:

- Harness-selected oracle tests: 7883
- Source-tree literal `ert-deftest` forms are not the compatibility count.
  Static grep-style counts vary with the pattern used and miss tests generated
  while files load.
- Files with oracle load errors: 1

Canonical progress denominator and order:

- Use `compat/oracle_tests_all.txt` as the only ordered compatibility manifest.
- Count test selectors with:

  ```sh
  awk 'BEGIN{count=0; files=0} /^[^ ].*: discovered=/{files++; next} /^  /{count++} END{print "files", files; print "tests", count}' compat/oracle_tests_all.txt
  ```

- The expected result is `files 518` and `tests 7883`.

The harness-selected count is the compatibility ordering source. It is not the
same thing as any count inferred directly from the Emacs source tree because
Emacs test files can generate tests while loading, and the harness applies ERT
selection after load.

Load-error files:

- `test/src/emacs-module-tests.el`: cannot open `emacs-module-resources/mod-test`
  (needs a separately compiled C dynamic module)

The 2026-08-26 regeneration re-included three files that every earlier
revision recorded as load errors with rationales that were simply false --
"needs remote/ssh access", "needs LSP servers", "native-compilation specific".
Measured directly against the pinned oracle under this manifest's own
selector, all three run and two pass GNU outright:

- `test/lisp/net/tramp-tests.el`: 59 selected, ~127-144 s, 52 passed / 7
  skipped / 0 unexpected.  Its default method is local; no ssh is used.
- `test/lisp/progmodes/eglot-tests.el`: 52 selected, ~24-30 s, 39 passed / 7
  skipped / 6 failed ON THE ORACLE.  clangd is present and eglot connects.
- `test/src/comp-tests.el`: 177 selected, ~95-132 s, 177 passed / 0
  unexpected.  These are the first `:nativecomp'-tagged outcomes the
  denominator has ever contained, despite `native_compilation: true` being
  pinned in `compat/oracle.lock.json` for exactly that purpose.

`tramp-tests.el` is the slowest file here and its binding phase leaves roughly
a minute of headroom against the 180-second per-phase default, so the frozen
procedure's `--timeout-seconds 3600` is not optional decoration.

The 2026-08-18 regeneration recovered five files that previously timed out
under the same 20-second limit: `ert-tests.el` (55 tests),
`simple-tests.el` (53), `python-tests.el` (366), `process-tests.el` (37),
and `c-ts-mode-tests.el` (4).

External toolchain (NOT pinned by `compat/oracle.lock.json`):

- clangd: Apple clangd version 21.0.0 (clang-2100.1.1.101)

All 52 `eglot-tests.el` outcomes depend on it, and 6 of them fail on the
oracle itself on this host.  A different clangd, or its absence, reshapes
those outcomes.  Both runners share the host so the comparison stays
apples-to-apples, but the denominator is not reproducible across machines
without matching this.
