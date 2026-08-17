# Oracle Test Inventory

Generated on 2026-08-18 with:

```sh
EMACS_TEST_TIMEOUT=20 cargo run --quiet --bin compat-harness -- list --scope all > compat/oracle_tests_all.txt
```

Oracle pin:

- Emacs version: 30.2
- Emacs repo commit: 636f166cfc86aa90d63f592fd99f3fdd9ef95ebd
- Selector: `all`
- System type: darwin
- Native compilation: true

Counts:

- Harness-selected oracle tests: 7595
- Source-tree literal `ert-deftest` forms are not the compatibility count.
  Static grep-style counts vary with the pattern used and miss tests generated
  while files load.
- Files with oracle load errors under the 20-second per-file timeout: 4

Canonical progress denominator and order:

- Use `compat/oracle_tests_all.txt` as the only ordered compatibility manifest.
- Count test selectors with:

  ```sh
  awk 'BEGIN{count=0; files=0} /^[^ ].*: discovered=/{files++; next} /^  /{count++} END{print "files", files; print "tests", count}' compat/oracle_tests_all.txt
  ```

- The expected result is `files 515` and `tests 7595`.

The harness-selected count is the compatibility ordering source. It is not the
same thing as any count inferred directly from the Emacs source tree because
Emacs test files can generate tests while loading, and the harness applies ERT
selection after load.

Load-error files:

- `test/lisp/net/tramp-tests.el`: process timed out (needs remote/ssh access)
- `test/lisp/progmodes/eglot-tests.el`: process timed out (needs LSP servers)
- `test/src/comp-tests.el`: process timed out (native-compilation specific)
- `test/src/emacs-module-tests.el`: cannot open `emacs-module-resources/mod-test`
  (needs a separately compiled C dynamic module)

The 2026-08-18 regeneration recovered five files that previously timed out
under the same 20-second limit: `ert-tests.el` (55 tests),
`simple-tests.el` (53), `python-tests.el` (366), `process-tests.el` (37),
and `c-ts-mode-tests.el` (4).
