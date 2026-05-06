# Oracle Test Inventory

Generated on 2026-05-03T14:27:07Z with:

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

- Harness-selected oracle tests: 7080
- Source-tree literal `ert-deftest` forms are not the compatibility count.
  Static grep-style counts vary with the pattern used and miss tests generated
  while files load.
- Files with oracle load errors under the 20-second per-file timeout: 9

Canonical progress denominator and order:

- Use `compat/oracle_tests_all.txt` as the only ordered compatibility manifest.
- Count test selectors with:

  ```sh
  awk 'BEGIN{count=0; files=0} /^[^ ].*: discovered=/{files++; next} /^  /{count++} END{print "files", files; print "tests", count}' compat/oracle_tests_all.txt
  ```

- The expected result is `files 510` and `tests 7080`.

The harness-selected count is the compatibility ordering source. It is not the
same thing as any count inferred directly from the Emacs source tree because
Emacs test files can generate tests while loading, and the harness applies ERT
selection after load.

Load-error files:

- `test/lisp/emacs-lisp/ert-tests.el`: process timed out
- `test/lisp/net/tramp-tests.el`: process timed out
- `test/lisp/progmodes/c-ts-mode-tests.el`: process timed out
- `test/lisp/progmodes/eglot-tests.el`: process timed out
- `test/lisp/progmodes/python-tests.el`: process timed out
- `test/lisp/simple-tests.el`: process timed out
- `test/src/comp-tests.el`: cannot find suitable native compilation output directory
- `test/src/emacs-module-tests.el`: cannot open `emacs-module-resources/mod-test`
- `test/src/process-tests.el`: operation not permitted while writing to DNS process
