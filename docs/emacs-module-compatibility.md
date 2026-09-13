# GNU dynamic modules

Emaxx implements the public GNU Emacs 30 module ABI in Rust. A library compiled
against GNU's `emacs-module.h` uses the same entry point and append-only runtime
and environment tables. No Rust value layout is exposed to the library.

Scoped opaque handles root Lisp values during a foreign call; global references
retain values independently. Module functions and user pointers participate in
the existing Lisp reachability pass, including their finalizers. Loaded
libraries stay resident, including after validation errors. On Unix their
symbols are globally visible, as GNU's module loader requires. Reentrant calls
use the interpreter's dynamic handler stack to preserve signals, throws and
pending nonlocal exits. Assertion mode
checks expired handles and invalid API use; live foreign objects cannot be
serialized into a portable dump.

The implementation includes strings and byte strings, integers and big integers,
vectors, time, interactive functions, user pointers, finalizers and process
channels. Channel descriptors use Unix pipes on both macOS and Linux. Signal
termination from synchronous subprocesses returns the host signal description,
as GNU does; normal exit statuses remain integers.

## Success gate

The frozen run lacked a compiled `mod-test` fixture, so even GNU could not load
the module test file. Building that fixture exposed Emaxx's missing ABI. The
gate now builds GNU's unchanged `mod-test.c` against the configured oracle's
public header, then loads that one library in both editors:

```sh
python3 tools/emacs_module_gate.py --output target/module-success
python3 -m unittest discover -s tools -p test_module_eglot_gates.py -v
```

The default oracle is `../emacs/src/emacs`, source is `../emacs`, and subject is
`target/gate/emaxx`; flags can override them. GNU Emacs 30.2 at source commit
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd` passes **all 38 tests**, as does Emaxx,
on macOS. Linux uses the same gate in
[the compatibility workflow](../.github/workflows/module-eglot.yml); only the
Darwin secondary-suffix test may skip there.

Reports retain each test's result, input and binary hashes, and the compiler
command. Both editors must succeed with the same selected outcomes. Missing
reports, matching failures and unexpected skips fail the gate. The frozen
manifest and upstream test bodies are unchanged. These tests and the Rust
regressions provide concrete ABI coverage, not proof that every third-party
module has been exercised.
