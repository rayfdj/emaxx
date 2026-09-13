# Native compilation integration audit, 2026-09-13

The integration starts from remote `main` at
`7cf3d61801745daf740c52d37dba71b337dd1d1a` and
remote `native-comp` at `a56c12c0c47750b2afdee4a439a7c6ad978c7902`.
The six incoming commits change the call path, image materialization,
collector accounting, symbol lookup caches, and test lock ordering.

## Findings corrected before the publication gate

- **VM aliasing.** The incoming VM kept `&mut Vec` references alive across
  Lisp calls while its root tracer read the same vectors through
  `UnsafeCell`. Inactive mutation does not end Rust's exclusive reference.
  The activation still registers its roots once, but now uses `RefCell`:
  each mutation ends before Lisp, GC, or an unwind handler can run. A call
  borrows its argument slice immutably, allowing the marker to read it.
  This preserves GNU's live bytecode stack without relying on aliasing
  that Rust forbids.
- **Borrowed backtrace lifetime.** The public push API could retain a
  pointer to temporary arguments. Internal push/pop pairs could also leave
  such a pointer behind after a Rust panic or an extra signaling frame.
  Explicitly retained frames now own their arguments. Evaluator and native
  activations use private closure scopes that borrow their arguments and
  truncate to the entry depth on return or panic, then resume a panic.
  Frame copies continue to own their arguments. The new regression checks
  real weak-key retention and release, temporary public arguments, and a
  panic with an additional signaling frame.
- **Module entry collection.** The new Ffuncall collection point must also
  cover the module-function arm merged from main. It now runs after the
  backtrace is recorded, with the resolved module function explicitly
  rooted. GNU `eval.c:Ffuncall` collects before `funcall_general`, which
  reaches `funcall_module` through `funcall_lambda`.
- **The identity of nil and t.** The new standard-obarray projection
  produced `Value::Symbol("nil")`, whose Rust representation is truthy,
  instead of the actual nil object. Both standard-obarray entry paths now
  preserve nil and t. The symbol-keyed plist reader also recognizes their
  canonical representations as property keys. A GNU differential control
  requires `(11 22 (ok) (ok))` for property lookup and both mapatoms forms.
- **Overlapping keymap optimization.** Both branches batched mutation
  snapshots. The resolution retains main's one-sort constructor instead
  of keeping duplicate implementations. Both the module runtime and GC
  size bookkeeping remain in the dump-root ownership ledger. The merged
  ledger retains main's Mac signing and keymap receipts.
- **Optimized build fingerprint.** LLVM folded the complemented default
  fingerprint back into a second copy of the patch pattern. The build
  tool then patched the stored fingerprint and its comparison operand,
  causing runtime file hashing even in a fingerprinted executable. Mac
  re-signing changes that hash and invalidates its dump. The comparison
  now reads complemented static bytes through a volatile load. A CLI
  regression changes executable bytes without changing program code
  (a new signing identifier on Mac, a trailer on Linux) and requires both
  GNU and Emaxx to load their original dump and retain their fingerprint.
  The fixture preserves GNU's native-library layout and explicitly runs
  the fingerprint-and-dump build even when a previous test left a valid
  image of an unprocessed Cargo executable.
- **Native signaling frames.** Direct native helper errors reached
  handler-bind after native unwinding had removed the signaling frames.
  The helper boundary now synchronizes the live handler list and dispatches
  before unwinding, following `eval.c:signal_or_quit`. The existing CLI
  comparisons exercise the unchanged startup backtraces. Native unwind
  records now also retain their backtrace and evaluation depths: GNU's
  specpdl removes younger activations before running a cleanup or variable
  watcher. This matters when cleanup signals a second error. Suspended
  thread tracing follows the same unwind values through the new record.
  Once a native condition-case catches a signal, its diagnostic snapshot
  is cleared as on the bytecode and interpreted paths. Otherwise the saved
  signaling arguments remain GC roots after their frames have unwound.
  A weak-key regression reproduced this retention before the correction
  (one entry survived where zero were required).
- **Linux assembly linkage.** The x86-64 trampolines used ELF `.hidden`
  without `.globl`, so they could only be called from the same object file.
  Moving a call into a Rust generic scope exposed an undefined symbol in
  Linux CI. Both trampolines now have global binding with hidden visibility,
  allowing calls across codegen units without exporting a dynamic ABI.
  The Mac declarations already use `.private_extern`.
- **TLS negotiation during reads.** The Mac full gate exposed an
  intermittent failure on an asynchronous TLS connection. A read could
  complete the session's handshake without updating the process initstage,
  so the next event-loop pass tried to negotiate again. Reads now report
  no available bytes until negotiation completes, as
  `gnutls.c:emacs_gnutls_read` returns EAGAIN before GNUTLS_STAGE_READY.
  The event loop retains ownership of negotiation, verification, and the
  open sentinel. A deterministic transport control failed before the fix:
  an ordinary read invoked both handshake and record-read callbacks when
  neither was permitted. It also checks that reads proceed after the
  explicit handshake. The real GnuTLS transport assertions remain intact.
- **Fixture startup lifetimes and environment.** The Rust image-cache
  test kept its writer alive while loading its native image into a second
  interpreter. GNU forbids reusing a loaded unit during dump restoration
  (`comp.c:load_comp_unit`), and the production loader's rejection remains.
  The test now records the first boot's answers and drops the writer before
  the second boot, following the existing native dump round-trip test.
  All startup comparisons, fingerprint naming, and loaded-state assertions
  remain. A scoped guard restores the prior fixture-directory setting on
  success or panic; removing it had also disabled the gate's shared cache
  for later tests in the process.
- **GnuTLS library lifetime.** Installing the real TLS server on Linux
  exposed a SIGSEGV after a successful transport test. The native backtrace
  enters an unloaded address from `__nptl_deallocate_tsd`: the thread-local
  library owner had unloaded GnuTLS before its native thread destructors
  ran. A process-wide `Mutex<Option<Arc<GnuTlsLibrary>>>` now retains a
  successful load, as GNU's linked library (or cached Windows DLL) does.
  Sessions share that owner without new unsafe trait implementations.
  Global initialization is serialized and retained after success, while
  failures remain retryable, following `gnutls.c:emacs_gnutls_global_init`.
  The original ten real TLS controls and their assertions remain unchanged;
  the diagnostic records signal 11 and the native stack before this fix.

## Adversarial boundary review

The audit covers every incoming runtime file and the changes to tests,
anti-cheating checks, and performance probes. The C behavior references
are the local pinned GNU sources: `eval.c`, `bytecode.c`, `alloc.c`,
`pdumper.c`, `fns.c`, `lread.c`, `buffer.c`, `comp.c`, `gnutls.c`, and
`process.c`.

No frozen manifest, GNU test body, generated primitive/native ABI table,
test ignore list, or native artifact comparison is changed. The removed
anti-cheating ledger entry names the removed backtrace argument pool;
the root ownership check itself remains enabled. The GC counter test now
checks the real counter type, monotonic elapsed time, and exactly one
explicit collection, rather than assuming startup never collected.
The process-permit retry changes test lock ordering only; production
startup still executes unchanged loadup.el.

The getter's hardcoded scrollbar/overwrite `choice` answers were removed
by the incoming branch in favor of the ordinary plists initialized by
`buffer.c:syms_of_buffer`. The new caches derive their contents from live
symbol tables and invalidate on removals or plist position changes.
The performance scripts are measurement inputs; production does not
select behavior using those paths or their names. No oracle delegation,
matching-failure acceptance, or assertion normalization was introduced.

## Validation

Evidence is recorded under `target/native-comp-merge-20260913/`. The
initial retained source is `c7811e021c31e31613a11d15b2befefbb7867a5a`; the
image/TLS-read continuation is `c54406a0c4ec4898fc5bae5027d5208dd9912821`.
`continuation-provenance.json` verifies both 177-file source manifests
against those Git objects and the six validation-tool hashes against the
continuation candidate. The final runtime candidate is
`fffa1db5d510907aef791c37bc3b6d9d442b721e`.
`tls-lifetime-source-sha256.json` binds its 177 source/configuration files;
`tls-lifetime-validation-tool-sha256.json` binds its seven gate tools.
The only subsequent source delta is the reviewed GnuTLS ownership and
initialization correction, including the test session's shared owner type.
The Mac reports identify the uncommitted merge's original HEAD; these
manifests bind their source to the CI candidates.

At the user's direction, completed tests were retained instead of
restarting the whole suite. `tools/native_comp_merge_followup.py` requires
the exact reviewed TLS runtime delta, unchanged original test inventory
plus the new TLS control, and unchanged ignore inventory. It retains the
original failures, reruns the corrected image and TLS controls, and runs
every previously missing group and Cargo target. A new failure keeps the
continuation unsuccessful; matching failures are never accepted as parity.

The Mac CLI target initially encountered an old sibling dump after Cargo
rebuilt the executable. Refreshing the dump and rerunning only that target
passed all 17 tests. The original 16 fingerprint-mismatch failures remain
in `full-mac-gate-continued/`; `full-mac-gate-completed/summary.json`
combines the successful targets with that corrected setup's receipt.
Eglot's first preflight selected a toolchain without rust-src; it resumed
using the already installed workspace Rust 1.75 toolchain. Neither setup
correction changed runtime behavior or test assertions.

Linux's continuation passed every remaining library group and all ten
Cargo targets, but the real TLS process crashed. The isolated diagnostic
preserved the SIGSEGV and native backtrace. After the lifetime fix, only
the ten real TLS controls were rerun on both platforms, plus the Mac
deterministic TLS-read control. No completed full suite was restarted.
`assemble_final_receipt.py` checks the saved named outcomes against both
complete inventories and their libtest totals, verifies source hashes and
matching GNU/Emaxx outcomes, and writes `final-merge-gate-receipt.json`.
Original failed reports remain intact; superseded outcomes retain their
original logs and source provenance.

- Mac Rust coverage: 2,650 library tests passed, two unchanged TTY ignores;
  all 43 binary tests and 33 integration tests passed.
- Mac native artifact identity: nine unchanged GNU source fixtures, eight
  complete ELN byte comparisons passed; the ninth fixture correctly emits
  no artifact. Emaxx compiles before the corresponding GNU artifact exists.
- Mac modules: GNU and Emaxx each passed all 38 tests using the same module.
- Mac Eglot: GNU and Emaxx each passed 45 of 52 tests with the same seven
  allowed optional-server or platform skips, and no failures.
- Mac upstream native ERT: GNU and Emaxx each passed the same 177 tests,
  with no skips or failures. Fixture hashes remained unchanged.
- Formatting and `cargo clippy --locked --all-targets --all-features --
  -D warnings`: passed on Mac and Linux for the final runtime candidate.
- Initial audit controls: 44 and 119 passed. The final caught-signal
  controls passed 99 tests, CLI controls passed six, and the focused TLS
  and image correction passed six. Both the weak-key retention and TLS
  transport controls have recorded failing-before/passing-after evidence.
- Linux Rust coverage: 2,656 library tests passed, the same two unchanged
  TTY ignores; all 43 binary tests and 33 integration tests passed.
- Linux modules: GNU and Emaxx each passed 37 of 38 tests with the same
  Mac-only suffix skip, using the same module. Eglot passed 45 of 52 on
  both, with the same seven allowed skips and no failures.
- Linux native artifact identity: all eight complete ELN byte comparisons
  passed; the ninth fixture correctly emits no artifact, as on Mac.
- Final real TLS controls: all ten passed on both platforms. The Mac
  deterministic read control also passed; the Linux read control's
  successful continuation result is retained.

The Linux evidence chain is the
[initial run](https://github.com/rayfdj/emaxx/actions/runs/34757432755),
[continuation](https://github.com/rayfdj/emaxx/actions/runs/34759612701),
[native crash diagnostic](https://github.com/rayfdj/emaxx/actions/runs/34761415620),
and [successful final TLS gate](https://github.com/rayfdj/emaxx/actions/runs/34762001090).
The completed receipt certifies this reviewed merge for publication using
retained coverage and the affected checks described above.

The native ERT selector excludes GNU's `:expensive-test` and `:unstable`
tags. An expensive native bootstrap and performance-parity measurements
are outside this publication gate; artifact identity does not claim them.
