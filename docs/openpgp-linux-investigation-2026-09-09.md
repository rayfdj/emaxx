# OpenPGP Linux investigation

The four remaining recorded `mml-secure-en-decrypt-1` through `-4`
mismatches came from Linux. The original GNU `test-conf` deliberately skips
Darwin, and fresh local runs on main `1b60d9b` confirm four skips in both
editors. Those are not fixes or successful encryption/decryption coverage.

An isolated GitHub Actions branch supplies a Linux host. This diagnostic
builds GNU Emacs 30.2 from the publicly available upstream revision
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`, with the documented Linux
capability set. The historical Linux lock instead identifies a locally
committed Ubuntu source repack (`6ee5c136...`); this run records its own
source, binary, package and feature identity and does not replace that lock
or certify the frozen corpus.

## Audit before the diagnostic checkpoint

- The workflow runs only on its named investigation branch or manual
  dispatch. Its token has read-only repository permissions; checkout does
  not persist credentials. Third-party action revisions are immutable SHAs.
- GNU source and the four original test bodies and assertions are unchanged.
  Only tracked public test-key resources are copied, with a SHA-256 inventory.
  Each editor has separate HOME, TMPDIR and GnuPG directories. No developer
  keyring is read; agent cleanup names only the newly created fixture home.
- Existing EasyPG debug output is retained after each test. No ciphertext,
  failure message, expected outcome, clock or random source is normalized.
- Acceptance requires all four tests actually passing in both editors and
  both editor processes exiting successfully. Matching failures, skips,
  incomplete results and missing editor reports cannot pass.
- The workflow uploads diagnostic logs and public test fixtures. It uses no
  repository secrets and sends no email. GnuPG operations use the upstream
  fixture recipients and keys.
- No Rust/runtime code or existing Rust test changes are included. This is
  an investigation checkpoint, not a claim that the four mismatches closed.

Local validation covers Python syntax, YAML parsing, the generated Lisp
capture driver against real GNU (it records four skips as skips), and the
six acceptance checks. An initial Python bytecode check hit macOS's protected
cache path; the syntax check was repeated with an explicit temporary output
path. Linux execution and actual decryption results follow separately.

## First Linux evidence and correction

Run 34332448535 at `25a01b7` completed. With short fixture paths GNU passed
all four original tests. Emaxx failed all four, despite real GPG decryption
reporting `DECRYPTION_OKAY` and `GOODMDC`. Its plaintext contained raw-byte
characters in place of ASCII (84 became 4194132). Thus the historical shared
failure had hidden a real runtime discrepancy; the earlier environment-only
explanation was incomplete.

The reduced GNU probe and source identify `set-buffer-multibyte` with a
non-t true flag: GNU's `ASCII_CHAR_P` branch precedes the flag check, whereas
Emaxx converted every byte to a raw-byte character. The correction preserves
ASCII before deciding whether to recognize a multi-byte sequence. The new
contract covers all 256 bytes for t, to, and another non-nil flag, and checks
that t decodes a valid UTF-8 pair while the other flags retain its bytes.
No test name, GPG message, ciphertext or MIME-specific behavior appears in
the runtime correction. The unchanged original tests remain the end-to-end
gate. Broader byte-conversion controls, de-cheating audits and strict Clippy run on
Linux with the correction.

A separate unproven fixture hypothesis also remains: the old harness's
GnuPG socket path can reach 109 bytes (117 for the browser socket). A second
run with the historical long directory layout records actual agent socket
paths and outcomes. It is an observation, separate from the short-path gate
which requires all four tests passing in both editors. No harness path
change has been made without that evidence.

## Correction audit

The complete production diff is the ASCII branch in
`multibyte_buffer_text`. Its caller uses it for current and saved buffer
text. Both the old and corrected ASCII branches consume one byte and
advance one character, so the position map's shape is unchanged. The t
path already recognized ASCII with width one; nil conversion uses a
separate function. Eight-bit conversion, return values, undo entries and
marker updates are unchanged. No scheduler, heap ownership, native ABI,
compiler, dump or startup code is changed.

The regression asks real GNU for the same contract before asserting Emaxx's
answer. The pre-fix reduction independently reproduced 84 becoming 4194132;
the original Linux run independently failed all four end-to-end tests.
A second local pre-fix probe with isolated HOME/TMPDIR timed out during
startup after 120 seconds; it contributes no passing evidence. The earlier
completed reduction and the Linux failure receipts remain preserved.

## First correction validation

Run 34335076568 at `3730f7d` passed all 40 selected Rust tests serially
(22 de-cheating checks and 18 byte-conversion controls), six Python
acceptance checks, and rustfmt. Strict Linux Clippy then rejected an
existing PTY test helper's `&mut size` argument: Linux libc takes a const
pointer, while Darwin takes a mutable pointer. The helper now supplies
`&raw mut size`, which satisfies either signature without a needless
mutable reference. No runtime code changed after the 40 passing tests.
The PTY contracts and Clippy are rerun; the 40 earlier passes are retained
rather than repeated. The four original OpenPGP tests had not run in this
job because Clippy stopped the workflow first.

## Completed validation and limits

[Run 34336177090](https://github.com/rayfdj/emaxx/actions/runs/34336177090)
at `769577d` completed successfully:

- All four unchanged `mml-secure-en-decrypt-1` through `-4` tests passed
  in both GNU and Emaxx, including their inline and MIME iterations. Both
  editor processes exited zero. The same four tests also passed in both
  editors with the historical long fixture-directory layout.
- The long-path experiment did **not** reproduce the old environment
  failure. GPG chose 59-byte sockets under `/run/user/1001/gnupg` for both
  layouts. The earlier socket-length hypothesis remains unproven for the
  historical host; no harness path or error normalization was changed.
- All 25 selected Rust checks passed serially: 22 de-cheating checks and
  the three PTY contracts using the corrected helper. Combined with the
  retained 40-check run, this covers **43 distinct Rust checks**, with
  no failed or ignored tests in those selections. These are focused
  receipts, not a new full Rust gate.
- All six Python acceptance checks and rustfmt passed. Strict Clippy
  (`--locked --all-targets --all-features -- -D warnings`) passed with
  zero warnings on Linux and separately on Darwin. The Darwin log is
  `/private/tmp/emaxx-openpgp-sept9/darwin-clippy-final.log`.
- The inventories of the 36 tracked GNU fixture files are identical
  before and after the correction. No upstream assertion, selector,
  manifest, oracle lock or expected ciphertext was changed.

Binary SHA-256 values for both successful fixture layouts:

| Binary | SHA-256 |
| --- | --- |
| GNU | `d324f30093c1507fa0e00ebe4bc1d9e3d8a96d0224b8892c2afaabb9e3f2a080` |
| Emaxx | `ce69051dfa23356cda64fe0e4aed70780432a10f0b10dec6109dcc6265a5621c` |

The final publication changes only this evidence, the honesty ledger and
future workflow selection after `769577d`. Future invocations run the full
43-check union; already passing checks were not repeated solely to record
these results. Runtime and Rust test source hashes remain those validated
above. No later native-comp commits were merged.

The four OpenPGP cases now have real passing Linux coverage. The known
remaining work discussed in this series is the two ERC startup mismatches
and one async-shell startup mismatch, pending the dumping/startup work.
This does not certify a fresh 7,883-test frozen run, the exact historical
Linux oracle binary, or 100% compatibility. Darwin's original OpenPGP
skips remain skips.
