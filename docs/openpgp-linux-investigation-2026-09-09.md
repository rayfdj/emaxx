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
