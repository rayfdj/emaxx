# Live keymaps and call metadata: complete checkpoint validation

Source174, published at `178fdf37d529a55e0fa1cf748faf668612a1d454`, now passes
the complete checkpoint validation. [PR #78](https://github.com/rayfdj/emaxx/pull/78)
replaces private keymap records, snapshots, binding caches and permanent roots
with actual Lisp cons/char-table storage. It retains queued event callbacks
across collection, builds independent mutable TLS reports from live sessions,
and reads call arity directly from each subr's authoritative descriptor.

The [audited portable receipts](handover/2026-09-30-call-validation-complete/manifest.json)
record:

- **3,023 macOS and 3,035 Linux Rust passes**, with the two existing terminal
  ignores on each platform. Both original lexical-thread and suspended-bytecode
  survival/reclamation contracts pass in these complete runs. GNU native
  artifact identity passes on both platforms.
- [Linux frozen comparison](https://github.com/rayfdj/emaxx/actions/runs/36680294974):
  **519 files / 7,928 matching outcomes**, with all **1,038 processes** successful.
  Each editor reports 7,670 passes, 47 existing expected failures and 211 skips;
  no unexpected outcome is counted as a pass.
- **223 terminal scenarios**, with every **651 screen and 28 filesystem
  comparison** matching source/ABI-matched GNU in the original inventory order.

The full [Linux Rust run](https://github.com/rayfdj/emaxx/actions/runs/36680285969),
all raw library test names/verdicts, Cargo summaries, native identity results,
frozen selectors/outcomes and terminal checkpoint sequences were audited.
The preceding [selected checkpoint](runtime-representation-call-metadata-checkpoint.md)
retains clean formatting/compiler/Clippy/diff checks, 395 gate and 395 release
controls, and twelve exact ordinary GNU comparisons. Full-run source identity
matches the published runtime and the documentation-only continuation commit.

The original Linux debugger evidence located stale pointer bytes beside a
one-byte metadata spill in `eval_call`. Reading the native descriptor removes
that copied optional metadata and a repeated function decode. Both complete
platform gates now pass the unchanged reclamation assertion. Source166 and
source170 failures remain failed historical runs; neither the earlier standalone
replay nor the later successful runs rewrites them. Source170's full GNU Eglot
timeout also remains preserved; this new full frozen comparison passes.

The original source manifests enumerated 345 Rust/test/build inputs but missed
four embedded text files. A subsequent compiler-dependency audit verifies the
three bytecode fixtures and compatibility helper against the recorded base,
isolated checkout and published source, bringing the checked set to 349.
Their hashes were captured after the initial manifests, and that limitation
remains explicit. Post-run binary/image hashes were checked; locally retained
executable/image bytes are omitted from the portable text archive. The remaining
images are not a transcript of individual test image use. Linux frozen binary
identities come from runner provenance.

No oracle pin, ignore, selector, timeout or original survival/reclamation
contract was relaxed. The dump test now checks actual restored Lisp roots and
shared parent cons cells, replacing assertions about the removed private-record
registry while retaining its behavioral coverage and adding collection.
Darwin's GNU executable matches source/native ABI but differs from its frozen
executable pin; complete final-source pinned Darwin validation remains required.

The [source178 input decoder](runtime-representation-input-decoder-draft.md) is
separately packaged continuation material. It passes 308 gate and 308 release
controls and seven exact ordinary comparisons, but its frontend integration and
broader reader contracts are unfinished and it is not applied to production.
Negative production input probes and the draft's earlier failures remain in
the archives. Symbol/object authority, physical allocation accounting,
post-deinit TLS behavior, the final adversarial audit, complete final-source
validation and locked performance parity remain open. This validated merge
checkpoint does not complete the full runtime goal or establish GNU speed parity.
