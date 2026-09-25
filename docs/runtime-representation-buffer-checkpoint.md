# Shared buffer object checkpoint

The Lisp buffer object now owns its editable Buffer payload. Interpreter,
bytecode and native transitions retain the same object word. Ordinary
current-buffer and buffer lookup stop allocating id/name wrapper objects.
Identity and hashing use the object address. Native buffer handles and their
identity entries are removed. Image copying memoizes buffer objects, and dump
restoration installs the preallocated object instead of a second payload.

Creation hooks may kill a newly created buffer and collect; the returned object
is rooted across that hook. Killing a buffer releases its text, saved text,
undo, property, overlay and decoded-text storage while preserving file metadata
on the object. The separate killed-buffer filename table is removed.

GNU reference: revision 636f166cfc86aa90d63f592fd99f3fdd9ef95ebd,
src/buffer.c Fcurrent_buffer, Fget_buffer_create and Fkill_buffer. In particular,
Fkill_buffer clears undo at line 2060, intervals/overlays at 2105–2110 and
frees text at 2136. Rust RefCell borrows enforce mutable-access exclusivity;
callers end them before Lisp callbacks and collection. This checkpoint does
not establish complete GNU buffer field layout or buffer-local slot authority.

Validation at commit preparation (source48):

- macOS cargo fmt --all -- --check, git diff --check, locked all-target/all-feature
  cargo check and strict Clippy pass with zero warnings.
- Fresh all-feature library test compilation passes with zero warnings. The
  new killed-buffer metadata and survival/reclamation controls both pass.
- The retained 73-test run is still running. The preceding source47 run passed
  70/71, including all 15 pdumper tests. Its one failure reports marker, overlay,
  char-table, frame and terminal bridge words; the assertion remains unchanged.
- Three identical Lisp fixtures were executed against local GNU. That GNU
  build is a behavioral diagnostic, not the configured pinned Darwin oracle.
- Existing frozen-run.yml Linux CI is to run validation=affected,
  rust_filter=buffer, file=test/src/buffer-tests.el. This selects 304 Rust tests
  in the source48 inventory. Linux results remain pending at commit preparation.

Reproduction: run the normal formatting/check/Clippy commands above, then
cargo test --locked --lib buffer -- --test-threads=1. The existing Linux
workflow builds the pinned GNU source and compares the complete unchanged
buffer test file. This affected run is not a full frozen or full Rust gate.
Local raw receipts, source hashes, exact test inventories and logs are under
target/runtime-goal/buffer-ownership-{check,build,controls}-48*.

No performance improvement is claimed. Conses remain 80 bytes, five other
object kinds still use bridge handles, the earlier full Linux gate has an
unresolved EIEIO/native-cons abort, and complete platform/native ABI/ownership
validation plus the locked GNU performance objective remain open. No ignores,
expected failures, upstream test edits or performance-workload changes are
introduced by this buffer checkpoint.
