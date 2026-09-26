Terminal object checkpoint — 2026-09-26

Terminals now have an allocator-owned object word shared by ordinary Lisp,
bytecode, and native transitions. Frames point to that object, and ordinary
terminal primitives use its fields directly. The live-terminal list controls
liveness; it is no longer the lookup needed to read a terminal's state.
Native heaps no longer allocate a separate terminal handle.

GNU references are `termhooks.h:struct terminal`, `terminal.c`'s terminal
parameter and deletion primitives, `frame.c`'s terminal ownership, and
`alloc.c`'s vector allocation census. The four Lisp fields begin immediately
after the pseudovector header. The parameter alist is authoritative;
`terminal-parameters` copies its spine and pairs while sharing their values,
as GNU's `Fcopy_alist` does. Terminal ids remain display and external keyboard
identifiers, not object identity.

The Rust state after those Lisp fields remains a `RefCell<TerminalState>`.
This checkpoint does not establish the complete GNU C terminal struct ABI.
Legacy keyboard bindings still resolve their saved numeric terminal id.
Deleted but reachable terminals retain their Lisp parameters until GC;
deleted unreachable terminals and their parameter values are reclaimed.
Image copying preserves terminal/frame sharing, and dump restoration creates
distinct dead terminal objects without putting them on the live list.

Buffer and terminal vector counts and sizes now come from their actual rounded
allocator footprints, including dead objects that remain reachable. This
removes their hard-coded GNU slot estimates and duplicate list-based counts.
Complete Rust heap accounting is still open: other modeled object counts and
the 80-byte cons representation have not been resolved by this change.

Local source `buffer-ownership-check-53c.json` passed locked all-target,
all-feature strict Clippy. Its fresh all-feature test binary has SHA-256
`66784f8916d12c9c9e84326b3c71749f5f920e02bd8c5a2c977f85149e62b6bd`,
with optimization level 1, assertions and overflow checks enabled, and zero
compiler warnings. All 402 selected terminal, frame, buffer, dump and roots
tests passed with zero ignored tests. `terminal-controls-53.json` retains the
complete inventories, commands and logs under `target/runtime-goal/`.

New controls cover independent native heaps and interpreters, a raw native
write to the parameter slot observed by ordinary Lisp, reachable and dead
parameter lifetimes with an unrooted negative control, and exact allocator
census changes. `tests/fixtures/shared-terminal-object-identity.el` is also
executed unchanged by GNU and Emaxx: it covers bytecode calls, copied parameter
alists, cyclic and shared values, forced GC, and identity-sensitive keys.
The local GNU executable is the documented diagnostic oracle, not the pinned
Darwin executable; these results do not certify the full frozen comparison.

The unchanged architecture probes still fail for raw cons fields and the four
remaining native bridge kinds: marker, overlay, char-table and frame. Their
failures are retained in `terminal-controls-53-architecture.log`; the combined
receipt deliberately remains failed. No performance improvement is claimed.

Linux run 36175304029, at diagnostic commit
`8c9e53e73f0d3de9af0d918b8556cea8ed35c75a`, passed build/format/lint and
failed the same two reclamation controls (17 passed, 2 failed, none ignored).
That run predates this terminal checkpoint. It shows the unwanted roots on
the current machine stack after explicit execution roots have released them.
The temporary test-only tracing remains pending diagnosis; the terminal
change neither resolves nor excuses those failures. Full platform validation,
compact conses, remaining object kinds and the locked performance goal remain
open.
