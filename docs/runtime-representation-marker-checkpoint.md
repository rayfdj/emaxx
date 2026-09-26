# Shared marker object checkpoint — 2026-09-26

This is an incomplete architecture milestone on the existing WIP branch. It is
not a full validation certificate or a performance result.

## Representation and GNU reference

GNU source revision: `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`.

- `lisp.h:struct Lisp_Marker` and `PVEC_MARKER`: one 48-byte allocation,
  including the 8-byte vector header. Buffer, next, character position and byte
  position are at offsets 8, 24, 32 and 40; insertion type is bit 1 at offset 16.
  A configured GNU C probe verifies size, alignment, offsets and flag bits on
  this macOS host. Receipt: `target/runtime-goal/gnu-marker-layout-67.json`.
- `marker.c` and `alloc.c:Fmake_marker`: Lisp, bytecode and native code now name
  the same allocator-owned marker. Ordinary position, buffer, insertion type,
  equality and identity operations read its fields. Interpreter marker IDs,
  the marker vector, per-buffer ID sets, reverse mark ownership table and native
  marker handles are removed. The buffer owns its actual mark object.
- `insdel.c`, `buffer.c:Fkill_buffer` and `Fbuffer_swap_text`: edits update the
  weak marker chain; text swapping moves the chain and actual mark objects;
  killing a buffer detaches markers pointing into that buffer.
- `alloc.c:sweep_buffers`: weak chains are unlinked before any vector storage
  is freed. Markers do not keep buffers alive. Undo, process and unwind owners
  trace actual marker objects. Marker allocation and vector census use the
  actual 48-byte allocation.
- `pdumper.c:dump_marker`: images relocate marker fields and buffer mark slots.
  Restoration allocates buffer identities before restoring existing marks,
  avoiding an extra temporary mark. The executable fingerprint still rejects
  incompatible old images; no image validation was relaxed.

`Cell` fields allow native stores without aliased mutable Rust references.
Standalone Rust `Buffer` values have a position-only mark until installed as a
Lisp buffer; the installed form holds the actual marker and its owning buffer.
These forms are mutually exclusive. The owner is needed when Lisp moves the
mark into another buffer. This host API constraint is a deviation from GNU's
buffer construction and its cost is not yet measured. It adds no position mirror
or marker lookup to ordinary Lisp operations.

The collector currently finds buffer chains by walking vector headers before
sweeping. Non-ASCII character-to-byte conversion walks text when an existing
marker cannot provide the byte position; extended GNU characters are included.
Neither cost has a performance result. Window point slots, indirect-buffer text
sharing and the complete GNU buffer ABI still require work.

## Adversarial review

The review covers the marker migration from documentation checkpoint
`2ef7d89c5a93171f6fd254e1608364e411c2041d`, its consumers, graph restoration,
GC ownership and new/changed controls. The original source is preserved in
`target/runtime-goal/marker-before-67.tar` (SHA256
`7cd713e0c8229f9e58f27fcde7329212389f96cb8c2d52c00710c8fa8eab3b70`).

Findings fixed in the draft, with GNU source/probe evidence:

- A fresh marker's last position is zero; detachment retains the last position.
- `set-marker-insertion-type` returns the actual argument, not normalized truth.
- `set-marker` decodes BUFFER before checking MARKER, accepts an actual buffer
  rather than a buffer-name string, clips to full buffer bounds and defaults to
  the current buffer even when POSITION names another buffer's marker.
- All detached markers compare equal and have compatible `equal` hashes.
- Killing a mark's former owner must preserve that marker when it points into
  another live buffer. GNU returns `(t 2 2)` in the retained probe.
- `search.c:Fset_match_data` coerces detached markers to zero. The previous
  generic buffer-position coercion rejected them. Correct detachment exposed
  this during GNU loadup; the failed run is retained rather than normalized.
  This localized repair is not a complete audit of search register semantics.

The shared fixture uses ordinary interpreter/bytecode calls, generated buffer
names, multibyte edits, forced GC, shared references, narrowing, text swapping,
error paths, detached hashing, buffer killing and match-data reseating. GNU
executes the same fixture. Native controls directly write marker fields and
require the same word across native heaps and bytecode, with no marker handles.
The reclamation control retains one marker and drops another, then drops the
last root and requires eventual reclamation. Existing survival/reclamation
assertions remain intact. Old table-specific assertions now inspect the actual
mark slot, chain and fields; test inputs and semantic contracts are retained.

No upstream selectors, expected outcomes, ignores, timeouts, performance inputs,
comparison strictness or CI workflow were changed by this marker milestone.
The unchanged all-kinds native acceptance control still exposes the remaining
bridge kinds. Full runtime ownership/API soundness, accounting and the earlier
64 KiB post-GC stack clearing remain open; this review does not establish
universal absence of cheating.

## Validation and limits

The first frozen draft, source67e, passed strict all-target/all-feature Clippy
and fresh compilation with zero warnings. Its exploratory marker run exposed
loadup's detached-marker failure and was stopped after that reproducible failure;
the incomplete inventory is explicitly not a pass. Native field sharing,
reclamation and the buffer/marker dump graph control passed in that draft.
Raw receipt: `target/runtime-goal/marker-controls-67.json`.

The corrected source67g passes formatting, strict all-target/all-feature
Clippy, whitespace checks and fresh all-feature compilation with zero warnings.
Its 33 marker controls pass with zero failures or ignores (196.93 seconds).
The test binary SHA256 is
`b3b27837c22bff11aed93c8116ebfd63909f6300924fd455b4e373282e5e82d7`.
All 19 unchanged root controls also pass with zero failures or ignores, including
both required survival/reclamation contracts. The broader run subsequently failed as recorded below. Its selected passes
are observations only; source68 supplies the completed verification. This is
a selected WIP checkpoint, not a full-gate result.
Receipts are `buffer-ownership-check-67g.json`, `buffer-ownership-build-67b.json`
and `marker-controls-67b.json` in `target/runtime-goal`. The local GNU executable
is a diagnostic oracle, not certification against the pinned Darwin configuration.

Conses remain 80 bytes, and overlays, char-tables and frames retain native
bridges. The locked 16-case suite and 3% tolerance are unchanged. No speedup,
GNU performance parity or completion of the overall runtime goal is claimed.


## Follow-up: broader run and exact census correction

The source67g broader run finished 321/324, with zero ignored tests. Two network
controls failed to bind localhost sockets under the local sandbox; one failed
in GNU for the same permission reason. The third assertion expected one new
vector allocation for a buffer. GNU `allocate_buffer` plus
`Fget_buffer_create`'s `Fmake_marker` allocate two, and the marker adds exactly
six words. The test now requires those exact object and byte deltas; its buffer
count and retained killed-object assertions are unchanged. Production runtime
code is unchanged by this follow-up. Evidence: `marker-census-audit-68.json`.

The final source check for that broader run also failed: 48 older files had
disappeared from the temporary checkout, including Cargo.toml and build.rs;
the .git pointer was missing too. Surviving source and the saved binary still
matched their hashes. The cause is not established, and this does not certify
that the missing files were unchanged throughout the run. The failed receipt
is retained explicitly; the earlier selected passes are observations, not a
successful completed validation run. Source68 uses a fresh checkout under the
workspace for verification, with the required socket access for the rerun.


## Linux evidence for the production marker change

Both existing CI workflows for `a1c6a17038db14cbc71f67959832f205927b48dd`
completed successfully. Raw artifact ZIP digests, clean commit identity,
executable/image hashes, discovered/selected/executed GNU inventories, process
success, zero skips and outcome summaries were verified:

- Run36257362542:33/33 Rust marker controls and3/3 unchanged GNU marker outcomes.
  The GNU file includes substantial help rendering. Body2370ms versus174ms
  (13.620x); setup650ms versus92ms.
- Run36257364649:19/19 unchanged root controls and406/406 unchanged GNU buffer
  outcomes. Body2818ms versus430ms (6.553x); setup4095ms versus177ms.
- Both runs pass formatting and all-target/all-feature strict Clippy. Their Rust
  affected-test logs record fresh compilation and binary paths, but the existing
  workflow does not retain the Rust test executable SHA; the GNU comparison
  does retain both editor executable and image hashes.
- Raw receipts: `target/runtime-goal/marker-ci-completed-67.json` and
  `target/runtime-goal/roots-buffer-ci-completed-67.json`.

These unfavorable single samples are diagnostic, not interleaved locked-suite
measurements. They establish neither a speedup nor a regression relative to the
previous checkpoint. The census assertion follow-up changes no runtime code;
its completed local and Linux verification is recorded below.


Source68's fresh workspace build and strict all-target/all-feature Clippy pass
with zero warnings. Formatting and whitespace checks also pass. The completed
local rerun passes the exact census control (1), marker controls (33), unchanged
root controls (19), and buffer/dump controls (324), with zero failures or ignores.
These groups overlap: they cover 360 distinct tests, not 377 distinct tests.
Both localhost controls pass with the required socket access. Final source and
saved executable hashes match their pre-run identities. The saved binary SHA256
is `9b15552497e40715816ef6e93ede9392b0824fdfed9621eb298f974e878f0eb1`.
Receipts: `buffer-ownership-check-68.json`, `buffer-ownership-build-68.json`
and `marker-controls-68.json` under `target/runtime-goal`.

The census follow-up is committed as
`2dfdfd308fcda45b2c5643dc9e414500f7837c7a`; production source remains unchanged
from marker67. Existing Linux run36258556054 passes formatting, strict Clippy,
the exact census control (1/1), and all 406 outcomes in the unchanged GNU buffer
file, with zero ignores or skips. Raw archive digest, source identity, editor
and image hashes, inventories, outcome counts and process status are verified.
Receipt: `target/runtime-goal/census-ci-completed-68.json`.

The additional Linux sample remains unfavorable: buffer body4139ms/GNU573ms
(7.223x), setup6073ms/GNU259ms, total10273ms/GNU851ms. All samples are retained;
these timings are diagnostic and are not controlled before/after performance
measurements. The selected marker checkpoint is now verified on macOS and Linux.
Full gates, pinned Darwin certification, release validation, compact conses,
the remaining native representations, general ownership soundness and the
locked performance objective remain open.
