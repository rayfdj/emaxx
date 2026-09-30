# Live input decoding draft — 30 September 2026

Production remains source174 at `178fdf37`. Its complete macOS Rust and terminal
runs and both Linux runs are still active. The Linux handles are
[Rust 36680285969](https://github.com/rayfdj/emaxx/actions/runs/36680285969) and
[frozen comparison 36680294974](https://github.com/rayfdj/emaxx/actions/runs/36680294974).
Keep following these runs; do not launch replacements just because a chat ends.
The macOS full gate has passed its original suspended-bytecode reclamation
assertion and all library groups, but its Cargo stages have not finished.
These partial results do not certify the complete candidate or clear earlier
failed runs.

The separately saved **source177 input-decoder draft is not applied to
production**. Its [portable bundle](handover/2026-09-30-input-decoder-draft/manifest.json)
contains complete patches for sources175–177, including their new files,
source hashes, closed validation receipts, GNU comparisons and reproduction
scripts. Each patch replays its recorded inventory from `ebfdbf95`; the newest
has 352 recorded Rust/test/build inputs. The live isolated checkout is
`target/runtime-goal/recovered-2026-09-30/decoder/emaxx`.

The draft replaces the special XTerm dispatch and eight-character snapshot
reader with incremental traversal of actual `input-decode-map`,
`local-function-key-map` and `key-translation-map` objects. It follows
`keyboard.c:keyremap_step` and `read_key_sequence`: reached prefixes survive
between reads, filters run through live keymap access, translators receive
the prompt and dynamically bound `current-key-remap-sequence`, and translation
results obey GNU's length and type checks. Rooted buffers retain maps, events
and callbacks across collection. Unchanged GNU `read-key` continues to own its
idle-timer fallback. Queue consumption advances the real unread list spine.

Source175's compiler completed with six dead-code warnings, which failed the
zero-warning requirement. Source176 removes the unused legacy traversal
helpers and passes all strict checks. Its selected gate **fails: 304 passes,
one failure, no ignores**. The existing XTerm test receives raw ESC after
discarding a translated mouse-down. Its ordinary executable repeats that
failure. Source177 follows GNU's cursor rewind when discarding that event;
the unchanged assertion now passes within its complete selected gate run.
Compiler, formatting, Clippy with warnings denied and diff checks pass.
All **306 selected gate controls** pass. Release and ordinary validation were
still running when this bundle was captured; consult the manifest's closed
stages and live receipts.

Source176's three ordinary input contracts match GNU exactly: general decoding,
extra callback/default cases, and the complete three-stage translation pipeline.
The same contracts fail on the source174 ordinary executable. Two invalid
local test premises were corrected using separate GNU evidence: an unconfigured
XTerm translator is not called, and `read-key` temporarily replaces the supplied
overriding map. Coverage now checks an explicitly configured translator,
unconfigured behavior and the actual bound-map `read-key-sequence-vector` path.
The valid failing XTerm batch assertion was not changed.

Further ordinary source176 comparisons identify open reader failures: it
returns only the first generated event when that event is command-bound,
omits case fallback, and accepts a non-string prompt. The unchanged GNU result
and differing Emaxx result are both in the bundle; successful process exits
do not count these comparisons as passes. These paths remain unchanged in
source177. Its terminal and minibuffer command loops still use the older
decoder and must share the new incremental mechanism. Mouse prefixes, fallback
and reader arguments also require continued GNU-based review.

The replay audit found a provenance limitation: earlier manifests covered Rust
files, tests and build settings but omitted three embedded bytecode fixtures
and `compat/emacs_compat_runner.el`. Supplemental compiler-dependency receipts
now verify those four files against the recorded base and current root. They
were checked after the original manifests; no old receipt was rewritten.
The two unsuccessful archive-audit helper invocations are preserved too.

To reconstruct the draft, extract the bundle, create an isolated checkout at
`ebfdbf9547b76e23013f8ce70f417aed02a744c2`, and apply
`source177-complete.patch`. Verify `source177-manifest.json` plus
`source177-supplemental-inputs.json` before building. The scripts record the
local checkout, target and GNU paths; relocate these explicitly when using a
different host. Preserve the frozen checkout while its supervisor
`queue-source177-validation.py` runs. Live receipts and process handles are
under `target/runtime-goal/resume-2026-09-28`; the decoder supervisor is PID
85019. Its original timeout and test inventories remain intact.

GNU matches source and native ABI but is not the frozen Darwin executable.
No pin, expected outcome, selector or comparison strictness changed. Input
integration, post-deinit TLS behavior, symbol/object authority, physical
allocation accounting, complete final-source validation, the final adversarial
audit and locked performance parity remain open. This draft is continuation
material, not a completed checkpoint or a speedup claim.
