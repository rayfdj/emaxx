# Actual keymap storage and live lookup — 30 September 2026

Source166 is the next candidate after [PR #77](https://github.com/rayfdj/emaxx/pull/77)
merged the source161 compatibility repairs into main at `e9bbd598`. All 339
compiled/test inputs match the separately validated architecture worktree.
The [portable source and receipts](handover/2026-09-30-keymap-live-authority-draft/manifest.json)
replay from published `d9caac1e`; that archive was packaged as an unfinished
draft before this production-candidate integration. The full goal is open.

Keymaps now use their actual Lisp cons and char-table storage. The private
record payload, reverse indexes, mutation snapshots, binding cache, permanent
roots and their clone/dump/tracing bookkeeping are removed. Interpreter and
native stores mutate the same binding pairs and parent tails. Restored images
retain actual sharing. An anonymous-map ordinary probe reports zero remaining
maps after its creator returns and collection runs; the previous main retains
two. Live-map survival assertions remain intact.

GNU `keymap.c:lookup_key_1` reads each array event when reached. Lookup now does
the same for vector slots, mutable strings, unibyte Meta, Lucid events and
positioned symbols. A collecting prefix callback can change a later event and
lookup observes that change. Original and translated key arrays receive their
type checks. Symbolic map stores resolve aliases and collecting keymap autoloads
before key validation.

`key-binding` follows GNU `Fkey_binding`: use actual lookup on current active
maps, invoke prefix and leaf menu filters, preserve ordinary error handling and
nonlocal exits, and reacquire active maps before command remapping. Existing
scoped roots protect inputs, active maps, events and pending commands across
callbacks. Command readers classify the already resolved binding, removing
hardcoded C-x/C-c/ESC prefix decisions and an extra lookup that ran leaf filters
twice. Composed menus retain the source161 GNU canonicalization repair.

Strict formatting, compiler, warnings-denied Clippy and diff checks pass.
Debug and release each pass **393 selected plus 69 disjoint command-loop
controls**, with no failures or ignores. All **25 ordinary GNU comparisons**
match exact output and successful process status. Native shared-pair/tail,
image sharing, forced collection, mutation, redefinition, quit/throw and
reclamation controls remain covered. [PR #78](https://github.com/rayfdj/emaxx/pull/78)
publishes these inputs at `3bada057`. Its [Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36647925886)
and [Linux frozen](https://github.com/rayfdj/emaxx/actions/runs/36647928962) runs are active.

The initial full macOS runs were interrupted. Five Rust groups pass (1,607
tests), then primitives receives SIGTERM before its result summary; later
groups and Cargo stages do not execute. The terminal log stops during scenario
33/223, without a final result or post-run hash verification. The
[interrupted-run receipts](handover/2026-09-30-keymap-interrupted-runs/manifest.json)
remain incomplete. Their interruption cause is not established. On continuation,
the old temporary worktrees/GNU source were absent. Persistent project checkouts
and a fresh source/ABI-matched GNU build are being restored for complete reruns.

Earlier failures stay preserved: source162's live-input mismatch, source163's
three ordinary mismatches, source164's compile error and source165's three
macro-dispatch mismatches. Retained main reproduces the callback and dispatch
failures. No existing selector, timeout, assertion or expected outcome is
weakened. Source161's complete validation certifies that earlier source only.

Temporary compound-event/enumeration adapters and bounded input-decoding and
signal paths still require work. Symbol value/function/property ownership,
physical allocation accounting, the final adversarial audit, complete
final-source compatibility and locked performance criteria remain mandatory.
Darwin GNU is source/ABI matched but differs from the frozen executable. The
focused review in the archive is not the final goal audit, and this checkpoint
makes no performance claim.
