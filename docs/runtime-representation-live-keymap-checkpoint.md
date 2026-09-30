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
publishes these inputs at `3bada057`. Full Linux validation now **fails**; the
[raw failure archive](handover/2026-09-30-keymap-linux-failures/manifest.json)
cross-checks the original inventories, all reported outcomes and retained Rust
artifact hashes.

[Linux Rust](https://github.com/rayfdj/emaxx/actions/runs/36647925886) passes
1,607 tests across five eval groups, then primitives passes 594 and fails the
unchanged `suspended_bytecode_retains_operand_and_unwind_roots` contract. Its
child observes `((1 t 1) (1 t 0))`, expected `((1 t 0) (1 t 0))`: the cleanup
closure's weak key survives after the thread finishes. The four remaining
groups and both Cargo stages do not execute. The reporting parser rejects the
nested failure summaries; that is downstream of the real assertion failure.
An [isolated replay](https://github.com/rayfdj/emaxx/actions/runs/36656497828)
using the same executable SHA-256 reproduces it with no GC verifier override.

[Linux frozen](https://github.com/rayfdj/emaxx/actions/runs/36647928962) completes
all 519 files / 7,928 outcomes and all 1,038 successful processes. It records
7,926 matching outcomes and two mismatches: autorevert tail mode signals
`(wrong-number-of-arguments file-notify--callback-inotify 2)`, while asynchronous
IPv6 TLS sees a nil certificate issuer. GNU reports 7,670 passes, 47 existing
expected failures and 211 skips; Emaxx reports 7,668 passes, 49 failures and
211 skips, including two unexpected failures. Separate ordinary replays of
[autorevert](https://github.com/rayfdj/emaxx/actions/runs/36656500347) and
[network-stream](https://github.com/rayfdj/emaxx/actions/runs/36656502557) abort
on `use of a cons the collector freed`, leaving no complete Emaxx result
inventory. Their subject executable and compiled-source identities match the
failed frozen run; independently built image hashes differ. None clears the
failed full comparison or establishes the cause of the earlier symptoms.

The initial full macOS runs were interrupted. Five Rust groups pass (1,607
tests), then primitives receives SIGTERM before its result summary; later
groups and Cargo stages do not execute. The terminal log stops during scenario
33/223, without a final result or post-run hash verification. The
[interrupted-run receipts](handover/2026-09-30-keymap-interrupted-runs/manifest.json)
remain incomplete. Their interruption cause is not established. On continuation,
the old temporary worktrees/GNU source were absent. Persistent project checkouts
and a fresh source/ABI-matched GNU build now support active complete reruns.
The fresh GNU native ABI check passes; its executable still differs from the
Darwin frozen pin. No pin, test selector, timeout or expected outcome changed.

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
