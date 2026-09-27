# Canonical overlay checkpoint — selected local and Linux controls verified

Commit `91000d02da3729a38d7ceb6c1a929ade1d347eba` (source71) replaces the overlay ID with a canonical 32-byte GNU
`Lisp_Overlay`: one vector header, its actual Lisp plist, a weak buffer pointer,
and an owned interval-node pointer. Interpreter, bytecode and native words use
that same object address and `PVEC_OVERLAY=4` header. Char-tables and frames
remain native bridge kinds; ordinary conses remain 80 bytes.

GNU source `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd` supplies the design:
`lisp.h:Lisp_Overlay`, `itree.h:itree_node`, `itree.c`, `buffer.c` overlay
primitives, `alloc.c:mark_overlay/mark_overlays/cleanup_vector`, and
`pdumper.c:dump_overlay`. The configured layout probe records the 32-byte
object and 80-byte node, including offsets and flag bits. A buffer owns the
interval index; the overlay owns its node. Node fields use Rust `Cell` to
permit offset validation and native field stores without aliased mutable Rust
references. Tree mutation requires an exclusive index borrow. Buffer sweeps
unlink the weak endpoints before either allocation can be freed.

Ordinary property access no longer scans buffers or looks up an ID. Edits use
GNU's augmented red-black tree and deferred subtree offsets. The detached
allocation map, allocation-ID counter, separate GC overlay mark set, guessed
overlay census, native overlay handle, and image holder-ID bookkeeping are
removed. The vector allocator counts the actual object footprint; its owned
80-byte node is included separately in allocation and retained-byte accounting.
The node/index design can use more storage than the previous flat Rust payload;
no memory reduction or elapsed-time improvement is claimed.

The migration also follows GNU's full-buffer endpoint clamping, marker-owner
and error-check order, `move-overlay` default buffer, current `overlay-lists`
shape, interval enumeration, and primary/secondary priority ordering.
GNU's nesting/secondary comparison
is not a total order: the adversarial seed in `overlay-sort-diagnostic-71.json`
made Rust's `sort_by` panic. Sorting now uses the same platform `qsort` as GNU,
with a callback that cannot call Lisp or unwind. The crossing-interval Lisp
fixture checks complete membership, repeated ordering, collection and changed
primary priority without prescribing an order GNU leaves platform-dependent.
Ordinary property queries visit intersecting intervals and decode bounds once;
the duplicate window-property filter and full-index scan are removed. Detached
overlays compare by their nil buffer and public -1 endpoints, then their plists;
equal hashing now follows those fields. Pending modification hooks retain their
actual Lisp function lists. Each callback checks that its overlay still belongs
to the current buffer. Rooting covers the complete before/edit/after lifetime,
including callbacks that remove overlays, mutate lists or collect.

Image restoration allocates each overlay once before relocating its fields.
The image-template copier memoizes the actual object graph and reconstructs
buffer links explicitly. Text snapshots carry no copied interval links or
extra overlay allocations. The existing dump-sharing assertions are retained.
The two-word cons objective, full buffer prefix/ABI, canonical symbol cells,
indirect text sharing, full runtime ownership/rooting soundness and allocation
accounting remain open.

Validation at this point:

- Source71e passes all-target/all-feature strict Clippy (`-D warnings`), and
  source71f repeats the same source through `cargo check`, with
  source hashes verified after execution. Earlier type-check/lint failures are
  retained in `buffer-ownership-check-71a` through `71c` receipts. The
  final source also passes `cargo fmt --all -- --check` and `git diff --check`.
- `tools/check_overlay_itree.py` compiles this Rust tree and the unmodified,
  configured GNU C source. All 32 seeds / 202,656 operations agree, including
  insertion/removal, moves, both gap operations, all traversal orders, narrowing,
  ties and empty intervals. An independent linear model checks endpoints,
  membership and complete output inventories. Source/executable hashes and all
  raw inputs and outputs are retained in `overlay-itree-final-71e/`, with
  compiler versions and GNU revision/diff.
- Negative controls reject wrong endpoints, missing/duplicate members,
  incorrect counts, truncated/extra output, unsuccessful children and
  timeouts with partial output. All four evidence-control tests pass.
- The new same-input Lisp fixture's GNU output is recorded in
  `overlay-object-oracle-71b.json`; an earlier fixture parse error is retained
  separately. The local oracle has the correct source revision but differs
  from the pinned Darwin executable, so this is diagnostic evidence.
- The first fresh runtime run passed 49 of 50 overlay tests. The new exact
  node-byte assertion counted unrelated garbage left by preceding tests in its
  baseline; the original test passes alone. The fix collects before taking the
  baseline and retains every live/dead-object and exact byte-count assertion.
  Both the failed grouped result (`overlay-object-controls-71.json`) and the
  isolated pass (`overlay-object-controls-71b.json`) remain in the evidence.
- That same build passes 20 root tests, the native identity control, and 391
  buffer/dump/TTY tests. Two existing release-only TTY tests remain unexecuted
  and are not passes. These earlier results do not certify the subsequent
  sort/query fix.
- Source71e's fresh all-feature binary has SHA-256
  `368b8d79aa8105a0f4c80ed0ae40a483b2b1a89578b2f5e37c95045ce2f7a751`.
  All 51 overlay tests, 20 root tests, the native identity test, and 391
  buffer/dump/TTY tests pass against verified source and executable hashes.
  There are 443 distinct passes across these overlapping selections and the
  same two unexecuted release-only tests. The build has zero compiler warnings;
  its opt-level-1 test profile is not the release or full gate profile.
- Both existing Linux workflows pass and their raw artifact archives are
  independently verified: [overlay/buffer](https://github.com/rayfdj/emaxx/actions/runs/36288069773)
  runs all 51 overlay controls and matches all 406 selected GNU buffer outcomes;
  [roots/allocation](https://github.com/rayfdj/emaxx/actions/runs/36288071452)
  runs all 20 root controls and matches all five selected GNU allocation
  outcomes. Both have zero ignores/skips, successful processes, complete matching
  inventories, clean committed source and their own executable/image identities.
  Each archive passes 31 consistency checks. Existing CI records a fresh Rust
  test build/path but does not retain that test executable's SHA; that evidence
  limitation remains explicit.
- Current-source release/full gates and locked performance measurements remain
  pending. The source70 checkpoint remains independently available.

The CI timing is still unfavorable. The buffer file's recorded body time is
2,820 ms in Rust versus 336 ms in GNU (reported ratio 8.392x); setup is
3,999/152 ms and total is 6,862/500 ms. Allocation body time is 38/5 ms,
setup 483/75 ms and total 551/100 ms; that body is too short for a useful speed
claim. These are uncontrolled single samples on CI, not paired performance
measurements or evidence of improvement over source70.

The retained archives have SHA-256:

- Overlay/buffer: `82dbbe50e055e950224d76abc45234dc4d6346c779d4cae984a467b666a84785`.
- Roots/allocation: `8dfb79d67f9b445775ee4e94a95b2d6f9e3ade1f437ec4e16e92d5e527ce3632`.

`overlay-object-milestone-71.json` links the source, audit, local receipts and
verified CI reports under `target/runtime-goal/`. The generated tree comparison
can be reproduced with `python3 tools/check_overlay_itree.py --gnu-src ../emacs
--output target/overlay-itree-check` using a freshly named output directory and
a configured GNU checkout; its rejection controls run with
`python3 -m unittest discover -s tools -p test_overlay_itree.py`.

Native controls require the raw header, plist, buffer and interval fields,
mutation visibility and bytecode word identity. GC controls retain live plist
cycles, verify weak buffer ownership and require reclamation after the last
root is removed. They pass on the final source, including the complete grouped
execution.
Category/alias-dependent evaporation on text deletion and nested overlay-hook
state still need the broader GNU semantics audit. Existing display-priority
approximations and indirect-buffer text ownership are not certified here.
The dumper's existing executable fingerprint prevents an old binary's overlay
record layout from being accepted by this binary.

No upstream selector, expectation, timeout, ignore, lint allowance, workload,
measurement tolerance or CI workflow was changed. This checkpoint is not final
certification of the runtime or universal absence of cheating.
