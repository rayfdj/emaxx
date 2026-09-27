# Runtime representation handover — 27 September 2026

This is an active implementation, correctness, and performance project, not a
completed optimization. Start by reading [the full goal](runtime-representation-goal.md).
The user wants GNU Emacs semantics, one authoritative compact object representation
across interpreter/bytecode/native execution, ordinary 16-byte conses, then less work
per instruction and call. Full adversarial de-cheating, clean rustfmt/compiler/Clippy,
Linux/macOS validation, and the locked performance criterion remain mandatory.

## Resume from here

The latest verified **runtime checkpoint is on remote `main`**. Its commit is
`9002451fa10c24fb75be650450614913c8c72575`; its last production change is
`91000d02da3729a38d7ceb6c1a929ade1d347eba`. The former changes documentation only.
The user explicitly requested integrating ready work into main and removing merged
branches. Main was fast-forwarded from `45eb1531caabb35b816fc83e2e8a5b88090dae4e`.
This handover is intended to be committed after that checkpoint without changing
production source.

On another machine:

1. Clone `https://github.com/rayfdj/emaxx.git`, fetch, and check out current `main`.
   Read any applicable `AGENTS.md`; none applied in the original workspace.
2. Read this file, the full goal above,
   [the original adversarial audit](adversarial-decheating-audit-2026-09-21.md),
   [the performance contract](core-runtime-performance-contract.md), and
   [the canonical-overlay checkpoint](runtime-representation-overlay-object-checkpoint.md).
3. Read [the portable evidence manifest](handover/2026-09-27/manifest.json).
   The unfinished char-table source is packaged separately in
   [char-table-wip.patch](handover/2026-09-27/char-table-wip.patch).
   Run `git apply --check docs/handover/2026-09-27/char-table-wip.patch` before applying
   it to a clean checkout. It targets the runtime source at `9002451`; later
   handover-only commits do not alter those files.
4. Reproduce the GNU baseline before changing assertions. Wire and validate the
   drafted char-table storage; then remove the remaining frame bridge and the
   duplicate cons payload. Do not skip to VM micro-optimizations while ordinary
   field access still reconciles representations.
5. Continue using the repository's existing GitHub Actions workflows for Linux.
   The user explicitly authorized that approach and asked for fewer branches.
   Do not recreate merged historical CI/WIP branches just to retain history.

The patch is a **development draft**: its allocator module is not wired into the
crate and has not compiled or passed tests. Keeping it as a patch makes the
handover's main checkout buildable while preserving the work. The patch also adds
one deliberately failing regression and its Lisp fixture. Do not call that test a
pass, ignore it, or weaken its expected output.

## What is actually verified

Source71 makes overlays actual GNU-layout objects: a 32-byte `Lisp_Overlay` owns an
80-byte interval node and its real Lisp plist. It removes overlay IDs, native
handles, detached registries, duplicate mark bookkeeping, and guessed node costs.
Buffers, terminals, markers, static subrs, finalizers, ordinary vectors, closures,
strings, records/bignums/reader objects and floats have already received related
ownership/word changes. Their historical milestones are recorded in
[runtime-representation-progress.md](runtime-representation-progress.md).

Final source71 selected validation:

- macOS: 443 distinct passing tests across overlapping overlay/root/native/
  buffer/dump/TTY selections. Two existing release-only TTY tests were unexecuted,
  **not passes**. Fresh binary SHA-256:
  `368b8d79aa8105a0f4c80ed0ae40a483b2b1a89578b2f5e37c95045ce2f7a751`.
- All-target/all-feature checking and strict Clippy `-D warnings`, formatting,
  diff checking, and the fresh test build passed with zero compiler warnings.
  The local build is opt-level 1 with assertions, not the release/full gate.
- Linux [36288069773](https://github.com/rayfdj/emaxx/actions/runs/36288069773):
  51 overlay controls and all 406 GNU buffer-file outcomes, no skips/ignores.
- Linux [36288071452](https://github.com/rayfdj/emaxx/actions/runs/36288071452):
  20 root controls and all 5 GNU allocation-file outcomes, no skips/ignores.
- Each Linux archive passed 31 raw consistency checks, including complete
  inventories, process success, source, executable/image identities and hashes.
  The existing affected workflow does not retain the Rust libtest executable's
  SHA; its GNU comparison does record editor/image identities. Keep that limitation.
- The actual Rust interval-tree source agrees with configured, unmodified GNU C
  for 32 seeds / 202,656 operations. An independent linear model and four evidence
  rejection controls also pass. Reproduce with `tools/check_overlay_itree.py`
  and `tools/test_overlay_itree.py`; see the overlay checkpoint for commands.

The portable directory includes the exact selected-check receipts. Their original
absolute paths are provenance, not paths expected to exist on a new machine.
Download complete CI artifacts from the linked runs if needed. Their archive hashes:

- Overlay/buffer: `82dbbe50e055e950224d76abc45234dc4d6346c779d4cae984a467b666a84785`.
- Roots/allocation: `8dfb79d67f9b445775ee4e94a95b2d6f9e3ade1f437ec4e16e92d5e527ce3632`.

**No GNU performance parity or measured speedup has been established.** The last
buffer diagnostic was Rust 2,820 ms / GNU 336 ms body (reported 8.392x), setup
3,999/152 ms and total 6,862/500 ms. Allocation body 38/5 ms is too short for useful
precision. These single CI samples are not the locked, repeated/interleaved
performance experiment. Keep the 16 workloads and 3% criterion unchanged.

## Immediate work: canonical char-tables

The current implementation uses a `CharTableState` ID registry, an append-only
write log, an ASCII index and resolved-range maps. The log retains overwritten
values. Native code uses a different bridge word. GNU uses inline slots and a
three-level radix structure; moving only metadata would not remove this overhead.

The new regression is
`char_tables_follow_gnu_defaults_inheritance_and_reclaim_overwritten_values` in
`src/lisp/primitives/tests.rs`, with
`tests/fixtures/char-table-authoritative-values.el`. Its exact failing baseline is
preserved in the portable directory:

```text
GNU:  (initial initial default parent parent replacement-default range t (slot-initial slot-initial t) (replacement 0))
Rust: (parent parent nil nil nil nil parent nil (nil nil t) (replacement 1))
```

Baseline source72a is unchanged production71 plus this test and fixture. It
compiled with zero warnings, then failed exit101: one executed test, no ignore,
no timeout, verified source and executable. Binary SHA-256:
`d1ce79f7bc742e781a6beb6699617951bf2f1095d5c4e0b245b55c66b82c4fab`.

The fixture deliberately allocates/replaces/clears its old value inside a separate
Lisp call frame, then collects after returning. Earlier GNU probes retained the
value conservatively while its evaluator frame was still active. The corrected
GNU output is zero weak keys. Do not change this expectation to one or move the
collection back into the allocating frame.

The draft `src/lisp/alloc/vectors/char_tables.rs` defines thin `CharTableRef` and
`SubCharTableRef` pointers, GNU header/slot allocation, get/set/range/copy, ASCII
aliasing, a leaf-range walk and compressed Unicode-property decoding. **Nothing
imports it yet.** It assumes future `VectorTag::{CharTable,SubCharTable}` and
`Kind::{CharTable(CharTableRef),SubCharTable(SubCharTableRef)}` variants, which do
not exist in main. No interpreter/native/GC migration has yet been applied.

GNU layout, confirmed by the included configured C probe:

- `PVEC_CHAR_TABLE=32`: 8-byte header; default offset8, parent16, purpose24,
  ASCII32, 64 contents starting40, extras552. There are68 standard Lisp slots.
  Logical size without extras is552 bytes; the allocator rounds to560.
- `PVEC_SUB_CHAR_TABLE=33`: header8, packed `int32 depth` at8 and `int32 min_char`
  at12, Lisp contents starting16. Contents counts16/32/128 at depths1/2/3.
- Character radix bits `[6,4,5,7]`, shifts `[16,12,7,0]`, maximum `0x3fffff`.
- GNU constructs these using ordinary vectors then changes the pseudovector tag.
  The public header records the slot count; do not invent REST bits for padding.
  `VectorHeader::nbytes` needs the correct rounding for these two tags.
- GC skips the packed depth/min_char word in subtables. It is not a Lisp value.
  Count real root/subtable allocations and trace current slots only.

GNU semantics to retain:

- INIT fills default, ASCII, contents **and extras**. Changing default preserves
  initialized contents. Nil content falls through child default, then parent.
- Purpose is the actual symbol object, including uninterned identity; do not
  reconstruct it from `Option<String>`.
- A range query returns the value at FROM. Extras are fixed at allocation;
  invalid indices error. Parent cycles error. `optimize-char-table` invokes its
  optional predicate and actually collapses nodes; current Rust silently no-ops.
- Copy duplicates internal subtables and shares ordinary Lisp values. ASCII is
  an alias of the actual leaf or uniform value, not another shadow array.
- Unicode-property compressed leaves, `#^`/`#^^` literals, dumping and equality
  must retain GNU's actual graph/structure semantics, not flatten write history.

Draft review still needed: compressed-input bounds/error behavior; interned
purpose identity (GNU `UNIPROP_TABLE_P` uses EQ); all raw-pointer/lifetime and GC
contracts. An unwired draft is not evidence of soundness or correct native access.

Key migration locations (use names, since line numbers will move):

| File | Work remaining |
| --- | --- |
| `src/lisp/types.rs` | Replace private immediate char-table ID with canonical pointer word; add subtable kind; update exhaustive matches. |
| `src/lisp/alloc/vectors.rs` | Wire module, tags32/33, header rounding, tracing/value reconstruction, destruction and actual census. |
| `src/lisp/eval.rs` | Remove `CharTableState` storage/log/indexes/global strong registry and ID marks; migrate startup tables, fields, roots, copier and census. |
| `src/lisp/eval/runtime.rs` | Replace ID lookup/mutator APIs; implement correct defaults/parent/range/extras/copy. |
| `src/lisp/primitives/dispatch/collections.rs` | GNU constructor purpose/extras, bounds/cycles, range/map/optimize behavior. |
| `src/lisp/primitives/print.rs`, `reader.rs` | Actual root/subtable literals, sharing and printing; existing materializer flattens. |
| `src/lisp/primitives/dispatch/strings.rs` | Compressed Unicode-property tables and decoder extras. |
| `src/lisp/primitives/values.rs`, `completion.rs`, `keys.rs` | Equality/hash, copy and current table values, not history. |
| `src/lisp/eval/coding.rs` | Case/category tables and category docs; inspect GNU `category.c` extras instead of retaining separate shadow docs. |
| `src/lisp/primitives/regexp.rs`, `syntax.rs` | Remove invalid mutation-door/cache assumptions. |
| `src/lisp/native_comp/runtime.rs` | Remove char-table native handle; raw canonical words/fields/GC. Frame is then the last bridge kind. |
| `src/lisp/primitives/pdumper/{context,load,image}.rs`, `eval/dump_roots.rs` | Canonical graph objects, sharing/roots, no numeric table IDs or write-log records. |

**Cache trap:** regex/syntax/case caches depend on generation changes through
`find_char_table_mut`. Native raw field stores bypass that door. A new pointer
layout with those caches unchanged remains wrong. GNU reads the actual tables in
matching; inspect those owners before preserving or replacing these caches.

The architecture notes copied into the portable directory describe the baseline
and intended migration. Their opening statement that no implementation has
started predates the unwired draft; this handover supersedes that status sentence.

## Remaining goal-wide obligations

1. Cons cells are still **80 bytes**: a16-byte native prefix, two24-byte typed
   value cells, and16 bytes of mark/serial metadata. Reads check agreement; writes
   touch epochs/watchers/native dirty queues. The raw-native nested-cons acceptance
   failure is preserved. Remove duplicate payload/conversion/notification work;
   moving it behind a lookup is not completion.
2. Char-table and frame native bridges remain. Complete shared object identity
   before shrinking conses that can contain those object kinds.
3. Full GNU buffer/native prefix/ABI, canonical symbol-cell authority, indirect
   buffer text sharing, GC accounting and public API/rooting/serialization
   soundness remain open. Serial tests alone do not prove ownership safety.
4. Conservative scanning of Rust storage and the old64KiB post-GC stack clear need
   review. That clear predates this goal at`7e3b762`; no current evidence justifies
   removing it or treating it as a certified mechanism.
5. Both required lexical-caller/suspended-bytecode survival/reclamation contracts
   now pass selected macOS/Linux root groups. Preserve their exact contracts and
   failed history. The compiler-spill diagnosis and direct let-environment fix
   are documented in the progress file; do not revive weaker ignored tests.
6. Historical full Linux validation aborted in EIEIO with an unknown native cons;
   current full gates have not been certified. Linux coverage also has the known
   missing `set-text-conversion-style` primitive. Do not count these as passes.
7. Overlay category/alias-dependent evaporation, nested-hook semantics and
   existing display-priority approximations still need the broader GNU audit.
8. Branch review found several old controls labeled “remaining bridge control”
   now create markers. Markers have since become canonical. Verify that controls
   intended to exercise a nonempty native handle table still do so; do not infer
   that path's coverage from their old names/comments or from a green result.
9. Final source needs Linux **and** macOS full frozen comparison, release-specific
   paths, native/module/dump/startup/thread/GC integration, complete adversarial
   review, profiles and the locked repeated/interleaved performance suite.

Required final commands from the goal:

```sh
cargo fmt --all -- --check
cargo check --locked --all-targets --all-features
cargo clippy --locked --all-targets --all-features -- -D warnings
git diff --check
python3 tools/serial_grouped_gate.py --scope full
```

Use unchanged selectors, expectations, ignore inventory, timeouts and reporting
strictness. Keep failures, unfavorable timings and unsuccessful child processes.
No new allowances, fake capabilities, oracle forwarding or missing-report passes.

## GNU and build environment

GNU source revision: `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`.
The original machine uses sibling checkout`../emacs`, executable`../emacs/src/emacs`.
That local executable's SHA-256 is
`12c762a0768f17c2b9779302cf98bc143c49ac7548c9b0412f17e38a967d19f1`.
It has the correct GNU source but differs from the pinned Darwin executable:
local GNU comparisons are diagnostic, not pinned Darwin certification.

Do not copy a Darwin image/native artifact to Linux and claim equivalence.
Follow the repository's oracle locks, generated native ABI configuration,
executable/image fingerprints and existing CI. The frozen workflow checks out
that GNU revision and verifies the generated Linux native subr table.
Its selected runs use `frozen-run.yml` with `validation=affected`, `file`, and
`rust_filter`; full modes already exist. Keep separate evidence for each mode.

The original machine is macOS ARM. The shared Cargo build cache is
`/Users/rayd/projects/emaxx/target/runtime-broad`; the frozen validation checkout is
`target/runtime-validation-68/emaxx`. These are optional local accelerators,
**not portable prerequisites**. Heavy builds were serialized. Existing socket
controls require localhost access. Gate environment uses `RUST_MIN_STACK=134217728`,
serial test threads and valid fixture images; the local selected runner timeout
is3600 seconds, not300. Freeze source during validation and verify it afterward.

## Git state and branch cleanup

Completed at this handover's creation:

- Remote and unused local`main` advanced to`9002451`.
- Deleted remote`wip/compact-runtime-shared-object-words` and
  `fix/frozen-run-success`, both contained in main.
- Deleted remote`native-comp`: all six divergent commits are patch-equivalent
  to commits already on main. Its older local branch was an ancestor of main
  and was also deleted.
- Pruned one missing temporary worktree registration. Kept the real validation
  checkout and all active work. A complete recovery bundle of these deleted
  branches is retained on the original machine under`target/runtime-goal/merge-main-73/`.

Three historical branches still exist while the final cleanup review finishes:

| Branch | Head | Review status |
| --- | --- | --- |
| `ci/runtime-representation-floats-20260923` | `16da098bf8544b7449085456be68becb28100f9d` | Earlier float/runtime snapshot superseded by representation work; exact tree recovered and compared. |
| `ci/runtime-representation-vectors-20260923` | `24373bb937e5e7396ecd91a6fadd57619b0c732b` | Earlier vector/relocation snapshot; exact tree recovered and compared. |
| `fix/native-relocation-root-lifetime` | `ee5c2f42cbc9fdede1ab02a4882d4ba81c27957a` | Fixes integrated into representation work, notably`51b9847`; old bridge-specific tests require final mapping before deletion. |

Two Git fetch attempts failed with connection resets. GitHub API patches were
applied using isolated indexes and their complete trees matched GitHub exactly:
`47ff353eb1735bb46959bf2a7cd07ce608037fb2`,
`66292e22c916ed1e44dfb1b26c8c618e2f32011d`, and
`30a1914f4b1de403174fb1ec765f9cc4520a7522`, respectively.
The proposed merge differences are older object layouts/bridge paths, moved
regression tests, and duplicated diagnostics, not a reason to restore old code.
Finish the mapping, preserve their history/evidence, then delete only the reviewed
heads with leases. Fetch current branch state first; this table is a snapshot.

On the original machine, the active local branch is still`compact-runtime` at
`45eb153`. Its dirty working tree contains the already-committed runtime changes
plus the char-table draft. **Do not reset, stash, clean, or wholesale checkout it.**
The normal index hash is
`a78eb42c142f43070f098cc07a7ba2b222fcc9ca1af140bd95cd7082cb64c36b`.
Commits were constructed with isolated indexes/`commit-tree` to preserve that state.
The file`docs/runtime-representation-checkpoint-23.md` is missing only from this
old working tree but exists in main; do not accidentally commit its deletion.

Use author/committer Ray `<26018378+rayfdj@users.noreply.github.com>`; no AI trailers.
No subagents were authorized. Keep the goal active: this handover and checkpoint
integration are milestones, not completion of the implementation/performance goal.
