# Editor-owned symbols — unfinished source319, 9 October 2026

This is a **separately packaged, unapplied draft**. PR82's runtime remains
source318; main contains source316 through merged PR81 (`30fa6e5b`). The
[portable draft and evidence](handover/2026-10-09-symbol-fields/source319-symbol-owner-unfinished-draft-manifest.json)
include an incremental patch on published `93b6546f` and a cumulative patch on
main `30fa6e5b`. Both independently replay all **563 inputs and modes**.
The complete runtime/performance goal remains active and incomplete.

GNU `lread.c:init_obarray_once` owns and roots the canonical symbols of one
Emacs process. An embedding process can run multiple editors. Source319 creates
an owner at the public batch, ERT, startup-image, version and terminal entries,
retains it in interpreter state, and restores a nested caller's owner on exit.
The canonical-name table, existing address lookup and fixed Qsymbol references
belong to that owner. The existing serialized allocator boundary remains; no
scope guard is added per VM instruction. Internal operations without an editor
entry still use the old host owner as an explicit migration adapter.

The original public-API negative is reproduced with byte-identical Lisp inputs
and the same normal, flushed public launcher. Source316 leaked `owner-one`
from the first editor's symbol-name string into the second. The draft returns
GNU's actual `(t owner-one)` then `(t nil)` results, with all processes successful.

The first descriptor probe appeared to pass because `SymbolName::PartialEq`
compared host spelling as well as identity. Replacing it with allocated-object
identity, following `data.c:Feq`, exposes the retained draft3 negative: the
second editor's standard hash descriptors return foreign `eq`, `eql` and
`equal` symbols. The actual result is `(nil nil nil t t)` against GNU's
`(t t t t t)`. The failed observation remains in the archive.

Draft4 makes each descriptor pool belong to its symbol owner. Standard tables
retain direct descriptor pointers; `hash-table-test` returns the descriptor's
actual name without reinterning or adjusting the answer. User descriptors retain
their real three Lisp fields as GNU `fns.c:mark_fns` requires. The unchanged probe
now returns `(t t t t t)` for both editor instances, matching GNU after forced
collection. A separate name-entry wrapper compares text for canonical lookup;
Lisp symbol equality compares identity. The old global weak uninterned book and
immutable host-name copy are still migration adapters.

Strict formatting, all-target/all-feature checking, Clippy and diff checks pass
with zero warnings. All **229 gate controls pass**, retaining the previous 208
and adding existing hash-table/vector controls. All 3,069 library names are
unchanged. Raw verdicts, public process logs, source captures, executable hashes
and both patch replays are independently checked. The first unused-import Clippy
failure and missing dependency-path launcher failure remain preserved.

The [completed selected continuation](handover/2026-10-09-symbol-fields/source319-symbol-owner-selected-results-manifest.json)
verifies **229 controls in each profile** with unchanged complete inventories,
plus **18 ordinary GNU/Emaxx processes / nine matching comparisons**, including
actual bytecode/native execution. Three restored public-image comparisons also
match. The first copied-image probe correctly fails the executable fingerprint
check; two subsequent standalone link setup failures are retained. The working
launcher links the same production library and builds its own valid image through
the unchanged normal CLI. The original executable/image checks are not bypassed.

The [public/native continuation](handover/2026-10-09-symbol-fields/source319-public-native-results-manifest.json)
verifies all **three existing public integration tests**, including concurrent
host entries, collection, nested version calls and complete visible ERT failures.
Four additional comparisons match across **12 successful GNU/Emaxx processes**.
Two successive editor entries load valid images, compile a native function that
returns their own canonical symbol, and collect; every actual native and identity
predicate agrees with GNU. The first native-worker attempt failed because the
standalone launcher treated child CLI options as input filenames. Its failure
remains recorded. The corrected launcher delegates normal options to the unchanged
CLI; the production library and Lisp input are unchanged. All local validation
processes for this source319 draft have ended.

The complete platform, frozen, terminal, native artifact identity, module and GC
contracts are not certified by these selected results. Source319 remains unapplied.

Remaining work is substantial and explicit. Private interpreter image clones
still share canonical symbols. Value/function/plist/alias fields remain in
per-interpreter cells, with IDs, enumeration and a weak-field GC fixed point.
Symbol roots can still be visited more than once. Global IDs also leave growing
prefixes in dense cell tables across successive editors; they are not a final
per-editor allocation strategy. The old descriptor allocations
still use static leaked storage; their reclamation and physical accounting are
unresolved. Other process-wide Lisp roots, including the Unicode menu table and
echo state, still need ownership review. `SymbolName` retains the old
name-borrowing trait whose text equality contract should be removed or separated
before this draft is accepted. No total-footprint reduction or timing gain is
claimed. Allocated authority, removal of the adapters, honest allocation/GC
counters, final ownership/adversarial audit, final validation and the unchanged
16-workload/nine-round/every-case GNU criterion remain required.
