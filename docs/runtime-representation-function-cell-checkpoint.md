# One function payload per interpreter symbol cell — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
work follows the combined ownership checkpoint at
`9f000b24bbe61c4c6142d629a33f861c828559e6`. Source 89 passes all 447 selected
release tests (506.90 seconds) and all 322 selected debug tests (2,979.29
seconds), with zero failures or ignores and the optional GC verifier enabled.
Strict formatting, all-target/all-feature compiler checks, Clippy with warnings
denied and diff checking pass. A fresh ordinary executable and image match GNU
on all 22 identical inputs. The [portable evidence](handover/2026-09-28-function-cells/manifest.json)
retains the original failed controls and every final result. No full, frozen or
performance certification is claimed.

## Removed copies

Function definitions previously occupied three mutable places: the symbol's
function cell, a `(String, Value)` vector, and a name-to-value hash table. A
second hash table tracked positions in that vector. Stores maintained all three
values; static GC roots traced the vector; image cloning copied every version.
Cycle validation walked the vector backwards instead of reading symbol cells.
GNU `lisp.h:set_symbol_function` writes one function cell, and `data.c:Ffset`
follows those same cells when checking for a cycle.

The function vector and both name-keyed hash tables are removed. Function lookup,
static root tracing, cycle checks and image copying read the existing symbol
cell. Tracing includes direct initialization/image stores as well as ordinary
Lisp definitions. No old payload remains to retain a replaced definition. The
existing generation-based invalidation of derived call resolutions remains in
the ordinary definition setter.

Enumeration retains symbol ids in first-definition order, with a one-based
position in the cell and bounded compaction of stale positions. Replacing a
bound definition keeps its position; voiding and defining it again moves it to
the end. This preserves the existing obarray enumeration without three owned
name copies, two extra value copies, reverse scans, or maintenance of those maps for each
function definition. The metadata is an adapter until the obarray directly owns
its enumeration. It is not an additional function payload or a new object model.

Read-only DWARF inspection of the immutable source-88 and source-89 debug
executables reports the per-interpreter `SymbolCell` as 64 bytes in both. The
four-byte optional position fits existing padding on this build. The id vector
uses four bytes per slot plus capacity, stale slots, and its vector header; its
live count is another machine word. These are layout/accounting observations,
not measurements of total heap savings or execution time. The tracer now scans
the actual dense and sparse function cells; enumeration, its incremental cache,
and compaction have not been profiled.

## Preserved failure evidence

Source 88 adds two architectural controls to unchanged production. Both fail.
They deliberately write the existing canonical function cell directly, as GNU's
single-cell setter does, without reconciling detached payload copies. The static
root tracer then marks the replaced value and misses the current value:
`(current=false, old=true)` instead of `(true, false)`. The cycle checker accepts
a new definition that closes a three-symbol cycle through another direct store.
These are controlled internal writes exposing representation disagreement, not
a claim that ordinary `fset` previously omitted its mirror updates.

Both controls remain byte-identical in source 89 and pass. A separate ordering
control checks replacement, nil and repeated void stores, redefinition after
removal, and 3,000 transient definitions through compaction. The tracer control
checks exact static edges without conservative stack retention; it is not a
full-collection reclamation sample. Both original suspended-thread survival and
reclamation contracts pass in the broader release run.

The three existing public function-cell fixtures match GNU on the immutable
pre-migration source-87 ordinary executable. Those passing before-results remain
separate from the two failed structural controls. The new ordinary comparison
passes those three plus every one of the earlier nineteen reader/printing
inputs: all 44 processes exit zero and stdout and stderr match byte for byte.
Software, executable and image identities remain unchanged. The executable
SHA-256 is `ad938409800b9f98688b0b8079d897e1dec3f22767eef2b68de7648c68292333`;
the image SHA-256 is `2a1b89091205a9bb67336f9c49ad50059792ad6892917b1d3e46fc6497846285`.

The release inventory contains 2,870 tests, preserving all 2,867 in source 87
and adding the two structural controls and the ordering control. No existing
selector is removed. The broader debug run also passes both original suspended
thread survival/reclamation contracts. These selected checks do not replace a
full gate on the eventual final source.

## Remaining goal

This is still a per-interpreter cell table indexed by symbol id. The allocated
symbol does not yet own GNU's full value/function/plist layout. Obarray ownership,
remaining symbol registries and derived call-cache behavior remain part of the
full representation and adversarial audit. Generic records still use a detached
slot vector; their inline layout and GNU size boundary are under separate implementation
and validation. No generic-record repair is included here.

The prior complete macOS and approved Linux runs remain failed as documented in
the [reader checkpoint](runtime-representation-reader-checkpoint.md). Linux
text-conversion dispatch, the earlier sixteen terminal divergences, remaining
hash/keymap and buffer/frame representation, dump mapping, real allocation
counters, VM/call profiles, final Linux/macOS full/frozen/terminal validation and
the locked sixteen-workload 3% performance criterion remain open. The local GNU
executable is source-matched but differs from the pinned Darwin executable.
