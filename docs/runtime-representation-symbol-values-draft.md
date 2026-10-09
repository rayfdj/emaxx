# Compact symbol fields — 9 October 2026

Source315 is a **separately packaged, unapplied draft** based on source314
`2712c057`. The runtime in PR80 remains source314. The [portable draft and
selected evidence](handover/2026-10-09-symbol-fields/source315-compact-symbol-draft-manifest.json)
preserve the exact patch, inputs, commands, outputs and GNU reference review.
The full runtime/performance goal remains active and incomplete.

Value and function payloads now use one Lisp word each. GNU's `Lisp_Symbol`
uses `Qunbound` for a void value and `Qnil` for a void function. The current
localized-value adapter still distinguishes an absent default from an explicitly
void binding; its existing enumeration position preserves that distinction.
The function cell tests its actual nil word, without a separate discriminator.
The cell's actual size is 64 bytes, checked in both profiles. This is not the
complete physical symbol footprint: allocated symbol/name storage, side tables
and enumeration storage remain additional costs.

The alias target exists only in the symbol cell. The interpreter's separate
`Vec<(String, String)>` and its repeated reverse search and string copies are
removed from normal alias installation and image restoration. An ordered ID
vector preserves existing enumeration without copying names or targets; its
entries are not GC roots. GNU `SET_SYMBOL_ALIAS`/`defvaralias` read and write the
actual target, and `pdumper.c:dump_symbol` records the same fields.

This is still an intermediate representation. Value and alias retain separate
slots instead of GNU's redirect union, and mutable fields remain per interpreter
while allocated symbols/names are process shared. The existing internal alias
transition tests remain byte-for-byte intact. Instance-aware interning and image
copying must precede direct allocated-symbol ownership; that work must remove
the weak side table, GC-only fixed-point walk and temporary ordered enumeration.
No new ordinary field access acquires a hash lookup.

Formatting, all-target/all-feature checking and strict Clippy pass without
warnings. **All 126 selected tests pass in gate and release**, including all
24 source314 controls, image/dump, alias/void/obarray controls and both original
suspended-root tests. The two new tests check nil versus void, image replacement,
alias retargeting/removal and enumeration through compaction. All previous
library selectors remain; the inventories add exactly those two names.

**Twelve ordinary GNU/Emaxx processes / six comparisons match**, using each
editor's own unchanged executable/image. The four source314 fixtures are
unchanged, with real bytecode and native execution also checked for plist
mutation. The portable patch reproduces all **560 source inputs and checkout
modes**; 102 auxiliary inputs remain unchanged. The GNU reference is the recorded
rebuild, not the lost original Darwin executable.

The separate [complete macOS Rust evidence](handover/2026-10-09-symbol-fields/source315-macos-rust-results-manifest.json)
verifies **3,163 passes / two existing ignores**: all 3,066 raw library names,
60 binary tests and 39 integration tests, including native artifact identity
and public runtime ownership. Both wrapper source captures and all retained
inputs agree with the selected draft. Source315 Linux, full frozen and timing
have not run; source314 results do not certify it. The cumulative
[source316 continuation](runtime-representation-symbol-redirect-draft.md) now
shares the value and alias payload. It requires its own complete validation.
No speedup, physical allocation accounting, allocated-symbol authority or GNU
parity is claimed.

To reproduce, start at `2712c057`, apply `evidence/compact-draft1.patch` from the
archive, and run the unchanged goal checks with the pinned GNU build. The archive
records both profiles' selectors and ordinary commands. The remaining complete
goal, including intervals/pure storage, real counters, ownership review and the
locked 16-workload/nine-round/every-workload 3% criterion, is unchanged.
