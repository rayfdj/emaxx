# Inline generic records — selected checkpoint, 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
checkpoint follows `9f000b24bbe61c4c6142d629a33f861c828559e6` in the reused
`runtime-generic-records` worktree. Source 103 passes 20 selected debug and
438 release tests, all strict static checks, 29 ordinary exact GNU comparisons,
two startup locales and four focused terminal report comparisons. The
[portable evidence](handover/2026-09-28-generic-records/manifest.json) retains
all prior failures, including three callback failures in source 98 and the
invalid source identity in source 99. Validation with the function-cell and
Linux text-conversion changes still remains. This is not a full gate, frozen
comparison, performance result or completion of the goal.

## Representation

GNU `alloc.c:allocate_record` allocates a vector, includes the type in its first
Lisp slot, and tags it `PVEC_RECORD`. `lisp.h:RECORDP`, `data.c:Faref/Faset` and
the collector read those same slots. The maximum is 4,095 total slots, including
the type. The draft's `LispRecordRef` follows this layout: one header word,
then `Cell<Value>` fields. There is no id, owner, detached slot vector, type-name
registry entry, retirement entry, or per-store cache invalidation for generic
records. Native words name this allocation directly.

The allocator reserves `(data_slots + 2) * 8` logical bytes and rounds physical
storage to sixteen bytes. Its existing block bitmap or large-object mark storage
is additional metadata. Allocation charging uses logical bytes; live vector
census uses physical header/payload/padding words, following GNU
`pseudovector_nbytes` and `sweep_vectors`. This is accounting, not a measured
heap saving or speedup. Monotonic GNU allocation counters remain unimplemented.

Remaining host pseudovectors still use `RecordState` and ids under the old
`PVEC_RECORD` adapter. Their header has zero inline Lisp fields; every real
record has at least its type slot. That temporary distinction must disappear
when those host kinds acquire their own GNU layouts and tags. Generic record
operations no longer enter that adapter. The removed `RecordKind::Record` cannot
construct another legacy generic record.

Reader materialization, equality, copying, printing, substitution, tracing and
graph cloning use the inline fields. Reader forms remain a parser adapter.
Dump reconstruction has a separate inline-record object entry with ordinary
relocations for every field, including the type descriptor. It still reconstructs
objects rather than mapping an authoritative GNU dump; mapped dump ownership
remains a full-goal requirement.

## Evidence and failures

Source 90 added layout and size controls to unchanged production. The layout
control failed: zero inline Lisp fields where at least one was required. The
size control first stopped on an erroneously appended newline in the expected
GNU bytes, before comparing Emaxx. Source 91 changed only that expected file to
the captured GNU bytes. Both controls then failed on the actual representation
and oversized-record acceptance (0 passes, 2 failures, 115.99 seconds in libtest).
Both immutable sources and their raw failures remain retained.

Source 92's first implementation failed compilation on two missing exhaustive
kind arms and a missing enum import in a test. No tests ran. Source 93 repairs
those errors: all strict static checks pass, but its 102 selected debug tests
finish with 94 passes and eight failures (1,693.06 seconds, zero ignores).
Five failures expect the old Rust storage variant. Two expose a genuine omitted
ERT migration: registration and result stores still use the old registry, and
the native test runner discovers zero tests. The eighth is a new test mistake:
it calls the deliberately shallow interpreter clone while requiring a deep
image clone. The raw failures remain preserved. Source 93 passes the unchanged
native layout/edge control and the original record graph survival/reclamation
checks. Its inventory preserves all 2,870 source-91 tests and adds three.

Source 94 adapts those five storage assertions without removing field/type
checks, migrates ERT record access, and calls the actual deep-image-clone API
while preserving the distinct-address, sharing and cycle assertions. Its new
scoped-root calls initially use fixed arrays unsupported by the existing root
trait, so debug/release builds, check and Clippy fail compilation; no tests run.
Source 95 changes those two calls to root the single `Value` directly. It passes
107 selected debug tests (1,656.51 seconds) and 425 selected release tests
(815.54 seconds), zero ignores, and all strict static checks. Those selected
passes do not cover the failures subsequently found below. No failure is counted
as a pass.

The ordinary source-87 before-probes preserve separate public behavior. The
graph fixture matches GNU exactly for sharing, cycles, equality, copy-sequence,
mutable descriptors and collection. Constructor and reader boundary probes
show oversized-record acceptance. The construction probe also preserves a
previously missing stderr message when `purecopy` strips text properties; its
value result already matches GNU. That diagnostic is not repaired in source 93.
The argument-error before-probe also exits with a capacity-overflow panic for
`most-positive-fixnum`, where GNU signals the record size error. Another
before-probe finds `text-property-not-all` returning nil instead of position 5
for two distinct but structurally equal record values. GNU's `textprop.c` uses
EQ for this search. The draft repairs both the buffer and string search paths,
and preserves record identity when combining adjacent property spans.

These before-probes use identical input and verify source, executable and image
hashes. The local GNU binary is source-matched but is not the pinned Darwin one.

The original native record survival/reclamation tests retain all live and dead
object assertions. The former generic owner/id collision assertion now uses
host pseudovectors, which still have owner/id spaces, alongside generic address
identity checks. The obsolete generic type-index test instead verifies direct
retagging, absence of generic registry growth, arbitrary type descriptors and
continued host-window indexing. No selector is removed. The dump graph helper
now compares every generic record field, replacing its former opaque-record
acceptance for this kind.

## Interactive copying and locale failures

Source 95 passes 27 of 28 ordinary exact GNU stdout/stderr comparisons. The
remaining construction case returns the correct value but prints grave quoting
from the dump builder's locale, instead of the current process's curved quoting.
A separate C-built-image probe passes startup under `LC_ALL=C` and fails under
`en_US.UTF-8`. Both original outputs are retained.

Interactive message callbacks also expose actual root and mutation defects. An
ordinary terminal probe returns `(1 130 nil t t nil)` instead of GNU's
`(1 130 t t t nil)`: collection during the final string's message loses earlier
copied vector fields. The separate five-sequence GC probe segfaults in source
95. Mutation of an original record during copying changes the eventual copied
slot, while GNU copies the original shallow snapshot. Changes to `purify-flag`
inside a message callback are ignored by the captured hash-cons table.

Source 97 roots the actual destination vectors and records, the pending copied
list cars and saved tail, copied hash keys/values, closure fields and the current
propertized string across callbacks. It shallow-copies vector/record fields
before recursing, and reads the current `purify-flag` for each table lookup and
insertion. Its record shallow-copy helper allocates the final slots directly.
After image restoration it refreshes the quoting locale from this process, as
GNU `emacs.c` does. This is still ordinary GC-managed storage, not GNU pure
storage.

The immutable source-97 ordinary binary and fresh image pass all 28 exact GNU
stdout/stderr comparisons and both independent startup locales. Four ordinary
terminal report comparisons also match GNU byte for byte: the original vector
callback, five sequence kinds with GC, vector/record source mutation without GC,
and three callback changes to `purify-flag`. Terminal screen bytes are retained
but were not compared. These focused reports are not a full terminal pass.

Keep the GNU limits: the expanded six-kind input aborts GNU during the hash
case; a five-kind plus source-mutation-and-GC input ends with pure-storage
overflow and SIGKILL. Neither provides a valid expected report. Source 97 exits
zero with preserved contents on the expanded stress input, but that is not a
GNU differential pass. The independently scoped five-kind GC, mutation without
GC, and flag-change inputs all finish in GNU and supply the three Rust fixture
expectations without changing their bytes.

Source 97's Rust test build, check and Clippy fail solely because the new locale
test compares a Result with an Option; no Rust tests run there. Source 98
corrects that assertion's extraction, preserving the expected value. Its builds
and all static checks pass, but the three new callback fixtures all report zero
callbacks in both debug and release. The same production code passes their real
terminal counterparts, so these failed embedded tests remain under diagnosis.
Source 99 adds standard reader interning/materialization and still fails all
three. Its formatter overlapped the validation launch, causing the recorded
source-identity checks to fail, and Clippy rejects an unnecessary mutable
reference. It is not an immutable-source certificate. Source 101's temporary
callback diagnostic fails compilation because the metric type lacks Debug.
Source 102's corrected diagnostic builds and the unchanged callback contract
still fails. Its trace identifies a real omitted C declaration: the lexical
environment holds the new callback, but the C-style value lookup still sees
`set-message-functions`. Window metrics exist and GC/redisplay evaluation are
not inhibited. Source 103 initializes xdisp.c's `set-message-function` slot to
nil and declares it special, alongside `clear-message-function`, before Lisp
startup. All diagnostic tracing is removed. A separate same-input GNU contract
covers lexical binding and disconnection; source 97 exits 255 with a void
variable where GNU exits zero. Source 103 passes all 20 selected debug tests (469.27 seconds, zero ignores)
and strict static checks, including the three unchanged callback contracts.
The 438 selected release tests also pass (987.09 seconds, zero ignores).
A fresh ordinary build and image pass, followed by all 29 exact stdout/stderr
comparisons, both startup locales and the four strict terminal report
comparisons. Source, fixture and executable/image identities remain unchanged.
The debug inventory grows from 2,870 in source 91 to 2,882 in source 103,
with twelve added selectors and none removed. Enumeration is not execution.
The source-103 ordinary executable SHA-256 is
`5bfaba6eff45e2e6e3dc3322ef98f6ad1549e855a5428eeccb905f622268d06d`;
its fresh image is
`4a1f262ea695250f2ea64820d38e111b2efeb2b6197e6a1c7031bb91ab911135`.

## Remaining validation

Validate the combined source with the function-cell and Linux text-conversion
changes. The source-103 callback repair passes the original assertions and
contains no temporary tracing. Preserve
ordinary exact stdout/stderr comparisons, native transitions, actual GC and
reclamation, image construction/restoration, and the complete test inventory.
Final Linux/macOS full, frozen and terminal runs, the older failed gates and
terminal divergences, full symbol and host-object authority, ownership audit,
allocation counters, measured VM/call work and the locked sixteen-workload 3%
criterion remain required. The full goal is incomplete.
