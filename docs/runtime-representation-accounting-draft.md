# Allocation accounting continuation — 1 October 2026

Read the [complete goal](runtime-representation-goal.md), [current handover](../HANDOVER.md)
and [cons repair](runtime-representation-cons-roots-draft.md) first. Source204 is
published at `d186d40bf014ca53d0a2652b4e2f2d921c5cb009`, with complete validation
running. This document records a verified remaining gap; it contains no counter
implementation or accounting-completion claim.

The [same-input negative baseline](handover/2026-09-30-shared-reader-draft/source204-allocation-counter-baseline-manifest.json)
runs ordinary `-Q --batch` commands in source/ABI-matched GNU and source204 using
the same Lisp program. Both exit zero with empty stderr and unchanged executable,
image and source inputs. GNU reports nonzero allocation deltas for conses,
floats, vector slots, symbols, string bytes and strings at sizes 1, 17 and 257.
Adding text properties increments its interval counter. Emaxx reports zero for
every corresponding variable, no interval increment, and an unsupported error
from `memory-use-counts`. The original mismatch remains failed.

GNU's cons deltas are 43, 91 and 811, not the three requested loop sizes. The
program executes ordinary interpreted Lisp, including macro expansion and
report construction. A repair must count actual allocations, including such
work; it must not infer counters from requested workload sizes or replace them
with live-object counts. `garbage-collect` does not decrease the cumulative
counters. The zero-valued Emaxx monotonicity result alone proves nothing.

The implementation still initializes `cons-cells-consed`, `floats-consed`,
`vector-cells-consed`, `symbols-consed`, `string-chars-consed`, `intervals-consed`
and `strings-consed` to zero in `eval.rs`. Its `memory-use-counts` dispatch
explicitly signals unsupported. The existing boundary test preserves that
error while separately checking GNU's arity and seven-element integer result.
When counters are implemented, retain that GNU contract and replace the local
unsupported expectation with genuine allocator and lifecycle assertions.

GNU `alloc.c` increments these counters at `Fcons`, `make_float`,
`allocate_vectorlike`, `Fmake_symbol`, string allocation/data construction and
`make_interval`. Its seven public variables are forwarded C integer slots:
writes, dynamic binding, detachment, buffer-local forwarding where supported,
thread switches and image restoration must agree with those actual slots. A
snapshot assembled from independent Lisp variables would not implement the API.
Source204 already has process-wide allocation arenas, but its category counters
and these forwarded slots are not connected to them.

Several representation and physical-accounting obligations remain coupled:

- Allocated symbols retain host name/id keys; value/function cells remain in
  per-interpreter dense/sparse tables, and property lists remain in name-indexed
  interpreter storage. Template cloning currently shares allocated symbols.
  Moving the authoritative cells into symbols requires resolving that sharing;
  adding another synchronization cache would preserve the architectural problem.
- Plain strings occupy a `StringCell` and Rust text storage. Mutable strings use
  `SharedStringState`, with separate host vectors for properties and extended
  characters. The current GC charge uses a GNU string-header/data formula; it is
  not a measurement of those Rust allocations and capacities.
- Buffer and string properties use `TextPropertySpan`/`StringPropertySpan` vectors,
  not GNU's allocated text-property intervals. A synthetic interval number would
  conceal that difference. Construction, cloning, storage and release need an
  explicit accounting model tied to actual objects.
- Host-only symbol keys are excluded from Lisp-string accounting but still occupy
  memory. They and the remaining representation adapters must be removed or
  accounted for separately. Allocator block metadata, capacity, retained memory
  and collection work also remain required physical measurements.

The [locked performance contract](core-runtime-performance-contract.md) requires
real allocation counters as well as all 16 workloads and the unchanged 3% ceiling.
The current missing counters, unresolved calibration and Darwin oracle identity
prevent final certification. The new probe is correctness evidence, not timing,
RSS, or a current GNU performance ratio.

The portable bundle retains the fixture, commands, exact outputs, process
results, helper and hashes of the inspected Rust/GNU sources. Large executables
and images stay in `target/runtime-goal/resume-2026-09-28` and the frozen
`target/runtime-goal/recovered-2026-09-30/cons-roots/emaxx` checkout. Keep that
checkout and its artifacts unchanged while the complete source204 runs finish.
