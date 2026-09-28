# Combined records, eager expansion and regexp profiling — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
continuation combines the inline generic records, function cells and
text-conversion buffer field, repairs a release-only collection failure in
eager macro expansion, and removes unnecessary hashing from regexp table
validation. Source 108 passes 147 selected debug tests and 663 selected release
tests, including both focused release regressions, plus all strict static
checks, 33 ordinary exact GNU comparisons, two locale comparisons and four
focused terminal report comparisons. The additional ordinary GC reproducer
also matches GNU exactly. The [portable evidence](handover/2026-09-28-combined-records/manifest.json)
retains all failed and passing attempts. The full macOS gate is running; it is
not yet a pass.

The independently validated text-conversion repair was already pushed as
`e9cd46bd7a762e9fe524f3424b6d2efdf45ed3ca`. Its existing
[Linux CI run](https://github.com/rayfdj/emaxx/actions/runs/36400765916)
is still running. It does not contain the combined record or eager-expansion
changes. The earlier failed Linux runs remain failed as documented in the
[text-conversion checkpoint](runtime-representation-text-conversion-checkpoint.md).

## Eager expansion lifetime

With the ABI-matched GNU native libraries enabled, source 105's selected release
run terminates with SIGSEGV after 760.79 seconds, during the original
`ert_source_diagnostic_loop_evaluates_function_alias_chain` test. It selected
585 tests but has no final libtest summary. Exact isolation of the unchanged
release executable fails with `VoidFunction("")`; enabling existing expansion
tracing instead produces a void uninterned variable. The same exact test passes
in debug. These outcomes and the macOS crash report are retained separately.

An eight-line ordinary Lisp source reduces the problem to a macro returning a
fresh `progn`. Its first form collects; its later form retains an uninterned
symbol, a 257-character string and a record containing a list. GNU prints
`("fresh-eager-symbol" 257 #s(eager-payload (17 29)))`. Both unchanged source 105
and 106 ordinary executables segfault, with the same symbol-name lookup stack
as the ERT failure. No tracing, private runtime entry, or altered collection
threshold is needed for this reproducer.

GNU `lread.c:readevalloop_eager_expand_eval` keeps the pending list on its scanned
C stack. Emaxx had copied the forms into an ordinary Rust heap `Vec`, which the
conservative stack scan cannot traverse. Source 107 registers that actual
snapshot with the existing scoped root mechanism for the duration of recursive
evaluation. The scope ends normally or during unwinding; it adds no permanent
root or duplicate Lisp object representation. The existing snapshot traversal
itself is unchanged.

The original ERT release test then passes, and the ordinary reproducer matches
GNU exactly. Source 107's new Rust regression nevertheless fails because it
uses diagnostic `Value::Display`, which prints records as opaque addresses.
Its debug result is 146 passes and one failure; its focused release result is
one pass and one failure. Source 108 changes only that new assertion to call
the actual Lisp printer, preserving every expected graph field. Both release
regressions pass. Its 2,892-test debug inventory preserves all 2,891 source-105
tests and adds this control. No existing assertion, selector or ignore changed.
The broader release selection retains every source-105 filter and adds the
regexp controls and the new regression: 663 passes, zero failures or ignores,
347.13 seconds in libtest, with unchanged source and executable.

## Regexp table traversal

Whole-process samples of four unchanged locked workloads locate substantial
startup and preparation work in `char_table_chain_signature`, particularly
hashing every integer case-table mapping into a cycle-detection set. Source
106 retains every signature word in the same order, but inserts only objects
whose mutable fields it visits into that set. Shared objects, cycles, mutable
strings and direct native stores still participate in validation. Existing
case, syntax, regexp and raw-native-mutation controls pass: 87 in debug and
87 in release, plus strict static checks and all 33 ordinary comparisons.

This removes unnecessary work within the existing backend constraint. GNU's
regexp implementation uses the translation table itself; Emaxx's current
backend derives character classes and must validate their source contents.
That architectural difference remains. No mutation epoch, lossy fingerprint,
special-case fixture path, or unchecked cache hit replaces the content check.

The 1 ms samples show scalar-hash self frames falling from roughly 1,000 per
Emaxx process to 19–22. Raw startup, preparation and body times are retained.
Both profiling sets ran concurrently with validation; they are not controlled
timing experiments and cannot certify a speedup. The later pilot body results
remain unfavorable:

| Locked workload | GNU seconds | Emaxx seconds | Emaxx / GNU |
| --- | ---: | ---: | ---: |
| interpreted-lexical | 0.2168 | 0.7315 | 3.374 |
| interpreted-calls | 0.2659 | 1.1298 | 4.250 |
| bytecode-calls | 0.1421 | 0.4811 | 3.385 |
| native-to-interpreted | 0.2457 | 0.8726 | 3.552 |

Each is one diagnostic pilot observation, with no warmup. All eight processes
exit zero and pass the unchanged result and execution-mode validator. GNU's
source-105 interpreted-lexical sample contains no useful call frames despite
the sampler exiting zero; that coverage gap remains. Allocation counters are
unavailable and remain errors, never fabricated zeroes. Later Emaxx samples
show GC tracing, conservative allocation lookup and evaluator work alongside
the remaining table traversal. They do not establish instruction counts or
completion of the sixteen-workload 3% criterion.

## Validation limits and next work

The ordinary executable uses its own fresh image in its recorded installation;
native library paths in that image are relative to the installation. The
pristine GNU candidate at revision
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd` matches the unchanged committed native
ABI. It still does not match the frozen Darwin executable pin. Installed local
candidates were hashed and none supplies that pin. No pin was changed.

Source 105's full macOS gate was deliberately superseded after its known release
failure was fixed. It finished eval_01 with 385 passes and eval_02 with 295;
eval_03 was interrupted, and the later groups and Cargo stages did not run.
The driver exits one after 2,221.09 seconds and confirms unchanged source. Its
partial log, interruption reason and immutable executable identity remain
retained. Source 108 starts the same unmodified complete gate from clean code
commit `eaa7463df7894fb95ba21e1cc709e2d44c650561`.

Generic records are now inline, but allocated-symbol value/function/property
authority, remaining host pseudovectors and hash/keymap ownership, mapped dump
ownership, allocation counters, the full public ownership/soundness audit and
VM/call optimization remain open. Final Linux/macOS full, frozen and terminal
validation, complete native artifact identity, adversarial review and the
locked performance experiment are still required. Earlier selected passes,
failed full gates and sixteen older terminal divergences remain historical
evidence, not final-source certification.
