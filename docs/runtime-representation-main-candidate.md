# Complete Rust gates for the main checkpoint — 29 September 2026

[PR #74](https://github.com/rayfdj/emaxx/pull/74) contains the shared allocation,
reader, GC, function-cell, hash-semantics and regexp work described by the
[earlier checkpoint](runtime-representation-regexp-checkpoint.md). The complete
Rust gates now pass on both supported platforms. The
[full runtime goal](runtime-representation-goal.md) remains open; integrating
this checkpoint does not establish architectural completion or GNU performance
parity.

## Complete validation

| Platform | Library passes | Existing ignores | Binary passes | Integration passes |
| --- | ---: | ---: | ---: | ---: |
| macOS ARM | 2,894 | 2 | 60 | 39 |
| Linux x86-64 | 2,902 | 2 | 61 | 42 |

Both runs use the original complete inventory, selectors, assertions, timeouts
and strict result checks. The two ignores are the existing terminal end-to-end
tests. Native artifact identity, package lifecycle, startup, runtime ownership
and real native thread continuation checks all pass. The original lexical-caller
and suspended-bytecode contracts retain both survival and reclamation assertions.

The macOS run tests clean commit
`d4950cf44016d32c016839429e26caebc970758f`, with the GC verifier enabled. Its
libtest executable SHA-256 is
`3dee0f8edc08fb30364ca438ef3eaaa28400ec6f1c4db44a958391721e95ad39`;
the complete driver takes 2,477.24 seconds. The
[Linux run](https://github.com/rayfdj/emaxx/actions/runs/36501085147) tests clean
commit `968bf33b43df990b3eb6b816e7a644654c646d98` with executable SHA-256
`5b7a78b11db80ab648ef5b0c7c70bb619a8461ca0b3248e1cdc2e6263a422fbe`.
All 297 runtime, test and build-input files in the source-115 manifest are
identical between those commits. Intervening changes add diagnostic tooling,
its controls, documentation and evidence; the full Rust gate is unchanged.
The source-115 release controls and strict formatting/compiler/Clippy checks
remain recorded in the earlier checkpoint; Linux also passes its formatting
and warnings-denied Clippy steps.

The [portable evidence](handover/2026-09-29-main-candidate/manifest.json)
contains the commands, complete raw gate logs, inventories, source and binary
identities, workflow output, startup-image experiment and ordinary profiles.
Large executable/image artifacts remain local with their original checksums.

## Earlier failures and limits remain explicit

The earlier complete Linux runs on sources 109 and 114 failed weak-key
reclamation. Their failed logs and unexecuted stages remain in the linked
historical bundles. The passing source-115 run does not establish why those
earlier executions retained the first key. A separate default-profile Linux
experiment runs the original first evaluation test before the original
suspended-bytecode test; both pass with the exact executable hash above. It
preserves the generated startup image and rules out that particular prelude as
a sufficient reproduction on this source. No assertion was weakened.

The original package installation terminal scenario passes all 11 checkpoints
on source 115. A new complete 223-scenario terminal comparison is running at
this checkpoint; it is not yet a complete pass. The earlier source-108 full
terminal failures remain recorded. GNU's native ABI matches the recorded
configuration, but the available GNU executable differs from the frozen Darwin
pin. No oracle lock has changed and no full pinned compatibility certificate
is claimed.

All 16 locked diagnostic workload results and execution modes match, but
source-115 body ratios remain 1.33–10.32 times GNU. Allocation counters are
unavailable. These single observations and the whole-process profiles are not
the controlled repeated performance experiment. Shared hash allocation, full
symbol/native-handle authority, allocation accounting, the remaining ownership
and adversarial review, final-source full pinned comparisons and the unchanged
performance criterion remain requirements.

## Separate follow-up work

Two later local commits are deliberately outside this main candidate:
`9d45fe3a90f6ef4ce0f41ce890a14af690a0503c` resolves native symbol calls using
the symbol already in hand; `021ea58de29a5484afb3b6135a901e9ab34132f8` visits
the existing window index when saving/restoring configurations. They pass
53 and 180 selected tests respectively in each of debug and release, strict
static checks, fresh ordinary images and all 16 workload result/mode checks.
Their combined source is undergoing separate validation. These checks do not
extend the complete source-115 gate results to the later code.
The portable bundle retains the two patches against `968bf33b`, their exact
source manifests, commands, raw selected checks and all ordinary pilot outcomes:
[native call patch](handover/2026-09-29-main-candidate/source117.patch) and
[window traversal patch](handover/2026-09-29-main-candidate/source118.patch).
