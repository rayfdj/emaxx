# Canonical printer draft — 2 October 2026

Read the [complete goal](runtime-representation-goal.md), [handover](../HANDOVER.md)
and [allocation recovery](runtime-representation-string-allocation-recovery.md).
The full goal remains incomplete. Applied runtime279 is `d64040a7`; main remains
`21d20f0e`, and PR79 remains draft. **Do not apply source281 yet.**

The [portable unfinished draft](handover/2026-09-30-shared-reader-draft/source281-canonical-printer-unfinished-manifest.json)
retains both source280 and source281 patches, 514 source/test input hashes, original
failures, commands and raw comparisons. Both incremental patches replay exactly
from `d64040a7`; both complete patches replay from main, including file modes.
Local frozen checkouts are `target/runtime-goal/recovered-2026-09-30/string-print-characters/emaxx`
(source280) and `string-print-decoder/emaxx` (source281, under the same parent).
Receipts/helpers are under `target/runtime-goal/resume-2026-09-28`.
Preserve these inputs and executed helpers; develop the repair in a successor.

## Printer behavior and bounded results

GNU `print.c:print_object` reads actual character codes before selecting octal,
hexadecimal or literal output. The old printer instead iterates a Rust text view,
which replaces non-Unicode Emacs characters with U+F8FF. The retained generated
probe has 53 wrong rows on source279 and 58 on source270; their first 192 rows
are byte-identical, so those printer failures predate the allocator checkpoint.

Source281 carries canonical GNU bytes through recursive printing, string results,
buffer/marker insertion, callback codes, batch stdout and native serialization.
`PrintOutput` replaces temporary host text; it adds no retained Lisp object mirror,
cache or per-access registry. Rust `str` cannot represent every Emacs character;
the existing buffer API still needs a temporary Unicode view plus exact extended
positions. Its cost is unmeasured. Legacy formatting, echo/message capture, symbol
and unreadable-object names still use host text. Callback mutation timing is also
unreviewed. This is not a complete printer migration.

Source280 fails compilation because the existing decoder is private. No runtime
control runs on it. Source281 changes only the decoder's crate visibility from280,
preserving the algorithm and all fixtures. Formatting, all-target checking, Clippy
and diff checks pass with zero warnings. Its 3,030-name gate inventory preserves
all 3,029 source279 names and adds one stream control; that control passes.
Every pre-existing test body, fixture and expected byte is preserved.

All 37 earlier ordinary outputs still match GNU, and the new 23-source stream
fixture matches its GNU-derived expected bytes. The successor generated fixture
retains and checks nine temporary allocations before releasing them, avoiding the
old unused-result bytecompiler warning while preserving the printed observations.
Its **198 rows / 4,088,269 bytes** match both fresh GNU and the original GNU output
exactly, with zero stderr. It covers interpreted and verified bytecode execution.
The first runner incorrectly used `--eval` on a multi-form file and produced empty
output in both editors. That equality result is **invalid coverage**, explicitly
retained and superseded by whole-file `-l` execution with mandatory row/byte checks.
The first portable auditor also used a wrong retained-output filename; its failed
helper remains, and the successor corrects the path without changing comparisons.

## Native reload failure and next implementation work

`printer-native-literals-83729.el` compiles a real lambda returning a shared vector
of full-range string constants, including properties, then checks native status,
post-GC character codes and sharing. GNU returns the original codes. Source279
signals a native compiler error. Source281 compiles and executes native code but
returns wrong codes: encoded extended characters become separate raw bytes.
All three original outputs are retained; neither Emaxx result matches GNU.

The remaining boundary is `src/lisp/native_comp/loader.rs:read_static_object`:
it calls `decode_utf8_bytes` before `read_one_form_in_env`. GNU
`comp.c:load_static_obj` passes the blob through `make_string` and `Fread`, retaining
its internal character encoding. The Rust reader currently takes host text or a
callable character source. Continue by carrying actual codes through the shared
reader boundary, including string and symbol semantics, rooting and lookahead.
Do not rewrite the native expected answer, emit substitute escapes to conceal the
boundary, or use a callback adapter with different symbol-byte semantics. The
existing `ReaderStream` function path deliberately has GNU function-stream rules;
it cannot be substituted blindly for a multibyte string source.

After the repair, rerun the unchanged native negative and ordinary comparisons,
both selected profiles with required socket permission, then full final-source
platform/integration checks. Existing native artifact identity must still pass.
No native repair or performance result is claimed here.

## Closed partial validation

Source281 supervisor85186 is **withdrawn**, with no process remaining. Its focused
run has **214 passes / one failure / two existing ignores**. The failure is a
loopback socket bind denied by the sandbox (`Operation not permitted`). Its affected
run completes **690 passes / five socket-permission failures** before withdrawal;
one control is interrupted and 375 never start. Release never starts. All five
failed socket controls pass together with the identical binary and loopback
permission; the focused socket control also passes separately. These diagnostics
do not turn the original incomplete suites into passes. The real native constant
failure remains independently reproducible.

An initial supervisor-preparation assertion counted bracketed strings instead of
the actual stage list and failed before launching tests; its successor validates
the six-stage AST list. The prepared runtime and helper bodies were unchanged.
Raw partial inventories and every original failure remain in the portable archive.

## Applied checkpoint validation and requested GNU wait

Source279's [complete Rust audits](handover/2026-09-30-shared-reader-draft/source279-complete-rust-platforms-manifest.json)
verify **3,126 macOS / 3,138 Linux passes**, two existing ignores each, all
3,029/3,037 library names, native artifact identity and retained actual GNU/Rust
inputs. Its [complete Linux frozen audit](handover/2026-09-30-shared-reader-draft/source279-complete-linux-frozen-manifest.json)
verifies **519 files / 7,928 matching outcomes / 1,038 successful processes**.
Each editor has 7,670 passes, 47 expected failures and 211 skips. The original
Eglot completion case completes and passes in both editors in this new full run.
Neither this pass nor a later census pass establishes the cause of an earlier failure.
The [supplementary execution inputs](handover/2026-09-30-shared-reader-draft/source279-linux-execution-markers-manifest.json)
retain all 1,038 readiness markers and both Linux AppArmor profiles, whose file
extensions were outside the primary archive's allowlist. Actual executables/images
remain retained locally; portable archives identify them by hash and require rebuilding.

The user specifically requested a reasonable longer GNU wait. The separately
[retained macOS retry](handover/2026-09-30-shared-reader-draft/source279-varied-output-and-eglot-retry-manifest.json)
uses 60-second JSON-RPC and 180-second process bounds for both editors, retains
all upstream assertions, and captures identical actual final buffer bytes.
Original GNU timeouts remain recorded separately. Never slow Emaxx or manufacture
a matching timeout. A completed answer is required before claiming equivalence;
an unresolved timeout remains inconclusive.

MacOS full supervisor77583 continues frozen/terminal validation in the clean
`string-allocation-full/emaxx` checkout at `d64040a7`. Read its actual receipts
before reporting completion. Printer/native repair, real intervals, unified pure
storage, symbol authority, physical accounting/counters, public ownership, VM/call
profiles, final adversarial audit and the locked 16 workloads with the unchanged
3% ceiling remain required. Passing source279 suites do not certify source281.
