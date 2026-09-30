# Live input decoding draft — 30 September 2026

**Source178 is the current isolated draft. It is not applied to production.**
The [portable bundle](handover/2026-09-30-reader-contract-draft/manifest.json)
contains its complete patch, all 360 pre-run input hashes, exact patch-replay
audit and closed validation receipts. Its checkout is
`target/runtime-goal/recovered-2026-09-30/reader-contract/emaxx`.
Production remains source174; see its
[complete validation checkpoint](runtime-representation-call-validation-complete.md).

The draft replaces special XTerm dispatch and eight-character snapshots with
incremental traversal of actual `input-decode-map`, `local-function-key-map`
and `key-translation-map` objects. It follows `keyboard.c:keyremap_step` and
`read_key_sequence`: reached prefixes survive between reads, live filters run
once per event, and translators receive the prompt and dynamically bound
`current-key-remap-sequence`. Rooted buffers retain maps, events and callbacks
across collection. Unchanged GNU `read-key` owns its idle-timer fallback.
Queue consumption advances the actual unread list spine.

Source177 adds GNU's cursor rewind after discarding an unbound translated
mouse-down. Source178 also returns every generated event, validates the prompt
before consuming input, and implements case fallback through the current case
table and event modifiers. It preserves GNU's distinction between a returned
uppercase event and the lowercase event recorded in `this-command-keys`.

All strict checks pass. **308 gate and 308 release controls** pass with complete
selected inventories and no ignores. **Seven ordinary exact GNU comparisons**
pass: general decoding, callback/default cases, the three-stage translation
pipeline, reader contracts, command-key case state, XTerm map installation and
case variants. These cover long/symbolic sequences, collecting filters, shared
queue mutation, invalid returns, nonlocal exits, generated suffixes, length
limits, Unicode/custom case tables and shifted function keys. Executable/image
hashes and raw outputs are retained. These are selected checks, not complete
new-source compatibility or performance certification.

The [earlier bundle](handover/2026-09-30-input-decoder-draft/manifest.json)
preserves source175's six compiler warnings and source176's **304 passes / one
failure** in the unchanged XTerm assertion. The ordinary source176 executable
repeats that raw-ESC failure. Source177 passes 306 gate and 306 release controls
plus four ordinary contracts, but its additional reader and case-state probes
still differ from GNU. Source178 repairs those two comparisons. The source177
XTerm event value is correct while its raw batch output lacks GNU's terminal
control bytes; that whole-output comparison remains failed. No output was
normalized to make it pass.

Two invalid local test premises were corrected against separate GNU evidence:
an unconfigured XTerm translator is not invoked, and `read-key` replaces the
supplied overriding map. Equivalent coverage now tests explicit configuration,
unconfigured behavior and the bound-map `read-key-sequence-vector` path. The
valid failing XTerm batch assertion was not changed.

The original source manifests omitted three embedded bytecode fixtures and
`compat/emacs_compat_runner.el`. Their later supplemental audit matches the
recorded base and current root without rewriting old receipts. Source178
includes all four in its pre-run manifest. Both unsuccessful archive-audit
helper invocations remain in the earlier bundle.

To reconstruct the current draft, extract the new bundle, create an isolated
checkout at `ebfdbf9547b76e23013f8ce70f417aed02a744c2`, and apply
`source178-complete.patch`. Verify every entry in `source178-manifest.json`
before building. The scripts record local checkout, target and GNU paths;
relocate those explicitly on another host. Live receipts are under
`target/runtime-goal/resume-2026-09-28`. Both source177 and source178 selected
validation supervisors have completed; their checkouts and artifacts remain
frozen for subsequent comparisons.

The terminal and minibuffer command loops still use the older decoder and must
share the incremental mechanism. Mouse prefixes/fallbacks, remaining reader
arguments, raw-event bookkeeping and buffer/terminal switches need further GNU
review. Complete validation on the eventual integrated source remains required.
GNU matches source and native ABI but is not the frozen Darwin executable.
Post-deinit TLS behavior, symbol/object authority, physical allocation
accounting, the final adversarial audit and locked performance parity remain
open. This draft does not complete the full goal.
