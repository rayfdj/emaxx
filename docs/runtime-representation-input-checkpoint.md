# Terminal input continuation — 28 September 2026

The [complete goal](runtime-representation-goal.md) remains incomplete. This
continues the [keymap checkpoint](runtime-representation-keymap-checkpoint.md)
at source 39. Its architectural limitations and earlier failures still apply.

## Mouse input repair

A source-36 isolated terminal comparison still lost the menu-bar click. The
source-37 diagnostic build traced the actual button release entering the queue
and then being discarded by the Lisp event poller. GNU simple.el's ordinary
M-x shortcut-suggestion timer was waiting in sit-for, whose timed read-event
must return the click so Lisp can put it back on unread-command-events.
The blocking reader also discarded every typed mouse event.

The command loop, blocking reader and poller now share conversion from queued
input to a Lisp event. Readers receive the current interpreter to classify
clicks using the displayed frame's window geometry. They preserve mouse/key
ordering and existing quit behavior. Temporary source-37 trace additions are
removed. GNU references are keyboard.c:make_lispy_event,
lread.c:read_filtered_event/Fread_event and the unchanged simple.el/subr.el
timer and sit-for implementations. This fixes the read boundary; the earlier
frontend limitation that text-area click positions use window point remains.

[The 99 portable receipts](handover/2026-09-28-input/manifest.json) preserve
commands, source manifests, patches, raw output and failures. Source 38's new
regression fails: the old reader skips both mouse events and returns three
following keyboard characters. Source 39 passes seven focused input/timer
tests and 81 selected release tests, zero ignored. Existing test assertions
are unchanged; 24 injected callbacks gain an unused interpreter argument.
The new test covers blocking, polling and timed event reads. Rustfmt,
all-target/all-feature compiler checks, strict Clippy and diff checks pass.

Source 39's ordinary release and fingerprinted image build successfully.
Executable SHA-256 is
`dacbfcaa1fe94b57d4e96a2523f82304562d3896cef3a195220d18469238ac7f`;
image SHA-256 is
`52a62fd6893ecf7d99c4c10d211370adf7f9cdb8637c35a005df2752a06f8a72`.
Isolated mouse-bar-click, mouse-bar-click-dismiss, mouse-cmenu and f10-cycle
sessions have zero terminal differences from GNU using the original actions,
timings and comparison functions, private fixtures and absolute Lisp paths.
These are diagnostics, not a full terminal or frozen-compatibility pass.

## Completed older run and remaining validation

Source 25's unchanged full terminal comparison finished all 223 scenarios:
**16 diverged**, exit 1, 5,483.12 seconds. Its following ordinary TTY smoke
passed, 55.78 seconds. Source, binary and image identities stayed unchanged.
The full raw log is retained. Failing scenarios were f10-cycle,
mouse-bar-click, mouse-cmenu, compile-run, compile-parse, compile-next-error,
help-key-then-quit, help-function-then-quit, bookmark-set-and-jump,
revert-buffer-confirm, package-menu-filter-install-cancel,
package-menu-install-refresh-remove, eglot-connect-diagnostics-completion-hover,
eglot-reconnect-shutdown, org-fieldnotes-table-motion and
supersession-accept-revisit. Several failures appear before commands finish;
their causes remain unproven. No timing, expected result or selector changed.
The menu repairs have selected later-source evidence, but the remaining
failures and a complete final-source terminal run remain open.

The full source-36 serial gate is still running in its frozen clean worktree,
outside the sandbox to permit the existing local network tests. Its first
four evaluation groups pass (385, 295, 320 and 254 tests); later groups are
pending. It does not validate source 39. Source 25's earlier failed full gate
and both network reruns remain documented and preserved.

The source-30–36 selected manifests covered 207 Cargo/build/src/fixture files.
A later 279-file union manifest adds the tools and integration inputs that
were not separately hashed during each selected run. Those additional files
match source 25 and clean source-36 commit f69e1f0d exactly. This post-run
verification does not retroactively enlarge the original manifests. Source
37 onward snapshots include all tracked Cargo/build/src/tests/tools inputs.

Local GNU remains source-matched but differs from the locked Darwin executable;
no pinned frozen pass or performance claim follows from these results. Linux
CI/publication remains pending the previously rejected push's explicit approval.
The lock, 16-workload suite and 3% performance criterion remain unchanged.

Frames still use native bridge handles, and conses still have 80-byte duplicate
payloads and synchronization. A separate local frame migration has begun from
the keymap checkpoint; its first probe reproduces equal words for distinct
frames whose interpreter-local IDs coincide. That unfinished work is not part
of this software checkpoint. The representation, rooting, ownership, audit,
VM/call profiling and complete platform-validation requirements remain open.
