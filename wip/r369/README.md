# Round 369, unfinished: windows are reached through one handle

`impl.patch` is a diff of `src/` and `tests/` against round 368
(`git apply wip/r369/impl.patch`).  It is packaged, not applied: `src/`
and `tests/` are round 368's, and the gate builds and tests nothing
under `wip/`.

The subject is the first stage of making a window GNU's `struct window'
(D3 and A3.5 of docs/milestones.md): a window is still a host record
whose slots are a vector apart from it, found by id through the record
registry, but every creation, slot read and write, and lookup by id or
value now goes through `WindowRef' (primitives/window.rs) and
`Interpreter::{window_ref, window_value, make_window, windows_where}`,
so that the next stage changes the representation in those places
alone.  No behaviour is meant to change.

State on 23 November 2026: the patch builds, clippy `-D warnings' and
rustfmt are clean, and the library's window, frame, minibuffer, tty,
redisplay, configuration, split, pdumper and dump-roots tests pass (383
tests).  Not yet done: the grouped gate, the integration targets, the
instruction counts, the review and CI.
