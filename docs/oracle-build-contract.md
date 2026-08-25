# The oracle build contract

Every number, manifest, and byte-for-byte expectation in this repository is
measured against **one specific GNU Emacs build**, not against "Emacs 30.2"
in the abstract:

- the pinned sibling checkout at `../emacs`, clean at the pinned commit
  (enforced by the harness before and after every run);
- built for **aarch64-apple-darwin** with the **NS (Cocoa) window system**
  and **native compilation** (`--with-ns --with-native-compilation=aot ...`
  -- the full option list is what the binary's own
  `system-configuration-options` reports);
- executed as `../emacs/src/emacs`.

## What depends on this

- `src/lisp/primitives/generated_gnu_c_primitives.rs` is regenerated from
  the oracle binary and gate-checked for byte identity
  (`anti_cheat::gnu_c_manifest_matches_fresh_regeneration`).  The subr set
  differs across builds: an X11 or terminal-only build lacks NS-specific
  primitives and exposes others.  **Against a non-Darwin oracle this gate
  is red by design**; it now detects that case and names this file.
- The dumped keymaps, `features`, coding-system inventory and several
  test expectations carry NS-build details (documented as they arise in
  `docs/honesty-audit-2026-08-18.md`; e.g. `utf-8-hfs`, the `<home>`
  rebinds from `term/ns-win.el`).
- The frozen compatibility manifest's 7,883 outcomes were produced by this
  build; its sha256 is pinned in the harness.  (It was 7,595 before the
  2026-08-26 regeneration and 7,080 before that; the count moves whenever the
  inventory is regenerated, so prefer the constants in the harness over any
  number quoted in prose.)

## What this means in practice

Reproducing this repository's measurements on another machine requires
building the same Emacs configuration at the pinned commit.  Running the
gates against any other build produces honest-but-different answers, and
the affected gates fail rather than silently re-baseline.  That is the
intended trade: the oracle is a fixed instrument, not a moving reference.
