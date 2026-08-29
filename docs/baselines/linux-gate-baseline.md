# Linux gate baseline — environmental failures, triaged

The full serial gate on the Linux container cannot reach the Mac's
2218/0: a fixed set of tests fails for reasons that live in the
environment or in platform contracts, not in the code under test.
Until this file existed, that set was tracked only in session
scratchpads under /tmp — this is the durable, auditable record.  The
gate discipline on Linux is therefore RELATIVE: a batch passes when
its failure list is name-for-name identical to this baseline (plus its
own new tests green).  Any change to this list must come with a fresh
baseline run on a clean tree and an updated triage here.

Gate environment (standardized 2026-08-29):

- `[profile.gate]` (release + debug-assertions + overflow-checks).
- Runs as the unprivileged `emaxx` user (root cannot observe
  permission-denied behavior; three tests were false-red under root).
- `LANG=C LC_ALL=C` pinned (finding 113: the harness measures under
  LANG=C, so the gate must too).
- `ulimit -n 20000` (the default 1024 caused EMFILE under the parallel
  suite; see ledger finding 130 for the silent-fallback hazard it
  exposed).
- Real locales generated (en_US.UTF-8) beside C/POSIX.

## The baseline list

Recorded 2026-08-29 from the standardized environment above (lib stage
2135 passed / 30 failed / 3 ignored; bins and integration fully green).

```
anti_cheat::gate_tests::builtin_arities_match_fresh_regeneration
anti_cheat::gate_tests::gnu_c_manifest_matches_fresh_regeneration
lisp::eval::tests::eval_02::truncate_string_to_width_uses_display_columns
lisp::eval::tests::eval_05::eshell_internal_command_feeds_external_pipeline_before_returning
lisp::primitives::tests::accept_process_output_honors_seconds_with_no_millis_argument
lisp::primitives::tests::accept_process_output_ignores_distractor_output_until_target_delivers
lisp::primitives::tests::accept_process_output_without_timeout_waits_for_requested_process
lisp::primitives::tests::make_network_process_ipv6_family_uses_an_ipv6_listener
lisp::primitives::tests::native_composite_c_family_and_text_property_identity_match_gnu
lisp::primitives::tests::native_display_connection_management_stops_at_the_headless_backend
lisp::primitives::tests::native_font_at_and_info_match_the_headless_gnu_boundary
lisp::primitives::tests::native_font_backend_boundary_and_glyph_validation_match_gnu
lisp::primitives::tests::native_fontset_registry_family_matches_gnu
lisp::primitives::tests::native_frame_focus_mouse_geometry_and_headless_errors_match_gnu
lisp::primitives::tests::native_fringe_bitmap_registry_family_matches_gnu
lisp::primitives::tests::native_gnutls_catalogs_and_error_diagnostics_use_the_host_library
lisp::primitives::tests::native_gnutls_session_encrypts_process_io_and_closes_the_same_transport
lisp::primitives::tests::native_gnutls_x509_verifies_explicit_trust_and_rejects_hostname_mismatch
lisp::primitives::tests::native_gui_creation_tip_and_chooser_boundary_matches_gnu
lisp::primitives::tests::native_image_cache_family_matches_the_headless_frame_contract
lisp::primitives::tests::native_image_variables_match_the_gnu_image_c_contract
lisp::primitives::tests::native_imagep_validates_the_shared_image_specification_shape
lisp::primitives::tests::native_menu_activity_predicate_is_false_without_a_graphical_menu
lisp::primitives::tests::native_treesit_queries_and_traversal_use_official_runtime
lisp::primitives::tests::native_treesit_runtime_capabilities_and_query_predicates_match_gnu
lisp::primitives::tests::native_x_display_queries_observe_the_headless_backend_boundary
lisp::primitives::tests::native_xdisp_headless_query_family_matches_gnu
lisp::primitives::tests::native_xfaces_frame_table_and_resource_boundary_match_gnu
lisp::primitives::tests::native_xfaces_lisp_face_registry_family_matches_gnu
lisp::primitives::tests::set_network_process_option_applies_the_option_or_refuses
```

## Triage, by root cause

- **Oracle build gap (the `native_*` cluster)**: "oracle disagreed" —
  the Linux oracle at `../emacs` is built without the feature set the
  Mac oracle has (gnutls, tree-sitter, image support), so the ORACLE's
  answers differ from the pinned contracts.  Solvable by rebuilding
  the Linux oracle with matching libraries; until then these measure
  the oracle build, not emaxx.
- **Darwin platform contracts (2 + policy)**: the anti-cheat
  regeneration pair (`gnu_c_manifest`, `builtin_arities`) compares
  against Darwin-pinned manifests; a Linux oracle legitimately exposes
  a different primitive set (no `ns-*`), and the gates fail up front
  with an explicit out-of-contract message (see
  docs/oracle-build-contract.md).  Per-platform pins are the open
  policy question.  `set_network_process_option` is the same class:
  Darwin socket-option answers differ from Linux ones.
- **Darwin image contract (1)**: `truncate_string_to_width…` — the
  pinned expectations exercise `truncate-string-ellipsis`, defined in
  `mule-util`.  GNU's loadup.el preloads mule-util ONLY under
  `(featurep 'ns)` + charprop (the Darwin NS build the oracle contract
  pins); emaxx replays loadup with host-derived features, so the Linux
  image truthfully lacks mule-util, exactly as a Linux GNU build's
  dump does.  Not a code defect: whether the gate should replay the
  Darwin image (contract features) or the host image is the same open
  policy question as the manifest pins above.
- **Container kernel (1)**: `make_network_process_ipv6…` — no
  AF_INET6 in the container.  Unfixable here.
- **Load/timing-sensitive (the eshell/process cluster)**: the
  finding-117 output-ordering defect and its timing siblings; they
  pass on the Mac and under low load.  Code-solvable — tracked as
  open task #25.  Do NOT fix by retry or test-side waiting.

## Fixed out of the list (2026-08-29 batch — no longer baselined)

The finding-113 real divergences that earlier scratchpad lists carried
are fixed, not baselined: `batch_runtime_applies_the_gnu_locale_startup_policy`
(locale-info via nl_langinfo; keyboard-coding-system defaults to
`no-conversion` per keyboard.c), `batch_runtime_preserves_the_mule_ccl_boundary`
and `write_process_output…` (stale UTF-8-locale expectations pinned to
explicit input coding), `batch_native_lisp_callables_preserve_help_arglists`
(doc.c's dynamic-docstring reads: `lisp-directory` is now set as
startup.el:1186 does, and (FILE . POS) references decode with doc.c's
`#@NNN` sanity check), `process_identity_supports_desktop_lock_checks`
(name-agnostic comm-prefix predicate instead of assuming "emacs"),
`eieio_persistence_recurses…` (the in-process upstream-test harness now
runs test files in a disposable writable `default-directory`, as GNU's
driver and the compat harness's isolated checkout both do), and the
root-privilege trio `byte_compile_file…`/`file_writable_p…`/`save_buffer…`
(red only under root, green for the unprivileged gate user).

## 2026-08-29 second era: the feature-rich Linux oracle

The Linux oracle was rebuilt (same pinned 30.2 source) as an X11/cairo
build with HarfBuzz, tree-sitter, the full image stack
(PNG/JPEG/GIF/TIFF/RSVG/WebP/XPM) and native-comp kept — the Linux
peer of the Darwin NS oracle.  Everything above this heading describes
the first (tty-only-oracle) era and is superseded.  Environment
additions: /usr/local/bin/rustfmt must exist for the unprivileged gate
user (the anti-cheat regeneration gates run rustfmt; installed
self-contained under /usr/local/lib/emaxx-rustfmt).

Baseline list (lib stage 2180 passed / 16 failed / 4 ignored; bins and
integration fully green):

Environmental (6):
```
lisp::eval::tests::eval_01::subprocess_exit_is_event_driven_and_notifies_newest_process_first_once
lisp::eval::tests::eval_02::truncate_string_to_width_uses_display_columns
lisp::eval::tests::eval_05::eshell_internal_command_feeds_external_pipeline_before_returning
lisp::primitives::tests::accept_process_output_ignores_distractor_output_until_target_delivers
lisp::primitives::tests::accept_process_output_without_timeout_waits_for_requested_process
lisp::primitives::tests::make_network_process_ipv6_family_uses_an_ipv6_listener
```
The first passes 3/3 solo — the finding-117 load/timing cluster
(task #25), joined by the two accept-process-output siblings and the
eshell pipeline.  IPv6 is the container kernel.  truncate is the
Darwin-image loadup contract (mule-util under the ns feature).

Real divergences, newly measurable and queued for fixes (10) — the
old oracle could not answer these probes at all; the rebuilt one
answers and emaxx disagrees:
```
lisp::primitives::tests::native_composite_c_family_and_text_property_identity_match_gnu
lisp::primitives::tests::native_font_backend_boundary_and_glyph_validation_match_gnu
lisp::primitives::tests::native_gnutls_catalogs_and_error_diagnostics_use_the_host_library
lisp::primitives::tests::native_gnutls_session_encrypts_process_io_and_closes_the_same_transport
lisp::primitives::tests::native_gnutls_x509_verifies_explicit_trust_and_rejects_hostname_mismatch
lisp::primitives::tests::native_gui_creation_tip_and_chooser_boundary_matches_gnu
lisp::primitives::tests::native_image_variables_match_the_gnu_image_c_contract
lisp::primitives::tests::native_treesit_runtime_capabilities_and_query_predicates_match_gnu
lisp::primitives::tests::native_xfaces_lisp_face_registry_family_matches_gnu
lisp::primitives::tests::set_network_process_option_applies_the_option_or_refuses
```
These are being fixed, not baselined away; each removal from this
list must come with the fix that earned it.  Green flips vs the first
era: both anti-cheat regeneration gates (per-platform contracts) and
ten native_* probes.
