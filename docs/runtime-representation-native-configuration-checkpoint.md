# Matching the committed Darwin native ABI — 28 September 2026

The unchanged native artifact identity test passes all nine upstream fixtures on
the function-cell checkpoint `316fda406287d3ce148545191e5bc6a15fcef460` (source 96).
It compares raw artifact bytes, including the complete compiler frontend and
upstream native-compiler suite; the no-byte-compile fixture creates no artifact
in either editor. The release test finishes in 177.55 seconds with no ignored
tests. This is a diagnostic result, not final-source or frozen validation. The
[complete goal](runtime-representation-goal.md) remains open.

The older source-72 full gate failed its first native fixture: the existing local
GNU build reported Darwin 25.5.0 and ABI suffix `adba4e3f`, while the committed
Emaxx configuration records Darwin 25.6.0 and `d6eeb69b`. That failed gate remains
failed. A new pristine GNU checkout at the same pinned source revision,
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`, was built on this host's actual Darwin
25.6.0 using the repository's existing `tools/build_macos_oracle.py` helper and
recorded options, including Apple's libxml2. No GNU source, committed native ABI,
oracle pin, test selector, timeout or comparison rule was changed.

The freshly generated ABI table is byte-identical to the committed one after the
standard `rustfmt --edition 2024` step used by CI. The initial comparison of
unformatted generator output failed; that raw result and both versions remain
in the [portable evidence](handover/2026-09-28-native-configuration/manifest.json).
The formatted table SHA-256 is
`dd88305cdd5a0ebd2d0030738d3c15ccf38d7afcef6d862dffc3f13b2c8a0406`.

The new GNU executable SHA-256 is
`02045bece8f8102607394bbc1686235e1b781ccc7fa27c955bcdd67bf48288e8`;
its image is `5d27373ee6d6e04825945c02eefdfc3d3ac5e4d141ebe9f9a66d1f5a9df491b9`.
The executable still differs from the pinned Darwin executable. The original
GNU build remains intact. Only the existing function-cell worktree's sibling
`emacs` symlink was directed to the candidate for this experiment.

Source 96 contains no software patch. Its release integration build and fresh
ordinary executable/image build pass, and all input and artifact identities
remain unchanged across testing. The ordinary Emaxx executable is
`5473019047852ae7f7a5a83e7b1d12b38f41d91fd2dba154112ed10f906d715f`;
its image is `2956e6f7bb4e74073320dc1c357bc48def5159f4907c3aab99ad7fa9ff67f1d5`.
The test's existing cleanup deletes generated native artifacts after success;
the portable packet retains the raw log and reproducible inputs and commands,
not those artifact bytes. It does not retrospectively certify source 72 or the
separate generic-record draft.

Reproduction uses the recorded helper command, native table generator and
formatter, then a release build of `tests/native_comp_identity.rs` and a fresh
ordinary executable/image against that same GNU checkout. The packet records
the exact commands, environment, source manifests, dependency/configuration
output and executable/image identities. Final macOS/Linux full and frozen runs,
terminal and other integration contracts, the architecture/ownership audit,
profiles and the locked performance requirements remain required.
