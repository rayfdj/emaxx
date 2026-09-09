# Native-comp merge: attribution of 13 Rust test failures

The 13 test failures discovered while validating terminal-frame work all
appear at the first native-comp merge on this macOS setup. They predate
the terminal changes; they do not predate native-comp.

| Runtime revision | Result for these 13 tests |
| --- | --- |
| `c3aac3e`, immediately before the first native-comp merge | 13 passed, 0 failed |
| `245ff40`, first native-comp merge, September 5 | 0 passed, 13 failed |
| `450cb77`, starting main for terminal work | All 13 also fail |

The first-merge failures have the same observed symptoms as starting main.
This establishes the first-parent merge boundary. It does not identify an
individual implementation commit within the native-comp branch, nor does
it establish the result on Linux.

## What actually fails

| Tests | Count | Observed failure after the merge |
| --- | ---: | --- |
| `cl_getf_*` | 2 | Increment returns 3, but the existing plist cell retains 1. |
| Backtrace display/navigation | 6 | `backtrace-print` signals `number-or-marker-p` on nil. |
| Nested terminal menu | 1 | `window-text-pixel-size` rejects seven arguments. |
| Completion popup sizing | 1 | Popup height is 11 rather than the expected 7. |
| Dired/file-directory contract | 1 | One of four embedded ERT cases passes; failures concern window-system availability and mocked free-space output. |
| Compressed-file coding contract | 1 | Only the function-representation assertion fails: `save-buffer` is a subr. All four coding-system values and buffer-state results match. |
| Help/function metadata | 1 | `last` is a subr where the test expects a byte-code function. |

The last two are representation assertions. Their failure alone does not
establish broken file encoding or incorrect GNU behavior with native
compilation enabled. They need review against GNU's matching compilation
configuration before choosing a fix. No assertions were weakened here.

## Comparison method and evidence

Both historical runtimes were extracted with `git archive` into separate
temporary directories. Runtime source was left unchanged. Both use the
same sibling GNU checkout, `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`, and
the same host environment. Tests run serially in the `gate` profile with
fresh interpreters (`EMAXX_IMAGE_TEMPLATE` unset), `LANG=C`, `LC_ALL=C`,
and `RUST_MIN_STACK=134217728`.

The named test bodies come from `450cb77`. Relative to the historical
versions, four backtrace tests only normalize their GNU resource path to
`source-directory`, and the metadata test only changes a comment. Test
programs, expected values, and assertions are otherwise identical. Each
temporary checkout records the normalized test names in `comparison.json`.

- Pre-merge log: `/private/tmp/emaxx-native-attribution/pre-tests.log`
  (13 passed, 223.32 seconds).
- First-merge log: `/private/tmp/emaxx-native-attribution/first-tests.log`
  (13 failed, 285.17 seconds).
- Exact test names: `/private/tmp/emaxx-native-attribution-tests.txt`.
- Starting-main evidence: `/private/tmp/emaxx-terminal-cl-getf-baseline.log`
  and `/private/tmp/emaxx-terminal-affected-baseline.log`.

No runtime fix, test exclusion, or change to the terminal implementation
was made during this attribution check.

## Corrections on the merge with `c5ec1b8`

The attribution above records the unchanged historical test run. The fixes
retain native execution and GNU's native function representations:

- Cons cells still expose their own two-word GNU ABI prefix. A cached list
  tail or relocation constant can be written without entering the current
  call's touched set. Its typed read barrier now reaches its stable owning
  native heap after the generated activation returns. Teardown reconciles
  remaining views before detaching them. Cached encoding stays O(1); no
  whole-heap scan was added to a native call boundary.
- `floatfns.c:Flog` treats an explicit nil base as a natural logarithm.
  Native fixed-arity calls supply that nil slot. Rejecting it caused all
  six backtrace failures.
- `xdisp.c:Fwindow_text_pixel_size` takes seven arguments. Accepting the
  seventh slot fixes the menu failure and allows popup fitting to finish.
- `fileio.c:Fnext_read_file_uses_dialog_p` requires a window-system frame.
  GNU's initial batch frame is a terminal frame, including in the pinned
  NS build. Treating `noninteractive` as a graphical-display flag caused
  Dired to enter an unavailable file dialog.
- The embedded test fixture retains the native trampoline setting that
  unchanged `loadup.el` established. It uses `comp-no-spawn` nil and
  `comp-running-batch-compilation` t to compile in the test process, including
  missing advice trampolines. Disabling trampolines made native callers
  ignore Dired's `file-system-info` replacement. A cold-cache control
  verifies actual caller and trampoline compilation without cached artifacts.
- The `save-buffer` and `last` checks accept bytecode or a native Elisp
  subroutine (`subr-native-elisp-p`). A C/Rust builtin substitute still
  fails these ownership checks. Both functions are native Elisp subrs in
  the actual GNU and Emaxx startup used here.

The template optimization also checks whether the startup image contains
native state. Such images use fresh construction. The prohibition against
cloning live compiler contexts, native descriptors, and relocation heaps
remains enforced in `NativeCompilerState::clone`.

GNU controls: its default batch dialog probe returns
`(nil t t nil t nil)` for `(next-read-file-uses-dialog-p)`, the two dialog
options, `last-nonmenu-event`, `(framep (selected-frame))`, and
`window-system`; the unchanged four-case Dired selection passes 4/4.
Evidence: `/private/tmp/emaxx-native-gnu-dired-default.log`.

Final merged-tree validation is recorded in the honesty audit.
