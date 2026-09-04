# The oracle build contract

Compatibility results describe a particular GNU Emacs 30.2 binary, not
"Emacs 30.2" in the abstract.  The source revision, target platform, configure
options, linked libraries, dump mode, and native-compilation mode are all part
of the oracle identity.  A changed capability can turn an executed test into a
skip, change the set of C primitives, or change which Lisp artifact Emacs
loads.

The repository has platform-specific locks:

| Oracle | Source revision | Platform | Lock |
| --- | --- | --- | --- |
| Darwin | `636f166cfc86aa90d63f592fd99f3fdd9ef95ebd` | `aarch64-apple-darwin`, NS/Cocoa | `compat/oracle.lock.json` |
| Linux | `6ee5c13660b94f2876ea5f9e5df0f626f886740a` | `gnu/linux`, X11 without a toolkit | `compat/oracle.lock.linux.json` |

The Linux revision includes the committed source-repack repairs described in
`docs/!!!AI_CONTINUATION_INSTRUCTIONS_DO_NOT_SKIP.md`; it is not permission to
use a different upstream release.

## Required capability floor

Every newly built or repinned oracle must deliberately provide this surface.
Do not rely on configure auto-detection without checking the finished binary.

| Capability | Darwin | Linux | Finished-binary evidence |
| --- | --- | --- | --- |
| Native compilation and libgccjit | AOT | enabled | `native-comp-available-p` is non-nil and `NATIVE_COMP` is listed |
| Dumping | pdumper | pdumper | `PDUMPER` is listed |
| Window system | NS/Cocoa | X11, `--with-x-toolkit=no` | `NS` or `X11` is listed |
| File notifications | kqueue | inotify | `KQUEUE` or `INOTIFY` is listed |
| Seccomp startup filter | absent | enabled | Linux lists `SECCOMP`; Darwin does not |
| Threads | enabled | enabled | `THREADS` is listed |
| GnuTLS | enabled | enabled | `gnutls-available-p` is non-nil |
| XML2 | enabled | enabled | `libxml-available-p` is non-nil |
| SQLite3, including extension loading | enabled | enabled | `sqlite-available-p` and `fboundp` of `sqlite-load-extension` are non-nil |
| Dynamic modules | enabled | enabled | `fboundp` of `module-load` is non-nil |
| Tree-sitter | enabled | enabled | `treesit-available-p` is non-nil |
| Little CMS 2 | enabled | **required at the next repin** | `featurep 'lcms2` and `lcms2-available-p` are non-nil |
| PNG, SVG/librsvg, and WebP | enabled | enabled | `PNG`, `RSVG`, and `WEBP` are listed |
| JPEG, GIF, TIFF, and XPM | absent | enabled | Linux lists `JPEG`, `GIF`, `TIFF`, and `XPM`; Darwin does not |
| Cairo and HarfBuzz | absent | enabled | Linux lists `CAIRO` and `HARFBUZZ`; Darwin does not |
| zlib | enabled | enabled | `ZLIB` is listed |

The current pinned Linux binary was explicitly configured
`--without-lcms2`.  That is known configuration drift from this desired
capability floor: GNU skips all six `test/src/lcms-tests.el` tests while Emaxx
advertises LCMS2.  Do not "fix" those six comparisons by disabling Emaxx's
working implementation.  The next coordinated Linux oracle rebuild must add
`liblcms2-dev`, enable LCMS2, regenerate the Linux C-primitive manifest, repin
the binary, and execute the six tests on both sides.

ImageMagick remains explicitly disabled in the current contract.  D-Bus and
additional text libraries such as libotf and m17n-flt must not be picked up
accidentally: either keep them absent or make a reviewed contract change and
repin.  Adding one is legitimate and may increase real coverage, but it is a
baseline change rather than an ordinary compatibility fix.

## Darwin build

Use `compat/build_emacs_homebrew.sh` from a clean checkout.  The pinned Darwin
binary reports these effective options:

```text
--with-native-compilation=aot --with-xml2 --with-gnutls --with-modules
--with-rsvg --with-webp --with-ns --disable-ns-self-contained
--with-tree-sitter --without-dbus
```

Its recorded `system-configuration-features` value is:

```text
ACL GLIB GNUTLS LCMS2 LIBXML2 MODULES NATIVE_COMP NOTIFY KQUEUE NS PDUMPER
PNG RSVG SQLITE3 THREADS TOOLKIT_SCROLL_BARS TREE_SITTER WEBP XIM ZLIB
```

Compiler and linker paths are machine-specific and are printed by the helper;
the finished binary and source revision, rather than Homebrew path strings,
are pinned by the harness.

## Linux build

Install the build dependencies, including `liblcms2-dev`, from one recorded
distribution snapshot.  The important feature packages are libgccjit,
GnuTLS, ncurses, GMP, XML2, SQLite3, zlib, tree-sitter, X11, XPM, JPEG, GIF,
TIFF, PNG, Xft/Xrender/Xt, Cairo, HarfBuzz, librsvg, WebP, LCMS2, and seccomp.
Then configure the clean pinned checkout with an explicit capability set:

```sh
./autogen.sh
./configure \
  --with-native-compilation \
  --with-x \
  --with-x-toolkit=no \
  --with-tree-sitter \
  --with-gnutls \
  --with-xml2 \
  --with-sqlite3 \
  --with-modules \
  --with-rsvg \
  --with-webp \
  --with-lcms2 \
  --with-cairo \
  --with-harfbuzz \
  --with-xpm \
  --with-jpeg \
  --with-tiff \
  --with-gif \
  --with-png \
  --without-imagemagick
make -j"$(getconf _NPROCESSORS_ONLN)"
```

This is the required recipe for the next Linux repin.  Until that coordinated
change lands, the committed generated Linux manifest still describes the old
`--without-lcms2` binary and will correctly reject a differently configured
oracle.

## Verify before pinning

Run this against the finished binary and retain its exact output with the
repin review:

```sh
../emacs/src/emacs -Q --batch --eval '
  (prin1
   (list emacs-version
         system-configuration
         system-configuration-options
         system-configuration-features
         (native-comp-available-p)
         (and (fboundp (quote gnutls-available-p))
              (gnutls-available-p))
         (and (fboundp (quote libxml-available-p))
              (libxml-available-p))
         (and (fboundp (quote sqlite-available-p))
              (sqlite-available-p))
         (fboundp (quote sqlite-load-extension))
         (fboundp (quote module-load))
         (and (fboundp (quote treesit-available-p))
              (treesit-available-p))
         (featurep (quote lcms2))
         (and (fboundp (quote lcms2-available-p))
              (lcms2-available-p)))))'
```

After the probe is correct, pin the exact binary and checkout:

```sh
cargo run --bin compat-harness -- oracle pin \
  --emacs ../emacs/src/emacs --repo ../emacs
```

The harness must reject a dirty checkout, a different source revision, a
different binary hash, or a different platform.  A repin must also regenerate
and review the host C-primitive manifest and rerun the canonical inventory;
otherwise the old and new compatibility totals are not directly comparable.

## What depends on this contract

- `src/lisp/primitives/generated_gnu_c_primitives.rs` and
  `src/lisp/primitives/generated_gnu_c_primitives_linux.rs` are generated from
  the respective oracle binaries and gate-checked for byte identity.
- Dumped keymaps, `features`, coding systems, platform primitives, and loaded
  `.elc`/`.eln` artifacts depend on the configured feature surface.
- The frozen 7,883-outcome manifest was produced by these pinned source trees.
  Changing a build capability may change statuses even when the test names do
  not change.

The oracle is a fixed measuring instrument.  A richer replacement is welcome,
but it must be introduced as a visible, reviewed baseline migration—never as
an incidental package auto-detection change and never as a way to convert a
failure into a skip.
