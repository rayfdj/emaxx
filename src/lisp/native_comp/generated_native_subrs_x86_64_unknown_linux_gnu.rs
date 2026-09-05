//! Generated from GNU Emacs C `syms_of_*`/`defsubr` order.
//! Regenerate with `tools/generate_native_subrs.rs`; do not hand edit.

use super::abi::{NativeMaxArgs, NativeSubr};

// These are inputs to comp.c:hash_native_abi for the C build whose
// active subroutine table is emitted below.  They describe the .eln
// ABI target, not Emaxx's user-visible host configuration.
pub(crate) const NATIVE_ABI_VERSION: &str = "6";
pub(crate) const NATIVE_ABI_SYSTEM_CONFIGURATION: &str = "x86_64-pc-linux-gnu";
pub(crate) const NATIVE_ABI_SYSTEM_CONFIGURATION_OPTIONS: &str = "--with-native-compilation --with-x --with-x-toolkit=no --with-tree-sitter --without-imagemagick --with-lcms2 --with-harfbuzz --without-libotf --without-m17n-flt";

pub(crate) const NATIVE_SUBRS: &[NativeSubr] = &[
    NativeSubr {
        name: "json-parse-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "json-parse-string",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "json-insert",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "json-serialize",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "pdumper-stats",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "dump-emacs-portable--sort-predicate-copied",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "dump-emacs-portable--sort-predicate",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "dump-emacs-portable",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "profiler-memory-log",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "profiler-memory-running-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "profiler-memory-stop",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "profiler-memory-start",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "profiler-cpu-log",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "profiler-cpu-running-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "profiler-cpu-stop",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "profiler-cpu-start",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "function-equal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "thread-last-error",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "condition-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "condition-mutex",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "condition-notify",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "condition-wait",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-condition-variable",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "mutex-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "mutex-unlock",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "mutex-lock",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-mutex",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "all-threads",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "thread--blocker",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "thread-join",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "thread-live-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "thread-signal",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "thread-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "current-thread",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "make-thread",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "thread-yield",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "inotify-valid-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "inotify-rm-watch",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "inotify-add-watch",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "gnutls-available-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "gnutls-symmetric-decrypt",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "gnutls-symmetric-encrypt",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "gnutls-hash-digest",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "gnutls-hash-mac",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "gnutls-digests",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "gnutls-macs",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "gnutls-ciphers",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "gnutls-format-certificate",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "gnutls-peer-status-warning-describe",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "gnutls-peer-status",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "gnutls-bye",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "gnutls-deinit",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "gnutls-boot",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "gnutls-error-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "gnutls-error-fatalp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "gnutls-errorp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "gnutls-asynchronous-parameters",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "gnutls-get-initstage",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "menu-bar-menu-at-x-y",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-popup-dialog",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-popup-menu",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "zlib-available-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "zlib-decompress-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "lcms-temp->white-point",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "lcms2-available-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "lcms-cam02-ucs",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "lcms-jab->jch",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "lcms-jch->jab",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "lcms-jch->xyz",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "lcms-xyz->jch",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "lcms-cie-de2000",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "libxml-available-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "libxml-parse-xml-region",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "libxml-parse-html-region",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "x-get-local-selection",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-register-dnd-atom",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-send-client-message",
        min_args: 6,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "x-get-atom-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-selection-exists-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-selection-owner-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-disown-selection-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-own-selection-internal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-get-selection-internal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "handle-save-session",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tool-bar-get-system-style",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "font-get-system-normal-font",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "font-get-system-font",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "fontset-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "fontset-font",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "fontset-info",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-fontset-font",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "new-fontset",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "query-fontset",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "menu-or-popup-active-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "x-export-frames",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-internal-focus-input-context",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-uses-old-gtk-dialog",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "x-get-modifier-masks",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-translate-coordinates",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "x-display-set-last-user-time",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-begin-drag",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "x-double-buffered-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-hide-tip",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "x-show-tip",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "x-backspace-delete-keys-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-synchronize",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-display-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "x-close-connection",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-open-connection",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-create-frame",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-wm-set-size-hint",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-set-mouse-absolute-pixel-position",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-mouse-absolute-pixel-position",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "x-frame-restack",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-frame-list-z-order",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-frame-edges",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-frame-geometry",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-monitor-attributes-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-save-under",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-backing-store",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-visual-class",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-color-cells",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-planes",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-screens",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-mm-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-mm-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-pixel-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-display-pixel-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-server-input-extension-version",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-server-version",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-server-vendor",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-server-max-request-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "xw-color-values",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "xw-color-defined-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-display-grayscale-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "xw-display-color-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-window-property-attributes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-window-property",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "x-delete-window-property",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-change-window-property",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "image-transforms-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "imagep",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "image-cache-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "image-metadata",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "image-mask-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "image-size",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "image-flush",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "clear-image-cache",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "init-image-library",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-fringe-bitmap-face",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "fringe-bitmaps-at-pos",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "define-fringe-bitmap",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "destroy-fringe-bitmap",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "font-info",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame-font-cache",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "font-at",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "font-match-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "font-has-char-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "font-get-glyphs",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "query-font",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "close-font",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "open-font",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "internal-char-font",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "font-variation-glyphs",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "font-shape-gstring",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "clear-font-cache",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "font-xlfd-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "find-font",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "font-family-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "list-fonts",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "font-put",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "font-face-attributes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "font-get",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "font-spec",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "fontp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "sqlite-available-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "sqlitep",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-version",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "sqlite-finalize",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-more-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-columns",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-next",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-load-extension",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "sqlite-pragma",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "sqlite-rollback",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-commit",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-transaction",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-execute-batch",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "sqlite-select",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "sqlite-execute",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "sqlite-close",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqlite-open",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "bidi-resolved-levels",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "long-line-optimizations-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "get-display-property",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "display--line-is-continued-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "bidi-find-overridden-directionality",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "move-point-visually",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-text-pixel-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "window-text-pixel-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "current-bidi-paragraph-direction",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "invisible-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "format-mode-line",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "line-pixel-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "lookup-image-map",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "tool-bar-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "tab-bar-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-buffer-redisplay",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "set-window-cursor-type",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-cursor-type",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-parameter",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "window-parameter",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-parameters",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-next-buffers",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-next-buffers",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-prev-buffers",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-prev-buffers",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-list-1",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "window-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "window-bump-use-time",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-configuration-equal-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-window-vscroll",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "window-vscroll",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-scroll-bars",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-scroll-bars",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "window-fringes",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-fringes",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "window-margins",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-margins",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "current-window-configuration",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-configuration",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "window-configuration-frame",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-configuration-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "move-to-window-line",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-text-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-text-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "recenter",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "minibuffer-selected-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "other-window-for-scrolling",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "scroll-right",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "scroll-left",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "scroll-down",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "scroll-up",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "split-window-internal",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "force-window-update",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "select-window",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "run-window-scroll-functions",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "run-window-configuration-change-hook",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-buffer",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "resize-mini-window-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "delete-window-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "delete-other-windows-internal",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "get-buffer-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "previous-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "next-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "set-window-display-table",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-display-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-dedicated-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-lines-pixel-dimensions",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "window-dedicated-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-start",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "set-window-point",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-end",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-start",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-old-point",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-point",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-at",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "coordinates-in-window-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-scroll-bar-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-scroll-bar-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-bottom-divider-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-right-divider-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-tab-line-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-header-line-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-mode-line-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-hscroll",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-hscroll",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-body-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-body-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-resize-apply-total",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-resize-apply",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-window-new-normal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-window-new-total",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "set-window-new-pixel",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "window-top-line",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-left-column",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-pixel-top",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-pixel-left",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-new-normal",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-new-total",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-new-pixel",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-normal-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-total-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-total-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-old-body-pixel-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-old-body-pixel-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-old-pixel-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-old-pixel-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-pixel-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-pixel-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-use-time",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-window-combination-limit",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "window-combination-limit",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-prev-sibling",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-next-sibling",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-left-child",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-top-child",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-parent",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-old-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-line-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "pos-visible-in-window-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "set-frame-selected-window",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "frame-old-selected-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-selected-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-first-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-root-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-live-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-valid-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "windowp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "window-minibuffer-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "minibuffer-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "old-selected-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "selected-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "composition-sort-rules",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "clear-composition-cache",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "composition-get-gstring",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "find-composition-internal",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "compose-string-internal",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "compose-region-internal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "text-property-not-all",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "text-property-any",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "remove-list-of-text-properties",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "remove-text-properties",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "add-face-text-property",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "set-text-properties",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "put-text-property",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "add-text-properties",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "previous-single-property-change",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "previous-property-change",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "next-single-property-change",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "next-property-change",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "previous-single-char-property-change",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "next-single-char-property-change",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "previous-char-property-change",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "next-char-property-change",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "get-char-property-and-overlay",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "get-char-property",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "get-text-property",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "text-properties-at",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "play-sound-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-available-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "treesit-subtree-stat",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-node-match-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "treesit-induce-sparse-tree",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "treesit-search-forward",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "treesit-search-subtree",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "treesit-query-capture",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "treesit-query-compile",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "treesit-query-expand",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-pattern-expand",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-node-eq",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-node-descendant-for-range",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "treesit-node-first-child-for-pos",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "treesit-node-prev-sibling",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-node-next-sibling",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-node-child-by-field-name",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-node-child-count",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-node-field-name-for-child",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-node-check",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-node-child",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "treesit-node-parent",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-node-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-node-end",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-node-start",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-node-type",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-remove-notifier",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-parser-add-notifier",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-parser-notifiers",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-included-ranges",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-set-included-ranges",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "treesit-parser-root-node",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-tag",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-language",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "treesit-parser-delete",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-create",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "treesit-node-parser",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-query-language",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-query-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-compiled-query-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-node-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-parser-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-language-abi-version",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-library-abi-version",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "treesit-language-available-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "module-load",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "undo-boundary",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "tty--output-buffer-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tty--set-output-buffer-size",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "resume-tty",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "suspend-tty",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tty-top-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "controlling-tty-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tty-type",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tty-no-underline",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tty-display-color-cells",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tty-display-color-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-terminal-parameter",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "terminal-parameter",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "terminal-parameters",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "terminal-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "terminal-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "terminal-live-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-terminal",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "delete-terminal",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "parse-partial-sexp",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "backward-prefix-chars",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "scan-sexps",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "scan-lists",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "forward-comment",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "skip-syntax-backward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "skip-syntax-forward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "skip-chars-backward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "skip-chars-forward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "forward-word",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal-describe-syntax-value",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "modify-syntax-entry",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "string-to-syntax",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "matching-paren",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "syntax-class-to-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-syntax",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-syntax-table",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "copy-syntax-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "standard-syntax-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "syntax-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "syntax-table-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "reconsider-frame-fonts",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-parse-geometry",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-get-resource",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "frame-scale-factor",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-frame-window-state-change",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame-window-state-change",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame--set-was-invisible",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame-pointer-visible-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-frame-position",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "frame-position",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-frame-size",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "set-frame-width",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "set-frame-height",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "tool-bar-pixel-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-bottom-divider-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-right-divider-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-internal-border-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-child-frame-border-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-fringe-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-scroll-bar-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-scroll-bar-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-text-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-text-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-total-lines",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-total-cols",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-text-lines",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-text-cols",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-native-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-native-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-char-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-char-height",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "modify-frame-parameters",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame-parameter",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame-parameters",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-focus",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "redirect-frame-focus",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame-after-make-frame",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "x-focus-frame",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "lower-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "raise-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "visible-frame-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "frame-visible-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "iconify-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-frame-invisible",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-frame-visible",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-mouse-pixel-position",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "set-mouse-position",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "mouse-pixel-position",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "mouse-position",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "delete-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "last-nonminibuffer-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "previous-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "next-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame-ancestor-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame-parent",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "old-selected-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "selected-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "handle-switch-frame",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "select-frame",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-terminal-frame",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-windows-min-size",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "window-system",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-live-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "framep",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-time-zone-rule",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "current-time-zone",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "current-time-string",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "encode-time",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "decode-time",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "float-time",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "format-time-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "time-equal-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "time-less-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "time-subtract",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "time-add",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "time-convert",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "current-cpu-time",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "current-time",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "get-internal-run-time",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "re--describe-compiled",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "newline-cache-check",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "regexp-quote",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "match-data--translate",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-match-data",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "match-data",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "match-end",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "match-beginning",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "replace-match",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "posix-search-backward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "posix-search-forward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "re-search-backward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "re-search-forward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "search-backward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "search-forward",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "posix-string-match",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "string-match",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "posix-looking-at",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "looking-at",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "signal-names",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "num-processors",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-attributes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "list-system-processes",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "process-inherit-coding-system-flag",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "get-buffer-process",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-coding-system",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-process-coding-system",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "internal-default-process-filter",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "internal-default-process-sentinel",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-type",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "waiting-for-user-input-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "signal-process",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "internal-default-signal-process",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "process-send-eof",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-running-child-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "continue-process",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "stop-process",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "quit-process",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "kill-process",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "interrupt-process",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "internal-default-interrupt-process",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-send-string",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-send-region",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "accept-process-output",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "set-process-datagram-address",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-datagram-address",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "network-interface-info",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "network-interface-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "network-lookup-address-info",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "format-network-address",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-network-process",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "set-network-process-option",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "make-serial-process",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "serial-process-configure",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "make-pipe-process",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "make-process",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "process-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "set-process-plist",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-plist",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-contact",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "process-query-on-exit-flag",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-process-query-on-exit-flag",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-process-inherit-coding-system-flag",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-process-window-size",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "process-thread",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-process-thread",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-sentinel",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-process-sentinel",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-filter",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-process-filter",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-mark",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-process-buffer",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-command",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-tty-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "process-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-id",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-exit-status",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "process-status",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "delete-process",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "get-process",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "processp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "completing-read",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(8),
    },
    NativeSubr {
        name: "assoc-string",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "test-completion",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "all-completions",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "try-completion",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "minibuffer-contents-no-properties",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "minibuffer-contents",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "minibuffer-prompt-end",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "abort-minibuffers",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "minibuffer-innermost-command-loop-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "innermost-minibuffer-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "minibufferp",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "minibuffer-prompt",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "minibuffer-depth",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "read-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "internal-complete-buffer",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "read-variable",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "read-command",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "read-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "read-from-minibuffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "set-minibuffer-window",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "active-minibuffer-window",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "set-marker-insertion-type",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "marker-insertion-type",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "copy-marker",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-marker",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "marker-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "marker-last-position",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "marker-position",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "store-kbd-macro-event",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "cancel-kbd-macro-events",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "execute-kbd-macro",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "call-last-kbd-macro",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "end-kbd-macro",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "start-kbd-macro",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "combine-after-change-execute",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "compute-motion",
        min_args: 7,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "vertical-motion",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "line-number-display-width",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "move-to-column",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "current-column",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "indent-to",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "current-indentation",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "file-locked-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "unlock-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "lock-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "unlock-file",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "lock-file",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "daemon-initialized",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "daemonp",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "invocation-directory",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "invocation-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "kill-emacs",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "transpose-regions",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "save-restriction",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "internal--labeled-widen",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal--labeled-narrow-to-region",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "narrow-to-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "widen",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "delete-and-extract-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "delete-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "translate-region-internal",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "subst-char-in-region",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "replace-buffer-contents",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "compare-buffer-substrings",
        min_args: 6,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "insert-buffer-substring",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "format-message",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "format",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "current-message",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "message-or-box",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "message-box",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "message",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "system-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "emacs-pid",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "user-full-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "group-real-gid",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "group-gid",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "user-real-uid",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "user-uid",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "user-real-login-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "group-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "user-login-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "ngettext",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "insert-byte",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "insert-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "insert-before-markers-and-inherit",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "insert-and-inherit",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "insert-before-markers",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "insert",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "char-before",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-after",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "preceding-char",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "following-char",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "eolp",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "bolp",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "eobp",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "bobp",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "byte-to-position",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "position-bytes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "gap-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "gap-position",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "point-max-marker",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "point-min-marker",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "point-min",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "point-max",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "buffer-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "save-current-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "save-excursion",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "pos-eol",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "pos-bol",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "line-end-position",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "line-beginning-position",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "constrain-to-field",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "delete-field",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "field-string-no-properties",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "field-string",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "field-end",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "field-beginning",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "region-end",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "region-beginning",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "point",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "mark-marker",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "point-marker",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "get-pos-property",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "buffer-string",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "buffer-substring-no-properties",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "buffer-substring",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "byte-to-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-to-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "string-to-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "goto-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-equal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "propertize",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "text-quoting-style",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "Snarf-documentation",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "documentation-property",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "internal-subr-documentation",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "documentation",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "documentation-stringp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal-show-cursor-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal-show-cursor",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "send-string-to-terminal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "sleep-for",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "redisplay",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "ding",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "open-termscript",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "frame-or-buffer-changed-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "display--update-for-mouse-movement",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "redraw-display",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "redraw-frame",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "system-groups",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "system-users",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "file-attributes-lessp",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "file-attributes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "file-name-all-completions",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "file-name-completion",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "directory-files-and-attributes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "directory-files",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "self-insert-command",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "delete-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "end-of-line",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "beginning-of-line",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "forward-line",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "backward-char",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "forward-char",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "get-byte",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "char-resolve-modifiers",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "unibyte-string",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "string",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "string-width",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "char-width",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "multibyte-char-to-unibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "unibyte-char-to-multibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "characterp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "max-char",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "register-code-conversion-map",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "register-ccl-program",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "ccl-execute-on-string",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "ccl-execute",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "ccl-program-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "modify-category-entry",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "category-set-mnemonics",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-category-set",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-category-table",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-category-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "copy-category-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "standard-category-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "category-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "category-table-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "get-unused-category",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "category-docstring",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "define-category",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "make-category-set",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-standard-case-table",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-case-table",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "standard-case-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "current-case-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "case-table-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "capitalize-word",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "downcase-word",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "upcase-word",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "upcase-initials-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "capitalize-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "downcase-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "upcase-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "upcase-initials",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "capitalize",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "downcase",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "upcase",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "prefix-numeric-value",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "funcall-interactively",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "call-interactively",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "interactive",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "internal-stack-stats",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "byte-code",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "restore-buffer-modified-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "overlay-put",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "overlay-get",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "overlay-lists",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "overlay-recenter",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "previous-overlay-change",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "next-overlay-change",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "overlays-in",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "overlays-at",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "overlay-properties",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "overlay-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "overlay-end",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "overlay-start",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "move-overlay",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "delete-all-overlays",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "delete-overlay",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-overlay",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "overlayp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "kill-all-local-variables",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-buffer-multibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-swap-text",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "erase-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "barf-if-buffer-read-only",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "current-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "set-buffer-major-mode",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "bury-buffer-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "kill-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-enable-undo",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "other-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "rename-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "buffer-chars-modified-tick",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal--set-buffer-modified-tick",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "buffer-modified-tick",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-buffer-modified-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "force-mode-line-update",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-modified-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-local-variables",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-local-value",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "buffer-base-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-file-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-last-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-name",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "generate-new-buffer-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-indirect-buffer",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "get-buffer-create",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "find-buffer",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "get-truename-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "get-file-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "get-buffer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-live-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "truncate",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "round",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "floor",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "ceiling",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "logb",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "float",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "abs",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sqrt",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "log",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "expt",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "exp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "ftruncate",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "fround",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "ffloor",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "fceiling",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "ldexp",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frexp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "copysign",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "isnan",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tan",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sin",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "cos",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "atan",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "asin",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "acos",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "functionp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "special-variable-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "backtrace--locals",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "backtrace-eval",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "backtrace--frames-from-thread",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "backtrace-frame--internal",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "mapbacktrace",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "backtrace-debug",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "run-hook-wrapped",
        min_args: 2,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "run-hook-with-args-until-failure",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "run-hook-with-args-until-success",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "run-hook-with-args",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "run-hooks",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "func-arity",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "funcall",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "apply",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "eval",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "autoload-do-load",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "autoload",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "commandp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "signal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "handler-bind-1",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "condition-case",
        min_args: 2,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "unwind-protect",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "throw",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "catch",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "macroexpand",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "funcall-with-delayed-message",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "while",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "let*",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "let",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "internal-make-var-non-special",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal--define-uninitialized-variable",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "defconst-1",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "defconst",
        min_args: 2,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "defvaralias",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "defvar-1",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "defvar",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "set-default-toplevel-value",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "default-toplevel-value",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-interpreted-closure",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "function",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "quote",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "setq",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "prog1",
        min_args: 1,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "progn",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "cond",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "if",
        min_args: 2,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "and",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "or",
        min_args: 0,
        max_args: NativeMaxArgs::Unevalled,
    },
    NativeSubr {
        name: "flush-standard-output",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "print--preprocess",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "redirect-debugging-output",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "write-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "terpri",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "print",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "princ",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "error-message-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "prin1-to-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "prin1",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "obarray-clear",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "obarrayp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "obarray-make",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal--obarray-buckets",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "locate-file-internal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "mapatoms",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "read-event",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "read-char-exclusive",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "read-char",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "eval-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "eval-buffer",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "load",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "get-load-suffixes",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "unintern",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "intern-soft",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "intern",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "lread--substitute-object-in-subtree",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "read-from-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "read-positioning-symbols",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "read",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "put-unicode-property-internal",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "get-unicode-property-internal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "unicode-property-table-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "map-char-table",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "optimize-char-table",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-char-table-range",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "char-table-range",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-char-table-extra-slot",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "char-table-extra-slot",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-char-table-parent",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "char-table-subtype",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-table-parent",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-char-table",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "call-process-region",
        min_args: 3,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "getenv-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "call-process",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "native-comp-available-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "native-elisp-load",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "comp--late-register-subr",
        min_args: 7,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "comp--register-subr",
        min_args: 7,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "comp--register-lambda",
        min_args: 7,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "comp-libgccjit-version",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "comp--compile-ctxt-to-file0",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "comp--release-ctxt",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "comp--init-ctxt",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "comp--install-trampoline",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "comp-native-compiler-options-effective-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "comp-native-driver-options-effective-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "comp-el-to-eln-filename",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "comp-el-to-eln-rel-filename",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "comp--subr-signature",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-text-conversion-style",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "coding-system-priority-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "coding-system-eol-type",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "coding-system-aliases",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "coding-system-plist",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "coding-system-base",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "coding-system-put",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "define-coding-system-alias",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "define-coding-system-internal",
        min_args: 13,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "set-coding-system-priority",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "find-operation-coding-system",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "keyboard-coding-system",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-keyboard-coding-system-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "terminal-coding-system",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-safe-terminal-coding-system-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-terminal-coding-system-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "encode-big5-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "decode-big5-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "encode-sjis-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "decode-sjis-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "encode-coding-string",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "decode-coding-string",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "encode-coding-region",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "decode-coding-region",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "check-coding-systems-region",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "unencodable-char-position",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "find-coding-systems-region-internal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "detect-coding-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "detect-coding-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "check-coding-system",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "read-non-nil-coding-system",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "read-coding-system",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "coding-system-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sort-charsets",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "charset-id-internal",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-charset-priority",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "charset-priority-list",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "clear-charset-maps",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "iso-charset",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "charset-after",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-charset",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "split-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "encode-char",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "decode-char",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "find-charset-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "find-charset-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "declare-equiv-charset",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "get-unused-iso-final-char",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "unify-charset",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "set-charset-plist",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "charset-plist",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "define-charset-alias",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "define-charset-internal",
        min_args: 17,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "map-charset-chars",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "charsetp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "external-debugging-output",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "malloc-trim",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "malloc-info",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "memory-use-counts",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "memory-info",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "garbage-collect-maybe",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "garbage-collect",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "purecopy",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-finalizer",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-marker",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "make-symbol",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-bool-vector",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-string",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "make-record",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "make-vector",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-list",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-closure",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "make-byte-code",
        min_args: 4,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "bool-vector",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "record",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "vector",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "list",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "cons",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "unix-sync",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "file-system-info",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-binary-mode",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "next-read-file-uses-dialog-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "recent-auto-save-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "clear-buffer-auto-save-failure",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "set-buffer-auto-saved",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "do-auto-save",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-visited-file-modtime",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "visited-file-modtime",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "verify-visited-file-modtime",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "car-less-than-car",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "write-region",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "insert-file-contents",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "file-newer-than-file-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "default-file-modes",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "set-default-file-modes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-file-selinux-context",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-file-acl",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "file-acl",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-selinux-context",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-file-times",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "set-file-modes",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "file-modes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "file-regular-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-accessible-directory-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-directory-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-symlink-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "access-file",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "file-writable-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-readable-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-executable-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-exists-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-name-absolute-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-symbolic-link",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "add-name-to-file",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "rename-file",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "file-name-case-insensitive-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "delete-file-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "delete-directory-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-directory-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "copy-file",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "substitute-in-file-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "expand-file-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "file-name-concat",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "make-temp-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-temp-file-internal",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "directory-file-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "directory-name-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-name-as-directory",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "unhandled-file-name-directory",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-name-nondirectory",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "file-name-directory",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "find-file-name-handler",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "buffer-line-statistics",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "locale-info",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "buffer-hash",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "secure-hash",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "secure-hash-algorithms",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "md5",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "base64url-encode-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "base64url-encode-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "base64-decode-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "base64-encode-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "base64-decode-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "base64-encode-region",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "widget-apply",
        min_args: 2,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "widget-get",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "widget-put",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "plist-member",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "provide",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "require",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "featurep",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "load-average",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "yes-or-no-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "mapconcat",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "mapcan",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "mapc",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "mapcar",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "nconc",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "clear-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "fillarray",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "value<",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "equal-including-properties",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "equal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "eql",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "put",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "plist-put",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "get",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "plist-get",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "sort",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "reverse",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "nreverse",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "delete",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "delq",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "rassoc",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "rassq",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "assoc",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "assq",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "memql",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "memq",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "member",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "elt",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "nth",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "nthcdr",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "ntake",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "take",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "substring-no-properties",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "substring",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "copy-alist",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "string-to-unibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "string-to-multibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "string-as-unibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "string-as-multibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "string-make-unibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "string-make-multibyte",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "copy-sequence",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "vconcat",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "concat",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "append",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "string-collate-equalp",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "string-collate-lessp",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "string-version-lessp",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "string-lessp",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "compare-strings",
        min_args: 6,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "string-equal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "string-distance",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "string-bytes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "proper-list-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "length=",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "length>",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "length<",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "safe-length",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "length",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "random",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "identity",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "line-number-at-pos",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "object-intervals",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "string-search",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "internal--hash-table-index-size",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal--hash-table-buckets",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal--hash-table-histogram",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "define-hash-table-test",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "maphash",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "remhash",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "puthash",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "gethash",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "clrhash",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "hash-table-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "hash-table-weakness",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "hash-table-test",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "hash-table-size",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "hash-table-rehash-threshold",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "hash-table-rehash-size",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "hash-table-count",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "copy-hash-table",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-hash-table",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "sxhash-equal-including-properties",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sxhash-equal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sxhash-eql",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sxhash-eq",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "get-variable-watchers",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "remove-variable-watcher",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "add-variable-watcher",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "bool-vector-count-population",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "bool-vector-count-consecutive",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "bool-vector-subsetp",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "bool-vector-not",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "bool-vector-set-difference",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "bool-vector-intersection",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "bool-vector-union",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "bool-vector-exclusive-or",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "user-ptrp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "native-comp-unit-set-file",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "native-comp-unit-file",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "subr-native-comp-unit",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "subr-type",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "subr-native-lambda-list",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "native-comp-function-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "subr-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "subr-arity",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "byteorder",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "lognot",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "1-",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "1+",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "ash",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "logcount",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "logxor",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "logior",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "logand",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "min",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "max",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "mod",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "%",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "/",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "*",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "-",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "+",
        min_args: 0,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "/=",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: ">=",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "<=",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: ">",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "<",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "=",
        min_args: 1,
        max_args: NativeMaxArgs::Many,
    },
    NativeSubr {
        name: "string-to-number",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "number-to-string",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "aset",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "aref",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "variable-binding-locus",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "local-variable-if-set-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "local-variable-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "kill-local-variable",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-local-variable",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-variable-buffer-local",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-default",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "default-value",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "default-boundp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "symbol-value",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "setplist",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "defalias",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "fset",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "fboundp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "boundp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "fmakunbound",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "makunbound",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "position-symbol",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "remove-pos-from-symbol",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "symbol-with-pos-pos",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "bare-symbol",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "symbol-name",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "symbol-plist",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "indirect-function",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "symbol-function",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "setcdr",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "setcar",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "cdr-safe",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "car-safe",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "cdr",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "car",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "condition-variable-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "mutexp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "threadp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-or-string-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "module-function-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "closurep",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "interpreted-function-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "byte-code-function-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "subrp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "markerp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "bufferp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "sequencep",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "arrayp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "bool-vector-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "vector-or-char-table-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "char-table-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "recordp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "vectorp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "multibyte-string-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "stringp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "keywordp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "symbolp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "symbol-with-pos-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "bare-symbol-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "natnump",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "floatp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "number-or-marker-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "numberp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "integer-or-marker-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "integerp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "atom",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "consp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "nlistp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "listp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "cl-type-of",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "type-of",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "null",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "eq",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "command-modes",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "interactive-form",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "indirect-variable",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "posn-at-x-y",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "posn-at-point",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "current-input-mode",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "set-input-mode",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "set-quit-char",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-input-meta-mode",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-output-flow-control",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "set-input-interrupt-mode",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "open-dribble-file",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "discard-input",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "top-level",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "command-error-default-function",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "recursion-depth",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "exit-recursive-edit",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "abort-recursive-edit",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "suspend-emacs",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "clear-this-command-keys",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set--this-command-keys",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "this-single-command-raw-keys",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "this-single-command-keys",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "this-command-keys-vector",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "this-command-keys",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "recent-keys",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "lossage-size",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "input-pending-p",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal--track-mouse",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "recursive-edit",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "read-key-sequence-vector",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "read-key-sequence",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(6),
    },
    NativeSubr {
        name: "internal-handle-focus-in",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "event-convert-list",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal-event-symbol-parse-modifiers",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "current-idle-time",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "describe-buffer-bindings",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "where-is-internal",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "text-char-description",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "single-key-description",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "describe-vector",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "help--describe-vector",
        min_args: 7,
        max_args: NativeMaxArgs::Fixed(7),
    },
    NativeSubr {
        name: "keymap--get-keyelt",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "key-description",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "accessible-keymaps",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "current-active-maps",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "current-minor-mode-maps",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "current-global-map",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "current-local-map",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(0),
    },
    NativeSubr {
        name: "use-local-map",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "use-global-map",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "lookup-key",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "define-key",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "minor-mode-key-binding",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "key-binding",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "command-remapping",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "copy-keymap",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "map-keymap",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "map-keymap-internal",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "make-sparse-keymap",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "make-keymap",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "set-keymap-parent",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "keymap-prompt",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "keymap-parent",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "keymapp",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "color-values-from-color-spec",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "x-family-fonts",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "internal-face-x-get-resource",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "x-list-fonts",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(5),
    },
    NativeSubr {
        name: "bitmap-spec-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "tty-suppress-bold-inverse-default-colors",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "clear-face-cache",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "face-attributes-as-vector",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal-set-alternative-font-registry-alist",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal-set-alternative-font-family-alist",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal-set-font-selection-order",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "color-distance",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "display-supports-face-attributes-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "frame--face-hash-table",
        min_args: 0,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "face-font",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "internal-merge-in-global-face",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "internal-copy-lisp-face",
        min_args: 4,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "internal-lisp-face-empty-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "internal-lisp-face-equal-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "internal-lisp-face-attribute-values",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(1),
    },
    NativeSubr {
        name: "internal-get-lisp-face-attribute",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "merge-face-attribute",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "face-attribute-relative-p",
        min_args: 2,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "color-supported-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(3),
    },
    NativeSubr {
        name: "color-gray-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "internal-set-lisp-face-attribute-from-resource",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "internal-set-lisp-face-attribute",
        min_args: 3,
        max_args: NativeMaxArgs::Fixed(4),
    },
    NativeSubr {
        name: "internal-lisp-face-p",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
    NativeSubr {
        name: "internal-make-lisp-face",
        min_args: 1,
        max_args: NativeMaxArgs::Fixed(2),
    },
];

pub(crate) fn native_subr_address(index: usize) -> *mut std::ffi::c_void {
    match index {
        0 => native_subr_0000 as *mut std::ffi::c_void,
        1 => native_subr_0001 as *mut std::ffi::c_void,
        2 => native_subr_0002 as *mut std::ffi::c_void,
        3 => native_subr_0003 as *mut std::ffi::c_void,
        4 => native_subr_0004 as *mut std::ffi::c_void,
        5 => native_subr_0005 as *mut std::ffi::c_void,
        6 => native_subr_0006 as *mut std::ffi::c_void,
        7 => native_subr_0007 as *mut std::ffi::c_void,
        8 => native_subr_0008 as *mut std::ffi::c_void,
        9 => native_subr_0009 as *mut std::ffi::c_void,
        10 => native_subr_0010 as *mut std::ffi::c_void,
        11 => native_subr_0011 as *mut std::ffi::c_void,
        12 => native_subr_0012 as *mut std::ffi::c_void,
        13 => native_subr_0013 as *mut std::ffi::c_void,
        14 => native_subr_0014 as *mut std::ffi::c_void,
        15 => native_subr_0015 as *mut std::ffi::c_void,
        16 => native_subr_0016 as *mut std::ffi::c_void,
        17 => native_subr_0017 as *mut std::ffi::c_void,
        18 => native_subr_0018 as *mut std::ffi::c_void,
        19 => native_subr_0019 as *mut std::ffi::c_void,
        20 => native_subr_0020 as *mut std::ffi::c_void,
        21 => native_subr_0021 as *mut std::ffi::c_void,
        22 => native_subr_0022 as *mut std::ffi::c_void,
        23 => native_subr_0023 as *mut std::ffi::c_void,
        24 => native_subr_0024 as *mut std::ffi::c_void,
        25 => native_subr_0025 as *mut std::ffi::c_void,
        26 => native_subr_0026 as *mut std::ffi::c_void,
        27 => native_subr_0027 as *mut std::ffi::c_void,
        28 => native_subr_0028 as *mut std::ffi::c_void,
        29 => native_subr_0029 as *mut std::ffi::c_void,
        30 => native_subr_0030 as *mut std::ffi::c_void,
        31 => native_subr_0031 as *mut std::ffi::c_void,
        32 => native_subr_0032 as *mut std::ffi::c_void,
        33 => native_subr_0033 as *mut std::ffi::c_void,
        34 => native_subr_0034 as *mut std::ffi::c_void,
        35 => native_subr_0035 as *mut std::ffi::c_void,
        36 => native_subr_0036 as *mut std::ffi::c_void,
        37 => native_subr_0037 as *mut std::ffi::c_void,
        38 => native_subr_0038 as *mut std::ffi::c_void,
        39 => native_subr_0039 as *mut std::ffi::c_void,
        40 => native_subr_0040 as *mut std::ffi::c_void,
        41 => native_subr_0041 as *mut std::ffi::c_void,
        42 => native_subr_0042 as *mut std::ffi::c_void,
        43 => native_subr_0043 as *mut std::ffi::c_void,
        44 => native_subr_0044 as *mut std::ffi::c_void,
        45 => native_subr_0045 as *mut std::ffi::c_void,
        46 => native_subr_0046 as *mut std::ffi::c_void,
        47 => native_subr_0047 as *mut std::ffi::c_void,
        48 => native_subr_0048 as *mut std::ffi::c_void,
        49 => native_subr_0049 as *mut std::ffi::c_void,
        50 => native_subr_0050 as *mut std::ffi::c_void,
        51 => native_subr_0051 as *mut std::ffi::c_void,
        52 => native_subr_0052 as *mut std::ffi::c_void,
        53 => native_subr_0053 as *mut std::ffi::c_void,
        54 => native_subr_0054 as *mut std::ffi::c_void,
        55 => native_subr_0055 as *mut std::ffi::c_void,
        56 => native_subr_0056 as *mut std::ffi::c_void,
        57 => native_subr_0057 as *mut std::ffi::c_void,
        58 => native_subr_0058 as *mut std::ffi::c_void,
        59 => native_subr_0059 as *mut std::ffi::c_void,
        60 => native_subr_0060 as *mut std::ffi::c_void,
        61 => native_subr_0061 as *mut std::ffi::c_void,
        62 => native_subr_0062 as *mut std::ffi::c_void,
        63 => native_subr_0063 as *mut std::ffi::c_void,
        64 => native_subr_0064 as *mut std::ffi::c_void,
        65 => native_subr_0065 as *mut std::ffi::c_void,
        66 => native_subr_0066 as *mut std::ffi::c_void,
        67 => native_subr_0067 as *mut std::ffi::c_void,
        68 => native_subr_0068 as *mut std::ffi::c_void,
        69 => native_subr_0069 as *mut std::ffi::c_void,
        70 => native_subr_0070 as *mut std::ffi::c_void,
        71 => native_subr_0071 as *mut std::ffi::c_void,
        72 => native_subr_0072 as *mut std::ffi::c_void,
        73 => native_subr_0073 as *mut std::ffi::c_void,
        74 => native_subr_0074 as *mut std::ffi::c_void,
        75 => native_subr_0075 as *mut std::ffi::c_void,
        76 => native_subr_0076 as *mut std::ffi::c_void,
        77 => native_subr_0077 as *mut std::ffi::c_void,
        78 => native_subr_0078 as *mut std::ffi::c_void,
        79 => native_subr_0079 as *mut std::ffi::c_void,
        80 => native_subr_0080 as *mut std::ffi::c_void,
        81 => native_subr_0081 as *mut std::ffi::c_void,
        82 => native_subr_0082 as *mut std::ffi::c_void,
        83 => native_subr_0083 as *mut std::ffi::c_void,
        84 => native_subr_0084 as *mut std::ffi::c_void,
        85 => native_subr_0085 as *mut std::ffi::c_void,
        86 => native_subr_0086 as *mut std::ffi::c_void,
        87 => native_subr_0087 as *mut std::ffi::c_void,
        88 => native_subr_0088 as *mut std::ffi::c_void,
        89 => native_subr_0089 as *mut std::ffi::c_void,
        90 => native_subr_0090 as *mut std::ffi::c_void,
        91 => native_subr_0091 as *mut std::ffi::c_void,
        92 => native_subr_0092 as *mut std::ffi::c_void,
        93 => native_subr_0093 as *mut std::ffi::c_void,
        94 => native_subr_0094 as *mut std::ffi::c_void,
        95 => native_subr_0095 as *mut std::ffi::c_void,
        96 => native_subr_0096 as *mut std::ffi::c_void,
        97 => native_subr_0097 as *mut std::ffi::c_void,
        98 => native_subr_0098 as *mut std::ffi::c_void,
        99 => native_subr_0099 as *mut std::ffi::c_void,
        100 => native_subr_0100 as *mut std::ffi::c_void,
        101 => native_subr_0101 as *mut std::ffi::c_void,
        102 => native_subr_0102 as *mut std::ffi::c_void,
        103 => native_subr_0103 as *mut std::ffi::c_void,
        104 => native_subr_0104 as *mut std::ffi::c_void,
        105 => native_subr_0105 as *mut std::ffi::c_void,
        106 => native_subr_0106 as *mut std::ffi::c_void,
        107 => native_subr_0107 as *mut std::ffi::c_void,
        108 => native_subr_0108 as *mut std::ffi::c_void,
        109 => native_subr_0109 as *mut std::ffi::c_void,
        110 => native_subr_0110 as *mut std::ffi::c_void,
        111 => native_subr_0111 as *mut std::ffi::c_void,
        112 => native_subr_0112 as *mut std::ffi::c_void,
        113 => native_subr_0113 as *mut std::ffi::c_void,
        114 => native_subr_0114 as *mut std::ffi::c_void,
        115 => native_subr_0115 as *mut std::ffi::c_void,
        116 => native_subr_0116 as *mut std::ffi::c_void,
        117 => native_subr_0117 as *mut std::ffi::c_void,
        118 => native_subr_0118 as *mut std::ffi::c_void,
        119 => native_subr_0119 as *mut std::ffi::c_void,
        120 => native_subr_0120 as *mut std::ffi::c_void,
        121 => native_subr_0121 as *mut std::ffi::c_void,
        122 => native_subr_0122 as *mut std::ffi::c_void,
        123 => native_subr_0123 as *mut std::ffi::c_void,
        124 => native_subr_0124 as *mut std::ffi::c_void,
        125 => native_subr_0125 as *mut std::ffi::c_void,
        126 => native_subr_0126 as *mut std::ffi::c_void,
        127 => native_subr_0127 as *mut std::ffi::c_void,
        128 => native_subr_0128 as *mut std::ffi::c_void,
        129 => native_subr_0129 as *mut std::ffi::c_void,
        130 => native_subr_0130 as *mut std::ffi::c_void,
        131 => native_subr_0131 as *mut std::ffi::c_void,
        132 => native_subr_0132 as *mut std::ffi::c_void,
        133 => native_subr_0133 as *mut std::ffi::c_void,
        134 => native_subr_0134 as *mut std::ffi::c_void,
        135 => native_subr_0135 as *mut std::ffi::c_void,
        136 => native_subr_0136 as *mut std::ffi::c_void,
        137 => native_subr_0137 as *mut std::ffi::c_void,
        138 => native_subr_0138 as *mut std::ffi::c_void,
        139 => native_subr_0139 as *mut std::ffi::c_void,
        140 => native_subr_0140 as *mut std::ffi::c_void,
        141 => native_subr_0141 as *mut std::ffi::c_void,
        142 => native_subr_0142 as *mut std::ffi::c_void,
        143 => native_subr_0143 as *mut std::ffi::c_void,
        144 => native_subr_0144 as *mut std::ffi::c_void,
        145 => native_subr_0145 as *mut std::ffi::c_void,
        146 => native_subr_0146 as *mut std::ffi::c_void,
        147 => native_subr_0147 as *mut std::ffi::c_void,
        148 => native_subr_0148 as *mut std::ffi::c_void,
        149 => native_subr_0149 as *mut std::ffi::c_void,
        150 => native_subr_0150 as *mut std::ffi::c_void,
        151 => native_subr_0151 as *mut std::ffi::c_void,
        152 => native_subr_0152 as *mut std::ffi::c_void,
        153 => native_subr_0153 as *mut std::ffi::c_void,
        154 => native_subr_0154 as *mut std::ffi::c_void,
        155 => native_subr_0155 as *mut std::ffi::c_void,
        156 => native_subr_0156 as *mut std::ffi::c_void,
        157 => native_subr_0157 as *mut std::ffi::c_void,
        158 => native_subr_0158 as *mut std::ffi::c_void,
        159 => native_subr_0159 as *mut std::ffi::c_void,
        160 => native_subr_0160 as *mut std::ffi::c_void,
        161 => native_subr_0161 as *mut std::ffi::c_void,
        162 => native_subr_0162 as *mut std::ffi::c_void,
        163 => native_subr_0163 as *mut std::ffi::c_void,
        164 => native_subr_0164 as *mut std::ffi::c_void,
        165 => native_subr_0165 as *mut std::ffi::c_void,
        166 => native_subr_0166 as *mut std::ffi::c_void,
        167 => native_subr_0167 as *mut std::ffi::c_void,
        168 => native_subr_0168 as *mut std::ffi::c_void,
        169 => native_subr_0169 as *mut std::ffi::c_void,
        170 => native_subr_0170 as *mut std::ffi::c_void,
        171 => native_subr_0171 as *mut std::ffi::c_void,
        172 => native_subr_0172 as *mut std::ffi::c_void,
        173 => native_subr_0173 as *mut std::ffi::c_void,
        174 => native_subr_0174 as *mut std::ffi::c_void,
        175 => native_subr_0175 as *mut std::ffi::c_void,
        176 => native_subr_0176 as *mut std::ffi::c_void,
        177 => native_subr_0177 as *mut std::ffi::c_void,
        178 => native_subr_0178 as *mut std::ffi::c_void,
        179 => native_subr_0179 as *mut std::ffi::c_void,
        180 => native_subr_0180 as *mut std::ffi::c_void,
        181 => native_subr_0181 as *mut std::ffi::c_void,
        182 => native_subr_0182 as *mut std::ffi::c_void,
        183 => native_subr_0183 as *mut std::ffi::c_void,
        184 => native_subr_0184 as *mut std::ffi::c_void,
        185 => native_subr_0185 as *mut std::ffi::c_void,
        186 => native_subr_0186 as *mut std::ffi::c_void,
        187 => native_subr_0187 as *mut std::ffi::c_void,
        188 => native_subr_0188 as *mut std::ffi::c_void,
        189 => native_subr_0189 as *mut std::ffi::c_void,
        190 => native_subr_0190 as *mut std::ffi::c_void,
        191 => native_subr_0191 as *mut std::ffi::c_void,
        192 => native_subr_0192 as *mut std::ffi::c_void,
        193 => native_subr_0193 as *mut std::ffi::c_void,
        194 => native_subr_0194 as *mut std::ffi::c_void,
        195 => native_subr_0195 as *mut std::ffi::c_void,
        196 => native_subr_0196 as *mut std::ffi::c_void,
        197 => native_subr_0197 as *mut std::ffi::c_void,
        198 => native_subr_0198 as *mut std::ffi::c_void,
        199 => native_subr_0199 as *mut std::ffi::c_void,
        200 => native_subr_0200 as *mut std::ffi::c_void,
        201 => native_subr_0201 as *mut std::ffi::c_void,
        202 => native_subr_0202 as *mut std::ffi::c_void,
        203 => native_subr_0203 as *mut std::ffi::c_void,
        204 => native_subr_0204 as *mut std::ffi::c_void,
        205 => native_subr_0205 as *mut std::ffi::c_void,
        206 => native_subr_0206 as *mut std::ffi::c_void,
        207 => native_subr_0207 as *mut std::ffi::c_void,
        208 => native_subr_0208 as *mut std::ffi::c_void,
        209 => native_subr_0209 as *mut std::ffi::c_void,
        210 => native_subr_0210 as *mut std::ffi::c_void,
        211 => native_subr_0211 as *mut std::ffi::c_void,
        212 => native_subr_0212 as *mut std::ffi::c_void,
        213 => native_subr_0213 as *mut std::ffi::c_void,
        214 => native_subr_0214 as *mut std::ffi::c_void,
        215 => native_subr_0215 as *mut std::ffi::c_void,
        216 => native_subr_0216 as *mut std::ffi::c_void,
        217 => native_subr_0217 as *mut std::ffi::c_void,
        218 => native_subr_0218 as *mut std::ffi::c_void,
        219 => native_subr_0219 as *mut std::ffi::c_void,
        220 => native_subr_0220 as *mut std::ffi::c_void,
        221 => native_subr_0221 as *mut std::ffi::c_void,
        222 => native_subr_0222 as *mut std::ffi::c_void,
        223 => native_subr_0223 as *mut std::ffi::c_void,
        224 => native_subr_0224 as *mut std::ffi::c_void,
        225 => native_subr_0225 as *mut std::ffi::c_void,
        226 => native_subr_0226 as *mut std::ffi::c_void,
        227 => native_subr_0227 as *mut std::ffi::c_void,
        228 => native_subr_0228 as *mut std::ffi::c_void,
        229 => native_subr_0229 as *mut std::ffi::c_void,
        230 => native_subr_0230 as *mut std::ffi::c_void,
        231 => native_subr_0231 as *mut std::ffi::c_void,
        232 => native_subr_0232 as *mut std::ffi::c_void,
        233 => native_subr_0233 as *mut std::ffi::c_void,
        234 => native_subr_0234 as *mut std::ffi::c_void,
        235 => native_subr_0235 as *mut std::ffi::c_void,
        236 => native_subr_0236 as *mut std::ffi::c_void,
        237 => native_subr_0237 as *mut std::ffi::c_void,
        238 => native_subr_0238 as *mut std::ffi::c_void,
        239 => native_subr_0239 as *mut std::ffi::c_void,
        240 => native_subr_0240 as *mut std::ffi::c_void,
        241 => native_subr_0241 as *mut std::ffi::c_void,
        242 => native_subr_0242 as *mut std::ffi::c_void,
        243 => native_subr_0243 as *mut std::ffi::c_void,
        244 => native_subr_0244 as *mut std::ffi::c_void,
        245 => native_subr_0245 as *mut std::ffi::c_void,
        246 => native_subr_0246 as *mut std::ffi::c_void,
        247 => native_subr_0247 as *mut std::ffi::c_void,
        248 => native_subr_0248 as *mut std::ffi::c_void,
        249 => native_subr_0249 as *mut std::ffi::c_void,
        250 => native_subr_0250 as *mut std::ffi::c_void,
        251 => native_subr_0251 as *mut std::ffi::c_void,
        252 => native_subr_0252 as *mut std::ffi::c_void,
        253 => native_subr_0253 as *mut std::ffi::c_void,
        254 => native_subr_0254 as *mut std::ffi::c_void,
        255 => native_subr_0255 as *mut std::ffi::c_void,
        256 => native_subr_0256 as *mut std::ffi::c_void,
        257 => native_subr_0257 as *mut std::ffi::c_void,
        258 => native_subr_0258 as *mut std::ffi::c_void,
        259 => native_subr_0259 as *mut std::ffi::c_void,
        260 => native_subr_0260 as *mut std::ffi::c_void,
        261 => native_subr_0261 as *mut std::ffi::c_void,
        262 => native_subr_0262 as *mut std::ffi::c_void,
        263 => native_subr_0263 as *mut std::ffi::c_void,
        264 => native_subr_0264 as *mut std::ffi::c_void,
        265 => native_subr_0265 as *mut std::ffi::c_void,
        266 => native_subr_0266 as *mut std::ffi::c_void,
        267 => native_subr_0267 as *mut std::ffi::c_void,
        268 => native_subr_0268 as *mut std::ffi::c_void,
        269 => native_subr_0269 as *mut std::ffi::c_void,
        270 => native_subr_0270 as *mut std::ffi::c_void,
        271 => native_subr_0271 as *mut std::ffi::c_void,
        272 => native_subr_0272 as *mut std::ffi::c_void,
        273 => native_subr_0273 as *mut std::ffi::c_void,
        274 => native_subr_0274 as *mut std::ffi::c_void,
        275 => native_subr_0275 as *mut std::ffi::c_void,
        276 => native_subr_0276 as *mut std::ffi::c_void,
        277 => native_subr_0277 as *mut std::ffi::c_void,
        278 => native_subr_0278 as *mut std::ffi::c_void,
        279 => native_subr_0279 as *mut std::ffi::c_void,
        280 => native_subr_0280 as *mut std::ffi::c_void,
        281 => native_subr_0281 as *mut std::ffi::c_void,
        282 => native_subr_0282 as *mut std::ffi::c_void,
        283 => native_subr_0283 as *mut std::ffi::c_void,
        284 => native_subr_0284 as *mut std::ffi::c_void,
        285 => native_subr_0285 as *mut std::ffi::c_void,
        286 => native_subr_0286 as *mut std::ffi::c_void,
        287 => native_subr_0287 as *mut std::ffi::c_void,
        288 => native_subr_0288 as *mut std::ffi::c_void,
        289 => native_subr_0289 as *mut std::ffi::c_void,
        290 => native_subr_0290 as *mut std::ffi::c_void,
        291 => native_subr_0291 as *mut std::ffi::c_void,
        292 => native_subr_0292 as *mut std::ffi::c_void,
        293 => native_subr_0293 as *mut std::ffi::c_void,
        294 => native_subr_0294 as *mut std::ffi::c_void,
        295 => native_subr_0295 as *mut std::ffi::c_void,
        296 => native_subr_0296 as *mut std::ffi::c_void,
        297 => native_subr_0297 as *mut std::ffi::c_void,
        298 => native_subr_0298 as *mut std::ffi::c_void,
        299 => native_subr_0299 as *mut std::ffi::c_void,
        300 => native_subr_0300 as *mut std::ffi::c_void,
        301 => native_subr_0301 as *mut std::ffi::c_void,
        302 => native_subr_0302 as *mut std::ffi::c_void,
        303 => native_subr_0303 as *mut std::ffi::c_void,
        304 => native_subr_0304 as *mut std::ffi::c_void,
        305 => native_subr_0305 as *mut std::ffi::c_void,
        306 => native_subr_0306 as *mut std::ffi::c_void,
        307 => native_subr_0307 as *mut std::ffi::c_void,
        308 => native_subr_0308 as *mut std::ffi::c_void,
        309 => native_subr_0309 as *mut std::ffi::c_void,
        310 => native_subr_0310 as *mut std::ffi::c_void,
        311 => native_subr_0311 as *mut std::ffi::c_void,
        312 => native_subr_0312 as *mut std::ffi::c_void,
        313 => native_subr_0313 as *mut std::ffi::c_void,
        314 => native_subr_0314 as *mut std::ffi::c_void,
        315 => native_subr_0315 as *mut std::ffi::c_void,
        316 => native_subr_0316 as *mut std::ffi::c_void,
        317 => native_subr_0317 as *mut std::ffi::c_void,
        318 => native_subr_0318 as *mut std::ffi::c_void,
        319 => native_subr_0319 as *mut std::ffi::c_void,
        320 => native_subr_0320 as *mut std::ffi::c_void,
        321 => native_subr_0321 as *mut std::ffi::c_void,
        322 => native_subr_0322 as *mut std::ffi::c_void,
        323 => native_subr_0323 as *mut std::ffi::c_void,
        324 => native_subr_0324 as *mut std::ffi::c_void,
        325 => native_subr_0325 as *mut std::ffi::c_void,
        326 => native_subr_0326 as *mut std::ffi::c_void,
        327 => native_subr_0327 as *mut std::ffi::c_void,
        328 => native_subr_0328 as *mut std::ffi::c_void,
        329 => native_subr_0329 as *mut std::ffi::c_void,
        330 => native_subr_0330 as *mut std::ffi::c_void,
        331 => native_subr_0331 as *mut std::ffi::c_void,
        332 => native_subr_0332 as *mut std::ffi::c_void,
        333 => native_subr_0333 as *mut std::ffi::c_void,
        334 => native_subr_0334 as *mut std::ffi::c_void,
        335 => native_subr_0335 as *mut std::ffi::c_void,
        336 => native_subr_0336 as *mut std::ffi::c_void,
        337 => native_subr_0337 as *mut std::ffi::c_void,
        338 => native_subr_0338 as *mut std::ffi::c_void,
        339 => native_subr_0339 as *mut std::ffi::c_void,
        340 => native_subr_0340 as *mut std::ffi::c_void,
        341 => native_subr_0341 as *mut std::ffi::c_void,
        342 => native_subr_0342 as *mut std::ffi::c_void,
        343 => native_subr_0343 as *mut std::ffi::c_void,
        344 => native_subr_0344 as *mut std::ffi::c_void,
        345 => native_subr_0345 as *mut std::ffi::c_void,
        346 => native_subr_0346 as *mut std::ffi::c_void,
        347 => native_subr_0347 as *mut std::ffi::c_void,
        348 => native_subr_0348 as *mut std::ffi::c_void,
        349 => native_subr_0349 as *mut std::ffi::c_void,
        350 => native_subr_0350 as *mut std::ffi::c_void,
        351 => native_subr_0351 as *mut std::ffi::c_void,
        352 => native_subr_0352 as *mut std::ffi::c_void,
        353 => native_subr_0353 as *mut std::ffi::c_void,
        354 => native_subr_0354 as *mut std::ffi::c_void,
        355 => native_subr_0355 as *mut std::ffi::c_void,
        356 => native_subr_0356 as *mut std::ffi::c_void,
        357 => native_subr_0357 as *mut std::ffi::c_void,
        358 => native_subr_0358 as *mut std::ffi::c_void,
        359 => native_subr_0359 as *mut std::ffi::c_void,
        360 => native_subr_0360 as *mut std::ffi::c_void,
        361 => native_subr_0361 as *mut std::ffi::c_void,
        362 => native_subr_0362 as *mut std::ffi::c_void,
        363 => native_subr_0363 as *mut std::ffi::c_void,
        364 => native_subr_0364 as *mut std::ffi::c_void,
        365 => native_subr_0365 as *mut std::ffi::c_void,
        366 => native_subr_0366 as *mut std::ffi::c_void,
        367 => native_subr_0367 as *mut std::ffi::c_void,
        368 => native_subr_0368 as *mut std::ffi::c_void,
        369 => native_subr_0369 as *mut std::ffi::c_void,
        370 => native_subr_0370 as *mut std::ffi::c_void,
        371 => native_subr_0371 as *mut std::ffi::c_void,
        372 => native_subr_0372 as *mut std::ffi::c_void,
        373 => native_subr_0373 as *mut std::ffi::c_void,
        374 => native_subr_0374 as *mut std::ffi::c_void,
        375 => native_subr_0375 as *mut std::ffi::c_void,
        376 => native_subr_0376 as *mut std::ffi::c_void,
        377 => native_subr_0377 as *mut std::ffi::c_void,
        378 => native_subr_0378 as *mut std::ffi::c_void,
        379 => native_subr_0379 as *mut std::ffi::c_void,
        380 => native_subr_0380 as *mut std::ffi::c_void,
        381 => native_subr_0381 as *mut std::ffi::c_void,
        382 => native_subr_0382 as *mut std::ffi::c_void,
        383 => native_subr_0383 as *mut std::ffi::c_void,
        384 => native_subr_0384 as *mut std::ffi::c_void,
        385 => native_subr_0385 as *mut std::ffi::c_void,
        386 => native_subr_0386 as *mut std::ffi::c_void,
        387 => native_subr_0387 as *mut std::ffi::c_void,
        388 => native_subr_0388 as *mut std::ffi::c_void,
        389 => native_subr_0389 as *mut std::ffi::c_void,
        390 => native_subr_0390 as *mut std::ffi::c_void,
        391 => native_subr_0391 as *mut std::ffi::c_void,
        392 => native_subr_0392 as *mut std::ffi::c_void,
        393 => native_subr_0393 as *mut std::ffi::c_void,
        394 => native_subr_0394 as *mut std::ffi::c_void,
        395 => native_subr_0395 as *mut std::ffi::c_void,
        396 => native_subr_0396 as *mut std::ffi::c_void,
        397 => native_subr_0397 as *mut std::ffi::c_void,
        398 => native_subr_0398 as *mut std::ffi::c_void,
        399 => native_subr_0399 as *mut std::ffi::c_void,
        400 => native_subr_0400 as *mut std::ffi::c_void,
        401 => native_subr_0401 as *mut std::ffi::c_void,
        402 => native_subr_0402 as *mut std::ffi::c_void,
        403 => native_subr_0403 as *mut std::ffi::c_void,
        404 => native_subr_0404 as *mut std::ffi::c_void,
        405 => native_subr_0405 as *mut std::ffi::c_void,
        406 => native_subr_0406 as *mut std::ffi::c_void,
        407 => native_subr_0407 as *mut std::ffi::c_void,
        408 => native_subr_0408 as *mut std::ffi::c_void,
        409 => native_subr_0409 as *mut std::ffi::c_void,
        410 => native_subr_0410 as *mut std::ffi::c_void,
        411 => native_subr_0411 as *mut std::ffi::c_void,
        412 => native_subr_0412 as *mut std::ffi::c_void,
        413 => native_subr_0413 as *mut std::ffi::c_void,
        414 => native_subr_0414 as *mut std::ffi::c_void,
        415 => native_subr_0415 as *mut std::ffi::c_void,
        416 => native_subr_0416 as *mut std::ffi::c_void,
        417 => native_subr_0417 as *mut std::ffi::c_void,
        418 => native_subr_0418 as *mut std::ffi::c_void,
        419 => native_subr_0419 as *mut std::ffi::c_void,
        420 => native_subr_0420 as *mut std::ffi::c_void,
        421 => native_subr_0421 as *mut std::ffi::c_void,
        422 => native_subr_0422 as *mut std::ffi::c_void,
        423 => native_subr_0423 as *mut std::ffi::c_void,
        424 => native_subr_0424 as *mut std::ffi::c_void,
        425 => native_subr_0425 as *mut std::ffi::c_void,
        426 => native_subr_0426 as *mut std::ffi::c_void,
        427 => native_subr_0427 as *mut std::ffi::c_void,
        428 => native_subr_0428 as *mut std::ffi::c_void,
        429 => native_subr_0429 as *mut std::ffi::c_void,
        430 => native_subr_0430 as *mut std::ffi::c_void,
        431 => native_subr_0431 as *mut std::ffi::c_void,
        432 => native_subr_0432 as *mut std::ffi::c_void,
        433 => native_subr_0433 as *mut std::ffi::c_void,
        434 => native_subr_0434 as *mut std::ffi::c_void,
        435 => native_subr_0435 as *mut std::ffi::c_void,
        436 => native_subr_0436 as *mut std::ffi::c_void,
        437 => native_subr_0437 as *mut std::ffi::c_void,
        438 => native_subr_0438 as *mut std::ffi::c_void,
        439 => native_subr_0439 as *mut std::ffi::c_void,
        440 => native_subr_0440 as *mut std::ffi::c_void,
        441 => native_subr_0441 as *mut std::ffi::c_void,
        442 => native_subr_0442 as *mut std::ffi::c_void,
        443 => native_subr_0443 as *mut std::ffi::c_void,
        444 => native_subr_0444 as *mut std::ffi::c_void,
        445 => native_subr_0445 as *mut std::ffi::c_void,
        446 => native_subr_0446 as *mut std::ffi::c_void,
        447 => native_subr_0447 as *mut std::ffi::c_void,
        448 => native_subr_0448 as *mut std::ffi::c_void,
        449 => native_subr_0449 as *mut std::ffi::c_void,
        450 => native_subr_0450 as *mut std::ffi::c_void,
        451 => native_subr_0451 as *mut std::ffi::c_void,
        452 => native_subr_0452 as *mut std::ffi::c_void,
        453 => native_subr_0453 as *mut std::ffi::c_void,
        454 => native_subr_0454 as *mut std::ffi::c_void,
        455 => native_subr_0455 as *mut std::ffi::c_void,
        456 => native_subr_0456 as *mut std::ffi::c_void,
        457 => native_subr_0457 as *mut std::ffi::c_void,
        458 => native_subr_0458 as *mut std::ffi::c_void,
        459 => native_subr_0459 as *mut std::ffi::c_void,
        460 => native_subr_0460 as *mut std::ffi::c_void,
        461 => native_subr_0461 as *mut std::ffi::c_void,
        462 => native_subr_0462 as *mut std::ffi::c_void,
        463 => native_subr_0463 as *mut std::ffi::c_void,
        464 => native_subr_0464 as *mut std::ffi::c_void,
        465 => native_subr_0465 as *mut std::ffi::c_void,
        466 => native_subr_0466 as *mut std::ffi::c_void,
        467 => native_subr_0467 as *mut std::ffi::c_void,
        468 => native_subr_0468 as *mut std::ffi::c_void,
        469 => native_subr_0469 as *mut std::ffi::c_void,
        470 => native_subr_0470 as *mut std::ffi::c_void,
        471 => native_subr_0471 as *mut std::ffi::c_void,
        472 => native_subr_0472 as *mut std::ffi::c_void,
        473 => native_subr_0473 as *mut std::ffi::c_void,
        474 => native_subr_0474 as *mut std::ffi::c_void,
        475 => native_subr_0475 as *mut std::ffi::c_void,
        476 => native_subr_0476 as *mut std::ffi::c_void,
        477 => native_subr_0477 as *mut std::ffi::c_void,
        478 => native_subr_0478 as *mut std::ffi::c_void,
        479 => native_subr_0479 as *mut std::ffi::c_void,
        480 => native_subr_0480 as *mut std::ffi::c_void,
        481 => native_subr_0481 as *mut std::ffi::c_void,
        482 => native_subr_0482 as *mut std::ffi::c_void,
        483 => native_subr_0483 as *mut std::ffi::c_void,
        484 => native_subr_0484 as *mut std::ffi::c_void,
        485 => native_subr_0485 as *mut std::ffi::c_void,
        486 => native_subr_0486 as *mut std::ffi::c_void,
        487 => native_subr_0487 as *mut std::ffi::c_void,
        488 => native_subr_0488 as *mut std::ffi::c_void,
        489 => native_subr_0489 as *mut std::ffi::c_void,
        490 => native_subr_0490 as *mut std::ffi::c_void,
        491 => native_subr_0491 as *mut std::ffi::c_void,
        492 => native_subr_0492 as *mut std::ffi::c_void,
        493 => native_subr_0493 as *mut std::ffi::c_void,
        494 => native_subr_0494 as *mut std::ffi::c_void,
        495 => native_subr_0495 as *mut std::ffi::c_void,
        496 => native_subr_0496 as *mut std::ffi::c_void,
        497 => native_subr_0497 as *mut std::ffi::c_void,
        498 => native_subr_0498 as *mut std::ffi::c_void,
        499 => native_subr_0499 as *mut std::ffi::c_void,
        500 => native_subr_0500 as *mut std::ffi::c_void,
        501 => native_subr_0501 as *mut std::ffi::c_void,
        502 => native_subr_0502 as *mut std::ffi::c_void,
        503 => native_subr_0503 as *mut std::ffi::c_void,
        504 => native_subr_0504 as *mut std::ffi::c_void,
        505 => native_subr_0505 as *mut std::ffi::c_void,
        506 => native_subr_0506 as *mut std::ffi::c_void,
        507 => native_subr_0507 as *mut std::ffi::c_void,
        508 => native_subr_0508 as *mut std::ffi::c_void,
        509 => native_subr_0509 as *mut std::ffi::c_void,
        510 => native_subr_0510 as *mut std::ffi::c_void,
        511 => native_subr_0511 as *mut std::ffi::c_void,
        512 => native_subr_0512 as *mut std::ffi::c_void,
        513 => native_subr_0513 as *mut std::ffi::c_void,
        514 => native_subr_0514 as *mut std::ffi::c_void,
        515 => native_subr_0515 as *mut std::ffi::c_void,
        516 => native_subr_0516 as *mut std::ffi::c_void,
        517 => native_subr_0517 as *mut std::ffi::c_void,
        518 => native_subr_0518 as *mut std::ffi::c_void,
        519 => native_subr_0519 as *mut std::ffi::c_void,
        520 => native_subr_0520 as *mut std::ffi::c_void,
        521 => native_subr_0521 as *mut std::ffi::c_void,
        522 => native_subr_0522 as *mut std::ffi::c_void,
        523 => native_subr_0523 as *mut std::ffi::c_void,
        524 => native_subr_0524 as *mut std::ffi::c_void,
        525 => native_subr_0525 as *mut std::ffi::c_void,
        526 => native_subr_0526 as *mut std::ffi::c_void,
        527 => native_subr_0527 as *mut std::ffi::c_void,
        528 => native_subr_0528 as *mut std::ffi::c_void,
        529 => native_subr_0529 as *mut std::ffi::c_void,
        530 => native_subr_0530 as *mut std::ffi::c_void,
        531 => native_subr_0531 as *mut std::ffi::c_void,
        532 => native_subr_0532 as *mut std::ffi::c_void,
        533 => native_subr_0533 as *mut std::ffi::c_void,
        534 => native_subr_0534 as *mut std::ffi::c_void,
        535 => native_subr_0535 as *mut std::ffi::c_void,
        536 => native_subr_0536 as *mut std::ffi::c_void,
        537 => native_subr_0537 as *mut std::ffi::c_void,
        538 => native_subr_0538 as *mut std::ffi::c_void,
        539 => native_subr_0539 as *mut std::ffi::c_void,
        540 => native_subr_0540 as *mut std::ffi::c_void,
        541 => native_subr_0541 as *mut std::ffi::c_void,
        542 => native_subr_0542 as *mut std::ffi::c_void,
        543 => native_subr_0543 as *mut std::ffi::c_void,
        544 => native_subr_0544 as *mut std::ffi::c_void,
        545 => native_subr_0545 as *mut std::ffi::c_void,
        546 => native_subr_0546 as *mut std::ffi::c_void,
        547 => native_subr_0547 as *mut std::ffi::c_void,
        548 => native_subr_0548 as *mut std::ffi::c_void,
        549 => native_subr_0549 as *mut std::ffi::c_void,
        550 => native_subr_0550 as *mut std::ffi::c_void,
        551 => native_subr_0551 as *mut std::ffi::c_void,
        552 => native_subr_0552 as *mut std::ffi::c_void,
        553 => native_subr_0553 as *mut std::ffi::c_void,
        554 => native_subr_0554 as *mut std::ffi::c_void,
        555 => native_subr_0555 as *mut std::ffi::c_void,
        556 => native_subr_0556 as *mut std::ffi::c_void,
        557 => native_subr_0557 as *mut std::ffi::c_void,
        558 => native_subr_0558 as *mut std::ffi::c_void,
        559 => native_subr_0559 as *mut std::ffi::c_void,
        560 => native_subr_0560 as *mut std::ffi::c_void,
        561 => native_subr_0561 as *mut std::ffi::c_void,
        562 => native_subr_0562 as *mut std::ffi::c_void,
        563 => native_subr_0563 as *mut std::ffi::c_void,
        564 => native_subr_0564 as *mut std::ffi::c_void,
        565 => native_subr_0565 as *mut std::ffi::c_void,
        566 => native_subr_0566 as *mut std::ffi::c_void,
        567 => native_subr_0567 as *mut std::ffi::c_void,
        568 => native_subr_0568 as *mut std::ffi::c_void,
        569 => native_subr_0569 as *mut std::ffi::c_void,
        570 => native_subr_0570 as *mut std::ffi::c_void,
        571 => native_subr_0571 as *mut std::ffi::c_void,
        572 => native_subr_0572 as *mut std::ffi::c_void,
        573 => native_subr_0573 as *mut std::ffi::c_void,
        574 => native_subr_0574 as *mut std::ffi::c_void,
        575 => native_subr_0575 as *mut std::ffi::c_void,
        576 => native_subr_0576 as *mut std::ffi::c_void,
        577 => native_subr_0577 as *mut std::ffi::c_void,
        578 => native_subr_0578 as *mut std::ffi::c_void,
        579 => native_subr_0579 as *mut std::ffi::c_void,
        580 => native_subr_0580 as *mut std::ffi::c_void,
        581 => native_subr_0581 as *mut std::ffi::c_void,
        582 => native_subr_0582 as *mut std::ffi::c_void,
        583 => native_subr_0583 as *mut std::ffi::c_void,
        584 => native_subr_0584 as *mut std::ffi::c_void,
        585 => native_subr_0585 as *mut std::ffi::c_void,
        586 => native_subr_0586 as *mut std::ffi::c_void,
        587 => native_subr_0587 as *mut std::ffi::c_void,
        588 => native_subr_0588 as *mut std::ffi::c_void,
        589 => native_subr_0589 as *mut std::ffi::c_void,
        590 => native_subr_0590 as *mut std::ffi::c_void,
        591 => native_subr_0591 as *mut std::ffi::c_void,
        592 => native_subr_0592 as *mut std::ffi::c_void,
        593 => native_subr_0593 as *mut std::ffi::c_void,
        594 => native_subr_0594 as *mut std::ffi::c_void,
        595 => native_subr_0595 as *mut std::ffi::c_void,
        596 => native_subr_0596 as *mut std::ffi::c_void,
        597 => native_subr_0597 as *mut std::ffi::c_void,
        598 => native_subr_0598 as *mut std::ffi::c_void,
        599 => native_subr_0599 as *mut std::ffi::c_void,
        600 => native_subr_0600 as *mut std::ffi::c_void,
        601 => native_subr_0601 as *mut std::ffi::c_void,
        602 => native_subr_0602 as *mut std::ffi::c_void,
        603 => native_subr_0603 as *mut std::ffi::c_void,
        604 => native_subr_0604 as *mut std::ffi::c_void,
        605 => native_subr_0605 as *mut std::ffi::c_void,
        606 => native_subr_0606 as *mut std::ffi::c_void,
        607 => native_subr_0607 as *mut std::ffi::c_void,
        608 => native_subr_0608 as *mut std::ffi::c_void,
        609 => native_subr_0609 as *mut std::ffi::c_void,
        610 => native_subr_0610 as *mut std::ffi::c_void,
        611 => native_subr_0611 as *mut std::ffi::c_void,
        612 => native_subr_0612 as *mut std::ffi::c_void,
        613 => native_subr_0613 as *mut std::ffi::c_void,
        614 => native_subr_0614 as *mut std::ffi::c_void,
        615 => native_subr_0615 as *mut std::ffi::c_void,
        616 => native_subr_0616 as *mut std::ffi::c_void,
        617 => native_subr_0617 as *mut std::ffi::c_void,
        618 => native_subr_0618 as *mut std::ffi::c_void,
        619 => native_subr_0619 as *mut std::ffi::c_void,
        620 => native_subr_0620 as *mut std::ffi::c_void,
        621 => native_subr_0621 as *mut std::ffi::c_void,
        622 => native_subr_0622 as *mut std::ffi::c_void,
        623 => native_subr_0623 as *mut std::ffi::c_void,
        624 => native_subr_0624 as *mut std::ffi::c_void,
        625 => native_subr_0625 as *mut std::ffi::c_void,
        626 => native_subr_0626 as *mut std::ffi::c_void,
        627 => native_subr_0627 as *mut std::ffi::c_void,
        628 => native_subr_0628 as *mut std::ffi::c_void,
        629 => native_subr_0629 as *mut std::ffi::c_void,
        630 => native_subr_0630 as *mut std::ffi::c_void,
        631 => native_subr_0631 as *mut std::ffi::c_void,
        632 => native_subr_0632 as *mut std::ffi::c_void,
        633 => native_subr_0633 as *mut std::ffi::c_void,
        634 => native_subr_0634 as *mut std::ffi::c_void,
        635 => native_subr_0635 as *mut std::ffi::c_void,
        636 => native_subr_0636 as *mut std::ffi::c_void,
        637 => native_subr_0637 as *mut std::ffi::c_void,
        638 => native_subr_0638 as *mut std::ffi::c_void,
        639 => native_subr_0639 as *mut std::ffi::c_void,
        640 => native_subr_0640 as *mut std::ffi::c_void,
        641 => native_subr_0641 as *mut std::ffi::c_void,
        642 => native_subr_0642 as *mut std::ffi::c_void,
        643 => native_subr_0643 as *mut std::ffi::c_void,
        644 => native_subr_0644 as *mut std::ffi::c_void,
        645 => native_subr_0645 as *mut std::ffi::c_void,
        646 => native_subr_0646 as *mut std::ffi::c_void,
        647 => native_subr_0647 as *mut std::ffi::c_void,
        648 => native_subr_0648 as *mut std::ffi::c_void,
        649 => native_subr_0649 as *mut std::ffi::c_void,
        650 => native_subr_0650 as *mut std::ffi::c_void,
        651 => native_subr_0651 as *mut std::ffi::c_void,
        652 => native_subr_0652 as *mut std::ffi::c_void,
        653 => native_subr_0653 as *mut std::ffi::c_void,
        654 => native_subr_0654 as *mut std::ffi::c_void,
        655 => native_subr_0655 as *mut std::ffi::c_void,
        656 => native_subr_0656 as *mut std::ffi::c_void,
        657 => native_subr_0657 as *mut std::ffi::c_void,
        658 => native_subr_0658 as *mut std::ffi::c_void,
        659 => native_subr_0659 as *mut std::ffi::c_void,
        660 => native_subr_0660 as *mut std::ffi::c_void,
        661 => native_subr_0661 as *mut std::ffi::c_void,
        662 => native_subr_0662 as *mut std::ffi::c_void,
        663 => native_subr_0663 as *mut std::ffi::c_void,
        664 => native_subr_0664 as *mut std::ffi::c_void,
        665 => native_subr_0665 as *mut std::ffi::c_void,
        666 => native_subr_0666 as *mut std::ffi::c_void,
        667 => native_subr_0667 as *mut std::ffi::c_void,
        668 => native_subr_0668 as *mut std::ffi::c_void,
        669 => native_subr_0669 as *mut std::ffi::c_void,
        670 => native_subr_0670 as *mut std::ffi::c_void,
        671 => native_subr_0671 as *mut std::ffi::c_void,
        672 => native_subr_0672 as *mut std::ffi::c_void,
        673 => native_subr_0673 as *mut std::ffi::c_void,
        674 => native_subr_0674 as *mut std::ffi::c_void,
        675 => native_subr_0675 as *mut std::ffi::c_void,
        676 => native_subr_0676 as *mut std::ffi::c_void,
        677 => native_subr_0677 as *mut std::ffi::c_void,
        678 => native_subr_0678 as *mut std::ffi::c_void,
        679 => native_subr_0679 as *mut std::ffi::c_void,
        680 => native_subr_0680 as *mut std::ffi::c_void,
        681 => native_subr_0681 as *mut std::ffi::c_void,
        682 => native_subr_0682 as *mut std::ffi::c_void,
        683 => native_subr_0683 as *mut std::ffi::c_void,
        684 => native_subr_0684 as *mut std::ffi::c_void,
        685 => native_subr_0685 as *mut std::ffi::c_void,
        686 => native_subr_0686 as *mut std::ffi::c_void,
        687 => native_subr_0687 as *mut std::ffi::c_void,
        688 => native_subr_0688 as *mut std::ffi::c_void,
        689 => native_subr_0689 as *mut std::ffi::c_void,
        690 => native_subr_0690 as *mut std::ffi::c_void,
        691 => native_subr_0691 as *mut std::ffi::c_void,
        692 => native_subr_0692 as *mut std::ffi::c_void,
        693 => native_subr_0693 as *mut std::ffi::c_void,
        694 => native_subr_0694 as *mut std::ffi::c_void,
        695 => native_subr_0695 as *mut std::ffi::c_void,
        696 => native_subr_0696 as *mut std::ffi::c_void,
        697 => native_subr_0697 as *mut std::ffi::c_void,
        698 => native_subr_0698 as *mut std::ffi::c_void,
        699 => native_subr_0699 as *mut std::ffi::c_void,
        700 => native_subr_0700 as *mut std::ffi::c_void,
        701 => native_subr_0701 as *mut std::ffi::c_void,
        702 => native_subr_0702 as *mut std::ffi::c_void,
        703 => native_subr_0703 as *mut std::ffi::c_void,
        704 => native_subr_0704 as *mut std::ffi::c_void,
        705 => native_subr_0705 as *mut std::ffi::c_void,
        706 => native_subr_0706 as *mut std::ffi::c_void,
        707 => native_subr_0707 as *mut std::ffi::c_void,
        708 => native_subr_0708 as *mut std::ffi::c_void,
        709 => native_subr_0709 as *mut std::ffi::c_void,
        710 => native_subr_0710 as *mut std::ffi::c_void,
        711 => native_subr_0711 as *mut std::ffi::c_void,
        712 => native_subr_0712 as *mut std::ffi::c_void,
        713 => native_subr_0713 as *mut std::ffi::c_void,
        714 => native_subr_0714 as *mut std::ffi::c_void,
        715 => native_subr_0715 as *mut std::ffi::c_void,
        716 => native_subr_0716 as *mut std::ffi::c_void,
        717 => native_subr_0717 as *mut std::ffi::c_void,
        718 => native_subr_0718 as *mut std::ffi::c_void,
        719 => native_subr_0719 as *mut std::ffi::c_void,
        720 => native_subr_0720 as *mut std::ffi::c_void,
        721 => native_subr_0721 as *mut std::ffi::c_void,
        722 => native_subr_0722 as *mut std::ffi::c_void,
        723 => native_subr_0723 as *mut std::ffi::c_void,
        724 => native_subr_0724 as *mut std::ffi::c_void,
        725 => native_subr_0725 as *mut std::ffi::c_void,
        726 => native_subr_0726 as *mut std::ffi::c_void,
        727 => native_subr_0727 as *mut std::ffi::c_void,
        728 => native_subr_0728 as *mut std::ffi::c_void,
        729 => native_subr_0729 as *mut std::ffi::c_void,
        730 => native_subr_0730 as *mut std::ffi::c_void,
        731 => native_subr_0731 as *mut std::ffi::c_void,
        732 => native_subr_0732 as *mut std::ffi::c_void,
        733 => native_subr_0733 as *mut std::ffi::c_void,
        734 => native_subr_0734 as *mut std::ffi::c_void,
        735 => native_subr_0735 as *mut std::ffi::c_void,
        736 => native_subr_0736 as *mut std::ffi::c_void,
        737 => native_subr_0737 as *mut std::ffi::c_void,
        738 => native_subr_0738 as *mut std::ffi::c_void,
        739 => native_subr_0739 as *mut std::ffi::c_void,
        740 => native_subr_0740 as *mut std::ffi::c_void,
        741 => native_subr_0741 as *mut std::ffi::c_void,
        742 => native_subr_0742 as *mut std::ffi::c_void,
        743 => native_subr_0743 as *mut std::ffi::c_void,
        744 => native_subr_0744 as *mut std::ffi::c_void,
        745 => native_subr_0745 as *mut std::ffi::c_void,
        746 => native_subr_0746 as *mut std::ffi::c_void,
        747 => native_subr_0747 as *mut std::ffi::c_void,
        748 => native_subr_0748 as *mut std::ffi::c_void,
        749 => native_subr_0749 as *mut std::ffi::c_void,
        750 => native_subr_0750 as *mut std::ffi::c_void,
        751 => native_subr_0751 as *mut std::ffi::c_void,
        752 => native_subr_0752 as *mut std::ffi::c_void,
        753 => native_subr_0753 as *mut std::ffi::c_void,
        754 => native_subr_0754 as *mut std::ffi::c_void,
        755 => native_subr_0755 as *mut std::ffi::c_void,
        756 => native_subr_0756 as *mut std::ffi::c_void,
        757 => native_subr_0757 as *mut std::ffi::c_void,
        758 => native_subr_0758 as *mut std::ffi::c_void,
        759 => native_subr_0759 as *mut std::ffi::c_void,
        760 => native_subr_0760 as *mut std::ffi::c_void,
        761 => native_subr_0761 as *mut std::ffi::c_void,
        762 => native_subr_0762 as *mut std::ffi::c_void,
        763 => native_subr_0763 as *mut std::ffi::c_void,
        764 => native_subr_0764 as *mut std::ffi::c_void,
        765 => native_subr_0765 as *mut std::ffi::c_void,
        766 => native_subr_0766 as *mut std::ffi::c_void,
        767 => native_subr_0767 as *mut std::ffi::c_void,
        768 => native_subr_0768 as *mut std::ffi::c_void,
        769 => native_subr_0769 as *mut std::ffi::c_void,
        770 => native_subr_0770 as *mut std::ffi::c_void,
        771 => native_subr_0771 as *mut std::ffi::c_void,
        772 => native_subr_0772 as *mut std::ffi::c_void,
        773 => native_subr_0773 as *mut std::ffi::c_void,
        774 => native_subr_0774 as *mut std::ffi::c_void,
        775 => native_subr_0775 as *mut std::ffi::c_void,
        776 => native_subr_0776 as *mut std::ffi::c_void,
        777 => native_subr_0777 as *mut std::ffi::c_void,
        778 => native_subr_0778 as *mut std::ffi::c_void,
        779 => native_subr_0779 as *mut std::ffi::c_void,
        780 => native_subr_0780 as *mut std::ffi::c_void,
        781 => native_subr_0781 as *mut std::ffi::c_void,
        782 => native_subr_0782 as *mut std::ffi::c_void,
        783 => native_subr_0783 as *mut std::ffi::c_void,
        784 => native_subr_0784 as *mut std::ffi::c_void,
        785 => native_subr_0785 as *mut std::ffi::c_void,
        786 => native_subr_0786 as *mut std::ffi::c_void,
        787 => native_subr_0787 as *mut std::ffi::c_void,
        788 => native_subr_0788 as *mut std::ffi::c_void,
        789 => native_subr_0789 as *mut std::ffi::c_void,
        790 => native_subr_0790 as *mut std::ffi::c_void,
        791 => native_subr_0791 as *mut std::ffi::c_void,
        792 => native_subr_0792 as *mut std::ffi::c_void,
        793 => native_subr_0793 as *mut std::ffi::c_void,
        794 => native_subr_0794 as *mut std::ffi::c_void,
        795 => native_subr_0795 as *mut std::ffi::c_void,
        796 => native_subr_0796 as *mut std::ffi::c_void,
        797 => native_subr_0797 as *mut std::ffi::c_void,
        798 => native_subr_0798 as *mut std::ffi::c_void,
        799 => native_subr_0799 as *mut std::ffi::c_void,
        800 => native_subr_0800 as *mut std::ffi::c_void,
        801 => native_subr_0801 as *mut std::ffi::c_void,
        802 => native_subr_0802 as *mut std::ffi::c_void,
        803 => native_subr_0803 as *mut std::ffi::c_void,
        804 => native_subr_0804 as *mut std::ffi::c_void,
        805 => native_subr_0805 as *mut std::ffi::c_void,
        806 => native_subr_0806 as *mut std::ffi::c_void,
        807 => native_subr_0807 as *mut std::ffi::c_void,
        808 => native_subr_0808 as *mut std::ffi::c_void,
        809 => native_subr_0809 as *mut std::ffi::c_void,
        810 => native_subr_0810 as *mut std::ffi::c_void,
        811 => native_subr_0811 as *mut std::ffi::c_void,
        812 => native_subr_0812 as *mut std::ffi::c_void,
        813 => native_subr_0813 as *mut std::ffi::c_void,
        814 => native_subr_0814 as *mut std::ffi::c_void,
        815 => native_subr_0815 as *mut std::ffi::c_void,
        816 => native_subr_0816 as *mut std::ffi::c_void,
        817 => native_subr_0817 as *mut std::ffi::c_void,
        818 => native_subr_0818 as *mut std::ffi::c_void,
        819 => native_subr_0819 as *mut std::ffi::c_void,
        820 => native_subr_0820 as *mut std::ffi::c_void,
        821 => native_subr_0821 as *mut std::ffi::c_void,
        822 => native_subr_0822 as *mut std::ffi::c_void,
        823 => native_subr_0823 as *mut std::ffi::c_void,
        824 => native_subr_0824 as *mut std::ffi::c_void,
        825 => native_subr_0825 as *mut std::ffi::c_void,
        826 => native_subr_0826 as *mut std::ffi::c_void,
        827 => native_subr_0827 as *mut std::ffi::c_void,
        828 => native_subr_0828 as *mut std::ffi::c_void,
        829 => native_subr_0829 as *mut std::ffi::c_void,
        830 => native_subr_0830 as *mut std::ffi::c_void,
        831 => native_subr_0831 as *mut std::ffi::c_void,
        832 => native_subr_0832 as *mut std::ffi::c_void,
        833 => native_subr_0833 as *mut std::ffi::c_void,
        834 => native_subr_0834 as *mut std::ffi::c_void,
        835 => native_subr_0835 as *mut std::ffi::c_void,
        836 => native_subr_0836 as *mut std::ffi::c_void,
        837 => native_subr_0837 as *mut std::ffi::c_void,
        838 => native_subr_0838 as *mut std::ffi::c_void,
        839 => native_subr_0839 as *mut std::ffi::c_void,
        840 => native_subr_0840 as *mut std::ffi::c_void,
        841 => native_subr_0841 as *mut std::ffi::c_void,
        842 => native_subr_0842 as *mut std::ffi::c_void,
        843 => native_subr_0843 as *mut std::ffi::c_void,
        844 => native_subr_0844 as *mut std::ffi::c_void,
        845 => native_subr_0845 as *mut std::ffi::c_void,
        846 => native_subr_0846 as *mut std::ffi::c_void,
        847 => native_subr_0847 as *mut std::ffi::c_void,
        848 => native_subr_0848 as *mut std::ffi::c_void,
        849 => native_subr_0849 as *mut std::ffi::c_void,
        850 => native_subr_0850 as *mut std::ffi::c_void,
        851 => native_subr_0851 as *mut std::ffi::c_void,
        852 => native_subr_0852 as *mut std::ffi::c_void,
        853 => native_subr_0853 as *mut std::ffi::c_void,
        854 => native_subr_0854 as *mut std::ffi::c_void,
        855 => native_subr_0855 as *mut std::ffi::c_void,
        856 => native_subr_0856 as *mut std::ffi::c_void,
        857 => native_subr_0857 as *mut std::ffi::c_void,
        858 => native_subr_0858 as *mut std::ffi::c_void,
        859 => native_subr_0859 as *mut std::ffi::c_void,
        860 => native_subr_0860 as *mut std::ffi::c_void,
        861 => native_subr_0861 as *mut std::ffi::c_void,
        862 => native_subr_0862 as *mut std::ffi::c_void,
        863 => native_subr_0863 as *mut std::ffi::c_void,
        864 => native_subr_0864 as *mut std::ffi::c_void,
        865 => native_subr_0865 as *mut std::ffi::c_void,
        866 => native_subr_0866 as *mut std::ffi::c_void,
        867 => native_subr_0867 as *mut std::ffi::c_void,
        868 => native_subr_0868 as *mut std::ffi::c_void,
        869 => native_subr_0869 as *mut std::ffi::c_void,
        870 => native_subr_0870 as *mut std::ffi::c_void,
        871 => native_subr_0871 as *mut std::ffi::c_void,
        872 => native_subr_0872 as *mut std::ffi::c_void,
        873 => native_subr_0873 as *mut std::ffi::c_void,
        874 => native_subr_0874 as *mut std::ffi::c_void,
        875 => native_subr_0875 as *mut std::ffi::c_void,
        876 => native_subr_0876 as *mut std::ffi::c_void,
        877 => native_subr_0877 as *mut std::ffi::c_void,
        878 => native_subr_0878 as *mut std::ffi::c_void,
        879 => native_subr_0879 as *mut std::ffi::c_void,
        880 => native_subr_0880 as *mut std::ffi::c_void,
        881 => native_subr_0881 as *mut std::ffi::c_void,
        882 => native_subr_0882 as *mut std::ffi::c_void,
        883 => native_subr_0883 as *mut std::ffi::c_void,
        884 => native_subr_0884 as *mut std::ffi::c_void,
        885 => native_subr_0885 as *mut std::ffi::c_void,
        886 => native_subr_0886 as *mut std::ffi::c_void,
        887 => native_subr_0887 as *mut std::ffi::c_void,
        888 => native_subr_0888 as *mut std::ffi::c_void,
        889 => native_subr_0889 as *mut std::ffi::c_void,
        890 => native_subr_0890 as *mut std::ffi::c_void,
        891 => native_subr_0891 as *mut std::ffi::c_void,
        892 => native_subr_0892 as *mut std::ffi::c_void,
        893 => native_subr_0893 as *mut std::ffi::c_void,
        894 => native_subr_0894 as *mut std::ffi::c_void,
        895 => native_subr_0895 as *mut std::ffi::c_void,
        896 => native_subr_0896 as *mut std::ffi::c_void,
        897 => native_subr_0897 as *mut std::ffi::c_void,
        898 => native_subr_0898 as *mut std::ffi::c_void,
        899 => native_subr_0899 as *mut std::ffi::c_void,
        900 => native_subr_0900 as *mut std::ffi::c_void,
        901 => native_subr_0901 as *mut std::ffi::c_void,
        902 => native_subr_0902 as *mut std::ffi::c_void,
        903 => native_subr_0903 as *mut std::ffi::c_void,
        904 => native_subr_0904 as *mut std::ffi::c_void,
        905 => native_subr_0905 as *mut std::ffi::c_void,
        906 => native_subr_0906 as *mut std::ffi::c_void,
        907 => native_subr_0907 as *mut std::ffi::c_void,
        908 => native_subr_0908 as *mut std::ffi::c_void,
        909 => native_subr_0909 as *mut std::ffi::c_void,
        910 => native_subr_0910 as *mut std::ffi::c_void,
        911 => native_subr_0911 as *mut std::ffi::c_void,
        912 => native_subr_0912 as *mut std::ffi::c_void,
        913 => native_subr_0913 as *mut std::ffi::c_void,
        914 => native_subr_0914 as *mut std::ffi::c_void,
        915 => native_subr_0915 as *mut std::ffi::c_void,
        916 => native_subr_0916 as *mut std::ffi::c_void,
        917 => native_subr_0917 as *mut std::ffi::c_void,
        918 => native_subr_0918 as *mut std::ffi::c_void,
        919 => native_subr_0919 as *mut std::ffi::c_void,
        920 => native_subr_0920 as *mut std::ffi::c_void,
        921 => native_subr_0921 as *mut std::ffi::c_void,
        922 => native_subr_0922 as *mut std::ffi::c_void,
        923 => native_subr_0923 as *mut std::ffi::c_void,
        924 => native_subr_0924 as *mut std::ffi::c_void,
        925 => native_subr_0925 as *mut std::ffi::c_void,
        926 => native_subr_0926 as *mut std::ffi::c_void,
        927 => native_subr_0927 as *mut std::ffi::c_void,
        928 => native_subr_0928 as *mut std::ffi::c_void,
        929 => native_subr_0929 as *mut std::ffi::c_void,
        930 => native_subr_0930 as *mut std::ffi::c_void,
        931 => native_subr_0931 as *mut std::ffi::c_void,
        932 => native_subr_0932 as *mut std::ffi::c_void,
        933 => native_subr_0933 as *mut std::ffi::c_void,
        934 => native_subr_0934 as *mut std::ffi::c_void,
        935 => native_subr_0935 as *mut std::ffi::c_void,
        936 => native_subr_0936 as *mut std::ffi::c_void,
        937 => native_subr_0937 as *mut std::ffi::c_void,
        938 => native_subr_0938 as *mut std::ffi::c_void,
        939 => native_subr_0939 as *mut std::ffi::c_void,
        940 => native_subr_0940 as *mut std::ffi::c_void,
        941 => native_subr_0941 as *mut std::ffi::c_void,
        942 => native_subr_0942 as *mut std::ffi::c_void,
        943 => native_subr_0943 as *mut std::ffi::c_void,
        944 => native_subr_0944 as *mut std::ffi::c_void,
        945 => native_subr_0945 as *mut std::ffi::c_void,
        946 => native_subr_0946 as *mut std::ffi::c_void,
        947 => native_subr_0947 as *mut std::ffi::c_void,
        948 => native_subr_0948 as *mut std::ffi::c_void,
        949 => native_subr_0949 as *mut std::ffi::c_void,
        950 => native_subr_0950 as *mut std::ffi::c_void,
        951 => native_subr_0951 as *mut std::ffi::c_void,
        952 => native_subr_0952 as *mut std::ffi::c_void,
        953 => native_subr_0953 as *mut std::ffi::c_void,
        954 => native_subr_0954 as *mut std::ffi::c_void,
        955 => native_subr_0955 as *mut std::ffi::c_void,
        956 => native_subr_0956 as *mut std::ffi::c_void,
        957 => native_subr_0957 as *mut std::ffi::c_void,
        958 => native_subr_0958 as *mut std::ffi::c_void,
        959 => native_subr_0959 as *mut std::ffi::c_void,
        960 => native_subr_0960 as *mut std::ffi::c_void,
        961 => native_subr_0961 as *mut std::ffi::c_void,
        962 => native_subr_0962 as *mut std::ffi::c_void,
        963 => native_subr_0963 as *mut std::ffi::c_void,
        964 => native_subr_0964 as *mut std::ffi::c_void,
        965 => native_subr_0965 as *mut std::ffi::c_void,
        966 => native_subr_0966 as *mut std::ffi::c_void,
        967 => native_subr_0967 as *mut std::ffi::c_void,
        968 => native_subr_0968 as *mut std::ffi::c_void,
        969 => native_subr_0969 as *mut std::ffi::c_void,
        970 => native_subr_0970 as *mut std::ffi::c_void,
        971 => native_subr_0971 as *mut std::ffi::c_void,
        972 => native_subr_0972 as *mut std::ffi::c_void,
        973 => native_subr_0973 as *mut std::ffi::c_void,
        974 => native_subr_0974 as *mut std::ffi::c_void,
        975 => native_subr_0975 as *mut std::ffi::c_void,
        976 => native_subr_0976 as *mut std::ffi::c_void,
        977 => native_subr_0977 as *mut std::ffi::c_void,
        978 => native_subr_0978 as *mut std::ffi::c_void,
        979 => native_subr_0979 as *mut std::ffi::c_void,
        980 => native_subr_0980 as *mut std::ffi::c_void,
        981 => native_subr_0981 as *mut std::ffi::c_void,
        982 => native_subr_0982 as *mut std::ffi::c_void,
        983 => native_subr_0983 as *mut std::ffi::c_void,
        984 => native_subr_0984 as *mut std::ffi::c_void,
        985 => native_subr_0985 as *mut std::ffi::c_void,
        986 => native_subr_0986 as *mut std::ffi::c_void,
        987 => native_subr_0987 as *mut std::ffi::c_void,
        988 => native_subr_0988 as *mut std::ffi::c_void,
        989 => native_subr_0989 as *mut std::ffi::c_void,
        990 => native_subr_0990 as *mut std::ffi::c_void,
        991 => native_subr_0991 as *mut std::ffi::c_void,
        992 => native_subr_0992 as *mut std::ffi::c_void,
        993 => native_subr_0993 as *mut std::ffi::c_void,
        994 => native_subr_0994 as *mut std::ffi::c_void,
        995 => native_subr_0995 as *mut std::ffi::c_void,
        996 => native_subr_0996 as *mut std::ffi::c_void,
        997 => native_subr_0997 as *mut std::ffi::c_void,
        998 => native_subr_0998 as *mut std::ffi::c_void,
        999 => native_subr_0999 as *mut std::ffi::c_void,
        1000 => native_subr_1000 as *mut std::ffi::c_void,
        1001 => native_subr_1001 as *mut std::ffi::c_void,
        1002 => native_subr_1002 as *mut std::ffi::c_void,
        1003 => native_subr_1003 as *mut std::ffi::c_void,
        1004 => native_subr_1004 as *mut std::ffi::c_void,
        1005 => native_subr_1005 as *mut std::ffi::c_void,
        1006 => native_subr_1006 as *mut std::ffi::c_void,
        1007 => native_subr_1007 as *mut std::ffi::c_void,
        1008 => native_subr_1008 as *mut std::ffi::c_void,
        1009 => native_subr_1009 as *mut std::ffi::c_void,
        1010 => native_subr_1010 as *mut std::ffi::c_void,
        1011 => native_subr_1011 as *mut std::ffi::c_void,
        1012 => native_subr_1012 as *mut std::ffi::c_void,
        1013 => native_subr_1013 as *mut std::ffi::c_void,
        1014 => native_subr_1014 as *mut std::ffi::c_void,
        1015 => native_subr_1015 as *mut std::ffi::c_void,
        1016 => native_subr_1016 as *mut std::ffi::c_void,
        1017 => native_subr_1017 as *mut std::ffi::c_void,
        1018 => native_subr_1018 as *mut std::ffi::c_void,
        1019 => native_subr_1019 as *mut std::ffi::c_void,
        1020 => native_subr_1020 as *mut std::ffi::c_void,
        1021 => native_subr_1021 as *mut std::ffi::c_void,
        1022 => native_subr_1022 as *mut std::ffi::c_void,
        1023 => native_subr_1023 as *mut std::ffi::c_void,
        1024 => native_subr_1024 as *mut std::ffi::c_void,
        1025 => native_subr_1025 as *mut std::ffi::c_void,
        1026 => native_subr_1026 as *mut std::ffi::c_void,
        1027 => native_subr_1027 as *mut std::ffi::c_void,
        1028 => native_subr_1028 as *mut std::ffi::c_void,
        1029 => native_subr_1029 as *mut std::ffi::c_void,
        1030 => native_subr_1030 as *mut std::ffi::c_void,
        1031 => native_subr_1031 as *mut std::ffi::c_void,
        1032 => native_subr_1032 as *mut std::ffi::c_void,
        1033 => native_subr_1033 as *mut std::ffi::c_void,
        1034 => native_subr_1034 as *mut std::ffi::c_void,
        1035 => native_subr_1035 as *mut std::ffi::c_void,
        1036 => native_subr_1036 as *mut std::ffi::c_void,
        1037 => native_subr_1037 as *mut std::ffi::c_void,
        1038 => native_subr_1038 as *mut std::ffi::c_void,
        1039 => native_subr_1039 as *mut std::ffi::c_void,
        1040 => native_subr_1040 as *mut std::ffi::c_void,
        1041 => native_subr_1041 as *mut std::ffi::c_void,
        1042 => native_subr_1042 as *mut std::ffi::c_void,
        1043 => native_subr_1043 as *mut std::ffi::c_void,
        1044 => native_subr_1044 as *mut std::ffi::c_void,
        1045 => native_subr_1045 as *mut std::ffi::c_void,
        1046 => native_subr_1046 as *mut std::ffi::c_void,
        1047 => native_subr_1047 as *mut std::ffi::c_void,
        1048 => native_subr_1048 as *mut std::ffi::c_void,
        1049 => native_subr_1049 as *mut std::ffi::c_void,
        1050 => native_subr_1050 as *mut std::ffi::c_void,
        1051 => native_subr_1051 as *mut std::ffi::c_void,
        1052 => native_subr_1052 as *mut std::ffi::c_void,
        1053 => native_subr_1053 as *mut std::ffi::c_void,
        1054 => native_subr_1054 as *mut std::ffi::c_void,
        1055 => native_subr_1055 as *mut std::ffi::c_void,
        1056 => native_subr_1056 as *mut std::ffi::c_void,
        1057 => native_subr_1057 as *mut std::ffi::c_void,
        1058 => native_subr_1058 as *mut std::ffi::c_void,
        1059 => native_subr_1059 as *mut std::ffi::c_void,
        1060 => native_subr_1060 as *mut std::ffi::c_void,
        1061 => native_subr_1061 as *mut std::ffi::c_void,
        1062 => native_subr_1062 as *mut std::ffi::c_void,
        1063 => native_subr_1063 as *mut std::ffi::c_void,
        1064 => native_subr_1064 as *mut std::ffi::c_void,
        1065 => native_subr_1065 as *mut std::ffi::c_void,
        1066 => native_subr_1066 as *mut std::ffi::c_void,
        1067 => native_subr_1067 as *mut std::ffi::c_void,
        1068 => native_subr_1068 as *mut std::ffi::c_void,
        1069 => native_subr_1069 as *mut std::ffi::c_void,
        1070 => native_subr_1070 as *mut std::ffi::c_void,
        1071 => native_subr_1071 as *mut std::ffi::c_void,
        1072 => native_subr_1072 as *mut std::ffi::c_void,
        1073 => native_subr_1073 as *mut std::ffi::c_void,
        1074 => native_subr_1074 as *mut std::ffi::c_void,
        1075 => native_subr_1075 as *mut std::ffi::c_void,
        1076 => native_subr_1076 as *mut std::ffi::c_void,
        1077 => native_subr_1077 as *mut std::ffi::c_void,
        1078 => native_subr_1078 as *mut std::ffi::c_void,
        1079 => native_subr_1079 as *mut std::ffi::c_void,
        1080 => native_subr_1080 as *mut std::ffi::c_void,
        1081 => native_subr_1081 as *mut std::ffi::c_void,
        1082 => native_subr_1082 as *mut std::ffi::c_void,
        1083 => native_subr_1083 as *mut std::ffi::c_void,
        1084 => native_subr_1084 as *mut std::ffi::c_void,
        1085 => native_subr_1085 as *mut std::ffi::c_void,
        1086 => native_subr_1086 as *mut std::ffi::c_void,
        1087 => native_subr_1087 as *mut std::ffi::c_void,
        1088 => native_subr_1088 as *mut std::ffi::c_void,
        1089 => native_subr_1089 as *mut std::ffi::c_void,
        1090 => native_subr_1090 as *mut std::ffi::c_void,
        1091 => native_subr_1091 as *mut std::ffi::c_void,
        1092 => native_subr_1092 as *mut std::ffi::c_void,
        1093 => native_subr_1093 as *mut std::ffi::c_void,
        1094 => native_subr_1094 as *mut std::ffi::c_void,
        1095 => native_subr_1095 as *mut std::ffi::c_void,
        1096 => native_subr_1096 as *mut std::ffi::c_void,
        1097 => native_subr_1097 as *mut std::ffi::c_void,
        1098 => native_subr_1098 as *mut std::ffi::c_void,
        1099 => native_subr_1099 as *mut std::ffi::c_void,
        1100 => native_subr_1100 as *mut std::ffi::c_void,
        1101 => native_subr_1101 as *mut std::ffi::c_void,
        1102 => native_subr_1102 as *mut std::ffi::c_void,
        1103 => native_subr_1103 as *mut std::ffi::c_void,
        1104 => native_subr_1104 as *mut std::ffi::c_void,
        1105 => native_subr_1105 as *mut std::ffi::c_void,
        1106 => native_subr_1106 as *mut std::ffi::c_void,
        1107 => native_subr_1107 as *mut std::ffi::c_void,
        1108 => native_subr_1108 as *mut std::ffi::c_void,
        1109 => native_subr_1109 as *mut std::ffi::c_void,
        1110 => native_subr_1110 as *mut std::ffi::c_void,
        1111 => native_subr_1111 as *mut std::ffi::c_void,
        1112 => native_subr_1112 as *mut std::ffi::c_void,
        1113 => native_subr_1113 as *mut std::ffi::c_void,
        1114 => native_subr_1114 as *mut std::ffi::c_void,
        1115 => native_subr_1115 as *mut std::ffi::c_void,
        1116 => native_subr_1116 as *mut std::ffi::c_void,
        1117 => native_subr_1117 as *mut std::ffi::c_void,
        1118 => native_subr_1118 as *mut std::ffi::c_void,
        1119 => native_subr_1119 as *mut std::ffi::c_void,
        1120 => native_subr_1120 as *mut std::ffi::c_void,
        1121 => native_subr_1121 as *mut std::ffi::c_void,
        1122 => native_subr_1122 as *mut std::ffi::c_void,
        1123 => native_subr_1123 as *mut std::ffi::c_void,
        1124 => native_subr_1124 as *mut std::ffi::c_void,
        1125 => native_subr_1125 as *mut std::ffi::c_void,
        1126 => native_subr_1126 as *mut std::ffi::c_void,
        1127 => native_subr_1127 as *mut std::ffi::c_void,
        1128 => native_subr_1128 as *mut std::ffi::c_void,
        1129 => native_subr_1129 as *mut std::ffi::c_void,
        1130 => native_subr_1130 as *mut std::ffi::c_void,
        1131 => native_subr_1131 as *mut std::ffi::c_void,
        1132 => native_subr_1132 as *mut std::ffi::c_void,
        1133 => native_subr_1133 as *mut std::ffi::c_void,
        1134 => native_subr_1134 as *mut std::ffi::c_void,
        1135 => native_subr_1135 as *mut std::ffi::c_void,
        1136 => native_subr_1136 as *mut std::ffi::c_void,
        1137 => native_subr_1137 as *mut std::ffi::c_void,
        1138 => native_subr_1138 as *mut std::ffi::c_void,
        1139 => native_subr_1139 as *mut std::ffi::c_void,
        1140 => native_subr_1140 as *mut std::ffi::c_void,
        1141 => native_subr_1141 as *mut std::ffi::c_void,
        1142 => native_subr_1142 as *mut std::ffi::c_void,
        1143 => native_subr_1143 as *mut std::ffi::c_void,
        1144 => native_subr_1144 as *mut std::ffi::c_void,
        1145 => native_subr_1145 as *mut std::ffi::c_void,
        1146 => native_subr_1146 as *mut std::ffi::c_void,
        1147 => native_subr_1147 as *mut std::ffi::c_void,
        1148 => native_subr_1148 as *mut std::ffi::c_void,
        1149 => native_subr_1149 as *mut std::ffi::c_void,
        1150 => native_subr_1150 as *mut std::ffi::c_void,
        1151 => native_subr_1151 as *mut std::ffi::c_void,
        1152 => native_subr_1152 as *mut std::ffi::c_void,
        1153 => native_subr_1153 as *mut std::ffi::c_void,
        1154 => native_subr_1154 as *mut std::ffi::c_void,
        1155 => native_subr_1155 as *mut std::ffi::c_void,
        1156 => native_subr_1156 as *mut std::ffi::c_void,
        1157 => native_subr_1157 as *mut std::ffi::c_void,
        1158 => native_subr_1158 as *mut std::ffi::c_void,
        1159 => native_subr_1159 as *mut std::ffi::c_void,
        1160 => native_subr_1160 as *mut std::ffi::c_void,
        1161 => native_subr_1161 as *mut std::ffi::c_void,
        1162 => native_subr_1162 as *mut std::ffi::c_void,
        1163 => native_subr_1163 as *mut std::ffi::c_void,
        1164 => native_subr_1164 as *mut std::ffi::c_void,
        1165 => native_subr_1165 as *mut std::ffi::c_void,
        1166 => native_subr_1166 as *mut std::ffi::c_void,
        1167 => native_subr_1167 as *mut std::ffi::c_void,
        1168 => native_subr_1168 as *mut std::ffi::c_void,
        1169 => native_subr_1169 as *mut std::ffi::c_void,
        1170 => native_subr_1170 as *mut std::ffi::c_void,
        1171 => native_subr_1171 as *mut std::ffi::c_void,
        1172 => native_subr_1172 as *mut std::ffi::c_void,
        1173 => native_subr_1173 as *mut std::ffi::c_void,
        1174 => native_subr_1174 as *mut std::ffi::c_void,
        1175 => native_subr_1175 as *mut std::ffi::c_void,
        1176 => native_subr_1176 as *mut std::ffi::c_void,
        1177 => native_subr_1177 as *mut std::ffi::c_void,
        1178 => native_subr_1178 as *mut std::ffi::c_void,
        1179 => native_subr_1179 as *mut std::ffi::c_void,
        1180 => native_subr_1180 as *mut std::ffi::c_void,
        1181 => native_subr_1181 as *mut std::ffi::c_void,
        1182 => native_subr_1182 as *mut std::ffi::c_void,
        1183 => native_subr_1183 as *mut std::ffi::c_void,
        1184 => native_subr_1184 as *mut std::ffi::c_void,
        1185 => native_subr_1185 as *mut std::ffi::c_void,
        1186 => native_subr_1186 as *mut std::ffi::c_void,
        1187 => native_subr_1187 as *mut std::ffi::c_void,
        1188 => native_subr_1188 as *mut std::ffi::c_void,
        1189 => native_subr_1189 as *mut std::ffi::c_void,
        1190 => native_subr_1190 as *mut std::ffi::c_void,
        1191 => native_subr_1191 as *mut std::ffi::c_void,
        1192 => native_subr_1192 as *mut std::ffi::c_void,
        1193 => native_subr_1193 as *mut std::ffi::c_void,
        1194 => native_subr_1194 as *mut std::ffi::c_void,
        1195 => native_subr_1195 as *mut std::ffi::c_void,
        1196 => native_subr_1196 as *mut std::ffi::c_void,
        1197 => native_subr_1197 as *mut std::ffi::c_void,
        1198 => native_subr_1198 as *mut std::ffi::c_void,
        1199 => native_subr_1199 as *mut std::ffi::c_void,
        1200 => native_subr_1200 as *mut std::ffi::c_void,
        1201 => native_subr_1201 as *mut std::ffi::c_void,
        1202 => native_subr_1202 as *mut std::ffi::c_void,
        1203 => native_subr_1203 as *mut std::ffi::c_void,
        1204 => native_subr_1204 as *mut std::ffi::c_void,
        1205 => native_subr_1205 as *mut std::ffi::c_void,
        1206 => native_subr_1206 as *mut std::ffi::c_void,
        1207 => native_subr_1207 as *mut std::ffi::c_void,
        1208 => native_subr_1208 as *mut std::ffi::c_void,
        1209 => native_subr_1209 as *mut std::ffi::c_void,
        1210 => native_subr_1210 as *mut std::ffi::c_void,
        1211 => native_subr_1211 as *mut std::ffi::c_void,
        1212 => native_subr_1212 as *mut std::ffi::c_void,
        1213 => native_subr_1213 as *mut std::ffi::c_void,
        1214 => native_subr_1214 as *mut std::ffi::c_void,
        1215 => native_subr_1215 as *mut std::ffi::c_void,
        1216 => native_subr_1216 as *mut std::ffi::c_void,
        1217 => native_subr_1217 as *mut std::ffi::c_void,
        1218 => native_subr_1218 as *mut std::ffi::c_void,
        1219 => native_subr_1219 as *mut std::ffi::c_void,
        1220 => native_subr_1220 as *mut std::ffi::c_void,
        1221 => native_subr_1221 as *mut std::ffi::c_void,
        1222 => native_subr_1222 as *mut std::ffi::c_void,
        1223 => native_subr_1223 as *mut std::ffi::c_void,
        1224 => native_subr_1224 as *mut std::ffi::c_void,
        1225 => native_subr_1225 as *mut std::ffi::c_void,
        1226 => native_subr_1226 as *mut std::ffi::c_void,
        1227 => native_subr_1227 as *mut std::ffi::c_void,
        1228 => native_subr_1228 as *mut std::ffi::c_void,
        1229 => native_subr_1229 as *mut std::ffi::c_void,
        1230 => native_subr_1230 as *mut std::ffi::c_void,
        1231 => native_subr_1231 as *mut std::ffi::c_void,
        1232 => native_subr_1232 as *mut std::ffi::c_void,
        1233 => native_subr_1233 as *mut std::ffi::c_void,
        1234 => native_subr_1234 as *mut std::ffi::c_void,
        1235 => native_subr_1235 as *mut std::ffi::c_void,
        1236 => native_subr_1236 as *mut std::ffi::c_void,
        1237 => native_subr_1237 as *mut std::ffi::c_void,
        1238 => native_subr_1238 as *mut std::ffi::c_void,
        1239 => native_subr_1239 as *mut std::ffi::c_void,
        1240 => native_subr_1240 as *mut std::ffi::c_void,
        1241 => native_subr_1241 as *mut std::ffi::c_void,
        1242 => native_subr_1242 as *mut std::ffi::c_void,
        1243 => native_subr_1243 as *mut std::ffi::c_void,
        1244 => native_subr_1244 as *mut std::ffi::c_void,
        1245 => native_subr_1245 as *mut std::ffi::c_void,
        1246 => native_subr_1246 as *mut std::ffi::c_void,
        1247 => native_subr_1247 as *mut std::ffi::c_void,
        1248 => native_subr_1248 as *mut std::ffi::c_void,
        1249 => native_subr_1249 as *mut std::ffi::c_void,
        1250 => native_subr_1250 as *mut std::ffi::c_void,
        1251 => native_subr_1251 as *mut std::ffi::c_void,
        1252 => native_subr_1252 as *mut std::ffi::c_void,
        1253 => native_subr_1253 as *mut std::ffi::c_void,
        1254 => native_subr_1254 as *mut std::ffi::c_void,
        1255 => native_subr_1255 as *mut std::ffi::c_void,
        1256 => native_subr_1256 as *mut std::ffi::c_void,
        1257 => native_subr_1257 as *mut std::ffi::c_void,
        1258 => native_subr_1258 as *mut std::ffi::c_void,
        1259 => native_subr_1259 as *mut std::ffi::c_void,
        1260 => native_subr_1260 as *mut std::ffi::c_void,
        1261 => native_subr_1261 as *mut std::ffi::c_void,
        1262 => native_subr_1262 as *mut std::ffi::c_void,
        1263 => native_subr_1263 as *mut std::ffi::c_void,
        1264 => native_subr_1264 as *mut std::ffi::c_void,
        1265 => native_subr_1265 as *mut std::ffi::c_void,
        1266 => native_subr_1266 as *mut std::ffi::c_void,
        1267 => native_subr_1267 as *mut std::ffi::c_void,
        1268 => native_subr_1268 as *mut std::ffi::c_void,
        1269 => native_subr_1269 as *mut std::ffi::c_void,
        1270 => native_subr_1270 as *mut std::ffi::c_void,
        1271 => native_subr_1271 as *mut std::ffi::c_void,
        1272 => native_subr_1272 as *mut std::ffi::c_void,
        1273 => native_subr_1273 as *mut std::ffi::c_void,
        1274 => native_subr_1274 as *mut std::ffi::c_void,
        1275 => native_subr_1275 as *mut std::ffi::c_void,
        1276 => native_subr_1276 as *mut std::ffi::c_void,
        1277 => native_subr_1277 as *mut std::ffi::c_void,
        1278 => native_subr_1278 as *mut std::ffi::c_void,
        1279 => native_subr_1279 as *mut std::ffi::c_void,
        1280 => native_subr_1280 as *mut std::ffi::c_void,
        1281 => native_subr_1281 as *mut std::ffi::c_void,
        1282 => native_subr_1282 as *mut std::ffi::c_void,
        1283 => native_subr_1283 as *mut std::ffi::c_void,
        1284 => native_subr_1284 as *mut std::ffi::c_void,
        1285 => native_subr_1285 as *mut std::ffi::c_void,
        1286 => native_subr_1286 as *mut std::ffi::c_void,
        1287 => native_subr_1287 as *mut std::ffi::c_void,
        1288 => native_subr_1288 as *mut std::ffi::c_void,
        1289 => native_subr_1289 as *mut std::ffi::c_void,
        1290 => native_subr_1290 as *mut std::ffi::c_void,
        1291 => native_subr_1291 as *mut std::ffi::c_void,
        1292 => native_subr_1292 as *mut std::ffi::c_void,
        1293 => native_subr_1293 as *mut std::ffi::c_void,
        1294 => native_subr_1294 as *mut std::ffi::c_void,
        1295 => native_subr_1295 as *mut std::ffi::c_void,
        1296 => native_subr_1296 as *mut std::ffi::c_void,
        1297 => native_subr_1297 as *mut std::ffi::c_void,
        1298 => native_subr_1298 as *mut std::ffi::c_void,
        1299 => native_subr_1299 as *mut std::ffi::c_void,
        1300 => native_subr_1300 as *mut std::ffi::c_void,
        1301 => native_subr_1301 as *mut std::ffi::c_void,
        1302 => native_subr_1302 as *mut std::ffi::c_void,
        1303 => native_subr_1303 as *mut std::ffi::c_void,
        1304 => native_subr_1304 as *mut std::ffi::c_void,
        1305 => native_subr_1305 as *mut std::ffi::c_void,
        1306 => native_subr_1306 as *mut std::ffi::c_void,
        1307 => native_subr_1307 as *mut std::ffi::c_void,
        1308 => native_subr_1308 as *mut std::ffi::c_void,
        1309 => native_subr_1309 as *mut std::ffi::c_void,
        1310 => native_subr_1310 as *mut std::ffi::c_void,
        1311 => native_subr_1311 as *mut std::ffi::c_void,
        1312 => native_subr_1312 as *mut std::ffi::c_void,
        1313 => native_subr_1313 as *mut std::ffi::c_void,
        1314 => native_subr_1314 as *mut std::ffi::c_void,
        1315 => native_subr_1315 as *mut std::ffi::c_void,
        1316 => native_subr_1316 as *mut std::ffi::c_void,
        1317 => native_subr_1317 as *mut std::ffi::c_void,
        1318 => native_subr_1318 as *mut std::ffi::c_void,
        1319 => native_subr_1319 as *mut std::ffi::c_void,
        1320 => native_subr_1320 as *mut std::ffi::c_void,
        1321 => native_subr_1321 as *mut std::ffi::c_void,
        1322 => native_subr_1322 as *mut std::ffi::c_void,
        1323 => native_subr_1323 as *mut std::ffi::c_void,
        1324 => native_subr_1324 as *mut std::ffi::c_void,
        1325 => native_subr_1325 as *mut std::ffi::c_void,
        1326 => native_subr_1326 as *mut std::ffi::c_void,
        1327 => native_subr_1327 as *mut std::ffi::c_void,
        1328 => native_subr_1328 as *mut std::ffi::c_void,
        1329 => native_subr_1329 as *mut std::ffi::c_void,
        1330 => native_subr_1330 as *mut std::ffi::c_void,
        1331 => native_subr_1331 as *mut std::ffi::c_void,
        1332 => native_subr_1332 as *mut std::ffi::c_void,
        1333 => native_subr_1333 as *mut std::ffi::c_void,
        1334 => native_subr_1334 as *mut std::ffi::c_void,
        1335 => native_subr_1335 as *mut std::ffi::c_void,
        1336 => native_subr_1336 as *mut std::ffi::c_void,
        1337 => native_subr_1337 as *mut std::ffi::c_void,
        1338 => native_subr_1338 as *mut std::ffi::c_void,
        1339 => native_subr_1339 as *mut std::ffi::c_void,
        1340 => native_subr_1340 as *mut std::ffi::c_void,
        1341 => native_subr_1341 as *mut std::ffi::c_void,
        1342 => native_subr_1342 as *mut std::ffi::c_void,
        1343 => native_subr_1343 as *mut std::ffi::c_void,
        1344 => native_subr_1344 as *mut std::ffi::c_void,
        1345 => native_subr_1345 as *mut std::ffi::c_void,
        1346 => native_subr_1346 as *mut std::ffi::c_void,
        1347 => native_subr_1347 as *mut std::ffi::c_void,
        1348 => native_subr_1348 as *mut std::ffi::c_void,
        1349 => native_subr_1349 as *mut std::ffi::c_void,
        1350 => native_subr_1350 as *mut std::ffi::c_void,
        1351 => native_subr_1351 as *mut std::ffi::c_void,
        1352 => native_subr_1352 as *mut std::ffi::c_void,
        1353 => native_subr_1353 as *mut std::ffi::c_void,
        1354 => native_subr_1354 as *mut std::ffi::c_void,
        1355 => native_subr_1355 as *mut std::ffi::c_void,
        1356 => native_subr_1356 as *mut std::ffi::c_void,
        1357 => native_subr_1357 as *mut std::ffi::c_void,
        1358 => native_subr_1358 as *mut std::ffi::c_void,
        1359 => native_subr_1359 as *mut std::ffi::c_void,
        1360 => native_subr_1360 as *mut std::ffi::c_void,
        1361 => native_subr_1361 as *mut std::ffi::c_void,
        1362 => native_subr_1362 as *mut std::ffi::c_void,
        1363 => native_subr_1363 as *mut std::ffi::c_void,
        1364 => native_subr_1364 as *mut std::ffi::c_void,
        1365 => native_subr_1365 as *mut std::ffi::c_void,
        1366 => native_subr_1366 as *mut std::ffi::c_void,
        1367 => native_subr_1367 as *mut std::ffi::c_void,
        1368 => native_subr_1368 as *mut std::ffi::c_void,
        1369 => native_subr_1369 as *mut std::ffi::c_void,
        1370 => native_subr_1370 as *mut std::ffi::c_void,
        1371 => native_subr_1371 as *mut std::ffi::c_void,
        1372 => native_subr_1372 as *mut std::ffi::c_void,
        1373 => native_subr_1373 as *mut std::ffi::c_void,
        1374 => native_subr_1374 as *mut std::ffi::c_void,
        1375 => native_subr_1375 as *mut std::ffi::c_void,
        1376 => native_subr_1376 as *mut std::ffi::c_void,
        1377 => native_subr_1377 as *mut std::ffi::c_void,
        1378 => native_subr_1378 as *mut std::ffi::c_void,
        1379 => native_subr_1379 as *mut std::ffi::c_void,
        1380 => native_subr_1380 as *mut std::ffi::c_void,
        1381 => native_subr_1381 as *mut std::ffi::c_void,
        1382 => native_subr_1382 as *mut std::ffi::c_void,
        1383 => native_subr_1383 as *mut std::ffi::c_void,
        1384 => native_subr_1384 as *mut std::ffi::c_void,
        1385 => native_subr_1385 as *mut std::ffi::c_void,
        1386 => native_subr_1386 as *mut std::ffi::c_void,
        1387 => native_subr_1387 as *mut std::ffi::c_void,
        1388 => native_subr_1388 as *mut std::ffi::c_void,
        1389 => native_subr_1389 as *mut std::ffi::c_void,
        1390 => native_subr_1390 as *mut std::ffi::c_void,
        1391 => native_subr_1391 as *mut std::ffi::c_void,
        1392 => native_subr_1392 as *mut std::ffi::c_void,
        1393 => native_subr_1393 as *mut std::ffi::c_void,
        1394 => native_subr_1394 as *mut std::ffi::c_void,
        1395 => native_subr_1395 as *mut std::ffi::c_void,
        1396 => native_subr_1396 as *mut std::ffi::c_void,
        1397 => native_subr_1397 as *mut std::ffi::c_void,
        1398 => native_subr_1398 as *mut std::ffi::c_void,
        1399 => native_subr_1399 as *mut std::ffi::c_void,
        1400 => native_subr_1400 as *mut std::ffi::c_void,
        1401 => native_subr_1401 as *mut std::ffi::c_void,
        1402 => native_subr_1402 as *mut std::ffi::c_void,
        1403 => native_subr_1403 as *mut std::ffi::c_void,
        1404 => native_subr_1404 as *mut std::ffi::c_void,
        1405 => native_subr_1405 as *mut std::ffi::c_void,
        1406 => native_subr_1406 as *mut std::ffi::c_void,
        1407 => native_subr_1407 as *mut std::ffi::c_void,
        1408 => native_subr_1408 as *mut std::ffi::c_void,
        1409 => native_subr_1409 as *mut std::ffi::c_void,
        1410 => native_subr_1410 as *mut std::ffi::c_void,
        1411 => native_subr_1411 as *mut std::ffi::c_void,
        1412 => native_subr_1412 as *mut std::ffi::c_void,
        1413 => native_subr_1413 as *mut std::ffi::c_void,
        1414 => native_subr_1414 as *mut std::ffi::c_void,
        1415 => native_subr_1415 as *mut std::ffi::c_void,
        1416 => native_subr_1416 as *mut std::ffi::c_void,
        1417 => native_subr_1417 as *mut std::ffi::c_void,
        1418 => native_subr_1418 as *mut std::ffi::c_void,
        1419 => native_subr_1419 as *mut std::ffi::c_void,
        1420 => native_subr_1420 as *mut std::ffi::c_void,
        1421 => native_subr_1421 as *mut std::ffi::c_void,
        1422 => native_subr_1422 as *mut std::ffi::c_void,
        1423 => native_subr_1423 as *mut std::ffi::c_void,
        1424 => native_subr_1424 as *mut std::ffi::c_void,
        1425 => native_subr_1425 as *mut std::ffi::c_void,
        1426 => native_subr_1426 as *mut std::ffi::c_void,
        1427 => native_subr_1427 as *mut std::ffi::c_void,
        1428 => native_subr_1428 as *mut std::ffi::c_void,
        1429 => native_subr_1429 as *mut std::ffi::c_void,
        1430 => native_subr_1430 as *mut std::ffi::c_void,
        1431 => native_subr_1431 as *mut std::ffi::c_void,
        1432 => native_subr_1432 as *mut std::ffi::c_void,
        1433 => native_subr_1433 as *mut std::ffi::c_void,
        1434 => native_subr_1434 as *mut std::ffi::c_void,
        1435 => native_subr_1435 as *mut std::ffi::c_void,
        1436 => native_subr_1436 as *mut std::ffi::c_void,
        1437 => native_subr_1437 as *mut std::ffi::c_void,
        1438 => native_subr_1438 as *mut std::ffi::c_void,
        1439 => native_subr_1439 as *mut std::ffi::c_void,
        1440 => native_subr_1440 as *mut std::ffi::c_void,
        1441 => native_subr_1441 as *mut std::ffi::c_void,
        1442 => native_subr_1442 as *mut std::ffi::c_void,
        1443 => native_subr_1443 as *mut std::ffi::c_void,
        1444 => native_subr_1444 as *mut std::ffi::c_void,
        1445 => native_subr_1445 as *mut std::ffi::c_void,
        1446 => native_subr_1446 as *mut std::ffi::c_void,
        1447 => native_subr_1447 as *mut std::ffi::c_void,
        1448 => native_subr_1448 as *mut std::ffi::c_void,
        1449 => native_subr_1449 as *mut std::ffi::c_void,
        1450 => native_subr_1450 as *mut std::ffi::c_void,
        1451 => native_subr_1451 as *mut std::ffi::c_void,
        1452 => native_subr_1452 as *mut std::ffi::c_void,
        1453 => native_subr_1453 as *mut std::ffi::c_void,
        1454 => native_subr_1454 as *mut std::ffi::c_void,
        _ => std::ptr::null_mut(),
    }
}

unsafe extern "C" fn native_subr_0000(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(0, nargs, args) }
}

unsafe extern "C" fn native_subr_0001(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1, nargs, args) }
}

unsafe extern "C" fn native_subr_0002(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(2, nargs, args) }
}

unsafe extern "C" fn native_subr_0003(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(3, nargs, args) }
}

extern "C" fn native_subr_0004() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(4, &[])
}

extern "C" fn native_subr_0005(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(5, &[arg_0, arg_1])
}

extern "C" fn native_subr_0006(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(6, &[arg_0, arg_1])
}

extern "C" fn native_subr_0007(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(7, &[arg_0, arg_1])
}

extern "C" fn native_subr_0008() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(8, &[])
}

extern "C" fn native_subr_0009() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(9, &[])
}

extern "C" fn native_subr_0010() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(10, &[])
}

extern "C" fn native_subr_0011() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(11, &[])
}

extern "C" fn native_subr_0012() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(12, &[])
}

extern "C" fn native_subr_0013() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(13, &[])
}

extern "C" fn native_subr_0014() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(14, &[])
}

extern "C" fn native_subr_0015(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(15, &[arg_0])
}

extern "C" fn native_subr_0016(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(16, &[arg_0, arg_1])
}

extern "C" fn native_subr_0017(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(17, &[arg_0])
}

extern "C" fn native_subr_0018(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(18, &[arg_0])
}

extern "C" fn native_subr_0019(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(19, &[arg_0])
}

extern "C" fn native_subr_0020(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(20, &[arg_0, arg_1])
}

extern "C" fn native_subr_0021(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(21, &[arg_0])
}

extern "C" fn native_subr_0022(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(22, &[arg_0, arg_1])
}

extern "C" fn native_subr_0023(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(23, &[arg_0])
}

extern "C" fn native_subr_0024(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(24, &[arg_0])
}

extern "C" fn native_subr_0025(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(25, &[arg_0])
}

extern "C" fn native_subr_0026(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(26, &[arg_0])
}

extern "C" fn native_subr_0027() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(27, &[])
}

extern "C" fn native_subr_0028(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(28, &[arg_0])
}

extern "C" fn native_subr_0029(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(29, &[arg_0])
}

extern "C" fn native_subr_0030(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(30, &[arg_0])
}

extern "C" fn native_subr_0031(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(31, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0032(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(32, &[arg_0])
}

extern "C" fn native_subr_0033() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(33, &[])
}

extern "C" fn native_subr_0034(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(34, &[arg_0, arg_1])
}

extern "C" fn native_subr_0035() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(35, &[])
}

extern "C" fn native_subr_0036(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(36, &[arg_0])
}

extern "C" fn native_subr_0037(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(37, &[arg_0])
}

extern "C" fn native_subr_0038(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(38, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0039() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(39, &[])
}

extern "C" fn native_subr_0040(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(40, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0041(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(41, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0042(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(42, &[arg_0, arg_1])
}

extern "C" fn native_subr_0043(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(43, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0044() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(44, &[])
}

extern "C" fn native_subr_0045() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(45, &[])
}

extern "C" fn native_subr_0046() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(46, &[])
}

extern "C" fn native_subr_0047(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(47, &[arg_0])
}

extern "C" fn native_subr_0048(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(48, &[arg_0])
}

extern "C" fn native_subr_0049(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(49, &[arg_0])
}

extern "C" fn native_subr_0050(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(50, &[arg_0, arg_1])
}

extern "C" fn native_subr_0051(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(51, &[arg_0])
}

extern "C" fn native_subr_0052(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(52, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0053(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(53, &[arg_0])
}

extern "C" fn native_subr_0054(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(54, &[arg_0])
}

extern "C" fn native_subr_0055(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(55, &[arg_0])
}

extern "C" fn native_subr_0056(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(56, &[arg_0, arg_1])
}

extern "C" fn native_subr_0057(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(57, &[arg_0])
}

extern "C" fn native_subr_0058(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(58, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0059(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(59, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0060(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(60, &[arg_0, arg_1])
}

extern "C" fn native_subr_0061() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(61, &[])
}

extern "C" fn native_subr_0062(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(62, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0063(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(63, &[arg_0])
}

extern "C" fn native_subr_0064() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(64, &[])
}

extern "C" fn native_subr_0065(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(65, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0066(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(66, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0067(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(67, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0068(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(68, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0069(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(69, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0070(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(70, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0071() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(71, &[])
}

extern "C" fn native_subr_0072(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(72, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0073(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(73, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0074(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(74, &[arg_0, arg_1])
}

extern "C" fn native_subr_0075(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(75, &[arg_0, arg_1])
}

extern "C" fn native_subr_0076(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(76, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0077(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(77, &[arg_0, arg_1])
}

extern "C" fn native_subr_0078(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(78, &[arg_0, arg_1])
}

extern "C" fn native_subr_0079(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(79, &[arg_0, arg_1])
}

extern "C" fn native_subr_0080(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(80, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0081(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(81, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0082(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(82, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0083(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(83, &[arg_0])
}

extern "C" fn native_subr_0084() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(84, &[])
}

extern "C" fn native_subr_0085() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(85, &[])
}

extern "C" fn native_subr_0086() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(86, &[])
}

extern "C" fn native_subr_0087() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(87, &[])
}

extern "C" fn native_subr_0088(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(88, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0089(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(89, &[arg_0, arg_1])
}

extern "C" fn native_subr_0090(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(90, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0091(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(91, &[arg_0, arg_1])
}

extern "C" fn native_subr_0092(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(92, &[arg_0, arg_1])
}

extern "C" fn native_subr_0093() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(93, &[])
}

extern "C" fn native_subr_0094(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(94, &[arg_0, arg_1])
}

extern "C" fn native_subr_0095(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(95, &[arg_0])
}

extern "C" fn native_subr_0096() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(96, &[])
}

extern "C" fn native_subr_0097(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(97, &[arg_0])
}

extern "C" fn native_subr_0098(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(98, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0099(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(99, &[arg_0, arg_1])
}

extern "C" fn native_subr_0100(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(100, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0101(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(101, &[arg_0])
}

extern "C" fn native_subr_0102() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(102, &[])
}

extern "C" fn native_subr_0103(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(103, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0104(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(104, &[arg_0])
}

extern "C" fn native_subr_0105(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(105, &[arg_0, arg_1])
}

extern "C" fn native_subr_0106() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(106, &[])
}

extern "C" fn native_subr_0107(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(107, &[arg_0])
}

extern "C" fn native_subr_0108(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(108, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0109(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(109, &[arg_0])
}

extern "C" fn native_subr_0110(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(110, &[arg_0])
}

extern "C" fn native_subr_0111(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(111, &[arg_0, arg_1])
}

extern "C" fn native_subr_0112() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(112, &[])
}

extern "C" fn native_subr_0113(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(113, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0114(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(114, &[arg_0])
}

extern "C" fn native_subr_0115(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(115, &[arg_0, arg_1])
}

extern "C" fn native_subr_0116(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(116, &[arg_0])
}

extern "C" fn native_subr_0117(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(117, &[arg_0])
}

extern "C" fn native_subr_0118(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(118, &[arg_0])
}

extern "C" fn native_subr_0119(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(119, &[arg_0])
}

extern "C" fn native_subr_0120(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(120, &[arg_0])
}

extern "C" fn native_subr_0121(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(121, &[arg_0])
}

extern "C" fn native_subr_0122(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(122, &[arg_0])
}

extern "C" fn native_subr_0123(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(123, &[arg_0])
}

extern "C" fn native_subr_0124(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(124, &[arg_0])
}

extern "C" fn native_subr_0125(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(125, &[arg_0])
}

extern "C" fn native_subr_0126(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(126, &[arg_0])
}

extern "C" fn native_subr_0127(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(127, &[arg_0])
}

extern "C" fn native_subr_0128(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(128, &[arg_0])
}

extern "C" fn native_subr_0129(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(129, &[arg_0])
}

extern "C" fn native_subr_0130(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(130, &[arg_0])
}

extern "C" fn native_subr_0131(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(131, &[arg_0])
}

extern "C" fn native_subr_0132(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(132, &[arg_0, arg_1])
}

extern "C" fn native_subr_0133(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(133, &[arg_0, arg_1])
}

extern "C" fn native_subr_0134(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(134, &[arg_0])
}

extern "C" fn native_subr_0135(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(135, &[arg_0])
}

extern "C" fn native_subr_0136(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(136, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0137(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(137, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0138(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(138, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0139(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(139, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_0140(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(140, &[arg_0])
}

extern "C" fn native_subr_0141(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(141, &[arg_0])
}

extern "C" fn native_subr_0142() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(142, &[])
}

extern "C" fn native_subr_0143(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(143, &[arg_0, arg_1])
}

extern "C" fn native_subr_0144(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(144, &[arg_0, arg_1])
}

extern "C" fn native_subr_0145(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(145, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0146(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(146, &[arg_0, arg_1])
}

extern "C" fn native_subr_0147(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(147, &[arg_0, arg_1])
}

extern "C" fn native_subr_0148(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(148, &[arg_0])
}

extern "C" fn native_subr_0149(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(149, &[arg_0, arg_1])
}

extern "C" fn native_subr_0150(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(150, &[arg_0, arg_1])
}

extern "C" fn native_subr_0151(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(151, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0152(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(152, &[arg_0])
}

extern "C" fn native_subr_0153(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(153, &[arg_0, arg_1])
}

extern "C" fn native_subr_0154(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(154, &[arg_0])
}

extern "C" fn native_subr_0155(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(155, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0156(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(156, &[arg_0, arg_1])
}

extern "C" fn native_subr_0157(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(157, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0158(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(158, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0159(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(159, &[arg_0])
}

extern "C" fn native_subr_0160(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(160, &[arg_0, arg_1])
}

extern "C" fn native_subr_0161(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(161, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0162(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(162, &[arg_0, arg_1])
}

extern "C" fn native_subr_0163(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(163, &[arg_0, arg_1])
}

extern "C" fn native_subr_0164(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(164, &[arg_0, arg_1])
}

extern "C" fn native_subr_0165() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(165, &[])
}

extern "C" fn native_subr_0166(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(166, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0167(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(167, &[arg_0, arg_1])
}

extern "C" fn native_subr_0168(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(168, &[arg_0])
}

extern "C" fn native_subr_0169(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(169, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0170(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(170, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0171(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(171, &[arg_0, arg_1])
}

extern "C" fn native_subr_0172(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(172, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_0173(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(173, nargs, args) }
}

extern "C" fn native_subr_0174(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(174, &[arg_0, arg_1])
}

extern "C" fn native_subr_0175() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(175, &[])
}

extern "C" fn native_subr_0176(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(176, &[arg_0])
}

extern "C" fn native_subr_0177() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(177, &[])
}

extern "C" fn native_subr_0178(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(178, &[arg_0])
}

extern "C" fn native_subr_0179(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(179, &[arg_0])
}

extern "C" fn native_subr_0180(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(180, &[arg_0])
}

extern "C" fn native_subr_0181(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(181, &[arg_0])
}

extern "C" fn native_subr_0182(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(182, &[arg_0, arg_1])
}

extern "C" fn native_subr_0183(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(183, &[arg_0, arg_1])
}

extern "C" fn native_subr_0184(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(184, &[arg_0])
}

extern "C" fn native_subr_0185(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(185, &[arg_0])
}

extern "C" fn native_subr_0186(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(186, &[arg_0])
}

extern "C" fn native_subr_0187(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(187, &[arg_0, arg_1])
}

extern "C" fn native_subr_0188(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(188, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0189(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(189, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0190(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(190, &[arg_0])
}

extern "C" fn native_subr_0191(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(191, &[arg_0])
}

extern "C" fn native_subr_0192(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(192, &[arg_0])
}

extern "C" fn native_subr_0193() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(193, &[])
}

extern "C" fn native_subr_0194(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(194, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0195() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(195, &[])
}

extern "C" fn native_subr_0196(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(196, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0197(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(197, &[arg_0])
}

extern "C" fn native_subr_0198(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(198, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0199(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(199, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_0200(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(200, &[arg_0])
}

extern "C" fn native_subr_0201(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(201, &[arg_0])
}

extern "C" fn native_subr_0202(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(202, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0203() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(203, &[])
}

extern "C" fn native_subr_0204(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(204, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0205(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(205, &[arg_0, arg_1])
}

extern "C" fn native_subr_0206(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(206, &[arg_0, arg_1])
}

extern "C" fn native_subr_0207(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(207, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0208(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(208, &[arg_0, arg_1])
}

extern "C" fn native_subr_0209(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(209, &[arg_0])
}

extern "C" fn native_subr_0210(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(210, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0211(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(211, &[arg_0, arg_1])
}

extern "C" fn native_subr_0212(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(212, &[arg_0])
}

extern "C" fn native_subr_0213(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(213, &[arg_0, arg_1])
}

extern "C" fn native_subr_0214(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(214, &[arg_0])
}

extern "C" fn native_subr_0215(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(215, &[arg_0, arg_1])
}

extern "C" fn native_subr_0216(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(216, &[arg_0])
}

extern "C" fn native_subr_0217(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(217, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0218(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(218, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0219(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(219, &[arg_0])
}

extern "C" fn native_subr_0220(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(220, &[arg_0, arg_1])
}

extern "C" fn native_subr_0221(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(221, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0222(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(222, &[arg_0, arg_1])
}

extern "C" fn native_subr_0223(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(223, &[arg_0])
}

extern "C" fn native_subr_0224(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(224, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0225(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(225, &[arg_0])
}

extern "C" fn native_subr_0226(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(226, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0227(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(227, &[arg_0])
}

extern "C" fn native_subr_0228(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(228, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0229(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(229, &[arg_0])
}

extern "C" fn native_subr_0230(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(230, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0231(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(231, &[arg_0])
}

extern "C" fn native_subr_0232(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(232, &[arg_0])
}

extern "C" fn native_subr_0233(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(233, &[arg_0])
}

extern "C" fn native_subr_0234(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(234, &[arg_0, arg_1])
}

extern "C" fn native_subr_0235(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(235, &[arg_0, arg_1])
}

extern "C" fn native_subr_0236(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(236, &[arg_0, arg_1])
}

extern "C" fn native_subr_0237() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(237, &[])
}

extern "C" fn native_subr_0238() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(238, &[])
}

extern "C" fn native_subr_0239(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(239, &[arg_0, arg_1])
}

extern "C" fn native_subr_0240(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(240, &[arg_0, arg_1])
}

extern "C" fn native_subr_0241(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(241, &[arg_0])
}

extern "C" fn native_subr_0242(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(242, &[arg_0])
}

extern "C" fn native_subr_0243(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(243, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0244(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(244, &[arg_0])
}

extern "C" fn native_subr_0245(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(245, &[arg_0, arg_1])
}

extern "C" fn native_subr_0246(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(246, &[arg_0])
}

extern "C" fn native_subr_0247(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(247, &[arg_0])
}

extern "C" fn native_subr_0248(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(248, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0249(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(249, &[arg_0])
}

extern "C" fn native_subr_0250(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(250, &[arg_0])
}

extern "C" fn native_subr_0251(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(251, &[arg_0, arg_1])
}

extern "C" fn native_subr_0252(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(252, &[arg_0, arg_1])
}

extern "C" fn native_subr_0253(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(253, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0254(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(254, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0255(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(255, &[arg_0, arg_1])
}

extern "C" fn native_subr_0256(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(256, &[arg_0])
}

extern "C" fn native_subr_0257(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(257, &[arg_0, arg_1])
}

extern "C" fn native_subr_0258(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(258, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0259(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(259, &[arg_0])
}

extern "C" fn native_subr_0260(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(260, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0261(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(261, &[arg_0, arg_1])
}

extern "C" fn native_subr_0262(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(262, &[arg_0, arg_1])
}

extern "C" fn native_subr_0263(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(263, &[arg_0])
}

extern "C" fn native_subr_0264(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(264, &[arg_0])
}

extern "C" fn native_subr_0265(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(265, &[arg_0])
}

extern "C" fn native_subr_0266(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(266, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0267(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(267, &[arg_0, arg_1])
}

extern "C" fn native_subr_0268(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(268, &[arg_0])
}

extern "C" fn native_subr_0269(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(269, &[arg_0])
}

extern "C" fn native_subr_0270(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(270, &[arg_0])
}

extern "C" fn native_subr_0271(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(271, &[arg_0])
}

extern "C" fn native_subr_0272(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(272, &[arg_0])
}

extern "C" fn native_subr_0273(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(273, &[arg_0])
}

extern "C" fn native_subr_0274(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(274, &[arg_0])
}

extern "C" fn native_subr_0275(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(275, &[arg_0, arg_1])
}

extern "C" fn native_subr_0276(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(276, &[arg_0])
}

extern "C" fn native_subr_0277(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(277, &[arg_0, arg_1])
}

extern "C" fn native_subr_0278(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(278, &[arg_0, arg_1])
}

extern "C" fn native_subr_0279(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(279, &[arg_0, arg_1])
}

extern "C" fn native_subr_0280(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(280, &[arg_0, arg_1])
}

extern "C" fn native_subr_0281(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(281, &[arg_0, arg_1])
}

extern "C" fn native_subr_0282(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(282, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0283(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(283, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0284(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(284, &[arg_0])
}

extern "C" fn native_subr_0285(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(285, &[arg_0])
}

extern "C" fn native_subr_0286(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(286, &[arg_0])
}

extern "C" fn native_subr_0287(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(287, &[arg_0])
}

extern "C" fn native_subr_0288(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(288, &[arg_0])
}

extern "C" fn native_subr_0289(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(289, &[arg_0])
}

extern "C" fn native_subr_0290(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(290, &[arg_0])
}

extern "C" fn native_subr_0291(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(291, &[arg_0, arg_1])
}

extern "C" fn native_subr_0292(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(292, &[arg_0, arg_1])
}

extern "C" fn native_subr_0293(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(293, &[arg_0, arg_1])
}

extern "C" fn native_subr_0294(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(294, &[arg_0])
}

extern "C" fn native_subr_0295(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(295, &[arg_0])
}

extern "C" fn native_subr_0296(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(296, &[arg_0])
}

extern "C" fn native_subr_0297(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(297, &[arg_0])
}

extern "C" fn native_subr_0298(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(298, &[arg_0])
}

extern "C" fn native_subr_0299(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(299, &[arg_0])
}

extern "C" fn native_subr_0300(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(300, &[arg_0])
}

extern "C" fn native_subr_0301(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(301, &[arg_0, arg_1])
}

extern "C" fn native_subr_0302(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(302, &[arg_0])
}

extern "C" fn native_subr_0303(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(303, &[arg_0])
}

extern "C" fn native_subr_0304(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(304, &[arg_0])
}

extern "C" fn native_subr_0305(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(305, &[arg_0])
}

extern "C" fn native_subr_0306(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(306, &[arg_0])
}

extern "C" fn native_subr_0307(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(307, &[arg_0])
}

extern "C" fn native_subr_0308(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(308, &[arg_0])
}

extern "C" fn native_subr_0309(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(309, &[arg_0])
}

extern "C" fn native_subr_0310(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(310, &[arg_0, arg_1])
}

extern "C" fn native_subr_0311(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(311, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0312(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(312, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0313(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(313, &[arg_0])
}

extern "C" fn native_subr_0314(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(314, &[arg_0])
}

extern "C" fn native_subr_0315(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(315, &[arg_0])
}

extern "C" fn native_subr_0316(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(316, &[arg_0])
}

extern "C" fn native_subr_0317(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(317, &[arg_0])
}

extern "C" fn native_subr_0318(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(318, &[arg_0])
}

extern "C" fn native_subr_0319(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(319, &[arg_0])
}

extern "C" fn native_subr_0320(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(320, &[arg_0])
}

extern "C" fn native_subr_0321(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(321, &[arg_0])
}

extern "C" fn native_subr_0322(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(322, &[arg_0])
}

extern "C" fn native_subr_0323() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(323, &[])
}

extern "C" fn native_subr_0324() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(324, &[])
}

extern "C" fn native_subr_0325(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(325, &[arg_0])
}

extern "C" fn native_subr_0326() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(326, &[])
}

extern "C" fn native_subr_0327(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(327, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0328(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(328, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0329(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(329, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0330(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(330, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0331(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(331, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0332(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(332, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0333(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(333, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0334(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(334, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0335(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(335, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0336(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(336, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0337(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(337, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0338(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(338, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0339(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(339, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0340(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(340, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0341(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(341, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0342(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(342, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0343(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(343, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0344(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(344, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0345(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(345, &[arg_0, arg_1])
}

extern "C" fn native_subr_0346(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(346, &[arg_0, arg_1])
}

extern "C" fn native_subr_0347(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(347, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0348(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(348, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0349(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(349, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0350(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(350, &[arg_0, arg_1])
}

extern "C" fn native_subr_0351(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(351, &[arg_0])
}

extern "C" fn native_subr_0352() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(352, &[])
}

extern "C" fn native_subr_0353(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(353, &[arg_0])
}

extern "C" fn native_subr_0354(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(354, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0355(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(355, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0356(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(356, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0357(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(357, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0358(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(358, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0359(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(359, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0360(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(360, &[arg_0])
}

extern "C" fn native_subr_0361(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(361, &[arg_0])
}

extern "C" fn native_subr_0362(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(362, &[arg_0, arg_1])
}

extern "C" fn native_subr_0363(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(363, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0364(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(364, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0365(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(365, &[arg_0, arg_1])
}

extern "C" fn native_subr_0366(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(366, &[arg_0, arg_1])
}

extern "C" fn native_subr_0367(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(367, &[arg_0, arg_1])
}

extern "C" fn native_subr_0368(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(368, &[arg_0, arg_1])
}

extern "C" fn native_subr_0369(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(369, &[arg_0, arg_1])
}

extern "C" fn native_subr_0370(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(370, &[arg_0, arg_1])
}

extern "C" fn native_subr_0371(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(371, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0372(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(372, &[arg_0])
}

extern "C" fn native_subr_0373(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(373, &[arg_0])
}

extern "C" fn native_subr_0374(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(374, &[arg_0])
}

extern "C" fn native_subr_0375(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(375, &[arg_0])
}

extern "C" fn native_subr_0376(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(376, &[arg_0])
}

extern "C" fn native_subr_0377(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(377, &[arg_0, arg_1])
}

extern "C" fn native_subr_0378(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(378, &[arg_0, arg_1])
}

extern "C" fn native_subr_0379(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(379, &[arg_0])
}

extern "C" fn native_subr_0380(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(380, &[arg_0])
}

extern "C" fn native_subr_0381(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(381, &[arg_0, arg_1])
}

extern "C" fn native_subr_0382(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(382, &[arg_0])
}

extern "C" fn native_subr_0383(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(383, &[arg_0])
}

extern "C" fn native_subr_0384(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(384, &[arg_0])
}

extern "C" fn native_subr_0385(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(385, &[arg_0])
}

extern "C" fn native_subr_0386(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(386, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0387(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(387, &[arg_0])
}

extern "C" fn native_subr_0388(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(388, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0389(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(389, &[arg_0])
}

extern "C" fn native_subr_0390(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(390, &[arg_0])
}

extern "C" fn native_subr_0391(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(391, &[arg_0])
}

extern "C" fn native_subr_0392(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(392, &[arg_0])
}

extern "C" fn native_subr_0393(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(393, &[arg_0])
}

extern "C" fn native_subr_0394(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(394, &[arg_0])
}

extern "C" fn native_subr_0395(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(395, &[arg_0])
}

extern "C" fn native_subr_0396(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(396, &[arg_0])
}

extern "C" fn native_subr_0397(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(397, &[arg_0, arg_1])
}

extern "C" fn native_subr_0398(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(398, &[arg_0])
}

extern "C" fn native_subr_0399() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(399, &[])
}

extern "C" fn native_subr_0400(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(400, &[arg_0])
}

extern "C" fn native_subr_0401(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(401, &[arg_0, arg_1])
}

extern "C" fn native_subr_0402(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(402, &[arg_0])
}

extern "C" fn native_subr_0403(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(403, &[arg_0])
}

extern "C" fn native_subr_0404(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(404, &[arg_0])
}

extern "C" fn native_subr_0405(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(405, &[arg_0])
}

extern "C" fn native_subr_0406(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(406, &[arg_0])
}

extern "C" fn native_subr_0407(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(407, &[arg_0])
}

extern "C" fn native_subr_0408(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(408, &[arg_0])
}

extern "C" fn native_subr_0409(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(409, &[arg_0])
}

extern "C" fn native_subr_0410(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(410, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0411(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(411, &[arg_0, arg_1])
}

extern "C" fn native_subr_0412(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(412, &[arg_0])
}

extern "C" fn native_subr_0413(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(413, &[arg_0])
}

extern "C" fn native_subr_0414() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(414, &[])
}

extern "C" fn native_subr_0415(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(415, &[arg_0])
}

extern "C" fn native_subr_0416(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(416, &[arg_0])
}

extern "C" fn native_subr_0417(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(417, &[arg_0, arg_1])
}

extern "C" fn native_subr_0418(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(418, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0419() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(419, &[])
}

extern "C" fn native_subr_0420(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(420, &[arg_0, arg_1])
}

extern "C" fn native_subr_0421(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(421, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0422(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(422, &[arg_0])
}

extern "C" fn native_subr_0423(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(423, &[arg_0, arg_1])
}

extern "C" fn native_subr_0424(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(424, &[arg_0, arg_1])
}

extern "C" fn native_subr_0425(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(425, &[arg_0, arg_1])
}

extern "C" fn native_subr_0426(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(426, &[arg_0, arg_1])
}

extern "C" fn native_subr_0427(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(427, &[arg_0])
}

extern "C" fn native_subr_0428(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(428, &[arg_0])
}

extern "C" fn native_subr_0429(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(429, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0430(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(430, &[arg_0])
}

extern "C" fn native_subr_0431(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(431, &[arg_0])
}

extern "C" fn native_subr_0432(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(432, &[arg_0])
}

extern "C" fn native_subr_0433(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(433, &[arg_0])
}

extern "C" fn native_subr_0434(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(434, &[arg_0])
}

extern "C" fn native_subr_0435(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(435, &[arg_0])
}

extern "C" fn native_subr_0436() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(436, &[])
}

extern "C" fn native_subr_0437() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(437, &[])
}

extern "C" fn native_subr_0438(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(438, &[arg_0])
}

extern "C" fn native_subr_0439(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(439, &[arg_0])
}

extern "C" fn native_subr_0440(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(440, &[arg_0])
}

extern "C" fn native_subr_0441(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(441, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0442(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(442, &[arg_0])
}

extern "C" fn native_subr_0443(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(443, &[arg_0, arg_1])
}

extern "C" fn native_subr_0444(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(444, &[arg_0])
}

extern "C" fn native_subr_0445(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(445, &[arg_0, arg_1])
}

extern "C" fn native_subr_0446(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(446, &[arg_0])
}

extern "C" fn native_subr_0447(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(447, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0448(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(448, &[arg_0])
}

extern "C" fn native_subr_0449(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(449, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0450(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(450, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0451(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(451, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0452(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(452, &[arg_0])
}

extern "C" fn native_subr_0453(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(453, &[arg_0])
}

extern "C" fn native_subr_0454(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(454, &[arg_0])
}

extern "C" fn native_subr_0455(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(455, &[arg_0])
}

extern "C" fn native_subr_0456(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(456, &[arg_0])
}

extern "C" fn native_subr_0457(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(457, &[arg_0])
}

extern "C" fn native_subr_0458(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(458, &[arg_0])
}

extern "C" fn native_subr_0459(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(459, &[arg_0])
}

extern "C" fn native_subr_0460(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(460, &[arg_0])
}

extern "C" fn native_subr_0461(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(461, &[arg_0])
}

extern "C" fn native_subr_0462(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(462, &[arg_0])
}

extern "C" fn native_subr_0463(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(463, &[arg_0])
}

extern "C" fn native_subr_0464(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(464, &[arg_0])
}

extern "C" fn native_subr_0465(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(465, &[arg_0])
}

extern "C" fn native_subr_0466(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(466, &[arg_0])
}

extern "C" fn native_subr_0467(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(467, &[arg_0])
}

extern "C" fn native_subr_0468(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(468, &[arg_0])
}

extern "C" fn native_subr_0469(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(469, &[arg_0])
}

extern "C" fn native_subr_0470(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(470, &[arg_0, arg_1])
}

extern "C" fn native_subr_0471(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(471, &[arg_0, arg_1])
}

extern "C" fn native_subr_0472(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(472, &[arg_0])
}

extern "C" fn native_subr_0473(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(473, &[arg_0])
}

extern "C" fn native_subr_0474(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(474, &[arg_0, arg_1])
}

extern "C" fn native_subr_0475(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(475, &[arg_0, arg_1])
}

extern "C" fn native_subr_0476(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(476, &[arg_0, arg_1])
}

extern "C" fn native_subr_0477(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(477, &[arg_0])
}

extern "C" fn native_subr_0478(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(478, &[arg_0])
}

extern "C" fn native_subr_0479() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(479, &[])
}

extern "C" fn native_subr_0480(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(480, &[arg_0])
}

extern "C" fn native_subr_0481(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(481, &[arg_0])
}

extern "C" fn native_subr_0482(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(482, &[arg_0, arg_1])
}

extern "C" fn native_subr_0483(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(483, &[arg_0])
}

extern "C" fn native_subr_0484(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(484, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0485(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(485, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0486() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(486, &[])
}

extern "C" fn native_subr_0487() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(487, &[])
}

extern "C" fn native_subr_0488(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(488, &[arg_0, arg_1])
}

extern "C" fn native_subr_0489() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(489, &[])
}

extern "C" fn native_subr_0490(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(490, &[arg_0, arg_1])
}

extern "C" fn native_subr_0491(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(491, &[arg_0, arg_1])
}

extern "C" fn native_subr_0492(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(492, &[arg_0, arg_1])
}

extern "C" fn native_subr_0493(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(493, &[arg_0])
}

extern "C" fn native_subr_0494() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(494, &[])
}

extern "C" fn native_subr_0495() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(495, &[])
}

extern "C" fn native_subr_0496() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(496, &[])
}

extern "C" fn native_subr_0497(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(497, &[arg_0])
}

extern "C" fn native_subr_0498(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(498, &[arg_0, arg_1])
}

extern "C" fn native_subr_0499(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(499, &[arg_0])
}

extern "C" fn native_subr_0500(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(500, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0501(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(501, &[arg_0])
}

extern "C" fn native_subr_0502(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(502, &[arg_0])
}

extern "C" fn native_subr_0503(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(503, &[arg_0])
}

extern "C" fn native_subr_0504(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(504, &[arg_0])
}

extern "C" fn native_subr_0505(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(505, &[arg_0, arg_1])
}

extern "C" fn native_subr_0506(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(506, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_0507(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(507, nargs, args) }
}

extern "C" fn native_subr_0508(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(508, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0509(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(509, &[arg_0])
}

extern "C" fn native_subr_0510(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(510, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0511(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(511, &[arg_0, arg_1])
}

extern "C" fn native_subr_0512(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(512, &[arg_0, arg_1])
}

extern "C" fn native_subr_0513(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(513, &[arg_0, arg_1])
}

extern "C" fn native_subr_0514(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(514, &[arg_0, arg_1])
}

extern "C" fn native_subr_0515(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(515, &[arg_0, arg_1])
}

extern "C" fn native_subr_0516() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(516, &[])
}

extern "C" fn native_subr_0517() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(517, &[])
}

extern "C" fn native_subr_0518() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(518, &[])
}

extern "C" fn native_subr_0519(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(519, &[arg_0, arg_1])
}

extern "C" fn native_subr_0520(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(520, &[arg_0])
}

extern "C" fn native_subr_0521(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(521, &[arg_0])
}

extern "C" fn native_subr_0522(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(522, &[arg_0])
}

extern "C" fn native_subr_0523(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(523, &[arg_0, arg_1])
}

extern "C" fn native_subr_0524(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(524, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0525(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(525, &[arg_0])
}

extern "C" fn native_subr_0526(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(526, &[arg_0])
}

extern "C" fn native_subr_0527(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(527, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0528(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(528, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0529(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(529, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0530(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(530, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0531(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(531, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0532(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(532, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0533(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(533, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0534(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(534, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0535(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(535, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0536(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(536, &[arg_0, arg_1])
}

extern "C" fn native_subr_0537(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(537, &[arg_0, arg_1])
}

extern "C" fn native_subr_0538() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(538, &[])
}

extern "C" fn native_subr_0539(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(539, &[arg_0])
}

extern "C" fn native_subr_0540(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(540, &[arg_0])
}

extern "C" fn native_subr_0541() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(541, &[])
}

extern "C" fn native_subr_0542(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(542, &[arg_0])
}

extern "C" fn native_subr_0543(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(543, &[arg_0])
}

extern "C" fn native_subr_0544(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(544, &[arg_0])
}

extern "C" fn native_subr_0545(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(545, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0546(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(546, &[arg_0, arg_1])
}

extern "C" fn native_subr_0547(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(547, &[arg_0, arg_1])
}

extern "C" fn native_subr_0548(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(548, &[arg_0])
}

extern "C" fn native_subr_0549() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(549, &[])
}

extern "C" fn native_subr_0550(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(550, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0551(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(551, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0552(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(552, &[arg_0])
}

extern "C" fn native_subr_0553(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(553, &[arg_0])
}

extern "C" fn native_subr_0554(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(554, &[arg_0, arg_1])
}

extern "C" fn native_subr_0555(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(555, &[arg_0, arg_1])
}

extern "C" fn native_subr_0556(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(556, &[arg_0, arg_1])
}

extern "C" fn native_subr_0557(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(557, &[arg_0, arg_1])
}

extern "C" fn native_subr_0558(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(558, &[arg_0, arg_1])
}

extern "C" fn native_subr_0559(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(559, &[arg_0, arg_1])
}

extern "C" fn native_subr_0560(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(560, &[arg_0, arg_1])
}

extern "C" fn native_subr_0561(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(561, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0562(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(562, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0563(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(563, &[arg_0, arg_1])
}

extern "C" fn native_subr_0564(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(564, &[arg_0])
}

extern "C" fn native_subr_0565(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(565, &[arg_0])
}

extern "C" fn native_subr_0566(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(566, &[arg_0, arg_1])
}

extern "C" fn native_subr_0567(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(567, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0568(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(568, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_0569(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(569, nargs, args) }
}

extern "C" fn native_subr_0570(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(570, &[arg_0, arg_1, arg_2, arg_3])
}

unsafe extern "C" fn native_subr_0571(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(571, nargs, args) }
}

unsafe extern "C" fn native_subr_0572(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(572, nargs, args) }
}

unsafe extern "C" fn native_subr_0573(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(573, nargs, args) }
}

unsafe extern "C" fn native_subr_0574(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(574, nargs, args) }
}

extern "C" fn native_subr_0575() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(575, &[])
}

extern "C" fn native_subr_0576(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(576, &[arg_0, arg_1])
}

extern "C" fn native_subr_0577(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(577, &[arg_0])
}

extern "C" fn native_subr_0578(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(578, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0579(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(579, &[arg_0])
}

extern "C" fn native_subr_0580(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(580, &[arg_0, arg_1])
}

extern "C" fn native_subr_0581(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(581, &[arg_0, arg_1])
}

extern "C" fn native_subr_0582(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(582, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0583(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(583, &[arg_0])
}

extern "C" fn native_subr_0584(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(584, &[arg_0, arg_1])
}

extern "C" fn native_subr_0585(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(585, &[arg_0])
}

extern "C" fn native_subr_0586(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(586, &[arg_0, arg_1])
}

extern "C" fn native_subr_0587(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(587, &[arg_0])
}

extern "C" fn native_subr_0588(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(588, &[arg_0, arg_1])
}

extern "C" fn native_subr_0589(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(589, &[arg_0])
}

extern "C" fn native_subr_0590(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(590, &[arg_0])
}

extern "C" fn native_subr_0591(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(591, &[arg_0, arg_1])
}

extern "C" fn native_subr_0592(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(592, &[arg_0])
}

extern "C" fn native_subr_0593(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(593, &[arg_0, arg_1])
}

extern "C" fn native_subr_0594(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(594, &[arg_0])
}

extern "C" fn native_subr_0595(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(595, &[arg_0])
}

extern "C" fn native_subr_0596(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(596, &[arg_0])
}

extern "C" fn native_subr_0597(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(597, &[arg_0])
}

extern "C" fn native_subr_0598(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(598, &[arg_0])
}

extern "C" fn native_subr_0599(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(599, &[arg_0])
}

extern "C" fn native_subr_0600(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(600, &[arg_0])
}

extern "C" fn native_subr_0601(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
    arg_7: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(
        601,
        &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6, arg_7],
    )
}

extern "C" fn native_subr_0602(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(602, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0603(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(603, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0604(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(604, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0605(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(605, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0606() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(606, &[])
}

extern "C" fn native_subr_0607() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(607, &[])
}

extern "C" fn native_subr_0608() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(608, &[])
}

extern "C" fn native_subr_0609() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(609, &[])
}

extern "C" fn native_subr_0610(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(610, &[arg_0])
}

extern "C" fn native_subr_0611(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(611, &[arg_0])
}

extern "C" fn native_subr_0612(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(612, &[arg_0, arg_1])
}

extern "C" fn native_subr_0613() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(613, &[])
}

extern "C" fn native_subr_0614() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(614, &[])
}

extern "C" fn native_subr_0615(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(615, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0616(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(616, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0617(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(617, &[arg_0, arg_1])
}

extern "C" fn native_subr_0618(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(618, &[arg_0, arg_1])
}

extern "C" fn native_subr_0619(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(619, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0620(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(620, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_0621(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(621, &[arg_0])
}

extern "C" fn native_subr_0622() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(622, &[])
}

extern "C" fn native_subr_0623(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(623, &[arg_0, arg_1])
}

extern "C" fn native_subr_0624(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(624, &[arg_0])
}

extern "C" fn native_subr_0625(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(625, &[arg_0, arg_1])
}

extern "C" fn native_subr_0626(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(626, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0627(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(627, &[arg_0])
}

extern "C" fn native_subr_0628(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(628, &[arg_0])
}

extern "C" fn native_subr_0629(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(629, &[arg_0])
}

extern "C" fn native_subr_0630(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(630, &[arg_0])
}

extern "C" fn native_subr_0631() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(631, &[])
}

extern "C" fn native_subr_0632(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(632, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0633(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(633, &[arg_0, arg_1])
}

extern "C" fn native_subr_0634(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(634, &[arg_0, arg_1])
}

extern "C" fn native_subr_0635(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(635, &[arg_0, arg_1])
}

extern "C" fn native_subr_0636() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(636, &[])
}

extern "C" fn native_subr_0637(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(637, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_0638(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(638, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0639(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(639, &[arg_0])
}

extern "C" fn native_subr_0640(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(640, &[arg_0, arg_1])
}

extern "C" fn native_subr_0641() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(641, &[])
}

extern "C" fn native_subr_0642(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(642, &[arg_0, arg_1])
}

extern "C" fn native_subr_0643() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(643, &[])
}

extern "C" fn native_subr_0644(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(644, &[arg_0])
}

extern "C" fn native_subr_0645() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(645, &[])
}

extern "C" fn native_subr_0646(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(646, &[arg_0])
}

extern "C" fn native_subr_0647(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(647, &[arg_0])
}

extern "C" fn native_subr_0648(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(648, &[arg_0])
}

extern "C" fn native_subr_0649() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(649, &[])
}

extern "C" fn native_subr_0650() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(650, &[])
}

extern "C" fn native_subr_0651() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(651, &[])
}

extern "C" fn native_subr_0652() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(652, &[])
}

extern "C" fn native_subr_0653(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(653, &[arg_0, arg_1])
}

extern "C" fn native_subr_0654(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(654, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0655(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(655, &[args])
}

extern "C" fn native_subr_0656(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(656, &[arg_0])
}

extern "C" fn native_subr_0657(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(657, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0658(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(658, &[arg_0, arg_1])
}

extern "C" fn native_subr_0659() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(659, &[])
}

extern "C" fn native_subr_0660(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(660, &[arg_0, arg_1])
}

extern "C" fn native_subr_0661(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(661, &[arg_0, arg_1])
}

extern "C" fn native_subr_0662(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(662, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0663(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(663, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0664(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(664, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0665(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(665, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0666(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(666, &[arg_0, arg_1, arg_2])
}

unsafe extern "C" fn native_subr_0667(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(667, nargs, args) }
}

unsafe extern "C" fn native_subr_0668(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(668, nargs, args) }
}

extern "C" fn native_subr_0669() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(669, &[])
}

unsafe extern "C" fn native_subr_0670(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(670, nargs, args) }
}

unsafe extern "C" fn native_subr_0671(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(671, nargs, args) }
}

unsafe extern "C" fn native_subr_0672(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(672, nargs, args) }
}

extern "C" fn native_subr_0673() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(673, &[])
}

extern "C" fn native_subr_0674() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(674, &[])
}

extern "C" fn native_subr_0675(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(675, &[arg_0])
}

extern "C" fn native_subr_0676() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(676, &[])
}

extern "C" fn native_subr_0677() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(677, &[])
}

extern "C" fn native_subr_0678() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(678, &[])
}

extern "C" fn native_subr_0679() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(679, &[])
}

extern "C" fn native_subr_0680() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(680, &[])
}

extern "C" fn native_subr_0681(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(681, &[arg_0])
}

extern "C" fn native_subr_0682(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(682, &[arg_0])
}

extern "C" fn native_subr_0683(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(683, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0684(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(684, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0685(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(685, &[arg_0, arg_1, arg_2])
}

unsafe extern "C" fn native_subr_0686(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(686, nargs, args) }
}

unsafe extern "C" fn native_subr_0687(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(687, nargs, args) }
}

unsafe extern "C" fn native_subr_0688(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(688, nargs, args) }
}

unsafe extern "C" fn native_subr_0689(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(689, nargs, args) }
}

extern "C" fn native_subr_0690(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(690, &[arg_0])
}

extern "C" fn native_subr_0691(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(691, &[arg_0])
}

extern "C" fn native_subr_0692() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(692, &[])
}

extern "C" fn native_subr_0693() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(693, &[])
}

extern "C" fn native_subr_0694() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(694, &[])
}

extern "C" fn native_subr_0695() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(695, &[])
}

extern "C" fn native_subr_0696() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(696, &[])
}

extern "C" fn native_subr_0697() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(697, &[])
}

extern "C" fn native_subr_0698(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(698, &[arg_0])
}

extern "C" fn native_subr_0699(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(699, &[arg_0])
}

extern "C" fn native_subr_0700() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(700, &[])
}

extern "C" fn native_subr_0701() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(701, &[])
}

extern "C" fn native_subr_0702() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(702, &[])
}

extern "C" fn native_subr_0703() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(703, &[])
}

extern "C" fn native_subr_0704() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(704, &[])
}

extern "C" fn native_subr_0705() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(705, &[])
}

extern "C" fn native_subr_0706(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(706, &[arg_0])
}

extern "C" fn native_subr_0707(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(707, &[args])
}

extern "C" fn native_subr_0708(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(708, &[args])
}

extern "C" fn native_subr_0709(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(709, &[arg_0])
}

extern "C" fn native_subr_0710(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(710, &[arg_0])
}

extern "C" fn native_subr_0711(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(711, &[arg_0])
}

extern "C" fn native_subr_0712(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(712, &[arg_0])
}

extern "C" fn native_subr_0713(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(713, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0714(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(714, &[arg_0])
}

extern "C" fn native_subr_0715(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(715, &[arg_0])
}

extern "C" fn native_subr_0716(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(716, &[arg_0])
}

extern "C" fn native_subr_0717(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(717, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0718(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(718, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0719() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(719, &[])
}

extern "C" fn native_subr_0720() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(720, &[])
}

extern "C" fn native_subr_0721() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(721, &[])
}

extern "C" fn native_subr_0722() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(722, &[])
}

extern "C" fn native_subr_0723() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(723, &[])
}

extern "C" fn native_subr_0724(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(724, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0725() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(725, &[])
}

extern "C" fn native_subr_0726(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(726, &[arg_0, arg_1])
}

extern "C" fn native_subr_0727(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(727, &[arg_0, arg_1])
}

extern "C" fn native_subr_0728(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(728, &[arg_0])
}

extern "C" fn native_subr_0729(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(729, &[arg_0])
}

extern "C" fn native_subr_0730(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(730, &[arg_0])
}

extern "C" fn native_subr_0731(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(731, &[arg_0])
}

extern "C" fn native_subr_0732(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(732, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_0733(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(733, nargs, args) }
}

extern "C" fn native_subr_0734() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(734, &[])
}

extern "C" fn native_subr_0735(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(735, &[arg_0])
}

extern "C" fn native_subr_0736(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(736, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0737(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(737, &[arg_0])
}

extern "C" fn native_subr_0738(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(738, &[arg_0, arg_1])
}

extern "C" fn native_subr_0739(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(739, &[arg_0])
}

extern "C" fn native_subr_0740(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(740, &[arg_0])
}

extern "C" fn native_subr_0741(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(741, &[arg_0, arg_1])
}

extern "C" fn native_subr_0742(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(742, &[arg_0, arg_1])
}

extern "C" fn native_subr_0743(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(743, &[arg_0, arg_1])
}

extern "C" fn native_subr_0744(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(744, &[arg_0])
}

extern "C" fn native_subr_0745(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(745, &[arg_0])
}

extern "C" fn native_subr_0746(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(746, &[arg_0])
}

extern "C" fn native_subr_0747(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(747, &[arg_0])
}

extern "C" fn native_subr_0748(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(748, &[arg_0, arg_1])
}

extern "C" fn native_subr_0749() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(749, &[])
}

extern "C" fn native_subr_0750(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(750, &[arg_0])
}

extern "C" fn native_subr_0751() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(751, &[])
}

extern "C" fn native_subr_0752() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(752, &[])
}

extern "C" fn native_subr_0753(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(753, &[arg_0, arg_1])
}

extern "C" fn native_subr_0754(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(754, &[arg_0, arg_1])
}

extern "C" fn native_subr_0755(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(755, &[arg_0, arg_1])
}

extern "C" fn native_subr_0756(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(756, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0757(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(757, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_0758(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(758, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0759(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(759, &[arg_0, arg_1])
}

extern "C" fn native_subr_0760(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(760, &[arg_0, arg_1])
}

extern "C" fn native_subr_0761(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(761, &[arg_0])
}

extern "C" fn native_subr_0762(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(762, &[arg_0])
}

extern "C" fn native_subr_0763(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(763, &[arg_0])
}

extern "C" fn native_subr_0764(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(764, &[arg_0])
}

extern "C" fn native_subr_0765(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(765, &[arg_0])
}

extern "C" fn native_subr_0766(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(766, &[arg_0, arg_1])
}

extern "C" fn native_subr_0767(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(767, &[arg_0])
}

unsafe extern "C" fn native_subr_0768(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(768, nargs, args) }
}

unsafe extern "C" fn native_subr_0769(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(769, nargs, args) }
}

extern "C" fn native_subr_0770(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(770, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0771(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(771, &[arg_0])
}

extern "C" fn native_subr_0772(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(772, &[arg_0])
}

extern "C" fn native_subr_0773(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(773, &[arg_0])
}

extern "C" fn native_subr_0774(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(774, &[arg_0, arg_1])
}

extern "C" fn native_subr_0775(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(775, &[arg_0])
}

extern "C" fn native_subr_0776(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(776, &[arg_0, arg_1])
}

extern "C" fn native_subr_0777(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(777, &[arg_0, arg_1])
}

extern "C" fn native_subr_0778(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(778, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0779(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(779, &[arg_0, arg_1])
}

extern "C" fn native_subr_0780(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(780, &[arg_0])
}

extern "C" fn native_subr_0781(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(781, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0782(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(782, &[arg_0])
}

extern "C" fn native_subr_0783(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(783, &[arg_0])
}

extern "C" fn native_subr_0784(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(784, &[arg_0])
}

extern "C" fn native_subr_0785() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(785, &[])
}

extern "C" fn native_subr_0786(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(786, &[arg_0])
}

extern "C" fn native_subr_0787() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(787, &[])
}

extern "C" fn native_subr_0788() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(788, &[])
}

extern "C" fn native_subr_0789(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(789, &[arg_0])
}

extern "C" fn native_subr_0790(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(790, &[arg_0])
}

extern "C" fn native_subr_0791(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(791, &[arg_0, arg_1])
}

extern "C" fn native_subr_0792(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(792, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0793(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(793, &[arg_0])
}

extern "C" fn native_subr_0794(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(794, &[arg_0])
}

extern "C" fn native_subr_0795(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(795, &[arg_0])
}

extern "C" fn native_subr_0796() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(796, &[])
}

extern "C" fn native_subr_0797() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(797, &[])
}

extern "C" fn native_subr_0798(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(798, &[arg_0])
}

extern "C" fn native_subr_0799(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(799, &[arg_0])
}

extern "C" fn native_subr_0800(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(800, &[arg_0])
}

extern "C" fn native_subr_0801(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(801, &[arg_0])
}

extern "C" fn native_subr_0802(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(802, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0803(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(803, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0804(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(804, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0805(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(805, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0806(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(806, &[arg_0])
}

extern "C" fn native_subr_0807(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(807, &[arg_0])
}

extern "C" fn native_subr_0808(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(808, &[arg_0])
}

extern "C" fn native_subr_0809(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(809, &[arg_0])
}

extern "C" fn native_subr_0810(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(810, &[arg_0])
}

unsafe extern "C" fn native_subr_0811(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(811, nargs, args) }
}

extern "C" fn native_subr_0812(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(812, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0813(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(813, &[args])
}

extern "C" fn native_subr_0814() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(814, &[])
}

extern "C" fn native_subr_0815(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(815, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0816(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(816, &[arg_0])
}

extern "C" fn native_subr_0817(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(817, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0818(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(818, &[arg_0, arg_1])
}

extern "C" fn native_subr_0819() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(819, &[])
}

extern "C" fn native_subr_0820(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(820, &[arg_0])
}

extern "C" fn native_subr_0821(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(821, &[arg_0])
}

extern "C" fn native_subr_0822(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(822, &[arg_0])
}

extern "C" fn native_subr_0823(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(823, &[arg_0, arg_1])
}

extern "C" fn native_subr_0824(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(824, &[arg_0, arg_1])
}

extern "C" fn native_subr_0825(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(825, &[arg_0])
}

extern "C" fn native_subr_0826(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(826, &[arg_0])
}

extern "C" fn native_subr_0827(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(827, &[arg_0])
}

extern "C" fn native_subr_0828(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(828, &[arg_0])
}

extern "C" fn native_subr_0829(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(829, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0830(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(830, &[arg_0])
}

extern "C" fn native_subr_0831(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(831, &[arg_0])
}

extern "C" fn native_subr_0832(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(832, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0833(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(833, &[arg_0])
}

extern "C" fn native_subr_0834(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(834, &[arg_0])
}

extern "C" fn native_subr_0835(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(835, &[arg_0])
}

extern "C" fn native_subr_0836(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(836, &[arg_0])
}

extern "C" fn native_subr_0837() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(837, &[])
}

extern "C" fn native_subr_0838(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(838, &[arg_0])
}

extern "C" fn native_subr_0839(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(839, &[arg_0])
}

extern "C" fn native_subr_0840() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(840, &[])
}

extern "C" fn native_subr_0841(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(841, &[arg_0])
}

extern "C" fn native_subr_0842(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(842, &[arg_0])
}

extern "C" fn native_subr_0843(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(843, &[arg_0])
}

extern "C" fn native_subr_0844(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(844, &[arg_0])
}

extern "C" fn native_subr_0845(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(845, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0846(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(846, &[arg_0, arg_1])
}

extern "C" fn native_subr_0847(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(847, &[arg_0])
}

extern "C" fn native_subr_0848(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(848, &[arg_0, arg_1])
}

extern "C" fn native_subr_0849(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(849, &[arg_0])
}

extern "C" fn native_subr_0850(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(850, &[arg_0])
}

extern "C" fn native_subr_0851(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(851, &[arg_0])
}

extern "C" fn native_subr_0852(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(852, &[arg_0])
}

extern "C" fn native_subr_0853(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(853, &[arg_0])
}

extern "C" fn native_subr_0854(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(854, &[arg_0, arg_1])
}

extern "C" fn native_subr_0855(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(855, &[arg_0])
}

extern "C" fn native_subr_0856(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(856, &[arg_0])
}

extern "C" fn native_subr_0857(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(857, &[arg_0])
}

extern "C" fn native_subr_0858(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(858, &[arg_0])
}

extern "C" fn native_subr_0859(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(859, &[arg_0, arg_1])
}

extern "C" fn native_subr_0860(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(860, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0861(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(861, &[arg_0, arg_1])
}

extern "C" fn native_subr_0862(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(862, &[arg_0, arg_1])
}

extern "C" fn native_subr_0863(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(863, &[arg_0])
}

extern "C" fn native_subr_0864(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(864, &[arg_0])
}

extern "C" fn native_subr_0865(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(865, &[arg_0])
}

extern "C" fn native_subr_0866(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(866, &[arg_0])
}

extern "C" fn native_subr_0867(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(867, &[arg_0])
}

extern "C" fn native_subr_0868(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(868, &[arg_0, arg_1])
}

extern "C" fn native_subr_0869(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(869, &[arg_0, arg_1])
}

extern "C" fn native_subr_0870(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(870, &[arg_0, arg_1])
}

extern "C" fn native_subr_0871(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(871, &[arg_0, arg_1])
}

extern "C" fn native_subr_0872(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(872, &[arg_0])
}

extern "C" fn native_subr_0873(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(873, &[arg_0])
}

extern "C" fn native_subr_0874(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(874, &[arg_0])
}

extern "C" fn native_subr_0875(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(875, &[arg_0])
}

extern "C" fn native_subr_0876(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(876, &[arg_0, arg_1])
}

extern "C" fn native_subr_0877(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(877, &[arg_0, arg_1])
}

extern "C" fn native_subr_0878(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(878, &[arg_0])
}

extern "C" fn native_subr_0879(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(879, &[arg_0])
}

extern "C" fn native_subr_0880(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(880, &[arg_0])
}

extern "C" fn native_subr_0881(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(881, &[arg_0])
}

extern "C" fn native_subr_0882(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(882, &[arg_0])
}

extern "C" fn native_subr_0883(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(883, &[arg_0, arg_1])
}

extern "C" fn native_subr_0884(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(884, &[arg_0])
}

extern "C" fn native_subr_0885(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(885, &[arg_0, arg_1])
}

extern "C" fn native_subr_0886(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(886, &[arg_0])
}

extern "C" fn native_subr_0887(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(887, &[arg_0])
}

extern "C" fn native_subr_0888(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(888, &[arg_0])
}

extern "C" fn native_subr_0889(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(889, &[arg_0])
}

extern "C" fn native_subr_0890(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(890, &[arg_0, arg_1])
}

extern "C" fn native_subr_0891(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(891, &[arg_0])
}

extern "C" fn native_subr_0892(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(892, &[arg_0])
}

extern "C" fn native_subr_0893(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(893, &[arg_0])
}

extern "C" fn native_subr_0894(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(894, &[arg_0])
}

extern "C" fn native_subr_0895(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(895, &[arg_0, arg_1])
}

extern "C" fn native_subr_0896(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(896, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0897(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(897, &[arg_0])
}

extern "C" fn native_subr_0898(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(898, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0899(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(899, &[arg_0, arg_1])
}

extern "C" fn native_subr_0900(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(900, &[arg_0, arg_1, arg_2])
}

unsafe extern "C" fn native_subr_0901(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(901, nargs, args) }
}

unsafe extern "C" fn native_subr_0902(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(902, nargs, args) }
}

unsafe extern "C" fn native_subr_0903(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(903, nargs, args) }
}

unsafe extern "C" fn native_subr_0904(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(904, nargs, args) }
}

unsafe extern "C" fn native_subr_0905(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(905, nargs, args) }
}

extern "C" fn native_subr_0906(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(906, &[arg_0])
}

unsafe extern "C" fn native_subr_0907(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(907, nargs, args) }
}

unsafe extern "C" fn native_subr_0908(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(908, nargs, args) }
}

extern "C" fn native_subr_0909(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(909, &[arg_0, arg_1])
}

extern "C" fn native_subr_0910(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(910, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0911(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(911, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0912(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(912, &[arg_0, arg_1])
}

extern "C" fn native_subr_0913(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(913, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_0914(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(914, nargs, args) }
}

extern "C" fn native_subr_0915(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(915, &[args])
}

extern "C" fn native_subr_0916(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(916, &[args])
}

extern "C" fn native_subr_0917(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(917, &[arg_0, arg_1])
}

extern "C" fn native_subr_0918(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(918, &[args])
}

extern "C" fn native_subr_0919(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(919, &[arg_0, arg_1])
}

extern "C" fn native_subr_0920(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(920, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0921(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(921, &[args])
}

extern "C" fn native_subr_0922(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(922, &[args])
}

extern "C" fn native_subr_0923(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(923, &[args])
}

extern "C" fn native_subr_0924(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(924, &[arg_0])
}

extern "C" fn native_subr_0925(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(925, &[arg_0, arg_1])
}

extern "C" fn native_subr_0926(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(926, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0927(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(927, &[args])
}

extern "C" fn native_subr_0928(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(928, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0929(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(929, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0930(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(930, &[args])
}

extern "C" fn native_subr_0931(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(931, &[arg_0, arg_1])
}

extern "C" fn native_subr_0932(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(932, &[arg_0])
}

extern "C" fn native_subr_0933(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(933, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0934(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(934, &[args])
}

extern "C" fn native_subr_0935(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(935, &[args])
}

extern "C" fn native_subr_0936(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(936, &[args])
}

extern "C" fn native_subr_0937(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(937, &[args])
}

extern "C" fn native_subr_0938(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(938, &[args])
}

extern "C" fn native_subr_0939(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(939, &[args])
}

extern "C" fn native_subr_0940(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(940, &[args])
}

extern "C" fn native_subr_0941(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(941, &[args])
}

extern "C" fn native_subr_0942(args: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(942, &[args])
}

extern "C" fn native_subr_0943() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(943, &[])
}

extern "C" fn native_subr_0944(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(944, &[arg_0])
}

extern "C" fn native_subr_0945(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(945, &[arg_0, arg_1])
}

extern "C" fn native_subr_0946(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(946, &[arg_0, arg_1])
}

extern "C" fn native_subr_0947(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(947, &[arg_0, arg_1])
}

extern "C" fn native_subr_0948(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(948, &[arg_0, arg_1])
}

extern "C" fn native_subr_0949(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(949, &[arg_0, arg_1])
}

extern "C" fn native_subr_0950(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(950, &[arg_0])
}

extern "C" fn native_subr_0951(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(951, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0952(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(952, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0953(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(953, &[arg_0])
}

extern "C" fn native_subr_0954(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(954, &[arg_0])
}

extern "C" fn native_subr_0955(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(955, &[arg_0])
}

extern "C" fn native_subr_0956(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(956, &[arg_0])
}

extern "C" fn native_subr_0957(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(957, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0958(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(958, &[arg_0, arg_1])
}

extern "C" fn native_subr_0959(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(959, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0960(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(960, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0961(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(961, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0962(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(962, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_0963(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(963, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0964(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(964, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_0965() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(965, &[])
}

extern "C" fn native_subr_0966(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(966, &[arg_0, arg_1])
}

extern "C" fn native_subr_0967(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(967, &[arg_0, arg_1])
}

extern "C" fn native_subr_0968(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(968, &[arg_0, arg_1])
}

extern "C" fn native_subr_0969(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(969, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0970(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(970, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0971(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(971, &[arg_0])
}

extern "C" fn native_subr_0972(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(972, &[arg_0])
}

extern "C" fn native_subr_0973(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(973, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0974(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(974, &[arg_0, arg_1])
}

extern "C" fn native_subr_0975(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(975, &[arg_0])
}

extern "C" fn native_subr_0976(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(976, &[arg_0, arg_1])
}

extern "C" fn native_subr_0977(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(977, &[arg_0, arg_1])
}

extern "C" fn native_subr_0978(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(978, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0979(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(979, &[arg_0, arg_1])
}

extern "C" fn native_subr_0980(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(980, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_0981(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(981, &[arg_0, arg_1])
}

extern "C" fn native_subr_0982(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(982, &[arg_0, arg_1])
}

extern "C" fn native_subr_0983(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(983, &[arg_0])
}

extern "C" fn native_subr_0984(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(984, &[arg_0])
}

extern "C" fn native_subr_0985(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(985, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_0986(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(986, nargs, args) }
}

extern "C" fn native_subr_0987(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(987, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_0988(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(988, nargs, args) }
}

extern "C" fn native_subr_0989() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(989, &[])
}

extern "C" fn native_subr_0990(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(990, &[arg_0, arg_1])
}

extern "C" fn native_subr_0991(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(991, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_0992(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(992, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_0993(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(993, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_0994() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(994, &[])
}

extern "C" fn native_subr_0995(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(995, &[arg_0])
}

extern "C" fn native_subr_0996() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(996, &[])
}

extern "C" fn native_subr_0997() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(997, &[])
}

extern "C" fn native_subr_0998(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(998, &[arg_0, arg_1])
}

extern "C" fn native_subr_0999() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(999, &[])
}

extern "C" fn native_subr_1000() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1000, &[])
}

extern "C" fn native_subr_1001(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1001, &[arg_0, arg_1])
}

extern "C" fn native_subr_1002(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1002, &[arg_0])
}

extern "C" fn native_subr_1003(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1003, &[arg_0])
}

extern "C" fn native_subr_1004(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1004, &[arg_0, arg_1])
}

extern "C" fn native_subr_1005(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1005, &[arg_0])
}

extern "C" fn native_subr_1006(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1006, &[arg_0])
}

extern "C" fn native_subr_1007(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1007, &[arg_0])
}

extern "C" fn native_subr_1008(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1008, &[arg_0])
}

extern "C" fn native_subr_1009(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1009, &[arg_0])
}

extern "C" fn native_subr_1010(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1010, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1011(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1011, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_1012(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1012, nargs, args) }
}

unsafe extern "C" fn native_subr_1013(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1013, nargs, args) }
}

unsafe extern "C" fn native_subr_1014(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1014, nargs, args) }
}

extern "C" fn native_subr_1015(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1015, &[arg_0])
}

extern "C" fn native_subr_1016(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1016, &[arg_0, arg_1])
}

extern "C" fn native_subr_1017(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1017, &[arg_0])
}

extern "C" fn native_subr_1018(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1018, &[arg_0])
}

extern "C" fn native_subr_1019(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1019, &[arg_0, arg_1])
}

extern "C" fn native_subr_1020(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1020, &[arg_0])
}

extern "C" fn native_subr_1021(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1021, &[arg_0])
}

extern "C" fn native_subr_1022(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1022, &[arg_0])
}

extern "C" fn native_subr_1023(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1023, &[arg_0])
}

extern "C" fn native_subr_1024(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1024, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1025(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1025, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1026(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1026, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1027(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1027, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1028(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1028, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1029(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1029, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_1030(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1030, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1031(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1031, &[arg_0, arg_1])
}

extern "C" fn native_subr_1032(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1032, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1033(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1033, &[arg_0])
}

extern "C" fn native_subr_1034(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1034, &[arg_0])
}

extern "C" fn native_subr_1035(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1035, &[arg_0, arg_1])
}

extern "C" fn native_subr_1036(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1036, &[arg_0])
}

extern "C" fn native_subr_1037(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1037, &[arg_0])
}

extern "C" fn native_subr_1038(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1038, &[arg_0])
}

unsafe extern "C" fn native_subr_1039(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1039, nargs, args) }
}

extern "C" fn native_subr_1040(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1040, &[arg_0])
}

extern "C" fn native_subr_1041() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1041, &[])
}

extern "C" fn native_subr_1042(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1042, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1043(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1043, &[arg_0])
}

extern "C" fn native_subr_1044(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1044, &[arg_0, arg_1])
}

extern "C" fn native_subr_1045(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1045, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_1046(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1046, &[arg_0])
}

extern "C" fn native_subr_1047(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1047, &[arg_0, arg_1])
}

extern "C" fn native_subr_1048(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1048, &[arg_0, arg_1])
}

extern "C" fn native_subr_1049(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1049, &[arg_0, arg_1])
}

extern "C" fn native_subr_1050(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1050, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1051(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1051, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1052(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1052, &[arg_0, arg_1])
}

extern "C" fn native_subr_1053(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1053, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1054(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1054, &[arg_0, arg_1])
}

extern "C" fn native_subr_1055(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1055, &[arg_0])
}

extern "C" fn native_subr_1056(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1056, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_1057(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1057, nargs, args) }
}

extern "C" fn native_subr_1058(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1058, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_1059(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1059, &[arg_0])
}

extern "C" fn native_subr_1060(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1060, &[arg_0])
}

extern "C" fn native_subr_1061(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1061, &[arg_0])
}

extern "C" fn native_subr_1062() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1062, &[])
}

extern "C" fn native_subr_1063() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1063, &[])
}

extern "C" fn native_subr_1064() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1064, &[])
}

extern "C" fn native_subr_1065(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1065, &[arg_0])
}

extern "C" fn native_subr_1066() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1066, &[])
}

extern "C" fn native_subr_1067(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1067, &[arg_0])
}

extern "C" fn native_subr_1068(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1068, &[arg_0])
}

extern "C" fn native_subr_1069() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1069, &[])
}

extern "C" fn native_subr_1070(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1070, &[arg_0])
}

extern "C" fn native_subr_1071(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1071, &[arg_0, arg_1])
}

extern "C" fn native_subr_1072(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1072, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1073(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1073, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1074(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1074, &[arg_0, arg_1])
}

extern "C" fn native_subr_1075(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1075, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_1076(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1076, nargs, args) }
}

unsafe extern "C" fn native_subr_1077(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1077, nargs, args) }
}

unsafe extern "C" fn native_subr_1078(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1078, nargs, args) }
}

unsafe extern "C" fn native_subr_1079(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1079, nargs, args) }
}

unsafe extern "C" fn native_subr_1080(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1080, nargs, args) }
}

unsafe extern "C" fn native_subr_1081(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1081, nargs, args) }
}

extern "C" fn native_subr_1082(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_cons(arg_0, arg_1)
}

extern "C" fn native_subr_1083() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1083, &[])
}

extern "C" fn native_subr_1084(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1084, &[arg_0])
}

extern "C" fn native_subr_1085(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1085, &[arg_0, arg_1])
}

extern "C" fn native_subr_1086() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1086, &[])
}

extern "C" fn native_subr_1087() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1087, &[])
}

extern "C" fn native_subr_1088() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1088, &[])
}

extern "C" fn native_subr_1089() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1089, &[])
}

extern "C" fn native_subr_1090(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1090, &[arg_0, arg_1])
}

extern "C" fn native_subr_1091(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1091, &[arg_0])
}

extern "C" fn native_subr_1092() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1092, &[])
}

extern "C" fn native_subr_1093(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1093, &[arg_0])
}

extern "C" fn native_subr_1094(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1094, &[arg_0, arg_1])
}

extern "C" fn native_subr_1095(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1095, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_1096(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1096, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_1097(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1097, &[arg_0, arg_1])
}

extern "C" fn native_subr_1098() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1098, &[])
}

extern "C" fn native_subr_1099(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1099, &[arg_0])
}

extern "C" fn native_subr_1100(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1100, &[arg_0, arg_1])
}

extern "C" fn native_subr_1101(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1101, &[arg_0, arg_1])
}

extern "C" fn native_subr_1102(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1102, &[arg_0])
}

extern "C" fn native_subr_1103(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1103, &[arg_0])
}

extern "C" fn native_subr_1104(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1104, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1105(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1105, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1106(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1106, &[arg_0, arg_1])
}

extern "C" fn native_subr_1107(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1107, &[arg_0])
}

extern "C" fn native_subr_1108(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1108, &[arg_0])
}

extern "C" fn native_subr_1109(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1109, &[arg_0])
}

extern "C" fn native_subr_1110(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1110, &[arg_0])
}

extern "C" fn native_subr_1111(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1111, &[arg_0, arg_1])
}

extern "C" fn native_subr_1112(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1112, &[arg_0])
}

extern "C" fn native_subr_1113(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1113, &[arg_0])
}

extern "C" fn native_subr_1114(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1114, &[arg_0])
}

extern "C" fn native_subr_1115(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1115, &[arg_0])
}

extern "C" fn native_subr_1116(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1116, &[arg_0])
}

extern "C" fn native_subr_1117(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1117, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1118(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1118, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1119(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1119, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1120(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1120, &[arg_0])
}

extern "C" fn native_subr_1121(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1121, &[arg_0])
}

extern "C" fn native_subr_1122(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1122, &[arg_0])
}

extern "C" fn native_subr_1123(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1123, &[arg_0])
}

extern "C" fn native_subr_1124(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1124, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_1125(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1125, &[arg_0])
}

extern "C" fn native_subr_1126(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1126, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_1127(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1127, nargs, args) }
}

extern "C" fn native_subr_1128(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1128, &[arg_0])
}

extern "C" fn native_subr_1129(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1129, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1130(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1130, &[arg_0])
}

extern "C" fn native_subr_1131(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1131, &[arg_0])
}

extern "C" fn native_subr_1132(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1132, &[arg_0])
}

extern "C" fn native_subr_1133(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1133, &[arg_0])
}

extern "C" fn native_subr_1134(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1134, &[arg_0])
}

extern "C" fn native_subr_1135(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1135, &[arg_0])
}

extern "C" fn native_subr_1136(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1136, &[arg_0, arg_1])
}

extern "C" fn native_subr_1137(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1137, &[arg_0])
}

extern "C" fn native_subr_1138(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1138, &[arg_0])
}

extern "C" fn native_subr_1139(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1139, &[arg_0])
}

extern "C" fn native_subr_1140(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1140, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_1141() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1141, &[])
}

extern "C" fn native_subr_1142(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1142, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_1143(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1143, &[arg_0, arg_1])
}

extern "C" fn native_subr_1144(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1144, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1145(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1145, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1146(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1146, &[arg_0, arg_1])
}

extern "C" fn native_subr_1147(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1147, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1148(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1148, &[arg_0, arg_1, arg_2])
}

unsafe extern "C" fn native_subr_1149(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1149, nargs, args) }
}

extern "C" fn native_subr_1150(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1150, &[arg_0, arg_1])
}

extern "C" fn native_subr_1151(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1151, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1152(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1152, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1153(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1153, &[arg_0, arg_1])
}

extern "C" fn native_subr_1154(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1154, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1155(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1155, &[arg_0, arg_1])
}

extern "C" fn native_subr_1156(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1156, &[arg_0])
}

extern "C" fn native_subr_1157(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1157, &[arg_0])
}

extern "C" fn native_subr_1158(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1158, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1159(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1159, &[arg_0, arg_1])
}

extern "C" fn native_subr_1160(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1160, &[arg_0, arg_1])
}

extern "C" fn native_subr_1161(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1161, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_1162(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1162, nargs, args) }
}

extern "C" fn native_subr_1163(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1163, &[arg_0])
}

extern "C" fn native_subr_1164(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1164, &[arg_0, arg_1])
}

extern "C" fn native_subr_1165(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1165, &[arg_0, arg_1])
}

extern "C" fn native_subr_1166(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1166, &[arg_0, arg_1])
}

extern "C" fn native_subr_1167(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1167, &[arg_0, arg_1])
}

extern "C" fn native_subr_1168(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1168, &[arg_0, arg_1])
}

extern "C" fn native_subr_1169(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1169, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1170(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1170, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1171(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1171, &[arg_0, arg_1])
}

extern "C" fn native_subr_1172(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1172, &[arg_0, arg_1, arg_2])
}

unsafe extern "C" fn native_subr_1173(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1173, nargs, args) }
}

extern "C" fn native_subr_1174(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1174, &[arg_0])
}

extern "C" fn native_subr_1175(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1175, &[arg_0])
}

extern "C" fn native_subr_1176(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1176, &[arg_0, arg_1])
}

extern "C" fn native_subr_1177(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1177, &[arg_0, arg_1])
}

extern "C" fn native_subr_1178(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1178, &[arg_0, arg_1])
}

extern "C" fn native_subr_1179(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1179, &[arg_0, arg_1])
}

extern "C" fn native_subr_1180(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1180, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1181(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1181, &[arg_0, arg_1])
}

extern "C" fn native_subr_1182(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1182, &[arg_0, arg_1])
}

extern "C" fn native_subr_1183(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1183, &[arg_0, arg_1])
}

extern "C" fn native_subr_1184(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1184, &[arg_0, arg_1])
}

extern "C" fn native_subr_1185(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1185, &[arg_0, arg_1])
}

extern "C" fn native_subr_1186(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1186, &[arg_0, arg_1])
}

extern "C" fn native_subr_1187(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1187, &[arg_0, arg_1])
}

extern "C" fn native_subr_1188(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1188, &[arg_0, arg_1])
}

extern "C" fn native_subr_1189(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1189, &[arg_0, arg_1])
}

extern "C" fn native_subr_1190(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1190, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1191(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1191, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1192(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1192, &[arg_0])
}

extern "C" fn native_subr_1193(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1193, &[arg_0])
}

extern "C" fn native_subr_1194(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1194, &[arg_0])
}

extern "C" fn native_subr_1195(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1195, &[arg_0])
}

extern "C" fn native_subr_1196(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1196, &[arg_0])
}

extern "C" fn native_subr_1197(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1197, &[arg_0])
}

extern "C" fn native_subr_1198(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1198, &[arg_0])
}

extern "C" fn native_subr_1199(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1199, &[arg_0])
}

unsafe extern "C" fn native_subr_1200(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1200, nargs, args) }
}

unsafe extern "C" fn native_subr_1201(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1201, nargs, args) }
}

unsafe extern "C" fn native_subr_1202(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1202, nargs, args) }
}

extern "C" fn native_subr_1203(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1203, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1204(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1204, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1205(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1205, &[arg_0, arg_1])
}

extern "C" fn native_subr_1206(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1206, &[arg_0, arg_1])
}

extern "C" fn native_subr_1207(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1207, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_1208(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1208, &[arg_0, arg_1])
}

extern "C" fn native_subr_1209(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1209, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1210(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1210, &[arg_0])
}

extern "C" fn native_subr_1211(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1211, &[arg_0])
}

extern "C" fn native_subr_1212(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1212, &[arg_0, arg_1])
}

extern "C" fn native_subr_1213(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1213, &[arg_0, arg_1])
}

extern "C" fn native_subr_1214(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1214, &[arg_0, arg_1])
}

extern "C" fn native_subr_1215(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1215, &[arg_0])
}

extern "C" fn native_subr_1216(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1216, &[arg_0])
}

extern "C" fn native_subr_1217(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1217, &[arg_0])
}

extern "C" fn native_subr_1218(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1218, &[arg_0])
}

extern "C" fn native_subr_1219(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1219, &[arg_0, arg_1])
}

extern "C" fn native_subr_1220(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1220, &[arg_0])
}

extern "C" fn native_subr_1221(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1221, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1222(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1222, &[arg_0])
}

extern "C" fn native_subr_1223(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1223, &[arg_0])
}

extern "C" fn native_subr_1224(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1224, &[arg_0])
}

extern "C" fn native_subr_1225(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1225, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1226(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1226, &[arg_0, arg_1])
}

extern "C" fn native_subr_1227(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1227, &[arg_0, arg_1])
}

extern "C" fn native_subr_1228(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1228, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1229(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1229, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1230(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1230, &[arg_0])
}

extern "C" fn native_subr_1231(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1231, &[arg_0])
}

extern "C" fn native_subr_1232(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1232, &[arg_0])
}

extern "C" fn native_subr_1233(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1233, &[arg_0])
}

extern "C" fn native_subr_1234(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1234, &[arg_0])
}

extern "C" fn native_subr_1235(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1235, &[arg_0])
}

extern "C" fn native_subr_1236(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1236, &[arg_0])
}

extern "C" fn native_subr_1237(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1237, &[arg_0])
}

extern "C" fn native_subr_1238(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1238, &[arg_0])
}

unsafe extern "C" fn native_subr_1239(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1239, nargs, args) }
}

extern "C" fn native_subr_1240(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1240, &[arg_0])
}

extern "C" fn native_subr_1241(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1241, &[arg_0])
}

extern "C" fn native_subr_1242(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1242, &[arg_0])
}

extern "C" fn native_subr_1243(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1243, &[arg_0])
}

extern "C" fn native_subr_1244(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1244, &[arg_0])
}

extern "C" fn native_subr_1245(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1245, &[arg_0, arg_1])
}

extern "C" fn native_subr_1246(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1246, &[arg_0, arg_1])
}

extern "C" fn native_subr_1247(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1247, &[arg_0])
}

extern "C" fn native_subr_1248(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1248, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1249(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1249, &[arg_0, arg_1])
}

extern "C" fn native_subr_1250(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1250, &[arg_0, arg_1])
}

extern "C" fn native_subr_1251(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1251, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1252(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1252, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1253(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1253, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1254(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1254, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1255(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1255, &[arg_0])
}

extern "C" fn native_subr_1256(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1256, &[arg_0, arg_1])
}

extern "C" fn native_subr_1257(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1257, &[arg_0])
}

extern "C" fn native_subr_1258(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1258, &[arg_0])
}

extern "C" fn native_subr_1259(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1259, &[arg_0])
}

extern "C" fn native_subr_1260(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1260, &[arg_0])
}

extern "C" fn native_subr_1261(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1261, &[arg_0])
}

extern "C" fn native_subr_1262(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1262, &[arg_0])
}

extern "C" fn native_subr_1263(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1263, &[arg_0])
}

extern "C" fn native_subr_1264() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1264, &[])
}

extern "C" fn native_subr_1265(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1265, &[arg_0])
}

extern "C" fn native_subr_1266(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1266, &[arg_0])
}

extern "C" fn native_subr_1267(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1267, &[arg_0])
}

extern "C" fn native_subr_1268(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1268, &[arg_0, arg_1])
}

extern "C" fn native_subr_1269(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1269, &[arg_0])
}

unsafe extern "C" fn native_subr_1270(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1270, nargs, args) }
}

unsafe extern "C" fn native_subr_1271(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1271, nargs, args) }
}

unsafe extern "C" fn native_subr_1272(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1272, nargs, args) }
}

unsafe extern "C" fn native_subr_1273(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1273, nargs, args) }
}

unsafe extern "C" fn native_subr_1274(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1274, nargs, args) }
}

extern "C" fn native_subr_1275(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1275, &[arg_0, arg_1])
}

extern "C" fn native_subr_1276(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1276, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_1277(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1277, nargs, args) }
}

unsafe extern "C" fn native_subr_1278(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1278, nargs, args) }
}

unsafe extern "C" fn native_subr_1279(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1279, nargs, args) }
}

unsafe extern "C" fn native_subr_1280(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe { super::runtime::invoke_subr_many(1280, nargs, args) }
}

extern "C" fn native_subr_1281(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1281, &[arg_0, arg_1])
}

unsafe extern "C" fn native_subr_1282(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe {
        super::runtime::invoke_numeric_comparison(
            1282,
            super::runtime::FixnumComparison::GreaterOrEqual,
            nargs,
            args,
        )
    }
}

unsafe extern "C" fn native_subr_1283(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe {
        super::runtime::invoke_numeric_comparison(
            1283,
            super::runtime::FixnumComparison::LessOrEqual,
            nargs,
            args,
        )
    }
}

unsafe extern "C" fn native_subr_1284(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe {
        super::runtime::invoke_numeric_comparison(
            1284,
            super::runtime::FixnumComparison::Greater,
            nargs,
            args,
        )
    }
}

unsafe extern "C" fn native_subr_1285(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe {
        super::runtime::invoke_numeric_comparison(
            1285,
            super::runtime::FixnumComparison::Less,
            nargs,
            args,
        )
    }
}

unsafe extern "C" fn native_subr_1286(
    nargs: isize,
    args: *const super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    unsafe {
        super::runtime::invoke_numeric_comparison(
            1286,
            super::runtime::FixnumComparison::Equal,
            nargs,
            args,
        )
    }
}

extern "C" fn native_subr_1287(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1287, &[arg_0, arg_1])
}

extern "C" fn native_subr_1288(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1288, &[arg_0])
}

extern "C" fn native_subr_1289(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1289, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1290(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1290, &[arg_0, arg_1])
}

extern "C" fn native_subr_1291(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1291, &[arg_0])
}

extern "C" fn native_subr_1292(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1292, &[arg_0, arg_1])
}

extern "C" fn native_subr_1293(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1293, &[arg_0, arg_1])
}

extern "C" fn native_subr_1294(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1294, &[arg_0])
}

extern "C" fn native_subr_1295(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1295, &[arg_0])
}

extern "C" fn native_subr_1296(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1296, &[arg_0])
}

extern "C" fn native_subr_1297(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1297, &[arg_0, arg_1])
}

extern "C" fn native_subr_1298(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1298, &[arg_0])
}

extern "C" fn native_subr_1299(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1299, &[arg_0])
}

extern "C" fn native_subr_1300(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1300, &[arg_0, arg_1])
}

extern "C" fn native_subr_1301(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1301, &[arg_0])
}

extern "C" fn native_subr_1302(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1302, &[arg_0, arg_1])
}

extern "C" fn native_subr_1303(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1303, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1304(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1304, &[arg_0, arg_1])
}

extern "C" fn native_subr_1305(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1305, &[arg_0])
}

extern "C" fn native_subr_1306(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1306, &[arg_0])
}

extern "C" fn native_subr_1307(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1307, &[arg_0])
}

extern "C" fn native_subr_1308(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1308, &[arg_0])
}

extern "C" fn native_subr_1309(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1309, &[arg_0, arg_1])
}

extern "C" fn native_subr_1310(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1310, &[arg_0])
}

extern "C" fn native_subr_1311(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1311, &[arg_0])
}

extern "C" fn native_subr_1312(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1312, &[arg_0])
}

extern "C" fn native_subr_1313(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1313, &[arg_0])
}

extern "C" fn native_subr_1314(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1314, &[arg_0])
}

extern "C" fn native_subr_1315(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1315, &[arg_0, arg_1])
}

extern "C" fn native_subr_1316(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1316, &[arg_0])
}

extern "C" fn native_subr_1317(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1317, &[arg_0, arg_1])
}

extern "C" fn native_subr_1318(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1318, &[arg_0, arg_1])
}

extern "C" fn native_subr_1319(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1319, &[arg_0])
}

extern "C" fn native_subr_1320(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1320, &[arg_0])
}

extern "C" fn native_subr_1321(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1321, &[arg_0])
}

extern "C" fn native_subr_1322(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1322, &[arg_0])
}

extern "C" fn native_subr_1323(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1323, &[arg_0])
}

extern "C" fn native_subr_1324(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1324, &[arg_0])
}

extern "C" fn native_subr_1325(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1325, &[arg_0])
}

extern "C" fn native_subr_1326(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1326, &[arg_0])
}

extern "C" fn native_subr_1327(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1327, &[arg_0])
}

extern "C" fn native_subr_1328(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1328, &[arg_0])
}

extern "C" fn native_subr_1329(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1329, &[arg_0])
}

extern "C" fn native_subr_1330(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1330, &[arg_0])
}

extern "C" fn native_subr_1331(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1331, &[arg_0])
}

extern "C" fn native_subr_1332(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1332, &[arg_0])
}

extern "C" fn native_subr_1333(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1333, &[arg_0])
}

extern "C" fn native_subr_1334(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1334, &[arg_0])
}

extern "C" fn native_subr_1335(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1335, &[arg_0])
}

extern "C" fn native_subr_1336(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1336, &[arg_0])
}

extern "C" fn native_subr_1337(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1337, &[arg_0])
}

extern "C" fn native_subr_1338(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1338, &[arg_0])
}

extern "C" fn native_subr_1339(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1339, &[arg_0])
}

extern "C" fn native_subr_1340(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1340, &[arg_0])
}

extern "C" fn native_subr_1341(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1341, &[arg_0])
}

extern "C" fn native_subr_1342(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1342, &[arg_0])
}

extern "C" fn native_subr_1343(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1343, &[arg_0])
}

extern "C" fn native_subr_1344(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1344, &[arg_0])
}

extern "C" fn native_subr_1345(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1345, &[arg_0])
}

extern "C" fn native_subr_1346(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1346, &[arg_0])
}

extern "C" fn native_subr_1347(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1347, &[arg_0])
}

extern "C" fn native_subr_1348(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1348, &[arg_0])
}

extern "C" fn native_subr_1349(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1349, &[arg_0])
}

extern "C" fn native_subr_1350(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1350, &[arg_0])
}

extern "C" fn native_subr_1351(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1351, &[arg_0])
}

extern "C" fn native_subr_1352(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1352, &[arg_0])
}

extern "C" fn native_subr_1353(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1353, &[arg_0])
}

extern "C" fn native_subr_1354(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1354, &[arg_0])
}

extern "C" fn native_subr_1355(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1355, &[arg_0])
}

extern "C" fn native_subr_1356(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1356, &[arg_0])
}

extern "C" fn native_subr_1357(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1357, &[arg_0])
}

extern "C" fn native_subr_1358(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1358, &[arg_0])
}

extern "C" fn native_subr_1359(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1359, &[arg_0])
}

extern "C" fn native_subr_1360(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1360, &[arg_0, arg_1])
}

extern "C" fn native_subr_1361(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1361, &[arg_0])
}

extern "C" fn native_subr_1362(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1362, &[arg_0])
}

extern "C" fn native_subr_1363(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1363, &[arg_0])
}

extern "C" fn native_subr_1364(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1364, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1365(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1365, &[arg_0, arg_1])
}

extern "C" fn native_subr_1366() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1366, &[])
}

extern "C" fn native_subr_1367(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1367, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1368(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1368, &[arg_0])
}

extern "C" fn native_subr_1369(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1369, &[arg_0, arg_1])
}

extern "C" fn native_subr_1370(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1370, &[arg_0, arg_1])
}

extern "C" fn native_subr_1371(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1371, &[arg_0])
}

extern "C" fn native_subr_1372(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1372, &[arg_0])
}

extern "C" fn native_subr_1373() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1373, &[])
}

extern "C" fn native_subr_1374() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1374, &[])
}

extern "C" fn native_subr_1375(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1375, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1376() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1376, &[])
}

extern "C" fn native_subr_1377() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1377, &[])
}

extern "C" fn native_subr_1378() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1378, &[])
}

extern "C" fn native_subr_1379(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1379, &[arg_0])
}

extern "C" fn native_subr_1380(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1380, &[arg_0])
}

extern "C" fn native_subr_1381(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1381, &[arg_0])
}

extern "C" fn native_subr_1382() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1382, &[])
}

extern "C" fn native_subr_1383() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1383, &[])
}

extern "C" fn native_subr_1384() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1384, &[])
}

extern "C" fn native_subr_1385() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1385, &[])
}

extern "C" fn native_subr_1386(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1386, &[arg_0])
}

extern "C" fn native_subr_1387(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1387, &[arg_0])
}

extern "C" fn native_subr_1388(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1388, &[arg_0])
}

extern "C" fn native_subr_1389(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1389, &[arg_0])
}

extern "C" fn native_subr_1390() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1390, &[])
}

extern "C" fn native_subr_1391(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1391, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_1392(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1392, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5])
}

extern "C" fn native_subr_1393(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1393, &[arg_0])
}

extern "C" fn native_subr_1394(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1394, &[arg_0])
}

extern "C" fn native_subr_1395(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1395, &[arg_0])
}

extern "C" fn native_subr_1396() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1396, &[])
}

extern "C" fn native_subr_1397(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1397, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1398(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1398, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_1399(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1399, &[arg_0])
}

extern "C" fn native_subr_1400(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1400, &[arg_0, arg_1])
}

extern "C" fn native_subr_1401(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1401, &[arg_0, arg_1])
}

extern "C" fn native_subr_1402(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
    arg_5: super::runtime::NativeWord,
    arg_6: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1402, &[arg_0, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6])
}

extern "C" fn native_subr_1403(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1403, &[arg_0, arg_1])
}

extern "C" fn native_subr_1404(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1404, &[arg_0, arg_1])
}

extern "C" fn native_subr_1405(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1405, &[arg_0, arg_1])
}

extern "C" fn native_subr_1406(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1406, &[arg_0, arg_1])
}

extern "C" fn native_subr_1407() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1407, &[])
}

extern "C" fn native_subr_1408() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1408, &[])
}

extern "C" fn native_subr_1409() -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1409, &[])
}

extern "C" fn native_subr_1410(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1410, &[arg_0])
}

extern "C" fn native_subr_1411(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1411, &[arg_0])
}

extern "C" fn native_subr_1412(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1412, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1413(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1413, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1414(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1414, &[arg_0, arg_1])
}

extern "C" fn native_subr_1415(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1415, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1416(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1416, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1417(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1417, &[arg_0])
}

extern "C" fn native_subr_1418(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1418, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1419(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1419, &[arg_0, arg_1])
}

extern "C" fn native_subr_1420(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1420, &[arg_0])
}

extern "C" fn native_subr_1421(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1421, &[arg_0])
}

extern "C" fn native_subr_1422(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1422, &[arg_0, arg_1])
}

extern "C" fn native_subr_1423(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1423, &[arg_0])
}

extern "C" fn native_subr_1424(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1424, &[arg_0])
}

extern "C" fn native_subr_1425(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1425, &[arg_0])
}

extern "C" fn native_subr_1426(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1426, &[arg_0])
}

extern "C" fn native_subr_1427(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1427, &[arg_0, arg_1])
}

extern "C" fn native_subr_1428(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1428, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1429(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
    arg_4: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1429, &[arg_0, arg_1, arg_2, arg_3, arg_4])
}

extern "C" fn native_subr_1430(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1430, &[arg_0])
}

extern "C" fn native_subr_1431(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1431, &[arg_0])
}

extern "C" fn native_subr_1432(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1432, &[arg_0])
}

extern "C" fn native_subr_1433(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1433, &[arg_0])
}

extern "C" fn native_subr_1434(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1434, &[arg_0])
}

extern "C" fn native_subr_1435(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1435, &[arg_0])
}

extern "C" fn native_subr_1436(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1436, &[arg_0])
}

extern "C" fn native_subr_1437(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1437, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1438(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1438, &[arg_0, arg_1])
}

extern "C" fn native_subr_1439(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1439, &[arg_0])
}

extern "C" fn native_subr_1440(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1440, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1441(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1441, &[arg_0, arg_1])
}

extern "C" fn native_subr_1442(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1442, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1443(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1443, &[arg_0, arg_1])
}

extern "C" fn native_subr_1444(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1444, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1445(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1445, &[arg_0])
}

extern "C" fn native_subr_1446(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1446, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1447(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1447, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1448(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1448, &[arg_0, arg_1])
}

extern "C" fn native_subr_1449(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1449, &[arg_0, arg_1, arg_2])
}

extern "C" fn native_subr_1450(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1450, &[arg_0, arg_1])
}

extern "C" fn native_subr_1451(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1451, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1452(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
    arg_2: super::runtime::NativeWord,
    arg_3: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1452, &[arg_0, arg_1, arg_2, arg_3])
}

extern "C" fn native_subr_1453(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1453, &[arg_0, arg_1])
}

extern "C" fn native_subr_1454(
    arg_0: super::runtime::NativeWord,
    arg_1: super::runtime::NativeWord,
) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1454, &[arg_0, arg_1])
}
