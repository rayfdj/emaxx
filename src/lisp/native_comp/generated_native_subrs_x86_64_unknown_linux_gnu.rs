//! Generated from GNU Emacs C `syms_of_*`/`defsubr` order.
//! Regenerate with `tools/generate_native_subrs.rs`; do not hand edit.

use super::abi::{NativeMaxArgs, NativeSubr};

// These are inputs to comp.c:hash_native_abi for the C build whose
// active subroutine table is emitted below.  They describe the .eln
// ABI target, not Emaxx's user-visible host configuration.
pub(crate) const NATIVE_ABI_VERSION: &str = "6";
pub(crate) const NATIVE_ABI_SYSTEM_CONFIGURATION: &str = "x86_64-pc-linux-gnu";
pub(crate) const NATIVE_ABI_SYSTEM_CONFIGURATION_OPTIONS: &str = "--with-native-compilation --with-x --with-x-toolkit=no --with-tree-sitter --without-imagemagick --with-lcms2 --with-harfbuzz --without-libotf --without-m17n-flt";

pub(crate) static NATIVE_SUBRS: [NativeSubr; 1455] = [
    NativeSubr::new(
        c"json-parse-buffer",
        0,
        NativeMaxArgs::Many,
        native_subr_0000 as *const std::ffi::c_void,
        0,
    ),
    NativeSubr::new(
        c"json-parse-string",
        1,
        NativeMaxArgs::Many,
        native_subr_0001 as *const std::ffi::c_void,
        1,
    ),
    NativeSubr::new(
        c"json-insert",
        1,
        NativeMaxArgs::Many,
        native_subr_0002 as *const std::ffi::c_void,
        2,
    ),
    NativeSubr::new(
        c"json-serialize",
        1,
        NativeMaxArgs::Many,
        native_subr_0003 as *const std::ffi::c_void,
        3,
    ),
    NativeSubr::new(
        c"pdumper-stats",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0004 as *const std::ffi::c_void,
        4,
    ),
    NativeSubr::new(
        c"dump-emacs-portable--sort-predicate-copied",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0005 as *const std::ffi::c_void,
        5,
    ),
    NativeSubr::new(
        c"dump-emacs-portable--sort-predicate",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0006 as *const std::ffi::c_void,
        6,
    ),
    NativeSubr::new(
        c"dump-emacs-portable",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0007 as *const std::ffi::c_void,
        7,
    ),
    NativeSubr::new(
        c"profiler-memory-log",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0008 as *const std::ffi::c_void,
        8,
    ),
    NativeSubr::new(
        c"profiler-memory-running-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0009 as *const std::ffi::c_void,
        9,
    ),
    NativeSubr::new(
        c"profiler-memory-stop",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0010 as *const std::ffi::c_void,
        10,
    ),
    NativeSubr::new(
        c"profiler-memory-start",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0011 as *const std::ffi::c_void,
        11,
    ),
    NativeSubr::new(
        c"profiler-cpu-log",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0012 as *const std::ffi::c_void,
        12,
    ),
    NativeSubr::new(
        c"profiler-cpu-running-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0013 as *const std::ffi::c_void,
        13,
    ),
    NativeSubr::new(
        c"profiler-cpu-stop",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0014 as *const std::ffi::c_void,
        14,
    ),
    NativeSubr::new(
        c"profiler-cpu-start",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0015 as *const std::ffi::c_void,
        15,
    ),
    NativeSubr::new(
        c"function-equal",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0016 as *const std::ffi::c_void,
        16,
    ),
    NativeSubr::new(
        c"thread-last-error",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0017 as *const std::ffi::c_void,
        17,
    ),
    NativeSubr::new(
        c"condition-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0018 as *const std::ffi::c_void,
        18,
    ),
    NativeSubr::new(
        c"condition-mutex",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0019 as *const std::ffi::c_void,
        19,
    ),
    NativeSubr::new(
        c"condition-notify",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0020 as *const std::ffi::c_void,
        20,
    ),
    NativeSubr::new(
        c"condition-wait",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0021 as *const std::ffi::c_void,
        21,
    ),
    NativeSubr::new(
        c"make-condition-variable",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0022 as *const std::ffi::c_void,
        22,
    ),
    NativeSubr::new(
        c"mutex-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0023 as *const std::ffi::c_void,
        23,
    ),
    NativeSubr::new(
        c"mutex-unlock",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0024 as *const std::ffi::c_void,
        24,
    ),
    NativeSubr::new(
        c"mutex-lock",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0025 as *const std::ffi::c_void,
        25,
    ),
    NativeSubr::new(
        c"make-mutex",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0026 as *const std::ffi::c_void,
        26,
    ),
    NativeSubr::new(
        c"all-threads",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0027 as *const std::ffi::c_void,
        27,
    ),
    NativeSubr::new(
        c"thread--blocker",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0028 as *const std::ffi::c_void,
        28,
    ),
    NativeSubr::new(
        c"thread-join",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0029 as *const std::ffi::c_void,
        29,
    ),
    NativeSubr::new(
        c"thread-live-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0030 as *const std::ffi::c_void,
        30,
    ),
    NativeSubr::new(
        c"thread-signal",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0031 as *const std::ffi::c_void,
        31,
    ),
    NativeSubr::new(
        c"thread-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0032 as *const std::ffi::c_void,
        32,
    ),
    NativeSubr::new(
        c"current-thread",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0033 as *const std::ffi::c_void,
        33,
    ),
    NativeSubr::new(
        c"make-thread",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0034 as *const std::ffi::c_void,
        34,
    ),
    NativeSubr::new(
        c"thread-yield",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0035 as *const std::ffi::c_void,
        35,
    ),
    NativeSubr::new(
        c"inotify-valid-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0036 as *const std::ffi::c_void,
        36,
    ),
    NativeSubr::new(
        c"inotify-rm-watch",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0037 as *const std::ffi::c_void,
        37,
    ),
    NativeSubr::new(
        c"inotify-add-watch",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0038 as *const std::ffi::c_void,
        38,
    ),
    NativeSubr::new(
        c"gnutls-available-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0039 as *const std::ffi::c_void,
        39,
    ),
    NativeSubr::new(
        c"gnutls-symmetric-decrypt",
        4,
        NativeMaxArgs::Fixed(5),
        native_subr_0040 as *const std::ffi::c_void,
        40,
    ),
    NativeSubr::new(
        c"gnutls-symmetric-encrypt",
        4,
        NativeMaxArgs::Fixed(5),
        native_subr_0041 as *const std::ffi::c_void,
        41,
    ),
    NativeSubr::new(
        c"gnutls-hash-digest",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0042 as *const std::ffi::c_void,
        42,
    ),
    NativeSubr::new(
        c"gnutls-hash-mac",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0043 as *const std::ffi::c_void,
        43,
    ),
    NativeSubr::new(
        c"gnutls-digests",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0044 as *const std::ffi::c_void,
        44,
    ),
    NativeSubr::new(
        c"gnutls-macs",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0045 as *const std::ffi::c_void,
        45,
    ),
    NativeSubr::new(
        c"gnutls-ciphers",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0046 as *const std::ffi::c_void,
        46,
    ),
    NativeSubr::new(
        c"gnutls-format-certificate",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0047 as *const std::ffi::c_void,
        47,
    ),
    NativeSubr::new(
        c"gnutls-peer-status-warning-describe",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0048 as *const std::ffi::c_void,
        48,
    ),
    NativeSubr::new(
        c"gnutls-peer-status",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0049 as *const std::ffi::c_void,
        49,
    ),
    NativeSubr::new(
        c"gnutls-bye",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0050 as *const std::ffi::c_void,
        50,
    ),
    NativeSubr::new(
        c"gnutls-deinit",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0051 as *const std::ffi::c_void,
        51,
    ),
    NativeSubr::new(
        c"gnutls-boot",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0052 as *const std::ffi::c_void,
        52,
    ),
    NativeSubr::new(
        c"gnutls-error-string",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0053 as *const std::ffi::c_void,
        53,
    ),
    NativeSubr::new(
        c"gnutls-error-fatalp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0054 as *const std::ffi::c_void,
        54,
    ),
    NativeSubr::new(
        c"gnutls-errorp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0055 as *const std::ffi::c_void,
        55,
    ),
    NativeSubr::new(
        c"gnutls-asynchronous-parameters",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0056 as *const std::ffi::c_void,
        56,
    ),
    NativeSubr::new(
        c"gnutls-get-initstage",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0057 as *const std::ffi::c_void,
        57,
    ),
    NativeSubr::new(
        c"menu-bar-menu-at-x-y",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0058 as *const std::ffi::c_void,
        58,
    ),
    NativeSubr::new(
        c"x-popup-dialog",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0059 as *const std::ffi::c_void,
        59,
    ),
    NativeSubr::new(
        c"x-popup-menu",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0060 as *const std::ffi::c_void,
        60,
    ),
    NativeSubr::new(
        c"zlib-available-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0061 as *const std::ffi::c_void,
        61,
    ),
    NativeSubr::new(
        c"zlib-decompress-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0062 as *const std::ffi::c_void,
        62,
    ),
    NativeSubr::new(
        c"lcms-temp->white-point",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0063 as *const std::ffi::c_void,
        63,
    ),
    NativeSubr::new(
        c"lcms2-available-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0064 as *const std::ffi::c_void,
        64,
    ),
    NativeSubr::new(
        c"lcms-cam02-ucs",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0065 as *const std::ffi::c_void,
        65,
    ),
    NativeSubr::new(
        c"lcms-jab->jch",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0066 as *const std::ffi::c_void,
        66,
    ),
    NativeSubr::new(
        c"lcms-jch->jab",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0067 as *const std::ffi::c_void,
        67,
    ),
    NativeSubr::new(
        c"lcms-jch->xyz",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0068 as *const std::ffi::c_void,
        68,
    ),
    NativeSubr::new(
        c"lcms-xyz->jch",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0069 as *const std::ffi::c_void,
        69,
    ),
    NativeSubr::new(
        c"lcms-cie-de2000",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_0070 as *const std::ffi::c_void,
        70,
    ),
    NativeSubr::new(
        c"libxml-available-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0071 as *const std::ffi::c_void,
        71,
    ),
    NativeSubr::new(
        c"libxml-parse-xml-region",
        0,
        NativeMaxArgs::Fixed(4),
        native_subr_0072 as *const std::ffi::c_void,
        72,
    ),
    NativeSubr::new(
        c"libxml-parse-html-region",
        0,
        NativeMaxArgs::Fixed(4),
        native_subr_0073 as *const std::ffi::c_void,
        73,
    ),
    NativeSubr::new(
        c"x-get-local-selection",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0074 as *const std::ffi::c_void,
        74,
    ),
    NativeSubr::new(
        c"x-register-dnd-atom",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0075 as *const std::ffi::c_void,
        75,
    ),
    NativeSubr::new(
        c"x-send-client-message",
        6,
        NativeMaxArgs::Fixed(6),
        native_subr_0076 as *const std::ffi::c_void,
        76,
    ),
    NativeSubr::new(
        c"x-get-atom-name",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0077 as *const std::ffi::c_void,
        77,
    ),
    NativeSubr::new(
        c"x-selection-exists-p",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0078 as *const std::ffi::c_void,
        78,
    ),
    NativeSubr::new(
        c"x-selection-owner-p",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0079 as *const std::ffi::c_void,
        79,
    ),
    NativeSubr::new(
        c"x-disown-selection-internal",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0080 as *const std::ffi::c_void,
        80,
    ),
    NativeSubr::new(
        c"x-own-selection-internal",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0081 as *const std::ffi::c_void,
        81,
    ),
    NativeSubr::new(
        c"x-get-selection-internal",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0082 as *const std::ffi::c_void,
        82,
    ),
    NativeSubr::new(
        c"handle-save-session",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0083 as *const std::ffi::c_void,
        83,
    ),
    NativeSubr::new(
        c"tool-bar-get-system-style",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0084 as *const std::ffi::c_void,
        84,
    ),
    NativeSubr::new(
        c"font-get-system-normal-font",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0085 as *const std::ffi::c_void,
        85,
    ),
    NativeSubr::new(
        c"font-get-system-font",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0086 as *const std::ffi::c_void,
        86,
    ),
    NativeSubr::new(
        c"fontset-list",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0087 as *const std::ffi::c_void,
        87,
    ),
    NativeSubr::new(
        c"fontset-font",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0088 as *const std::ffi::c_void,
        88,
    ),
    NativeSubr::new(
        c"fontset-info",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0089 as *const std::ffi::c_void,
        89,
    ),
    NativeSubr::new(
        c"set-fontset-font",
        3,
        NativeMaxArgs::Fixed(5),
        native_subr_0090 as *const std::ffi::c_void,
        90,
    ),
    NativeSubr::new(
        c"new-fontset",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0091 as *const std::ffi::c_void,
        91,
    ),
    NativeSubr::new(
        c"query-fontset",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0092 as *const std::ffi::c_void,
        92,
    ),
    NativeSubr::new(
        c"menu-or-popup-active-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0093 as *const std::ffi::c_void,
        93,
    ),
    NativeSubr::new(
        c"x-export-frames",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0094 as *const std::ffi::c_void,
        94,
    ),
    NativeSubr::new(
        c"x-internal-focus-input-context",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0095 as *const std::ffi::c_void,
        95,
    ),
    NativeSubr::new(
        c"x-uses-old-gtk-dialog",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0096 as *const std::ffi::c_void,
        96,
    ),
    NativeSubr::new(
        c"x-get-modifier-masks",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0097 as *const std::ffi::c_void,
        97,
    ),
    NativeSubr::new(
        c"x-translate-coordinates",
        1,
        NativeMaxArgs::Fixed(6),
        native_subr_0098 as *const std::ffi::c_void,
        98,
    ),
    NativeSubr::new(
        c"x-display-set-last-user-time",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0099 as *const std::ffi::c_void,
        99,
    ),
    NativeSubr::new(
        c"x-begin-drag",
        1,
        NativeMaxArgs::Fixed(6),
        native_subr_0100 as *const std::ffi::c_void,
        100,
    ),
    NativeSubr::new(
        c"x-double-buffered-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0101 as *const std::ffi::c_void,
        101,
    ),
    NativeSubr::new(
        c"x-hide-tip",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0102 as *const std::ffi::c_void,
        102,
    ),
    NativeSubr::new(
        c"x-show-tip",
        1,
        NativeMaxArgs::Fixed(6),
        native_subr_0103 as *const std::ffi::c_void,
        103,
    ),
    NativeSubr::new(
        c"x-backspace-delete-keys-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0104 as *const std::ffi::c_void,
        104,
    ),
    NativeSubr::new(
        c"x-synchronize",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0105 as *const std::ffi::c_void,
        105,
    ),
    NativeSubr::new(
        c"x-display-list",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0106 as *const std::ffi::c_void,
        106,
    ),
    NativeSubr::new(
        c"x-close-connection",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0107 as *const std::ffi::c_void,
        107,
    ),
    NativeSubr::new(
        c"x-open-connection",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0108 as *const std::ffi::c_void,
        108,
    ),
    NativeSubr::new(
        c"x-create-frame",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0109 as *const std::ffi::c_void,
        109,
    ),
    NativeSubr::new(
        c"x-wm-set-size-hint",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0110 as *const std::ffi::c_void,
        110,
    ),
    NativeSubr::new(
        c"x-set-mouse-absolute-pixel-position",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0111 as *const std::ffi::c_void,
        111,
    ),
    NativeSubr::new(
        c"x-mouse-absolute-pixel-position",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0112 as *const std::ffi::c_void,
        112,
    ),
    NativeSubr::new(
        c"x-frame-restack",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0113 as *const std::ffi::c_void,
        113,
    ),
    NativeSubr::new(
        c"x-frame-list-z-order",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0114 as *const std::ffi::c_void,
        114,
    ),
    NativeSubr::new(
        c"x-frame-edges",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0115 as *const std::ffi::c_void,
        115,
    ),
    NativeSubr::new(
        c"x-frame-geometry",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0116 as *const std::ffi::c_void,
        116,
    ),
    NativeSubr::new(
        c"x-display-monitor-attributes-list",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0117 as *const std::ffi::c_void,
        117,
    ),
    NativeSubr::new(
        c"x-display-save-under",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0118 as *const std::ffi::c_void,
        118,
    ),
    NativeSubr::new(
        c"x-display-backing-store",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0119 as *const std::ffi::c_void,
        119,
    ),
    NativeSubr::new(
        c"x-display-visual-class",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0120 as *const std::ffi::c_void,
        120,
    ),
    NativeSubr::new(
        c"x-display-color-cells",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0121 as *const std::ffi::c_void,
        121,
    ),
    NativeSubr::new(
        c"x-display-planes",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0122 as *const std::ffi::c_void,
        122,
    ),
    NativeSubr::new(
        c"x-display-screens",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0123 as *const std::ffi::c_void,
        123,
    ),
    NativeSubr::new(
        c"x-display-mm-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0124 as *const std::ffi::c_void,
        124,
    ),
    NativeSubr::new(
        c"x-display-mm-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0125 as *const std::ffi::c_void,
        125,
    ),
    NativeSubr::new(
        c"x-display-pixel-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0126 as *const std::ffi::c_void,
        126,
    ),
    NativeSubr::new(
        c"x-display-pixel-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0127 as *const std::ffi::c_void,
        127,
    ),
    NativeSubr::new(
        c"x-server-input-extension-version",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0128 as *const std::ffi::c_void,
        128,
    ),
    NativeSubr::new(
        c"x-server-version",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0129 as *const std::ffi::c_void,
        129,
    ),
    NativeSubr::new(
        c"x-server-vendor",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0130 as *const std::ffi::c_void,
        130,
    ),
    NativeSubr::new(
        c"x-server-max-request-size",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0131 as *const std::ffi::c_void,
        131,
    ),
    NativeSubr::new(
        c"xw-color-values",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0132 as *const std::ffi::c_void,
        132,
    ),
    NativeSubr::new(
        c"xw-color-defined-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0133 as *const std::ffi::c_void,
        133,
    ),
    NativeSubr::new(
        c"x-display-grayscale-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0134 as *const std::ffi::c_void,
        134,
    ),
    NativeSubr::new(
        c"xw-display-color-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0135 as *const std::ffi::c_void,
        135,
    ),
    NativeSubr::new(
        c"x-window-property-attributes",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0136 as *const std::ffi::c_void,
        136,
    ),
    NativeSubr::new(
        c"x-window-property",
        1,
        NativeMaxArgs::Fixed(6),
        native_subr_0137 as *const std::ffi::c_void,
        137,
    ),
    NativeSubr::new(
        c"x-delete-window-property",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0138 as *const std::ffi::c_void,
        138,
    ),
    NativeSubr::new(
        c"x-change-window-property",
        2,
        NativeMaxArgs::Fixed(7),
        native_subr_0139 as *const std::ffi::c_void,
        139,
    ),
    NativeSubr::new(
        c"image-transforms-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0140 as *const std::ffi::c_void,
        140,
    ),
    NativeSubr::new(
        c"imagep",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0141 as *const std::ffi::c_void,
        141,
    ),
    NativeSubr::new(
        c"image-cache-size",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0142 as *const std::ffi::c_void,
        142,
    ),
    NativeSubr::new(
        c"image-metadata",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0143 as *const std::ffi::c_void,
        143,
    ),
    NativeSubr::new(
        c"image-mask-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0144 as *const std::ffi::c_void,
        144,
    ),
    NativeSubr::new(
        c"image-size",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0145 as *const std::ffi::c_void,
        145,
    ),
    NativeSubr::new(
        c"image-flush",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0146 as *const std::ffi::c_void,
        146,
    ),
    NativeSubr::new(
        c"clear-image-cache",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0147 as *const std::ffi::c_void,
        147,
    ),
    NativeSubr::new(
        c"init-image-library",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0148 as *const std::ffi::c_void,
        148,
    ),
    NativeSubr::new(
        c"set-fringe-bitmap-face",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0149 as *const std::ffi::c_void,
        149,
    ),
    NativeSubr::new(
        c"fringe-bitmaps-at-pos",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0150 as *const std::ffi::c_void,
        150,
    ),
    NativeSubr::new(
        c"define-fringe-bitmap",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_0151 as *const std::ffi::c_void,
        151,
    ),
    NativeSubr::new(
        c"destroy-fringe-bitmap",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0152 as *const std::ffi::c_void,
        152,
    ),
    NativeSubr::new(
        c"font-info",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0153 as *const std::ffi::c_void,
        153,
    ),
    NativeSubr::new(
        c"frame-font-cache",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0154 as *const std::ffi::c_void,
        154,
    ),
    NativeSubr::new(
        c"font-at",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0155 as *const std::ffi::c_void,
        155,
    ),
    NativeSubr::new(
        c"font-match-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0156 as *const std::ffi::c_void,
        156,
    ),
    NativeSubr::new(
        c"font-has-char-p",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0157 as *const std::ffi::c_void,
        157,
    ),
    NativeSubr::new(
        c"font-get-glyphs",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0158 as *const std::ffi::c_void,
        158,
    ),
    NativeSubr::new(
        c"query-font",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0159 as *const std::ffi::c_void,
        159,
    ),
    NativeSubr::new(
        c"close-font",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0160 as *const std::ffi::c_void,
        160,
    ),
    NativeSubr::new(
        c"open-font",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0161 as *const std::ffi::c_void,
        161,
    ),
    NativeSubr::new(
        c"internal-char-font",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0162 as *const std::ffi::c_void,
        162,
    ),
    NativeSubr::new(
        c"font-variation-glyphs",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0163 as *const std::ffi::c_void,
        163,
    ),
    NativeSubr::new(
        c"font-shape-gstring",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0164 as *const std::ffi::c_void,
        164,
    ),
    NativeSubr::new(
        c"clear-font-cache",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0165 as *const std::ffi::c_void,
        165,
    ),
    NativeSubr::new(
        c"font-xlfd-name",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0166 as *const std::ffi::c_void,
        166,
    ),
    NativeSubr::new(
        c"find-font",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0167 as *const std::ffi::c_void,
        167,
    ),
    NativeSubr::new(
        c"font-family-list",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0168 as *const std::ffi::c_void,
        168,
    ),
    NativeSubr::new(
        c"list-fonts",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0169 as *const std::ffi::c_void,
        169,
    ),
    NativeSubr::new(
        c"font-put",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0170 as *const std::ffi::c_void,
        170,
    ),
    NativeSubr::new(
        c"font-face-attributes",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0171 as *const std::ffi::c_void,
        171,
    ),
    NativeSubr::new(
        c"font-get",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0172 as *const std::ffi::c_void,
        172,
    ),
    NativeSubr::new(
        c"font-spec",
        0,
        NativeMaxArgs::Many,
        native_subr_0173 as *const std::ffi::c_void,
        173,
    ),
    NativeSubr::new(
        c"fontp",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0174 as *const std::ffi::c_void,
        174,
    ),
    NativeSubr::new(
        c"sqlite-available-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0175 as *const std::ffi::c_void,
        175,
    ),
    NativeSubr::new(
        c"sqlitep",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0176 as *const std::ffi::c_void,
        176,
    ),
    NativeSubr::new(
        c"sqlite-version",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0177 as *const std::ffi::c_void,
        177,
    ),
    NativeSubr::new(
        c"sqlite-finalize",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0178 as *const std::ffi::c_void,
        178,
    ),
    NativeSubr::new(
        c"sqlite-more-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0179 as *const std::ffi::c_void,
        179,
    ),
    NativeSubr::new(
        c"sqlite-columns",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0180 as *const std::ffi::c_void,
        180,
    ),
    NativeSubr::new(
        c"sqlite-next",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0181 as *const std::ffi::c_void,
        181,
    ),
    NativeSubr::new(
        c"sqlite-load-extension",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0182 as *const std::ffi::c_void,
        182,
    ),
    NativeSubr::new(
        c"sqlite-pragma",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0183 as *const std::ffi::c_void,
        183,
    ),
    NativeSubr::new(
        c"sqlite-rollback",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0184 as *const std::ffi::c_void,
        184,
    ),
    NativeSubr::new(
        c"sqlite-commit",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0185 as *const std::ffi::c_void,
        185,
    ),
    NativeSubr::new(
        c"sqlite-transaction",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0186 as *const std::ffi::c_void,
        186,
    ),
    NativeSubr::new(
        c"sqlite-execute-batch",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0187 as *const std::ffi::c_void,
        187,
    ),
    NativeSubr::new(
        c"sqlite-select",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0188 as *const std::ffi::c_void,
        188,
    ),
    NativeSubr::new(
        c"sqlite-execute",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0189 as *const std::ffi::c_void,
        189,
    ),
    NativeSubr::new(
        c"sqlite-close",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0190 as *const std::ffi::c_void,
        190,
    ),
    NativeSubr::new(
        c"sqlite-open",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0191 as *const std::ffi::c_void,
        191,
    ),
    NativeSubr::new(
        c"bidi-resolved-levels",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0192 as *const std::ffi::c_void,
        192,
    ),
    NativeSubr::new(
        c"long-line-optimizations-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0193 as *const std::ffi::c_void,
        193,
    ),
    NativeSubr::new(
        c"get-display-property",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0194 as *const std::ffi::c_void,
        194,
    ),
    NativeSubr::new(
        c"display--line-is-continued-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0195 as *const std::ffi::c_void,
        195,
    ),
    NativeSubr::new(
        c"bidi-find-overridden-directionality",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0196 as *const std::ffi::c_void,
        196,
    ),
    NativeSubr::new(
        c"move-point-visually",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0197 as *const std::ffi::c_void,
        197,
    ),
    NativeSubr::new(
        c"buffer-text-pixel-size",
        0,
        NativeMaxArgs::Fixed(4),
        native_subr_0198 as *const std::ffi::c_void,
        198,
    ),
    NativeSubr::new(
        c"window-text-pixel-size",
        0,
        NativeMaxArgs::Fixed(7),
        native_subr_0199 as *const std::ffi::c_void,
        199,
    ),
    NativeSubr::new(
        c"current-bidi-paragraph-direction",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0200 as *const std::ffi::c_void,
        200,
    ),
    NativeSubr::new(
        c"invisible-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0201 as *const std::ffi::c_void,
        201,
    ),
    NativeSubr::new(
        c"format-mode-line",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0202 as *const std::ffi::c_void,
        202,
    ),
    NativeSubr::new(
        c"line-pixel-height",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0203 as *const std::ffi::c_void,
        203,
    ),
    NativeSubr::new(
        c"lookup-image-map",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0204 as *const std::ffi::c_void,
        204,
    ),
    NativeSubr::new(
        c"tool-bar-height",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0205 as *const std::ffi::c_void,
        205,
    ),
    NativeSubr::new(
        c"tab-bar-height",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0206 as *const std::ffi::c_void,
        206,
    ),
    NativeSubr::new(
        c"set-buffer-redisplay",
        4,
        NativeMaxArgs::Fixed(4),
        native_subr_0207 as *const std::ffi::c_void,
        207,
    ),
    NativeSubr::new(
        c"set-window-cursor-type",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0208 as *const std::ffi::c_void,
        208,
    ),
    NativeSubr::new(
        c"window-cursor-type",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0209 as *const std::ffi::c_void,
        209,
    ),
    NativeSubr::new(
        c"set-window-parameter",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0210 as *const std::ffi::c_void,
        210,
    ),
    NativeSubr::new(
        c"window-parameter",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0211 as *const std::ffi::c_void,
        211,
    ),
    NativeSubr::new(
        c"window-parameters",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0212 as *const std::ffi::c_void,
        212,
    ),
    NativeSubr::new(
        c"set-window-next-buffers",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0213 as *const std::ffi::c_void,
        213,
    ),
    NativeSubr::new(
        c"window-next-buffers",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0214 as *const std::ffi::c_void,
        214,
    ),
    NativeSubr::new(
        c"set-window-prev-buffers",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0215 as *const std::ffi::c_void,
        215,
    ),
    NativeSubr::new(
        c"window-prev-buffers",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0216 as *const std::ffi::c_void,
        216,
    ),
    NativeSubr::new(
        c"window-list-1",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0217 as *const std::ffi::c_void,
        217,
    ),
    NativeSubr::new(
        c"window-list",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0218 as *const std::ffi::c_void,
        218,
    ),
    NativeSubr::new(
        c"window-bump-use-time",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0219 as *const std::ffi::c_void,
        219,
    ),
    NativeSubr::new(
        c"window-configuration-equal-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0220 as *const std::ffi::c_void,
        220,
    ),
    NativeSubr::new(
        c"set-window-vscroll",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0221 as *const std::ffi::c_void,
        221,
    ),
    NativeSubr::new(
        c"window-vscroll",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0222 as *const std::ffi::c_void,
        222,
    ),
    NativeSubr::new(
        c"window-scroll-bars",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0223 as *const std::ffi::c_void,
        223,
    ),
    NativeSubr::new(
        c"set-window-scroll-bars",
        1,
        NativeMaxArgs::Fixed(6),
        native_subr_0224 as *const std::ffi::c_void,
        224,
    ),
    NativeSubr::new(
        c"window-fringes",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0225 as *const std::ffi::c_void,
        225,
    ),
    NativeSubr::new(
        c"set-window-fringes",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_0226 as *const std::ffi::c_void,
        226,
    ),
    NativeSubr::new(
        c"window-margins",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0227 as *const std::ffi::c_void,
        227,
    ),
    NativeSubr::new(
        c"set-window-margins",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0228 as *const std::ffi::c_void,
        228,
    ),
    NativeSubr::new(
        c"current-window-configuration",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0229 as *const std::ffi::c_void,
        229,
    ),
    NativeSubr::new(
        c"set-window-configuration",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0230 as *const std::ffi::c_void,
        230,
    ),
    NativeSubr::new(
        c"window-configuration-frame",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0231 as *const std::ffi::c_void,
        231,
    ),
    NativeSubr::new(
        c"window-configuration-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0232 as *const std::ffi::c_void,
        232,
    ),
    NativeSubr::new(
        c"move-to-window-line",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0233 as *const std::ffi::c_void,
        233,
    ),
    NativeSubr::new(
        c"window-text-height",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0234 as *const std::ffi::c_void,
        234,
    ),
    NativeSubr::new(
        c"window-text-width",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0235 as *const std::ffi::c_void,
        235,
    ),
    NativeSubr::new(
        c"recenter",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0236 as *const std::ffi::c_void,
        236,
    ),
    NativeSubr::new(
        c"minibuffer-selected-window",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0237 as *const std::ffi::c_void,
        237,
    ),
    NativeSubr::new(
        c"other-window-for-scrolling",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0238 as *const std::ffi::c_void,
        238,
    ),
    NativeSubr::new(
        c"scroll-right",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0239 as *const std::ffi::c_void,
        239,
    ),
    NativeSubr::new(
        c"scroll-left",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0240 as *const std::ffi::c_void,
        240,
    ),
    NativeSubr::new(
        c"scroll-down",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0241 as *const std::ffi::c_void,
        241,
    ),
    NativeSubr::new(
        c"scroll-up",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0242 as *const std::ffi::c_void,
        242,
    ),
    NativeSubr::new(
        c"split-window-internal",
        4,
        NativeMaxArgs::Fixed(4),
        native_subr_0243 as *const std::ffi::c_void,
        243,
    ),
    NativeSubr::new(
        c"force-window-update",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0244 as *const std::ffi::c_void,
        244,
    ),
    NativeSubr::new(
        c"select-window",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0245 as *const std::ffi::c_void,
        245,
    ),
    NativeSubr::new(
        c"run-window-scroll-functions",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0246 as *const std::ffi::c_void,
        246,
    ),
    NativeSubr::new(
        c"run-window-configuration-change-hook",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0247 as *const std::ffi::c_void,
        247,
    ),
    NativeSubr::new(
        c"set-window-buffer",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0248 as *const std::ffi::c_void,
        248,
    ),
    NativeSubr::new(
        c"resize-mini-window-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0249 as *const std::ffi::c_void,
        249,
    ),
    NativeSubr::new(
        c"delete-window-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0250 as *const std::ffi::c_void,
        250,
    ),
    NativeSubr::new(
        c"delete-other-windows-internal",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0251 as *const std::ffi::c_void,
        251,
    ),
    NativeSubr::new(
        c"get-buffer-window",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0252 as *const std::ffi::c_void,
        252,
    ),
    NativeSubr::new(
        c"previous-window",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0253 as *const std::ffi::c_void,
        253,
    ),
    NativeSubr::new(
        c"next-window",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0254 as *const std::ffi::c_void,
        254,
    ),
    NativeSubr::new(
        c"set-window-display-table",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0255 as *const std::ffi::c_void,
        255,
    ),
    NativeSubr::new(
        c"window-display-table",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0256 as *const std::ffi::c_void,
        256,
    ),
    NativeSubr::new(
        c"set-window-dedicated-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0257 as *const std::ffi::c_void,
        257,
    ),
    NativeSubr::new(
        c"window-lines-pixel-dimensions",
        0,
        NativeMaxArgs::Fixed(6),
        native_subr_0258 as *const std::ffi::c_void,
        258,
    ),
    NativeSubr::new(
        c"window-dedicated-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0259 as *const std::ffi::c_void,
        259,
    ),
    NativeSubr::new(
        c"set-window-start",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0260 as *const std::ffi::c_void,
        260,
    ),
    NativeSubr::new(
        c"set-window-point",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0261 as *const std::ffi::c_void,
        261,
    ),
    NativeSubr::new(
        c"window-end",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0262 as *const std::ffi::c_void,
        262,
    ),
    NativeSubr::new(
        c"window-start",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0263 as *const std::ffi::c_void,
        263,
    ),
    NativeSubr::new(
        c"window-old-point",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0264 as *const std::ffi::c_void,
        264,
    ),
    NativeSubr::new(
        c"window-point",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0265 as *const std::ffi::c_void,
        265,
    ),
    NativeSubr::new(
        c"window-at",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0266 as *const std::ffi::c_void,
        266,
    ),
    NativeSubr::new(
        c"coordinates-in-window-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0267 as *const std::ffi::c_void,
        267,
    ),
    NativeSubr::new(
        c"window-scroll-bar-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0268 as *const std::ffi::c_void,
        268,
    ),
    NativeSubr::new(
        c"window-scroll-bar-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0269 as *const std::ffi::c_void,
        269,
    ),
    NativeSubr::new(
        c"window-bottom-divider-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0270 as *const std::ffi::c_void,
        270,
    ),
    NativeSubr::new(
        c"window-right-divider-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0271 as *const std::ffi::c_void,
        271,
    ),
    NativeSubr::new(
        c"window-tab-line-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0272 as *const std::ffi::c_void,
        272,
    ),
    NativeSubr::new(
        c"window-header-line-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0273 as *const std::ffi::c_void,
        273,
    ),
    NativeSubr::new(
        c"window-mode-line-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0274 as *const std::ffi::c_void,
        274,
    ),
    NativeSubr::new(
        c"set-window-hscroll",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0275 as *const std::ffi::c_void,
        275,
    ),
    NativeSubr::new(
        c"window-hscroll",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0276 as *const std::ffi::c_void,
        276,
    ),
    NativeSubr::new(
        c"window-body-width",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0277 as *const std::ffi::c_void,
        277,
    ),
    NativeSubr::new(
        c"window-body-height",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0278 as *const std::ffi::c_void,
        278,
    ),
    NativeSubr::new(
        c"window-resize-apply-total",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0279 as *const std::ffi::c_void,
        279,
    ),
    NativeSubr::new(
        c"window-resize-apply",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0280 as *const std::ffi::c_void,
        280,
    ),
    NativeSubr::new(
        c"set-window-new-normal",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0281 as *const std::ffi::c_void,
        281,
    ),
    NativeSubr::new(
        c"set-window-new-total",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0282 as *const std::ffi::c_void,
        282,
    ),
    NativeSubr::new(
        c"set-window-new-pixel",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0283 as *const std::ffi::c_void,
        283,
    ),
    NativeSubr::new(
        c"window-top-line",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0284 as *const std::ffi::c_void,
        284,
    ),
    NativeSubr::new(
        c"window-left-column",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0285 as *const std::ffi::c_void,
        285,
    ),
    NativeSubr::new(
        c"window-pixel-top",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0286 as *const std::ffi::c_void,
        286,
    ),
    NativeSubr::new(
        c"window-pixel-left",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0287 as *const std::ffi::c_void,
        287,
    ),
    NativeSubr::new(
        c"window-new-normal",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0288 as *const std::ffi::c_void,
        288,
    ),
    NativeSubr::new(
        c"window-new-total",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0289 as *const std::ffi::c_void,
        289,
    ),
    NativeSubr::new(
        c"window-new-pixel",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0290 as *const std::ffi::c_void,
        290,
    ),
    NativeSubr::new(
        c"window-normal-size",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0291 as *const std::ffi::c_void,
        291,
    ),
    NativeSubr::new(
        c"window-total-height",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0292 as *const std::ffi::c_void,
        292,
    ),
    NativeSubr::new(
        c"window-total-width",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0293 as *const std::ffi::c_void,
        293,
    ),
    NativeSubr::new(
        c"window-old-body-pixel-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0294 as *const std::ffi::c_void,
        294,
    ),
    NativeSubr::new(
        c"window-old-body-pixel-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0295 as *const std::ffi::c_void,
        295,
    ),
    NativeSubr::new(
        c"window-old-pixel-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0296 as *const std::ffi::c_void,
        296,
    ),
    NativeSubr::new(
        c"window-old-pixel-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0297 as *const std::ffi::c_void,
        297,
    ),
    NativeSubr::new(
        c"window-pixel-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0298 as *const std::ffi::c_void,
        298,
    ),
    NativeSubr::new(
        c"window-pixel-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0299 as *const std::ffi::c_void,
        299,
    ),
    NativeSubr::new(
        c"window-use-time",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0300 as *const std::ffi::c_void,
        300,
    ),
    NativeSubr::new(
        c"set-window-combination-limit",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0301 as *const std::ffi::c_void,
        301,
    ),
    NativeSubr::new(
        c"window-combination-limit",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0302 as *const std::ffi::c_void,
        302,
    ),
    NativeSubr::new(
        c"window-prev-sibling",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0303 as *const std::ffi::c_void,
        303,
    ),
    NativeSubr::new(
        c"window-next-sibling",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0304 as *const std::ffi::c_void,
        304,
    ),
    NativeSubr::new(
        c"window-left-child",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0305 as *const std::ffi::c_void,
        305,
    ),
    NativeSubr::new(
        c"window-top-child",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0306 as *const std::ffi::c_void,
        306,
    ),
    NativeSubr::new(
        c"window-parent",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0307 as *const std::ffi::c_void,
        307,
    ),
    NativeSubr::new(
        c"window-old-buffer",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0308 as *const std::ffi::c_void,
        308,
    ),
    NativeSubr::new(
        c"window-buffer",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0309 as *const std::ffi::c_void,
        309,
    ),
    NativeSubr::new(
        c"window-line-height",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0310 as *const std::ffi::c_void,
        310,
    ),
    NativeSubr::new(
        c"pos-visible-in-window-p",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0311 as *const std::ffi::c_void,
        311,
    ),
    NativeSubr::new(
        c"set-frame-selected-window",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0312 as *const std::ffi::c_void,
        312,
    ),
    NativeSubr::new(
        c"frame-old-selected-window",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0313 as *const std::ffi::c_void,
        313,
    ),
    NativeSubr::new(
        c"frame-selected-window",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0314 as *const std::ffi::c_void,
        314,
    ),
    NativeSubr::new(
        c"frame-first-window",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0315 as *const std::ffi::c_void,
        315,
    ),
    NativeSubr::new(
        c"frame-root-window",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0316 as *const std::ffi::c_void,
        316,
    ),
    NativeSubr::new(
        c"window-frame",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0317 as *const std::ffi::c_void,
        317,
    ),
    NativeSubr::new(
        c"window-live-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0318 as *const std::ffi::c_void,
        318,
    ),
    NativeSubr::new(
        c"window-valid-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0319 as *const std::ffi::c_void,
        319,
    ),
    NativeSubr::new(
        c"windowp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0320 as *const std::ffi::c_void,
        320,
    ),
    NativeSubr::new(
        c"window-minibuffer-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0321 as *const std::ffi::c_void,
        321,
    ),
    NativeSubr::new(
        c"minibuffer-window",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0322 as *const std::ffi::c_void,
        322,
    ),
    NativeSubr::new(
        c"old-selected-window",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0323 as *const std::ffi::c_void,
        323,
    ),
    NativeSubr::new(
        c"selected-window",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0324 as *const std::ffi::c_void,
        324,
    ),
    NativeSubr::new(
        c"composition-sort-rules",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0325 as *const std::ffi::c_void,
        325,
    ),
    NativeSubr::new(
        c"clear-composition-cache",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0326 as *const std::ffi::c_void,
        326,
    ),
    NativeSubr::new(
        c"composition-get-gstring",
        4,
        NativeMaxArgs::Fixed(4),
        native_subr_0327 as *const std::ffi::c_void,
        327,
    ),
    NativeSubr::new(
        c"find-composition-internal",
        4,
        NativeMaxArgs::Fixed(4),
        native_subr_0328 as *const std::ffi::c_void,
        328,
    ),
    NativeSubr::new(
        c"compose-string-internal",
        3,
        NativeMaxArgs::Fixed(5),
        native_subr_0329 as *const std::ffi::c_void,
        329,
    ),
    NativeSubr::new(
        c"compose-region-internal",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0330 as *const std::ffi::c_void,
        330,
    ),
    NativeSubr::new(
        c"text-property-not-all",
        4,
        NativeMaxArgs::Fixed(5),
        native_subr_0331 as *const std::ffi::c_void,
        331,
    ),
    NativeSubr::new(
        c"text-property-any",
        4,
        NativeMaxArgs::Fixed(5),
        native_subr_0332 as *const std::ffi::c_void,
        332,
    ),
    NativeSubr::new(
        c"remove-list-of-text-properties",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0333 as *const std::ffi::c_void,
        333,
    ),
    NativeSubr::new(
        c"remove-text-properties",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0334 as *const std::ffi::c_void,
        334,
    ),
    NativeSubr::new(
        c"add-face-text-property",
        3,
        NativeMaxArgs::Fixed(5),
        native_subr_0335 as *const std::ffi::c_void,
        335,
    ),
    NativeSubr::new(
        c"set-text-properties",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0336 as *const std::ffi::c_void,
        336,
    ),
    NativeSubr::new(
        c"put-text-property",
        4,
        NativeMaxArgs::Fixed(5),
        native_subr_0337 as *const std::ffi::c_void,
        337,
    ),
    NativeSubr::new(
        c"add-text-properties",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0338 as *const std::ffi::c_void,
        338,
    ),
    NativeSubr::new(
        c"previous-single-property-change",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0339 as *const std::ffi::c_void,
        339,
    ),
    NativeSubr::new(
        c"previous-property-change",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0340 as *const std::ffi::c_void,
        340,
    ),
    NativeSubr::new(
        c"next-single-property-change",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0341 as *const std::ffi::c_void,
        341,
    ),
    NativeSubr::new(
        c"next-property-change",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0342 as *const std::ffi::c_void,
        342,
    ),
    NativeSubr::new(
        c"previous-single-char-property-change",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0343 as *const std::ffi::c_void,
        343,
    ),
    NativeSubr::new(
        c"next-single-char-property-change",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0344 as *const std::ffi::c_void,
        344,
    ),
    NativeSubr::new(
        c"previous-char-property-change",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0345 as *const std::ffi::c_void,
        345,
    ),
    NativeSubr::new(
        c"next-char-property-change",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0346 as *const std::ffi::c_void,
        346,
    ),
    NativeSubr::new(
        c"get-char-property-and-overlay",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0347 as *const std::ffi::c_void,
        347,
    ),
    NativeSubr::new(
        c"get-char-property",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0348 as *const std::ffi::c_void,
        348,
    ),
    NativeSubr::new(
        c"get-text-property",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0349 as *const std::ffi::c_void,
        349,
    ),
    NativeSubr::new(
        c"text-properties-at",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0350 as *const std::ffi::c_void,
        350,
    ),
    NativeSubr::new(
        c"play-sound-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0351 as *const std::ffi::c_void,
        351,
    ),
    NativeSubr::new(
        c"treesit-available-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0352 as *const std::ffi::c_void,
        352,
    ),
    NativeSubr::new(
        c"treesit-subtree-stat",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0353 as *const std::ffi::c_void,
        353,
    ),
    NativeSubr::new(
        c"treesit-node-match-p",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0354 as *const std::ffi::c_void,
        354,
    ),
    NativeSubr::new(
        c"treesit-induce-sparse-tree",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0355 as *const std::ffi::c_void,
        355,
    ),
    NativeSubr::new(
        c"treesit-search-forward",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0356 as *const std::ffi::c_void,
        356,
    ),
    NativeSubr::new(
        c"treesit-search-subtree",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_0357 as *const std::ffi::c_void,
        357,
    ),
    NativeSubr::new(
        c"treesit-query-capture",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_0358 as *const std::ffi::c_void,
        358,
    ),
    NativeSubr::new(
        c"treesit-query-compile",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0359 as *const std::ffi::c_void,
        359,
    ),
    NativeSubr::new(
        c"treesit-query-expand",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0360 as *const std::ffi::c_void,
        360,
    ),
    NativeSubr::new(
        c"treesit-pattern-expand",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0361 as *const std::ffi::c_void,
        361,
    ),
    NativeSubr::new(
        c"treesit-node-eq",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0362 as *const std::ffi::c_void,
        362,
    ),
    NativeSubr::new(
        c"treesit-node-descendant-for-range",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0363 as *const std::ffi::c_void,
        363,
    ),
    NativeSubr::new(
        c"treesit-node-first-child-for-pos",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0364 as *const std::ffi::c_void,
        364,
    ),
    NativeSubr::new(
        c"treesit-node-prev-sibling",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0365 as *const std::ffi::c_void,
        365,
    ),
    NativeSubr::new(
        c"treesit-node-next-sibling",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0366 as *const std::ffi::c_void,
        366,
    ),
    NativeSubr::new(
        c"treesit-node-child-by-field-name",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0367 as *const std::ffi::c_void,
        367,
    ),
    NativeSubr::new(
        c"treesit-node-child-count",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0368 as *const std::ffi::c_void,
        368,
    ),
    NativeSubr::new(
        c"treesit-node-field-name-for-child",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0369 as *const std::ffi::c_void,
        369,
    ),
    NativeSubr::new(
        c"treesit-node-check",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0370 as *const std::ffi::c_void,
        370,
    ),
    NativeSubr::new(
        c"treesit-node-child",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0371 as *const std::ffi::c_void,
        371,
    ),
    NativeSubr::new(
        c"treesit-node-parent",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0372 as *const std::ffi::c_void,
        372,
    ),
    NativeSubr::new(
        c"treesit-node-string",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0373 as *const std::ffi::c_void,
        373,
    ),
    NativeSubr::new(
        c"treesit-node-end",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0374 as *const std::ffi::c_void,
        374,
    ),
    NativeSubr::new(
        c"treesit-node-start",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0375 as *const std::ffi::c_void,
        375,
    ),
    NativeSubr::new(
        c"treesit-node-type",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0376 as *const std::ffi::c_void,
        376,
    ),
    NativeSubr::new(
        c"treesit-parser-remove-notifier",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0377 as *const std::ffi::c_void,
        377,
    ),
    NativeSubr::new(
        c"treesit-parser-add-notifier",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0378 as *const std::ffi::c_void,
        378,
    ),
    NativeSubr::new(
        c"treesit-parser-notifiers",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0379 as *const std::ffi::c_void,
        379,
    ),
    NativeSubr::new(
        c"treesit-parser-included-ranges",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0380 as *const std::ffi::c_void,
        380,
    ),
    NativeSubr::new(
        c"treesit-parser-set-included-ranges",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0381 as *const std::ffi::c_void,
        381,
    ),
    NativeSubr::new(
        c"treesit-parser-root-node",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0382 as *const std::ffi::c_void,
        382,
    ),
    NativeSubr::new(
        c"treesit-parser-tag",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0383 as *const std::ffi::c_void,
        383,
    ),
    NativeSubr::new(
        c"treesit-parser-language",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0384 as *const std::ffi::c_void,
        384,
    ),
    NativeSubr::new(
        c"treesit-parser-buffer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0385 as *const std::ffi::c_void,
        385,
    ),
    NativeSubr::new(
        c"treesit-parser-list",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0386 as *const std::ffi::c_void,
        386,
    ),
    NativeSubr::new(
        c"treesit-parser-delete",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0387 as *const std::ffi::c_void,
        387,
    ),
    NativeSubr::new(
        c"treesit-parser-create",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0388 as *const std::ffi::c_void,
        388,
    ),
    NativeSubr::new(
        c"treesit-node-parser",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0389 as *const std::ffi::c_void,
        389,
    ),
    NativeSubr::new(
        c"treesit-query-language",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0390 as *const std::ffi::c_void,
        390,
    ),
    NativeSubr::new(
        c"treesit-query-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0391 as *const std::ffi::c_void,
        391,
    ),
    NativeSubr::new(
        c"treesit-compiled-query-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0392 as *const std::ffi::c_void,
        392,
    ),
    NativeSubr::new(
        c"treesit-node-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0393 as *const std::ffi::c_void,
        393,
    ),
    NativeSubr::new(
        c"treesit-parser-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0394 as *const std::ffi::c_void,
        394,
    ),
    NativeSubr::new(
        c"treesit-language-abi-version",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0395 as *const std::ffi::c_void,
        395,
    ),
    NativeSubr::new(
        c"treesit-library-abi-version",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0396 as *const std::ffi::c_void,
        396,
    ),
    NativeSubr::new(
        c"treesit-language-available-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0397 as *const std::ffi::c_void,
        397,
    ),
    NativeSubr::new(
        c"module-load",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0398 as *const std::ffi::c_void,
        398,
    ),
    NativeSubr::new(
        c"undo-boundary",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0399 as *const std::ffi::c_void,
        399,
    ),
    NativeSubr::new(
        c"tty--output-buffer-size",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0400 as *const std::ffi::c_void,
        400,
    ),
    NativeSubr::new(
        c"tty--set-output-buffer-size",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0401 as *const std::ffi::c_void,
        401,
    ),
    NativeSubr::new(
        c"resume-tty",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0402 as *const std::ffi::c_void,
        402,
    ),
    NativeSubr::new(
        c"suspend-tty",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0403 as *const std::ffi::c_void,
        403,
    ),
    NativeSubr::new(
        c"tty-top-frame",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0404 as *const std::ffi::c_void,
        404,
    ),
    NativeSubr::new(
        c"controlling-tty-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0405 as *const std::ffi::c_void,
        405,
    ),
    NativeSubr::new(
        c"tty-type",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0406 as *const std::ffi::c_void,
        406,
    ),
    NativeSubr::new(
        c"tty-no-underline",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0407 as *const std::ffi::c_void,
        407,
    ),
    NativeSubr::new(
        c"tty-display-color-cells",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0408 as *const std::ffi::c_void,
        408,
    ),
    NativeSubr::new(
        c"tty-display-color-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0409 as *const std::ffi::c_void,
        409,
    ),
    NativeSubr::new(
        c"set-terminal-parameter",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0410 as *const std::ffi::c_void,
        410,
    ),
    NativeSubr::new(
        c"terminal-parameter",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0411 as *const std::ffi::c_void,
        411,
    ),
    NativeSubr::new(
        c"terminal-parameters",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0412 as *const std::ffi::c_void,
        412,
    ),
    NativeSubr::new(
        c"terminal-name",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0413 as *const std::ffi::c_void,
        413,
    ),
    NativeSubr::new(
        c"terminal-list",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0414 as *const std::ffi::c_void,
        414,
    ),
    NativeSubr::new(
        c"terminal-live-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0415 as *const std::ffi::c_void,
        415,
    ),
    NativeSubr::new(
        c"frame-terminal",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0416 as *const std::ffi::c_void,
        416,
    ),
    NativeSubr::new(
        c"delete-terminal",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0417 as *const std::ffi::c_void,
        417,
    ),
    NativeSubr::new(
        c"parse-partial-sexp",
        2,
        NativeMaxArgs::Fixed(6),
        native_subr_0418 as *const std::ffi::c_void,
        418,
    ),
    NativeSubr::new(
        c"backward-prefix-chars",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0419 as *const std::ffi::c_void,
        419,
    ),
    NativeSubr::new(
        c"scan-sexps",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0420 as *const std::ffi::c_void,
        420,
    ),
    NativeSubr::new(
        c"scan-lists",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0421 as *const std::ffi::c_void,
        421,
    ),
    NativeSubr::new(
        c"forward-comment",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0422 as *const std::ffi::c_void,
        422,
    ),
    NativeSubr::new(
        c"skip-syntax-backward",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0423 as *const std::ffi::c_void,
        423,
    ),
    NativeSubr::new(
        c"skip-syntax-forward",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0424 as *const std::ffi::c_void,
        424,
    ),
    NativeSubr::new(
        c"skip-chars-backward",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0425 as *const std::ffi::c_void,
        425,
    ),
    NativeSubr::new(
        c"skip-chars-forward",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0426 as *const std::ffi::c_void,
        426,
    ),
    NativeSubr::new(
        c"forward-word",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0427 as *const std::ffi::c_void,
        427,
    ),
    NativeSubr::new(
        c"internal-describe-syntax-value",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0428 as *const std::ffi::c_void,
        428,
    ),
    NativeSubr::new(
        c"modify-syntax-entry",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0429 as *const std::ffi::c_void,
        429,
    ),
    NativeSubr::new(
        c"string-to-syntax",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0430 as *const std::ffi::c_void,
        430,
    ),
    NativeSubr::new(
        c"matching-paren",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0431 as *const std::ffi::c_void,
        431,
    ),
    NativeSubr::new(
        c"syntax-class-to-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0432 as *const std::ffi::c_void,
        432,
    ),
    NativeSubr::new(
        c"char-syntax",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0433 as *const std::ffi::c_void,
        433,
    ),
    NativeSubr::new(
        c"set-syntax-table",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0434 as *const std::ffi::c_void,
        434,
    ),
    NativeSubr::new(
        c"copy-syntax-table",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0435 as *const std::ffi::c_void,
        435,
    ),
    NativeSubr::new(
        c"standard-syntax-table",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0436 as *const std::ffi::c_void,
        436,
    ),
    NativeSubr::new(
        c"syntax-table",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0437 as *const std::ffi::c_void,
        437,
    ),
    NativeSubr::new(
        c"syntax-table-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0438 as *const std::ffi::c_void,
        438,
    ),
    NativeSubr::new(
        c"reconsider-frame-fonts",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0439 as *const std::ffi::c_void,
        439,
    ),
    NativeSubr::new(
        c"x-parse-geometry",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0440 as *const std::ffi::c_void,
        440,
    ),
    NativeSubr::new(
        c"x-get-resource",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0441 as *const std::ffi::c_void,
        441,
    ),
    NativeSubr::new(
        c"frame-scale-factor",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0442 as *const std::ffi::c_void,
        442,
    ),
    NativeSubr::new(
        c"set-frame-window-state-change",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0443 as *const std::ffi::c_void,
        443,
    ),
    NativeSubr::new(
        c"frame-window-state-change",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0444 as *const std::ffi::c_void,
        444,
    ),
    NativeSubr::new(
        c"frame--set-was-invisible",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0445 as *const std::ffi::c_void,
        445,
    ),
    NativeSubr::new(
        c"frame-pointer-visible-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0446 as *const std::ffi::c_void,
        446,
    ),
    NativeSubr::new(
        c"set-frame-position",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0447 as *const std::ffi::c_void,
        447,
    ),
    NativeSubr::new(
        c"frame-position",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0448 as *const std::ffi::c_void,
        448,
    ),
    NativeSubr::new(
        c"set-frame-size",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0449 as *const std::ffi::c_void,
        449,
    ),
    NativeSubr::new(
        c"set-frame-width",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0450 as *const std::ffi::c_void,
        450,
    ),
    NativeSubr::new(
        c"set-frame-height",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0451 as *const std::ffi::c_void,
        451,
    ),
    NativeSubr::new(
        c"tool-bar-pixel-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0452 as *const std::ffi::c_void,
        452,
    ),
    NativeSubr::new(
        c"frame-bottom-divider-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0453 as *const std::ffi::c_void,
        453,
    ),
    NativeSubr::new(
        c"frame-right-divider-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0454 as *const std::ffi::c_void,
        454,
    ),
    NativeSubr::new(
        c"frame-internal-border-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0455 as *const std::ffi::c_void,
        455,
    ),
    NativeSubr::new(
        c"frame-child-frame-border-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0456 as *const std::ffi::c_void,
        456,
    ),
    NativeSubr::new(
        c"frame-fringe-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0457 as *const std::ffi::c_void,
        457,
    ),
    NativeSubr::new(
        c"frame-scroll-bar-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0458 as *const std::ffi::c_void,
        458,
    ),
    NativeSubr::new(
        c"frame-scroll-bar-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0459 as *const std::ffi::c_void,
        459,
    ),
    NativeSubr::new(
        c"frame-text-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0460 as *const std::ffi::c_void,
        460,
    ),
    NativeSubr::new(
        c"frame-text-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0461 as *const std::ffi::c_void,
        461,
    ),
    NativeSubr::new(
        c"frame-total-lines",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0462 as *const std::ffi::c_void,
        462,
    ),
    NativeSubr::new(
        c"frame-total-cols",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0463 as *const std::ffi::c_void,
        463,
    ),
    NativeSubr::new(
        c"frame-text-lines",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0464 as *const std::ffi::c_void,
        464,
    ),
    NativeSubr::new(
        c"frame-text-cols",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0465 as *const std::ffi::c_void,
        465,
    ),
    NativeSubr::new(
        c"frame-native-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0466 as *const std::ffi::c_void,
        466,
    ),
    NativeSubr::new(
        c"frame-native-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0467 as *const std::ffi::c_void,
        467,
    ),
    NativeSubr::new(
        c"frame-char-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0468 as *const std::ffi::c_void,
        468,
    ),
    NativeSubr::new(
        c"frame-char-height",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0469 as *const std::ffi::c_void,
        469,
    ),
    NativeSubr::new(
        c"modify-frame-parameters",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0470 as *const std::ffi::c_void,
        470,
    ),
    NativeSubr::new(
        c"frame-parameter",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0471 as *const std::ffi::c_void,
        471,
    ),
    NativeSubr::new(
        c"frame-parameters",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0472 as *const std::ffi::c_void,
        472,
    ),
    NativeSubr::new(
        c"frame-focus",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0473 as *const std::ffi::c_void,
        473,
    ),
    NativeSubr::new(
        c"redirect-frame-focus",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0474 as *const std::ffi::c_void,
        474,
    ),
    NativeSubr::new(
        c"frame-after-make-frame",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0475 as *const std::ffi::c_void,
        475,
    ),
    NativeSubr::new(
        c"x-focus-frame",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0476 as *const std::ffi::c_void,
        476,
    ),
    NativeSubr::new(
        c"lower-frame",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0477 as *const std::ffi::c_void,
        477,
    ),
    NativeSubr::new(
        c"raise-frame",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0478 as *const std::ffi::c_void,
        478,
    ),
    NativeSubr::new(
        c"visible-frame-list",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0479 as *const std::ffi::c_void,
        479,
    ),
    NativeSubr::new(
        c"frame-visible-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0480 as *const std::ffi::c_void,
        480,
    ),
    NativeSubr::new(
        c"iconify-frame",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0481 as *const std::ffi::c_void,
        481,
    ),
    NativeSubr::new(
        c"make-frame-invisible",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0482 as *const std::ffi::c_void,
        482,
    ),
    NativeSubr::new(
        c"make-frame-visible",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0483 as *const std::ffi::c_void,
        483,
    ),
    NativeSubr::new(
        c"set-mouse-pixel-position",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0484 as *const std::ffi::c_void,
        484,
    ),
    NativeSubr::new(
        c"set-mouse-position",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0485 as *const std::ffi::c_void,
        485,
    ),
    NativeSubr::new(
        c"mouse-pixel-position",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0486 as *const std::ffi::c_void,
        486,
    ),
    NativeSubr::new(
        c"mouse-position",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0487 as *const std::ffi::c_void,
        487,
    ),
    NativeSubr::new(
        c"delete-frame",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0488 as *const std::ffi::c_void,
        488,
    ),
    NativeSubr::new(
        c"last-nonminibuffer-frame",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0489 as *const std::ffi::c_void,
        489,
    ),
    NativeSubr::new(
        c"previous-frame",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0490 as *const std::ffi::c_void,
        490,
    ),
    NativeSubr::new(
        c"next-frame",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0491 as *const std::ffi::c_void,
        491,
    ),
    NativeSubr::new(
        c"frame-ancestor-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0492 as *const std::ffi::c_void,
        492,
    ),
    NativeSubr::new(
        c"frame-parent",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0493 as *const std::ffi::c_void,
        493,
    ),
    NativeSubr::new(
        c"frame-list",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0494 as *const std::ffi::c_void,
        494,
    ),
    NativeSubr::new(
        c"old-selected-frame",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0495 as *const std::ffi::c_void,
        495,
    ),
    NativeSubr::new(
        c"selected-frame",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0496 as *const std::ffi::c_void,
        496,
    ),
    NativeSubr::new(
        c"handle-switch-frame",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0497 as *const std::ffi::c_void,
        497,
    ),
    NativeSubr::new(
        c"select-frame",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0498 as *const std::ffi::c_void,
        498,
    ),
    NativeSubr::new(
        c"make-terminal-frame",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0499 as *const std::ffi::c_void,
        499,
    ),
    NativeSubr::new(
        c"frame-windows-min-size",
        4,
        NativeMaxArgs::Fixed(4),
        native_subr_0500 as *const std::ffi::c_void,
        500,
    ),
    NativeSubr::new(
        c"window-system",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0501 as *const std::ffi::c_void,
        501,
    ),
    NativeSubr::new(
        c"frame-live-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0502 as *const std::ffi::c_void,
        502,
    ),
    NativeSubr::new(
        c"framep",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0503 as *const std::ffi::c_void,
        503,
    ),
    NativeSubr::new(
        c"set-time-zone-rule",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0504 as *const std::ffi::c_void,
        504,
    ),
    NativeSubr::new(
        c"current-time-zone",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0505 as *const std::ffi::c_void,
        505,
    ),
    NativeSubr::new(
        c"current-time-string",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0506 as *const std::ffi::c_void,
        506,
    ),
    NativeSubr::new(
        c"encode-time",
        1,
        NativeMaxArgs::Many,
        native_subr_0507 as *const std::ffi::c_void,
        507,
    ),
    NativeSubr::new(
        c"decode-time",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0508 as *const std::ffi::c_void,
        508,
    ),
    NativeSubr::new(
        c"float-time",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0509 as *const std::ffi::c_void,
        509,
    ),
    NativeSubr::new(
        c"format-time-string",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0510 as *const std::ffi::c_void,
        510,
    ),
    NativeSubr::new(
        c"time-equal-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0511 as *const std::ffi::c_void,
        511,
    ),
    NativeSubr::new(
        c"time-less-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0512 as *const std::ffi::c_void,
        512,
    ),
    NativeSubr::new(
        c"time-subtract",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0513 as *const std::ffi::c_void,
        513,
    ),
    NativeSubr::new(
        c"time-add",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0514 as *const std::ffi::c_void,
        514,
    ),
    NativeSubr::new(
        c"time-convert",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0515 as *const std::ffi::c_void,
        515,
    ),
    NativeSubr::new(
        c"current-cpu-time",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0516 as *const std::ffi::c_void,
        516,
    ),
    NativeSubr::new(
        c"current-time",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0517 as *const std::ffi::c_void,
        517,
    ),
    NativeSubr::new(
        c"get-internal-run-time",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0518 as *const std::ffi::c_void,
        518,
    ),
    NativeSubr::new(
        c"re--describe-compiled",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0519 as *const std::ffi::c_void,
        519,
    ),
    NativeSubr::new(
        c"newline-cache-check",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0520 as *const std::ffi::c_void,
        520,
    ),
    NativeSubr::new(
        c"regexp-quote",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0521 as *const std::ffi::c_void,
        521,
    ),
    NativeSubr::new(
        c"match-data--translate",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0522 as *const std::ffi::c_void,
        522,
    ),
    NativeSubr::new(
        c"set-match-data",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0523 as *const std::ffi::c_void,
        523,
    ),
    NativeSubr::new(
        c"match-data",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0524 as *const std::ffi::c_void,
        524,
    ),
    NativeSubr::new(
        c"match-end",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0525 as *const std::ffi::c_void,
        525,
    ),
    NativeSubr::new(
        c"match-beginning",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0526 as *const std::ffi::c_void,
        526,
    ),
    NativeSubr::new(
        c"replace-match",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_0527 as *const std::ffi::c_void,
        527,
    ),
    NativeSubr::new(
        c"posix-search-backward",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0528 as *const std::ffi::c_void,
        528,
    ),
    NativeSubr::new(
        c"posix-search-forward",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0529 as *const std::ffi::c_void,
        529,
    ),
    NativeSubr::new(
        c"re-search-backward",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0530 as *const std::ffi::c_void,
        530,
    ),
    NativeSubr::new(
        c"re-search-forward",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0531 as *const std::ffi::c_void,
        531,
    ),
    NativeSubr::new(
        c"search-backward",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0532 as *const std::ffi::c_void,
        532,
    ),
    NativeSubr::new(
        c"search-forward",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0533 as *const std::ffi::c_void,
        533,
    ),
    NativeSubr::new(
        c"posix-string-match",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0534 as *const std::ffi::c_void,
        534,
    ),
    NativeSubr::new(
        c"string-match",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0535 as *const std::ffi::c_void,
        535,
    ),
    NativeSubr::new(
        c"posix-looking-at",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0536 as *const std::ffi::c_void,
        536,
    ),
    NativeSubr::new(
        c"looking-at",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0537 as *const std::ffi::c_void,
        537,
    ),
    NativeSubr::new(
        c"signal-names",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0538 as *const std::ffi::c_void,
        538,
    ),
    NativeSubr::new(
        c"num-processors",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0539 as *const std::ffi::c_void,
        539,
    ),
    NativeSubr::new(
        c"process-attributes",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0540 as *const std::ffi::c_void,
        540,
    ),
    NativeSubr::new(
        c"list-system-processes",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0541 as *const std::ffi::c_void,
        541,
    ),
    NativeSubr::new(
        c"process-inherit-coding-system-flag",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0542 as *const std::ffi::c_void,
        542,
    ),
    NativeSubr::new(
        c"get-buffer-process",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0543 as *const std::ffi::c_void,
        543,
    ),
    NativeSubr::new(
        c"process-coding-system",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0544 as *const std::ffi::c_void,
        544,
    ),
    NativeSubr::new(
        c"set-process-coding-system",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0545 as *const std::ffi::c_void,
        545,
    ),
    NativeSubr::new(
        c"internal-default-process-filter",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0546 as *const std::ffi::c_void,
        546,
    ),
    NativeSubr::new(
        c"internal-default-process-sentinel",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0547 as *const std::ffi::c_void,
        547,
    ),
    NativeSubr::new(
        c"process-type",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0548 as *const std::ffi::c_void,
        548,
    ),
    NativeSubr::new(
        c"waiting-for-user-input-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0549 as *const std::ffi::c_void,
        549,
    ),
    NativeSubr::new(
        c"signal-process",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0550 as *const std::ffi::c_void,
        550,
    ),
    NativeSubr::new(
        c"internal-default-signal-process",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0551 as *const std::ffi::c_void,
        551,
    ),
    NativeSubr::new(
        c"process-send-eof",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0552 as *const std::ffi::c_void,
        552,
    ),
    NativeSubr::new(
        c"process-running-child-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0553 as *const std::ffi::c_void,
        553,
    ),
    NativeSubr::new(
        c"continue-process",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0554 as *const std::ffi::c_void,
        554,
    ),
    NativeSubr::new(
        c"stop-process",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0555 as *const std::ffi::c_void,
        555,
    ),
    NativeSubr::new(
        c"quit-process",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0556 as *const std::ffi::c_void,
        556,
    ),
    NativeSubr::new(
        c"kill-process",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0557 as *const std::ffi::c_void,
        557,
    ),
    NativeSubr::new(
        c"interrupt-process",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0558 as *const std::ffi::c_void,
        558,
    ),
    NativeSubr::new(
        c"internal-default-interrupt-process",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0559 as *const std::ffi::c_void,
        559,
    ),
    NativeSubr::new(
        c"process-send-string",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0560 as *const std::ffi::c_void,
        560,
    ),
    NativeSubr::new(
        c"process-send-region",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0561 as *const std::ffi::c_void,
        561,
    ),
    NativeSubr::new(
        c"accept-process-output",
        0,
        NativeMaxArgs::Fixed(4),
        native_subr_0562 as *const std::ffi::c_void,
        562,
    ),
    NativeSubr::new(
        c"set-process-datagram-address",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0563 as *const std::ffi::c_void,
        563,
    ),
    NativeSubr::new(
        c"process-datagram-address",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0564 as *const std::ffi::c_void,
        564,
    ),
    NativeSubr::new(
        c"network-interface-info",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0565 as *const std::ffi::c_void,
        565,
    ),
    NativeSubr::new(
        c"network-interface-list",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0566 as *const std::ffi::c_void,
        566,
    ),
    NativeSubr::new(
        c"network-lookup-address-info",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0567 as *const std::ffi::c_void,
        567,
    ),
    NativeSubr::new(
        c"format-network-address",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0568 as *const std::ffi::c_void,
        568,
    ),
    NativeSubr::new(
        c"make-network-process",
        0,
        NativeMaxArgs::Many,
        native_subr_0569 as *const std::ffi::c_void,
        569,
    ),
    NativeSubr::new(
        c"set-network-process-option",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0570 as *const std::ffi::c_void,
        570,
    ),
    NativeSubr::new(
        c"make-serial-process",
        0,
        NativeMaxArgs::Many,
        native_subr_0571 as *const std::ffi::c_void,
        571,
    ),
    NativeSubr::new(
        c"serial-process-configure",
        0,
        NativeMaxArgs::Many,
        native_subr_0572 as *const std::ffi::c_void,
        572,
    ),
    NativeSubr::new(
        c"make-pipe-process",
        0,
        NativeMaxArgs::Many,
        native_subr_0573 as *const std::ffi::c_void,
        573,
    ),
    NativeSubr::new(
        c"make-process",
        0,
        NativeMaxArgs::Many,
        native_subr_0574 as *const std::ffi::c_void,
        574,
    ),
    NativeSubr::new(
        c"process-list",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0575 as *const std::ffi::c_void,
        575,
    ),
    NativeSubr::new(
        c"set-process-plist",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0576 as *const std::ffi::c_void,
        576,
    ),
    NativeSubr::new(
        c"process-plist",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0577 as *const std::ffi::c_void,
        577,
    ),
    NativeSubr::new(
        c"process-contact",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0578 as *const std::ffi::c_void,
        578,
    ),
    NativeSubr::new(
        c"process-query-on-exit-flag",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0579 as *const std::ffi::c_void,
        579,
    ),
    NativeSubr::new(
        c"set-process-query-on-exit-flag",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0580 as *const std::ffi::c_void,
        580,
    ),
    NativeSubr::new(
        c"set-process-inherit-coding-system-flag",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0581 as *const std::ffi::c_void,
        581,
    ),
    NativeSubr::new(
        c"set-process-window-size",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0582 as *const std::ffi::c_void,
        582,
    ),
    NativeSubr::new(
        c"process-thread",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0583 as *const std::ffi::c_void,
        583,
    ),
    NativeSubr::new(
        c"set-process-thread",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0584 as *const std::ffi::c_void,
        584,
    ),
    NativeSubr::new(
        c"process-sentinel",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0585 as *const std::ffi::c_void,
        585,
    ),
    NativeSubr::new(
        c"set-process-sentinel",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0586 as *const std::ffi::c_void,
        586,
    ),
    NativeSubr::new(
        c"process-filter",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0587 as *const std::ffi::c_void,
        587,
    ),
    NativeSubr::new(
        c"set-process-filter",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0588 as *const std::ffi::c_void,
        588,
    ),
    NativeSubr::new(
        c"process-mark",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0589 as *const std::ffi::c_void,
        589,
    ),
    NativeSubr::new(
        c"process-buffer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0590 as *const std::ffi::c_void,
        590,
    ),
    NativeSubr::new(
        c"set-process-buffer",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0591 as *const std::ffi::c_void,
        591,
    ),
    NativeSubr::new(
        c"process-command",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0592 as *const std::ffi::c_void,
        592,
    ),
    NativeSubr::new(
        c"process-tty-name",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0593 as *const std::ffi::c_void,
        593,
    ),
    NativeSubr::new(
        c"process-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0594 as *const std::ffi::c_void,
        594,
    ),
    NativeSubr::new(
        c"process-id",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0595 as *const std::ffi::c_void,
        595,
    ),
    NativeSubr::new(
        c"process-exit-status",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0596 as *const std::ffi::c_void,
        596,
    ),
    NativeSubr::new(
        c"process-status",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0597 as *const std::ffi::c_void,
        597,
    ),
    NativeSubr::new(
        c"delete-process",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0598 as *const std::ffi::c_void,
        598,
    ),
    NativeSubr::new(
        c"get-process",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0599 as *const std::ffi::c_void,
        599,
    ),
    NativeSubr::new(
        c"processp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0600 as *const std::ffi::c_void,
        600,
    ),
    NativeSubr::new(
        c"completing-read",
        2,
        NativeMaxArgs::Fixed(8),
        native_subr_0601 as *const std::ffi::c_void,
        601,
    ),
    NativeSubr::new(
        c"assoc-string",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0602 as *const std::ffi::c_void,
        602,
    ),
    NativeSubr::new(
        c"test-completion",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0603 as *const std::ffi::c_void,
        603,
    ),
    NativeSubr::new(
        c"all-completions",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0604 as *const std::ffi::c_void,
        604,
    ),
    NativeSubr::new(
        c"try-completion",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0605 as *const std::ffi::c_void,
        605,
    ),
    NativeSubr::new(
        c"minibuffer-contents-no-properties",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0606 as *const std::ffi::c_void,
        606,
    ),
    NativeSubr::new(
        c"minibuffer-contents",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0607 as *const std::ffi::c_void,
        607,
    ),
    NativeSubr::new(
        c"minibuffer-prompt-end",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0608 as *const std::ffi::c_void,
        608,
    ),
    NativeSubr::new(
        c"abort-minibuffers",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0609 as *const std::ffi::c_void,
        609,
    ),
    NativeSubr::new(
        c"minibuffer-innermost-command-loop-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0610 as *const std::ffi::c_void,
        610,
    ),
    NativeSubr::new(
        c"innermost-minibuffer-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0611 as *const std::ffi::c_void,
        611,
    ),
    NativeSubr::new(
        c"minibufferp",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0612 as *const std::ffi::c_void,
        612,
    ),
    NativeSubr::new(
        c"minibuffer-prompt",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0613 as *const std::ffi::c_void,
        613,
    ),
    NativeSubr::new(
        c"minibuffer-depth",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0614 as *const std::ffi::c_void,
        614,
    ),
    NativeSubr::new(
        c"read-buffer",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_0615 as *const std::ffi::c_void,
        615,
    ),
    NativeSubr::new(
        c"internal-complete-buffer",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0616 as *const std::ffi::c_void,
        616,
    ),
    NativeSubr::new(
        c"read-variable",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0617 as *const std::ffi::c_void,
        617,
    ),
    NativeSubr::new(
        c"read-command",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0618 as *const std::ffi::c_void,
        618,
    ),
    NativeSubr::new(
        c"read-string",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_0619 as *const std::ffi::c_void,
        619,
    ),
    NativeSubr::new(
        c"read-from-minibuffer",
        1,
        NativeMaxArgs::Fixed(7),
        native_subr_0620 as *const std::ffi::c_void,
        620,
    ),
    NativeSubr::new(
        c"set-minibuffer-window",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0621 as *const std::ffi::c_void,
        621,
    ),
    NativeSubr::new(
        c"active-minibuffer-window",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0622 as *const std::ffi::c_void,
        622,
    ),
    NativeSubr::new(
        c"set-marker-insertion-type",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0623 as *const std::ffi::c_void,
        623,
    ),
    NativeSubr::new(
        c"marker-insertion-type",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0624 as *const std::ffi::c_void,
        624,
    ),
    NativeSubr::new(
        c"copy-marker",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0625 as *const std::ffi::c_void,
        625,
    ),
    NativeSubr::new(
        c"set-marker",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0626 as *const std::ffi::c_void,
        626,
    ),
    NativeSubr::new(
        c"marker-buffer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0627 as *const std::ffi::c_void,
        627,
    ),
    NativeSubr::new(
        c"marker-last-position",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0628 as *const std::ffi::c_void,
        628,
    ),
    NativeSubr::new(
        c"marker-position",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0629 as *const std::ffi::c_void,
        629,
    ),
    NativeSubr::new(
        c"store-kbd-macro-event",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0630 as *const std::ffi::c_void,
        630,
    ),
    NativeSubr::new(
        c"cancel-kbd-macro-events",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0631 as *const std::ffi::c_void,
        631,
    ),
    NativeSubr::new(
        c"execute-kbd-macro",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0632 as *const std::ffi::c_void,
        632,
    ),
    NativeSubr::new(
        c"call-last-kbd-macro",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0633 as *const std::ffi::c_void,
        633,
    ),
    NativeSubr::new(
        c"end-kbd-macro",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0634 as *const std::ffi::c_void,
        634,
    ),
    NativeSubr::new(
        c"start-kbd-macro",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0635 as *const std::ffi::c_void,
        635,
    ),
    NativeSubr::new(
        c"combine-after-change-execute",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0636 as *const std::ffi::c_void,
        636,
    ),
    NativeSubr::new(
        c"compute-motion",
        7,
        NativeMaxArgs::Fixed(7),
        native_subr_0637 as *const std::ffi::c_void,
        637,
    ),
    NativeSubr::new(
        c"vertical-motion",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0638 as *const std::ffi::c_void,
        638,
    ),
    NativeSubr::new(
        c"line-number-display-width",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0639 as *const std::ffi::c_void,
        639,
    ),
    NativeSubr::new(
        c"move-to-column",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0640 as *const std::ffi::c_void,
        640,
    ),
    NativeSubr::new(
        c"current-column",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0641 as *const std::ffi::c_void,
        641,
    ),
    NativeSubr::new(
        c"indent-to",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0642 as *const std::ffi::c_void,
        642,
    ),
    NativeSubr::new(
        c"current-indentation",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0643 as *const std::ffi::c_void,
        643,
    ),
    NativeSubr::new(
        c"file-locked-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0644 as *const std::ffi::c_void,
        644,
    ),
    NativeSubr::new(
        c"unlock-buffer",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0645 as *const std::ffi::c_void,
        645,
    ),
    NativeSubr::new(
        c"lock-buffer",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0646 as *const std::ffi::c_void,
        646,
    ),
    NativeSubr::new(
        c"unlock-file",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0647 as *const std::ffi::c_void,
        647,
    ),
    NativeSubr::new(
        c"lock-file",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0648 as *const std::ffi::c_void,
        648,
    ),
    NativeSubr::new(
        c"daemon-initialized",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0649 as *const std::ffi::c_void,
        649,
    ),
    NativeSubr::new(
        c"daemonp",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0650 as *const std::ffi::c_void,
        650,
    ),
    NativeSubr::new(
        c"invocation-directory",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0651 as *const std::ffi::c_void,
        651,
    ),
    NativeSubr::new(
        c"invocation-name",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0652 as *const std::ffi::c_void,
        652,
    ),
    NativeSubr::new(
        c"kill-emacs",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0653 as *const std::ffi::c_void,
        653,
    ),
    NativeSubr::new(
        c"transpose-regions",
        4,
        NativeMaxArgs::Fixed(5),
        native_subr_0654 as *const std::ffi::c_void,
        654,
    ),
    NativeSubr::new(
        c"save-restriction",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0655 as *const std::ffi::c_void,
        655,
    ),
    NativeSubr::new(
        c"internal--labeled-widen",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0656 as *const std::ffi::c_void,
        656,
    ),
    NativeSubr::new(
        c"internal--labeled-narrow-to-region",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0657 as *const std::ffi::c_void,
        657,
    ),
    NativeSubr::new(
        c"narrow-to-region",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0658 as *const std::ffi::c_void,
        658,
    ),
    NativeSubr::new(
        c"widen",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0659 as *const std::ffi::c_void,
        659,
    ),
    NativeSubr::new(
        c"delete-and-extract-region",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0660 as *const std::ffi::c_void,
        660,
    ),
    NativeSubr::new(
        c"delete-region",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0661 as *const std::ffi::c_void,
        661,
    ),
    NativeSubr::new(
        c"translate-region-internal",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0662 as *const std::ffi::c_void,
        662,
    ),
    NativeSubr::new(
        c"subst-char-in-region",
        4,
        NativeMaxArgs::Fixed(5),
        native_subr_0663 as *const std::ffi::c_void,
        663,
    ),
    NativeSubr::new(
        c"replace-buffer-contents",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0664 as *const std::ffi::c_void,
        664,
    ),
    NativeSubr::new(
        c"compare-buffer-substrings",
        6,
        NativeMaxArgs::Fixed(6),
        native_subr_0665 as *const std::ffi::c_void,
        665,
    ),
    NativeSubr::new(
        c"insert-buffer-substring",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0666 as *const std::ffi::c_void,
        666,
    ),
    NativeSubr::new(
        c"format-message",
        1,
        NativeMaxArgs::Many,
        native_subr_0667 as *const std::ffi::c_void,
        667,
    ),
    NativeSubr::new(
        c"format",
        1,
        NativeMaxArgs::Many,
        native_subr_0668 as *const std::ffi::c_void,
        668,
    ),
    NativeSubr::new(
        c"current-message",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0669 as *const std::ffi::c_void,
        669,
    ),
    NativeSubr::new(
        c"message-or-box",
        1,
        NativeMaxArgs::Many,
        native_subr_0670 as *const std::ffi::c_void,
        670,
    ),
    NativeSubr::new(
        c"message-box",
        1,
        NativeMaxArgs::Many,
        native_subr_0671 as *const std::ffi::c_void,
        671,
    ),
    NativeSubr::new(
        c"message",
        1,
        NativeMaxArgs::Many,
        native_subr_0672 as *const std::ffi::c_void,
        672,
    ),
    NativeSubr::new(
        c"system-name",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0673 as *const std::ffi::c_void,
        673,
    ),
    NativeSubr::new(
        c"emacs-pid",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0674 as *const std::ffi::c_void,
        674,
    ),
    NativeSubr::new(
        c"user-full-name",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0675 as *const std::ffi::c_void,
        675,
    ),
    NativeSubr::new(
        c"group-real-gid",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0676 as *const std::ffi::c_void,
        676,
    ),
    NativeSubr::new(
        c"group-gid",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0677 as *const std::ffi::c_void,
        677,
    ),
    NativeSubr::new(
        c"user-real-uid",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0678 as *const std::ffi::c_void,
        678,
    ),
    NativeSubr::new(
        c"user-uid",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0679 as *const std::ffi::c_void,
        679,
    ),
    NativeSubr::new(
        c"user-real-login-name",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0680 as *const std::ffi::c_void,
        680,
    ),
    NativeSubr::new(
        c"group-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0681 as *const std::ffi::c_void,
        681,
    ),
    NativeSubr::new(
        c"user-login-name",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0682 as *const std::ffi::c_void,
        682,
    ),
    NativeSubr::new(
        c"ngettext",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0683 as *const std::ffi::c_void,
        683,
    ),
    NativeSubr::new(
        c"insert-byte",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0684 as *const std::ffi::c_void,
        684,
    ),
    NativeSubr::new(
        c"insert-char",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0685 as *const std::ffi::c_void,
        685,
    ),
    NativeSubr::new(
        c"insert-before-markers-and-inherit",
        0,
        NativeMaxArgs::Many,
        native_subr_0686 as *const std::ffi::c_void,
        686,
    ),
    NativeSubr::new(
        c"insert-and-inherit",
        0,
        NativeMaxArgs::Many,
        native_subr_0687 as *const std::ffi::c_void,
        687,
    ),
    NativeSubr::new(
        c"insert-before-markers",
        0,
        NativeMaxArgs::Many,
        native_subr_0688 as *const std::ffi::c_void,
        688,
    ),
    NativeSubr::new(
        c"insert",
        0,
        NativeMaxArgs::Many,
        native_subr_0689 as *const std::ffi::c_void,
        689,
    ),
    NativeSubr::new(
        c"char-before",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0690 as *const std::ffi::c_void,
        690,
    ),
    NativeSubr::new(
        c"char-after",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0691 as *const std::ffi::c_void,
        691,
    ),
    NativeSubr::new(
        c"preceding-char",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0692 as *const std::ffi::c_void,
        692,
    ),
    NativeSubr::new(
        c"following-char",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0693 as *const std::ffi::c_void,
        693,
    ),
    NativeSubr::new(
        c"eolp",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0694 as *const std::ffi::c_void,
        694,
    ),
    NativeSubr::new(
        c"bolp",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0695 as *const std::ffi::c_void,
        695,
    ),
    NativeSubr::new(
        c"eobp",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0696 as *const std::ffi::c_void,
        696,
    ),
    NativeSubr::new(
        c"bobp",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0697 as *const std::ffi::c_void,
        697,
    ),
    NativeSubr::new(
        c"byte-to-position",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0698 as *const std::ffi::c_void,
        698,
    ),
    NativeSubr::new(
        c"position-bytes",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0699 as *const std::ffi::c_void,
        699,
    ),
    NativeSubr::new(
        c"gap-size",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0700 as *const std::ffi::c_void,
        700,
    ),
    NativeSubr::new(
        c"gap-position",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0701 as *const std::ffi::c_void,
        701,
    ),
    NativeSubr::new(
        c"point-max-marker",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0702 as *const std::ffi::c_void,
        702,
    ),
    NativeSubr::new(
        c"point-min-marker",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0703 as *const std::ffi::c_void,
        703,
    ),
    NativeSubr::new(
        c"point-min",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0704 as *const std::ffi::c_void,
        704,
    ),
    NativeSubr::new(
        c"point-max",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0705 as *const std::ffi::c_void,
        705,
    ),
    NativeSubr::new(
        c"buffer-size",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0706 as *const std::ffi::c_void,
        706,
    ),
    NativeSubr::new(
        c"save-current-buffer",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0707 as *const std::ffi::c_void,
        707,
    ),
    NativeSubr::new(
        c"save-excursion",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0708 as *const std::ffi::c_void,
        708,
    ),
    NativeSubr::new(
        c"pos-eol",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0709 as *const std::ffi::c_void,
        709,
    ),
    NativeSubr::new(
        c"pos-bol",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0710 as *const std::ffi::c_void,
        710,
    ),
    NativeSubr::new(
        c"line-end-position",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0711 as *const std::ffi::c_void,
        711,
    ),
    NativeSubr::new(
        c"line-beginning-position",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0712 as *const std::ffi::c_void,
        712,
    ),
    NativeSubr::new(
        c"constrain-to-field",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_0713 as *const std::ffi::c_void,
        713,
    ),
    NativeSubr::new(
        c"delete-field",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0714 as *const std::ffi::c_void,
        714,
    ),
    NativeSubr::new(
        c"field-string-no-properties",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0715 as *const std::ffi::c_void,
        715,
    ),
    NativeSubr::new(
        c"field-string",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0716 as *const std::ffi::c_void,
        716,
    ),
    NativeSubr::new(
        c"field-end",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0717 as *const std::ffi::c_void,
        717,
    ),
    NativeSubr::new(
        c"field-beginning",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0718 as *const std::ffi::c_void,
        718,
    ),
    NativeSubr::new(
        c"region-end",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0719 as *const std::ffi::c_void,
        719,
    ),
    NativeSubr::new(
        c"region-beginning",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0720 as *const std::ffi::c_void,
        720,
    ),
    NativeSubr::new(
        c"point",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0721 as *const std::ffi::c_void,
        721,
    ),
    NativeSubr::new(
        c"mark-marker",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0722 as *const std::ffi::c_void,
        722,
    ),
    NativeSubr::new(
        c"point-marker",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0723 as *const std::ffi::c_void,
        723,
    ),
    NativeSubr::new(
        c"get-pos-property",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0724 as *const std::ffi::c_void,
        724,
    ),
    NativeSubr::new(
        c"buffer-string",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0725 as *const std::ffi::c_void,
        725,
    ),
    NativeSubr::new(
        c"buffer-substring-no-properties",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0726 as *const std::ffi::c_void,
        726,
    ),
    NativeSubr::new(
        c"buffer-substring",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0727 as *const std::ffi::c_void,
        727,
    ),
    NativeSubr::new(
        c"byte-to-string",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0728 as *const std::ffi::c_void,
        728,
    ),
    NativeSubr::new(
        c"char-to-string",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0729 as *const std::ffi::c_void,
        729,
    ),
    NativeSubr::new(
        c"string-to-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0730 as *const std::ffi::c_void,
        730,
    ),
    NativeSubr::new(
        c"goto-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0731 as *const std::ffi::c_void,
        731,
    ),
    NativeSubr::new(
        c"char-equal",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0732 as *const std::ffi::c_void,
        732,
    ),
    NativeSubr::new(
        c"propertize",
        1,
        NativeMaxArgs::Many,
        native_subr_0733 as *const std::ffi::c_void,
        733,
    ),
    NativeSubr::new(
        c"text-quoting-style",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0734 as *const std::ffi::c_void,
        734,
    ),
    NativeSubr::new(
        c"Snarf-documentation",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0735 as *const std::ffi::c_void,
        735,
    ),
    NativeSubr::new(
        c"documentation-property",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0736 as *const std::ffi::c_void,
        736,
    ),
    NativeSubr::new(
        c"internal-subr-documentation",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0737 as *const std::ffi::c_void,
        737,
    ),
    NativeSubr::new(
        c"documentation",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0738 as *const std::ffi::c_void,
        738,
    ),
    NativeSubr::new(
        c"documentation-stringp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0739 as *const std::ffi::c_void,
        739,
    ),
    NativeSubr::new(
        c"internal-show-cursor-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0740 as *const std::ffi::c_void,
        740,
    ),
    NativeSubr::new(
        c"internal-show-cursor",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0741 as *const std::ffi::c_void,
        741,
    ),
    NativeSubr::new(
        c"send-string-to-terminal",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0742 as *const std::ffi::c_void,
        742,
    ),
    NativeSubr::new(
        c"sleep-for",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0743 as *const std::ffi::c_void,
        743,
    ),
    NativeSubr::new(
        c"redisplay",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0744 as *const std::ffi::c_void,
        744,
    ),
    NativeSubr::new(
        c"ding",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0745 as *const std::ffi::c_void,
        745,
    ),
    NativeSubr::new(
        c"open-termscript",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0746 as *const std::ffi::c_void,
        746,
    ),
    NativeSubr::new(
        c"frame-or-buffer-changed-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0747 as *const std::ffi::c_void,
        747,
    ),
    NativeSubr::new(
        c"display--update-for-mouse-movement",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0748 as *const std::ffi::c_void,
        748,
    ),
    NativeSubr::new(
        c"redraw-display",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0749 as *const std::ffi::c_void,
        749,
    ),
    NativeSubr::new(
        c"redraw-frame",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0750 as *const std::ffi::c_void,
        750,
    ),
    NativeSubr::new(
        c"system-groups",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0751 as *const std::ffi::c_void,
        751,
    ),
    NativeSubr::new(
        c"system-users",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0752 as *const std::ffi::c_void,
        752,
    ),
    NativeSubr::new(
        c"file-attributes-lessp",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0753 as *const std::ffi::c_void,
        753,
    ),
    NativeSubr::new(
        c"file-attributes",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0754 as *const std::ffi::c_void,
        754,
    ),
    NativeSubr::new(
        c"file-name-all-completions",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0755 as *const std::ffi::c_void,
        755,
    ),
    NativeSubr::new(
        c"file-name-completion",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0756 as *const std::ffi::c_void,
        756,
    ),
    NativeSubr::new(
        c"directory-files-and-attributes",
        1,
        NativeMaxArgs::Fixed(6),
        native_subr_0757 as *const std::ffi::c_void,
        757,
    ),
    NativeSubr::new(
        c"directory-files",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_0758 as *const std::ffi::c_void,
        758,
    ),
    NativeSubr::new(
        c"self-insert-command",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0759 as *const std::ffi::c_void,
        759,
    ),
    NativeSubr::new(
        c"delete-char",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0760 as *const std::ffi::c_void,
        760,
    ),
    NativeSubr::new(
        c"end-of-line",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0761 as *const std::ffi::c_void,
        761,
    ),
    NativeSubr::new(
        c"beginning-of-line",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0762 as *const std::ffi::c_void,
        762,
    ),
    NativeSubr::new(
        c"forward-line",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0763 as *const std::ffi::c_void,
        763,
    ),
    NativeSubr::new(
        c"backward-char",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0764 as *const std::ffi::c_void,
        764,
    ),
    NativeSubr::new(
        c"forward-char",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0765 as *const std::ffi::c_void,
        765,
    ),
    NativeSubr::new(
        c"get-byte",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0766 as *const std::ffi::c_void,
        766,
    ),
    NativeSubr::new(
        c"char-resolve-modifiers",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0767 as *const std::ffi::c_void,
        767,
    ),
    NativeSubr::new(
        c"unibyte-string",
        0,
        NativeMaxArgs::Many,
        native_subr_0768 as *const std::ffi::c_void,
        768,
    ),
    NativeSubr::new(
        c"string",
        0,
        NativeMaxArgs::Many,
        native_subr_0769 as *const std::ffi::c_void,
        769,
    ),
    NativeSubr::new(
        c"string-width",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0770 as *const std::ffi::c_void,
        770,
    ),
    NativeSubr::new(
        c"char-width",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0771 as *const std::ffi::c_void,
        771,
    ),
    NativeSubr::new(
        c"multibyte-char-to-unibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0772 as *const std::ffi::c_void,
        772,
    ),
    NativeSubr::new(
        c"unibyte-char-to-multibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0773 as *const std::ffi::c_void,
        773,
    ),
    NativeSubr::new(
        c"characterp",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0774 as *const std::ffi::c_void,
        774,
    ),
    NativeSubr::new(
        c"max-char",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0775 as *const std::ffi::c_void,
        775,
    ),
    NativeSubr::new(
        c"register-code-conversion-map",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0776 as *const std::ffi::c_void,
        776,
    ),
    NativeSubr::new(
        c"register-ccl-program",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0777 as *const std::ffi::c_void,
        777,
    ),
    NativeSubr::new(
        c"ccl-execute-on-string",
        3,
        NativeMaxArgs::Fixed(5),
        native_subr_0778 as *const std::ffi::c_void,
        778,
    ),
    NativeSubr::new(
        c"ccl-execute",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0779 as *const std::ffi::c_void,
        779,
    ),
    NativeSubr::new(
        c"ccl-program-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0780 as *const std::ffi::c_void,
        780,
    ),
    NativeSubr::new(
        c"modify-category-entry",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0781 as *const std::ffi::c_void,
        781,
    ),
    NativeSubr::new(
        c"category-set-mnemonics",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0782 as *const std::ffi::c_void,
        782,
    ),
    NativeSubr::new(
        c"char-category-set",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0783 as *const std::ffi::c_void,
        783,
    ),
    NativeSubr::new(
        c"set-category-table",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0784 as *const std::ffi::c_void,
        784,
    ),
    NativeSubr::new(
        c"make-category-table",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0785 as *const std::ffi::c_void,
        785,
    ),
    NativeSubr::new(
        c"copy-category-table",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0786 as *const std::ffi::c_void,
        786,
    ),
    NativeSubr::new(
        c"standard-category-table",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0787 as *const std::ffi::c_void,
        787,
    ),
    NativeSubr::new(
        c"category-table",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0788 as *const std::ffi::c_void,
        788,
    ),
    NativeSubr::new(
        c"category-table-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0789 as *const std::ffi::c_void,
        789,
    ),
    NativeSubr::new(
        c"get-unused-category",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0790 as *const std::ffi::c_void,
        790,
    ),
    NativeSubr::new(
        c"category-docstring",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0791 as *const std::ffi::c_void,
        791,
    ),
    NativeSubr::new(
        c"define-category",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0792 as *const std::ffi::c_void,
        792,
    ),
    NativeSubr::new(
        c"make-category-set",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0793 as *const std::ffi::c_void,
        793,
    ),
    NativeSubr::new(
        c"set-standard-case-table",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0794 as *const std::ffi::c_void,
        794,
    ),
    NativeSubr::new(
        c"set-case-table",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0795 as *const std::ffi::c_void,
        795,
    ),
    NativeSubr::new(
        c"standard-case-table",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0796 as *const std::ffi::c_void,
        796,
    ),
    NativeSubr::new(
        c"current-case-table",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0797 as *const std::ffi::c_void,
        797,
    ),
    NativeSubr::new(
        c"case-table-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0798 as *const std::ffi::c_void,
        798,
    ),
    NativeSubr::new(
        c"capitalize-word",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0799 as *const std::ffi::c_void,
        799,
    ),
    NativeSubr::new(
        c"downcase-word",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0800 as *const std::ffi::c_void,
        800,
    ),
    NativeSubr::new(
        c"upcase-word",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0801 as *const std::ffi::c_void,
        801,
    ),
    NativeSubr::new(
        c"upcase-initials-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0802 as *const std::ffi::c_void,
        802,
    ),
    NativeSubr::new(
        c"capitalize-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0803 as *const std::ffi::c_void,
        803,
    ),
    NativeSubr::new(
        c"downcase-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0804 as *const std::ffi::c_void,
        804,
    ),
    NativeSubr::new(
        c"upcase-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0805 as *const std::ffi::c_void,
        805,
    ),
    NativeSubr::new(
        c"upcase-initials",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0806 as *const std::ffi::c_void,
        806,
    ),
    NativeSubr::new(
        c"capitalize",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0807 as *const std::ffi::c_void,
        807,
    ),
    NativeSubr::new(
        c"downcase",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0808 as *const std::ffi::c_void,
        808,
    ),
    NativeSubr::new(
        c"upcase",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0809 as *const std::ffi::c_void,
        809,
    ),
    NativeSubr::new(
        c"prefix-numeric-value",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0810 as *const std::ffi::c_void,
        810,
    ),
    NativeSubr::new(
        c"funcall-interactively",
        1,
        NativeMaxArgs::Many,
        native_subr_0811 as *const std::ffi::c_void,
        811,
    ),
    NativeSubr::new(
        c"call-interactively",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0812 as *const std::ffi::c_void,
        812,
    ),
    NativeSubr::new(
        c"interactive",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0813 as *const std::ffi::c_void,
        813,
    ),
    NativeSubr::new(
        c"internal-stack-stats",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0814 as *const std::ffi::c_void,
        814,
    ),
    NativeSubr::new(
        c"byte-code",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0815 as *const std::ffi::c_void,
        815,
    ),
    NativeSubr::new(
        c"restore-buffer-modified-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0816 as *const std::ffi::c_void,
        816,
    ),
    NativeSubr::new(
        c"overlay-put",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0817 as *const std::ffi::c_void,
        817,
    ),
    NativeSubr::new(
        c"overlay-get",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0818 as *const std::ffi::c_void,
        818,
    ),
    NativeSubr::new(
        c"overlay-lists",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0819 as *const std::ffi::c_void,
        819,
    ),
    NativeSubr::new(
        c"overlay-recenter",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0820 as *const std::ffi::c_void,
        820,
    ),
    NativeSubr::new(
        c"previous-overlay-change",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0821 as *const std::ffi::c_void,
        821,
    ),
    NativeSubr::new(
        c"next-overlay-change",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0822 as *const std::ffi::c_void,
        822,
    ),
    NativeSubr::new(
        c"overlays-in",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0823 as *const std::ffi::c_void,
        823,
    ),
    NativeSubr::new(
        c"overlays-at",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0824 as *const std::ffi::c_void,
        824,
    ),
    NativeSubr::new(
        c"overlay-properties",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0825 as *const std::ffi::c_void,
        825,
    ),
    NativeSubr::new(
        c"overlay-buffer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0826 as *const std::ffi::c_void,
        826,
    ),
    NativeSubr::new(
        c"overlay-end",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0827 as *const std::ffi::c_void,
        827,
    ),
    NativeSubr::new(
        c"overlay-start",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0828 as *const std::ffi::c_void,
        828,
    ),
    NativeSubr::new(
        c"move-overlay",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_0829 as *const std::ffi::c_void,
        829,
    ),
    NativeSubr::new(
        c"delete-all-overlays",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0830 as *const std::ffi::c_void,
        830,
    ),
    NativeSubr::new(
        c"delete-overlay",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0831 as *const std::ffi::c_void,
        831,
    ),
    NativeSubr::new(
        c"make-overlay",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_0832 as *const std::ffi::c_void,
        832,
    ),
    NativeSubr::new(
        c"overlayp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0833 as *const std::ffi::c_void,
        833,
    ),
    NativeSubr::new(
        c"kill-all-local-variables",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0834 as *const std::ffi::c_void,
        834,
    ),
    NativeSubr::new(
        c"set-buffer-multibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0835 as *const std::ffi::c_void,
        835,
    ),
    NativeSubr::new(
        c"buffer-swap-text",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0836 as *const std::ffi::c_void,
        836,
    ),
    NativeSubr::new(
        c"erase-buffer",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0837 as *const std::ffi::c_void,
        837,
    ),
    NativeSubr::new(
        c"barf-if-buffer-read-only",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0838 as *const std::ffi::c_void,
        838,
    ),
    NativeSubr::new(
        c"set-buffer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0839 as *const std::ffi::c_void,
        839,
    ),
    NativeSubr::new(
        c"current-buffer",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0840 as *const std::ffi::c_void,
        840,
    ),
    NativeSubr::new(
        c"set-buffer-major-mode",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0841 as *const std::ffi::c_void,
        841,
    ),
    NativeSubr::new(
        c"bury-buffer-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0842 as *const std::ffi::c_void,
        842,
    ),
    NativeSubr::new(
        c"kill-buffer",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0843 as *const std::ffi::c_void,
        843,
    ),
    NativeSubr::new(
        c"buffer-enable-undo",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0844 as *const std::ffi::c_void,
        844,
    ),
    NativeSubr::new(
        c"other-buffer",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0845 as *const std::ffi::c_void,
        845,
    ),
    NativeSubr::new(
        c"rename-buffer",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0846 as *const std::ffi::c_void,
        846,
    ),
    NativeSubr::new(
        c"buffer-chars-modified-tick",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0847 as *const std::ffi::c_void,
        847,
    ),
    NativeSubr::new(
        c"internal--set-buffer-modified-tick",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0848 as *const std::ffi::c_void,
        848,
    ),
    NativeSubr::new(
        c"buffer-modified-tick",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0849 as *const std::ffi::c_void,
        849,
    ),
    NativeSubr::new(
        c"set-buffer-modified-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0850 as *const std::ffi::c_void,
        850,
    ),
    NativeSubr::new(
        c"force-mode-line-update",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0851 as *const std::ffi::c_void,
        851,
    ),
    NativeSubr::new(
        c"buffer-modified-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0852 as *const std::ffi::c_void,
        852,
    ),
    NativeSubr::new(
        c"buffer-local-variables",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0853 as *const std::ffi::c_void,
        853,
    ),
    NativeSubr::new(
        c"buffer-local-value",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0854 as *const std::ffi::c_void,
        854,
    ),
    NativeSubr::new(
        c"buffer-base-buffer",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0855 as *const std::ffi::c_void,
        855,
    ),
    NativeSubr::new(
        c"buffer-file-name",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0856 as *const std::ffi::c_void,
        856,
    ),
    NativeSubr::new(
        c"buffer-last-name",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0857 as *const std::ffi::c_void,
        857,
    ),
    NativeSubr::new(
        c"buffer-name",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0858 as *const std::ffi::c_void,
        858,
    ),
    NativeSubr::new(
        c"generate-new-buffer-name",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0859 as *const std::ffi::c_void,
        859,
    ),
    NativeSubr::new(
        c"make-indirect-buffer",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0860 as *const std::ffi::c_void,
        860,
    ),
    NativeSubr::new(
        c"get-buffer-create",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0861 as *const std::ffi::c_void,
        861,
    ),
    NativeSubr::new(
        c"find-buffer",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0862 as *const std::ffi::c_void,
        862,
    ),
    NativeSubr::new(
        c"get-truename-buffer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0863 as *const std::ffi::c_void,
        863,
    ),
    NativeSubr::new(
        c"get-file-buffer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0864 as *const std::ffi::c_void,
        864,
    ),
    NativeSubr::new(
        c"get-buffer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0865 as *const std::ffi::c_void,
        865,
    ),
    NativeSubr::new(
        c"buffer-list",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0866 as *const std::ffi::c_void,
        866,
    ),
    NativeSubr::new(
        c"buffer-live-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0867 as *const std::ffi::c_void,
        867,
    ),
    NativeSubr::new(
        c"truncate",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0868 as *const std::ffi::c_void,
        868,
    ),
    NativeSubr::new(
        c"round",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0869 as *const std::ffi::c_void,
        869,
    ),
    NativeSubr::new(
        c"floor",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0870 as *const std::ffi::c_void,
        870,
    ),
    NativeSubr::new(
        c"ceiling",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0871 as *const std::ffi::c_void,
        871,
    ),
    NativeSubr::new(
        c"logb",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0872 as *const std::ffi::c_void,
        872,
    ),
    NativeSubr::new(
        c"float",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0873 as *const std::ffi::c_void,
        873,
    ),
    NativeSubr::new(
        c"abs",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0874 as *const std::ffi::c_void,
        874,
    ),
    NativeSubr::new(
        c"sqrt",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0875 as *const std::ffi::c_void,
        875,
    ),
    NativeSubr::new(
        c"log",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0876 as *const std::ffi::c_void,
        876,
    ),
    NativeSubr::new(
        c"expt",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0877 as *const std::ffi::c_void,
        877,
    ),
    NativeSubr::new(
        c"exp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0878 as *const std::ffi::c_void,
        878,
    ),
    NativeSubr::new(
        c"ftruncate",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0879 as *const std::ffi::c_void,
        879,
    ),
    NativeSubr::new(
        c"fround",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0880 as *const std::ffi::c_void,
        880,
    ),
    NativeSubr::new(
        c"ffloor",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0881 as *const std::ffi::c_void,
        881,
    ),
    NativeSubr::new(
        c"fceiling",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0882 as *const std::ffi::c_void,
        882,
    ),
    NativeSubr::new(
        c"ldexp",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0883 as *const std::ffi::c_void,
        883,
    ),
    NativeSubr::new(
        c"frexp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0884 as *const std::ffi::c_void,
        884,
    ),
    NativeSubr::new(
        c"copysign",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0885 as *const std::ffi::c_void,
        885,
    ),
    NativeSubr::new(
        c"isnan",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0886 as *const std::ffi::c_void,
        886,
    ),
    NativeSubr::new(
        c"tan",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0887 as *const std::ffi::c_void,
        887,
    ),
    NativeSubr::new(
        c"sin",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0888 as *const std::ffi::c_void,
        888,
    ),
    NativeSubr::new(
        c"cos",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0889 as *const std::ffi::c_void,
        889,
    ),
    NativeSubr::new(
        c"atan",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0890 as *const std::ffi::c_void,
        890,
    ),
    NativeSubr::new(
        c"asin",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0891 as *const std::ffi::c_void,
        891,
    ),
    NativeSubr::new(
        c"acos",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0892 as *const std::ffi::c_void,
        892,
    ),
    NativeSubr::new(
        c"functionp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0893 as *const std::ffi::c_void,
        893,
    ),
    NativeSubr::new(
        c"special-variable-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0894 as *const std::ffi::c_void,
        894,
    ),
    NativeSubr::new(
        c"backtrace--locals",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0895 as *const std::ffi::c_void,
        895,
    ),
    NativeSubr::new(
        c"backtrace-eval",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0896 as *const std::ffi::c_void,
        896,
    ),
    NativeSubr::new(
        c"backtrace--frames-from-thread",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0897 as *const std::ffi::c_void,
        897,
    ),
    NativeSubr::new(
        c"backtrace-frame--internal",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0898 as *const std::ffi::c_void,
        898,
    ),
    NativeSubr::new(
        c"mapbacktrace",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0899 as *const std::ffi::c_void,
        899,
    ),
    NativeSubr::new(
        c"backtrace-debug",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0900 as *const std::ffi::c_void,
        900,
    ),
    NativeSubr::new(
        c"run-hook-wrapped",
        2,
        NativeMaxArgs::Many,
        native_subr_0901 as *const std::ffi::c_void,
        901,
    ),
    NativeSubr::new(
        c"run-hook-with-args-until-failure",
        1,
        NativeMaxArgs::Many,
        native_subr_0902 as *const std::ffi::c_void,
        902,
    ),
    NativeSubr::new(
        c"run-hook-with-args-until-success",
        1,
        NativeMaxArgs::Many,
        native_subr_0903 as *const std::ffi::c_void,
        903,
    ),
    NativeSubr::new(
        c"run-hook-with-args",
        1,
        NativeMaxArgs::Many,
        native_subr_0904 as *const std::ffi::c_void,
        904,
    ),
    NativeSubr::new(
        c"run-hooks",
        0,
        NativeMaxArgs::Many,
        native_subr_0905 as *const std::ffi::c_void,
        905,
    ),
    NativeSubr::new(
        c"func-arity",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0906 as *const std::ffi::c_void,
        906,
    ),
    NativeSubr::new(
        c"funcall",
        1,
        NativeMaxArgs::Many,
        native_subr_0907 as *const std::ffi::c_void,
        907,
    ),
    NativeSubr::new(
        c"apply",
        1,
        NativeMaxArgs::Many,
        native_subr_0908 as *const std::ffi::c_void,
        908,
    ),
    NativeSubr::new(
        c"eval",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0909 as *const std::ffi::c_void,
        909,
    ),
    NativeSubr::new(
        c"autoload-do-load",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0910 as *const std::ffi::c_void,
        910,
    ),
    NativeSubr::new(
        c"autoload",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_0911 as *const std::ffi::c_void,
        911,
    ),
    NativeSubr::new(
        c"commandp",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0912 as *const std::ffi::c_void,
        912,
    ),
    NativeSubr::new(
        c"signal",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0913 as *const std::ffi::c_void,
        913,
    ),
    NativeSubr::new(
        c"handler-bind-1",
        1,
        NativeMaxArgs::Many,
        native_subr_0914 as *const std::ffi::c_void,
        914,
    ),
    NativeSubr::new(
        c"condition-case",
        2,
        NativeMaxArgs::Unevalled,
        native_subr_0915 as *const std::ffi::c_void,
        915,
    ),
    NativeSubr::new(
        c"unwind-protect",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0916 as *const std::ffi::c_void,
        916,
    ),
    NativeSubr::new(
        c"throw",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0917 as *const std::ffi::c_void,
        917,
    ),
    NativeSubr::new(
        c"catch",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0918 as *const std::ffi::c_void,
        918,
    ),
    NativeSubr::new(
        c"macroexpand",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0919 as *const std::ffi::c_void,
        919,
    ),
    NativeSubr::new(
        c"funcall-with-delayed-message",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0920 as *const std::ffi::c_void,
        920,
    ),
    NativeSubr::new(
        c"while",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0921 as *const std::ffi::c_void,
        921,
    ),
    NativeSubr::new(
        c"let*",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0922 as *const std::ffi::c_void,
        922,
    ),
    NativeSubr::new(
        c"let",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0923 as *const std::ffi::c_void,
        923,
    ),
    NativeSubr::new(
        c"internal-make-var-non-special",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0924 as *const std::ffi::c_void,
        924,
    ),
    NativeSubr::new(
        c"internal--define-uninitialized-variable",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0925 as *const std::ffi::c_void,
        925,
    ),
    NativeSubr::new(
        c"defconst-1",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0926 as *const std::ffi::c_void,
        926,
    ),
    NativeSubr::new(
        c"defconst",
        2,
        NativeMaxArgs::Unevalled,
        native_subr_0927 as *const std::ffi::c_void,
        927,
    ),
    NativeSubr::new(
        c"defvaralias",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0928 as *const std::ffi::c_void,
        928,
    ),
    NativeSubr::new(
        c"defvar-1",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_0929 as *const std::ffi::c_void,
        929,
    ),
    NativeSubr::new(
        c"defvar",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0930 as *const std::ffi::c_void,
        930,
    ),
    NativeSubr::new(
        c"set-default-toplevel-value",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0931 as *const std::ffi::c_void,
        931,
    ),
    NativeSubr::new(
        c"default-toplevel-value",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0932 as *const std::ffi::c_void,
        932,
    ),
    NativeSubr::new(
        c"make-interpreted-closure",
        3,
        NativeMaxArgs::Fixed(5),
        native_subr_0933 as *const std::ffi::c_void,
        933,
    ),
    NativeSubr::new(
        c"function",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0934 as *const std::ffi::c_void,
        934,
    ),
    NativeSubr::new(
        c"quote",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0935 as *const std::ffi::c_void,
        935,
    ),
    NativeSubr::new(
        c"setq",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0936 as *const std::ffi::c_void,
        936,
    ),
    NativeSubr::new(
        c"prog1",
        1,
        NativeMaxArgs::Unevalled,
        native_subr_0937 as *const std::ffi::c_void,
        937,
    ),
    NativeSubr::new(
        c"progn",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0938 as *const std::ffi::c_void,
        938,
    ),
    NativeSubr::new(
        c"cond",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0939 as *const std::ffi::c_void,
        939,
    ),
    NativeSubr::new(
        c"if",
        2,
        NativeMaxArgs::Unevalled,
        native_subr_0940 as *const std::ffi::c_void,
        940,
    ),
    NativeSubr::new(
        c"and",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0941 as *const std::ffi::c_void,
        941,
    ),
    NativeSubr::new(
        c"or",
        0,
        NativeMaxArgs::Unevalled,
        native_subr_0942 as *const std::ffi::c_void,
        942,
    ),
    NativeSubr::new(
        c"flush-standard-output",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0943 as *const std::ffi::c_void,
        943,
    ),
    NativeSubr::new(
        c"print--preprocess",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0944 as *const std::ffi::c_void,
        944,
    ),
    NativeSubr::new(
        c"redirect-debugging-output",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0945 as *const std::ffi::c_void,
        945,
    ),
    NativeSubr::new(
        c"write-char",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0946 as *const std::ffi::c_void,
        946,
    ),
    NativeSubr::new(
        c"terpri",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_0947 as *const std::ffi::c_void,
        947,
    ),
    NativeSubr::new(
        c"print",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0948 as *const std::ffi::c_void,
        948,
    ),
    NativeSubr::new(
        c"princ",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0949 as *const std::ffi::c_void,
        949,
    ),
    NativeSubr::new(
        c"error-message-string",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0950 as *const std::ffi::c_void,
        950,
    ),
    NativeSubr::new(
        c"prin1-to-string",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0951 as *const std::ffi::c_void,
        951,
    ),
    NativeSubr::new(
        c"prin1",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0952 as *const std::ffi::c_void,
        952,
    ),
    NativeSubr::new(
        c"obarray-clear",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0953 as *const std::ffi::c_void,
        953,
    ),
    NativeSubr::new(
        c"obarrayp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0954 as *const std::ffi::c_void,
        954,
    ),
    NativeSubr::new(
        c"obarray-make",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0955 as *const std::ffi::c_void,
        955,
    ),
    NativeSubr::new(
        c"internal--obarray-buckets",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0956 as *const std::ffi::c_void,
        956,
    ),
    NativeSubr::new(
        c"locate-file-internal",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0957 as *const std::ffi::c_void,
        957,
    ),
    NativeSubr::new(
        c"mapatoms",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0958 as *const std::ffi::c_void,
        958,
    ),
    NativeSubr::new(
        c"read-event",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0959 as *const std::ffi::c_void,
        959,
    ),
    NativeSubr::new(
        c"read-char-exclusive",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0960 as *const std::ffi::c_void,
        960,
    ),
    NativeSubr::new(
        c"read-char",
        0,
        NativeMaxArgs::Fixed(3),
        native_subr_0961 as *const std::ffi::c_void,
        961,
    ),
    NativeSubr::new(
        c"eval-region",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_0962 as *const std::ffi::c_void,
        962,
    ),
    NativeSubr::new(
        c"eval-buffer",
        0,
        NativeMaxArgs::Fixed(5),
        native_subr_0963 as *const std::ffi::c_void,
        963,
    ),
    NativeSubr::new(
        c"load",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_0964 as *const std::ffi::c_void,
        964,
    ),
    NativeSubr::new(
        c"get-load-suffixes",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0965 as *const std::ffi::c_void,
        965,
    ),
    NativeSubr::new(
        c"unintern",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0966 as *const std::ffi::c_void,
        966,
    ),
    NativeSubr::new(
        c"intern-soft",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0967 as *const std::ffi::c_void,
        967,
    ),
    NativeSubr::new(
        c"intern",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0968 as *const std::ffi::c_void,
        968,
    ),
    NativeSubr::new(
        c"lread--substitute-object-in-subtree",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0969 as *const std::ffi::c_void,
        969,
    ),
    NativeSubr::new(
        c"read-from-string",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_0970 as *const std::ffi::c_void,
        970,
    ),
    NativeSubr::new(
        c"read-positioning-symbols",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0971 as *const std::ffi::c_void,
        971,
    ),
    NativeSubr::new(
        c"read",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_0972 as *const std::ffi::c_void,
        972,
    ),
    NativeSubr::new(
        c"put-unicode-property-internal",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0973 as *const std::ffi::c_void,
        973,
    ),
    NativeSubr::new(
        c"get-unicode-property-internal",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0974 as *const std::ffi::c_void,
        974,
    ),
    NativeSubr::new(
        c"unicode-property-table-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0975 as *const std::ffi::c_void,
        975,
    ),
    NativeSubr::new(
        c"map-char-table",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0976 as *const std::ffi::c_void,
        976,
    ),
    NativeSubr::new(
        c"optimize-char-table",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0977 as *const std::ffi::c_void,
        977,
    ),
    NativeSubr::new(
        c"set-char-table-range",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0978 as *const std::ffi::c_void,
        978,
    ),
    NativeSubr::new(
        c"char-table-range",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0979 as *const std::ffi::c_void,
        979,
    ),
    NativeSubr::new(
        c"set-char-table-extra-slot",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_0980 as *const std::ffi::c_void,
        980,
    ),
    NativeSubr::new(
        c"char-table-extra-slot",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0981 as *const std::ffi::c_void,
        981,
    ),
    NativeSubr::new(
        c"set-char-table-parent",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0982 as *const std::ffi::c_void,
        982,
    ),
    NativeSubr::new(
        c"char-table-subtype",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0983 as *const std::ffi::c_void,
        983,
    ),
    NativeSubr::new(
        c"char-table-parent",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0984 as *const std::ffi::c_void,
        984,
    ),
    NativeSubr::new(
        c"make-char-table",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0985 as *const std::ffi::c_void,
        985,
    ),
    NativeSubr::new(
        c"call-process-region",
        3,
        NativeMaxArgs::Many,
        native_subr_0986 as *const std::ffi::c_void,
        986,
    ),
    NativeSubr::new(
        c"getenv-internal",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0987 as *const std::ffi::c_void,
        987,
    ),
    NativeSubr::new(
        c"call-process",
        1,
        NativeMaxArgs::Many,
        native_subr_0988 as *const std::ffi::c_void,
        988,
    ),
    NativeSubr::new(
        c"native-comp-available-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0989 as *const std::ffi::c_void,
        989,
    ),
    NativeSubr::new(
        c"native-elisp-load",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_0990 as *const std::ffi::c_void,
        990,
    ),
    NativeSubr::new(
        c"comp--late-register-subr",
        7,
        NativeMaxArgs::Fixed(7),
        native_subr_0991 as *const std::ffi::c_void,
        991,
    ),
    NativeSubr::new(
        c"comp--register-subr",
        7,
        NativeMaxArgs::Fixed(7),
        native_subr_0992 as *const std::ffi::c_void,
        992,
    ),
    NativeSubr::new(
        c"comp--register-lambda",
        7,
        NativeMaxArgs::Fixed(7),
        native_subr_0993 as *const std::ffi::c_void,
        993,
    ),
    NativeSubr::new(
        c"comp-libgccjit-version",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0994 as *const std::ffi::c_void,
        994,
    ),
    NativeSubr::new(
        c"comp--compile-ctxt-to-file0",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_0995 as *const std::ffi::c_void,
        995,
    ),
    NativeSubr::new(
        c"comp--release-ctxt",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0996 as *const std::ffi::c_void,
        996,
    ),
    NativeSubr::new(
        c"comp--init-ctxt",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0997 as *const std::ffi::c_void,
        997,
    ),
    NativeSubr::new(
        c"comp--install-trampoline",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_0998 as *const std::ffi::c_void,
        998,
    ),
    NativeSubr::new(
        c"comp-native-compiler-options-effective-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_0999 as *const std::ffi::c_void,
        999,
    ),
    NativeSubr::new(
        c"comp-native-driver-options-effective-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1000 as *const std::ffi::c_void,
        1000,
    ),
    NativeSubr::new(
        c"comp-el-to-eln-filename",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1001 as *const std::ffi::c_void,
        1001,
    ),
    NativeSubr::new(
        c"comp-el-to-eln-rel-filename",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1002 as *const std::ffi::c_void,
        1002,
    ),
    NativeSubr::new(
        c"comp--subr-signature",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1003 as *const std::ffi::c_void,
        1003,
    ),
    NativeSubr::new(
        c"set-text-conversion-style",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1004 as *const std::ffi::c_void,
        1004,
    ),
    NativeSubr::new(
        c"coding-system-priority-list",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1005 as *const std::ffi::c_void,
        1005,
    ),
    NativeSubr::new(
        c"coding-system-eol-type",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1006 as *const std::ffi::c_void,
        1006,
    ),
    NativeSubr::new(
        c"coding-system-aliases",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1007 as *const std::ffi::c_void,
        1007,
    ),
    NativeSubr::new(
        c"coding-system-plist",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1008 as *const std::ffi::c_void,
        1008,
    ),
    NativeSubr::new(
        c"coding-system-base",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1009 as *const std::ffi::c_void,
        1009,
    ),
    NativeSubr::new(
        c"coding-system-put",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1010 as *const std::ffi::c_void,
        1010,
    ),
    NativeSubr::new(
        c"define-coding-system-alias",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1011 as *const std::ffi::c_void,
        1011,
    ),
    NativeSubr::new(
        c"define-coding-system-internal",
        13,
        NativeMaxArgs::Many,
        native_subr_1012 as *const std::ffi::c_void,
        1012,
    ),
    NativeSubr::new(
        c"set-coding-system-priority",
        0,
        NativeMaxArgs::Many,
        native_subr_1013 as *const std::ffi::c_void,
        1013,
    ),
    NativeSubr::new(
        c"find-operation-coding-system",
        1,
        NativeMaxArgs::Many,
        native_subr_1014 as *const std::ffi::c_void,
        1014,
    ),
    NativeSubr::new(
        c"keyboard-coding-system",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1015 as *const std::ffi::c_void,
        1015,
    ),
    NativeSubr::new(
        c"set-keyboard-coding-system-internal",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1016 as *const std::ffi::c_void,
        1016,
    ),
    NativeSubr::new(
        c"terminal-coding-system",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1017 as *const std::ffi::c_void,
        1017,
    ),
    NativeSubr::new(
        c"set-safe-terminal-coding-system-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1018 as *const std::ffi::c_void,
        1018,
    ),
    NativeSubr::new(
        c"set-terminal-coding-system-internal",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1019 as *const std::ffi::c_void,
        1019,
    ),
    NativeSubr::new(
        c"encode-big5-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1020 as *const std::ffi::c_void,
        1020,
    ),
    NativeSubr::new(
        c"decode-big5-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1021 as *const std::ffi::c_void,
        1021,
    ),
    NativeSubr::new(
        c"encode-sjis-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1022 as *const std::ffi::c_void,
        1022,
    ),
    NativeSubr::new(
        c"decode-sjis-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1023 as *const std::ffi::c_void,
        1023,
    ),
    NativeSubr::new(
        c"encode-coding-string",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_1024 as *const std::ffi::c_void,
        1024,
    ),
    NativeSubr::new(
        c"decode-coding-string",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_1025 as *const std::ffi::c_void,
        1025,
    ),
    NativeSubr::new(
        c"encode-coding-region",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_1026 as *const std::ffi::c_void,
        1026,
    ),
    NativeSubr::new(
        c"decode-coding-region",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_1027 as *const std::ffi::c_void,
        1027,
    ),
    NativeSubr::new(
        c"check-coding-systems-region",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1028 as *const std::ffi::c_void,
        1028,
    ),
    NativeSubr::new(
        c"unencodable-char-position",
        3,
        NativeMaxArgs::Fixed(5),
        native_subr_1029 as *const std::ffi::c_void,
        1029,
    ),
    NativeSubr::new(
        c"find-coding-systems-region-internal",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1030 as *const std::ffi::c_void,
        1030,
    ),
    NativeSubr::new(
        c"detect-coding-string",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1031 as *const std::ffi::c_void,
        1031,
    ),
    NativeSubr::new(
        c"detect-coding-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1032 as *const std::ffi::c_void,
        1032,
    ),
    NativeSubr::new(
        c"check-coding-system",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1033 as *const std::ffi::c_void,
        1033,
    ),
    NativeSubr::new(
        c"read-non-nil-coding-system",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1034 as *const std::ffi::c_void,
        1034,
    ),
    NativeSubr::new(
        c"read-coding-system",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1035 as *const std::ffi::c_void,
        1035,
    ),
    NativeSubr::new(
        c"coding-system-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1036 as *const std::ffi::c_void,
        1036,
    ),
    NativeSubr::new(
        c"sort-charsets",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1037 as *const std::ffi::c_void,
        1037,
    ),
    NativeSubr::new(
        c"charset-id-internal",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1038 as *const std::ffi::c_void,
        1038,
    ),
    NativeSubr::new(
        c"set-charset-priority",
        1,
        NativeMaxArgs::Many,
        native_subr_1039 as *const std::ffi::c_void,
        1039,
    ),
    NativeSubr::new(
        c"charset-priority-list",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1040 as *const std::ffi::c_void,
        1040,
    ),
    NativeSubr::new(
        c"clear-charset-maps",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1041 as *const std::ffi::c_void,
        1041,
    ),
    NativeSubr::new(
        c"iso-charset",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1042 as *const std::ffi::c_void,
        1042,
    ),
    NativeSubr::new(
        c"charset-after",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1043 as *const std::ffi::c_void,
        1043,
    ),
    NativeSubr::new(
        c"char-charset",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1044 as *const std::ffi::c_void,
        1044,
    ),
    NativeSubr::new(
        c"make-char",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_1045 as *const std::ffi::c_void,
        1045,
    ),
    NativeSubr::new(
        c"split-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1046 as *const std::ffi::c_void,
        1046,
    ),
    NativeSubr::new(
        c"encode-char",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1047 as *const std::ffi::c_void,
        1047,
    ),
    NativeSubr::new(
        c"decode-char",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1048 as *const std::ffi::c_void,
        1048,
    ),
    NativeSubr::new(
        c"find-charset-string",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1049 as *const std::ffi::c_void,
        1049,
    ),
    NativeSubr::new(
        c"find-charset-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1050 as *const std::ffi::c_void,
        1050,
    ),
    NativeSubr::new(
        c"declare-equiv-charset",
        4,
        NativeMaxArgs::Fixed(4),
        native_subr_1051 as *const std::ffi::c_void,
        1051,
    ),
    NativeSubr::new(
        c"get-unused-iso-final-char",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1052 as *const std::ffi::c_void,
        1052,
    ),
    NativeSubr::new(
        c"unify-charset",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1053 as *const std::ffi::c_void,
        1053,
    ),
    NativeSubr::new(
        c"set-charset-plist",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1054 as *const std::ffi::c_void,
        1054,
    ),
    NativeSubr::new(
        c"charset-plist",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1055 as *const std::ffi::c_void,
        1055,
    ),
    NativeSubr::new(
        c"define-charset-alias",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1056 as *const std::ffi::c_void,
        1056,
    ),
    NativeSubr::new(
        c"define-charset-internal",
        17,
        NativeMaxArgs::Many,
        native_subr_1057 as *const std::ffi::c_void,
        1057,
    ),
    NativeSubr::new(
        c"map-charset-chars",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_1058 as *const std::ffi::c_void,
        1058,
    ),
    NativeSubr::new(
        c"charsetp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1059 as *const std::ffi::c_void,
        1059,
    ),
    NativeSubr::new(
        c"external-debugging-output",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1060 as *const std::ffi::c_void,
        1060,
    ),
    NativeSubr::new(
        c"malloc-trim",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1061 as *const std::ffi::c_void,
        1061,
    ),
    NativeSubr::new(
        c"malloc-info",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1062 as *const std::ffi::c_void,
        1062,
    ),
    NativeSubr::new(
        c"memory-use-counts",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1063 as *const std::ffi::c_void,
        1063,
    ),
    NativeSubr::new(
        c"memory-info",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1064 as *const std::ffi::c_void,
        1064,
    ),
    NativeSubr::new(
        c"garbage-collect-maybe",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1065 as *const std::ffi::c_void,
        1065,
    ),
    NativeSubr::new(
        c"garbage-collect",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1066 as *const std::ffi::c_void,
        1066,
    ),
    NativeSubr::new(
        c"purecopy",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1067 as *const std::ffi::c_void,
        1067,
    ),
    NativeSubr::new(
        c"make-finalizer",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1068 as *const std::ffi::c_void,
        1068,
    ),
    NativeSubr::new(
        c"make-marker",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1069 as *const std::ffi::c_void,
        1069,
    ),
    NativeSubr::new(
        c"make-symbol",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1070 as *const std::ffi::c_void,
        1070,
    ),
    NativeSubr::new(
        c"make-bool-vector",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1071 as *const std::ffi::c_void,
        1071,
    ),
    NativeSubr::new(
        c"make-string",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1072 as *const std::ffi::c_void,
        1072,
    ),
    NativeSubr::new(
        c"make-record",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1073 as *const std::ffi::c_void,
        1073,
    ),
    NativeSubr::new(
        c"make-vector",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1074 as *const std::ffi::c_void,
        1074,
    ),
    NativeSubr::new(
        c"make-list",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1075 as *const std::ffi::c_void,
        1075,
    ),
    NativeSubr::new(
        c"make-closure",
        1,
        NativeMaxArgs::Many,
        super::runtime::direct_native_make_closure as *const std::ffi::c_void,
        1076,
    ),
    NativeSubr::new(
        c"make-byte-code",
        4,
        NativeMaxArgs::Many,
        native_subr_1077 as *const std::ffi::c_void,
        1077,
    ),
    NativeSubr::new(
        c"bool-vector",
        0,
        NativeMaxArgs::Many,
        native_subr_1078 as *const std::ffi::c_void,
        1078,
    ),
    NativeSubr::new(
        c"record",
        1,
        NativeMaxArgs::Many,
        native_subr_1079 as *const std::ffi::c_void,
        1079,
    ),
    NativeSubr::new(
        c"vector",
        0,
        NativeMaxArgs::Many,
        native_subr_1080 as *const std::ffi::c_void,
        1080,
    ),
    NativeSubr::new(
        c"list",
        0,
        NativeMaxArgs::Many,
        native_subr_1081 as *const std::ffi::c_void,
        1081,
    ),
    NativeSubr::new(
        c"cons",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1082 as *const std::ffi::c_void,
        1082,
    ),
    NativeSubr::new(
        c"unix-sync",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1083 as *const std::ffi::c_void,
        1083,
    ),
    NativeSubr::new(
        c"file-system-info",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1084 as *const std::ffi::c_void,
        1084,
    ),
    NativeSubr::new(
        c"set-binary-mode",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1085 as *const std::ffi::c_void,
        1085,
    ),
    NativeSubr::new(
        c"next-read-file-uses-dialog-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1086 as *const std::ffi::c_void,
        1086,
    ),
    NativeSubr::new(
        c"recent-auto-save-p",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1087 as *const std::ffi::c_void,
        1087,
    ),
    NativeSubr::new(
        c"clear-buffer-auto-save-failure",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1088 as *const std::ffi::c_void,
        1088,
    ),
    NativeSubr::new(
        c"set-buffer-auto-saved",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1089 as *const std::ffi::c_void,
        1089,
    ),
    NativeSubr::new(
        c"do-auto-save",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_1090 as *const std::ffi::c_void,
        1090,
    ),
    NativeSubr::new(
        c"set-visited-file-modtime",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1091 as *const std::ffi::c_void,
        1091,
    ),
    NativeSubr::new(
        c"visited-file-modtime",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1092 as *const std::ffi::c_void,
        1092,
    ),
    NativeSubr::new(
        c"verify-visited-file-modtime",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1093 as *const std::ffi::c_void,
        1093,
    ),
    NativeSubr::new(
        c"car-less-than-car",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1094 as *const std::ffi::c_void,
        1094,
    ),
    NativeSubr::new(
        c"write-region",
        3,
        NativeMaxArgs::Fixed(7),
        native_subr_1095 as *const std::ffi::c_void,
        1095,
    ),
    NativeSubr::new(
        c"insert-file-contents",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_1096 as *const std::ffi::c_void,
        1096,
    ),
    NativeSubr::new(
        c"file-newer-than-file-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1097 as *const std::ffi::c_void,
        1097,
    ),
    NativeSubr::new(
        c"default-file-modes",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1098 as *const std::ffi::c_void,
        1098,
    ),
    NativeSubr::new(
        c"set-default-file-modes",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1099 as *const std::ffi::c_void,
        1099,
    ),
    NativeSubr::new(
        c"set-file-selinux-context",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1100 as *const std::ffi::c_void,
        1100,
    ),
    NativeSubr::new(
        c"set-file-acl",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1101 as *const std::ffi::c_void,
        1101,
    ),
    NativeSubr::new(
        c"file-acl",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1102 as *const std::ffi::c_void,
        1102,
    ),
    NativeSubr::new(
        c"file-selinux-context",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1103 as *const std::ffi::c_void,
        1103,
    ),
    NativeSubr::new(
        c"set-file-times",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1104 as *const std::ffi::c_void,
        1104,
    ),
    NativeSubr::new(
        c"set-file-modes",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1105 as *const std::ffi::c_void,
        1105,
    ),
    NativeSubr::new(
        c"file-modes",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1106 as *const std::ffi::c_void,
        1106,
    ),
    NativeSubr::new(
        c"file-regular-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1107 as *const std::ffi::c_void,
        1107,
    ),
    NativeSubr::new(
        c"file-accessible-directory-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1108 as *const std::ffi::c_void,
        1108,
    ),
    NativeSubr::new(
        c"file-directory-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1109 as *const std::ffi::c_void,
        1109,
    ),
    NativeSubr::new(
        c"file-symlink-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1110 as *const std::ffi::c_void,
        1110,
    ),
    NativeSubr::new(
        c"access-file",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1111 as *const std::ffi::c_void,
        1111,
    ),
    NativeSubr::new(
        c"file-writable-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1112 as *const std::ffi::c_void,
        1112,
    ),
    NativeSubr::new(
        c"file-readable-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1113 as *const std::ffi::c_void,
        1113,
    ),
    NativeSubr::new(
        c"file-executable-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1114 as *const std::ffi::c_void,
        1114,
    ),
    NativeSubr::new(
        c"file-exists-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1115 as *const std::ffi::c_void,
        1115,
    ),
    NativeSubr::new(
        c"file-name-absolute-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1116 as *const std::ffi::c_void,
        1116,
    ),
    NativeSubr::new(
        c"make-symbolic-link",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1117 as *const std::ffi::c_void,
        1117,
    ),
    NativeSubr::new(
        c"add-name-to-file",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1118 as *const std::ffi::c_void,
        1118,
    ),
    NativeSubr::new(
        c"rename-file",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1119 as *const std::ffi::c_void,
        1119,
    ),
    NativeSubr::new(
        c"file-name-case-insensitive-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1120 as *const std::ffi::c_void,
        1120,
    ),
    NativeSubr::new(
        c"delete-file-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1121 as *const std::ffi::c_void,
        1121,
    ),
    NativeSubr::new(
        c"delete-directory-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1122 as *const std::ffi::c_void,
        1122,
    ),
    NativeSubr::new(
        c"make-directory-internal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1123 as *const std::ffi::c_void,
        1123,
    ),
    NativeSubr::new(
        c"copy-file",
        2,
        NativeMaxArgs::Fixed(6),
        native_subr_1124 as *const std::ffi::c_void,
        1124,
    ),
    NativeSubr::new(
        c"substitute-in-file-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1125 as *const std::ffi::c_void,
        1125,
    ),
    NativeSubr::new(
        c"expand-file-name",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1126 as *const std::ffi::c_void,
        1126,
    ),
    NativeSubr::new(
        c"file-name-concat",
        1,
        NativeMaxArgs::Many,
        native_subr_1127 as *const std::ffi::c_void,
        1127,
    ),
    NativeSubr::new(
        c"make-temp-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1128 as *const std::ffi::c_void,
        1128,
    ),
    NativeSubr::new(
        c"make-temp-file-internal",
        4,
        NativeMaxArgs::Fixed(4),
        native_subr_1129 as *const std::ffi::c_void,
        1129,
    ),
    NativeSubr::new(
        c"directory-file-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1130 as *const std::ffi::c_void,
        1130,
    ),
    NativeSubr::new(
        c"directory-name-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1131 as *const std::ffi::c_void,
        1131,
    ),
    NativeSubr::new(
        c"file-name-as-directory",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1132 as *const std::ffi::c_void,
        1132,
    ),
    NativeSubr::new(
        c"unhandled-file-name-directory",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1133 as *const std::ffi::c_void,
        1133,
    ),
    NativeSubr::new(
        c"file-name-nondirectory",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1134 as *const std::ffi::c_void,
        1134,
    ),
    NativeSubr::new(
        c"file-name-directory",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1135 as *const std::ffi::c_void,
        1135,
    ),
    NativeSubr::new(
        c"find-file-name-handler",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1136 as *const std::ffi::c_void,
        1136,
    ),
    NativeSubr::new(
        c"buffer-line-statistics",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1137 as *const std::ffi::c_void,
        1137,
    ),
    NativeSubr::new(
        c"locale-info",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1138 as *const std::ffi::c_void,
        1138,
    ),
    NativeSubr::new(
        c"buffer-hash",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1139 as *const std::ffi::c_void,
        1139,
    ),
    NativeSubr::new(
        c"secure-hash",
        2,
        NativeMaxArgs::Fixed(5),
        native_subr_1140 as *const std::ffi::c_void,
        1140,
    ),
    NativeSubr::new(
        c"secure-hash-algorithms",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1141 as *const std::ffi::c_void,
        1141,
    ),
    NativeSubr::new(
        c"md5",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_1142 as *const std::ffi::c_void,
        1142,
    ),
    NativeSubr::new(
        c"base64url-encode-string",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1143 as *const std::ffi::c_void,
        1143,
    ),
    NativeSubr::new(
        c"base64url-encode-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1144 as *const std::ffi::c_void,
        1144,
    ),
    NativeSubr::new(
        c"base64-decode-string",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1145 as *const std::ffi::c_void,
        1145,
    ),
    NativeSubr::new(
        c"base64-encode-string",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1146 as *const std::ffi::c_void,
        1146,
    ),
    NativeSubr::new(
        c"base64-decode-region",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_1147 as *const std::ffi::c_void,
        1147,
    ),
    NativeSubr::new(
        c"base64-encode-region",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1148 as *const std::ffi::c_void,
        1148,
    ),
    NativeSubr::new(
        c"widget-apply",
        2,
        NativeMaxArgs::Many,
        native_subr_1149 as *const std::ffi::c_void,
        1149,
    ),
    NativeSubr::new(
        c"widget-get",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1150 as *const std::ffi::c_void,
        1150,
    ),
    NativeSubr::new(
        c"widget-put",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1151 as *const std::ffi::c_void,
        1151,
    ),
    NativeSubr::new(
        c"plist-member",
        2,
        NativeMaxArgs::Fixed(3),
        super::runtime::direct_native_plist_member as *const std::ffi::c_void,
        1152,
    ),
    NativeSubr::new(
        c"provide",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1153 as *const std::ffi::c_void,
        1153,
    ),
    NativeSubr::new(
        c"require",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1154 as *const std::ffi::c_void,
        1154,
    ),
    NativeSubr::new(
        c"featurep",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1155 as *const std::ffi::c_void,
        1155,
    ),
    NativeSubr::new(
        c"load-average",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1156 as *const std::ffi::c_void,
        1156,
    ),
    NativeSubr::new(
        c"yes-or-no-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1157 as *const std::ffi::c_void,
        1157,
    ),
    NativeSubr::new(
        c"mapconcat",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1158 as *const std::ffi::c_void,
        1158,
    ),
    NativeSubr::new(
        c"mapcan",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1159 as *const std::ffi::c_void,
        1159,
    ),
    NativeSubr::new(
        c"mapc",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1160 as *const std::ffi::c_void,
        1160,
    ),
    NativeSubr::new(
        c"mapcar",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1161 as *const std::ffi::c_void,
        1161,
    ),
    NativeSubr::new(
        c"nconc",
        0,
        NativeMaxArgs::Many,
        native_subr_1162 as *const std::ffi::c_void,
        1162,
    ),
    NativeSubr::new(
        c"clear-string",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1163 as *const std::ffi::c_void,
        1163,
    ),
    NativeSubr::new(
        c"fillarray",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1164 as *const std::ffi::c_void,
        1164,
    ),
    NativeSubr::new(
        c"value<",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1165 as *const std::ffi::c_void,
        1165,
    ),
    NativeSubr::new(
        c"equal-including-properties",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1166 as *const std::ffi::c_void,
        1166,
    ),
    NativeSubr::new(
        c"equal",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1167 as *const std::ffi::c_void,
        1167,
    ),
    NativeSubr::new(
        c"eql",
        2,
        NativeMaxArgs::Fixed(2),
        super::runtime::direct_native_eql as *const std::ffi::c_void,
        1168,
    ),
    NativeSubr::new(
        c"put",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1169 as *const std::ffi::c_void,
        1169,
    ),
    NativeSubr::new(
        c"plist-put",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_1170 as *const std::ffi::c_void,
        1170,
    ),
    NativeSubr::new(
        c"get",
        2,
        NativeMaxArgs::Fixed(2),
        super::runtime::direct_native_get as *const std::ffi::c_void,
        1171,
    ),
    NativeSubr::new(
        c"plist-get",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1172 as *const std::ffi::c_void,
        1172,
    ),
    NativeSubr::new(
        c"sort",
        1,
        NativeMaxArgs::Many,
        native_subr_1173 as *const std::ffi::c_void,
        1173,
    ),
    NativeSubr::new(
        c"reverse",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1174 as *const std::ffi::c_void,
        1174,
    ),
    NativeSubr::new(
        c"nreverse",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_nreverse as *const std::ffi::c_void,
        1175,
    ),
    NativeSubr::new(
        c"delete",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1176 as *const std::ffi::c_void,
        1176,
    ),
    NativeSubr::new(
        c"delq",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1177 as *const std::ffi::c_void,
        1177,
    ),
    NativeSubr::new(
        c"rassoc",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1178 as *const std::ffi::c_void,
        1178,
    ),
    NativeSubr::new(
        c"rassq",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1179 as *const std::ffi::c_void,
        1179,
    ),
    NativeSubr::new(
        c"assoc",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1180 as *const std::ffi::c_void,
        1180,
    ),
    NativeSubr::new(
        c"assq",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1181 as *const std::ffi::c_void,
        1181,
    ),
    NativeSubr::new(
        c"memql",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1182 as *const std::ffi::c_void,
        1182,
    ),
    NativeSubr::new(
        c"memq",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1183 as *const std::ffi::c_void,
        1183,
    ),
    NativeSubr::new(
        c"member",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1184 as *const std::ffi::c_void,
        1184,
    ),
    NativeSubr::new(
        c"elt",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1185 as *const std::ffi::c_void,
        1185,
    ),
    NativeSubr::new(
        c"nth",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1186 as *const std::ffi::c_void,
        1186,
    ),
    NativeSubr::new(
        c"nthcdr",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1187 as *const std::ffi::c_void,
        1187,
    ),
    NativeSubr::new(
        c"ntake",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1188 as *const std::ffi::c_void,
        1188,
    ),
    NativeSubr::new(
        c"take",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1189 as *const std::ffi::c_void,
        1189,
    ),
    NativeSubr::new(
        c"substring-no-properties",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1190 as *const std::ffi::c_void,
        1190,
    ),
    NativeSubr::new(
        c"substring",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1191 as *const std::ffi::c_void,
        1191,
    ),
    NativeSubr::new(
        c"copy-alist",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1192 as *const std::ffi::c_void,
        1192,
    ),
    NativeSubr::new(
        c"string-to-unibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1193 as *const std::ffi::c_void,
        1193,
    ),
    NativeSubr::new(
        c"string-to-multibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1194 as *const std::ffi::c_void,
        1194,
    ),
    NativeSubr::new(
        c"string-as-unibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1195 as *const std::ffi::c_void,
        1195,
    ),
    NativeSubr::new(
        c"string-as-multibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1196 as *const std::ffi::c_void,
        1196,
    ),
    NativeSubr::new(
        c"string-make-unibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1197 as *const std::ffi::c_void,
        1197,
    ),
    NativeSubr::new(
        c"string-make-multibyte",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1198 as *const std::ffi::c_void,
        1198,
    ),
    NativeSubr::new(
        c"copy-sequence",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1199 as *const std::ffi::c_void,
        1199,
    ),
    NativeSubr::new(
        c"vconcat",
        0,
        NativeMaxArgs::Many,
        native_subr_1200 as *const std::ffi::c_void,
        1200,
    ),
    NativeSubr::new(
        c"concat",
        0,
        NativeMaxArgs::Many,
        native_subr_1201 as *const std::ffi::c_void,
        1201,
    ),
    NativeSubr::new(
        c"append",
        0,
        NativeMaxArgs::Many,
        native_subr_1202 as *const std::ffi::c_void,
        1202,
    ),
    NativeSubr::new(
        c"string-collate-equalp",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_1203 as *const std::ffi::c_void,
        1203,
    ),
    NativeSubr::new(
        c"string-collate-lessp",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_1204 as *const std::ffi::c_void,
        1204,
    ),
    NativeSubr::new(
        c"string-version-lessp",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1205 as *const std::ffi::c_void,
        1205,
    ),
    NativeSubr::new(
        c"string-lessp",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1206 as *const std::ffi::c_void,
        1206,
    ),
    NativeSubr::new(
        c"compare-strings",
        6,
        NativeMaxArgs::Fixed(7),
        native_subr_1207 as *const std::ffi::c_void,
        1207,
    ),
    NativeSubr::new(
        c"string-equal",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1208 as *const std::ffi::c_void,
        1208,
    ),
    NativeSubr::new(
        c"string-distance",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1209 as *const std::ffi::c_void,
        1209,
    ),
    NativeSubr::new(
        c"string-bytes",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1210 as *const std::ffi::c_void,
        1210,
    ),
    NativeSubr::new(
        c"proper-list-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1211 as *const std::ffi::c_void,
        1211,
    ),
    NativeSubr::new(
        c"length=",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1212 as *const std::ffi::c_void,
        1212,
    ),
    NativeSubr::new(
        c"length>",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1213 as *const std::ffi::c_void,
        1213,
    ),
    NativeSubr::new(
        c"length<",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1214 as *const std::ffi::c_void,
        1214,
    ),
    NativeSubr::new(
        c"safe-length",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1215 as *const std::ffi::c_void,
        1215,
    ),
    NativeSubr::new(
        c"length",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_length as *const std::ffi::c_void,
        1216,
    ),
    NativeSubr::new(
        c"random",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1217 as *const std::ffi::c_void,
        1217,
    ),
    NativeSubr::new(
        c"identity",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_identity as *const std::ffi::c_void,
        1218,
    ),
    NativeSubr::new(
        c"line-number-at-pos",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_1219 as *const std::ffi::c_void,
        1219,
    ),
    NativeSubr::new(
        c"object-intervals",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1220 as *const std::ffi::c_void,
        1220,
    ),
    NativeSubr::new(
        c"string-search",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1221 as *const std::ffi::c_void,
        1221,
    ),
    NativeSubr::new(
        c"internal--hash-table-index-size",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1222 as *const std::ffi::c_void,
        1222,
    ),
    NativeSubr::new(
        c"internal--hash-table-buckets",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1223 as *const std::ffi::c_void,
        1223,
    ),
    NativeSubr::new(
        c"internal--hash-table-histogram",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1224 as *const std::ffi::c_void,
        1224,
    ),
    NativeSubr::new(
        c"define-hash-table-test",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1225 as *const std::ffi::c_void,
        1225,
    ),
    NativeSubr::new(
        c"maphash",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1226 as *const std::ffi::c_void,
        1226,
    ),
    NativeSubr::new(
        c"remhash",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1227 as *const std::ffi::c_void,
        1227,
    ),
    NativeSubr::new(
        c"puthash",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1228 as *const std::ffi::c_void,
        1228,
    ),
    NativeSubr::new(
        c"gethash",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1229 as *const std::ffi::c_void,
        1229,
    ),
    NativeSubr::new(
        c"clrhash",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1230 as *const std::ffi::c_void,
        1230,
    ),
    NativeSubr::new(
        c"hash-table-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1231 as *const std::ffi::c_void,
        1231,
    ),
    NativeSubr::new(
        c"hash-table-weakness",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1232 as *const std::ffi::c_void,
        1232,
    ),
    NativeSubr::new(
        c"hash-table-test",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1233 as *const std::ffi::c_void,
        1233,
    ),
    NativeSubr::new(
        c"hash-table-size",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1234 as *const std::ffi::c_void,
        1234,
    ),
    NativeSubr::new(
        c"hash-table-rehash-threshold",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1235 as *const std::ffi::c_void,
        1235,
    ),
    NativeSubr::new(
        c"hash-table-rehash-size",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1236 as *const std::ffi::c_void,
        1236,
    ),
    NativeSubr::new(
        c"hash-table-count",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1237 as *const std::ffi::c_void,
        1237,
    ),
    NativeSubr::new(
        c"copy-hash-table",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1238 as *const std::ffi::c_void,
        1238,
    ),
    NativeSubr::new(
        c"make-hash-table",
        0,
        NativeMaxArgs::Many,
        native_subr_1239 as *const std::ffi::c_void,
        1239,
    ),
    NativeSubr::new(
        c"sxhash-equal-including-properties",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1240 as *const std::ffi::c_void,
        1240,
    ),
    NativeSubr::new(
        c"sxhash-equal",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1241 as *const std::ffi::c_void,
        1241,
    ),
    NativeSubr::new(
        c"sxhash-eql",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1242 as *const std::ffi::c_void,
        1242,
    ),
    NativeSubr::new(
        c"sxhash-eq",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1243 as *const std::ffi::c_void,
        1243,
    ),
    NativeSubr::new(
        c"get-variable-watchers",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1244 as *const std::ffi::c_void,
        1244,
    ),
    NativeSubr::new(
        c"remove-variable-watcher",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1245 as *const std::ffi::c_void,
        1245,
    ),
    NativeSubr::new(
        c"add-variable-watcher",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1246 as *const std::ffi::c_void,
        1246,
    ),
    NativeSubr::new(
        c"bool-vector-count-population",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1247 as *const std::ffi::c_void,
        1247,
    ),
    NativeSubr::new(
        c"bool-vector-count-consecutive",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1248 as *const std::ffi::c_void,
        1248,
    ),
    NativeSubr::new(
        c"bool-vector-subsetp",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1249 as *const std::ffi::c_void,
        1249,
    ),
    NativeSubr::new(
        c"bool-vector-not",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1250 as *const std::ffi::c_void,
        1250,
    ),
    NativeSubr::new(
        c"bool-vector-set-difference",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1251 as *const std::ffi::c_void,
        1251,
    ),
    NativeSubr::new(
        c"bool-vector-intersection",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1252 as *const std::ffi::c_void,
        1252,
    ),
    NativeSubr::new(
        c"bool-vector-union",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1253 as *const std::ffi::c_void,
        1253,
    ),
    NativeSubr::new(
        c"bool-vector-exclusive-or",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1254 as *const std::ffi::c_void,
        1254,
    ),
    NativeSubr::new(
        c"user-ptrp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1255 as *const std::ffi::c_void,
        1255,
    ),
    NativeSubr::new(
        c"native-comp-unit-set-file",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1256 as *const std::ffi::c_void,
        1256,
    ),
    NativeSubr::new(
        c"native-comp-unit-file",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1257 as *const std::ffi::c_void,
        1257,
    ),
    NativeSubr::new(
        c"subr-native-comp-unit",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1258 as *const std::ffi::c_void,
        1258,
    ),
    NativeSubr::new(
        c"subr-type",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1259 as *const std::ffi::c_void,
        1259,
    ),
    NativeSubr::new(
        c"subr-native-lambda-list",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1260 as *const std::ffi::c_void,
        1260,
    ),
    NativeSubr::new(
        c"native-comp-function-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1261 as *const std::ffi::c_void,
        1261,
    ),
    NativeSubr::new(
        c"subr-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1262 as *const std::ffi::c_void,
        1262,
    ),
    NativeSubr::new(
        c"subr-arity",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1263 as *const std::ffi::c_void,
        1263,
    ),
    NativeSubr::new(
        c"byteorder",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1264 as *const std::ffi::c_void,
        1264,
    ),
    NativeSubr::new(
        c"lognot",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1265 as *const std::ffi::c_void,
        1265,
    ),
    NativeSubr::new(
        c"1-",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1266 as *const std::ffi::c_void,
        1266,
    ),
    NativeSubr::new(
        c"1+",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1267 as *const std::ffi::c_void,
        1267,
    ),
    NativeSubr::new(
        c"ash",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1268 as *const std::ffi::c_void,
        1268,
    ),
    NativeSubr::new(
        c"logcount",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1269 as *const std::ffi::c_void,
        1269,
    ),
    NativeSubr::new(
        c"logxor",
        0,
        NativeMaxArgs::Many,
        native_subr_1270 as *const std::ffi::c_void,
        1270,
    ),
    NativeSubr::new(
        c"logior",
        0,
        NativeMaxArgs::Many,
        native_subr_1271 as *const std::ffi::c_void,
        1271,
    ),
    NativeSubr::new(
        c"logand",
        0,
        NativeMaxArgs::Many,
        native_subr_1272 as *const std::ffi::c_void,
        1272,
    ),
    NativeSubr::new(
        c"min",
        1,
        NativeMaxArgs::Many,
        native_subr_1273 as *const std::ffi::c_void,
        1273,
    ),
    NativeSubr::new(
        c"max",
        1,
        NativeMaxArgs::Many,
        native_subr_1274 as *const std::ffi::c_void,
        1274,
    ),
    NativeSubr::new(
        c"mod",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1275 as *const std::ffi::c_void,
        1275,
    ),
    NativeSubr::new(
        c"%",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1276 as *const std::ffi::c_void,
        1276,
    ),
    NativeSubr::new(
        c"/",
        1,
        NativeMaxArgs::Many,
        native_subr_1277 as *const std::ffi::c_void,
        1277,
    ),
    NativeSubr::new(
        c"*",
        0,
        NativeMaxArgs::Many,
        native_subr_1278 as *const std::ffi::c_void,
        1278,
    ),
    NativeSubr::new(
        c"-",
        0,
        NativeMaxArgs::Many,
        native_subr_1279 as *const std::ffi::c_void,
        1279,
    ),
    NativeSubr::new(
        c"+",
        0,
        NativeMaxArgs::Many,
        native_subr_1280 as *const std::ffi::c_void,
        1280,
    ),
    NativeSubr::new(
        c"/=",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1281 as *const std::ffi::c_void,
        1281,
    ),
    NativeSubr::new(
        c">=",
        1,
        NativeMaxArgs::Many,
        native_subr_1282 as *const std::ffi::c_void,
        1282,
    ),
    NativeSubr::new(
        c"<=",
        1,
        NativeMaxArgs::Many,
        native_subr_1283 as *const std::ffi::c_void,
        1283,
    ),
    NativeSubr::new(
        c">",
        1,
        NativeMaxArgs::Many,
        native_subr_1284 as *const std::ffi::c_void,
        1284,
    ),
    NativeSubr::new(
        c"<",
        1,
        NativeMaxArgs::Many,
        native_subr_1285 as *const std::ffi::c_void,
        1285,
    ),
    NativeSubr::new(
        c"=",
        1,
        NativeMaxArgs::Many,
        native_subr_1286 as *const std::ffi::c_void,
        1286,
    ),
    NativeSubr::new(
        c"string-to-number",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1287 as *const std::ffi::c_void,
        1287,
    ),
    NativeSubr::new(
        c"number-to-string",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1288 as *const std::ffi::c_void,
        1288,
    ),
    NativeSubr::new(
        c"aset",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1289 as *const std::ffi::c_void,
        1289,
    ),
    NativeSubr::new(
        c"aref",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1290 as *const std::ffi::c_void,
        1290,
    ),
    NativeSubr::new(
        c"variable-binding-locus",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1291 as *const std::ffi::c_void,
        1291,
    ),
    NativeSubr::new(
        c"local-variable-if-set-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1292 as *const std::ffi::c_void,
        1292,
    ),
    NativeSubr::new(
        c"local-variable-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1293 as *const std::ffi::c_void,
        1293,
    ),
    NativeSubr::new(
        c"kill-local-variable",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1294 as *const std::ffi::c_void,
        1294,
    ),
    NativeSubr::new(
        c"make-local-variable",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1295 as *const std::ffi::c_void,
        1295,
    ),
    NativeSubr::new(
        c"make-variable-buffer-local",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1296 as *const std::ffi::c_void,
        1296,
    ),
    NativeSubr::new(
        c"set-default",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1297 as *const std::ffi::c_void,
        1297,
    ),
    NativeSubr::new(
        c"default-value",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1298 as *const std::ffi::c_void,
        1298,
    ),
    NativeSubr::new(
        c"default-boundp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1299 as *const std::ffi::c_void,
        1299,
    ),
    NativeSubr::new(
        c"set",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1300 as *const std::ffi::c_void,
        1300,
    ),
    NativeSubr::new(
        c"symbol-value",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_symbol_value as *const std::ffi::c_void,
        1301,
    ),
    NativeSubr::new(
        c"setplist",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1302 as *const std::ffi::c_void,
        1302,
    ),
    NativeSubr::new(
        c"defalias",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1303 as *const std::ffi::c_void,
        1303,
    ),
    NativeSubr::new(
        c"fset",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1304 as *const std::ffi::c_void,
        1304,
    ),
    NativeSubr::new(
        c"fboundp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1305 as *const std::ffi::c_void,
        1305,
    ),
    NativeSubr::new(
        c"boundp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1306 as *const std::ffi::c_void,
        1306,
    ),
    NativeSubr::new(
        c"fmakunbound",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1307 as *const std::ffi::c_void,
        1307,
    ),
    NativeSubr::new(
        c"makunbound",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1308 as *const std::ffi::c_void,
        1308,
    ),
    NativeSubr::new(
        c"position-symbol",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1309 as *const std::ffi::c_void,
        1309,
    ),
    NativeSubr::new(
        c"remove-pos-from-symbol",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1310 as *const std::ffi::c_void,
        1310,
    ),
    NativeSubr::new(
        c"symbol-with-pos-pos",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1311 as *const std::ffi::c_void,
        1311,
    ),
    NativeSubr::new(
        c"bare-symbol",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1312 as *const std::ffi::c_void,
        1312,
    ),
    NativeSubr::new(
        c"symbol-name",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1313 as *const std::ffi::c_void,
        1313,
    ),
    NativeSubr::new(
        c"symbol-plist",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1314 as *const std::ffi::c_void,
        1314,
    ),
    NativeSubr::new(
        c"indirect-function",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1315 as *const std::ffi::c_void,
        1315,
    ),
    NativeSubr::new(
        c"symbol-function",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1316 as *const std::ffi::c_void,
        1316,
    ),
    NativeSubr::new(
        c"setcdr",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1317 as *const std::ffi::c_void,
        1317,
    ),
    NativeSubr::new(
        c"setcar",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1318 as *const std::ffi::c_void,
        1318,
    ),
    NativeSubr::new(
        c"cdr-safe",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_cdr_safe as *const std::ffi::c_void,
        1319,
    ),
    NativeSubr::new(
        c"car-safe",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_car_safe as *const std::ffi::c_void,
        1320,
    ),
    NativeSubr::new(
        c"cdr",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_cdr as *const std::ffi::c_void,
        1321,
    ),
    NativeSubr::new(
        c"car",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_car as *const std::ffi::c_void,
        1322,
    ),
    NativeSubr::new(
        c"condition-variable-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1323 as *const std::ffi::c_void,
        1323,
    ),
    NativeSubr::new(
        c"mutexp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1324 as *const std::ffi::c_void,
        1324,
    ),
    NativeSubr::new(
        c"threadp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1325 as *const std::ffi::c_void,
        1325,
    ),
    NativeSubr::new(
        c"char-or-string-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1326 as *const std::ffi::c_void,
        1326,
    ),
    NativeSubr::new(
        c"module-function-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1327 as *const std::ffi::c_void,
        1327,
    ),
    NativeSubr::new(
        c"closurep",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1328 as *const std::ffi::c_void,
        1328,
    ),
    NativeSubr::new(
        c"interpreted-function-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1329 as *const std::ffi::c_void,
        1329,
    ),
    NativeSubr::new(
        c"byte-code-function-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1330 as *const std::ffi::c_void,
        1330,
    ),
    NativeSubr::new(
        c"subrp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1331 as *const std::ffi::c_void,
        1331,
    ),
    NativeSubr::new(
        c"markerp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1332 as *const std::ffi::c_void,
        1332,
    ),
    NativeSubr::new(
        c"bufferp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1333 as *const std::ffi::c_void,
        1333,
    ),
    NativeSubr::new(
        c"sequencep",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1334 as *const std::ffi::c_void,
        1334,
    ),
    NativeSubr::new(
        c"arrayp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1335 as *const std::ffi::c_void,
        1335,
    ),
    NativeSubr::new(
        c"bool-vector-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1336 as *const std::ffi::c_void,
        1336,
    ),
    NativeSubr::new(
        c"vector-or-char-table-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1337 as *const std::ffi::c_void,
        1337,
    ),
    NativeSubr::new(
        c"char-table-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1338 as *const std::ffi::c_void,
        1338,
    ),
    NativeSubr::new(
        c"recordp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1339 as *const std::ffi::c_void,
        1339,
    ),
    NativeSubr::new(
        c"vectorp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1340 as *const std::ffi::c_void,
        1340,
    ),
    NativeSubr::new(
        c"multibyte-string-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1341 as *const std::ffi::c_void,
        1341,
    ),
    NativeSubr::new(
        c"stringp",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_stringp as *const std::ffi::c_void,
        1342,
    ),
    NativeSubr::new(
        c"keywordp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1343 as *const std::ffi::c_void,
        1343,
    ),
    NativeSubr::new(
        c"symbolp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1344 as *const std::ffi::c_void,
        1344,
    ),
    NativeSubr::new(
        c"symbol-with-pos-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1345 as *const std::ffi::c_void,
        1345,
    ),
    NativeSubr::new(
        c"bare-symbol-p",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_bare_symbol_p as *const std::ffi::c_void,
        1346,
    ),
    NativeSubr::new(
        c"natnump",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1347 as *const std::ffi::c_void,
        1347,
    ),
    NativeSubr::new(
        c"floatp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1348 as *const std::ffi::c_void,
        1348,
    ),
    NativeSubr::new(
        c"number-or-marker-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1349 as *const std::ffi::c_void,
        1349,
    ),
    NativeSubr::new(
        c"numberp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1350 as *const std::ffi::c_void,
        1350,
    ),
    NativeSubr::new(
        c"integer-or-marker-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1351 as *const std::ffi::c_void,
        1351,
    ),
    NativeSubr::new(
        c"integerp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1352 as *const std::ffi::c_void,
        1352,
    ),
    NativeSubr::new(
        c"atom",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_atom as *const std::ffi::c_void,
        1353,
    ),
    NativeSubr::new(
        c"consp",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_consp as *const std::ffi::c_void,
        1354,
    ),
    NativeSubr::new(
        c"nlistp",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_nlistp as *const std::ffi::c_void,
        1355,
    ),
    NativeSubr::new(
        c"listp",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_listp as *const std::ffi::c_void,
        1356,
    ),
    NativeSubr::new(
        c"cl-type-of",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1357 as *const std::ffi::c_void,
        1357,
    ),
    NativeSubr::new(
        c"type-of",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_type_of as *const std::ffi::c_void,
        1358,
    ),
    NativeSubr::new(
        c"null",
        1,
        NativeMaxArgs::Fixed(1),
        super::runtime::direct_native_null as *const std::ffi::c_void,
        1359,
    ),
    NativeSubr::new(
        c"eq",
        2,
        NativeMaxArgs::Fixed(2),
        super::runtime::direct_native_eq as *const std::ffi::c_void,
        1360,
    ),
    NativeSubr::new(
        c"command-modes",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1361 as *const std::ffi::c_void,
        1361,
    ),
    NativeSubr::new(
        c"interactive-form",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1362 as *const std::ffi::c_void,
        1362,
    ),
    NativeSubr::new(
        c"indirect-variable",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1363 as *const std::ffi::c_void,
        1363,
    ),
    NativeSubr::new(
        c"posn-at-x-y",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_1364 as *const std::ffi::c_void,
        1364,
    ),
    NativeSubr::new(
        c"posn-at-point",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_1365 as *const std::ffi::c_void,
        1365,
    ),
    NativeSubr::new(
        c"current-input-mode",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1366 as *const std::ffi::c_void,
        1366,
    ),
    NativeSubr::new(
        c"set-input-mode",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_1367 as *const std::ffi::c_void,
        1367,
    ),
    NativeSubr::new(
        c"set-quit-char",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1368 as *const std::ffi::c_void,
        1368,
    ),
    NativeSubr::new(
        c"set-input-meta-mode",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1369 as *const std::ffi::c_void,
        1369,
    ),
    NativeSubr::new(
        c"set-output-flow-control",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1370 as *const std::ffi::c_void,
        1370,
    ),
    NativeSubr::new(
        c"set-input-interrupt-mode",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1371 as *const std::ffi::c_void,
        1371,
    ),
    NativeSubr::new(
        c"open-dribble-file",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1372 as *const std::ffi::c_void,
        1372,
    ),
    NativeSubr::new(
        c"discard-input",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1373 as *const std::ffi::c_void,
        1373,
    ),
    NativeSubr::new(
        c"top-level",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1374 as *const std::ffi::c_void,
        1374,
    ),
    NativeSubr::new(
        c"command-error-default-function",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1375 as *const std::ffi::c_void,
        1375,
    ),
    NativeSubr::new(
        c"recursion-depth",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1376 as *const std::ffi::c_void,
        1376,
    ),
    NativeSubr::new(
        c"exit-recursive-edit",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1377 as *const std::ffi::c_void,
        1377,
    ),
    NativeSubr::new(
        c"abort-recursive-edit",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1378 as *const std::ffi::c_void,
        1378,
    ),
    NativeSubr::new(
        c"suspend-emacs",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1379 as *const std::ffi::c_void,
        1379,
    ),
    NativeSubr::new(
        c"clear-this-command-keys",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1380 as *const std::ffi::c_void,
        1380,
    ),
    NativeSubr::new(
        c"set--this-command-keys",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1381 as *const std::ffi::c_void,
        1381,
    ),
    NativeSubr::new(
        c"this-single-command-raw-keys",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1382 as *const std::ffi::c_void,
        1382,
    ),
    NativeSubr::new(
        c"this-single-command-keys",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1383 as *const std::ffi::c_void,
        1383,
    ),
    NativeSubr::new(
        c"this-command-keys-vector",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1384 as *const std::ffi::c_void,
        1384,
    ),
    NativeSubr::new(
        c"this-command-keys",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1385 as *const std::ffi::c_void,
        1385,
    ),
    NativeSubr::new(
        c"recent-keys",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1386 as *const std::ffi::c_void,
        1386,
    ),
    NativeSubr::new(
        c"lossage-size",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1387 as *const std::ffi::c_void,
        1387,
    ),
    NativeSubr::new(
        c"input-pending-p",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1388 as *const std::ffi::c_void,
        1388,
    ),
    NativeSubr::new(
        c"internal--track-mouse",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1389 as *const std::ffi::c_void,
        1389,
    ),
    NativeSubr::new(
        c"recursive-edit",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1390 as *const std::ffi::c_void,
        1390,
    ),
    NativeSubr::new(
        c"read-key-sequence-vector",
        1,
        NativeMaxArgs::Fixed(6),
        native_subr_1391 as *const std::ffi::c_void,
        1391,
    ),
    NativeSubr::new(
        c"read-key-sequence",
        1,
        NativeMaxArgs::Fixed(6),
        native_subr_1392 as *const std::ffi::c_void,
        1392,
    ),
    NativeSubr::new(
        c"internal-handle-focus-in",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1393 as *const std::ffi::c_void,
        1393,
    ),
    NativeSubr::new(
        c"event-convert-list",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1394 as *const std::ffi::c_void,
        1394,
    ),
    NativeSubr::new(
        c"internal-event-symbol-parse-modifiers",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1395 as *const std::ffi::c_void,
        1395,
    ),
    NativeSubr::new(
        c"current-idle-time",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1396 as *const std::ffi::c_void,
        1396,
    ),
    NativeSubr::new(
        c"describe-buffer-bindings",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1397 as *const std::ffi::c_void,
        1397,
    ),
    NativeSubr::new(
        c"where-is-internal",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_1398 as *const std::ffi::c_void,
        1398,
    ),
    NativeSubr::new(
        c"text-char-description",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1399 as *const std::ffi::c_void,
        1399,
    ),
    NativeSubr::new(
        c"single-key-description",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1400 as *const std::ffi::c_void,
        1400,
    ),
    NativeSubr::new(
        c"describe-vector",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1401 as *const std::ffi::c_void,
        1401,
    ),
    NativeSubr::new(
        c"help--describe-vector",
        7,
        NativeMaxArgs::Fixed(7),
        native_subr_1402 as *const std::ffi::c_void,
        1402,
    ),
    NativeSubr::new(
        c"keymap--get-keyelt",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1403 as *const std::ffi::c_void,
        1403,
    ),
    NativeSubr::new(
        c"key-description",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1404 as *const std::ffi::c_void,
        1404,
    ),
    NativeSubr::new(
        c"accessible-keymaps",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1405 as *const std::ffi::c_void,
        1405,
    ),
    NativeSubr::new(
        c"current-active-maps",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_1406 as *const std::ffi::c_void,
        1406,
    ),
    NativeSubr::new(
        c"current-minor-mode-maps",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1407 as *const std::ffi::c_void,
        1407,
    ),
    NativeSubr::new(
        c"current-global-map",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1408 as *const std::ffi::c_void,
        1408,
    ),
    NativeSubr::new(
        c"current-local-map",
        0,
        NativeMaxArgs::Fixed(0),
        native_subr_1409 as *const std::ffi::c_void,
        1409,
    ),
    NativeSubr::new(
        c"use-local-map",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1410 as *const std::ffi::c_void,
        1410,
    ),
    NativeSubr::new(
        c"use-global-map",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1411 as *const std::ffi::c_void,
        1411,
    ),
    NativeSubr::new(
        c"lookup-key",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1412 as *const std::ffi::c_void,
        1412,
    ),
    NativeSubr::new(
        c"define-key",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_1413 as *const std::ffi::c_void,
        1413,
    ),
    NativeSubr::new(
        c"minor-mode-key-binding",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1414 as *const std::ffi::c_void,
        1414,
    ),
    NativeSubr::new(
        c"key-binding",
        1,
        NativeMaxArgs::Fixed(4),
        native_subr_1415 as *const std::ffi::c_void,
        1415,
    ),
    NativeSubr::new(
        c"command-remapping",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1416 as *const std::ffi::c_void,
        1416,
    ),
    NativeSubr::new(
        c"copy-keymap",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1417 as *const std::ffi::c_void,
        1417,
    ),
    NativeSubr::new(
        c"map-keymap",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1418 as *const std::ffi::c_void,
        1418,
    ),
    NativeSubr::new(
        c"map-keymap-internal",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1419 as *const std::ffi::c_void,
        1419,
    ),
    NativeSubr::new(
        c"make-sparse-keymap",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1420 as *const std::ffi::c_void,
        1420,
    ),
    NativeSubr::new(
        c"make-keymap",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1421 as *const std::ffi::c_void,
        1421,
    ),
    NativeSubr::new(
        c"set-keymap-parent",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1422 as *const std::ffi::c_void,
        1422,
    ),
    NativeSubr::new(
        c"keymap-prompt",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1423 as *const std::ffi::c_void,
        1423,
    ),
    NativeSubr::new(
        c"keymap-parent",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1424 as *const std::ffi::c_void,
        1424,
    ),
    NativeSubr::new(
        c"keymapp",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1425 as *const std::ffi::c_void,
        1425,
    ),
    NativeSubr::new(
        c"color-values-from-color-spec",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1426 as *const std::ffi::c_void,
        1426,
    ),
    NativeSubr::new(
        c"x-family-fonts",
        0,
        NativeMaxArgs::Fixed(2),
        native_subr_1427 as *const std::ffi::c_void,
        1427,
    ),
    NativeSubr::new(
        c"internal-face-x-get-resource",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1428 as *const std::ffi::c_void,
        1428,
    ),
    NativeSubr::new(
        c"x-list-fonts",
        1,
        NativeMaxArgs::Fixed(5),
        native_subr_1429 as *const std::ffi::c_void,
        1429,
    ),
    NativeSubr::new(
        c"bitmap-spec-p",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1430 as *const std::ffi::c_void,
        1430,
    ),
    NativeSubr::new(
        c"tty-suppress-bold-inverse-default-colors",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1431 as *const std::ffi::c_void,
        1431,
    ),
    NativeSubr::new(
        c"clear-face-cache",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1432 as *const std::ffi::c_void,
        1432,
    ),
    NativeSubr::new(
        c"face-attributes-as-vector",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1433 as *const std::ffi::c_void,
        1433,
    ),
    NativeSubr::new(
        c"internal-set-alternative-font-registry-alist",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1434 as *const std::ffi::c_void,
        1434,
    ),
    NativeSubr::new(
        c"internal-set-alternative-font-family-alist",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1435 as *const std::ffi::c_void,
        1435,
    ),
    NativeSubr::new(
        c"internal-set-font-selection-order",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1436 as *const std::ffi::c_void,
        1436,
    ),
    NativeSubr::new(
        c"color-distance",
        2,
        NativeMaxArgs::Fixed(4),
        native_subr_1437 as *const std::ffi::c_void,
        1437,
    ),
    NativeSubr::new(
        c"display-supports-face-attributes-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1438 as *const std::ffi::c_void,
        1438,
    ),
    NativeSubr::new(
        c"frame--face-hash-table",
        0,
        NativeMaxArgs::Fixed(1),
        native_subr_1439 as *const std::ffi::c_void,
        1439,
    ),
    NativeSubr::new(
        c"face-font",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1440 as *const std::ffi::c_void,
        1440,
    ),
    NativeSubr::new(
        c"internal-merge-in-global-face",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1441 as *const std::ffi::c_void,
        1441,
    ),
    NativeSubr::new(
        c"internal-copy-lisp-face",
        4,
        NativeMaxArgs::Fixed(4),
        native_subr_1442 as *const std::ffi::c_void,
        1442,
    ),
    NativeSubr::new(
        c"internal-lisp-face-empty-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1443 as *const std::ffi::c_void,
        1443,
    ),
    NativeSubr::new(
        c"internal-lisp-face-equal-p",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1444 as *const std::ffi::c_void,
        1444,
    ),
    NativeSubr::new(
        c"internal-lisp-face-attribute-values",
        1,
        NativeMaxArgs::Fixed(1),
        native_subr_1445 as *const std::ffi::c_void,
        1445,
    ),
    NativeSubr::new(
        c"internal-get-lisp-face-attribute",
        2,
        NativeMaxArgs::Fixed(3),
        native_subr_1446 as *const std::ffi::c_void,
        1446,
    ),
    NativeSubr::new(
        c"merge-face-attribute",
        3,
        NativeMaxArgs::Fixed(3),
        native_subr_1447 as *const std::ffi::c_void,
        1447,
    ),
    NativeSubr::new(
        c"face-attribute-relative-p",
        2,
        NativeMaxArgs::Fixed(2),
        native_subr_1448 as *const std::ffi::c_void,
        1448,
    ),
    NativeSubr::new(
        c"color-supported-p",
        1,
        NativeMaxArgs::Fixed(3),
        native_subr_1449 as *const std::ffi::c_void,
        1449,
    ),
    NativeSubr::new(
        c"color-gray-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1450 as *const std::ffi::c_void,
        1450,
    ),
    NativeSubr::new(
        c"internal-set-lisp-face-attribute-from-resource",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_1451 as *const std::ffi::c_void,
        1451,
    ),
    NativeSubr::new(
        c"internal-set-lisp-face-attribute",
        3,
        NativeMaxArgs::Fixed(4),
        native_subr_1452 as *const std::ffi::c_void,
        1452,
    ),
    NativeSubr::new(
        c"internal-lisp-face-p",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1453 as *const std::ffi::c_void,
        1453,
    ),
    NativeSubr::new(
        c"internal-make-lisp-face",
        1,
        NativeMaxArgs::Fixed(2),
        native_subr_1454 as *const std::ffi::c_void,
        1454,
    ),
];

pub(crate) fn native_subr_address(index: usize) -> *mut std::ffi::c_void {
    NATIVE_SUBRS
        .get(index)
        .map_or(std::ptr::null_mut(), |subr| subr.function.cast_mut())
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

extern "C" fn native_subr_1217(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1217, &[arg_0])
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

extern "C" fn native_subr_1343(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1343, &[arg_0])
}

extern "C" fn native_subr_1344(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1344, &[arg_0])
}

extern "C" fn native_subr_1345(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1345, &[arg_0])
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

extern "C" fn native_subr_1357(arg_0: super::runtime::NativeWord) -> super::runtime::NativeWord {
    super::runtime::invoke_subr(1357, &[arg_0])
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
