use super::{CharTableEntry, CodingSystemState};
use crate::lisp::types::Value;

fn coding_plist(mnemonic: char, extras: impl IntoIterator<Item = (String, Value)>) -> Value {
    let mut items = vec![
        Value::Symbol(":mnemonic".into()),
        Value::Integer(mnemonic as i64),
    ];
    for (key, value) in extras {
        items.push(Value::Symbol(key));
        items.push(value);
    }
    Value::list(items)
}

fn syntax_spec_value(spec: &str) -> Value {
    Value::String(spec.to_string())
}

pub(super) fn default_mode_line_format() -> Value {
    let symbol = |name: &str| Value::Symbol(name.into());
    Value::list([
        Value::String("%e".into()),
        symbol("mode-line-front-space"),
        Value::list([
            symbol(":propertize"),
            Value::list([
                Value::String(String::new()),
                symbol("mode-line-mule-info"),
                symbol("mode-line-client"),
                symbol("mode-line-modified"),
                symbol("mode-line-remote"),
                symbol("mode-line-window-dedicated"),
            ]),
            symbol("display"),
            Value::list([symbol("min-width"), Value::list([Value::Float(6.0)])]),
        ]),
        symbol("mode-line-frame-identification"),
        symbol("mode-line-buffer-identification"),
        Value::String("   ".into()),
        symbol("mode-line-position"),
        Value::list([
            symbol("project-mode-line"),
            symbol("project-mode-line-format"),
        ]),
        Value::list([symbol("vc-mode"), symbol("vc-mode")]),
        Value::String("  ".into()),
        symbol("mode-line-modes"),
        symbol("mode-line-misc-info"),
        symbol("mode-line-end-spaces"),
    ])
}

pub(super) fn standard_syntax_table_entries() -> Vec<CharTableEntry> {
    vec![
        CharTableEntry {
            start: ' ' as u32,
            end: ' ' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '\t' as u32,
            end: '\t' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '\n' as u32,
            end: '\n' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '\r' as u32,
            end: '\r' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '\u{0c}' as u32,
            end: '\u{0c}' as u32,
            value: syntax_spec_value(" "),
        },
        CharTableEntry {
            start: '_' as u32,
            end: '_' as u32,
            value: syntax_spec_value("_"),
        },
        CharTableEntry {
            start: '\\' as u32,
            end: '\\' as u32,
            value: syntax_spec_value("\\"),
        },
        CharTableEntry {
            start: '\'' as u32,
            end: '\'' as u32,
            value: syntax_spec_value("'"),
        },
        CharTableEntry {
            start: '"' as u32,
            end: '"' as u32,
            value: syntax_spec_value("\""),
        },
        CharTableEntry {
            start: '(' as u32,
            end: '(' as u32,
            value: syntax_spec_value("()"),
        },
        CharTableEntry {
            start: ')' as u32,
            end: ')' as u32,
            value: syntax_spec_value(")("),
        },
        CharTableEntry {
            start: '[' as u32,
            end: '[' as u32,
            value: syntax_spec_value("(]"),
        },
        CharTableEntry {
            start: ']' as u32,
            end: ']' as u32,
            value: syntax_spec_value(")["),
        },
        CharTableEntry {
            start: '{' as u32,
            end: '{' as u32,
            value: syntax_spec_value("(}"),
        },
        CharTableEntry {
            start: '}' as u32,
            end: '}' as u32,
            value: syntax_spec_value("){"),
        },
    ]
}

/// GNU lisp-data-mode-syntax-table (lisp-mode.el): every non-alphanumeric
/// ASCII character is a symbol constituent unless overridden below (Lisp
/// symbols carry -, ., {, } and friends).  Lookup is last-entry-wins, so
/// the specific overrides follow the symbol-constituent ranges.
pub(super) fn lisp_data_syntax_table_entries() -> Vec<CharTableEntry> {
    let mut entries: Vec<CharTableEntry> = [(0u32, 47u32), (58, 64), (91, 96), (123, 127)]
        .into_iter()
        .map(|(start, end)| CharTableEntry {
            start,
            end,
            value: syntax_spec_value("_"),
        })
        .collect();
    for ch in [' ', '\t', '\x0c', '\u{a0}'] {
        entries.push(CharTableEntry {
            start: ch as u32,
            end: ch as u32,
            value: syntax_spec_value(" "),
        });
    }
    for (ch, spec) in [
        ('\n', ">"),
        (';', "<"),
        ('`', "'"),
        ('\'', "'"),
        (',', "'"),
        ('#', "'"),
        ('@', "_ p"),
        ('"', "\""),
        ('\\', "\\"),
        ('(', "()"),
        (')', ")("),
        ('[', "(]"),
        (']', ")["),
    ] {
        entries.push(CharTableEntry {
            start: ch as u32,
            end: ch as u32,
            value: syntax_spec_value(spec),
        });
    }
    entries
}

pub(super) fn current_exec_path() -> Value {
    match std::env::var_os("PATH") {
        Some(path) => Value::list(
            std::env::split_paths(&path).map(|entry| Value::String(entry.display().to_string())),
        ),
        None => Value::Nil,
    }
}

pub(super) fn tab_bar_new_tab_choice_custom_type() -> Value {
    Value::list([
        Value::Symbol("choice".into()),
        Value::list([
            Value::Symbol("const".into()),
            Value::Symbol(":tag".into()),
            Value::String("Current buffer".into()),
            Value::T,
        ]),
        Value::list([
            Value::Symbol("const".into()),
            Value::Symbol(":tag".into()),
            Value::String("Current window".into()),
            Value::Symbol("window".into()),
        ]),
        Value::list([
            Value::Symbol("string".into()),
            Value::Symbol(":tag".into()),
            Value::String("Buffer".into()),
            Value::String("*scratch*".into()),
        ]),
        Value::list([
            Value::Symbol("directory".into()),
            Value::Symbol(":tag".into()),
            Value::String("Directory".into()),
            Value::Symbol(":value".into()),
            Value::String("~/".into()),
        ]),
        Value::list([
            Value::Symbol("file".into()),
            Value::Symbol(":tag".into()),
            Value::String("File".into()),
            Value::Symbol(":value".into()),
            Value::String("~/.emacs".into()),
        ]),
        Value::list([
            Value::Symbol("function".into()),
            Value::Symbol(":tag".into()),
            Value::String("Function".into()),
        ]),
        Value::list([
            Value::Symbol("const".into()),
            Value::Symbol(":tag".into()),
            Value::String("Duplicate tab".into()),
            Value::Symbol("clone".into()),
        ]),
    ])
}

pub(super) fn builtin_coding_systems() -> Vec<CodingSystemState> {
    let mut systems = vec![
        CodingSystemState {
            name: "undecided".into(),
            base: "undecided".into(),
            kind: "undecided".into(),
            eol_type: None,
            plist: coding_plist('?', std::iter::empty()),
        },
        CodingSystemState {
            name: "undecided-unix".into(),
            base: "undecided".into(),
            kind: "undecided".into(),
            eol_type: Some(0),
            plist: coding_plist('?', std::iter::empty()),
        },
        CodingSystemState {
            name: "undecided-dos".into(),
            base: "undecided".into(),
            kind: "undecided".into(),
            eol_type: Some(1),
            plist: coding_plist('?', std::iter::empty()),
        },
        CodingSystemState {
            name: "undecided-mac".into(),
            base: "undecided".into(),
            kind: "undecided".into(),
            eol_type: Some(2),
            plist: coding_plist('?', std::iter::empty()),
        },
        CodingSystemState {
            name: "no-conversion".into(),
            base: "no-conversion".into(),
            kind: "raw-text".into(),
            eol_type: None,
            plist: coding_plist('=', std::iter::empty()),
        },
        CodingSystemState {
            name: "unix".into(),
            base: "unix".into(),
            kind: "us-ascii".into(),
            eol_type: Some(0),
            plist: coding_plist('U', std::iter::empty()),
        },
        CodingSystemState {
            name: "dos".into(),
            base: "dos".into(),
            kind: "us-ascii".into(),
            eol_type: Some(1),
            plist: coding_plist('D', std::iter::empty()),
        },
        CodingSystemState {
            name: "mac".into(),
            base: "mac".into(),
            kind: "us-ascii".into(),
            eol_type: Some(2),
            plist: coding_plist('M', std::iter::empty()),
        },
        CodingSystemState {
            name: "us-ascii".into(),
            base: "us-ascii".into(),
            kind: "us-ascii".into(),
            eol_type: None,
            plist: coding_plist('A', std::iter::empty()),
        },
        CodingSystemState {
            name: "us-ascii-unix".into(),
            base: "us-ascii".into(),
            kind: "us-ascii".into(),
            eol_type: Some(0),
            plist: coding_plist('A', std::iter::empty()),
        },
        CodingSystemState {
            name: "us-ascii-dos".into(),
            base: "us-ascii".into(),
            kind: "us-ascii".into(),
            eol_type: Some(1),
            plist: coding_plist('A', std::iter::empty()),
        },
        CodingSystemState {
            name: "iso-latin-1".into(),
            base: "iso-latin-1".into(),
            kind: "iso-latin-1".into(),
            eol_type: None,
            plist: coding_plist('L', std::iter::empty()),
        },
        CodingSystemState {
            name: "iso-latin-1-unix".into(),
            base: "iso-latin-1".into(),
            kind: "iso-latin-1".into(),
            eol_type: Some(0),
            plist: coding_plist('L', std::iter::empty()),
        },
        CodingSystemState {
            name: "iso-latin-1-dos".into(),
            base: "iso-latin-1".into(),
            kind: "iso-latin-1".into(),
            eol_type: Some(1),
            plist: coding_plist('L', std::iter::empty()),
        },
        CodingSystemState {
            name: "windows-1252".into(),
            base: "windows-1252".into(),
            kind: "charset".into(),
            eol_type: None,
            plist: coding_plist(
                '*',
                [
                    (
                        ":charset-list".into(),
                        Value::list([Value::Symbol("windows-1252".into())]),
                    ),
                    (":mime-charset".into(), Value::Symbol("windows-1252".into())),
                ],
            ),
        },
        CodingSystemState {
            name: "cyrillic-koi8".into(),
            base: "cyrillic-koi8".into(),
            kind: "charset".into(),
            eol_type: None,
            plist: coding_plist('K', std::iter::empty()),
        },
        CodingSystemState {
            name: "cyrillic-koi8-unix".into(),
            base: "cyrillic-koi8".into(),
            kind: "charset".into(),
            eol_type: Some(0),
            plist: coding_plist('K', std::iter::empty()),
        },
        CodingSystemState {
            name: "cyrillic-koi8-dos".into(),
            base: "cyrillic-koi8".into(),
            kind: "charset".into(),
            eol_type: Some(1),
            plist: coding_plist('K', std::iter::empty()),
        },
        CodingSystemState {
            name: "cyrillic-koi8-mac".into(),
            base: "cyrillic-koi8".into(),
            kind: "charset".into(),
            eol_type: Some(2),
            plist: coding_plist('K', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8".into(),
            base: "utf-8".into(),
            kind: "utf-8".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-unix".into(),
            base: "utf-8".into(),
            kind: "utf-8".into(),
            eol_type: Some(0),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-dos".into(),
            base: "utf-8".into(),
            kind: "utf-8".into(),
            eol_type: Some(1),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-mac".into(),
            base: "utf-8".into(),
            kind: "utf-8".into(),
            eol_type: Some(2),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-with-signature".into(),
            base: "utf-8-with-signature".into(),
            kind: "utf-8-with-signature".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-with-signature-unix".into(),
            base: "utf-8-with-signature".into(),
            kind: "utf-8-with-signature".into(),
            eol_type: Some(0),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-with-signature-dos".into(),
            base: "utf-8-with-signature".into(),
            kind: "utf-8-with-signature".into(),
            eol_type: Some(1),
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-with-signature-mac".into(),
            base: "utf-8-with-signature".into(),
            kind: "utf-8-with-signature".into(),
            eol_type: Some(2),
            plist: coding_plist('u', std::iter::empty()),
        },
        // UTF-16 variants: `utf-16' is big-endian with a BOM; the -le/-be
        // forms are explicit-endian, BOM-less (mule-conf.el).
        CodingSystemState {
            name: "utf-16".into(),
            base: "utf-16".into(),
            kind: "utf-16".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-16le".into(),
            base: "utf-16le".into(),
            kind: "utf-16le".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-16be".into(),
            base: "utf-16be".into(),
            kind: "utf-16be".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-16-le".into(),
            base: "utf-16le".into(),
            kind: "utf-16le".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-16-be".into(),
            base: "utf-16be".into(),
            kind: "utf-16be".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "utf-8-auto".into(),
            base: "utf-8-auto".into(),
            kind: "utf-8-auto".into(),
            eol_type: None,
            plist: coding_plist('u', std::iter::empty()),
        },
        CodingSystemState {
            name: "prefer-utf-8".into(),
            base: "prefer-utf-8".into(),
            kind: "prefer-utf-8".into(),
            eol_type: None,
            plist: coding_plist('p', std::iter::empty()),
        },
        CodingSystemState {
            name: "prefer-utf-8-unix".into(),
            base: "prefer-utf-8".into(),
            kind: "prefer-utf-8".into(),
            eol_type: Some(0),
            plist: coding_plist('p', std::iter::empty()),
        },
        CodingSystemState {
            name: "raw-text".into(),
            base: "raw-text".into(),
            kind: "raw-text".into(),
            eol_type: None,
            plist: coding_plist('r', std::iter::empty()),
        },
        CodingSystemState {
            name: "raw-text-unix".into(),
            base: "raw-text".into(),
            kind: "raw-text".into(),
            eol_type: Some(0),
            plist: coding_plist('r', std::iter::empty()),
        },
        CodingSystemState {
            name: "raw-text-dos".into(),
            base: "raw-text".into(),
            kind: "raw-text".into(),
            eol_type: Some(1),
            plist: coding_plist('r', std::iter::empty()),
        },
        CodingSystemState {
            name: "raw-text-mac".into(),
            base: "raw-text".into(),
            kind: "raw-text".into(),
            eol_type: Some(2),
            plist: coding_plist('r', std::iter::empty()),
        },
        CodingSystemState {
            name: "mac-roman".into(),
            base: "mac-roman".into(),
            kind: "iso-latin-1".into(),
            eol_type: None,
            plist: coding_plist('m', std::iter::empty()),
        },
        CodingSystemState {
            name: "mac-roman-mac".into(),
            base: "mac-roman".into(),
            kind: "iso-latin-1".into(),
            eol_type: Some(2),
            plist: coding_plist('m', std::iter::empty()),
        },
        CodingSystemState {
            name: "euc-jp".into(),
            base: "euc-jp".into(),
            kind: "euc-jp".into(),
            eol_type: None,
            plist: coding_plist('E', std::iter::empty()),
        },
        CodingSystemState {
            name: "euc-jp-dos".into(),
            base: "euc-jp".into(),
            kind: "euc-jp".into(),
            eol_type: Some(1),
            plist: coding_plist('E', std::iter::empty()),
        },
        CodingSystemState {
            name: "iso-2022-7bit".into(),
            base: "iso-2022-7bit".into(),
            kind: "iso-2022-7bit".into(),
            eol_type: None,
            plist: coding_plist('I', std::iter::empty()),
        },
        CodingSystemState {
            name: "sjis".into(),
            base: "sjis".into(),
            kind: "sjis".into(),
            eol_type: None,
            plist: coding_plist('S', std::iter::empty()),
        },
        CodingSystemState {
            name: "big5".into(),
            base: "big5".into(),
            kind: "big5".into(),
            eol_type: None,
            plist: coding_plist('B', std::iter::empty()),
        },
        CodingSystemState {
            name: "chinese-gb18030".into(),
            base: "chinese-gb18030".into(),
            kind: "raw-text".into(),
            eol_type: None,
            plist: coding_plist('C', std::iter::empty()),
        },
    ];

    // GNU's ordinary text coding systems expose a complete EOL family.
    // Keep that invariant in one place instead of growing a hand-maintained
    // subset whenever a caller first asks for BASE-unix/dos/mac.
    let bases = systems
        .iter()
        .filter(|coding| {
            coding.eol_type.is_none()
                && coding.name == coding.base
                && coding.name != "no-conversion"
        })
        .cloned()
        .collect::<Vec<_>>();
    for base in bases {
        for (suffix, eol_type) in [("unix", 0), ("dos", 1), ("mac", 2)] {
            let name = format!("{}-{suffix}", base.name);
            if systems.iter().any(|coding| coding.name == name) {
                continue;
            }
            systems.push(CodingSystemState {
                name,
                base: base.base.clone(),
                kind: base.kind.clone(),
                eol_type: Some(eol_type),
                plist: base.plist.clone(),
            });
        }
    }
    // GNU's dumped mule.el implements public queries such as
    // `coding-system-type' in Lisp over the plist returned by the C coding
    // registry.  Bootstrap entries therefore need the same public metadata;
    // the Rust `kind' field is an internal codec discriminator and is not
    // always the public coding type (for example euc-jp is iso-2022).
    for coding in &mut systems {
        let public_type = if matches!(coding.name.as_str(), "unix" | "dos" | "mac") {
            "undecided"
        } else {
            match coding.kind.as_str() {
                "utf-8" | "utf-8-with-signature" | "utf-8-auto" => "utf-8",
                "utf-16" | "utf-16be" | "utf-16le" => "utf-16",
                "undecided" | "prefer-utf-8" => "undecided",
                "raw-text" | "no-conversion" => "raw-text",
                "euc-jp" | "iso-2022-7bit" => "iso-2022",
                "sjis" => "shift-jis",
                "big5" => "big5",
                "us-ascii" | "iso-latin-1" | "cyrillic-koi8" | "windows-1252" | "mac-roman"
                | "chinese-gb18030" | "charset" => "charset",
                other => other,
            }
        };
        let mut plist = coding.plist.to_vec().unwrap_or_default();
        let charset_list = match coding.kind.as_str() {
            "utf-8" | "utf-8-with-signature" | "utf-8-auto" => {
                Some(Value::list([Value::Symbol("unicode".into())]))
            }
            "us-ascii" | "undecided" | "prefer-utf-8" => {
                Some(Value::list([Value::Symbol("ascii".into())]))
            }
            "iso-latin-1" => Some(Value::list([Value::Symbol("iso-8859-1".into())])),
            _ => None,
        };
        if matches!(
            coding.kind.as_str(),
            "utf-8"
                | "utf-8-with-signature"
                | "utf-8-auto"
                | "us-ascii"
                | "undecided"
                | "prefer-utf-8"
                | "iso-latin-1"
                | "raw-text"
        ) {
            plist.extend([Value::Symbol(":ascii-compatible-p".into()), Value::T]);
        }
        if let Some(charset_list) = charset_list {
            plist.extend([Value::Symbol(":charset-list".into()), charset_list]);
        }
        plist.extend([
            Value::Symbol(":name".into()),
            Value::Symbol(coding.name.clone()),
            Value::Symbol(":coding-type".into()),
            Value::Symbol(public_type.into()),
        ]);
        coding.plist = Value::list(plist);
    }
    systems
}

pub(super) fn builtin_coding_aliases() -> Vec<(String, String)> {
    vec![
        ("ascii".into(), "us-ascii".into()),
        ("latin-1".into(), "iso-latin-1".into()),
        ("latin-1-unix".into(), "iso-latin-1-unix".into()),
        ("latin-1-dos".into(), "iso-latin-1-dos".into()),
        ("iso-8859-1".into(), "iso-latin-1".into()),
        ("iso-8859-1-unix".into(), "iso-latin-1-unix".into()),
        ("iso-8859-1-dos".into(), "iso-latin-1-dos".into()),
        ("cp1252".into(), "windows-1252".into()),
        ("koi8-r".into(), "cyrillic-koi8".into()),
        ("binary".into(), "raw-text".into()),
        ("utf8".into(), "utf-8".into()),
        ("utf-8-emacs".into(), "utf-8".into()),
        ("utf-8-emacs-unix".into(), "utf-8-unix".into()),
        ("utf-8-emacs-dos".into(), "utf-8-dos".into()),
        ("utf-8-emacs-mac".into(), "utf-8-mac".into()),
    ]
}

pub(super) fn builtin_coding_priority() -> Vec<String> {
    vec![
        "prefer-utf-8".into(),
        "utf-8".into(),
        "utf-8-auto".into(),
        "raw-text".into(),
        "iso-latin-1".into(),
        "us-ascii".into(),
        "undecided".into(),
        "no-conversion".into(),
        "sjis".into(),
        "big5".into(),
        "euc-jp".into(),
        "iso-2022-7bit".into(),
    ]
}
