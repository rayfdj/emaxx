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
    vec![
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
    ]
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
