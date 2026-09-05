use super::{CharTableEntry, CodingSystemState};
use crate::lisp::types::Value;

fn symbol(name: &str) -> Value {
    Value::Symbol(name.into())
}

/// The plist coding.c's syms_of_coding hands `define-coding-system-internal'
/// for its two C-defined systems, with the `:ascii-compatible-p' and
/// `:category' entries Fdefine_coding_system_internal prepends.
fn c_defined_coding_plist(name: &str) -> Value {
    let (category, mnemonic, coding_type, tail): (&str, i64, &str, Vec<Value>) = match name {
        "no-conversion" => (
            "coding-category-raw-text",
            i64::from(b'='),
            "raw-text",
            vec![
                symbol(":default-char"),
                Value::Integer(0),
                symbol(":for-unibyte"),
                Value::T,
                symbol(":docstring"),
                Value::String(
                    "Do no conversion.\n\nWhen you visit a file with this coding, the file is \
                     read into a\nunibyte buffer as is, thus each byte of a file is treated as \
                     a\ncharacter."
                        .into(),
                ),
                symbol(":eol-type"),
                symbol("unix"),
            ],
        ),
        _ => (
            "coding-category-undecided",
            i64::from(b'-'),
            "undecided",
            vec![
                symbol(":charset-list"),
                Value::list([symbol("ascii")]),
                symbol(":for-unibyte"),
                Value::Nil,
                symbol(":docstring"),
                Value::String(
                    "No conversion on encoding, automatic conversion on decoding.".into(),
                ),
                symbol(":eol-type"),
                Value::Nil,
            ],
        ),
    };
    Value::list(
        [
            symbol(":ascii-compatible-p"),
            Value::T,
            symbol(":category"),
            symbol(category),
            symbol(":name"),
            symbol(name),
            symbol(":mnemonic"),
            Value::Integer(mnemonic),
            symbol(":coding-type"),
            symbol(coding_type),
            symbol(":ascii-compatible-p"),
            Value::T,
        ]
        .into_iter()
        .chain(tail),
    )
}

pub(super) fn syntax_spec_value(spec: &str) -> Value {
    Value::String(spec.to_string().into())
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
            value: syntax_spec_value("."),
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
            std::env::split_paths(&path)
                .map(|entry| Value::String(entry.display().to_string().into())),
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
    // coding.c calls Fdefine_coding_system_internal exactly twice, for
    // `no-conversion' and `undecided'.  Every other coding system GNU has
    // comes from international/mule-conf.el and language/*.el, which the
    // reconstructed image loads; hardcoding them here made Emaxx report 277
    // systems to GNU's 271, inventing eight that GNU treats as aliases and
    // missing two it really has.
    let mut systems = vec![
        CodingSystemState {
            name: "undecided".into(),
            base: "undecided".into(),
            kind: "undecided".into(),
            eol_type: None,
            plist: c_defined_coding_plist("undecided"),
            category: super::coding::CODING_CATEGORY_UNDECIDED,
            charset_list: Value::list([Value::Symbol("ascii".into())]),
            default_char: b' ' as u32,
            // coding.c:12292: the inhibit attributes are 0, so the
            // `inhibit-null-byte-detection' and
            // `inhibit-iso-escape-detection' variables decide.
            type_args: vec![Value::Integer(0), Value::Integer(0), Value::Nil],
        },
        CodingSystemState {
            name: "no-conversion".into(),
            base: "no-conversion".into(),
            kind: "raw-text".into(),
            eol_type: None,
            plist: c_defined_coding_plist("no-conversion"),
            category: super::coding::CODING_CATEGORY_RAW_TEXT,
            charset_list: Value::list([Value::Symbol("ascii".into())]),
            default_char: 0,
            type_args: Vec::new(),
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
                category: base.category,
                charset_list: base.charset_list.clone(),
                default_char: base.default_char,
                type_args: base.type_args.clone(),
            });
        }
    }
    systems
}

pub(super) fn builtin_coding_aliases() -> Vec<(String, String)> {
    // coding.c registers no aliases: every one GNU has comes from
    // `define-coding-system-alias' in international/mule-conf.el and the
    // language files.  The hardcoded table invented `utf8' (not a coding
    // system in GNU at all) and modelled `utf-8-emacs' as an alias of utf-8
    // when GNU defines it as its own system.
    Vec::new()
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
