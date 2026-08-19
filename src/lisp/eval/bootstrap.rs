use super::{CharTableEntry, CodingSystemState};
use crate::lisp::types::Value;

fn coding_plist(mnemonic: char, extras: impl IntoIterator<Item = (String, Value)>) -> Value {
    let mut items = vec![
        Value::Symbol(":mnemonic".into()),
        Value::Integer(mnemonic as i64),
    ];
    for (key, value) in extras {
        items.push(Value::Symbol(key.into()));
        items.push(value);
    }
    Value::list(items)
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
            plist: coding_plist('?', std::iter::empty()),
        },
        CodingSystemState {
            name: "no-conversion".into(),
            base: "no-conversion".into(),
            kind: "raw-text".into(),
            eol_type: None,
            // coding.c's C bootstrap and mule-conf.el both mark the raw
            // families `:for-unibyte t'.
            plist: coding_plist('=', std::iter::once((
                ":for-unibyte".to_string(),
                Value::T,
            ))),
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
        match coding.kind.as_str() {
            "utf-8-with-signature" => plist.extend([Value::Symbol(":bom".into()), Value::T]),
            "utf-8-auto" => plist.extend([
                Value::Symbol(":bom".into()),
                Value::cons(
                    Value::Symbol("utf-8-with-signature".into()),
                    Value::Symbol("utf-8".into()),
                ),
            ]),
            _ => {}
        }
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
            Value::Symbol(coding.name.clone().into()),
            Value::Symbol(":coding-type".into()),
            Value::Symbol(public_type.into()),
        ]);
        coding.plist = Value::list(plist);
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
