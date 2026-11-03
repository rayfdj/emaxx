use super::{CodingSpec, CodingSystemState};
use crate::lisp::types::{CharTableRef, Kind, Value};
use std::collections::HashMap;

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

/// syntax.c:init_syntax_once.  The shared code conses are the actual Lisp
/// descriptors, including their observable identity across syntax tables.
pub(super) fn initial_syntax_table(codes: Value) -> CharTableRef {
    let Kind::Vector(codes) = codes.kind() else {
        unreachable!()
    };
    let code = |index| codes.get(index).expect("syntax code");
    let table = CharTableRef::new(Value::symbol("syntax-table"), code(0), 0);
    for ch in 0..32 {
        table.set(ch, code(1));
    }
    table.set(127, code(1));
    for ch in [' ', '\t', '\n', '\r', '\u{c}'] {
        table.set(ch as u32, code(0));
    }
    for (start, end) in [('a', 'z'), ('A', 'Z'), ('0', '9')] {
        for ch in start as u32..=end as u32 {
            table.set(ch, code(2));
        }
    }
    for ch in ['$', '%'] {
        table.set(ch as u32, code(2));
    }
    for (ch, class, matching) in [
        ('(', 4, ')'),
        (')', 5, '('),
        ('[', 4, ']'),
        (']', 5, '['),
        ('{', 4, '}'),
        ('}', 5, '{'),
    ] {
        table.set(
            ch as u32,
            Value::cons(Value::Integer(class), Value::Integer(matching as i64)),
        );
    }
    // GNU creates separate conses for these two initial entries.
    for (ch, class) in [('"', 7), ('\\', 9)] {
        table.set(ch as u32, Value::cons(Value::Integer(class), Value::Nil));
    }
    for ch in "_-+*/&|<>=".chars() {
        table.set(ch as u32, code(3));
    }
    for ch in ".,;:?!#@~^'`".chars() {
        table.set(ch as u32, code(1));
    }
    table.set_range(128, 0x3fffff, code(2));
    table
}

/// coding.c's syms_of_coding: Fdefine_coding_system_internal's
/// attribute vectors, specs and Vcoding_system_hash_table for the two
/// systems it defines, `no-conversion' with eol type `unix' and then
/// `undecided' with its three subsidiaries.  Every other coding system
/// comes from international/mule-conf.el and language/*.el.  The two
/// share one argument array, so `undecided' keeps no-conversion's
/// :default-char attribute 0.
pub(super) fn builtin_coding_systems() -> (
    Vec<CodingSystemState>,
    Vec<CodingSpec>,
    HashMap<String, usize, crate::lisp::primitives::FnvBuildHasher>,
) {
    let attrs = vec![
        CodingSystemState {
            base: "no-conversion".into(),
            mnemonic: i64::from(b'='),
            kind: "raw-text".into(),
            ascii_compatible: Value::T,
            decode_table: Value::Nil,
            encode_table: Value::Nil,
            post_read: Value::Nil,
            pre_write: Value::Nil,
            for_unibyte: true,
            plist: c_defined_coding_plist("no-conversion"),
            category: super::coding::CODING_CATEGORY_RAW_TEXT,
            charset_list: Value::Nil,
            default_char: 0,
            type_args: Vec::new(),
            derived: Default::default(),
        },
        CodingSystemState {
            base: "undecided".into(),
            mnemonic: i64::from(b'-'),
            kind: "undecided".into(),
            ascii_compatible: Value::T,
            decode_table: Value::Nil,
            encode_table: Value::Nil,
            post_read: Value::Nil,
            pre_write: Value::Nil,
            for_unibyte: false,
            plist: c_defined_coding_plist("undecided"),
            category: super::coding::CODING_CATEGORY_UNDECIDED,
            charset_list: Value::list([symbol("ascii")]),
            default_char: 0,
            // coding.c:12292: the inhibit attributes are 0, so the
            // `inhibit-null-byte-detection' and
            // `inhibit-iso-escape-detection' variables decide.
            type_args: vec![Value::Integer(0), Value::Integer(0), Value::Nil],
            derived: Default::default(),
        },
    ];
    let spec = |attrs, name: &str, eol_type| CodingSpec {
        attrs,
        aliases: Value::list([symbol(name)]),
        eol_type,
    };
    let specs = vec![
        spec(0, "no-conversion", symbol("unix")),
        spec(1, "undecided-unix", symbol("unix")),
        spec(1, "undecided-dos", symbol("dos")),
        spec(1, "undecided-mac", symbol("mac")),
        spec(
            1,
            "undecided",
            Value::vector([
                symbol("undecided-unix"),
                symbol("undecided-dos"),
                symbol("undecided-mac"),
            ]),
        ),
    ];
    let hash = specs
        .iter()
        .enumerate()
        .map(|(index, spec)| {
            let name = spec
                .aliases
                .cons_values()
                .and_then(|(name, _)| name.symbol_str().map(str::to_string));
            (name.expect("a spec's name"), index)
        })
        .collect();
    (attrs, specs, hash)
}
