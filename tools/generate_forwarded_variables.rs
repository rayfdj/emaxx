//! Inventory GNU's forwarded-variable declarations from C and Objective-C.
//!
//! Development-only metadata generation: no GNU executable or Elisp is run.
//! This describes declarations across supported source files, not which
//! platform's slots were actually installed in a particular interpreter.
//! Runtime users must separately track their real C initialization boundary.
//!
//! Build: rustc --edition 2024 -D warnings tools/generate_forwarded_variables.rs
//! Usage: generate_forwarded_variables GNU-SRC OUTPUT

use std::collections::BTreeMap;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Token<'a> {
    Identifier(&'a str),
    String(&'a str),
    Open,
    Comma,
    Other,
}

/// A deliberately small C lexer: the declaration boundary needs only an
/// identifier, an opening parenthesis, and a literal first argument. Comments,
/// strings, and character constants are consumed whole, so text inside them
/// cannot forge a declaration. Whitespace is irrelevant to token boundaries.
fn tokens(source: &str) -> Vec<Token<'_>> {
    let bytes = source.as_bytes();
    let mut output = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        if bytes[offset].is_ascii_whitespace() {
            offset += 1;
            continue;
        }
        if bytes[offset..].starts_with(b"/*") {
            let end = source[offset + 2..]
                .find("*/")
                .expect("unterminated C comment");
            offset += end + 4;
            continue;
        }
        if bytes[offset..].starts_with(b"//") {
            offset = source[offset..]
                .find('\n')
                .map_or(bytes.len(), |end| offset + end);
            continue;
        }
        if bytes[offset] == b'#' {
            // After phase-2 line splicing a directive occupies one logical
            // line. In particular buffer.c defines DEFVAR_PER_BUFFER itself;
            // its formal parameter list is not a variable declaration.
            // Conditional bodies remain visible: this is the all-platform
            // declaration inventory, not the active preprocessed build.
            offset = source[offset..]
                .find('\n')
                .map_or(bytes.len(), |end| offset + end);
            continue;
        }
        let start = offset;
        let token = match bytes[offset] {
            quote @ (b'"' | b'\'') => {
                offset += 1;
                while offset < bytes.len() && bytes[offset] != quote {
                    if bytes[offset] == b'\\' {
                        offset += 1;
                    }
                    offset += 1;
                }
                assert!(offset < bytes.len(), "unterminated C literal");
                let value = &source[start + 1..offset];
                offset += 1;
                if quote == b'"' {
                    Token::String(value)
                } else {
                    Token::Other
                }
            }
            b'(' => {
                offset += 1;
                Token::Open
            }
            b',' => {
                offset += 1;
                Token::Comma
            }
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => {
                offset += 1;
                while offset < bytes.len()
                    && (bytes[offset].is_ascii_alphanumeric() || bytes[offset] == b'_')
                {
                    offset += 1;
                }
                Token::Identifier(&source[start..offset])
            }
            _ => {
                // Advance by a whole UTF-8 character; some GNU source
                // comments and diagnostics contain non-ASCII text.
                offset += source[offset..]
                    .chars()
                    .next()
                    .expect("remaining character")
                    .len_utf8();
                Token::Other
            }
        };
        output.push(token);
    }
    output
}

fn declarations(source: &str) -> BTreeMap<String, &'static str> {
    // C translation phase 2 precedes comment/literal tokenization. In
    // particular, a spliced // comment continues onto the next physical line.
    let source = source.replace("\\\r\n", "").replace("\\\n", "");
    let tokens = tokens(&source);
    let mut declarations = BTreeMap::new();
    for (index, token) in tokens.iter().enumerate() {
        let Token::Identifier(declaration) = token else {
            continue;
        };
        let kind = match *declaration {
            "DEFVAR_LISP" => "Lisp",
            "DEFVAR_LISP_NOPRO" => "LispNoPro",
            "DEFVAR_BOOL" => "Bool",
            "DEFVAR_INT" => "Int",
            "DEFVAR_KBOARD" => "Keyboard",
            "DEFVAR_PER_BUFFER" => "Buffer",
            _ => continue,
        };
        if tokens.get(index + 1) != Some(&Token::Open) {
            continue;
        }
        let Some([Token::String(name), Token::Comma]) = tokens.get(index + 2..index + 4) else {
            panic!("{declaration} needs a supported literal first argument followed by a comma");
        };
        // The pinned declarations use ordinary literal ASCII names. Reject
        // an unfamiliar spelling instead of publishing escaped C bytes as
        // a Lisp symbol name or silently dropping that declaration.
        assert!(
            name.is_ascii() && !name.contains('\\'),
            "forwarded name requires C literal decoding: {name:?}"
        );
        if let Some(previous) = declarations.insert((*name).to_string(), kind) {
            assert_eq!(previous, kind, "conflicting declarations of {name}");
        }
    }
    declarations
}

fn inventory(directory: &Path) -> BTreeMap<String, &'static str> {
    let mut inventory = BTreeMap::new();
    for entry in std::fs::read_dir(directory).expect("read configured GNU src directory") {
        let path = entry.expect("read GNU source entry").path();
        if !matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("c" | "m")
        ) {
            continue;
        }
        // Some pinned sources have non-UTF-8 bytes in comments, never in
        // the declaration names. Lossy decoding does not change those names.
        let bytes = std::fs::read(&path).expect("read GNU source");
        for (name, kind) in declarations(&String::from_utf8_lossy(&bytes)) {
            if let Some(previous) = inventory.insert(name.clone(), kind) {
                assert_eq!(
                    previous,
                    kind,
                    "conflicting declarations of {name} in {}",
                    path.display()
                );
            }
        }
    }
    inventory
}

fn main() {
    let arguments = std::env::args_os().skip(1).collect::<Vec<_>>();
    let [directory, output] = arguments.as_slice() else {
        panic!("usage: TOOL GNU-SRC OUTPUT");
    };
    let inventory = inventory(Path::new(directory));
    assert!(!inventory.is_empty(), "no forwarded C declarations found");
    let mut generated = String::from(
        "// @generated by tools/generate_forwarded_variables.rs from GNU C/Objective-C.\n\
         // Declaration ownership only: active forwarding is established by C initialization.\n\n\
         #[derive(Clone, Copy, Debug, PartialEq, Eq)]\n\
         pub(crate) enum ForwardedVariableKind { Lisp, LispNoPro, Bool, Int, Keyboard, Buffer }\n\n\
         pub(crate) const GNU_C_FORWARDED_VARIABLES: &[(&str, ForwardedVariableKind)] = &[\n",
    );
    for (name, kind) in inventory {
        generated.push_str(&format!("    ({name:?}, ForwardedVariableKind::{kind}),\n"));
    }
    generated.push_str("];\n");
    std::fs::write(output, generated).expect("write forwarded C declaration inventory");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declaration_tokens_ignore_layout_comments_and_quoted_decoys() {
        let source = r#"
            #define DEFVAR_PER_BUFFER(lname, slot, predicate, doc) \
                record_forwarding(lname, slot)
            #define TEXT "DEFVAR_BOOL(\"directive-decoy\", slot, doc)"
            // DEFVAR_BOOL ("line-decoy", ignored, ignored);
            // A continued comment must hide the next physical line too. \
            DEFVAR_BOOL("continued-comment-decoy", ignored, ignored);
            /* DEFVAR_BOOL("comment-decoy", ignored, ignored); */
            const char *message = "DEFVAR_BOOL (\"string-decoy\", ignored, ignored)";
            DEFVAR_BOOL("tight", slot, doc);
            DEFVAR_BOOL /* a comment */ ( "spaced", slot, doc);
            DEFVAR_LISP
              ("object", slot, doc);
            DEFVAR_LISP_NOPRO("unprotected", slot, doc);
            DEFVAR_INT("integer", slot, doc);
            DEFVAR_KBOARD("keyboard", slot, doc);
            DEFVAR_PER_BUFFER("buffer", slot, doc);
            NOT_DEFVAR_BOOL("identifier-decoy", slot, doc);
        "#;
        assert_eq!(
            declarations(source),
            BTreeMap::from([
                ("buffer".into(), "Buffer"),
                ("integer".into(), "Int"),
                ("keyboard".into(), "Keyboard"),
                ("object".into(), "Lisp"),
                ("spaced".into(), "Bool"),
                ("tight".into(), "Bool"),
                ("unprotected".into(), "LispNoPro"),
            ])
        );
    }

    #[test]
    fn unsupported_declaration_arguments_fail_instead_of_being_omitted() {
        for source in [
            "DEFVAR_BOOL(dynamic_name, slot, doc);",
            "DEFVAR_BOOL(\"first\" \"second\", slot, doc);",
            "DEFVAR_BOOL(\"escaped\\nname\", slot, doc);",
        ] {
            assert!(std::panic::catch_unwind(|| declarations(source)).is_err());
        }
    }

    #[test]
    fn conflicting_declaration_kinds_cannot_overwrite_each_other() {
        let source =
            "DEFVAR_LISP(\"same\", object_slot, doc); DEFVAR_BOOL(\"same\", bool_slot, doc);";
        assert!(std::panic::catch_unwind(|| declarations(source)).is_err());
    }

    #[test]
    fn inventory_includes_objective_c_sources_and_normalizes_duplicates() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock follows Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "emaxx-forwarded-inventory-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).expect("unique inventory fixture");
        for (filename, source) in [
            ("ordinary.c", "DEFVAR_BOOL(\"tight\", slot, doc);"),
            (
                "platform.m",
                "DEFVAR_BOOL (\"platform\", slot, doc); DEFVAR_BOOL(\"tight\", slot, doc);",
            ),
            ("notes.txt", "DEFVAR_BOOL(\"not-code\", slot, doc);"),
        ] {
            std::fs::write(directory.join(filename), source).expect("write test-only C fixture");
        }
        assert_eq!(
            inventory(&directory),
            BTreeMap::from([("platform".into(), "Bool"), ("tight".into(), "Bool"),])
        );
        std::fs::remove_dir_all(directory).expect("remove successful inventory fixture");
    }
}
