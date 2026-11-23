//! Regenerate the native compiler's C-subroutine ABI order from GNU C source.
//!
//! This is a development tool, not runtime machinery.  It preprocesses GNU
//! `emacs.c` with the reference build's own compiler configuration, walks the
//! argument-less startup calls in `main` in order, follows each into the
//! source file that defines it, reads that function's active `defsubr` calls
//! after the same preprocessing, and finally reverses the sequence because
//! GNU `defsubr` conses each entry onto `Vcomp_subr_list`.  Window-system and
//! feature differences between reference builds therefore come from the
//! configured tree itself rather than from a hand-maintained file list.
//!
//! Usage: `generate_native_subrs GNU-SRC GNU-BINARY OUTPUT`, where GNU-SRC is
//! the configured and built `src` directory of the pinned checkout, and
//! GNU-BINARY is the unstripped `emacs` executable it produced.  Build it with
//! `rustc --edition 2024 -O tools/generate_native_subrs.rs`.
//!
//! `generate_native_subrs --reemit EXISTING OUTPUT` emits again the table an
//! earlier run wrote to EXISTING: the same subroutines, arities and ABI
//! strings, read back from that file, under this tool's present choice of
//! each entry.  It is how a table is brought up to date for a target whose
//! reference build is not at hand; run on a table this tool has just
//! written, it writes the same bytes.

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Clone)]
struct Primitive {
    name: String,
    min_args: u16,
    max_args: i32,
}

/// What a generated file says of its build: the subroutines in table order
/// and the three inputs to comp.c:hash_native_abi.
struct Table {
    registration: Vec<Primitive>,
    abi_version: String,
    abi_configuration: String,
    abi_configuration_options: String,
}

fn main() {
    let arguments = env::args_os()
        .skip(1)
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let (table, output) = match arguments.as_slice() {
        [mode, existing, output] if mode.as_os_str() == "--reemit" => {
            (read_generated_table(existing), output)
        }
        [gnu_src, gnu_binary, output] => (read_gnu_table(gnu_src, gnu_binary), output),
        _ => panic!("usage: TOOL GNU-SRC GNU-BINARY OUTPUT, or TOOL --reemit EXISTING OUTPUT"),
    };
    fs::write(output, emit(&table))
        .unwrap_or_else(|error| panic!("write {}: {error}", output.display()));
}

/// The table of the reference build: GNU-SRC's registrations that
/// GNU-BINARY defines, in `Vcomp_subr_list' order.
fn read_gnu_table(gnu_src: &Path, gnu_binary: &Path) -> Table {
    let min_arg_constants = read_min_arg_constants(gnu_src);
    let definitions = read_defun_definitions(gnu_src, &min_arg_constants);
    let compiled_subrs = read_compiled_subrs(gnu_binary);
    let abi_version = read_c_define_string(&gnu_src.join("comp.c"), "ABI_VERSION");
    let abi_configuration = read_c_define_string(&gnu_src.join("config.h"), "EMACS_CONFIGURATION");
    let abi_configuration_options =
        read_c_define_string(&gnu_src.join("config.h"), "EMACS_CONFIG_OPTIONS");
    let mut registration = Vec::with_capacity(compiled_subrs.len());
    let mut seen = HashSet::new();
    let mut preprocessed: HashMap<String, String> = HashMap::new();

    for function in startup_calls(gnu_src) {
        for source in definition_sources(gnu_src, &function) {
            let text = preprocessed
                .entry(source.clone())
                .or_insert_with(|| preprocess_registration_source(gnu_src, &source));
            let Some(body) = function_body(text, &function) else {
                continue;
            };
            let local_definitions = defun_definitions(text, &min_arg_constants);
            for identifier in defsubr_identifiers(body) {
                if !compiled_subrs.contains(&identifier) {
                    eprintln!(
                        "warning: {source}:{function} registers {identifier}, which the reference binary does not define"
                    );
                    continue;
                }
                let primitive = local_definitions
                    .get(&identifier)
                    .or_else(|| definitions.get(&identifier))
                    .unwrap_or_else(|| {
                        panic!("{source}:{function} registers {identifier} without a DEFUN")
                    });
                assert!(
                    seen.insert(primitive.name.clone()),
                    "compiled primitive {:?} was registered more than once",
                    primitive.name
                );
                registration.push(primitive.clone());
            }
        }
    }

    assert!(
        registration.len() <= compiled_subrs.len(),
        "registration cannot exceed compiled subroutine symbols"
    );

    registration.reverse();
    Table {
        registration,
        abi_version,
        abi_configuration,
        abi_configuration_options,
    }
}

/// The table an earlier run emitted to PATH, read back from its text as
/// written or as rustfmt left it: the three ABI strings, and the name and
/// arity of each `NativeSubr::new' entry in order.  The entries' targets are
/// not read; `emit' chooses them again.
fn read_generated_table(path: &Path) -> Table {
    let text =
        fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    let constant = |name: &str| {
        let declaration = format!("const {name}: &str =");
        let value = text
            .split_once(&declaration)
            .unwrap_or_else(|| panic!("find {name} in {}", path.display()))
            .1;
        parse_c_string(value.trim_start())
            .unwrap_or_else(|| panic!("parse {name} in {}", path.display()))
    };
    let mut registration = Vec::new();
    for arguments in call_arguments(&text, "NativeSubr::new(") {
        let parts = split_top_level(arguments)
            .into_iter()
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        let [name, min_args, max_args, _target, index] = parts.as_slice() else {
            panic!("a table entry has five fields: {arguments}");
        };
        let name = name
            .strip_prefix('c')
            .and_then(parse_c_string)
            .unwrap_or_else(|| panic!("a table entry names its subroutine: {arguments}"));
        let max_args = match *max_args {
            "NativeMaxArgs::Many" => -2,
            "NativeMaxArgs::Unevalled" => -1,
            fixed => fixed
                .strip_prefix("NativeMaxArgs::Fixed(")
                .and_then(|count| count.strip_suffix(')'))
                .and_then(|count| count.parse::<i32>().ok())
                .unwrap_or_else(|| panic!("a table entry gives its arity: {arguments}")),
        };
        assert_eq!(
            index.parse::<usize>().ok(),
            Some(registration.len()),
            "table entries are numbered in order: {arguments}"
        );
        registration.push(Primitive {
            name,
            min_args: min_args
                .parse::<u16>()
                .unwrap_or_else(|_| panic!("a table entry gives its minimum: {arguments}")),
            max_args,
        });
    }
    assert!(
        text.contains(&format!("[NativeSubr; {}]", registration.len())),
        "the table declares as many entries as it lists"
    );
    Table {
        registration,
        abi_version: constant("NATIVE_ABI_VERSION"),
        abi_configuration: constant("NATIVE_ABI_SYSTEM_CONFIGURATION"),
        abi_configuration_options: constant("NATIVE_ABI_SYSTEM_CONFIGURATION_OPTIONS"),
    }
}

/// The argument text of each call of CALLEE (its name and open parenthesis)
/// in Rust TEXT, up to the parenthesis that closes it.
fn call_arguments<'a>(text: &'a str, callee: &str) -> Vec<&'a str> {
    let mut result = Vec::new();
    let mut offset = 0;
    while let Some(relative) = text[offset..].find(callee) {
        let start = offset + relative + callee.len();
        let mut depth = 1_i32;
        let mut quoted = false;
        let mut escaped = false;
        let mut end = None;
        for (relative, character) in text[start..].char_indices() {
            if quoted {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    quoted = false;
                }
            } else if character == '"' {
                quoted = true;
            } else if character == '(' {
                depth += 1;
            } else if character == ')' {
                depth -= 1;
                if depth == 0 {
                    end = Some(start + relative);
                    break;
                }
            }
        }
        let end = end.unwrap_or_else(|| panic!("a call of {callee} is closed"));
        result.push(&text[start..end]);
        offset = end;
    }
    result
}

/// The generated file for TABLE: the ABI strings, the table with each
/// subroutine's entry, and the entries this file defines itself.
fn emit(table: &Table) -> String {
    let Table {
        registration,
        abi_version,
        abi_configuration,
        abi_configuration_options,
    } = table;
    let mut generated = String::from(
        "//! Generated from GNU Emacs C `syms_of_*`/`defsubr` order.\n\
         //! Regenerate with `tools/generate_native_subrs.rs`; do not hand edit.\n\n\
         use super::abi::{NativeMaxArgs, NativeSubr};\n\n",
    );
    generated.push_str(&format!(
        "// These are inputs to comp.c:hash_native_abi for the C build whose\n\
         // active subroutine table is emitted below.  They describe the .eln\n\
         // ABI target, not Emaxx's user-visible host configuration.\n\
         pub(crate) const NATIVE_ABI_VERSION: &str = {abi_version:?};\n\
         pub(crate) const NATIVE_ABI_SYSTEM_CONFIGURATION: &str = {abi_configuration:?};\n\
         pub(crate) const NATIVE_ABI_SYSTEM_CONFIGURATION_OPTIONS: &str = {abi_configuration_options:?};\n\n"
    ));
    generated.push_str(&format!(
        "pub(crate) static NATIVE_SUBRS: [NativeSubr; {}] = [\n",
        registration.len()
    ));
    for (index, primitive) in registration.iter().enumerate() {
        let max_args = match primitive.max_args {
            -2 => "NativeMaxArgs::Many".to_string(),
            -1 => "NativeMaxArgs::Unevalled".to_string(),
            value => format!("NativeMaxArgs::Fixed({value})"),
        };
        assert!(
            primitive.name.is_ascii(),
            "GNU primitive names must be ASCII"
        );
        let target = direct_native_target(&primitive.name).map_or_else(
            || format!("native_subr_{index:04}"),
            |target| format!("super::runtime::{target}"),
        );
        generated.push_str(&format!(
            "    NativeSubr::new(c{:?}, {}, {max_args}, {target} as *const std::ffi::c_void, {index}),\n",
            primitive.name, primitive.min_args
        ));
    }
    generated.push_str("];\n\n");
    generated.push_str(
        "pub(crate) fn native_subr_address(index: usize) -> *mut std::ffi::c_void {\n\
         \x20   NATIVE_SUBRS.get(index).map_or(std::ptr::null_mut(), |subr| subr.function.cast_mut())\n\
         }\n\n",
    );
    for (index, primitive) in registration.iter().enumerate() {
        if direct_native_target(&primitive.name).is_some() {
            continue;
        }
        if let Some(target) = indexed_native_target(&primitive.name) {
            assert!(
                primitive.max_args >= 0,
                "an entry that passes its index takes a fixed number of arguments"
            );
            let (parameters, arguments) = fixed_parameters(primitive.max_args);
            generated.push_str(&format!(
                "extern \"C\" fn native_subr_{index:04}({parameters}) -> super::runtime::NativeWord {{\n    super::runtime::{target}({index}, {arguments})\n}}\n\n"
            ));
            continue;
        }
        if primitive.name == "cons" {
            assert_eq!(primitive.max_args, 2, "alloc.c:Fcons has fixed arity two");
            generated.push_str(&format!(
                "extern \"C\" fn native_subr_{index:04}(arg_0: super::runtime::NativeWord, arg_1: super::runtime::NativeWord) -> super::runtime::NativeWord {{\n    super::runtime::invoke_cons(arg_0, arg_1)\n}}\n\n"
            ));
            continue;
        }
        let comparison = match primitive.name.as_str() {
            "<" => Some("Less"),
            ">" => Some("Greater"),
            "<=" => Some("LessOrEqual"),
            ">=" => Some("GreaterOrEqual"),
            "=" => Some("Equal"),
            _ => None,
        };
        if let Some(comparison) = comparison {
            assert_eq!(
                primitive.max_args, -2,
                "data.c numeric comparisons use the MANY ABI"
            );
            generated.push_str(&format!(
                "unsafe extern \"C\" fn native_subr_{index:04}(nargs: isize, args: *const super::runtime::NativeWord) -> super::runtime::NativeWord {{\n    unsafe {{ super::runtime::invoke_numeric_comparison({index}, super::runtime::FixnumComparison::{comparison}, nargs, args) }}\n}}\n\n"
            ));
            continue;
        }
        match primitive.max_args {
            -2 => generated.push_str(&format!(
                "unsafe extern \"C\" fn native_subr_{index:04}(nargs: isize, args: *const super::runtime::NativeWord) -> super::runtime::NativeWord {{\n    unsafe {{ super::runtime::invoke_subr_many({index}, nargs, args) }}\n}}\n\n"
            )),
            -1 => generated.push_str(&format!(
                "extern \"C\" fn native_subr_{index:04}(args: super::runtime::NativeWord) -> super::runtime::NativeWord {{\n    super::runtime::invoke_subr({index}, &[args])\n}}\n\n"
            )),
            count => {
                let (parameters, arguments) = fixed_parameters(count);
                generated.push_str(&format!(
                    "extern \"C\" fn native_subr_{index:04}({parameters}) -> super::runtime::NativeWord {{\n    super::runtime::invoke_subr({index}, &[{arguments}])\n}}\n\n"
                ));
            }
        }
    }
    generated
}

/// The parameter list of a generated entry of COUNT arguments, and the
/// same arguments as it passes them on.
fn fixed_parameters(count: i32) -> (String, String) {
    let parameters = (0..count)
        .map(|argument| format!("arg_{argument}: super::runtime::NativeWord"))
        .collect::<Vec<_>>()
        .join(", ");
    let arguments = (0..count)
        .map(|argument| format!("arg_{argument}"))
        .collect::<Vec<_>>()
        .join(", ");
    (parameters, arguments)
}

// Store the chosen Rust implementation in the subr itself, as DEFUN stores
// its C entry. Runtime funcall and ABI table construction need no name match.
fn direct_native_target(name: &str) -> Option<&'static str> {
    Some(match name {
        "null" => "direct_native_null",
        "consp" => "direct_native_consp",
        "atom" => "direct_native_atom",
        "bare-symbol-p" => "direct_native_bare_symbol_p",
        "symbolp" => "direct_native_symbolp",
        "fboundp" => "direct_native_fboundp",
        "symbol-function" => "direct_native_symbol_function",
        "car" => "direct_native_car",
        "car-safe" => "direct_native_car_safe",
        "cdr" => "direct_native_cdr",
        "cdr-safe" => "direct_native_cdr_safe",
        "listp" => "direct_native_listp",
        "nlistp" => "direct_native_nlistp",
        "identity" => "direct_native_identity",
        "stringp" => "direct_native_stringp",
        "symbol-with-pos-p" => "direct_native_symbol_with_pos_p",
        "vectorp" => "direct_native_vectorp",
        "bufferp" => "direct_native_bufferp",
        "markerp" => "direct_native_markerp",
        "subrp" => "direct_native_subrp",
        "closurep" => "direct_native_closurep",
        "byte-code-function-p" => "direct_native_byte_code_function_p",
        "interpreted-function-p" => "direct_native_interpreted_function_p",
        "floatp" => "direct_native_floatp",
        "integerp" => "direct_native_integerp",
        "numberp" => "direct_native_numberp",
        "natnump" => "direct_native_natnump",
        "integer-or-marker-p" => "direct_native_integer_or_marker_p",
        "number-or-marker-p" => "direct_native_number_or_marker_p",
        "char-or-string-p" => "direct_native_char_or_string_p",
        "arrayp" => "direct_native_arrayp",
        "remove-pos-from-symbol" => "direct_native_remove_pos_from_symbol",
        "keywordp" => "direct_native_keywordp",
        "eq" => "direct_native_eq",
        "eql" => "direct_native_eql",
        "type-of" => "direct_native_type_of",
        "symbol-value" => "direct_native_symbol_value",
        "get" => "direct_native_get",
        "funcall" => "direct_native_funcall",
        "list" => "direct_native_list",
        "memq" => "direct_native_memq",
        "assq" => "direct_native_assq",
        "nreverse" => "direct_native_nreverse",
        "nconc" => "direct_native_nconc",
        "aref" => "direct_native_aref",
        "char-table-extra-slot" => "direct_native_char_table_extra_slot",
        "functionp" => "direct_native_functionp",
        "aset" => "direct_native_aset",
        "autoload-do-load" => "direct_native_autoload_do_load",
        "gethash" => "direct_native_gethash",
        "puthash" => "direct_native_puthash",
        "length" => "direct_native_length",
        "plist-member" => "direct_native_plist_member",
        "make-closure" => "direct_native_make_closure",
        _ => return None,
    })
}

// A subr whose runtime function answers from its argument words where the
// subr does not signal, and makes the subr's own call of any other
// arguments.  That call needs the subr, so the entry is generated: it
// passes its index in the table with its arguments to the runtime
// function, which is inlined into it, so that the index is a constant of
// the entry and nothing is searched for at run time.
fn indexed_native_target(name: &str) -> Option<&'static str> {
    Some(match name {
        "bare-symbol" => "direct_native_bare_symbol",
        "symbol-with-pos-pos" => "direct_native_symbol_with_pos_pos",
        "nth" => "direct_native_nth",
        _ => return None,
    })
}

fn read_min_arg_constants(directory: &Path) -> HashMap<String, u16> {
    [
        ("charset.h", "charset_arg_max"),
        ("coding.h", "coding_arg_max"),
    ]
    .into_iter()
    .map(|(header, name)| {
        let text = read_source(&directory.join(header));
        let end =
            identifier_position(&text, name).unwrap_or_else(|| panic!("find {name} in {header}"));
        let start = text[..end]
            .rfind('{')
            .unwrap_or_else(|| panic!("find enum containing {name} in {header}"));
        let ordinal = text[start + 1..end]
            .split(',')
            .filter(|entry| !entry.trim().is_empty())
            .count();
        (
            name.to_string(),
            u16::try_from(ordinal).expect("DEFUN minimum arity fits u16"),
        )
    })
    .collect()
}

fn read_defun_definitions(
    directory: &Path,
    min_arg_constants: &HashMap<String, u16>,
) -> HashMap<String, Primitive> {
    let mut result: HashMap<String, Primitive> = HashMap::new();
    for entry in fs::read_dir(directory).expect("read GNU src directory") {
        let path = entry.expect("read GNU src entry").path();
        if !matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("c" | "m")
        ) {
            continue;
        }
        let text = read_source(&path);
        for (identifier, primitive) in defun_definitions(&text, min_arg_constants) {
            if let Some(previous) = result.get(&identifier) {
                if previous.name != primitive.name
                    || previous.min_args != primitive.min_args
                    || previous.max_args != primitive.max_args
                {
                    // The X, NS, W32, and Android implementations can reuse a
                    // DEFUN identifier with platform-specific signatures.  A
                    // registration source's local definition is authoritative;
                    // retain the first definition here only as a cross-file
                    // fallback for the uncommon split-definition case.
                    continue;
                }
            }
            result.insert(identifier, primitive);
        }
    }
    result
}

fn defun_definitions(
    text: &str,
    min_arg_constants: &HashMap<String, u16>,
) -> HashMap<String, Primitive> {
    let mut result = HashMap::new();
    for arguments in macro_arguments(text, "DEFUN") {
        let parts = split_top_level(&arguments);
        if parts.len() < 5 {
            continue;
        }
        let Some(name) = parse_c_string(parts[0].trim()) else {
            continue;
        };
        let identifier = parts[2].trim().to_string();
        let min_args_text = parts[3].trim();
        let Some(min_args) = min_args_text
            .parse::<u16>()
            .ok()
            .or_else(|| min_arg_constants.get(min_args_text).copied())
        else {
            continue;
        };
        let max_args = match parts[4].trim() {
            "MANY" => -2,
            "UNEVALLED" => -1,
            value => match value.parse::<i32>() {
                Ok(value) => value,
                Err(_) => continue,
            },
        };
        if identifier.starts_with('S') {
            let primitive = Primitive {
                name,
                min_args,
                max_args,
            };
            if let Some(previous) = result.insert(identifier.clone(), primitive.clone()) {
                assert_eq!(
                    previous.name, primitive.name,
                    "DEFUN identifier {identifier} is ambiguous within one source"
                );
                assert_eq!(
                    previous.min_args, primitive.min_args,
                    "DEFUN identifier {identifier} has inconsistent minimum arity within one source"
                );
                assert_eq!(
                    previous.max_args, primitive.max_args,
                    "DEFUN identifier {identifier} has inconsistent arity within one source"
                );
            }
        }
    }
    result
}

fn read_compiled_subrs(binary: &Path) -> HashSet<String> {
    let output = Command::new("nm")
        .args(["-n"])
        .arg(binary)
        .output()
        .unwrap_or_else(|error| panic!("run nm on {}: {error}", binary.display()));
    assert!(
        output.status.success(),
        "nm failed for {}",
        binary.display()
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .map(|symbol| symbol.strip_prefix('_').unwrap_or(symbol))
        .filter(|symbol| symbol.starts_with('S'))
        .map(str::to_string)
        .collect()
}

fn defsubr_identifiers(text: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut offset = 0;
    while let Some(relative) = text[offset..].find("defsubr") {
        let start = offset + relative + "defsubr".len();
        let tail = &text[start..];
        let Some(open) = tail.find('(') else {
            break;
        };
        let tail = &tail[open + 1..];
        let Some(ampersand) = tail.find("&S") else {
            offset = start;
            continue;
        };
        if ampersand > 32 {
            offset = start;
            continue;
        }
        let identifier = tail[ampersand + 1..]
            .chars()
            .take_while(|character| character.is_ascii_alphanumeric() || *character == '_')
            .collect::<String>();
        if !identifier.is_empty() {
            result.push(identifier);
        }
        offset = start;
    }
    result
}

fn macro_arguments(text: &str, name: &str) -> Vec<String> {
    let needle = format!("{name} (");
    let mut result = Vec::new();
    let mut offset = 0;
    while let Some(relative) = text[offset..].find(&needle) {
        let start = offset + relative + needle.len();
        let mut depth = 1_i32;
        let mut commas = 0_u8;
        let mut quoted = false;
        let mut escaped = false;
        let mut end = start;
        for (relative, character) in text[start..].char_indices() {
            if quoted {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    quoted = false;
                }
            } else if character == '"' {
                quoted = true;
            } else if character == '(' {
                depth += 1;
            } else if character == ')' {
                depth -= 1;
            } else if character == ',' && depth == 1 {
                commas += 1;
                if commas == 5 {
                    end = start + relative;
                    break;
                }
            }
        }
        if commas != 5 {
            offset = start;
            continue;
        }
        result.push(text[start..end].to_string());
        offset = end + 1;
    }
    result
}

fn split_top_level(arguments: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in arguments.char_indices() {
        if quoted {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                quoted = false;
            }
        } else if character == '"' {
            quoted = true;
        } else if character == '(' {
            depth += 1;
        } else if character == ')' {
            depth -= 1;
        } else if character == ',' && depth == 0 {
            result.push(&arguments[start..index]);
            start = index + 1;
        }
    }
    result.push(&arguments[start..]);
    result
}

fn parse_c_string(value: &str) -> Option<String> {
    let value = value.strip_prefix('"')?;
    let end = find_string_end(value, 0)?;
    Some(unescape_string(&value[..end]))
}

fn read_c_define_string(path: &Path, name: &str) -> String {
    let text = read_source(path);
    let prefix = format!("#define {name}");
    let value = text
        .lines()
        .map(str::trim)
        .find_map(|line| {
            line.strip_prefix(&prefix)
                .filter(|tail| tail.starts_with(char::is_whitespace))
                .map(str::trim)
        })
        .unwrap_or_else(|| panic!("find C string definition {name} in {}", path.display()));
    parse_c_string(value)
        .unwrap_or_else(|| panic!("parse C string definition {name} in {}", path.display()))
}

fn identifier_position(text: &str, identifier: &str) -> Option<usize> {
    text.match_indices(identifier).find_map(|(position, _)| {
        let before = text[..position].chars().next_back();
        let after = text[position + identifier.len()..].chars().next();
        let is_identifier = |character: char| character.is_ascii_alphanumeric() || character == '_';
        (!before.is_some_and(is_identifier) && !after.is_some_and(is_identifier))
            .then_some(position)
    })
}

fn find_string_end(value: &str, start: usize) -> Option<usize> {
    let mut escaped = false;
    for (relative, byte) in value.as_bytes()[start..].iter().enumerate() {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            return Some(start + relative);
        }
    }
    None
}

fn unescape_string(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            result.push(character);
            continue;
        }
        match characters.next().expect("unterminated string escape") {
            '\\' => result.push('\\'),
            '"' => result.push('"'),
            'n' => result.push('\n'),
            'r' => result.push('\r'),
            't' => result.push('\t'),
            other => panic!("unsupported string escape \\{other}"),
        }
    }
    result
}

fn read_source(path: &Path) -> String {
    let bytes = fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    strip_comments(&String::from_utf8_lossy(&bytes))
}

/// C text without its comments.  Directives-only preprocessing keeps
/// comments, and GNU sources comment out calls such as `syms_of_keymap ();`
/// and write apostrophes in prose, so every scan runs on stripped text.
fn strip_comments(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut quoted: Option<char> = None;
    let mut escaped = false;
    let mut rest = text;
    while let Some(character) = rest.chars().next() {
        if let Some(quote) = quoted {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == quote {
                quoted = None;
            }
        } else if let Some(tail) = rest.strip_prefix("/*") {
            rest = tail.find("*/").map_or("", |end| &tail[end + 2..]);
            result.push(' ');
            continue;
        } else if rest.starts_with("//") {
            rest = rest.find('\n').map_or("", |end| &rest[end..]);
            continue;
        } else if character == '"' || character == '\'' {
            quoted = Some(character);
        }
        result.push(character);
        rest = &rest[character.len_utf8()..];
    }
    result
}

/// The argument-less calls made by `main` in the configured `emacs.c`, in
/// order.  Every C-owned subroutine registration happens inside one of them.
fn startup_calls(directory: &Path) -> Vec<String> {
    let text = preprocess_registration_source(directory, "emacs.c");
    let body = function_body(&text, "main").expect("emacs.c defines main");
    let mut result = Vec::new();
    let mut offset = 0;
    while let Some(relative) = body[offset..].find(" ();") {
        let end = offset + relative;
        let start = body[..end]
            .rfind(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
            .map_or(0, |position| position + 1);
        if start < end {
            result.push(body[start..end].to_string());
        }
        offset = end + " ();".len();
    }
    result
}

/// Source files under GNU `src` that the reference build compiled, register
/// subroutines, and define `NAME (void)`.  Platform ports can define the
/// same startup function in different files, and sources for other
/// platforms do not even preprocess on this one, so a candidate must have
/// its object file in the built tree; the preprocessed text then decides
/// which definition is compiled in.
fn definition_sources(directory: &Path, name: &str) -> Vec<String> {
    let mut result = Vec::new();
    for entry in fs::read_dir(directory).expect("read GNU src directory") {
        let path = entry.expect("read GNU src entry").path();
        if !matches!(
            path.extension().and_then(|value| value.to_str()),
            Some("c" | "m")
        ) {
            continue;
        }
        if !path.with_extension("o").is_file() {
            continue;
        }
        let text = read_source(&path);
        if text.contains("defsubr") && function_body(&text, name).is_some() {
            result.push(
                path.file_name()
                    .and_then(|value| value.to_str())
                    .expect("GNU source names are UTF-8")
                    .to_string(),
            );
        }
    }
    result.sort();
    result
}

/// The brace-delimited body of the definition `NAME (...)` in C text, if the
/// text contains one.  Only a definition at column zero counts, which is how
/// GNU formats every function definition.
fn function_body<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    let needle = format!("\n{name} (");
    let mut offset = 0;
    while let Some(relative) = text[offset..].find(&needle) {
        let start = offset + relative + 1;
        let open = text[start..].find('{')? + start;
        let signature = &text[start..open];
        if signature.contains(';') {
            offset = start;
            continue;
        }
        let mut depth = 0_i32;
        let mut quoted: Option<char> = None;
        let mut escaped = false;
        for (relative, character) in text[open..].char_indices() {
            if let Some(quote) = quoted {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == quote {
                    quoted = None;
                }
            } else if character == '"' || character == '\'' {
                quoted = Some(character);
            } else if character == '{' {
                depth += 1;
            } else if character == '}' {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[open..open + relative + 1]);
                }
            }
        }
        return None;
    }
    None
}

fn preprocess_registration_source(directory: &Path, source: &str) -> String {
    let object = Path::new(source).with_extension("o");
    let dry_run = Command::new("make")
        .args(["-W", source, "-n", "V=1"])
        .arg(&object)
        .current_dir(directory)
        .output()
        .unwrap_or_else(|error| panic!("query build command for {source}: {error}"));
    assert!(
        dry_run.status.success(),
        "querying build command for {source} failed: {}",
        String::from_utf8_lossy(&dry_run.stderr)
    );

    let output = String::from_utf8_lossy(&dry_run.stdout);
    let compile = output
        .lines()
        .rev()
        .find(|line| {
            line.contains(" -c ")
                && line
                    .split_ascii_whitespace()
                    .next_back()
                    .is_some_and(|argument| argument == source)
        })
        .unwrap_or_else(|| panic!("make did not expose the compile command for {source}"));
    // Directives-only preprocessing resolves the build's `#if` structure
    // while leaving `DEFUN (...)` and `defsubr (&S...)` readable.  GCC rejects
    // that mode together with `-Wunused-macros`, and it cannot evaluate
    // gnulib's `__COUNTER__` probe inside a directive, so the diagnostic is
    // dropped and the builtin is undefined; neither changes which code is
    // active.
    let preprocess = compile.replacen(" -c ", " -E -fdirectives-only -U__COUNTER__ ", 1);
    let preprocess = preprocess.replace(" -Wunused-macros ", " ");
    // The ordinary compile command requests a dependency side file.  Point
    // that output at /dev/null so regeneration is a read-only operation on
    // the reference tree.
    let preprocess = format!("{preprocess} -MF /dev/null");
    let preprocessed = Command::new("/bin/sh")
        .args(["-c", &preprocess])
        .current_dir(directory)
        .output()
        .unwrap_or_else(|error| panic!("preprocess {source}: {error}"));
    assert!(
        preprocessed.status.success(),
        "preprocessing {source} failed: {}",
        String::from_utf8_lossy(&preprocessed.stderr)
    );
    strip_comments(&String::from_utf8_lossy(&preprocessed.stdout))
}
