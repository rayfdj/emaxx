//! Regenerate the native compiler's C-subroutine ABI order from GNU C source.
//!
//! This is a development tool, not runtime machinery.  It follows the
//! startup registration order in GNU `emacs.c`, reads each active C-owned
//! `defsubr` call after applying the reference build's C preprocessor
//! configuration, filters it through the linked GNU binary, and finally
//! reverses the sequence because GNU `defsubr` conses each entry onto
//! `Vcomp_subr_list`.

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const REGISTRATION_SOURCES: &[&str] = &[
    "xfaces.c",
    "keymap.c",
    "keyboard.c",
    "data.c",
    "fns.c",
    "fileio.c",
    "alloc.c",
    "charset.c",
    "coding.c",
    "textconv.c",
    "comp.c",
    "callproc.c",
    "chartab.c",
    "lread.c",
    "print.c",
    "eval.c",
    "floatfns.c",
    "buffer.c",
    "bytecode.c",
    "callint.c",
    "casefiddle.c",
    "casetab.c",
    "category.c",
    "ccl.c",
    "character.c",
    "cmds.c",
    "dired.c",
    "dispnew.c",
    "doc.c",
    "editfns.c",
    "emacs.c",
    "filelock.c",
    "indent.c",
    "insdel.c",
    "macros.c",
    "marker.c",
    "minibuf.c",
    "process.c",
    "search.c",
    "sysdep.c",
    "timefns.c",
    "frame.c",
    "syntax.c",
    "terminal.c",
    "term.c",
    "undo.c",
    "emacs-module.c",
    "treesit.c",
    "sound.c",
    "textprop.c",
    "composite.c",
    "window.c",
    "xdisp.c",
    "sqlite.c",
    "font.c",
    "fringe.c",
    "image.c",
    "xml.c",
    "lcms.c",
    "decompress.c",
    "menu.c",
    "nsterm.m",
    "nsfns.m",
    "nsmenu.m",
    "nsselect.m",
    "fontset.c",
    "gnutls.c",
    "kqueue.c",
    "xwidget.c",
    "thread.c",
    "profiler.c",
    "pdumper.c",
    "json.c",
];

#[derive(Clone)]
struct Primitive {
    name: String,
    min_args: u16,
    max_args: i32,
}

fn main() {
    let mut arguments = env::args_os().skip(1);
    let gnu_src = PathBuf::from(
        arguments
            .next()
            .expect("usage: TOOL GNU-SRC GNU-BINARY OUTPUT"),
    );
    let gnu_binary = PathBuf::from(
        arguments
            .next()
            .expect("usage: TOOL GNU-SRC GNU-BINARY OUTPUT"),
    );
    let output = PathBuf::from(
        arguments
            .next()
            .expect("usage: TOOL GNU-SRC GNU-BINARY OUTPUT"),
    );
    assert!(
        arguments.next().is_none(),
        "usage: TOOL GNU-SRC GNU-BINARY OUTPUT"
    );

    let min_arg_constants = read_min_arg_constants(&gnu_src);
    let definitions = read_defun_definitions(&gnu_src, &min_arg_constants);
    let compiled_subrs = read_compiled_subrs(&gnu_binary);
    let abi_version = read_c_define_string(&gnu_src.join("comp.c"), "ABI_VERSION");
    let abi_configuration = read_c_define_string(&gnu_src.join("config.h"), "EMACS_CONFIGURATION");
    let abi_configuration_options =
        read_c_define_string(&gnu_src.join("config.h"), "EMACS_CONFIG_OPTIONS");
    let mut registration = Vec::with_capacity(compiled_subrs.len());
    let mut seen = HashSet::new();

    for source in REGISTRATION_SOURCES {
        let path = gnu_src.join(source);
        if !path.exists() {
            continue;
        }
        let text = preprocess_registration_source(&gnu_src, source);
        let local_definitions = defun_definitions(&text, &min_arg_constants);
        for identifier in defsubr_identifiers(&text) {
            // print.c registers this debugger helper from init_print_once,
            // which emacs.c calls between syms_of_alloc and syms_of_charset;
            // it is not part of the later syms_of_print registration pass.
            if *source == "print.c" && identifier == "Sexternal_debugging_output" {
                continue;
            }
            if !compiled_subrs.contains(&identifier) {
                continue;
            }
            let Some(primitive) = local_definitions
                .get(&identifier)
                .or_else(|| definitions.get(&identifier))
            else {
                continue;
            };
            assert!(
                seen.insert(primitive.name.clone()),
                "compiled primitive {:?} was registered more than once",
                primitive.name
            );
            registration.push(primitive.clone());
        }
        if *source == "alloc.c" {
            let identifier = "Sexternal_debugging_output";
            let primitive = definitions
                .get(identifier)
                .expect("print.c defines external-debugging-output");
            assert!(
                compiled_subrs.contains(identifier),
                "external-debugging-output is missing from the reference binary"
            );
            assert!(
                seen.insert(primitive.name.clone()),
                "compiled primitive {:?} was registered more than once",
                primitive.name
            );
            registration.push(primitive.clone());
        }
    }

    assert!(
        registration.len() <= compiled_subrs.len(),
        "registration cannot exceed compiled subroutine symbols"
    );

    registration.reverse();
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
    generated.push_str("pub(crate) const NATIVE_SUBRS: &[NativeSubr] = &[\n");
    for primitive in &registration {
        let max_args = match primitive.max_args {
            -2 => "NativeMaxArgs::Many".to_string(),
            -1 => "NativeMaxArgs::Unevalled".to_string(),
            value => format!("NativeMaxArgs::Fixed({value})"),
        };
        generated.push_str(&format!(
            "    NativeSubr {{ name: {:?}, min_args: {}, max_args: {max_args} }},\n",
            primitive.name, primitive.min_args
        ));
    }
    generated.push_str("];\n\n");
    generated.push_str(
        "pub(crate) fn native_subr_address(index: usize) -> *mut std::ffi::c_void {\n\
         \x20   match index {\n",
    );
    for (index, _) in registration.iter().enumerate() {
        generated.push_str(&format!(
            "        {index} => native_subr_{index:04} as *mut std::ffi::c_void,\n"
        ));
    }
    generated.push_str(
        "        _ => std::ptr::null_mut(),\n\
         \x20   }\n\
         }\n\n",
    );
    for (index, primitive) in registration.iter().enumerate() {
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
                let parameters = (0..count)
                    .map(|argument| format!("arg_{argument}: super::runtime::NativeWord"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let arguments = (0..count)
                    .map(|argument| format!("arg_{argument}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                generated.push_str(&format!(
                    "extern \"C\" fn native_subr_{index:04}({parameters}) -> super::runtime::NativeWord {{\n    super::runtime::invoke_subr({index}, &[{arguments}])\n}}\n\n"
                ));
            }
        }
    }
    fs::write(&output, generated)
        .unwrap_or_else(|error| panic!("write {}: {error}", output.display()));
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
        .args(["-nm"])
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
    String::from_utf8_lossy(&bytes).into_owned()
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
    let preprocess = compile.replacen(" -c ", " -E -fdirectives-only ", 1);
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
    String::from_utf8_lossy(&preprocessed.stdout).into_owned()
}
