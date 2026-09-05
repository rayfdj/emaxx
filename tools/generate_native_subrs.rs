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
//! `TOOL --layout GNU-SRC PROBE-C OUTPUT-EXECUTABLE` also compiles and runs
//! the test-only header layout probe with the configured C compiler flags.

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

fn main() {
    let mut arguments = env::args_os().skip(1);
    let first = arguments
        .next()
        .expect("usage: TOOL GNU-SRC GNU-BINARY OUTPUT");
    if first == "--layout" {
        let paths = arguments.map(PathBuf::from).collect::<Vec<_>>();
        let [source, probe, executable] = paths.as_slice() else {
            panic!("usage: TOOL --layout GNU-SRC PROBE-C OUTPUT-EXECUTABLE");
        };
        report_configured_layout(source, probe, executable);
        return;
    }
    let gnu_src = PathBuf::from(first);
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
    let mut preprocessed: HashMap<String, String> = HashMap::new();

    for function in startup_calls(&gnu_src) {
        for source in definition_sources(&gnu_src, &function) {
            let text = preprocessed
                .entry(source.clone())
                .or_insert_with(|| preprocess_registration_source(&gnu_src, &source));
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

fn configured_compile_command(directory: &Path, source: &str) -> String {
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
    output
        .lines()
        .rev()
        .find(|line| {
            line.contains(" -c ")
                && line
                    .split_ascii_whitespace()
                    .next_back()
                    .is_some_and(|argument| argument == source)
        })
        .unwrap_or_else(|| panic!("make did not expose the compile command for {source}"))
        .to_string()
}

fn report_configured_layout(directory: &Path, probe: &Path, executable: &Path) {
    let compile = configured_compile_command(directory, "emacs.c");
    let flags = compile.strip_suffix("emacs.c").expect("emacs.c recipe");
    assert!(!flags.contains(" -o "), "unexpected configured output flag");
    // Keep every ABI-affecting flag and header search path. Only replace
    // compile-only mode, source, output, and dependency destination. Paths
    // are shell positional arguments, never interpolated into the recipe.
    let compile = format!(
        "{} \"$1\" -MF /dev/null -o \"$2\" -Werror",
        flags.replacen(" -c ", " ", 1)
    );
    let compiled = Command::new("/bin/sh")
        .args(["-c", &compile, "native-abi-layout"])
        .arg(probe)
        .arg(executable)
        .current_dir(directory)
        .output()
        .expect("compile configured GNU-header layout probe");
    assert!(
        compiled.status.success() && compiled.stderr.is_empty(),
        "GNU-header layout probe compilation failed or emitted diagnostics:\n{}\n{}",
        String::from_utf8_lossy(&compiled.stdout),
        String::from_utf8_lossy(&compiled.stderr)
    );
    let measured = Command::new(executable)
        .output()
        .expect("run configured GNU-header layout probe");
    assert!(
        measured.status.success() && measured.stderr.is_empty(),
        "GNU-header layout probe failed: {}",
        String::from_utf8_lossy(&measured.stderr)
    );
    print!(
        "{}",
        String::from_utf8(measured.stdout).expect("ASCII layout facts")
    );
}

fn preprocess_registration_source(directory: &Path, source: &str) -> String {
    let compile = configured_compile_command(directory, source);
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
