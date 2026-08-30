/// Define a primitive dispatcher and derive its name probe from the same arms.
///
/// Dispatch routing and implementation must share one inventory: otherwise a
/// newly implemented primitive can remain unreachable merely because a second
/// `handles` list was not updated.
macro_rules! dispatch_handles {
    ($name:ident;) => {
        false
    };
    ($name:ident; , $($rest:tt)*) => {
        dispatch_handles!($name; $($rest)*)
    };
    (
        $name:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        matches!($name, $pattern) || dispatch_handles!($name; $($rest)*)
    };
    (
        $name:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        matches!($name, $pattern) || dispatch_handles!($name; $($rest)*)
    };
}

#[cfg(test)]
macro_rules! dispatch_visit_patterns {
    ($visitor:ident;) => {
        ()
    };
    ($visitor:ident; , $($rest:tt)*) => {
        dispatch_visit_patterns!($visitor; $($rest)*)
    };
    (
        $visitor:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {{
        $visitor(stringify!($pattern));
        dispatch_visit_patterns!($visitor; $($rest)*)
    }};
    (
        $visitor:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {{
        $visitor(stringify!($pattern));
        dispatch_visit_patterns!($visitor; $($rest)*)
    }};
}

macro_rules! dispatch_select_builtin_override {
    ($name:ident, $pattern:pat =>) => {
        false
    };
    ($name:ident, $pattern:pat => builtin_override $(, $rest:ident)*) => {
        matches!($name, $pattern)
    };
    ($name:ident, $pattern:pat => $other:ident $(, $rest:ident)*) => {
        dispatch_select_builtin_override!($name, $pattern => $($rest),*)
    };
}

macro_rules! dispatch_property {
    ($selector:ident, $name:ident;) => {
        false
    };
    ($selector:ident, $name:ident; , $($rest:tt)*) => {
        dispatch_property!($selector, $name; $($rest)*)
    };
    (
        $selector:ident, $name:ident;
        #[dispatch($($property:ident),+)]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        $selector!($name, $pattern => $($property),+)
            || dispatch_property!($selector, $name; $($rest)*)
    };
    (
        $selector:ident, $name:ident;
        #[dispatch($($property:ident),+)]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        $selector!($name, $pattern => $($property),+)
            || dispatch_property!($selector, $name; $($rest)*)
    };
    (
        $selector:ident, $name:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        dispatch_property!($selector, $name; $($rest)*)
    };
    (
        $selector:ident, $name:ident;
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_property!($selector, $name; $($rest)*)
    };
}

macro_rules! dispatch_call {
    ($name:ident; $($arms:tt)*) => {
        dispatch_call!(@collect $name [] $($arms)*)
    };
    (@collect $name:ident [$($collected:tt)*]) => {
        match $name {
            $($collected)*
            _ => unreachable!("primitive dispatcher called for unsupported name: {}", $name),
        }
    };
    (@collect $name:ident [$($collected:tt)*] , $($rest:tt)*) => {
        dispatch_call!(@collect $name [$($collected)*] $($rest)*)
    };
    (
        @collect $name:ident [$($collected:tt)*]
        #[dispatch($($property:ident),+)]
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect $name [$($collected)*] $($rest)*)
    };
    (
        @collect $name:ident [$($collected:tt)*]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:block
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect $name [
            $($collected)*
            $(#[$attribute])*
            $pattern $(if $guard)? => $body,
        ] $($rest)*)
    };
    (
        @collect $name:ident [$($collected:tt)*]
        $(#[$attribute:meta])*
        $pattern:pat $(if $guard:expr)? => $body:expr,
        $($rest:tt)*
    ) => {
        dispatch_call!(@collect $name [
            $($collected)*
            $(#[$attribute])*
            $pattern $(if $guard)? => $body,
        ] $($rest)*)
    };
}

macro_rules! define_dispatch {
    (
        $(#[$attribute:meta])*
        $visibility:vis fn $call:ident(
            $($argument:ident: $argument_type:ty),* $(,)?
        ) -> $return_type:ty {
            match $name:ident {
                $($arms:tt)*
            }
        }
    ) => {
        $visibility fn handles(name: &str) -> bool {
            dispatch_handles!(name; $($arms)*)
        }

        $visibility fn prefer_builtin(name: &str) -> bool {
            let _ = name;
            dispatch_property!(dispatch_select_builtin_override, name; $($arms)*)
        }

        #[cfg(test)]
        $visibility fn visit_handled_patterns(visitor: &mut impl FnMut(&'static str)) {
            dispatch_visit_patterns!(visitor; $($arms)*);
        }

        $(#[$attribute])*
        $visibility fn $call(
            $($argument: $argument_type),*
        ) -> $return_type {
            dispatch_call!($name; $($arms)*)
        }
    };
}

pub mod bytecode;
pub mod eval;
pub mod json;
pub mod primitives;
pub mod reader;
pub mod sqlite;
pub mod types;

use std::collections::HashMap;
use std::path::Path;

use crate::compat::TestStatus;

/// One test's outcome: name, passed, optional error message.
pub type TestResult = (String, bool, Option<String>);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct SourceFileSettings {
    lexical_binding: bool,
    read_symbol_shorthands: Vec<(String, String)>,
}

fn append_message(interp: &mut eval::Interpreter, text: &str) {
    let buffer_id = interp
        .find_buffer("*Messages*")
        .map(|(id, _)| id)
        .unwrap_or_else(|| interp.create_buffer("*Messages*").0);
    if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        let end = buffer.point_max();
        buffer.goto_char(end);
        buffer.insert(&(text.to_string() + "\n"));
    }
}

fn unescaped_character_literal_value(reader: &reader::Reader<'_>) -> types::Value {
    types::Value::list(
        reader
            .unescaped_character_literals()
            .map(types::Value::Integer),
    )
}

pub(crate) fn unescaped_character_literal_warning(
    interp: &mut eval::Interpreter,
    env: &mut types::Env,
) -> Result<Option<String>, types::LispError> {
    let Ok(function) =
        interp.lookup_function("byte-run--unescaped-character-literals-warning", env)
    else {
        return Ok(None);
    };
    let warning = interp.call_function_value(
        function,
        Some("byte-run--unescaped-character-literals-warning"),
        &[],
        env,
    )?;
    if warning.is_nil() {
        Ok(None)
    } else {
        primitives::string_text(&warning).map(Some)
    }
}

fn format_loading_warning(
    interp: &mut eval::Interpreter,
    env: &mut types::Env,
    path: &Path,
    warning: String,
) -> Result<String, types::LispError> {
    let function = interp.lookup_function("format-message", env)?;
    let message = interp.call_function_value(
        function,
        Some("format-message"),
        &[
            types::Value::String("Loading `%s': %s".into()),
            types::Value::String(path.display().to_string().into()),
            types::Value::String(warning.into()),
        ],
        env,
    )?;
    primitives::string_text(&message)
}

fn lisp_string_literal(text: &str) -> String {
    let mut rendered = String::from("\"");
    for ch in text.chars() {
        match ch {
            '\\' => rendered.push_str("\\\\"),
            '"' => rendered.push_str("\\\""),
            '\n' => rendered.push_str("\\n"),
            '\r' => rendered.push_str("\\r"),
            '\t' => rendered.push_str("\\t"),
            _ => rendered.push(ch),
        }
    }
    rendered.push('"');
    rendered
}

/// Copy one reader escape outside strings and comments.
///
/// GNU's reader treats the backslash and the following byte as part of the
/// same symbol token.  Lazy `.elc' markers therefore must not recognize the
/// escaped `#' in names such as `byte-compile--\#$'.
fn copy_reader_escape(bytes: &[u8], index: &mut usize, out: &mut Vec<u8>) -> bool {
    if bytes.get(*index) != Some(&b'\\') {
        return false;
    }
    out.push(b'\\');
    *index += 1;
    if let Some(byte) = bytes.get(*index) {
        out.push(*byte);
        *index += 1;
    }
    true
}

fn rewrite_lazy_doc_refs(
    text: &str,
    path: &Path,
    docs: &HashMap<usize, String>,
    force_load_doc_strings: bool,
) -> String {
    let path_literal = lisp_string_literal(&path.display().to_string());
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut in_comment = false;

    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            out.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if in_comment {
            out.push(byte);
            if byte == b'\n' {
                in_comment = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            out.push(byte);
            index += 1;
            continue;
        }
        if byte == b';' {
            in_comment = true;
            out.push(byte);
            index += 1;
            continue;
        }
        if copy_reader_escape(bytes, &mut index, &mut out) {
            continue;
        }

        if bytes[index..].starts_with(b"(#$ . ") {
            let digits_start = index + 6;
            let mut cursor = digits_start;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if cursor > digits_start && cursor < bytes.len() && bytes[cursor] == b')' {
                let offset = text[digits_start..cursor].parse::<usize>().ok();
                if force_load_doc_strings
                    && let Some(offset) = offset
                    && let Some(doc) = docs.get(&offset)
                {
                    out.extend_from_slice(lisp_string_literal(doc).as_bytes());
                } else {
                    out.push(b'(');
                    out.extend_from_slice(path_literal.as_bytes());
                    out.extend_from_slice(b" . ");
                    out.extend_from_slice(&bytes[digits_start..cursor]);
                    out.push(b')');
                }
                index = cursor + 1;
                continue;
            }
        }

        if bytes[index..].starts_with(b"#$") {
            out.extend_from_slice(path_literal.as_bytes());
            index += 2;
            continue;
        }

        out.push(byte);
        index += 1;
    }

    String::from_utf8(out).expect("rewriting valid UTF-8 preserves UTF-8")
}

pub(crate) fn preprocess_lazy_doc_source(
    path: &Path,
    source: &str,
    force_load_doc_strings: bool,
) -> String {
    let bytes = source.as_bytes();
    let mut docs = HashMap::new();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    let mut in_comment = false;

    while index < bytes.len() {
        let byte = bytes[index];
        if in_string {
            out.push(byte);
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if in_comment {
            out.push(byte);
            if byte == b'\n' {
                in_comment = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            out.push(byte);
            index += 1;
            continue;
        }
        if byte == b';' {
            in_comment = true;
            out.push(byte);
            index += 1;
            continue;
        }
        if copy_reader_escape(bytes, &mut index, &mut out) {
            continue;
        }

        if bytes[index..].starts_with(b"#@") {
            let digits_start = index + 2;
            let mut cursor = digits_start;
            while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                cursor += 1;
            }
            if cursor > digits_start {
                let count = source[digits_start..cursor].parse::<usize>().unwrap_or(0);
                if count == 0 {
                    out.extend_from_slice(b"nil");
                    break;
                }
                if cursor < bytes.len() {
                    cursor += 1;
                }
                let content_start = cursor;
                // COUNT is measured from the first byte after the digits,
                // so the byte just consumed is part of it; the counted
                // region ends with the ^_ terminator.  Reading one byte
                // past it ate the `#' of an immediately following `#@N'
                // block (adjacent docstrings, e.g. a documented lambda
                // inside a documented defun).
                let content_end = content_start
                    .saturating_add(count.saturating_sub(1))
                    .min(bytes.len());
                let mut doc =
                    String::from_utf8_lossy(&bytes[content_start..content_end]).into_owned();
                if doc.ends_with('\n') {
                    doc.pop();
                }
                if doc.ends_with('\u{1f}') {
                    doc.pop();
                }
                docs.insert(content_start, doc);
                index = content_end;
                continue;
            }
        }

        out.push(byte);
        index += 1;
    }

    let out = String::from_utf8(out).expect("removing byte ranges preserves valid UTF-8");
    rewrite_lazy_doc_refs(&out, path, &docs, force_load_doc_strings)
}

fn push_raw_source_string_byte(output: &mut String, byte: u8, in_string: bool) -> bool {
    if !in_string {
        return false;
    }
    // A no-conversion .elc embeds the byte-code instruction stream directly
    // inside a Lisp string.  Those bytes are not necessarily a well-formed
    // utf-8-emacs sequence.  Re-express one such byte as a reader escape so
    // the resulting Lisp string stays unibyte and recovers the exact octet.
    output.push_str(&format!("\\x{byte:X}\\ "));
    true
}

fn decode_utf8_emacs_source(path: &Path, bytes: &[u8]) -> Result<String, types::LispError> {
    let mut output = String::with_capacity(bytes.len());
    let mut index = 0usize;
    let mut in_string = false;
    let mut in_comment = false;
    let mut escaped = false;

    while index < bytes.len() {
        let byte = bytes[index];
        if byte < 0x80 {
            let ch = char::from(byte);
            output.push(ch);
            index += 1;
            if in_comment {
                if ch == '\n' {
                    in_comment = false;
                }
                continue;
            }
            if in_string {
                if escaped {
                    escaped = false;
                } else if ch == '\\' {
                    escaped = true;
                } else if ch == '"' {
                    in_string = false;
                }
                continue;
            }
            if ch == ';' {
                in_comment = true;
            } else if ch == '"' {
                in_string = true;
            }
            continue;
        }

        let (width, raw_byte8) = match byte {
            0xC0..=0xC1 => (2, true),
            0xC2..=0xDF => (2, false),
            0xE0..=0xEF => (3, false),
            0xF0..=0xF7 => (4, false),
            0xF8 => (5, false),
            _ if push_raw_source_string_byte(&mut output, byte, in_string) => {
                index += 1;
                continue;
            }
            _ => {
                return Err(types::LispError::Signal(format!(
                    "Cannot decode {} as utf-8-emacs at byte {}",
                    path.display(),
                    index
                )));
            }
        };
        let Some(sequence) = bytes.get(index..index + width) else {
            if push_raw_source_string_byte(&mut output, byte, in_string) {
                index += 1;
                continue;
            }
            return Err(types::LispError::Signal(format!(
                "Truncated utf-8-emacs sequence in {} at byte {}",
                path.display(),
                index
            )));
        };
        if !sequence[1..]
            .iter()
            .all(|continuation| (0x80..=0xBF).contains(continuation))
        {
            if push_raw_source_string_byte(&mut output, byte, in_string) {
                index += 1;
                continue;
            }
            return Err(types::LispError::Signal(format!(
                "Invalid utf-8-emacs sequence in {} at byte {}",
                path.display(),
                index
            )));
        }
        let mut code = u32::from(byte & (0x7F >> width));
        for continuation in &sequence[1..] {
            code = (code << 6) | u32::from(continuation & 0x3F);
        }
        let minimum = match width {
            2 => 0x80,
            3 => 0x800,
            4 => 0x1_0000,
            5 => 0x20_0000,
            _ => unreachable!(),
        };
        if raw_byte8 {
            // GNU's internal C0/C1 two-byte form carries a raw byte in
            // 0x3FFF80..0x3FFFFF, not the public BYTE8_TO_CHAR base used
            // before subtracting the high-bit offset.
            code += 0x3F_FF80;
        } else if code < minimum || code > 0x3F_FF7F || (width == 4 && code > 0x1F_FFFF) {
            if push_raw_source_string_byte(&mut output, byte, in_string) {
                index += 1;
                continue;
            }
            return Err(types::LispError::Signal(format!(
                "Invalid utf-8-emacs codepoint in {} at byte {}",
                path.display(),
                index
            )));
        }
        if let Some(ch) = char::from_u32(code) {
            output.push(ch);
        } else if in_comment {
            // Comments are not reader data.  Keep the diagnostic meaning
            // without pretending the out-of-Unicode codepoint is a Rust char.
            output.push_str(&format!("U+{code:X}"));
        } else if in_string {
            // The reader owns the typed representation of extended string
            // characters.  Re-express the raw utf-8-emacs spelling as an
            // unambiguous GNU hex escape; backslash-space terminates the
            // variable-width escape without adding a character.
            output.push_str(&format!("\\x{code:X}\\ "));
        } else if output.ends_with('?') {
            // The Lisp reader already represents characters as integers up to
            // MAX_CHAR.  Re-express the utf-8-emacs byte spelling as GNU's
            // equivalent hex character-literal spelling so no string-level
            // surrogate representation is introduced.
            output.push_str(&format!("\\x{code:X}"));
        } else {
            return Err(types::LispError::Signal(format!(
                "Cannot represent utf-8-emacs character U+{code:X} from {} in a Lisp symbol",
                path.display()
            )));
        }
        index += width;
    }

    Ok(output)
}

fn decode_source_bytes(path: &Path, bytes: Vec<u8>) -> Result<String, types::LispError> {
    // `byte-write-target-file' binds `coding-system-for-write' to
    // `no-conversion': a compiled file therefore contains Emacs's internal
    // utf-8-emacs byte representation and has no source coding cookie.  The
    // `;ELC' magic is the authoritative format marker.
    if bytes.starts_with(b";ELC") {
        return decode_utf8_emacs_source(path, &bytes);
    }
    match String::from_utf8(bytes) {
        Ok(source) => Ok(source),
        Err(error) => {
            let bytes = error.into_bytes();
            match primitives::coding_tag_from_bytes(&bytes).as_deref() {
                Some("utf-8-emacs") => decode_utf8_emacs_source(path, &bytes),
                coding => Err(types::LispError::Signal(format!(
                    "Cannot read {} as {} source",
                    path.display(),
                    coding.unwrap_or("UTF-8")
                ))),
            }
        }
    }
}

fn read_source(path: &Path) -> Result<String, types::LispError> {
    decode_source_bytes(path, read_source_bytes(path)?)
}

fn read_source_bytes(path: &Path) -> Result<Vec<u8>, types::LispError> {
    std::fs::read(path).map_err(|error| {
        types::LispError::Signal(format!("Cannot read {}: {}", path.display(), error))
    })
}

fn source_settings(source: &str) -> Result<SourceFileSettings, types::LispError> {
    let lexical_binding =
        extract_mode_line_variable(source, "lexical-binding").as_deref() == Some("t");
    let read_symbol_shorthands = match extract_file_local_variable(source, "read-symbol-shorthands")
    {
        Some(raw_value) => parse_symbol_shorthands(&raw_value)?,
        None => Vec::new(),
    };
    Ok(SourceFileSettings {
        lexical_binding,
        read_symbol_shorthands,
    })
}

fn extract_mode_line_variable(source: &str, variable: &str) -> Option<String> {
    for line in source.lines().take(2) {
        let Some(start) = line.find("-*-") else {
            continue;
        };
        let contents = &line[start + 3..];
        let Some(end) = contents.find("-*-") else {
            continue;
        };
        for field in contents[..end].split(';') {
            let Some((name, value)) = field.split_once(':') else {
                continue;
            };
            if name.trim() == variable {
                return Some(value.trim().to_string());
            }
        }
    }
    None
}

fn extract_file_local_variable(source: &str, variable: &str) -> Option<String> {
    let mut current_block = None;
    let mut last_block = None;
    for line in source.lines() {
        let trimmed = line.trim_start();
        let comment_text = trimmed.trim_start_matches(';');
        let comment_text = comment_text.strip_prefix(' ').unwrap_or(comment_text);
        if comment_text.trim() == "Local Variables:" {
            current_block = Some(Vec::new());
            continue;
        }
        if comment_text.trim() == "End:" {
            if let Some(block) = current_block.take() {
                last_block = Some(block);
            }
            continue;
        }
        if let Some(block) = current_block.as_mut() {
            block.push(comment_text.to_string());
        }
    }

    let declaration = format!("{variable}:");
    let mut value = None;
    for line in last_block? {
        if value.is_none() {
            let Some(first_line) = line.strip_prefix(&declaration) else {
                continue;
            };
            value = Some(first_line.trim().to_string());
        } else if let Some(value) = value.as_mut() {
            value.push('\n');
            value.push_str(&line);
        }

        let candidate = value.as_ref().expect("file-local value was initialized");
        match reader::Reader::new(candidate).read() {
            Ok(Some(_)) => return value,
            Err(types::LispError::EndOfInput) | Ok(None) => {}
            Err(_) => return value,
        }
    }
    value
}

fn parse_shorthand_string(value: &types::Value) -> Result<String, types::LispError> {
    match value {
        types::Value::String(text) => Ok(text.to_string()),
        types::Value::StringObject(state) => Ok(state.borrow().text.clone()),
        other => Err(types::LispError::WrongTypeArgument(
            "stringp".into(),
            other.clone(),
        )),
    }
}

fn parse_symbol_shorthands(raw_value: &str) -> Result<Vec<(String, String)>, types::LispError> {
    let mut reader = reader::Reader::new(raw_value);
    let Some(value) = reader.read()? else {
        return Ok(Vec::new());
    };
    let entries = value.to_vec()?;
    let mut shorthands = Vec::with_capacity(entries.len());
    for entry in entries {
        let Some((from, to)) = entry.cons_values() else {
            return Err(types::LispError::WrongTypeArgument(
                "consp".into(),
                entry.clone(),
            ));
        };
        shorthands.push((parse_shorthand_string(&from)?, parse_shorthand_string(&to)?));
    }
    Ok(shorthands)
}

fn read_symbol_shorthands_value(shorthands: &[(String, String)]) -> types::Value {
    types::Value::list(shorthands.iter().map(|(from, to)| {
        types::Value::cons(
            types::Value::String(from.clone().into()),
            types::Value::String(to.clone().into()),
        )
    }))
}

pub fn read_forms(path: &Path) -> Result<Vec<types::Value>, types::LispError> {
    let source = read_source(path)?;
    read_source_forms(&source)
}

pub(crate) fn read_source_forms(source: &str) -> Result<Vec<types::Value>, types::LispError> {
    read_source_forms_with_unescaped_literals(source).map(|(forms, _)| forms)
}

pub(crate) fn read_source_forms_with_unescaped_literals(
    source: &str,
) -> Result<(Vec<types::Value>, types::Value), types::LispError> {
    let settings = source_settings(source)?;
    let mut reader =
        reader::Reader::with_symbol_shorthands(source, settings.read_symbol_shorthands);
    let forms = reader.read_all()?;
    let literals = unescaped_character_literal_value(&reader);
    Ok((forms, literals))
}

pub fn load_file_strict(
    interp: &mut eval::Interpreter,
    path: &Path,
) -> Result<(), types::LispError> {
    let requested_source = read_source_bytes(path)?;
    // Resolution has already selected the file.  GNU executes that exact
    // path: an explicit or resolver-selected `.elc' must never silently run a
    // sibling `.el'.  Versioned `.elc' files can contain ordinary readable
    // Lisp as well as `#[...]' bytecode objects; the same reader handles both.
    let versioned_elc = requested_source.starts_with(b";ELC\x1e");
    let source = decode_source_bytes(path, requested_source)?;
    // GNU readevalloop decides this once, before reading the first form: it
    // eagerly expands source only when macroexp.el's owner function is
    // already installed, and never for byte-compiled input.  A nested
    // `require' that defines the function must not retroactively change the
    // policy for the outer file.
    let macroexpander_ready = interp
        .lookup_function("internal-macroexpand-for-load", &types::Env::new())
        .is_ok();
    let eager_macroexpand = macroexpander_ready
        && !versioned_elc
        && path.extension().is_none_or(|extension| extension != "elc");
    let settings = source_settings(&source)?;
    let force_load_doc_strings = interp
        .lookup_var("load-force-doc-strings", &types::Env::new())
        .is_some_and(|value| value.is_truthy());
    let source = if source.starts_with(";ELC") {
        preprocess_lazy_doc_source(path, &source, force_load_doc_strings)
    } else {
        source
    };
    let load_file = interp
        .load_source_provenance_path(path)
        .display()
        .to_string();
    interp.with_lambda_eval_context(settings.lexical_binding, false, |interp| {
        let previous = interp.set_current_load_file(Some(load_file.clone()));
        let mut env = if settings.lexical_binding {
            vec![types::EnvFrame::with_lisp_environment_and_identity(
                Vec::new(),
                types::Value::list([types::Value::T]),
                eval::Interpreter::fresh_frame_identity(),
            )]
        } else {
            types::Env::new()
        };
        // GNU `load' establishes these as real specbind layers.  A Rust-only
        // current-file side channel is insufficient: an outer Lisp binding such
        // as `(let ((load-file-name nil)) (load ...))' must be shadowed by the
        // file being loaded, then restored on every exit path.
        let macroexp_dynvars = interp
            .lookup_var("macroexp--dynvars", &env)
            .unwrap_or(types::Value::Nil);
        let mut dynamic_restores = Vec::with_capacity(9);
        for (name, value) in [
            (
                "load-file-name",
                types::Value::String(load_file.clone().into()),
            ),
            (
                "load-true-file-name",
                types::Value::String(load_file.clone().into()),
            ),
            ("inhibit-file-name-operation", types::Value::Nil),
            ("load-in-progress", types::Value::T),
            (
                "lexical-binding",
                if settings.lexical_binding {
                    types::Value::T
                } else {
                    types::Value::Nil
                },
            ),
            ("macroexp--dynvars", macroexp_dynvars),
            (
                "read-symbol-shorthands",
                read_symbol_shorthands_value(&settings.read_symbol_shorthands),
            ),
            (
                "current-load-list",
                types::Value::list([types::Value::String(load_file.clone().into())]),
            ),
            ("lread--unescaped-character-literals", types::Value::Nil),
        ] {
            match interp.bind_special_dynamic(name, value, &mut env) {
                Ok(restore) => dynamic_restores.push(restore),
                Err(error) => {
                    let _ =
                        restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                    interp.set_current_load_file(previous);
                    return Err(error);
                }
            }
        }
        let mut source_reader = reader::Reader::with_symbol_shorthands(
            &source,
            settings.read_symbol_shorthands.clone(),
        );
        let forms = match source_reader.read_all() {
            Ok(forms) => forms,
            Err(error) => {
                if std::env::var_os("EMAXX_TRACE_LOAD_ERRORS").is_some() {
                    eprintln!(
                        "read error in {} at byte offset {}: {error:?}",
                        path.display(),
                        source_reader.position()
                    );
                }
                let _ = restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                interp.set_current_load_file(previous);
                return Err(error);
            }
        };
        interp.set_variable(
            "lread--unescaped-character-literals",
            unescaped_character_literal_value(&source_reader),
            &mut env,
        );
        let warning_message =
            match unescaped_character_literal_warning(interp, &mut env).and_then(|warning| {
                warning
                    .map(|warning| format_loading_warning(interp, &mut env, path, warning))
                    .transpose()
            }) {
                Ok(message) => message,
                Err(error) => {
                    let _ =
                        restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                    interp.set_current_load_file(previous);
                    return Err(error);
                }
            };
        // GNU's readevalloop reads, constructs, and evaluates one top-level object
        // before reading the next.  Emaxx's parser produces all syntax trees up
        // front, but the observable Interpreter-dependent reader work must remain
        // in that same per-form order: a later compiled object can depend on a
        // definition installed by an earlier top-level form.
        for (form_index, form) in forms.into_iter().enumerate() {
            // GNU's reader interns ordinary symbols and returns fully constructed
            // identity-bearing objects before invoking the Lisp-owned eager
            // macroexpander.  Delaying source `#^[...]' materialization until eval
            // leaked a private ReaderForm into macroexp.el and made generated
            // Unicode tables look like source syntax instead of opaque values.
            interp.intern_symbols_in_value(&form);
            let form = match interp.materialize_read_object_literals(form) {
                Ok(form) => form,
                Err(error) => {
                    if std::env::var_os("EMAXX_TRACE_LOAD_ERRORS").is_some() {
                        eprintln!(
                            "literal materialization error in {} at form {}: {error:?}",
                            path.display(),
                            form_index + 1
                        );
                    }
                    let _ =
                        restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                    interp.set_current_load_file(previous);
                    return Err(error);
                }
            };
            let result = if eager_macroexpand {
                primitives::eager_expand_eval(interp, &form, &mut env)
            } else {
                interp.eval(&form, &mut env)
            };
            if let Err(error) = result {
                if std::env::var_os("EMAXX_TRACE_LOAD_ERRORS").is_some() {
                    let head = form
                        .car()
                        .ok()
                        .and_then(|value| value.as_symbol().ok().map(str::to_owned))
                        .unwrap_or_else(|| form.type_name());
                    eprintln!(
                        "load error in {} at form {} ({head}): {error:?}",
                        path.display(),
                        form_index + 1
                    );
                    if let Some(snapshot) = interp.take_batch_error_backtrace() {
                        for (_, function, _, _) in snapshot.frames.into_iter().take(20) {
                            eprintln!("  load frame: {function}");
                        }
                    }
                }
                let _ = restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env);
                interp.set_current_load_file(previous);
                return Err(error);
            }
        }
        let current_load_list = interp
            .lookup_var("current-load-list", &types::Env::new())
            .unwrap_or_else(|| {
                types::Value::list([types::Value::String(load_file.clone().into())])
            });
        interp.commit_entire_load_history(&load_file, current_load_list);
        if let Some(message) = warning_message {
            append_message(interp, &message);
        }
        restore_special_dynamic_bindings(interp, &mut dynamic_restores, &mut env)?;
        interp.set_current_load_file(previous);

        // GNU 30.2 lread.c:Fload calls the Lisp-owned
        // `do-after-load-evaluation' only after all load bindings unwind.
        // Invoke that actual GNU Elisp owner when it is present; early
        // bootstrap loads deliberately have no such function yet.
        if let Ok(after_load) = interp.lookup_function("do-after-load-evaluation", &env) {
            interp.call_function_value(
                after_load,
                Some("do-after-load-evaluation"),
                &[types::Value::String(load_file.into())],
                &mut env,
            )?;
        }
        Ok(())
    })
}

/// Whether `load'/`require' prefer a compiled `.elc' over its `.el' source.
///
/// GNU's `load-suffixes' is (".so" ".dylib" ".elc" ".el") with
/// `load-prefer-newer' nil, so a `.elc' wins whenever one exists; that is the
/// faithful default here too.  `EMAXX_BYTECODE_VM=0' forces source loads for
/// debugging.  This selects resolution only -- the bytecode VM itself is
/// always available.
pub(crate) fn bytecode_vm_enabled() -> bool {
    std::env::var_os("EMAXX_BYTECODE_VM").is_none_or(|flag| flag != "0")
}

fn restore_special_dynamic_bindings(
    interp: &mut eval::Interpreter,
    restores: &mut Vec<eval::SpecialBindingRestore>,
    env: &mut types::Env,
) -> Result<(), types::LispError> {
    while let Some(restore) = restores.pop() {
        interp.restore_special_dynamic(restore, env)?;
    }
    Ok(())
}

/// Load and run an ERT test file, returning (passed, failed, total) and
/// detailed results for each test.
pub fn run_ert_file(
    path: &Path,
) -> Result<(usize, usize, usize, Vec<TestResult>), types::LispError> {
    let source = read_source(path)?;
    let settings = source_settings(&source)?;
    let mut interp = eval::Interpreter::new();
    let previous = interp.set_current_load_file(Some(path.display().to_string()));
    let mut env = types::Env::new();
    let mut dynamic_restores = Vec::with_capacity(3);
    for (name, value) in [
        (
            "lexical-binding",
            if settings.lexical_binding {
                types::Value::T
            } else {
                types::Value::Nil
            },
        ),
        (
            "read-symbol-shorthands",
            read_symbol_shorthands_value(&settings.read_symbol_shorthands),
        ),
        (
            "current-load-list",
            types::Value::list([types::Value::String(path.display().to_string().into())]),
        ),
    ] {
        match interp.bind_special_dynamic(name, value, &mut env) {
            Ok(restore) => dynamic_restores.push(restore),
            Err(error) => {
                let _ =
                    restore_special_dynamic_bindings(&mut interp, &mut dynamic_restores, &mut env);
                interp.set_current_load_file(previous);
                return Err(error);
            }
        }
    }
    let forms = match reader::Reader::with_symbol_shorthands(
        &source,
        settings.read_symbol_shorthands.clone(),
    )
    .read_all()
    {
        Ok(forms) => forms,
        Err(error) => {
            let _ = restore_special_dynamic_bindings(&mut interp, &mut dynamic_restores, &mut env);
            interp.set_current_load_file(previous);
            return Err(error);
        }
    };

    // Evaluate all top-level forms (this collects ert-deftest definitions)
    for form in &forms {
        interp.intern_symbols_in_value(form);
    }
    for form in &forms {
        // Ignore errors in top-level forms (e.g. require of missing features)
        let _ = interp.eval(form, &mut env);
    }
    restore_special_dynamic_bindings(&mut interp, &mut dynamic_restores, &mut env)?;
    interp.set_current_load_file(previous);

    // Run the collected tests
    let (passed, failed, total) = interp.run_ert_tests();
    let results = interp
        .test_results
        .iter()
        .map(|result| {
            (
                result.name.clone(),
                result.status == TestStatus::Passed,
                result.message.clone(),
            )
        })
        .collect();

    Ok((passed, failed, total, results))
}

#[cfg(test)]
mod tests {
    use super::{
        decode_source_bytes, extract_file_local_variable, parse_symbol_shorthands,
        preprocess_lazy_doc_source, read_source_forms, source_settings,
    };
    use std::path::Path;

    #[test]
    fn parses_compact_lexical_binding_modelines() {
        for source in [
            ";;; -*- lexical-binding:t -*-\n",
            "#!/bin/sh\n;;; -*- mode: emacs-lisp; lexical-binding: t; -*-\n",
        ] {
            assert!(
                source_settings(source)
                    .expect("source settings should parse a valid modeline")
                    .lexical_binding
            );
        }

        for source in [
            ";;; -*- lexical-binding:nil -*-\n",
            ";;; -*- not-lexical-binding: t -*-\n",
        ] {
            assert!(
                !source_settings(source)
                    .expect("source settings should reject unrelated or nil fields")
                    .lexical_binding
            );
        }
    }

    #[test]
    fn parses_read_symbol_shorthands_from_local_variables_block() {
        let source = r#"
(ert-deftest ft-sample ())

;; Local Variables:
;; read-symbol-shorthands: (("ft-" . "fns-tests-"))
;; This comment is not a variable assignment.
;; End:
"#;

        assert_eq!(
            extract_file_local_variable(source, "read-symbol-shorthands"),
            Some(r#"(("ft-" . "fns-tests-"))"#.into())
        );
        assert_eq!(
            parse_symbol_shorthands(r#"(("ft-" . "fns-tests-"))"#)
                .expect("symbol shorthand alist should parse"),
            vec![("ft-".into(), "fns-tests-".into())]
        );
        assert_eq!(
            source_settings(source)
                .expect("source settings should parse file-local symbol shorthands")
                .read_symbol_shorthands,
            vec![("ft-".into(), "fns-tests-".into())]
        );
    }

    #[test]
    fn parses_multiline_read_symbol_shorthands_from_magit_style_blocks() {
        let source = r#"
(provide 'sample)

;; Local Variables:
;; indent-tabs-mode: nil
;; read-symbol-shorthands: (
;;   ("and$"     . "cond-let--and$")
;;   ("when-let" . "cond-let--when-let"))
;; End:
"#;

        assert_eq!(
            source_settings(source)
                .expect("multiline file-local symbol shorthands should parse")
                .read_symbol_shorthands,
            vec![
                ("and$".into(), "cond-let--and$".into()),
                ("when-let".into(), "cond-let--when-let".into()),
            ]
        );
    }

    #[test]
    fn lazy_doc_preprocessing_ignores_markers_inside_strings_and_comments() {
        let source = ";ELC\x1e\n(#[0 \"raw #@12 bytes #$ (#$ . 9) \\\"tail\" [nil] 1])\n\
                      ; #@7 and #$ are comment text\n\
                      (list byte-compile--\\#$ byte-compile--\\#@7)\n";

        assert_eq!(
            preprocess_lazy_doc_source(Path::new("/tmp/sample.elc"), source, false),
            source
        );
    }

    #[test]
    fn lazy_doc_preprocessing_rewrites_only_top_level_references() {
        let prefix = ";ELC\x1e\n#@7 ";
        let content_offset = prefix.len();
        let source = format!(
            "{prefix}hello\x1f(list #$ (#$ . {content_offset}) \"#$ (#$ . {content_offset})\")"
        );
        let path = Path::new("/tmp/sample.elc");

        assert_eq!(
            preprocess_lazy_doc_source(path, &source, false),
            format!(
                ";ELC\x1e\n(list \"/tmp/sample.elc\" (\"/tmp/sample.elc\" . {content_offset}) \"#$ (#$ . {content_offset})\")"
            )
        );
        assert_eq!(
            preprocess_lazy_doc_source(path, &source, true),
            format!(
                ";ELC\x1e\n(list \"/tmp/sample.elc\" \"hello\" \"#$ (#$ . {content_offset})\")"
            )
        );
    }

    #[test]
    fn utf8_emacs_source_preserves_extended_character_literals_as_integers() {
        let mut bytes = b";;; -*- coding: utf-8-emacs; -*-\n(setq sample ?".to_vec();
        bytes.extend([0xF6, 0xA0, 0x87, 0x8A]);
        bytes.extend(b")\n");
        let source = decode_source_bytes(Path::new("ethiopic-sample.el"), bytes)
            .expect("decode utf-8-emacs character literal");
        assert!(source.contains(r"?\x1A01CA"));
        assert_eq!(
            read_source_forms(&source).expect("read decoded source")[0],
            super::types::Value::list([
                super::types::Value::symbol("setq"),
                super::types::Value::symbol("sample"),
                super::types::Value::Integer(0x1A_01CA),
            ])
        );
    }

    #[test]
    fn utf8_emacs_source_preserves_extended_string_characters() {
        let mut bytes = b";;; -*- coding: utf-8-emacs; -*-\n(setq sample \"".to_vec();
        bytes.extend([0xF6, 0xA0, 0x87, 0x8A]);
        bytes.extend(b"\")\n");
        let source = decode_source_bytes(Path::new("extended-string.el"), bytes)
            .expect("decode extended string character");
        assert!(source.contains(r"\x1A01CA\ "));
        let form = read_source_forms(&source)
            .expect("read decoded source")
            .remove(0)
            .to_vec()
            .expect("setq form");
        let string = super::primitives::string_like(&form[2]).expect("string value");
        assert_eq!(string.character_codes(), vec![0x1A_01CA]);
        assert_eq!(string.extended_chars, vec![(0, 0x1A_01CA)]);
        assert_eq!(string.byte_len().expect("utf-8-emacs byte length"), 4);
        assert_eq!(
            super::primitives::internal_string_bytes(&string)
                .expect("encode utf-8-emacs string bytes"),
            [0xF6, 0xA0, 0x87, 0x8A]
        );
    }

    #[test]
    fn utf8_emacs_source_decodes_five_byte_and_raw_byte8_characters() {
        let mut bytes = b";;; -*- coding: utf-8-emacs; -*-\n(setq sample \"".to_vec();
        bytes.extend([0xF8, 0x88, 0x80, 0x80, 0x80]);
        bytes.extend([0xC0, 0x80]);
        bytes.extend(b"\")\n");
        let source = decode_source_bytes(Path::new("extended-ranges.el"), bytes)
            .expect("decode complete utf-8-emacs character range");
        let form = read_source_forms(&source)
            .expect("read decoded source")
            .remove(0)
            .to_vec()
            .expect("setq form");
        let string = super::primitives::string_like(&form[2]).expect("string value");
        assert_eq!(string.character_codes(), vec![0x20_0000, 0x3F_FF80]);
        assert_eq!(string.byte_len().expect("utf-8-emacs byte length"), 7);
        assert_eq!(
            super::primitives::internal_string_bytes(&string)
                .expect("encode complete utf-8-emacs range"),
            [0xF8, 0x88, 0x80, 0x80, 0x80, 0xC0, 0x80]
        );
    }

    #[test]
    fn compiled_lisp_decodes_internal_utf8_emacs_without_a_cookie() {
        let mut bytes = b";ELC\x1e\0\0\0\n(setq sample \"".to_vec();
        bytes.extend([0xF6, 0xA0, 0x87, 0x8A]);
        bytes.extend(b"\")\n");
        let source = decode_source_bytes(Path::new("extended-string.elc"), bytes)
            .expect("decode compiled utf-8-emacs string");
        let form = read_source_forms(&source)
            .expect("read decoded compiled form")
            .remove(0)
            .to_vec()
            .expect("setq form");
        let string = super::primitives::string_like(&form[2]).expect("string value");
        assert_eq!(string.character_codes(), vec![0x1A_01CA]);
        assert_eq!(
            super::primitives::internal_string_bytes(&string)
                .expect("encode compiled utf-8-emacs string bytes"),
            [0xF6, 0xA0, 0x87, 0x8A]
        );
    }

    #[test]
    fn compiled_lisp_preserves_raw_bytecode_octets_inside_strings() {
        let mut bytes = b";ELC\x1e\0\0\0\n(setq sample \"".to_vec();
        bytes.extend([0xC0, 0x01, 0xC2, 0xFF]);
        bytes.extend(b"\")\n");
        let source = decode_source_bytes(Path::new("raw-bytecode.elc"), bytes)
            .expect("decode compiled raw byte string");
        let form = read_source_forms(&source)
            .expect("read decoded raw byte form")
            .remove(0)
            .to_vec()
            .expect("setq form");
        let string = super::primitives::string_like(&form[2]).expect("string value");
        assert!(!string.multibyte);
        assert_eq!(
            super::primitives::internal_string_bytes(&string)
                .expect("recover compiled raw byte string"),
            [0xC0, 0x01, 0xC2, 0xFF]
        );
    }

    #[test]
    fn file_loader_materializes_reader_objects_before_macroexpansion_and_eval() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "emaxx-reader-boundary-{}-{unique}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).expect("create reader-boundary fixture directory");

        let roots = std::iter::repeat_n("nil", 64).collect::<Vec<_>>().join(" ");
        let source_path = directory.join("reader-object.el");
        std::fs::write(
            &source_path,
            format!("(setq loaded-source-table #^[nil nil test nil {roots}])\n"),
        )
        .expect("write source reader-object fixture");
        let compiled_path = directory.join("reader-object.elc");
        std::fs::write(
            &compiled_path,
            format!(";ELC\x1e\n(setq loaded-compiled-table #^[nil nil test nil {roots}])\n"),
        )
        .expect("write compiled reader-object fixture");

        let mut interp = super::eval::Interpreter::new();
        let mut env = super::types::Env::new();
        for form in super::reader::Reader::new(
            "(setq eager-owner-saw-char-table nil)\n\
             (defalias 'internal-macroexpand-for-load\n\
               (function\n\
                 (lambda (form full)\n\
                   (if (and full\n\
                            (eq (car-safe form) 'setq)\n\
                            (char-table-p\n\
                              (car-safe (cdr-safe (cdr-safe form)))))\n\
                       (setq eager-owner-saw-char-table t))\n\
                   form)))",
        )
        .read_all()
        .expect("read eager-owner fixture")
        {
            interp
                .eval(&form, &mut env)
                .expect("install eager-owner fixture");
        }

        super::load_file_strict(&mut interp, &source_path)
            .expect("load source reader-object fixture");
        assert_eq!(
            interp.lookup_var("eager-owner-saw-char-table", &env),
            Some(super::types::Value::T),
            "GNU's Lisp macroexpander must receive the C-reader object, not a private placeholder"
        );
        assert!(matches!(
            interp.lookup_var("loaded-source-table", &env),
            Some(super::types::Value::CharTable(_))
        ));

        super::load_file_strict(&mut interp, &compiled_path)
            .expect("load compiled reader-object fixture");
        assert!(matches!(
            interp.lookup_var("loaded-compiled-table", &env),
            Some(super::types::Value::CharTable(_))
        ));

        std::fs::remove_dir_all(directory).expect("remove reader-boundary fixture directory");
    }

    #[test]
    fn closure_reader_materialization_never_evaluates_slot_data() {
        let callable_looking_data = super::types::Value::list([
            super::types::Value::symbol("reader-data-must-not-be-called"),
            super::types::Value::symbol("payload"),
        ]);
        let literal =
            super::types::Value::ReaderForm(std::rc::Rc::new(super::types::ReaderForm::Closure {
                kind: super::types::ReaderClosureKind::ByteCode,
                slots: vec![
                    super::types::Value::Integer(0),
                    super::types::Value::String(String::new().into()),
                    super::types::Value::list([super::types::Value::symbol("vector-literal")]),
                    super::types::Value::Integer(0),
                    callable_looking_data.clone(),
                ],
            }));
        let mut interp = super::eval::Interpreter::new();
        let materialized = interp
            .materialize_read_object_literals(literal)
            .expect("reader construction must treat every closure slot as data");
        let super::types::Value::Record(record_id) = materialized else {
            panic!("byte-code reader form must become a closure pseudovector");
        };
        let record = interp
            .find_record(record_id)
            .expect("materialized closure record");
        assert_eq!(record.slots[4], callable_looking_data);
    }
}
