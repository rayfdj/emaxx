use super::*;
use crate::lisp::types::{Kind, PropertyName};

/// buffer.c:Frestore_buffer_modified_p.  The base buffer's file truename
/// is locked when the text becomes modified and unlocked when it becomes
/// unmodified, unless `inhibit-modification-hooks'; testing its
/// `buffer-file-name' makes binding that to nil effective.  A nil FLAG
/// sets SAVE_MODIFF to MODIFF (and so `recent-auto-save-p' to nil);
/// another makes the text modified, and `autosaved' also marks the base
/// buffer auto-saved at that count.
fn restore_buffer_modified_p(
    interp: &mut Interpreter,
    env: &mut Env,
    flag: Value,
) -> Result<Value, LispError> {
    let current = interp.current_buffer_id();
    let base = interp.root_buffer_id(current);
    if !interp
        .lookup_var_key(cached_symbol!("inhibit-modification-hooks"), env)
        .is_some_and(|value| value.is_truthy())
    {
        // `!NILP (fn) && !NILP (BVAR (b, filename))', FN the truename.
        let truename = interp.get_buffer_by_id(base).and_then(|buffer| {
            (buffer.visits_file() && !buffer.file_truename().is_nil())
                .then(|| buffer.file_truename())
        });
        if let Some(truename) = truename {
            let already = interp.buffer.borrow().is_modified();
            if !already && flag.is_truthy() {
                flock_file(interp, env, truename)?;
            } else if already && flag.is_nil() {
                funlock_file(interp, env, truename)?;
            }
        }
    }
    if flag.is_nil() {
        interp.buffer.borrow_mut().set_unmodified();
    } else {
        interp.buffer.borrow_mut().set_modified();
        if crate::lisp::types::eq_symbol_named(flag, "autosaved") {
            let modiff = interp.buffer.borrow().modified_tick();
            if let Some(mut buffer) = interp.get_buffer_by_id_mut(base) {
                buffer.set_autosave_modiff(modiff);
            }
        }
    }

    Ok(flag)
}

// cmds.c:236/288: `self-insert-command' and `delete-char' amalgamate
// their undo with a preceding run of the same command when the count's
// magnitude is below 2 -- through simple.el's own `undo-auto-amalgamate',
// reached as an ordinary Lisp call.
/// call0 (Qundo_auto_amalgamate), once simple.el has defined it.
fn call_undo_auto_amalgamate(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    // call0 (Qundo_auto_amalgamate), the symbol in hand; before simple.el
    // defines it there is nothing to call.
    let symbol = cached_symbol!("undo-auto-amalgamate").with(Clone::clone);
    if interp.lookup_function_symbol(&symbol, env).is_ok() {
        interp.call_function_value(Value::Symbol(symbol), None, &[], env)?;
    }
    Ok(())
}

/// cmds.c:Fself_insert_command: N a fixnum, C (nil for
/// `last-command-event', otherwise stored there) a character through
/// `translation-table-for-input', or the bell.
fn self_insert_command(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("self-insert-command", args, 1, 2)?;
    let n = match args[0].kind() {
        Kind::Integer(n) => n,
        _ => return Err(LispError::WrongTypeArgument("fixnump".into(), args[0])),
    };
    let c = match args.get(1).filter(|value| !value.is_nil()) {
        Some(c) => {
            interp.set_variable_key(cached_symbol!("last-command-event"), *c, env);
            *c
        }
        None => interp
            .lookup_var_key(cached_symbol!("last-command-event"), env)
            .unwrap_or(Value::Nil),
    };
    if n < 0 {
        return Err(LispError::Signal(format!(
            "Negative repetition argument {n}"
        )));
    }
    if n < 2 {
        call_undo_auto_amalgamate(interp, env)?;
    }
    match c.kind() {
        Kind::Integer(code) if (0..=0x3F_FFFF).contains(&code) => {
            let table = interp
                .lookup_var_key(cached_symbol!("translation-table-for-input"), env)
                .unwrap_or(Value::Nil);
            let character = translate_char(interp, &table, code as u32);
            if internal_self_insert(interp, character, n, env)? == 2 {
                interp.set_variable_key(
                    cached_symbol!("undo-auto--this-command-amalgamating"),
                    Value::Nil,
                    env,
                );
            }
        }
        _ => super::display::bitch_at_user(interp, env)?,
    }
    Ok(Value::Nil)
}

/// cmds.c:internal_self_insert: N copies of C at point, with overwrite
/// mode's replacement, abbrev expansion, auto-fill and
/// `post-self-insert-hook'.  0 for a simple insertion, 1 for one that
/// may not have been, 2 for one that calls for an undo boundary.
fn internal_self_insert(
    interp: &mut Interpreter,
    c: u32,
    n: i64,
    env: &mut Env,
) -> Result<i32, LispError> {
    const MOST_POSITIVE_FIXNUM: i64 = (1 << 61) - 1;
    let truthy = |interp: &Interpreter, env: &Env, name: crate::lisp::types::symbols::SymbolKey| {
        interp
            .lookup_var_key(name, env)
            .is_some_and(|value| value.is_truthy())
    };
    let mut hairy = 0;
    let overwrite = interp
        .lookup_var_key(cached_symbol!("overwrite-mode"), env)
        .unwrap_or(Value::Nil);
    if truthy(interp, env, cached_symbol!("before-change-functions"))
        || truthy(interp, env, cached_symbol!("after-change-functions"))
    {
        hairy = 1;
    }
    let multibyte = interp.buffer.borrow().is_multibyte();
    let mut chars_to_delete: i64 = 0;
    let mut spaces_to_insert: i64 = 0;
    let (pt, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point(), buffer.point_max())
    };
    if !overwrite.is_nil() && pt < zv {
        // In overwrite mode C replaces the character after point (C2),
        // with spaces filling for a narrower C and more characters
        // deleted for a wider one, so that the rest of the line keeps
        // its columns.
        let c2 = public_buffer_char_code_at(interp, pt).unwrap_or(0);
        if overwrite.is_key(cached_symbol!("overwrite-mode-binary")) {
            chars_to_delete = n;
        } else if c != u32::from(b'\n') && c2 != i64::from(b'\n') {
            let cwidth = super::call(interp, "char-width", &[Value::Integer(c.into())], env)?
                .as_integer()?;
            if cwidth != 0 {
                let curcol = super::call(interp, "current-column", &[], env)?.as_integer()?;
                if n <= (MOST_POSITIVE_FIXNUM - curcol) / cwidth {
                    let target_clm = curcol + n * cwidth;
                    let actual_clm = super::call(
                        interp,
                        "move-to-column",
                        &[Value::Integer(target_clm), Value::Nil],
                        env,
                    )?
                    .as_integer()?;
                    let now = interp.buffer.borrow().point();
                    chars_to_delete = now as i64 - pt as i64;
                    if actual_clm > target_clm {
                        // Too many columns would go: keep a tab before
                        // point, else fill with spaces.
                        if interp.buffer.borrow().char_before() == Some('\t') {
                            chars_to_delete -= 1;
                        } else {
                            spaces_to_insert = actual_clm - target_clm;
                        }
                    }
                    interp.buffer.borrow_mut().goto_char(pt);
                }
            }
        }
        hairy = 2;
    }

    let syntax_class = |interp: &Interpreter, code: u32| {
        syntax::syntax_entry_for_code(interp, interp.current_syntax_table_id(), code).class
    };
    let point = interp.buffer.borrow().point();
    if truthy(interp, env, cached_symbol!("abbrev-mode"))
        && syntax_class(interp, c) != syntax::SyntaxClass::Word
        && !truthy(interp, env, cached_symbol!("buffer-read-only"))
        && point > interp.buffer.borrow().point_min()
        && syntax_class(interp, {
            // Fprevious_char, a unibyte buffer's byte as UNIBYTE_TO_CHAR.
            let previous = public_buffer_char_code_at(interp, point - 1).unwrap_or(0) as u32;
            if !multibyte && previous >= 0x80 {
                previous + RAW_BYTE8_BASE
            } else {
                previous
            }
        }) == syntax::SyntaxClass::Word
    {
        let modiff = interp.buffer.borrow().modified_tick();
        let sym = interp.call_function_value(
            Value::Symbol("expand-abbrev".into()),
            Some("expand-abbrev"),
            &[],
            env,
        )?;
        // An abbrev whose hook is a symbol with a non-nil
        // `no-self-insert' property is all there is to insert.
        if let Kind::Symbol(abbrev) = sym.kind() {
            let hook = super::call(interp, "symbol-function", &[Value::Symbol(abbrev)], env)?;
            if !hook.is_nil()
                && hook.is_symbol()
                && super::call(
                    interp,
                    "get",
                    &[hook, Value::Symbol("no-self-insert".into())],
                    env,
                )?
                .is_truthy()
            {
                return Ok(1);
            }
        }
        if interp.buffer.borrow().modified_tick() != modiff {
            hairy = 2;
        }
    }

    if chars_to_delete > 0 {
        let mc = if !multibyte && (0x80..0x100).contains(&c) {
            c + RAW_BYTE8_BASE
        } else {
            c
        };
        let mut codes = vec![mc; n as usize];
        codes.extend(std::iter::repeat_n(
            u32::from(b' '),
            spaces_to_insert.max(0) as usize,
        ));
        let from = interp.buffer.borrow().point();
        let to = (from as i64).saturating_add(chars_to_delete) as usize;
        let to = to.min(interp.buffer.borrow().point_max());
        replace_range_with_hooks(interp, from, to, &codes, env)?;
        let point = interp.buffer.borrow().point();
        interp.buffer.borrow_mut().goto_char(point + n as usize);
    } else if n > 0 {
        let mut text = String::new();
        let mut extended_chars = Vec::new();
        for offset in 0..n as usize {
            push_buffer_character(&mut text, &mut extended_chars, offset, c, multibyte);
        }
        insert_text_with_hooks(interp, &text, None, &extended_chars, true, false, env)?;
    }

    let auto_fill_character = match interp
        .lookup_var_key(cached_symbol!("auto-fill-chars"), env)
        .map(|value| value.kind())
    {
        Some(Kind::CharTable(table)) => interp
            .char_table_get(table, c)
            .is_some_and(|value| value.is_truthy()),
        _ => c == u32::from(b' ') || c == u32::from(b'\n'),
    };
    if auto_fill_character && truthy(interp, env, cached_symbol!("auto-fill-function")) {
        // After a newline, fill the line it ends, with the newline in
        // place already.
        if c == u32::from(b'\n') {
            let point = interp.buffer.borrow().point();
            interp.buffer.borrow_mut().goto_char(point - 1);
        }
        let result = interp.call_function_value(
            Value::Symbol("internal-auto-fill".into()),
            Some("internal-auto-fill"),
            &[],
            env,
        )?;
        if c == u32::from(b'\n') {
            let point = interp.buffer.borrow().point();
            if point < interp.buffer.borrow().point_max() {
                interp.buffer.borrow_mut().goto_char(point + 1);
            }
        }
        if !result.is_nil() {
            hairy = 2;
        }
    }

    run_hook(
        interp,
        &cached_symbol!("post-self-insert-hook").symbol(),
        env,
    )?;
    Ok(hairy)
}

/// editfns.c:Fwiden: the whole buffer accessible, or the bounds of the
/// innermost labeled restriction, which goes once it is the outermost one
/// (no labeled restriction is in effect then).
pub(super) fn widen_current_buffer(interp: &mut Interpreter) {
    let buffer_id = interp.current_buffer_id();
    let bounds = interp.buffer.borrow().accessible_bounds();
    match interp.innermost_labeled_restriction(buffer_id) {
        None => interp.buffer.borrow_mut().widen(),
        Some((label, start, end)) => {
            interp.buffer.borrow_mut().narrow_to_region(start, end);
            if label.is_none() {
                interp.pop_labeled_restriction(buffer_id);
            }
        }
    }
    // `current_buffer->clip_changed = 1' when the bounds move.
    if interp.buffer.borrow().accessible_bounds() != bounds {
        interp.set_clip_changed(buffer_id);
    }
    // Changing the buffer bounds invalidates any recorded current column.
    interp.invalidate_current_column();
}

/// editfns.c:Fnarrow_to_region: START and END by fix_position, in either
/// order, within the whole buffer (args-out-of-range of them as given),
/// then within the innermost labeled restriction's bounds.
pub(super) fn narrow_current_buffer(
    interp: &mut Interpreter,
    start: &Value,
    end: &Value,
) -> Result<(), LispError> {
    let mut s = crate::lisp::primitives::buffers::fix_position(interp, start)?;
    let mut e = crate::lisp::primitives::buffers::fix_position(interp, end)?;
    if e < s {
        std::mem::swap(&mut s, &mut e);
    }
    let z = interp.buffer.borrow().size_total() as i64 + 1;
    if !(1 <= s && s <= e && e <= z) {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            *start,
            *end,
        ])));
    }
    let (mut s, mut e) = (s as usize, e as usize);
    if let Some((_, begv, zv)) = interp.innermost_labeled_restriction(interp.current_buffer_id()) {
        s = s.clamp(begv, zv);
        e = e.clamp(begv, zv);
    }
    if interp.buffer.borrow().accessible_bounds() != (s, e) {
        let current = interp.current_buffer_id();
        interp.set_clip_changed(current);
    }
    interp.buffer.borrow_mut().narrow_to_region(s, e);
    interp.invalidate_current_column();
    Ok(())
}

fn character_byte_value(character: Option<char>, multibyte: bool) -> Result<Value, LispError> {
    let Some(character) = character else {
        // GNU exposes the terminating NUL at point-max and at the end of an
        // empty string when no explicit position was supplied.
        return Ok(Value::Integer(0));
    };
    let raw_byte = raw_byte_from_regex_char(character);
    let code = raw_byte.map_or(character as u32, u32::from);
    if multibyte && raw_byte.is_none() && code > 0x7f {
        return Err(LispError::Signal(format!(
            "Not an ASCII nor an 8-bit character: {code}"
        )));
    }
    Ok(Value::Integer(i64::from(code & 0xff)))
}

fn unibyte_text_bytes(text: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(text.len());
    for character in text.chars() {
        match raw_byte_from_regex_char(character).or_else(|| u8::try_from(character as u32).ok()) {
            Some(byte) => bytes.push(byte),
            None => {
                let mut encoded = [0; 4];
                bytes.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            }
        }
    }
    bytes
}

/// The eight-bit characters of a buffer's TEXT, whose EXTENDED holds the
/// codes its view cannot carry: each raw byte's character, and any code
/// from BYTE8_TO_CHAR (0x80) up.
fn eight_bit_chars(text: &str, extended: &[(usize, u32)]) -> usize {
    text.chars()
        .filter(|&character| raw_byte_from_regex_char(character).is_some())
        .count()
        + extended
            .iter()
            .filter(|&&(_, code)| code >= 0x3F_FF80)
            .count()
}

/// buffer.c:Fset_buffer_multibyte's conversion to multibyte: each byte of
/// the unibyte TEXT kept, an ASCII one as itself, the bytes of a valid
/// internal sequence (multibyte_length without ALLOW_8BIT, so not a byte8
/// head) as their
/// character when FLAG is t, any other byte as its eight-bit character.
/// What converting a unibyte buffer's text to multibyte makes of it.
struct MultibyteText {
    /// The new text.
    text: String,
    /// The new position of every old one, point-max's included.
    positions: Vec<usize>,
    /// The characters the text's view cannot carry, by position.
    extended: Vec<(usize, u32)>,
    /// The indices of the bytes made eight-bit characters.
    eight_bit: Vec<usize>,
}

/// The text, the new position of every old one (point-max's included),
/// the characters the text's view cannot carry, by position, and the
/// indices of the bytes made eight-bit characters.
fn multibyte_buffer_text(text: &str, preserve_utf8_sequences: bool) -> MultibyteText {
    let bytes = unibyte_text_bytes(text);
    let mut converted = String::new();
    let mut extended = Vec::new();
    let mut position_map = vec![1; bytes.len() + 1];
    let mut byte_index = 0;
    let mut new_position = 1;
    let mut eight_bit = Vec::new();

    while byte_index < bytes.len() {
        position_map[byte_index] = new_position;
        let byte = bytes[byte_index];
        let width = if byte.is_ascii() {
            1
        } else if preserve_utf8_sequences {
            crate::lisp::types::string_data::multibyte_length(&bytes[byte_index..], false)
        } else {
            0
        };
        let consumed = match (width > 0)
            .then(|| {
                crate::lisp::types::string_data::decode_character(
                    &bytes[byte_index..byte_index + width],
                )
                .ok()
            })
            .flatten()
        {
            Some((code, _)) => {
                crate::lisp::primitives::push_character_code(
                    &mut converted,
                    &mut extended,
                    new_position,
                    code,
                );
                width
            }
            None => {
                converted.push(raw_byte_regex_char(byte));
                eight_bit.push(byte_index);
                1
            }
        };
        new_position += 1;
        position_map[byte_index + 1..=byte_index + consumed].fill(new_position);
        byte_index += consumed;
    }
    MultibyteText {
        text: converted,
        positions: position_map,
        extended,
        eight_bit,
    }
}

/// buffer.c:Fset_buffer_multibyte's conversion to unibyte: the bytes of
/// each character's internal representation, an eight-bit character's
/// own byte (EXTENDED holds the codes the text's view cannot carry, by
/// character index), each a unibyte character.
fn unibyte_buffer_text(text: &str, extended: &[(usize, u32)]) -> (String, Vec<usize>) {
    let mut converted = String::with_capacity(text.len());
    let mut position_map = Vec::with_capacity(text.chars().count() + 1);
    position_map.push(1);
    // Each byte of a character's encoding becomes one unibyte character;
    // the map records the count after each source character.
    let mut count = 0;
    let mut sidecar = extended.iter().peekable();
    for (index, character) in text.chars().enumerate() {
        let code = match sidecar.peek() {
            Some(&&(at, code)) if at == index => {
                sidecar.next();
                Some(code)
            }
            _ => None,
        };
        if let Some(code) = code {
            let (encoded, width) = crate::lisp::types::string_data::encode_character(code)
                .expect("a buffer character's code");
            for &byte in &encoded[..width] {
                converted.push(char::from(byte));
                count += 1;
            }
        } else if let Some(byte) = raw_byte_from_regex_char(character) {
            converted.push(char::from(byte));
            count += 1;
        } else {
            let mut encoded = [0; 4];
            for &byte in character.encode_utf8(&mut encoded).as_bytes() {
                converted.push(char::from(byte));
                count += 1;
            }
        }
        position_map.push(count + 1);
    }
    (converted, position_map)
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Buffer operations ──
            "insert" => direct_insert(interp, args, env),
            "insert-and-inherit" => insert_impl(interp, args, env, true, false),
            "insert-char" => insert_char_impl(interp, args, env),
            "self-insert-command" => self_insert_command(interp, args, env),
            "insert-byte" => {
                need_arg_range(name, args, 2, 3)?;
                // editfns.c:Finsert_byte: CHECK_FIXNUM, 0..255 or
                // args-out-of-range, a byte from 128 the raw-byte character
                // in a multibyte buffer, then Finsert_char.
                let byte = match args[0].kind() {
                    Kind::Integer(byte) => byte,
                    _ => return Err(LispError::WrongTypeArgument("fixnump".into(), args[0])),
                };
                if !(0..=255).contains(&byte) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("args-out-of-range"),
                        args[0],
                        Value::Integer(0),
                        Value::Integer(255),
                    ])));
                }
                let character = if byte >= 128 && interp.buffer.borrow().is_multibyte() {
                    byte + RAW_BYTE8_BASE as i64
                } else {
                    byte
                };
                let inherit = args.get(2).copied().unwrap_or(Value::Nil);
                insert_char_impl(interp, &[Value::Integer(character), args[1], inherit], env)
            }
            "insert-buffer-substring" => {
                if args.is_empty() || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // editfns.c: Fget_buffer and nsberror, then the liveness.
                let source = interp.get_buffer_or_error(&args[0])?;
                let buffer_id = interp
                    .live_buffer_id(source)
                    .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))?;
                let (begv, zv) = {
                    let source = source.borrow();
                    (source.point_min() as i64, source.point_max() as i64)
                };
                // editfns.c: nil START and END the accessible portion's
                // bounds, the others fix_position'd, in order, within it,
                // else (args-out-of-range START END).
                let start_arg = args.get(1).copied().unwrap_or(Value::Nil);
                let end_arg = args.get(2).copied().unwrap_or(Value::Nil);
                let b = if start_arg.is_nil() {
                    begv
                } else {
                    fix_position(interp, &start_arg)?
                };
                let e = if end_arg.is_nil() {
                    zv
                } else {
                    fix_position(interp, &end_arg)?
                };
                let (b, e) = (b.min(e), b.max(e));
                if !(begv <= b && e <= zv) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("args-out-of-range"),
                        start_arg,
                        end_arg,
                    ])));
                }
                let (start, end) = (b as usize, e as usize);
                let original = interp.current_buffer_id();
                if buffer_id != original {
                    interp.set_current_buffer_id(buffer_id)?;
                }
                let updated = update_buffer_properties(interp, start, end, env);
                if buffer_id != original && interp.has_buffer_id(original) {
                    interp.set_current_buffer_id(original)?;
                }
                updated?;
                let source = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", buffer_id)))?;
                let text = source
                    .buffer_substring(start, end)
                    .map_err(|e| LispError::Signal(e.to_string()))?;
                let extended_chars = source.substring_extended_chars(start, end);
                drop(source);
                insert_buffer_text_with_hooks(
                    interp,
                    &text,
                    buffer_id,
                    start,
                    &extended_chars,
                    env,
                )?;
                Ok(Value::Nil)
            }
            "point" => direct_point(interp, args, env),
            "point-min" => direct_point_min(interp, args, env),
            "point-max" => direct_point_max(interp, args, env),
            "minibuffer-prompt-end" => {
                let prompt_length = interp
                    .minibuffer_prompt_text()
                    .map(|prompt| prompt.chars().count())
                    .unwrap_or(0);
                Ok(Value::Integer(
                    interp
                        .buffer
                        .borrow()
                        .point_min()
                        .saturating_add(prompt_length) as i64,
                ))
            }
            "combine-after-change-execute" => {
                need_args(name, args, 0)?;
                flush_combined_after_change(interp, env)
            }
            "goto-char" => direct_goto_char(interp, args, env),
            "forward-char" => direct_forward_char(interp, args, env),
            "forward-word" => {
                need_arg_range(name, args, 0, 1)?;
                super::super::syntax_scan::forward_word(interp, env, args.first())
            }
            "skip-chars-forward" => direct_skip_chars_forward(interp, args, env),
            "skip-chars-backward" => direct_skip_chars_backward(interp, args, env),
            "skip-syntax-forward" => direct_skip_syntax_forward(interp, args, env),
            "skip-syntax-backward" => direct_skip_syntax_backward(interp, args, env),
            "backward-char" => direct_backward_char(interp, args, env),
            "beginning-of-line" => direct_beginning_of_line(interp, args, env),
            "end-of-line" => direct_end_of_line(interp, args, env),
            "forward-line" => direct_forward_line(interp, args, env),
            "compute-motion" => {
                need_args(name, args, 7)?;
                compute_motion_value(interp, env, args)
            }
            "vertical-motion" => {
                need_arg_range(name, args, 1, 3)?;
                // indent.c:Fvertical_motion: LINES may be (COLS . LINES),
                // COLS checked by CHECK_NUMBER; LINES must be a fixnum.
                let (goal_cols, lines) = match args[0].cons_values() {
                    Some((cols, lines)) => {
                        if !matches!(
                            cols.kind(),
                            Kind::Float(_) | Kind::Integer(_) | Kind::BigInteger(_)
                        ) {
                            return Err(wrong_type_argument("numberp", cols));
                        }
                        (Some(cols), lines)
                    }
                    None => (None, args[0]),
                };
                let Kind::Integer(n) = lines.kind() else {
                    return Err(wrong_type_argument("fixnump", lines));
                };
                // WINDOW lends only its geometry: the motion runs in the
                // current buffer (Fvertical_motion).
                let window = live_motion_window(interp, args.get(1).unwrap_or(&Value::Nil))?;
                let window_id = window_record_id_from_value(interp, &window)
                    .unwrap_or_else(|| interp.selected_window_id());
                // Noninteractively vmotion moves by lines alone; the
                // display iterator's motion first converts CUR-COL
                // (extract_float) and then COLS to x with window_column_x.
                let (start_x, goal_col) = if interactive_motion(interp, env) {
                    let window_arg = args.get(1).copied().unwrap_or(Value::Nil);
                    let start_x = match args.get(2).copied().filter(|value| !value.is_nil()) {
                        Some(cur_col) => {
                            if !matches!(
                                cur_col.kind(),
                                Kind::Float(_) | Kind::Integer(_) | Kind::BigInteger(_)
                            ) {
                                return Err(wrong_type_argument("numberp", cur_col));
                            }
                            Some(window_column_x(interp, window_arg, cur_col)?)
                        }
                        None => None,
                    };
                    let goal_col = goal_cols
                        .map(|cols| window_column_x(interp, window_arg, cols))
                        .transpose()?;
                    (start_x, goal_col)
                } else {
                    (None, None)
                };
                // Interactively the motion is start_display's, whose
                // init_iterator frees the frame's realized faces after a
                // face change.
                if crate::lisp::primitives::interactive_window_metrics().is_some() {
                    interp.init_iterator_faces_window(window_id);
                }
                let moved =
                    visual_vertical_motion_from(interp, env, window_id, n, goal_col, start_x)?;
                Ok(Value::Integer(moved))
            }
            "search-forward" | "search-backward" => search_named(interp, name, args, env),
            "re-search-forward" => direct_re_search_forward(interp, args, env),
            "re-search-backward" => direct_re_search_backward(interp, args, env),
            "posix-search-forward" => regexp::buffer_regex_search(interp, args, env, true, true),
            "posix-search-backward" => regexp::buffer_regex_search(interp, args, env, false, true),
            "forward-comment" => {
                need_arg_range(name, args, 1, 1)?;
                syntax_scan::forward_comment(interp, env, &args[0])
            }
            "scan-lists" => {
                need_arg_range(name, args, 3, 3)?;
                syntax_scan::scan_lists(interp, env, &args[0], &args[1], &args[2])
            }
            "scan-sexps" => {
                need_arg_range(name, args, 2, 2)?;
                syntax_scan::scan_sexps(interp, env, &args[0], &args[1])
            }
            "backward-prefix-chars" => {
                need_arg_range(name, args, 0, 0)?;
                syntax_scan::backward_prefix_chars(interp, env)
            }
            "parse-partial-sexp" => {
                need_arg_range(name, args, 2, 6)?;
                syntax_scan::parse_partial_sexp(interp, env, args)
            }
            "buffer-string" => {
                let (begv, zv) = {
                    let buffer = interp.buffer.borrow();
                    (buffer.point_min(), buffer.point_max())
                };
                update_buffer_properties(interp, begv, zv, env)?;
                Ok(string_like_value_with_extended_chars(
                    interp.buffer.borrow().buffer_string(),
                    interp.buffer.borrow().string_intervals(
                        interp.buffer.borrow().point_min(),
                        interp.buffer.borrow().point_max(),
                    ),
                    interp.buffer.borrow().is_multibyte(),
                    interp.buffer.borrow().substring_extended_chars(
                        interp.buffer.borrow().point_min(),
                        interp.buffer.borrow().point_max(),
                    ),
                ))
            }
            "minibuffer-contents" | "minibuffer-contents-no-properties" => {
                need_arg_range(name, args, 0, 0)?;
                let active_minibuffer =
                    interp.active_minibuffer_buffer_id() == Some(interp.current_buffer_id());
                let prompt_length = if active_minibuffer {
                    interp
                        .minibuffer_prompt_text()
                        .map(|prompt| prompt.chars().count())
                        .unwrap_or(0)
                } else {
                    0
                };
                let start = interp
                    .buffer
                    .borrow()
                    .point_min()
                    .saturating_add(prompt_length)
                    .min(interp.buffer.borrow().point_max());
                let end = interp.buffer.borrow().point_max();
                // minibuf.c: make_buffer_string with properties.
                if name == "minibuffer-contents" {
                    crate::lisp::primitives::textprop::update_buffer_properties(
                        interp, start, end, env,
                    )?;
                }
                let (start, end) = {
                    let buffer = interp.buffer.borrow();
                    let end = end.min(buffer.point_max());
                    (start.min(end), end)
                };
                let text = interp
                    .buffer
                    .borrow()
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let props = if name == "minibuffer-contents" {
                    interp.buffer.borrow().string_intervals(start, end)
                } else {
                    None
                };
                Ok(string_like_value_with_extended_chars(
                    text,
                    props,
                    interp.buffer.borrow().is_multibyte(),
                    interp.buffer.borrow().substring_extended_chars(start, end),
                ))
            }
            "buffer-substring" | "buffer-substring-no-properties" => {
                buffer_substring_named(interp, name, args, env)
            }
            "invisible-p" => {
                need_args(name, args, 1)?;
                // xdisp.c:Finvisible_p: a natural number or marker is a
                // position whose `invisible' char-property is read, any
                // other object the property's value itself; nil, t, or 2
                // for text replaced by an ellipsis.
                let prop = match args[0].kind() {
                    Kind::Integer(position) if position >= 0 => get_char_property(
                        interp,
                        &args[0],
                        PropertyName::symbol("invisible"),
                        Value::Nil,
                    )?,
                    Kind::Marker(_) => get_char_property(
                        interp,
                        &args[0],
                        PropertyName::symbol("invisible"),
                        Value::Nil,
                    )?,
                    _ => args[0],
                };
                Ok(match invisibility_level(interp, &prop, env) {
                    0 => Value::Nil,
                    1 => Value::T,
                    level => Value::Integer(i64::from(level)),
                })
            }
            "buffer-size" => {
                need_arg_range(name, args, 0, 1)?;
                // editfns.c: CHECK_BUFFER; a killed buffer's size is the
                // one its text had.
                let buffer = interp.decode_buffer(args.first())?;
                let size = if interp.live_buffer_id(buffer).is_some() {
                    buffer.borrow().size_total()
                } else {
                    buffer.borrow().killed_size()
                };
                Ok(Value::Integer(size as i64))
            }
            "buffer-enable-undo" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c:Fbuffer_enable_undo: BUFFER through Fget_buffer
                // (a name must name a live buffer, nsberror's message),
                // whose list becomes nil where it is t.
                let buffer = match args.first().filter(|value| !value.is_nil()) {
                    None => Some(interp.buffer),
                    Some(value) => match interp.get_buffer(value)? {
                        Some(buffer) => Some(buffer),
                        None => return Err(crate::lisp::eval::runtime::nsberror(value)),
                    },
                };
                if let Some(buffer) = buffer {
                    buffer.borrow_mut().enable_undo();
                }
                Ok(Value::Nil)
            }
            // editfns.c:Fgap_position and Fgap_size: GPT and GAP_SIZE.
            "gap-position" => Ok(Value::Integer(interp.buffer.borrow().gap_position() as i64)),
            "gap-size" => Ok(Value::Integer(interp.buffer.borrow().gap_size() as i64)),
            "buffer-line-statistics" => {
                need_arg_range(name, args, 0, 1)?;
                buffer_line_statistics_value(interp, args.first())
            }
            "max-char" => {
                need_arg_range(name, args, 0, 1)?;
                // GNU keeps a wider internal character space for raw-byte and
                // legacy character representations, but a non-nil UNICODE
                // argument asks for the Unicode scalar ceiling specifically.
                // Unicode-wide Lisp scans rely on this distinction to stop at
                // U+10FFFF rather than traversing the whole internal space.
                Ok(Value::Integer(
                    if args.first().is_some_and(Value::is_truthy) {
                        0x10_FFFF
                    } else {
                        0x3F_FFFF
                    },
                ))
            }
            "position-bytes" => {
                let pos = if args.is_empty() {
                    interp.buffer.borrow().point()
                } else {
                    position_from_value(interp, &args[0])?
                };
                Ok(position_bytes(interp, pos)
                    .map(|byte_pos| Value::Integer(byte_pos as i64))
                    .unwrap_or(Value::Nil))
            }
            "byte-to-position" => {
                need_args(name, args, 1)?;
                // editfns.c:Fbyte_to_position: CHECK_FIXNUM.
                let byte = args[0].as_fixnum()?;
                if byte <= 0 {
                    return Ok(Value::Nil);
                }
                Ok(byte_to_position(interp, byte as usize)
                    .map(|pos| Value::Integer(pos as i64))
                    .unwrap_or(Value::Nil))
            }

            "buffer-name" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c:decode_buffer: nil for a killed buffer.
                let buffer = interp.decode_buffer(args.first())?;
                if interp.live_buffer_id(buffer).is_none() {
                    return Ok(Value::Nil);
                }
                // BVAR (decode_buffer (BUFFER), name): the object itself.
                Ok(buffer.borrow().name())
            }
            "set-buffer-multibyte" => {
                need_args(name, args, 1)?;
                let enabled = args[0].is_truthy();
                // buffer.c:Fset_buffer_multibyte reads the restriction, the
                // modified flag and the undo list before anything else.
                let (narrowed, modified_p, old_undo) = {
                    let buffer = interp.buffer.borrow();
                    (
                        buffer.restriction() != (1, buffer.size_total() + 1),
                        buffer.is_modified(),
                        buffer.undo_list(),
                    )
                };
                if interp.buffer_base_id(interp.current_buffer_id()).is_some() {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Cannot do `set-buffer-multibyte' on an indirect buffer",
                        &[],
                    ));
                }
                if enabled == interp.buffer.borrow().is_multibyte() {
                    return Ok(args[0]);
                }
                // The changes below are not recorded; an entry for the
                // whole conversion stands for them.  The list stays t when
                // the narrowed buffer's error leaves.
                interp.buffer.borrow_mut().set_undo_list(Value::T);
                if narrowed {
                    return Err(LispError::Signal(
                        "Changing multibyteness in a narrowed buffer".into(),
                    ));
                }

                let original = interp.buffer.borrow().full_buffer_string();
                let preserve_utf8_sequences = matches!(args[0].kind(), Kind::T);
                // Each eight-bit character converted is one byte inserted
                // (insert_1_both) or deleted (del_range_2) as the buffer's
                // bytes change form, each a modification of the text.
                // Those edits' marker adjustments, in the byte positions
                // the markers then hold, check the selected window's old
                // point as the edits before have moved it
                // (adjust_suspend_auto_hscroll).
                let old_point = interp.suspended_old_point_in(interp.current_buffer_id());
                let mut touched = false;
                let (converted, positions, extended, converted_bytes) = if enabled {
                    let MultibyteText {
                        text: converted,
                        positions,
                        extended,
                        eight_bit: eight_bit_bytes,
                    } = multibyte_buffer_text(&original, preserve_utf8_sequences);
                    // insert_1_both of a byte after each one made eight-bit.
                    if let Some(mut old) = old_point {
                        for (inserted, &index) in eight_bit_bytes.iter().enumerate() {
                            let at = index + 1 + inserted + 1;
                            if at <= old && old <= at + 1 {
                                touched = true;
                                break;
                            }
                            if old > at {
                                old += 1;
                            }
                        }
                    }
                    let eight_bit = eight_bit_chars(&converted, &extended);
                    (converted, positions, extended, eight_bit)
                } else {
                    let buffer = interp.buffer.borrow();
                    let original_extended = buffer.substring_extended_chars(1, usize::MAX);
                    // del_range_2 of a byte of each eight-bit character.
                    if let Some(old_point) = old_point {
                        let mut old = buffer.marker_byte_position(old_point);
                        let mut extended_codes = original_extended.iter().peekable();
                        let mut deleted = 0;
                        for (index, character) in original.chars().enumerate() {
                            let code = match extended_codes.peek() {
                                Some(&&(at, code)) if at == index => {
                                    extended_codes.next();
                                    Some(code)
                                }
                                _ => None,
                            };
                            if raw_byte_from_regex_char(character).is_none()
                                && !code.is_some_and(|code| code >= 0x3F_FF80)
                            {
                                continue;
                            }
                            let at = buffer.marker_byte_position(index + 1) - deleted;
                            if at <= old && old <= at + 1 {
                                touched = true;
                                break;
                            }
                            if old > at + 1 {
                                old -= 1;
                            } else if old > at {
                                old = at;
                            }
                            deleted += 1;
                        }
                    }
                    let eight_bit = eight_bit_chars(&original, &original_extended);
                    let (converted, positions) = unibyte_buffer_text(&original, &original_extended);
                    (converted, positions, Vec::new(), eight_bit)
                };
                if touched {
                    interp.resume_auto_hscroll();
                }
                {
                    let mut buffer = interp.buffer.borrow_mut();
                    buffer.set_multibyte_representation(enabled, converted, &positions, extended);
                    buffer.count_unrecorded_byte_edits(converted_bytes);
                }
                // Its indirect buffers take its new multibyte status, and
                // their point and bounds from their converted markers.
                let current_id = interp.current_buffer_id();
                let related = interp.related_buffer_ids(current_id);
                for buffer_id in &related {
                    if *buffer_id != current_id
                        && let Some(mut buffer) = interp.get_buffer_by_id_mut(*buffer_id)
                    {
                        buffer.set_multibyte(enabled);
                        buffer.set_prevent_redisplay_optimizations_p(true);
                    }
                }
                interp.fetch_family_positions(&related);
                if !matches!(old_undo.kind(), Kind::T) {
                    let entry = Value::list([
                        Value::Symbol(cached_symbol!("apply").with(Clone::clone)),
                        Value::Symbol(cached_symbol!("set-buffer-multibyte").with(Clone::clone)),
                        if enabled { Value::Nil } else { Value::T },
                    ]);
                    interp
                        .buffer
                        .borrow_mut()
                        .set_undo_list(Value::cons(entry, old_undo));
                }
                interp.prevent_redisplay_optimizations(current_id);
                // A buffer shown in a window makes redisplay consider every
                // window.
                if interp.buffer_window_count(interp.current_buffer_id()) > 0 {
                    interp.windows_or_buffers_changed = 10;
                }
                // The modifiedness of the buffer restored.
                if !modified_p && interp.buffer.borrow().is_modified() {
                    restore_buffer_modified_p(interp, env, Value::Nil)?;
                }
                Ok(args[0])
            }
            "char-after" => direct_char_after(interp, args, env),
            "char-before" => direct_char_before(interp, args, env),
            "matching-paren" => {
                need_args(name, args, 1)?;
                // syntax.c:Fmatching_paren: CHECK_CHARACTER, then the
                // current syntax table's match for an open or close paren.
                let code = check_character(&args[0])?;
                Ok(crate::lisp::primitives::syntax::matching_paren_value(
                    interp, code,
                ))
            }
            "get-byte" => {
                if let Some(string_value) = args.get(1).filter(|value| !value.is_nil()) {
                    let string = string_like(string_value).ok_or_else(|| {
                        LispError::WrongTypeArgument("stringp".into(), *string_value)
                    })?;
                    let position = match args
                        .first()
                        .filter(|value| !value.is_nil())
                        .map(|v| v.kind())
                    {
                        Some(Kind::Integer(position)) if position >= 0 => position as usize,
                        Some(value) => {
                            return Err(wrong_type_argument("wholenump", value.value()));
                        }
                        None => 0,
                    };
                    let mut characters = string.text.chars();
                    let character = characters.nth(position);
                    if character.is_none() && (position != 0 || !string.text.is_empty()) {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("args-out-of-range".into()),
                            *string_value,
                            Value::Integer(position as i64),
                        ])));
                    }
                    character_byte_value(character, string.multibyte)
                } else {
                    let position = match args.first().filter(|value| !value.is_nil()) {
                        Some(value) => {
                            let position = position_from_value(interp, value)?;
                            if position < interp.buffer.borrow().point_min()
                                || position >= interp.buffer.borrow().point_max()
                            {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("args-out-of-range".into()),
                                    *value,
                                    Value::Integer(interp.buffer.borrow().point_min() as i64),
                                    Value::Integer(interp.buffer.borrow().point_max() as i64),
                                ])));
                            }
                            position
                        }
                        None => interp.buffer.borrow().point(),
                    };
                    character_byte_value(
                        interp.buffer.borrow().char_at(position),
                        interp.buffer.borrow().is_multibyte(),
                    )
                }
            }
            "bobp" => direct_bobp(interp, args, env),
            "eobp" => direct_eobp(interp, args, env),
            "bolp" => direct_bolp(interp, args, env),
            "eolp" => direct_eolp(interp, args, env),
            "delete-region" => direct_delete_region(interp, args, env),
            "delete-and-extract-region" => {
                need_args(name, args, 2)?;
                let (from, to) = validate_region(interp, &args[0], &args[1])?;
                extract_region_with_hooks(interp, from, to, env)
            }
            "delete-char" => {
                // cmds.c:Fdelete_char: CHECK_FIXNUM N; a non-nil KILLFLAG
                // hands N to `kill-forward-chars'.
                need_arg_range(name, args, 1, 2)?;
                let Kind::Integer(n) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("fixnump".into(), args[0]));
                };
                if n.unsigned_abs() < 2 {
                    call_undo_auto_amalgamate(interp, env)?;
                }
                if args.get(1).is_some_and(Value::is_truthy) {
                    let function = cached_symbol!("kill-forward-chars").with(Clone::clone);
                    interp.call_function_value(
                        Value::Symbol(function),
                        None,
                        &[Value::Integer(n)],
                        env,
                    )?;
                    return Ok(Value::Nil);
                }
                let point = interp.buffer.borrow().point();
                if n >= 0 {
                    let to = point + n as usize;
                    if to > interp.buffer.borrow().point_max() {
                        Err(crate::buffer::BufferError::EndOfBuffer.into())
                    } else {
                        delete_region_with_hooks(interp, point, to, env)?;
                        Ok(Value::Nil)
                    }
                } else {
                    let count = (-n) as usize;
                    if point < interp.buffer.borrow().point_min() + count {
                        Err(crate::buffer::BufferError::BeginningOfBuffer.into())
                    } else {
                        delete_region_with_hooks(interp, point - count, point, env)?;
                        Ok(Value::Nil)
                    }
                }
            }
            "erase-buffer" => {
                // buffer.c:Ferase_buffer: Fwiden, then del_range (BEG, Z),
                // which del_range_1 clamps to what a labeled restriction
                // leaves accessible; then the saved size is reset, so
                // auto-saving does not take the shrink for lost text.
                widen_current_buffer(interp);
                let min = interp.buffer.borrow().point_min();
                let max = interp.buffer.borrow().point_max();
                if min < max {
                    delete_region_with_hooks(interp, min, max, env)?;
                }
                let buffer_id = interp.current_buffer_id();
                interp.set_buffer_local_value_symbol(
                    buffer_id,
                    &cached_symbol!("buffer-saved-size").with(Clone::clone),
                    Value::Integer(0),
                );
                Ok(Value::Nil)
            }
            "upcase-region"
            | "downcase-region"
            | "capitalize-region"
            | "upcase-initials-region" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let action = match name {
                    "upcase-region" => CaseAction::Up,
                    "downcase-region" => CaseAction::Down,
                    "capitalize-region" => CaseAction::Capitalize,
                    _ => CaseAction::UpcaseInitials,
                };
                if args.get(2).is_some_and(Value::is_truthy) {
                    let extractor = interp
                        .lookup_var_key(cached_symbol!("region-extract-function"), env)
                        .ok_or_else(|| LispError::Void(Value::symbol("region-extract-function")))?;
                    let bounds = call_function_value(
                        interp,
                        &extractor,
                        &[Value::Symbol("bounds".into())],
                        env,
                    )?;
                    // casefiddle.c:casify_pnc_region: each bound a cons,
                    // validated as casify_region's own region, the list
                    // walked as FOR_EACH_TAIL walks it (Brent's check for a
                    // cycle) and proper.
                    let mut tail = bounds;
                    let mut tortoise = bounds;
                    let (mut steps, mut limit) = (0_usize, 2_usize);
                    while let Kind::Cons(cell) = tail.kind() {
                        let bound = cell.car.get();
                        let Kind::Cons(range) = bound.kind() else {
                            return Err(LispError::WrongTypeArgument("consp".into(), bound));
                        };
                        let (start, end) =
                            validate_region(interp, &range.car.get(), &range.cdr.get())?;
                        casify_buffer_region(interp, start, end, action, env)?;
                        tail = cell.cdr.get();
                        steps += 1;
                        if steps == limit {
                            tortoise = tail;
                            steps = 0;
                            limit *= 2;
                        } else if let (Kind::Cons(current), Kind::Cons(lagging)) =
                            (tail.kind(), tortoise.kind())
                            && crate::lisp::types::SharedCons::ptr_eq(&current, &lagging)
                        {
                            return Err(LispError::SignalValue(Value::list([
                                Value::symbol("circular-list"),
                                bounds,
                            ])));
                        }
                    }
                    if !tail.is_nil() {
                        return Err(LispError::WrongTypeArgument("listp".into(), bounds));
                    }
                    Ok(Value::Nil)
                } else {
                    // casefiddle.c:casify_region's validate_region.
                    let (start, end) = validate_region(interp, &args[0], &args[1])?;
                    casify_buffer_region(interp, start, end, action, env)?;
                    Ok(Value::Nil)
                }
            }
            "upcase-word" | "downcase-word" | "capitalize-word" => {
                need_args(name, args, 1)?;
                let action = match name {
                    "upcase-word" => CaseAction::Up,
                    "downcase-word" => CaseAction::Down,
                    _ => CaseAction::Capitalize,
                };
                // casefiddle.c's casify_word.
                let count = super::super::syntax_scan::check_fixnum(&args[0])?;
                let point = interp.buffer.borrow().point();
                let farend = super::super::syntax_scan::scan_words(interp, env, point, count)?
                    .unwrap_or_else(|| {
                        let buffer = interp.buffer.borrow();
                        if count <= 0 {
                            buffer.point_min()
                        } else {
                            buffer.point_max()
                        }
                    });
                let new_end = casify_buffer_region(interp, point, farend, action, env)?;
                interp.buffer.borrow_mut().goto_char(new_end);
                Ok(Value::Nil)
            }
            "current-column" => Ok(Value::Integer(current_column(interp, env)? as i64)),
            "current-indentation" => {
                // indent.c:Fcurrent_indentation: position_indentation from
                // the line's start.
                let point = interp.buffer.borrow().point();
                let bol = line_beginning_at(interp, point);
                Ok(Value::Integer(position_indentation(interp, env, bol) as i64))
            }
            "indent-to" => {
                // indent.c Findent_to: CHECK_FIXNUM both COLUMN and MINIMUM.
                need_arg_range(name, args, 1, 2)?;
                let target = syntax_scan::check_fixnum(&args[0])?;
                let minimum = match args.get(1) {
                    Some(value) if !value.is_nil() => syntax_scan::check_fixnum(value)?,
                    _ => 0,
                };
                indent_to(interp, target, minimum, env).map(Value::Integer)
            }
            "move-to-column" => {
                // indent.c Fmove_to_column: scan_for_column to the goal.
                need_arg_range(name, args, 1, 2)?;
                let goal = match args[0].kind() {
                    Kind::Integer(goal) if goal >= 0 => goal as usize,
                    _ => return Err(LispError::WrongTypeArgument("wholenump".into(), args[0])),
                };
                let force = args.get(1).copied().unwrap_or(Value::Nil);
                let point = interp.buffer.borrow().point();
                let start = line_beginning_at(interp, point);
                let zv = interp.buffer.borrow().point_max();
                let scan = scan_for_column(interp, env, start, zv, Some(goal))?;
                let mut col = scan.col;
                interp.buffer.borrow_mut().goto_char(scan.pos);
                // If a tab made us overshoot, change it to spaces and scan
                // through it again.
                if !force.is_nil()
                    && col > goal
                    && interp.buffer.borrow().char_at(scan.prev_pos) == Some('\t')
                    && scan.prev_col < goal
                    && scan.prev_pos < scan.pos
                {
                    // Insert spaces in front of the tab first, so a marker
                    // at the end of the tab gets adjusted.
                    interp.buffer.borrow_mut().goto_char(scan.prev_pos);
                    let spaces = " ".repeat(goal - scan.prev_col);
                    insert_text_with_hooks(interp, &spaces, None, &[], true, false, env)?;
                    // Now delete the tab, and indent to COL.
                    let point = interp.buffer.borrow().point();
                    delete_region_with_hooks(interp, point, point + 1, env)?;
                    let goal_pt = interp.buffer.borrow().point();
                    indent_to(interp, col as i64, 0, env)?;
                    interp.buffer.borrow_mut().goto_char(goal_pt);
                    col = goal;
                }
                // If the line ends prematurely, add space to the end.
                if col < goal && matches!(force.kind(), Kind::T) {
                    col = goal;
                    indent_to(interp, goal as i64, 0, env)?;
                }
                remember_current_column(interp, col);
                Ok(Value::Integer(col as i64))
            }
            "line-number-at-pos" => {
                need_arg_range(name, args, 0, 2)?;
                let pos = if args.is_empty() || args[0].is_nil() {
                    interp.buffer.borrow().point()
                } else {
                    match args[0].kind() {
                        Kind::Integer(pos) => {
                            if pos < 0 {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::Symbol("args-out-of-range".into()),
                                    Value::Integer(pos),
                                    Value::Integer(1),
                                    Value::Integer(
                                        (interp.buffer.borrow().size_total() + 1) as i64,
                                    ),
                                ])));
                            }
                            pos as usize
                        }
                        Kind::Marker(id) => interp
                            .marker_position(id)
                            .ok_or_else(marker_points_nowhere)?,
                        // Fline_number_at_pos: CHECK_FIXNUM.
                        _ => {
                            return Err(LispError::WrongTypeArgument("fixnump".into(), args[0]));
                        }
                    }
                };
                let absolute_max = interp.buffer.borrow().size_total() + 1;
                // Only a fixnum POSITION is checked against BEG and Z; a
                // marker's (another buffer's) position is taken as it is.
                let is_marker = matches!(
                    args.first().map(|value| value.kind()),
                    Some(Kind::Marker(_))
                );
                if !is_marker && (pos < 1 || pos > absolute_max) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("args-out-of-range".into()),
                        Value::Integer(pos as i64),
                        Value::Integer(1),
                        Value::Integer(absolute_max as i64),
                    ])));
                }
                let absolute = args.get(1).is_some_and(Value::is_truthy);
                let start = if absolute {
                    1
                } else {
                    interp.buffer.borrow().point_min()
                };
                let pos = if absolute {
                    pos
                } else {
                    pos.clamp(
                        interp.buffer.borrow().point_min(),
                        interp.buffer.borrow().point_max(),
                    )
                };
                // The second check, `args_out_of_range_3 (BYTE_TO_CHAR
                // (pos_byte), BEG, Z)', past Z (an absolute marker position).
                if pos > absolute_max {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("args-out-of-range".into()),
                        Value::Integer(pos as i64),
                        Value::Integer(1),
                        Value::Integer(absolute_max as i64),
                    ])));
                }
                // fns.c: count_lines (start_byte, pos_byte) + 1.
                let line = interp.buffer.borrow().count_newlines(start, pos) + 1;
                Ok(Value::Integer(line as i64))
            }
            "line-number-display-width" => {
                need_arg_range(name, args, 0, 1)?;
                line_number_display_width_value(interp, env, args.first())
            }
            "line-beginning-position" | "pos-bol" => {
                need_arg_range(name, args, 0, 1)?;
                let n = line_count_arg(args.first())?;
                Ok(Value::Integer(if name == "pos-bol" {
                    bol(interp, n) as i64
                } else {
                    line_beginning_position(interp, n)?
                }))
            }
            "line-end-position" | "pos-eol" => {
                need_arg_range(name, args, 0, 1)?;
                let n = line_count_arg(args.first())?;
                Ok(Value::Integer(if name == "pos-eol" {
                    eol(interp, n) as i64
                } else {
                    line_end_position(interp, n)?
                }))
            }
            "narrow-to-region" => {
                need_args(name, args, 2)?;
                narrow_current_buffer(interp, &args[0], &args[1])?;
                Ok(Value::Nil)
            }
            "widen" => {
                widen_current_buffer(interp);
                Ok(Value::Nil)
            }
            "buffer-modified-p" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c:decode_buffer: a killed buffer's ticks as it
                // left them.
                let buffer = interp.decode_buffer(args.first())?;
                let buffer = buffer.borrow();
                Ok(if buffer.is_autosaved() {
                    Value::Symbol("autosaved".into())
                } else if buffer.is_modified() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "buffer-chars-modified-tick" | "buffer-modified-tick" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer = interp.decode_buffer(args.first())?;
                let buffer = buffer.borrow();
                let tick = if name == "buffer-chars-modified-tick" {
                    buffer.chars_modified_tick()
                } else {
                    buffer.modified_tick()
                };
                Ok(Value::Integer(tick))
            }
            "internal--set-buffer-modified-tick" => {
                need_arg_range(name, args, 1, 2)?;
                let Kind::Integer(tick) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("fixnump".into(), args[0]));
                };
                let buffer = interp.decode_buffer(args.get(1))?;
                buffer.borrow_mut().set_modified_tick(tick);
                // BUF_MODIFF of the text a live buffer's family shares.
                if let Some(id) = interp.live_buffer_id(buffer) {}
                Ok(Value::Nil)
            }
            "set-buffer-modified-p" => {
                need_args(name, args, 1)?;
                // Frestore_buffer_modified_p, then Fforce_mode_line_update
                // (Qnil)'s value.
                restore_buffer_modified_p(interp, env, args[0])?;
                interp.force_mode_line_update(false);
                Ok(Value::Nil)
            }
            "restore-buffer-modified-p" => {
                need_args(name, args, 1)?;
                restore_buffer_modified_p(interp, env, args[0])
            }
            "get-char-property" => {
                need_arg_range(name, args, 2, 3)?;
                get_char_property(
                    interp,
                    &args[0],
                    PropertyName::new(args[1]),
                    args.get(2).copied().unwrap_or(Value::Nil),
                )
            }
            "get-pos-property" => {
                need_arg_range(name, args, 2, 3)?;
                get_pos_property(
                    interp,
                    &args[0],
                    PropertyName::new(args[1]),
                    args.get(2).copied().unwrap_or(Value::Nil),
                )
            }
            "get-char-property-and-overlay" => {
                need_arg_range(name, args, 2, 3)?;
                let (value, overlay) = get_char_property_and_overlay(
                    interp,
                    &args[0],
                    PropertyName::new(args[1]),
                    args.get(2).copied().unwrap_or(Value::Nil),
                )?;
                Ok(Value::cons(
                    value,
                    overlay.map_or(Value::Nil, Value::Overlay),
                ))
            }
            "get-text-property" => {
                need_arg_range(name, args, 2, 3)?;
                get_text_property(
                    interp,
                    &args[0],
                    PropertyName::new(args[1]),
                    args.get(2).copied().unwrap_or(Value::Nil),
                )
            }
            "text-property-any" | "text-property-not-all" => {
                if args.len() < 4 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                text_property_any(interp, args, name == "text-property-any")
            }
            "next-single-property-change" => {
                if args.len() < 2 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                next_single_property_change(interp, args)
            }
            "next-property-change" => {
                need_arg_range(name, args, 1, 3)?;
                next_property_change(
                    interp,
                    &args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                    args.get(2).copied().unwrap_or(Value::Nil),
                )
            }
            "next-single-char-property-change" => {
                need_arg_range(name, args, 2, 4)?;
                next_single_char_property_change(
                    interp,
                    &args[0],
                    args[1],
                    args.get(2).copied().unwrap_or(Value::Nil),
                    args.get(3).copied().unwrap_or(Value::Nil),
                )
            }
            "previous-single-char-property-change" => {
                need_arg_range(name, args, 2, 4)?;
                previous_single_char_property_change(
                    interp,
                    &args[0],
                    args[1],
                    args.get(2).copied().unwrap_or(Value::Nil),
                    args.get(3).copied().unwrap_or(Value::Nil),
                )
            }
            "previous-single-property-change" => {
                if args.len() < 2 || args.len() > 4 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                previous_single_property_change(interp, args)
            }
            "previous-property-change" => {
                need_arg_range(name, args, 1, 3)?;
                previous_property_change(
                    interp,
                    &args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                    args.get(2).copied().unwrap_or(Value::Nil),
                )
            }
            "next-char-property-change" => {
                need_arg_range(name, args, 1, 2)?;
                next_char_property_change(
                    interp,
                    &args[0],
                    &args.get(1).copied().unwrap_or(Value::Nil),
                )
            }
            "previous-char-property-change" => {
                need_arg_range(name, args, 1, 2)?;
                previous_char_property_change(
                    interp,
                    &args[0],
                    &args.get(1).copied().unwrap_or(Value::Nil),
                )
            }
            "text-properties-at" => {
                need_arg_range(name, args, 1, 2)?;
                text_properties_at(interp, &args[0], args.get(1).copied().unwrap_or(Value::Nil))
            }
            "object-intervals" => {
                need_args(name, args, 1)?;
                object_intervals_value(interp, &args[0])
            }
            "put-text-property" => {
                need_arg_range(name, args, 4, 5)?;
                let properties = Value::list([args[2], args[3]]);
                add_text_properties(
                    interp,
                    &args[0],
                    &args[1],
                    properties,
                    args.get(4).copied().unwrap_or(Value::Nil),
                    env,
                )?;
                Ok(Value::Nil)
            }
            "add-text-properties" => {
                need_arg_range(name, args, 3, 4)?;
                let changed = add_text_properties(
                    interp,
                    &args[0],
                    &args[1],
                    args[2],
                    args.get(3).copied().unwrap_or(Value::Nil),
                    env,
                )?;
                Ok(if changed { Value::T } else { Value::Nil })
            }
            "set-text-properties" => {
                need_arg_range(name, args, 3, 4)?;
                let changed = set_text_properties(
                    interp,
                    &args[0],
                    &args[1],
                    args[2],
                    args.get(3).copied().unwrap_or(Value::Nil),
                    env,
                )?;
                Ok(if changed { Value::T } else { Value::Nil })
            }
            "remove-text-properties" | "remove-list-of-text-properties" => {
                need_arg_range(name, args, 3, 4)?;
                let changed = remove_text_properties(
                    interp,
                    &args[0],
                    &args[1],
                    args[2],
                    args.get(3).copied().unwrap_or(Value::Nil),
                    name == "remove-list-of-text-properties",
                    env,
                )?;
                Ok(if changed { Value::T } else { Value::Nil })
            }
            "add-face-text-property" => {
                need_arg_range(name, args, 3, 5)?;
                let append = args.get(3).is_some_and(|value| value.is_truthy());
                add_face_text_property(
                    interp,
                    &args[0],
                    &args[1],
                    args[2],
                    append,
                    args.get(4).copied().unwrap_or(Value::Nil),
                    env,
                )?;
                Ok(Value::Nil)
            }
            "put" => {
                need_args(name, args, 3)?;
                // GNU 30.2 fns.c:Fput shares `get's CHECK_SYMBOL/XSYMBOL
                // treatment of source-positioned symbols.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                let property_name = super::misc::bare_symbol_name(interp, env, &args[1]);
                // GNU Elisp owns `ert-set-test' and `define-symbol-prop',
                // including duplicate-definition policy and load-history
                // provenance.  Mirror the final public property write into
                // Emaxx's native runner index only when it is an actual ERT
                // record; ordinary user properties remain ordinary `put'.
                if property_name.as_deref() == Some("ert--test")
                    && matches!(args[2].kind(), Kind::LispRecord(_))
                {
                    return interp.ert_set_test(symbol.as_str(), &args[2]);
                }
                let plist = super::collections::plist_put_eq(
                    interp,
                    interp.symbol_plist_symbol(&symbol),
                    args[1],
                    args[2],
                    env,
                )?;
                interp.set_symbol_plist_symbol(&symbol, plist)?;
                Ok(args[2])
            }
        }
    }
);

/// Column width of a display spec form: integers are columns, one-element
/// lists are pixels (one per column on the batch frame), symbols are
/// variables, and (+ ...)/(- ...) combine recursively.
fn display_spec_width(interp: &mut Interpreter, env: &mut Env, spec: &Value) -> i64 {
    match spec.kind() {
        Kind::Integer(value) => value,
        Kind::Float(value) => value.get() as i64,
        Kind::Symbol(name) => interp
            .lookup_var_symbol(&name, env)
            .map(|value| display_spec_width(interp, env, &value))
            .unwrap_or(0),
        Kind::Cons(_) => {
            let Ok(items) = spec.to_vec() else { return 0 };
            match items.first().map(|v| v.kind()) {
                Some(Kind::Symbol(op)) if op.is_named("-") || op.is_named("+") => {
                    let mut acc = items
                        .get(1)
                        .map(|item| display_spec_width(interp, env, item))
                        .unwrap_or(0);
                    for item in items.iter().skip(2) {
                        let value = display_spec_width(interp, env, item);
                        if op.is_named("-") {
                            acc -= value;
                        } else {
                            acc += value;
                        }
                    }
                    acc
                }
                Some(inner) if items.len() == 1 => display_spec_width(interp, env, &inner.value()),
                _ => 0,
            }
        }
        _ => 0,
    }
}

/// Column width contributed by a `line-prefix'/`wrap-prefix' property.
fn prefix_property_width(interp: &mut Interpreter, env: &mut Env, prop: Option<Value>) -> usize {
    let Some(prop) = prop else { return 0 };
    if let Some(text) = string_like(&prop) {
        return text.text.chars().count();
    }
    if let Ok(items) = prop.to_vec()
        && matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head.is_named("space"))
    {
        let mut cursor = 1;
        while cursor + 1 < items.len() {
            if crate::lisp::types::eq_symbol_named(items[cursor], ":width") {
                return display_spec_width(interp, env, &items[cursor + 1]).max(0) as usize;
            }
            cursor += 2;
        }
    }
    0
}

/// Per-position display widths for [bol, eol): `usize::MAX` marks a TAB
/// (resolved against the running column), 0 marks invisible or
/// display-replaced text, and display strings count once per run.
pub(crate) fn visual_char_widths(
    interp: &mut Interpreter,
    env: &mut Env,
    bol: usize,
    eol: usize,
) -> Vec<(usize, bool)> {
    let _ = env;
    let mut widths = Vec::with_capacity(eol.saturating_sub(bol));
    let mut pos = bol;
    while pos < eol {
        let display = interp.buffer.borrow().text_property_at(pos, "display");
        if let Some(display_value) = display.filter(|value| !value.is_nil()) {
            let mut end = pos;
            while end < eol && interp.buffer.borrow().text_property_at(end, "display") == display {
                end += 1;
            }
            let run_width = string_like(&display_value)
                .map(|text| text.text.chars().count())
                .unwrap_or(0);
            widths.push((run_width, false));
            for _ in pos + 1..end {
                widths.push((0, false));
            }
            pos = end;
            continue;
        }
        if interp
            .buffer
            .borrow()
            .text_property_at(pos, "invisible")
            .is_some_and(|value| value.is_truthy())
        {
            widths.push((0, false));
            pos += 1;
            continue;
        }
        match interp.buffer.borrow().char_at(pos) {
            Some('\t') => widths.push((usize::MAX, true)),
            Some(' ') => widths.push((1, true)),
            Some(_) => widths.push((1, false)),
            None => widths.push((0, false)),
        }
        pos += 1;
    }
    widths
}

/// Screen-line segment start positions of the logical line [bol, eol),
/// modeling GNU's batch display: continuation at frame-width (reserving the
/// continuation column unless word-wrapping) minus prefix widths.
/// The columns a `display-line-numbers' column occupies during
/// screen-line motion: Fvertical_motion primes its iterator's
/// lnum_width from line_number_display_width — the width redisplay
/// computes at the window's start — so wrapped lines break that many
/// columns earlier.
fn motion_line_number_columns(interp: &mut Interpreter, env: &mut Env) -> usize {
    let window_id = interp.selected_window_id();
    window_line_number_columns(interp, env, window_id)
}

/// The `display-line-numbers' columns of WINDOW, which shows the current
/// buffer: the width redisplay computes at its start.
pub(crate) fn window_line_number_columns(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
) -> usize {
    let buffer_id = interp.current_buffer_id();
    let enabled = interp
        .buffer_local_value_key(buffer_id, cached_symbol!("display-line-numbers"))
        .or_else(|| {
            interp.lookup_var_key(
                cached_symbol!("display-line-numbers"),
                &crate::lisp::types::Env::new(),
            )
        })
        .unwrap_or(Value::Nil);
    if enabled.is_nil() {
        return 0;
    }
    let window = interp.record_value(window_id);
    let text_rows = super::call(interp, "window-body-height", &[window], env)
        .ok()
        .and_then(|value| value.integer_value())
        .unwrap_or(0)
        .max(1) as usize;
    let (point_min, point_max) = {
        let buffer = interp.buffer.borrow();
        (buffer.point_min(), buffer.point_max())
    };
    let start = crate::lisp::primitives::window::window_start(interp, Some(&window))
        .unwrap_or(point_min)
        .clamp(point_min, point_max);
    let point = if window_id == interp.selected_window_id() {
        interp.buffer.borrow().point()
    } else {
        super::call(interp, "window-point", &[window], env)
            .ok()
            .and_then(|value| value.integer_value())
            .map_or(point_min, |point| point.max(0) as usize)
    }
    .clamp(point_min, point_max);
    let top_line = interp.buffer.borrow().line_number_at_pos(start);
    let point_line = interp.buffer.borrow().line_number_at_pos(point);
    let begv_line = interp
        .buffer
        .borrow()
        .line_number_at_pos(interp.buffer.borrow().point_min());
    crate::lisp::primitives::window_line_number_layout(
        interp, buffer_id, top_line, point_line, begv_line, text_rows,
    )
    .map_or(0, |layout| layout.cols)
}

/// How WINDOW, which shows the current buffer, lays its text out on a
/// terminal: its line-number columns, the text width right of them,
/// truncation and hscroll.
pub(crate) fn window_text_geometry(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
) -> crate::tty::WindowTextGeometry {
    let lnum_cols = window_line_number_columns(interp, env, window_id);
    let truncate = crate::tty::window_truncates(interp, window_id, interp.current_buffer_id());
    let state = crate::lisp::primitives::window_hscroll_state(interp, window_id);
    let hscroll = state.hscroll.max(0) as usize;
    let min_hscroll = state.min_hscroll.max(0) as usize;
    let current_line = interp
        .buffer_local_value(interp.current_buffer_id(), "auto-hscroll-mode")
        .or_else(|| interp.default_value("auto-hscroll-mode"))
        .is_some_and(|mode| crate::lisp::types::eq_symbol_named(mode, "current-line"));
    crate::tty::WindowTextGeometry {
        lnum_cols,
        text_width: crate::lisp::primitives::window_text_area_columns(interp, window_id)
            .saturating_sub(lnum_cols)
            .max(1),
        truncate,
        hscroll,
        // init_iterator: hscrolling_current_line_p hscrolls only the row
        // showing point, the others by the min_hscroll.
        first_visible: if truncate && current_line && !state.suspended {
            min_hscroll
        } else {
            hscroll
        },
        // buffer_posn_from_coords tests `auto-hscroll-mode' alone.
        point_row_hscroll: (truncate && current_line).then(|| hscroll.saturating_sub(min_hscroll)),
    }
}

/// Whether screen-line motion follows a live terminal display rather
/// than the batch model: Fvertical_motion's `noninteractive' test.
pub(crate) fn interactive_motion(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var_key(cached_symbol!("noninteractive"), env)
        .is_some_and(|value| value.is_nil())
}

/// The display line [BOL, EOL) of the current buffer as WINDOW lays it
/// out on a terminal (tty.rs), for interactive motion.
pub(crate) fn interactive_screen_line(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    bol: usize,
) -> crate::tty::ScreenLineLayout {
    let lnum_cols = motion_line_number_columns(interp, env);
    let width = crate::lisp::primitives::window_text_area_columns(interp, window_id)
        .saturating_sub(lnum_cols)
        .max(1);
    let truncate = crate::tty::window_truncates(interp, window_id, interp.current_buffer_id());
    crate::tty::screen_line_layout(interp, window_id, bol, width, truncate, lnum_cols)
}

/// xdisp.c:window_text_pixel_size for the current buffer shown in
/// WINDOW on a terminal frame, where a pixel is a cell and every screen
/// line is one high: the iterator walks the screen lines tty.rs lays out
/// from FROM to TO.  Each line it leaves counts its height and the x it
/// reached (its newline's, or the whole width past `last_visible_x' for
/// a continued or truncated line, move_it_to's max_current_x).  The line
/// it stops on counts only when a glyph came before TO on it.
pub(crate) fn window_text_pixel_size(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    args: [Value; 6],
) -> Result<Value, LispError> {
    let [from, to, x_limit, y_limit, _mode_lines, ignore_line_at_end] = args;
    let (begv, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point_min(), buffer.point_max())
    };
    let char_at = |interp: &Interpreter, pos: usize| interp.buffer.borrow().char_at(pos);
    let clip = |value: i64, low: usize, high: usize| value.clamp(low as i64, high as i64) as usize;
    let mut vertical_offset = 0i64;
    let mut start = match from.kind() {
        Kind::Nil => begv,
        Kind::T => {
            let mut start = begv;
            while start < zv && matches!(char_at(interp, start), Some(' ' | '\t' | '\n' | '\r')) {
                start += 1;
            }
            while start > begv {
                start -= 1;
                if !matches!(char_at(interp, start), Some(' ' | '\t')) {
                    break;
                }
            }
            start
        }
        Kind::Cons(_) => {
            let (car, cdr) = from.cons_values().expect("a cons");
            let position = clip(
                crate::lisp::primitives::fix_position(interp, &car)?,
                begv,
                zv,
            );
            let Kind::Integer(offset) = cdr.kind() else {
                return Err(wrong_type_argument("fixnump", cdr));
            };
            vertical_offset = offset;
            position
        }
        _ => clip(
            crate::lisp::primitives::fix_position(interp, &from)?,
            begv,
            zv,
        ),
    };
    let end = match to.kind() {
        Kind::Nil => zv,
        Kind::T => {
            let mut end = zv;
            while end > begv {
                end -= 1;
                if !matches!(char_at(interp, end), Some(' ' | '\t' | '\n' | '\r')) {
                    end += 1;
                    break;
                }
            }
            while end < zv {
                let c = char_at(interp, end);
                end += 1;
                if !matches!(c, Some(' ' | '\t')) {
                    break;
                }
            }
            end
        }
        _ => clip(
            crate::lisp::primitives::fix_position(interp, &to)?,
            start,
            zv,
        ),
    };
    const INT_MAX: usize = i32::MAX as usize;
    let max_x = match x_limit.kind() {
        Kind::Integer(limit @ 0..=0x7fff_ffff) => Some(limit as usize),
        Kind::Nil => None,
        _ => Some(INT_MAX),
    };
    let max_y = match y_limit.kind() {
        Kind::Nil => INT_MAX,
        Kind::Integer(limit @ 0..=0x7fff_ffff) => limit as usize,
        _ => 0,
    };
    // The iterator runs `fontification-functions' for the text it
    // fetches (handle_fontified_prop) before what it measures is read:
    // start_display at FROM, move_it_by_lines (0) back to FROM's screen
    // line and forward to FROM again, the vertical offset's lines, then
    // move_it_to TO, no further down than the row holding Y-LIMIT.
    let mut fetches = crate::tty::IteratorFetches::new(interp, env, window_id);
    if let Some(geometry) = fetches.geometry() {
        crate::tty::fetch_start_display(interp, window_id, &mut fetches, start);
        crate::tty::fetch_screen_line_start(interp, window_id, &mut fetches, start);
        let from = if vertical_offset != 0 {
            super::super::move_it::move_it_by_lines(
                interp,
                env,
                window_id,
                start,
                vertical_offset,
                &mut fetches,
            )
            .0
            .clamp(begv, zv)
        } else {
            start
        };
        // The iterator move_it_by_lines left, which stops again where
        // it stands.
        fetches.begin_walk();
        crate::tty::IteratorWalk::at(interp, window_id, geometry, from).walk_to_within(
            &mut fetches,
            from,
            end,
            max_y,
        );
        fetches.fontify(interp, env, window_id);
    }
    let lnum = motion_line_number_columns(interp, env);
    let text_area = crate::lisp::primitives::window_text_area_columns(interp, window_id);
    // it.last_visible_x: the text area less the continuation column, which
    // FROM is reached with; TO is then measured to X-LIMIT itself.
    let default_width = text_area.saturating_sub(lnum).max(1);
    let width = match max_x {
        Some(limit) => limit.saturating_add(1).saturating_sub(lnum).max(1),
        None => default_width,
    };
    let truncate = crate::tty::window_truncates(interp, window_id, interp.current_buffer_id());
    // init_iterator's first_visible_x: the window's hscroll (its
    // min_hscroll when only the current line hscrolls), which the
    // iterator's x counts from and its last_visible_x lies past.
    let hscroll = if truncate {
        window_text_geometry(interp, env, window_id).first_visible
    } else {
        0
    };
    let default_last_visible_x = hscroll + lnum + default_width.saturating_sub(1).max(1);
    // X-LIMIT is it.last_visible_x itself.
    let last_visible_x = if max_x.is_some() {
        lnum + width.saturating_sub(1).max(1)
    } else {
        default_last_visible_x
    };
    // The iterator's x of a glyph at COLUMN: the line-number glyphs come
    // once a glyph reaches past first_visible_x.
    let iterator_x = move |column: usize| {
        if hscroll == 0 || column > hscroll {
            lnum + column
        } else {
            column
        }
    };
    if vertical_offset != 0 {
        // move_it_by_lines (&it, 0), then up or down that many lines.
        let (landed, _) = super::super::move_it::move_it_by_lines(
            interp,
            env,
            window_id,
            start,
            vertical_offset,
            &mut crate::tty::IteratorFetches::off(),
        );
        start = landed.clamp(begv, zv);
    }
    // One display context for every line the iterator walks.
    let interp: &Interpreter = interp;
    let layouter = crate::tty::ScreenLineLayouter::new(interp, window_id, lnum);
    let layout_with = |pos: usize, width: usize| {
        let (bol, eol) = visual_line_bounds(interp, pos);
        (layouter.layout(bol, width, truncate), eol)
    };
    let (mut layout, mut eol) = layout_with(start, width);
    let mut start_x = if vertical_offset != 0 {
        iterator_x(0)
    } else if width == default_width {
        iterator_x(layout.position_cell(start).1)
    } else {
        iterator_x(layout_with(start, default_width).0.position_cell(start).1)
    };
    let mut row = layout.position_cell(start).0;
    let start_on_newline = char_at(interp, start) == Some('\n');
    let mut iterator_at = start;
    // FROM in a truncated line's hidden rest: the iterator stops where the
    // line was cut and reseats at the next line's start, still that far
    // right unless FROM is a newline.
    let mut carried = false;
    if truncate && start_x >= default_last_visible_x {
        start_x = default_last_visible_x;
        if eol < zv {
            (layout, eol) = layout_with(eol + 1, width);
            row = 0;
            iterator_at = layout.row_starts[0];
            carried = !start_on_newline;
        }
    }
    // The first line's glyphs follow FROM's x, or x 0 when FROM is a
    // newline (the newline takes no place on display).
    let mut adjust: isize = if iterator_at == start {
        let base = if start_on_newline {
            iterator_x(0)
        } else {
            start_x
        };
        base as isize - iterator_x(layout.position_cell(start).1) as isize
    } else {
        0
    };
    let mut x = 0usize;
    let mut lines = 0usize;
    // move_it_vertically leaves the iterator on its line's first glyph,
    // the line's height already counted.
    let landed_on_glyph = vertical_offset != 0 && start < zv;
    let final_height;
    loop {
        let row_start = layout.row_starts[row.min(layout.row_starts.len() - 1)];
        // A truncated line is one screen line; the iterator reseats past
        // its hidden rest.
        let next_row = if truncate {
            None
        } else {
            layout.row_starts.get(row + 1).copied()
        };
        let end_on_row = next_row.map_or(end <= eol, |next| end < next);
        let x_of = |layout: &crate::tty::ScreenLineLayout, pos: usize, adjust: isize| {
            (iterator_x(layout.position_cell(pos).1) as isize + adjust).max(0) as usize
        };
        if carried && lines == 0 {
            // Every glyph is past the edge: the line is cut at once.
            x = x.max(last_visible_x);
            if end_on_row || end < iterator_at {
                // The retry's it.current_x - it.first_visible_x >=
                // it.last_visible_x holds at the cut only unhscrolled.
                final_height = 1 + usize::from(end < iterator_at && hscroll == 0);
                break;
            }
            lines += 1;
            if eol >= zv {
                final_height = 0;
                break;
            }
            (layout, eol) = layout_with(eol + 1, width);
            row = 0;
            iterator_at = layout.row_starts[0];
            continue;
        }
        // MOVE_TO_Y: the line holding Y-LIMIT ends the walk where the
        // iterator first reaches x 0 on it (all of it under X-LIMIT).
        if lines == max_y && !(end_on_row && end == iterator_at) {
            if max_x.is_none() {
                x = x.max(x_of(&layout, iterator_at, adjust));
                final_height = 1;
                break;
            }
            if !end_on_row {
                let reached = if next_row.is_some() || truncate {
                    last_visible_x.min(x_of(&layout, eol, adjust))
                } else {
                    x_of(&layout, eol, adjust)
                };
                x = x.max(reached);
                final_height = 1;
                break;
            }
        }
        if end_on_row {
            let at_end = x_of(&layout, end, adjust);
            let hidden = truncate && at_end >= last_visible_x;
            x = x.max(if hidden { last_visible_x } else { at_end });
            // TO in a truncated line's hidden rest: move_it_to reseats past
            // it and the retry before TO counts the line once more, except
            // at a ZV that ends no newline.
            final_height = usize::from(row_start < end || (lines == 0 && landed_on_glyph))
                // GNU's test, it.current_x - it.first_visible_x >=
                // it.last_visible_x, holds at the cut only unhscrolled.
                + usize::from(hidden && end < zv && hscroll == 0);
            break;
        }
        let row_x = if next_row.is_some() {
            last_visible_x
        } else if truncate {
            x_of(&layout, eol, adjust).min(last_visible_x)
        } else {
            x_of(&layout, eol, adjust)
        };
        x = x.max(row_x);
        lines += 1;
        adjust = 0;
        if let Some(next) = next_row {
            row += 1;
            iterator_at = next;
        } else {
            if eol >= zv {
                final_height = 0;
                break;
            }
            (layout, eol) = layout_with(eol + 1, width);
            row = 0;
            iterator_at = layout.row_starts[0];
        }
    }
    if let Some(limit) = max_x {
        x = x.min(limit);
    }
    // Text spanning more than one line starts its later lines at x 0.
    let start_x = if lines > 0 { 0 } else { start_x };
    let y = if ignore_line_at_end.is_nil() {
        lines + final_height
    } else {
        lines
    }
    .min(max_y);
    let x = x as i64 - start_x as i64;
    Ok(if vertical_offset == 0 {
        Value::cons(Value::Integer(x), Value::Integer(y as i64))
    } else {
        Value::list([
            Value::Integer(x),
            Value::Integer(y as i64),
            Value::Integer(start as i64),
        ])
    })
}

/// Buffer positions where the screen lines of the display line [BOL,
/// EOL) begin: WINDOW's terminal layout in an interactive session,
/// vmotion's batch model otherwise.
pub(crate) fn screen_row_starts(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    bol: usize,
    eol: usize,
) -> Vec<usize> {
    if interactive_motion(interp, env) {
        interactive_screen_line(interp, env, window_id, bol).row_starts
    } else {
        visual_segment_starts(interp, env, bol, eol)
    }
}

/// The screen line among a display line's row STARTS where an iterator
/// at buffer position POS is: the first beginning at POS (a display
/// string spanning screen lines begins several rows at its position, and
/// an iterator delivering the string from its start is on the first),
/// else the last beginning before POS.
pub(crate) fn row_index_of(starts: &[usize], pos: usize) -> usize {
    starts
        .iter()
        .position(|&start| start == pos)
        .unwrap_or_else(|| starts.iter().rposition(|&start| start < pos).unwrap_or(0))
}

pub(crate) fn visual_segment_starts(
    interp: &mut Interpreter,
    env: &mut Env,
    bol: usize,
    eol: usize,
) -> Vec<usize> {
    let mut starts = vec![bol];
    if interp
        .lookup_var_key(cached_symbol!("truncate-lines"), env)
        .is_some_and(|value| value.is_truthy())
    {
        return starts;
    }
    // GNU's batch display wraps mid-word at frame-width minus the
    // continuation column, even under `word-wrap' (vmotion in indent.c).
    // A line-number column narrows the text area on every row.
    let frame_width = interp
        .frame_width()
        .max(2)
        .saturating_sub(motion_line_number_columns(interp, env) as i64)
        .max(2) as usize;
    let reserve = 1;
    let line_prefix_width = {
        let prop = interp.buffer.borrow().text_property_at(bol, "line-prefix");
        prefix_property_width(interp, env, prop)
    };
    let wrap_prefix_width = {
        let prop = interp.buffer.borrow().text_property_at(bol, "wrap-prefix");
        prefix_property_width(interp, env, prop)
    };
    let widths = visual_char_widths(interp, env, bol, eol);
    let mut seg_start = bol;
    let mut first = true;
    loop {
        let usable = frame_width
            .saturating_sub(reserve)
            .saturating_sub(if first {
                line_prefix_width
            } else {
                wrap_prefix_width
            })
            .max(1);
        let mut pos = seg_start;
        let mut col = 0usize;
        let mut wrap_at: Option<usize> = None;
        while pos < eol {
            let (raw, _) = widths[pos - bol];
            let width = if raw == usize::MAX {
                8 - (col % 8)
            } else {
                raw
            };
            if col + width > usable && pos > seg_start {
                wrap_at = Some(pos);
                break;
            }
            col += width;
            pos += 1;
        }
        let Some(next_start) = wrap_at else { break };
        if next_start <= seg_start {
            break;
        }
        starts.push(next_start);
        seg_start = next_start;
        first = false;
    }
    starts
}

pub(crate) fn visual_line_bounds(interp: &Interpreter, pos: usize) -> (usize, usize) {
    let point_min = interp.buffer.borrow().point_min();
    let point_max = interp.buffer.borrow().point_max();
    let raw_bol = |mut from: usize| {
        while from > point_min && interp.buffer.borrow().char_at(from - 1) != Some('\n') {
            from -= 1;
        }
        from
    };
    let raw_eol = |mut from: usize| {
        while from < point_max && interp.buffer.borrow().char_at(from) != Some('\n') {
            from += 1;
        }
        from
    };
    let mut bol = raw_bol(pos.max(point_min));
    let mut eol = raw_eol(pos.max(point_min));
    // GNU's vertical-motion walks display lines through the iterator:
    // a newline hidden by the `invisible' property does not end the
    // line, so a display line spans every raw line an invisible run
    // joins (org's folded subtrees collapse onto their headline).
    let spec = crate::lisp::primitives::resolve_buffer_invisibility(
        interp,
        &interp.buffer.borrow(),
        interp.current_buffer_id(),
    );
    if spec.active {
        while bol > point_min
            && crate::lisp::primitives::invisible_class_at(&interp.buffer.borrow(), &spec, bol - 1)
                != 0
        {
            bol = raw_bol(bol - 1);
        }
        while eol < point_max {
            let Some((run_end, _)) =
                crate::lisp::primitives::invisible_run_at(&interp.buffer.borrow(), &spec, eol)
            else {
                break;
            };
            eol = raw_eol(run_end);
        }
    }
    (bol, eol)
}

fn live_motion_window(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    let window = if value.is_nil() {
        interp.selected_window_value()
    } else {
        *value
    };
    let Some(window_id) = window_record_id_from_value(interp, &window) else {
        return Err(wrong_type_argument("window-live-p", window));
    };
    let kind = crate::lisp::primitives::WindowKind::of_slot(
        interp
            .find_record(window_id)
            .and_then(|record| record.slots.get(WINDOW_KIND_SLOT)),
    );
    if !matches!(
        kind,
        crate::lisp::primitives::WindowKind::Leaf | crate::lisp::primitives::WindowKind::Minibuffer
    ) || window_buffer_id(interp, &window).is_none()
    {
        return Err(wrong_type_argument("window-live-p", window));
    }
    Ok(window)
}

/// CHECK_CONS of VALUE and CHECK_FIXNUM of its car and cdr.
fn motion_pair(value: &Value) -> Result<(i64, i64), LispError> {
    let (horizontal, vertical) = value
        .cons_values()
        .ok_or_else(|| wrong_type_argument("consp", *value))?;
    Ok((horizontal.as_fixnum()?, vertical.as_fixnum()?))
}

/// POSITION within BEGV..ZV, else `(args-out-of-range POSITION BEGV ZV)'.
fn checked_motion_position(interp: &Interpreter, position: i64) -> Result<usize, LispError> {
    let (begv, zv) = {
        let buffer = interp.buffer.borrow();
        (buffer.point_min(), buffer.point_max())
    };
    if position < begv as i64 || position > zv as i64 {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            Value::Integer(position),
            Value::Integer(begv as i64),
            Value::Integer(zv as i64),
        ])));
    }
    Ok(position as usize)
}

fn display_motion_width(
    interp: &Interpreter,
    env: &Env,
    position: usize,
    character: char,
    hpos: i64,
    hscroll: i64,
    tab_offset: i64,
) -> i64 {
    // Redisplay's motion reads a property whose lookup signals as nil.
    if char_is_invisible(interp, position, env).unwrap_or(false) {
        return 0;
    }
    if let Some(display) = interp
        .buffer
        .borrow()
        .text_property_at(position, "display")
        .filter(|value| !value.is_nil())
    {
        let begins_run = position == interp.buffer.borrow().point_min()
            || interp
                .buffer
                .borrow()
                .text_property_at(position - 1, "display")
                != Some(display);
        if !begins_run {
            return 0;
        }
        return string_like(&display)
            .map(|string| {
                string
                    .text
                    .chars()
                    .map(|character| character.width().unwrap_or(0) as i64)
                    .sum()
            })
            .unwrap_or(0);
    }
    if character == '\t' {
        let tab_width = interp
            .lookup_var_key(cached_symbol!("tab-width"), env)
            .and_then(|value| value.integer_value())
            .unwrap_or(8)
            .max(1);
        let origin = hpos + tab_offset + hscroll - i64::from(hscroll > 0);
        return tab_width - origin.rem_euclid(tab_width);
    }
    if character.is_control() {
        return if interp
            .lookup_var_key(cached_symbol!("ctl-arrow"), env)
            .is_some_and(|value| value.is_truthy())
        {
            2
        } else {
            4
        };
    }
    character.width().unwrap_or(0) as i64
}

/// indent.c Fcompute_motion: its arguments checked in GNU's order, then
/// compute_motion from FROM at FROMPOS to TO or TOPOS (by default the
/// window's text area: its height in lines and its width less the
/// continuation glyph's column) in WINDOW, which also chooses the display
/// table and the overlays that count.
fn compute_motion_value(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
    let from = crate::lisp::primitives::textprop::fixnum_coerce_marker(interp, &args[0])?;
    let (fromhpos, fromvpos) = motion_pair(&args[1])?;
    let to = crate::lisp::primitives::textprop::fixnum_coerce_marker(interp, &args[2])?;
    let topos = if args[3].is_nil() {
        None
    } else {
        Some(motion_pair(&args[3])?)
    };
    let width = if args[4].is_nil() {
        -1
    } else {
        args[4].as_fixnum()?
    };
    let (hscroll, tab_offset) = if args[5].is_nil() {
        (0, 0)
    } else {
        let (hscroll, tab_offset) = motion_pair(&args[5])?;
        if hscroll < 0 || !(0..=i64::from(i32::MAX)).contains(&tab_offset) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                Value::Integer(hscroll),
                Value::Integer(tab_offset),
            ])));
        }
        (hscroll, tab_offset)
    };
    let window = live_motion_window(interp, &args[6])?;
    let from = checked_motion_position(interp, from)?;
    let to = checked_motion_position(interp, to)?;
    let window_id =
        window_record_id_from_value(interp, &window).unwrap_or_else(|| interp.selected_window_id());
    let contents = crate::lisp::primitives::window_buffer_id(interp, &window)
        .unwrap_or_else(|| interp.current_buffer_id());
    let (tohpos, tovpos) = match topos {
        Some(topos) => topos,
        None => {
            let body_width = super::call(
                interp,
                "window-body-width",
                std::slice::from_ref(&window),
                env,
            )?
            .as_integer()?;
            let body_height = super::call(
                interp,
                "window-body-height",
                std::slice::from_ref(&window),
                env,
            )?
            .as_integer()?;
            (body_width - 1, body_height)
        }
    };
    let pos = crate::lisp::primitives::indent_motion::compute_motion(
        interp,
        env,
        from,
        fromvpos,
        fromhpos,
        false,
        to,
        tovpos,
        tohpos,
        width,
        hscroll,
        tab_offset,
        crate::lisp::primitives::indent_motion::MotionWindow {
            id: window_id,
            contents,
        },
    );
    Ok(Value::list([
        Value::Integer(pos.bufpos as i64),
        Value::Integer(pos.hpos),
        Value::Integer(pos.vpos),
        Value::Integer(pos.prevhpos),
        if pos.contin { Value::T } else { Value::Nil },
    ]))
}

pub(super) struct DisplayMotion {
    pub(super) position: usize,
    pub(super) hpos: i64,
    pub(super) vpos: i64,
}

// dispnew.c:buffer_posn_from_coords stops on the glyph containing the
// requested coordinate. Share the ordinary display-motion traversal,
// rather than fabricating a nil buffer position for every coordinate.
pub(super) fn display_motion(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
    stop_on_glyph: bool,
) -> Result<DisplayMotion, LispError> {
    // indent.c:Fcompute_motion checks its arguments in this order, then
    // decodes WINDOW, then ranges the positions it coerced.
    let from = crate::lisp::primitives::textprop::fixnum_coerce_marker(interp, &args[0])?;
    let (mut hpos, mut vpos) = motion_pair(&args[1])?;
    let to = crate::lisp::primitives::textprop::fixnum_coerce_marker(interp, &args[2])?;
    let topos = if args[3].is_nil() {
        None
    } else {
        Some(motion_pair(&args[3])?)
    };
    let width = if args[4].is_nil() {
        None
    } else {
        Some(args[4].as_fixnum()?)
    };
    let (hscroll, mut tab_offset) = if args[5].is_nil() {
        (0, 0)
    } else {
        let (hscroll, tab_offset) = motion_pair(&args[5])?;
        if hscroll < 0 || !(0..=i64::from(i32::MAX)).contains(&tab_offset) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                Value::Integer(hscroll),
                Value::Integer(tab_offset),
            ])));
        }
        (hscroll, tab_offset)
    };
    let window = live_motion_window(interp, &args[6])?;
    let from = checked_motion_position(interp, from)?;
    let to = checked_motion_position(interp, to)?;

    let body_width = super::call(
        interp,
        "window-body-width",
        std::slice::from_ref(&window),
        env,
    )?
    .as_integer()?;
    let body_height = super::call(
        interp,
        "window-body-height",
        std::slice::from_ref(&window),
        env,
    )?
    .as_integer()?;
    let (target_hpos, target_vpos) = topos.unwrap_or(((body_width - 1).max(0), body_height));
    let mut width = width.unwrap_or(body_width - 1);
    if width < 0 {
        width = body_width - 1;
    }
    // A zero-width window has no useful headless rendering model; GNU's
    // redisplay engine still guarantees forward progress through one
    // canonical display cell.
    width = width.max(1);
    let truncates_partial_window = match interp
        .lookup_var_key(cached_symbol!("truncate-partial-width-windows"), env)
        .map(|v| v.kind())
    {
        Some(Kind::Integer(threshold)) => width + 1 < interp.frame_width() && width + 1 < threshold,
        Some(value) => value.value().is_truthy() && width + 1 < interp.frame_width(),
        None => false,
    };
    let truncates = hscroll > 0
        || truncates_partial_window
        || interp
            .lookup_var_key(cached_symbol!("truncate-lines"), env)
            .is_some_and(|value| value.is_truthy());
    let left_margin = if hscroll > 0 { 1 - hscroll } else { 0 };

    let mut position = from;
    loop {
        let next_character = interp.buffer.borrow().char_at(position);

        // A position at the right edge belongs to the beginning of the
        // continuation line whenever more text follows on the logical line.
        if hpos >= width && next_character.is_some_and(|character| character != '\n') {
            if truncates {
                while position < to
                    && interp
                        .buffer
                        .borrow()
                        .char_at(position)
                        .is_some_and(|character| character != '\n')
                {
                    position += 1;
                }
                hpos = width;
            } else {
                vpos += 1;
                hpos = left_margin;
                tab_offset += width;
            }
        }

        if vpos > target_vpos || (vpos == target_vpos && hpos >= target_hpos) {
            break;
        }
        if position >= to || position >= interp.buffer.borrow().point_max() {
            break;
        }
        let Some(character) = interp.buffer.borrow().char_at(position) else {
            break;
        };
        if character == '\n' {
            if stop_on_glyph && vpos == target_vpos {
                break;
            }
            position += 1;
            vpos += 1;
            hpos = left_margin;
            tab_offset = 0;
            continue;
        }

        let character_width =
            display_motion_width(interp, env, position, character, hpos, hscroll, tab_offset);
        if stop_on_glyph && vpos == target_vpos && hpos + character_width > target_hpos {
            // Each expanded TAB cell has the tab's buffer position.
            if character == '\t' {
                hpos = target_hpos;
            }
            break;
        }
        if !truncates
            && character != '\t'
            && character_width > 1
            && hpos > left_margin
            && hpos + character_width > width
        {
            vpos += 1;
            hpos = left_margin;
            tab_offset += width;
        }

        hpos += character_width;
        position += 1;
        if hpos > width {
            if truncates {
                while position < to
                    && interp
                        .buffer
                        .borrow()
                        .char_at(position)
                        .is_some_and(|character| character != '\n')
                {
                    position += 1;
                }
                hpos = width;
            } else {
                hpos -= width;
                vpos += 1;
                tab_offset += width;
            }
        }
    }

    Ok(DisplayMotion {
        position,
        hpos,
        vpos,
    })
}

fn line_number_display_width_value(
    interp: &mut Interpreter,
    env: &mut Env,
    pixelwise: Option<&Value>,
) -> Result<Value, LispError> {
    let none = || {
        if matches!(pixelwise.map(|v| v.kind()), Some(Kind::Symbol(name)) if name.is_named("columns"))
        {
            Value::float(0.0)
        } else {
            Value::Integer(0)
        }
    };
    // indent.c:line_number_display_width tests Vdisplay_line_numbers in
    // the current buffer, before it makes the window's buffer current.
    if interp
        .lookup_var_key(cached_symbol!("display-line-numbers"), env)
        .unwrap_or(Value::Nil)
        .is_nil()
    {
        return Ok(none());
    }
    // SET_TEXT_POS_FROM_MARKER (startpos, w->start), whose marker_position
    // signals for a deleted selected window's start, before the window's
    // buffer is made current.
    let window_start = interp.selected_window_start_checked()?;
    let body_height = super::call(interp, "window-body-height", &[], env)?
        .as_integer()?
        .max(1) as usize;
    // set_buffer_internal_1 (XBUFFER (w->contents)): a live window's
    // buffer, a deleted one having signaled for its start.
    let saved_buffer = interp.current_buffer_id();
    let selected_buffer = interp
        .selected_window_contents_buffer_id()
        .unwrap_or(saved_buffer);
    interp.set_current_buffer_id(selected_buffer)?;
    let result = (|| -> Result<Value, LispError> {
        // The iterator lays out line numbers by the window's buffer's
        // value: none while it is nil there.
        let mode = interp
            .lookup_var_key(cached_symbol!("display-line-numbers"), env)
            .unwrap_or(Value::Nil);
        if mode.is_nil() {
            return Ok(none());
        }

        let start = window_start.clamp(
            interp.buffer.borrow().point_min(),
            interp.buffer.borrow().point_max(),
        );
        let start_line = interp.buffer.borrow().line_number_at_pos(start);
        let last_line = interp
            .buffer
            .borrow()
            .line_number_at_pos(interp.buffer.borrow().point_max());
        let visible_end_line = start_line
            .saturating_add(body_height.saturating_sub(1))
            .min(last_line);
        let displayed_maximum = if matches!(
            mode.kind(),
            Kind::Symbol(ref name) if matches!(name.as_str(), "relative" | "visual")
        ) {
            let point_line = interp
                .buffer
                .borrow()
                .line_number_at_pos(interp.buffer.borrow().point());
            point_line
                .abs_diff(start_line)
                .max(point_line.abs_diff(visible_end_line))
        } else {
            visible_end_line
        };
        let computed_width = displayed_maximum.max(1).to_string().len().max(2) as i64;
        let requested_width = interp
            .lookup_var_key(cached_symbol!("display-line-numbers-width"), env)
            .and_then(|value| value.integer_value())
            .unwrap_or(0)
            .max(0);
        let columns = computed_width.max(requested_width);
        // The line-number face contributes two canonical columns of
        // left/right padding in GNU's headless terminal display.
        let pixels = columns + 2;
        Ok(match pixelwise.map(|v| v.kind()) {
            Some(Kind::Symbol(name)) if name.is_named("columns") => Value::float(pixels as f64),
            Some(value) if value.value().is_truthy() => Value::Integer(pixels),
            _ => Value::Integer(columns),
        })
    })();
    let restore = interp.set_current_buffer_id(saved_buffer);
    match (result, restore) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(value), Ok(())) => Ok(value),
    }
}

/// GNU `vertical-motion': move by screen lines, honoring continuation.
/// indent.c window_column_x: COL * FRAME_COLUMN_WIDTH + 0.5 as an int
/// pixel count (a terminal's column is one), out of range signaling
/// against WINDOW and COLUMN.
fn window_column_x(interp: &Interpreter, window: Value, column: Value) -> Result<i64, LispError> {
    let col = match column.kind() {
        Kind::Float(float) => *float,
        Kind::Integer(column) => column as f64,
        _ => integer_like_bigint(interp, &column)?
            .to_string()
            .parse::<f64>()
            .unwrap_or(f64::INFINITY),
    };
    let x = col + 0.5;
    if !(f64::from(i32::MIN) <= x && x <= f64::from(i32::MAX)) {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            window,
            column,
        ])));
    }
    Ok(x as i64)
}

/// Fvertical_motion by N lines in WINDOW to the goal x GOAL_COL (a
/// window_column_x), the iterator at point's row start.
pub(crate) fn visual_vertical_motion(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    n: i64,
    goal_col: Option<i64>,
) -> Result<i64, LispError> {
    visual_vertical_motion_from(interp, env, window_id, n, goal_col, None)
}

/// visual_vertical_motion with Fvertical_motion's CUR-COL as START_X:
/// the iterator then starts at point with that x rather than at its
/// line's start, and with N 0 stays there when START_X is at most the
/// goal's x (-1 without a goal), the goal move counting from it.
pub(crate) fn visual_vertical_motion_from(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    n: i64,
    goal_col: Option<i64>,
    start_x: Option<i64>,
) -> Result<i64, LispError> {
    if interactive_motion(interp, env) {
        // The display iterator's walk, then move_it_in_display_line to
        // the goal X on the screen line it reached (line-move-visual's
        // temporary-goal-column contract), made again while the
        // `fontification-functions' run for what they fetched may have
        // changed it.
        let mut passes = 0;
        let mut fetches = crate::tty::IteratorFetches::off();
        loop {
            fetches.begin_pass(interp, env, window_id);
            // to_x: the goal's x plus the line numbers' width.
            let to_x = goal_col.map(|goal| goal + motion_line_number_columns(interp, env) as i64);
            // nlines 0 skips move_it_by_lines when the given start x is
            // at most to_x (-1 without a goal).
            let stays = n == 0 && start_x.is_some_and(|start| start <= to_x.unwrap_or(-1));
            let (landing, nlines) = if stays {
                let point = interp.buffer.borrow().point();
                (
                    crate::lisp::primitives::move_it::Landing {
                        pos: point,
                        moved: 0,
                        row: None,
                    },
                    0,
                )
            } else {
                crate::lisp::primitives::move_it::vertical_motion(
                    interp,
                    env,
                    window_id,
                    n,
                    &mut fetches,
                )
            };
            let (mut target, mut moved) = (landing.pos, landing.moved);
            if let Some(goal) = to_x {
                // move_it_in_display_line to first_x + to_x from where the
                // iterator is on its row; then, moving down, while it
                // stopped in an overlay string (not a `display' string)
                // with a newline still ahead, a line down and to the goal
                // again, the lines counted.
                // init_iterator's first_visible_x, the min_hscroll when
                // `auto-hscroll-mode' hscrolls only the current line,
                // which display_line alone applies.
                let geometry = crate::tty::WindowTextGeometry {
                    lnum_cols: motion_line_number_columns(interp, env),
                    point_row_hscroll: None,
                    ..window_text_geometry(interp, env, window_id)
                };
                let point = interp.buffer.borrow().point();
                let mut row = landing.row;
                // An iterator left at point with CUR-COL's x moves the
                // goal's distance from it.
                let mut from_x = start_x.filter(|_| stays);
                loop {
                    let (bol, _) = visual_line_bounds(interp, target);
                    let layout = interactive_screen_line(interp, env, window_id, bol);
                    let index = row
                        .filter(|&row| row < layout.row_starts.len())
                        .unwrap_or_else(|| row_index_of(&layout.row_starts, target));
                    let row_start = target;
                    // From CUR-COL's x at point, the goal's distance from
                    // there on point's row.
                    let from_point = from_x.take();
                    let goal_here = match from_point {
                        Some(start) => {
                            let (_, column) = layout.position_cell(target);
                            geometry.glyph_x(column) as i64 - geometry.first_visible as i64 + goal
                                - start
                        }
                        None => goal,
                    };
                    // An iterator part-way along the row (after a display
                    // string there) already past the goal stays.
                    let past = from_point.is_none()
                        && layout.row_starts.get(index) != Some(&target)
                        && match layout.position_cell(target) {
                            (cell_row, column) if cell_row == index => {
                                (geometry.lnum_cols + column) as i64
                                    > goal + geometry.first_visible as i64
                            }
                            _ => false,
                        };
                    let stop = (!past).then(|| {
                        let at =
                            crate::tty::row_x_stop(&layout, index, goal_here, geometry, point).0;
                        // move_it_in_display_line: under `word-wrap', a
                        // move that went past a continued row's glyphs
                        // (MOVE_LINE_CONTINUED) moves again to one short
                        // of the x it reached there, the row's last glyph.
                        match at.continued_at {
                            Some(x) if layout.word_wrap() => {
                                let prev_x =
                                    x.saturating_sub(1) as i64 - geometry.first_visible as i64;
                                crate::tty::row_x_stop(&layout, index, prev_x, geometry, point).0
                            }
                            _ => at,
                        }
                    });
                    if let Some(stop) = stop {
                        target = stop.position;
                    }
                    if let Some(geometry) = fetches.geometry() {
                        crate::tty::IteratorWalk::at(interp, window_id, geometry, row_start)
                            .walk_to(&mut fetches, row_start, target);
                    }
                    let in_string_before_newline = stop.is_some_and(|stop| {
                        !stop.display
                            && stop.object.is_some_and(|(string, index)| {
                                crate::lisp::primitives::string_text(&string).is_ok_and(|text| {
                                    text.chars().skip(index).any(|character| character == '\n')
                                })
                            })
                    });
                    if nlines < 0 || !in_string_before_newline {
                        break;
                    }
                    let down = crate::lisp::primitives::move_it::move_it_by_lines_from(
                        interp,
                        env,
                        window_id,
                        row_start,
                        Some(index),
                        1,
                        &mut fetches,
                    );
                    if down.moved == 0 {
                        break;
                    }
                    moved += down.moved;
                    (target, row) = (down.pos, down.row);
                }
            }
            passes += 1;
            if passes > crate::tty::ITERATOR_FONTIFICATION_PASSES
                || !fetches.fontify(interp, env, window_id)
            {
                interp.buffer.borrow_mut().goto_char(target);
                return Ok(moved);
            }
        }
    }
    // Noninteractively, indent.c vmotion from point, with the current
    // buffer shown in WINDOW meanwhile (Fvertical_motion's
    // restore_window_buffer leaves nothing else changed); a cons LINES's
    // goal column has no part in it.
    let _ = (goal_col, start_x);
    let point = interp.buffer.borrow().point();
    let pos = crate::lisp::primitives::indent_motion::vmotion(
        interp,
        env,
        point,
        n,
        crate::lisp::primitives::indent_motion::MotionWindow {
            id: window_id,
            contents: interp.current_buffer_id(),
        },
    );
    interp.buffer.borrow_mut().goto_char(pos.bufpos);
    Ok(pos.vpos)
}

/// insdel.c:insert_string: TEXT at point as Finsert inserts a string:
/// prepare_to_modify_buffer's read-only checks and change hooks, the
/// undo record and signal_after_change.
pub(crate) fn insert_string(
    interp: &mut Interpreter,
    text: &str,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    direct_insert(interp, &[Value::string(text)], env).map(drop)
}

/// The `goto-char' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_goto_char(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "goto-char";
    need_args(name, args, 1)?;
    // editfns.c:Fgoto_char takes a marker or a fixnum; a bignum is
    // wrong_type_argument (Qinteger_or_marker_p) there.
    if matches!(args[0].kind(), Kind::BigInteger(_)) {
        return Err(LispError::WrongTypeArgument(
            "integer-or-marker-p".into(),
            args[0],
        ));
    }
    let pos = position_from_value(interp, &args[0])?;
    interp.buffer.borrow_mut().goto_char(pos);
    // GNU Fgoto_char returns its POSITION argument unchanged (a
    // marker stays a marker), not the clamped integer point —
    // erc-display-msg does (marker-position (goto-char MARKER)).
    Ok(args[0])
}

/// The `forward-char' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_forward_char(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    // cmds.c:move_point: CHECK_FIXNUM.
    let n = if args.is_empty() || args[0].is_nil() {
        1
    } else {
        args[0].as_fixnum()?
    };
    match interp.buffer.borrow_mut().forward_char(n as isize) {
        Ok(_) => Ok(Value::Nil),
        Err(e) => Err(e.into()),
    }
}

/// The `backward-char' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_backward_char(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    // cmds.c:move_point: CHECK_FIXNUM.
    let n = if args.is_empty() || args[0].is_nil() {
        1
    } else {
        args[0].as_fixnum()?
    };
    match interp.buffer.borrow_mut().forward_char(-(n as isize)) {
        Ok(_) => Ok(Value::Nil),
        Err(e) => Err(e.into()),
    }
}

/// The `skip-chars-forward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_skip_chars_forward(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "skip-chars-forward";
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    regexp::skip_chars_forward_impl(interp, &args[0], args.get(1), env)
}

/// The `skip-chars-backward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_skip_chars_backward(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "skip-chars-backward";
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    regexp::skip_chars_backward_impl(interp, &args[0], args.get(1), env)
}

/// The `skip-syntax-forward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_skip_syntax_forward(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "skip-syntax-forward";
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    syntax_scan::skip_syntaxes(interp, env, true, &args[0], args.get(1))
}

/// The `skip-syntax-backward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_skip_syntax_backward(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "skip-syntax-backward";
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    syntax_scan::skip_syntaxes(interp, env, false, &args[0], args.get(1))
}

/// The `beginning-of-line' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_beginning_of_line(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    // cmds.c:Fbeginning_of_line: point to Fline_beginning_position.
    let n = line_command_count(args.first())?;
    let pos = line_beginning_position(interp, n)?;
    interp.buffer.borrow_mut().goto_char(pos as usize);
    Ok(Value::Nil)
}

/// The `end-of-line' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_end_of_line(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    // cmds.c:Fend_of_line: point to Fline_end_position (no intangible
    // text moves point past it).
    let n = line_command_count(args.first())?;
    let pos = line_end_position(interp, n)?;
    interp.buffer.borrow_mut().goto_char(pos as usize);
    Ok(Value::Nil)
}

/// The line count N of `line-beginning-position' and its relatives: nil
/// 1, else CHECK_INTEGER, a bignum counting as far as BUF_BYTES_MAX lines
/// in its direction (editfns.c's bol and eol).
fn line_count_arg(n: Option<&Value>) -> Result<i64, LispError> {
    const BUF_BYTES_MAX: i64 = (1_i64 << 61) - 2;
    match n.map(|value| value.kind()) {
        None | Some(Kind::Nil) => Ok(1),
        Some(Kind::BigInteger(n)) => Ok(if n.sign() == num_bigint::Sign::Minus {
            -BUF_BYTES_MAX
        } else {
            BUF_BYTES_MAX
        }),
        Some(value) => value.value().as_integer(),
    }
}

/// cmds.c's N of `beginning-of-line' and `end-of-line': nil 1, else
/// CHECK_FIXNUM.
fn line_command_count(n: Option<&Value>) -> Result<i64, LispError> {
    match n.map(|value| value.kind()) {
        None | Some(Kind::Nil) => Ok(1),
        Some(Kind::Integer(n)) => Ok(n),
        Some(_) => Err(LispError::WrongTypeArgument(
            "fixnump".into(),
            *n.expect("an argument"),
        )),
    }
}

/// editfns.c:bol: the start of the line N - 1 lines from point's.
fn bol(interp: &mut Interpreter, n: i64) -> usize {
    let saved = interp.buffer.borrow().point();
    let count = (n - 1) as isize;
    let shortage = if count != 0 {
        interp.buffer.borrow_mut().forward_line(count)
    } else {
        0
    };
    // Crossing an unterminated final line lands at ZV, which is the
    // requested next-line position even though moving to the beginning of
    // its containing line would jump backward.
    let at_unterminated_eob = count > 0
        && interp.buffer.borrow().point() == interp.buffer.borrow().point_max()
        && interp.buffer.borrow().char_before() != Some('\n');
    // If forward_line otherwise overshot, point is already at
    // point-max/point-min; preserve that shortage position.
    if !at_unterminated_eob
        && (shortage == 0
            || (count > 0 && interp.buffer.borrow().point() < interp.buffer.borrow().point_max()))
    {
        interp.buffer.borrow_mut().beginning_of_line();
    }
    let result = interp.buffer.borrow().point();
    interp.buffer.borrow_mut().goto_char(saved);
    result
}

/// editfns.c:Fline_beginning_position: bol constrained to point's field,
/// escaping its edge after line motion, only within the line.
fn line_beginning_position(interp: &mut Interpreter, n: i64) -> Result<i64, LispError> {
    let charpos = bol(interp, n) as i64;
    let point = interp.buffer.borrow().point() as i64;
    constrain_to_field(
        interp,
        &Value::Integer(charpos),
        &Value::Integer(point),
        n != 1,
        true,
        Value::Nil,
    )
}

/// editfns.c:eol: the end of the line N - 1 lines from point's.
fn eol(interp: &mut Interpreter, n: i64) -> usize {
    let saved = interp.buffer.borrow().point();
    let count = (n - 1) as isize;
    let shortage = if count != 0 {
        interp.buffer.borrow_mut().forward_line(count)
    } else {
        0
    };
    // search.c's find_before_next_newline: a backward scan that runs out
    // of newlines yields BEGV itself, not the end of the first accessible
    // line.
    let result = if count < 0 && shortage != 0 {
        interp.buffer.borrow().point_min()
    } else {
        interp.buffer.borrow_mut().end_of_line();
        interp.buffer.borrow().point()
    };
    interp.buffer.borrow_mut().goto_char(saved);
    result
}

/// editfns.c:Fline_end_position: eol constrained to point's field, only
/// within the line.
fn line_end_position(interp: &mut Interpreter, n: i64) -> Result<i64, LispError> {
    let charpos = eol(interp, n) as i64;
    let point = interp.buffer.borrow().point() as i64;
    constrain_to_field(
        interp,
        &Value::Integer(charpos),
        &Value::Integer(point),
        false,
        true,
        Value::Nil,
    )
}

/// The `forward-line' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_forward_line(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    // A count that is nil or a fixnum moves by an isize, as
    // `forward_line_bigint' does for any count in that range, without
    // the bignums it builds for the count, its bounds and the shortage.
    let step = match args.first().map(|value| value.kind()) {
        None | Some(Kind::Nil) => Some(1),
        Some(Kind::Integer(count)) => isize::try_from(count).ok(),
        Some(_) => None,
    };
    if let Some(step) = step {
        let shortage = interp.buffer.borrow_mut().forward_line(step);
        return Ok(Value::Integer(shortage as i64));
    }
    // cmds.c:Fforward_line's CHECK_INTEGER: a bignum moves as far as the
    // buffer allows; a marker or any other object signals.
    let n: BigInt = match args[0].kind() {
        Kind::Integer(count) => BigInt::from(count),
        Kind::BigInteger(count) => count.into(),
        _ => return Err(LispError::WrongTypeArgument("integerp".into(), args[0])),
    };
    Ok(normalize_bigint_value(forward_line_bigint(
        &mut interp.buffer.borrow_mut(),
        n,
    )))
}

/// The `char-after' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_char_after(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let pos = match args.first().map(|v| v.kind()) {
        None | Some(Kind::Nil) => Some(interp.buffer.borrow().point()),
        Some(Kind::Integer(position)) if position >= 0 => usize::try_from(position).ok(),
        // fix_position takes a bignum as the farthest fixnum, outside
        // any buffer.
        Some(Kind::Integer(_) | Kind::BigInteger(_)) => None,
        // marker_byte_position: a marker pointing nowhere is an error.
        Some(Kind::Marker(id)) => Some(
            interp
                .marker_position(id)
                .ok_or_else(marker_points_nowhere)?,
        ),
        Some(value) => {
            return Err(LispError::WrongTypeArgument(
                "integer-or-marker-p".into(),
                value.value(),
            ));
        }
    };
    match pos.and_then(|position| public_buffer_char_code_at(interp, position)) {
        Some(code) => Ok(Value::Integer(code)),
        None => Ok(Value::Nil),
    }
}

/// The `char-before' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_char_before(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let pos = match args.first().map(|v| v.kind()) {
        None | Some(Kind::Nil) => Some(interp.buffer.borrow().point()),
        Some(Kind::Integer(position)) if position >= 0 => usize::try_from(position).ok(),
        // fix_position takes a bignum as the farthest fixnum, outside
        // any buffer.
        Some(Kind::Integer(_) | Kind::BigInteger(_)) => None,
        // marker_byte_position: a marker pointing nowhere is an error.
        Some(Kind::Marker(id)) => Some(
            interp
                .marker_position(id)
                .ok_or_else(marker_points_nowhere)?,
        ),
        Some(value) => {
            return Err(LispError::WrongTypeArgument(
                "integer-or-marker-p".into(),
                value.value(),
            ));
        }
    };
    let Some(pos) = pos else {
        return Ok(Value::Nil);
    };
    if pos <= interp.buffer.borrow().point_min() {
        Ok(Value::Nil)
    } else {
        match public_buffer_char_code_at(interp, pos - 1) {
            Some(code) => Ok(Value::Integer(code)),
            None => Ok(Value::Nil),
        }
    }
}

/// indent.c:Findent_to: from point's column to COLUMN (MINIMUM columns
/// at least), tabs first under `indent-tabs-mode' and then spaces, each an
/// insertion of its own that inherits the surrounding properties.  The
/// column reached.
/// indent.c:current_column: the column recorded for point while neither
/// point nor the buffer's modiff has changed (nor the buffer, its
/// restriction), else point's column scanned from its line's beginning,
/// then recorded.
pub(crate) fn current_column(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
) -> Result<usize, LispError> {
    let (point, modiff) = {
        let buffer = interp.buffer.borrow();
        (buffer.point(), buffer.modified_tick())
    };
    let (column, known_point, known_modiff) = interp.last_known_column;
    if point == known_point && modiff == known_modiff {
        return Ok(column);
    }
    let bol = line_beginning_at(interp, point);
    // Avoid becoming abysmally slow for very long lines: the distance
    // from the line's start, an approximation, which is not recorded.
    if interp.buffer.borrow().long_line_optimizations_p()
        && let Some(Kind::Integer(threshold)) = interp
            .lookup_var_key(cached_symbol!("long-line-threshold"), env)
            .map(|value| value.kind())
        && (point - bol) as i64 > threshold
    {
        return Ok(point - bol);
    }
    let column = match plain_current_column(interp, env, bol, point) {
        Some(column) => column,
        None => column_at(interp, env, bol, point)?,
    };
    remember_current_column(interp, column);
    Ok(column)
}

/// indent.c current_column's own scan, back from POINT to the line's
/// start BOL, of a buffer with no intervals or overlays whose characters
/// are each one byte (Z == Z_BYTE); None for any other buffer, which
/// current_column_1 scans.
fn plain_current_column(
    interp: &Interpreter,
    env: &crate::lisp::types::Env,
    bol: usize,
    point: usize,
) -> Option<usize> {
    {
        let buffer = interp.buffer.borrow();
        if buffer.has_interval_tree()
            || !buffer.overlays.is_empty()
            || (buffer.is_multibyte() && !buffer.text_is_ascii())
        {
            return None;
        }
    }
    // SANE_TAB_WIDTH.
    let tab_width = interp
        .lookup_var_key(cached_symbol!("tab-width"), env)
        .and_then(|value| value.integer_value())
        .filter(|width| (1..=1000).contains(width))
        .unwrap_or(8) as usize;
    let ctl_arrow = interp
        .lookup_var_key(cached_symbol!("ctl-arrow"), env)
        .is_some_and(|value| value.is_truthy());
    let selective_display = interp
        .lookup_var_key(cached_symbol!("selective-display"), env)
        .is_some_and(|value| matches!(value.kind(), Kind::T));
    let display_table = crate::lisp::primitives::buffer_display_table(interp, env);
    let buffer = interp.buffer.borrow();
    let (before_gap, after_gap) = buffer.text_halves(bol, point);
    let (mut col, mut tab_seen, mut post_tab) = (0usize, false, 0usize);
    let tab = |col: usize| (col + tab_width) / tab_width * tab_width;
    // One character C, shown as itself or (FROM_VECTOR) as a display
    // vector's glyph: whether the line's start is reached.
    let mut step = |c: u32, from_vector: bool| {
        if (0o40..0o177).contains(&c) {
            col += 1;
        } else if c == u32::from(b'\n') || (c == u32::from(b'\r') && selective_display) {
            return true;
        } else if c == u32::from(b'\t') {
            if tab_seen {
                col = tab(col);
            }
            post_tab += col;
            col = 0;
            tab_seen = true;
        } else if from_vector {
            col += 1;
        } else {
            col += if ctl_arrow && c < 0o200 { 2 } else { 4 };
        }
        false
    };
    'scan: for ch in after_gap.chars().rev().chain(before_gap.chars().rev()) {
        // The byte: a unibyte buffer's raw byte is stored as its marker.
        let c = crate::lisp::primitives::raw_byte_from_regex_char(ch).unwrap_or(ch as u8);
        let charvec = display_table.and_then(|table| match table.get(u32::from(c)).kind() {
            Kind::Vector(vector) => Some(vector),
            _ => None,
        });
        match charvec {
            Some(vector) => {
                let glyphs: Vec<Value> = vector.slots().collect();
                for glyph in glyphs.iter().rev() {
                    if step(glyph_code_char(glyph), true) {
                        break 'scan;
                    }
                }
            }
            None => {
                if step(u32::from(c), false) {
                    break 'scan;
                }
            }
        }
    }
    if tab_seen {
        col = tab(col) + post_tab;
    }
    Some(col)
}

/// GLYPH_CODE_CHAR of a display vector's ENTRY when GLYPH_CODE_P, else a
/// space.
fn glyph_code_char(entry: &Value) -> u32 {
    match entry.kind() {
        Kind::Integer(code) if code >= 0 => (code & 0x3F_FFFF) as u32,
        Kind::Cons(_) => entry
            .car()
            .ok()
            .and_then(|car| car.integer_value())
            .map_or(u32::from(b' '), |code| code as u32),
        _ => u32::from(b' '),
    }
}

/// last_known_column, last_known_column_point and
/// last_known_column_modified set for point as it now is.
fn remember_current_column(interp: &mut Interpreter, column: usize) {
    let buffer = interp.buffer.borrow();
    let known = (column, buffer.point(), buffer.modified_tick());
    drop(buffer);
    interp.last_known_column = known;
}

pub(super) fn indent_to(
    interp: &mut Interpreter,
    column: i64,
    minimum: i64,
    env: &mut crate::lisp::types::Env,
) -> Result<i64, LispError> {
    let mut from_col = current_column(interp, env)? as i64;
    let min_col = (from_col + minimum).max(column);
    if min_col == from_col {
        return Ok(min_col);
    }
    // SANE_TAB_WIDTH.
    let tab_width = interp
        .lookup_var_key(cached_symbol!("tab-width"), env)
        .and_then(|value| value.integer_value())
        .filter(|width| (1..=1000).contains(width))
        .unwrap_or(8);
    if interp
        .lookup_var_key(cached_symbol!("indent-tabs-mode"), env)
        .is_some_and(|value| value.is_truthy())
    {
        let tabs = min_col / tab_width - from_col / tab_width;
        if tabs > 0 {
            insert_text_with_hooks(
                interp,
                &"\t".repeat(tabs as usize),
                None,
                &[],
                true,
                false,
                env,
            )?;
            from_col = (min_col / tab_width) * tab_width;
        }
    }
    let spaces = (min_col - from_col).max(0) as usize;
    insert_text_with_hooks(interp, &" ".repeat(spaces), None, &[], true, false, env)?;
    remember_current_column(interp, min_col as usize);
    Ok(min_col)
}

/// The `delete-region' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_delete_region(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "delete-region";
    need_args(name, args, 2)?;
    let (from, to) = validate_region(interp, &args[0], &args[1])?;
    delete_region_with_hooks(interp, from, to, env)?;
    Ok(Value::Nil)
}

/// The `search-forward' family under NAME: one body, each name a subr's
/// function pointer below.
fn search_named(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 4 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    let direction = if name == "search-forward" { 1 } else { -1 };
    crate::lisp::primitives::search::search_command(interp, env, args, direction, false, false)
}

/// The `search-forward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_search_forward(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    search_named(interp, "search-forward", args, env)
}

/// The `search-backward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_search_backward(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    search_named(interp, "search-backward", args, env)
}

/// The `buffer-substring' family under NAME: one body, each name a subr's
/// function pointer below.
fn buffer_substring_named(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_args(name, args, 2)?;
    let (start, end) = validate_region(interp, &args[0], &args[1])?;
    if name == "buffer-substring" {
        update_buffer_properties(interp, start, end, env)?;
    }
    // make_buffer_string_both copies the text from where it stands: a span
    // one side of the gap holds, with no character beyond Unicode, goes
    // into the string with no copy between.
    {
        let buffer = interp.buffer.borrow();
        if !buffer.has_extended_chars_in(start, end)
            && let Some(text) = buffer.contiguous_text(start, end)
        {
            let props = if name == "buffer-substring" {
                buffer.string_intervals(start, end)
            } else {
                None
            };
            return Ok(Value::StringObject(
                crate::lisp::types::StringObjectRef::from_text_ref(
                    &text,
                    crate::lisp::types::string_data::StringProperties::new(props),
                    buffer.is_multibyte(),
                    &[],
                ),
            ));
        }
    }
    match interp.buffer.borrow().buffer_substring(start, end) {
        Ok(s) => {
            if name == "buffer-substring" {
                Ok(string_like_value_with_extended_chars(
                    s,
                    interp.buffer.borrow().string_intervals(start, end),
                    interp.buffer.borrow().is_multibyte(),
                    interp.buffer.borrow().substring_extended_chars(start, end),
                ))
            } else {
                Ok(string_like_value_with_extended_chars(
                    s,
                    None,
                    interp.buffer.borrow().is_multibyte(),
                    interp.buffer.borrow().substring_extended_chars(start, end),
                ))
            }
        }
        Err(e) => Err(LispError::Signal(e.to_string())),
    }
}

/// The `buffer-substring' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_buffer_substring(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    buffer_substring_named(interp, "buffer-substring", args, env)
}

/// The `buffer-substring-no-properties' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_buffer_substring_no_properties(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    buffer_substring_named(interp, "buffer-substring-no-properties", args, env)
}

/// The `point' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_point(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(Value::Integer(interp.buffer.borrow().point() as i64))
}

/// The `point-min' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_point_min(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(Value::Integer(interp.buffer.borrow().point_min() as i64))
}

/// The `point-max' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_point_max(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(Value::Integer(interp.buffer.borrow().point_max() as i64))
}

/// The `re-search-forward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_re_search_forward(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    regexp::buffer_regex_search(interp, args, env, true, false)
}

/// The `re-search-backward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_re_search_backward(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    regexp::buffer_regex_search(interp, args, env, false, false)
}

/// The `insert' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_insert(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    insert_impl(interp, args, env, false, false)
}

/// The `bolp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_bolp(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(if interp.buffer.borrow().bolp() {
        Value::T
    } else {
        Value::Nil
    })
}

/// The `eolp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_eolp(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(if interp.buffer.borrow().eolp() {
        Value::T
    } else {
        Value::Nil
    })
}

/// The `bobp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_bobp(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(if interp.buffer.borrow().bobp() {
        Value::T
    } else {
        Value::Nil
    })
}

/// The `eobp' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_eobp(
    interp: &mut Interpreter,
    _args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    Ok(if interp.buffer.borrow().eobp() {
        Value::T
    } else {
        Value::Nil
    })
}
