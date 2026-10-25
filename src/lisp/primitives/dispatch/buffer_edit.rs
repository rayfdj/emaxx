use super::*;
use crate::lisp::types::{Kind, PropertyName};

// cmds.c:236/288: `self-insert-command' and `delete-char' amalgamate
// their undo with a preceding run of the same command when the count's
// magnitude is below 2 -- through simple.el's own `undo-auto-amalgamate',
// reached as an ordinary Lisp call.
/// call0 (Qundo_auto_amalgamate), once simple.el has defined it.
fn call_undo_auto_amalgamate(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    if interp.lookup_function("undo-auto-amalgamate", env).is_ok() {
        interp.call_function_value(
            Value::Symbol("undo-auto-amalgamate".into()),
            Some("undo-auto-amalgamate"),
            &[],
            env,
        )?;
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
            interp.set_variable("last-command-event", *c, env);
            *c
        }
        None => interp
            .lookup_var("last-command-event", env)
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
                .lookup_var("translation-table-for-input", env)
                .unwrap_or(Value::Nil);
            let character = translate_char(interp, &table, code as u32);
            if internal_self_insert(interp, character, n, env)? == 2 {
                interp.set_variable("undo-auto--this-command-amalgamating", Value::Nil, env);
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
    let truthy = |interp: &Interpreter, env: &Env, name: &str| {
        interp
            .lookup_var(name, env)
            .is_some_and(|value| value.is_truthy())
    };
    let mut hairy = 0;
    let overwrite = interp
        .lookup_var("overwrite-mode", env)
        .unwrap_or(Value::Nil);
    if truthy(interp, env, "before-change-functions")
        || truthy(interp, env, "after-change-functions")
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
        if overwrite.symbol_str() == Some("overwrite-mode-binary") {
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
    if truthy(interp, env, "abbrev-mode")
        && syntax_class(interp, c) != syntax::SyntaxClass::Word
        && !truthy(interp, env, "buffer-read-only")
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
        insert_text_with_hooks(interp, &text, &[], &extended_chars, true, false, env)?;
    }

    let auto_fill_character = match interp
        .lookup_var("auto-fill-chars", env)
        .map(|value| value.kind())
    {
        Some(Kind::CharTable(table)) => interp
            .char_table_get(table, c)
            .is_some_and(|value| value.is_truthy()),
        _ => c == u32::from(b' ') || c == u32::from(b'\n'),
    };
    if auto_fill_character && truthy(interp, env, "auto-fill-function") {
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

    run_named_hooks(
        interp,
        "post-self-insert-hook",
        env,
        Some(interp.current_buffer_id()),
    )?;
    Ok(hairy)
}

/// editfns.c:Fwiden: the whole buffer accessible, or as much of it as the
/// innermost labeled restriction allows.
fn widen_current_buffer(interp: &Interpreter) {
    if let Some((start, end)) =
        interp.effective_labeled_restriction(interp.current_buffer_id(), None)
    {
        interp.buffer.borrow_mut().narrow_to_region(start, end);
    } else {
        interp.buffer.borrow_mut().widen();
    }
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

fn utf8_sequence_width(first: u8) -> usize {
    match first {
        0x00..=0x7f => 1,
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => 0,
    }
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

fn multibyte_buffer_text(text: &str, preserve_utf8_sequences: bool) -> (String, Vec<usize>) {
    let bytes = unibyte_text_bytes(text);
    let mut converted = String::new();
    let mut position_map = vec![1; bytes.len() + 1];
    let mut byte_index = 0;
    let mut new_position = 1;

    while byte_index < bytes.len() {
        position_map[byte_index] = new_position;
        // buffer.c:Fset_buffer_multibyte preserves ASCII for every non-nil
        // flag. Only recognition of multi-byte sequences depends on FLAG=t.
        let width = if bytes[byte_index].is_ascii() {
            1
        } else if preserve_utf8_sequences {
            utf8_sequence_width(bytes[byte_index])
        } else {
            0
        };
        let character = (width > 0 && byte_index + width <= bytes.len())
            .then(|| std::str::from_utf8(&bytes[byte_index..byte_index + width]).ok())
            .flatten()
            .and_then(|valid| valid.chars().next());
        let consumed = if let Some(character) = character {
            converted.push(character);
            width
        } else {
            converted.push(raw_byte_regex_char(bytes[byte_index]));
            1
        };
        new_position += 1;
        position_map[byte_index + 1..=byte_index + consumed].fill(new_position);
        byte_index += consumed;
    }
    (converted, position_map)
}

fn unibyte_buffer_text(text: &str) -> (String, Vec<usize>) {
    let mut converted = String::with_capacity(text.len());
    let mut position_map = Vec::with_capacity(text.chars().count() + 1);
    position_map.push(1);
    // Each byte of a character's encoding becomes one unibyte character;
    // the map records the count after each source character.
    let mut count = 0;
    for character in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(character) {
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
                let buffer_id = interp.resolve_buffer_id(&args[0])?;
                let (begv, zv) = {
                    let source = interp.get_buffer_by_id(buffer_id).ok_or_else(|| {
                        LispError::Signal(format!("No buffer with id {}", buffer_id))
                    })?;
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
                let props = source.substring_property_spans(start, end);
                let extended_chars = source.substring_extended_chars(start, end);
                drop(source);
                insert_text_with_hooks(interp, &text, &props, &extended_chars, false, false, env)?;
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
                let (goal_col, n) = match args[0].kind() {
                    cons @ Kind::Cons(_) => {
                        let (car, cdr) = cons
                            .value()
                            .cons_values()
                            .ok_or_else(|| LispError::WrongTypeArgument("consp".into(), args[0]))?;
                        // COLS may be a float (line-move-visual divides
                        // pixels by the frame char width); GNU truncates
                        // it to a pixel count.
                        let goal = match car.kind() {
                            Kind::Float(float) => float.max(0.0) as usize,
                            other => other.value().as_integer()?.max(0) as usize,
                        };
                        (Some(goal), cdr.as_integer()?)
                    }
                    other => {
                        let big = integer_like_bigint(interp, &other.value())?;
                        (None, big.to_i64().unwrap_or(i64::MAX / 2))
                    }
                };
                // WINDOW lends only its geometry: the motion runs in the
                // current buffer (Fvertical_motion).
                let window = live_motion_window(interp, args.get(1).unwrap_or(&Value::Nil))?;
                let window_id = window_record_id_from_value(interp, &window)
                    .unwrap_or_else(|| interp.selected_window_id());
                let moved = visual_vertical_motion(interp, env, window_id, n, goal_col)?;
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
                    interp.buffer.borrow().substring_property_spans(
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
                let text = interp
                    .buffer
                    .borrow()
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let props = if name == "minibuffer-contents" {
                    interp.buffer.borrow().substring_property_spans(start, end)
                } else {
                    Vec::new()
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
                let invisible = match position_from_value(interp, &args[0]) {
                    Ok(position) if position >= 1 => char_is_invisible(interp, position, env),
                    _ => invisibility_value_is_hidden(interp, &args[0], env),
                };
                Ok(if invisible { Value::T } else { Value::Nil })
            }
            "buffer-size" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = match args.first() {
                    Some(buffer) if !buffer.is_nil() => interp.resolve_buffer_id(buffer)?,
                    _ => interp.current_buffer_id(),
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::TypeError("buffer".into(), "killed".into()))?;
                Ok(Value::Integer(buffer.size_total() as i64))
            }
            "buffer-enable-undo" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c:Fbuffer_enable_undo: BUFFER through Fget_buffer
                // (a name must name a live buffer, nsberror's message),
                // whose list becomes nil where it is t.
                let buffer = match args.first().filter(|value| !value.is_nil()) {
                    None => Some(interp.buffer),
                    Some(value) => match value.kind() {
                        Kind::Buffer(buffer) => Some(buffer),
                        _ => {
                            let Some(found) =
                                with_string_text(value, |text| interp.find_buffer_id(text))
                            else {
                                return Err(wrong_type_argument("stringp", *value));
                            };
                            let Some(id) = found else {
                                return Err(LispError::Signal(format!(
                                    "No buffer named {}",
                                    string_text(value)?
                                )));
                            };
                            interp.buffer_object(id)
                        }
                    },
                };
                if let Some(buffer) = buffer {
                    buffer.borrow_mut().enable_undo();
                }
                Ok(Value::Nil)
            }
            "gap-position" => Ok(Value::Integer(interp.buffer.borrow().point() as i64)),
            "gap-size" => Ok(Value::Integer(0)),
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
                let byte = args[0].as_integer()?;
                if byte <= 0 {
                    return Ok(Value::Nil);
                }
                Ok(byte_to_position(interp, byte as usize)
                    .map(|pos| Value::Integer(pos as i64))
                    .unwrap_or(Value::Nil))
            }

            "buffer-name" => {
                if !args.is_empty()
                    && let Kind::Buffer(buffer) = args[0].kind()
                {
                    return Ok(interp
                        .get_buffer_by_id(buffer.id)
                        .map(|buffer| Value::String(buffer.name.clone().into()))
                        .unwrap_or(Value::Nil));
                }
                Ok(Value::String(interp.buffer.borrow().name.clone().into()))
            }
            "set-buffer-multibyte" => {
                need_args(name, args, 1)?;
                let enabled = args[0].is_truthy();
                if enabled == interp.buffer.borrow().is_multibyte() {
                    return Ok(args[0]);
                }
                // A buffer shown in a window makes redisplay consider the
                // other windows.
                if interp.buffer_window_count(interp.current_buffer_id()) > 0 {
                    interp.redisplay_other_windows();
                }
                if interp.buffer.borrow().restriction()
                    != (
                        1,
                        interp.buffer.borrow().full_buffer_string().chars().count() + 1,
                    )
                {
                    return Err(LispError::Signal(
                        "Changing multibyteness in a narrowed buffer".into(),
                    ));
                }

                let original = interp.buffer.borrow().full_buffer_string();
                let saved = interp.buffer.borrow().saved_text().to_string();
                let preserve_utf8_sequences = matches!(args[0].kind(), Kind::T);
                let (converted, positions) = if enabled {
                    multibyte_buffer_text(&original, preserve_utf8_sequences)
                } else {
                    unibyte_buffer_text(&original)
                };
                let converted_saved = if enabled {
                    multibyte_buffer_text(&saved, preserve_utf8_sequences).0
                } else {
                    unibyte_buffer_text(&saved).0
                };
                // The deletions already recorded keep the strings they were
                // recorded with, multibyte as the buffer was then.
                if interp.buffer.borrow().undo_enabled() {
                    interp.buffer.borrow().undo_list_value();
                }
                interp.buffer.borrow_mut().set_multibyte_representation(
                    enabled,
                    converted,
                    converted_saved,
                    &positions,
                );
                if interp.buffer.borrow().undo_enabled() {
                    interp
                        .buffer
                        .borrow_mut()
                        .push_undo_entry(crate::buffer::UndoEntry::Opaque(Value::list([
                            Value::Symbol(cached_symbol!("apply").with(Clone::clone)),
                            Value::Symbol(
                                cached_symbol!("set-buffer-multibyte").with(Clone::clone),
                            ),
                            if enabled { Value::Nil } else { Value::T },
                        ])));
                }
                Ok(args[0])
            }
            "char-after" => direct_char_after(interp, args, env),
            "char-before" => direct_char_before(interp, args, env),
            "matching-paren" => {
                need_args(name, args, 1)?;
                let ch = args[0].as_integer()? as u32;
                let matching = match char::from_u32(ch) {
                    Some('(') => Some(')'),
                    Some(')') => Some('('),
                    Some('[') => Some(']'),
                    Some(']') => Some('['),
                    Some('{') => Some('}'),
                    Some('}') => Some('{'),
                    Some('<') => Some('>'),
                    Some('>') => Some('<'),
                    _ => None,
                };
                Ok(matching
                    .map(|ch| Value::Integer(ch as i64))
                    .unwrap_or(Value::Nil))
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
                let n = if args.is_empty() {
                    1
                } else {
                    args[0].as_integer()?
                };
                if n.abs() < 2 {
                    call_undo_auto_amalgamate(interp, env)?;
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
                        .lookup_var("region-extract-function", env)
                        .ok_or_else(|| LispError::Void("region-extract-function".into()))?;
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
            "current-column" => {
                let pt = interp.buffer.borrow().point();
                let bol = {
                    let saved = interp.buffer.borrow().point();
                    interp.buffer.borrow_mut().beginning_of_line();
                    let bol = interp.buffer.borrow().point();
                    interp.buffer.borrow_mut().goto_char(saved);
                    bol
                };
                Ok(Value::Integer(column_at(interp, env, bol, pt) as i64))
            }
            "current-indentation" => {
                let saved = interp.buffer.borrow().point();
                interp.buffer.borrow_mut().beginning_of_line();
                while matches!(
                    interp
                        .buffer
                        .borrow()
                        .char_at(interp.buffer.borrow().point()),
                    Some(' ' | '\t')
                ) {
                    let _ = interp.buffer.borrow_mut().forward_char(1);
                }
                let pt = interp.buffer.borrow().point();
                let bol = {
                    let saved = interp.buffer.borrow().point();
                    interp.buffer.borrow_mut().beginning_of_line();
                    let bol = interp.buffer.borrow().point();
                    interp.buffer.borrow_mut().goto_char(saved);
                    bol
                };
                let indentation = column_at(interp, env, bol, pt) as i64;
                interp.buffer.borrow_mut().goto_char(saved);
                Ok(Value::Integer(indentation))
            }
            "indent-to" => {
                need_arg_range(name, args, 1, 2)?;
                let target = args[0].as_integer()?;
                let minimum = match args.get(1) {
                    Some(value) if !value.is_nil() => value.as_integer()?,
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
                let scan = scan_for_column(interp, env, start, zv, Some(goal));
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
                    insert_text_with_hooks(interp, &spaces, &[], &[], true, false, env)?;
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
                        Kind::Marker(id) => interp.marker_position(id).ok_or_else(|| {
                            LispError::WrongTypeArgument("integer-or-marker-p".into(), args[0])
                        })?,
                        _ => {
                            return Err(LispError::WrongTypeArgument(
                                "integer-or-marker-p".into(),
                                args[0],
                            ));
                        }
                    }
                };
                let absolute_max = interp.buffer.borrow().size_total() + 1;
                if pos < 1 || pos > absolute_max {
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
                let line = interp
                    .buffer
                    .borrow()
                    .line_number_at_pos(pos)
                    .saturating_sub(interp.buffer.borrow().line_number_at_pos(start))
                    + 1;
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
                let mut start = position_from_value(interp, &args[0])?;
                let mut end = position_from_value(interp, &args[1])?;
                // editfns.c: the bounds, in either order, within the whole
                // buffer before any labeled restriction clamps them.
                if end < start {
                    std::mem::swap(&mut start, &mut end);
                }
                if start < 1 || end > interp.buffer.borrow().size_total() + 1 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("args-out-of-range"),
                        args[0],
                        args[1],
                    ])));
                }
                if let Some((clamp_start, clamp_end)) =
                    interp.effective_labeled_restriction(interp.current_buffer_id(), None)
                {
                    start = start.max(clamp_start);
                    end = end.min(clamp_end);
                }
                interp.buffer.borrow_mut().narrow_to_region(start, end);
                Ok(Value::Nil)
            }
            "widen" => {
                widen_current_buffer(interp);
                Ok(Value::Nil)
            }
            "buffer-modified-p" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = match args.first() {
                    Some(buffer) if !buffer.is_nil() => interp.resolve_buffer_id(buffer)?,
                    _ => interp.current_buffer_id(),
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::TypeError("buffer".into(), "killed".into()))?;
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
                let buffer_id = match args.first() {
                    Some(buffer) if !buffer.is_nil() => interp.resolve_buffer_id(buffer)?,
                    _ => interp.current_buffer_id(),
                };
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::TypeError("buffer".into(), "killed".into()))?;
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
                let buffer_id = match args.get(1) {
                    Some(buffer) if !buffer.is_nil() => interp.resolve_buffer_id(buffer)?,
                    _ => interp.current_buffer_id(),
                };
                interp
                    .get_buffer_by_id_mut(buffer_id)
                    .ok_or_else(|| LispError::TypeError("buffer".into(), "killed".into()))?
                    .set_modified_tick(tick);
                Ok(Value::Nil)
            }
            "set-buffer-modified-p" => {
                need_args(name, args, 1)?;
                let modified = !args[0].is_nil();
                let was_modified = interp.buffer.borrow().is_modified();
                let update_lock = !interp
                    .lookup_var("inhibit-modification-hooks", env)
                    .is_some_and(|value| value.is_truthy())
                    && interp.buffer.borrow().file.is_some()
                    && interp.buffer.borrow().file_truename.is_some();
                if was_modified && !modified && update_lock {
                    unlock_current_buffer(interp, env)?;
                } else if !was_modified && modified && update_lock {
                    maybe_lock_current_buffer_file(interp, env)?;
                }
                if modified {
                    interp.buffer.borrow_mut().set_modified();
                } else {
                    interp.buffer.borrow_mut().set_unmodified();
                }
                Ok(Value::Nil)
            }
            "restore-buffer-modified-p" => {
                need_args(name, args, 1)?;
                let flag = args[0];
                let modified = !flag.is_nil();
                let was_modified = interp.buffer.borrow().is_modified();
                let update_lock = !interp
                    .lookup_var("inhibit-modification-hooks", env)
                    .is_some_and(|value| value.is_truthy())
                    && interp.buffer.borrow().file.is_some()
                    && interp.buffer.borrow().file_truename.is_some();
                if was_modified && !modified && update_lock {
                    unlock_current_buffer(interp, env)?;
                } else if !was_modified && modified && update_lock {
                    maybe_lock_current_buffer_file(interp, env)?;
                }
                if flag.is_nil() {
                    interp.buffer.borrow_mut().set_unmodified();
                } else if matches!(flag.kind(), Kind::Symbol(symbol) if symbol == "autosaved") {
                    interp.buffer.borrow_mut().set_modified();
                    interp.buffer.borrow_mut().set_autosaved();
                } else {
                    interp.buffer.borrow_mut().set_modified();
                }
                Ok(flag)
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
            .lookup_var(&name, env)
            .map(|value| display_spec_width(interp, env, &value))
            .unwrap_or(0),
        Kind::Cons(_) => {
            let Ok(items) = spec.to_vec() else { return 0 };
            match items.first().map(|v| v.kind()) {
                Some(Kind::Symbol(op)) if op == "-" || op == "+" => {
                    let mut acc = items
                        .get(1)
                        .map(|item| display_spec_width(interp, env, item))
                        .unwrap_or(0);
                    for item in items.iter().skip(2) {
                        let value = display_spec_width(interp, env, item);
                        if op == "-" {
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
        && matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head == "space")
    {
        let mut cursor = 1;
        while cursor + 1 < items.len() {
            if matches!(items[cursor].kind(), Kind::Symbol(key) if key == ":width") {
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
    let buffer_id = interp.current_buffer_id();
    let enabled = interp
        .buffer_local_value(buffer_id, "display-line-numbers")
        .or_else(|| interp.lookup_var("display-line-numbers", &crate::lisp::types::Env::new()))
        .unwrap_or(Value::Nil);
    if enabled.is_nil() {
        return 0;
    }
    let text_rows = super::call(interp, "window-body-height", &[], env)
        .ok()
        .and_then(|value| value.integer_value())
        .unwrap_or(0)
        .max(1) as usize;
    let start = interp.selected_window_start().clamp(
        interp.buffer.borrow().point_min(),
        interp.buffer.borrow().point_max(),
    );
    let top_line = interp.buffer.borrow().line_number_at_pos(start);
    let point_line = interp
        .buffer
        .borrow()
        .line_number_at_pos(interp.buffer.borrow().point());
    let begv_line = interp
        .buffer
        .borrow()
        .line_number_at_pos(interp.buffer.borrow().point_min());
    crate::lisp::primitives::window_line_number_layout(
        interp, buffer_id, top_line, point_line, begv_line, text_rows,
    )
    .map_or(0, |layout| layout.cols)
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
        .lookup_var("truncate-lines", env)
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
    let kind = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
        .cloned()
        .unwrap_or(Value::Nil);
    if matches!(
        kind.kind(),
        Kind::Symbol(ref kind)
            if matches!(
                kind.as_str(),
                INTERNAL_HORIZONTAL_WINDOW_KIND
                    | INTERNAL_VERTICAL_WINDOW_KIND
                    | DELETED_WINDOW_KIND
            )
    ) || window_buffer_id(interp, &window).is_none()
    {
        return Err(wrong_type_argument("window-live-p", window));
    }
    Ok(window)
}

fn motion_pair(value: &Value) -> Result<(i64, i64), LispError> {
    let (horizontal, vertical) = value
        .cons_values()
        .ok_or_else(|| wrong_type_argument("consp", *value))?;
    Ok((horizontal.as_integer()?, vertical.as_integer()?))
}

fn checked_motion_position(interp: &Interpreter, value: &Value) -> Result<usize, LispError> {
    let position = position_from_value(interp, value)?;
    if position < interp.buffer.borrow().point_min()
        || position > interp.buffer.borrow().point_max()
    {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            *value,
            Value::Integer(interp.buffer.borrow().point_min() as i64),
            Value::Integer(interp.buffer.borrow().point_max() as i64),
        ])));
    }
    Ok(position)
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
    if char_is_invisible(interp, position, env) {
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
            .lookup_var("tab-width", env)
            .and_then(|value| value.integer_value())
            .unwrap_or(8)
            .max(1);
        let origin = hpos + tab_offset + hscroll - i64::from(hscroll > 0);
        return tab_width - origin.rem_euclid(tab_width);
    }
    if character.is_control() {
        return if interp
            .lookup_var("ctl-arrow", env)
            .is_some_and(|value| value.is_truthy())
        {
            2
        } else {
            4
        };
    }
    character.width().unwrap_or(0) as i64
}

fn compute_motion_value(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
    let motion = display_motion(interp, env, args, false)?;
    Ok(Value::list([
        Value::Integer(motion.position as i64),
        Value::Integer(motion.hpos),
        Value::Integer(motion.vpos),
        Value::Integer(motion.previous_hpos),
        if motion.continued {
            Value::T
        } else {
            Value::Nil
        },
    ]))
}

pub(super) struct DisplayMotion {
    pub(super) position: usize,
    pub(super) hpos: i64,
    pub(super) vpos: i64,
    previous_hpos: i64,
    continued: bool,
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
    let from = checked_motion_position(interp, &args[0])?;
    let (mut hpos, mut vpos) = motion_pair(&args[1])?;
    let to = checked_motion_position(interp, &args[2])?;
    let window = live_motion_window(interp, &args[6])?;

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
    let (target_hpos, target_vpos) = if args[3].is_nil() {
        ((body_width - 1).max(0), body_height)
    } else {
        motion_pair(&args[3])?
    };
    let mut width = if args[4].is_nil() {
        body_width - 1
    } else {
        args[4].as_integer()?
    };
    if width < 0 {
        width = body_width - 1;
    }
    // A zero-width window has no useful headless rendering model; GNU's
    // redisplay engine still guarantees forward progress through one
    // canonical display cell.
    width = width.max(1);

    let (hscroll, mut tab_offset) = if args[5].is_nil() {
        (0, 0)
    } else {
        let (hscroll, tab_offset) = motion_pair(&args[5])?;
        if hscroll < 0 || tab_offset < 0 {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                Value::Integer(hscroll),
                Value::Integer(tab_offset),
            ])));
        }
        (hscroll, tab_offset)
    };
    let truncates_partial_window = match interp
        .lookup_var("truncate-partial-width-windows", env)
        .map(|v| v.kind())
    {
        Some(Kind::Integer(threshold)) => width + 1 < interp.frame_width() && width + 1 < threshold,
        Some(value) => value.value().is_truthy() && width + 1 < interp.frame_width(),
        None => false,
    };
    let truncates = hscroll > 0
        || truncates_partial_window
        || interp
            .lookup_var("truncate-lines", env)
            .is_some_and(|value| value.is_truthy());
    let left_margin = if hscroll > 0 { 1 - hscroll } else { 0 };

    let mut position = from;
    let mut previous_hpos = 0;
    let mut continuation_hpos = None;
    let mut consumed_character_at_limit = false;
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
                previous_hpos = width;
                consumed_character_at_limit = false;
            } else {
                let origin = hpos;
                vpos += 1;
                hpos = left_margin;
                tab_offset += width;
                previous_hpos = 0;
                continuation_hpos = Some(origin);
                consumed_character_at_limit = false;
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
        previous_hpos = hpos;
        if character == '\n' {
            if stop_on_glyph && vpos == target_vpos {
                break;
            }
            position += 1;
            vpos += 1;
            hpos = left_margin;
            tab_offset = 0;
            continuation_hpos = None;
            consumed_character_at_limit = true;
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
            let origin = hpos;
            vpos += 1;
            hpos = left_margin;
            tab_offset += width;
            continuation_hpos = Some(origin);
        }

        previous_hpos = hpos;
        hpos += character_width;
        position += 1;
        consumed_character_at_limit = true;
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
                previous_hpos = width;
                consumed_character_at_limit = false;
            } else {
                let origin = previous_hpos;
                hpos -= width;
                vpos += 1;
                tab_offset += width;
                previous_hpos = 0;
                continuation_hpos = Some(origin);
            }
        }
    }

    if position == to && to < interp.buffer.borrow().point_max() && consumed_character_at_limit {
        previous_hpos = hpos;
        continuation_hpos = None;
    }
    let continued = continuation_hpos.is_some() && previous_hpos == 0;
    let previous_hpos = if continued {
        continuation_hpos.unwrap_or(previous_hpos)
    } else {
        previous_hpos
    };
    Ok(DisplayMotion {
        position,
        hpos,
        vpos,
        previous_hpos,
        continued,
    })
}

fn line_number_display_width_value(
    interp: &mut Interpreter,
    env: &mut Env,
    pixelwise: Option<&Value>,
) -> Result<Value, LispError> {
    let body_height = super::call(interp, "window-body-height", &[], env)?
        .as_integer()?
        .max(1) as usize;
    let selected_buffer = interp.selected_window_buffer_id();
    let saved_buffer = interp.current_buffer_id();
    interp.set_current_buffer_id(selected_buffer)?;
    let result = (|| -> Result<Value, LispError> {
        let mode = interp
            .lookup_var("display-line-numbers", env)
            .unwrap_or(Value::Nil);
        if mode.is_nil() {
            return Ok(
                if matches!(pixelwise.map(|v| v.kind()), Some(Kind::Symbol(name)) if name == "columns")
                {
                    Value::float(0.0)
                } else {
                    Value::Integer(0)
                },
            );
        }

        let start = interp.selected_window_start().clamp(
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
            .lookup_var("display-line-numbers-width", env)
            .and_then(|value| value.integer_value())
            .unwrap_or(0)
            .max(0);
        let columns = computed_width.max(requested_width);
        // The line-number face contributes two canonical columns of
        // left/right padding in GNU's headless terminal display.
        let pixels = columns + 2;
        Ok(match pixelwise.map(|v| v.kind()) {
            Some(Kind::Symbol(name)) if name == "columns" => Value::float(pixels as f64),
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
pub(crate) fn visual_vertical_motion(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    n: i64,
    goal_col: Option<usize>,
) -> Result<i64, LispError> {
    if interactive_motion(interp, env) {
        // The display iterator's walk, then move_it_in_display_line to
        // the goal X on the screen line it reached (line-move-visual's
        // temporary-goal-column contract).
        let (mut target, moved) =
            crate::lisp::primitives::move_it::vertical_motion(interp, env, window_id, n);
        if let Some(goal) = goal_col.filter(|&goal| goal > 0) {
            let (bol, eol) = visual_line_bounds(interp, target);
            let starts = screen_row_starts(interp, env, window_id, bol, eol);
            let index = row_index_of(&starts, target);
            target = interactive_screen_line(interp, env, window_id, bol)
                .position_at_column(index, goal);
        }
        interp.buffer.borrow_mut().goto_char(target);
        return Ok(moved);
    }
    let point_min = interp.buffer.borrow().point_min();
    let point_max = interp.buffer.borrow().point_max();
    let point = interp.buffer.borrow().point();
    let (mut bol, mut eol) = visual_line_bounds(interp, point);
    let mut starts = screen_row_starts(interp, env, window_id, bol, eol);
    let mut index = row_index_of(&starts, point);
    let mut moved = 0i64;
    let mut exhausted_forward = false;
    while moved < n {
        if index + 1 < starts.len() {
            index += 1;
            moved += 1;
        } else if eol < point_max {
            let (next_bol, next_eol) = visual_line_bounds(interp, eol + 1);
            bol = next_bol;
            eol = next_eol;
            starts = screen_row_starts(interp, env, window_id, bol, eol);
            index = 0;
            moved += 1;
        } else {
            exhausted_forward = true;
            break;
        }
    }
    while moved > n {
        if index > 0 {
            index -= 1;
            moved -= 1;
        } else if bol > point_min {
            let (prev_bol, prev_eol) = visual_line_bounds(interp, bol - 1);
            bol = prev_bol;
            eol = prev_eol;
            starts = screen_row_starts(interp, env, window_id, bol, eol);
            index = starts.len().saturating_sub(1);
            moved -= 1;
        } else {
            break;
        }
    }
    // GNU's batch path (vmotion) ignores a cons LINES's goal column:
    // point lands at the start of the target screen line (or buffer end
    // on overshoot).
    let target = if exhausted_forward {
        point_max
    } else {
        starts[index]
    };
    interp.buffer.borrow_mut().goto_char(target);
    Ok(moved)
}

/// The `goto-char' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_goto_char(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "goto-char";
    need_args(name, args, 1)?;
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
    let n = if args.is_empty() || args[0].is_nil() {
        1
    } else {
        args[0].as_integer()?
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
    let n = if args.is_empty() || args[0].is_nil() {
        1
    } else {
        args[0].as_integer()?
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
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "skip-chars-forward";
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    regexp::skip_chars_forward_impl(interp, &args[0], args.get(1))
}

/// The `skip-chars-backward' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_skip_chars_backward(
    interp: &mut Interpreter,
    args: &[Value],
    _env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "skip-chars-backward";
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    regexp::skip_chars_backward_impl(interp, &args[0], args.get(1))
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
/// 1, else an integer.
fn line_count_arg(n: Option<&Value>) -> Result<i64, LispError> {
    match n.map(|value| value.kind()) {
        None | Some(Kind::Nil) => Ok(1),
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
        Some(Kind::Integer(_)) => None,
        Some(Kind::Marker(id)) => interp.marker_position(id),
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
        Some(Kind::Integer(_)) => None,
        Some(Kind::Marker(id)) => interp.marker_position(id),
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
fn indent_to(
    interp: &mut Interpreter,
    column: i64,
    minimum: i64,
    env: &mut crate::lisp::types::Env,
) -> Result<i64, LispError> {
    let saved = interp.buffer.borrow().point();
    interp.buffer.borrow_mut().beginning_of_line();
    let bol = interp.buffer.borrow().point();
    interp.buffer.borrow_mut().goto_char(saved);
    let mut from_col = column_at(interp, env, bol, saved) as i64;
    let min_col = (from_col + minimum).max(column);
    if min_col == from_col {
        return Ok(min_col);
    }
    // SANE_TAB_WIDTH.
    let tab_width = interp
        .lookup_var("tab-width", env)
        .and_then(|value| value.integer_value())
        .filter(|width| (1..=1000).contains(width))
        .unwrap_or(8);
    if interp
        .lookup_var("indent-tabs-mode", env)
        .is_some_and(|value| value.is_truthy())
    {
        let tabs = min_col / tab_width - from_col / tab_width;
        if tabs > 0 {
            insert_text_with_hooks(
                interp,
                &"\t".repeat(tabs as usize),
                &[],
                &[],
                true,
                false,
                env,
            )?;
            from_col = (min_col / tab_width) * tab_width;
        }
    }
    let spaces = (min_col - from_col).max(0) as usize;
    insert_text_with_hooks(interp, &" ".repeat(spaces), &[], &[], true, false, env)?;
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
    match interp.buffer.borrow().buffer_substring(start, end) {
        Ok(s) => {
            if name == "buffer-substring" {
                Ok(string_like_value_with_extended_chars(
                    s,
                    interp.buffer.borrow().substring_property_spans(start, end),
                    interp.buffer.borrow().is_multibyte(),
                    interp.buffer.borrow().substring_extended_chars(start, end),
                ))
            } else {
                Ok(string_like_value_with_extended_chars(
                    s,
                    Vec::new(),
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
