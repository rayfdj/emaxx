use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

/// One `compare-buffer-substrings' operand: its buffer and ordered bounds,
/// nil bounds meaning the buffer's accessible region. editfns.c signals
/// `args-out-of-range' with the original bounds for a range outside it.
fn comparison_range(
    interp: &Interpreter,
    buffer: &Value,
    start: &Value,
    end: &Value,
) -> Result<(u64, usize, usize), LispError> {
    // Fget_buffer and nsberror, then the buffer's liveness.
    let buffer_id = if buffer.is_nil() {
        interp.current_buffer_id()
    } else {
        let buffer = interp.get_buffer_or_error(buffer)?;
        interp
            .live_buffer_id(buffer)
            .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))?
    };
    let buffer = interp
        .get_buffer_by_id(buffer_id)
        .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
    let from = if start.is_nil() {
        buffer.point_min()
    } else {
        position_from_value(interp, start)?
    };
    let to = if end.is_nil() {
        buffer.point_max()
    } else {
        position_from_value(interp, end)?
    };
    let (from, to) = (from.min(to), from.max(to));
    if from < buffer.point_min() || to > buffer.point_max() {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            *start,
            *end,
        ])));
    }
    Ok((buffer_id, from, to))
}

/// The characters of BUFFER from FROM on, as BUF_FETCH_MULTIBYTE_CHAR
/// reads a multibyte buffer's and make_char_multibyte makes a unibyte
/// buffer's bytes characters.
fn char_codes_from<'a>(
    buffer: &'a crate::buffer::Buffer,
    (first, second): &'a (std::cell::Ref<'a, str>, std::cell::Ref<'a, str>),
    from: usize,
) -> impl Iterator<Item = u32> + 'a {
    use crate::lisp::primitives::case::{RAW_BYTE8_BASE, public_buffer_char_code};
    let multibyte = buffer.is_multibyte();
    first
        .chars()
        .chain(second.chars())
        .zip(from..)
        .map(move |(ch, position)| {
            if ch == crate::lisp::regex_emacs::EXTENDED_PLACEHOLDER
                && let Some(code) = buffer.extended_char_at(position)
            {
                return code;
            }
            let code = public_buffer_char_code(ch, multibyte) as u32;
            if multibyte || code < 0x80 {
                code
            } else {
                code + RAW_BYTE8_BASE
            }
        })
}

/// editfns.c:Fcompare_buffer_substrings: the ranges' characters compared
/// from their starts, each start's byte position the one conversion, to
/// the first that differs once `case-fold-search' translates both through
/// the current buffer's case_canon_table (even when neither range is in
/// it); the buffers are let go at each 65536th character for rarely_quit.
fn compare_buffer_substrings(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let trt = interp
        .case_fold_search_active()
        .then(|| interp.current_case_tables().canon);
    let (buffer1, begp1, endp1) = comparison_range(interp, &args[0], &args[1], &args[2])?;
    let (buffer2, begp2, endp2) = comparison_range(interp, &args[3], &args[4], &args[5])?;
    // char_table_translate: the entry when it is a character, else C.
    let translate = |c: u32| match trt {
        Some(table) => table
            .get(c)
            .fixnum()
            .filter(|code| (0..=i64::from(crate::lisp::regex_emacs::MAX_CHAR)).contains(code))
            .map_or(c, |code| code as u32),
        None => c,
    };
    let (length1, length2) = (endp1 - begp1, endp2 - begp2);
    let limit = length1.min(length2);
    let mut chars = 0usize;
    while chars < limit {
        let stretch_end = limit.min((chars | 0xFFFF) + 1);
        let differ = {
            let live = |id: u64| {
                interp
                    .get_buffer_by_id(id)
                    .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))
            };
            let (left, right) = (live(buffer1)?, live(buffer2)?);
            let (text1, text2) = (
                left.text_from(begp1 + chars),
                right.text_from(begp2 + chars),
            );
            let mut codes1 = char_codes_from(&left, &text1, begp1 + chars);
            let mut codes2 = char_codes_from(&right, &text2, begp2 + chars);
            let mut differ = None;
            while chars < stretch_end {
                let (Some(c1), Some(c2)) = (codes1.next(), codes2.next()) else {
                    break;
                };
                let (c1, c2) = (translate(c1), translate(c2));
                if c1 != c2 {
                    differ = Some(c1 < c2);
                    break;
                }
                chars += 1;
            }
            differ
        };
        let matched = chars as i64;
        match differ {
            Some(true) => return Ok(Value::Integer(-1 - matched)),
            Some(false) => return Ok(Value::Integer(matched + 1)),
            None if chars & 0xFFFF == 0 => interp.maybe_quit(env)?,
            None => {}
        }
    }
    // The strings match as far as they go; a shorter one is less.
    let matched = chars as i64;
    Ok(Value::Integer(if chars < length1 {
        matched + 1
    } else if chars < length2 {
        -matched - 1
    } else {
        0
    }))
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "unencodable-char-position" => {
                need_arg_range(name, args, 3, 5)?;
                unencodable_char_position_value(interp, args, env)
            }
            "compare-buffer-substrings" => {
                need_args(name, args, 6)?;
                compare_buffer_substrings(interp, args, env)
            }
            "field-beginning" | "field-end" => {
                need_arg_range(name, args, 0, 3)?;
                let escape_from_edge = args.get(1).is_some_and(|value| !value.is_nil());
                let bound = if name == "field-beginning" {
                    field_beginning(
                        interp,
                        &args.first().copied().unwrap_or(Value::Nil),
                        escape_from_edge,
                        args.get(2).copied().unwrap_or(Value::Nil),
                    )?
                } else {
                    field_end(
                        interp,
                        &args.first().copied().unwrap_or(Value::Nil),
                        escape_from_edge,
                        args.get(2).copied().unwrap_or(Value::Nil),
                    )?
                };
                Ok(Value::Integer(bound))
            }
            "field-string" | "field-string-no-properties" => {
                need_arg_range(name, args, 0, 1)?;
                let (beg, end) = find_field(
                    interp,
                    &args.first().copied().unwrap_or(Value::Nil),
                    false,
                    Value::Nil,
                    true,
                    Value::Nil,
                    true,
                )?;
                let bounds = [Value::Integer(beg), Value::Integer(end)];
                if name == "field-string" {
                    super::buffer_edit::direct_buffer_substring(interp, &bounds, env)
                } else {
                    super::buffer_edit::direct_buffer_substring_no_properties(interp, &bounds, env)
                }
            }
            "delete-field" => {
                need_arg_range(name, args, 0, 1)?;
                let (beg, end) = find_field(
                    interp,
                    &args.first().copied().unwrap_or(Value::Nil),
                    false,
                    Value::Nil,
                    true,
                    Value::Nil,
                    true,
                )?;
                if beg != end {
                    delete_region_with_hooks(interp, beg as usize, end as usize, env)?;
                }
                Ok(Value::Nil)
            }
            "constrain-to-field" => {
                need_arg_range(name, args, 2, 5)?;
                Ok(Value::Integer(constrain_to_field(
                    interp,
                    &args[0],
                    &args[1],
                    args.get(2).is_some_and(|value| !value.is_nil()),
                    args.get(3).is_some_and(|value| !value.is_nil()),
                    args.get(4).copied().unwrap_or(Value::Nil),
                )?))
            }
            "current-buffer" => Ok(Value::Buffer(interp.buffer)),
            "get-buffer" => {
                need_args(name, args, 1)?;
                Ok(interp
                    .get_buffer(&args[0])?
                    .map_or(Value::Nil, Value::Buffer))
            }
            "get-buffer-create" => {
                need_args(name, args, 1)?;
                let inhibit_hooks = args.get(1).is_some_and(|value| value.is_truthy());
                // buffer.c:Fget_buffer_create returns buffer objects as given,
                // even when renamed or dead; only strings select by name.
                if let Some(buffer) = interp.get_buffer(&args[0])? {
                    return Ok(Value::Buffer(buffer));
                }
                {
                    let buf_name = string_text(&args[0])?;
                    if buf_name.is_empty() {
                        return Err(LispError::Signal(
                            "Empty string for buffer name is not allowed".into(),
                        ));
                    }
                    let (id, _) = interp.create_buffer(&buf_name);
                    interp.set_buffer_hooks_inhibited(id, inhibit_hooks);
                    let buffer = interp.buffer_value(id).expect("new buffer object");
                    // buffer.c:Fget_buffer_create returns its allocation even
                    // when the update hook kills it. Keep the Rust local rooted
                    // across hooks and collection, after it leaves buffer-list.
                    interp.with_lisp_stack_roots(&buffer, |interp| {
                        if !inhibit_hooks {
                            run_hook(
                                interp,
                                &cached_symbol!("buffer-list-update-hook").symbol(),
                                env,
                            )?;
                        }
                        Ok(buffer)
                    })
                }
            }
            "generate-new-buffer-name" => {
                need_args(name, args, 1)?;
                let ignore = args.get(1).and_then(|ignore| {
                    with_string_text(ignore, |ignore| {
                        with_string_text(&args[0], |base| base == ignore)
                    })
                    .flatten()
                });
                let taken = with_string_text(&args[0], |base| interp.has_buffer(base))
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[0]))?;
                if !taken || ignore == Some(true) {
                    // buffer.c returns NAME itself, not a copy.
                    Ok(args[0])
                } else {
                    let base = string_text(&args[0])?;
                    let ignore = args
                        .get(1)
                        .and_then(|ignore| string_like(ignore).map(|s| s.text));
                    // bug#1229: a name that starts with a space first tries
                    // a random -NNNNNN suffix, so that thousands of
                    // internal buffers do not each scan <2>, <3>, ...
                    let genbase = if base.starts_with(' ') {
                        let number = crate::lisp::primitives::get_random() % 1_000_000;
                        let genbase = format!("{base}-{number}");
                        if !interp.has_buffer(&genbase) {
                            return Ok(Value::String(genbase.into()));
                        }
                        genbase
                    } else {
                        base
                    };
                    let mut n = 2;
                    loop {
                        let candidate = format!("{genbase}<{n}>");
                        if ignore.as_deref() == Some(&candidate) || !interp.has_buffer(&candidate) {
                            break Ok(Value::String(candidate.into()));
                        }
                        n += 1;
                    }
                }
            }
            "make-indirect-buffer" => {
                if args.len() < 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // buffer.c:Fmake_indirect_buffer: NAME a string no buffer
                // has, BASE-BUFFER through Fget_buffer and live, NAME not
                // empty.
                let new_name = string_text(&args[1])?;
                if interp.find_buffer_id(&new_name).is_some() {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Buffer name `{}' is in use",
                        &[&new_name],
                    ));
                }
                let base = interp.get_buffer(&args[0])?.ok_or_else(|| {
                    crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "No such buffer: `{}'",
                        &[&string_text(&args[0]).unwrap_or_default()],
                    )
                })?;
                let base_id = interp
                    .live_buffer_id(base)
                    .ok_or_else(|| LispError::Signal("Base buffer has been killed".into()))?;
                if new_name.is_empty() {
                    return Err(LispError::Signal(
                        "Empty string for buffer name is not allowed".into(),
                    ));
                }
                let clone = args.get(2).is_some_and(|value| value.is_truthy());
                let inhibit_hooks = args.get(3).is_some_and(|value| value.is_truthy());
                // No double indirection: an indirect buffer's base is a
                // buffer with its own text.
                let base_id = interp.root_buffer_id(base_id);
                let (new_id, _) = interp.create_buffer(&new_name);
                let (base_object, object) =
                    match (interp.buffer_object(base_id), interp.buffer_object(new_id)) {
                        (Some(base), Some(object)) => (base, object),
                        _ => {
                            return Err(LispError::Signal(format!(
                                "No buffer with id {}",
                                base_id
                            )));
                        }
                    };
                let mut state = crate::buffer::Buffer::new(&new_name);
                state.text_conversion_style = interp.native_text_conversion_style_default();
                object.replace_state(state);
                // Use the base buffer's text object, and its chain of
                // markers.
                object.share_marker_chain(base_object);
                let base_overlays = {
                    let mut base = base_object.borrow_mut();
                    let mut buffer = object.borrow_mut();
                    buffer.share_text_of(&base, base_object);
                    // GNU indirect buffers never visit their base's file:
                    // make-indirect-buffer clears buffer-file-name and
                    // buffer-file-truename even when CLONE is non-nil.
                    buffer.inhibit_hooks = inhibit_hooks;
                    let (begv, zv) = base.restriction();
                    buffer.restore_restriction(begv, zv);
                    buffer.goto_char(base.point());
                    // Make sure the base buffer has markers for its
                    // narrowing.
                    if base.position_markers().is_none() {
                        let at = (base.point(), begv, zv);
                        let current = base_id == interp.current_buffer_id();
                        base.make_position_markers(base_object, at, current);
                    }
                    if clone {
                        // clone_per_buffer_values: the base's mark and
                        // `mark-active', and its markers copied.
                        if let Some(mark) = base.mark() {
                            buffer.set_mark_position(mark);
                        }
                        buffer.set_mark_active_value(base.mark_active_value());
                        let markers = base.position_markers().expect("the base's markers");
                        let at = (
                            markers.pt.last_position(),
                            markers.begv.last_position(),
                            markers.zv.last_position(),
                        );
                        buffer.make_position_markers(object, at, false);
                        base.overlays.iter().collect::<Vec<_>>()
                    } else {
                        // A fresh mark pointing nowhere and `mark-active'
                        // nil (reset_buffer); markers for its narrowing.
                        let at = (buffer.point(), begv, zv);
                        buffer.make_position_markers(object, at, false);
                        Vec::new()
                    }
                };
                // buffer.c:copy_overlays walks ascending intervals,
                // allocates new objects, and copies only plist spines.
                for source in base_overlays {
                    let overlay = crate::overlay::OverlayRef::new(
                        source.front_advance(),
                        source.rear_advance(),
                    );
                    overlay.set_plist(Value::list(source.plist().to_vec()?));
                    overlay.move_to(object, source.beg(), source.end());
                }
                interp.register_indirect_buffer(new_id, base_id);

                if clone {
                    interp.clone_buffer_local_state(base_id, new_id);
                    // The clone visits no file, has its own display count,
                    // backup history and auto-save file, and no minor modes.
                    for (slot, value) in [
                        ("buffer-display-count", Value::Integer(0)),
                        ("buffer-backed-up", Value::Nil),
                        ("local-minor-modes", Value::Nil),
                        ("buffer-auto-save-file-name", Value::Nil),
                    ] {
                        interp.set_buffer_local_value(new_id, slot, value);
                    }
                    // Then, in the clone, Fset of buffer-save-without-query
                    // and buffer-file-number, and no buffer-local
                    // buffer-stale-function the base has.
                    let saved = interp.current_buffer_id();
                    interp.set_current_buffer_id(new_id)?;
                    let result = (|| {
                        for variable in ["buffer-save-without-query", "buffer-file-number"] {
                            call_named_function(
                                interp,
                                "set",
                                &[Value::symbol(variable), Value::Nil],
                                env,
                            )?;
                        }
                        if interp.has_buffer_local_binding(base_id, "buffer-stale-function") {
                            call_named_function(
                                interp,
                                "kill-local-variable",
                                &[Value::symbol("buffer-stale-function")],
                                env,
                            )?;
                        }
                        Ok::<(), LispError>(())
                    })();
                    interp.set_current_buffer_id(saved)?;
                    result?;
                    interp.run_clone_indirect_buffer_hook(new_id, env)?;
                }
                if !inhibit_hooks {
                    run_hook(
                        interp,
                        &cached_symbol!("buffer-list-update-hook").symbol(),
                        env,
                    )?;
                }
                Ok(interp.buffer_value(new_id).expect("live buffer object"))
            }
            "rename-buffer" => {
                need_args(name, args, 1)?;
                // buffer.c:Frename_buffer.
                let new_name = string_text(&args[0])?;
                if new_name.is_empty() {
                    return Err(LispError::Signal(
                        "Empty string is invalid as a buffer name".into(),
                    ));
                }
                let old_name = interp.buffer.borrow().name.clone();
                let unique = args.len() > 1 && args[1].is_truthy();
                let final_name = if interp.has_buffer(&new_name) {
                    if !unique && new_name == old_name {
                        return Ok(Value::String(old_name.into()));
                    }
                    if !unique {
                        return Err(crate::lisp::primitives::values::doprnt_error(
                            interp,
                            "Buffer name `{}' is in use",
                            &[&new_name],
                        ));
                    }
                    // Fgenerate_new_buffer_name with the old name as the
                    // one that may be kept, so a buffer already in NAME's
                    // <N> sequence keeps its name.
                    let generated = super::call(
                        interp,
                        "generate-new-buffer-name",
                        &[args[0], Value::String(old_name.clone().into())],
                        env,
                    )?;
                    string_text(&generated)?
                } else {
                    new_name
                };
                if let Some(pos) = interp.buffer_list.iter().position(|(_, n)| *n == old_name) {
                    interp.buffer_list[pos].1 = final_name.clone();
                }
                interp.buffer.borrow_mut().last_name = Some(old_name);
                interp.buffer.borrow_mut().name = final_name.clone();
                // A buffer visiting no file renames its auto-save file.
                let visits_file = interp
                    .lookup_var_key(cached_symbol!("buffer-file-name"), env)
                    .is_some_and(|value| value.is_truthy());
                let auto_saves = interp
                    .lookup_var_key(cached_symbol!("buffer-auto-save-file-name"), env)
                    .is_some_and(|value| value.is_truthy());
                if !visits_file && auto_saves {
                    crate::lisp::primitives::hooks_overlays::call_named_function(
                        interp,
                        "rename-auto-save-file",
                        &[],
                        env,
                    )?;
                }
                run_hook(
                    interp,
                    &cached_symbol!("buffer-list-update-hook").symbol(),
                    env,
                )?;
                // GNU buffer.c calls the preloaded Elisp owner directly
                // after installing the provisional name.  Uniquify uses
                // that boundary to replace a generated `<N>' suffix with
                // directory-derived policy; keep that policy in uniquify.el.
                if interp.is_fbound("uniquify--rename-buffer-advice") {
                    let callback = interp.lookup_function_key(
                        cached_symbol!("uniquify--rename-buffer-advice"),
                        env,
                    )?;
                    interp.call_function_value(
                        callback,
                        Some("uniquify--rename-buffer-advice"),
                        &[args[0], args.get(1).cloned().unwrap_or(Value::Nil)],
                        env,
                    )?;
                }
                Ok(Value::String(interp.buffer.borrow().name.clone().into()))
            }
            "other-buffer" => {
                need_arg_range(name, args, 0, 3)?;
                let frame = super::frames::decode_live_frame(interp, args.get(2), true)?;
                other_buffer(
                    interp,
                    args.first().copied().unwrap_or(Value::Nil),
                    args.get(1).is_some_and(Value::is_truthy),
                    frame,
                    env,
                )
            }
            "buffer-base-buffer" => {
                // buffer.c Fbuffer_base_buffer: a nil BUFFER means the
                // current buffer, exactly like an omitted argument.
                // A killed indirect buffer keeps its base.
                let buffer = interp.decode_buffer(args.first())?;
                let base = buffer.borrow().base_buffer();
                Ok(base.map_or(Value::Nil, Value::Buffer))
            }
            "buffer-swap-text" => {
                need_args(name, args, 1)?;
                let Kind::Buffer(other) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("bufferp".into(), args[0]));
                };
                let other_id = interp.live_buffer_id(other).ok_or_else(|| {
                    crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Cannot swap a dead buffer's text",
                        &[],
                    )
                })?;
                let current_id = interp.current_buffer_id();
                if interp.buffer_base_id(other_id).is_some()
                    || interp.buffer_base_id(current_id).is_some()
                {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Cannot swap indirect buffers's text",
                        &[],
                    ));
                }
                if interp.related_buffer_ids(other_id).len() > 1
                    || interp.related_buffer_ids(current_id).len() > 1
                {
                    return Err(LispError::Signal(
                        "One of the buffers to swap has indirect buffers".into(),
                    ));
                }
                interp.swap_buffer_text_state(current_id, other_id)?;
                Ok(Value::Nil)
            }
            "buffer-local-value" => {
                need_args(name, args, 2)?;
                // data.c:buffer_local_value: CHECK_SYMBOL, the alias chain's
                // end by its symbols, CHECK_BUFFER; a killed buffer has no
                // locals left and an empty history.
                let symbol = interp
                    .resolve_variable_symbol(&checked_symbol_identity(interp, &args[0], env)?)?;
                let Kind::Buffer(buffer) = args[1].kind() else {
                    return Err(LispError::WrongTypeArgument("bufferp".into(), args[1]));
                };
                let undo_list = cached_symbol!("buffer-undo-list").with(|undo| symbol == *undo);
                if interp.live_buffer_id(buffer).is_none() {
                    let killed = interp
                        .killed_buffer_local_slots(&buffer.borrow())
                        .into_iter()
                        .find(|(name, _)| *name == symbol.as_str())
                        .map(|(_, value)| value);
                    let value = if undo_list {
                        Some(crate::lisp::primitives::buffer_undo_list_value(
                            &buffer.borrow(),
                        ))
                    } else if let Some(value) = killed {
                        (!matches!(value.kind(), Kind::Unbound)).then_some(value)
                    } else {
                        default_value_here(interp, &symbol)?
                    };
                    return value.ok_or_else(|| {
                        LispError::SignalValue(Value::list([
                            Value::symbol("void-variable"),
                            args[0],
                        ]))
                    });
                }
                let buffer_id = buffer.id;
                // data.c:buffer_local_value: BUFFER's own alist cell if it
                // has one (a void cell is void), else the default; another
                // buffer's local value must not leak through (erc-open's
                // prior-session detection reads `erc--target').  Fbuffer_local_value
                // signals void-variable with VARIABLE as given.
                let value = if interp.has_native_text_conversion_style(&symbol) {
                    interp.buffer_local_value(buffer_id, symbol.as_str())
                } else if undo_list && symbol.cell().has_c_slot() {
                    // BVAR (b, undo_list): BUFFER's own history, t while
                    // it keeps none, held by the buffer and not its locals.
                    interp
                        .get_buffer_by_id(buffer_id)
                        .map(|buffer| crate::lisp::primitives::buffer_undo_list_value(&buffer))
                } else {
                    match interp.buffer_local_binding_symbol(buffer_id, &symbol) {
                        Some(local) => local,
                        None => default_value_here(interp, &symbol)?,
                    }
                };
                value.ok_or_else(|| {
                    LispError::SignalValue(Value::list([Value::symbol("void-variable"), args[0]]))
                })
            }
            "buffer-local-variables" => {
                // buffer.c:decode_buffer; a killed buffer has its
                // always-local slots and an empty history left.
                let buffer = interp.decode_buffer(args.first())?;
                let buffer_id = buffer.id;
                // buffer.c:Fbuffer_local_variables: `buffer-undo-list'
                // first, then the DEFVAR_PER_BUFFER slots local in BUFFER
                // (every index -1 one) in struct buffer's order reversed,
                // then the alist cells oldest first
                // (buffer_lisp_local_variables); a void one is the bare
                // symbol, a bound one `(symbol . value)'.
                let entry = |name: &str, value: Value| {
                    if matches!(value.kind(), Kind::Unbound) {
                        Value::Symbol(name.into())
                    } else {
                        Value::cons(Value::Symbol(name.into()), value)
                    }
                };
                let undo_list = buffer_undo_list_value(&buffer.borrow());
                let mut vars = vec![Value::cons(
                    Value::Symbol("buffer-undo-list".into()),
                    undo_list,
                )];
                if interp.live_buffer_id(buffer).is_none() {
                    vars.extend(
                        interp
                            .killed_buffer_local_slots(&buffer.borrow())
                            .into_iter()
                            .map(|(name, value)| entry(name, value)),
                    );
                    return Ok(Value::list(vars));
                }
                // FOR_EACH_PER_BUFFER_OBJECT_AT reads each slot, whatever
                // its symbol's redirect: a variable makunbound leaves plain
                // keeps its slot and flag, and an ordinary local made for
                // it after that is an alist cell listed besides.
                for name in crate::lisp::eval::BUFFER_LOCAL_VARIABLES_ORDER {
                    let local = if *name == "text-conversion-style" {
                        interp.text_conversion_style_slot(buffer_id).map(Some)
                    } else {
                        interp.buffer_local_binding(buffer_id, name)
                    };
                    if let Some(local) = local {
                        vars.push(entry(name, local.unwrap_or(Value::Unbound)));
                    }
                }
                vars.extend(
                    interp
                        .buffer_alist_local_variables(buffer_id)
                        .into_iter()
                        .filter(|(symbol, _)| {
                            symbol.is_named("text-conversion-style")
                                || crate::lisp::eval::native_per_buffer_slot(symbol).is_none()
                        })
                        .map(|(symbol, value)| {
                            if matches!(value.kind(), Kind::Unbound) {
                                Value::Symbol(symbol)
                            } else {
                                Value::cons(Value::Symbol(symbol), value)
                            }
                        }),
                );
                Ok(Value::list(vars))
            }
            #[cfg(target_os = "linux")]
            "set-text-conversion-style" => {
                need_arg_range(name, args, 1, 2)?;
                // textconv.c:Fset_text_conversion_style first uses bset,
                // without localization or watchers. This terminal backend
                // has no text_interface, so GNU returns nil at that point.
                interp.buffer.borrow_mut().text_conversion_style = args[0];
                Ok(Value::Nil)
            }
            "kill-local-variable" => {
                need_args(name, args, 1)?;
                // CHECK_SYMBOL, then the alias chain followed by the symbols.
                let checked = checked_symbol_identity(interp, &args[0], env)?;
                let symbol = interp.resolve_variable_symbol(&checked)?;
                // data.c:Fkill_local_variable's BUFFER_OBJFWD branch: an
                // indexed slot returns to its default with its flag
                // cleared, an always-local one (index -1) stays as it is;
                // no watcher is told, and VARIABLE comes back as supplied,
                // an alias included, before the alist branch rewrites it.
                if let Some(slot) = crate::lisp::eval::native_per_buffer_slot(&symbol)
                    && interp.is_per_buffer_symbol(&symbol)
                {
                    if !slot.always_local {
                        interp
                            .remove_buffer_local_value_symbol(interp.current_buffer_id(), &symbol);
                    }
                    return Ok(args[0]);
                }
                // A SYMBOL_PLAINVAL or other SYMBOL_FORWARDED symbol has no
                // binding in any buffer: VARIABLE comes back unchanged and
                // no watcher hears of it.  Only a localized one is told
                // makunbound, whether or not this buffer has a cell.
                if !interp.is_localized_symbol(&symbol) {
                    return Ok(args[0]);
                }
                interp.notify_variable_watchers_symbol(
                    &symbol,
                    Value::Nil,
                    "makunbound",
                    Some(interp.current_buffer_id()),
                    env,
                )?;
                interp.remove_buffer_local_value_symbol(interp.current_buffer_id(), &symbol);
                Ok(Value::Symbol(symbol))
            }
            "make-local-variable" => {
                need_args(name, args, 1)?;
                // data.c:Fmake_local_variable: CHECK_SYMBOL, follow the alias,
                // refuse a constant (SYMBOL_NOWRITE) with VARIABLE as given,
                // then give this buffer its own cell holding the default
                // cell's value -- void stays void -- unless it has one
                // already; a let made for this buffer draws a message.
                let checked = checked_symbol_identity(interp, &args[0], env)?;
                let symbol = interp.resolve_variable_symbol(&checked)?;
                check_may_be_buffer_local(interp, &checked, &symbol)?;
                if interp.is_constant_symbol(&symbol) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("setting-constant"),
                        args[0],
                    ])));
                }
                let buffer_id = interp.current_buffer_id();
                if interp.has_native_text_conversion_style(&symbol) {
                    // BUFFER_OBJFWD has a field already: only set its flag.
                    interp.buffer.borrow_mut().text_conversion_style_is_local = true;
                    return Ok(args[0]);
                }
                // A native always-local slot (buffer-file-name, mode-name,
                // default-directory, ...) is already local by construction
                // (a buffer_local_flags index of -1 returns at once).
                if !interp.is_always_buffer_local_symbol(&symbol)
                    && !interp.has_buffer_local_binding_symbol(buffer_id, &symbol)
                {
                    // A BUFFER_OBJFWD variable has its per-buffer flag set
                    // and returns; the message is a SYMBOL_LOCALIZED one's.
                    let buffer_objfwd = interp.is_buffer_objfwd_symbol(&symbol);
                    if !buffer_objfwd && interp.let_shadows_buffer_binding(symbol.as_str()) {
                        call_named_function(
                            interp,
                            "message",
                            &[
                                Value::string("Making %s buffer-local while locally let-bound!"),
                                Value::string(crate::lisp::types::visible_symbol_name(
                                    symbol.as_str(),
                                )),
                            ],
                            env,
                        )?;
                    }
                    let value = interp
                        .symbol_value_cell_symbol(&symbol)
                        .unwrap_or(Value::Unbound);
                    interp.set_buffer_local_value_symbol(buffer_id, &symbol, value);
                }
                Ok(Value::Symbol(symbol))
            }
            "buffer-list" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c:Fbuffer_list: for a FRAME, its own buffers first,
                // then the others, then its buried buffers, least recently
                // buried first.
                let general = interp.buffer_list.iter().map(|(id, _)| *id);
                let ids: Vec<u64> = match args.first().map(|frame| frame.kind()) {
                    Some(Kind::Frame(frame)) => {
                        let frame = frame.borrow();
                        let mut ids = frame.buffer_list.clone();
                        ids.extend(general.filter(|id| {
                            !frame.buffer_list.contains(id)
                                && !frame.buried_buffer_list.contains(id)
                        }));
                        ids.extend(frame.buried_buffer_list.iter().rev());
                        ids
                    }
                    _ => general.collect(),
                };
                Ok(Value::list(
                    ids.into_iter().filter_map(|id| interp.buffer_value(id)),
                ))
            }
            "add-variable-watcher" => {
                need_args(name, args, 2)?;
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                interp.add_variable_watcher(&symbol, args[1])?;
                Ok(Value::Nil)
            }
            "remove-variable-watcher" => {
                need_args(name, args, 2)?;
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                interp.remove_variable_watcher(&symbol, &args[1])?;
                Ok(Value::Nil)
            }
            "get-variable-watchers" => {
                need_args(name, args, 1)?;
                // data.c reads SYMBOL_TRAPPED_WRITE_P without CHECK_SYMBOL,
                // undefined for what is no symbol; it is checked here.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                interp.variable_watchers(&symbol)
            }
            "command-modes" => {
                need_args(name, args, 1)?;
                let mut function = args[0];
                while let Kind::Symbol(symbol) = function.kind() {
                    if let Some(modes) = interp.get_symbol_property_of(
                        &symbol,
                        &cached_symbol!("command-modes").with(Clone::clone),
                    ) && !modes.is_nil()
                    {
                        return Ok(modes);
                    }
                    function = match interp.lookup_function_symbol(&symbol, env) {
                        Ok(function) => function,
                        Err(_) => return Ok(Value::Nil),
                    };
                }
                Ok(match function.kind() {
                    Kind::Closure(lambda) => lambda.command_modes().unwrap_or(Value::Nil),
                    _ => Value::Nil,
                })
            }
            "indirect-function" => {
                need_arg_range(name, args, 1, 2)?;
                let mut seen = Vec::new();
                let mut current = args[0];
                loop {
                    let symbol = match current.kind() {
                        Kind::Symbol(symbol) => symbol,
                        // SYMBOLP (t): its function cell is read too.
                        Kind::T => crate::lisp::types::symbols::builtin_boolean(true),
                        _ if symbols_with_pos_enabled(interp, env) => {
                            let Some((Kind::Symbol(symbol), _)) =
                                symbol_with_pos_parts(interp, &current)
                                    .map(|(a0, a1)| (a0.kind(), a1))
                            else {
                                return Ok(current);
                            };
                            symbol
                        }
                        _ => return Ok(current),
                    };
                    if seen.iter().any(|existing| existing == &symbol) {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("cyclic-function-indirection".into()),
                            Value::Symbol(symbol),
                        ])));
                    }
                    seen.push(symbol);
                    match interp
                        .lookup_function_symbol(&symbol, env)
                        .map_err(LispError::into_kind)
                    {
                        Ok(resolved) if matches!(resolved.kind(), Kind::Symbol(_) | Kind::T) => {
                            current = resolved;
                        }
                        Ok(resolved) => return Ok(resolved),
                        // Since Emacs 24.4, a void function cell is
                        // represented as nil and indirect-function returns
                        // that nil.  Calling the result is where a
                        // void-function condition belongs.
                        Err(LispErrorKind::VoidFunction(_)) => return Ok(Value::Nil),
                        Err(error) => return Err(LispError::from(error)),
                    }
                }
            }
            "byteorder" => {
                need_args(name, args, 0)?;
                Ok(Value::Integer(if cfg!(target_endian = "big") {
                    'B' as i64
                } else {
                    'l' as i64
                }))
            }
            "subr-arity" => {
                need_args(name, args, 1)?;
                match args[0].kind() {
                    Kind::BuiltinFunc(subr) => Ok(subr.arity_value()),
                    Kind::NativeFunction(function) => Ok(function.arity_value()),
                    // GNU data.c CHECK_SUBR signals the subrp predicate with
                    // the offending value itself.
                    other => Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp",
                        other.value(),
                    )),
                }
            }
            "func-arity" => {
                need_args(name, args, 1)?;
                // GNU eval.c:Ffunc_arity strips a `(macro . FN)' wrapper and
                // reports the expander's arity.
                let unwrap_macro = |value: &Value| -> Value {
                    if let Some((car, cdr)) = value.cons_values()
                        && values_eq_in_env(interp, &car, &Value::Symbol("macro".into()), env)
                    {
                        cdr
                    } else {
                        *value
                    }
                };
                let function = resolve_callable(interp, &args[0], env)?;
                function_arity_value(interp, &unwrap_macro(&function), &args[0], env)
            }
            "subr-name" => {
                need_args(name, args, 1)?;
                match args[0].kind() {
                    Kind::BuiltinFunc(symbol) => Ok(Value::string(symbol.as_str())),
                    Kind::NativeFunction(function) => Ok(Value::string(function.name())),
                    other => Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp",
                        other.value(),
                    )),
                }
            }
            "subr-native-lambda-list" => {
                need_args(name, args, 1)?;
                match args[0].kind() {
                    Kind::BuiltinFunc(_) => Ok(Value::T),
                    Kind::NativeFunction(function) => Ok(if function.is_dynamic() {
                        function.lambda_list()
                    } else {
                        Value::T
                    }),
                    other => Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp",
                        other.value(),
                    )),
                }
            }
            "native-comp-available-p" => {
                need_args(name, args, 0)?;
                Ok(
                    if crate::lisp::native_comp::NativeCompilerState::available() {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "comp--subr-signature" => {
                need_args(name, args, 1)?;
                let Kind::BuiltinFunc(symbol) = args[0].kind() else {
                    return Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp", args[0],
                    ));
                };
                let arity = builtin_arity_value(&symbol)
                    .or_else(|| special_form_arity_value(&symbol))
                    .ok_or_else(|| {
                        LispError::Signal(format!("emaxx: no GNU-derived arity for subr {symbol}"))
                    })?;
                Ok(Value::String(
                    format!("{symbol}{}", render_prin1(interp, &arity, env)?).into(),
                ))
            }
            "comp-libgccjit-version" => {
                need_args(name, args, 0)?;
                Ok(crate::lisp::native_comp::NativeCompilerState::version()
                    .map(|(major, minor, patch)| {
                        Value::list([
                            Value::Integer(i64::from(major)),
                            Value::Integer(i64::from(minor)),
                            Value::Integer(i64::from(patch)),
                        ])
                    })
                    .unwrap_or(Value::Nil))
            }
            "comp-native-compiler-options-effective-p"
            | "comp-native-driver-options-effective-p" => {
                need_args(name, args, 0)?;
                Ok(
                    if crate::lisp::native_comp::NativeCompilerState::version()
                        .is_some_and(|(major, _, _)| major >= 9)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "dump-emacs-portable--sort-predicate" => {
                need_args(name, args, 2)?;
                let relocation_offset =
                    |entry: &Value| -> Result<i64, LispError> { entry.cdr()?.car()?.as_integer() };
                Ok(
                    if relocation_offset(&args[0])? < relocation_offset(&args[1])? {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "dump-emacs-portable" => {
                need_arg_range(name, args, 1, 2)?;
                crate::lisp::primitives::pdumper::dump_emacs_portable(
                    interp,
                    env,
                    &args[0],
                    args.get(1),
                )
            }
            "dump-emacs-portable--sort-predicate-copied" => {
                need_args(name, args, 2)?;
                // GNU orders addresses of objects copied from its static C image.
                // Rust values have no equivalent identity outside a real dump.
                Err(LispError::Signal(PORTABLE_DUMPER_UNAVAILABLE.into()))
            }
            "pdumper-stats" => {
                need_args(name, args, 0)?;
                // Fpdumper_stats: nil unless this process was restored from a
                // dump; then the alist of the load time and the expanded file
                // name.
                let Some(record) = interp.pdumper_load_record().cloned() else {
                    return Ok(Value::Nil);
                };
                let file_name = crate::lisp::primitives::system::expand_file_name_runtime(
                    interp,
                    env,
                    &record.filename,
                    None,
                )?;
                Ok(Value::list([
                    Value::cons(Value::symbol("dumped-with-pdumper"), Value::T),
                    Value::cons(
                        Value::symbol("load-time"),
                        Value::float(record.load_time.as_secs_f64()),
                    ),
                    Value::cons(Value::symbol("dump-file-name"), Value::string(&file_name)),
                ]))
            }
            "native-comp-function-p" => {
                need_args(name, args, 1)?;
                Ok(if matches!(args[0].kind(), Kind::NativeFunction(_)) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "subr-native-comp-unit" => {
                need_args(name, args, 1)?;
                match args[0].kind() {
                    Kind::BuiltinFunc(_) => Ok(Value::Nil),
                    Kind::NativeFunction(function) => Ok(function.unit()),
                    other => Err(crate::lisp::primitives::wrong_type_argument(
                        "subrp",
                        other.value(),
                    )),
                }
            }
            "native-comp-unit-file" => {
                need_args(name, args, 1)?;
                let Kind::NativeCompUnit(unit) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument(
                        "native-comp-unit".into(),
                        args[0],
                    ));
                };
                Ok(unit.field(0))
            }
            "native-comp-unit-set-file" => {
                need_args(name, args, 2)?;
                let Kind::NativeCompUnit(unit) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument(
                        "native-comp-unit".into(),
                        args[0],
                    ));
                };
                unit.set_field(0, args[1]);
                Ok(args[0])
            }
            "decode-char" => {
                need_args(name, args, 2)?;
                crate::lisp::primitives::charset::decode_char(interp, &args[0], &args[1])
            }
            "encode-char" => {
                need_args(name, args, 2)?;
                crate::lisp::primitives::charset::encode_char(interp, &args[0], &args[1])
            }
            "char-charset" => {
                need_arg_range(name, args, 1, 2)?;
                // CHECK_CHARACTER
                let Some(character) = args[0]
                    .as_integer()
                    .ok()
                    .and_then(|value| u32::try_from(value).ok())
                    .filter(|value| *value <= 0x3f_ffff)
                else {
                    return Err(LispError::WrongTypeArgument("characterp".into(), args[0]));
                };
                // ENCODE_CHAR takes GNU's character number as given, as in
                // the `encode-char' arm above.
                let internal = character;
                let restriction = args.get(1).filter(|value| !value.is_nil());
                let Some(restriction) = restriction else {
                    // CHAR_CHARSET: the charset priority walk.
                    return Ok(Value::Symbol(char_charset(interp, internal).0));
                };
                if restriction.cons_values().is_some() {
                    // A list RESTRICTION: the first charset that encodes
                    // CH wins, in list order; every entry must name a
                    // charset (CHECK_CHARSET_GET_CHARSET).
                    let mut tail = *restriction;
                    while let Some((head, rest)) = tail.cons_values() {
                        let charset = crate::lisp::eval::coding::CharsetRef::charset_symbol(&head)
                            .filter(|charset| interp.has_charset(charset));
                        let Some(charset) = charset else {
                            return Err(LispError::WrongTypeArgument("charsetp".into(), head));
                        };
                        if encode_charset_char(interp, &charset, internal).is_some() {
                            return Ok(head);
                        }
                        tail = rest;
                    }
                    return Ok(Value::Nil);
                }
                // A coding-system RESTRICTION: search the charsets that
                // coding system supports (coding_system_charset_list).  An
                // unknown coding system signals (coding-system-error NAME)
                // via CHECK_CODING_SYSTEM_GET_ID.  GNU substitutes global
                // charset lists for full-support iso-2022 and emacs-mule
                // codings; Emaxx models only the :charset-list attribute,
                // so those two families report fewer supported charsets.
                // CHECK_CODING_SYSTEM_GET_ID: Fcheck_coding_system's
                // CHECK_SYMBOL first.
                let symbol = match restriction.kind() {
                    Kind::T => "t".to_string(),
                    Kind::Symbol(name) => name.to_string(),
                    _ => return Err(LispError::WrongTypeArgument("symbolp".into(), *restriction)),
                };
                let known =
                    Some(symbol).filter(|coding| interp.coding_system_base_name(coding).is_some());
                let Some(coding) = known else {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("coding-system-error".into()),
                        (*restriction),
                    ])));
                };
                // char_charset substitutes the full charset priority order
                // for a nil charset list (raw-text codings carry none);
                // coding_system_charset_list reads a full-support
                // iso-2022 system's `iso-2022' as Viso_2022_charset_list
                // and emacs-mule's `emacs-mule' as Vemacs_mule_charset_list.
                let mut charsets = coding_system_charset_names(interp, &coding);
                if charsets.is_empty() {
                    let symbolic = interp.coding_system(&coding).and_then(|state| {
                        match state.charset_list.kind() {
                            Kind::Symbol(name) => Some(name.to_string()),
                            _ => None,
                        }
                    });
                    charsets = match symbolic.as_deref() {
                        Some("iso-2022") => interp.iso_2022_charset_list(),
                        Some("emacs-mule") => {
                            crate::lisp::primitives::coding::emacs_mule_charset_list(interp)
                        }
                        _ => interp.charset_priority_list(),
                    };
                }
                for charset in charsets {
                    if encode_charset_char(interp, &charset, internal).is_some() {
                        return Ok(Value::Symbol(charset));
                    }
                }
                Ok(Value::Nil)
            }
            "charsetp" => {
                need_args(name, args, 1)?;
                // CHARSETP: Vcharset_hash_table holds the object (by EQ).
                Ok(if interp.has_charset(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "charset-id-internal" => {
                need_arg_range(name, args, 0, 1)?;
                let symbol = check_charset(interp, args.first().unwrap_or(&Value::Nil))?;
                Ok(interp
                    .charset_id(&symbol)
                    .map(Value::Integer)
                    .unwrap_or(Value::Nil))
            }
            "charset-plist" => {
                need_args(name, args, 1)?;
                // CHECK_CHARSET_GET_ATTR.
                let symbol = check_charset(interp, &args[0])?;
                Ok(interp.charset_plist_value(&symbol).unwrap_or_else(|| {
                    default_charset_plist(&symbol, interp).unwrap_or(Value::Nil)
                }))
            }
            "charset-priority-list" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let priority = interp.charset_priority_list();
                if args.first().is_some_and(Value::is_truthy) {
                    Ok(priority
                        .first()
                        .cloned()
                        .map(Value::Symbol)
                        .unwrap_or(Value::Nil))
                } else {
                    Ok(Value::list(
                        priority.into_iter().map(Value::Symbol).collect::<Vec<_>>(),
                    ))
                }
            }
            "charset-after" => {
                need_arg_range(name, args, 0, 1)?;
                // charset.c:Fcharset_after: CHAR_CHARSET of the character
                // Fchar_after finds, nil where it finds none.
                let ch = super::buffer_edit::direct_char_after(interp, args, env)?;
                Ok(match ch.kind() {
                    Kind::Integer(code) => Value::Symbol(char_charset(interp, code as u32).0),
                    _ => ch,
                })
            }
            "find-charset-string" => {
                need_arg_range(name, args, 1, 2)?;
                // charset.c:Ffind_charset_string: CHECK_STRING, then the
                // string's characters (a unibyte one's bytes).
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                let (codes, multibyte) = {
                    let state = state.borrow();
                    (state.codes(), state.is_multibyte())
                };
                let table = args.get(1).copied().unwrap_or(Value::Nil);
                Ok(Value::list(find_charsets_in_text(
                    interp, &codes, multibyte, &table,
                )))
            }
            "find-charset-region" => {
                need_arg_range(name, args, 2, 3)?;
                // charset.c:Ffind_charset_region: validate_region, then the
                // region's characters (a unibyte buffer's bytes).
                let (start, end) = validate_region(interp, &args[0], &args[1])?;
                let codes = (start..end)
                    .map(|pos| public_buffer_char_code_at(interp, pos).unwrap_or(0) as u32)
                    .collect::<Vec<_>>();
                let multibyte = interp.buffer.borrow().is_multibyte();
                let table = args.get(2).copied().unwrap_or(Value::Nil);
                Ok(Value::list(find_charsets_in_text(
                    interp, &codes, multibyte, &table,
                )))
            }
            "map-charset-chars" => {
                if args.len() < 2 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let function = args[0];
                // CHECK_CHARSET_GET_CHARSET.
                let charset =
                    crate::lisp::primitives::charset::check_charset_get_id(interp, &args[1])?;
                let arg = args.get(2).cloned().unwrap_or(Value::Nil);
                // Fmap_charset_chars: FROM-CODE and TO-CODE as the C's
                // unsigned codes, raised to CHARSET_MIN_CODE and lowered to
                // CHARSET_MAX_CODE, a nil one (as a native caller passes an
                // omitted one) standing for them.
                let Some((min_code, max_code)) =
                    crate::lisp::primitives::coding::charset_code_bounds(interp, &charset)
                else {
                    return Err(LispError::WrongTypeArgument("charsetp".into(), args[1]));
                };
                // XFIXNUM of a code that is no fixnum reads its tagged word,
                // an address: taken as the largest code, a FROM-CODE maps
                // nothing and a TO-CODE stands for the maximum.
                let code = |value: &Value| match value.kind() {
                    Kind::Integer(n) => n as u32,
                    _ => u32::MAX,
                };
                let from = match args.get(3).filter(|from| !from.is_nil()) {
                    Some(from) => code(from).max(min_code),
                    None => min_code,
                };
                let to = match args.get(4).filter(|to| !to.is_nil()) {
                    Some(to) => code(to).min(max_code),
                    None => max_code,
                };
                use crate::lisp::primitives::coding::CharsetRanges;
                let segments = crate::lisp::primitives::coding::map_charset_char_segments(
                    interp, &charset, from, to,
                )
                .unwrap_or_default();
                for segment in segments {
                    match segment {
                        // map_char_table_for_charset's one RANGE cons, its car
                        // set at a run's first character, its cdr at the run's
                        // end, and its car nil again after each call.
                        CharsetRanges::Walk(ranges) => {
                            let range = Value::cons(Value::Nil, Value::Nil);
                            for (start, end) in ranges {
                                range.set_car(Value::Integer(start))?;
                                range.set_cdr(Value::Integer(end))?;
                                call_function_value(interp, &function, &[range, arg], env)?;
                                range.set_car(Value::Nil)?;
                            }
                        }
                        CharsetRanges::Offset(start, end) => {
                            call_function_value(
                                interp,
                                &function,
                                &[Value::cons(Value::Integer(start), Value::Integer(end)), arg],
                                env,
                            )?;
                        }
                    }
                }
                Ok(Value::Nil)
            }
            "define-charset-internal" => {
                crate::lisp::primitives::charset::define_charset_internal(interp, args, env)
            }
            "define-charset-alias" => {
                need_args(name, args, 2)?;
                crate::lisp::primitives::charset::define_charset_alias(interp, &args[0], &args[1])
            }
            "set-charset-plist" => {
                need_args(name, args, 2)?;
                crate::lisp::primitives::charset::set_charset_plist(interp, &args[0], args[1])
            }
            "unify-charset" => {
                // charset.c Funify_charset.
                need_arg_range(name, args, 1, 3)?;
                crate::lisp::primitives::charset::unify_charset(
                    interp,
                    &args[0],
                    args.get(1),
                    args.get(2).is_some_and(Value::is_truthy),
                )
            }
            "get-unused-iso-final-char" => {
                need_args(name, args, 2)?;
                crate::lisp::primitives::charset::get_unused_iso_final_char(
                    interp, &args[0], &args[1],
                )
            }
            "declare-equiv-charset" => {
                need_args(name, args, 4)?;
                crate::lisp::primitives::charset::declare_equiv_charset(interp, args)
            }
            "iso-charset" => {
                need_args(name, args, 3)?;
                crate::lisp::primitives::charset::iso_charset(interp, args)
            }
            "split-char" => {
                // charset.c Fsplit_char: CHAR_CHARSET's charset and the
                // bytes of ENCODE_CHAR's code, one per dimension.
                need_args(name, args, 1)?;
                let Some(character) = args[0]
                    .as_integer()
                    .ok()
                    .and_then(|value| u32::try_from(value).ok())
                    .filter(|value| *value <= 0x3f_ffff)
                else {
                    return Err(LispError::WrongTypeArgument("characterp".into(), args[0]));
                };
                let (charset, code) = char_charset(interp, character);
                let dimension =
                    crate::lisp::primitives::coding::charset_dimension(interp, &charset);
                let mut items = vec![Value::Symbol(charset)];
                for shift in (0..dimension).rev() {
                    items.push(Value::Integer(i64::from((code >> (8 * shift)) & 0xFF)));
                }
                Ok(Value::list(items))
            }
            "clear-charset-maps" => Ok(Value::Nil),
            "set-charset-priority" => {
                crate::lisp::primitives::charset::set_charset_priority(interp, args)
            }
            "sort-charsets" => {
                need_args(name, args, 1)?;
                crate::lisp::primitives::charset::sort_charsets(interp, &args[0], env)
            }
            "coding-system-p" => {
                need_args(name, args, 1)?;
                Ok(if coding_system_p(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "check-coding-system" => {
                need_args(name, args, 1)?;
                super::coding_systems::check_coding_system(interp, &args[0], env)
            }
            "read-coding-system" | "read-non-nil-coding-system" => {
                let non_nil = name == "read-non-nil-coding-system";
                need_arg_range(name, args, 1, if non_nil { 1 } else { 2 })?;
                let collection = interp
                    .lookup_var_key(cached_symbol!("coding-system-alist"), env)
                    .unwrap_or(Value::Nil);
                // coding.c: a symbol default is its name; read-coding-system
                // binds `completion-ignore-case' to t around the reading.
                let default = if non_nil {
                    Value::Nil
                } else {
                    match args.get(1).copied().unwrap_or(Value::Nil) {
                        default
                            if matches!(default.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T) =>
                        {
                            default.symbol_str().map_or(default, Value::string)
                        }
                        default => default,
                    }
                };
                let completion_args = [
                    args[0],
                    collection,
                    Value::Nil,
                    Value::T,
                    Value::Nil,
                    Value::symbol("coding-system-history"),
                    default,
                    Value::Nil,
                ];
                loop {
                    let result = if non_nil {
                        completing_read(interp, &completion_args, env)
                    } else {
                        Interpreter::push_bindings(
                            env,
                            vec![("completion-ignore-case".into(), Value::T)],
                        );
                        let result = completing_read(interp, &completion_args, env);
                        env.pop();
                        result
                    }?;
                    let entered = string_text(&result)?;
                    if entered.is_empty() {
                        if non_nil {
                            continue;
                        }
                        return Ok(Value::Nil);
                    }
                    return super::call(interp, "intern", &[result], env);
                }
            }
            "coding-system-priority-list" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let priority = interp.coding_system_priority_list();
                if args.first().is_some_and(Value::is_truthy) {
                    Ok(priority
                        .first()
                        .cloned()
                        .map(|value| Value::Symbol(value.into()))
                        .unwrap_or(Value::Nil))
                } else {
                    Ok(Value::list(
                        priority
                            .into_iter()
                            .map(|value| Value::Symbol(value.into()))
                            .collect::<Vec<_>>(),
                    ))
                }
            }
            "coding-system-aliases" | "coding-system-plist" | "coding-system-base" => {
                need_args(name, args, 1)?;
                // GNU treats nil as `no-conversion'.
                let coding = if args[0].is_nil() {
                    Value::symbol("no-conversion")
                } else {
                    args[0]
                };
                let spec = super::coding_systems::checked_coding_spec(interp, &coding, env)?;
                let attrs = interp.coding_specs[spec].attrs;
                Ok(match name {
                    "coding-system-aliases" => interp.coding_specs[spec].aliases,
                    "coding-system-plist" => interp.coding_attrs[attrs].plist,
                    _ => Value::Symbol(interp.coding_attrs[attrs].base),
                })
            }
            "coding-system-put" => {
                need_args(name, args, 3)?;
                super::coding_systems::coding_system_put(interp, &args[0], &args[1], &args[2], env)
            }
            "coding-system-eol-type" => {
                need_args(name, args, 1)?;
                // nil is `no-conversion', and what CODING_SYSTEM_P rejects
                // has no eol type.
                let spec = if args[0].is_nil() {
                    interp.coding_spec_index("no-conversion")
                } else {
                    args[0]
                        .symbol_cell()
                        .and_then(|symbol| interp.coding_spec_index_symbol(&symbol))
                };
                let Some(spec) = spec.map(|spec| &interp.coding_specs[spec]) else {
                    return Ok(Value::Nil);
                };
                Ok(match spec.eol_type.kind() {
                    Kind::Vector(subsidiaries) => Value::vector(subsidiaries.slots()),
                    _ => Value::Integer(if spec.eol_type.is_key(cached_symbol!("unix")) {
                        0
                    } else if spec.eol_type.is_key(cached_symbol!("dos")) {
                        1
                    } else {
                        2
                    }),
                })
            }
            "check-coding-systems-region" => {
                need_args(name, args, 3)?;
                check_coding_systems_region_value(interp, &args[0], args.get(1), &args[2], env)
            }
            "detect-coding-string" => {
                need_args(name, args, 1)?;
                detect_coding_string_value(interp, &args[0], args.get(1), env)
            }
            "detect-coding-region" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                detect_coding_region_value(interp, &args[0], &args[1], args.get(2), env)
            }
            "find-coding-systems-region-internal" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                find_coding_systems_region_internal_value(
                    interp,
                    &args[0],
                    &args[1],
                    args.get(2),
                    env,
                )
            }
            "decode-sjis-char" => {
                // coding.c Fdecode_sjis_char, converting through the
                // charsets of Vsjis_coding_system -- the LAST defined
                // shift-jis system, japanese-shift-jis-2004 in a full
                // load, so the kanji bank is JIS X 0213 plane 1 while
                // the `sjis' string codec stays on JIS X 0208 (both
                // answers oracle-probed: #x8940 is 38498 here and the
                // 0208 offset character through decode-coding-string).
                need_args(name, args, 1)?;
                // coding.c CHECK_FIXNAT: any natural fixnum passes and
                // oversized codes fall to the "Invalid code" validation
                // below (oracle rows).
                let code = check_fixnat(&args[0])?;
                let invalid = || LispError::Signal(format!("Invalid code: {code}"));
                if code <= 0x7F {
                    return Ok(Value::Integer(code));
                }
                let charsets = sjis_primitive_charsets(interp).ok_or_else(invalid)?;
                let decoded = if (0xA0..0xDF).contains(&code) {
                    decode_charset_code(interp, &charsets.1, code as u32 - 0x80)
                } else {
                    let (c1, c2) = (code >> 8, code & 0xFF);
                    if c1 < 0x81
                        || (c1 > 0x9F && c1 < 0xE0)
                        || c1 > 0xEF
                        || c2 < 0x40
                        || c2 == 0x7F
                        || c2 > 0xFC
                    {
                        return Err(invalid());
                    }
                    decode_charset_code(interp, &charsets.2, sjis_to_jis(code as u32))
                };
                decoded
                    .map(|character| Value::Integer(i64::from(character)))
                    .ok_or_else(invalid)
            }
            "encode-sjis-char" => {
                // coding.c Fencode_sjis_char: find the character in
                // Vsjis_coding_system's charset list and push the code
                // through JIS_TO_SJIS unconditionally (halfwidth kana
                // codes included -- the oracle answers 0x70AF for
                // U+FF71).  GNU's unencodable path reads past a null
                // charset pointer (it ABORTS on U+00A5 and returns
                // stack garbage elsewhere); emaxx takes the error the
                // docstring promises, disclosed in the ledger.
                need_args(name, args, 1)?;
                // CHECK_CHARACTER.
                let character = i64::from(check_character(&args[0])?);
                if character <= 0x7F {
                    return Ok(Value::Integer(character));
                }
                let coding = interp.sjis_coding_system.clone();
                let code = coding_system_charset_names(interp, &coding)
                    .iter()
                    .find_map(|charset| encode_charset_char(interp, charset, character as u32));
                match code {
                    Some(code) => Ok(Value::Integer(i64::from(jis_to_sjis(code)))),
                    None => Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Can't encode by shift_jis encoding: {}",
                        &[&char::from_u32(character as u32).unwrap_or('?')],
                    )),
                }
            }
            "decode-big5-char" => {
                // coding.c Fdecode_big5_char over Vbig5_coding_system's
                // charsets -- INCLUDING its bug: the second byte is
                // masked with 0x7F before validation, so every code
                // whose low byte has bit 7 set (half of Big5, 0xA4A4
                // among them) is rejected as invalid while the encode
                // direction happily produces it.  The oracle confirms
                // both halves of the asymmetry.
                need_args(name, args, 1)?;
                // CHECK_FIXNAT as above, no upper bound.
                let code = check_fixnat(&args[0])?;
                let invalid = || LispError::Signal(format!("Invalid code: {code}"));
                if code <= 0x7F {
                    return Ok(Value::Integer(code));
                }
                let (b1, b2) = (code >> 8, code & 0x7F);
                if !(0xA1..=0xFE).contains(&b1)
                    || b2 < 0x40
                    || (b2 > 0x7E && b2 < 0xA1)
                    || b2 > 0xFE
                {
                    return Err(invalid());
                }
                let charsets =
                    coding_system_charset_names(interp, &interp.big5_coding_system.clone());
                let big5 = charsets.get(1).ok_or_else(invalid)?;
                decode_charset_code(interp, big5, code as u32)
                    .map(|character| Value::Integer(i64::from(character)))
                    .ok_or_else(invalid)
            }
            "encode-big5-char" => {
                // coding.c Fencode_big5_char: the character's code in
                // Vbig5_coding_system's charset list, verbatim (no
                // JIS-style shuffling).  GNU's unencodable path shares
                // Fencode_sjis_char's null-charset read; emaxx signals
                // the docstring's error instead.
                need_args(name, args, 1)?;
                // CHECK_CHARACTER.
                let character = i64::from(check_character(&args[0])?);
                if character <= 0x7F {
                    return Ok(Value::Integer(character));
                }
                let coding = interp.big5_coding_system.clone();
                coding_system_charset_names(interp, &coding)
                    .iter()
                    .find_map(|charset| encode_charset_char(interp, charset, character as u32))
                    .map(|code| Value::Integer(i64::from(code)))
                    .ok_or_else(|| {
                        crate::lisp::primitives::values::doprnt_error(
                            interp,
                            "Can't encode by Big5 encoding: {}",
                            &[&char::from_u32(character as u32).unwrap_or('?')],
                        )
                    })
            }
            "terminal-coding-system" | "keyboard-coding-system" => {
                need_arg_range(name, args, 0, 1)?;
                let target = args.first().unwrap_or(&Value::Nil);
                let terminal = interp
                    .decode_terminal(target)
                    .ok_or_else(|| wrong_type_argument("terminal-live-p", *target))?;
                let terminal = terminal.borrow();
                let coding = if name == "terminal-coding-system" {
                    &terminal.terminal_coding
                } else {
                    &terminal.keyboard_coding
                };
                Ok(coding
                    .as_deref()
                    .filter(|coding| name != "terminal-coding-system" || *coding != "undecided")
                    .map(Value::symbol)
                    .unwrap_or(Value::Nil))
            }
            "set-terminal-coding-system-internal" | "set-keyboard-coding-system-internal" => {
                need_arg_range(name, args, 1, 2)?;
                let target = args.get(1).unwrap_or(&Value::Nil);
                let terminal = interp
                    .decode_terminal(target)
                    .ok_or_else(|| wrong_type_argument("terminal-live-p", *target))?;
                // coding.c keeps the coding system as given, an alias
                // naming the terminal's coding by its own name.
                load_coding_system(interp, &args[0], env)?;
                let coding = checked_coding_given(interp, &args[0])?;
                if name == "set-terminal-coding-system-internal" {
                    // tset_charset_list: the coding's charsets when it
                    // encodes at all, else ASCII alone (nil sets up
                    // `undecided', which converts nothing).
                    let encodes = coding.as_deref().is_some_and(|coding| {
                        !matches!(
                            interp.coding_system_kind_name(coding).as_deref(),
                            Some("no-conversion" | "raw-text" | "undecided")
                        )
                    });
                    let mut terminal = terminal.borrow_mut();
                    terminal.charset_list_coding = Some(match &coding {
                        Some(coding) if encodes => coding.clone(),
                        _ => "us-ascii".into(),
                    });
                    terminal.terminal_coding = coding;
                } else {
                    terminal.borrow_mut().keyboard_coding =
                        coding.or_else(|| Some("no-conversion".into()));
                }
                Ok(Value::Nil)
            }
            "set-safe-terminal-coding-system-internal" => {
                need_args(name, args, 1)?;
                load_coding_system(interp, &args[0], env)?;
                let coding = checked_coding_given(interp, &args[0])?;
                interp.safe_terminal_coding = coding;
                Ok(Value::Nil)
            }
            "find-operation-coding-system" => find_operation_coding_system_value(interp, args, env),
            "set-coding-system-priority" => {
                let mut codings = Vec::with_capacity(args.len());
                for coding in args {
                    super::coding_systems::checked_coding_spec(interp, coding, env)?;
                    codings.push(match coding.kind() {
                        Kind::T => crate::lisp::types::symbols::builtin_boolean(true),
                        _ => coding.symbol_cell().expect("a coding system is a symbol"),
                    });
                }
                interp.set_coding_system_categories_priority(&codings, env)?;
                Ok(Value::Nil)
            }
            "define-coding-system-internal" => {
                super::coding_systems::define_coding_system_internal(interp, args, env)
            }
            "define-coding-system-alias" => {
                need_args(name, args, 2)?;
                super::coding_systems::define_coding_system_alias(interp, &args[0], &args[1], env)
            }
        }
    }
);

/// buffer.c:Fother_buffer: the most recently selected buffer other than
/// BUFFER (when BUFFER is a buffer), live and not hidden
/// (candidate_buffer), that FRAME's buffer predicate accepts, among
/// FRAME's buffers and then all of them; one no window shows unless
/// VISIBLE_OK, else the first one shown; else *scratch*, made when there
/// is none.
pub(crate) fn other_buffer(
    interp: &mut Interpreter,
    buffer: Value,
    visible_ok: bool,
    frame: crate::lisp::types::FrameRef,
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let predicate =
        super::frames::frame_parameter_value(interp, frame, cached_symbol!("buffer-predicate"));
    let excluded = match buffer.kind() {
        Kind::Buffer(buffer) => Some(buffer.id),
        _ => None,
    };
    let candidates: Vec<u64> = frame
        .borrow()
        .buffer_list
        .iter()
        .copied()
        .chain(interp.buffer_list.iter().map(|(id, _)| *id))
        .collect();
    let mut not_so_good = None;
    for id in candidates {
        // The predicate may have killed or renamed a buffer.
        let hidden = match interp.buffer_list.iter().find(|(other, _)| *other == id) {
            Some((_, name)) => name.starts_with(' '),
            None => true,
        };
        if Some(id) == excluded || hidden {
            continue;
        }
        let candidate = interp.buffer_value(id).expect("live buffer object");
        if !predicate.is_nil()
            && call_function_value(interp, &predicate, &[candidate], env)?.is_nil()
        {
            continue;
        }
        if visible_ok
            || super::display::buffer_window(interp, id, &Value::symbol("visible")).is_none()
        {
            return Ok(candidate);
        }
        not_so_good.get_or_insert(candidate);
    }
    if let Some(candidate) = not_so_good {
        return Ok(candidate);
    }
    if let Some(id) = interp.find_buffer_id("*scratch*") {
        return Ok(interp.buffer_value(id).expect("live buffer object"));
    }
    let scratch = super::call(
        interp,
        "get-buffer-create",
        &[Value::string("*scratch*"), Value::Nil],
        env,
    )?;
    super::call(interp, "set-buffer-major-mode", &[scratch], env)?;
    Ok(scratch)
}

/// Fmake_local_variable's and Fmake_variable_buffer_local's refusal of a
/// keyboard's slot (KBOARD_OBJFWDP), named by VARIABLE as given, before
/// any other check.
pub(super) fn check_may_be_buffer_local(
    interp: &Interpreter,
    variable: &crate::lisp::types::SymbolName,
    symbol: &crate::lisp::types::SymbolName,
) -> Result<(), LispError> {
    if interp.symbol_redirect(symbol) != crate::lisp::eval::Redirect::KeyboardSlot {
        return Ok(());
    }
    Err(crate::lisp::primitives::values::doprnt_error(
        interp,
        "Symbol {} may not be buffer-local",
        &[&crate::lisp::types::visible_symbol_name(variable.as_str())],
    ))
}

/// data.c:buffer_local_value's Fdefault_value of SYMBOL (no alias) for a
/// buffer without a binding of it: a localized variable's default, void
/// when that is (the current buffer's own binding must not answer), and
/// any other's value as its cell holds it.
fn default_value_here(
    interp: &Interpreter,
    symbol: &crate::lisp::types::SymbolName,
) -> Result<Option<Value>, LispError> {
    Ok(match interp.default_value_symbol(symbol)? {
        Some(value) => Some(value),
        None if interp.symbol_redirect(symbol) == crate::lisp::eval::Redirect::Localized => None,
        None => interp.symbol_value_cell_symbol(symbol).ok(),
    })
}
