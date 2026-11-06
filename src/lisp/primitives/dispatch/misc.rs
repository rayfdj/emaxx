use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

/// fringe.c's MAX_STANDARD_FRINGE_BITMAPS: the standard bitmaps' numbers
/// and NO_FRINGE_BITMAP's 0 below it.
fn max_standard_fringe_bitmaps() -> i64 {
    STANDARD_FRINGE_BITMAPS.len() as i64 + 1
}

/// fringe.c:lookup_fringe_bitmap: the number BITMAP's `fringe' property
/// holds, when it is a standard bitmap's or one defined now; else none.
fn lookup_fringe_bitmap(interp: &Interpreter, bitmap: &Value) -> Option<i64> {
    let number = interp
        .get_property_of_symbol_value(*bitmap, cached_symbol!("fringe"))?
        .integer_value()?;
    // max_used_fringe_bitmap: one past the highest number in use.
    let max_used = interp
        .fringe_bitmap_states
        .iter()
        .map(|state| state.id + 1)
        .max()
        .unwrap_or(0)
        .max(max_standard_fringe_bitmaps());
    (number > 0
        && number < max_used
        && (number < max_standard_fringe_bitmaps()
            || interp
                .fringe_bitmap_states
                .iter()
                .any(|state| state.id == number)))
    .then_some(number)
}

/// CHECK_SYMBOL of a fringe bitmap argument: the bare symbol, which
/// `Fget' and `Fput' reach through XSYMBOL of a positioned one while
/// `symbols-with-pos-enabled'.
fn check_fringe_symbol(interp: &Interpreter, value: &Value) -> Result<Value, LispError> {
    crate::lisp::primitives::interactive::check_symbol(interp, *value)
}

fn fringe_bits_length(value: &Value) -> Result<usize, LispError> {
    if is_vector_value(value) {
        return Ok(vector_items(value)?.len());
    }
    if let Some(string) = string_like(value) {
        return Ok(string.text.chars().count());
    }
    Err(wrong_type_argument("arrayp", *value))
}

fn fringe_fixnum(value: &Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(value) => Ok(value),
        _ => Err(wrong_type_argument("fixnump", *value)),
    }
}

fn define_fringe_bitmap(
    interp: &mut Interpreter,
    args: &[Value],
    env: &Env,
) -> Result<Value, LispError> {
    let symbol = check_fringe_symbol(interp, &args[0])?;
    let bits_length = fringe_bits_length(&args[1])?;
    let height = match args.get(2).filter(|value| !value.is_nil()) {
        Some(value) => fringe_fixnum(value)?.clamp(0, 255),
        None => bits_length.min(255) as i64,
    };
    let width = match args.get(3).filter(|value| !value.is_nil()) {
        Some(value) => {
            let width = fringe_fixnum(value)?;
            if !(1..=16).contains(&width) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("args-out-of-range"),
                    *value,
                    Value::String("Width must be from 1 to 16".into()),
                ])));
            }
            width
        }
        None => 8,
    };
    let mut align = args.get(4).cloned().unwrap_or(Value::Nil);
    let mut periodic = false;
    if let Some((head, tail)) = align.cons_values() {
        if let Some((period, _)) = tail.cons_values() {
            periodic = period.is_truthy();
        }
        align = head;
    }
    if !align.is_nil()
        && !align.is_key(cached_symbol!("top"))
        && !align.is_key(cached_symbol!("bottom"))
        && !align.is_key(cached_symbol!("center"))
    {
        return Err(LispError::Signal("Bad align argument".into()));
    }

    let existing_id = lookup_fringe_bitmap(interp, &symbol);
    let bitmap_id = existing_id.unwrap_or_else(|| {
        interp
            .fringe_bitmap_states
            .iter()
            .map(|bitmap| bitmap.id)
            .max()
            .unwrap_or(STANDARD_FRINGE_BITMAPS.len() as i64)
            + 1
    });
    if existing_id.is_none() {
        let mut bitmaps = vec![args[0]];
        bitmaps.extend(
            interp
                .lookup_var_key(cached_symbol!("fringe-bitmaps"), env)
                .and_then(|value| value.to_vec().ok())
                .unwrap_or_default(),
        );
        interp.set_global_binding_key(cached_symbol!("fringe-bitmaps"), Value::list(bitmaps));
        // Fput (bitmap, Qfringe, ...) on the symbol itself.
        if let Some(symbol) = symbol.symbol_cell() {
            interp.put_symbol_property_on(&symbol, "fringe", Value::Integer(bitmap_id));
        }
    }
    let definition = Value::list([
        args[1],
        Value::Integer(height),
        Value::Integer(width),
        align,
        if periodic { Value::T } else { Value::Nil },
    ]);
    if let Some(bitmap) = interp
        .fringe_bitmap_states
        .iter_mut()
        .find(|bitmap| bitmap.id == bitmap_id)
    {
        bitmap.definition = Some(definition);
        // GNU destroys the old platform bitmap before replacing it, which
        // also resets any face override.
        bitmap.face = Value::Nil;
    } else {
        interp
            .fringe_bitmap_states
            .push(crate::lisp::eval::FringeBitmapState {
                id: bitmap_id,
                standard: false,
                definition: Some(definition),
                face: Value::Nil,
            });
    }
    Ok(args[0])
}

fn destroy_fringe_bitmap(
    interp: &mut Interpreter,
    bitmap: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    let symbol = check_fringe_symbol(interp, bitmap)?;
    let Some(id) = lookup_fringe_bitmap(interp, &symbol) else {
        return Ok(Value::Nil);
    };
    let Some(index) = interp
        .fringe_bitmap_states
        .iter()
        .position(|bitmap| bitmap.id == id)
    else {
        return Ok(Value::Nil);
    };
    if interp.fringe_bitmap_states[index].standard {
        interp.fringe_bitmap_states[index].definition = None;
        interp.fringe_bitmap_states[index].face = Value::Nil;
    } else {
        interp.fringe_bitmap_states.remove(index);
        if let Some(symbol) = symbol.symbol_cell() {
            interp.put_symbol_property_on(&symbol, "fringe", Value::Nil);
        }
        let bitmaps = interp
            .lookup_var_key(cached_symbol!("fringe-bitmaps"), env)
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .into_iter()
            .filter(|candidate| !values_eq_in_env(interp, candidate, bitmap, env))
            .collect::<Vec<_>>();
        interp.set_global_binding_key(cached_symbol!("fringe-bitmaps"), Value::list(bitmaps));
    }
    Ok(Value::Nil)
}

/// Bare name of VALUE when it is a symbol, or a positioned symbol while
/// `symbols-with-pos-enabled' is non-nil — the same view GNU's `eq' takes
/// inside `assq'/`plist-get' during byte compilation.
/// `bare_symbol_name' without the name copy: the symbol itself.
/// Fintern and Fintern_soft's oblookup of a string NAME (ARGS' first)
/// when they would look it up in the initial obarray as it is: no
/// OBARRAY argument and `obarray' holding that one, and no
/// `read-symbol-shorthands' to rewrite it.  The symbol found, or None
/// (not found, or any other case) for the full arm.
fn standard_oblookup(
    interp: &Interpreter,
    args: &[Value],
) -> Option<crate::lisp::types::SymbolName> {
    if args.get(1).is_some_and(|obarray| !obarray.is_nil()) {
        return None;
    }
    let obarray = interp.symbol_value_key(cached_symbol!("obarray"))?;
    if !matches!(obarray.kind(), Kind::Record(id) if interp.is_standard_obarray_id(id.id))
        || interp
            .symbol_value_key(cached_symbol!("read-symbol-shorthands"))
            .is_some_and(|shorthands| !shorthands.is_nil())
    {
        return None;
    }
    // oblookup by the string's own bytes and characters.
    crate::lisp::types::SymbolName::lookup_initial(args[0]).and_then(Result::ok)
}

pub(super) fn bare_symbol_identity(
    interp: &Interpreter,
    env: &Env,
    value: &Value,
) -> Option<crate::lisp::types::SymbolName> {
    match value.kind() {
        Kind::Symbol(symbol) => return Some(symbol),
        Kind::Nil => return Some(crate::lisp::types::symbols::builtin_boolean(false)),
        Kind::T => return Some(crate::lisp::types::symbols::builtin_boolean(true)),
        _ => {}
    }
    if symbols_with_pos_enabled(interp, env)
        && let Some((bare, _)) = symbol_with_pos_parts(interp, value)
    {
        return match bare.kind() {
            Kind::Symbol(symbol) => Some(symbol),
            Kind::Nil => Some(crate::lisp::types::symbols::builtin_boolean(false)),
            Kind::T => Some(crate::lisp::types::symbols::builtin_boolean(true)),
            _ => None,
        };
    }
    None
}

pub(super) fn bare_symbol_name(interp: &Interpreter, env: &Env, value: &Value) -> Option<String> {
    if let Ok(symbol) = value.as_symbol() {
        return Some(symbol.to_string());
    }
    if symbols_with_pos_enabled(interp, env)
        && let Some((bare, _)) = symbol_with_pos_parts(interp, value)
        && let Ok(symbol) = bare.as_symbol()
    {
        return Some(symbol.to_string());
    }
    None
}

fn symbol_property_by_eq(
    interp: &Interpreter,
    env: &Env,
    symbol: &crate::lisp::types::SymbolName,
    property: &Value,
) -> Option<Value> {
    let mut tail = interp.symbol_plist_symbol(symbol);
    let mut guard = crate::lisp::types::CycleGuard::new();
    while let Kind::Cons(ref cell) = tail.kind() {
        if guard.step(crate::lisp::types::ConsCell::identity(cell)) {
            return None;
        }
        let key = tail.car_if_list()?;
        let rest = tail.cdr_if_list()?;
        let value = rest.car_if_list()?;
        if values_eq_in_env(interp, &key, property, env) {
            return Some(value);
        }
        tail = rest.cdr_if_list()?;
    }
    None
}

/// The non-nil value of PROPERTY for SYMBOL from the first matching
/// `overriding-plist-environment' entry, GNU fns.c:Fget's pre-plist source.
fn overriding_plist_property(
    interp: &Interpreter,
    env: &Env,
    symbol: &crate::lisp::types::SymbolName,
    property: &crate::lisp::types::SymbolName,
) -> Option<Value> {
    // fns.c:Fget reads Voverriding_plist_environment directly.  Its
    // DEFVAR_LISP cell already reflects any active dynamic binding.
    let mut entries = interp.overriding_plist_environment_value();
    if entries.is_nil() {
        return None;
    }
    let mut entry_guard = crate::lisp::types::CycleGuard::new();
    while let Kind::Cons(ref entries_cell) = entries.kind() {
        if entry_guard.step(crate::lisp::types::ConsCell::identity(entries_cell)) {
            break;
        }
        let entry = entries.car_if_list()?;
        // Fassq: the entry's key EQ to SYMBOL.
        let entry_key = entry
            .car()
            .ok()
            .and_then(|key| bare_symbol_identity(interp, env, &key));
        if entry_key.as_ref() == Some(symbol) {
            let mut plist = entry.cdr_if_list()?;
            let mut plist_guard = crate::lisp::types::CycleGuard::new();
            while let Kind::Cons(ref plist_cell) = plist.kind() {
                if plist_guard.step(crate::lisp::types::ConsCell::identity(plist_cell)) {
                    break;
                }
                let key = plist.car_if_list()?;
                let rest = plist.cdr_if_list()?;
                if !matches!(rest.kind(), Kind::Cons(_)) {
                    break;
                }
                if bare_symbol_identity(interp, env, &key).as_ref() == Some(property) {
                    let value = rest.car_if_list()?;
                    // plist_get stops at the first key, even when its
                    // value is nil. Fget then falls back to the real
                    // plist instead of examining a later duplicate.
                    return (!value.is_nil()).then_some(value);
                }
                plist = rest.cdr_if_list()?;
            }
            // GNU reads only the first assq hit before the real plist.
            return None;
        }
        entries = entries.cdr_if_list()?;
    }
    None
}

fn macroexpand_dispatch(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if args.is_empty() || args.len() > 2 {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    let environment = args.get(1).filter(|value| value.is_truthy());
    let mut form = args[0];
    loop {
        let expanded = interp.macroexpand_1_form_with_environment(&form, environment, env)?;
        if expanded == form {
            return Ok(form);
        }
        form = expanded;
    }
}

/// eval.c:Frun_hooks: each HOOK run in turn by run_hook, whose
/// find_symbol_value is CHECK_SYMBOL's.
fn run_hooks_dispatch(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    for hook in args {
        let hook = checked_symbol_identity(interp, hook, env)?;
        run_hook(interp, &hook, env)?;
    }
    Ok(Value::Nil)
}

/// data.c:set_internal for the symbol in hand: its alias chain, flags,
/// scope and cells are read from the object (see `resolve_variable_symbol').
pub(crate) fn set_internal_symbol(
    interp: &mut Interpreter,
    symbol: &crate::lisp::types::SymbolName,
    value: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    // data.c:set_internal's SYMBOL_PLAINVAL, untrapped: the store alone;
    // SYMBOL_LOCALIZED with the current buffer's binding loaded: the
    // store into it alone.
    if interp.assign_plain_global(symbol, value) || interp.assign_loaded_local(symbol, value) {
        return Ok(());
    }
    if interp.check_constant_assignment_symbol(symbol, value)? {
        return Ok(());
    }
    let resolved = interp.resolve_variable_symbol(symbol)?;
    // data.c:set_internal notifies with NEWVAL before
    // store_symval_forwarding checks or normalizes it: the watchers hear
    // the caller's object, a DEFVAR_BOOL's t/nil being what is stored, and
    // a DEFVAR_INT's non-integer only after them.
    interp.notify_assignment_symbol(&resolved, value, env)?;
    let stored = interp.prepare_forwarded_assignment_symbol(&resolved, value)?;
    interp.check_per_buffer_set(&resolved, value, env)?;
    interp.set_symbol_value_cell_resolved(&resolved, stored);
    if resolved == *symbol {
        interp.learn_plain_store(symbol);
    }
    Ok(())
}

/// data.c:set_default_internal checks the original object's trapped_write,
/// then follows its alias redirect to the actual default cell.
pub(crate) fn set_default_internal_symbol(
    interp: &mut Interpreter,
    symbol: &crate::lisp::types::SymbolName,
    value: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    if interp.store_default_value(symbol, value) {
        return Ok(());
    }
    if interp.check_constant_assignment_symbol(symbol, value)? {
        return Ok(());
    }
    let resolved = interp.resolve_variable_symbol(symbol)?;
    // set_default_internal tells the watchers of a symbol that is not
    // SYMBOL_PLAINVAL (an alias is SYMBOL_VARALIAS), then, for one whose
    // variable is plain or a C variable, stores through set_internal,
    // which tells them again; store_symval_forwarding's checks follow.
    if resolved.cell().is_trapped_write() {
        use crate::lisp::eval::Redirect;
        let redirect = interp.symbol_redirect(&resolved);
        if resolved != *symbol || redirect != Redirect::Plain {
            interp.notify_variable_watchers_symbol(&resolved, value, "set_default", None, env)?;
        }
        if matches!(redirect, Redirect::Plain | Redirect::Forwarded) {
            interp.notify_variable_watchers_symbol(&resolved, value, "set", None, env)?;
        }
    }
    let stored = interp.prepare_forwarded_assignment_symbol(&resolved, value)?;
    interp.set_global_binding_resolved(&resolved, stored);
    // A plain variable's default store is set_internal's (data.c's
    // set_default_internal for SYMBOL_PLAINVAL), learned as that is.
    if resolved == *symbol && !symbol.cell().plain_store() {
        interp.learn_plain_store(symbol);
    }
    Ok(())
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            // ── Reader ──
            "read" | "read-positioning-symbols" => {
                need_arg_range(name, args, 0, 1)?;
                let source = args
                    .first()
                    .filter(|source| !source.is_nil())
                    .cloned()
                    .or_else(|| interp.lookup_var_key(cached_symbol!("standard-input"), env))
                    .unwrap_or(Value::Nil);
                // lread.c:Fread and Fread_positioning_symbols share these
                // defaults and resolve read-minibuffer through its cell.
                if matches!(source.kind(), Kind::T) || source.is_key(cached_symbol!("read-char")) {
                    call_named_function(
                        interp,
                        "read-minibuffer",
                        &[Value::string("Lisp expression: ")],
                        env,
                    )
                } else if name == "read-positioning-symbols" {
                    read_positioning_symbols_from_lisp_source(interp, &source, env)
                } else {
                    read_from_lisp_source(interp, &source, env)
                }
            }
            "read-from-string" => {
                if args.is_empty() || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                };
                let contents = state.borrow();
                let length = contents.len();
                // lread.c:read_internal_start uses fns.c:validate_subarray:
                // check both types before the joint range and retain the
                // caller's original indices in the signaled condition.
                let (start, end) = validate_subarray(
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                    args.get(2).copied().unwrap_or(Value::Nil),
                    length,
                )?;
                // read_internal_start converts only START, through fns.c's
                // string_char_to_byte cache; reading on from where the
                // last read stopped scans only the characters between.
                let first = contents.char_to_byte(start);
                let last = if end == length {
                    contents.bytes().len()
                } else {
                    contents.bytes_after_chars(first, end - start)
                };
                match read_encoded_form_in_env(
                    interp,
                    &contents.bytes()[first..last],
                    contents.is_multibyte(),
                    env,
                ) {
                    Ok((val, consumed)) => {
                        Ok(Value::cons(val, Value::Integer((start + consumed) as i64)))
                    }
                    Err(error) => Err(error),
                }
            }
            "md5" => {
                need_arg_range(name, args, 1, 5)?;
                let bytes = md5_source_bytes(interp, args, env)?;
                Ok(Value::String(crate::lisp::md5::md5_hex(&bytes).into()))
            }
            "secure-hash" => {
                need_arg_range(name, args, 2, 5)?;
                // CHECK_SYMBOL; the algorithm is chosen by EQ once the
                // data is extracted.
                let algorithm = checked_symbol_identity(interp, &args[0], env)?;
                secure_hash_value(
                    interp,
                    algorithm,
                    &args[1],
                    args.get(2),
                    args.get(3),
                    args.get(4),
                    env,
                )
            }
            // hex-util.el (native for speed; the feature is compat-preloaded).
            "secure-hash-algorithms" => {
                need_args(name, args, 0)?;
                Ok(Value::list([
                    Value::Symbol("md5".into()),
                    Value::Symbol("sha1".into()),
                    Value::Symbol("sha224".into()),
                    Value::Symbol("sha256".into()),
                    Value::Symbol("sha384".into()),
                    Value::Symbol("sha512".into()),
                ]))
            }
            "buffer-hash" => {
                need_arg_range(name, args, 0, 1)?;
                buffer_hash_value(interp, args.first())
            }

            // ── Misc ──
            // xsmfns.c:Fhandle_save_session: with no session manager
            // interaction pending, an event (_ t ...) kills Emacs.
            "play-sound-internal" => {
                need_args(name, args, 1)?;
                super::super::sound::play_sound_internal(interp, args[0], env)
            }
            "handle-save-session" => {
                need_args(name, args, 1)?;
                let kill = args[0]
                    .cons_values()
                    .and_then(|(_, rest)| rest.cons_values())
                    .is_some_and(|(flag, _)| flag == Value::T);
                if kill {
                    super::call(interp, "kill-emacs", &[Value::Nil, Value::Nil], env)?;
                }
                Ok(Value::Nil)
            }
            "kill-emacs" => {
                need_arg_range(name, args, 0, 2)?;

                // In supported Emaxx use the Lisp runtime is owned by the batch
                // process.  GNU uses safe_run_hooks in this mode: an ordinary
                // hook error is reported but cannot cancel orderly shutdown.
                safe_run_hooks(interp, &cached_symbol!("kill-emacs-hook").symbol(), env)?;
                // shut_down_emacs (0, ...): no hook runs from now on, each
                // buffer is auto-saved and the files unlocked (the
                // subprocesses are hung up when the process ends).
                interp.hooks_cleared = true;
                crate::lisp::primitives::file_io::do_auto_save(interp, env, Value::T, Value::Nil)?;
                unlock_all_files(interp, env)?;
                // The auto-save list goes, Emacs exiting deliberately.
                if let Some(list_file) = interp
                    .lookup_var_key(cached_symbol!("auto-save-list-file-name"), env)
                    .filter(Value::is_string)
                {
                    let list_file = crate::lisp::primitives::call(
                        interp,
                        "expand-file-name",
                        &[list_file, Value::Nil],
                        env,
                    )?;
                    if let Ok(host) = crate::lisp::primitives::system::host_file_name(
                        interp,
                        env,
                        &string_text(&list_file)?,
                    ) {
                        let _ = std::fs::remove_file(host);
                    }
                }

                // emacs.c accepts only a fixnum here.  Preserve its explicit
                // INT_MIN/INT_MAX masking before the CLI boundary narrows the
                // platform status to what the parent process can observe.
                let exit_code = match args.first().map(|v| v.kind()) {
                    Some(Kind::Integer(value)) if value < 0 => {
                        ((value as u32) | (i32::MIN as u32)) as i32
                    }
                    Some(Kind::Integer(value)) => ((value as u32) & (i32::MAX as u32)) as i32,
                    _ => 0,
                };
                let termination = EmacsTermination {
                    exit_code,
                    restart: args.get(1).is_some_and(Value::is_truthy),
                };
                interp.request_termination(termination.clone());
                Err(LispError::Terminate(termination))
            }
            "signal" => {
                if args.is_empty() {
                    return Err(LispError::Signal("signal".into()));
                }
                let mut condition = args[0];
                let data = args.get(1).cloned().unwrap_or(Value::Nil);
                // eval.c:Fsignal: "If they call us with nonsensical
                // arguments, produce "peculiar error"."
                if condition.is_nil() && data.cons_values().is_none() {
                    condition = Value::symbol("error");
                }
                // signal_or_quit's error object: DATA itself for a nil
                // symbol (the memory-full form), a non-symbol without data
                // as it is, else the symbol consed onto DATA, a list or not
                // ((signal 'foo 4) is caught as (foo . 4), a list's cells
                // shared).  Its car's conditions are read with Fget, whose
                // CHECK_SYMBOL refuses anything else.
                let is_symbol =
                    |value: &Value| matches!(value.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T);
                let error = if condition.is_nil() {
                    data
                } else if !is_symbol(&condition) && data.is_nil() {
                    condition
                } else {
                    Value::cons(condition, data)
                };
                let real = error.cons_values().map_or(condition, |(car, _)| car);
                if !is_symbol(&real) {
                    return Err(wrong_type_argument("symbolp", real));
                }
                Err(LispError::SignalValue(error))
            }
            "throw" => {
                if args.len() != 2 {
                    return Err(LispError::WrongNumberOfArgs("throw".into(), args.len()));
                }
                interp.throw_value(args[0], args[1], env)
            }
            "defalias" => {
                need_arg_range(name, args, 2, 3)?;
                interp.defalias_value(args, env)
            }
            "provide" => {
                need_arg_range(name, args, 1, 2)?;
                // GNU 30.2 fns.c:Fprovide uses CHECK_SYMBOL/XSYMBOL, so
                // positioned reader symbols denote their underlying symbol
                // while `symbols-with-pos-enabled' is dynamically non-nil.
                let feature = checked_symbol_identity(interp, &args[0], env)?;
                let subfeatures = args.get(1).cloned().unwrap_or(Value::Nil);
                // CHECK_LIST, even when the feature was already present.
                if !matches!(subfeatures.kind(), Kind::Nil | Kind::Cons(_)) {
                    return Err(wrong_type_argument("listp", subfeatures));
                }
                interp
                    .provide_feature_with_after_load(feature, subfeatures, env)
                    .map(|_| args[0])
            }
            "require" => {
                need_arg_range(name, args, 1, 3)?;
                // Like `provide', GNU's Frequire accepts a positioned symbol
                // through CHECK_SYMBOL and returns the original object.
                let feature = checked_symbol_identity(interp, &args[0], env)?;
                let filename = args.get(1).cloned().unwrap_or(Value::Nil);
                let noerror = args.get(2).cloned().unwrap_or(Value::Nil);
                // NOERROR goes to Fload, where it excuses only the file's
                // absence: an error the found file signals propagates.
                let loaded = interp.require_feature_with_target(feature, filename, noerror, env)?;
                Ok(if loaded.is_nil() { Value::Nil } else { args[0] })
            }
            "define-fringe-bitmap" => {
                need_arg_range(name, args, 2, 5)?;
                define_fringe_bitmap(interp, args, env)
            }
            "destroy-fringe-bitmap" => {
                need_args(name, args, 1)?;
                destroy_fringe_bitmap(interp, &args[0], env)
            }
            "set-fringe-bitmap-face" => {
                need_arg_range(name, args, 1, 2)?;
                let symbol = check_fringe_symbol(interp, &args[0])?;
                let Some(bitmap) = lookup_fringe_bitmap(interp, &symbol).and_then(|id| {
                    interp
                        .fringe_bitmap_states
                        .iter_mut()
                        .find(|candidate| candidate.id == id)
                }) else {
                    return Err(LispError::Signal("Undefined fringe bitmap".into()));
                };
                bitmap.face = args.get(1).cloned().unwrap_or(Value::Nil);
                Ok(Value::Nil)
            }
            "fringe-bitmaps-at-pos" => {
                need_arg_range(name, args, 0, 2)?;
                let window = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .unwrap_or_else(|| interp.selected_window_value());
                let Some(window_id) = window_record_id_from_value(interp, &window) else {
                    return Err(wrong_type_argument("windowp", window));
                };
                // decode_any_window, then the position: POS, point in the
                // selected window, or marker_position (w->pointm), which
                // signals for a deleted window's unchained marker.
                if args.first().is_none_or(Value::is_nil)
                    && window_id != interp.selected_window_id()
                    && crate::lisp::primitives::window::window_marker(
                        interp,
                        window_id,
                        crate::lisp::primitives::WINDOW_POINT_SLOT,
                    )
                    .is_some_and(|marker| marker.position().is_none())
                {
                    return Err(LispError::Signal("Marker does not point anywhere".into()));
                }
                if let Some(position) = args.first().filter(|value| !value.is_nil()) {
                    let position_value = position_from_value(interp, position)?;
                    if position_value < interp.buffer.borrow().point_min()
                        || position_value > interp.buffer.borrow().point_max()
                    {
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("args-out-of-range"),
                            window,
                            *position,
                        ])));
                    }
                }
                // The headless renderer retains no glyph matrix, so no display
                // row can own fringe bitmaps.  GNU returns nil in this state.
                Ok(Value::Nil)
            }
            "intern" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                if let Some(symbol) = standard_oblookup(interp, args) {
                    return Ok(Value::Symbol(symbol));
                }
                let obarray = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .or_else(|| {
                        interp
                            .lookup_var_key(cached_symbol!("obarray"), env)
                            .filter(|value| !value.is_nil())
                    })
                    .map(|obarray| coerce_legacy_vector_obarray(interp, &obarray))
                    .transpose()?;
                if let Some(obarray) = &obarray
                    && !is_obarray_like_value(interp, obarray)
                {
                    return Err(wrong_type_argument("obarrayp", *obarray));
                }
                // lread.c:Fintern checks OBARRAY before STRING, then supplies
                // the original string (or GNU's longhand/purecopy) on a miss.
                let original_name = string_text(&args[0])?;
                let symbol_name = apply_symbol_shorthands_in_env(interp, &original_name, env)?;
                // Fintern's make_specified_string explicitly marks longhand
                // as multibyte, including ASCII longhand; oblookup compares
                // the longhand, or else STRING, by its bytes.
                let longhand = (symbol_name != original_name).then(|| {
                    make_shared_string_value_with_multibyte(symbol_name.clone(), None, true)
                });
                let key = longhand.unwrap_or(args[0]);
                let mut make_name = |interp: &mut Interpreter| match longhand {
                    Some(longhand) => Ok(longhand),
                    None => purecopy_value(interp, &args[0], env),
                };
                if let Some(obarray) = obarray {
                    intern_in_obarray_with_name(
                        interp,
                        &obarray,
                        &symbol_name,
                        Some(key),
                        make_name,
                    )
                } else {
                    if !interp.standard_obarray_contains_symbol(&symbol_name) {
                        let lisp_name = make_name(interp)?;
                        crate::lisp::types::SymbolName::intern_with_lisp_name(
                            symbol_name.clone(),
                            Some(lisp_name),
                        );
                    }
                    interp.intern_symbol_name(&symbol_name);
                    Ok(crate::lisp::types::interned_symbol_value(symbol_name))
                }
            }
            "intern-soft" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let obarray = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .or_else(|| {
                        interp
                            .lookup_var_key(cached_symbol!("obarray"), env)
                            .filter(|value| !value.is_nil())
                    });
                // lread.c:Fintern_soft: check_obarray before NAME is looked
                // at.
                let obarray = match obarray {
                    Some(obarray) => {
                        let coerced =
                            crate::lisp::primitives::completion::coerce_legacy_vector_obarray(
                                interp, &obarray,
                            )?;
                        let standard = matches!(coerced.kind(), Kind::Record(id) if interp.is_standard_obarray_id(id.id));
                        if !standard
                            && !crate::lisp::primitives::completion::is_obarray_like_value(
                                interp, &coerced,
                            )
                        {
                            return Err(wrong_type_argument("obarrayp", obarray));
                        }
                        Some(coerced)
                    }
                    None => None,
                };
                if let Some(symbol) = standard_oblookup(interp, args) {
                    return Ok(Value::Symbol(symbol));
                }
                if let Some(symbol) = bare_symbol_identity(interp, env, &args[0]) {
                    // Fintern_soft's symbol argument searches for this exact
                    // object. A later symbol with the same name is different.
                    let present = match &obarray {
                        None => interp.standard_obarray_contains_object(&symbol),
                        Some(value) if matches!(value.kind(), Kind::Record(id) if interp.is_standard_obarray_id(id.id)) => {
                            interp.standard_obarray_contains_object(&symbol)
                        }
                        Some(value) => {
                            intern_soft_in_obarray(
                                interp,
                                value,
                                crate::lisp::types::visible_symbol_name(symbol.as_str()),
                                Some(symbol.lisp_name()),
                            )? == Value::Symbol(symbol)
                        }
                    };
                    return Ok(if present { args[0] } else { Value::Nil });
                }
                let original_name = string_text(&args[0])?;
                let symbol_name = apply_symbol_shorthands_in_env(interp, &original_name, env)?;
                if let Some(obarray) = obarray {
                    // lread.c:Fintern_soft only looks in the requested obarray.
                    // A function/value probe can intern a missing name and
                    // change the result of the next lookup.  oblookup
                    // compares STRING by its bytes, a longhand by its text.
                    let key = (symbol_name == original_name).then_some(args[0]);
                    intern_soft_in_obarray(interp, &obarray, &symbol_name, key)
                } else {
                    Ok(if interp.standard_obarray_contains_symbol(&symbol_name) {
                        crate::lisp::types::interned_symbol_value(symbol_name)
                    } else {
                        Value::Nil
                    })
                }
            }
            "unintern" => {
                need_arg_range(name, args, 1, 2)?;
                let obarray = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .or_else(|| {
                        interp
                            .lookup_var_key(cached_symbol!("obarray"), env)
                            .filter(|value| !value.is_nil())
                    });
                match obarray {
                    Some(obarray) => {
                        Ok(if unintern_from_obarray(interp, &obarray, &args[0], env)? {
                            Value::T
                        } else {
                            Value::Nil
                        })
                    }
                    _ => Ok(Value::Nil),
                }
            }
            "make-symbol" => {
                need_args(name, args, 1)?;
                let base = string_text(&args[0])?;
                let id = MAKE_SYMBOL_COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(Value::Symbol(
                    crate::lisp::types::SymbolName::make_uninterned(args[0], &base, id),
                ))
            }
            "autoload" => {
                if args.len() < 2 || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // eval.c:Fautoload: CHECK_SYMBOL, CHECK_STRING, then
                // Fdefalias of the stub, which holds FILE itself.
                let function = checked_symbol_identity(interp, &args[0], env)?;
                if !matches!(args[1].kind(), Kind::StringObject(_)) {
                    return Err(wrong_type_argument("stringp", args[1]));
                }
                let docstring = args.get(2).cloned().unwrap_or(Value::Nil);
                let interactive = args.get(3).cloned().unwrap_or(Value::Nil);
                let kind = args.get(4).cloned().unwrap_or(Value::Nil);
                // GNU `autoload' does nothing when FUNCTION already has a real
                // (non-autoload) definition; subrs count as definitions, so the
                // cl-loaddefs autoloads must not shadow emaxx's native cl-*
                // primitives either.
                // XSYMBOL (function)->u.s.function, the symbol's own cell.
                let old_definition = interp.raw_function_binding_symbol(&function, &Env::new());
                if old_definition
                    .as_ref()
                    .is_some_and(|existing| autoload_parts(existing).is_none())
                {
                    return Ok(Value::Nil);
                }
                let autoload = Value::list([
                    Value::Symbol("autoload".into()),
                    args[1],
                    docstring,
                    interactive,
                    kind,
                ]);
                interp.defalias_value(&[args[0], autoload], env)
            }
            "autoload-do-load" => {
                need_arg_range(name, args, 1, 3)?;
                let funname = args.get(1).cloned().unwrap_or(Value::Nil);
                let macro_only = args.get(2).cloned().unwrap_or(Value::Nil);
                interp.autoload_do_load(args[0], funname, macro_only, env)
            }
            "set" => {
                need_args(name, args, 2)?;
                // GNU 30.2 data.c:Fset reaches set_internal's CHECK_SYMBOL.
                let checked = checked_symbol_identity(interp, &args[0], env)?;
                set_internal_symbol(interp, &checked, args[1], env)?;
                Ok(args[1])
            }
            "set-default" => {
                need_args(name, args, 2)?;
                // GNU 30.2 data.c:Fset_default reaches
                // set_default_internal's CHECK_SYMBOL.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                set_default_internal_symbol(interp, &symbol, args[1], env)?;
                Ok(args[1])
            }
            "symbol-value" => direct_symbol_value(interp, args, env),
            "default-value" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fdefault_value delegates to
                // default_value, which uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                interp.default_value_symbol(&symbol)?.ok_or_else(|| {
                    LispError::SignalValue(Value::list([
                        Value::symbol("void-variable"),
                        Value::Symbol(symbol),
                    ]))
                })
            }
            "default-toplevel-value" => {
                need_args(name, args, 1)?;
                // GNU 30.2 eval.c:Fdefault_toplevel_value ultimately uses
                // the same CHECK_SYMBOL default-value path.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                interp
                    .default_toplevel_value_symbol(&symbol)?
                    .ok_or_else(|| {
                        LispError::SignalValue(Value::list([
                            Value::symbol("void-variable"),
                            Value::Symbol(symbol),
                        ]))
                    })
            }
            "set-default-toplevel-value" => {
                need_args(name, args, 2)?;
                // GNU 30.2 eval.c:Fset_default_toplevel_value uses the
                // CHECK_SYMBOL default binding path.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                if !interp.set_active_toplevel_value_symbol(&symbol, args[1]) {
                    set_default_internal_symbol(interp, &symbol, args[1], env)?;
                }
                Ok(Value::Nil)
            }
            "symbol-plist" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fsymbol_plist uses CHECK_SYMBOL/XSYMBOL.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                Ok(interp.symbol_plist_symbol(&symbol))
            }
            "setplist" => {
                need_args(name, args, 2)?;
                // GNU 30.2 data.c:Fsetplist uses CHECK_SYMBOL.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                interp.set_symbol_plist_symbol(&symbol, args[1])
            }
            "interactive-form" => {
                need_args(name, args, 1)?;
                // GNU returns nil for an unbound symbol (advice.el probes the
                // interactive form of not-yet-defined advice functions).
                let Ok(mut value) = resolve_callable(interp, &args[0], env) else {
                    return Ok(Value::Nil);
                };
                // data.c:1141-1151 consults an `interactive-form' property
                // FIRST -- unconditionally, and walking the symbol-function
                // alias chain -- before any subr/closure/oclosure inspection.
                // The `indirect_function' nil check above already ran, which is
                // why an unbound symbol never reaches this walk.
                // Unbounded, exactly as data.c:1144 is: `defalias' signals
                // `cyclic-function-indirection' in both binaries, so no cyclic
                // chain can reach here.  A defensive cap was measured to
                // diverge from GNU at the 64th link, and the sibling
                // `command-modes' walk is likewise uncapped.
                let mut probe = args[0];
                loop {
                    let symbol = match probe.kind() {
                        Kind::Symbol(symbol) => symbol,
                        Kind::Nil => crate::lisp::types::symbols::builtin_boolean(false),
                        Kind::T => crate::lisp::types::symbols::builtin_boolean(true),
                        _ => break,
                    };
                    if let Some(form) = cached_symbol!("interactive-form")
                        .with(|property| interp.get_symbol_property_of(&symbol, property))
                        && !matches!(form.kind(), Kind::Nil)
                    {
                        return Ok(form);
                    }
                    match interp.raw_function_binding_symbol(&symbol, env) {
                        Some(next) => probe = next,
                        None => break,
                    }
                }
                // GNU's C interactive_form then consults
                // `oclosure-interactive-form' for OClosures (nadvice's advice
                // objects compose their spec), but an (interactive ...) form
                // IN THE BODY outranks the generic (oclosure-lambda bodies may
                // carry their own spec).
                // Cheap guard first: GNU compares a function pointer here,
                // so do not pay an `oclosure-type' Lisp call when the generic
                // it feeds is not even loaded.
                if interp.is_fbound("oclosure-interactive-form")
                    && super::misc_keymaps::value_is_oclosure(interp, &value, env)
                {
                    if let Some(items) = callable_interactive_form_items(interp, &value) {
                        return Ok(Value::list(items));
                    }
                    return interp.call_function_value(
                        Value::Symbol("oclosure-interactive-form".into()),
                        Some("oclosure-interactive-form"),
                        std::slice::from_ref(&value),
                        env,
                    );
                }
                // data.c: an autoload's form is that of what loading it
                // defines (Fautoload_do_load with CMD as the name).
                if autoload_parts(&value).is_some() {
                    let loaded = interp.autoload_do_load(value, args[0], Value::Nil, env)?;
                    return super::call(interp, "interactive-form", &[loaded], env);
                }
                Ok(callable_interactive_form_items(interp, &value)
                    .map(Value::list)
                    .unwrap_or(Value::Nil))
            }
            "daemonp" => {
                need_args(name, args, 0)?;
                Ok(daemonp_value())
            }
            "daemon-initialized" => {
                need_args(name, args, 0)?;
                if daemonp_value().is_nil() {
                    return Err(LispError::Signal(
                        "This function can only be called if emacs is run as a daemon".into(),
                    ));
                }
                if interp
                    .lookup_var_key(cached_symbol!("after-init-time"), env)
                    .is_none_or(|value| value.is_nil())
                {
                    return Err(LispError::Signal(
                        "This function can only be called after loading the init files".into(),
                    ));
                }
                match daemon_initialized() {
                    Ok(true) => Ok(Value::T),
                    Ok(false) => Err(LispError::Signal(
                        "The daemon has already been initialized".into(),
                    )),
                    Err(()) => Err(LispError::Signal(
                        "I/O error during daemon initialization".into(),
                    )),
                }
            }
            "invocation-name" | "invocation-directory" => {
                need_args(name, args, 0)?;
                let value = interp.lookup_var(name, env).unwrap_or(Value::Nil);
                Ok(string_like(&value)
                    .map(|string| Value::String(string.text.into()))
                    .unwrap_or(value))
            }
            "Snarf-documentation" => {
                need_args(name, args, 1)?;
                snarf_documentation(interp, &args[0], env)
            }
            "documentation" => {
                need_arg_range(name, args, 1, 2)?;
                documentation(interp, args, env)
            }
            "documentation-property" => {
                need_arg_range(name, args, 2, 3)?;
                documentation_property(interp, args, env)
            }
            "internal-subr-documentation" => {
                need_args(name, args, 1)?;
                internal_subr_documentation(interp, &args[0], env)
            }
            "get" => direct_get(interp, args, env),
            "makunbound" => {
                need_args(name, args, 1)?;
                // GNU 30.2 data.c:Fmakunbound uses CHECK_SYMBOL/XSYMBOL and
                // returns the original symbol argument.
                let checked = checked_symbol_identity(interp, &args[0], env)?;
                if interp.is_constant_symbol(&checked)
                    || (checked.cell().has_c_slot() && checked.is_named("initial-window-system"))
                {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("setting-constant".into()),
                        args[0],
                    ])));
                }
                let symbol = interp.resolve_variable_symbol(&checked)?;
                let buffer_id = interp.current_buffer_id();
                if !symbol.cell().has_c_slot()
                    && symbol.cell().flags.get() & crate::lisp::eval::symbol_cell_flags::LOCALIZED
                        == 0
                {
                    interp.notify_variable_watchers_symbol(
                        &symbol,
                        Value::Nil,
                        "makunbound",
                        None,
                        env,
                    )?;
                    interp.remove_global_binding_symbol(&symbol);
                    return Ok(args[0]);
                }
                // Fset (symbol, Qunbound) through data.c:set_internal's
                // SYMBOL_LOCALIZED case: an existing alist cell (bound or
                // void) becomes void and stays a local binding; a
                // local_if_set symbol without a cell, outside a let made for
                // this buffer, gets a new void cell.  The default is
                // untouched either way.
                let localized_store = !interp.has_native_text_conversion_style(&symbol)
                    && (interp.has_buffer_local_binding_symbol(buffer_id, &symbol)
                        || (interp.is_auto_buffer_local_symbol(&symbol)
                            && !interp.is_per_buffer_symbol(&symbol)
                            && !interp.let_shadows_buffer_binding(symbol.as_str())));
                if localized_store {
                    // set_internal's WHERE is nil, and a `makunbound' is
                    // told without the buffer.
                    interp.notify_variable_watchers_symbol(
                        &symbol,
                        Value::Nil,
                        "makunbound",
                        None,
                        env,
                    )?;
                    interp.set_buffer_local_value_symbol(buffer_id, &symbol, Value::Unbound);
                } else {
                    interp.notify_variable_watchers_symbol(
                        &symbol,
                        Value::Nil,
                        "makunbound",
                        None,
                        env,
                    )?;
                    // data.c set_internal: storing void into a symbol
                    // forwarded to a C slot detaches it for good -- the
                    // built-in fallback stops answering, a later store makes
                    // a plain (uncoerced) variable, and the C slot keeps the
                    // value it holds now for its native readers.
                    // Direct evaluator fields already own their C value.
                    // Keep only a detachment marker for them: retaining a
                    // snapshot here would falsely root an old object after
                    // C independently overwrites its slot.
                    let slot_value = if interp.has_native_text_conversion_style(&symbol) {
                        if !interp.buffer.borrow().text_conversion_style_is_local {
                            if interp.let_shadows_buffer_binding(symbol.as_str()) {
                                // set_internal first changes the C default
                                // under LET_DEFAULT, then disconnects the
                                // symbol without overwriting the C field.
                                interp.notify_variable_watchers_symbol(
                                    &symbol,
                                    Value::Unbound,
                                    "set",
                                    None,
                                    env,
                                )?;
                                interp.set_global_binding_resolved(&symbol, Value::Unbound);
                            } else {
                                interp.buffer.borrow_mut().text_conversion_style_is_local = true;
                            }
                        }
                        interp.native_text_conversion_style_default()
                    } else if interp.forwarded_eval_cell_value(symbol.as_str()).is_some() {
                        Value::Nil
                    } else {
                        interp
                            .forwarded_c_value(symbol.as_str(), env)
                            .unwrap_or(Value::Nil)
                    };
                    interp.remove_global_binding_symbol(&symbol);
                    interp.detach_forwarded_variable_symbol(&symbol, slot_value);
                }
                Ok(args[0])
            }
            "lread--substitute-object-in-subtree" => {
                need_args(name, args, 3)?;
                substitute_object_in_subtree(interp, &args[0], &args[1], &args[2])?;
                Ok(Value::Nil)
            }
            "defvaralias" => {
                // GNU 30.2 eval.c:Fdefvaralias, in its order: CHECK_SYMBOL
                // both, constant, non-circularity from BASE, the alias's
                // redirect kind, the 2008 value hand-over or the
                // losing-value warning, the let-bound check, watchers, then
                // the redirect itself.  Returns BASE.
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let alias = checked_symbol_identity(interp, &args[0], env)?;
                let base = checked_symbol_identity(interp, &args[1], env)?;
                let visible =
                    |symbol: &str| crate::lisp::types::visible_symbol_name(symbol).to_owned();
                if interp.is_constant_symbol(&alias) {
                    return Err(LispError::Signal(format!(
                        "Cannot make a constant an alias: {}",
                        visible(alias.as_str())
                    )));
                }
                interp.check_variable_alias_cycle_symbol(&alias, &base)?;
                // A DEFVAR_PER_BUFFER slot is SYMBOL_FORWARDED until
                // something localizes it; Emaxx marks those auto-buffer-local
                // from the start, so for them the oracle's own startup
                // classification decides, and the forwarded manifest is
                // consulted before Emaxx's flag.
                let flags = alias.cell().flags.get();
                use crate::lisp::eval::symbol_cell_flags::{FORWARDED, LOCALIZED, PER_BUFFER};
                if (flags & LOCALIZED != 0 && flags & PER_BUFFER == 0)
                    || (alias.cell().has_c_slot()
                        && interp.is_localized_at_startup_in_gnu(alias.as_str()))
                {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Don't know how to make a buffer-local variable an alias: {}",
                        &[&visible(alias.as_str())],
                    ));
                }
                if flags & FORWARDED != 0 {
                    return Err(LispError::Signal(format!(
                        "Cannot make a built-in variable an alias: {}",
                        visible(alias.as_str())
                    )));
                }
                if flags & LOCALIZED != 0 {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Don't know how to make a buffer-local variable an alias: {}",
                        &[&visible(alias.as_str())],
                    ));
                }
                // https://lists.gnu.org/r/emacs-devel/2008-04/msg00834.html
                let alias_value = interp.symbol_value_cell_symbol(&alias).ok();
                match interp
                    .symbol_value_cell_symbol(&base)
                    .map_err(LispError::into_kind)
                {
                    Err(LispErrorKind::Void(_)) => {
                        // set_internal of the alias's Qunbound: the base's
                        // watchers hear `let' with nil, and it stays void.
                        if alias_value.is_none() {
                            let resolved = interp.resolve_variable_symbol(&base)?;
                            interp.notify_variable_watchers_symbol(
                                &resolved,
                                Value::Nil,
                                "let",
                                None,
                                env,
                            )?;
                        }
                        if let Some(value) = alias_value {
                            // set_internal (base, value, Qnil, SET_INTERNAL_BIND)
                            interp.check_constant_assignment_symbol(&base, value)?;
                            let resolved = interp.resolve_variable_symbol(&base)?;
                            let stored =
                                interp.prepare_forwarded_assignment_symbol(&resolved, value)?;
                            interp.notify_variable_watchers_symbol(
                                &resolved, value, "let", None, env,
                            )?;
                            interp.check_per_buffer_predicate(&resolved, value, env)?;
                            interp.set_symbol_value_cell_resolved(&resolved, stored);
                        }
                    }
                    Err(error) => return Err(LispError::from(error)),
                    Ok(base_value) => {
                        if let Some(alias_value) = alias_value.as_ref()
                            && !values_eq_in_env(interp, alias_value, &base_value, env)
                        {
                            let formatted = call_named_function(
                                interp,
                                "format-message",
                                &[
                                    Value::string("Overwriting value of `%s' by aliasing to `%s'"),
                                    Value::Symbol(alias),
                                    Value::Symbol(base),
                                ],
                                env,
                            )?;
                            let warning = Value::list([
                                Value::Symbol("defvaralias".into()),
                                Value::Symbol("losing-value".into()),
                                Value::Symbol(alias),
                            ]);
                            call_named_function(
                                interp,
                                "display-warning",
                                &[warning, formatted],
                                env,
                            )?;
                        }
                    }
                }
                if interp.is_let_bound_special(&alias) {
                    return Err(crate::lisp::primitives::values::doprnt_error(
                        interp,
                        "Don't know how to make a let-bound variable an alias: {}",
                        &[&visible(alias.as_str())],
                    ));
                }
                interp.notify_variable_watchers_symbol(
                    &alias,
                    Value::Symbol(base),
                    "defvaralias",
                    None,
                    env,
                )?;
                interp.mark_special_variable_symbol(&alias);
                interp.mark_special_variable_symbol(&base);
                interp.set_variable_alias_symbol(&alias, &base)?;
                // The alias's stores are trapped as its base's are.
                alias
                    .cell()
                    .set_trapped_write(base.cell().is_trapped_write());
                // LOADHIST_ATTACH (new_alias): the bare symbol, as for defvar.
                interp.record_symbol_definition_in_load_history("defvar", &alias);
                // "Even if docstring is nil: remove old docstring."
                let doc = args.get(2).cloned().unwrap_or(Value::Nil);
                super::call(
                    interp,
                    "put",
                    &[
                        Value::Symbol(alias),
                        Value::symbol("variable-documentation"),
                        doc,
                    ],
                    env,
                )?;
                Ok(Value::Symbol(base))
            }
            "indirect-variable" => {
                need_args(name, args, 1)?;
                // data.c:Findirect_variable answers a non-symbol as given.
                let Ok(symbol) = checked_symbol_identity(interp, &args[0], env) else {
                    return Ok(args[0]);
                };
                Ok(Value::Symbol(interp.resolve_variable_symbol(&symbol)?))
            }
            "internal--define-uninitialized-variable" => {
                // GNU: (SYMBOL &optional DOC) — cus-start.el passes one arg.
                need_arg_range(name, args, 1, 2)?;
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                interp.define_uninitialized_variable(
                    &symbol,
                    args.get(1).copied().unwrap_or(Value::Nil),
                    env,
                )?;
                Ok(Value::Nil)
            }
            "defvar-1" => {
                // eval.c:Fdefvar_1: CHECK_SYMBOL, then defvar (SYM,
                // INITVALUE, DOCSTRING, false).
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                let init = args[1];
                interp.defvar_symbol(
                    &symbol,
                    args.get(2).copied().unwrap_or(Value::Nil),
                    |_, _| Ok(init),
                    env,
                )?;
                Ok(args[0])
            }
            "defconst-1" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // eval.c:Fdefconst_1: CHECK_SYMBOL, then SYMBOL as given
                // declared and stored with Fset_default (which refuses a
                // constant); SYMBOL itself comes back.
                let symbol = checked_symbol_identity(interp, &args[0], env)?;
                let doc = args.get(2).copied().unwrap_or(Value::Nil);
                interp.defconst_1(&symbol, args[1], doc, env)?;
                Ok(args[0])
            }
            "internal-make-var-non-special" => {
                need_args(name, args, 1)?;
                let symbol = crate::lisp::primitives::interactive::check_symbol(interp, args[0])?;
                interp.unmark_special_variable(symbol.as_symbol()?);
                // Finternal_make_var_non_special returns nil.
                Ok(Value::Nil)
            }
            "make-interpreted-closure" => {
                need_arg_range(name, args, 3, 5)?;
                interp.make_interpreted_closure(
                    args[0],
                    args[1],
                    args[2],
                    args.get(3).copied().unwrap_or(Value::Nil),
                    args.get(4).copied().unwrap_or(Value::Nil),
                )
            }
            "getenv-internal" => {
                // callproc.c Fgetenv_internal: a list ENV is searched
                // instead of `process-environment', where a negative entry
                // gives t; anything else names the frame for DISPLAY.
                need_arg_range(name, args, 1, 2)?;
                let Kind::StringObject(variable) = args[0].kind() else {
                    return Err(wrong_type_argument("stringp", args[0]));
                };
                let variable = variable.borrow().bytes().to_vec();
                let environment = args.get(1).copied().unwrap_or(Value::Nil);
                let value = if environment.is_cons() {
                    match getenv_internal_1(&variable, environment) {
                        Some(Some(value)) => Some(value),
                        Some(None) => return Ok(Value::T),
                        None => None,
                    }
                } else {
                    getenv_internal(interp, env, &variable, &environment)?
                };
                match value {
                    Some(value) => {
                        make_string(value).map_err(|error| string_storage_error(interp, env, error))
                    }
                    None => Ok(Value::Nil),
                }
            }

            "purecopy" => {
                need_args(name, args, 1)?;
                purecopy_value(interp, &args[0], env)
            }

            "macroexpand" => macroexpand_dispatch(interp, name, args, env),

            "current-idle-time" => {
                need_args(name, args, 0)?;
                // keyboard.c Fcurrent_idle_time: the span since idleness
                // began, as (HIGH LOW USEC PSEC), or nil while input is
                // being processed (and always nil in batch, which has no
                // input loop).
                match crate::lisp::primitives::tty_current_idle_duration() {
                    Some(elapsed) => {
                        let secs = elapsed.as_secs();
                        let usec = elapsed.subsec_micros();
                        let psec = (elapsed.subsec_nanos() % 1_000) * 1_000_000;
                        Ok(Value::list([
                            Value::Integer((secs >> 16) as i64),
                            Value::Integer((secs & 0xffff) as i64),
                            Value::Integer(i64::from(usec)),
                            Value::Integer(i64::from(psec)),
                        ]))
                    }
                    None => Ok(Value::Nil),
                }
            }
            "subr-type" => {
                need_args(name, args, 1)?;
                match args[0].kind() {
                    Kind::BuiltinFunc(_) => Ok(Value::Nil),
                    Kind::NativeFunction(function) => Ok(function.native_type()),
                    other => Err(wrong_type_argument("subrp", other.value())),
                }
            }
            "function-equal" => {
                need_args(name, args, 2)?;
                // profiler.c:Ffunction_equal first compares the objects,
                // then CLOSURE_CODE by identity for either closure kind.
                let code = |function: Value| match function.kind() {
                    Kind::Closure(lambda) => Some(lambda.body()),
                    _ => None,
                };
                let same = values_eq_in_env(interp, &args[0], &args[1], env)
                    || matches!((code(args[0]), code(args[1])), (Some(left), Some(right))
                        if values_eq_in_env(interp, &left, &right, env));
                Ok(if same { Value::T } else { Value::Nil })
            }
            "get-internal-run-time" => {
                need_args(name, args, 0)?;
                process_cpu_time_value()
            }
            "flush-standard-output" => {
                need_args(name, args, 0)?;
                crate::lisp::primitives::batch_stdout::flush()
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                Ok(Value::Nil)
            }
            "lossage-size" => {
                need_arg_range(name, args, 0, 1)?;
                if args.first().is_none_or(Value::is_nil) {
                    return Ok(Value::Integer(interp.lossage_size));
                }
                let Kind::Integer(new_size) = args[0].kind() else {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("user-error"),
                        Value::String("Value must be a positive integer".into()),
                    ])));
                };
                if new_size < 0 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("user-error"),
                        Value::String("Value must be a positive integer".into()),
                    ])));
                }
                if new_size < 100 {
                    return Err(LispError::SignalValue(Value::list([
                        Value::symbol("user-error"),
                        Value::String("Value must be >= 100".into()),
                    ])));
                }
                interp.lossage_size = new_size;
                let new_size = new_size as usize;
                if interp.keyboard_input.recent_keys.len() > new_size {
                    let excess = interp.keyboard_input.recent_keys.len() - new_size;
                    interp.keyboard_input.recent_keys.drain(0..excess);
                }
                Ok(Value::Integer(interp.lossage_size))
            }
            "run-hooks" => run_hooks_dispatch(interp, args, env),

            "run-hook-with-args" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let hook = checked_symbol_identity(interp, &args[0], env)?;
                run_hook_with_args(interp, &hook, env, |interp, function, env| {
                    call_function_value(interp, &function, &args[1..], env)?;
                    Ok(Value::Nil)
                })?;
                Ok(Value::Nil)
            }
            "run-hook-with-args-until-success" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let hook = checked_symbol_identity(interp, &args[0], env)?;
                run_hook_with_args(interp, &hook, env, |interp, function, env| {
                    call_function_value(interp, &function, &args[1..], env)
                })
            }
            "run-hook-with-args-until-failure" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // run_hook_with_args_until_failure_funcall: a nil answer
                // stops the run.
                let hook = checked_symbol_identity(interp, &args[0], env)?;
                let failed = run_hook_with_args(interp, &hook, env, |interp, function, env| {
                    let answer = call_function_value(interp, &function, &args[1..], env)?;
                    Ok(if answer.is_nil() {
                        Value::T
                    } else {
                        Value::Nil
                    })
                })?;
                Ok(if failed.is_nil() {
                    Value::T
                } else {
                    Value::Nil
                })
            }

            "run-hook-wrapped" => {
                if args.len() < 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // run_hook_wrapped_funcall: WRAP-FUNCTION called with
                // each function and ARGS, its non-nil answer stopping the
                // run.
                let hook = checked_symbol_identity(interp, &args[0], env)?;
                let wrapper = args[1];
                // The function's slot is filled from the list, which the
                // run holds; the other arguments are the caller's.
                let mut wrapper_args = args[1..].to_vec();
                run_hook_with_args(interp, &hook, env, |interp, function, env| {
                    wrapper_args[0] = function;
                    call_function_value(interp, &wrapper, &wrapper_args, env)
                })
            }

            "mapatoms" => {
                need_arg_range(name, args, 1, 2)?;
                // lread.c:Fmapatoms checks the selected obarray before
                // calling FUNCTION. A nil argument uses the dynamically
                // bound obarray, and legacy vectors are converted on use.
                let obarray = args
                    .get(1)
                    .filter(|value| !value.is_nil())
                    .cloned()
                    .or_else(|| interp.lookup_var_key(cached_symbol!("obarray"), env))
                    .unwrap_or(Value::Nil);
                let obarray = coerce_legacy_vector_obarray(interp, &obarray)?;
                // The standard obarray: walk the shared enumeration in place
                // (lread.c walks the buckets; a symbol interned during the
                // walk joins a fresh enumeration, which this snapshot does
                // not see, as GNU's walk need not) instead of copying every
                // symbol into a vector first -- a quarter of a walk's cost.
                if let Kind::Record(id) = obarray.kind()
                    && interp.is_standard_obarray_id(id.id)
                {
                    let symbols = interp.known_symbols_shared();
                    for symbol in symbols.iter() {
                        // The shared Value constructor preserves the nil/t ABI.
                        let symbol = Value::Symbol(*symbol);
                        call_function_value(interp, &args[0], &[symbol], env)?;
                    }
                    return Ok(Value::Nil);
                }
                let symbols = obarray_symbols(interp, &obarray)?;
                for symbol in symbols {
                    // Resolve a symbol's current function cell on every
                    // call, as call1 does. Empty obarrays never call it.
                    call_function_value(interp, &args[0], &[symbol], env)?;
                }
                Ok(Value::Nil)
            }
        }
    }
);

#[cfg(unix)]
fn process_cpu_time_parts() -> Result<(i64, i64), LispError> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the pointed-to rusage structure and does
    // not retain the pointer.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: the successful getrusage call initialized every field.
    let usage = unsafe { usage.assume_init() };
    fn libc_time_to_i64<T: Into<i64>>(value: T) -> i64 {
        value.into()
    }

    let mut seconds =
        libc_time_to_i64(usage.ru_utime.tv_sec) + libc_time_to_i64(usage.ru_stime.tv_sec);
    let mut micros =
        libc_time_to_i64(usage.ru_utime.tv_usec) + libc_time_to_i64(usage.ru_stime.tv_usec);
    if micros >= 1_000_000 {
        seconds += micros / 1_000_000;
        micros %= 1_000_000;
    }
    Ok((seconds, micros))
}

#[cfg(unix)]
pub(super) fn current_cpu_time_value() -> Result<Value, LispError> {
    // timefns.c:Fcurrent_cpu_time reports the current process's user plus
    // system CPU time as (TICKS . HZ), independently of current-time-list.
    // Kernel resource accounting gives that CPU total at microsecond
    // resolution, excluding elapsed sleep and subprocess CPU time.
    let (seconds, micros) = process_cpu_time_parts()?;
    Ok(Value::cons(
        normalize_bigint_value(BigInt::from(seconds) * 1_000_000 + micros),
        Value::Integer(1_000_000),
    ))
}

#[cfg(windows)]
pub(super) fn current_cpu_time_value() -> Result<Value, LispError> {
    // The Windows C runtime clock uses 1000 ticks per second, as in GNU.
    // SAFETY: clock takes no arguments and retains no pointers.
    let ticks = unsafe { libc::clock() };
    Ok(Value::cons(
        Value::Integer(i64::from(ticks)),
        Value::Integer(1000),
    ))
}

#[cfg(not(any(unix, windows)))]
pub(super) fn current_cpu_time_value() -> Result<Value, LispError> {
    Err(LispError::Signal(
        "Process CPU clock is unavailable on this platform".into(),
    ))
}

#[cfg(unix)]
fn process_cpu_time_value() -> Result<Value, LispError> {
    let (seconds, micros) = process_cpu_time_parts()?;
    Ok(Value::list([
        Value::Integer(seconds >> 16),
        Value::Integer(seconds & 0xffff),
        Value::Integer(micros),
        Value::Integer(0),
    ]))
}

#[cfg(not(unix))]
fn process_cpu_time_value() -> Result<Value, LispError> {
    system_time_list_value(SystemTime::now())
}

#[derive(Clone, Debug)]
struct DocFileEntry {
    kind: u8,
    name: String,
    offset: i64,
    user_variable: bool,
    skip: bool,
}

fn parse_doc_file_entries(bytes: &[u8]) -> Result<Vec<DocFileEntry>, LispError> {
    let mut entries = Vec::new();
    let mut cursor = 0;
    while let Some(relative) = bytes[cursor..].iter().position(|byte| *byte == 0x1f) {
        let marker = cursor + relative;
        if marker + 2 > bytes.len() {
            return Err(LispError::Signal(format!(
                "DOC file invalid at position {marker}"
            )));
        }
        let kind = bytes[marker + 1];
        let name_start = marker + 2;
        let Some(relative_newline) = bytes[name_start..].iter().position(|byte| *byte == b'\n')
        else {
            return Err(LispError::Signal(format!(
                "DOC file invalid at position {marker}"
            )));
        };
        let newline = name_start + relative_newline;
        let content_start = newline + 1;
        let next_marker = bytes[content_start..]
            .iter()
            .position(|byte| *byte == 0x1f)
            .map(|relative| content_start + relative)
            .unwrap_or(bytes.len());
        if !matches!(kind, b'F' | b'V' | b'S') {
            return Err(LispError::Signal(format!(
                "DOC file invalid at position {marker}"
            )));
        }
        entries.push(DocFileEntry {
            kind,
            name: String::from_utf8_lossy(&bytes[name_start..newline]).into_owned(),
            offset: content_start as i64,
            user_variable: kind == b'V' && bytes.get(content_start) == Some(&b'*'),
            skip: bytes[content_start..next_marker].starts_with(b"SKIP"),
        });
        cursor = next_marker;
        if cursor == bytes.len() {
            break;
        }
    }
    Ok(entries)
}

fn doc_path(directory: &Value, filename: &Value) -> Result<PathBuf, LispError> {
    let directory = string_like(directory)
        .ok_or_else(|| wrong_type_argument("stringp", *directory))?
        .text;
    let filename = string_like(filename)
        .ok_or_else(|| wrong_type_argument("stringp", *filename))?
        .text;
    let filename = PathBuf::from(filename);
    Ok(if filename.is_absolute() {
        filename
    } else {
        Path::new(&directory).join(filename)
    })
}

fn standard_doc_path(interp: &Interpreter, env: &Env) -> Result<PathBuf, LispError> {
    let directory = interp
        .lookup_var_key(cached_symbol!("doc-directory"), env)
        .unwrap_or(Value::Nil);
    let filename = interp
        .lookup_var_key(cached_symbol!("internal-doc-file-name"), env)
        .unwrap_or(Value::Nil);
    doc_path(&directory, &filename)
}

fn decode_doc_string(
    bytes: &[u8],
    position: i64,
    dynamic: bool,
) -> Result<Option<String>, LispError> {
    Ok(decode_doc_bytes(bytes, position, dynamic)?
        .map(|decoded| String::from_utf8_lossy(&decoded).into_owned()))
}

/// get_doc_string's checks and ^A unquoting of the doc string at POSITION
/// in BYTES, a block read from its file: its bytes, or None where GNU
/// answers nil.
fn decode_doc_bytes(
    bytes: &[u8],
    position: i64,
    dynamic: bool,
) -> Result<Option<Vec<u8>>, LispError> {
    let Some(position) = position
        .checked_abs()
        .and_then(|value| usize::try_from(value).ok())
    else {
        return Ok(None);
    };
    if position == 0 || position > bytes.len() {
        return Ok(None);
    }
    // doc.c:236-264 sanity checking.  A dynamic (FILE . POS) docstring in a
    // `.elc' starts right after a '\037' delimiter or a "#@NNN " comment
    // header; a DOC-file entry starts after the "\037<tag><name>\n" preamble.
    if dynamic {
        if bytes[position - 1] != 0x1f {
            if bytes[position - 1] != b' ' {
                return Ok(None);
            }
            let mut test = 2usize;
            while position
                .checked_sub(test)
                .is_some_and(|at| bytes[at].is_ascii_digit())
            {
                test += 1;
            }
            let Some(at) = position.checked_sub(test) else {
                return Ok(None);
            };
            if bytes[at] != b'@' {
                return Ok(None);
            }
            test += 1;
            let Some(at) = position.checked_sub(test) else {
                return Ok(None);
            };
            if bytes[at] != b'#' {
                return Ok(None);
            }
        }
    } else {
        if bytes[position - 1] != b'\n' {
            return Ok(None);
        }
        let mut test = 2usize;
        while position
            .checked_sub(test)
            .is_some_and(|at| bytes[at] > b' ')
        {
            test += 1;
        }
        if position
            .checked_sub(test)
            .is_none_or(|at| bytes[at] != 0x1f)
        {
            return Ok(None);
        }
    }
    let end = bytes[position..]
        .iter()
        .position(|byte| *byte == 0x1f)
        .map(|relative| position + relative)
        .unwrap_or(bytes.len());
    let mut decoded = Vec::with_capacity(end - position);
    let mut cursor = position;
    while cursor < end {
        if bytes[cursor] != 1 {
            decoded.push(bytes[cursor]);
            cursor += 1;
            continue;
        }
        let Some(escaped) = bytes.get(cursor + 1).copied() else {
            return Err(LispError::Signal(
                "Invalid data in documentation file".into(),
            ));
        };
        decoded.push(match escaped {
            1 => 1,
            b'0' => 0,
            b'_' => 0x1f,
            _ => {
                return Err(LispError::Signal(format!(
                    "Invalid data in documentation file -- {} followed by code {escaped:03o}",
                    1
                )));
            }
        });
        cursor += 2;
    }
    Ok(Some(decoded))
}

fn doc_reference_parts(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Option<(PathBuf, i64)>, LispError> {
    // doc.c:get_doc_string pairs the position with a file and directory:
    // an integer FILEPOS reads `internal-doc-file-name' in `doc-directory',
    // a (FILE . POS) cons reads FILE against `lisp-directory'.  A
    // non-string directory or file answers nil there (doc.c:135-139),
    // never a wrong-type-argument signal.
    let (filename, directory, position) = if let Kind::Integer(position) = value.kind() {
        (
            interp
                .lookup_var_key(cached_symbol!("internal-doc-file-name"), env)
                .unwrap_or(Value::Nil),
            interp
                .lookup_var_key(cached_symbol!("doc-directory"), env)
                .unwrap_or(Value::Nil),
            position,
        )
    } else {
        let Some((filename, position)) = value.cons_values() else {
            return Ok(None);
        };
        let Kind::Integer(position) = position.kind() else {
            return Ok(None);
        };
        (
            filename,
            interp
                .lookup_var_key(cached_symbol!("lisp-directory"), env)
                .unwrap_or(Value::Nil),
            position,
        )
    };
    if string_like(&directory).is_none() || string_like(&filename).is_none() {
        return Ok(None);
    }
    Ok(Some((doc_path(&directory, &filename)?, position)))
}

fn resolve_doc_reference(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Option<Value>, LispError> {
    get_doc_string(interp, value, env, false)
}

/// doc.c get_doc_string (FILEPOS, true), lread.c get_lazy_string's
/// fallback: the unibyte doc string or byte-code a reference names, nil
/// where GNU answers nil.
pub(crate) fn unibyte_doc_string(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Value, LispError> {
    Ok(get_doc_string(interp, value, env, true)?.unwrap_or(Value::Nil))
}

/// doc.c get_doc_string: the doc string an integer DOC position or a
/// (FILE . POS) reference names, unibyte when UNIBYTE.
fn get_doc_string(
    interp: &Interpreter,
    value: &Value,
    env: &Env,
    unibyte: bool,
) -> Result<Option<Value>, LispError> {
    let Some((path, position)) = doc_reference_parts(interp, value, env)? else {
        return Ok(None);
    };
    match read_doc_string_block(&path, position.unsigned_abs()) {
        Ok((bytes, offset)) => {
            let dynamic = !matches!(value.kind(), Kind::Integer(_));
            Ok(if unibyte {
                decode_doc_bytes(&bytes, offset as i64, dynamic)?
                    .map(|decoded| Value::StringObject(StringObjectRef::from_unibyte(decoded)))
            } else {
                decode_doc_string(&bytes, offset as i64, dynamic)?
                    .map(|value| Value::String(value.into()))
            })
        }
        Err(error) if matches!(error.kind(), std::io::ErrorKind::NotFound) => {
            let filename = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_else(|| path.to_str().unwrap_or(""));
            Ok(Some(Value::String(
                format!("Cannot open doc string file \"{filename}\"\n").into(),
            )))
        }
        Err(error) => Err(LispError::SignalValue(file_error_value(
            &error.to_string(),
            &path.to_string_lossy(),
        ))),
    }
}

/// doc.c get_doc_string's read: from the disk block before POSITION, at
/// least 1024 bytes ahead of it for the consistency checks, a block at a
/// time to the docstring's terminating 037 or the end of the file.  The
/// result is the bytes read and POSITION's offset in them.
fn read_doc_string_block(path: &Path, position: u64) -> std::io::Result<(Vec<u8>, usize)> {
    use std::io::{Read, Seek, SeekFrom};
    const BLOCK: u64 = 8 * 1024;
    let mut file = std::fs::File::open(path)?;
    let offset = position.min(1024.max(position % BLOCK));
    file.seek(SeekFrom::Start(position - offset))?;
    let offset = offset as usize;
    let mut bytes = Vec::new();
    let mut block = [0u8; BLOCK as usize];
    loop {
        let read = match file.read(&mut block) {
            Ok(read) => read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if read == 0 {
            break;
        }
        let searched = bytes.len().max(offset);
        bytes.extend_from_slice(&block[..read]);
        if bytes
            .get(searched..)
            .is_some_and(|rest| rest.contains(&0x1f))
        {
            break;
        }
    }
    Ok((bytes, offset))
}

fn is_doc_reference(value: &Value) -> bool {
    matches!(value.kind(), Kind::Integer(_))
        || value.cons_values().is_some_and(|(filename, position)| {
            string_like(&filename).is_some() && matches!(position.kind(), Kind::Integer(_))
        })
}

fn substitute_doc_keys(
    interp: &mut Interpreter,
    doc: Value,
    raw: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    if raw || string_like(&doc).is_none() {
        return Ok(doc);
    }
    interp.call_function_value(
        Value::symbol("substitute-command-keys"),
        Some("substitute-command-keys"),
        &[doc],
        env,
    )
}

fn snarf_documentation(
    interp: &mut Interpreter,
    filename: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if string_like(filename).is_none() {
        return Err(wrong_type_argument("stringp", *filename));
    }
    let directory = interp
        .lookup_var_key(cached_symbol!("doc-directory"), env)
        .unwrap_or(Value::Nil);
    // doc.c names the file by appending FILENAME to `doc-directory'.
    let directory = string_like(&directory)
        .ok_or_else(|| wrong_type_argument("stringp", directory))?
        .text;
    let path = directory + &string_text(filename)?;
    // doc.c: `build-files' is nil until the first snarf, which conses it
    // from buildobj.h before opening the file.
    let build_files_unset = interp
        .lookup_var_key(cached_symbol!("build-files"), env)
        .is_none_or(|value| value.is_nil());
    if build_files_unset && let Some(names) = crate::lisp::primitives::gnu_build_files() {
        interp.set_variable_key(
            cached_symbol!("build-files"),
            Value::list(names.iter().map(|name| Value::String((*name).into()))),
            env,
        );
    }
    let bytes = std::fs::read(&path).map_err(|error| {
        crate::lisp::primitives::file_io::file_operation_error(
            "Opening doc string file",
            &error,
            &path,
        )
    })?;
    let entries = parse_doc_file_entries(&bytes)?;
    interp.set_variable_key(cached_symbol!("internal-doc-file-name"), *filename, env);
    if build_files_unset && crate::lisp::primitives::gnu_build_files().is_none() {
        // No oracle's buildobj.h is recorded for this platform: DOC's S
        // records, which name every file make-docfile read, stand in.
        interp.set_variable_key(
            cached_symbol!("build-files"),
            Value::list(
                entries
                    .iter()
                    .filter(|entry| entry.kind == b'S')
                    .map(|entry| Value::String(entry.name.clone().into())),
            ),
            env,
        );
    }
    let delayed = interp
        .lookup_var_key(cached_symbol!("custom-delayed-init-variables"), env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    for entry in entries {
        if entry.skip {
            continue;
        }
        // doc.c finds each entry's symbol with oblookup: the DEFUNs and
        // DEFVARs DOC lists under `#if 0', GLYPH_DEBUG or another
        // platform's file name no symbol, and none is made for them.
        let Some(symbol) = crate::lisp::types::SymbolName::find(&entry.name) else {
            continue;
        };
        match entry.kind {
            b'F' => {
                if let Ok(Kind::BuiltinFunc(native_name)) = interp
                    .lookup_function_symbol(&symbol, env)
                    .map(|v| v.kind())
                {
                    interp
                        .builtin_doc_offsets
                        .insert(native_name.to_string(), entry.offset);
                }
            }
            b'V' if interp.lookup_var_symbol(&symbol, env).is_some()
                || delayed
                    .iter()
                    .any(|value| matches!(value.kind(), Kind::Symbol(name) if name == symbol)) =>
            {
                interp.put_symbol_property(
                    &entry.name,
                    "variable-documentation",
                    Value::Integer(if entry.user_variable {
                        -entry.offset
                    } else {
                        entry.offset
                    }),
                );
            }
            b'V' | b'S' => {}
            _ => unreachable!("DOC entry kinds were validated while parsing"),
        }
    }
    Ok(Value::Nil)
}

fn ensure_builtin_doc_offset(
    interp: &mut Interpreter,
    name: &str,
    env: &Env,
) -> Result<i64, LispError> {
    if let Some(offset) = interp.builtin_doc_offsets.get(name) {
        return Ok(*offset);
    }
    let path = standard_doc_path(interp, env)?;
    let Ok(bytes) = std::fs::read(path) else {
        return Ok(0);
    };
    let offset = parse_doc_file_entries(&bytes)?
        .into_iter()
        .find(|entry| entry.kind == b'F' && entry.name == name && !entry.skip)
        .map(|entry| entry.offset)
        .unwrap_or(0);
    interp.builtin_doc_offsets.insert(name.to_string(), offset);
    Ok(offset)
}

fn internal_subr_documentation(
    interp: &mut Interpreter,
    function: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match function.kind() {
        Kind::BuiltinFunc(name) => Ok(Value::Integer(ensure_builtin_doc_offset(
            interp, &name, env,
        )?)),
        Kind::NativeFunction(function) => {
            crate::lisp::native_comp::function_documentation(interp, env, function)
        }
        _ => Ok(Value::T),
    }
}

fn documentation_property(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    // doc.c:Fdocumentation_property: Fget of PROP, whatever it is; a
    // missing `variable-documentation' is looked for on the indirect
    // variable.
    let mut doc = super::call(interp, "get", &[args[0], args[1]], env)?;
    if doc.is_nil() && args[1].eq_value(Value::symbol("variable-documentation")) {
        let indirect = super::call(interp, "indirect-variable", &[args[0]], env)?;
        if !indirect.is_nil() {
            doc = super::call(interp, "get", &[indirect, args[1]], env)?;
        }
    }
    if doc == Value::Integer(0) {
        doc = Value::Nil;
    } else if is_doc_reference(&doc) {
        doc = resolve_doc_reference(interp, &doc, env)?.unwrap_or(Value::Nil);
    } else if string_like(&doc).is_none() && !doc.is_nil() {
        // Feval (tem, Qnil): dynamically, outside the caller's scope.
        doc = interp.eval(&doc, &mut Env::new())?;
    }
    substitute_doc_keys(interp, doc, args.get(2).is_some_and(Value::is_truthy), env)
}

fn documentation(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let raw = args.get(1).is_some_and(Value::is_truthy);
    if let Kind::Symbol(symbol) = args[0].kind()
        && interp
            .get_symbol_property_of(
                &symbol,
                &cached_symbol!("function-documentation").with(Clone::clone),
            )
            .is_some_and(|value| !value.is_nil())
    {
        return documentation_property(
            interp,
            &[
                args[0],
                Value::symbol("function-documentation"),
                if raw { Value::T } else { Value::Nil },
            ],
            env,
        );
    }

    let function = resolve_callable(interp, &args[0], env)?;
    // doc.c Fdocumentation: a macro's documentation lives on the function
    // inside its (macro . FUNCTION) cons, unwrapped before the dispatch.
    let function = if matches!(function.car().map(|v| v.kind()), Ok(Kind::Symbol(ref name)) if name.is_named("macro"))
    {
        function.cdr()?
    } else {
        function
    };
    // doc.c delegates to the Lisp generic `function-documentation'
    // (simple.el), which oclosure accessors and advice types specialize.
    // The native reading below serves only the bootstrap window before
    // simple.el defines the generic.
    let generic_available = interp
        .lookup_function_key(cached_symbol!("function-documentation"), env)
        .is_ok();
    let module_function = matches!(function.kind(), Kind::Record(id) if interp.find_record(id).is_some_and(|record| record.kind == crate::lisp::eval::RecordKind::ModuleFunction));
    let mut doc = if generic_available
        && !matches!(function.kind(), Kind::BuiltinFunc(_))
        && !module_function
    {
        interp.call_function_value(
            Value::symbol("function-documentation"),
            Some("function-documentation"),
            std::slice::from_ref(&function),
            env,
        )?
    } else {
        match function.kind() {
            Kind::BuiltinFunc(name) => {
                let offset = ensure_builtin_doc_offset(interp, &name, env)?;
                if offset == 0 {
                    fallback_function_documentation(interp, &name)
                        .map(|value| Value::String(value.into()))
                        .unwrap_or(Value::Nil)
                } else {
                    resolve_doc_reference(interp, &Value::Integer(offset), env)?
                        .unwrap_or(Value::Nil)
                }
            }
            _ => function_documentation(interp, &function, env).unwrap_or(Value::Nil),
        }
    };
    if doc.is_nil()
        && let Kind::Symbol(symbol) = args[0].kind()
    {
        doc = fallback_function_documentation(interp, symbol.as_str())
            .map(|value| Value::String(value.into()))
            .unwrap_or(Value::Nil);
    }
    if doc == Value::Integer(0) {
        doc = Value::Nil;
    } else if is_doc_reference(&doc) {
        doc = resolve_doc_reference(interp, &doc, env)?.unwrap_or(Value::Nil);
    }
    substitute_doc_keys(interp, doc, raw, env)
}

/// A lazily-populated docstring cache: the source path it was built from and a
/// shared name → docstring map.
type DocSourceCache = Option<(
    String,
    std::rc::Rc<std::collections::HashMap<String, String>>,
)>;

thread_local! {
    // Cache of (DOC-file-path -> {function-name -> docstring}) parsed lazily.
    static DOC_FILE_CACHE: std::cell::RefCell<DocSourceCache> =
        const { std::cell::RefCell::new(None) };
}

/// Look up FUNCTION's docstring in the version's `DOC` file (the same file GNU
/// Emacs distributes in its data directory).  Returns `None` when the DOC file
/// cannot be located or has no entry for the function.
fn lisp_source_root(interp: &Interpreter) -> Option<PathBuf> {
    crate::lisp::primitives::compat_data_directory()
        .map(PathBuf::from)
        .and_then(|etc| etc.parent().map(|root| root.join("lisp")))
        .filter(|root| fs::is_directory(root))
        .or_else(|| {
            interp
                .configured_load_path()
                .iter()
                .find(|path| path.file_name().is_some_and(|name| name == "lisp"))
                .cloned()
        })
}

fn builtin_doc_from_doc_file(interp: &Interpreter, function: &str) -> Option<String> {
    let path = crate::lisp::primitives::compat_data_directory()
        .map(PathBuf::from)
        .map(|etc| etc.join("DOC"))
        .filter(|path| fs::is_regular_file(path))
        .or_else(|| {
            lisp_source_root(interp)?
                .parent()
                .map(|root| root.join("etc/DOC"))
        })?;
    let path_str = path.to_string_lossy().to_string();

    let map = DOC_FILE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some((cached_path, cached_map)) = cache.as_ref()
            && *cached_path == path_str
        {
            return Some(cached_map.clone());
        }
        let bytes = std::fs::read(&path).ok()?;
        let parsed = std::rc::Rc::new(parse_doc_file(&bytes));
        *cache = Some((path_str.clone(), parsed.clone()));
        Some(parsed)
    })?;

    map.get(function).cloned()
}

thread_local! {
    // Cache of (lisp-root-path -> {function-name -> docstring}) scanned lazily.
    static LISP_SOURCE_DOC_CACHE: std::cell::RefCell<DocSourceCache> =
        const { std::cell::RefCell::new(None) };
}

/// Look up FUNCTION's docstring in the GNU lisp sources on the load path.
///
/// The version's `lisp/` tree sits next to the data directory.  Many functions
/// (subr.el, files.el, simple.el, …) are implemented natively in emaxx and so
/// have no lambda body to read a docstring from, yet their docstrings are not in
/// the `DOC` file either — they live inline in the byte-compiled sources.  We
/// scan the `.el` sources once and cache a name → first-docstring map.
pub(crate) fn fallback_function_documentation(
    interp: &Interpreter,
    function: &str,
) -> Option<String> {
    // The etc/DOC database is the sole honest source for native
    // documentation; scraping GNU .el sources forged Lisp provenance for
    // natively implemented names.
    builtin_doc_from_doc_file(interp, function)
}

/// Parse a GNU Emacs `DOC` file into a map from function name to docstring.
///
/// Entries are separated by the `\x1f` (unit-separator) byte and prefixed by a
/// type tag: `F` for functions, `V` for variables, `S` for source-file markers.
/// Only function entries are collected here.  The stored docstring keeps the
/// trailing `(fn ...)` usage line exactly as GNU stores it.
fn parse_doc_file(bytes: &[u8]) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for chunk in bytes.split(|&b| b == 0x1f) {
        // Each chunk is `<tag><name>\n<doc>`; skip empties and non-function tags.
        let Some((&tag, rest)) = chunk.split_first() else {
            continue;
        };
        if tag != b'F' {
            continue;
        }
        let Some(newline) = rest.iter().position(|&b| b == b'\n') else {
            continue;
        };
        let name = String::from_utf8_lossy(&rest[..newline]).to_string();
        let doc = String::from_utf8_lossy(&rest[newline + 1..]).to_string();
        map.entry(name).or_insert(doc);
    }
    map
}

/// fns.c:Fget of the bare symbol SYMBOL's PROPERTY, as C calls it:
/// `overriding-plist-environment' first, then the symbol's plist.
pub(in crate::lisp::primitives) fn bare_symbol_get(
    interp: &Interpreter,
    symbol: &crate::lisp::types::SymbolName,
    property: &crate::lisp::types::SymbolName,
) -> Value {
    overriding_plist_property(interp, &crate::lisp::types::Env::new(), symbol, property)
        .or_else(|| interp.get_symbol_property_of(symbol, property))
        .unwrap_or(Value::Nil)
}

/// The `get' primitive, callable directly (a subr's function pointer).
pub(in crate::lisp::primitives) fn direct_get(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "get";
    need_args(name, args, 2)?;
    // fns.c:Fget on two bare symbols with no overriding plist
    // environment: plist_get on the symbol's plist, nothing
    // else read (the general path below resolves positioned
    // symbols and the overriding alist first).
    if let (Kind::Symbol(symbol), Kind::Symbol(property)) = (args[0].kind(), args[1].kind())
        && interp.overriding_plist_environment_is_nil()
    {
        return Ok(interp
            .get_symbol_property_of(&symbol, &property)
            .unwrap_or(Value::Nil));
    }
    // GNU 30.2 fns.c:Fget applies CHECK_SYMBOL to SYMBOL and
    // XSYMBOL to the same underlying bare symbol.
    let symbol = checked_symbol_identity(interp, &args[0], env)?;
    let property = bare_symbol_identity(interp, env, &args[1]);
    // GNU fns.c:Fget consults `overriding-plist-environment'
    // (populated by bytecomp's compile-time handler for top-level
    // `function-put'/`define-symbol-prop') before the symbol's
    // own plist, returning the first non-nil hit.
    if let Some(property) = &property {
        if let Some(overriding) = overriding_plist_property(interp, env, &symbol, property) {
            return Ok(overriding);
        }
        return Ok(interp
            .get_symbol_property_of(&symbol, property)
            .unwrap_or(Value::Nil));
    }
    Ok(symbol_property_by_eq(interp, env, &symbol, &args[1]).unwrap_or(Value::Nil))
}

/// The `symbol-value' primitive, callable directly (a subr's function pointer).
pub(super) fn direct_symbol_value(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let name = "symbol-value";
    need_args(name, args, 1)?;
    // GNU 30.2 data.c:Fsymbol_value reaches
    // find_symbol_value's CHECK_SYMBOL.
    let symbol = checked_symbol_identity(interp, &args[0], env)?;
    match interp
        .symbol_value_cell_symbol(&symbol)
        .map_err(LispError::into_kind)
    {
        // Fsymbol_value signals with its original argument,
        // even after find_symbol_value follows an alias or
        // XSYMBOL unwraps a symbol-with-position.
        Err(LispErrorKind::Void(_)) => Err(LispError::SignalValue(Value::list([
            Value::symbol("void-variable"),
            args[0],
        ]))),
        Err(other) => Err(LispError::from(other)),
        Ok(value) => Ok(value),
    }
}
