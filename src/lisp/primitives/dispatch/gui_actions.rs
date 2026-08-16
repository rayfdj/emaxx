use super::*;

fn require_live_frame(interp: &Interpreter, frame: Option<&Value>) -> Result<(), LispError> {
    match frame {
        None | Some(Value::Nil) => Ok(()),
        Some(Value::Frame(id)) if interp.frame_is_live(*id) => Ok(()),
        Some(frame) => Err(wrong_type_argument("frame-live-p", frame.clone())),
    }
}

fn require_any_frame(frame: Option<&Value>) -> Result<(), LispError> {
    match frame {
        None | Some(Value::Nil | Value::Frame(_)) => Ok(()),
        Some(frame) => Err(wrong_type_argument("framep", frame.clone())),
    }
}

fn window_system_unavailable() -> LispError {
    LispError::Signal("Window system is not in use or not initialized".into())
}

fn window_system_frame_required() -> LispError {
    LispError::Signal("Window system frame should be used".into())
}

fn require_fixnum(value: &Value) -> Result<(), LispError> {
    if matches!(value, Value::Integer(_)) {
        Ok(())
    } else {
        Err(wrong_type_argument("fixnump", value.clone()))
    }
}

fn validate_popup_position(interp: &Interpreter, position: &Value) -> Result<(), LispError> {
    if position == &Value::T {
        return Ok(());
    }
    let items = position
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", position.clone()))?;
    if let Some(Value::Cons(_)) = items.first() {
        let coordinates = items[0]
            .to_vec()
            .map_err(|_| wrong_type_argument("listp", items[0].clone()))?;
        if coordinates.len() >= 2 {
            require_fixnum(&coordinates[0])?;
            require_fixnum(&coordinates[1])?;
        }
        if let Some(window) = items.get(1) {
            let valid_window = matches!(window, Value::Frame(id) if interp.frame_is_live(*id))
                || matches!(window, Value::Record(id)
                        if interp.find_record(*id).is_some_and(|record|
                            record.kind == crate::lisp::eval::RecordKind::Window));
            if !valid_window {
                return Err(wrong_type_argument("windowp", window.clone()));
            }
        }
    }
    Ok(())
}

fn validate_popup_menu(interp: &Interpreter, menu: &Value) -> Result<(), LispError> {
    if is_keymap_value(interp, menu) {
        return Ok(());
    }
    let items = menu
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", menu.clone()))?;
    if items.iter().all(|item| is_keymap_value(interp, item)) && !items.is_empty() {
        return Ok(());
    }
    let title = items.first().cloned().unwrap_or(Value::Nil);
    if !title.is_string() {
        return Err(wrong_type_argument("stringp", title));
    }
    for pane in items.iter().skip(1) {
        let pane_items = pane
            .to_vec()
            .map_err(|_| wrong_type_argument("listp", pane.clone()))?;
        let pane_title = pane_items.first().cloned().unwrap_or(Value::Nil);
        if !pane_title.is_string() {
            return Err(wrong_type_argument("stringp", pane_title));
        }
    }
    Ok(())
}

/// The first (X . Y) coordinate pair inside POSITION, whatever of the
/// accepted position forms carries it.
fn popup_position_xy(position: &Value) -> Option<(i64, i64)> {
    match position {
        Value::Cons(_) => {
            if let (Ok(Value::Integer(x)), Ok(Value::Integer(y))) = (position.car(), position.cdr())
            {
                return Some((x, y));
            }
            let mut tail = position.clone();
            while let Value::Cons(_) = tail {
                if let Ok(car) = tail.car()
                    && let Some(xy) = popup_position_xy(&car)
                {
                    return Some(xy);
                }
                tail = tail.cdr().unwrap_or(Value::Nil);
            }
            None
        }
        _ => None,
    }
}

/// term.c's tty_menu_show for a single menu keymap: draw and activate
/// the dropdown through the frontend, under tty-menu-navigation-map.
fn tty_popup_menu(
    interp: &mut Interpreter,
    position: &Value,
    menu: &Value,
) -> Result<Value, LispError> {
    let mut env = Vec::new();
    let (x, y) = popup_position_xy(position).unwrap_or((0, 0));
    // The pane title is the menu keymap's prompt string ("File").
    let title = {
        let projected = crate::lisp::primitives::public_keymap_value(interp, menu);
        let mut title = String::new();
        let mut tail = projected.cdr().unwrap_or(Value::Nil);
        while let Value::Cons(_) = tail {
            if let Ok(car) = tail.car()
                && let Ok(text) = string_text(&car)
            {
                title = text;
                break;
            }
            tail = tail.cdr().unwrap_or(Value::Nil);
        }
        title
    };
    let pane = crate::lisp::primitives::tty_menu_pane_from_keymap(interp, &mut env, menu, &title);
    if pane.items.is_empty() {
        return Ok(Value::Nil);
    }
    let x0 = x.max(1) as usize;
    let y0 = y.max(1) as usize;
    // GNU specbinds overriding-terminal-local-map to the navigation map
    // for the whole activation, so ordinary key resolution answers the
    // tty-menu-* commands.
    let navigation = interp
        .lookup_var("tty-menu-navigation-map", &env)
        .unwrap_or(Value::Nil);
    let restore =
        interp.bind_special_dynamic("overriding-terminal-local-map", navigation, &mut env)?;
    let outcome = crate::lisp::primitives::run_tty_menu_executor(interp, &mut env, &pane, x0, y0);
    interp.restore_special_dynamic(restore, &mut env)?;
    match outcome {
        Some(crate::lisp::primitives::TtyMenuOutcome::Selected(index)) => Ok(Value::list([pane
            .items
            .get(index)
            .map(|item| item.key.clone())
            .unwrap_or(Value::Nil)])),
        Some(
            direction @ (crate::lisp::primitives::TtyMenuOutcome::NextMenu
            | crate::lisp::primitives::TtyMenuOutcome::PrevMenu),
        ) => {
            // tty_menu_new_item_coords: the adjacent menu-bar item's
            // column, wrapping at either end.
            let items = crate::lisp::primitives::menu_bar_row_items(interp, &mut env);
            if items.is_empty() {
                return Ok(Value::Nil);
            }
            let current = items
                .iter()
                .position(|(caption, _, column)| {
                    (*column as i64) <= x && x < (*column + caption.chars().count() + 1) as i64
                })
                .unwrap_or(0);
            let next = match direction {
                crate::lisp::primitives::TtyMenuOutcome::NextMenu => (current + 1) % items.len(),
                _ => (current + items.len() - 1) % items.len(),
            };
            Ok(Value::cons(
                Value::Integer(items[next].2 as i64),
                Value::Integer(0),
            ))
        }
        Some(crate::lisp::primitives::TtyMenuOutcome::NoSelect) => Ok(Value::Nil),
        Some(crate::lisp::primitives::TtyMenuOutcome::Quit) => {
            // TTYM_NO_SELECT with MENU_FOR_CLICK: the event-shaped
            // menu-bar position counts as a click, so cancelling answers
            // nil without signalling quit (no "Quit" echo, as GNU).
            Ok(Value::Nil)
        }
        None => Ok(Value::Nil),
    }
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
    ) -> Result<Value, LispError> {
        match name {
            "menu-bar-menu-at-x-y" => {
                need_arg_range(name, args, 2, 3)?;
                require_any_frame(args.get(2))?;
                require_fixnum(&args[0])?;
                require_fixnum(&args[1])?;
                let x = args[0].as_integer()?;
                let y = args[1].as_integer()?;
                // menu.c: the menu-bar item whose caption spans column X
                // of row Y (the item's recorded display geometry).  A
                // click in the gap after a caption still counts as that
                // item, GNU's <= comparison against the next start.
                if y != 0 {
                    return Ok(Value::Nil);
                }
                let mut env = Vec::new();
                let items = crate::lisp::primitives::menu_bar_row_items(interp, &mut env);
                let mut chosen = Value::Nil;
                for (caption, key, column) in items {
                    if (column as i64) <= x {
                        if x < (column + caption.chars().count() + 1) as i64 {
                            chosen = key;
                            break;
                        }
                    } else {
                        break;
                    }
                }
                Ok(chosen)
            }
            "x-begin-drag" => {
                need_arg_range(name, args, 1, 6)?;
                require_live_frame(interp, args.get(2))?;
                Err(window_system_frame_required())
            }
            "x-close-connection" => {
                need_args(name, args, 1)?;
                match &args[0] {
                    Value::Nil | Value::String(_) | Value::StringObject(_) => {}
                    Value::Frame(id) if interp.frame_is_live(*id) => {}
                    Value::Terminal(id) if *id == 0 && interp.terminal_live() => {}
                    terminal => {
                        return Err(wrong_type_argument("frame-live-p", terminal.clone()));
                    }
                }
                Err(window_system_unavailable())
            }
            "x-create-frame" => {
                need_args(name, args, 1)?;
                args[0]
                    .to_vec()
                    .map_err(|_| wrong_type_argument("listp", args[0].clone()))?;
                Err(window_system_unavailable())
            }
            "x-show-tip" => {
                need_arg_range(name, args, 1, 6)?;
                if !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0].clone()));
                }
                string_text(&args[0])?;
                require_live_frame(interp, args.get(1))?;
                Err(window_system_frame_required())
            }
            "x-file-dialog" => {
                need_arg_range(name, args, 2, 5)?;
                Err(window_system_unavailable())
            }
            "x-open-connection" => {
                need_arg_range(name, args, 1, 3)?;
                if !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0].clone()));
                }
                string_text(&args[0])?;
                // The Nextstep implementation ignores the optional resource and
                // must-succeed arguments.  Emaxx deliberately does not launch a
                // platform GUI application from its terminal runtime.
                Err(window_system_unavailable())
            }
            "x-popup-dialog" => {
                need_arg_range(name, args, 2, 3)?;
                match &args[0] {
                    Value::T => {}
                    Value::Frame(id) if interp.frame_is_live(*id) => {}
                    Value::Record(id)
                        if interp.find_record(*id).is_some_and(|record| {
                            record.kind == crate::lisp::eval::RecordKind::Window
                        }) => {}
                    Value::Cons(_) => {}
                    _ => return Err(wrong_type_argument("windowp", Value::Nil)),
                }
                let contents = args[1]
                    .to_vec()
                    .map_err(|_| wrong_type_argument("listp", args[1].clone()))?;
                let title = contents.first().cloned().unwrap_or(Value::Nil);
                if !title.is_string() {
                    return Err(wrong_type_argument("stringp", title));
                }
                // GNU's initial batch frame has no menu hook, so a well-formed
                // dialog is parsed but cannot produce a selection.
                Ok(Value::Nil)
            }
            "x-popup-menu" => {
                need_args(name, args, 2)?;
                // GNU preserves this obsolete cache-warming form as an immediate
                // no-op and deliberately does not inspect MENU.
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                validate_popup_position(interp, &args[0])?;
                validate_popup_menu(interp, &args[1])?;
                // A live tty frontend shows the real dropdown (term.c's
                // tty_menu_show); batch keeps GNU's menu-hook-less answer.
                if crate::lisp::primitives::has_tty_menu_executor()
                    && is_keymap_value(interp, &args[1])
                {
                    return tty_popup_menu(interp, &args[0], &args[1]);
                }
                Ok(Value::Nil)
            }
            "x-select-font" => {
                need_arg_range(name, args, 0, 2)?;
                require_live_frame(interp, args.first())?;
                Err(window_system_frame_required())
            }
        }
    }
);
