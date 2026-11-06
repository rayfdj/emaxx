use super::*;
use crate::lisp::types::Kind;

fn require_live_frame(interp: &Interpreter, frame: Option<&Value>) -> Result<(), LispError> {
    match frame.map(|v| v.kind()) {
        None | Some(Kind::Nil) => Ok(()),
        Some(Kind::Frame(id)) if interp.frame_is_live(id) => Ok(()),
        Some(frame) => Err(wrong_type_argument("frame-live-p", frame.value())),
    }
}

fn require_any_frame(frame: Option<&Value>) -> Result<(), LispError> {
    match frame.map(|v| v.kind()) {
        None | Some(Kind::Nil | Kind::Frame(_)) => Ok(()),
        Some(frame) => Err(wrong_type_argument("framep", frame.value())),
    }
}

/// The non-X hosts' window-system boundary.
#[cfg(not(target_os = "linux"))]
fn window_system_unavailable() -> LispError {
    LispError::Signal("Window system is not in use or not initialized".into())
}

fn window_system_frame_required() -> LispError {
    LispError::Signal("Window system frame should be used".into())
}

/// CHECK_SYMBOL of an optional argument (nil when absent).
fn check_symbol(value: Option<&Value>) -> Result<(), LispError> {
    match value {
        Some(value) if !value.is_symbol() => Err(wrong_type_argument("symbolp", *value)),
        _ => Ok(()),
    }
}

/// xselect.c's frame_for_x_selection: the X frame a selection call uses,
/// of which Emaxx has none; only a terminal designator is decoded first.
fn frame_for_x_selection(interp: &Interpreter, object: Option<&Value>) -> Result<(), LispError> {
    match object {
        Some(object) if matches!(object.kind(), Kind::Terminal(_)) => interp
            .decode_terminal(object)
            .map(|_| ())
            .ok_or_else(|| wrong_type_argument("terminal-live-p", *object)),
        _ => Ok(()),
    }
}

fn require_fixnum(value: &Value) -> Result<(), LispError> {
    if matches!(value.kind(), Kind::Integer(_)) {
        Ok(())
    } else {
        Err(wrong_type_argument("fixnump", *value))
    }
}

fn validate_popup_position(interp: &Interpreter, position: &Value) -> Result<(), LispError> {
    if position == &Value::T {
        return Ok(());
    }
    let items = position
        .to_vec()
        .map_err(|_| wrong_type_argument("listp", *position))?;
    if let Some(Kind::Cons(_)) = items.first().map(|v| v.kind()) {
        let coordinates = items[0]
            .to_vec()
            .map_err(|_| wrong_type_argument("listp", items[0]))?;
        if coordinates.len() >= 2 {
            require_fixnum(&coordinates[0])?;
            require_fixnum(&coordinates[1])?;
        }
        if let Some(window) = items.get(1) {
            let valid_window = matches!(window.kind(), Kind::Frame(id) if interp.frame_is_live(id))
                || matches!(window.kind(), Kind::Record(id)
                        if interp.find_record(id).is_some_and(|record|
                            record.kind == crate::lisp::eval::RecordKind::Window));
            if !valid_window {
                return Err(wrong_type_argument("windowp", *window));
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
        .map_err(|_| wrong_type_argument("listp", *menu))?;
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
            .map_err(|_| wrong_type_argument("listp", *pane))?;
        let pane_title = pane_items.first().cloned().unwrap_or(Value::Nil);
        if !pane_title.is_string() {
            return Err(wrong_type_argument("stringp", pane_title));
        }
        // menu.c:list_of_panes CHECK_CONSes the pane's items, and
        // list_of_items CHECK_STRINGs the name of each (NAME . VALUE).
        if pane_items.len() < 2 {
            return Err(wrong_type_argument(
                "consp",
                pane.cdr().unwrap_or(Value::Nil),
            ));
        }
        for item in &pane_items[1..] {
            if let Kind::Cons(_) = item.kind() {
                let name = item.car().unwrap_or(Value::Nil);
                if !name.is_string() {
                    return Err(wrong_type_argument("stringp", name));
                }
            }
        }
    }
    Ok(())
}

/// The first (X . Y) coordinate pair inside POSITION, whatever of the
/// accepted position forms carries it.
fn popup_position_xy(position: &Value) -> Option<(i64, i64)> {
    match position.kind() {
        Kind::Cons(_) => {
            if let (Ok(Kind::Integer(x)), Ok(Kind::Integer(y))) = (
                position.car().map(|v| v.kind()),
                position.cdr().map(|v| v.kind()),
            ) {
                return Some((x, y));
            }
            let mut tail = *position;
            while let Kind::Cons(_) = tail.kind() {
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

/// x_popup_menu_1's reading of POSITION on a tty frame: the
/// frame-relative column and row, MENU_FOR_CLICK (POSITION is a click
/// event, not t, a bar position or ((XOFFSET YOFFSET) WINDOW)) and
/// MENU_KBD_NAVIGATION (a click on the menu bar made by hand, not the
/// last real event).
struct PopupPlace {
    x: i64,
    y: i64,
    for_click: bool,
    kbd_navigation: bool,
}

/// The column and row of WINDOW's top-left corner on its frame, the
/// frame's origin for anything else.
fn popup_window_origin(interp: &mut Interpreter, window: &Value) -> (i64, i64) {
    if let Kind::Record(id) = window.kind() {
        let layout = crate::lisp::primitives::window_render_layout(interp);
        if let Some(info) = layout.iter().find(|info| info.window_id == id.id) {
            return (info.left as i64, info.top as i64);
        }
    }
    (0, 0)
}

fn popup_place(interp: &mut Interpreter, position: &Value) -> PopupPlace {
    let head = position.car().unwrap_or(Value::Nil);
    let second = position
        .cdr()
        .and_then(|tail| tail.car())
        .unwrap_or(Value::Nil);
    if let Kind::Cons(_) = head.kind() {
        // ((XOFFSET YOFFSET) WINDOW): offsets from WINDOW's edges, or
        // from the frame's origin when WINDOW is a frame.
        let x = head
            .car()
            .ok()
            .and_then(|v| v.as_integer().ok())
            .unwrap_or(0);
        let y = head
            .cdr()
            .and_then(|tail| tail.car())
            .ok()
            .and_then(|v| v.as_integer().ok())
            .unwrap_or(0);
        let (left, top) = popup_window_origin(interp, &second);
        return PopupPlace {
            x: x + left,
            y: y + top,
            for_click: false,
            kbd_navigation: false,
        };
    }
    let bar = *position == Value::T
        || (head.is_key(cached_symbol!("menu-bar"))
            || head.is_key(cached_symbol!("tab-bar"))
            || head.is_key(cached_symbol!("tool-bar")))
        || second.is_key(cached_symbol!("tab-bar"));
    let (mut x, mut y) = popup_position_xy(position).unwrap_or((0, 0));
    if bar {
        return PopupPlace {
            x,
            y,
            for_click: false,
            kbd_navigation: false,
        };
    }
    // A click event's posn carries window-relative coordinates; the
    // dropdown draws in frame coordinates (Fx_popup_menu converts the
    // same way before calling the terminal's menu hook).
    let window = second.car().unwrap_or(Value::Nil);
    let (left, top) = popup_window_origin(interp, &window);
    x += left;
    y += top;
    // POSN_POSN of the event's start: (menu-bar) for the menu bar.  A
    // posn other than the last real event's was made by hand, as
    // menu-bar-open makes one for F10.
    let posn_posn = |value: &Value| {
        value
            .cdr()
            .and_then(|tail| tail.car())
            .unwrap_or(Value::Nil)
    };
    let area = posn_posn(&second);
    let last = interp
        .lookup_var_key(
            cached_symbol!("last-nonmenu-event"),
            &crate::lisp::types::Env::new(),
        )
        .unwrap_or(Value::Nil);
    let kbd_navigation = !crate::lisp::types::eq(posn_posn(&last), posn_posn(position))
        && matches!(area.kind(), Kind::Cons(_))
        && area
            .car()
            .is_ok_and(|head| head.is_key(cached_symbol!("menu-bar")));
    PopupPlace {
        x,
        y,
        for_click: true,
        kbd_navigation,
    }
}

/// term.c:tty_menu_show's activation of PANE at X, Y under
/// `tty-menu-navigation-map', through the frontend's dropdown.
fn run_tty_pane(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    pane: &crate::lisp::primitives::TtyMenuPane,
    x: i64,
    y: i64,
) -> Result<Option<crate::lisp::primitives::TtyMenuOutcome>, LispError> {
    let x0 = x.max(1) as usize;
    let y0 = y.max(1) as usize;
    // GNU specbinds overriding-terminal-local-map to the navigation map
    // for the whole activation, so ordinary key resolution answers the
    // tty-menu-* commands.
    let navigation = interp
        .lookup_var_key(cached_symbol!("tty-menu-navigation-map"), env)
        .unwrap_or(Value::Nil);
    let restore = interp.bind_special_dynamic("overriding-terminal-local-map", navigation, env)?;
    let outcome = crate::lisp::primitives::run_tty_menu_executor(interp, env, pane, x0, y0);
    match &outcome {
        Err(error) => interp
            .with_lisp_stack_roots(error, |interp| interp.restore_special_dynamic(restore, env)),
        Ok(_) => interp.restore_special_dynamic(restore, env),
    }?;
    outcome
}

/// tty_menu_show's TTYM_NO_SELECT: "Cancel" is C-g unless the menu was
/// opened by a click.
fn tty_menu_cancelled(place: &PopupPlace) -> Result<Value, LispError> {
    if place.for_click {
        Ok(Value::Nil)
    } else {
        Err(LispError::SignalValue(Value::list([Value::symbol("quit")])))
    }
}

/// menu.c's list_of_panes and list_of_items for an old-fashioned
/// (TITLE (PANE-NAME ITEM...)) menu of one pane, which tty_menu_activate
/// shows as its only pane under the pane's name: a string ITEM is
/// disabled, (NAME . VALUE) answers VALUE, and anything else (a dialog's
/// left-right boundary) shows nothing on a tty.
fn tty_popup_list_menu(
    interp: &mut Interpreter,
    position: &Value,
    menu: &Value,
) -> Result<Value, LispError> {
    let panes = menu
        .cdr()
        .and_then(|panes| panes.to_vec())
        .unwrap_or_default();
    // Several panes cascade from a menu of their names, which the
    // frontend's dropdown does not draw.
    let [pane] = panes.as_slice() else {
        return Ok(Value::Nil);
    };
    let entries = pane.to_vec().unwrap_or_default();
    let Some(title) = entries.first().and_then(string_like) else {
        return Ok(Value::Nil);
    };
    let mut items = Vec::new();
    for entry in &entries[1..] {
        match entry.kind() {
            Kind::StringObject(_) => items.push(crate::lisp::primitives::TtyMenuPaneItem {
                text: string_text(entry)?,
                enabled: false,
                key: Value::Nil,
            }),
            Kind::Cons(_) => items.push(crate::lisp::primitives::TtyMenuPaneItem {
                text: string_text(&entry.car()?)?,
                enabled: true,
                key: entry.cdr()?,
            }),
            _ => {}
        }
    }
    if items.is_empty() {
        return Ok(Value::Nil);
    }
    let width = items
        .iter()
        .map(|item| item.text.chars().count())
        .max()
        .unwrap_or(0);
    let place = popup_place(interp, position);
    let pane = crate::lisp::primitives::TtyMenuPane {
        title: title.text,
        items,
        width,
        kbd_navigation: place.kbd_navigation,
    };
    let mut env = crate::lisp::types::Env::new();
    match run_tty_pane(interp, &mut env, &pane, place.x, place.y)? {
        Some(crate::lisp::primitives::TtyMenuOutcome::Selected(index)) => Ok(pane
            .items
            .get(index)
            .map(|item| item.key)
            .unwrap_or(Value::Nil)),
        Some(crate::lisp::primitives::TtyMenuOutcome::Quit) => tty_menu_cancelled(&place),
        // The adjacent menu-bar menu's coordinates are a keymap menu's
        // answer; tty_menu_new_item_coords is reached only under
        // KBD_NAVIGATION.
        Some(_) | None => Ok(Value::Nil),
    }
}

/// term.c's tty_menu_show for a single menu keymap: draw and activate
/// the dropdown through the frontend, under tty-menu-navigation-map.
fn tty_popup_menu(
    interp: &mut Interpreter,
    position: &Value,
    menu: &Value,
) -> Result<Value, LispError> {
    let mut env = crate::lisp::types::Env::new();
    let place = popup_place(interp, position);
    let (x, y) = (place.x, place.y);
    // The pane title is the menu keymap's prompt string ("File").
    let title = {
        let mut title = String::new();
        let mut tail = menu.cdr().unwrap_or(Value::Nil);
        while let Kind::Cons(_) = tail.kind() {
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
    match run_tty_pane(interp, &mut env, &pane, x, y)? {
        Some(crate::lisp::primitives::TtyMenuOutcome::Selected(index)) => Ok(Value::list([pane
            .items
            .get(index)
            .map(|item| item.key)
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
        // TTYM_NO_SELECT: the event-shaped menu-bar position counts as
        // a click, so cancelling it answers nil without signalling quit
        // (no "Quit" echo, as GNU).
        Some(crate::lisp::primitives::TtyMenuOutcome::Quit) => tty_menu_cancelled(&place),
        None => Ok(Value::Nil),
    }
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "menu-bar-menu-at-x-y" => {
                need_arg_range(name, args, 2, 3)?;
                require_any_frame(args.get(2))?;
                // menu.c reads X and Y with XFIXNUM unchecked, which crashes
                // GNU on what is no fixnum; they are checked here instead.
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
                let mut env = crate::lisp::types::Env::new();
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
            "x-change-window-property" => {
                need_arg_range(name, args, 2, 7)?;
                // xfns.c decodes a live window-system frame before reading
                // PROP, VALUE or FORMAT. The current frontend owns terminal
                // frames, which GNU rejects at this same boundary.
                super::frames::decode_live_frame(interp, args.get(2), true)?;
                Err(window_system_frame_required())
            }
            "x-window-property" | "x-window-property-attributes" | "x-delete-window-property" => {
                need_arg_range(
                    name,
                    args,
                    1,
                    if name == "x-window-property" { 6 } else { 3 },
                )?;
                super::frames::decode_live_frame(interp, args.get(1), true)?;
                Err(window_system_frame_required())
            }
            "x-close-connection" => {
                need_args(name, args, 1)?;
                Err(super::frames::check_x_display_info(
                    interp,
                    args.first(),
                    env,
                ))
            }
            "x-send-client-message" => {
                need_args(name, args, 6)?;
                Err(super::frames::check_x_display_info(
                    interp,
                    args.first(),
                    env,
                ))
            }
            "x-get-atom-name" | "x-register-dnd-atom" => {
                need_arg_range(name, args, 1, 2)?;
                Err(super::frames::window_system_frame_error(
                    interp,
                    args.get(1),
                ))
            }
            // xselect.c: no frame is an X frame, so frame_for_x_selection
            // finds none (after decoding a terminal designator).
            "x-selection-exists-p" | "x-selection-owner-p" => {
                need_arg_range(name, args, 0, 2)?;
                frame_for_x_selection(interp, args.get(1))?;
                check_symbol(args.first())?;
                Ok(Value::Nil)
            }
            "x-disown-selection-internal" => {
                need_arg_range(name, args, 1, 3)?;
                frame_for_x_selection(interp, args.get(2))?;
                Ok(Value::Nil)
            }
            "x-get-selection-internal" => {
                need_arg_range(name, args, 2, 4)?;
                frame_for_x_selection(interp, args.get(3))?;
                check_symbol(args.first())?;
                check_symbol(args.get(1))?;
                if args[1] == Value::symbol("MULTIPLE") {
                    return Err(LispError::Signal(
                        "Retrieving MULTIPLE selections is currently unimplemented".into(),
                    ));
                }
                Err(LispError::Signal(
                    "X selection unavailable for this frame".into(),
                ))
            }
            "x-own-selection-internal" => {
                need_arg_range(name, args, 2, 3)?;
                Err(LispError::Signal(
                    "X selection unavailable for this frame".into(),
                ))
            }
            // VALUE is (NAME SELECTION-VALUE TIMESTAMP FRAME . _).
            "x-get-local-selection" => {
                need_arg_range(name, args, 0, 2)?;
                check_symbol(args.get(1))?;
                let value = args.first().copied().unwrap_or(Value::Nil);
                let check_cons = |value: Value| {
                    value
                        .cons_values()
                        .ok_or_else(|| wrong_type_argument("consp", value))
                };
                let (selection_name, rest) = check_cons(value)?;
                let (_, rest) = check_cons(rest)?;
                let (timestamp, rest) = check_cons(rest)?;
                let (frame, _) = check_cons(rest)?;
                check_symbol(Some(&selection_name))?;
                crate::lisp::primitives::system::cons_to_unsigned(&timestamp, u64::MAX)?;
                Err(super::frames::window_system_frame_error(
                    interp,
                    Some(&frame),
                ))
            }
            "x-create-frame" => {
                need_args(name, args, 1)?;
                let parms = super::call(interp, "copy-alist", &[args[0]], env)?;
                #[cfg(target_os = "linux")]
                {
                    // xfns.c: the resource name starts as the invocation
                    // name, and the display is the `terminal', else the
                    // `display' parameter (gui_display_get_arg: PARMS, then
                    // `default-frame-alist'), checked as an X display.
                    let invocation_name = interp
                        .lookup_var_key(cached_symbol!("invocation-name"), env)
                        .unwrap_or(Value::Nil);
                    interp.set_c_variable("x-resource-name", invocation_name);
                    let defaults = interp
                        .lookup_var_key(cached_symbol!("default-frame-alist"), env)
                        .unwrap_or(Value::Nil);
                    let mut display = None;
                    for parameter in ["terminal", "display"] {
                        for alist in [parms, defaults] {
                            let found = super::call(
                                interp,
                                "assq",
                                &[Value::symbol(parameter), alist],
                                env,
                            )?;
                            if let Some((_, value)) = found.cons_values() {
                                display = Some(value);
                                break;
                            }
                        }
                        if display.is_some() {
                            break;
                        }
                    }
                    let display = display.unwrap_or(Value::Nil);
                    Err(super::frames::check_x_display_info(
                        interp,
                        Some(&display),
                        env,
                    ))
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let _ = parms;
                    Err(window_system_unavailable())
                }
            }
            "x-show-tip" => {
                need_arg_range(name, args, 1, 6)?;
                if !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0]));
                }
                string_text(&args[0])?;
                require_live_frame(interp, args.get(1))?;
                Err(window_system_frame_required())
            }
            "x-open-connection" => {
                need_arg_range(name, args, 1, 3)?;
                if !args[0].is_string() {
                    return Err(wrong_type_argument("stringp", args[0]));
                }
                string_text(&args[0])?;
                #[cfg(target_os = "linux")]
                {
                    // xfns.c: the resource string is checked, the resource
                    // name validated, and x_term_init cannot open the
                    // display (Emaxx opens none).
                    if let Some(xrm) = args.get(1).filter(|xrm| !xrm.is_nil())
                        && !xrm.is_string()
                    {
                        return Err(wrong_type_argument("stringp", *xrm));
                    }
                    super::frames::validate_x_resource_name(interp, env)?;
                    Err(super::frames::display_cannot_be_opened(interp, &args[0]))
                }
                // The Nextstep implementation ignores the optional resource and
                // must-succeed arguments.  Emaxx deliberately does not launch a
                // platform GUI application from its terminal runtime.
                #[cfg(not(target_os = "linux"))]
                Err(window_system_unavailable())
            }
            "x-popup-dialog" => {
                need_arg_range(name, args, 2, 3)?;
                let mut env = crate::lisp::types::Env::new();
                let (position, contents) = (args[0], args[1]);
                // menu.c:Fx_popup_dialog: the window or frame POSITION
                // names, the selected window for t or a bar position.
                let head = position.car().unwrap_or(Value::Nil);
                let window = if position == Value::T
                    || (head.is_key(cached_symbol!("menu-bar"))
                        || head.is_key(cached_symbol!("tab-bar"))
                        || head.is_key(cached_symbol!("tool-bar")))
                {
                    interp.selected_window_value()
                } else if let Kind::Cons(_) = position.kind() {
                    let rest = position.cdr()?;
                    if let Kind::Cons(_) = head.kind() {
                        super::call(interp, "car", &[rest], &mut env)?
                    } else {
                        let start = super::call(interp, "car", &[rest], &mut env)?;
                        super::call(interp, "car", &[start], &mut env)?
                    }
                } else if matches!(position.kind(), Kind::Frame(_) | Kind::Record(_)) {
                    position
                } else {
                    Value::Nil
                };
                let frame = match window.kind() {
                    Kind::Frame(_) => window,
                    Kind::Record(id)
                        if interp.find_record(id).is_some_and(|record| {
                            record.kind == crate::lisp::eval::RecordKind::Window
                        }) =>
                    {
                        if super::call(interp, "window-live-p", &[window], &mut env)?.is_nil() {
                            return Err(wrong_type_argument("window-live-p", window));
                        }
                        super::call(interp, "window-frame", &[window], &mut env)?
                    }
                    _ => return Err(wrong_type_argument("windowp", window)),
                };
                super::call(interp, "redisplay", &[Value::T], &mut env)?;
                // No terminal of Emaxx's has a popup_dialog_hook, so
                // emulate_dialog_with_menu asks x-popup-menu for a menu of
                // CONTENTS whose prompt is centred on the frame.
                let prompt = super::call(interp, "car", &[contents], &mut env)?;
                let mut x =
                    super::call(interp, "frame-text-cols", &[frame], &mut env)?.as_integer()?;
                if let Some(text) = string_like(&prompt) {
                    x -= text.text.chars().count() as i64;
                }
                let y =
                    super::call(interp, "frame-total-lines", &[frame], &mut env)?.as_integer()?;
                let newpos = Value::list([
                    Value::list([Value::Integer(x / 2), Value::Integer(y / 2)]),
                    frame,
                ]);
                super::call(
                    interp,
                    "x-popup-menu",
                    &[newpos, Value::list([prompt, contents])],
                    &mut env,
                )
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
                if crate::lisp::primitives::has_tty_menu_executor() {
                    if is_keymap_value(interp, &args[1]) {
                        return tty_popup_menu(interp, &args[0], &args[1]);
                    }
                    if args[1].car().is_ok_and(|title| title.is_string()) {
                        return tty_popup_list_menu(interp, &args[0], &args[1]);
                    }
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
