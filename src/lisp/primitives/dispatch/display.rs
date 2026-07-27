use super::*;
use crate::lisp::primitives::processes::wait_pumping_processes;

fn valid_image_spec(interp: &Interpreter, spec: &Value, env: &Env) -> bool {
    let Ok(items) = spec.to_vec() else {
        return false;
    };
    if !matches!(items.first(), Some(Value::Symbol(head)) if head == "image")
        || items.len() < 5
        || (items.len() - 1) % 2 != 0
    {
        return false;
    }
    let mut properties = std::collections::HashMap::new();
    for pair in items[1..].chunks_exact(2) {
        let Value::Symbol(key) = &pair[0] else {
            return false;
        };
        if !key.starts_with(':') || properties.insert(key.as_str(), &pair[1]).is_some() {
            return false;
        }
    }
    let Some(Value::Symbol(image_type)) = properties.get(":type").copied() else {
        return false;
    };
    let type_supported = interp
        .lookup_var("image-types", env)
        .and_then(|types| types.to_vec().ok())
        .is_some_and(|types| {
            types
                .iter()
                .any(|value| matches!(value, Value::Symbol(name) if name == image_type))
        });
    let file = properties.get(":file").copied();
    let data = properties.get(":data").copied();
    type_supported
        && matches!(
            (file, data),
            (Some(Value::String(_) | Value::StringObject(_)), None)
                | (None, Some(Value::String(_) | Value::StringObject(_)))
        )
}

pub(super) fn handles(name: &str) -> bool {
    matches!(
        name,
        "substitute-command-keys"
            | "message"
            | "message-box"
            | "message-or-box"
            | "warn"
            | "display-warning"
            | "current-message"
            | "error-message-string"
            | "command-error-default-function"
            | "ding"
            | "make-progress-reporter"
            | "progress-reporter-update"
            | "progress-reporter-done"
            | "vc-refresh-state"
            | "sleep-for"
            | "sit-for"
            | "accept-process-output"
            | "input-pending-p"
            | "discard-input"
            | "prin1"
            | "cl-prin1"
            | "princ"
            | "print"
            | "terpri"
            | "prin1-to-string"
            | "cl-prin1-to-string"
            | "cl-print--expand-ellipsis"
            | "write-char"
            | "redirect-debugging-output"
            | "external-debugging-output"
            | "print--preprocess"
            | "read-char-choice"
            | "y-or-n-p"
            | "yes-or-no-p"
            | "char-equal"
            | "number-sequence"
            | "kbd"
            | "key-description"
            | "single-key-description"
            | "text-char-description"
            | "following-char"
            | "preceding-char"
            | "buffer-last-name"
            | "display-graphic-p"
            | "display-supports-face-attributes-p"
            | "display-images-p"
            | "display-color-p"
            | "display-grayscale-p"
            | "display-color-cells"
            | "window-system"
            | "frame-parameter"
            | "frame-parameters"
            | "modify-frame-parameters"
            | "set-frame-parameter"
            | "char-displayable-p"
            | "internal-char-font"
            | "current-input-mode"
            | "set-input-interrupt-mode"
            | "set-output-flow-control"
            | "set-input-meta-mode"
            | "set-input-mode"
            | "set-quit-char"
            | "frame-width"
            | "frame-height"
            | "set-frame-width"
            | "set-frame-height"
            | "frame-char-width"
            | "frame-char-height"
            | "frame-internal-border-width"
            | "frame-native-width"
            | "frame-native-height"
            | "frame-pixel-width"
            | "frame-pixel-height"
            | "frame-text-width"
            | "frame-text-height"
            | "frame-text-cols"
            | "frame-text-lines"
            | "frame-fringe-width"
            | "frame-scroll-bar-width"
            | "frame-scroll-bar-height"
            | "frame-right-divider-width"
            | "frame-bottom-divider-width"
            | "display-popup-menus-p"
            | "menu-or-popup-active-p"
            | "imagep"
            | "clear-image-cache"
            | "image-cache-size"
            | "image-flush"
            | "image-transforms-p"
            | "transient-mark-mode"
            | "font-lock-mode"
            | "visual-line-mode"
            | "header-line-indent-mode"
            | "font-lock-specified-p"
            | "font-lock-add-keywords"
            | "font-lock-remove-keywords"
            | "font-lock-flush"
            | "font-lock-ensure"
            | "font-lock-fontify-region"
            | "find-image"
            | "image-size"
            | "image-mask-p"
            | "image-metadata"
            | "imagemagick-types"
            | "init-image-library"
            | "window-start"
            | "window-end"
            | "window-point"
            | "window-hscroll"
            | "window-vscroll"
            | "set-window-hscroll"
            | "scroll-left"
            | "scroll-right"
            | "other-window-for-scrolling"
            | "window-margins"
            | "set-window-margins"
            | "coordinates-in-window-p"
            | "pos-visible-in-window-p"
            | "window-body-width"
            | "window-body-height"
            | "window-text-width"
            | "window-text-height"
            | "window-line-height"
            | "window-lines-pixel-dimensions"
            | "window-total-width"
            | "window-total-height"
            | "window-width"
            | "window-height"
            | "window-use-time"
            | "window-bump-use-time"
            | "window-pixel-width"
            | "window-pixel-height"
            | "window-pixel-left"
            | "window-pixel-top"
            | "window-left-column"
            | "window-top-line"
            | "window-old-pixel-width"
            | "window-old-pixel-height"
            | "window-old-body-pixel-width"
            | "window-old-body-pixel-height"
            | "window-new-pixel"
            | "window-new-total"
            | "window-new-normal"
            | "window-normal-size"
            | "set-window-new-pixel"
            | "set-window-new-total"
            | "set-window-new-normal"
            | "window-resize-apply"
            | "window-resize-apply-total"
            | "window-mode-line-height"
            | "window-header-line-height"
            | "window-tab-line-height"
            | "window-right-divider-width"
            | "window-bottom-divider-width"
            | "window-scroll-bar-width"
            | "window-scroll-bar-height"
            | "window-fringes"
            | "set-window-fringes"
            | "window-scroll-bars"
            | "set-window-scroll-bars"
            | "window-cursor-type"
            | "set-window-cursor-type"
            | "set-window-display-table"
            | "move-to-window-line"
            | "recenter"
            | "scroll-up"
            | "scroll-down"
            | "window-text-pixel-size"
            | "buffer-text-pixel-size"
            | "get-display-property"
            | "bidi-find-overridden-directionality"
            | "redisplay"
            | "redraw-frame"
            | "redraw-display"
            | "display--update-for-mouse-movement"
            | "open-termscript"
            | "frame-or-buffer-changed-p"
            | "internal-show-cursor"
            | "internal-show-cursor-p"
            | "set-buffer-redisplay"
            | "format-mode-line"
            | "font-spec"
            | "font-get"
            | "face-attribute"
            | "face-name"
            | "face-foreground"
            | "face-background"
            | "set-face-attribute"
            | "face-spec-set"
            | "color-distance"
            | "color-values"
            | "color-values-from-color-spec"
            | "make-frame-visible"
            | "make-frame-invisible"
            | "iconify-frame"
            | "frame-visible-p"
            | "visible-frame-list"
            | "selected-window"
            | "old-selected-window"
            | "frame-selected-window"
            | "frame-old-selected-window"
            | "set-frame-selected-window"
            | "select-window"
            | "current-window-configuration"
            | "set-window-configuration"
            | "window-configuration-p"
            | "window-configuration-equal-p"
            | "window-configuration-frame"
            | "force-window-update"
            | "run-window-configuration-change-hook"
            | "resize-mini-window-internal"
            | "window-buffer"
            | "window-old-buffer"
            | "window-old-point"
            | "set-window-buffer"
            | "window-list"
            | "window-list-1"
            | "next-window"
            | "previous-window"
            | "delete-other-windows"
            | "frame-first-window"
            | "frame-root-window"
            | "window-prev-buffers"
            | "set-window-prev-buffers"
            | "window-next-buffers"
            | "set-window-next-buffers"
            | "window-parameter"
            | "set-window-parameter"
            | "window-parameters"
            | "walk-windows"
            | "selected-frame"
            | "last-nonminibuffer-frame"
            | "window-frame"
            | "frame-live-p"
            | "framep"
            | "frame-terminal"
            | "frame-list"
            | "face-set-after-frame-default"
            | "windowp"
            | "window-live-p"
            | "window-valid-p"
            | "window-parent"
            | "window-prev-sibling"
            | "window-next-sibling"
            | "window-top-child"
            | "window-left-child"
            | "window-combination-limit"
            | "set-window-combination-limit"
            | "window-minibuffer-p"
            | "window-at"
            | "split-window"
            | "split-window-below"
            | "split-window-vertically"
            | "split-window-right"
            | "split-window-horizontally"
            | "split-window-internal"
            | "delete-window-internal"
            | "delete-other-windows-internal"
            | "run-window-scroll-functions"
            | "window-combined-p"
            | "window-dedicated-p"
            | "set-window-dedicated-p"
            | "window-splittable-p"
            | "window-edges"
            | "window-body-edges"
            | "window-inside-edges"
            | "window-pixel-edges"
            | "window-body-pixel-edges"
            | "window-inside-pixel-edges"
            | "posn-at-x-y"
            | "posn-at-point"
            | "window-display-table"
            | "terminal-live-p"
            | "terminal-list"
            | "terminal-name"
            | "delete-terminal"
            | "tty-type"
            | "tty-display-color-p"
            | "tty-display-color-cells"
            | "controlling-tty-p"
            | "tty-top-frame"
            | "tty-no-underline"
            | "suspend-tty"
            | "resume-tty"
            | "tty--set-output-buffer-size"
            | "tty--output-buffer-size"
            | "terminal-parameters"
            | "terminal-parameter"
            | "set-terminal-parameter"
            | "send-string-to-terminal"
            | "get-buffer-window"
            | "minibuffer-window"
            | "set-minibuffer-window"
            | "minibuffer-selected-window"
            | "minibuffer-window-active-p"
            | "get-mru-window"
            | "get-buffer-window-list"
            | "buffer-match-p"
            | "display-buffer"
            | "quit-window"
            | "active-minibuffer-window"
            | "set-window-start"
            | "set-window-point"
            | "set-window-vscroll"
            | "facemenu-add-face"
    )
}

fn default_frame_parameters(interp: &Interpreter) -> Vec<(String, Value)> {
    vec![
        ("tab-bar-lines".into(), Value::Integer(0)),
        ("menu-bar-lines".into(), Value::Integer(1)),
        ("modeline".into(), Value::T),
        ("width".into(), Value::Integer(interp.frame_width())),
        ("height".into(), Value::Integer(interp.frame_height())),
        ("name".into(), interp.frame_name_value()),
        ("font".into(), Value::String("tty".into())),
        (
            "background-color".into(),
            Value::String("unspecified-bg".into()),
        ),
        (
            "foreground-color".into(),
            Value::String("unspecified-fg".into()),
        ),
        ("cursor-color".into(), Value::String("white".into())),
        ("scroll-bar-background".into(), Value::Nil),
        ("scroll-bar-foreground".into(), Value::Nil),
        ("background-mode".into(), Value::Symbol("dark".into())),
        ("display-type".into(), Value::Symbol("mono".into())),
        ("minibuffer".into(), Value::T),
    ]
}

fn decode_live_frame(
    frame: Option<&Value>,
    nil_defaults_to_selected: bool,
) -> Result<Value, LispError> {
    match frame {
        None => Ok(Value::Symbol("frame".into())),
        Some(Value::Nil) if nil_defaults_to_selected => Ok(Value::Symbol("frame".into())),
        Some(Value::Symbol(frame)) if frame == "frame" => Ok(Value::Symbol(frame.clone())),
        Some(frame) => Err(LispError::TypeError(
            "frame-live-p".into(),
            frame.type_name(),
        )),
    }
}

fn frame_parameter_value(interp: &Interpreter, parameter: &str) -> Value {
    interp
        .frame_parameter_override(parameter)
        .or_else(|| {
            default_frame_parameters(interp)
                .into_iter()
                .find(|(name, _)| name == parameter)
                .map(|(_, value)| value)
        })
        .unwrap_or(Value::Nil)
}

fn store_frame_parameter(interp: &mut Interpreter, parameter: String, value: Value) {
    match (parameter.as_str(), &value) {
        ("width", Value::Integer(width)) => interp.set_frame_width(*width),
        ("height", Value::Integer(height)) => interp.set_frame_height(*height),
        _ => {}
    }
    interp.set_frame_parameter_override(parameter, value);
}

fn frame_parameters_value(interp: &Interpreter) -> Value {
    let overrides = interp.frame_parameter_overrides();
    let mut parameters = overrides.to_vec();
    parameters.extend(
        default_frame_parameters(interp)
            .into_iter()
            .filter(|(name, _)| !overrides.iter().any(|(overridden, _)| overridden == name)),
    );
    Value::list(
        parameters
            .into_iter()
            .map(|(name, value)| Value::cons(Value::Symbol(name), value)),
    )
}

fn require_live_terminal(interp: &Interpreter, value: Option<&Value>) -> Result<(), LispError> {
    if !interp.terminal_live() {
        return Err(wrong_type_argument(
            "terminal-live-p",
            value.cloned().unwrap_or(Value::Nil),
        ));
    }
    match value.unwrap_or(&Value::Nil) {
        Value::Nil => Ok(()),
        Value::Symbol(name) if name == "terminal" || name == "frame" => Ok(()),
        value => Err(wrong_type_argument("terminal-live-p", value.clone())),
    }
}

fn current_frame_and_buffer_state(interp: &Interpreter) -> Vec<Value> {
    let mut state = Vec::new();
    if interp.terminal_live() {
        state.push(Value::symbol("frame"));
        state.push(
            interp
                .frame_parameter_override("name")
                .unwrap_or_else(|| interp.frame_name_value()),
        );
    }
    for (buffer_id, name) in &interp.buffer_list {
        if name.starts_with(' ') {
            continue;
        }
        let Some(buffer) = interp.get_buffer_by_id(*buffer_id) else {
            continue;
        };
        state.push(Value::Buffer(*buffer_id, name.clone()));
        state.push(
            interp
                .buffer_local_value(*buffer_id, "buffer-read-only")
                .or_else(|| interp.default_value("buffer-read-only"))
                .unwrap_or(Value::Nil),
        );
        state.push(if buffer.is_modified() {
            Value::T
        } else {
            Value::Nil
        });
    }
    state
}

fn frame_or_buffer_changed(
    interp: &mut Interpreter,
    variable: Option<&Value>,
    env: &mut Env,
) -> Result<Value, LispError> {
    let variable_name = match variable {
        None | Some(Value::Nil) => None,
        Some(value) => Some(value.as_symbol()?.to_string()),
    };
    let old_state = if let Some(name) = variable_name.as_deref() {
        interp.lookup(name, env)?
    } else {
        interp.frame_and_buffer_state()
    };
    let current = current_frame_and_buffer_state(interp);
    let old_items = is_vector_value(&old_state)
        .then(|| vector_items(&old_state))
        .transpose()?;
    let unchanged = old_items.as_ref().is_some_and(|items| {
        items.len() > current.len()
            && current
                .iter()
                .zip(items)
                .all(|(new, old)| values_eq_in_env(interp, new, old, env))
            && matches!(
                items.get(current.len()),
                Some(Value::Symbol(symbol)) if symbol == "lambda"
            )
    });
    if unchanged {
        return Ok(Value::Nil);
    }

    let required = current.len() + 1;
    let reusable = old_items.as_ref().is_some_and(|items| {
        required <= items.len() && required.saturating_add(20) >= items.len() / 2
    });
    let state = if reusable {
        old_state
    } else {
        Value::list(
            std::iter::once(Value::symbol("vector-literal"))
                .chain(std::iter::repeat_n(Value::symbol("lambda"), required + 20)),
        )
    };
    for (index, value) in current.into_iter().enumerate() {
        aset_vector_value(&state, index, value)?;
    }
    let slots = vector_items(&state)?.len();
    for index in required - 1..slots {
        aset_vector_value(&state, index, Value::symbol("lambda"))?;
    }

    if !reusable {
        if let Some(name) = variable_name {
            interp.set_variable(&name, state, env);
        } else {
            interp.set_frame_and_buffer_state(state);
        }
    }
    Ok(Value::T)
}

fn window_id_or_selected(interp: &Interpreter, value: &Value) -> Result<u64, LispError> {
    if value.is_nil() {
        return Ok(interp.selected_window_id());
    }
    window_record_id_from_value(interp, value)
        .ok_or_else(|| LispError::TypeError("window".into(), value.type_name()))
}

fn window_parameter_value(interp: &Interpreter, window_id: u64, parameter: &Value) -> Value {
    let Some(params) = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
    else {
        return Value::Nil;
    };
    let Ok(items) = params.to_vec() else {
        return Value::Nil;
    };
    for item in items {
        if let Ok(key) = item.car()
            && values_equal(interp, &key, parameter)
            && let Ok(value) = item.cdr()
        {
            return value;
        }
    }
    Value::Nil
}

fn set_window_parameter_value(
    interp: &mut Interpreter,
    window_id: u64,
    parameter: Value,
    value: Value,
) -> Result<Value, LispError> {
    let existing = interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
        .cloned()
        .unwrap_or(Value::Nil);
    let mut items: Vec<Value> = existing.to_vec().unwrap_or_default();
    items.retain(|item| match item.car() {
        Ok(key) => !values_equal(interp, &key, &parameter),
        Err(_) => true,
    });
    if value.is_truthy() {
        items.push(Value::cons(parameter, value.clone()));
    }
    let Some(record) = interp.find_record_mut(window_id) else {
        return Err(LispError::TypeError("window".into(), "deleted".into()));
    };
    if record.slots.len() <= WINDOW_PARAMETERS_SLOT {
        record.slots.resize(WINDOW_PARAMETERS_SLOT + 1, Value::Nil);
    }
    record.slots[WINDOW_PARAMETERS_SLOT] = Value::list(items);
    Ok(value)
}

fn window_slot_value(interp: &Interpreter, window_id: u64, slot: usize) -> Value {
    interp
        .find_record(window_id)
        .and_then(|record| record.slots.get(slot))
        .cloned()
        .unwrap_or(Value::Nil)
}

fn set_window_slot_value(
    interp: &mut Interpreter,
    window_id: u64,
    slot: usize,
    value: Value,
) -> Result<Value, LispError> {
    let Some(record) = interp.find_record_mut(window_id) else {
        return Err(LispError::TypeError("window".into(), "deleted".into()));
    };
    if record.slots.len() <= slot {
        record.slots.resize(slot + 1, Value::Nil);
    }
    record.slots[slot] = value.clone();
    Ok(value)
}

fn window_link(interp: &Interpreter, window_id: u64, slot: usize) -> Option<u64> {
    match window_slot_value(interp, window_id, slot) {
        Value::Record(id) => Some(id),
        _ => None,
    }
}

fn window_geometry(interp: &Interpreter, window_id: u64) -> (i64, i64, i64, i64) {
    let integer_slot = |slot, fallback| {
        window_slot_value(interp, window_id, slot)
            .as_integer()
            .unwrap_or(fallback)
    };
    (
        integer_slot(WINDOW_PIXEL_WIDTH_SLOT, interp.frame_width()),
        integer_slot(WINDOW_PIXEL_HEIGHT_SLOT, interp.frame_height()),
        integer_slot(WINDOW_PIXEL_LEFT_SLOT, 0),
        integer_slot(WINDOW_PIXEL_TOP_SLOT, 0),
    )
}

fn set_window_geometry(
    interp: &mut Interpreter,
    window_id: u64,
    geometry: (i64, i64, i64, i64),
) -> Result<(), LispError> {
    for (slot, value) in [
        (WINDOW_PIXEL_WIDTH_SLOT, geometry.0),
        (WINDOW_PIXEL_HEIGHT_SLOT, geometry.1),
        (WINDOW_PIXEL_LEFT_SLOT, geometry.2),
        (WINDOW_PIXEL_TOP_SLOT, geometry.3),
    ] {
        set_window_slot_value(interp, window_id, slot, Value::Integer(value))?;
    }
    Ok(())
}

fn frame_root_window_value(interp: &Interpreter) -> Value {
    interp
        .global_binding_value("emaxx-root-window")
        .unwrap_or_else(|| interp.selected_window_value())
}

fn live_ordinary_window_ids(interp: &Interpreter) -> Vec<u64> {
    interp
        .record_ids_by_type("window")
        .into_iter()
        .filter(|id| {
            let kind = window_slot_value(interp, *id, WINDOW_KIND_SLOT);
            !matches!(
                kind,
                Value::Symbol(ref kind)
                    if matches!(
                        kind.as_str(),
                        MINIBUFFER_WINDOW_KIND
                            | INTERNAL_HORIZONTAL_WINDOW_KIND
                            | INTERNAL_VERTICAL_WINDOW_KIND
                            | DELETED_WINDOW_KIND
                    )
            ) && window_buffer_id(interp, &Value::Record(*id)).is_some()
        })
        .collect()
}

fn live_window_id_or_selected(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<u64, LispError> {
    let window = value
        .filter(|window| !window.is_nil())
        .cloned()
        .unwrap_or_else(|| interp.selected_window_value());
    let window_id = window_record_id_from_value(interp, &window)
        .ok_or_else(|| LispError::TypeError("window-live-p".into(), window.type_name()))?;
    let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
    if matches!(
        kind,
        Value::Symbol(ref kind)
            if matches!(
                kind.as_str(),
                INTERNAL_HORIZONTAL_WINDOW_KIND
                    | INTERNAL_VERTICAL_WINDOW_KIND
                    | DELETED_WINDOW_KIND
            )
    ) || window_buffer_id(interp, &window).is_none()
    {
        return Err(LispError::TypeError(
            "window-live-p".into(),
            window.type_name(),
        ));
    }
    Ok(window_id)
}

fn select_window_value(
    interp: &mut Interpreter,
    window: &Value,
    norecord: bool,
) -> Result<Value, LispError> {
    let window_id = live_window_id_or_selected(interp, Some(window))?;
    interp.set_selected_window_id(window_id);
    if !norecord {
        interp.record_window_selection(window_id);
    }
    if let Some(buffer_id) = window_buffer_id(interp, window)
        && interp.has_buffer_id(buffer_id)
    {
        interp.switch_to_buffer_id_preserving_window_history(buffer_id)?;
        if !norecord {
            interp.record_buffer_front(buffer_id);
        }
    }
    Ok(window.clone())
}

fn split_window_tree(
    interp: &mut Interpreter,
    old: &Value,
    pixel_size: &Value,
    side: &Value,
    normal_size: &Value,
) -> Result<Value, LispError> {
    let old_id = window_id_or_selected(interp, old)?;
    let kind = window_slot_value(interp, old_id, WINDOW_KIND_SLOT);
    if matches!(kind, Value::Symbol(ref kind) if kind == MINIBUFFER_WINDOW_KIND)
        || window_buffer_id(interp, &Value::Record(old_id)).is_none()
    {
        return Err(LispError::Signal(
            "Attempt to split a non-live window".into(),
        ));
    }
    let horizontal = matches!(side, Value::T)
        || matches!(side, Value::Symbol(side) if matches!(side.as_str(), "left" | "right"));
    let before = matches!(side, Value::Symbol(side) if matches!(side.as_str(), "above" | "left"));
    let requested = pixel_size.as_integer()?;
    let (width, height, left, top) = window_geometry(interp, old_id);
    let available = if horizontal { width } else { height };
    if requested <= 0 || requested >= available {
        return Err(LispError::Signal(
            "Size of new window too small (after split)".into(),
        ));
    }
    let old_parent = window_link(interp, old_id, WINDOW_PARENT_SLOT);
    let outside_prev = window_link(interp, old_id, WINDOW_PREV_SIBLING_SLOT);
    let outside_next = window_link(interp, old_id, WINDOW_NEXT_SIBLING_SLOT);
    let buffer_id = window_buffer_id(interp, &Value::Record(old_id))
        .ok_or_else(|| LispError::Signal("Attempt to split a non-live window".into()))?;
    let start = window_slot_value(interp, old_id, WINDOW_START_SLOT)
        .as_integer()
        .unwrap_or(1)
        .max(1) as usize;

    let (old_geometry, new_geometry) = if horizontal {
        let old_width = available - requested;
        if before {
            (
                (old_width, height, left + requested, top),
                (requested, height, left, top),
            )
        } else {
            (
                (old_width, height, left, top),
                (requested, height, left + old_width, top),
            )
        }
    } else {
        let old_height = available - requested;
        if before {
            (
                (width, old_height, left, top + requested),
                (width, requested, left, top),
            )
        } else {
            (
                (width, old_height, left, top),
                (width, requested, left, top + old_height),
            )
        }
    };

    let parent_kind = if horizontal {
        INTERNAL_HORIZONTAL_WINDOW_KIND
    } else {
        INTERNAL_VERTICAL_WINDOW_KIND
    };
    let parent = interp.create_record(
        "window",
        window_record_slots(
            None,
            1,
            Value::Symbol(parent_kind.into()),
            (width, height, left, top),
        ),
    );
    let Value::Record(parent_id) = parent else {
        unreachable!("window records use Value::Record");
    };
    let new = interp.create_record(
        "window",
        window_record_slots(Some(buffer_id), start, Value::Nil, new_geometry),
    );
    let Value::Record(new_id) = new else {
        unreachable!("window records use Value::Record");
    };

    set_window_slot_value(
        interp,
        parent_id,
        WINDOW_PARENT_SLOT,
        old_parent.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_slot_value(
        interp,
        parent_id,
        WINDOW_PREV_SIBLING_SLOT,
        outside_prev.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_slot_value(
        interp,
        parent_id,
        WINDOW_NEXT_SIBLING_SLOT,
        outside_next.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    if let Some(parent) = old_parent
        && window_link(interp, parent, WINDOW_FIRST_CHILD_SLOT) == Some(old_id)
    {
        set_window_slot_value(
            interp,
            parent,
            WINDOW_FIRST_CHILD_SLOT,
            Value::Record(parent_id),
        )?;
    }
    if let Some(previous) = outside_prev {
        set_window_slot_value(
            interp,
            previous,
            WINDOW_NEXT_SIBLING_SLOT,
            Value::Record(parent_id),
        )?;
    }
    if let Some(next) = outside_next {
        set_window_slot_value(
            interp,
            next,
            WINDOW_PREV_SIBLING_SLOT,
            Value::Record(parent_id),
        )?;
    }
    for window_id in [old_id, new_id] {
        set_window_slot_value(
            interp,
            window_id,
            WINDOW_PARENT_SLOT,
            Value::Record(parent_id),
        )?;
    }
    let (first, second) = if before {
        (new_id, old_id)
    } else {
        (old_id, new_id)
    };
    set_window_slot_value(
        interp,
        parent_id,
        WINDOW_FIRST_CHILD_SLOT,
        Value::Record(first),
    )?;
    set_window_slot_value(interp, first, WINDOW_PREV_SIBLING_SLOT, Value::Nil)?;
    set_window_slot_value(
        interp,
        first,
        WINDOW_NEXT_SIBLING_SLOT,
        Value::Record(second),
    )?;
    set_window_slot_value(
        interp,
        second,
        WINDOW_PREV_SIBLING_SLOT,
        Value::Record(first),
    )?;
    set_window_slot_value(interp, second, WINDOW_NEXT_SIBLING_SLOT, Value::Nil)?;
    set_window_slot_value(interp, new_id, WINDOW_NEW_NORMAL_SLOT, normal_size.clone())?;
    for (window_id, pixel_size) in [
        (parent_id, available),
        (old_id, available - requested),
        (new_id, requested),
    ] {
        set_window_slot_value(
            interp,
            window_id,
            WINDOW_NEW_PIXEL_SLOT,
            Value::Integer(pixel_size),
        )?;
        set_window_slot_value(
            interp,
            window_id,
            WINDOW_NEW_TOTAL_SLOT,
            Value::Integer(pixel_size),
        )?;
    }
    set_window_geometry(interp, old_id, old_geometry)?;
    if old_parent.is_none() {
        interp.set_global_binding("emaxx-root-window", Value::Record(parent_id));
    }
    Ok(Value::Record(new_id))
}

fn delete_window_from_tree(interp: &mut Interpreter, window_id: u64) -> Result<(), LispError> {
    let parent_id = window_link(interp, window_id, WINDOW_PARENT_SLOT)
        .ok_or_else(|| LispError::Signal("Attempt to delete sole ordinary window".into()))?;
    let sibling_id = window_link(interp, window_id, WINDOW_PREV_SIBLING_SLOT)
        .or_else(|| window_link(interp, window_id, WINDOW_NEXT_SIBLING_SLOT))
        .ok_or_else(|| LispError::Signal("Attempt to delete sole window of parent".into()))?;
    let grandparent = window_link(interp, parent_id, WINDOW_PARENT_SLOT);
    let outside_prev = window_link(interp, parent_id, WINDOW_PREV_SIBLING_SLOT);
    let outside_next = window_link(interp, parent_id, WINDOW_NEXT_SIBLING_SLOT);
    let parent_geometry = window_geometry(interp, parent_id);

    if let Some(grandparent_id) = grandparent
        && window_link(interp, grandparent_id, WINDOW_FIRST_CHILD_SLOT) == Some(parent_id)
    {
        set_window_slot_value(
            interp,
            grandparent_id,
            WINDOW_FIRST_CHILD_SLOT,
            Value::Record(sibling_id),
        )?;
    }
    if let Some(previous) = outside_prev {
        set_window_slot_value(
            interp,
            previous,
            WINDOW_NEXT_SIBLING_SLOT,
            Value::Record(sibling_id),
        )?;
    }
    if let Some(next) = outside_next {
        set_window_slot_value(
            interp,
            next,
            WINDOW_PREV_SIBLING_SLOT,
            Value::Record(sibling_id),
        )?;
    }
    set_window_slot_value(
        interp,
        sibling_id,
        WINDOW_PARENT_SLOT,
        grandparent.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_slot_value(
        interp,
        sibling_id,
        WINDOW_PREV_SIBLING_SLOT,
        outside_prev.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_slot_value(
        interp,
        sibling_id,
        WINDOW_NEXT_SIBLING_SLOT,
        outside_next.map(Value::Record).unwrap_or(Value::Nil),
    )?;
    set_window_geometry(interp, sibling_id, parent_geometry)?;
    for deleted_id in [window_id, parent_id] {
        set_window_slot_value(
            interp,
            deleted_id,
            WINDOW_KIND_SLOT,
            Value::Symbol(DELETED_WINDOW_KIND.into()),
        )?;
        set_window_slot_value(interp, deleted_id, WINDOW_BUFFER_SLOT, Value::Nil)?;
        set_window_slot_value(interp, deleted_id, WINDOW_PARENT_SLOT, Value::Nil)?;
        set_window_slot_value(interp, deleted_id, WINDOW_PREV_SIBLING_SLOT, Value::Nil)?;
        set_window_slot_value(interp, deleted_id, WINDOW_NEXT_SIBLING_SLOT, Value::Nil)?;
        set_window_slot_value(interp, deleted_id, WINDOW_FIRST_CHILD_SLOT, Value::Nil)?;
    }
    if grandparent.is_none() {
        interp.set_global_binding("emaxx-root-window", Value::Record(sibling_id));
    }
    if interp.selected_window_id() == window_id {
        interp.set_selected_window_id(sibling_id);
        if let Some(buffer_id) = window_buffer_id(interp, &Value::Record(sibling_id)) {
            interp.switch_to_buffer_id_preserving_window_history(buffer_id)?;
        }
    }
    Ok(())
}

fn delete_other_windows_from_tree(
    interp: &mut Interpreter,
    window_id: u64,
) -> Result<(), LispError> {
    let root = frame_root_window_value(interp);
    let root_id = window_id_or_selected(interp, &root)?;
    let root_geometry = window_geometry(interp, root_id);
    for id in interp.record_ids_by_type("window") {
        if id == window_id {
            continue;
        }
        let kind = window_slot_value(interp, id, WINDOW_KIND_SLOT);
        if !matches!(kind, Value::Symbol(ref kind) if kind == MINIBUFFER_WINDOW_KIND) {
            set_window_slot_value(
                interp,
                id,
                WINDOW_KIND_SLOT,
                Value::Symbol(DELETED_WINDOW_KIND.into()),
            )?;
            set_window_slot_value(interp, id, WINDOW_BUFFER_SLOT, Value::Nil)?;
        }
    }
    set_window_slot_value(interp, window_id, WINDOW_PARENT_SLOT, Value::Nil)?;
    set_window_slot_value(interp, window_id, WINDOW_PREV_SIBLING_SLOT, Value::Nil)?;
    set_window_slot_value(interp, window_id, WINDOW_NEXT_SIBLING_SLOT, Value::Nil)?;
    set_window_geometry(interp, window_id, root_geometry)?;
    interp.set_global_binding("emaxx-root-window", Value::Record(window_id));
    Ok(())
}

fn window_buffer_id_or_selected(
    interp: &Interpreter,
    window: Option<&Value>,
) -> Result<u64, LispError> {
    match window {
        None | Some(Value::Nil) => Ok(interp.selected_window_buffer_id()),
        Some(window) => {
            window_id_or_selected(interp, window)?;
            window_buffer_id(interp, window)
                .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))
        }
    }
}

fn window_line_height(
    interp: &Interpreter,
    buffer_id: u64,
    format_variable: &str,
    env: &Env,
) -> i64 {
    let format = if buffer_id == interp.current_buffer_id() {
        interp.lookup_var(format_variable, env)
    } else {
        interp
            .buffer_local_toplevel_value(buffer_id, format_variable)
            .or_else(|| interp.global_binding_value(format_variable))
    };
    i64::from(format.is_some_and(|format| !format.is_nil()))
}

fn window_non_body_height(interp: &Interpreter, buffer_id: u64, env: &Env) -> i64 {
    ["mode-line-format", "header-line-format", "tab-line-format"]
        .into_iter()
        .map(|variable| window_line_height(interp, buffer_id, variable, env))
        .sum()
}

fn window_text_width_columns(interp: &Interpreter, window_id: u64) -> i64 {
    let total = window_geometry(interp, window_id).0;
    let (left, right) = interp.window_margins(window_id);
    (total - left.unwrap_or(0) - right.unwrap_or(0)).max(0)
}

fn set_window_hscroll_value(
    interp: &mut Interpreter,
    window_id: u64,
    requested: i64,
) -> Result<Value, LispError> {
    let hscroll = requested.max(0);
    set_window_slot_value(
        interp,
        window_id,
        WINDOW_HSCROLL_SLOT,
        Value::Integer(hscroll),
    )?;
    set_window_slot_value(
        interp,
        window_id,
        WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
        Value::T,
    )?;
    Ok(Value::Integer(hscroll))
}

fn valid_window_cursor_type(value: &Value) -> bool {
    match value {
        Value::Nil | Value::T => true,
        Value::Symbol(symbol) => matches!(symbol.as_str(), "box" | "hollow" | "bar" | "hbar"),
        Value::Cons(_, _) => {
            let Ok(kind) = value
                .car()
                .and_then(|kind| kind.as_symbol().map(str::to_owned))
            else {
                return false;
            };
            matches!(kind.as_str(), "box" | "bar" | "hbar")
                && value.cdr().is_ok_and(|width| width.as_integer().is_ok())
        }
        _ => false,
    }
}

fn window_list_value(interp: &Interpreter, env: &Env, minibuf: Option<&Value>) -> Value {
    let mut windows = live_ordinary_window_ids(interp)
        .into_iter()
        .map(Value::Record)
        .collect::<Vec<_>>();
    let include_minibuffer = matches!(minibuf, Some(Value::T));
    if !include_minibuffer {
        return Value::list(windows);
    }
    let minibuffer = interp
        .lookup_var("emaxx-minibuffer-window", env)
        .unwrap_or_else(|| interp.selected_window_value());
    if !windows
        .iter()
        .any(|window| values_equal(interp, window, &minibuffer))
    {
        windows.push(minibuffer);
    }
    Value::list(windows)
}

fn display_action_parts(
    interp: &Interpreter,
    action: &Value,
    env: &Env,
) -> (Vec<Value>, Vec<Value>) {
    if action.is_nil() {
        return (Vec::new(), Vec::new());
    }
    if callable_display_action_function(interp, action, env).is_some() {
        return (vec![action.clone()], Vec::new());
    }
    let Ok(functions) = action.car() else {
        return (Vec::new(), Vec::new());
    };
    let functions = if callable_display_action_function(interp, &functions, env).is_some() {
        vec![functions]
    } else {
        functions
            .to_vec()
            .unwrap_or_default()
            .into_iter()
            .filter(|function| callable_display_action_function(interp, function, env).is_some())
            .collect()
    };
    if functions.is_empty()
        && let Ok(entries) = action.to_vec()
        && entries.first().is_some_and(|entry| {
            entry
                .car()
                .is_ok_and(|key| callable_display_action_function(interp, &key, env).is_none())
        })
    {
        // A bare action alist such as `((inhibit-same-window . t))' has no
        // function head.  Its first entry is part of the alist, not a failed
        // attempt at naming an action function.
        return (Vec::new(), entries);
    }
    let alist = action
        .cdr()
        .ok()
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    (functions, alist)
}

fn display_alist_value(interp: &Interpreter, alist: &[Value], key: &str) -> Value {
    alist
        .iter()
        .find_map(|entry| {
            let entry_key = entry.car().ok()?;
            if values_equal(interp, &entry_key, &Value::Symbol(key.into())) {
                entry.cdr().ok()
            } else {
                None
            }
        })
        .unwrap_or(Value::Nil)
}

fn buffer_match_condition(
    interp: &mut Interpreter,
    condition: &Value,
    buffer: &Value,
    extra_args: &[Value],
    env: &mut Env,
) -> Result<bool, LispError> {
    if condition.is_nil() {
        return Ok(false);
    }
    if matches!(condition, Value::T) {
        return Ok(true);
    }
    if string_like(condition).is_some() {
        let buffer_id = interp.resolve_buffer_id(buffer)?;
        let buffer_name = interp
            .get_buffer_by_id(buffer_id)
            .map(|buffer| buffer.name.clone())
            .unwrap_or_default();
        return Ok(super::call(
            interp,
            "string-match-p",
            &[condition.clone(), Value::String(buffer_name)],
            env,
        )?
        .is_truthy());
    }
    if let Some((operator, operands)) = condition.cons_values()
        && let Ok(operator) = operator.as_symbol()
    {
        let conditions = || operands.to_vec().unwrap_or_default();
        match operator {
            "major-mode" | "derived-mode" => {
                let buffer_id = interp.resolve_buffer_id(buffer)?;
                let mode = interp
                    .buffer_local_value(buffer_id, "major-mode")
                    .or_else(|| interp.lookup_var("major-mode", env))
                    .unwrap_or(Value::Nil);
                return if operator == "major-mode" {
                    Ok(values_equal(interp, &mode, &operands))
                } else {
                    Ok(
                        super::call(interp, "provided-mode-derived-p", &[mode, operands], env)?
                            .is_truthy(),
                    )
                };
            }
            "category" => {
                let action_alist = extra_args
                    .first()
                    .map(|action| display_action_parts(interp, action, env).1)
                    .unwrap_or_default();
                return Ok(values_equal(
                    interp,
                    &display_alist_value(interp, &action_alist, "category"),
                    &operands,
                ));
            }
            "not" => {
                for nested in conditions() {
                    if buffer_match_condition(interp, &nested, buffer, extra_args, env)? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            "or" => {
                for nested in conditions() {
                    if buffer_match_condition(interp, &nested, buffer, extra_args, env)? {
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            "and" => {
                for nested in conditions() {
                    if !buffer_match_condition(interp, &nested, buffer, extra_args, env)? {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            _ => {}
        }
    }
    let Some(function) = callable_display_action_function(interp, condition, env) else {
        return Ok(false);
    };
    let mut args = Vec::with_capacity(extra_args.len() + 1);
    args.push(buffer.clone());
    args.extend_from_slice(extra_args);
    Ok(interp
        .call_function_value(function, condition.as_symbol().ok(), &args, env)?
        .is_truthy())
}

fn matching_display_buffer_action(
    interp: &mut Interpreter,
    buffer: &Value,
    action: &Value,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let entries = interp
        .lookup_var("display-buffer-alist", env)
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    for entry in entries {
        let Ok(condition) = entry.car() else {
            continue;
        };
        if buffer_match_condition(
            interp,
            &condition,
            buffer,
            std::slice::from_ref(action),
            env,
        )? {
            return Ok(entry.cdr().ok());
        }
    }
    Ok(None)
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    match name {
        "format-mode-line" => {
            need_arg_range(name, args, 1, 4)?;
            // GNU's C primitive intentionally makes formatting a no-op in
            // noninteractive mode, before it inspects FORMAT or frame state.
            // Emaxx currently has only this batch/noninteractive host mode.
            Ok(Value::String(String::new()))
        }
        // ── Output ──
        "substitute-command-keys" => {
            need_arg_range(name, args, 1, 3)?;
            Ok(Value::String(substitute_command_keys(
                interp,
                &string_text(&args[0])?,
                env,
            )))
        }
        "message" => {
            let text = if args.is_empty() || args.first().is_some_and(Value::is_nil) {
                String::new()
            } else {
                string_text(&super::call(interp, "format", args, env)?)?
            };
            let buffer_name = interp
                .lookup_var("messages-buffer-name", env)
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_else(|| "*Messages*".into());
            // GNU message_dolog: nothing is logged for an empty message or
            // with `message-log-max' nil; a fixnum keeps that many lines.
            let log_max = interp
                .lookup_var("message-log-max", env)
                .unwrap_or(Value::T);
            if !text.is_empty() && !log_max.is_nil() {
                let buffer_id = interp
                    .find_buffer(&buffer_name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(&buffer_name).0);
                if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                    let end = buffer.point_max();
                    buffer.goto_char(end);
                    buffer.insert(&(text.clone() + "\n"));
                    if let Ok(max_lines) = log_max.as_integer()
                        && max_lines >= 0
                    {
                        let contents = buffer.full_buffer_string();
                        let lines = contents.matches('\n').count();
                        if lines > max_lines as usize {
                            let drop = lines - max_lines as usize;
                            let mut offset = 0usize;
                            for _ in 0..drop {
                                if let Some(next) = contents[offset..].find('\n') {
                                    offset += next + 1;
                                }
                            }
                            let char_end = contents[..offset].chars().count();
                            let _ = buffer.delete_region(1, char_end + 1);
                        }
                    }
                }
            }
            // The upstream capture advice ignores `(message nil)' and
            // `(message "")', which edebug uses to clear the echo area.
            let capturable = !args.is_empty()
                && !args.first().is_some_and(Value::is_nil)
                && args
                    .first()
                    .and_then(string_like)
                    .is_none_or(|string| !string.text.is_empty());
            if capturable {
                interp.append_message_capture(&text, true, env);
            }
            if args.first().is_some_and(Value::is_nil) {
                Ok(Value::Nil)
            } else {
                Ok(Value::String(text))
            }
        }
        "message-box" | "message-or-box" => {
            if args.is_empty() {
                return Err(LispError::WrongNumberOfArgs(name.into(), 0));
            }
            // Emaxx currently has no graphical frame, so GNU's documented
            // dialog fallback is the ordinary echo-area message path.
            super::call(interp, "message", args, env)
        }
        "warn" => {
            let text = if args.is_empty() {
                String::new()
            } else {
                string_text(&super::call(interp, "format", args, env)?)?
            };
            let warning = if text.is_empty() {
                "Warning".to_string()
            } else {
                format!("Warning: {text}")
            };
            let _ = super::call(interp, "message", &[Value::String(warning.clone())], env)?;
            append_to_warnings_buffer(interp, &warning);
            Ok(Value::Nil)
        }
        "display-warning" => {
            need_arg_range(name, args, 2, 4)?;
            let warning_type = args[0].to_string();
            let message = string_text(&args[1])?;
            let warning = if warning_type == "nil" {
                format!("Warning: {message}")
            } else {
                format!("Warning ({warning_type}): {message}")
            };
            let _ = super::call(interp, "message", &[Value::String(warning.clone())], env)?;
            let buffer_name = args
                .get(3)
                .and_then(string_like)
                .map(|string| string.text)
                .unwrap_or_else(|| "*Warnings*".into());
            let warning = if let Some(prefix_function) =
                interp.lookup_var("warning-prefix-function", env)
                && prefix_function.is_truthy()
            {
                let prefix = interp.call_function_value(
                    prefix_function,
                    None,
                    &[
                        args[2].clone(),
                        Value::list([args[0].clone(), args[1].clone()]),
                    ],
                    env,
                )?;
                string_like(&prefix)
                    .map(|prefix| format!("{}{}", prefix.text, warning))
                    .unwrap_or(warning)
            } else {
                warning
            };
            append_to_named_warnings_buffer(interp, &buffer_name, &warning);
            Ok(Value::Nil)
        }
        "current-message" => {
            need_args(name, args, 0)?;
            // GNU batch mode has no echo area; `current-message' is nil.
            if interp
                .lookup_var("noninteractive", env)
                .is_some_and(|value| value.is_truthy())
            {
                return Ok(Value::Nil);
            }
            let buffer_name = interp
                .lookup_var("messages-buffer-name", env)
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_else(|| "*Messages*".into());
            let Some((buffer_id, _)) = interp.find_buffer(&buffer_name) else {
                return Ok(Value::Nil);
            };
            let Some(buffer) = interp.get_buffer_by_id(buffer_id) else {
                return Ok(Value::Nil);
            };
            Ok(buffer
                .buffer_string()
                .lines()
                .next_back()
                .map(|line| Value::String(line.to_string()))
                .unwrap_or(Value::Nil))
        }
        "error-message-string" => {
            need_args(name, args, 1)?;
            if let Err(LispError::SignalValue(signal)) = args[0].to_vec()
                && circular_list_signal_p(&signal)
            {
                return Err(LispError::SignalValue(signal));
            }
            let items = args[0].to_vec().ok();
            if let Some(items) = items
                && let Some(message) = items.get(1).and_then(string_like)
            {
                Ok(Value::String(message.text))
            } else {
                Ok(Value::String(args[0].to_string()))
            }
        }
        "command-error-default-function" => {
            need_args(name, args, 3)?;
            let _context = string_text(&args[1])?;
            // GNU prints the unhandled error (and exits in noninteractive
            // mode).  Emaxx embeds the evaluator in its batch runner, whose
            // outer error boundary owns process termination; this primitive
            // must remain callable so dumped help.el can wrap it without
            // turning the original error into a void-function failure.
            let _ = super::call(interp, "error-message-string", &[args[0].clone()], env)?;
            Ok(Value::Nil)
        }
        "ding" => Ok(Value::Nil),
        "make-progress-reporter" => {
            need_arg_range(name, args, 1, 6)?;
            Ok(Value::list([
                Value::Symbol("progress-reporter".into()),
                args[0].clone(),
            ]))
        }
        "progress-reporter-update" | "progress-reporter-done" => {
            need_arg_range(name, args, 1, 3)?;
            Ok(Value::Nil)
        }
        "vc-refresh-state" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "sleep-for" => {
            need_arg_range(name, args, 1, 2)?;
            // GNU processes subprocess output whenever it waits; epg relies
            // on the trailing (sleep-for 0.1) in epg-wait-for-completion to
            // flush gpg's final status lines through the process filter.
            wait_pumping_processes(interp, env, Some(wait_duration(args)?), false, None)?;
            Ok(Value::Nil)
        }
        "sit-for" => {
            need_arg_range(name, args, 0, 3)?;
            // GNU: (sit-for SECONDS &optional NODISP), with the obsolete
            // (sit-for SECONDS MILLISEC NODISP) form still accepted when
            // MILLISEC is a number — a non-numeric second arg is NODISP.
            let duration_args = match args.get(1) {
                Some(Value::Integer(_) | Value::Float(_)) => args.get(0..2).unwrap_or(args),
                _ => args.get(0..1).unwrap_or(args),
            };
            wait_pumping_processes(
                interp,
                env,
                Some(wait_duration(duration_args)?),
                false,
                None,
            )?;
            Ok(Value::T)
        }
        "accept-process-output" => {
            need_arg_range(name, args, 0, 4)?;
            // GNU: (accept-process-output &optional PROCESS SECONDS MILLISEC
            // JUST-THIS-ONE) - the wait always comes from args 2 and 3.  GNU
            // may service unrelated processes during the wait, but when
            // PROCESS is non-nil their output does not satisfy this call.
            let duration_args = if args.len() > 1 {
                &args[1..args.len().min(3)]
            } else {
                &[]
            };
            let target_process_id = args
                .first()
                .filter(|process| !process.is_nil())
                .map(|process| interp.resolve_process_id(process))
                .transpose()?;
            if let Some(process_id) = target_process_id {
                interp.ensure_process_owned_by_current_thread(process_id)?;
            }
            let timeout = if !duration_args.is_empty() {
                Some(wait_duration(duration_args)?)
            } else if target_process_id.is_none() {
                // With neither PROCESS nor a timeout GNU performs one event
                // pump (including due timers) and returns.  Only a specified
                // process makes the timeout-less form wait indefinitely.
                Some(std::time::Duration::ZERO)
            } else {
                None
            };
            let delivered = wait_pumping_processes(interp, env, timeout, true, target_process_id)?;
            Ok(if delivered { Value::T } else { Value::Nil })
        }
        "input-pending-p" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(if unread_command_events(interp, env)?.is_empty() {
                Value::Nil
            } else {
                Value::T
            })
        }
        "discard-input" => {
            need_args(name, args, 0)?;
            if interp
                .lookup_var("defining-kbd-macro", env)
                .is_some_and(|value| value.is_truthy())
            {
                interp
                    .kbd_macro_definition
                    .truncate(interp.kbd_macro_committed_len);
                let last_macro = Value::list(
                    std::iter::once(Value::symbol("vector-literal"))
                        .chain(interp.kbd_macro_definition.iter().cloned()),
                );
                interp.set_variable("last-kbd-macro", last_macro, env);
            }
            interp.set_variable("unread-command-events", Value::Nil, env);
            interp.set_variable("defining-kbd-macro", Value::Nil, env);
            Ok(Value::Nil)
        }
        "prin1" => {
            need_arg_range(name, args, 1, 3)?;
            let rendered = if matches!(args.get(2), None | Some(Value::Nil)) {
                render_prin1(interp, &args[0], env)?
            } else {
                let mut print_env = printer_env_with_overrides(env, args.get(2))?;
                let rendered = render_prin1(interp, &args[0], &mut print_env)?;
                sync_print_number_table(env, args.get(2), &print_env);
                let stream = printer_stream_value(interp, &print_env, args.get(1));
                write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                return Ok(args[0].clone());
            };
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "cl-prin1" => {
            need_arg_range(name, args, 1, 3)?;
            let rendered = if matches!(args.get(2), None | Some(Value::Nil)) {
                render_cl_prin1(interp, &args[0], env)?
            } else {
                let mut print_env = printer_env_with_overrides(env, args.get(2))?;
                let rendered = render_cl_prin1(interp, &args[0], &mut print_env)?;
                sync_print_number_table(env, args.get(2), &print_env);
                let stream = printer_stream_value(interp, &print_env, args.get(1));
                write_printer_output(interp, &rendered, stream.as_ref(), env)?;
                return Ok(args[0].clone());
            };
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "princ" => {
            if args.is_empty() {
                return Ok(Value::Nil);
            }
            let rendered = render_princ(&args[0]);
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "print" => {
            if args.is_empty() {
                return Ok(Value::Nil);
            }
            let rendered = format!("\n{}\n", render_prin1(interp, &args[0], env)?);
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "terpri" => {
            need_arg_range(name, args, 0, 2)?;
            let stream = printer_stream_value(interp, env, args.first());
            if args.get(1).is_some_and(Value::is_truthy)
                && printer_stream_at_line_start(interp, stream.as_ref())?
            {
                return Ok(Value::Nil);
            }
            write_printer_output(interp, "\n", stream.as_ref(), env)?;
            Ok(Value::T)
        }
        "prin1-to-string" => {
            need_arg_range(name, args, 1, 3)?;
            // NOESCAPE non-nil prints like `princ' (no quoting).
            if args.get(1).is_some_and(|value| value.is_truthy()) {
                return Ok(Value::String(render_princ(&args[0])));
            }
            if matches!(args.get(2), None | Some(Value::Nil)) {
                return Ok(Value::String(render_prin1(interp, &args[0], env)?));
            }
            let mut print_env = printer_env_with_overrides(env, args.get(2))?;
            let rendered = render_prin1(interp, &args[0], &mut print_env)?;
            sync_print_number_table(env, args.get(2), &print_env);
            Ok(Value::String(rendered))
        }
        "cl-prin1-to-string" => {
            need_arg_range(name, args, 1, 3)?;
            if matches!(args.get(2), None | Some(Value::Nil)) {
                return render_cl_prin1_value(interp, &args[0], env);
            }
            let mut print_env = printer_env_with_overrides(env, args.get(2))?;
            let rendered = render_cl_prin1_value(interp, &args[0], &mut print_env)?;
            sync_print_number_table(env, args.get(2), &print_env);
            Ok(rendered)
        }
        "cl-print--expand-ellipsis" => {
            need_args(name, args, 2)?;
            let parts = args[0].to_vec()?;
            let [Value::Symbol(tag), expansion] = parts.as_slice() else {
                return Err(LispError::TypeError(
                    "cl-print-ellipsis".into(),
                    args[0].type_name(),
                ));
            };
            if tag != "emaxx-cl-print-ellipsis" {
                return Err(LispError::TypeError(
                    "cl-print-ellipsis".into(),
                    args[0].type_name(),
                ));
            }
            let expansion = string_text(expansion)?;
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &expansion, stream.as_ref(), env)?;
            Ok(Value::Nil)
        }
        "write-char" => {
            need_arg_range(name, args, 1, 2)?;
            let rendered = format_char_conversion(&args[0])?;
            let stream = printer_stream_value(interp, env, args.get(1));
            write_printer_output(interp, &rendered, stream.as_ref(), env)?;
            Ok(args[0].clone())
        }
        "redirect-debugging-output" => {
            need_arg_range(name, args, 0, 2)?;
            let target = match args.first() {
                None | Some(Value::Nil) => Value::Nil,
                Some(value) => Value::String(string_text(value)?),
            };
            interp.set_global_binding("emaxx-external-debugging-output-target", target.clone());
            Ok(target)
        }
        "external-debugging-output" => {
            need_args(name, args, 1)?;
            let rendered = string_like(&args[0])
                .map(|value| value.text)
                .unwrap_or(format_char_conversion(&args[0])?);
            append_external_debugging_output(interp, &rendered)?;
            Ok(args[0].clone())
        }
        "print--preprocess" => {
            need_args(name, args, 1)?;
            print_preprocess(interp, &args[0], env)
        }
        "read-char-choice" => {
            need_arg_range(name, args, 2, 3)?;
            ensure_interaction_allowed(interp, env)?;
            if interp
                .lookup_var("read-char-choice-use-read-key", env)
                .is_none_or(|value| value.is_nil())
                && let Ok(function) = interp.lookup_function("read-char-from-minibuffer", env)
            {
                return interp.call_function_value(
                    function,
                    Some("read-char-from-minibuffer"),
                    &args[..2],
                    env,
                );
            }
            Ok(first_choice_value(&args[1]).unwrap_or(Value::Integer('y' as i64)))
        }
        "y-or-n-p" | "yes-or-no-p" => {
            need_args(name, args, 1)?;
            ensure_interaction_allowed(interp, env)?;
            let _ = super::call(interp, "message", args, env)?;
            match pop_unread_command_event_value(interp, env)
                .ok()
                .and_then(|event| unread_event_char(&event))
                .map(|ch| ch.to_ascii_lowercase())
            {
                Some('n') => Ok(Value::Nil),
                Some('y') => Ok(Value::T),
                _ => Ok(Value::T),
            }
        }

        // ── More string/char ops ──
        "char-equal" => {
            need_args(name, args, 2)?;
            let a = args[0].as_integer()?;
            let b = args[1].as_integer()?;
            let case_fold = interp
                .lookup_var("case-fold-search", env)
                .map(|v| v.is_truthy())
                .unwrap_or(false);
            let eq = if case_fold {
                a == b || (a as u8 as char).eq_ignore_ascii_case(&(b as u8 as char))
            } else {
                a == b
            };
            Ok(if eq { Value::T } else { Value::Nil })
        }
        "number-sequence" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(
                    "number-sequence".into(),
                    args.len(),
                ));
            }
            let integer_sequence = args.iter().all(Value::is_integer);
            if integer_sequence {
                let from = integer_like_bigint(interp, &args[0])?;
                let to = if args.len() > 1 {
                    integer_like_bigint(interp, &args[1])?
                } else {
                    from.clone()
                };
                let step = if args.len() > 2 {
                    integer_like_bigint(interp, &args[2])?
                } else {
                    BigInt::from(1)
                };
                if step.is_zero() {
                    return Err(LispError::Signal(
                        "number-sequence: step must not be 0".into(),
                    ));
                }
                let mut result = Vec::new();
                let mut i = from;
                if step.sign() != Sign::Minus {
                    while i <= to {
                        result.push(normalize_bigint_value(i.clone()));
                        i += &step;
                    }
                } else {
                    while i >= to {
                        result.push(normalize_bigint_value(i.clone()));
                        i += &step;
                    }
                }
                return Ok(Value::list(result));
            }

            let from = numeric_to_f64(interp, &args[0])?;
            let to = if args.len() > 1 {
                numeric_to_f64(interp, &args[1])?
            } else {
                from
            };
            let step_value = args.get(2).cloned().unwrap_or(Value::Integer(1));
            let step = numeric_to_f64(interp, &step_value)?;
            if step == 0.0 {
                return Err(LispError::Signal(
                    "number-sequence: step must not be 0".into(),
                ));
            }
            let mut result = Vec::new();
            let mut current_float = from;
            let mut current_value = args[0].clone();
            let integer_step = step_value.is_integer();
            if step > 0.0 {
                while current_float <= to {
                    result.push(current_value.clone());
                    current_float += step;
                    current_value = if current_value.is_integer() && integer_step {
                        normalize_bigint_value(
                            integer_like_bigint(interp, &current_value)?
                                + integer_like_bigint(interp, &step_value)?,
                        )
                    } else {
                        Value::Float(current_float)
                    };
                }
            } else {
                while current_float >= to {
                    result.push(current_value.clone());
                    current_float += step;
                    current_value = if current_value.is_integer() && integer_step {
                        normalize_bigint_value(
                            integer_like_bigint(interp, &current_value)?
                                + integer_like_bigint(interp, &step_value)?,
                        )
                    } else {
                        Value::Float(current_float)
                    };
                }
            }
            Ok(Value::list(result))
        }
        "kbd" => {
            need_args(name, args, 1)?;
            parse_kbd_sequence(&string_text(&args[0])?)
        }
        "key-description" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(
                    "key-description".into(),
                    args.len(),
                ));
            }
            let mut parts = Vec::new();
            if let Some(prefix) = args.get(1) {
                // PREFIX and KEYS are one event sequence for display.  In
                // particular a trailing ESC in PREFIX combines with the
                // first character in KEYS (`ESC' + `v' -> `M-v').
                let mut events = key_description_events(prefix)?;
                events.extend(key_description_events(&args[0])?);
                let sequence = Value::list(
                    std::iter::once(Value::Symbol("vector-literal".into())).chain(events),
                );
                append_key_description_parts(&sequence, &mut parts)?;
            } else {
                append_key_description_parts(&args[0], &mut parts)?;
            }
            Ok(Value::String(parts.join(" ")))
        }
        "single-key-description" => {
            if args.is_empty() || args.len() > 2 {
                return Err(LispError::WrongNumberOfArgs(
                    "single-key-description".into(),
                    args.len(),
                ));
            }
            let no_angles = args.get(1).is_some_and(Value::is_truthy);
            Ok(Value::String(single_key_description_text(
                &args[0], no_angles,
            )?))
        }
        "text-char-description" => {
            need_args(name, args, 1)?;
            Ok(Value::String(text_char_description_text(
                args[0].as_integer()?,
            )?))
        }

        // ── More buffer ops ──
        "following-char" => match interp.buffer.char_at(interp.buffer.point()) {
            Some(c) => Ok(Value::Integer(public_buffer_char_code(
                c,
                interp.buffer.is_multibyte(),
            ))),
            None => Ok(Value::Integer(0)),
        },
        "preceding-char" => {
            let pt = interp.buffer.point();
            if pt <= interp.buffer.point_min() {
                Ok(Value::Integer(0))
            } else {
                match interp.buffer.char_at(pt - 1) {
                    Some(c) => Ok(Value::Integer(public_buffer_char_code(
                        c,
                        interp.buffer.is_multibyte(),
                    ))),
                    None => Ok(Value::Integer(0)),
                }
            }
        }
        "buffer-last-name" => Ok(Value::String(
            interp
                .buffer
                .last_name
                .clone()
                .unwrap_or_else(|| interp.buffer.name.clone()),
        )),

        // ── Display stubs ──
        "display-graphic-p" | "display-images-p" | "window-system" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "tty-type" | "tty-display-color-p" | "controlling-tty-p" | "tty-top-frame" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Ok(Value::Nil)
        }
        "tty-display-color-cells" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Ok(Value::Integer(0))
        }
        "tty-no-underline" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Ok(Value::Nil)
        }
        "suspend-tty" | "resume-tty" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Err(LispError::Signal(format!(
                "Attempt to {} a non-text terminal device",
                if name == "suspend-tty" {
                    "suspend"
                } else {
                    "resume"
                }
            )))
        }
        "tty--output-buffer-size" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Err(LispError::Signal("Not a tty terminal".into()))
        }
        "tty--set-output-buffer-size" => {
            need_arg_range(name, args, 1, 2)?;
            if !matches!(args[0], Value::Integer(size) if size >= 0) {
                return Err(LispError::Signal("Invalid output buffer size".into()));
            }
            require_live_terminal(interp, args.get(1))?;
            Err(LispError::Signal(
                "Attempt to suspend a non-text terminal device".into(),
            ))
        }
        // Batch sessions have no color support (GNU: nil / 0).
        "display-color-p" | "display-grayscale-p" => Ok(Value::Nil),
        "display-color-cells" => Ok(Value::Integer(0)),
        // emaxx is a batch/TTY display: no face-attribute display support
        // (rmc.el underlines the shortcut key only on graphical terminals).
        "display-supports-face-attributes-p" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(Value::Nil)
        }
        "frame-parameter" => {
            need_arg_range(name, args, 1, 2)?;
            let parameter = args
                .get(1)
                .ok_or_else(|| LispError::WrongNumberOfArgs(name.into(), args.len()))?
                .as_symbol()?;
            Ok(frame_parameter_value(interp, parameter))
        }
        "modify-frame-parameters" => {
            need_args(name, args, 2)?;
            let parameters = args[1].to_vec()?;
            // GNU extracts the alist and applies it in reverse, so when a
            // parameter appears more than once its first occurrence wins.
            for entry in parameters.into_iter().rev() {
                let Some((parameter, value)) = entry.cons_values() else {
                    return Err(LispError::TypeError("consp".into(), entry.type_name()));
                };
                store_frame_parameter(interp, parameter.as_symbol()?.to_string(), value);
            }
            Ok(Value::Nil)
        }
        "set-frame-parameter" => {
            need_args(name, args, 3)?;
            store_frame_parameter(interp, args[1].as_symbol()?.to_string(), args[2].clone());
            Ok(Value::Nil)
        }
        "frame-parameters" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(frame_parameters_value(interp))
        }
        "char-displayable-p" => {
            need_args(name, args, 1)?;
            match &args[0] {
                Value::Integer(codepoint) if char::from_u32(*codepoint as u32).is_some() => {
                    Ok(Value::T)
                }
                Value::String(text) if text.chars().count() == 1 => Ok(Value::T),
                Value::StringObject(state) if state.borrow().text.chars().count() == 1 => {
                    Ok(Value::T)
                }
                _ => Ok(Value::Nil),
            }
        }
        "internal-char-font" => {
            need_arg_range(name, args, 1, 2)?;
            if args[0].is_nil() {
                let Some(character) = args.get(1) else {
                    return Err(LispError::TypeError("characterp".into(), "nil".into()));
                };
                let valid = matches!(character, Value::Integer(codepoint)
                    if u32::try_from(*codepoint).ok().and_then(char::from_u32).is_some());
                if !valid {
                    return Err(LispError::TypeError(
                        "characterp".into(),
                        character.type_name(),
                    ));
                }
            } else {
                let position = position_from_value(interp, &args[0])?;
                if position < interp.buffer.point_min() || position >= interp.buffer.point_max() {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("args-out-of-range".into()),
                        args[0].clone(),
                        Value::Integer(interp.buffer.point_min() as i64),
                        Value::Integer(interp.buffer.point_max() as i64),
                    ])));
                }
                if let Some(character) = args.get(1)
                    && !character.is_nil()
                {
                    let valid = matches!(character, Value::Integer(codepoint)
                        if u32::try_from(*codepoint).ok().and_then(char::from_u32).is_some());
                    if !valid {
                        return Err(LispError::TypeError(
                            "characterp".into(),
                            character.type_name(),
                        ));
                    }
                }
            }
            // Emaxx currently models a headless batch terminal and has no
            // redisplay font object or terminal glyph-code service.  GNU's
            // corresponding batch frame reports nil for both cases.
            Ok(Value::Nil)
        }
        "current-input-mode" => {
            need_args(name, args, 0)?;
            Ok(Value::list([
                if interp.input_interrupt_mode() {
                    Value::T
                } else {
                    Value::Nil
                },
                Value::Nil,
                Value::T,
                Value::Integer(7),
            ]))
        }
        "set-input-interrupt-mode" => {
            need_args(name, args, 1)?;
            interp.set_input_interrupt_mode(args[0].is_truthy());
            Ok(Value::Nil)
        }
        "set-output-flow-control" | "set-input-meta-mode" => {
            need_arg_range(name, args, 1, 2)?;
            // GNU's batch frame has no decoded tty terminal, so these
            // terminal-local setters are successful no-ops.
            Ok(Value::Nil)
        }
        "set-input-mode" => {
            need_arg_range(name, args, 3, 4)?;
            interp.set_input_interrupt_mode(args[0].is_truthy());
            Ok(Value::Nil)
        }
        "set-quit-char" => {
            need_args(name, args, 1)?;
            // No controlling tty in batch mode; GNU returns before
            // validating or changing its tty-local quit character.
            Ok(Value::Nil)
        }
        "frame-width" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(interp.frame_width()))
        }
        "frame-height" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(interp.frame_height()))
        }
        "set-frame-width" => {
            need_arg_range(name, args, 2, 4)?;
            interp.set_frame_width(args[1].as_integer()?);
            Ok(Value::Nil)
        }
        "set-frame-height" => {
            need_arg_range(name, args, 2, 4)?;
            interp.set_frame_height(args[1].as_integer()?);
            Ok(Value::Nil)
        }
        "frame-char-width" | "frame-char-height" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(1))
        }
        "frame-native-width" | "frame-pixel-width" | "frame-text-width" | "frame-text-cols" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(interp.frame_width()))
        }
        "frame-native-height" | "frame-pixel-height" | "frame-text-height" | "frame-text-lines" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(interp.frame_height() + 1))
        }
        "frame-internal-border-width"
        | "frame-fringe-width"
        | "frame-scroll-bar-width"
        | "frame-scroll-bar-height"
        | "frame-right-divider-width"
        | "frame-bottom-divider-width" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(0))
        }
        "display-popup-menus-p" => Ok(Value::Nil),
        "menu-or-popup-active-p" => {
            need_args(name, args, 0)?;
            // A headless Emaxx process cannot have an active native menu.
            Ok(Value::Nil)
        }
        "imagep" => {
            need_args(name, args, 1)?;
            Ok(if valid_image_spec(interp, &args[0], env) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "clear-image-cache" => {
            need_arg_range(name, args, 0, 2)?;
            if let Some(animation_cache) = args.get(1).filter(|value| !value.is_nil()) {
                if animation_cache.cons_values().is_none() {
                    return Err(wrong_type_argument("consp", animation_cache.clone()));
                }
                return Ok(Value::Nil);
            }
            let filter = args.first().cloned().unwrap_or(Value::Nil);
            if filter.is_nil() || matches!(filter, Value::Symbol(ref frame) if frame == "frame") {
                return Err(LispError::Signal(
                    "Window system frame should be used".into(),
                ));
            }
            Ok(Value::Nil)
        }
        "image-cache-size" => {
            need_args(name, args, 0)?;
            // The single batch/TTY frame has no native image cache.
            Ok(Value::Integer(0))
        }
        "image-flush" => {
            need_arg_range(name, args, 1, 2)?;
            if !valid_image_spec(interp, &args[0], env) {
                return Err(LispError::Signal("Invalid image specification".into()));
            }
            if matches!(args.get(1), Some(Value::T)) {
                return Ok(Value::Nil);
            }
            Err(LispError::Signal(
                "Window system frame should be used".into(),
            ))
        }
        "image-transforms-p" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Nil)
        }
        "transient-mark-mode" => {
            let enabled = args.first().is_some_and(Value::is_truthy);
            interp.set_variable(
                "transient-mark-mode",
                if enabled { Value::T } else { Value::Nil },
                env,
            );
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "font-lock-mode" => {
            let enabled = args
                .first()
                .map(|arg| !arg.is_nil() && !matches!(arg, Value::Integer(number) if *number <= 0))
                .unwrap_or(true);
            let buffer_id = interp.current_buffer_id();
            if enabled {
                interp.set_buffer_local_value(buffer_id, "font-lock-mode", Value::T);
                interp.set_buffer_local_value(buffer_id, "jit-lock-mode", Value::T);
                if interp
                    .buffer_local_value(buffer_id, "jit-lock-functions")
                    .is_none()
                {
                    interp.set_buffer_local_value(
                        buffer_id,
                        "jit-lock-functions",
                        Value::list([Value::Symbol("ignore".into())]),
                    );
                }
                interp.set_buffer_local_value(buffer_id, "font-lock-fontified", Value::T);
                font_lock_mode_run_mode_function(interp, buffer_id, Value::T, env)?;
                Ok(Value::T)
            } else {
                interp.set_buffer_local_value(buffer_id, "font-lock-mode", Value::Nil);
                interp.set_buffer_local_value(buffer_id, "jit-lock-mode", Value::Nil);
                interp.set_buffer_local_value(buffer_id, "jit-lock-functions", Value::Nil);
                interp.set_buffer_local_value(buffer_id, "font-lock-fontified", Value::Nil);
                font_lock_mode_run_mode_function(interp, buffer_id, Value::Nil, env)?;
                Ok(Value::Nil)
            }
        }
        "visual-line-mode" => {
            let enabled = args
                .first()
                .map(|arg| !arg.is_nil() && !matches!(arg, Value::Integer(number) if *number <= 0))
                .unwrap_or(true);
            let buffer_id = interp.current_buffer_id();
            interp.set_buffer_local_value(
                buffer_id,
                "visual-line-mode",
                if enabled { Value::T } else { Value::Nil },
            );
            let mut local_modes = interp
                .buffer_local_value(buffer_id, "local-minor-modes")
                .and_then(|value| value.to_vec().ok())
                .unwrap_or_default();
            let mode = Value::Symbol("visual-line-mode".into());
            if enabled {
                if !local_modes.iter().any(|entry| entry == &mode) {
                    local_modes.insert(0, mode);
                }
            } else {
                local_modes.retain(|entry| entry != &mode);
            }
            interp.set_buffer_local_value(buffer_id, "local-minor-modes", Value::list(local_modes));
            crate::lisp::primitives::call_function_value(
                interp,
                &Value::Symbol("run-hooks".into()),
                &[
                    Value::Symbol("visual-line-mode-hook".into()),
                    Value::Symbol(
                        if enabled {
                            "visual-line-mode-on-hook"
                        } else {
                            "visual-line-mode-off-hook"
                        }
                        .into(),
                    ),
                ],
                env,
            )?;
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "header-line-indent-mode" => {
            let enabled = args
                .first()
                .map(|arg| !arg.is_nil() && !matches!(arg, Value::Integer(number) if *number <= 0))
                .unwrap_or(true);
            let buffer_id = interp.current_buffer_id();
            interp.set_buffer_local_value(
                buffer_id,
                "header-line-indent-mode",
                if enabled { Value::T } else { Value::Nil },
            );
            interp.set_buffer_local_value(
                buffer_id,
                "header-line-indent",
                Value::String(String::new()),
            );
            interp.set_buffer_local_value(buffer_id, "header-line-indent-width", Value::Integer(0));
            Ok(if enabled { Value::T } else { Value::Nil })
        }
        "font-lock-specified-p" => {
            need_arg_range(name, args, 0, 1)?;
            let mode = args.first().is_some_and(Value::is_truthy);
            let defaults = interp
                .lookup_var("font-lock-defaults", env)
                .unwrap_or(Value::Nil);
            let keywords = interp
                .lookup_var("font-lock-keywords", env)
                .unwrap_or(Value::Nil);
            let major_mode = interp.lookup_var("major-mode", env).unwrap_or(Value::Nil);
            let font_lock_major_mode = interp
                .lookup_var("font-lock-major-mode", env)
                .unwrap_or(Value::Nil);
            let set_defaults = interp
                .lookup_var("font-lock-set-defaults", env)
                .unwrap_or(Value::Nil);
            Ok(
                if defaults.is_truthy()
                    || keywords.is_truthy()
                    || (mode
                        && set_defaults.is_truthy()
                        && font_lock_major_mode.is_truthy()
                        && font_lock_major_mode != major_mode)
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "font-lock-add-keywords" => {
            need_arg_range(name, args, 2, 3)?;
            let buffer_id = interp.current_buffer_id();
            let mut current = font_lock_raw_keyword_specs(
                interp.buffer_local_value(buffer_id, "font-lock-keywords"),
            );
            let additions = args[1].to_vec()?;
            if args.get(2).is_some_and(|value| !value.is_nil()) {
                current.extend(additions);
            } else {
                let mut updated = additions;
                updated.extend(current);
                current = updated;
            }
            interp.set_buffer_local_value(
                buffer_id,
                "font-lock-keywords",
                font_lock_keywords_value(&current),
            );
            Ok(Value::Nil)
        }
        "font-lock-remove-keywords" => {
            need_args(name, args, 2)?;
            let buffer_id = interp.current_buffer_id();
            let mut current = interp
                .buffer_local_value(buffer_id, "font-lock-keywords")
                .unwrap_or(Value::Nil)
                .to_vec()
                .unwrap_or_default();
            let removals = args[1].to_vec()?;
            current.retain(|existing| {
                !removals
                    .iter()
                    .any(|keyword| values_equal(interp, existing, keyword))
            });
            interp.set_buffer_local_value(buffer_id, "font-lock-keywords", Value::list(current));
            Ok(Value::Nil)
        }
        "font-lock-flush" => {
            need_arg_range(name, args, 0, 2)?;
            if !interp
                .lookup_var("font-lock-mode", env)
                .unwrap_or(Value::Nil)
                .is_truthy()
                || !interp
                    .lookup_var("font-lock-fontified", env)
                    .unwrap_or(Value::Nil)
                    .is_truthy()
            {
                return Ok(Value::Nil);
            }
            let start = args
                .first()
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point_min());
            let end = args
                .get(1)
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point_max());
            font_lock_ensure_region(interp, start, end, env)?;
            Ok(Value::Nil)
        }
        "font-lock-ensure" | "font-lock-fontify-region" => {
            // font-lock-fontify-region also takes GNU's optional LOUDLY.
            need_arg_range(name, args, 0, 3)?;
            // GNU fontifies whenever fontification is specified for the
            // buffer (font-lock-specified-p), even with font-lock-mode off
            // in batch.
            if std::env::var("EMAXX_DEBUG_FONTLOCK").is_ok() {
                eprintln!(
                    "FONTLOCK ensure called buffer={}",
                    interp.current_buffer_id()
                );
            }
            font_lock_install_mode_defaults(interp, env)?;
            let specified = interp
                .lookup_var("font-lock-defaults", env)
                .is_some_and(|value| value.is_truthy());
            if !specified
                && !interp
                    .lookup_var("font-lock-mode", env)
                    .unwrap_or(Value::Nil)
                    .is_truthy()
            {
                return Ok(Value::Nil);
            }
            let start = args
                .first()
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point_min());
            let end = args
                .get(1)
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| interp.buffer.point_max());
            font_lock_ensure_region(interp, start, end, env)?;
            if name == "font-lock-fontify-region"
                && super::call(
                    interp,
                    "fboundp",
                    &[Value::Symbol(
                        "emaxx--font-lock-fontify-region-extras".into(),
                    )],
                    env,
                )?
                .is_truthy()
            {
                interp.call_function_value(
                    Value::Symbol("emaxx--font-lock-fontify-region-extras".into()),
                    Some("emaxx--font-lock-fontify-region-extras"),
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                )?;
            }
            Ok(Value::Nil)
        }
        "find-image" => {
            need_args(name, args, 1)?;
            let specs = args[0].to_vec()?;
            Ok(specs.into_iter().next().unwrap_or(Value::Nil))
        }
        "image-size" | "image-mask-p" | "image-metadata" => Err(LispError::Signal(
            "Images are unavailable on a nongraphical display".into(),
        )),
        "imagemagick-types" => Ok(Value::list([
            Value::Symbol("png".into()),
            Value::Symbol("jpeg".into()),
            Value::Symbol("gif".into()),
        ])),
        "init-image-library" => {
            need_args(name, args, 1)?;
            let image_type = args[0].as_symbol()?;
            Ok(
                if matches!(
                    image_type,
                    "pbm" | "png" | "jpeg" | "gif" | "svg" | "xbm" | "xpm" | "webp" | "tiff"
                ) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "window-start" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Integer(window_start(interp, args.first())? as i64))
        }
        "window-end" => {
            need_arg_range(name, args, 0, 1)?;
            let buffer_id = if let Some(window) = args.first() {
                window_buffer_id(interp, window)
                    .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?
            } else {
                interp.selected_window_buffer_id()
            };
            let (_, point_max) = buffer_point_bounds(interp, buffer_id);
            Ok(Value::Integer(point_max as i64))
        }
        "window-point" => {
            need_arg_range(name, args, 0, 1)?;
            let buffer_id = if let Some(window) = args.first() {
                window_buffer_id(interp, window)
                    .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?
            } else {
                interp.selected_window_buffer_id()
            };
            let point = if buffer_id == interp.current_buffer_id() {
                interp.buffer.point()
            } else {
                interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.point())
                    .unwrap_or_else(|| interp.buffer.point())
            };
            Ok(Value::Integer(point as i64))
        }
        "window-hscroll" => {
            need_arg_range(name, args, 0, 1)?;
            let window_id = live_window_id_or_selected(interp, args.first())?;
            Ok(window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
                .as_integer()
                .map(Value::Integer)
                .unwrap_or(Value::Integer(0)))
        }
        "window-vscroll" => {
            need_arg_range(name, args, 0, 2)?;
            live_window_id_or_selected(interp, args.first())?;
            // A terminal frame has no smooth pixel scrolling; GNU returns
            // zero in both canonical-line and pixel modes.
            Ok(Value::Integer(0))
        }
        "set-window-hscroll" => {
            need_args(name, args, 2)?;
            let window_id = live_window_id_or_selected(interp, args.first())?;
            let Value::Integer(requested) = args[1] else {
                return Err(wrong_type_argument("fixnump", args[1].clone()));
            };
            set_window_hscroll_value(interp, window_id, requested)
        }
        "scroll-left" | "scroll-right" => {
            need_arg_range(name, args, 0, 2)?;
            let window_id = live_window_id_or_selected(interp, None)?;
            let requested = match args.first() {
                None | Some(Value::Nil) => window_text_width_columns(interp, window_id) - 2,
                Some(value) => prefix_numeric_value(value)?.as_integer()?,
            };
            let current = window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT)
                .as_integer()
                .unwrap_or(0);
            let requested = if name == "scroll-left" {
                current.saturating_add(requested)
            } else {
                current.saturating_sub(requested)
            };
            let result = set_window_hscroll_value(interp, window_id, requested)?;
            if args.get(1).is_some_and(Value::is_truthy) {
                set_window_slot_value(interp, window_id, WINDOW_MIN_HSCROLL_SLOT, result.clone())?;
            }
            Ok(result)
        }
        "other-window-for-scrolling" => {
            need_args(name, args, 0)?;
            let selected_id = live_window_id_or_selected(interp, None)?;
            let selected_kind = window_slot_value(interp, selected_id, WINDOW_KIND_SLOT);
            let mut candidate = if matches!(
                selected_kind,
                Value::Symbol(ref kind) if kind == MINIBUFFER_WINDOW_KIND
            ) {
                interp
                    .lookup_var("minibuffer-scroll-window", env)
                    .filter(Value::is_truthy)
            } else {
                None
            };

            if candidate.is_none()
                && let Some(buffer @ Value::Buffer(buffer_id, _)) =
                    interp.lookup_var("other-window-scroll-buffer", env)
                && interp.has_buffer_id(buffer_id)
            {
                candidate = live_ordinary_window_ids(interp)
                    .into_iter()
                    .find(|window_id| {
                        window_buffer_id(interp, &Value::Record(*window_id)) == Some(buffer_id)
                    })
                    .map(Value::Record);
                if candidate.is_none() {
                    candidate = Some(call_function_value(
                        interp,
                        &Value::symbol("display-buffer"),
                        &[buffer, Value::T],
                        env,
                    )?);
                }
            }

            if candidate.is_none()
                && let Some(default) = interp
                    .lookup_var("other-window-scroll-default", env)
                    .filter(Value::is_truthy)
                && crate::lisp::primitives::call(
                    interp,
                    "functionp",
                    std::slice::from_ref(&default),
                    env,
                )?
                .is_truthy()
            {
                candidate = Some(call_function_value(interp, &default, &[], env)?);
            }

            let candidate = candidate.unwrap_or_else(|| {
                let ids = live_ordinary_window_ids(interp);
                let index = ids.iter().position(|id| *id == selected_id).unwrap_or(0);
                ids.get((index + 1) % ids.len())
                    .copied()
                    .map(Value::Record)
                    .unwrap_or_else(|| interp.selected_window_value())
            });
            let candidate_id = live_window_id_or_selected(interp, Some(&candidate))?;
            if candidate_id == selected_id {
                return Err(LispError::Signal("There is no other window".into()));
            }
            Ok(Value::Record(candidate_id))
        }
        "set-window-margins" => {
            need_arg_range(name, args, 1, 3)?;
            let window_id = if args.first().is_none_or(|value| value.is_nil()) {
                interp.selected_window_id()
            } else {
                crate::lisp::primitives::window::window_record_id_from_value(interp, &args[0])
                    .ok_or_else(|| LispError::TypeError("window".into(), args[0].type_name()))?
            };
            let margin = |value: Option<&Value>| -> Result<Option<i64>, LispError> {
                match value {
                    None | Some(Value::Nil) => Ok(None),
                    Some(value) => Ok(Some(value.as_integer()?.max(0))),
                }
            };
            let left = margin(args.get(1))?;
            let right = margin(args.get(2))?;
            interp.set_window_margins(window_id, left, right);
            Ok(Value::T)
        }
        "window-margins" => {
            need_arg_range(name, args, 0, 1)?;
            let window_id = if args.first().is_none_or(|value| value.is_nil()) {
                interp.selected_window_id()
            } else {
                crate::lisp::primitives::window::window_record_id_from_value(interp, &args[0])
                    .ok_or_else(|| LispError::TypeError("window".into(), args[0].type_name()))?
            };
            let (left, right) = interp.window_margins(window_id);
            let to_value = |margin: Option<i64>| margin.map(Value::Integer).unwrap_or(Value::Nil);
            Ok(Value::cons(to_value(left), to_value(right)))
        }
        "coordinates-in-window-p" => {
            need_args(name, args, 2)?;
            let window_id = live_window_id_or_selected(interp, Some(&args[1]))?;
            let x = args[0].car()?.as_float()?.trunc() as i64;
            let y = args[0].cdr()?.as_float()?.trunc() as i64;
            let (width, height, left, top) = window_geometry(interp, window_id);
            if x < left || x >= left + width || y < top || y >= top + height {
                return Ok(Value::Nil);
            }
            let local_x = x - left;
            let local_y = y - top;
            let buffer_id = window_buffer_id(interp, &Value::Record(window_id))
                .ok_or_else(|| wrong_type_argument("window-live-p", args[1].clone()))?;
            let tab_line = window_line_height(interp, buffer_id, "tab-line-format", env);
            let header_line = window_line_height(interp, buffer_id, "header-line-format", env);
            let mode_line = window_line_height(interp, buffer_id, "mode-line-format", env);
            if tab_line > 0 && local_y < tab_line {
                return Ok(Value::symbol("tab-line"));
            }
            if header_line > 0 && local_y < tab_line + header_line {
                return Ok(Value::symbol("header-line"));
            }
            if mode_line > 0 && local_y >= height - mode_line {
                return Ok(Value::symbol("mode-line"));
            }
            let (left_margin, right_margin) = interp.window_margins(window_id);
            let left_margin = left_margin.unwrap_or(0);
            let right_margin = right_margin.unwrap_or(0);
            if local_x < left_margin {
                return Ok(Value::symbol("left-margin"));
            }
            if local_x >= width - right_margin {
                return Ok(Value::symbol("right-margin"));
            }
            Ok(Value::cons(
                Value::Integer(local_x - left_margin),
                Value::Integer(local_y),
            ))
        }
        "pos-visible-in-window-p" => {
            need_arg_range(name, args, 0, 3)?;
            if interp
                .lookup_var("noninteractive", env)
                .is_some_and(|value| !value.is_nil())
            {
                return Ok(Value::Nil);
            }
            let window = args.get(1).filter(|value| !value.is_nil());
            let buffer_id = if let Some(window) = window {
                window_buffer_id(interp, window)
                    .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?
            } else {
                interp.selected_window_buffer_id()
            };
            let (point_min, point_max) = buffer_point_bounds(interp, buffer_id);
            let pos = match args.first() {
                None | Some(Value::Nil) => interp.buffer.point(),
                Some(Value::T) => point_max,
                Some(value) => position_from_value(interp, value)?,
            };
            let start = window_start(interp, window)?;
            let first_visible = start.max(point_min);
            let visible_line = line_distance_in_buffer(interp, buffer_id, first_visible, pos);
            Ok(
                if pos >= first_visible
                    && pos <= point_max
                    && visible_line < DEFAULT_SELECTED_WINDOW_HEIGHT
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "window-width" | "window-total-width" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(Value::Integer(window_geometry(interp, window_id).0))
        }
        "window-height" | "window-total-height" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(Value::Integer(window_geometry(interp, window_id).1))
        }
        "window-use-time" => {
            need_arg_range(name, args, 0, 1)?;
            let window_id = live_window_id_or_selected(interp, args.first())?;
            Ok(Value::Integer(interp.window_use_time(window_id)))
        }
        "window-bump-use-time" => {
            need_arg_range(name, args, 0, 1)?;
            let window_id = live_window_id_or_selected(interp, args.first())?;
            Ok(interp
                .bump_window_use_time(window_id)
                .map(Value::Integer)
                .unwrap_or(Value::Nil))
        }
        "window-body-width" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            window_buffer_id_or_selected(interp, args.first())?;
            Ok(Value::Integer(window_text_width_columns(interp, window_id)))
        }
        "window-body-height" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
            Ok(Value::Integer(
                window_geometry(interp, window_id).1
                    - window_non_body_height(interp, buffer_id, env),
            ))
        }
        "window-text-width" => {
            need_arg_range(name, args, 0, 2)?;
            let window_id = live_window_id_or_selected(interp, args.first())?;
            Ok(Value::Integer(window_text_width_columns(interp, window_id)))
        }
        "window-text-height" => {
            need_arg_range(name, args, 0, 2)?;
            let window_id = live_window_id_or_selected(interp, args.first())?;
            let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
            Ok(Value::Integer(
                window_geometry(interp, window_id).1
                    - window_non_body_height(interp, buffer_id, env),
            ))
        }
        "window-line-height" => {
            need_arg_range(name, args, 0, 2)?;
            live_window_id_or_selected(interp, args.get(1))?;
            // GNU has no current glyph matrix in batch mode.
            Ok(Value::Nil)
        }
        "window-lines-pixel-dimensions" => {
            need_arg_range(name, args, 0, 6)?;
            live_window_id_or_selected(interp, args.first())?;
            // GNU has no current glyph matrix in batch mode.
            Ok(Value::Nil)
        }
        "window-pixel-width" | "window-pixel-height" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let geometry = window_geometry(interp, window_id);
            Ok(Value::Integer(if name == "window-pixel-width" {
                geometry.0
            } else {
                geometry.1
            }))
        }
        "window-pixel-left" | "window-pixel-top" | "window-left-column" | "window-top-line" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let geometry = window_geometry(interp, window_id);
            Ok(Value::Integer(
                if matches!(name, "window-pixel-left" | "window-left-column") {
                    geometry.2
                } else {
                    geometry.3
                },
            ))
        }
        "window-old-pixel-width"
        | "window-old-pixel-height"
        | "window-old-body-pixel-width"
        | "window-old-body-pixel-height" => {
            need_arg_range(name, args, 0, 1)?;
            if let Some(window) = args.first() {
                window_id_or_selected(interp, window)?;
            }
            Ok(Value::Integer(0))
        }
        "window-new-pixel" | "window-new-total" | "window-new-normal" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let slot = match name {
                "window-new-pixel" => WINDOW_NEW_PIXEL_SLOT,
                "window-new-total" => WINDOW_NEW_TOTAL_SLOT,
                _ => WINDOW_NEW_NORMAL_SLOT,
            };
            Ok(interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(slot))
                .cloned()
                .unwrap_or(Value::Integer(0)))
        }
        "window-normal-size" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let slot = if args.get(1).is_some_and(Value::is_truthy) {
                WINDOW_NORMAL_WIDTH_SLOT
            } else {
                WINDOW_NORMAL_HEIGHT_SLOT
            };
            Ok(interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(slot))
                .cloned()
                .unwrap_or(Value::Float(1.0)))
        }
        "set-window-new-pixel" | "set-window-new-total" => {
            need_arg_range(name, args, 2, 3)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            let requested = args[1].as_integer()?;
            let slot = if name == "set-window-new-pixel" {
                WINDOW_NEW_PIXEL_SLOT
            } else {
                WINDOW_NEW_TOTAL_SLOT
            };
            let current = window_slot_value(interp, window_id, slot)
                .as_integer()
                .unwrap_or(0);
            let requested = if args.get(2).is_some_and(Value::is_truthy) {
                current.saturating_add(requested)
            } else {
                requested
            };
            if name == "set-window-new-pixel" && requested < 0 {
                return Err(LispError::Signal(format!("Args out of range: {requested}")));
            }
            set_window_slot_value(interp, window_id, slot, Value::Integer(requested))
        }
        "set-window-new-normal" => {
            need_arg_range(name, args, 1, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            set_window_slot_value(
                interp,
                window_id,
                WINDOW_NEW_NORMAL_SLOT,
                args.get(1).cloned().unwrap_or(Value::Nil),
            )
        }
        "window-resize-apply" => {
            need_arg_range(name, args, 0, 2)?;
            if let Some(frame) = args.first().filter(|frame| !frame.is_nil())
                && !matches!(frame, Value::Symbol(symbol) if symbol == "frame")
            {
                return Err(LispError::TypeError("frame".into(), frame.type_name()));
            }
            let horizontal = args.get(1).is_some_and(Value::is_truthy);
            let root = frame_root_window_value(interp);
            let root_id = window_id_or_selected(interp, &root)?;
            let requested = window_slot_value(interp, root_id, WINDOW_NEW_PIXEL_SLOT)
                .as_integer()
                .unwrap_or(0);
            let geometry = window_geometry(interp, root_id);
            let actual = if horizontal { geometry.0 } else { geometry.1 };
            Ok(if requested == actual {
                Value::T
            } else {
                Value::Nil
            })
        }
        "window-resize-apply-total" => {
            need_arg_range(name, args, 0, 2)?;
            if let Some(frame) = args.first().filter(|frame| !frame.is_nil())
                && !matches!(frame, Value::Symbol(symbol) if symbol == "frame")
            {
                return Err(LispError::TypeError("frame".into(), frame.type_name()));
            }
            Ok(Value::T)
        }
        "window-mode-line-height" | "window-header-line-height" | "window-tab-line-height" => {
            need_arg_range(name, args, 0, 1)?;
            let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
            let format_variable = match name {
                "window-mode-line-height" => "mode-line-format",
                "window-header-line-height" => "header-line-format",
                _ => "tab-line-format",
            };
            Ok(Value::Integer(window_line_height(
                interp,
                buffer_id,
                format_variable,
                env,
            )))
        }
        "window-right-divider-width"
        | "window-bottom-divider-width"
        | "window-scroll-bar-width"
        | "window-scroll-bar-height" => {
            need_arg_range(name, args, 0, 1)?;
            if let Some(window) = args.first() {
                window_id_or_selected(interp, window)?;
            }
            Ok(Value::Integer(0))
        }
        "move-to-window-line" => {
            need_arg_range(name, args, 0, 1)?;
            let line = resolve_window_line(args.first(), DEFAULT_SELECTED_WINDOW_HEIGHT / 2)?;
            let window_start = current_window_start(interp);
            let (target, shortage) = move_lines_from(interp, window_start, line);
            interp.buffer.goto_char(target);
            let actual = if shortage > 0 {
                line - shortage
            } else if shortage < 0 {
                line + shortage.abs()
            } else {
                line
            };
            Ok(Value::Integer(actual as i64))
        }
        "recenter" => {
            need_arg_range(name, args, 0, 2)?;
            let line = resolve_window_line(args.first(), DEFAULT_SELECTED_WINDOW_HEIGHT / 2)?;
            let point_line = beginning_of_line_at(interp, interp.buffer.point());
            let (new_start, _) = move_lines_from(interp, point_line, -line);
            set_current_window_start(interp, new_start);
            Ok(Value::Nil)
        }
        "scroll-up" => {
            need_arg_range(name, args, 0, 1)?;
            let count = if let Some(value) = args.first() {
                prefix_numeric_value(value)?.as_integer()?
            } else {
                1
            };
            scroll_selected_window(interp, count as isize, env)?;
            Ok(Value::Nil)
        }
        "scroll-down" => {
            need_arg_range(name, args, 0, 1)?;
            let count = if let Some(value) = args.first() {
                prefix_numeric_value(value)?.as_integer()?
            } else {
                1
            };
            scroll_selected_window(interp, -(count as isize), env)?;
            Ok(Value::Nil)
        }
        "window-text-pixel-size" => {
            let width = interp
                .buffer
                .buffer_string()
                .lines()
                .map(|line| line.chars().count())
                .max()
                .unwrap_or(0);
            let height = interp.buffer.buffer_string().lines().count().max(1);
            Ok(Value::cons(
                Value::Integer(width as i64),
                Value::Integer(height as i64),
            ))
        }
        "buffer-text-pixel-size" => {
            // (buffer-text-pixel-size &optional WINDOW FROM TO X-LIMIT).
            // Without a graphical frame there is no font, so report the
            // widest line's character count as the pixel width and the line
            // count as the pixel height (one nominal unit per character).
            // Like GNU, honor `display' replacements: a string spec
            // substitutes for the covered text and a margin spec removes it
            // from the line flow entirely.
            let point_min = interp.buffer.point_min();
            let point_max = interp.buffer.point_max();
            let mut effective = String::new();
            let mut pos = point_min;
            while pos < point_max {
                let display = interp.buffer.text_property_at(pos, "display");
                if let Some(display_value) = display.clone().filter(|value| !value.is_nil()) {
                    let mut end = pos;
                    while end < point_max
                        && interp.buffer.text_property_at(end, "display") == display
                    {
                        end += 1;
                    }
                    if let Some(replacement) = string_like(&display_value) {
                        effective.push_str(&replacement.text);
                        pos = end;
                        continue;
                    }
                    let in_margin = display_value.to_vec().is_ok_and(|items| {
                        items.first().is_some_and(|head| {
                            head.to_vec().is_ok_and(|spec| {
                                matches!(spec.first(),
                                    Some(Value::Symbol(kind)) if kind == "margin")
                            })
                        })
                    });
                    if in_margin {
                        pos = end;
                        continue;
                    }
                }
                match interp.buffer.char_at(pos) {
                    Some(ch) => effective.push(ch),
                    None => break,
                }
                pos += 1;
            }
            let width = effective
                .lines()
                .map(|line| line.chars().count())
                .max()
                .unwrap_or(0);
            let height = effective.lines().count().max(1);
            Ok(Value::cons(
                Value::Integer(width as i64),
                Value::Integer(height as i64),
            ))
        }
        "get-display-property" => {
            need_args(name, args, 2)?;
            let pos = args[0].as_integer()?.max(0) as usize;
            let property = args[1].as_symbol()?;
            let display = interp
                .buffer
                .text_property_at(pos, "display")
                .unwrap_or(Value::Nil);
            Ok(display_property_value(&display, property).unwrap_or(Value::Nil))
        }
        "bidi-find-overridden-directionality" => {
            need_args(name, args, 2)?;
            let start = position_from_value(interp, &args[0])?;
            let end = position_from_value(interp, &args[1])?;
            Ok(find_bidi_override(interp, start, end)
                .map(|pos| Value::Integer(pos as i64))
                .unwrap_or(Value::Nil))
        }
        "redisplay" => {
            need_arg_range(name, args, 0, 1)?;
            // The cache-free batch renderer cannot be preempted by pending
            // terminal input, so redisplay always completes.
            Ok(Value::T)
        }
        "redraw-frame" => {
            need_arg_range(name, args, 0, 1)?;
            decode_live_frame(args.first(), true)?;
            // GNU clears glyph matrices and immediately recomputes them.
            // Emaxx retains no glyph matrix between batch evaluations.
            Ok(Value::Nil)
        }
        "redraw-display" => {
            need_args(name, args, 0)?;
            Ok(Value::Nil)
        }
        "display--update-for-mouse-movement" => {
            need_args(name, args, 2)?;
            for coordinate in args {
                if !matches!(coordinate, Value::Integer(_)) {
                    return Err(wrong_type_argument("fixnump", coordinate.clone()));
                }
            }
            // Mouse-face/help-echo redisplay is absent from the headless
            // renderer, but the native fixnum surface remains observable.
            Ok(Value::Nil)
        }
        "open-termscript" => {
            need_args(name, args, 1)?;
            // GNU's batch bootstrap frame is output_initial, not termcap;
            // it rejects both a filename and nil before inspecting FILE.
            Err(LispError::Signal(
                "Current frame is not on a tty device".into(),
            ))
        }
        "frame-or-buffer-changed-p" => {
            need_arg_range(name, args, 0, 1)?;
            frame_or_buffer_changed(interp, args.first(), env)
        }
        "internal-show-cursor" => {
            need_args(name, args, 2)?;
            let window_id = if args[0].is_nil() {
                interp.selected_window_id()
            } else {
                window_record_id_from_value(interp, &args[0])
                    .ok_or_else(|| LispError::TypeError("window".into(), args[0].type_name()))?
            };
            interp.set_window_cursor_visible(window_id, args[1].is_truthy());
            Ok(Value::Nil)
        }
        "internal-show-cursor-p" => {
            need_arg_range(name, args, 0, 1)?;
            let window_id = match args.first() {
                None | Some(Value::Nil) => interp.selected_window_id(),
                Some(window) => window_record_id_from_value(interp, window)
                    .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?,
            };
            Ok(if interp.window_cursor_visible(window_id) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "set-buffer-redisplay" => {
            need_args(name, args, 4)?;
            // GNU's xdisp.c primitive invalidates redisplay caches.  Emaxx's
            // batch renderer has no retained glyph cache, so the observable
            // watcher contract is the arity and nil return value.
            Ok(Value::Nil)
        }
        "font-spec" => {
            let mut name_spec = None;
            let mut index = 0;
            while index + 1 < args.len() {
                if let Value::Symbol(keyword) = &args[index]
                    && keyword == ":name"
                {
                    name_spec = Some(string_text(&args[index + 1])?);
                }
                index += 2;
            }
            Ok(interp.create_record(
                "font-spec",
                vec![Value::String(name_spec.unwrap_or_default())],
            ))
        }
        "font-get" => {
            need_args(name, args, 2)?;
            let property = args[1].as_symbol()?;
            let info = font_spec_info(interp, &args[0])?;
            Ok(match property {
                ":family" => info.family.map(Value::Symbol).unwrap_or(Value::Nil),
                ":size" => info.size.map(Value::Float).unwrap_or(Value::Nil),
                ":weight" => info.weight.map(Value::Symbol).unwrap_or(Value::Nil),
                ":slant" => info.slant.map(Value::Symbol).unwrap_or(Value::Nil),
                ":spacing" => info.spacing.map(Value::Integer).unwrap_or(Value::Nil),
                ":foundry" => info.foundry.map(Value::Symbol).unwrap_or(Value::Nil),
                _ => Value::Nil,
            })
        }
        "face-attribute" => {
            if args.len() < 2 || args.len() > 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = args[0].as_symbol()?;
            let attribute = args[1].as_symbol()?;
            Ok(face_attribute_value(interp, face, attribute, args.get(3)))
        }
        "face-name" => {
            need_args(name, args, 1)?;
            let face = args[0].as_symbol()?;
            if !face_exists(interp, face) {
                return Err(LispError::Signal(format!("Not a face: {face}")));
            }
            Ok(Value::String(face.to_string()))
        }
        "face-foreground" | "face-background" => {
            if args.is_empty() || args.len() > 3 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = args[0].as_symbol()?;
            let attribute = if name == "face-foreground" {
                ":foreground"
            } else {
                ":background"
            };
            let value = face_attribute_value(interp, face, attribute, args.get(2));
            Ok(if is_unspecified_face_attribute(&value) {
                Value::Nil
            } else {
                value
            })
        }
        "face-spec-set" => {
            // GNU faces.el: store SPEC under the requested spec property,
            // define FACE, and apply the spec's default-display attributes
            // (emaxx's face model realizes attributes as symbol properties).
            need_arg_range(name, args, 2, 3)?;
            let mut face = args[0].as_symbol()?.to_string();
            while let Some(alias) = interp
                .get_symbol_property(&face, "face-alias")
                .and_then(|value| value.as_symbol().ok().map(str::to_string))
            {
                face = alias;
            }
            let spec = args[1].clone();
            let spec_type = args
                .get(2)
                .and_then(|value| value.as_symbol().ok())
                .filter(|symbol| !symbol.is_empty())
                .unwrap_or("face-override-spec")
                .to_string();
            if matches!(
                spec_type.as_str(),
                "face-defface-spec" | "face-override-spec" | "customized-face" | "saved-face"
            ) {
                interp.put_symbol_property(&face, &spec_type, spec.clone());
            }
            if matches!(spec_type.as_str(), "reset" | "saved-face") {
                interp.put_symbol_property(&face, "customized-face", Value::Nil);
            }
            if matches!(
                spec_type.as_str(),
                "customized-face" | "saved-face" | "reset"
            ) {
                interp.put_symbol_property(&face, "face-override-spec", Value::Nil);
            }
            interp.put_symbol_property(&face, "face-modified", Value::Nil);
            if spec_type != "reset" {
                interp.record_defface_runtime_attributes(&face, &spec)?;
            }
            Ok(Value::Nil)
        }
        "set-face-attribute" => {
            if args.len() < 4 {
                return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
            }
            let face = args[0].as_symbol()?.to_string();
            if !face_exists(interp, &face) {
                return Err(LispError::SignalValue(Value::list([
                    Value::Symbol("error".into()),
                    Value::String("Invalid face".into()),
                    Value::Symbol(face),
                ])));
            }
            let mut index = 2;
            while index + 1 < args.len() {
                let attribute = args[index].as_symbol()?;
                let value = &args[index + 1];
                if attribute == ":inherit" {
                    let inherit = match value {
                        Value::Nil => None,
                        Value::Symbol(symbol) => Some(symbol.clone()),
                        _ => return Err(LispError::TypeError("symbol".into(), value.type_name())),
                    };
                    interp.set_face_inherit_target(&face, inherit)?;
                }
                interp.put_symbol_property(
                    &face,
                    &face_attribute_property_name(attribute),
                    value.clone(),
                );
                index += 2;
            }
            Ok(Value::Nil)
        }
        "color-distance" => {
            need_args(name, args, 2)?;
            let left = parse_color_spec(&string_text(&args[0])?)
                .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
            let right = parse_color_spec(&string_text(&args[1])?)
                .ok_or_else(|| LispError::Signal("Invalid color specification".into()))?;
            let distance = left
                .into_iter()
                .zip(right)
                .map(|(a, b)| {
                    let diff = i64::from(a) - i64::from(b);
                    diff * diff
                })
                .sum::<i64>();
            Ok(Value::Integer(distance))
        }
        "color-values-from-color-spec" => {
            need_args(name, args, 1)?;
            Ok(parse_color_spec(&string_text(&args[0])?)
                .map(|[r, g, b]| {
                    Value::list([
                        Value::Integer(i64::from(r)),
                        Value::Integer(i64::from(g)),
                        Value::Integer(i64::from(b)),
                    ])
                })
                .unwrap_or(Value::Nil))
        }
        "color-values" => {
            need_arg_range(name, args, 1, 2)?;
            if matches!(&args[0], Value::Symbol(symbol) if symbol == "unspecified")
                || matches!(&args[0], Value::String(text) if matches!(text.as_str(), "unspecified-fg" | "unspecified-bg"))
            {
                return Ok(Value::Nil);
            }
            Ok(parse_color_spec(&string_text(&args[0])?)
                .map(|[r, g, b]| {
                    Value::list([
                        Value::Integer(i64::from(r)),
                        Value::Integer(i64::from(g)),
                        Value::Integer(i64::from(b)),
                    ])
                })
                .unwrap_or(Value::Nil))
        }
        "selected-window" => Ok(interp.selected_window_value()),
        "old-selected-window" => Ok(interp.old_selected_window_value()),
        "frame-selected-window" => Ok(interp.selected_window_value()),
        "frame-old-selected-window" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(interp.frame_old_selected_window_value())
        }
        "set-frame-selected-window" => {
            need_arg_range(name, args, 2, 3)?;
            if !args[0].is_nil() && !matches!(&args[0], Value::Symbol(frame) if frame == "frame") {
                return Err(LispError::TypeError(
                    "frame-live-p".into(),
                    args[0].type_name(),
                ));
            }
            select_window_value(interp, &args[1], args.get(2).is_some_and(Value::is_truthy))
        }
        "select-window" => {
            need_arg_range(name, args, 1, 2)?;
            select_window_value(interp, &args[0], args.get(1).is_some_and(Value::is_truthy))
        }
        "current-window-configuration" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(interp.window_configuration_value())
        }
        "set-window-configuration" => {
            need_arg_range(name, args, 1, 3)?;
            let restored = interp.apply_window_configuration_value(&args[0])?;
            Ok(if restored { Value::T } else { Value::Nil })
        }
        "window-configuration-p" => {
            need_args(name, args, 1)?;
            Ok(if interp.is_window_configuration_value(&args[0]) {
                Value::T
            } else {
                Value::Nil
            })
        }
        "window-configuration-equal-p" => {
            need_args(name, args, 2)?;
            Ok(if interp.window_configurations_equal(&args[0], &args[1])? {
                Value::T
            } else {
                Value::Nil
            })
        }
        "window-configuration-frame" => {
            need_args(name, args, 1)?;
            if !interp.is_window_configuration_value(&args[0]) {
                return Err(wrong_type_argument(
                    "window-configuration-p",
                    args[0].clone(),
                ));
            }
            Ok(Value::symbol("frame"))
        }
        "force-window-update" => {
            need_arg_range(name, args, 0, 1)?;
            let Some(object) = args.first().filter(|value| !value.is_nil()) else {
                return Ok(Value::T);
            };
            if live_window_id_or_selected(interp, Some(object)).is_ok() {
                return Ok(Value::T);
            }
            let buffer_id = match object {
                Value::Buffer(id, _) if interp.has_buffer_id(*id) => Some(*id),
                _ => string_like(object)
                    .and_then(|string| interp.find_buffer(&string.text).map(|(id, _)| id)),
            };
            Ok(
                if buffer_id.is_some_and(|buffer_id| {
                    live_ordinary_window_ids(interp)
                        .into_iter()
                        .any(|window_id| {
                            window_buffer_id(interp, &Value::Record(window_id)) == Some(buffer_id)
                        })
                }) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "run-window-configuration-change-hook" => {
            need_arg_range(name, args, 0, 1)?;
            if let Some(frame) = args.first()
                && !frame.is_nil()
                && !matches!(frame, Value::Symbol(name) if name == "frame")
            {
                return Err(wrong_type_argument("frame-live-p", frame.clone()));
            }
            let original_window = interp.selected_window_id();
            let original_buffer = interp.current_buffer_id();
            let mut result = Ok(());
            for window_id in live_ordinary_window_ids(interp) {
                let Some(buffer_id) = window_buffer_id(interp, &Value::Record(window_id)) else {
                    continue;
                };
                let Some(local_hooks) =
                    interp.buffer_local_hook(buffer_id, "window-configuration-change-hook")
                else {
                    continue;
                };
                interp.set_selected_window_id(window_id);
                if let Err(error) = interp.set_current_buffer_id(buffer_id) {
                    result = Err(error);
                    break;
                }
                for hook in local_hooks
                    .into_iter()
                    .filter(|hook| !matches!(hook, Value::T))
                {
                    if let Err(error) = call_function_value(interp, &hook, &[], env) {
                        result = Err(error);
                        break;
                    }
                }
                if result.is_err() {
                    break;
                }
            }

            interp.set_selected_window_id(original_window);
            if result.is_ok()
                && let Some(selected_buffer) =
                    window_buffer_id(interp, &Value::Record(original_window))
            {
                result = interp.set_current_buffer_id(selected_buffer);
            }
            if result.is_ok() {
                let default_hooks = interp
                    .default_value("window-configuration-change-hook")
                    .map(|value| value.to_vec().unwrap_or_else(|_| vec![value]))
                    .unwrap_or_default();
                for hook in default_hooks
                    .into_iter()
                    .filter(|hook| !matches!(hook, Value::T))
                {
                    if let Err(error) = call_function_value(interp, &hook, &[], env) {
                        result = Err(error);
                        break;
                    }
                }
            }
            interp.set_selected_window_id(original_window);
            if interp.has_buffer_id(original_buffer) {
                let restore = interp.set_current_buffer_id(original_buffer);
                if result.is_ok() {
                    result = restore;
                }
            }
            result.map(|()| Value::Nil)
        }
        "resize-mini-window-internal" => {
            need_args(name, args, 1)?;
            let window_id = live_window_id_or_selected(interp, Some(&args[0]))?;
            let minibuffer_id = interp
                .lookup_var("emaxx-minibuffer-window", env)
                .and_then(|window| window_record_id_from_value(interp, &window));
            if minibuffer_id != Some(window_id) {
                return Err(LispError::Signal("Not a valid minibuffer window".into()));
            }
            let root_id = window_record_id_from_value(interp, &frame_root_window_value(interp))
                .ok_or_else(|| LispError::Signal("Cannot resize mini window".into()))?;
            let root_geometry = window_geometry(interp, root_id);
            let mini_geometry = window_geometry(interp, window_id);
            let root_requested = window_slot_value(interp, root_id, WINDOW_NEW_PIXEL_SLOT)
                .as_integer()
                .unwrap_or(0);
            let mini_requested = window_slot_value(interp, window_id, WINDOW_NEW_PIXEL_SLOT)
                .as_integer()
                .unwrap_or(0);
            let old_height = root_geometry.1 + mini_geometry.1;
            if root_requested <= 0
                || mini_requested <= 0
                || root_requested.saturating_add(mini_requested) != old_height
            {
                return Err(LispError::Signal("Cannot resize mini window".into()));
            }
            set_window_geometry(
                interp,
                root_id,
                (
                    root_geometry.0,
                    root_requested,
                    root_geometry.2,
                    root_geometry.3,
                ),
            )?;
            set_window_geometry(
                interp,
                window_id,
                (
                    mini_geometry.0,
                    mini_requested,
                    mini_geometry.2,
                    root_geometry.3 + root_requested,
                ),
            )?;
            Ok(Value::T)
        }
        "window-buffer" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            if window_record_id_from_value(interp, &window).is_none() {
                return Err(LispError::TypeError("window".into(), window.type_name()));
            }
            let Some(buffer_id) = window_buffer_id(interp, &window) else {
                return Ok(Value::Nil);
            };
            if buffer_id == interp.current_buffer_id() {
                Ok(Value::Buffer(buffer_id, interp.buffer.name.clone()))
            } else if let Some((_, name)) = interp
                .buffer_list
                .iter()
                .find(|(id, _)| *id == buffer_id)
                .cloned()
            {
                Ok(Value::Buffer(buffer_id, name))
            } else {
                Ok(Value::Nil)
            }
        }
        "window-old-buffer" => {
            need_arg_range(name, args, 0, 1)?;
            let window_id = live_window_id_or_selected(interp, args.first())?;
            Ok(window_slot_value(interp, window_id, WINDOW_OLD_BUFFER_SLOT))
        }
        "window-old-point" => {
            need_arg_range(name, args, 0, 1)?;
            let window_id = live_window_id_or_selected(interp, args.first())?;
            Ok(window_slot_value(interp, window_id, WINDOW_OLD_POINT_SLOT))
        }
        "set-window-buffer" => {
            need_arg_range(name, args, 2, 3)?;
            let window = if args[0].is_nil() {
                interp.selected_window_value()
            } else {
                args[0].clone()
            };
            let Some(window_id) = window_record_id_from_value(interp, &window) else {
                return Err(LispError::TypeError("window".into(), window.type_name()));
            };
            let buffer_id = interp.resolve_buffer_id(&args[1])?;
            let changes_buffer =
                window_buffer_id(interp, &window).is_some_and(|previous| previous != buffer_id);
            if changes_buffer {
                if matches!(
                    window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT),
                    Value::T
                ) {
                    return Err(LispError::Signal(
                        "Window is strongly dedicated to its buffer".into(),
                    ));
                }
                set_window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT, Value::Nil)?;
                if let Ok(record_window_buffer) =
                    interp.lookup_function("record-window-buffer", env)
                {
                    interp.call_function_value(
                        record_window_buffer,
                        Some("record-window-buffer"),
                        std::slice::from_ref(&window),
                        env,
                    )?;
                }
            }
            if window_id == interp.selected_window_id() {
                interp.set_selected_window_buffer_id(buffer_id);
            } else {
                let Some(record) = interp.find_record_mut(window_id) else {
                    return Err(LispError::TypeError("window".into(), window.type_name()));
                };
                if record.slots.len() == WINDOW_BUFFER_SLOT {
                    record.slots.resize(WINDOW_BUFFER_SLOT + 1, Value::Nil);
                }
                record.slots[WINDOW_BUFFER_SLOT] = Value::Integer(buffer_id as i64);
            }
            if changes_buffer {
                let point = interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.point())
                    .unwrap_or(1);
                set_window_slot_value(
                    interp,
                    window_id,
                    WINDOW_OLD_POINT_SLOT,
                    Value::Integer(point as i64),
                )?;
                set_window_slot_value(interp, window_id, WINDOW_HSCROLL_SLOT, Value::Integer(0))?;
                set_window_slot_value(
                    interp,
                    window_id,
                    WINDOW_MIN_HSCROLL_SLOT,
                    Value::Integer(0),
                )?;
                set_window_slot_value(
                    interp,
                    window_id,
                    WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
                    Value::Nil,
                )?;
            }
            Ok(Value::Nil)
        }
        "window-list" | "window-list-1" => {
            need_arg_range(name, args, 0, 3)?;
            Ok(window_list_value(interp, env, args.get(1)))
        }
        "next-window" | "previous-window" => {
            need_arg_range(name, args, 0, 3)?;
            let current = args
                .first()
                .filter(|window| !window.is_nil())
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let current_id = window_id_or_selected(interp, &current)?;
            let mut ids = live_ordinary_window_ids(interp);
            let include_minibuffer = matches!(args.get(1), Some(Value::T));
            if include_minibuffer
                && let Some(Value::Record(minibuffer_id)) =
                    interp.lookup_var("emaxx-minibuffer-window", env)
            {
                ids.push(minibuffer_id);
            }
            if ids.is_empty() {
                return Ok(interp.selected_window_value());
            }
            let index = ids.iter().position(|id| *id == current_id).unwrap_or(0);
            let next = if name == "next-window" {
                (index + 1) % ids.len()
            } else {
                (index + ids.len() - 1) % ids.len()
            };
            Ok(Value::Record(ids[next]))
        }
        "delete-other-windows" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let window_id = window_id_or_selected(interp, &window)?;
            delete_other_windows_from_tree(interp, window_id)?;
            Ok(Value::Nil)
        }
        "frame-root-window" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(frame_root_window_value(interp))
        }
        "frame-first-window" => {
            need_arg_range(name, args, 0, 1)?;
            let mut window = frame_root_window_value(interp);
            while let Value::Record(id) = window {
                let Some(child) = window_link(interp, id, WINDOW_FIRST_CHILD_SLOT) else {
                    return Ok(Value::Record(id));
                };
                window = Value::Record(child);
            }
            Ok(interp.selected_window_value())
        }
        "window-prev-buffers" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(window_slot_value(
                interp,
                window_id,
                WINDOW_PREV_BUFFERS_SLOT,
            ))
        }
        "set-window-prev-buffers" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            set_window_slot_value(interp, window_id, WINDOW_PREV_BUFFERS_SLOT, args[1].clone())
        }
        "window-next-buffers" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(window_slot_value(
                interp,
                window_id,
                WINDOW_NEXT_BUFFERS_SLOT,
            ))
        }
        "set-window-next-buffers" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            set_window_slot_value(interp, window_id, WINDOW_NEXT_BUFFERS_SLOT, args[1].clone())
        }
        "window-parameter" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            Ok(window_parameter_value(interp, window_id, &args[1]))
        }
        "set-window-parameter" => {
            need_args(name, args, 3)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            set_window_parameter_value(interp, window_id, args[1].clone(), args[2].clone())
        }
        "window-parameters" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_PARAMETERS_SLOT))
                .cloned()
                .unwrap_or(Value::Nil))
        }
        "walk-windows" => {
            need_arg_range(name, args, 1, 3)?;
            for window_id in live_ordinary_window_ids(interp) {
                call_function_value(interp, &args[0], &[Value::Record(window_id)], env)?;
            }
            Ok(Value::Nil)
        }
        "make-frame-visible" => {
            need_arg_range(name, args, 0, 1)?;
            decode_live_frame(args.first(), true)
        }
        "make-frame-invisible" => {
            need_arg_range(name, args, 0, 2)?;
            decode_live_frame(args.first(), true)?;
            if !args.get(1).is_some_and(Value::is_truthy) {
                return Err(LispError::Signal(
                    "Attempt to make invisible the sole visible or iconified frame".into(),
                ));
            }
            // The batch frame is a text-terminal frame and therefore remains
            // visible even when FORCE permits this operation.
            Ok(Value::Nil)
        }
        "iconify-frame" => {
            need_arg_range(name, args, 0, 1)?;
            decode_live_frame(args.first(), true)?;
            // Text-terminal frames cannot be iconified.
            Ok(Value::Nil)
        }
        "frame-visible-p" => {
            need_args(name, args, 1)?;
            decode_live_frame(args.first(), false)?;
            Ok(Value::T)
        }
        "visible-frame-list" => {
            need_args(name, args, 0)?;
            Ok(Value::list([Value::Symbol("frame".into())]))
        }
        "selected-frame" | "last-nonminibuffer-frame" => Ok(Value::Symbol("frame".into())),
        "window-frame" => {
            // emaxx has a single frame; any live window belongs to it.
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Symbol("frame".into()))
        }
        "framep" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::Symbol(symbol) if symbol == "frame") {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "frame-live-p" => {
            need_args(name, args, 1)?;
            Ok(
                if matches!(&args[0], Value::Symbol(symbol) if symbol == "frame") {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "frame-terminal" => {
            need_arg_range(name, args, 0, 1)?;
            Ok(Value::Symbol("terminal".into()))
        }
        "frame-list" => Ok(Value::list([Value::Symbol("frame".into())])),
        "face-set-after-frame-default" => {
            need_arg_range(name, args, 1, 2)?;
            Ok(Value::Nil)
        }
        "windowp" | "window-live-p" | "window-valid-p" => {
            need_args(name, args, 1)?;
            let Some(window_id) = window_record_id_from_value(interp, &args[0]) else {
                return Ok(Value::Nil);
            };
            let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
            let valid = !matches!(
                kind,
                Value::Symbol(ref kind) if kind == DELETED_WINDOW_KIND
            );
            let live = valid && window_buffer_id(interp, &args[0]).is_some();
            Ok(
                if name == "windowp" || (name == "window-valid-p" && valid) || live {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "window-parent" | "window-prev-sibling" | "window-next-sibling" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let slot = match name {
                "window-parent" => WINDOW_PARENT_SLOT,
                "window-prev-sibling" => WINDOW_PREV_SIBLING_SLOT,
                _ => WINDOW_NEXT_SIBLING_SLOT,
            };
            Ok(window_link(interp, window_id, slot)
                .map(Value::Record)
                .unwrap_or(Value::Nil))
        }
        "window-top-child" | "window-left-child" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
            let matching_orientation = matches!(
                (name, kind),
                ("window-top-child", Value::Symbol(kind))
                    if kind == INTERNAL_VERTICAL_WINDOW_KIND
            ) || matches!(
                (name, window_slot_value(interp, window_id, WINDOW_KIND_SLOT)),
                ("window-left-child", Value::Symbol(kind))
                    if kind == INTERNAL_HORIZONTAL_WINDOW_KIND
            );
            Ok(if matching_orientation {
                window_link(interp, window_id, WINDOW_FIRST_CHILD_SLOT)
                    .map(Value::Record)
                    .unwrap_or(Value::Nil)
            } else {
                Value::Nil
            })
        }
        "window-combination-limit" | "set-window-combination-limit" => {
            need_args(
                name,
                args,
                if name == "window-combination-limit" {
                    1
                } else {
                    2
                },
            )?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            let kind = window_slot_value(interp, window_id, WINDOW_KIND_SLOT);
            if !matches!(
                kind,
                Value::Symbol(ref kind)
                    if matches!(
                        kind.as_str(),
                        INTERNAL_HORIZONTAL_WINDOW_KIND | INTERNAL_VERTICAL_WINDOW_KIND
                    )
            ) {
                return Err(LispError::Signal(
                    "Combination limit is meaningful for internal windows only".into(),
                ));
            }
            if name == "set-window-combination-limit" {
                set_window_slot_value(
                    interp,
                    window_id,
                    WINDOW_COMBINATION_LIMIT_SLOT,
                    args[1].clone(),
                )
            } else {
                Ok(window_slot_value(
                    interp,
                    window_id,
                    WINDOW_COMBINATION_LIMIT_SLOT,
                ))
            }
        }
        "window-minibuffer-p" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let Some(window_id) = window_record_id_from_value(interp, &window) else {
                return Err(LispError::TypeError("window".into(), window.type_name()));
            };
            let is_minibuffer = interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot, Value::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
            Ok(if is_minibuffer { Value::T } else { Value::Nil })
        }
        "window-at" => {
            need_arg_range(name, args, 2, 3)?;
            Ok(interp.selected_window_value())
        }
        "split-window-internal" => {
            need_args(name, args, 4)?;
            split_window_tree(interp, &args[0], &args[1], &args[2], &args[3])
        }
        "delete-window-internal" => {
            need_args(name, args, 1)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            delete_window_from_tree(interp, window_id)?;
            Ok(Value::Nil)
        }
        "delete-other-windows-internal" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            delete_other_windows_from_tree(interp, window_id)?;
            Ok(Value::Nil)
        }
        "run-window-scroll-functions" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let buffer_id = window_buffer_id(interp, &window)
                .ok_or_else(|| LispError::TypeError("window-live-p".into(), window.type_name()))?;
            let start = Value::Integer(window_start(interp, Some(&window))? as i64);
            let original_buffer = interp.current_buffer_id();
            interp.set_current_buffer_id(buffer_id)?;
            let hooks = hook_values(interp, "window-scroll-functions", env, Some(buffer_id));
            let mut result = Ok(Value::Nil);
            for hook in hooks {
                if let Err(error) =
                    call_function_value(interp, &hook, &[window.clone(), start.clone()], env)
                {
                    result = Err(error);
                    break;
                }
            }
            interp.set_current_buffer_id(original_buffer)?;
            result
        }
        "split-window"
        | "split-window-below"
        | "split-window-vertically"
        | "split-window-right"
        | "split-window-horizontally" => {
            need_arg_range(name, args, 0, 4)?;
            Ok(interp.selected_window_value())
        }
        "window-combined-p" => {
            need_arg_range(name, args, 0, 2)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let Some(parent_id) = window_link(interp, window_id, WINDOW_PARENT_SLOT) else {
                return Ok(Value::Nil);
            };
            let parent_kind = window_slot_value(interp, parent_id, WINDOW_KIND_SLOT);
            let horizontal = args.get(1).is_some_and(Value::is_truthy);
            Ok(
                if matches!(
                    (horizontal, parent_kind),
                    (true, Value::Symbol(kind)) if kind == INTERNAL_HORIZONTAL_WINDOW_KIND
                ) || matches!(
                    (
                        horizontal,
                        window_slot_value(interp, parent_id, WINDOW_KIND_SLOT)
                    ),
                    (false, Value::Symbol(kind)) if kind == INTERNAL_VERTICAL_WINDOW_KIND
                ) {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "window-dedicated-p" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            Ok(window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT))
        }
        "set-window-dedicated-p" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            set_window_slot_value(interp, window_id, WINDOW_DEDICATED_SLOT, args[1].clone())
        }
        "window-splittable-p" => {
            need_arg_range(name, args, 0, 2)?;
            Ok(Value::Nil)
        }
        "window-edges" | "window-pixel-edges" => {
            need_arg_range(name, args, 0, 4)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let (width, height, left, top) = window_geometry(interp, window_id);
            Ok(Value::list([
                Value::Integer(left),
                Value::Integer(top),
                Value::Integer(left + width),
                Value::Integer(top + height),
            ]))
        }
        "window-body-edges"
        | "window-inside-edges"
        | "window-body-pixel-edges"
        | "window-inside-pixel-edges" => {
            need_arg_range(name, args, 0, 4)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let buffer_id = window_buffer_id_or_selected(interp, args.first())?;
            let (width, height, left, top) = window_geometry(interp, window_id);
            let body_height = height - window_non_body_height(interp, buffer_id, env);
            Ok(Value::list([
                Value::Integer(left),
                Value::Integer(top),
                Value::Integer(left + width),
                Value::Integer(top + body_height),
            ]))
        }
        "posn-at-x-y" => {
            need_arg_range(name, args, 2, 4)?;
            let x = args[0].as_integer()?;
            let y = args[1].as_integer()?;
            let pos_y = if y > 0 { y - 1 } else { y };
            let window = args
                .get(2)
                .filter(|value| is_window_value(interp, value))
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            Ok(Value::list([
                window,
                Value::Nil,
                Value::cons(Value::Integer(x), Value::Integer(pos_y)),
                Value::Integer(0),
            ]))
        }
        "posn-at-point" => {
            need_arg_range(name, args, 0, 2)?;
            // Like GNU --batch, Emaxx has no realized glyph matrix from
            // which to derive a screen position.
            Ok(Value::Nil)
        }
        "window-display-table" | "window-cursor-type" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args.first().cloned().unwrap_or(Value::Nil);
            let window_id = window_id_or_selected(interp, &window)?;
            let slot = if name == "window-display-table" {
                WINDOW_DISPLAY_TABLE_SLOT
            } else {
                WINDOW_CURSOR_TYPE_SLOT
            };
            Ok(interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(slot))
                .cloned()
                .unwrap_or(if name == "window-cursor-type" {
                    Value::T
                } else {
                    Value::Nil
                }))
        }
        "set-window-display-table" | "set-window-cursor-type" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            if name == "set-window-cursor-type" && !valid_window_cursor_type(&args[1]) {
                return Err(LispError::Signal("Invalid cursor type".into()));
            }
            let slot = if name == "set-window-display-table" {
                WINDOW_DISPLAY_TABLE_SLOT
            } else {
                WINDOW_CURSOR_TYPE_SLOT
            };
            set_window_slot_value(interp, window_id, slot, args[1].clone())
        }
        "window-fringes" => {
            need_arg_range(name, args, 0, 1)?;
            window_buffer_id_or_selected(interp, args.first())?;
            Ok(Value::list([
                Value::Integer(0),
                Value::Integer(0),
                Value::Nil,
                Value::Nil,
            ]))
        }
        "set-window-fringes" => {
            need_arg_range(name, args, 2, 5)?;
            window_id_or_selected(interp, &args[0])?;
            // GNU's TTY implementation leaves fringe settings unchanged.
            Ok(Value::Nil)
        }
        "window-scroll-bars" => {
            need_arg_range(name, args, 0, 1)?;
            window_buffer_id_or_selected(interp, args.first())?;
            Ok(Value::list([
                Value::Nil,
                Value::Integer(0),
                Value::T,
                Value::Nil,
                Value::Integer(0),
                Value::T,
                Value::Nil,
            ]))
        }
        "set-window-scroll-bars" => {
            need_arg_range(name, args, 1, 6)?;
            window_id_or_selected(interp, &args[0])?;
            // GNU's TTY implementation leaves scroll-bar settings unchanged.
            Ok(Value::Nil)
        }
        "terminal-live-p" => {
            need_args(name, args, 1)?;
            Ok(if !interp.terminal_live() {
                Value::Nil
            } else if matches!(&args[0], Value::Nil) {
                Value::T
            } else if let Value::Symbol(symbol) = &args[0] {
                if symbol == "terminal" || symbol == "frame" {
                    Value::T
                } else {
                    Value::Nil
                }
            } else {
                Value::Nil
            })
        }
        "terminal-list" => {
            need_arg_range(name, args, 0, 0)?;
            Ok(if interp.terminal_live() {
                Value::list([Value::Symbol("terminal".into())])
            } else {
                Value::Nil
            })
        }
        "terminal-name" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Ok(Value::String("initial_terminal".into()))
        }
        "delete-terminal" => {
            need_arg_range(name, args, 0, 2)?;
            if !interp.terminal_live() {
                return Ok(Value::Nil);
            }
            let target = args.first().cloned().unwrap_or(Value::Nil);
            let designates_live_terminal = target.is_nil()
                || matches!(
                    &target,
                    Value::Symbol(symbol) if symbol == "terminal" || symbol == "frame"
                );
            if !designates_live_terminal {
                // decode_terminal returns NULL for an object that does not
                // designate a live terminal; Fdelete_terminal treats that as
                // an already-deleted/no-op target.
                return Ok(Value::Nil);
            }
            if !args.get(1).is_some_and(Value::is_truthy) {
                return Err(LispError::Signal(
                    "Attempt to delete the sole active display terminal".into(),
                ));
            }

            // Fdelete_terminal uses safe_calln here.  An ordinary hook error
            // is reported/demoted and cannot cancel deletion; terminal
            // process control remains non-catchable.
            match super::call(
                interp,
                "run-hook-with-args",
                &[Value::symbol("delete-terminal-functions"), target],
                env,
            ) {
                Ok(_) => {}
                Err(error @ (LispError::Throw(_, _) | LispError::Terminate(_))) => {
                    return Err(error);
                }
                Err(_) => {}
            }
            interp.delete_terminal_state();
            Ok(Value::Nil)
        }
        "terminal-parameters" => {
            need_arg_range(name, args, 0, 1)?;
            require_live_terminal(interp, args.first())?;
            Ok(interp.terminal_parameters())
        }
        "terminal-parameter" => {
            need_args(name, args, 2)?;
            require_live_terminal(interp, args.first())?;
            if !args[1].is_symbol() {
                return Err(wrong_type_argument("symbolp", args[1].clone()));
            }
            Ok(interp.terminal_parameter(&args[1]).unwrap_or(Value::Nil))
        }
        "set-terminal-parameter" => {
            need_args(name, args, 3)?;
            require_live_terminal(interp, args.first())?;
            Ok(interp.set_terminal_parameter(args[1].clone(), args[2].clone()))
        }
        "send-string-to-terminal" => {
            need_arg_range(name, args, 1, 2)?;
            let _ = string_text(&args[0])?;
            Ok(Value::Nil)
        }
        "get-buffer-window" => {
            need_arg_range(name, args, 0, 3)?;
            let buffer_id = if let Some(buffer) = args.first() {
                if buffer.is_nil() {
                    Some(interp.current_buffer_id())
                } else if let Some(string) = string_like(buffer) {
                    interp.find_buffer(&string.text).map(|(id, _)| id)
                } else {
                    Some(interp.resolve_buffer_id(buffer)?)
                }
            } else {
                Some(interp.current_buffer_id())
            };
            Ok(buffer_id
                .and_then(|buffer_id| {
                    live_ordinary_window_ids(interp)
                        .into_iter()
                        .find(|id| window_buffer_id(interp, &Value::Record(*id)) == Some(buffer_id))
                })
                .map(Value::Record)
                .unwrap_or(Value::Nil))
        }
        "minibuffer-window" => Ok(interp
            .lookup_var("emaxx-minibuffer-window", env)
            .unwrap_or_else(|| interp.selected_window_value())),
        "set-minibuffer-window" => {
            need_args(name, args, 1)?;
            let Some(window_id) = window_record_id_from_value(interp, &args[0]) else {
                return Err(wrong_type_argument("windowp", args[0].clone()));
            };
            let is_minibuffer = interp
                .find_record(window_id)
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot, Value::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
            if !is_minibuffer {
                return Err(LispError::Signal(
                    "Window is not a minibuffer window".into(),
                ));
            }
            interp.set_global_binding("emaxx-minibuffer-window", args[0].clone());
            Ok(args[0].clone())
        }
        "minibuffer-selected-window" | "get-mru-window" => Ok(interp
            .lookup_var("emaxx-minibuffer-selected-window", env)
            .filter(|value| !value.is_nil())
            .unwrap_or_else(|| interp.selected_window_value())),
        "minibuffer-window-active-p" => {
            need_arg_range(name, args, 0, 1)?;
            let window = args
                .first()
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let is_minibuffer = window_record_id_from_value(interp, &window)
                .and_then(|id| interp.find_record(id))
                .and_then(|record| record.slots.get(WINDOW_KIND_SLOT))
                .is_some_and(
                    |slot| matches!(slot, Value::Symbol(kind) if kind == MINIBUFFER_WINDOW_KIND),
                );
            Ok(if is_minibuffer { Value::T } else { Value::Nil })
        }
        "get-buffer-window-list" => {
            need_arg_range(name, args, 0, 4)?;
            let buffer_id = if let Some(buffer) = args.first() {
                if buffer.is_nil() {
                    interp.current_buffer_id()
                } else {
                    interp.resolve_buffer_id(buffer)?
                }
            } else {
                interp.current_buffer_id()
            };
            Ok(Value::list(
                live_ordinary_window_ids(interp)
                    .into_iter()
                    .filter(|id| window_buffer_id(interp, &Value::Record(*id)) == Some(buffer_id))
                    .map(Value::Record),
            ))
        }
        "buffer-match-p" => {
            need_args(name, args, 2)?;
            Ok(
                if buffer_match_condition(interp, &args[0], &args[1], &args[2..], env)? {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "display-buffer" => {
            need_arg_range(name, args, 1, 2)?;
            let buffer_id = if let Some(name) = string_like(&args[0]).map(|string| string.text) {
                interp
                    .find_buffer(&name)
                    .map(|(id, _)| id)
                    .unwrap_or_else(|| interp.create_buffer(&name).0)
            } else {
                interp.resolve_buffer_id(&args[0])?
            };
            let buffer_name = if buffer_id == interp.current_buffer_id() {
                interp.buffer.name.clone()
            } else {
                interp
                    .buffer_list
                    .iter()
                    .find(|(id, _)| *id == buffer_id)
                    .map(|(_, name)| name.clone())
                    .unwrap_or_else(|| interp.buffer.name.clone())
            };
            let buffer = Value::Buffer(buffer_id, buffer_name);
            let action = args.get(1).cloned().unwrap_or(Value::Nil);
            let mut actions = Vec::new();
            if let Some(user_action) =
                matching_display_buffer_action(interp, &buffer, &action, env)?
            {
                actions.push(user_action);
            }
            actions.push(action);
            let mut functions = Vec::new();
            let mut alist = Vec::new();
            for action in actions {
                let (action_functions, action_alist) = display_action_parts(interp, &action, env);
                functions.extend(action_functions);
                alist.extend(action_alist);
            }
            let action_alist = Value::list(alist);
            for function in functions {
                let result = interp.call_function_value(
                    function.clone(),
                    function.as_symbol().ok(),
                    &[buffer.clone(), action_alist.clone()],
                    env,
                )?;
                if is_window_value(interp, &result) {
                    return Ok(result);
                }
            }
            if display_action_inhibits_same_window(&action_alist) {
                return Ok(Value::Nil);
            }
            interp.set_selected_window_buffer_id(buffer_id);
            Ok(interp.selected_window_value())
        }
        "quit-window" => {
            // GNU owns quit-window policy in preloaded window.el.  Keep this
            // compact fallback only for file-less bootstrap interpreters.
            if interp.has_lisp_function(name)
                && let Some(function) = interp.logical_function_binding(name, env)
                && !matches!(&function, Value::BuiltinFunc(builtin) if builtin == name)
            {
                return interp.call_function_value(function, Some(name), args, env);
            }
            need_arg_range(name, args, 0, 2)?;
            let kill = args.first().is_some_and(Value::is_truthy);
            let window = args
                .get(1)
                .cloned()
                .unwrap_or_else(|| interp.selected_window_value());
            let buffer_id = window_buffer_id(interp, &window)
                .ok_or_else(|| LispError::TypeError("window".into(), window.type_name()))?;
            run_named_hooks(interp, "quit-window-hook", env, Some(buffer_id))?;
            if kill {
                interp.kill_buffer_id(buffer_id);
                return Ok(Value::Nil);
            }
            if buffer_id == interp.current_buffer_id() {
                if let Some(index) = interp
                    .buffer_list
                    .iter()
                    .position(|(id, _)| *id == buffer_id)
                {
                    let entry = interp.buffer_list.remove(index);
                    interp.buffer_list.push(entry);
                }
                let next = interp
                    .selected_window_previous_buffer_id()
                    .filter(|id| *id != buffer_id)
                    .or_else(|| {
                        interp
                            .buffer_list
                            .iter()
                            .find(|(id, _)| *id != buffer_id)
                            .map(|(id, _)| *id)
                    });
                if let Some(next_id) = next {
                    interp.switch_to_buffer_id_preserving_window_history(next_id)?;
                }
            }
            Ok(Value::Nil)
        }
        "active-minibuffer-window" => {
            // Non-nil while a minibuffer-with-setup-hook hook runs (the
            // approximation of GNU's activated minibuffer).
            if interp
                .lookup_var("emaxx--active-minibuffer", env)
                .is_some_and(|value| value.is_truthy())
            {
                Ok(interp.selected_window_value())
            } else {
                Ok(Value::Nil)
            }
        }
        "set-window-start" => {
            need_arg_range(name, args, 2, 4)?;
            let start = position_from_value(interp, &args[1])?;
            set_window_start_value(interp, &args[0], start)?;
            Ok(Value::T)
        }
        "set-window-point" => {
            need_args(name, args, 2)?;
            let window_id = window_id_or_selected(interp, &args[0])?;
            let pos = position_from_value(interp, &args[1])?;
            // GNU: setting the selected window's point moves point in the
            // window's buffer (emaxx has no separate window points).
            if window_id == interp.selected_window_id() {
                let buffer_id = window_buffer_id(interp, &Value::Record(window_id))
                    .unwrap_or_else(|| interp.current_buffer_id());
                if buffer_id == interp.current_buffer_id() {
                    interp.buffer.goto_char(pos);
                } else if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                    buffer.goto_char(pos);
                }
            }
            Ok(args[1].clone())
        }
        "set-window-vscroll" => {
            need_arg_range(name, args, 2, 4)?;
            let _ = args[1].as_integer()?;
            Ok(Value::Integer(0))
        }
        "facemenu-add-face" => {
            need_args(name, args, 3)?;
            let face = args[0].clone();
            let start = position_from_value(interp, &args[1])?;
            let end = position_from_value(interp, &args[2])?;
            interp.buffer.put_text_property(start, end, "face", face);
            Ok(Value::Nil)
        }

        _ => unreachable!("dispatch chunk called for unsupported primitive"),
    }
}

fn font_lock_raw_keyword_specs(current: Option<Value>) -> Vec<Value> {
    let items = current.unwrap_or(Value::Nil).to_vec().unwrap_or_default();
    if items.first() != Some(&Value::T) {
        return items;
    }
    items
        .get(1)
        .and_then(|specs| specs.to_vec().ok())
        .unwrap_or_default()
}

fn font_lock_keywords_value(raw_specs: &[Value]) -> Value {
    let mut items = vec![Value::T, Value::list(raw_specs.iter().cloned())];
    items.extend(raw_specs.iter().filter_map(font_lock_compiled_keyword_spec));
    Value::list(items)
}

fn font_lock_compiled_keyword_spec(spec: &Value) -> Option<Value> {
    let parts = spec.to_vec().ok()?;
    if parts.len() < 3 {
        return None;
    }
    Some(Value::list([
        parts[0].clone(),
        Value::list([parts[1].clone(), parts[2].clone()]),
    ]))
}

fn append_to_warnings_buffer(interp: &mut Interpreter, warning: &str) {
    append_to_named_warnings_buffer(interp, "*Warnings*", warning);
}

fn append_to_named_warnings_buffer(interp: &mut Interpreter, buffer_name: &str, warning: &str) {
    let buffer_id = interp
        .find_buffer(buffer_name)
        .map(|(id, _)| id)
        .unwrap_or_else(|| interp.create_buffer(buffer_name).0);
    if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
        let end = buffer.point_max();
        buffer.goto_char(end);
        buffer.insert(&(warning.to_string() + "\n"));
    }
}

// GNU font-core's `font-lock-mode' body runs the buffer's
// `font-lock-function' with the new mode value; modes like ERT's results
// buffer install a redraw hook there.
fn font_lock_mode_run_mode_function(
    interp: &mut Interpreter,
    buffer_id: u64,
    mode: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some(function) = interp.buffer_local_value(buffer_id, "font-lock-function") else {
        return Ok(());
    };
    if matches!(&function, Value::Symbol(name) if name == "font-lock-default-function")
        || function.is_nil()
    {
        return Ok(());
    }
    crate::lisp::primitives::call_function_value(interp, &function, &[mode], env)?;
    Ok(())
}
