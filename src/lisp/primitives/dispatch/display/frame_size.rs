//! A text frame's size: frame.c's adjust_frame_size, set_menu_bar_lines
//! and set_tab_bar_lines, dispnew.c's change_frame_size and
//! do_pending_window_change, and window.c's resize_frame_windows and
//! window-resize-apply-total.  A text frame's character cell is one
//! pixel and it has no internal border, fringes or scroll bars, so its
//! text, inner and native sizes differ only by the bar lines above the
//! window tree (FRAME_TOP_MARGIN).

use super::*;
use crate::lisp::types::{Env, FrameRef};

fn lisp_bool(flag: bool) -> Value {
    if flag { Value::T } else { Value::Nil }
}

/// frame.h's FRAME_TOP_MARGIN (and FRAME_TOP_MARGIN_HEIGHT, FRAME_MARGINS
/// and FRAME_MARGIN_HEIGHT): the menu and tab bar lines.
pub(crate) fn frame_top_margin(frame: FrameRef) -> i64 {
    let frame = frame.borrow();
    frame.menu_bar_lines + frame.tab_bar_lines
}

/// FRAME's minibuffer window when it shows one below its root window
/// (FRAME_HAS_MINIBUF_P and not FRAME_MINIBUF_ONLY_P).
fn frame_mini_window(interp: &Interpreter, frame: FrameRef) -> Option<u64> {
    let (root, mini) = {
        let frame = frame.borrow();
        (frame.root_window_id(), frame.minibuffer_window_id())
    };
    (mini != root && interp.window_frame_id(mini) == Some(frame)).then_some(mini)
}

/// frame.c:set_menu_bar_lines: a text frame's menu bar takes VALUE's
/// lines (0 unless a fixnum), and the frame is resized to keep its
/// native size at the next do_pending_window_change.
pub(crate) fn set_menu_bar_lines(interp: &mut Interpreter, frame: FrameRef, value: Value) {
    set_bar_lines(interp, frame, value, true);
}

/// frame.c:set_tab_bar_lines, as set_menu_bar_lines for the tab bar.
pub(crate) fn set_tab_bar_lines(interp: &mut Interpreter, frame: FrameRef, value: Value) {
    set_bar_lines(interp, frame, value, false);
}

fn set_bar_lines(interp: &mut Interpreter, frame: FrameRef, value: Value, menu: bool) {
    let nlines = value
        .integer_value()
        .filter(|lines| i32::try_from(*lines).is_ok())
        .unwrap_or(0);
    let minibuffer_only = {
        let frame = frame.borrow();
        frame.root_window_id() == frame.minibuffer_window_id()
    };
    let olines = if menu {
        frame.borrow().menu_bar_lines
    } else {
        frame.borrow().tab_bar_lines
    };
    if minibuffer_only || nlines == olines {
        return;
    }
    interp.redisplay_other_windows();
    {
        let mut state = frame.borrow_mut();
        if menu {
            state.menu_bar_lines = nlines;
        } else {
            state.tab_bar_lines = nlines;
        }
    }
    let (width, height) = {
        let state = frame.borrow();
        (state.width, state.height)
    };
    // change_frame_size (f, FRAME_PIXEL_WIDTH (f), FRAME_PIXEL_HEIGHT (f),
    // false, true, false): DELAY queues the size for later.
    queue_frame_size(frame, width, height);
}

fn queue_frame_size(frame: FrameRef, width: i64, height: i64) {
    let mut state = frame.borrow_mut();
    state.new_width = width;
    state.new_height = height;
    state.new_size_p = true;
}

/// dispnew.c:change_frame_size: FRAME's native size becomes WIDTH x
/// HEIGHT, now, or with DELAY at the next do_pending_window_change.
/// Interpreter states are never mid-redisplay here, so SAFE only
/// matters to the caller's choice of DELAY.
pub(crate) fn change_frame_size(
    interp: &mut Interpreter,
    frame: FrameRef,
    width: i64,
    height: i64,
    pretend: bool,
    delay: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    if delay {
        queue_frame_size(frame, width, height);
        return Ok(());
    }
    {
        let mut state = frame.borrow_mut();
        state.new_width = -1;
        state.new_height = -1;
        state.new_size_p = false;
    }
    // adjust_frame_size takes text sizes: the native ones less the bars.
    let margin = frame_top_margin(frame);
    adjust_frame_size(interp, frame, width, height - margin, 5, pretend, env)
}

/// dispnew.c:do_pending_window_change: apply the sizes change_frame_size
/// queued for any frame.
pub(crate) fn do_pending_window_change(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<(), LispError> {
    loop {
        let pending = interp.frame_states.iter().copied().find(|frame| {
            frame.is_live() && {
                let state = frame.borrow();
                state.new_size_p && (state.new_height >= 0 || state.new_width >= 0)
            }
        });
        let Some(frame) = pending else {
            return Ok(());
        };
        let (width, height) = {
            let state = frame.borrow();
            (state.new_width, state.new_height)
        };
        change_frame_size(interp, frame, width, height, false, false, env)?;
    }
}

/// frame.c:frame_windows_min_size: FRAME's minimum inner size, from its
/// `min-width' or `min-height' parameter or `frame-windows-min-size',
/// never so few lines on a text terminal that the bars, a mode line, a
/// text line and the echo area do not fit.
fn frame_windows_min_size(
    interp: &mut Interpreter,
    frame: FrameRef,
    horizontal: bool,
    ignore: Value,
    env: &mut Env,
) -> Result<i64, LispError> {
    let parameter = super::super::frames::frame_parameter_value(
        interp,
        frame,
        if horizontal {
            "min-width"
        } else {
            "min-height"
        },
    );
    let size = match parameter.integer_value() {
        Some(size) if i32::try_from(size).is_ok() => size.max(1),
        _ => crate::lisp::primitives::hooks_overlays::call_named_function(
            interp,
            "frame-windows-min-size",
            &[Value::Frame(frame), lisp_bool(horizontal), ignore, Value::T],
            env,
        )?
        .as_integer()?,
    };
    let termcap = frame.borrow().tty_sized;
    if termcap && !horizontal {
        let wants_modeline =
            super::super::frames::frame_parameter_value(interp, frame, "modeline").is_truthy();
        let min_height = frame_top_margin(frame) + i64::from(wants_modeline) + 2;
        return Ok(size.max(min_height));
    }
    Ok(size)
}

/// frame.c:adjust_frame_size for a text frame: FRAME's text area becomes
/// NEW_TEXT_WIDTH x NEW_TEXT_HEIGHT (-1 keeps one), never less than its
/// windows' minimum sizes; the window tree is resized to the new inner
/// size and placed below the bars.  INHIBIT is 5 for change_frame_size
/// (the size the terminal has), 1 for the set-frame-size family.
pub(crate) fn adjust_frame_size(
    interp: &mut Interpreter,
    frame: FrameRef,
    new_text_width: i64,
    new_text_height: i64,
    inhibit: u8,
    pretend: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    let _ = pretend;
    let (old_native_width, old_native_height, old_text_cols, old_text_lines, root) = {
        let state = frame.borrow();
        (
            state.width,
            state.height,
            state.width,
            state.text_height,
            state.root_window_id(),
        )
    };
    let mini = frame_mini_window(interp, frame);
    let old_inner_width = window_geometry(interp, root).0;
    let old_inner_height =
        window_geometry(interp, root).1 + mini.map_or(0, |mini| window_geometry(interp, mini).1);
    let ignore = if inhibit == 5 {
        Value::symbol("safe")
    } else {
        Value::Nil
    };
    let min_inner_width = frame_windows_min_size(interp, frame, true, ignore, env)?;
    let min_inner_height = frame_windows_min_size(interp, frame, false, ignore, env)?;
    let margin = frame_top_margin(frame);
    let new_text_width = if new_text_width == -1 {
        old_text_cols
    } else {
        new_text_width
    };
    let new_text_height = if new_text_height == -1 {
        old_text_lines
    } else {
        new_text_height
    };
    // INHIBIT 4 (set-window-configuration's) keeps the frame's size while
    // its windows' minimum sizes fit; 1 and 5 always resize (2 and 3,
    // the window systems' implied resizes, never reach a text frame).
    let (inhibit_horizontal, inhibit_vertical) = if inhibit == 4 {
        (
            old_native_width >= min_inner_width,
            old_native_height - margin >= min_inner_height,
        )
    } else {
        (inhibit == 5, inhibit == 5)
    };
    let new_native_width = if inhibit_horizontal && inhibit < 5 {
        old_native_width
    } else {
        new_text_width.max(min_inner_width)
    };
    let new_inner_width = new_native_width;
    let new_text_cols = new_native_width;
    let new_native_height = if inhibit_vertical && inhibit < 5 {
        old_native_height
    } else {
        (new_text_height + margin).max(min_inner_height + margin)
    };
    let new_inner_height = new_native_height - margin;
    let new_text_lines = new_native_height - margin;

    let root_top = window_geometry(interp, root).3;
    if root_top == margin
        && new_text_cols == old_text_cols
        && new_text_lines == old_text_lines
        && new_inner_width == old_inner_width
        && new_inner_height == old_inner_height
        && new_native_width == old_native_width
        && new_native_height == old_native_height
    {
        return Ok(());
    }

    if new_inner_width != old_inner_width {
        resize_frame_windows(interp, frame, new_inner_width, true, env)?;
    } else if new_text_cols != old_text_cols {
        window_pixel_to_total(interp, frame, true, env)?;
    }
    if new_inner_height != old_inner_height || window_geometry(interp, root).3 != margin {
        resize_frame_windows(interp, frame, new_inner_height, false, env)?;
    } else if new_text_lines != old_text_lines {
        window_pixel_to_total(interp, frame, false, env)?;
    }

    {
        let mut state = frame.borrow_mut();
        state.width = new_text_cols;
        state.text_height = new_text_lines;
        state.height = new_native_height;
    }
    // SET_FRAME_GARBAGED.
    interp.redisplay_other_windows();
    Ok(())
}

/// window.c:window_pixel_to_total: `window--pixel-to-total' assigns
/// FRAME's windows total sizes from their pixel sizes.
fn window_pixel_to_total(
    interp: &mut Interpreter,
    frame: FrameRef,
    horizontal: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    crate::lisp::primitives::hooks_overlays::call_named_function(
        interp,
        "window--pixel-to-total",
        &[Value::Frame(frame), lisp_bool(horizontal)],
        env,
    )
    .map(|_| ())
}

/// window.c:resize_root_window: `window--resize-root-window' stages
/// WINDOW's subtree for a DELTA-pixel resize.
fn resize_root_window(
    interp: &mut Interpreter,
    window: u64,
    delta: i64,
    horizontal: bool,
    ignore: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    crate::lisp::primitives::hooks_overlays::call_named_function(
        interp,
        "window--resize-root-window",
        &[
            interp.record_value(window),
            Value::Integer(delta),
            lisp_bool(horizontal),
            lisp_bool(ignore),
            Value::T,
        ],
        env,
    )
    .map(|_| ())
}

/// window.c:resize_frame_windows: FRAME's inner height (width with
/// HORFLAG) became SIZE: the root window takes it less a one-line mini
/// window, below the bars, its subtree resized as `window--resize-root-
/// window' proposes when that adds up.
fn resize_frame_windows(
    interp: &mut Interpreter,
    frame: FrameRef,
    size: i64,
    horflag: bool,
    env: &mut Env,
) -> Result<(), LispError> {
    let root = frame.borrow().root_window_id();
    let old_pixel_size = window_pixel_size(interp, root, horflag);
    let mini = frame_mini_window(interp, frame);
    // unit + m->pixel_height - window_body_height (m): a mini window has
    // no mode, header or tab line, so one line.
    let mini_height = if mini.is_some() { 1 } else { 0 };
    let margin = frame_top_margin(frame);
    let new_pixel_size = (if horflag { size } else { size - mini_height }).max(1);
    let new_size = new_pixel_size;

    let (width, height, left, top) = window_geometry(interp, root);
    if new_pixel_size == old_pixel_size && (horflag || top == margin) {
    } else if window_combination(interp, root).is_none() {
        if horflag {
            set_window_char_slot(interp, root, WINDOW_TOTAL_COLS_SLOT, new_size);
            set_window_geometry(interp, root, (new_pixel_size, height, left, top))?;
        } else {
            set_window_char_slot(interp, root, WINDOW_TOP_LINE_SLOT, margin);
            set_window_char_slot(interp, root, WINDOW_TOTAL_LINES_SLOT, new_size);
            set_window_geometry(interp, root, (width, new_pixel_size, left, margin))?;
        }
    } else {
        if !horflag {
            set_window_char_slot(interp, root, WINDOW_TOP_LINE_SLOT, margin);
            set_window_geometry(interp, root, (width, height, left, margin))?;
        }
        let delta = new_pixel_size - old_pixel_size;
        for ignore in [false, true] {
            resize_root_window(interp, root, delta, horflag, ignore, env)?;
            if window_resize_check(interp, root, horflag)
                && new_pixel_size == window_new_pixel(interp, root)
            {
                window_resize_apply(interp, root, horflag)?;
                window_pixel_to_total(interp, frame, horflag, env)?;
                break;
            }
        }
    }

    if let Some(mini) = mini {
        let (mini_width, mini_pixel_height, mini_left, mini_top) = window_geometry(interp, mini);
        if horflag {
            set_window_char_slot(interp, mini, WINDOW_TOTAL_COLS_SLOT, new_size);
            set_window_geometry(
                interp,
                mini,
                (new_pixel_size, mini_pixel_height, mini_left, mini_top),
            )?;
        } else {
            let (_, root_height, _, root_top) = window_geometry(interp, root);
            let (_, root_lines, _, root_top_line) = window_char_geometry(interp, root);
            set_window_char_slot(interp, mini, WINDOW_TOTAL_LINES_SLOT, mini_height);
            set_window_char_slot(
                interp,
                mini,
                WINDOW_TOP_LINE_SLOT,
                root_top_line + root_lines,
            );
            set_window_geometry(
                interp,
                mini,
                (mini_width, mini_height, mini_left, root_top + root_height),
            )?;
        }
    }
    // fset_redisplay (f).
    interp.redisplay_other_windows();
    Ok(())
}

/// window.c:window_resize_apply_total: W's total size (width with
/// HORFLAG) becomes its new total, its children placed from its column
/// or line, advancing across an iso-combination.
fn window_resize_apply_total(interp: &mut Interpreter, window: u64, horflag: bool) {
    let new_total = window_slot_value(interp, window, WINDOW_NEW_TOTAL_SLOT)
        .integer_value()
        .unwrap_or(0);
    let (_, _, left_col, top_line) = window_char_geometry(interp, window);
    let mut edge = if horflag {
        set_window_char_slot(interp, window, WINDOW_TOTAL_COLS_SLOT, new_total);
        left_col
    } else {
        set_window_char_slot(interp, window, WINDOW_TOTAL_LINES_SLOT, new_total);
        top_line
    };
    let Some(horizontal) = window_combination(interp, window) else {
        return;
    };
    let mut child = window_link(interp, window, WINDOW_FIRST_CHILD_SLOT);
    while let Some(child_id) = child {
        set_window_char_slot(
            interp,
            child_id,
            if horflag {
                WINDOW_LEFT_COL_SLOT
            } else {
                WINDOW_TOP_LINE_SLOT
            },
            edge,
        );
        window_resize_apply_total(interp, child_id, horflag);
        if horizontal == horflag {
            let (cols, lines, _, _) = window_char_geometry(interp, child_id);
            edge += if horflag { cols } else { lines };
        }
        child = window_link(interp, child_id, WINDOW_NEXT_SIBLING_SLOT);
    }
}

/// window.c:Fwindow_resize_apply_total: FRAME's windows take their new
/// total sizes, the root window at column 0 below the bars and the mini
/// window below it.
pub(super) fn window_resize_apply_total_for_frame(
    interp: &mut Interpreter,
    frame: FrameRef,
    horizontal: bool,
) {
    let root = frame.borrow().root_window_id();
    // Necessary when deleting the top-/or leftmost window.
    set_window_char_slot(interp, root, WINDOW_LEFT_COL_SLOT, 0);
    set_window_char_slot(interp, root, WINDOW_TOP_LINE_SLOT, frame_top_margin(frame));
    window_resize_apply_total(interp, root, horizontal);
    if let Some(mini) = frame_mini_window(interp, frame) {
        let new_total = window_slot_value(interp, mini, WINDOW_NEW_TOTAL_SLOT)
            .integer_value()
            .unwrap_or(0);
        if horizontal {
            set_window_char_slot(interp, mini, WINDOW_TOTAL_COLS_SLOT, new_total);
        } else {
            let (_, root_lines, _, root_top_line) = window_char_geometry(interp, root);
            set_window_char_slot(
                interp,
                mini,
                WINDOW_TOP_LINE_SLOT,
                root_top_line + root_lines,
            );
            set_window_char_slot(interp, mini, WINDOW_TOTAL_LINES_SLOT, new_total);
        }
    }
}
