//! xdisp.c's handling of long lines: redisplay_window's detection of a
//! buffer holding a line longer than `long-line-threshold', which sets
//! the buffer's long_line_optimizations_p for good.

use crate::lisp::cached_symbol;
use crate::lisp::eval::Interpreter;
use crate::lisp::types::Kind;

/// redisplay_window's "Check whether the buffer to be displayed contains
/// long lines": with `long-line-threshold' non-nil (its value in the
/// buffer), a buffer not yet known to hold long lines, and its text
/// changed enough since redisplay last took its counts (CHARS_MODIFF -
/// UNCHANGED_MODIFIED > 8) or its restriction changed, the accessible
/// text is scanned line by line.
pub(super) fn note_long_lines(interp: &mut Interpreter, buffer_id: u64) {
    let key = cached_symbol!("long-line-threshold");
    let threshold = interp
        .buffer_local_value_key(buffer_id, key)
        .or_else(|| interp.default_value_key(key));
    let threshold = match threshold.map(|value| value.kind()) {
        // XFIXNUM (Vlong_line_threshold); the variable holds an integer
        // or nil.
        Some(Kind::Integer(threshold)) => threshold,
        _ => return,
    };
    let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) else {
        return;
    };
    if buffer.long_line_optimizations_p()
        || !(buffer.chars_modified_tick() - buffer.unchanged_modified() > 8
            || buffer.clip_changed())
    {
        return;
    }
    if buffer.has_line_longer_than(threshold) {
        buffer.set_long_line_optimizations_p(true);
    }
}
