//! Terminal frontend: the interactive command loop over the Lisp runtime.
//!
//! This is GNU's keyboard.c/dispnew.c layer in miniature: read terminal
//! events, translate them to Lisp key events, resolve the pending key
//! sequence through the runtime's own keymaps (`key-binding'), execute the
//! binding with `command-execute', and redraw the frame from the runtime's
//! buffer state.  Commands are Lisp — nothing here dispatches editing by
//! name, so anything the runtime can `command-execute' works from a key.
//!
//! Redisplay is deliberately minimal for now: a full redraw of the selected
//! buffer's visible window, a mode line, and the echo area.  Incremental
//! matrices, faces on glass, and multiple live windows layer on top of this
//! loop without changing its shape.

use std::io::{self, Write};

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::{cursor, event, execute, queue, style, terminal};

use crate::batch;
use crate::lisp::eval::Interpreter;
use crate::lisp::primitives::{
    InvisibilitySpec, invisible_class_at, invisible_run_at, resolve_buffer_invisibility,
    visual_line_first_line,
};
use crate::lisp::types::{Env, Kind, LispError, LispErrorKind, Value};

mod layout;
mod redisplay_window;

/// Append diagnostics to `EMAXX_TTY_LOG' when set; raw-mode sessions have
/// no usable stderr, so a file is the only trace channel.
fn debug_log(message: &str) {
    if let Some(path) = std::env::var_os("EMAXX_TTY_LOG")
        && let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
    {
        let _ = writeln!(file, "{message}");
    }
}

/// Restore the terminal even when the session ends by panic or error.
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        terminal::enable_raw_mode()?;
        execute!(io::stdout(), terminal::EnterAlternateScreen, cursor::Show)?;
        crate::lisp::eval::terminal::activate_frame_route();
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        crate::lisp::eval::terminal::clear_frame_route();
        let _ = execute!(io::stdout(), terminal::LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
    }
}

/// One window's display anchor, the frontend's half of GNU's
/// window-start contract.
#[derive(Clone, Copy, Default)]
struct WindowView {
    /// 1-based buffer line shown on the window's first text row.
    top_line: usize,
    /// Continuation segment of `top_line' shown first: a window can start
    /// mid-line when long lines wrap, like GNU's window-start position.
    top_seg: usize,
    /// The start marker, accessible region and buffer modification count
    /// of the window's last display: with point unmoved and none of them
    /// changed, try_cursor_movement keeps the display as it is.
    last_start: Option<usize>,
    last_restriction: Option<(usize, usize)>,
    last_modiff: Option<crate::buffer::ModCount>,
    /// Redisplay state from the preceding frame.  GNU can leave point-based
    /// mode-line constructs on their preceding value after point moves inside
    /// invisible text, until the next input invalidates that incremental
    /// display state.  These fields identify that narrow transition without
    /// caching unrelated mode-line state.
    last_buffer_id: Option<u64>,
    last_point: Option<usize>,
    last_cursor: Option<(usize, usize)>,
    last_top_pos: Option<usize>,
    last_chars_modiff: Option<crate::buffer::ModCount>,
    /// The point GNU's incremental mode-line matrix keeps displaying until
    /// the next input event after a folded same-row motion.
    deferred_mode_line_point: Option<usize>,
}

/// The terminal's color capability, GNU's terminfo Co# in miniature:
/// xterm-family terminals answer 8 colors, 256-color variants 256, and
/// anything unrecognizably dumb stays colorless.
fn terminal_color_cells() -> i64 {
    let term = std::env::var("TERM").unwrap_or_default();
    if term.is_empty() || term == "dumb" {
        0
    } else if term.contains("256color") {
        256
    } else {
        8
    }
}

fn terminal_output() -> Box<dyn Write> {
    struct FailedOutput(io::Error);
    impl Write for FailedOutput {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(self.0.kind(), self.0.to_string()))
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(self.0.kind(), self.0.to_string()))
        }
    }
    crate::lisp::eval::terminal::output().unwrap_or_else(|error| Box::new(FailedOutput(error)))
}

type CellAttrs = crate::lisp::primitives::TtyFaceAttrs;

/// A face layered over a base: attributes the face leaves unspecified
/// keep the base's — GNU's face merging for spans drawn inside an
/// already-faced area (the mode line's buffer name over the mode line).
fn merge_cell_attrs(base: CellAttrs, over: CellAttrs) -> CellAttrs {
    CellAttrs {
        foreground: over.foreground.or(base.foreground),
        background: over.background.or(base.background),
        bold: base.bold || over.bold,
        underline: base.underline || over.underline,
        reverse: base.reverse || over.reverse,
        extend: base.extend || over.extend,
    }
}

/// A padding glyph: a cell after the first of a wide character (term.c
/// append_glyph's padding_p glyphs).  The terminal draws the character
/// across it, so it is never printed.
const PADDING_GLYPH: char = '\0';
/// In display text, the character after this marker draws in the cell
/// before it: a zero-width character the terminal composes with that
/// cell's (a tty composition's components, compose-gstring-for-terminal).
/// Control characters never reach display text untranslated, so neither
/// marker can be a real glyph.
const ATTACHED_GLYPH: char = '\u{1}';

/// The cell each display offset of TEXT begins at, one past the end
/// included: a padding glyph is a cell, an attached character and its
/// marker take none.
fn display_cells(text: &str) -> Vec<usize> {
    let mut cells = Vec::with_capacity(text.len() + 1);
    let mut cell = 0usize;
    let mut attaching = false;
    for character in text.chars() {
        cells.push(cell);
        if character == ATTACHED_GLYPH {
            attaching = true;
        } else if attaching {
            attaching = false;
        } else {
            cell += 1;
        }
    }
    cells.push(cell);
    cells
}

/// The terminal cells display text TEXT takes.
fn cell_count(text: &str) -> usize {
    let mut cells = 0usize;
    let mut attaching = false;
    for character in text.chars() {
        if character == ATTACHED_GLYPH {
            attaching = true;
        } else if attaching {
            attaching = false;
        } else {
            cells += 1;
        }
    }
    cells
}

/// The first CELLS cells of display text TEXT, as display_string ends a
/// row: a wide character whose padding would not fit is left out and
/// PAD fills its cells (pad_mode_line: `-' in a mode line, a space in a
/// header or tab line).  Also the cells kept before that fill.
fn take_cells_padded(text: &str, cells: usize, pad: char) -> (String, usize) {
    let chars: Vec<char> = text.chars().collect();
    let starts = display_cells(text);
    let mut end = starts
        .partition_point(|&cell| cell < cells)
        .min(chars.len());
    // Keep the characters attached to the last kept cell.
    while end < chars.len() && starts[end] == cells && chars[end] == ATTACHED_GLYPH {
        end += 2;
    }
    let mut kept: String = chars[..end.min(chars.len())].iter().collect();
    if end < chars.len() && chars[end] == PADDING_GLYPH {
        // The wide character starting the last kept run loses its padding.
        let mut start = end;
        while start > 0 && chars[start - 1] == PADDING_GLYPH {
            start -= 1;
        }
        let start = start.saturating_sub(1);
        kept = chars[..start].iter().collect();
        kept.extend(std::iter::repeat_n(pad, end - start));
        return (kept, starts[start]);
    }
    (kept, cells)
}

fn take_cells(text: &str, cells: usize) -> String {
    take_cells_padded(text, cells, ' ').0
}

/// One painted terminal row: its characters and each cell's face
/// attributes.  Two rows compare equal exactly when the glass would look
/// identical.
#[derive(Clone, Debug, PartialEq)]
struct PaintRow {
    text: Vec<char>,
    attrs: Vec<CellAttrs>,
    /// Zero-width characters printed after a cell's own, by cell.
    marks: Vec<(usize, char)>,
}

impl PaintRow {
    fn blank(cols: usize) -> Self {
        Self {
            text: vec![' '; cols],
            attrs: vec![CellAttrs::default(); cols],
            marks: Vec::new(),
        }
    }

    /// A sentinel no real row equals, forcing the first paint.
    fn unpainted() -> Self {
        Self {
            text: vec!['\u{0}'],
            attrs: vec![CellAttrs::default()],
            marks: Vec::new(),
        }
    }

    /// Put CHARACTER in cell COL, dropping the cell's marks and blanking
    /// what remains of a wide character the write cuts.
    fn put_cell(&mut self, col: usize, character: char) {
        if col >= self.text.len() {
            return;
        }
        if self.text[col] == PADDING_GLYPH && character != PADDING_GLYPH {
            let mut start = col;
            while start > 0 && self.text[start - 1] == PADDING_GLYPH {
                start -= 1;
            }
            if start > 0 {
                self.text[start - 1] = ' ';
                self.marks.retain(|(cell, _)| *cell != start - 1);
            }
            for cell in start..col {
                self.text[cell] = ' ';
            }
        }
        let mut after = col + 1;
        while after < self.text.len() && self.text[after] == PADDING_GLYPH {
            self.text[after] = ' ';
            after += 1;
        }
        self.marks.retain(|(cell, _)| *cell != col);
        self.text[col] = character;
    }

    /// Write display text TEXT from cell COL: padding glyphs take their
    /// cells, attached characters join the cell before them.
    fn write_cells(&mut self, col: usize, text: &str, attrs: Option<CellAttrs>) {
        let mut at = col;
        let mut attaching = false;
        for character in text.chars() {
            if character == ATTACHED_GLYPH {
                attaching = true;
                continue;
            }
            if attaching {
                attaching = false;
                if at > col && at - 1 < self.text.len() {
                    let cell = at - 1;
                    let index = self.marks.partition_point(|(mark, _)| *mark <= cell);
                    self.marks.insert(index, (cell, character));
                }
                continue;
            }
            if at >= self.text.len() {
                break;
            }
            if character == PADDING_GLYPH {
                self.marks.retain(|(cell, _)| *cell != at);
                self.text[at] = PADDING_GLYPH;
            } else {
                self.put_cell(at, character);
            }
            if let Some(attrs) = attrs {
                self.attrs[at] = attrs;
            }
            at += 1;
        }
    }

    fn blit(&mut self, col: usize, text: &str, attrs: CellAttrs) {
        self.write_cells(col, text, Some(attrs));
    }

    fn blit_text(&mut self, col: usize, text: &str) {
        self.write_cells(col, text, None);
    }

    /// Layer FACE attributes over the cells in [FROM, TO): attributes the
    /// face leaves unspecified keep what the cell already shows.
    fn overlay(&mut self, from: usize, to: usize, attrs: CellAttrs) {
        for at in from..to.min(self.attrs.len()) {
            self.attrs[at] = merge_cell_attrs(self.attrs[at], attrs);
        }
    }

    /// Force the foreground of the cells in [FROM, TO), `None' meaning the
    /// terminal's own default.  A separate glyph object realized with the
    /// default face carries that face's (unspecified) foreground rather
    /// than whatever color the underlying cell already shows; term.c's
    /// turn_on_face emits no SGR color at all when
    /// `face_tty_specified_color' (dispextern.h) rejects the default
    /// sentinel.
    fn force_foreground(&mut self, from: usize, to: usize, foreground: Option<u8>) {
        for at in from..to.min(self.attrs.len()) {
            self.attrs[at].foreground = foreground;
        }
    }

    /// Start a separate display object's face stack in [FROM, TO).  Overlay
    /// before/after strings do not inherit attributes from the buffer glyph
    /// at their anchor, so their default face replaces that underlying cell.
    fn replace_attrs(&mut self, from: usize, to: usize, attrs: CellAttrs) {
        for at in from..to.min(self.attrs.len()) {
            self.attrs[at] = attrs;
        }
    }
}

struct TtyState {
    terminal_id: u64,
    other_terminals: std::collections::HashMap<u64, Box<TtyState>>,
    /// Per-window display anchors, keyed by window record id.
    views: std::collections::HashMap<u64, WindowView>,
    /// A `C-u' sequence is accumulating: digits and `-' extend the prefix
    /// argument instead of dispatching (GNU's universal-argument--mode).
    prefix_active: bool,
    /// Events of the in-progress (multi-key) sequence.
    /// Frontend-owned echo text (key-sequence progress, command errors);
    /// when empty, the session's `message' echo line shows instead.
    echo: String,
    /// The frame as last painted: every row above the echo area, the echo
    /// row, and the terminal size they were painted for.  Redraw emits
    /// only rows that differ — GNU's dispnew current-matrix idea, one
    /// line deep.
    painted_rows: Vec<PaintRow>,
    painted_echo: Vec<PaintRow>,
    /// `max-mini-window-height' as resolved at the last full redisplay,
    /// so the interpreter-less painter can grow the echo area too.
    echo_max_rows: usize,
    /// Rows the mini window currently occupies.  GNU's
    /// `resize-mini-windows' default `grow-only': a message taller than
    /// the window grows it (up to `max-mini-window-height'), a shorter
    /// one keeps the height, and an empty echo area returns it to one
    /// row.
    echo_rows: usize,
    painted_size: (usize, usize),
    /// The terminal's size when last read: a change is GNU's
    /// SIGWINCH (handle_window_change_signal), none yet its
    /// init_display_interactive.
    terminal_size: Option<(usize, usize)>,
    /// Resolved face attributes, valid while the interpreter's face
    /// generation is unchanged (GNU's face_change flag).
    face_cache: std::collections::HashMap<String, CellAttrs>,
    face_cache_generation: u64,
    /// The composed echo row currently belongs to an active minibuffer:
    /// the interpreter-less blocking-read repaint must not overwrite it
    /// with the plain message channel (the composed paint carries the
    /// prompt face and overlay strings).
    minibuffer_owns_echo: bool,
    /// Identity of the read whose grow-only minibuffer height is retained.
    /// Minibuffer buffers are reused, so the buffer id alone cannot detect
    /// entry into a fresh read.
    minibuffer_activation_id: Option<u64>,
    /// The menu-bar row's caption text, valid while the selected
    /// window's buffer, major mode, and active keymap set are unchanged
    /// — GNU recomputes menu_bar_items on buffer, window, or mode-line
    /// changes (isearch entering is one), not per key.
    menu_bar_row: Option<((u64, String, usize), String)>,
    /// The message-emission count the glass reflects.  A menu's modal
    /// loop repaints the echo row only when this falls behind — GNU's
    /// message3 paints through a frozen redisplay while read_char's
    /// input-arrival wipe waits for the next redisplay.
    painted_message_tick: u64,
    /// The window layout the last redisplay showed (each window's id and
    /// edges) and each shown buffer's modification count then: a change
    /// in either is a windows_or_buffers_changed event (fset_redisplay,
    /// bset_redisplay).
    last_layout: Vec<(u64, usize, usize, usize, usize)>,
    buffer_modiffs: std::collections::HashMap<u64, crate::buffer::ModCount>,
}

impl TtyState {
    fn activate_terminal(&mut self, id: u64) {
        if id == self.terminal_id {
            return;
        }
        let mut states = std::mem::take(&mut self.other_terminals);
        let next = states
            .remove(&id)
            .map(|state| *state)
            .unwrap_or_else(Self::new);
        let old = std::mem::replace(self, next);
        states.insert(old.terminal_id, Box::new(old));
        self.terminal_id = id;
        self.other_terminals = states;
    }

    fn new() -> Self {
        Self {
            terminal_id: 0,
            other_terminals: Default::default(),
            views: std::collections::HashMap::new(),
            prefix_active: false,
            echo: String::new(),
            painted_rows: Vec::new(),
            painted_echo: Vec::new(),
            echo_rows: 1,
            echo_max_rows: 6,
            painted_size: (0, 0),
            terminal_size: None,
            face_cache: std::collections::HashMap::new(),
            face_cache_generation: 0,
            minibuffer_owns_echo: false,
            minibuffer_activation_id: None,
            menu_bar_row: None,
            painted_message_tick: 0,
            last_layout: Vec::new(),
            buffer_modiffs: std::collections::HashMap::new(),
        }
    }

    fn note_input(&mut self) {
        for view in self.views.values_mut() {
            view.deferred_mode_line_point = None;
        }
    }
}

pub fn run(command_line_args: &[String], options: &batch::BatchRunOptions) -> Result<i32, String> {
    crate::lisp::runtime::with_runtime(|| run_owned(command_line_args, options))
}

fn run_owned(
    command_line_args: &[String],
    options: &batch::BatchRunOptions,
) -> Result<i32, String> {
    let _symbols = crate::lisp::types::symbols::EditorSymbolScope::enter();
    let mut interpreter = batch::initialize_interactive_interpreter(options)?;
    let mut env: Env = crate::lisp::types::Env::new();
    // emacs.c:init_display establishes the terminal before keyboard.c
    // evaluates top-level. startup.el then owns palette registration,
    // terminal Lisp initialization, buffers, file visitation, and messages.
    interpreter.set_tty_display_colors(terminal_color_cells());
    interpreter.set_tty_terminal_type(std::env::var("TERM").ok());
    batch::initialize_initial_frame_faces(&mut interpreter)?;

    let guard = TerminalGuard::enter().map_err(|error| error.to_string())?;
    interpreter.note_selected_frame(interpreter.selected_frame_id);
    let queue = SharedEventQueue::default();
    let state = std::rc::Rc::new(std::cell::RefCell::new(TtyState::new()));
    crate::lisp::primitives::set_tty_event_reader(Some(make_event_reader(
        queue.clone(),
        std::rc::Rc::clone(&state),
    )));
    crate::lisp::primitives::set_tty_event_poller(Some(make_event_poller(
        queue.clone(),
        std::rc::Rc::clone(&state),
    )));
    // Command code that reads events itself (the minibuffer) repaints the
    // frame through this hook, so window-configuration changes made
    // mid-read — a *Completions* pop-up — reach the glass immediately.
    crate::lisp::primitives::set_tty_frame_redraw(Some(Box::new({
        let queue = queue.clone();
        let state = std::rc::Rc::clone(&state);
        move |interpreter, env| {
            if queue.input_pending() {
                // Painting yields to the input, but redisplay_internal
                // runs the window change functions before any update.
                let _ = crate::lisp::primitives::run_window_change_functions(interpreter, env);
                return;
            }
            if let Ok(mut state) = state.try_borrow_mut() {
                let _ = redraw(interpreter, env, &mut state);
            }
        }
    })));
    // term.c's menu_show_hook: the dropdown executor F10's popup path
    // reaches through x-popup-menu.
    crate::lisp::primitives::set_tty_menu_executor(Some(make_menu_executor(
        queue.clone(),
        std::rc::Rc::clone(&state),
    )));
    // keyboard.c's input_pending probe: `input-pending-p' and sit-for's
    // early exit see queued terminal input without consuming it.
    crate::lisp::primitives::set_tty_input_pending_check(Some(Box::new({
        let queue = queue.clone();
        move || queue.input_pending()
    })));
    // keyboard.c's Fdiscard_input: the keyboard buffer empties.
    crate::lisp::primitives::set_tty_input_discard(Some(Box::new({
        let queue = queue.clone();
        move || queue.discard()
    })));
    // keyboard.c:command_loop enters top_level_1 after terminal input is
    // available, then the editing loop. Do not replay Lisp startup actions.
    // emacs.c runs after-pdump-load-hook after init_display. Hook code can
    // therefore inspect the real terminal or read events before top-level.
    let startup = batch::safe_run_hooks(&mut interpreter, "after-pdump-load-hook")
        .and_then(|()| crate::lisp::primitives::set_initial_minibuffer_mode(&mut interpreter))
        .and_then(|()| batch::run_startup_top_level(&mut interpreter, command_line_args));
    let termination = match startup.map_err(LispError::into_kind) {
        Err(LispErrorKind::Terminate(termination)) => Some(termination),
        Err(error) => {
            let text =
                command_error_text(&mut interpreter, &mut env, &LispError::from(error.clone()));
            crate::lisp::primitives::set_echo_area_message(Some(text));
            interpreter.take_pending_termination()
        }
        Ok(_) => interpreter.take_pending_termination(),
    };
    let code = if let Some(termination) = termination {
        Ok(termination.exit_code)
    } else {
        command_loop(&mut interpreter, &mut env, &queue, &state)
    };
    crate::lisp::primitives::set_tty_frame_redraw(None);
    crate::lisp::primitives::set_tty_event_reader(None);
    crate::lisp::primitives::set_tty_event_poller(None);
    crate::lisp::primitives::set_tty_input_pending_check(None);
    crate::lisp::primitives::set_tty_input_discard(None);
    crate::lisp::primitives::set_tty_menu_executor(None);
    crate::lisp::primitives::set_interactive_window_metrics(None);
    drop(guard);
    code
}

/// The session's single event stream, shared between the command loop and
/// command code that pulls events itself (`y-or-n-p', `read-event').  One
/// queue means an event decoded for either consumer is never lost to the
/// other, matching GNU's single keyboard buffer.
/// keyboard.c's struct input_event analog: terminal input the command loop
/// has not yet turned into a Lisp event.  Mouse input stays typed until the
/// frame state needed to build GNU's click event is in reach — exactly the
/// role of MOUSE_CLICK_EVENT entries in GNU's kbd_buffer — while key input
/// is already the Lisp event GNU's tty reader would produce.
#[derive(Clone)]
enum QueuedInput {
    Lisp(Value),
    Mouse(RawMouseInput),
}

impl QueuedInput {
    /// keyboard.c:make_lispy_event is shared by command-loop reads and
    /// Lisp read-event/read-char. A timer's timed read must receive the
    /// same click so sit-for can put it back on unread-command-events.
    fn into_lisp(self, interpreter: &mut Interpreter) -> Option<Value> {
        match self {
            Self::Lisp(event) => Some(event),
            Self::Mouse(raw) => synthesize_mouse_event(interpreter, raw),
        }
    }
}

#[derive(Clone, Copy)]
struct RawMouseInput {
    button: i64,
    modifiers: i64,
    column: u16,
    row: u16,
    press: bool,
}

#[derive(Clone, Default)]
struct SharedEventQueue(std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<QueuedInput>>>);

impl SharedEventQueue {
    /// Whether an event can be delivered without blocking.  Redisplay is
    /// skipped while input is pending, GNU's redisplay preemption: a burst
    /// of keys paints once at the end, and scrolling recenters against the
    /// final point, not each intermediate one.
    fn input_pending(&self) -> bool {
        !self.0.borrow().is_empty()
            || event::poll(std::time::Duration::from_millis(0)).unwrap_or(false)
    }

    /// Drop every event already read from the terminal: the queued ones
    /// and those the terminal has ready, which read_avail_input would
    /// already hold in GNU's keyboard buffer.
    fn discard(&self) {
        self.0.borrow_mut().clear();
        while event::poll(std::time::Duration::from_millis(0)).unwrap_or(false) {
            if event::read().is_err() {
                break;
            }
        }
    }

    /// Pop an event without blocking: a queued one, or whatever the
    /// terminal has ready.  `Err' means the terminal is gone.
    fn try_next_event(&self) -> Result<Option<QueuedInput>, ()> {
        if let Some(event) = self.0.borrow_mut().pop_front() {
            return Ok(Some(event));
        }
        while event::poll(std::time::Duration::from_millis(0)).unwrap_or(false) {
            let Ok(event) = event::read() else {
                return Err(());
            };
            match event {
                Event::Key(key) => {
                    let mut events = encode_key(key);
                    if events.is_empty() {
                        continue;
                    }
                    if crate::lisp::eval::terminal::secondary_output_active()
                        && let Some(frame) = crate::lisp::eval::terminal::primary_top_frame()
                    {
                        events.insert(
                            0,
                            Value::list([Value::symbol("switch-frame"), Value::Frame(frame)]),
                        );
                    }
                    let first = events.remove(0);
                    self.0
                        .borrow_mut()
                        .extend(events.into_iter().map(QueuedInput::Lisp));
                    return Ok(Some(QueuedInput::Lisp(first)));
                }
                Event::Mouse(mouse) => {
                    if let Some(raw) = encode_mouse(mouse) {
                        return Ok(Some(QueuedInput::Mouse(raw)));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }

    /// Pop the next event, blocking on the terminal when empty.  Returns
    /// `None' only on terminal loss.
    fn next_event(&self) -> Option<QueuedInput> {
        if let Some(event) = self.0.borrow_mut().pop_front() {
            return Some(event);
        }
        loop {
            let Ok(event) = event::read() else {
                return None;
            };
            match event {
                Event::Key(key) => {
                    let mut events = encode_key(key);
                    if events.is_empty() {
                        continue;
                    }
                    if crate::lisp::eval::terminal::secondary_output_active()
                        && let Some(frame) = crate::lisp::eval::terminal::primary_top_frame()
                    {
                        events.insert(
                            0,
                            Value::list([Value::symbol("switch-frame"), Value::Frame(frame)]),
                        );
                    }
                    let first = events.remove(0);
                    self.0
                        .borrow_mut()
                        .extend(events.into_iter().map(QueuedInput::Lisp));
                    return Some(QueuedInput::Lisp(first));
                }
                Event::Mouse(mouse) => {
                    if let Some(raw) = encode_mouse(mouse) {
                        return Some(QueuedInput::Mouse(raw));
                    }
                }
                _ => {}
            }
        }
    }
}

/// Blocking single-event reader for command code that pulls events itself.
/// The echo area is repainted before blocking so a prompt just issued with
/// `message' (y-or-n-p's protocol) is visible while the terminal waits.
/// C-g is answered as the event it is: read_char's caller decides whether
/// it quits.
fn make_event_reader(
    queue: SharedEventQueue,
    state: std::rc::Rc<std::cell::RefCell<TtyState>>,
) -> crate::lisp::primitives::TtyEventReader {
    Box::new(move |interpreter| {
        draw_echo_row(interpreter, &state);
        loop {
            let input = queue.next_event()?;
            if let Ok(mut state) = state.try_borrow_mut() {
                state.note_input();
            }
            if let Some(event) = input.into_lisp(interpreter) {
                return Some(event);
            }
        }
    })
}

/// The polling companion waits briefly when the terminal is quiet so Lisp
/// readers can service timers between polls.
fn make_event_poller(
    queue: SharedEventQueue,
    state: std::rc::Rc<std::cell::RefCell<TtyState>>,
) -> crate::lisp::primitives::TtyEventPoller {
    Box::new(move |interpreter| {
        draw_echo_row(interpreter, &state);
        match queue.try_next_event() {
            Err(()) => None,
            Ok(Some(input)) => {
                if let Ok(mut state) = state.try_borrow_mut() {
                    state.note_input();
                }
                Some(input.into_lisp(interpreter))
            }
            Ok(None) => {
                let _ = event::poll(std::time::Duration::from_millis(50));
                Some(None)
            }
        }
    })
}

/// Slice one unbounded echo paint into mini-window rows exactly as the
/// GNU tty displays a tall message: embedded newlines break rows, a
/// wider line continues with the `\' marker in the last column, and the
/// result is capped at MAX_ROWS (`max-mini-window-height').
fn wrap_echo_paint(long: &PaintRow, cols: usize, max_rows: usize) -> Vec<PaintRow> {
    let used = long
        .text
        .iter()
        .zip(long.attrs.iter())
        .rposition(|(c, attrs)| *c != ' ' || *attrs != CellAttrs::default())
        .map(|last| last + 1)
        .unwrap_or(0)
        .max(long.marks.last().map_or(0, |(cell, _)| cell + 1));
    let has_newline = long.text[..used].contains(&'\n');
    let copy_cell = |row: &mut PaintRow, col: usize, index: usize| {
        row.text[col] = long.text[index];
        row.attrs[col] = long.attrs[index];
        row.marks.extend(
            long.marks
                .iter()
                .filter(|(cell, _)| *cell == index)
                .map(|(_, mark)| (col, *mark)),
        );
    };
    if used <= cols && !has_newline {
        let mut row = PaintRow::blank(cols);
        for index in 0..long.text.len().min(cols) {
            copy_cell(&mut row, index, index);
        }
        return vec![row];
    }
    let usable = cols.saturating_sub(1).max(1);
    let mut rows = Vec::new();
    let mut row = PaintRow::blank(cols);
    let mut col = 0usize;
    for index in 0..used {
        let c = long.text[index];
        if c == '\n' {
            rows.push(std::mem::replace(&mut row, PaintRow::blank(cols)));
            col = 0;
            if rows.len() == max_rows {
                return rows;
            }
            continue;
        }
        if c == PADDING_GLYPH && col == 0 {
            // The rest of a wide character that moved to this row.
            continue;
        }
        // A wide character whose padding would reach the last column
        // moves down whole, the cells it leaves filled with `'.
        let width = 1 + long.text[index + 1..used]
            .iter()
            .take_while(|cell| **cell == PADDING_GLYPH)
            .count();
        let wraps = c != PADDING_GLYPH && col < usable && col + width > usable;
        if col == usable || wraps {
            for cell in col..cols {
                row.text[cell] = '\\';
            }
            rows.push(std::mem::replace(&mut row, PaintRow::blank(cols)));
            col = 0;
            if rows.len() == max_rows {
                return rows;
            }
        }
        if c == PADDING_GLYPH {
            row.text[col] = PADDING_GLYPH;
            row.attrs[col] = long.attrs[index];
        } else {
            copy_cell(&mut row, col, index);
        }
        col += 1;
    }
    // A terminal display string ending in a newline terminates its final
    // visible row; it does not allocate another empty mini-window row.
    // Vertico's candidate overlay ends every candidate (including the last)
    // this way.
    if used == 0 || long.text[used - 1] != '\n' {
        rows.push(row);
    }
    rows
}

/// Map a character boundary in the unbounded echo paint to the mini-window
/// row and column produced by `wrap_echo_paint'.  The continuation marker
/// occupies the last column, so wrapped content advances in COLS-1 chunks.
fn wrapped_echo_cursor(
    long: &PaintRow,
    position: usize,
    cols: usize,
    max_rows: usize,
) -> (usize, usize) {
    let used = long
        .text
        .iter()
        .zip(long.attrs.iter())
        .rposition(|(c, attrs)| *c != ' ' || *attrs != CellAttrs::default())
        .map(|last| last + 1)
        .unwrap_or(0);
    let extent = used.max(position.min(long.text.len()));
    if extent <= cols && !long.text[..extent].contains(&'\n') {
        return (0, position.min(cols.saturating_sub(1)));
    }
    let usable = cols.saturating_sub(1).max(1);
    let mut row = 0usize;
    let mut col = 0usize;
    for index in 0..position.min(extent) {
        let c = long.text[index];
        if c == '\n' {
            if row + 1 >= max_rows {
                break;
            }
            row += 1;
            col = 0;
        } else {
            // wrap_echo_paint's row breaks, a wide character moving down
            // whole.
            let width = 1 + long.text[index + 1..extent]
                .iter()
                .take_while(|cell| **cell == PADDING_GLYPH)
                .count();
            let wraps = c != PADDING_GLYPH && col < usable && col + width > usable;
            if col == usable || wraps {
                if row + 1 >= max_rows {
                    break;
                }
                row += 1;
                col = 0;
            }
            col += 1;
        }
    }
    if col == usable && position < extent && row + 1 < max_rows {
        row += 1;
        col = 0;
    }
    (
        row.min(max_rows.saturating_sub(1)),
        col.min(cols.saturating_sub(1)),
    )
}

/// The mini window's height ceiling, GNU's `max-mini-window-height'
/// (default 0.25 of the frame).
fn max_mini_window_rows(interpreter: &Interpreter, rows: usize) -> usize {
    match interpreter
        .lookup_var("max-mini-window-height", &crate::lisp::types::Env::new())
        .map(|v| v.kind())
    {
        Some(Kind::Integer(lines)) if lines > 0 => (lines as usize).min(rows.saturating_sub(2)),
        Some(Kind::Float(fraction)) if fraction.get() > 0.0 => {
            (((rows as f64) * fraction.get()) as usize).clamp(1, rows.saturating_sub(2))
        }
        _ => ((rows as f64 * 0.25) as usize).clamp(1, rows.saturating_sub(2)),
    }
}

/// Paint the echo area through the same character and face translation as
/// full redisplay, including when a blocking event reader requests repaint.
fn draw_echo_row_composed(
    interpreter: &mut Interpreter,
    env: &mut Env,
    state: &std::rc::Rc<std::cell::RefCell<TtyState>>,
) {
    let Ok((cols, rows)) = crate::lisp::eval::terminal::output_size() else {
        return;
    };
    let Ok(mut state) = state.try_borrow_mut() else {
        return;
    };
    if state.minibuffer_owns_echo {
        return;
    }
    let frontend_echo = state.echo.clone();
    let cols = cols.max(10) as usize;
    let max_rows = state.echo_max_rows.max(1);
    let (long, _, echo_cursor) = compose_echo_row(
        interpreter,
        env,
        &frontend_echo,
        cols * max_rows,
        &mut state.face_cache,
    );
    let mut echo_paint = wrap_echo_paint(&long, cols, max_rows);
    let mut mini_rows = state.echo_rows.max(1);
    if echo_paint.len() > mini_rows {
        for row in (rows.max(4) as usize).saturating_sub(echo_paint.len())..(rows.max(4) as usize) {
            if row < state.painted_rows.len() {
                state.painted_rows[row] = PaintRow::unpainted();
            }
        }
        state.echo_rows = echo_paint.len();
        mini_rows = echo_paint.len();
    }
    echo_paint.resize(mini_rows, PaintRow::blank(cols));
    encode_terminal_cells(interpreter, &mut echo_paint);
    let base = (rows.max(4) as usize).saturating_sub(mini_rows);
    let mut out = terminal_output();
    for (index, echo_row) in echo_paint.iter().enumerate() {
        let _ = paint_row(&mut out, base + index, echo_row);
    }
    if interpreter
        .lookup_var("cursor-in-echo-area", env)
        .is_some_and(|value| value.is_truthy())
    {
        let (row, col) = wrapped_echo_cursor(&long, echo_cursor.unwrap_or(0), cols, mini_rows);
        let _ = queue!(
            out,
            cursor::MoveTo(
                col.min(cols.saturating_sub(1)) as u16,
                (base + row).min(rows.saturating_sub(1) as usize) as u16,
            ),
            cursor::Show,
        );
    }
    let _ = out.flush();
    state.painted_message_tick = crate::lisp::primitives::echo_area_message_tick();
    state.painted_echo = echo_paint;
}

/// The readers already hold the interpreter, so table selection and face
/// resolution need no separate state or interpreter lookup.
fn draw_echo_row(interpreter: &mut Interpreter, state: &std::rc::Rc<std::cell::RefCell<TtyState>>) {
    let Ok(current) = state.try_borrow() else {
        return;
    };
    if current.minibuffer_owns_echo
        || (current.painted_message_tick == crate::lisp::primitives::echo_area_message_tick()
            && !crate::lisp::primitives::tty_cursor_in_echo_area())
    {
        return;
    }
    drop(current);
    draw_echo_row_composed(interpreter, &mut Env::new(), state);
}

fn command_loop(
    interpreter: &mut Interpreter,
    env: &mut Env,
    queue: &SharedEventQueue,
    shared_state: &std::rc::Rc<std::cell::RefCell<TtyState>>,
) -> Result<i32, String> {
    let mut reader: Option<crate::lisp::primitives::KeySequenceReader> = None;
    crate::lisp::primitives::command_loop_entry(interpreter, env)
        .map_err(|error| error.to_string())?;
    loop {
        // keyboard.c's command_loop_1 reselects the selected window's
        // buffer at the top of every command cycle.  A display action can
        // replace the selected window's buffer while `save-current-buffer'
        // restores the command's former current buffer (Magit does exactly
        // this when opening a log).  Key lookup and the next command must
        // operate in what the selected window shows, not that stale buffer.
        select_command_loop_buffer(interpreter).map_err(|error| error.to_string())?;
        if reader.is_none() {
            crate::lisp::primitives::clear_this_command(interpreter, env);
        }
        // GNU redisplays only when the input queue is quiet; a key burst
        // paints once at the end.
        if !queue.input_pending() {
            let mut state = shared_state.borrow_mut();
            redraw_at_command_boundary(interpreter, env, &mut state)
                .map_err(|error| error.to_string())?;
        }
        // GNU fires ripe timers while the loop waits for input
        // (keyboard.c's timer_check): isearch's lazy highlight and every
        // other timer-driven update land between keystrokes.  A timer
        // that ran gets its work repainted before the wait resumes.
        let mut idle_since: Option<std::time::Instant> = None;
        let event = loop {
            // GNU's read_char consumes `unread-command-events' before the
            // terminal: an event a command read and pushed back (subr.el's
            // sit-for) re-enters the key stream here.
            if let Some(event) =
                crate::lisp::primitives::take_unread_command_event(interpreter, env)
            {
                break QueuedInput::Lisp(event);
            }
            match queue.try_next_event() {
                Err(()) => return Ok(0),
                Ok(Some(event)) => break event,
                Ok(None) => {}
            }
            // Entering the wait means Emacs is idle: keyboard.c's
            // timer_start_idle marks every idle timer runnable again
            // (once per idle period) and starts the `current-idle-time'
            // clock.
            if idle_since.is_none() {
                crate::lisp::primitives::tty_note_idle_start(interpreter, env);
            }
            idle_since.get_or_insert_with(std::time::Instant::now);
            match interpreter
                .service_async_runtime_events(env, true, None)
                .map_err(LispError::into_kind)
            {
                Ok(true) => {
                    let mut state = shared_state.borrow_mut();
                    let _ = redraw(interpreter, env, &mut state);
                }
                Ok(false) => {}
                Err(LispErrorKind::Terminate(termination)) => {
                    return Ok(termination.exit_code);
                }
                Err(error) => {
                    let text =
                        command_error_text(interpreter, env, &LispError::from(error.clone()));
                    crate::lisp::primitives::set_echo_area_message(Some(text));
                    let mut state = shared_state.borrow_mut();
                    let _ = redraw(interpreter, env, &mut state);
                }
            }
            // keyboard.c's kbd_buffer_get_event blocks inside
            // wait_reading_process_output (READ_KBD -1, do_display set):
            // subprocess output reaches filters and sentinels while the
            // loop waits for a key, and a delivery repaints the glass,
            // process.c's redisplay_preserve_echo_area.  Process output
            // does not end the idle period; only keyboard input does.
            match pump_processes_during_wait(interpreter, env).map_err(LispError::into_kind) {
                Ok(true) => {
                    let mut state = shared_state.borrow_mut();
                    let _ = redraw(interpreter, env, &mut state);
                }
                Ok(false) => {}
                Err(LispErrorKind::Terminate(termination)) => {
                    return Ok(termination.exit_code);
                }
                Err(_) => {}
            }
            let _ = event::poll(std::time::Duration::from_millis(50));
        };
        // Input arrived: the idle period is over (timer_stop_idle).
        crate::lisp::primitives::tty_note_idle_end();
        shared_state.borrow_mut().note_input();
        // Typed mouse input becomes GNU's click event now that the frame
        // state is in reach; motion and wheel produce nothing.
        let Some(event) = event.into_lisp(interpreter) else {
            continue;
        };
        // read_char wipes a lingering message the moment any input event
        // arrives — sequences and silently-discarded button-downs
        // included; the glass catches up at the next redisplay.
        crate::lisp::primitives::expire_echo_area_message();
        if event
            .car()
            .ok()
            .and_then(|value| value.symbol_str().map(str::to_owned))
            .as_deref()
            == Some("switch-frame")
        {
            crate::lisp::primitives::call(interpreter, "handle-switch-frame", &[event], env)
                .map_err(|error| error.to_string())?;
            shared_state
                .borrow_mut()
                .activate_terminal(interpreter.selected_terminal_id());
            continue;
        }
        // The state borrow is scoped: `execute_binding' below may re-enter
        // redisplay through the minibuffer's frame-redraw hook, and
        // resolution itself can run the whole dropdown executor (a
        // keymap-bound mouse click pops it), both of which borrow the
        // same cell.
        if reader.is_none() {
            let state = &mut *shared_state.borrow_mut();
            if !state.prefix_active {
                state.echo.clear();
            }
        }
        let resolution = (|| {
            if reader.is_none() {
                reader = Some(crate::lisp::primitives::KeySequenceReader::new(
                    interpreter,
                    Value::Nil,
                    env,
                )?);
            }
            reader
                .as_mut()
                .expect("active key reader")
                .read_event(interpreter, event, env)
        })();
        let resolution = match resolution.map_err(LispError::into_kind) {
            Ok(resolution) => resolution,
            Err(LispErrorKind::Terminate(termination)) => return Ok(termination.exit_code),
            Err(error) => {
                let text = command_error_text(interpreter, env, &LispError::from(error.clone()));
                crate::lisp::primitives::set_echo_area_message(Some(text));
                reader = None;
                continue;
            }
        };
        let active = reader.as_ref().expect("active key reader");
        let original = active.command_binding();
        debug_log(&format!(
            "keys {:?} -> {}",
            describe_keys(active.keys()),
            match &resolution {
                Resolution::Command(binding) => format!("command {binding}"),
                Resolution::Prefix => "prefix".to_string(),
                Resolution::Undefined => "undefined".to_string(),
            }
        ));
        let binding = match resolution {
            Resolution::Command(binding) => binding,
            Resolution::Prefix => {
                let mut state = shared_state.borrow_mut();
                if active.keys().is_empty() {
                    state.echo.clear();
                } else {
                    state.echo = format!("{}-", describe_keys(active.keys()));
                }
                continue;
            }
            // An undefined key sequence is a nil command, which
            // command_loop_1 runs as `undefined' once `pre-command-hook'
            // has run.
            Resolution::Undefined => Value::Nil,
        };
        if !shared_state.borrow().prefix_active {
            shared_state.borrow_mut().echo.clear();
        }
        let keys = reader
            .take()
            .expect("completed key reader")
            .finish(interpreter, false, env);
        let last_event = keys.last().cloned().unwrap_or(Value::Nil);
        let command_error = match crate::lisp::primitives::execute_read_key_command_binding(
            interpreter,
            env,
            original,
            binding,
            &keys,
            last_event,
        )
        .map_err(LispError::into_kind)
        {
            Ok(()) => None,
            Err(LispErrorKind::Terminate(termination)) => {
                return Ok(termination.exit_code);
            }
            Err(error) => {
                debug_log(&format!("command error: {error:?}"));
                if std::env::var_os("EMAXX_TTY_LOG").is_some() {
                    for (_, function, args, _) in
                        interpreter.backtrace_frames_snapshot().iter().take(12)
                    {
                        debug_log(&format!("  frame: {function} nargs={}", args.len()));
                    }
                }
                match settle_command_error(interpreter, env, error) {
                    Ok(text) => text,
                    Err(termination) => return Ok(termination.exit_code),
                }
            }
        };
        if let Some(termination) = interpreter.take_pending_termination() {
            return Ok(termination.exit_code);
        }
        // A prefix command (simple.el's universal-argument family) left
        // an accumulating prefix-arg behind: keep echoing the chain,
        // GNU's echo_keystrokes display of the pending prefix keys.
        // Any other command consumes the chain and its echo.
        let prefix_pending = interpreter
            .lookup_var("prefix-arg", env)
            .is_some_and(|prefix| prefix.is_truthy());
        {
            let state = &mut *shared_state.borrow_mut();
            if prefix_pending {
                state.echo = append_prefix_echo(&state.echo, &describe_keys(&keys));
                state.prefix_active = true;
            } else {
                state.prefix_active = false;
                state.echo.clear();
            }
            if let Some(text) = command_error {
                state.echo = text;
            }
            // A blocking reader may have painted the echo row outside the
            // matrix; repaint it against fresh state next frame.
            state.painted_echo = Vec::new();
        }
    }
}

/// keyboard.c's command_loop around a command that left with ERROR: a
/// `top-level' throw re-enters command_loop_1; anything else (an
/// unmatched throw's no-catch among them) goes to command_loop_2's
/// handler, cmd_error, which reports it through `command-error-function',
/// and command_loop_1 is then entered again: the post-command hooks with
/// `this-command' still the command's, then `last-command'.  The text to
/// echo when reporting failed in turn; the termination that ends the
/// session.
fn settle_command_error(
    interpreter: &mut Interpreter,
    env: &mut Env,
    error: LispErrorKind,
) -> Result<Option<String>, crate::lisp::types::EmacsTermination> {
    let top_level = matches!(
        &error,
        LispErrorKind::Throw(tag, _)
            if matches!(tag.kind(), Kind::Symbol(name) if name == "top-level")
    );
    let error = LispError::from(error);
    let reported = if top_level {
        Ok(())
    } else {
        crate::lisp::primitives::cmd_error(interpreter, env, &error)
    };
    match reported
        .and_then(|()| crate::lisp::primitives::command_loop_entry(interpreter, env))
        .map_err(LispError::into_kind)
    {
        Ok(()) => Ok(None),
        Err(LispErrorKind::Terminate(termination)) => Err(termination),
        Err(error) => Ok(Some(command_error_text(
            interpreter,
            env,
            &LispError::from(error),
        ))),
    }
}

fn select_command_loop_buffer(interpreter: &mut Interpreter) -> Result<(), LispError> {
    let selected_buffer = interpreter.selected_window_buffer_id();
    if selected_buffer != interpreter.current_buffer_id()
        && interpreter.has_buffer_id(selected_buffer)
    {
        interpreter.set_current_buffer_id(selected_buffer)?;
    }
    Ok(())
}

use crate::lisp::primitives::KeyResolution as Resolution;

#[cfg(test)]
fn resolve_pending(interpreter: &mut Interpreter, env: &mut Env, pending: &[Value]) -> Resolution {
    let mut reader = crate::lisp::primitives::KeySequenceReader::new(interpreter, Value::Nil, env)
        .expect("key reader");
    let mut resolution = Resolution::Prefix;
    for event in pending {
        resolution = reader
            .read_event(interpreter, *event, env)
            .expect("key resolution");
    }
    resolution
}

#[cfg(test)]
fn execute_binding(
    interpreter: &mut Interpreter,
    env: &mut Env,
    binding: Value,
    keys: &[Value],
    last_event: Value,
) -> Result<(), LispError> {
    crate::lisp::primitives::execute_command_binding(interpreter, env, binding, keys, last_event)
}

/// Extend the echoed `C-u' sequence: "C-u-" then "C-u 8-", GNU's prefix
/// echo shape.
fn append_prefix_echo(echo: &str, key: &str) -> String {
    match echo.strip_suffix('-') {
        Some(base) if !base.is_empty() => format!("{base} {key}-"),
        _ => format!("{key}-"),
    }
}

/// The echo-area text for a command's error, GNU's
/// `error-message-string' rendering ("Quit", "Beginning of buffer", a
/// user-error's own message).
fn command_error_text(interpreter: &mut Interpreter, env: &mut Env, error: &LispError) -> String {
    crate::lisp::primitives::command_error_echo_text(interpreter, env, error)
}

/// One pump of subprocess and network output for the key wait.
/// process.c wraps every filter call (read_process_output_call under
/// internal_condition_case_1) and sentinel call (exec_sentinel's
/// handler) so an error reports to the echo area and the wait
/// continues; only a termination unwinds the command loop.  Returns
/// whether anything was delivered, which the caller repaints.
fn pump_processes_during_wait(
    interpreter: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut progressed = false;
    for outcome in [
        crate::lisp::primitives::pump_external_process_output(interpreter, env),
        crate::lisp::primitives::pump_connection_processes(interpreter, env),
    ] {
        match outcome.map_err(LispError::into_kind) {
            Ok(pumped) => progressed |= pumped,
            Err(error @ LispErrorKind::Terminate(_)) => return Err(LispError::from(error)),
            Err(error) => {
                let text = command_error_text(interpreter, env, &LispError::from(error.clone()));
                crate::lisp::primitives::set_echo_area_message(Some(text));
                progressed = true;
            }
        }
    }
    Ok(progressed)
}

// ── Event encoding ──────────────────────────────────────────────────────

/// A terminal mouse press or release as an internal raw token; the
/// command loop turns it into GNU's click event shape once it can see
/// the frame (term.c's GPM path builds tty mouse events at the same
/// C layer).  The token never reaches Lisp.
fn encode_mouse(mouse: event::MouseEvent) -> Option<RawMouseInput> {
    use event::{MouseButton, MouseEventKind};
    let (press, button) = match mouse.kind {
        MouseEventKind::Down(button) => (true, button),
        MouseEventKind::Up(button) => (false, button),
        _ => return None,
    };
    let number = match button {
        MouseButton::Left => 1,
        MouseButton::Middle => 2,
        MouseButton::Right => 3,
    };
    let mut modifier_bits = 0i64;
    if mouse.modifiers.contains(KeyModifiers::CONTROL) {
        modifier_bits |= 1;
    }
    if mouse.modifiers.contains(KeyModifiers::ALT) {
        modifier_bits |= 2;
    }
    if mouse.modifiers.contains(KeyModifiers::SHIFT) {
        modifier_bits |= 4;
    }
    Some(RawMouseInput {
        button: number,
        modifiers: modifier_bits,
        column: mouse.column,
        row: mouse.row,
        press,
    })
}

/// Build GNU's click event from typed mouse input: (EVENT-SYMBOL POSN),
/// posn being (FRAME menu-bar (X . Y) TIME) on the menu-bar row and
/// (WINDOW POS (X . Y) TIME) with window-relative coordinates in a text
/// area — the shapes xt-mouse.el and term.c's GPM support hand to the
/// command loop.  POS stands in with the window's point: the tty layer
/// has no per-cell buffer-position map yet, and the mouse consumers the
/// frontend drives (menu-bar-open-mouse, keymap popups) read only the
/// coordinates.
fn synthesize_mouse_event(interpreter: &mut Interpreter, raw: RawMouseInput) -> Option<Value> {
    let button = raw.button;
    let modifier_bits = raw.modifiers;
    let col = raw.column as i64;
    let row = raw.row as i64;
    let press = raw.press;
    let mut name = String::new();
    if modifier_bits & 2 != 0 {
        name.push_str("M-");
    }
    if modifier_bits & 1 != 0 {
        name.push_str("C-");
    }
    if modifier_bits & 4 != 0 {
        name.push_str("S-");
    }
    if press {
        name.push_str("down-");
    }
    name.push_str(&format!("mouse-{button}"));
    // xt-mouse.el and keyboard.c:make_lispy_event publish this on the
    // actual event symbol; readers consult it even after Lisp renaming.
    interpreter.put_symbol_property(&name, "event-kind", Value::symbol("mouse-click"));

    // Classify the click against the frame geometry used by its window
    // tree, including while a Lisp reader owns the terminal input loop.
    let menu_bar_rows = (interpreter.frame_height() - interpreter.frame_text_height()).clamp(0, 1);
    let posn = if menu_bar_rows > 0 && row == 0 {
        // xt-mouse builds the menu-bar posn with a nil window slot —
        // menu-bar-open-mouse refuses events that sit inside a window.
        Value::list([
            Value::Nil,
            Value::Symbol("menu-bar".into()),
            Value::cons(Value::Integer(col), Value::Integer(0)),
            Value::Integer(0),
        ])
    } else {
        let layout = crate::lisp::primitives::window_render_layout(interpreter);
        let window = layout.iter().find(|info| {
            (info.left as i64) <= col
                && col < (info.left + info.width) as i64
                && (info.top as i64) <= row
                && row < (info.top + info.height) as i64
        })?;
        let pos = if window.buffer_id == interpreter.current_buffer_id() {
            interpreter.buffer.borrow().point()
        } else {
            interpreter
                .get_buffer_by_id(window.buffer_id)
                .map(|buffer| buffer.point())
                .unwrap_or(1)
        };
        Value::list([
            interpreter.record_value(window.window_id),
            Value::Integer(pos as i64),
            Value::cons(
                Value::Integer(col - window.left as i64),
                Value::Integer(row - window.top as i64),
            ),
            Value::Integer(0),
        ])
    };
    Some(Value::list([Value::Symbol(name.into()), posn]))
}

/// Translate a terminal key event into GNU key events.  Meta becomes the
/// ESC prefix so the runtime's standard `esc-map' resolves M- bindings, and
/// control characters use their ASCII codes, exactly as a real tty delivers
/// them to GNU.
fn encode_key(key: KeyEvent) -> Vec<Value> {
    let mut events = Vec::new();
    if key.modifiers.contains(KeyModifiers::ALT) {
        events.push(Value::Integer(27));
    }
    let control = key.modifiers.contains(KeyModifiers::CONTROL);
    let base = match key.code {
        KeyCode::Char(c) => {
            if control {
                match c {
                    ' ' | '@' => Some(Value::Integer(0)),
                    'a'..='z' => Some(Value::Integer((c as u8 - b'a' + 1) as i64)),
                    'A'..='Z' => Some(Value::Integer((c as u8 - b'A' + 1) as i64)),
                    '[' | '3' => Some(Value::Integer(27)),
                    '\\' | '4' => Some(Value::Integer(28)),
                    ']' | '5' => Some(Value::Integer(29)),
                    '^' | '6' => Some(Value::Integer(30)),
                    // Terminals send C-_ as 0x1f, which parsers report as
                    // Ctrl plus any of these; C-2..C-8 follow xterm's
                    // control-digit convention.
                    '_' | '/' | '7' => Some(Value::Integer(31)),
                    '?' | '8' => Some(Value::Integer(127)),
                    '2' => Some(Value::Integer(0)),
                    _ => Some(Value::Integer(c as i64)),
                }
            } else {
                Some(Value::Integer(c as i64))
            }
        }
        KeyCode::Enter => Some(Value::Integer(13)),
        KeyCode::Tab => Some(Value::Integer(9)),
        // term/xterm decodes `\e[Z' (kcbt) to the `backtab' function
        // key; org's S-TAB global cycling lives on it.
        KeyCode::BackTab => Some(Value::Symbol("backtab".into())),
        KeyCode::Backspace => Some(Value::Integer(127)),
        KeyCode::Esc => Some(Value::Integer(27)),
        KeyCode::Up => Some(Value::Symbol("up".into())),
        KeyCode::Down => Some(Value::Symbol("down".into())),
        KeyCode::Left => Some(Value::Symbol("left".into())),
        KeyCode::Right => Some(Value::Symbol("right".into())),
        KeyCode::Home => Some(Value::Symbol("home".into())),
        KeyCode::End => Some(Value::Symbol("end".into())),
        KeyCode::PageUp => Some(Value::Symbol("prior".into())),
        KeyCode::PageDown => Some(Value::Symbol("next".into())),
        KeyCode::Delete => Some(Value::Symbol("deletechar".into())),
        KeyCode::Insert => Some(Value::Symbol("insert".into())),
        KeyCode::F(n) => Some(Value::Symbol(format!("f{n}").into())),
        _ => None,
    };
    match base {
        Some(event) => events.push(event),
        None => events.clear(),
    }
    events
}

fn describe_keys(events: &[Value]) -> String {
    let mut parts = Vec::new();
    let mut meta = false;
    for event in events {
        match event.kind() {
            Kind::Integer(27) => {
                meta = true;
                continue;
            }
            Kind::Integer(code) => parts.push(describe_char(code, meta)),
            Kind::Symbol(name) => parts.push(if meta {
                format!("M-<{name}>")
            } else {
                format!("<{name}>")
            }),
            other => parts.push(format!("{other}")),
        }
        meta = false;
    }
    if meta {
        parts.push("ESC".to_string());
    }
    parts.join(" ")
}

fn describe_char(code: i64, meta: bool) -> String {
    let prefix = if meta { "M-" } else { "" };
    match code {
        0 => format!("{prefix}C-SPC"),
        9 => format!("{prefix}TAB"),
        13 => format!("{prefix}RET"),
        27 => format!("{prefix}ESC"),
        32 => format!("{prefix}SPC"),
        127 => format!("{prefix}DEL"),
        1..=26 => format!("{prefix}C-{}", (b'a' + (code as u8 - 1)) as char),
        28..=31 => format!("{prefix}C-{}", (b'\\' + (code as u8 - 28)) as char),
        _ => char::from_u32(code as u32)
            .map(|c| format!("{prefix}{c}"))
            .unwrap_or_else(|| format!("{prefix}#{code}")),
    }
}

// ── Redisplay ───────────────────────────────────────────────────────────

/// A window's planned text rows and display geometry for one redisplay.
struct WindowPlan {
    /// Text rows, top to bottom, each at most the window's body width,
    /// with the buffer line, wrap segment, start position, and hscroll
    /// each row shows — the anchors face spans map through.
    rendered: Vec<(String, usize, usize, usize, usize)>,
    /// Buffer position where the window's display starts.
    top_pos: usize,
    /// Position just past the last displayed character (GNU window-end).
    window_end: usize,
    /// Cursor cell within the window body (including any line-number gutter),
    /// for the selected window.
    cursor: Option<(usize, usize)>,
}

/// GNU's `truncate-partial-width-windows' default: windows narrower than
/// this — when not the frame's full width — truncate long lines with `$'
/// instead of wrapping them.
const TRUNCATE_PARTIAL_WIDTH: i64 = 50;

/// Whether a line of LINE_W display columns, shown from column HSCROLL in
/// a window W columns wide, still extends past the right edge (GNU's
/// row->truncated_on_right_p).  A left-truncated row spends its first
/// column on the `$' glyph.
fn truncated_on_right(line_w: usize, hscroll: usize, w: usize) -> bool {
    let hidden = hscroll + usize::from(hscroll > 0);
    let remaining = line_w.saturating_sub(hidden);
    let available = w.saturating_sub(usize::from(hscroll > 0)).max(1);
    remaining >= available
}

/// The window's line-display geometry for this redisplay: whether lines
/// truncate, and the horizontal scroll after xdisp.c's
/// hscroll_window_tree ran for the window.
struct RenderGeometry {
    truncate: bool,
    hscroll: usize,
    /// `auto-hscroll-mode' `current-line': only the row showing point
    /// hscrolls by `hscroll'; every other row keeps `min_hscroll'
    /// (xdisp.c hscrolling_current_line_p).
    current_line_only: bool,
    min_hscroll: usize,
    /// The `display-line-numbers' column this window reserves, if any:
    /// its glyphs precede the text on every row, so the text area
    /// shrinks by `cols' and hscroll_window_tree counts them in the
    /// cursor's x (the x_offset loop over charpos < 0 glyphs).
    lnum: Option<crate::lisp::primitives::LineNumberLayout>,
}

/// init_iterator's wrap decision plus hscroll_window_tree, for one
/// window.  Lines truncate when the buffer-local `truncate-lines' is
/// non-nil, when the window is hscrolled, or when the window is
/// narrower than the frame and `truncate-partial-width-windows' says so
/// (t, or an integer exceeding the window's width).  Auto-hscroll
/// (xdisp.c:16114) then keeps point's column visible: an explicit
/// scroll-left/right suspends it until window point moves, and with the
/// default `hscroll-step' 0 the recomputed hscroll centers point — or
/// leaves four columns of headroom when point sits at the end of its
/// line.  `auto-hscroll-mode' `current-line' is treated as t (the
/// single-line variant is not modeled).
fn window_render_geometry(
    interpreter: &mut Interpreter,
    env: &mut Env,
    info: &crate::lisp::primitives::WindowRenderInfo,
    body_width: usize,
    frame_cols: usize,
    lnum: Option<crate::lisp::primitives::LineNumberLayout>,
) -> RenderGeometry {
    let lnum_cols = lnum.map_or(0, |layout| layout.cols);
    // The window's buffer decides; a buffer with no local binding sees
    // the DEFAULT value — never the current buffer's local (the occur
    // window must not truncate because the selected org buffer does).
    let buffer_local = |interpreter: &Interpreter, name: &str| {
        interpreter
            .buffer_local_value(info.buffer_id, name)
            .or_else(|| interpreter.default_value(name))
            .unwrap_or(Value::Nil)
    };
    let truncate_lines = buffer_local(interpreter, "truncate-lines").is_truthy();
    let partial = info.width < frame_cols;
    let partial_truncates = partial
        && match interpreter
            .lookup_var("truncate-partial-width-windows", env)
            .map(|v| v.kind())
        {
            Some(Kind::Integer(columns)) => columns > info.width as i64,
            Some(value) => value.value().is_truthy(),
            None => TRUNCATE_PARTIAL_WIDTH > info.width as i64,
        };

    let state = crate::lisp::primitives::window_hscroll_state(interpreter, info.window_id);
    let mut hscroll = state.hscroll.max(0);
    let mut suspended = state.suspended;
    let point = info.point as i64;
    // An explicit scroll's suspension lifts once the window's point has
    // moved (hscroll_window_tree compares against w->old_pointm).
    if suspended && state.old_point != Some(point) {
        suspended = false;
    }

    let auto_mode_value = buffer_local(interpreter, "auto-hscroll-mode");
    let auto_mode = auto_mode_value.is_truthy();
    // hscrolling_current_line_p: `current-line' hscrolls only the row
    // showing point, and only while auto-hscroll is not suspended.
    let current_line_only = !suspended
        && matches!(auto_mode_value.kind(), Kind::Symbol(name) if name == "current-line");
    let truncate_now = truncate_lines || hscroll > 0 || partial_truncates;
    if auto_mode && !suspended && truncate_now {
        let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
            Some(interpreter.buffer.borrow())
        } else {
            interpreter.get_buffer_by_id(info.buffer_id)
        }) else {
            return RenderGeometry {
                truncate: truncate_now,
                hscroll: hscroll.max(0) as usize,
                current_line_only: false,
                min_hscroll: state.min_hscroll.max(0) as usize,
                lnum,
            };
        };
        let point_pos = info.point.clamp(buffer.point_min(), buffer.point_max());
        // hscroll_window_tree measures the cursor's row with the display
        // iterator: the visual line's laid-out columns, prefix included.
        let spec = resolve_buffer_invisibility(interpreter, &buffer, info.buffer_id);
        let context = CharacterDisplayContext::new(interpreter, info.buffer_id, info.window_id)
            .with_line_numbers(lnum_cols);
        let point_line =
            visual_line_first_line(&buffer, &spec, buffer.line_number_at_pos(point_pos));
        let visual = glyphless_visual_line_at(&buffer, &spec, point_line, Some(&context));
        let laid = layout::lay_out_visual_line(&visual, body_width, true);
        let point_index = visual
            .map
            .get(point_pos - buffer.display_line_start_of(point_line))
            .copied()
            .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0));
        let point_dcol = laid.columns[point_index.min(laid.columns.len() - 1)] as i64;
        let line_w = laid.columns.last().copied().unwrap_or(0);
        let w = body_width as i64;
        let margin = interpreter
            .lookup_var("hscroll-margin", env)
            .and_then(|value| value.integer_value())
            .unwrap_or(5)
            .clamp(0, 1_000_000);
        let cursor_x = point_dcol - hscroll;
        // A line-number column's glyphs count into the cursor's x
        // (hscroll_window_tree's x_offset loop); on a tty the left
        // truncation glyph then replaces the column's first glyph, so
        // the margin loses one column back.
        let x_offset: i64 = lnum_cols as i64 + if hscroll > 0 { -1 } else { 0 };
        // In the trigger, the column offset appears on both sides of
        // the left-margin comparison and cancels; the right margin and
        // the truncation test see the narrowed text area.
        let text_w = w - lnum_cols as i64;
        let row_truncated_right = truncated_on_right(
            line_w,
            hscroll.max(0) as usize,
            body_width.saturating_sub(lnum_cols),
        );
        // The third hscroll_window_tree trigger: when only the current
        // line hscrolls and point moved to a line that does not need
        // it, the recomputation brings hscroll back toward min_hscroll.
        let trigger = (hscroll > 0 && cursor_x <= margin + x_offset - lnum_cols as i64)
            || (row_truncated_right && cursor_x >= text_w - margin)
            || (current_line_only && hscroll != state.min_hscroll && hscroll > 0);
        if trigger {
            // ITERATOR_AT_END_OF_LINE_P: the iterator sits on a newline
            // (at ZV it does not, so the cursor centers).
            let at_eol = buffer.char_at(point_pos) == Some('\n');
            // hscroll_window_tree's iterator runs with the line-number
            // glyphs produced, so its current_x carries their columns.
            let current_x = point_dcol + lnum_cols as i64;
            let step = interpreter.lookup_var("hscroll-step", env);
            let new_hscroll = match step.map(|v| v.kind()) {
                Some(Kind::Float(relative)) if relative.get() >= 0.0 => {
                    let wanted = if cursor_x >= text_w - margin {
                        (w as f64) * (1.0 - relative.get()) - margin as f64
                    } else {
                        (w as f64) * relative.get() + (margin + x_offset) as f64
                    };
                    (current_x - wanted as i64).max(0)
                }
                Some(Kind::Integer(step)) if step > 0 => {
                    let wanted = if cursor_x >= text_w - margin {
                        w - step - margin
                    } else {
                        step + margin + x_offset
                    };
                    (current_x - wanted).max(0)
                }
                _ => (current_x - if at_eol { w - 4 } else { w / 2 }).max(0),
            };
            hscroll = new_hscroll.max(state.min_hscroll);
        }
    }
    crate::lisp::primitives::store_window_hscroll_state(
        interpreter,
        info.window_id,
        hscroll,
        suspended,
        point,
    );
    RenderGeometry {
        truncate: truncate_lines || hscroll > 0 || partial_truncates,
        hscroll: hscroll.max(0) as usize,
        current_line_only,
        min_hscroll: state.min_hscroll.max(0) as usize,
        lnum,
    }
}

/// The `display' `(space :align-to COL)' target of the property value,
/// if the value is such a space spec (xdisp.c's stretch glyphs;
/// completion--insert-strings builds its columns from them).
fn space_align_to_target(value: &Value) -> Option<usize> {
    let items = value.to_vec().ok()?;
    if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head == "space") {
        return None;
    }
    let position = items
        .iter()
        .position(|item| matches!(item.kind(), Kind::Symbol(name) if name == ":align-to"))?;
    match items.get(position + 1).map(|v| v.kind()) {
        Some(Kind::Integer(column)) if column >= 0 => Some(column as usize),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum SpecifiedSpace {
    Width(usize),
    AlignTo(usize),
}

/// Decode the TTY-supported dimensions of a `(space ...)' display spec.
/// xdisp computes numeric widths in canonical-character units and truncates
/// the resulting pixel count to an integer.  On a terminal the canonical
/// width is one cell, so `:width 0.5' is deliberately a zero-cell stretch.
fn specified_space(value: &Value) -> Option<SpecifiedSpace> {
    let items = value.to_vec().ok()?;
    if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head == "space") {
        return None;
    }
    if let Some(position) = items
        .iter()
        .position(|item| matches!(item.kind(), Kind::Symbol(name) if name == ":width"))
        && let Some(width) = items
            .get(position + 1)
            .and_then(|value| value.as_float().ok())
            .filter(|width| width.is_finite())
    {
        let columns = if width < 0.0 {
            1
        } else {
            width.trunc().min(usize::MAX as f64) as usize
        };
        return Some(SpecifiedSpace::Width(columns));
    }
    if let Some(target) = space_align_to_target(value) {
        return Some(SpecifiedSpace::AlignTo(target));
    }
    // A malformed or dimensionless space spec falls back to one canonical
    // cell; it still replaces the source character with a blank.
    Some(SpecifiedSpace::Width(1))
}

/// handle_single_display_spec's `((margin LOCATION) VALUE)': LOCATION
/// (`left-margin', `right-margin', or nil for the text area) and VALUE,
/// when VALUE can be displayed there (a string or a `(space ...)' spec on
/// a terminal).  A spec without the margin prefix answers None.
fn margin_display_spec(display: &Value) -> Option<(Option<String>, Value)> {
    let (head, rest) = display.cons_values()?;
    let (tag, location) = head.cons_values()?;
    if !matches!(tag.kind(), Kind::Symbol(name) if name == "margin") {
        return None;
    }
    let location = match location.cons_values() {
        Some((side, _)) => side,
        None => Value::Nil,
    };
    let location = match location.kind() {
        Kind::Nil => None,
        Kind::Symbol(name) if name == "left-margin" || name == "right-margin" => {
            Some(name.to_string())
        }
        _ => return None,
    };
    let value = rest.cons_values().map_or(rest, |(value, _)| value);
    let valid = crate::lisp::primitives::string_text(&value).is_ok()
        || value
            .cons_values()
            .is_some_and(|(car, _)| matches!(car.kind(), Kind::Symbol(name) if name == "space"));
    valid.then_some((location, value))
}

/// A buffer `display' property shown in a window margin: the covered text
/// leaves the text area, and the margin shows VALUE instead.
fn buffer_margin_display(display: &Value) -> Option<(String, Value)> {
    match margin_display_spec(display)? {
        (Some(side), value) => Some((side, value)),
        (None, _) => None,
    }
}

/// Decode a string `display' property of the form
/// `((margin SIDE) PAYLOAD)'.  The string's source characters do not enter
/// the text body; PAYLOAD is rendered in the requested window margin.
fn window_margin_display(value: &Value) -> Option<(String, Value)> {
    let display = crate::lisp::primitives::string_property_at(value, 0, "display")?;
    let (side, payload) = buffer_margin_display(&display)?;
    crate::lisp::primitives::string_text(&payload).ok()?;
    Some((side, payload))
}

/// A string character displayed in a graphical fringe has no text glyph on
/// a terminal.  Bookmark's fringe mark is the common case: its source "x"
/// must not leak into the buffer body when no fringe exists.
fn tty_fringe_display(value: &Value, position: usize) -> bool {
    let Some(display) = crate::lisp::primitives::string_property_at(value, position, "display")
    else {
        return false;
    };
    display.to_vec().ok().is_some_and(|parts| {
        matches!(
            parts.first().map(|v| v.kind()),
            Some(Kind::Symbol(name)) if name == "left-fringe" || name == "right-fringe"
        )
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum GlyphlessDisplayMethod {
    ZeroWidth,
    ThinSpace,
    EmptyBox,
    HexCode,
    Acronym(String),
}

impl GlyphlessDisplayMethod {
    fn render(&self, character: char) -> String {
        self.render_code(u32::from(character))
    }

    fn render_code(&self, code: u32) -> String {
        match self {
            Self::ZeroWidth => String::new(),
            Self::ThinSpace => " ".into(),
            Self::EmptyBox => {
                let width = char::from_u32(code)
                    .and_then(unicode_width::UnicodeWidthChar::width)
                    .unwrap_or(0)
                    .clamp(1, 4);
                format!("[{}]", " ".repeat(width))
            }
            Self::HexCode if code < 0x10000 => format!("\\u{code:04X}"),
            Self::HexCode if code <= 0x10ffff => format!("\\U{code:06X}"),
            Self::HexCode => format!("\\U{code:08X}"),
            Self::Acronym(acronym) if acronym.chars().count() == 1 => acronym.clone(),
            Self::Acronym(acronym) => {
                let acronym: String = acronym.chars().take(6).take_while(char::is_ascii).collect();
                format!("[{acronym}]")
            }
        }
    }
}

struct CharacterDisplayContext<'a> {
    interpreter: &'a Interpreter,
    table_id: Option<crate::lisp::types::CharTableRef>,
    display_table: Option<crate::lisp::types::CharTableRef>,
    /// The coding whose charsets the terminal's t->charset_list holds
    /// (None: every character is drawn).
    charset_list_coding: Option<String>,
    buffer_id: u64,
    window_id: u64,
    /// The buffer's `word-wrap', with `word-wrap-by-category'.
    line_wrap: layout::LineWrap,
    /// The buffer's category table, read when wrapping by category.
    category_table: Option<crate::lisp::types::CharTableRef>,
    tab_width: usize,
    /// The buffer's `line-prefix' and `wrap-prefix' values.
    line_prefix: Value,
    wrap_prefix: Value,
    /// Whether any text property or overlay of the buffer carries a
    /// `line-prefix' or `wrap-prefix' (computed on first use).
    prefix_properties: std::cell::Cell<Option<bool>>,
    /// Columns of the window's `display-line-numbers' gutter, which
    /// precedes each row's prefix and text (it->lnum_pixel_width).
    lnum_cols: usize,
    /// The buffer's `ctl-arrow' (it->ctl_arrow_p).
    ctl_arrow: bool,
    char_width_table: Option<crate::lisp::types::CharTableRef>,
    printable_chars: Option<crate::lisp::types::CharTableRef>,
    unicode_category_table: Option<crate::lisp::types::CharTableRef>,
    /// `nobreak-char-display': nil, t (highlight), or another value
    /// (show the escape glyph).
    nobreak_char_display: Value,
    nobreak_char_ascii_display: bool,
    display_raw_bytes_as_hex: bool,
    /// Automatic composition is on for this terminal: a zero-width
    /// character composes with the cell before it.
    auto_composition: bool,
}

impl<'a> CharacterDisplayContext<'a> {
    fn new(interpreter: &'a Interpreter, buffer_id: u64, window_id: u64) -> Self {
        // window.c:window_display_table selects one valid table.  An absent
        // character entry does not fall through to the next table.
        let display_table = [
            interpreter.find_record(window_id).and_then(|window| {
                window
                    .slots
                    .get(crate::lisp::primitives::WINDOW_DISPLAY_TABLE_SLOT)
                    .copied()
            }),
            interpreter
                .buffer_local_toplevel_value(buffer_id, "buffer-display-table")
                .or_else(|| interpreter.default_toplevel_value("buffer-display-table")),
            interpreter.default_toplevel_value("standard-display-table"),
        ]
        .into_iter()
        .flatten()
        .find_map(|value| match value.kind() {
            Kind::CharTable(table)
                if table.has_purpose("display-table") && table.extra_count() == 6 =>
            {
                Some(table)
            }
            _ => None,
        });
        let table_id = match interpreter
            .buffer_local_toplevel_value(buffer_id, "glyphless-char-display")
            .or_else(|| interpreter.default_toplevel_value("glyphless-char-display"))
            .map(|v| v.kind())
        {
            Some(Kind::CharTable(table_id)) => Some(table_id),
            _ => None,
        };
        // The shared term.c policy is what makes LANG=C produce glyphless
        // hex escapes instead of sending raw UTF-8.
        let charset_list_coding = interpreter.terminal_charset_list_coding();
        // Redisplay reads these with the window's buffer current.
        let local = |name: &str| {
            interpreter
                .buffer_local_toplevel_value(buffer_id, name)
                .or_else(|| interpreter.default_toplevel_value(name))
                .unwrap_or(Value::Nil)
        };
        let line_wrap = if local("word-wrap").is_truthy() {
            layout::LineWrap::Word {
                by_category: interpreter
                    .default_toplevel_value("word-wrap-by-category")
                    .is_some_and(|value| value.is_truthy()),
            }
        } else {
            layout::LineWrap::Window
        };
        let category_table = match line_wrap {
            layout::LineWrap::Word { by_category: true } => match local("category-table").kind() {
                Kind::CharTable(table) => Some(table),
                _ => interpreter.standard_category_table(),
            },
            _ => None,
        };
        // buffer.h's SANE_TAB_WIDTH.
        let tab_width = match local("tab-width").kind() {
            Kind::Integer(width) if (1..=1000).contains(&width) => width as usize,
            _ => 8,
        };
        // Redisplay reads these C variables with the window's buffer
        // current, so a buffer-local binding applies.
        let local_char_table = |name: &str| match local(name).kind() {
            Kind::CharTable(table) => Some(table),
            _ => None,
        };
        // composite.c inhibit_auto_composition: nil turns automatic
        // composition off, a string only on the terminal type it names.
        let composition_mode = local("auto-composition-mode");
        let auto_composition = match crate::lisp::primitives::string_text(&composition_mode) {
            Ok(terminal) => interpreter
                .tty_terminal_type()
                .is_none_or(|current| *current != terminal),
            Err(_) => composition_mode.is_truthy(),
        };
        Self {
            interpreter,
            table_id,
            display_table,
            charset_list_coding,
            buffer_id,
            window_id,
            line_wrap,
            category_table,
            tab_width,
            line_prefix: local("line-prefix"),
            wrap_prefix: local("wrap-prefix"),
            prefix_properties: std::cell::Cell::new(None),
            lnum_cols: 0,
            ctl_arrow: local("ctl-arrow").is_truthy(),
            char_width_table: local_char_table("char-width-table"),
            printable_chars: local_char_table("printable-chars"),
            unicode_category_table: local_char_table("unicode-category-table"),
            nobreak_char_display: local("nobreak-char-display"),
            nobreak_char_ascii_display: local("nobreak-char-ascii-display").is_truthy(),
            display_raw_bytes_as_hex: local("display-raw-bytes-as-hex").is_truthy(),
            auto_composition,
        }
    }

    /// The same context for a window whose rows start with a LNUM_COLS
    /// line-number gutter.
    fn with_line_numbers(mut self, lnum_cols: usize) -> Self {
        self.lnum_cols = lnum_cols;
        self
    }

    /// The context display_string draws a mode, header or tab line in:
    /// reseat_to_string's iterator uses `standard-display-table' when it
    /// is a display table, else the window's.
    fn for_mode_line(interpreter: &'a Interpreter, buffer_id: u64, window_id: u64) -> Self {
        let mut context = Self::new(interpreter, buffer_id, window_id);
        if let Some(Kind::CharTable(table)) = interpreter
            .default_toplevel_value("standard-display-table")
            .map(|value| value.kind())
            && table.has_purpose("display-table")
            && table.extra_count() == 6
        {
            context.display_table = Some(table);
        }
        context
    }

    /// produce_stretch_glyph's width, in cells, of a `(space . PLIST)'
    /// display spec at X on a mode-line row: an :align-to position there
    /// counts from the text area's left edge when it names no window
    /// position.
    fn mode_line_stretch_width(&self, plist: &[Value], x: usize) -> usize {
        let get = |key: &str| {
            plist
                .chunks(2)
                .find(|pair| matches!(pair[0].kind(), Kind::Symbol(name) if name == key))
                .and_then(|pair| pair.get(1).copied())
                .filter(|value| !value.is_nil())
        };
        let (width, zero_ok) = if let Some(width) =
            get(":width").and_then(|prop| self.pixel_width(&prop, None))
        {
            (width as i64, true)
        } else if let Some(factor) = get(":relative-width").and_then(|prop| prop.as_float().ok())
            && factor > 0.0
        {
            (factor as i64, false)
        } else if let Some(prop) = get(":align-to") {
            let mut align_to = -1i64;
            match self.pixel_width(&prop, Some(&mut align_to)) {
                Some(target) => {
                    let left = self
                        .interpreter
                        .window_margins(self.window_id)
                        .0
                        .unwrap_or(0)
                        .max(0);
                    let origin = if align_to < 0 { left } else { align_to };
                    ((target as i64 + origin - x as i64).max(0), true)
                }
                None => (1, false),
            }
        } else {
            (1, false)
        };
        if width < 0 || (width == 0 && !zero_ok) {
            1
        } else {
            width as usize
        }
    }

    /// The `word-wrap-by-category' bits (layout.rs) of CHARACTER.
    fn wrap_category_bits(&self, character: char) -> u8 {
        let Some(table) = self.category_table else {
            return 0;
        };
        let set = table.get(u32::from(character));
        let member = |category: u8| crate::lisp::primitives::category_member(&set, category);
        let mut bits = 0;
        if member(b'|') {
            bits |= layout::LINE_BREAKABLE;
        }
        if member(b'<') {
            bits |= layout::NOT_AT_EOL;
        }
        if member(b'>') {
            bits |= layout::NOT_AT_BOL;
        }
        bits
    }

    /// The `line-prefix' (or with WRAP the `wrap-prefix') at POS:
    /// get_line_prefix_it_property's char property, else the variable.
    fn prefix_value_at(&self, buffer: &crate::buffer::Buffer, pos: usize, wrap: bool) -> Value {
        let name = if wrap { "wrap-prefix" } else { "line-prefix" };
        let has_properties = match self.prefix_properties.get() {
            Some(known) => known,
            None => {
                let known = buffer.has_text_property_named("line-prefix")
                    || buffer.has_text_property_named("wrap-prefix")
                    || buffer.overlays.iter().any(|overlay| {
                        !overlay.is_dead()
                            && (overlay
                                .get_prop(&Value::Symbol("line-prefix".into()))
                                .is_some()
                                || overlay
                                    .get_prop(&Value::Symbol("wrap-prefix".into()))
                                    .is_some())
                    });
                self.prefix_properties.set(Some(known));
                known
            }
        };
        if has_properties && pos <= buffer.point_max() {
            let property = crate::lisp::primitives::highest_priority_overlay_property(
                self.interpreter,
                buffer,
                pos,
                name,
                false,
                Some(self.window_id),
            )
            .filter(|value| !value.is_nil())
            .or_else(|| buffer.text_property_at(pos, name))
            .filter(|value| !value.is_nil());
            if let Some(property) = property {
                return property;
            }
        }
        if wrap {
            self.wrap_prefix
        } else {
            self.line_prefix
        }
    }

    /// The glyphs push_prefix_prop shows for prefix VALUE: a string's
    /// characters, or a `(space ...)' stretch of blanks; anything else
    /// (or an empty string) pushes no prefix.
    fn prefix_glyphs(&self, value: &Value) -> Option<String> {
        if value.is_nil() {
            return None;
        }
        if let Ok(text) = crate::lisp::primitives::string_text(value) {
            return (!text.is_empty()).then(|| self.prefix_string_cells(&text).0);
        }
        let items = value.to_vec().ok()?;
        if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head == "space") {
            return None;
        }
        Some(" ".repeat(self.stretch_width(&items[1..])))
    }

    /// A prefix string's glyphs, from the row's start: its display
    /// translations and widths, a TAB to the next tab stop from where it
    /// starts (term.c measures a prefix's TABs from the row, not the
    /// line), and the cell each of TEXT's characters begins at, one past
    /// the end included.
    fn prefix_string_cells(&self, text: &str) -> (String, Vec<usize>) {
        let display = self.translate(text);
        let mut glyphs = String::with_capacity(display.text.len());
        let mut cells = Vec::with_capacity(display.offsets.len());
        let mut cell_of_offset = Vec::with_capacity(display.text.len() + 1);
        let mut cell = 0usize;
        let mut attaching = false;
        for character in display.text.chars() {
            cell_of_offset.push(cell);
            if character == ATTACHED_GLYPH {
                attaching = true;
                glyphs.push(character);
            } else if attaching {
                attaching = false;
                glyphs.push(character);
            } else if character == '\t' {
                let next = (cell + self.tab_width) / self.tab_width * self.tab_width;
                glyphs.extend(std::iter::repeat_n(' ', next - cell));
                cell = next;
            } else {
                glyphs.push(character);
                cell += 1;
            }
        }
        cell_of_offset.push(cell);
        for offset in &display.offsets {
            cells.push(cell_of_offset[(*offset).min(cell_of_offset.len() - 1)]);
        }
        (glyphs, cells)
    }

    /// produce_stretch_glyph's width, in cells, of a `(space . PLIST)'
    /// display spec at the start of a row.
    fn stretch_width(&self, plist: &[Value]) -> usize {
        let get = |key: &str| {
            plist
                .chunks(2)
                .find(|pair| matches!(pair[0].kind(), Kind::Symbol(name) if name == key))
                .and_then(|pair| pair.get(1).copied())
                .filter(|value| !value.is_nil())
        };
        let (width, zero_ok) = if let Some(width) =
            get(":width").and_then(|prop| self.pixel_width(&prop, None))
        {
            (width as i64, true)
        } else if let Some(factor) = get(":relative-width").and_then(|prop| prop.as_float().ok())
            && factor > 0.0
        {
            // The width of the character the stretch replaces: one cell.
            (factor as i64, false)
        } else if let Some(prop) = get(":align-to") {
            let mut align_to = -1i64;
            match self.pixel_width(&prop, Some(&mut align_to)) {
                Some(target) => ((target as i64 + align_to.max(0)).max(0), true),
                None => (1, false),
            }
        } else {
            (1, false)
        };
        if width < 0 || (width == 0 && !zero_ok) {
            1
        } else {
            width as usize
        }
    }

    /// xdisp.c calc_pixel_width_or_height on a terminal, width
    /// dimension: canonical character units are one cell, symbols read
    /// the buffer-local value, `(NUM)' is NUM cells.  With ALIGN_TO the
    /// first window element names an X position.  None is an invalid
    /// spec.
    fn pixel_width(&self, prop: &Value, mut align_to: Option<&mut i64>) -> Option<f64> {
        match prop.kind() {
            Kind::Nil => Some(0.0),
            Kind::Symbol(name) => {
                match name.as_str() {
                    // A terminal frame has no resolution for physical units.
                    "in" | "mm" | "cm" => return None,
                    "height" | "width" => return Some(1.0),
                    "text" => return Some(self.text_area_width() as f64),
                    _ => {}
                }
                if let Some(target) = align_to.as_deref_mut()
                    && *target < 0
                {
                    let position = match name.as_str() {
                        "left" | "left-margin" | "left-fringe" | "scroll-bar" => Some(0),
                        "center" => Some(self.text_area_width() as i64 / 2),
                        "right" | "right-fringe" | "right-margin" => {
                            Some(self.text_area_width() as i64)
                        }
                        _ => None,
                    };
                    if let Some(position) = position {
                        *target = position;
                        return Some(0.0);
                    }
                } else if matches!(
                    name.as_str(),
                    "left-fringe" | "right-fringe" | "left-margin" | "right-margin" | "scroll-bar"
                ) {
                    return Some(0.0);
                }
                let value = self
                    .interpreter
                    .buffer_local_toplevel_value(self.buffer_id, &name)
                    .or_else(|| self.interpreter.default_toplevel_value(&name))
                    .unwrap_or(Value::Nil);
                if matches!(value.kind(), Kind::Symbol(_)) {
                    // A symbol naming a symbol: GNU evaluates once.
                    return value.as_float().ok();
                }
                self.pixel_width(&value, align_to)
            }
            Kind::Integer(_) | Kind::Float(_) => prop.as_float().ok(),
            Kind::Cons(cell) => {
                let car = cell.car.get();
                let cdr = cell.cdr.get();
                let mut car = car;
                if let Kind::Symbol(name) = car.kind() {
                    if name == "+" || name == "-" {
                        let mut pixels = 0.0;
                        let mut first = true;
                        let mut tail = cdr;
                        while let Kind::Cons(item) = tail.kind() {
                            let px = self.pixel_width(&item.car.get(), align_to.as_deref_mut())?;
                            if first {
                                pixels = if name == "+" { px } else { -px };
                                first = false;
                            } else {
                                pixels += px;
                            }
                            tail = item.cdr.get();
                        }
                        return Some(if name == "-" { -pixels } else { pixels });
                    }
                    car = self
                        .interpreter
                        .buffer_local_toplevel_value(self.buffer_id, &name)
                        .or_else(|| self.interpreter.default_toplevel_value(&name))
                        .unwrap_or(Value::Nil);
                }
                if matches!(car.kind(), Kind::Integer(_) | Kind::Float(_)) {
                    let pixels = car.as_float().ok()?;
                    if cdr.is_nil() {
                        return Some(pixels);
                    }
                    let factor = self.pixel_width(&cdr, align_to)?;
                    return Some(pixels * factor);
                }
                None
            }
            _ => None,
        }
    }

    /// window_box_width of the window's text area, in cells.
    fn text_area_width(&self) -> usize {
        crate::lisp::primitives::window_text_area_columns(self.interpreter, self.window_id)
    }

    fn method_from_value(value: Value, no_font_fallback: bool) -> Option<GlyphlessDisplayMethod> {
        let value = value
            .cons_values()
            .map(|(_, text_terminal)| text_terminal)
            .unwrap_or(value);
        let method = match value.kind() {
            Kind::Symbol(name) if name == "zero-width" && no_font_fallback => {
                GlyphlessDisplayMethod::EmptyBox
            }
            Kind::Symbol(name) if name == "zero-width" => GlyphlessDisplayMethod::ZeroWidth,
            Kind::Symbol(name) if name == "thin-space" => GlyphlessDisplayMethod::ThinSpace,
            Kind::Symbol(name) if name == "empty-box" => GlyphlessDisplayMethod::EmptyBox,
            Kind::Symbol(name) if name == "hex-code" => GlyphlessDisplayMethod::HexCode,
            value => match crate::lisp::primitives::string_text(&value.value()) {
                Ok(acronym) => GlyphlessDisplayMethod::Acronym(acronym),
                Err(_) if no_font_fallback => GlyphlessDisplayMethod::EmptyBox,
                Err(_) => return None,
            },
        };
        Some(method)
    }

    /// The method `glyphless-char-display' itself names for CHARACTER.
    fn table_method_for(&self, character: char) -> Option<GlyphlessDisplayMethod> {
        let table_id = self.table_id?;
        self.interpreter
            .char_table_get(table_id, u32::from(character))
            .and_then(|value| Self::method_from_value(value, false))
    }

    /// CHARACTER_WIDTH of a display character: one cell for ASCII, else
    /// `char-width-table''s entry, sanitized.
    fn cell_width(&self, character: char) -> usize {
        if character.is_ascii() {
            return 1;
        }
        match self
            .char_width_table
            .and_then(|table| self.interpreter.char_table_get(table, u32::from(character)))
            .map(|width| width.kind())
        {
            Some(Kind::Integer(width)) if (0..=1000).contains(&width) => width as usize,
            Some(Kind::Integer(_)) => 1000,
            _ => 1,
        }
    }

    /// term.c produce_glyphs for a displayable CHARACTER: its glyph and
    /// a padding glyph for each further cell.  A zero-width character
    /// composes with the cell before it (compose-gstring-for-terminal,
    /// which composes one with no base character after a space); with
    /// automatic composition off it produces no glyph.
    fn push_cells(&self, display: &mut CharacterDisplay, count: &mut usize, character: char) {
        match self.cell_width(character) {
            0 => {
                if !self.auto_composition {
                    return;
                }
                if display
                    .text
                    .chars()
                    .next_back()
                    .is_none_or(|base| base == '\n' || base == '\t')
                {
                    display.text.push(' ');
                    *count += 1;
                }
                display.text.push(ATTACHED_GLYPH);
                display.text.push(character);
                *count += 2;
            }
            width => {
                display.text.push(character);
                display
                    .text
                    .extend(std::iter::repeat_n(PADDING_GLYPH, width - 1));
                *count += width;
            }
        }
    }

    /// A display-table glyph slot of the window's display table (DISP_
    /// ESCAPE_GLYPH is extra slot 2, DISP_CTRL_GLYPH slot 3): its
    /// character and the face it names, else DEFAULT in `escape-glyph'.
    fn special_glyph(&self, slot: usize, default: char) -> (char, Value) {
        let escape_face = Value::symbol("escape-glyph");
        let Some((code, face_id)) = self
            .display_table
            .and_then(|table| self.interpreter.char_table_extra_slot(table, slot))
            .and_then(display_table_glyph)
        else {
            return (default, escape_face);
        };
        let character = crate::lisp::primitives::char_from_integer(code).unwrap_or(default);
        let face = (face_id > 0)
            .then(|| self.interpreter.lisp_face_name_of_id(face_id))
            .flatten()
            .unwrap_or(escape_face);
        (character, face)
    }

    /// xdisp.c get_next_display_element's translation of a control
    /// character (`^C' under `ctl-arrow', else `\003'), a non-ASCII space
    /// or hyphen under `nobreak-char-display', a raw byte (`\200', or
    /// `\x80' under `display-raw-bytes-as-hex') or a character outside
    /// `printable-chars': the glyphs it shows as and the face merged
    /// over them.  MODE_LINE translates a newline too.
    fn escape_glyphs(&self, character: char, mode_line: bool) -> Option<(Vec<char>, Value)> {
        let code = crate::lisp::primitives::string_character_code(true, character);
        let raw_byte = crate::lisp::primitives::raw_byte_from_regex_char(character);
        let ascii = code < 0x80;
        let nobreak = !ascii && !self.nobreak_char_display.is_nil();
        let nonascii_space = nobreak
            && self
                .unicode_category_table
                .and_then(|table| self.interpreter.char_table_get(table, code as u32))
                .is_some_and(|category| category.integer_value() == Some(UNICODE_CATEGORY_ZS));
        let nonascii_hyphen = nobreak && matches!(code, 0xAD | 0x2010 | 0x2011);
        let translate = if code < 0x20 || code == 0x7F {
            (mode_line && character != '\t') || (character != '\n' && character != '\t')
        } else {
            nonascii_space
                || nonascii_hyphen
                || raw_byte.is_some()
                || !((0x20..0x7F).contains(&code)
                    || self.printable_chars.is_some_and(|table| {
                        self.interpreter
                            .char_table_get(table, code as u32)
                            .is_some_and(|value| !value.is_nil())
                    }))
        };
        if !translate {
            return None;
        }
        if ascii && self.ctl_arrow {
            let (caret, face) = self.special_glyph(3, '^');
            let control = char::from_u32((code ^ 0o100) as u32).unwrap_or('?');
            return Some((vec![caret, control], face));
        }
        let highlight = self.nobreak_char_display.symbol_str() == Some("t");
        if nonascii_space && highlight {
            let shown = if self.nobreak_char_ascii_display {
                ' '
            } else {
                character
            };
            return Some((vec![shown], Value::symbol("nobreak-space")));
        }
        if nonascii_hyphen && highlight {
            let shown = if self.nobreak_char_ascii_display {
                '-'
            } else {
                character
            };
            return Some((vec![shown], Value::symbol("nobreak-hyphen")));
        }
        let (escape, face) = self.special_glyph(2, '\\');
        if nonascii_space || nonascii_hyphen {
            return Some((vec![escape, if nonascii_space { ' ' } else { '-' }], face));
        }
        let value = raw_byte.map_or(code, i64::from);
        let digits = if self.display_raw_bytes_as_hex {
            format!("x{value:02x}")
        } else {
            format!("{value:03o}")
        };
        let mut glyphs = vec![escape];
        glyphs.extend(digits.chars());
        Some((glyphs, face))
    }

    fn no_font_method_for(&self, character: char) -> Option<GlyphlessDisplayMethod> {
        // term.c produce_glyphs draws a character of the terminal's
        // t->charset_list (every charset while it is nil); raw eight-bit
        // characters always go out as their byte.
        let encodable = character.is_ascii()
            || crate::lisp::primitives::raw_byte_from_regex_char(character).is_some()
            || self.charset_list_coding.as_deref().is_none_or(|coding| {
                crate::lisp::primitives::terminal_char_encodable(
                    self.interpreter,
                    coding,
                    character as u32,
                )
            });
        if encodable {
            return None;
        }
        Some(self.no_font_method())
    }

    fn no_font_method(&self) -> GlyphlessDisplayMethod {
        let fallback = self
            .table_id
            .and_then(|table_id| self.interpreter.char_table_extra_slot(table_id, 0))
            .unwrap_or(Value::Nil);
        Self::method_from_value(fallback, true).unwrap_or(GlyphlessDisplayMethod::EmptyBox)
    }

    fn translate(&self, text: &str) -> CharacterDisplay {
        self.translate_for(text, false)
    }

    /// TEXT as display glyphs, one display character a terminal cell
    /// (PADDING_GLYPH and ATTACHED_GLYPH mark the rest), in the order
    /// get_next_display_element and produce_glyphs apply: a display-table
    /// vector, an explicit `glyphless-char-display' method, the escape
    /// translations, then a character the terminal cannot encode as a
    /// no-font glyphless method.  MODE_LINE is display_string's row in a
    /// mode, header or tab line.
    fn translate_for(&self, text: &str, mode_line: bool) -> CharacterDisplay {
        let mut display = CharacterDisplay {
            text: String::with_capacity(text.len()),
            offsets: Vec::with_capacity(text.chars().count() + 1),
            glyphless_spans: Vec::new(),
            face_spans: Vec::new(),
        };
        let mut count = 0;
        for character in text.chars() {
            display.offsets.push(count);
            let vector = self
                .display_table
                .and_then(|table| {
                    self.interpreter.char_table_get(
                        table,
                        crate::lisp::primitives::string_character_code(true, character) as u32,
                    )
                })
                .and_then(|value| match value.kind() {
                    Kind::Vector(vector) => Some(vector),
                    _ => None,
                });
            if let Some(vector) = vector {
                // xdisp.c:get_next_display_element gives vectors precedence
                // over glyphless handling.  Their glyphs are not recursively
                // looked up in either character table; an empty vector hides
                // the source character.
                for index in 0..vector.len() {
                    let glyph = vector
                        .get(index)
                        .expect("display vector index is in bounds");
                    let (code, face_id) = display_table_glyph(glyph).unwrap_or((32, 0));
                    let from = count;
                    let character = crate::lisp::primitives::char_from_integer(code).ok();
                    let method = character.map_or_else(
                        || Some(self.no_font_method()),
                        |character| self.no_font_method_for(character),
                    );
                    if let Some(method) = method {
                        let replacement = method.render_code(code as u32);
                        count += replacement.chars().count();
                        display.text.push_str(&replacement);
                        display.glyphless_spans.push((from, count));
                    } else {
                        self.push_cells(
                            &mut display,
                            &mut count,
                            character.expect("encodable glyph has a character"),
                        );
                    }
                    if face_id > 0
                        && let Some(face) = self.interpreter.lisp_face_name_of_id(face_id)
                    {
                        display.face_spans.push((from, count, face));
                    }
                }
            } else if let Some(method) = self.table_method_for(character) {
                self.push_glyphless(&mut display, &mut count, &method, character);
            } else if let Some((glyphs, face)) = self.escape_glyphs(character, mode_line) {
                let from = count;
                for glyph in glyphs {
                    match self.no_font_method_for(glyph) {
                        Some(method) => {
                            self.push_glyphless(&mut display, &mut count, &method, glyph)
                        }
                        None => self.push_cells(&mut display, &mut count, glyph),
                    }
                }
                if from < count {
                    display.face_spans.push((from, count, face));
                }
            } else if let Some(method) = self.no_font_method_for(character) {
                self.push_glyphless(&mut display, &mut count, &method, character);
            } else {
                self.push_cells(&mut display, &mut count, character);
            }
        }
        display.offsets.push(count);
        display
    }

    fn push_glyphless(
        &self,
        display: &mut CharacterDisplay,
        count: &mut usize,
        method: &GlyphlessDisplayMethod,
        character: char,
    ) {
        let from = *count;
        let replacement = method.render(character);
        *count += replacement.chars().count();
        display.text.push_str(&replacement);
        if from < *count {
            display.glyphless_spans.push((from, *count));
        }
    }
}

/// enum unicode_category's Zs (character.h), as `unicode-category-table'
/// stores it.
const UNICODE_CATEGORY_ZS: i64 = 23;

/// dispextern.h:GLYPH_CODE_P accepts a packed character/face integer or a
/// (character . face-id) pair.  Invalid entries display a space.
fn display_table_glyph(value: Value) -> Option<(i64, i64)> {
    const CHARACTER_BITS: u32 = 22;
    const MAX_CHARACTER: i64 = (1 << CHARACTER_BITS) - 1;
    const MAX_FACE: i64 = (1 << 20) - 1;
    match value.kind() {
        Kind::Integer(code) if (0..(1 << 42)).contains(&code) => {
            Some((code & MAX_CHARACTER, code >> CHARACTER_BITS))
        }
        Kind::Cons(_) => match value
            .cons_values()
            .map(|(car, cdr)| (car.kind(), cdr.kind()))
        {
            Some((Kind::Integer(character), Kind::Integer(face)))
                if (0..=MAX_CHARACTER).contains(&character) && (0..=MAX_FACE).contains(&face) =>
            {
                Some((character, face))
            }
            _ => None,
        },
        _ => None,
    }
}

struct CharacterDisplay {
    text: String,
    offsets: Vec<usize>,
    glyphless_spans: Vec<(usize, usize)>,
    face_spans: Vec<(usize, usize, Value)>,
}

impl CharacterDisplay {
    fn offset(&self, source: usize) -> usize {
        self.offsets
            .get(source)
            .copied()
            .unwrap_or_else(|| *self.offsets.last().unwrap_or(&0))
    }

    fn remap_faces(&self, spans: &mut [(usize, usize, Value)]) {
        for (from, to, _) in spans {
            *from = self.offset(*from);
            *to = self.offset(*to);
        }
    }
}

/// A mode, header or tab line's text as display_string shows it: the
/// window's display translations and glyph widths, with SPANS moved
/// from character offsets to the cells the glyphs take.
/// The terminal's measures of a window's mode, header or tab line row
/// for the interpreter's display_string.
pub(crate) struct ModeLineMeasure {
    buffer_id: u64,
    window_id: u64,
}

impl ModeLineMeasure {
    /// The measures of WINDOW_ID's rows, which show its buffer.
    #[cfg(test)]
    pub(crate) fn for_window(interpreter: &Interpreter, window_id: u64) -> Self {
        let buffer_id = crate::lisp::primitives::window_buffer_id(
            interpreter,
            &interpreter.record_value(window_id),
        )
        .unwrap_or_else(|| interpreter.current_buffer_id());
        Self {
            buffer_id,
            window_id,
        }
    }
}

impl crate::lisp::primitives::ModeLineCells for ModeLineMeasure {
    fn widths(&self, interpreter: &Interpreter, text: &[char]) -> Vec<usize> {
        let source: String = text.iter().collect();
        let display =
            CharacterDisplayContext::for_mode_line(interpreter, self.buffer_id, self.window_id)
                .translate_for(&source, true);
        let glyphs: Vec<char> = display.text.chars().collect();
        display
            .offsets
            .windows(2)
            .map(|pair| {
                let piece: String = glyphs[pair[0].min(glyphs.len())..pair[1].min(glyphs.len())]
                    .iter()
                    .collect();
                cell_count(&piece)
            })
            .collect()
    }

    fn stretch(&self, interpreter: &Interpreter, plist: &[Value], x: usize) -> usize {
        CharacterDisplayContext::for_mode_line(interpreter, self.buffer_id, self.window_id)
            .mode_line_stretch_width(plist, x)
    }

    fn width_of(&self, interpreter: &Interpreter, spec: &Value) -> Option<f64> {
        CharacterDisplayContext::for_mode_line(interpreter, self.buffer_id, self.window_id)
            .pixel_width(spec, None)
    }
}

fn apply_character_display(
    interpreter: &Interpreter,
    buffer_id: u64,
    window_id: u64,
    text: String,
    spans: &mut Vec<(usize, usize, Value)>,
) -> String {
    let display = CharacterDisplayContext::for_mode_line(interpreter, buffer_id, window_id)
        .translate_for(&text, true);
    display.remap_faces(spans);
    spans.extend(display.face_spans);
    spans.extend(
        display
            .glyphless_spans
            .into_iter()
            .map(|(from, to)| (from, to, Value::symbol("glyphless-char"))),
    );
    let cells = display_cells(&display.text);
    let cell_of = |offset: usize| cells[offset.min(cells.len() - 1)];
    for (from, to, _) in spans.iter_mut() {
        *from = cell_of(*from);
        *to = cell_of(*to);
    }
    display.text
}

/// A visual line under invisibility: the laid-out text starting at the
/// display line holding FIRST_LINE, with invisible runs skipped, the
/// display-table ellipsis spliced where a run asks for it, and hidden
/// newlines joining the raw lines that follow.  `map' takes a raw char
/// offset (from the first raw line's start) to the display char index
/// where it (or the next visible content) lands; `raw_of_display' is the
/// reverse.  `lines_spanned' counts the raw buffer lines consumed.
struct VisualLine {
    text: String,
    map: Vec<usize>,
    raw_of_display: Vec<usize>,
    lines_spanned: usize,
    /// Display char indexes where a three-dot ellipsis begins.
    ellipses: Vec<usize>,
    /// Display char ranges produced by `glyphless-char-display'.
    glyphless_spans: Vec<(usize, usize)>,
    /// Faces carried by overlay before/after strings, in display indexes.
    display_face_spans: Vec<(usize, usize, Value)>,
    /// `line-prefix'/`wrap-prefix' runs over the line's raw offsets.
    prefixes: Vec<layout::PrefixRun>,
    /// The buffer's `word-wrap' (and `word-wrap-by-category').
    line_wrap: layout::LineWrap,
    /// `word-wrap-by-category' category bits of each display index.
    wrap_categories: Vec<u8>,
    /// SANE_TAB_WIDTH of the buffer.
    tab_width: usize,
    /// Columns of the line-number gutter before each row's prefix.
    lnum_cols: usize,
}

/// Render an overlay before/after string under the window buffer's
/// invisibility spec, retaining a raw-string to displayed-string boundary
/// map so its face spans follow the surviving cells.  These strings are
/// separate xdisp objects: their own `invisible' text property governs each
/// character even though the property is not present in the buffer text.
fn visible_overlay_string(
    value: &Value,
    spec: &InvisibilitySpec,
) -> (String, Vec<(usize, usize, Value)>) {
    let raw = crate::lisp::primitives::string_text(value).unwrap_or_default();
    let characters: Vec<char> = raw.chars().collect();
    let mut rendered = String::new();
    let mut map = Vec::with_capacity(characters.len() + 1);
    let mut display_chars = 0usize;
    let class_at = |position: usize| {
        crate::lisp::primitives::string_property_at(value, position, "invisible")
            .map(|property| crate::lisp::primitives::invisible_value_class(spec, &property))
            .unwrap_or(0)
    };
    let mut position = 0usize;
    while position < characters.len() {
        let class = class_at(position);
        if class == 0 {
            map.push(display_chars);
            if !tty_fringe_display(value, position) {
                rendered.push(characters[position]);
                display_chars += 1;
            }
            position += 1;
        } else {
            let start = position;
            let ellipsis_start = display_chars;
            let mut last_class = class;
            while position < characters.len() {
                let next = class_at(position);
                if next == 0 {
                    break;
                }
                last_class = next;
                position += 1;
            }
            if last_class == 2 {
                rendered.push_str("...");
                display_chars += 3;
            }
            for hidden in start..position {
                map.push(if hidden == start {
                    ellipsis_start
                } else {
                    display_chars
                });
            }
        }
    }
    map.push(display_chars);
    let spans = crate::lisp::primitives::string_face_spans(value)
        .into_iter()
        .filter_map(|(from, to, face)| {
            let from = map.get(from).copied().unwrap_or(display_chars);
            let to = map.get(to).copied().unwrap_or(display_chars);
            (from < to).then_some((from, to, face))
        })
        .collect();
    (rendered, spans)
}

fn visual_line_at(
    buffer: &crate::buffer::Buffer,
    spec: &InvisibilitySpec,
    first_line: usize,
) -> VisualLine {
    let has_overlay_display = buffer.overlays.iter().any(|overlay| {
        !overlay.is_dead()
            && ["before-string", "after-string", "display"]
                .iter()
                .any(|name| {
                    overlay
                        .get_prop(&Value::Symbol((*name).into()))
                        .is_some_and(|value| value.is_string())
                })
    });
    if !spec.active && !has_overlay_display {
        let (text, display_map) = displayed_line_with_map(buffer, first_line);
        let display_count = text.chars().count();
        let map = display_map.unwrap_or_else(|| (0..=display_count).collect());
        // Invert the (raw offset -> display index) map; `display'
        // padding cells belong to the raw char that produced them.
        let mut raw_of_display = vec![0usize; display_count + 1];
        for raw in 0..map.len().saturating_sub(1) {
            for cell in raw_of_display
                .iter_mut()
                .take(map[raw + 1].min(display_count + 1))
                .skip(map[raw])
            {
                *cell = raw;
            }
        }
        if let Some(last) = map.last() {
            for cell in raw_of_display.iter_mut().skip(*last) {
                *cell = map.len().saturating_sub(1);
            }
        }
        return VisualLine {
            text,
            map,
            raw_of_display,
            lines_spanned: 1,
            ellipses: Vec::new(),
            glyphless_spans: Vec::new(),
            display_face_spans: Vec::new(),
            prefixes: Vec::new(),
            line_wrap: layout::LineWrap::Window,
            wrap_categories: Vec::new(),
            tab_width: 8,
            lnum_cols: 0,
        };
    }
    let line_begin = buffer.display_line_start_of(first_line);
    let end = buffer.point_max();
    let has_display_prop = buffer.has_text_property_named("display");
    let mut text = String::new();
    let mut map: Vec<usize> = Vec::new();
    let mut raw_of_display: Vec<usize> = Vec::new();
    let mut display_chars = 0usize;
    let mut col = 0usize;
    let mut lines_spanned = 1usize;
    let mut ellipses: Vec<usize> = Vec::new();
    let mut display_face_spans: Vec<(usize, usize, Value)> = Vec::new();
    let mut overlay_strings = Vec::new();
    let mut overlay_displays = Vec::new();
    for overlay in &buffer.overlays {
        if overlay.is_dead() {
            continue;
        }
        if overlay.beg() < overlay.end()
            && let Some(value) = overlay.get_prop(&Value::Symbol("display".into()))
            && crate::lisp::primitives::string_text(&value).is_ok()
        {
            let priority = overlay
                .get_prop(&Value::Symbol("priority".into()))
                .and_then(|value| value.integer_value())
                .unwrap_or(0);
            overlay_displays.push((
                overlay.beg(),
                overlay.end(),
                priority,
                overlay.identity(),
                value,
            ));
        }
        for (name, after) in [("before-string", false), ("after-string", true)] {
            let Some(value) = overlay.get_prop(&Value::Symbol(name.into())) else {
                continue;
            };
            if window_margin_display(&value).is_some() {
                continue;
            }
            if crate::lisp::primitives::string_text(&value).is_err() {
                continue;
            }
            overlay_strings.push((
                if after { overlay.end() } else { overlay.beg() },
                after,
                overlay.identity(),
                value,
            ));
        }
    }
    overlay_strings.sort_by_key(|(position, after, id, _)| (*position, *after, *id));
    let mut overlay_string_index = 0usize;
    // At a boundary, point follows before-strings but precedes after-strings.
    // Preserve that mapping even though both display objects are spliced
    // before the following buffer character in the flattened visual text.
    let mut after_string_boundaries = Vec::new();
    let mut string_run: Option<String> = None;
    let mut space_run: Option<Value> = None;
    let mut pos = line_begin;
    while pos < end {
        while overlay_strings
            .get(overlay_string_index)
            .is_some_and(|(position, ..)| *position < pos)
        {
            overlay_string_index += 1;
        }
        while let Some((position, after, _, value)) = overlay_strings.get(overlay_string_index) {
            if *position != pos {
                break;
            }
            if *after
                && after_string_boundaries
                    .last()
                    .is_none_or(|(raw, _)| *raw != pos - line_begin)
            {
                after_string_boundaries.push((pos - line_begin, display_chars));
            }
            let (string, spans) = visible_overlay_string(value, spec);
            let start = display_chars;
            for character in string.chars() {
                text.push(character);
                raw_of_display.push(pos - line_begin);
                display_chars += 1;
                if character == '\n' {
                    col = 0;
                } else {
                    col += if character == '\t' { 8 - (col % 8) } else { 1 };
                }
            }
            // Before/after strings are independent display objects.  They
            // do not inherit the buffer character's face merely because
            // their insertion point lies inside that face's span.
            if start < display_chars {
                display_face_spans.push((start, display_chars, Value::Symbol("default".into())));
            }
            display_face_spans.extend(
                spans
                    .into_iter()
                    .map(|(from, to, face)| (start + from, start + to, face)),
            );
            overlay_string_index += 1;
        }
        // A string-valued overlay `display' property replaces the covered
        // buffer glyphs with one independent display object.  Corfu uses a
        // priority-1000 display overlay to preview the selected completion.
        if let Some((beg, _, _, _, value)) = overlay_displays
            .iter()
            .filter(|(beg, end, ..)| *beg <= pos && pos < *end)
            .max_by_key(|(_, _, priority, id, _)| (*priority, *id))
        {
            map.push(display_chars);
            if pos == (*beg).max(line_begin) {
                let replacement = crate::lisp::primitives::string_text(value).unwrap_or_default();
                let start = display_chars;
                for character in replacement.chars() {
                    text.push(character);
                    raw_of_display.push(pos - line_begin);
                    display_chars += 1;
                    if character == '\n' {
                        col = 0;
                    } else {
                        col += if character == '\t' { 8 - (col % 8) } else { 1 };
                    }
                }
                if start < display_chars {
                    display_face_spans.push((
                        start,
                        display_chars,
                        Value::Symbol("default".into()),
                    ));
                }
                display_face_spans.extend(
                    crate::lisp::primitives::string_face_spans(value)
                        .into_iter()
                        .map(|(from, to, face)| (start + from, start + to, face)),
                );
            }
            string_run = None;
            space_run = None;
            pos += 1;
            continue;
        }
        if let Some((run_end, ellipsis)) = invisible_run_at(buffer, spec, pos) {
            string_run = None;
            space_run = None;
            lines_spanned += buffer
                .buffer_substring(pos, run_end)
                .map(|hidden| hidden.matches('\n').count())
                .unwrap_or(0);
            let ellipsis_start = display_chars;
            if ellipsis {
                ellipses.push(display_chars);
                for _ in 0..3 {
                    text.push('.');
                    raw_of_display.push(run_end - line_begin);
                    display_chars += 1;
                    col += 1;
                }
            }
            // Point exactly at the start of the hidden run sits before the
            // ellipsis; positions inside the hidden text land after it.
            // This is visible after isearch stops at the end of a folded Org
            // heading.  Face spans over the hidden interior still collapse
            // past the dots (GNU gives the ellipsis the preceding face).
            for hidden in pos..run_end {
                map.push(if hidden == pos {
                    ellipsis_start
                } else {
                    display_chars
                });
            }
            pos = run_end;
            continue;
        }
        let Some(ch) = buffer.char_at(pos) else {
            break;
        };
        if ch == '\n' {
            break;
        }
        let offset = pos - line_begin;
        let display = has_display_prop
            .then(|| buffer.text_property_at(pos, "display"))
            .flatten();
        // `((margin nil) VALUE)' displays VALUE in the text area; a margin
        // location takes the covered text out of it.
        let display = match display.as_ref().and_then(margin_display_spec) {
            Some((None, value)) => Some(value),
            Some((Some(_), _)) => {
                map.push(display_chars);
                string_run = None;
                space_run = None;
                pos += 1;
                continue;
            }
            None => display,
        };
        let replacement = display
            .as_ref()
            .and_then(|value| crate::lisp::primitives::string_text(value).ok());
        if let Some(replacement) = replacement {
            map.push(display_chars);
            let same_run = string_run
                .as_deref()
                .is_some_and(|previous| previous == replacement);
            if !same_run {
                for character in replacement.chars() {
                    text.push(character);
                    raw_of_display.push(offset);
                    display_chars += 1;
                    col += if character == '\t' { 8 - (col % 8) } else { 1 };
                }
            }
            string_run = Some(replacement);
            space_run = None;
            pos += 1;
            continue;
        }
        string_run = None;
        let space = display.as_ref().and_then(specified_space);
        if let Some(space) = space {
            map.push(display_chars);
            let same_run = space_run
                .as_ref()
                .is_some_and(|previous| display.as_ref() == Some(previous));
            if !same_run {
                let pad = match space {
                    SpecifiedSpace::Width(width) => width,
                    SpecifiedSpace::AlignTo(target) => target.saturating_sub(col),
                };
                for _ in 0..pad {
                    text.push(' ');
                    raw_of_display.push(offset);
                    display_chars += 1;
                }
                col += pad;
            }
            space_run = display;
        } else {
            space_run = None;
            map.push(display_chars);
            text.push(ch);
            raw_of_display.push(offset);
            display_chars += 1;
            col += if ch == '\t' { 8 - (col % 8) } else { 1 };
        }
        pos += 1;
    }
    // A zero-width overlay at ZV is still a display object.  Popon anchors
    // its terminal popup there when point is at the end of the buffer.
    while let Some((position, after, _, value)) = overlay_strings.get(overlay_string_index) {
        if *position != pos {
            break;
        }
        if *after
            && after_string_boundaries
                .last()
                .is_none_or(|(raw, _)| *raw != pos - line_begin)
        {
            after_string_boundaries.push((pos - line_begin, display_chars));
        }
        let (string, spans) = visible_overlay_string(value, spec);
        let start = display_chars;
        for character in string.chars() {
            text.push(character);
            raw_of_display.push(pos - line_begin);
            display_chars += 1;
            if character == '\n' {
                col = 0;
            } else {
                col += if character == '\t' { 8 - (col % 8) } else { 1 };
            }
        }
        if start < display_chars {
            display_face_spans.push((start, display_chars, Value::Symbol("default".into())));
        }
        display_face_spans.extend(
            spans
                .into_iter()
                .map(|(from, to, face)| (start + from, start + to, face)),
        );
        overlay_string_index += 1;
    }
    // A before-string precedes its buffer position, so point at ZV follows
    // the entire display object.  With Popon's multiline popup GNU leaves
    // the terminal cursor at the end of the final candidate row.
    map.push(display_chars);
    for (raw, display) in after_string_boundaries {
        if let Some(boundary) = map.get_mut(raw) {
            *boundary = display;
        }
    }
    raw_of_display.push(pos - line_begin);
    VisualLine {
        text,
        map,
        raw_of_display,
        lines_spanned,
        ellipses,
        glyphless_spans: Vec::new(),
        display_face_spans,
        prefixes: Vec::new(),
        line_wrap: layout::LineWrap::Window,
        wrap_categories: Vec::new(),
        tab_width: 8,
        lnum_cols: 0,
    }
}

fn glyphless_visual_line_at(
    buffer: &crate::buffer::Buffer,
    spec: &InvisibilitySpec,
    first_line: usize,
    context: Option<&CharacterDisplayContext<'_>>,
) -> VisualLine {
    let mut visual = visual_line_at(buffer, spec, first_line);
    let Some(context) = context else {
        return visual;
    };
    visual.line_wrap = context.line_wrap;
    visual.tab_width = context.tab_width;
    visual.lnum_cols = context.lnum_cols;
    visual.prefixes = line_prefix_runs(
        buffer,
        context,
        buffer.display_line_start_of(first_line),
        &visual,
    );
    let display = context.translate(&visual.text);
    let final_raw = visual.raw_of_display.last().copied().unwrap_or(0);
    let mut raw_of_display = Vec::with_capacity(display.offset(usize::MAX) + 1);
    for (index, offsets) in display.offsets.windows(2).enumerate() {
        let raw = visual
            .raw_of_display
            .get(index)
            .copied()
            .unwrap_or(final_raw);
        raw_of_display.extend(std::iter::repeat_n(raw, offsets[1] - offsets[0]));
    }
    raw_of_display.push(final_raw);
    for offset in &mut visual.map {
        *offset = display.offset(*offset);
    }
    for offset in &mut visual.ellipses {
        *offset = display.offset(*offset);
    }
    display.remap_faces(&mut visual.display_face_spans);
    visual.display_face_spans.extend(display.face_spans);
    visual.text = display.text;
    visual.raw_of_display = raw_of_display;
    visual.glyphless_spans = display.glyphless_spans;
    if context.category_table.is_some() {
        visual.wrap_categories = visual
            .text
            .chars()
            .map(|character| context.wrap_category_bits(character))
            .collect();
    }
    visual
}

/// One display line of a window's buffer as its terminal redisplay lays
/// it out (layout.rs): where each glyph row begins, and the row and
/// screen column of each of the line's buffer positions.  Interactive
/// `vertical-motion', `posn-at-point', recentering and scrolling walk
/// these rows, exactly as GNU's commands walk its display iterator.
pub(crate) struct ScreenLineLayout {
    /// Buffer position where each row begins.
    pub(crate) row_starts: Vec<usize>,
    /// The width the line was laid out in, and whether it truncates.
    pub(crate) text_width: usize,
    pub(crate) truncate: bool,
    /// Columns a `display-line-numbers' gutter takes left of the text.
    pub(crate) lnum_cols: usize,
    bol: usize,
    /// (row, column) of each raw offset from BOL, one past the end
    /// included.
    cells: Vec<(usize, usize)>,
}

impl ScreenLineLayout {
    /// The row showing POS and POS's screen column on it.
    pub(crate) fn position_cell(&self, pos: usize) -> (usize, usize) {
        let offset = pos.saturating_sub(self.bol).min(self.cells.len() - 1);
        self.cells[offset]
    }

    /// The first position on ROW drawn at or right of screen column
    /// COLUMN (move_it_in_display_line to an X), else the row's last.
    pub(crate) fn position_at_column(&self, row: usize, column: usize) -> usize {
        let mut last = None;
        for (offset, &(cell_row, cell_column)) in self.cells.iter().enumerate() {
            if cell_row != row {
                if cell_row > row && last.is_some() {
                    break;
                }
                continue;
            }
            if cell_column >= column {
                return self.bol + offset;
            }
            last = Some(offset);
        }
        self.bol
            + last.unwrap_or_else(|| {
                self.row_starts
                    .get(row)
                    .map_or(0, |start| start.saturating_sub(self.bol))
            })
    }
}

/// The current buffer's display line starting at BOL, laid out as WINDOW
/// (showing that buffer) lays it out TEXT_WIDTH cells wide right of a
/// LNUM_COLS line-number gutter, wrapped or, with TRUNCATE, truncated.
pub(crate) fn screen_line_layout(
    interpreter: &Interpreter,
    window_id: u64,
    bol: usize,
    text_width: usize,
    truncate: bool,
    lnum_cols: usize,
) -> ScreenLineLayout {
    let buffer_id = interpreter.current_buffer_id();
    let buffer = interpreter.buffer.borrow();
    let spec = resolve_buffer_invisibility(interpreter, &buffer, buffer_id);
    let context = CharacterDisplayContext::new(interpreter, buffer_id, window_id)
        .with_line_numbers(lnum_cols);
    let first_line = visual_line_first_line(&buffer, &spec, buffer.line_number_at_pos(bol));
    let line_start = buffer.display_line_start_of(first_line);
    let visual = glyphless_visual_line_at(&buffer, &spec, first_line, Some(&context));
    let laid = layout::lay_out_motion_line(&visual, text_width.max(1), truncate);
    let raw_of = |index: usize| {
        visual
            .raw_of_display
            .get(index)
            .copied()
            .unwrap_or_else(|| visual.raw_of_display.last().copied().unwrap_or(0))
    };
    let mut row_starts: Vec<usize> = laid
        .rows
        .iter()
        .enumerate()
        .map(|(row, segment)| {
            if row == 0 {
                line_start
            } else {
                line_start + raw_of(segment.start_index)
            }
        })
        .collect();
    // move_it_by_lines past a truncated row reseats at the next line
    // start; for a last line ending at ZV that is ZV itself, one more
    // screen line down (count-screen-lines sees it, the glass does not).
    let line_end = line_start + visual.raw_of_display.last().copied().unwrap_or(0);
    if truncate
        && line_end == buffer.point_max()
        && line_end > line_start
        && laid.columns.last().copied().unwrap_or(0) >= text_width.max(1)
    {
        row_starts.push(line_end);
    }
    let cells = (0..visual.map.len())
        .map(|offset| {
            let index = visual.map[offset];
            let column = laid.columns[index.min(laid.columns.len() - 1)];
            if truncate {
                (0, column)
            } else {
                wrapped_position_of_column(
                    &laid.rows,
                    column,
                    visual
                        .glyphless_spans
                        .iter()
                        .any(|(from, _)| *from == index),
                )
            }
        })
        .collect();
    ScreenLineLayout {
        row_starts,
        text_width: text_width.max(1),
        truncate,
        lnum_cols,
        bol: line_start,
        cells,
    }
}

/// init_iterator's choice to truncate rather than continue WINDOW's
/// lines: `truncate-lines' in its buffer, a partial-width window under
/// `truncate-partial-width-windows', or a horizontal scroll.
pub(crate) fn window_truncates(interpreter: &Interpreter, window_id: u64, buffer_id: u64) -> bool {
    let value = |name: &str| {
        interpreter
            .buffer_local_value(buffer_id, name)
            .or_else(|| interpreter.default_value(name))
            .unwrap_or(Value::Nil)
    };
    if value("truncate-lines").is_truthy()
        || crate::lisp::primitives::window_hscroll_state(interpreter, window_id).hscroll > 0
    {
        return true;
    }
    let width = crate::lisp::primitives::window_total_columns(interpreter, window_id);
    width < interpreter.frame_width()
        && match value("truncate-partial-width-windows").kind() {
            Kind::Integer(columns) => columns > width,
            other => other.value().is_truthy(),
        }
}

/// The `line-prefix' and `wrap-prefix' over the raw offsets of VISUAL
/// (whose first raw line starts at LINE_BEGIN): one run per stretch
/// where neither the text properties nor the overlays change.
fn line_prefix_runs(
    buffer: &crate::buffer::Buffer,
    context: &CharacterDisplayContext<'_>,
    line_begin: usize,
    visual: &VisualLine,
) -> Vec<layout::PrefixRun> {
    let raw_end = visual.raw_of_display.last().copied().unwrap_or(0);
    let mut runs: Vec<layout::PrefixRun> = Vec::new();
    let mut raw = 0usize;
    loop {
        let pos = line_begin + raw;
        let line = context.prefix_glyphs(&context.prefix_value_at(buffer, pos, false));
        let wrap = context.prefix_glyphs(&context.prefix_value_at(buffer, pos, true));
        if runs
            .last()
            .is_none_or(|run| run.line != line || run.wrap != wrap)
        {
            runs.push(layout::PrefixRun {
                raw_start: raw,
                line,
                wrap,
            });
        }
        if context.prefix_properties.get() != Some(true) || pos >= buffer.point_max() {
            break;
        }
        let (_, property_end) = buffer.text_property_interval_around(pos);
        let overlay_end = crate::lisp::primitives::next_overlay_change_position(buffer, pos);
        let next = property_end.min(overlay_end).max(pos + 1);
        if next >= line_begin + raw_end {
            break;
        }
        raw = next - line_begin;
    }
    if runs.len() == 1 && runs[0].line.is_none() && runs[0].wrap.is_none() {
        runs.clear();
    }
    runs
}

/// One buffer line as redisplay lays it out: a character carrying a
/// `(space :align-to COL)' `display' property renders as blank space up
/// to display column COL in place of the character itself.  Returns the
/// laid-out text and, when any expansion happened, a map from raw char
/// offsets to laid-out char offsets (face spans anchor on raw buffer
/// positions).
fn displayed_line_with_map(
    buffer: &crate::buffer::Buffer,
    line: usize,
) -> (String, Option<Vec<usize>>) {
    let raw = buffer
        .lines_from(line, 1)
        .into_iter()
        .next()
        .unwrap_or_default();
    // Only the accessible part of the line displays: from BEGV when the
    // region begins inside it, up to ZV when it ends inside it.
    let line_begin = buffer.display_line_start_of(line);
    let skip = line_begin - buffer.line_start_of(line);
    let raw = if skip > 0 || line_begin + raw.chars().count() > buffer.point_max() {
        raw.chars()
            .skip(skip)
            .take(buffer.point_max().saturating_sub(line_begin))
            .collect()
    } else {
        raw
    };
    if !buffer.has_text_property_named("display") {
        return (raw, None);
    }
    let mut expanded = String::new();
    let mut expanded_chars = 0usize;
    let mut map = Vec::with_capacity(raw.chars().count() + 1);
    let mut col = 0usize;
    let mut changed = false;
    // A string-valued `display' property shows the string once for the
    // whole run carrying the same value, in place of the covered text
    // (handle_single_display_spec; grep's --null separator renders as
    // ":" this way).
    let mut string_run: Option<String> = None;
    let mut space_run: Option<Value> = None;
    for (offset, ch) in raw.chars().enumerate() {
        map.push(expanded_chars);
        let display = buffer.text_property_at(line_begin + offset, "display");
        let display = match display.as_ref().and_then(margin_display_spec) {
            Some((None, value)) => Some(value),
            Some((Some(_), _)) => {
                changed = true;
                string_run = None;
                space_run = None;
                continue;
            }
            None => display,
        };
        // Lisp often builds the replacement with `concat' or
        // `substitute-command-keys', so it can itself be a propertized
        // string.  It is still a string for display purposes; matching
        // only the two direct Rust string variants dropped Dired's
        // free-space suffix.
        let replacement = display
            .as_ref()
            .and_then(|value| crate::lisp::primitives::string_text(value).ok());
        if let Some(replacement) = replacement {
            changed = true;
            let same_run = string_run
                .as_deref()
                .is_some_and(|previous| previous == replacement);
            if !same_run {
                for c in replacement.chars() {
                    expanded.push(c);
                    expanded_chars += 1;
                    col += 1;
                }
            }
            string_run = Some(replacement);
            space_run = None;
            continue;
        }
        string_run = None;
        let space = display.as_ref().and_then(specified_space);
        if let Some(space) = space {
            changed = true;
            let same_run = space_run
                .as_ref()
                .is_some_and(|previous| display.as_ref() == Some(previous));
            if !same_run {
                let pad = match space {
                    SpecifiedSpace::Width(width) => width,
                    SpecifiedSpace::AlignTo(target) => target.saturating_sub(col),
                };
                for _ in 0..pad {
                    expanded.push(' ');
                }
                expanded_chars += pad;
                col += pad;
            }
            space_run = display;
        } else if ch == '\t' {
            space_run = None;
            expanded.push(ch);
            expanded_chars += 1;
            col += 8 - (col % 8);
        } else {
            space_run = None;
            expanded.push(ch);
            expanded_chars += 1;
            col += 1;
        }
    }
    map.push(expanded_chars);
    if changed {
        (expanded, Some(map))
    } else {
        (raw, None)
    }
}

/// What planning one window yields: either a request to run
/// `window-scroll-functions' first, or the window's rows.
enum PlanOutcome {
    Hook {
        stage: redisplay_window::Stage,
        start: usize,
        consumed: redisplay_window::Consumed,
    },
    Planned {
        plan: WindowPlan,
        chosen: redisplay_window::Chosen,
        consumed: redisplay_window::Consumed,
    },
}

/// Plan one window's text: choose its start as redisplay_window does
/// (redisplay_window.rs), then lay out the visible rows under the
/// window's own wrap-or-truncate geometry.
#[allow(clippy::too_many_arguments)]
fn plan_window_text(
    buffer: &crate::buffer::Buffer,
    spec: &InvisibilitySpec,
    glyphless: Option<&CharacterDisplayContext<'_>>,
    view: &mut WindowView,
    state: &redisplay_window::StartState,
    text_rows: usize,
    body_width: usize,
    truncate: bool,
    geometry: &RenderGeometry,
    selected: bool,
) -> PlanOutcome {
    let hscroll = geometry.hscroll;
    // A line-number column's glyphs precede the text on every row, so
    // the text lays out in the remaining columns.
    let lnum_cols = geometry.lnum.map_or(0, |layout| layout.cols);
    let body_width = body_width.saturating_sub(lnum_cols).max(1);
    let usable = body_width.saturating_sub(1).max(1);
    let rows = redisplay_window::Rows::new(buffer, spec, glyphless, body_width, truncate);
    let (decision, consumed) = redisplay_window::decide(&rows, state, text_rows);
    let chosen = match decision {
        redisplay_window::Decision::Hook { stage, start } => {
            return PlanOutcome::Hook {
                stage,
                start,
                consumed,
            };
        }
        redisplay_window::Decision::Display(chosen) => chosen,
    };
    let point = chosen
        .point
        .unwrap_or(state.point)
        .clamp(buffer.point_min(), buffer.point_max());

    let point_line = visual_line_first_line(buffer, spec, buffer.line_number_at_pos(point));
    let point_visual = glyphless_visual_line_at(buffer, spec, point_line, glyphless);
    let point_offset = point.saturating_sub(buffer.display_line_start_of(point_line));
    let point_index = point_visual
        .map
        .get(point_offset)
        .copied()
        .unwrap_or_else(|| point_visual.map.last().copied().unwrap_or(0));
    let point_layout = layout::lay_out_visual_line(&point_visual, body_width, truncate);
    let point_dcol = point_layout.columns[point_index.min(point_layout.columns.len() - 1)];
    let cursor_col = if truncate {
        // Without a line-number gutter, the left `$' occupies one screen
        // cell before source column HSCROLL.  A point hidden farther left
        // still clamps to the marker itself.
        let on_screen = HscrollLayout::new(
            &point_visual,
            &point_layout.columns,
            hscroll,
            lnum_cols == 0,
        )
        .screen_column(point_dcol)
        .unwrap_or(0);
        on_screen.min(body_width.saturating_sub(1))
    } else {
        wrapped_position_of_column(
            &point_layout.rows,
            point_dcol,
            point_visual
                .glyphless_spans
                .iter()
                .any(|(from, _)| *from == point_index),
        )
        .1
    };
    let cursor_hidden_left = truncate
        && hscroll > 0
        && lnum_cols > 0
        && HscrollLayout::new(&point_visual, &point_layout.columns, hscroll, false)
            .screen_column(point_dcol)
            .is_none();
    let cursor_col = if lnum_cols == 0 {
        cursor_col
    } else if cursor_hidden_left {
        lnum_cols.saturating_sub(1)
    } else {
        lnum_cols.saturating_add(cursor_col)
    };
    view.top_line = chosen.top.line;
    view.top_seg = chosen.top.seg;
    // Without point in the window (scroll functions that moved the start
    // away from it), the cursor goes to the first row.
    let (point_row, cursor_col) = chosen.cursor_row.map_or((0, 0), |row| (row, cursor_col));

    // Fetch only the window: redisplay cost must follow the screen size,
    // never the buffer size.  A wholly invisible tail contributes no glyph
    // row; every other visual line contributes at least one.
    let last_line = buffer.line_number_at_pos(buffer.point_max());
    let mut rendered: Vec<(String, usize, usize, usize, usize)> = Vec::with_capacity(text_rows);
    // First row past the window, as (line, segment): the window's end.
    let mut past_window: Option<(usize, usize)> = None;
    let mut invisible_tail_start: Option<usize> = None;
    let mut fill_line = view.top_line;
    let mut first_fill = true;
    'fill: while rendered.len() < text_rows && fill_line <= last_line {
        let visual = glyphless_visual_line_at(buffer, spec, fill_line, glyphless);
        let line_start = buffer.display_line_start_of(fill_line);
        if visual.text.is_empty()
            && line_start < buffer.point_max()
            && invisible_class_at(buffer, spec, line_start) != 0
            && visual.raw_of_display.last().copied()
                == Some(buffer.point_max().saturating_sub(line_start))
        {
            // No glyph represents a non-ellipsis invisible tail.  GNU's
            // window-end remains at the first hidden position even though
            // the buffer has no later visible row (and %p therefore says
            // "Top", not "All").
            invisible_tail_start = Some(line_start);
            break;
        }
        // `auto-hscroll-mode' `current-line': the row showing point
        // hscrolls by the window's hscroll, every other row keeps the
        // explicit minimum.
        let row_hscroll = if !geometry.current_line_only || fill_line == point_line {
            hscroll
        } else {
            geometry.min_hscroll
        };
        let segments = if truncate {
            vec![if lnum_cols > 0 {
                truncate_row_from(&visual, body_width, row_hscroll)
            } else {
                truncate_row_hscrolled(&visual, body_width, row_hscroll)
            }]
        } else {
            wrap_glyphless_visual_line(&visual, body_width)
                .into_iter()
                .map(|segment| segment.text)
                .collect()
        };
        let from = if first_fill {
            view.top_seg.min(segments.len() - 1)
        } else {
            0
        };
        first_fill = false;
        let next_line = fill_line + visual.lines_spanned;
        for (seg_index, segment) in segments.iter().enumerate().skip(from) {
            let row_start =
                position_of_visual_row(buffer, spec, glyphless, fill_line, seg_index, usable);
            rendered.push((
                segment.clone(),
                fill_line,
                seg_index,
                row_start,
                row_hscroll,
            ));
            if rendered.len() == text_rows {
                past_window = Some(if seg_index + 1 < segments.len() {
                    (fill_line, seg_index + 1)
                } else {
                    (next_line, 0)
                });
                break 'fill;
            }
        }
        fill_line = next_line;
    }
    rendered.resize(text_rows, (String::new(), 0, 0, usize::MAX, 0));

    let top_pos = rows.start_pos(chosen.top);
    let window_end = match past_window {
        // A row past the final buffer line means the window shows
        // everything: window-end is ZV (a buffer without a trailing
        // newline has no line beyond its last).
        Some((line, _)) if line > last_line => buffer.point_max(),
        Some((line, seg)) => position_of_visual_row(buffer, spec, glyphless, line, seg, usable),
        None => invisible_tail_start.unwrap_or_else(|| buffer.point_max()),
    };
    PlanOutcome::Planned {
        plan: WindowPlan {
            rendered,
            top_pos,
            window_end,
            cursor: selected.then_some((point_row, cursor_col)),
        },
        chosen,
        consumed,
    }
}

/// What redisplay_window reads of INFO's window: its start and point,
/// the redisplay flags commands left, and its buffer's scrolling options.
/// Also the accessible region and modification count the display is
/// made under.
fn window_start_state(
    interpreter: &Interpreter,
    info: &crate::lisp::primitives::WindowRenderInfo,
    previous: &WindowView,
    text_rows: usize,
    top_rows: usize,
    windows_or_buffers_changed: bool,
) -> (
    redisplay_window::StartState,
    (usize, usize),
    crate::buffer::ModCount,
) {
    use crate::lisp::primitives::{
        WINDOW_FORCE_START_SLOT, WINDOW_OPTIONAL_NEW_START_SLOT, WINDOW_START_AT_LINE_BEG_SLOT,
        window_flag,
    };
    let value = |name: &str| {
        interpreter
            .buffer_local_value(info.buffer_id, name)
            .or_else(|| interpreter.default_value(name))
    };
    let integer = |name: &str| {
        value(name)
            .and_then(|value| value.integer_value())
            .unwrap_or(0)
    };
    // NUMBERP: an integer or a float fraction.
    let number = |name: &str| {
        value(name).and_then(|value| match value.kind() {
            Kind::Integer(number) => Some(number as f64),
            Kind::Float(number) => Some(number.get()),
            _ => None,
        })
    };
    let scroll = redisplay_window::ScrollParams {
        margin: crate::lisp::primitives::scroll_margin_lines(
            integer("scroll-margin"),
            value("maximum-scroll-margin").unwrap_or(Value::Nil),
            text_rows,
        ),
        conservatively: integer("scroll-conservatively"),
        step: integer("scroll-step"),
        up_aggressively: number("scroll-up-aggressively"),
        down_aggressively: number("scroll-down-aggressively"),
    };
    let (buffer_point, restriction, modiff) = if info.buffer_id == interpreter.current_buffer_id() {
        let buffer = interpreter.buffer.borrow();
        (
            buffer.point(),
            (buffer.point_min(), buffer.point_max()),
            buffer.modified_tick(),
        )
    } else {
        interpreter
            .get_buffer_by_id(info.buffer_id)
            .map(|buffer| {
                (
                    buffer.point(),
                    (buffer.point_min(), buffer.point_max()),
                    buffer.modified_tick(),
                )
            })
            .unwrap_or((info.point, (1, 1), Default::default()))
    };
    let force_start = window_flag(interpreter, info.window_id, WINDOW_FORCE_START_SLOT);
    let optional_new_start =
        window_flag(interpreter, info.window_id, WINDOW_OPTIONAL_NEW_START_SLOT);
    let window_outdated =
        previous.last_buffer_id != Some(info.buffer_id) || previous.last_modiff != Some(modiff);
    let cursor_unmoved = !windows_or_buffers_changed
        && !window_outdated
        && !force_start
        && !optional_new_start
        && previous.last_point == Some(info.point)
        && previous.last_start == Some(info.start)
        && previous.last_restriction == Some(restriction);
    let state = redisplay_window::StartState {
        start: info.start,
        point: info.point,
        buffer_point,
        force_start,
        optional_new_start,
        start_at_line_beg: window_flag(interpreter, info.window_id, WINDOW_START_AT_LINE_BEG_SLOT),
        windows_or_buffers_changed,
        window_outdated,
        cursor_unmoved,
        scroll_functions: value("window-scroll-functions").is_some_and(|hook| hook.is_truthy()),
        make_window_start_visible: value("make-window-start-visible")
            .is_some_and(|flag| flag.is_truthy()),
        total_cells: info.width.saturating_mul(info.height),
        top_rows,
        scroll,
        resume: None,
    };
    (state, restriction, modiff)
}

/// The force_start and optional_new_start flags redisplay consumed.
fn consume_window_start_flags(
    interpreter: &mut Interpreter,
    window_id: u64,
    consumed: redisplay_window::Consumed,
) {
    use crate::lisp::primitives::{
        WINDOW_FORCE_START_SLOT, WINDOW_OPTIONAL_NEW_START_SLOT, set_window_flag,
    };
    if consumed.force_start {
        set_window_flag(interpreter, window_id, WINDOW_FORCE_START_SLOT, false);
    }
    if consumed.optional_new_start {
        set_window_flag(
            interpreter,
            window_id,
            WINDOW_OPTIONAL_NEW_START_SLOT,
            false,
        );
    }
}

/// xdisp.c run_window_scroll_functions: `window-scroll-functions' with
/// the window's buffer current, quits inhibited and errors reported
/// (safe_run_hooks_2).
fn run_window_scroll_functions(
    interpreter: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    buffer_id: u64,
    start: usize,
) {
    let saved = interpreter.current_buffer_id();
    if saved != buffer_id && interpreter.set_current_buffer_id(buffer_id).is_err() {
        return;
    }
    let _ = env;
    let window = interpreter.record_value(window_id);
    let _ = batch::safe_run_hooks_with_args(
        interpreter,
        "window-scroll-functions",
        &[window, Value::Integer(start as i64)],
    );
    if interpreter.current_buffer_id() != saved && interpreter.has_buffer_id(saved) {
        let _ = interpreter.set_current_buffer_id(saved);
    }
}

/// Store what redisplay decided for INFO's window: its start marker and
/// start_at_line_beg, a point it moved into view (the buffer's own for
/// the selected window), a non-selected window's point clamped to the
/// accessible region, and window_end_pos.  Answers INFO as displayed.
fn publish_window_start(
    interpreter: &mut Interpreter,
    info: &crate::lisp::primitives::WindowRenderInfo,
    chosen: &redisplay_window::Chosen,
    window_end: usize,
) -> crate::lisp::primitives::WindowRenderInfo {
    use crate::lisp::primitives::{
        WINDOW_START_AT_LINE_BEG_SLOT, record_window_display, set_window_flag,
        set_window_start_marker,
    };
    set_window_start_marker(interpreter, info.window_id, chosen.start);
    set_window_flag(
        interpreter,
        info.window_id,
        WINDOW_START_AT_LINE_BEG_SLOT,
        chosen.start_at_line_beg,
    );
    let point = chosen.point.unwrap_or(info.point);
    if info.selected && chosen.point.is_some() {
        if info.buffer_id == interpreter.current_buffer_id() {
            interpreter.buffer.borrow_mut().goto_char(point);
        } else if let Some(mut buffer) = interpreter.get_buffer_by_id_mut(info.buffer_id) {
            buffer.goto_char(point);
        }
    }
    record_window_display(
        interpreter,
        info.window_id,
        info.buffer_id,
        point,
        window_end,
    );
    crate::lisp::primitives::WindowRenderInfo {
        start: chosen.start,
        point,
        ..*info
    }
}

/// Point to use while rendering this frame's mode/header line.
///
/// GNU's redisplay matrix keeps the previous point-dependent mode-line cells
/// after motion within one folded display row, until a subsequent input event
/// invalidates that state.  Preserve only the point argument, and only when
/// buffer text, cursor row, and window start are unchanged; every other
/// mode-line input is still evaluated from current state.
#[allow(clippy::too_many_arguments)]
fn mode_line_display_point(
    previous: WindowView,
    buffer_id: u64,
    point: usize,
    cursor: Option<(usize, usize)>,
    top_pos: usize,
    chars_modiff: crate::buffer::ModCount,
    invisibility_active: bool,
) -> usize {
    if cursor.is_some()
        && invisibility_active
        && previous.last_buffer_id == Some(buffer_id)
        && previous.last_point.is_some_and(|old| old != point)
        && previous.last_cursor.map(|(row, _)| row) == cursor.map(|(row, _)| row)
        && previous.last_top_pos == Some(top_pos)
        && previous.last_chars_modiff == Some(chars_modiff)
    {
        previous.last_point.unwrap_or(point)
    } else {
        point
    }
}

// xdisp.c:4532 handle_fontified_prop, at window granularity: before a
// window's glyphs are produced, every visible position must carry a
// non-nil `fontified' char-property, or `fontification-functions' runs
// for it -- jit-lock fontifies a chunk and marks it -- with the
// variable rebound to nil around the call exactly as GNU specbinds it.
// A `t' in a buffer-local hook value runs the global entries too.
fn fontify_window_ranges(
    interpreter: &mut Interpreter,
    env: &mut Env,
    layout: &[crate::lisp::primitives::WindowRenderInfo],
) {
    let saved_buffer = interpreter.current_buffer_id();
    for info in layout {
        if interpreter.current_buffer_id() != info.buffer_id
            && interpreter.set_current_buffer_id(info.buffer_id).is_err()
        {
            continue;
        }
        // The variable is buffer-local where jit-lock registered it:
        // GNU's handle_fontified_prop reads it with the window's buffer
        // current, so a window whose buffer fontifies must not be
        // skipped because the selected buffer does not.
        if interpreter
            .lookup_var("fontification-functions", env)
            .is_none_or(|value| value.is_nil())
        {
            continue;
        }
        let z = interpreter.buffer.borrow().point_max();
        // Every cell could hold one character (continuation lines
        // included); past that the window cannot reach this frame.
        let start = info.start.min(z);
        let end = info.start.saturating_add(info.width * info.height).min(z);
        fontify_buffer_range(interpreter, env, start, end);
    }
    if interpreter.current_buffer_id() != saved_buffer {
        let _ = interpreter.set_current_buffer_id(saved_buffer);
    }
}

/// One fontification sweep over [START, END) of the current buffer:
/// every position must carry a non-nil `fontified' property or
/// `fontification-functions' runs for it.  The estimate pass above uses
/// the window's cell count; the per-window pass after planning uses the
/// planned window end, which invisible text (org's folds) can push far
/// beyond the cell-count estimate.
fn fontify_buffer_range(interpreter: &mut Interpreter, env: &mut Env, start: usize, end: usize) {
    {
        let mut pos = start;
        let mut rounds = 0usize;
        let round_limit = end.saturating_sub(start).max(512);
        while pos < end && rounds < round_limit {
            rounds += 1;
            let fontified = crate::lisp::primitives::buffer_char_property_at(
                interpreter,
                &interpreter.buffer.borrow(),
                pos,
                "fontified",
            );
            if !fontified.is_nil() {
                let (_, span_end) = interpreter
                    .buffer
                    .borrow()
                    .text_property_interval_around(pos);
                pos = span_end.max(pos + 1);
                continue;
            }
            let functions = interpreter
                .lookup_var("fontification-functions", env)
                .unwrap_or(Value::Nil);
            let Ok(restore) =
                interpreter.bind_special_dynamic("fontification-functions", Value::Nil, env)
            else {
                break;
            };
            run_fontification_functions(interpreter, env, &functions, pos);
            let _ = interpreter.restore_special_dynamic(restore, env);
            let fontified = crate::lisp::primitives::buffer_char_property_at(
                interpreter,
                &interpreter.buffer.borrow(),
                pos,
                "fontified",
            );
            if fontified.is_nil() {
                // The functions declined to mark this position; a loop
                // here could never terminate (GNU simply moves on with
                // the iterator).
                break;
            }
        }
    }
}

// The hook-call shape of handle_fontified_prop: a non-list (or a bare
// lambda) is called directly; a list runs each element, with `t'
// standing for the global value's entries.  Errors are swallowed as
// GNU's dsafe_call1 swallows them.
fn run_fontification_functions(
    interpreter: &mut Interpreter,
    env: &mut Env,
    value: &Value,
    pos: usize,
) {
    let pos_value = Value::Integer(pos as i64);
    let is_bare_lambda = matches!(
        value.car().map(|v| v.kind()),
        Ok(Kind::Symbol(ref name)) if name == "lambda"
    );
    if !matches!(value.kind(), Kind::Cons(_)) || is_bare_lambda {
        let _ =
            interpreter.call_function_value(*value, None, std::slice::from_ref(&pos_value), env);
        return;
    }
    let mut rest = *value;
    while let Kind::Cons(_) = rest.kind() {
        let Ok(function) = rest.car() else {
            break;
        };
        if matches!(function.kind(), Kind::T) {
            let mut globals = interpreter
                .default_value("fontification-functions")
                .unwrap_or(Value::Nil);
            while let Kind::Cons(_) = globals.kind() {
                let Ok(global_fn) = globals.car() else {
                    break;
                };
                if !matches!(global_fn.kind(), Kind::T) {
                    let handlers = interpreter
                        .push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
                    let _ = interpreter.call_function_value(
                        global_fn,
                        None,
                        std::slice::from_ref(&pos_value),
                        env,
                    );
                    interpreter.pop_handler_bindings(handlers);
                }
                let Ok(next) = globals.cdr() else {
                    break;
                };
                globals = next;
            }
        } else {
            // safe_call1: internal_condition_case_n with Qt.
            let handlers =
                interpreter.push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
            let _ = interpreter.call_function_value(
                function,
                None,
                std::slice::from_ref(&pos_value),
                env,
            );
            interpreter.pop_handler_bindings(handlers);
        }
        let Ok(next) = rest.cdr() else {
            break;
        };
        rest = next;
    }
}

fn redraw(interpreter: &mut Interpreter, env: &mut Env, state: &mut TtyState) -> io::Result<()> {
    redraw_with_echo_policy(interpreter, env, state, false)
}

/// The command loop's redraw: keyboard.c calls resize_echo_area_exactly
/// between commands, so a displayed message shrinks the mini window
/// back to the rows it needs (grow-only holds only within a command).
fn redraw_at_command_boundary(
    interpreter: &mut Interpreter,
    env: &mut Env,
    state: &mut TtyState,
) -> io::Result<()> {
    redraw_with_echo_policy(interpreter, env, state, true)
}

fn redraw_with_echo_policy(
    interpreter: &mut Interpreter,
    env: &mut Env,
    state: &mut TtyState,
    exact_echo: bool,
) -> io::Result<()> {
    state.activate_terminal(interpreter.selected_terminal_id());
    let (cols, rows) = crate::lisp::eval::terminal::output_size()?;
    let cols = cols.max(10) as usize;
    let rows = rows.max(4) as usize;
    // The frame takes the terminal's size when the session starts on it
    // (init_display_interactive), and at the next redisplay after the
    // terminal changes size (handle_window_change_signal's delayed
    // change_frame_size); redisplay_internal first applies what is
    // pending, `menu-bar-lines' changes included.
    match state.terminal_size {
        None => interpreter.set_tty_frame_size(cols as i64, rows as i64),
        Some(size) if size != (cols, rows) => {
            interpreter.tty_window_change(cols as i64, rows as i64)
        }
        Some(_) => {}
    }
    state.terminal_size = Some((cols, rows));
    let _ = crate::lisp::primitives::do_pending_window_change(interpreter, env);
    // The frame's size as it stands: set-frame-size can make it smaller
    // than the terminal, whose remaining rows and columns stay as they
    // were.
    let cols = cols.min(interpreter.frame_width().max(10) as usize);
    let rows = rows.min(interpreter.frame_height().max(4) as usize);
    run_pre_redisplay_function(interpreter, env);
    // Rows above the window tree belong to the frame's menu and tab bars.
    let menu_lines = crate::lisp::primitives::frame_top_margin(interpreter.selected_frame_id)
        .clamp(0, rows as i64 - 2) as usize;
    // The mini window's height comes first: a tall message shrinks the
    // window tree above it (GNU's resize_mini_window, grow-only policy).
    let max_mini = max_mini_window_rows(interpreter, rows);
    state.echo_max_rows = max_mini.max(1);
    let frontend_echo_early = state.echo.clone();
    let (echo_long, echo_from_minibuffer, echo_cursor) = compose_echo_row(
        interpreter,
        env,
        &frontend_echo_early,
        cols * max_mini.max(1),
        &mut state.face_cache,
    );
    let echo_paint = wrap_echo_paint(&echo_long, cols, max_mini.max(1));
    let minibuffer_activation_id = interpreter.active_minibuffer_activation_id();
    let new_minibuffer_activation =
        echo_from_minibuffer && state.minibuffer_activation_id != minibuffer_activation_id;
    state.minibuffer_activation_id = minibuffer_activation_id;
    let echo_empty = echo_paint.len() == 1
        && echo_paint[0].text.iter().all(|c| *c == ' ')
        && !echo_from_minibuffer;
    state.echo_rows = if echo_empty {
        1
    } else if new_minibuffer_activation {
        // read_minibuf starts a fresh mini-window sizing lifecycle even
        // when get_minibuffer reuses the same ` *Minibuf-N*' buffer.  Keep
        // grow-only behavior within this read, never across activations.
        echo_paint.len().min(max_mini.max(1))
    } else if exact_echo && !echo_from_minibuffer {
        // resize_echo_area_exactly: at a command boundary a displayed
        // message sizes the mini window to exactly its rows; an active
        // minibuffer keeps its grown size (echo_area_buffer[0] is nil
        // there, so keyboard.c never resizes it exactly).
        echo_paint.len().min(max_mini.max(1))
    } else {
        state.echo_rows.max(echo_paint.len()).min(max_mini.max(1))
    };
    // GNU's grow_mini_window/shrink_mini_window resize the real window
    // tree.  Reconcile against the tree's live height, rather than applying
    // the change in frontend echo rows as a blind delta: restoring a
    // minibuffer window configuration restores the root geometry before
    // redisplay, while the frontend still remembers the formerly tall
    // minibuffer.  Applying that stale delta grows the root past the frame
    // and makes the next split too tall.
    let desired_root_height = rows
        .saturating_sub(state.echo_rows)
        .saturating_sub(menu_lines)
        .max(1) as i64;
    // Failures from the Lisp resizer are deliberately not propagated: the
    // render-level clamp below stays as the safety net when window.el's
    // resizer declines (grow_mini_window likewise gives up quietly when
    // the root cannot shrink).
    let _ = (|interpreter: &mut Interpreter, env: &mut Env| -> Result<(), LispError> {
        let root = call(interpreter, env, "frame-root-window", &[])?;
        let current = call(
            interpreter,
            env,
            "window-total-height",
            std::slice::from_ref(&root),
        )?
        .as_integer()?;
        let delta = desired_root_height - current;
        if delta == 0 {
            return Ok(());
        }
        let recovered = call(
            interpreter,
            env,
            "window--resize-root-window-vertically",
            &[root, Value::Integer(delta), Value::T],
        )?;
        crate::lisp::primitives::apply_staged_root_window_height(
            interpreter,
            recovered.as_integer().unwrap_or(0),
        )?;
        Ok(())
    })(interpreter, env);
    // xdisp.c:redisplay_internal runs the window change functions once
    // the mini window has its size; their errors are already muted.
    let _ = crate::lisp::primitives::run_window_change_functions(interpreter, env);
    let frame_rows = rows - state.echo_rows; // everything above the echo area
    let full_repaint = state.painted_size != (cols, rows);
    if full_repaint {
        // A resize changes the wrap geometry under every saved segment
        // index; re-anchor each window instead of trusting stale ones.
        state.views.clear();
    }
    // Resolved faces stay cached until a face definition changes (GNU's
    // face_change flag) — resolution evaluates Lisp per attribute.
    let face_generation = interpreter.face_definitions_generation();
    let face_changed = state.face_cache_generation != face_generation;
    if full_repaint || face_changed {
        state.face_cache.clear();
        state.face_cache_generation = face_generation;
    }

    let mut layout = crate::lisp::primitives::window_render_layout(interpreter);
    // The Lisp window records assume a one-row echo area; when the mini
    // window grew, the windows overlapping the reclaimed rows give them
    // up (grow_mini_window shrinks the tree above the same way).
    for window in &mut layout {
        if window.top + window.height > frame_rows {
            window.height = frame_rows.saturating_sub(window.top).max(2);
        }
    }
    let layout_fits = !layout.is_empty()
        && layout.iter().all(|window| {
            window.width >= 2
                && window.height >= 2
                && window.left + window.width <= cols
                && window.top + window.height <= frame_rows
        });
    if !layout_fits {
        // No window tree, or one that disagrees with the glass (frame
        // records mid-rebuild): render the selected buffer full-frame.
        let buffer = interpreter.buffer.borrow();
        layout = vec![crate::lisp::primitives::WindowRenderInfo {
            window_id: interpreter.selected_window_id(),
            buffer_id: interpreter.current_buffer_id(),
            left: 0,
            top: menu_lines,
            width: cols,
            height: frame_rows - menu_lines,
            start: crate::lisp::primitives::current_window_start(interpreter)
                .clamp(buffer.point_min(), buffer.point_max()),
            point: buffer.point(),
            selected: true,
        }];
    }
    state
        .views
        .retain(|id, _| layout.iter().any(|window| window.window_id == *id));

    // Fontify what this frame will show before any text is snapshotted:
    // GNU's iterator handles the `fontified' property before producing
    // glyphs (xdisp.c handle_fontified_prop).
    fontify_window_ranges(interpreter, env, &layout);

    let mut frame = vec![PaintRow::blank(cols); frame_rows];
    // GNU's tty vertical border draws in the vertical-border face (its
    // inherit chain reaches mode-line-inactive's reverse video).
    let divider_attrs = if layout.iter().any(|info| info.left + info.width < cols) {
        *state
            .face_cache
            .entry("vertical-border".into())
            .or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(
                    interpreter,
                    env,
                    &Value::Symbol("vertical-border".into()),
                )
            })
    } else {
        CellAttrs::default()
    };
    // xdisp.c's windows_or_buffers_changed for this redisplay: set by the
    // interpreter (wset_redisplay of another window, window-tree and
    // frame changes), by a resize or face change (face_change), by window
    // edges differing from the last ones shown (a mini window's growth),
    // and by bset_redisplay's rule: a buffer modified since then that
    // shows in a window other than the selected one, or in two windows.
    let windows_or_buffers_changed = {
        // Window edges only: a buffer shown or a window selected marks
        // the windows involved (wset_redisplay) when GNU does.
        let signature: Vec<(u64, usize, usize, usize, usize)> = layout
            .iter()
            .map(|info| (info.window_id, info.left, info.top, info.width, info.height))
            .collect();
        let mut changed = std::mem::take(&mut interpreter.windows_or_buffers_changed)
            || full_repaint
            || face_changed
            || signature != state.last_layout;
        let selected_buffer = layout
            .iter()
            .find(|info| info.selected)
            .map(|info| info.buffer_id);
        let mut seen = std::collections::HashSet::new();
        for info in &layout {
            if !seen.insert(info.buffer_id) {
                continue;
            }
            let modiff = if info.buffer_id == interpreter.current_buffer_id() {
                interpreter.buffer.borrow().modified_tick()
            } else if let Some(buffer) = interpreter.get_buffer_by_id(info.buffer_id) {
                buffer.modified_tick()
            } else {
                continue;
            };
            let shown = layout
                .iter()
                .filter(|other| other.buffer_id == info.buffer_id)
                .count();
            if (shown > 1 || Some(info.buffer_id) != selected_buffer)
                && state.buffer_modiffs.get(&info.buffer_id) != Some(&modiff)
            {
                changed = true;
            }
        }
        state.last_layout = signature;
        changed
    };
    let mut cursor_position = (0u16, 0u16);
    let mut selected_sync: Option<crate::lisp::primitives::InteractiveWindowMetrics> = None;
    struct ModeLineJob {
        window_id: u64,
        buffer_id: u64,
        point: usize,
        row: usize,
        left: usize,
        body_width: usize,
        metrics: crate::lisp::primitives::InteractiveWindowMetrics,
        /// The window's mode line shows in `mode-line-active'.
        active: bool,
    }
    let mut mode_line_jobs: Vec<ModeLineJob> = Vec::new();
    // A tab line and header line consume body rows above the window text,
    // in that order.  Both render through display-mode-line machinery in
    // their respective faces and leave at least one text row visible.
    let mut tab_line_jobs: Vec<ModeLineJob> = Vec::new();
    let mut header_line_jobs: Vec<ModeLineJob> = Vec::new();
    // Face spans over window text apply after the text lands (their
    // resolution evaluates Lisp, which the buffer borrow above forbids).
    struct TextFaceJob {
        window_id: u64,
        buffer_id: u64,
        selected: bool,
        top: usize,
        /// Left edge of the text area (past any line-number column).
        left: usize,
        lnum_cols: usize,
        truncate: bool,
        body_width: usize,
        /// Reserved cells immediately after the text body.  An `:extend'
        /// face covering the newline paints through this margin too.
        right_margin: usize,
        usable: usize,
        start: usize,
        window_end: usize,
        rows: Vec<(usize, usize, usize, usize)>,
    }
    let mut text_face_jobs: Vec<TextFaceJob> = Vec::new();
    // A window with `display-line-numbers' paints its number column
    // after the text lands, exactly like the face spans: resolving the
    // line-number faces evaluates Lisp.
    struct LineNumberJob {
        window_id: u64,
        buffer_id: u64,
        layout: crate::lisp::primitives::LineNumberLayout,
        top: usize,
        left: usize,
        text_rows: usize,
        truncate: bool,
        text_width: usize,
        point: usize,
        window_end: usize,
        cursor_row: Option<usize>,
        rows: Vec<(usize, usize, usize, usize)>,
    }
    // A string whose `display' property targets a window margin (most
    // visibly Flymake's TTY diagnostic indicators).  The source string
    // lives in an overlay before-string; the displayed payload and its
    // faces paint into the reserved margin rather than the text body.
    struct MarginStringJob {
        row: usize,
        left: usize,
        width: usize,
        value: Value,
    }
    let mut margin_string_jobs: Vec<MarginStringJob> = Vec::new();
    let mut line_number_jobs: Vec<LineNumberJob> = Vec::new();
    // GNU's overlay arrow on a tty: a variable in
    // `overlay-arrow-variable-list' holding a marker overlays its
    // string ("=>", or the symbol's `overlay-arrow-string' property)
    // over the first cells of the row starting the marker's line —
    // after the face spans land, since the arrow glyphs carry the
    // default face over whatever the text properties painted.
    let mut overlay_arrow_jobs: Vec<(usize, usize, String)> = Vec::new();

    'windows: for info in &layout {
        // A window not flush with the frame's right edge spends its last
        // column on the vertical border; the mode line spans the body.
        let body_width = if info.left + info.width < cols {
            info.width - 1
        } else {
            info.width
        };
        let has_header_line = interpreter
            .buffer_local_value(info.buffer_id, "header-line-format")
            .or_else(|| interpreter.default_value("header-line-format"))
            .is_some_and(|format| format.is_truthy());
        let has_tab_line = interpreter
            .buffer_local_value(info.buffer_id, "tab-line-format")
            .or_else(|| interpreter.default_value("tab-line-format"))
            .is_some_and(|format| format.is_truthy());
        let tab_rows = usize::from(has_tab_line && info.height > 2);
        let header_rows = usize::from(has_header_line && info.height > 2 + tab_rows);
        let text_top = info.top + tab_rows + header_rows;
        let text_rows = info.height - 1 - tab_rows - header_rows;
        // `display-line-numbers': the column's width follows the
        // window's start line, which planning itself may move
        // (recentering); iterate until the width the plan was laid out
        // with matches the width its final top line asks for.
        let (left_margin, right_margin) = interpreter.window_margins(info.window_id);
        let left_margin = left_margin.unwrap_or(0).max(0) as usize;
        let right_margin = right_margin.unwrap_or(0).max(0) as usize;
        let left_margin = left_margin.min(body_width.saturating_sub(1));
        let right_margin = right_margin.min(body_width.saturating_sub(left_margin + 1));
        let text_body_width = body_width.saturating_sub(left_margin + right_margin).max(1);
        let lnum_for = |interpreter: &Interpreter, top_line: usize| {
            let buffer = if info.buffer_id == interpreter.current_buffer_id() {
                Some(interpreter.buffer.borrow())
            } else {
                interpreter.get_buffer_by_id(info.buffer_id)
            }?;
            let begv_line = buffer.line_number_at_pos(buffer.point_min());
            let point = info.point.clamp(buffer.point_min(), buffer.point_max());
            let point_line = buffer.line_number_at_pos(point);
            crate::lisp::primitives::window_line_number_layout(
                interpreter,
                info.buffer_id,
                top_line.max(begv_line),
                point_line,
                begv_line,
                text_rows,
            )
        };
        let mut lnum = lnum_for(
            interpreter,
            state
                .views
                .get(&info.window_id)
                .map(|view| view.top_line)
                .unwrap_or(0),
        );
        let previous_view = state
            .views
            .get(&info.window_id)
            .copied()
            .unwrap_or_default();
        let (mut start_state, restriction, modiff) = window_start_state(
            interpreter,
            info,
            &previous_view,
            text_rows,
            tab_rows + header_rows,
            windows_or_buffers_changed,
        );
        let mut hook_rounds = 0usize;
        let (geometry, plan, chosen, invisibility_active, chars_modiff) = loop {
            let outcome = loop {
                let geometry =
                    window_render_geometry(interpreter, env, info, text_body_width, cols, lnum);
                let view = state.views.entry(info.window_id).or_default();
                let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
                    Some(interpreter.buffer.borrow())
                } else {
                    interpreter.get_buffer_by_id(info.buffer_id)
                }) else {
                    continue 'windows;
                };
                let invisibility =
                    resolve_buffer_invisibility(interpreter, &buffer, info.buffer_id);
                let glyphless =
                    CharacterDisplayContext::new(interpreter, info.buffer_id, info.window_id)
                        .with_line_numbers(geometry.lnum.map_or(0, |layout| layout.cols));
                match plan_window_text(
                    &buffer,
                    &invisibility,
                    Some(&glyphless),
                    view,
                    &start_state,
                    text_rows,
                    text_body_width,
                    geometry.truncate,
                    &geometry,
                    info.selected,
                ) {
                    PlanOutcome::Hook {
                        stage,
                        start,
                        consumed,
                    } => break Err((stage, start, consumed)),
                    PlanOutcome::Planned {
                        plan,
                        chosen,
                        consumed,
                    } => {
                        let top_line = view.top_line;
                        let settled = lnum_for(interpreter, top_line);
                        if settled.map(|layout| layout.cols) == lnum.map(|layout| layout.cols) {
                            break Ok((
                                geometry,
                                plan,
                                chosen,
                                consumed,
                                invisibility.active,
                                buffer.chars_modified_tick(),
                            ));
                        }
                        lnum = settled;
                    }
                }
            };
            match outcome {
                Err((stage, start, consumed)) => {
                    // run_window_scroll_functions: the hooks see the start
                    // redisplay chose, and redisplay continues from the
                    // start they leave.
                    consume_window_start_flags(interpreter, info.window_id, consumed);
                    crate::lisp::primitives::set_window_start_marker(
                        interpreter,
                        info.window_id,
                        start,
                    );
                    run_window_scroll_functions(
                        interpreter,
                        env,
                        info.window_id,
                        info.buffer_id,
                        start,
                    );
                    start_state.resume = Some((
                        stage,
                        crate::lisp::primitives::window_start_marker(
                            interpreter,
                            info.window_id,
                            info.buffer_id,
                        ),
                    ));
                    hook_rounds += 1;
                    if hook_rounds >= 4 {
                        start_state.scroll_functions = false;
                    }
                }
                Ok((geometry, plan, chosen, consumed, active, chars)) => {
                    consume_window_start_flags(interpreter, info.window_id, consumed);
                    break (geometry, plan, chosen, active, chars);
                }
            }
        };
        let info = &publish_window_start(interpreter, info, &chosen, plan.window_end);
        let newly_deferred_point = mode_line_display_point(
            previous_view,
            info.buffer_id,
            info.point,
            plan.cursor,
            plan.top_pos,
            chars_modiff,
            invisibility_active,
        );
        let existing_deferred_point = previous_view.deferred_mode_line_point.filter(|_| {
            invisibility_active
                && previous_view.last_buffer_id == Some(info.buffer_id)
                && previous_view.last_top_pos == Some(plan.top_pos)
                && previous_view.last_chars_modiff == Some(chars_modiff)
        });
        let mode_line_point = existing_deferred_point.unwrap_or(newly_deferred_point);
        // The pre-pass fontified a cell-count estimate of the window;
        // the plan knows the real end, which invisible text (org's
        // folds) can push thousands of characters past the estimate.
        // Fontify exactly the VISIBLE stretches: GNU's iterator jumps
        // invisible runs, so hidden text is never fontified — a folded
        // src block must not load its language mode (whose setup
        // message would land in the echo area) until it unfolds.
        {
            let visible_ranges: Vec<(usize, usize)> = {
                let window_buffer = if info.buffer_id == interpreter.current_buffer_id() {
                    Some(interpreter.buffer.borrow())
                } else {
                    interpreter.get_buffer_by_id(info.buffer_id)
                };
                if let Some(buffer) = window_buffer {
                    let spec = resolve_buffer_invisibility(interpreter, &buffer, info.buffer_id);
                    let z = buffer.point_max();
                    let mut pos = plan.top_pos.min(z);
                    let end = plan.window_end.min(z);
                    let mut ranges = Vec::new();
                    if !spec.active {
                        ranges.push((pos, end));
                    } else {
                        while pos < end {
                            if crate::lisp::primitives::invisible_class_at(&buffer, &spec, pos) != 0
                            {
                                let run_end =
                                    crate::lisp::primitives::invisible_run_at(&buffer, &spec, pos)
                                        .map(|(run_end, _)| run_end)
                                        .unwrap_or(pos + 1);
                                pos = run_end.max(pos + 1);
                                continue;
                            }
                            let visible_start = pos;
                            while pos < end
                                && crate::lisp::primitives::invisible_class_at(&buffer, &spec, pos)
                                    == 0
                            {
                                pos += 1;
                            }
                            ranges.push((visible_start, pos));
                        }
                    }
                    ranges
                } else {
                    Vec::new()
                }
            };
            let saved = interpreter.current_buffer_id();
            if saved == info.buffer_id || interpreter.set_current_buffer_id(info.buffer_id).is_ok()
            {
                if interpreter
                    .lookup_var("fontification-functions", env)
                    .is_some_and(|value| !value.is_nil())
                {
                    for (start, end) in visible_ranges {
                        fontify_buffer_range(interpreter, env, start, end);
                    }
                }
                if interpreter.current_buffer_id() != saved {
                    let _ = interpreter.set_current_buffer_id(saved);
                }
            }
        }
        let lnum_cols = geometry.lnum.map_or(0, |layout| layout.cols);
        let text_left = info.left + left_margin + lnum_cols;
        for (row, (rendered, _, _, _, _)) in plan.rendered.iter().enumerate() {
            frame[text_top + row].blit(text_left, rendered, CellAttrs::default());
        }
        {
            let mut variables = interpreter
                .lookup_var(
                    "overlay-arrow-variable-list",
                    &crate::lisp::types::Env::new(),
                )
                .unwrap_or(Value::Nil);
            while let Kind::Cons(_) = variables.kind() {
                let Ok(symbol_value) = variables.car() else {
                    break;
                };
                variables = variables.cdr().unwrap_or(Value::Nil);
                let Ok(symbol) = symbol_value.as_symbol() else {
                    continue;
                };
                let Some(Kind::Marker(marker)) = interpreter
                    .lookup_var(symbol, &crate::lisp::types::Env::new())
                    .map(|v| v.kind())
                else {
                    continue;
                };
                if interpreter.marker_buffer_id(marker) != Some(info.buffer_id) {
                    continue;
                }
                let Some(position) = interpreter.marker_position(marker) else {
                    continue;
                };
                let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
                    Some(interpreter.buffer.borrow())
                } else {
                    interpreter.get_buffer_by_id(info.buffer_id)
                }) else {
                    continue;
                };
                let line_start =
                    buffer.line_start_at(position.clamp(buffer.point_min(), buffer.point_max()));
                let Some(row) = plan.rendered.iter().position(|(_, _, seg, start, _)| {
                    *seg == 0 && *start != usize::MAX && *start == line_start
                }) else {
                    continue;
                };
                let arrow = interpreter
                    .get_symbol_property(symbol, "overlay-arrow-string")
                    .filter(|value| value.is_string())
                    .or_else(|| {
                        interpreter
                            .lookup_var("overlay-arrow-string", &crate::lisp::types::Env::new())
                    })
                    .and_then(|value| value.as_string().ok())
                    .unwrap_or_else(|| "=>".to_string());
                let arrow: String = arrow
                    .chars()
                    .take(text_body_width.saturating_sub(lnum_cols))
                    .collect();
                overlay_arrow_jobs.push((text_top + row, text_left, arrow));
            }
        }
        {
            let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
                Some(interpreter.buffer.borrow())
            } else {
                interpreter.get_buffer_by_id(info.buffer_id)
            }) else {
                continue 'windows;
            };
            for overlay in &buffer.overlays {
                if overlay.is_dead() {
                    continue;
                }
                for name in ["before-string", "after-string"] {
                    let Some(value) = overlay.get_prop(&Value::Symbol(name.into())) else {
                        continue;
                    };
                    let Some((side, payload)) = window_margin_display(&value) else {
                        continue;
                    };
                    let (margin_left, margin_width) = match side.as_str() {
                        "left-margin" if left_margin > 0 => (info.left, left_margin),
                        "right-margin" if right_margin > 0 => {
                            (info.left + body_width - right_margin, right_margin)
                        }
                        _ => continue,
                    };
                    let line_start = buffer
                        .line_start_at(overlay.beg().clamp(buffer.point_min(), buffer.point_max()));
                    let Some(row) = plan.rendered.iter().position(|(_, _, seg, start, _)| {
                        *seg == 0 && *start != usize::MAX && *start == line_start
                    }) else {
                        continue;
                    };
                    margin_string_jobs.push(MarginStringJob {
                        row: text_top + row,
                        left: margin_left,
                        width: margin_width,
                        value: payload,
                    });
                }
            }
            // handle_single_display_spec: buffer text whose `display'
            // puts a string in a margin shows it there, on the row where
            // the property's run is displayed, after any margin glyphs
            // the row already has.
            if buffer.has_text_property_named("display") {
                let end = plan.window_end.min(buffer.point_max());
                let from = plan.top_pos.clamp(buffer.point_min(), end);
                let mut used: Vec<(usize, bool, usize)> = Vec::new();
                let mut starts = Vec::new();
                let _ = buffer.try_for_each_interval(from, end, |start, _, plist| {
                    if !plist.is_nil() {
                        starts.push(start.max(from));
                    }
                    Ok::<(), ()>(())
                });
                for position in starts {
                    let Some(display) = buffer.text_property_at(position, "display") else {
                        continue;
                    };
                    let Some((side, payload)) = buffer_margin_display(&display) else {
                        continue;
                    };
                    let Ok(text) = crate::lisp::primitives::string_text(&payload) else {
                        continue;
                    };
                    let right = side == "right-margin";
                    let (margin_left, margin_width) = match right {
                        false if left_margin > 0 => (info.left, left_margin),
                        true if right_margin > 0 => {
                            (info.left + body_width - right_margin, right_margin)
                        }
                        _ => continue,
                    };
                    let Some(row) = plan.rendered.iter().rposition(|(_, _, _, start, _)| {
                        *start != usize::MAX && *start <= position
                    }) else {
                        continue;
                    };
                    let offset = match used.iter_mut().find(|(r, s, _)| *r == row && *s == right) {
                        Some((_, _, cols)) => {
                            let offset = *cols;
                            *cols += text.chars().count();
                            offset
                        }
                        None => {
                            used.push((row, right, text.chars().count()));
                            0
                        }
                    };
                    if offset >= margin_width {
                        continue;
                    }
                    margin_string_jobs.push(MarginStringJob {
                        row: text_top + row,
                        left: margin_left + offset,
                        width: margin_width - offset,
                        value: payload,
                    });
                }
            }
        }
        if let Some(layout) = geometry.lnum {
            line_number_jobs.push(LineNumberJob {
                window_id: info.window_id,
                buffer_id: info.buffer_id,
                layout,
                top: text_top,
                left: info.left + left_margin,
                text_rows,
                truncate: geometry.truncate,
                text_width: text_body_width.saturating_sub(lnum_cols).max(1),
                point: info.point,
                window_end: plan.window_end,
                cursor_row: if info.selected {
                    plan.cursor.map(|(row, _)| row)
                } else {
                    None
                },
                rows: plan
                    .rendered
                    .iter()
                    .map(|(_, line, seg, start, row_hscroll)| (*line, *seg, *start, *row_hscroll))
                    .collect(),
            });
        }
        text_face_jobs.push(TextFaceJob {
            window_id: info.window_id,
            buffer_id: info.buffer_id,
            selected: info.selected,
            top: text_top,
            left: text_left,
            lnum_cols,
            truncate: geometry.truncate,
            body_width: text_body_width.saturating_sub(lnum_cols).max(1),
            right_margin,
            usable: text_body_width
                .saturating_sub(lnum_cols)
                .saturating_sub(1)
                .max(1),
            start: plan.top_pos,
            window_end: plan.window_end,
            rows: plan
                .rendered
                .iter()
                .map(|(_, line, seg, start, row_hscroll)| (*line, *seg, *start, *row_hscroll))
                .collect(),
        });
        if body_width < info.width {
            for row in 0..info.height {
                frame[info.top + row].blit(info.left + body_width, "|", divider_attrs);
            }
        }
        let metrics = crate::lisp::primitives::InteractiveWindowMetrics {
            text_height: text_rows,
            window_end: plan.window_end,
        };
        if info.selected {
            if let Some((row, col)) = plan.cursor {
                cursor_position = (
                    (info.left + left_margin + col).min(cols - 1) as u16,
                    (text_top + row).min(frame_rows - 1) as u16,
                );
            }
            selected_sync = Some(metrics);
        }
        {
            let view = state.views.entry(info.window_id).or_default();
            view.last_start = Some(info.start);
            view.last_restriction = Some(restriction);
            view.last_modiff = Some(modiff);
            view.last_buffer_id = Some(info.buffer_id);
            view.last_point = Some(info.point);
            view.last_cursor = plan.cursor;
            view.last_top_pos = Some(plan.top_pos);
            view.last_chars_modiff = Some(chars_modiff);
            view.deferred_mode_line_point =
                (mode_line_point != info.point).then_some(mode_line_point);
        }
        let mode_line_active = info.selected
            || interpreter
                .lookup_var("mode-line-in-non-selected-windows", env)
                .is_some_and(|value| value.is_nil())
            || (interpreter.minibuffer_depth() > 0
                && interpreter.minibuffer_selected_window_id() == Some(info.window_id)
                && interpreter.selected_window_id() == interpreter.minibuffer_window_id());
        mode_line_jobs.push(ModeLineJob {
            window_id: info.window_id,
            buffer_id: info.buffer_id,
            point: mode_line_point,
            row: info.top + info.height - 1,
            left: info.left,
            body_width,
            metrics,
            active: mode_line_active,
        });
        if header_rows > 0 {
            header_line_jobs.push(ModeLineJob {
                window_id: info.window_id,
                buffer_id: info.buffer_id,
                point: mode_line_point,
                row: info.top + tab_rows,
                left: info.left,
                body_width,
                metrics,
                active: mode_line_active,
            });
        }
        if tab_rows > 0 {
            tab_line_jobs.push(ModeLineJob {
                window_id: info.window_id,
                buffer_id: info.buffer_id,
                point: mode_line_point,
                row: info.top,
                left: info.left,
                body_width,
                metrics,
                active: mode_line_active,
            });
        }
    }

    // Publish the selected window's displayed geometry to the interpreter
    // (each window's start and end went to its record as it was planned).
    // This precedes the mode-line renders.
    if let Some(metrics) = selected_sync {
        crate::lisp::primitives::set_interactive_window_metrics(Some(metrics));
    }
    // The shown buffers' modification counts as of this redisplay (after
    // its fontification): bset_redisplay's flag is cleared with the rest
    // of windows_or_buffers_changed at the end of redisplay_internal.
    state.buffer_modiffs = layout
        .iter()
        .filter_map(|info| {
            let modiff = if info.buffer_id == interpreter.current_buffer_id() {
                interpreter.buffer.borrow().modified_tick()
            } else {
                interpreter
                    .get_buffer_by_id(info.buffer_id)?
                    .modified_tick()
            };
            Some((info.buffer_id, modiff))
        })
        .collect();

    // Face spans over window text — buffer `face' properties, the
    // selected window's active region, and overlay faces (isearch's
    // highlights) — resolved through the face machinery and mapped onto
    // cells through each row's line/segment anchors.
    for job in &text_face_jobs {
        if job.start >= job.window_end {
            continue;
        }
        let spans = crate::lisp::primitives::window_face_spans(
            interpreter,
            env,
            job.buffer_id,
            job.start,
            job.window_end,
            job.selected,
        );
        let resolved: Vec<(usize, usize, CellAttrs)> = spans
            .iter()
            .map(|(begin, end, face)| {
                let key = format!("{face}");
                let attrs = *state.face_cache.entry(key).or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, face)
                });
                (*begin, *end, attrs)
            })
            .collect();
        let glyphless_attrs = *state
            .face_cache
            .entry("glyphless-char".into())
            .or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(
                    interpreter,
                    env,
                    &Value::Symbol("glyphless-char".into()),
                )
            });
        state.face_cache.entry("default".into()).or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs(
                interpreter,
                env,
                &Value::Symbol("default".into()),
            )
        });
        let mut display_face_jobs = Vec::new();
        let mut ellipsis_jobs = Vec::new();
        let buffer = if job.buffer_id == interpreter.current_buffer_id() {
            interpreter.buffer.borrow()
        } else {
            match interpreter.get_buffer_by_id(job.buffer_id) {
                Some(buffer) => buffer,
                None => continue,
            }
        };
        let job_invisibility = resolve_buffer_invisibility(interpreter, &buffer, job.buffer_id);
        let glyphless = CharacterDisplayContext::new(interpreter, job.buffer_id, job.window_id)
            .with_line_numbers(job.lnum_cols);
        for (index, (line, seg, row_start, row_hscroll)) in job.rows.iter().enumerate() {
            if *row_start == usize::MAX {
                continue;
            }
            let row_end = job
                .rows
                .get(index + 1)
                .map(|(_, _, next, _)| *next)
                .filter(|next| *next != usize::MAX)
                .unwrap_or(job.window_end);
            let visual =
                glyphless_visual_line_at(&buffer, &job_invisibility, *line, Some(&glyphless));
            let laid = layout::lay_out_visual_line(&visual, job.body_width, job.truncate);
            let column_of = |index: usize| laid.columns[index.min(laid.columns.len() - 1)];
            let line_width = laid.columns.last().copied().unwrap_or(0);
            let wrapped = (!job.truncate).then_some(&laid.rows);
            let segment = wrapped.and_then(|segments| segments.get(*seg));
            let segment_start = segment.map_or(0, |segment| segment.start_column);
            let segment_visible_end = segment.map_or(usize::MAX, |segment| {
                segment
                    .end_column
                    .min(segment.start_column + cell_count(&segment.text))
            });
            let hscroll_layout =
                HscrollLayout::new(&visual, &laid.columns, *row_hscroll, job.lnum_cols == 0);
            let line_begin = buffer.display_line_start_of(*line);
            // The right-truncation `$' glyph keeps the default face
            // (produce_special_glyphs); spans stop one cell short of it.
            // A continued row's glyphs end at its own end column: a
            // word-wrapped row before the full width (GNU paints the rest
            // only from a face with `:extend', extend_face_to_end_of_line),
            // a `\' row one cell short of it.
            let continued_row = wrapped.is_some_and(|segments| *seg + 1 < segments.len());
            let col_cap = (if continued_row {
                segment.map_or(job.usable, |segment| {
                    (segment.end_column - segment.start_column).min(job.usable)
                })
            } else {
                job.body_width
            }) - usize::from(
                job.truncate && truncated_on_right(line_width, *row_hscroll, job.body_width),
            );
            for (span_begin, span_end, attrs) in &resolved {
                let begin = (*span_begin).max(*row_start);
                let end = (*span_end).min(row_end);
                if begin >= end {
                    continue;
                }
                let display_index_of = |pos: usize| {
                    let offset = pos.saturating_sub(line_begin);
                    visual
                        .map
                        .get(offset)
                        .copied()
                        .unwrap_or_else(|| visual.map.last().copied().unwrap_or(offset))
                };
                // A span lying entirely inside hidden text occupies no
                // display cells: nothing to paint (its newline included).
                if display_index_of(begin) == display_index_of(end)
                    && begin.saturating_sub(line_begin) < visual.map.len().saturating_sub(1)
                {
                    continue;
                }
                // With a line-number column the left `$' replaces the
                // column's first glyph, so text column HSCROLL itself is
                // visible at text column zero; without one, the `$' owns
                // column zero and text resumes from column one.
                let col_of = |pos: usize| {
                    let offset = display_index_of(pos);
                    let column = column_of(offset);
                    if *row_hscroll > 0 {
                        hscroll_layout
                            .screen_column(column)
                            .unwrap_or(hscroll_layout.screen_start)
                    } else {
                        column.saturating_sub(segment_start)
                    }
                };
                let from_col = col_of(begin).min(col_cap);
                // A span covering the newline paints its glyph's cell —
                // and an `:extend' face (the region) keeps painting to
                // the window edge, GNU's whole-row highlight.
                let line_chars = visual.map.len().saturating_sub(1);
                let mut to_col = col_of(end);
                let covers_newline = end > begin && buffer.char_at(end - 1) == Some('\n');
                // A word-wrapped row extends an `:extend' face that runs on
                // into the next row (back_to_wrap's
                // extend_face_to_end_of_line); a `\' row is already full.
                let extends_wrapped = attrs.extend
                    && continued_row
                    && *span_end > row_end
                    && segment.is_some_and(|segment| !segment.continued_glyph);
                let extends_row = extends_wrapped
                    || (attrs.extend
                        && (covers_newline || end.saturating_sub(line_begin) > line_chars));
                if end.saturating_sub(line_begin) > line_chars {
                    to_col = if attrs.extend {
                        job.body_width + job.right_margin
                    } else {
                        to_col + 1
                    };
                } else if (covers_newline && attrs.extend) || extends_wrapped {
                    to_col = job.body_width + job.right_margin;
                }
                let to_col = to_col.min(if extends_row {
                    job.body_width + job.right_margin
                } else {
                    col_cap
                });
                if from_col >= to_col {
                    continue;
                }
                frame[job.top + index].overlay(job.left + from_col, job.left + to_col, *attrs);
            }
            // Terminal no-font glyphs merge `glyphless-char' over the face
            // already selected for the source character.  Apply them after
            // buffer/overlay faces so unspecified attributes inherit from
            // that underlying face exactly as merge_glyphless_glyph_face does.
            for &(span_begin, span_end) in &visual.glyphless_spans {
                let begin_column = column_of(span_begin).max(segment_start);
                let end_column = column_of(span_end).min(segment_visible_end);
                if begin_column >= end_column {
                    continue;
                }
                let (from_col, to_col) = if *row_hscroll > 0 {
                    (
                        hscroll_layout
                            .screen_column(begin_column)
                            .unwrap_or(hscroll_layout.screen_start),
                        hscroll_layout
                            .screen_column(end_column)
                            .unwrap_or(hscroll_layout.screen_start),
                    )
                } else {
                    (
                        begin_column.saturating_sub(segment_start),
                        end_column.saturating_sub(segment_start),
                    )
                };
                let from_col = from_col.min(col_cap);
                let to_col = to_col.min(col_cap);
                if from_col < to_col {
                    frame[job.top + index].overlay(
                        job.left + from_col,
                        job.left + to_col,
                        glyphless_attrs,
                    );
                }
            }
            for (span_begin, span_end, face) in &visual.display_face_spans {
                let begin_column = column_of(*span_begin).max(segment_start);
                let end_column = column_of(*span_end).min(segment_visible_end);
                if begin_column >= end_column {
                    continue;
                }
                let (from_col, to_col) = if *row_hscroll > 0 {
                    (
                        hscroll_layout
                            .screen_column(begin_column)
                            .unwrap_or(hscroll_layout.screen_start),
                        hscroll_layout
                            .screen_column(end_column)
                            .unwrap_or(hscroll_layout.screen_start),
                    )
                } else {
                    (
                        begin_column.saturating_sub(segment_start),
                        end_column.saturating_sub(segment_start),
                    )
                };
                let from_col = from_col.min(col_cap);
                let to_col = to_col.min(col_cap);
                if from_col < to_col {
                    display_face_jobs.push((
                        job.top + index,
                        job.left + from_col,
                        job.left + to_col,
                        *face,
                    ));
                }
            }
            // The row's prefix cells (handle_line_prefix): a stretch
            // takes the face of the row's first position, a string shows
            // its own `face' properties over the default face.
            let laid_row = if job.truncate {
                laid.rows.first()
            } else {
                laid.rows.get(*seg)
            };
            if *row_hscroll == 0
                && let Some(row) = laid_row
                && row.prefix_cols > 0
            {
                let prefix_end = row.prefix_cols.min(col_cap);
                let value = glyphless.prefix_value_at(&buffer, *row_start, row.continuation);
                if let Ok(text) = crate::lisp::primitives::string_text(&value) {
                    let (_, cells) = glyphless.prefix_string_cells(&text);
                    let cell_of = |offset: usize| cells[offset.min(cells.len() - 1)];
                    for (from, to, face) in crate::lisp::primitives::string_face_spans(&value) {
                        let (from, to) = (cell_of(from), cell_of(to).min(prefix_end));
                        if from < to {
                            display_face_jobs.push((
                                job.top + index,
                                job.left + from,
                                job.left + to,
                                face,
                            ));
                        }
                    }
                } else {
                    for (span_begin, span_end, attrs) in &resolved {
                        if *span_begin <= *row_start && *row_start < *span_end {
                            frame[job.top + index].overlay(job.left, job.left + prefix_end, *attrs);
                        }
                    }
                }
            }
            // The ellipsis takes the face of the text before it
            // (display_ellipsis draws with the iterator's saved face):
            // copy the preceding cell's attributes over the dots.
            for &ellipsis_index in &visual.ellipses {
                let mut column = column_of(ellipsis_index);
                if *row_hscroll > 0 {
                    let Some(shifted) = hscroll_layout.screen_column(column) else {
                        continue;
                    };
                    column = shifted;
                } else {
                    column = column.saturating_sub(segment_start);
                }
                if column == 0 || column >= col_cap {
                    continue;
                }
                ellipsis_jobs.push((
                    job.top + index,
                    job.left + column,
                    (col_cap - column).min(3),
                ));
            }
        }
        drop(buffer);
        for (row_index, from, to, face) in display_face_jobs {
            let attrs = *state
                .face_cache
                .entry(format!("{face}"))
                .or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
                });
            let row = &mut frame[row_index];
            if matches!(face.kind(), Kind::Symbol(name) if name == "default") {
                row.replace_attrs(from, to, attrs);
            } else {
                row.overlay(from, to, attrs);
            }
        }
        for (row_index, from, length) in ellipsis_jobs {
            let row = &mut frame[row_index];
            let inherited = row.attrs[(from - 1).min(row.attrs.len() - 1)];
            for cell in from..(from + length).min(row.attrs.len()) {
                row.attrs[cell] = inherited;
            }
        }
    }

    for (row, left, arrow) in &overlay_arrow_jobs {
        frame[*row].blit(*left, arrow, CellAttrs::default());
    }

    for job in &margin_string_jobs {
        let Ok(text) = crate::lisp::primitives::string_text(&job.value) else {
            continue;
        };
        let clipped: String = text.chars().take(job.width).collect();
        // Margin glyphs are separate display objects, but an extending
        // selection face already painted over the complete row remains the
        // base beneath them.  Replace characters without erasing that base,
        // then merge the margin string's own faces.
        frame[job.row].blit_text(job.left, &clipped);
        // A margin string is a separate glyph object.  Its unpropertized
        // cells carry the default foreground, while an extending face from
        // the buffer row remains the background beneath it.
        let default = *state.face_cache.entry("default".into()).or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs(
                interpreter,
                env,
                &Value::Symbol("default".into()),
            )
        });
        // The margin string is its own glyph object realized over the
        // default face: its cells take the default foreground (the
        // terminal's own default when the face leaves it unspecified —
        // term.c emits no SGR color for the default sentinel), while the
        // extending background from the buffer row remains beneath.
        frame[job.row].force_foreground(
            job.left,
            job.left + clipped.chars().count(),
            default.foreground,
        );
        for (from, to, face) in crate::lisp::primitives::string_face_spans(&job.value) {
            let key = format!("{face}");
            let attrs = *state.face_cache.entry(key).or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
            });
            frame[job.row].overlay(
                job.left + from.min(job.width),
                job.left + to.min(job.width),
                attrs,
            );
        }
    }

    // The `display-line-numbers' columns (maybe_produce_line_number):
    // a number on each row that begins a buffer line, a blank prefix on
    // continuation rows and on every row at or past ZV, all in the
    // line-number faces; under hscroll the left `$' replaces the
    // column's first cell in the default face.
    for job in &line_number_jobs {
        use crate::lisp::primitives::LineNumberMode as LnMode;
        let layout = job.layout;
        let mut resolve = |name: &str| -> CellAttrs {
            *state.face_cache.entry(name.to_string()).or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(
                    interpreter,
                    env,
                    &Value::Symbol(name.into()),
                )
            })
        };
        let number_attrs = resolve("line-number");
        let current_attrs = resolve("line-number-current-line");
        let major_attrs = (layout.major_tick > 0).then(|| resolve("line-number-major-tick"));
        let minor_attrs = (layout.minor_tick > 0).then(|| resolve("line-number-minor-tick"));
        let glyphless = CharacterDisplayContext::new(interpreter, job.buffer_id, job.window_id)
            .with_line_numbers(layout.cols);
        let Some(buffer) = (if job.buffer_id == interpreter.current_buffer_id() {
            Some(interpreter.buffer.borrow())
        } else {
            interpreter.get_buffer_by_id(job.buffer_id)
        }) else {
            continue;
        };
        let spec = resolve_buffer_invisibility(interpreter, &buffer, job.buffer_id);
        let point = job.point.clamp(buffer.point_min(), buffer.point_max());
        let point_line_abs = buffer.line_number_at_pos(point);
        let begv_line = buffer.line_number_at_pos(buffer.point_min());
        let displayed = |line: usize| {
            if layout.widen {
                line as i64
            } else {
                line as i64 - begv_line as i64 + 1
            }
        };
        let point_max = buffer.point_max();
        let segs_of = |visual: &VisualLine| {
            if job.truncate {
                1
            } else {
                wrap_glyphless_visual_line(visual, job.text_width).len()
            }
        };
        // Visual mode: the window-relative screen row showing point,
        // extrapolated in screen lines when point is off-screen
        // (display_count_lines_visually counts glyph rows).
        let point_row: i64 = if layout.mode == LnMode::Visual {
            let contains_point = |index: usize| {
                let (_, _, row_start, _) = job.rows[index];
                if row_start == usize::MAX || point < row_start {
                    return false;
                }
                let row_end = job
                    .rows
                    .get(index + 1)
                    .map(|(_, _, next, _)| *next)
                    .filter(|next| *next != usize::MAX)
                    .unwrap_or(job.window_end);
                point < row_end || row_end <= row_start
            };
            let rows_between = |from: (usize, usize), to: (usize, usize)| -> i64 {
                let mut rows: i64 = 0;
                let mut line = from.0;
                let mut first_seg = from.1 as i64;
                while line < to.0 {
                    let visual = glyphless_visual_line_at(&buffer, &spec, line, Some(&glyphless));
                    rows += segs_of(&visual) as i64 - first_seg;
                    first_seg = 0;
                    line += visual.lines_spanned.max(1);
                }
                rows + to.1 as i64 - first_seg
            };
            if let Some(row) = job.cursor_row {
                row as i64
            } else if let Some(index) = (0..job.rows.len()).find(|&index| contains_point(index)) {
                index as i64
            } else {
                let point_vline = visual_line_first_line(&buffer, &spec, point_line_abs);
                let point_seg = if job.truncate {
                    0
                } else {
                    let visual =
                        glyphless_visual_line_at(&buffer, &spec, point_vline, Some(&glyphless));
                    let offset = point.saturating_sub(buffer.display_line_start_of(point_vline));
                    let index = visual
                        .map
                        .get(offset)
                        .copied()
                        .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0));
                    let laid = layout::lay_out_visual_line(&visual, job.text_width, false);
                    let column = laid.columns[index.min(laid.columns.len() - 1)];
                    wrapped_position_of_column(
                        &laid.rows,
                        column,
                        visual
                            .glyphless_spans
                            .iter()
                            .any(|(from, _)| *from == index),
                    )
                    .0
                };
                let first = job
                    .rows
                    .iter()
                    .find(|(_, _, row_start, _)| *row_start != usize::MAX)
                    .map(|(line, seg, _, _)| (*line, *seg));
                match first {
                    Some((line, seg)) if (point_vline, point_seg) < (line, seg) => {
                        -rows_between((point_vline, point_seg), (line, seg))
                    }
                    Some((line, seg)) => rows_between((line, seg), (point_vline, point_seg)),
                    None => 0,
                }
            }
        } else {
            0
        };
        for row_index in 0..job.text_rows {
            let (line, seg, row_start, row_hscroll) =
                job.rows
                    .get(row_index)
                    .copied()
                    .unwrap_or((0, 0, usize::MAX, 0));
            let beyond = row_start == usize::MAX || row_start >= point_max;
            let value: i64 = if beyond {
                0
            } else {
                match layout.mode {
                    LnMode::Absolute => (displayed(line) + layout.offset).abs(),
                    LnMode::Relative => {
                        let distance = (displayed(line) - displayed(point_line_abs)).abs();
                        if distance == 0 && layout.current_absolute {
                            displayed(point_line_abs)
                        } else {
                            distance
                        }
                    }
                    LnMode::Visual => {
                        let distance = (row_index as i64 - point_row).abs();
                        if distance == 0 && layout.current_absolute {
                            displayed(point_line_abs)
                        } else {
                            distance
                        }
                    }
                }
            };
            let blank = beyond || (layout.mode != LnMode::Visual && seg > 0);
            let text = if blank {
                " ".repeat(layout.cols)
            } else {
                format!("{:>width$} ", value, width = layout.width + 1)
            };
            let is_current = !beyond
                && number_attrs != current_attrs
                && match layout.mode {
                    LnMode::Visual => row_index as i64 == point_row,
                    _ => line == point_line_abs,
                };
            let tick_attrs = if beyond {
                None
            } else if major_attrs.is_some() && value % layout.major_tick.max(1) == 0 {
                major_attrs
            } else if minor_attrs.is_some() && value % layout.minor_tick.max(1) == 0 {
                minor_attrs
            } else {
                None
            };
            let attrs = if is_current {
                current_attrs
            } else {
                tick_attrs.unwrap_or(number_attrs)
            };
            frame[job.top + row_index].blit(job.left, &text, attrs);
            if !beyond
                && row_hscroll > 0
                && !glyphless_visual_line_at(&buffer, &spec, line, Some(&glyphless))
                    .text
                    .is_empty()
            {
                frame[job.top + row_index].blit(job.left, "$", CellAttrs::default());
            }
        }
    }

    // Mode lines: each window's real `mode-line-format', rendered by the
    // interpreter's engine in that window's context and painted in the
    // realized mode-line face.
    // CURRENT_MODE_LINE_ACTIVE_FACE_ID: `mode-line-active' for the
    // selected window (and the window a selected minibuffer was entered
    // from), `mode-line-inactive' for the others.
    let mut mode_line_face = |name: &str| {
        *state.face_cache.entry(name.into()).or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &Value::symbol(name))
        })
    };
    let mode_line_active_attrs = mode_line_face("mode-line-active");
    let mode_line_inactive_attrs = mode_line_face("mode-line-inactive");
    for job in &mode_line_jobs {
        let measure = ModeLineMeasure {
            buffer_id: job.buffer_id,
            window_id: job.window_id,
        };
        let (mut mode_line, mut spans) = match crate::lisp::primitives::render_window_mode_line(
            interpreter,
            env,
            job.window_id,
            job.point,
            job.metrics,
            job.body_width,
            &measure,
        ) {
            Ok((text, spans)) => (text, spans),
            // A GNU-shaped fabrication here would feed the very
            // differential tool that checks mode lines; render failures
            // must be visible.
            Err(error) => {
                debug_log(&format!("mode-line render: {error:?}"));
                (format!("[mode-line render error: {error:?}]"), Vec::new())
            }
        };
        mode_line = apply_character_display(
            interpreter,
            job.buffer_id,
            job.window_id,
            mode_line,
            &mut spans,
        );
        // display_mode_line fills the row with spaces; a wide character
        // cut at its edge leaves pad_mode_line's `-'.
        let cells = cell_count(&mode_line);
        if cells < job.body_width {
            mode_line.extend(std::iter::repeat_n(' ', job.body_width - cells));
        }
        let mut shown = job.body_width;
        if cells > job.body_width {
            (mode_line, shown) = take_cells_padded(&mode_line, job.body_width, '-');
        }
        let mode_line_attrs = if job.active {
            mode_line_active_attrs
        } else {
            mode_line_inactive_attrs
        };
        frame[job.row].blit(job.left, &mode_line, mode_line_attrs);
        // The renderer's face spans (the buffer name's mode-line-buffer-id)
        // layer over the base mode-line face, clipped to the body.
        for (from, to, face) in spans {
            let key = format!("{face}");
            let attrs = *state.face_cache.entry(key).or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
            });
            let from = job.left + from.min(shown);
            let to = job.left + to.min(shown);
            frame[job.row].overlay(from, to, attrs);
        }
    }

    // Tab lines sit above header lines and use the `tab-line' face.  A
    // package can install an evaluated per-buffer format here without
    // enabling the global tab-bar UI (Flycheck's grouping controls do so).
    if !tab_line_jobs.is_empty() {
        let tab_attrs = *state
            .face_cache
            .entry("tab-line".into())
            .or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(
                    interpreter,
                    env,
                    &Value::Symbol("tab-line".into()),
                )
            });
        for job in &tab_line_jobs {
            let measure = ModeLineMeasure {
                buffer_id: job.buffer_id,
                window_id: job.window_id,
            };
            let (mut tab, mut spans) = match crate::lisp::primitives::render_window_tab_line(
                interpreter,
                env,
                job.window_id,
                job.point,
                job.metrics,
                job.body_width,
                &measure,
            ) {
                Ok((text, spans)) => (text, spans),
                Err(error) => {
                    debug_log(&format!("tab-line render: {error:?}"));
                    (format!("[tab-line render error: {error:?}]"), Vec::new())
                }
            };
            tab =
                apply_character_display(interpreter, job.buffer_id, job.window_id, tab, &mut spans);
            let mut shown = job.body_width;
            if cell_count(&tab) > job.body_width {
                (tab, shown) = take_cells_padded(&tab, job.body_width, ' ');
            }
            let mut padded = tab.clone();
            padded.extend(std::iter::repeat_n(
                ' ',
                job.body_width.saturating_sub(cell_count(&tab)),
            ));
            frame[job.row].blit(job.left, &padded, tab_attrs);
            for (from, to, face) in spans {
                let key = format!("{face}");
                let attrs = *state.face_cache.entry(key).or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
                });
                let from = job.left + from.min(shown);
                let to = job.left + to.min(shown);
                frame[job.row].overlay(from, to, attrs);
            }
        }
    }

    // Header lines paint through the same machinery in the
    // `header-line' face, padded with spaces to the body (GNU's
    // display_mode_line for the header row).
    if !header_line_jobs.is_empty() {
        let header_attrs = *state
            .face_cache
            .entry("header-line".into())
            .or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(
                    interpreter,
                    env,
                    &Value::Symbol("header-line".into()),
                )
            });
        for job in &header_line_jobs {
            let measure = ModeLineMeasure {
                buffer_id: job.buffer_id,
                window_id: job.window_id,
            };
            let (mut header, mut spans) = match crate::lisp::primitives::render_window_header_line(
                interpreter,
                env,
                job.window_id,
                job.point,
                job.metrics,
                job.body_width,
                &measure,
            ) {
                Ok((text, spans)) => (text, spans),
                Err(error) => {
                    debug_log(&format!("header-line render: {error:?}"));
                    (format!("[header-line render error: {error:?}]"), Vec::new())
                }
            };
            header = apply_character_display(
                interpreter,
                job.buffer_id,
                job.window_id,
                header,
                &mut spans,
            );
            let mut shown = job.body_width;
            if cell_count(&header) > job.body_width {
                (header, shown) = take_cells_padded(&header, job.body_width, ' ');
            }
            let mut padded = header.clone();
            padded.extend(std::iter::repeat_n(
                ' ',
                job.body_width.saturating_sub(cell_count(&header)),
            ));
            frame[job.row].blit(job.left, &padded, header_attrs);
            for (from, to, face) in spans {
                let key = format!("{face}");
                let attrs = *state.face_cache.entry(key).or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
                });
                let from = job.left + from.min(shown);
                let to = job.left + to.min(shown);
                frame[job.row].overlay(from, to, attrs);
            }
        }
    }

    // The menu bar row (GNU's display_menu_bar): every active keymap's
    // top-level captions one space apart, the whole row in the `menu'
    // face.  Recomputed — after menu-bar-update-hook, like GNU's
    // update_menu_bar — when the selected buffer or its major mode
    // changed, not per keystroke.
    if menu_lines > 0 && !frame.is_empty() {
        let mode = interpreter
            .lookup_var("major-mode", env)
            .map(|mode| format!("{mode}"))
            .unwrap_or_default();
        let cache_key = (
            interpreter.current_buffer_id(),
            mode,
            crate::lisp::primitives::active_keymap_count(interpreter, env),
        );
        if state
            .menu_bar_row
            .as_ref()
            .is_none_or(|(key, _)| *key != cache_key)
        {
            let _ = interpreter.call_function_value(
                Value::Symbol("run-hooks".into()),
                None,
                &[
                    Value::Symbol("activate-menubar-hook".into()),
                    Value::Symbol("menu-bar-update-hook".into()),
                ],
                env,
            );
            let captions = crate::lisp::primitives::menu_bar_row_captions(interpreter, env);
            let mut text = String::new();
            for caption in &captions {
                text.push_str(caption);
                text.push(' ');
            }
            state.menu_bar_row = Some((cache_key, text));
        }
        let menu_attrs = *state.face_cache.entry("menu".into()).or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs(
                interpreter,
                env,
                &Value::Symbol("menu".into()),
            )
        });
        let text = state
            .menu_bar_row
            .as_ref()
            .map(|(_, text)| text.clone())
            .unwrap_or_default();
        let mut row = PaintRow::blank(cols);
        row.attrs = vec![menu_attrs; cols];
        row.blit(0, &text, menu_attrs);
        frame[0] = row;
    }

    // Emit only rows that changed since the last paint (dispnew's
    // current-matrix idea, one line deep): a self-insert repaints one
    // text row, not the frame.
    if full_repaint {
        state.painted_echo = Vec::new();
        state.painted_size = (cols, rows);
        state.painted_rows.clear();
    }
    state.painted_rows.resize(frame_rows, PaintRow::unpainted());

    encode_terminal_cells(interpreter, &mut frame);
    let mut out = terminal_output();
    queue!(out, cursor::Hide)?;
    if full_repaint {
        // xdisp.c:clear_garbaged_frames: a resized frame is redrawn from a
        // cleared terminal (redraw_frame's clear_frame), so rows a smaller
        // frame no longer covers do not keep the old paint.
        queue!(out, terminal::Clear(terminal::ClearType::All))?;
    }
    for (row, rendered) in frame.into_iter().enumerate() {
        if state.painted_rows[row] != rendered {
            paint_row(&mut out, row, &rendered)?;
            state.painted_rows[row] = rendered;
        }
    }

    // Echo area: frontend echo (key progress, errors) wins; otherwise the
    // session's live `message' line shows, GNU's echo-area behavior.  An
    // active minibuffer's row comes from its buffer with the text
    // properties read_minibuf applied — the prompt's face reaches the
    // glass.
    state.minibuffer_owns_echo = echo_from_minibuffer;
    // Full redisplay reflects the message channel; the glass is caught
    // up with every emission made so far.
    state.painted_message_tick = crate::lisp::primitives::echo_area_message_tick();
    let mut echo_paint = echo_paint;
    echo_paint.resize(state.echo_rows, PaintRow::blank(cols));
    encode_terminal_cells(interpreter, &mut echo_paint);
    if state.painted_echo != echo_paint {
        for (index, echo_row) in echo_paint.iter().enumerate() {
            paint_row(&mut out, frame_rows + index, echo_row)?;
        }
        state.painted_echo = echo_paint;
    }
    if (minibuffer_echo_owns_hardware_cursor(interpreter, echo_from_minibuffer)
        || (!echo_from_minibuffer
            && frontend_echo_early.is_empty()
            && interpreter.active_minibuffer_buffer_id().is_some()))
        && let Some(position) = echo_cursor
    {
        let (row, col) = wrapped_echo_cursor(&echo_long, position, cols, state.echo_rows);
        cursor_position = (
            col as u16,
            (frame_rows + row).min(rows.saturating_sub(1)) as u16,
        );
    } else if interpreter
        .lookup_var("cursor-in-echo-area", env)
        .is_some_and(|value| value.is_truthy())
        && !frontend_echo_early.is_empty()
    {
        let (row, col) =
            wrapped_echo_cursor(&echo_long, echo_cursor.unwrap_or(0), cols, state.echo_rows);
        cursor_position = (
            col as u16,
            (frame_rows + row).min(rows.saturating_sub(1)) as u16,
        );
    }
    queue!(
        out,
        cursor::MoveTo(cursor_position.0, cursor_position.1),
        cursor::Show
    )?;
    out.flush()
}

/// Run the Lisp redisplay coordinator before taking any buffer snapshots.
///
/// GNU's redisplay calls `pre-redisplay-function' with t when every window is
/// being considered.  Among other things, simple.el uses this to maintain the
/// window-local cursor-face overlay in *Completions*.  Like GNU's dsafe call,
/// redisplay must survive a callback error and continue painting the frame.
fn run_pre_redisplay_function(interpreter: &mut Interpreter, env: &mut Env) {
    let Some(function) = interpreter
        .lookup_var("pre-redisplay-function", env)
        .filter(Value::is_truthy)
    else {
        return;
    };
    let _ =
        interpreter.call_function_value(function, Some("pre-redisplay-function"), &[Value::T], env);
}

fn minibuffer_echo_owns_hardware_cursor(
    interpreter: &Interpreter,
    echo_from_minibuffer: bool,
) -> bool {
    echo_from_minibuffer && interpreter.selected_window_id() == interpreter.minibuffer_window_id()
}

/// The echo row's paint: an active minibuffer renders from its buffer
/// with the face text properties read_minibuf applied (the prompt's
/// face); every other echo — messages, key progress, errors — paints in
/// the default face, as GNU does.
fn compose_echo_row(
    interpreter: &mut Interpreter,
    env: &mut Env,
    frontend_echo: &str,
    cols: usize,
    face_cache: &mut std::collections::HashMap<String, CellAttrs>,
) -> (PaintRow, bool, Option<usize>) {
    let mut row;
    let mut message_cursor = None;
    let minibuffer_id = if frontend_echo.is_empty() {
        interpreter
            .active_minibuffer_buffer_id()
            .filter(|id| interpreter.has_buffer_id(*id))
            .and_then(|id| {
                let buffer = if id == interpreter.current_buffer_id() {
                    interpreter.buffer.borrow()
                } else {
                    interpreter
                        .get_buffer_by_id(id)
                        .unwrap_or(interpreter.buffer.borrow())
                };
                // The minibuffer command loop mirrors its prompt text into
                // the echo channel before blocking.  A different live value
                // is an intentional transient message (notably the value
                // printed by eval-expression after a nested read) and owns
                // the echo area until the next input event.  Printing to the
                // echo stream leaves the hardware cursor after that value.
                match crate::lisp::primitives::echo_area_message() {
                    Some(message) if message != buffer.buffer_string() => {
                        message_cursor = Some(message.chars().count());
                        None
                    }
                    _ => Some(id),
                }
            })
    } else {
        None
    };
    if let Some(buffer_id) = minibuffer_id {
        // The minibuffer's row renders like any window line in GNU:
        // buffer text under its face spans (text properties, overlays —
        // rfn-eshadow's shadow), with overlay before/after-strings
        // spliced in at their positions (set-minibuffer-message's
        // " [No match]" lives in an after-string at the end).
        struct OverlayString {
            position: usize,
            after: bool,
            id: usize,
            text: String,
            base_faces: Vec<Value>,
            spans: crate::lisp::primitives::EchoSpans,
            cursor: Option<usize>,
        }
        let (text, point_min, point, mut strings) = {
            let buffer = if buffer_id == interpreter.current_buffer_id() {
                interpreter.buffer.borrow()
            } else if let Some(buffer) = interpreter.get_buffer_by_id(buffer_id) {
                buffer
            } else {
                interpreter.buffer.borrow()
            };
            let mut strings = Vec::new();
            for overlay in &buffer.overlays {
                if overlay.is_dead() {
                    continue;
                }
                for (name, after) in [("before-string", false), ("after-string", true)] {
                    let Some(value) = overlay.get_prop(&Value::Symbol(name.into())) else {
                        continue;
                    };
                    let Ok(text) = crate::lisp::primitives::string_text(&value) else {
                        continue;
                    };
                    strings.push(OverlayString {
                        position: if after { overlay.end() } else { overlay.beg() },
                        after,
                        id: overlay.identity(),
                        text,
                        base_faces: Vec::new(),
                        spans: crate::lisp::primitives::string_face_spans(&value),
                        cursor: tty_string_cursor_position(&value),
                    });
                }
            }
            (
                buffer.buffer_string(),
                buffer.point_min(),
                buffer.point(),
                strings,
            )
        };
        strings.sort_by_key(|string| (string.position, string.after, string.id));
        let face_spans = crate::lisp::primitives::window_face_spans(
            interpreter,
            env,
            buffer_id,
            point_min,
            point_min + text.chars().count(),
            buffer_id == interpreter.current_buffer_id(),
        );
        let text_end = point_min + text.chars().count();
        for string in &mut strings {
            // A before-string attached to a real following character starts
            // with that character's face.  Strings at point-max and
            // after-strings are independent display objects with a default
            // base; notably Vertico's multiline point-max popup must not take
            // the prompt face, while its status prefix at point-min does.
            if !string.after && string.position < text_end {
                string.base_faces = face_spans
                    .iter()
                    .filter(|(from, to, _)| *from <= string.position && string.position < *to)
                    .map(|(_, _, face)| *face)
                    .collect();
            }
        }
        // Retain all source characters until translation: hidden entries
        // can bring text beyond the original column limit onto the screen.
        let output_cols = cols;
        let cols = text.chars().count()
            + strings
                .iter()
                .map(|string| string.text.chars().count())
                .sum::<usize>();
        row = PaintRow::blank(cols);
        // Lay the cells out with their source: buffer positions keep a
        // column map for the face spans; overlay strings carry their own.
        let mut col = 0usize;
        let mut column_of = std::collections::HashMap::new();
        let mut chars = text.chars();
        let mut cursor = None;
        let mut overlay_cursor = None;
        let splice = |row: &mut PaintRow,
                      col: &mut usize,
                      string: &OverlayString,
                      face_cache: &mut std::collections::HashMap<String, CellAttrs>,
                      interpreter: &mut Interpreter,
                      env: &mut Env| {
            let start = *col;
            row.blit(start, &string.text, CellAttrs::default());
            *col += string.text.chars().count();
            for face in &string.base_faces {
                let key = format!("{face}");
                let attrs = *face_cache.entry(key).or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, face)
                });
                row.overlay(start.min(cols), (*col).min(cols), attrs);
            }
            for (from, to, face) in &string.spans {
                let key = format!("{face}");
                let attrs = *face_cache.entry(key).or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, face)
                });
                row.overlay((start + from).min(cols), (start + to).min(cols), attrs);
            }
            string.cursor.map(|offset| start + offset)
        };
        let mut index = 0usize;
        for position in point_min..=point_min + text.chars().count() {
            // Before-strings at this position precede the buffer insertion
            // point; after-strings begin after it.  Capture the hardware
            // cursor between those two classes so minibuffer-message's
            // trailing " [No match]" can wrap without moving point onto
            // the continuation row.
            while index < strings.len()
                && (strings[index].position < position
                    || (strings[index].position == position && !strings[index].after))
            {
                let string_cursor = splice(
                    &mut row,
                    &mut col,
                    &strings[index],
                    face_cache,
                    interpreter,
                    env,
                );
                if overlay_cursor.is_none() {
                    overlay_cursor = string_cursor;
                }
                index += 1;
            }
            if position == point {
                cursor = Some(col);
            }
            while index < strings.len() && strings[index].position == position {
                let string_cursor = splice(
                    &mut row,
                    &mut col,
                    &strings[index],
                    face_cache,
                    interpreter,
                    env,
                );
                if overlay_cursor.is_none() {
                    overlay_cursor = string_cursor;
                }
                index += 1;
            }
            if let Some(c) = chars.next() {
                column_of.insert(position, col);
                row.blit(col, &c.to_string(), CellAttrs::default());
                col += 1;
            }
        }
        for (from, to, face) in &face_spans {
            let key = format!("{face}");
            let attrs = *face_cache.entry(key).or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, face)
            });
            for position in *from..*to {
                if let Some(&col) = column_of.get(&position) {
                    row.overlay(col.min(cols), (col + 1).min(cols), attrs);
                }
            }
        }
        row.text.truncate(col);
        row.attrs.truncate(col);
        let (row, cursor) = translate_echo_paint(
            interpreter,
            env,
            buffer_id,
            row,
            overlay_cursor.or(cursor),
            output_cols,
            face_cache,
        );
        return (row, true, cursor);
    }
    let (echo, spans) = if frontend_echo.is_empty() {
        crate::lisp::primitives::echo_display_message().unwrap_or_default()
    } else {
        (frontend_echo.to_string(), Vec::new())
    };
    let output_cols = cols;
    let cols = echo.chars().count();
    row = PaintRow::blank(cols);
    row.blit(0, &echo, CellAttrs::default());
    for (from, to, face) in spans {
        let key = format!("{face}");
        let attrs = *face_cache.entry(key).or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
        });
        row.overlay(from.min(cols), to.min(cols), attrs);
    }
    let (row, cursor) = translate_echo_paint(
        interpreter,
        env,
        0,
        row,
        Some(message_cursor.unwrap_or(cols)),
        output_cols,
        face_cache,
    );
    (row, false, cursor)
}

fn translate_echo_paint(
    interpreter: &mut Interpreter,
    env: &mut Env,
    buffer_id: u64,
    source: PaintRow,
    cursor: Option<usize>,
    cols: usize,
    face_cache: &mut std::collections::HashMap<String, CellAttrs>,
) -> (PaintRow, Option<usize>) {
    let text: String = source.text.iter().collect();
    let display =
        CharacterDisplayContext::new(interpreter, buffer_id, interpreter.minibuffer_window_id())
            .translate(&text);
    // Display offsets to the cells their glyphs take.
    let cells = display_cells(&display.text);
    let cell_of = |offset: usize| cells[offset.min(cells.len() - 1)];
    let cursor = cursor.map(|offset| cell_of(display.offset(offset)));
    let mut row = PaintRow::blank(cols);
    row.blit(0, &display.text, CellAttrs::default());
    for (index, offsets) in display.offsets.windows(2).enumerate() {
        row.replace_attrs(
            cell_of(offsets[0]).min(cols),
            cell_of(offsets[1]).min(cols),
            source.attrs[index],
        );
    }
    for (from, to, face) in display.face_spans.into_iter().chain(
        display
            .glyphless_spans
            .into_iter()
            .map(|(from, to)| (from, to, Value::symbol("glyphless-char"))),
    ) {
        let attrs = *face_cache.entry(format!("{face}")).or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
        });
        row.overlay(cell_of(from).min(cols), cell_of(to).min(cols), attrs);
    }
    (row, cursor)
}

/// GNU display strings can carry a non-nil `cursor' property selecting the
/// hardware-cursor glyph independently of buffer point.  Vertico uses it on
/// the spacer before its newline-separated candidate overlay.
fn tty_string_cursor_position(value: &Value) -> Option<usize> {
    let length = crate::lisp::primitives::string_text(value)
        .ok()?
        .chars()
        .count();
    (0..length).find(|position| {
        crate::lisp::primitives::string_property_at(value, *position, "cursor")
            .is_some_and(|cursor| cursor.is_truthy())
    })
}

/// The SGR sequence selecting ATTRS from a reset state.
fn sgr_sequence(attrs: &CellAttrs) -> String {
    // term.c selects colors through the opposite foreground/background
    // capability while standout is active.  Once both colors are concrete,
    // that is the same terminal state as swapping them without retaining the
    // reverse flag; GNU's Magit highlight is a visible example.  A face with
    // either terminal-default color still needs actual reverse video.
    let (foreground, background, reverse) =
        match (attrs.foreground, attrs.background, attrs.reverse) {
            (Some(foreground), Some(background), true) => {
                (Some(background), Some(foreground), false)
            }
            _ => (attrs.foreground, attrs.background, attrs.reverse),
        };
    let mut codes: Vec<String> = vec!["0".into()];
    if attrs.bold {
        codes.push("1".into());
    }
    if attrs.underline {
        codes.push("4".into());
    }
    if reverse {
        codes.push("7".into());
    }
    if let Some(fg) = foreground {
        codes.push(if fg < 8 {
            format!("{}", 30 + fg)
        } else {
            format!("38;5;{fg}")
        });
    }
    if let Some(bg) = background {
        codes.push(if bg < 8 {
            format!("{}", 40 + bg)
        } else {
            format!("48;5;{bg}")
        });
    }
    format!("\x1b[{}m", codes.join(";"))
}

/// term.c encode_terminal_code: a glyph the output coding (the
/// terminal's, or the safe coding when that converts nothing) cannot
/// encode goes out as `?', once for each cell it takes, and so does a
/// character composed into a cell.
fn encode_terminal_cells(interpreter: &Interpreter, rows: &mut [PaintRow]) {
    let coding = interpreter.effective_terminal_coding_system();
    let mut known = std::collections::HashMap::new();
    let mut encodable = |character: char| {
        character.is_ascii()
            || crate::lisp::primitives::raw_byte_from_regex_char(character).is_some()
            || *known.entry(character).or_insert_with(|| {
                crate::lisp::primitives::terminal_char_encodable(
                    interpreter,
                    &coding,
                    u32::from(character),
                )
            })
    };
    for row in rows {
        let mut index = 0;
        while index < row.text.len() {
            if row.text[index] != PADDING_GLYPH && !encodable(row.text[index]) {
                row.text[index] = '?';
                while index + 1 < row.text.len() && row.text[index + 1] == PADDING_GLYPH {
                    index += 1;
                    row.text[index] = '?';
                }
            }
            index += 1;
        }
        for (_, mark) in &mut row.marks {
            if !encodable(*mark) {
                *mark = '?';
            }
        }
    }
}

/// Emit one terminal row from its paint model: runs of same-attribute
/// cells print inside one SGR selection, and trailing blank
/// default-attribute cells are cleared, not printed.
fn paint_row(out: &mut impl Write, row: usize, rendered: &PaintRow) -> io::Result<()> {
    queue!(
        out,
        cursor::MoveTo(0, row as u16),
        terminal::Clear(terminal::ClearType::CurrentLine),
    )?;
    let cols = rendered.text.len();
    let default_attrs = CellAttrs::default();
    let mut end = cols;
    while end > 0
        && rendered.text[end - 1] == ' '
        && rendered.attrs[end - 1] == default_attrs
        && !rendered.marks.iter().any(|(cell, _)| *cell == end - 1)
    {
        end -= 1;
    }
    let mut at = 0usize;
    while at < end {
        let attrs = rendered.attrs[at];
        let mut run_end = at;
        while run_end < end && rendered.attrs[run_end] == attrs {
            run_end += 1;
        }
        let mut text = String::new();
        let mut marks = rendered
            .marks
            .iter()
            .skip_while(|(cell, _)| *cell < at)
            .peekable();
        for cell in at..run_end {
            if rendered.text[cell] != PADDING_GLYPH {
                text.push(rendered.text[cell]);
            }
            while let Some((_, mark)) = marks.next_if(|(mark_cell, _)| *mark_cell == cell) {
                text.push(*mark);
            }
        }
        if attrs == default_attrs {
            queue!(out, style::Print(&text))?;
        } else {
            queue!(
                out,
                style::Print(sgr_sequence(&attrs)),
                style::Print(&text),
                style::Print("\x1b[0m"),
            )?;
        }
        at = run_end;
    }
    Ok(())
}

/// An hscrolled row when a line-number column precedes the text: the
/// left `$' replaces the column's own first glyph (produce_special_glyphs
/// overwrites the row's first glyph, which is now a line-number cell),
/// so the text area uses the gutter form of `HscrollLayout' and truncates
/// on the right as usual.
fn truncate_row_from(visual: &VisualLine, width: usize, hscroll: usize) -> String {
    let (expanded, columns) = truncated_cells(visual, width);
    if hscroll == 0 {
        return clip_truncated(&expanded, width);
    }
    let layout = HscrollLayout::new(visual, &columns, hscroll, false);
    let visible = skip_cells(&expanded, layout.source_start);
    clip_truncated(&visible, width)
}

/// Display text TEXT without its first CELLS cells; a wide character
/// the cut splits leaves blanks in its remaining cells.
fn skip_cells(text: &str, cells: usize) -> String {
    let starts = display_cells(text);
    let mut rest = String::with_capacity(text.len());
    let mut leading = true;
    for (index, character) in text.chars().enumerate() {
        if starts[index] < cells {
            continue;
        }
        if leading && character == PADDING_GLYPH {
            rest.push(' ');
            continue;
        }
        leading = false;
        rest.push(character);
    }
    rest
}

/// VISUAL laid out unwrapped (layout.rs): its cells -- `line-prefix'
/// first, TABs expanded -- and the column of each display index.
fn truncated_cells(visual: &VisualLine, width: usize) -> (String, Vec<usize>) {
    let laid = layout::lay_out_visual_line(visual, width, true);
    let text = laid
        .rows
        .iter()
        .map(|row| row.text.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    (text, laid.columns)
}

/// Laid-out cells as a truncating window of WIDTH shows them: once they
/// reach the last column, display_line's truncation glyphs replace the
/// glyph there from its character's first cell to the row's end, so a
/// wide character cut by the edge leaves `$$'.
fn clip_truncated(expanded: &str, width: usize) -> String {
    if cell_count(expanded) < width {
        return expanded.to_string();
    }
    let kept = take_cells(expanded, width);
    let chars: Vec<char> = kept.chars().collect();
    let starts = display_cells(&kept);
    // The last character starting a cell of the row, back over padding.
    let mut last = chars.len();
    while last > 0 {
        last -= 1;
        if chars[last] != PADDING_GLYPH
            && chars[last] != ATTACHED_GLYPH
            && (last == 0 || chars[last - 1] != ATTACHED_GLYPH)
        {
            break;
        }
    }
    let first_cell = starts[last];
    let mut row: String = chars[..last].iter().collect();
    row.extend(std::iter::repeat_n('$', width.saturating_sub(first_cell)));
    row
}

/// GNU's horizontal-scroll layout for the text area.  Normally text starts at
/// HSCROLL; an inline left `$' overwrites that first glyph.  If the column is
/// anywhere inside a multi-cell glyphless element, redisplay restarts the
/// whole element.  An inline marker overwrites its first cell, while a marker
/// in the line-number gutter leaves the whole restarted element visible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HscrollLayout {
    /// First expanded display column copied into the visible text area.
    source_start: usize,
    /// Screen column where `source_start' is drawn (one after a left `$'
    /// marker, or zero when the marker occupies a line-number gutter).
    screen_start: usize,
}

impl HscrollLayout {
    /// COLUMNS are the truncated layout's (layout.rs) columns of VISUAL.
    fn new(visual: &VisualLine, columns: &[usize], hscroll: usize, left_marker: bool) -> Self {
        if hscroll == 0 {
            return Self {
                source_start: 0,
                screen_start: 0,
            };
        }
        let screen_start = usize::from(left_marker);
        let column_of = |index: usize| columns[index.min(columns.len() - 1)];
        let glyphless_start = visual.glyphless_spans.iter().find_map(|(from, to)| {
            let from = column_of(*from);
            let to = column_of(*to);
            (from <= hscroll && hscroll < to).then_some(from)
        });
        let source_start = glyphless_start
            .unwrap_or(hscroll)
            .saturating_add(screen_start);
        Self {
            source_start,
            screen_start,
        }
    }

    fn screen_column(self, source_column: usize) -> Option<usize> {
        (source_column >= self.source_start)
            .then(|| self.screen_start + source_column - self.source_start)
    }
}

fn truncate_row_hscrolled(visual: &VisualLine, width: usize, hscroll: usize) -> String {
    let (expanded, columns) = truncated_cells(visual, width);
    if hscroll == 0 {
        return clip_truncated(&expanded, width);
    }
    if expanded.is_empty() {
        return expanded;
    }
    let layout = HscrollLayout::new(visual, &columns, hscroll, true);
    let remaining = skip_cells(&expanded, layout.source_start);
    let available = width.saturating_sub(1).max(1);
    let mut row = String::from("$");
    row.push_str(&clip_truncated(&remaining, available));
    row
}

/// Buffer position (1-based) where the visual row (LINE, SEG) begins
/// under the current wrap geometry.
fn position_of_visual_row(
    buffer: &crate::buffer::Buffer,
    spec: &InvisibilitySpec,
    glyphless: Option<&CharacterDisplayContext<'_>>,
    line: usize,
    seg: usize,
    usable: usize,
) -> usize {
    let start = buffer.display_line_start_of(line);
    if seg == 0 {
        return start;
    }
    let visual = glyphless_visual_line_at(buffer, spec, line, glyphless);
    let index = wrap_glyphless_visual_line(&visual, usable + 1)
        .get(seg)
        .map_or(visual.raw_of_display.len().saturating_sub(1), |segment| {
            segment.start_index
        });
    start
        + visual
            .raw_of_display
            .get(index)
            .copied()
            .unwrap_or_else(|| visual.raw_of_display.last().copied().unwrap_or(0))
}

/// One glyph row of a display line (layout.rs): its cells as drawn --
/// prefix, glyphs and any `\' continuation glyph -- and the columns it
/// covers in GNU's x + continuation_lines_width space.  A row restarting
/// an element that straddled the previous row's edge begins at that
/// edge, while the element's own column stays where it first appeared.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WrappedVisualSegment {
    text: String,
    start_column: usize,
    end_column: usize,
    /// First display index the row shows (after its prefix).
    start_index: usize,
    /// Leading cells taken by `line-prefix' or `wrap-prefix'.
    prefix_cols: usize,
    /// Whether the row continues its line (its prefix is `wrap-prefix').
    continuation: bool,
    /// Whether the row ends in the `\' continuation glyph (a mid-word
    /// break) rather than at a word-wrap point or the line's end.
    continued_glyph: bool,
}

fn wrap_glyphless_visual_line(visual: &VisualLine, cols: usize) -> Vec<WrappedVisualSegment> {
    layout::lay_out_visual_line(visual, cols, false).rows
}

fn wrapped_position_of_column(
    segments: &[WrappedVisualSegment],
    column: usize,
    prefer_first: bool,
) -> (usize, usize) {
    let matches = segments.iter().enumerate().filter(|(index, segment)| {
        segment.start_column <= column
            && (column < segment.end_column
                || (*index + 1 == segments.len() && column == segment.end_column))
    });
    let selected = if prefer_first {
        matches.into_iter().next()
    } else {
        matches.into_iter().next_back()
    };
    selected
        .map(|(index, segment)| (index, column.saturating_sub(segment.start_column)))
        .unwrap_or_else(|| {
            let index = segments.len().saturating_sub(1);
            let start = segments
                .get(index)
                .map_or(0, |segment| segment.start_column);
            (index, column.saturating_sub(start))
        })
}

// ── Interpreter plumbing ────────────────────────────────────────────────

fn call(
    interpreter: &mut Interpreter,
    env: &mut Env,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    interpreter.call_function_value(Value::Symbol(name.into()), None, args, env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::initialized_upstream_interactive_interpreter as initialized_interactive_runtime;

    /// plan_window_text for a window with no redisplay flags set, as a
    /// window freshly shown with START and POINT.
    #[allow(clippy::too_many_arguments)]
    fn plan_window_text(
        buffer: &crate::buffer::Buffer,
        spec: &InvisibilitySpec,
        glyphless: Option<&CharacterDisplayContext<'_>>,
        view: &mut WindowView,
        start: usize,
        point: usize,
        text_rows: usize,
        body_width: usize,
        truncate: bool,
        geometry: &RenderGeometry,
        selected: bool,
    ) -> WindowPlan {
        let state = redisplay_window::StartState {
            start,
            point,
            buffer_point: point,
            force_start: false,
            optional_new_start: false,
            start_at_line_beg: false,
            windows_or_buffers_changed: false,
            window_outdated: true,
            cursor_unmoved: false,
            scroll_functions: false,
            make_window_start_visible: false,
            total_cells: (text_rows + 1) * body_width,
            top_rows: 0,
            scroll: Default::default(),
            resume: None,
        };
        match super::plan_window_text(
            buffer, spec, glyphless, view, &state, text_rows, body_width, truncate, geometry,
            selected,
        ) {
            PlanOutcome::Planned { plan, .. } => plan,
            PlanOutcome::Hook { .. } => unreachable!("no window-scroll-functions"),
        }
    }

    fn display_test_eval(interpreter: &mut Interpreter, text: &str) -> Value {
        let form = crate::lisp::reader::Reader::new(text)
            .read()
            .expect("display setup parses")
            .expect("display setup has a form");
        interpreter
            .eval(&form, &mut Env::new())
            .expect("display setup evaluates")
    }

    #[test]
    fn display_tables_select_one_valid_window_buffer_or_standard_table() {
        let mut interpreter = initialized_interactive_runtime();
        display_test_eval(
            &mut interpreter,
            "(progn
          (require 'disp-table)
          (setq standard-display-table (make-display-table))
          (aset standard-display-table ?x [83])
          (setq buffer-display-table (make-display-table))
          (aset buffer-display-table ?x [66])
          (set-window-display-table nil (make-display-table)))",
        );
        let rendered = |interpreter: &Interpreter| {
            CharacterDisplayContext::new(
                interpreter,
                interpreter.current_buffer_id(),
                interpreter.selected_window_id(),
            )
            .translate("x")
            .text
        };
        assert_eq!(
            rendered(&interpreter),
            "x",
            "an empty window entry does not use the buffer entry"
        );
        display_test_eval(
            &mut interpreter,
            "(set-window-display-table nil (make-char-table 'syntax-table))",
        );
        assert_eq!(
            rendered(&interpreter),
            "B",
            "an invalid window table selects the buffer table"
        );
        display_test_eval(&mut interpreter, "(setq buffer-display-table nil)");
        assert_eq!(rendered(&interpreter), "S");
        display_test_eval(&mut interpreter, "(aset standard-display-table ?x [77 78])");
        assert_eq!(
            rendered(&interpreter),
            "MN",
            "the next redisplay sees the live entry"
        );
        display_test_eval(
            &mut interpreter,
            "(progn
          (set-char-table-range standard-display-table nil [68])
          (aset standard-display-table ?x nil)
          (let ((parent (make-display-table)))
            (aset parent ?x [80])
            (set-char-table-parent standard-display-table parent)))",
        );
        assert_eq!(
            rendered(&interpreter),
            "D",
            "a table default precedes its parent"
        );
        display_test_eval(
            &mut interpreter,
            "(set-char-table-range standard-display-table nil nil)",
        );
        assert_eq!(
            rendered(&interpreter),
            "P",
            "parent lookup uses the live char table"
        );
    }

    #[test]
    fn display_vectors_remap_positions_and_faces_without_recursive_translation() {
        let mut interpreter = initialized_interactive_runtime();
        display_test_eval(&mut interpreter, "(progn
          (require 'disp-table) (erase-buffer)
          (setq standard-display-table (make-display-table))
          (aset standard-display-table ?x (vector (make-glyph-code ?A 'bold) (cons ?B (face-id 'underline))))
          (aset standard-display-table ?A [90])
          (aset standard-display-table ?y [])
          (aset standard-display-table ?z [nil -1 (65 . -1)])
          (insert \"xyAz!\"))");
        let context = CharacterDisplayContext::new(
            &interpreter,
            interpreter.current_buffer_id(),
            interpreter.selected_window_id(),
        );
        let visual = glyphless_visual_line_at(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            1,
            Some(&context),
        );
        assert_eq!(visual.text, "ABZ   !");
        assert_eq!(visual.map, vec![0, 2, 2, 3, 6, 7]);
        assert_eq!(visual.raw_of_display, vec![0, 0, 2, 3, 3, 3, 4, 5]);
        assert!(visual.glyphless_spans.is_empty());
        assert_eq!(
            visual.display_face_spans,
            vec![
                (0, 1, Value::symbol("bold")),
                (1, 2, Value::symbol("underline"))
            ]
        );
        let mut spans = vec![(0, 2, Value::symbol("italic"))];
        let rendered = apply_character_display(
            &interpreter,
            interpreter.current_buffer_id(),
            interpreter.selected_window_id(),
            "xyA".into(),
            &mut spans,
        );
        assert_eq!(rendered, "ABZ");
        assert_eq!(spans[0], (0, 2, Value::symbol("italic")));
        assert_eq!(
            layout::lay_out_visual_line(&visual, 80, false).columns[visual.map[3]],
            3
        );
    }

    #[test]
    fn display_tables_translate_echo_before_clipping_and_remap_minibuffer_point() {
        let mut interpreter = initialized_interactive_runtime();
        let mut env = Env::new();
        display_test_eval(
            &mut interpreter,
            "(progn
          (require 'disp-table)
          (setq standard-display-table (make-display-table))
          (aset standard-display-table ?x [65 66])
          (aset standard-display-table ?y []))",
        );
        let mut cache = std::collections::HashMap::new();
        let (row, mini, cursor) =
            compose_echo_row(&mut interpreter, &mut env, "yyyxZ", 3, &mut cache);
        assert!(!mini);
        assert_eq!(row.text.iter().collect::<String>(), "ABZ");
        assert_eq!(cursor, Some(3));
        let active = crate::lisp::primitives::activate_minibuffer(
            &mut interpreter,
            &Value::String("x: ".into()),
            &Value::String("yx".into()),
            Value::Nil,
            &mut env,
        )
        .expect("display test minibuffer activates");
        crate::lisp::primitives::set_echo_area_message(None);
        let (row, mini, cursor) = compose_echo_row(&mut interpreter, &mut env, "", 8, &mut cache);
        crate::lisp::primitives::restore_active_minibuffer(&mut interpreter, active, &mut env);
        assert!(mini);
        assert_eq!(row.text.iter().collect::<String>(), "AB: AB  ");
        assert_eq!(cursor, Some(6));
        assert_eq!(
            row.attrs[0], row.attrs[1],
            "both prompt glyphs inherit the source face"
        );
        assert_ne!(row.attrs[0], CellAttrs::default());
    }

    #[test]
    fn display_glyph_codes_validate_both_representations_and_full_character_bits() {
        for code in [0, 65, 0xd800, 0x110000, 0x3fffff] {
            for face in [0, 1, (1 << 20) - 1] {
                assert_eq!(
                    display_table_glyph(Value::Integer(code | (face << 22))),
                    Some((code, face))
                );
                assert_eq!(
                    display_table_glyph(Value::cons(Value::Integer(code), Value::Integer(face))),
                    Some((code, face))
                );
            }
        }
        for glyph in [
            Value::Nil,
            Value::Integer(-1),
            Value::Integer(1 << 42),
            Value::cons(Value::Integer(1 << 22), Value::Integer(0)),
            Value::cons(Value::Integer(65), Value::Integer(1 << 20)),
        ] {
            assert_eq!(display_table_glyph(glyph), None);
        }
    }
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn tty_lisp_readers_preserve_mouse_events_and_keyboard_order() {
        use crate::lisp::primitives::{set_tty_event_poller, set_tty_event_reader};
        for mode in ["blocking", "polling", "timed"] {
            // The terminal frame's size comes from adjust_frame_size, which
            // calls window.el's owners.
            let mut interpreter = crate::test_support::initialized_gnu_window_lisp_interpreter();
            let mut env = Env::new();
            interpreter.set_variable("noninteractive", Value::Nil, &mut env);
            interpreter.set_tty_frame_size(80, 24);
            let queue = SharedEventQueue::default();
            queue.0.borrow_mut().extend([
                QueuedInput::Mouse(RawMouseInput {
                    button: 1,
                    modifiers: 0,
                    column: 5,
                    row: 0,
                    press: true,
                }),
                QueuedInput::Mouse(RawMouseInput {
                    button: 1,
                    modifiers: 0,
                    column: 5,
                    row: 0,
                    press: false,
                }),
                QueuedInput::Lisp(Value::Integer(97)),
                // Sentinels make the broken reader fail without blocking.
                QueuedInput::Lisp(Value::Integer(98)),
                QueuedInput::Lisp(Value::Integer(99)),
            ]);
            let state = std::rc::Rc::new(std::cell::RefCell::new(TtyState::new()));
            set_tty_event_reader(Some(make_event_reader(
                queue.clone(),
                std::rc::Rc::clone(&state),
            )));
            if mode != "blocking" {
                set_tty_event_poller(Some(make_event_poller(queue.clone(), state)));
            }
            let args = if mode == "timed" {
                vec![Value::Nil, Value::Nil, Value::Integer(0)]
            } else {
                Vec::new()
            };
            let observed: Vec<_> = (0..3)
                .map(|_| {
                    crate::lisp::primitives::call(&mut interpreter, "read-event", &args, &mut env)
                })
                .collect();
            set_tty_event_reader(None);
            set_tty_event_poller(None);
            let observed: Vec<_> = observed
                .into_iter()
                .map(|event| event.expect("read queued terminal event").to_string())
                .collect();
            // GNU lread.c:read_filtered_event returns parameterized mouse
            // events unchanged when ASCII_REQUIRED is false (read-event).
            assert_eq!(
                observed,
                [
                    "(down-mouse-1 (nil menu-bar (5 . 0) 0))",
                    "(mouse-1 (nil menu-bar (5 . 0) 0))",
                    "97",
                ],
                "mode={mode}"
            );
            assert_eq!(queue.0.borrow().len(), 2);
        }
    }

    fn test_visual_line(line: &str) -> VisualLine {
        let chars = line.chars().count();
        VisualLine {
            text: line.into(),
            map: (0..=chars).collect(),
            raw_of_display: (0..=chars).collect(),
            lines_spanned: 1,
            ellipses: Vec::new(),
            glyphless_spans: Vec::new(),
            display_face_spans: Vec::new(),
            prefixes: Vec::new(),
            line_wrap: layout::LineWrap::Window,
            wrap_categories: Vec::new(),
            tab_width: 8,
            lnum_cols: 0,
        }
    }

    fn wrapped_test_line(line: &str, cols: usize) -> Vec<String> {
        wrap_glyphless_visual_line(&test_visual_line(line), cols)
            .into_iter()
            .map(|segment| segment.text)
            .collect()
    }

    #[test]
    fn terminal_glyphless_expansion_tracks_coding_faces_and_point_columns() {
        let mut interpreter = crate::test_support::initialized_upstream_batch_interpreter();
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut crate::lisp::types::Env::new(),
            "(erase-buffer)",
        )
        .expect("erase scratch");
        interpreter.set_terminal_coding_system(None);
        interpreter.buffer.borrow_mut().insert("AöB€C😀D\n");
        let context = CharacterDisplayContext::new(
            &interpreter,
            interpreter.current_buffer_id(),
            interpreter.selected_window_id(),
        );
        let visual = glyphless_visual_line_at(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            1,
            Some(&context),
        );
        assert_eq!(visual.text, "A\\u00F6B\\u20ACC\\U01F600D");
        assert_eq!(visual.map, vec![0, 1, 7, 8, 14, 15, 23, 24]);
        assert_eq!(visual.glyphless_spans, vec![(1, 7), (8, 14), (15, 23)]);
        assert!(visual.raw_of_display[1..7].iter().all(|raw| *raw == 1));
        assert!(visual.raw_of_display[8..14].iter().all(|raw| *raw == 3));
        assert!(visual.raw_of_display[15..23].iter().all(|raw| *raw == 5));

        drop(context);
        interpreter.set_terminal_coding_system(Some("utf-8-unix".into()));
        let context = CharacterDisplayContext::new(
            &interpreter,
            interpreter.current_buffer_id(),
            interpreter.selected_window_id(),
        );
        let visual = glyphless_visual_line_at(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            1,
            Some(&context),
        );
        // The emoji's `char-width-table' width is 2: a padding glyph
        // follows it (term.c append_glyph).
        assert_eq!(visual.text, "AöB€C😀\0D");
        assert_eq!(visual.map, vec![0, 1, 2, 3, 4, 5, 7, 8]);
        assert!(visual.glyphless_spans.is_empty());
    }

    // get_next_display_element's escape translations, with the faces GNU
    // merges over them.
    #[test]
    fn control_raw_and_nobreak_characters_translate_like_get_next_display_element() {
        let mut interpreter = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = crate::lisp::types::Env::new();
        crate::test_support::eval_lisp(&mut interpreter, &mut env, "(erase-buffer)")
            .expect("erase scratch");
        interpreter.set_terminal_coding_system(Some("utf-8-unix".into()));
        let raw = crate::lisp::primitives::raw_byte_regex_char(0x80);
        interpreter
            .buffer
            .borrow_mut()
            .insert(&format!("a\u{1}\u{7f}{raw}\u{85}\u{a0}\u{ad}中\u{301}\n"));
        let line = |interpreter: &Interpreter| {
            let context = CharacterDisplayContext::new(
                interpreter,
                interpreter.current_buffer_id(),
                interpreter.selected_window_id(),
            );
            let visual = glyphless_visual_line_at(
                &interpreter.buffer.borrow(),
                &InvisibilitySpec::default(),
                1,
                Some(&context),
            );
            (visual.text, visual.display_face_spans)
        };
        let (text, faces) = line(&interpreter);
        assert_eq!(text, "a^A^?\\200\\205\u{a0}\u{ad}中\0\u{1}\u{301}");
        let face_names: Vec<String> = faces
            .iter()
            .map(|(from, to, face)| format!("{from}-{to} {face}"))
            .collect();
        assert_eq!(
            face_names,
            vec![
                "1-3 escape-glyph",
                "3-5 escape-glyph",
                "5-9 escape-glyph",
                "9-13 escape-glyph",
                "13-14 nobreak-space",
                "14-15 nobreak-hyphen",
            ]
        );
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            "(setq ctl-arrow nil nobreak-char-display 'escape)",
        )
        .expect("set display options");
        let (text, _) = line(&interpreter);
        assert_eq!(text, "a\\001\\177\\200\\205\\ \\-中\0\u{1}\u{301}");
    }

    #[test]
    fn wide_padding_fills_the_row_with_continuation_glyphs_like_display_line() {
        let visual = test_visual_line(&format!("{}中\0文\0", "a".repeat(78)));
        assert_eq!(
            laid_rows(&visual, 80),
            vec![format!("{}\\\\", "a".repeat(78)), "中\0文\0".to_string()]
        );
        // Under word-wrap the padding rule comes first; move_it's motion
        // layout backs up to the wrap point instead.
        let words =
            prefixed_test_line(&format!("{} 中\0文\0", "b".repeat(75)), None, None, true, 8);
        assert_eq!(
            laid_rows(&words, 80),
            vec![format!("{} 中\0\\\\", "b".repeat(75)), "文\0".to_string()]
        );
        let motion = layout::lay_out_motion_line(&words, 80, false);
        assert_eq!(motion.rows[1].start_index, 76);
        assert_eq!(
            clip_truncated(&format!("{}中\0文\0", "a".repeat(78)), 80),
            format!("{}$$", "a".repeat(78))
        );
    }

    #[test]
    fn glyphless_methods_follow_text_terminal_cons_and_width_rules() {
        assert_eq!(GlyphlessDisplayMethod::HexCode.render('😀'), "\\U01F600");
        assert_eq!(
            GlyphlessDisplayMethod::Acronym("ABCDEFG".into()).render('x'),
            "[ABCDEF]"
        );
        assert_eq!(GlyphlessDisplayMethod::EmptyBox.render('😀'), "[  ]");

        let mut interpreter = crate::test_support::initialized_upstream_batch_interpreter();
        interpreter.set_terminal_coding_system(None);
        // Startup left the *scratch* banner in the current buffer.
        crate::lisp::primitives::call(
            &mut interpreter,
            "erase-buffer",
            &[],
            &mut crate::lisp::types::Env::new(),
        )
        .expect("erase the scratch banner");
        interpreter.buffer.borrow_mut().insert("ö😀\n");
        let Kind::CharTable(table_id) = interpreter
            .default_toplevel_value("glyphless-char-display")
            .expect("initialized glyphless table")
            .kind()
        else {
            panic!("glyphless-char-display is a char table")
        };
        interpreter
            .char_table_set(
                table_id,
                u32::from('ö'),
                Value::cons(Value::Symbol("empty-box".into()), Value::String("o".into())),
            )
            .expect("set text-terminal acronym");
        interpreter
            .set_char_table_extra_slot(table_id, 0, Value::Symbol("empty-box".into()))
            .expect("set no-font fallback");
        let context = CharacterDisplayContext::new(
            &interpreter,
            interpreter.current_buffer_id(),
            interpreter.selected_window_id(),
        );
        let visual = glyphless_visual_line_at(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            1,
            Some(&context),
        );
        assert_eq!(visual.text, "o[  ]");
        assert_eq!(visual.map, vec![0, 1, 5]);
        assert_eq!(visual.glyphless_spans, vec![(0, 1), (1, 5)]);
    }

    #[test]
    fn glyphless_wrap_restarts_a_split_display_element_like_gnu() {
        let mut interpreter = crate::test_support::initialized_upstream_batch_interpreter();
        interpreter.set_terminal_coding_system(None);
        crate::lisp::primitives::call(
            &mut interpreter,
            "erase-buffer",
            &[],
            &mut crate::lisp::types::Env::new(),
        )
        .expect("erase the scratch banner");
        interpreter
            .buffer
            .borrow_mut()
            .insert(&format!("{}öZ\n", "x".repeat(76)));
        let context = CharacterDisplayContext::new(
            &interpreter,
            interpreter.current_buffer_id(),
            interpreter.selected_window_id(),
        );
        let visual = glyphless_visual_line_at(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            1,
            Some(&context),
        );
        let segments = wrap_glyphless_visual_line(&visual, 80);
        assert_eq!(
            segments
                .iter()
                .map(|segment| segment.text.clone())
                .collect::<Vec<_>>(),
            vec![format!("{}\\u0\\", "x".repeat(76)), "\\u00F6Z".into()]
        );
        assert_eq!(
            segments
                .iter()
                .map(|segment| (segment.start_column, segment.end_column))
                .collect::<Vec<_>>(),
            vec![(0, 79), (79, 86)]
        );
        assert_eq!(wrapped_position_of_column(&segments, 76, true), (0, 76));
        assert_eq!(wrapped_position_of_column(&segments, 86, false), (1, 7));
        let boundary = vec![
            WrappedVisualSegment {
                text: String::new(),
                start_column: 0,
                end_column: 79,
                start_index: 0,
                prefix_cols: 0,
                continuation: false,
                continued_glyph: true,
            },
            WrappedVisualSegment {
                text: String::new(),
                start_column: 79,
                end_column: 85,
                start_index: 79,
                prefix_cols: 0,
                continuation: true,
                continued_glyph: false,
            },
        ];
        assert_eq!(wrapped_position_of_column(&boundary, 79, true), (1, 0));
    }

    #[test]
    fn tabulated_list_glyphless_table_uses_tty_sort_indicators() {
        let mut interpreter = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn (require 'tabulated-list) (tabulated-list-mode))",
        )
        .read()
        .expect("tabulated-list setup parses")
        .expect("tabulated-list setup exists");
        interpreter
            .eval(&form, &mut env)
            .expect("tabulated-list mode initializes");
        let mut spans = vec![(0, 3, Value::Symbol("bold".into()))];
        let rendered = apply_character_display(
            &interpreter,
            interpreter.current_buffer_id(),
            interpreter.selected_window_id(),
            "a▼▲".to_string(),
            &mut spans,
        );
        assert_eq!(rendered, "av^");
        assert_eq!(
            spans,
            vec![
                (0, 3, Value::Symbol("bold".into())),
                (1, 2, Value::Symbol("glyphless-char".into())),
                (2, 3, Value::Symbol("glyphless-char".into())),
            ]
        );
    }

    #[test]
    fn invisible_same_row_motion_defers_mode_line_point_until_input() {
        let previous = WindowView {
            last_buffer_id: Some(7),
            last_point: Some(11),
            last_cursor: Some((3, 4)),
            last_top_pos: Some(1),
            last_chars_modiff: Some(9),
            ..WindowView::default()
        };
        assert_eq!(
            mode_line_display_point(previous, 7, 30, Some((3, 12)), 1, 9, true),
            11,
            "same-row invisible motion keeps the preceding point"
        );
        assert_eq!(
            mode_line_display_point(previous, 7, 30, Some((3, 4)), 1, 9, false),
            30,
            "ordinary visible motion renders the live point"
        );
        assert_eq!(
            mode_line_display_point(previous, 7, 30, None, 1, 9, true),
            30,
            "a non-selected window has no cursor row whose motion can be deferred"
        );
        assert_eq!(
            mode_line_display_point(previous, 7, 30, Some((3, 4)), 1, 10, true),
            30,
            "a text change must refresh the mode line"
        );

        let next = WindowView {
            last_buffer_id: Some(7),
            last_point: Some(30),
            last_cursor: Some((3, 4)),
            last_top_pos: Some(1),
            last_chars_modiff: Some(9),
            ..WindowView::default()
        };
        assert_eq!(
            mode_line_display_point(next, 7, 30, Some((3, 4)), 1, 9, true),
            30,
            "without a stored deferral, an unchanged point renders live"
        );

        let mut state = TtyState::new();
        state.views.insert(
            2,
            WindowView {
                deferred_mode_line_point: Some(11),
                ..WindowView::default()
            },
        );
        state.note_input();
        assert_eq!(state.views[&2].deferred_mode_line_point, None);
    }

    #[test]
    fn align_to_display_specs_lay_out_as_blank_columns() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let forms = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"ab \\tcd\\n\")
               (put-text-property 4 5 'display '(space :align-to 10)))",
        )
        .read_all()
        .expect("read align-to probe");
        for form in &forms {
            interp
                .eval(form, &mut env)
                .expect("evaluate align-to probe");
        }
        let (text, map) = displayed_line_with_map(&interp.buffer.borrow(), 1);
        // "ab " is 3 columns; the propertized tab becomes 7 blanks up
        // to column 10, exactly where xdisp.c's stretch glyph ends.
        assert_eq!(text, "ab        cd");
        let map = map.expect("expansion happened");
        // Raw offsets: a=0 b=1 space=2 tab=3 c=4 d=5 map onto the
        // laid-out string, with cd starting at column 10.
        assert_eq!(&map[..6], &[0, 1, 2, 3, 10, 11]);
    }

    #[test]
    fn width_display_specs_use_tty_cells_and_collapse_equal_runs() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let forms = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"aXbYcZZd\\n\")
               (put-text-property 2 3 'display '(space :width 0.5))
               (put-text-property 4 5 'display '(space :width 1.9))
               (put-text-property 6 8 'display '(space :width 2)))",
        )
        .read_all()
        .expect("read specified-width probe");
        for form in &forms {
            interp
                .eval(form, &mut env)
                .expect("evaluate specified-width probe");
        }

        let (text, map) = displayed_line_with_map(&interp.buffer.borrow(), 1);
        assert_eq!(text, "ab c  d");
        assert_eq!(
            map.expect("display widths changed the line"),
            vec![0, 1, 1, 2, 3, 4, 6, 6, 7]
        );
    }

    #[test]
    fn propertized_string_display_specs_replace_the_covered_text() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"x:y\n\")
               (put-text-property
                2 3 'display (propertize \": (166 GiB available)\" 'face 'bold)))",
        )
        .read()
        .expect("read display replacement probe")
        .expect("display replacement form exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate display replacement probe");
        let (text, map) = displayed_line_with_map(&interp.buffer.borrow(), 1);
        assert_eq!(text, "x: (166 GiB available)y");
        assert_eq!(
            map.expect("replacement changes the display"),
            vec![0, 1, 22, 23]
        );
        let visual = visual_line_at(
            &interp.buffer.borrow(),
            &InvisibilitySpec {
                active: true,
                ..InvisibilitySpec::default()
            },
            1,
        );
        assert_eq!(
            visual.text, "x: (166 GiB available)y",
            "display replacement also applies on buffers with active invisibility"
        );
    }

    #[test]
    fn point_at_hidden_run_start_stays_before_the_ellipsis() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"head\nhidden\nnext\n\")
               (put-text-property 5 13 'invisible 'fold))",
        )
        .read()
        .expect("read folded line probe")
        .expect("folded line form exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate folded line probe");
        let visual = visual_line_at(
            &interp.buffer.borrow(),
            &InvisibilitySpec {
                entries: vec![(Value::Symbol("fold".into()), true)],
                active: true,
                ..InvisibilitySpec::default()
            },
            1,
        );
        assert_eq!(visual.text, "head...next");
        assert_eq!(visual.map[4], 4, "run start maps before the ellipsis");
        assert_eq!(visual.map[5], 7, "the hidden interior maps after it");
    }

    #[test]
    fn canonical_t_invisibility_spec_hides_true_overlay_regions() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"head\\nhidden\\nnext\\n\")
               (let ((overlay (make-overlay 5 13)))
                 (overlay-put overlay 'invisible t)))",
        )
        .read()
        .expect("read true-overlay invisibility probe")
        .expect("true-overlay invisibility form exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate true-overlay invisibility probe");
        interp.set_variable("buffer-invisibility-spec", Value::T, &mut env);
        let spec = resolve_buffer_invisibility(
            &interp,
            &interp.buffer.borrow(),
            interp.current_buffer_id(),
        );
        assert!(spec.all, "canonical Value::T means every non-nil source");
        assert_eq!(
            visual_line_at(&interp.buffer.borrow(), &spec, 1).text,
            "headnext"
        );
    }

    #[test]
    fn window_overlay_after_strings_splice_before_hidden_newlines_with_faces() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"head\\nhidden\\nnext\\n\")
               (let ((hidden (make-overlay 5 13))
                     (indicator (make-overlay 4 5)))
                 (overlay-put hidden 'invisible t)
                 (overlay-put indicator 'after-string
                              (propertize \"…\" 'font-lock-face 'bold))))",
        )
        .read()
        .expect("read window overlay-string probe")
        .expect("window overlay-string form exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate window overlay-string probe");
        interp.set_variable("buffer-invisibility-spec", Value::T, &mut env);
        let spec = resolve_buffer_invisibility(
            &interp,
            &interp.buffer.borrow(),
            interp.current_buffer_id(),
        );
        let visual = visual_line_at(&interp.buffer.borrow(), &spec, 1);
        assert_eq!(visual.text, "head…next");
        assert_eq!(
            visual.display_face_spans,
            vec![
                (4, 5, Value::Symbol("default".into())),
                (4, 5, Value::Symbol("bold".into()))
            ]
        );
    }

    #[test]
    fn point_max_overlay_before_string_contributes_hard_display_rows() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            r#"(progn
                 (insert "a")
                 (let ((popup (make-overlay (point-max) (point-max))))
                   (overlay-put
                    popup 'before-string
                    (concat "\n"
                            (propertize " alpha " 'face 'highlight)
                            "\n amber "))))"#,
        )
        .read()
        .expect("point-max overlay probe should parse")
        .expect("point-max overlay probe should exist");
        interp
            .eval(&form, &mut env)
            .expect("point-max overlay probe should evaluate");

        let visual = visual_line_at(&interp.buffer.borrow(), &InvisibilitySpec::default(), 1);
        assert_eq!(visual.text, "a\n alpha \n amber ");
        assert_eq!(
            visual.map,
            vec![0, visual.text.chars().count()],
            "point follows the point-max before-string"
        );
        assert!(
            visual
                .display_face_spans
                .contains(&(2, 9, Value::Symbol("highlight".into())))
        );
        assert_eq!(
            wrap_glyphless_visual_line(&visual, 80)
                .into_iter()
                .map(|segment| segment.text)
                .collect::<Vec<_>>(),
            vec!["a", " alpha ", " amber "]
        );
    }

    #[test]
    fn point_precedes_a_point_max_overlay_after_string() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            r#"(progn
                 (insert "alp")
                 (let ((preview (make-overlay (point-max) (point-max))))
                   (overlay-put preview 'after-string "haBeta")))"#,
        )
        .read()
        .expect("point-max after-string probe should parse")
        .expect("point-max after-string probe should exist");
        interp
            .eval(&form, &mut env)
            .expect("point-max after-string probe should evaluate");

        let visual = visual_line_at(&interp.buffer.borrow(), &InvisibilitySpec::default(), 1);
        assert_eq!(visual.text, "alphaBeta");
        assert_eq!(
            visual.map,
            vec![0, 1, 2, 3],
            "point at the overlay boundary stays before its after-string"
        );
    }

    #[test]
    fn overlay_display_string_replaces_its_covered_buffer_range() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            r#"(progn
                 (insert "a")
                 (let ((preview (make-overlay (point-min) (point-max))))
                   (overlay-put preview 'priority 1000)
                   (overlay-put preview 'display
                                (propertize "amber" 'face 'highlight))))"#,
        )
        .read()
        .expect("overlay display probe should parse")
        .expect("overlay display probe should exist");
        interp
            .eval(&form, &mut env)
            .expect("overlay display probe should evaluate");

        let visual = visual_line_at(&interp.buffer.borrow(), &InvisibilitySpec::default(), 1);
        assert_eq!(visual.text, "amber");
        assert_eq!(visual.map, vec![0, 5]);
        assert!(
            visual
                .display_face_spans
                .contains(&(0, 5, Value::Symbol("highlight".into())))
        );
    }

    #[test]
    fn invisible_properties_on_overlay_strings_remove_their_tty_cells() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"[-] root\\n\")
               (let ((button (make-overlay 1 4)))
                 (overlay-put button 'before-string
                              (propertize \" \" 'invisible t))))",
        )
        .read()
        .expect("read invisible overlay-string probe")
        .expect("invisible overlay-string probe exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate invisible overlay-string probe");
        interp.set_variable("buffer-invisibility-spec", Value::T, &mut env);
        let spec = resolve_buffer_invisibility(
            &interp,
            &interp.buffer.borrow(),
            interp.current_buffer_id(),
        );
        assert!(spec.active && spec.all);
        assert_eq!(
            visual_line_at(&interp.buffer.borrow(), &spec, 1).text,
            "[-] root"
        );
    }

    #[test]
    fn window_margin_display_strings_do_not_enter_the_text_body() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"Recent commits\n\")
               (let ((indicator (make-overlay 2 15)))
                 (overlay-put
                  indicator 'before-string
                  (propertize \"o\" 'display
                              '((margin right-margin) \"date\")))))",
        )
        .read()
        .expect("read margin overlay-string probe")
        .expect("margin overlay-string probe exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate margin overlay-string probe");
        let visual = visual_line_at(&interp.buffer.borrow(), &InvisibilitySpec::default(), 1);
        assert_eq!(visual.text, "Recent commits");
        assert!(visual.display_face_spans.is_empty());
    }

    #[test]
    fn buffer_text_in_a_margin_display_leaves_the_text_area() {
        // handle_single_display_spec: `((margin right-margin) STAMP)' on
        // buffer text (ERC's right-margin timestamps) sends STAMP to the
        // margin; `((margin nil) X)' displays X in the text area.
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"<a> hi \")
               (insert (propertize \"[10:00]\" 'display
                                   '((margin right-margin) \"[10:00]\")))
               (insert \" and \")
               (insert (propertize \"zz\" 'display '((margin nil) \"Q\")))
               (insert \"\\n\"))",
        )
        .read()
        .expect("read margin text probe")
        .expect("margin text probe exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate margin text probe");
        let buffer = interp.buffer.borrow();
        let visual = visual_line_at(&buffer, &InvisibilitySpec::default(), 1);
        assert_eq!(visual.text, "<a> hi  and Q");
        let (text, _) = displayed_line_with_map(&buffer, 1);
        assert_eq!(text, "<a> hi  and Q");
    }

    #[test]
    fn a_line_cut_by_narrowing_displays_from_begv_to_zv() {
        // The display iterator never looks outside the accessible region:
        // a line the narrowing cuts lays out from BEGV and stops at ZV.
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn (insert \"abcdefghij\\nklmnop\\n\")
                    (put-text-property 1 3 'display \"\")
                    (narrow-to-region 4 9))",
        )
        .read()
        .expect("read narrowing probe")
        .expect("narrowing probe exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate narrowing probe");
        let buffer = interp.buffer.borrow();
        assert_eq!(buffer.display_line_start_of(1), 4);
        let (text, _) = displayed_line_with_map(&buffer, 1);
        assert_eq!(text, "defgh");
        let visual = visual_line_at(&buffer, &InvisibilitySpec::default(), 1);
        assert_eq!(visual.text, "defgh");
    }

    #[test]
    fn graphical_fringe_overlay_strings_have_no_tty_text_glyph() {
        let mut interp = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"alpha\\n\")
               (let ((mark (make-overlay 1 2)))
                 (overlay-put
                  mark 'before-string
                  (propertize \"x\" 'display
                              '(left-fringe bookmark-mark bookmark-face)))))",
        )
        .read()
        .expect("read fringe overlay-string probe")
        .expect("fringe overlay-string probe exists");
        interp
            .eval(&form, &mut env)
            .expect("evaluate fringe overlay-string probe");
        let visual = visual_line_at(&interp.buffer.borrow(), &InvisibilitySpec::default(), 1);
        assert_eq!(visual.text, "alpha");
        assert!(visual.display_face_spans.is_empty());
    }

    #[test]
    fn command_cycle_reselects_the_selected_windows_displayed_buffer() {
        let mut interp = Interpreter::new();
        let original = interp.current_buffer_id();
        let (displayed, _) = interp.create_buffer("*command-cycle-displayed*");
        interp.set_selected_window_buffer_id(displayed);
        assert_eq!(
            interp.current_buffer_id(),
            original,
            "set-window-buffer itself does not change the current buffer"
        );
        select_command_loop_buffer(&mut interp).expect("command-cycle buffer selection succeeds");
        assert_eq!(interp.current_buffer_id(), displayed);
        assert_eq!(interp.selected_window_buffer_id(), displayed);
    }

    #[test]
    fn interactive_startup_initializes_scratch_and_messages_buffers() {
        let mut interp = initialized_interactive_runtime();
        let mut env: Env = crate::lisp::types::Env::new();
        interp.set_variable("noninteractive", Value::Nil, &mut env);
        // This fixture now reaches the end of GNU startup, not the former
        // handwritten midpoint before its startup message. A separate GNU
        // -nw -Q session confirms this line in *Messages*. Check the text
        // too: an unrelated preload diagnostic can have the same length.
        let (messages, _) = interp
            .find_buffer("*Messages*")
            .expect("startup messages buffer");
        assert_eq!(
            interp
                .get_buffer_by_id(messages)
                .expect("live messages buffer")
                .full_buffer_string(),
            "For information about GNU Emacs and the GNU system, type C-h C-a.\n",
        );
        let probe = crate::lisp::reader::Reader::new(
            "(list
               (with-current-buffer \"*scratch*\"
                 (list major-mode (buffer-size) (buffer-modified-p)))
               (with-current-buffer \"*Messages*\"
                 (list major-mode (buffer-size))))",
        )
        .read()
        .expect("read startup buffer probe")
        .expect("startup buffer probe exists");
        assert_eq!(
            interp
                .eval(&probe, &mut env)
                .expect("run startup buffer probe"),
            Value::list([
                Value::list([
                    Value::Symbol("lisp-interaction-mode".into()),
                    Value::Integer(147),
                    Value::Nil,
                ]),
                Value::list([
                    Value::Symbol("messages-buffer-mode".into()),
                    Value::Integer(66),
                ]),
            ])
        );
    }

    #[test]
    fn plain_characters_encode_as_their_codes() {
        assert_eq!(
            encode_key(key(KeyCode::Char('h'), KeyModifiers::NONE)),
            vec![Value::Integer(104)]
        );
        assert_eq!(
            encode_key(key(KeyCode::Char('É'), KeyModifiers::NONE)),
            vec![Value::Integer(0xC9)]
        );
    }

    #[test]
    fn control_characters_use_ascii_codes() {
        assert_eq!(
            encode_key(key(KeyCode::Char('a'), KeyModifiers::CONTROL)),
            vec![Value::Integer(1)]
        );
        assert_eq!(
            encode_key(key(KeyCode::Char('x'), KeyModifiers::CONTROL)),
            vec![Value::Integer(24)]
        );
        assert_eq!(
            encode_key(key(KeyCode::Char(' '), KeyModifiers::CONTROL)),
            vec![Value::Integer(0)]
        );
        assert_eq!(
            encode_key(key(KeyCode::Char('_'), KeyModifiers::CONTROL)),
            vec![Value::Integer(31)]
        );
        assert_eq!(
            encode_key(key(KeyCode::Char('?'), KeyModifiers::CONTROL)),
            vec![Value::Integer(127)]
        );
    }

    #[test]
    fn meta_becomes_the_escape_prefix() {
        assert_eq!(
            encode_key(key(KeyCode::Char('x'), KeyModifiers::ALT)),
            vec![Value::Integer(27), Value::Integer(120)]
        );
        assert_eq!(
            encode_key(key(
                KeyCode::Char('w'),
                KeyModifiers::ALT | KeyModifiers::CONTROL
            )),
            vec![Value::Integer(27), Value::Integer(23)]
        );
    }

    #[test]
    fn function_keys_encode_as_symbols() {
        assert_eq!(
            encode_key(key(KeyCode::Up, KeyModifiers::NONE)),
            vec![Value::Symbol("up".into())]
        );
        assert_eq!(
            encode_key(key(KeyCode::Delete, KeyModifiers::NONE)),
            vec![Value::Symbol("deletechar".into())]
        );
        assert_eq!(
            encode_key(key(KeyCode::F(5), KeyModifiers::NONE)),
            vec![Value::Symbol("f5".into())]
        );
        assert_eq!(
            encode_key(key(KeyCode::Up, KeyModifiers::ALT)),
            vec![Value::Integer(27), Value::Symbol("up".into())]
        );
    }

    #[test]
    fn terminal_editing_keys_use_gnu_codes() {
        assert_eq!(
            encode_key(key(KeyCode::Enter, KeyModifiers::NONE)),
            vec![Value::Integer(13)]
        );
        assert_eq!(
            encode_key(key(KeyCode::Tab, KeyModifiers::NONE)),
            vec![Value::Integer(9)]
        );
        assert_eq!(
            encode_key(key(KeyCode::Backspace, KeyModifiers::NONE)),
            vec![Value::Integer(127)]
        );
        assert_eq!(
            encode_key(key(KeyCode::Esc, KeyModifiers::NONE)),
            vec![Value::Integer(27)]
        );
    }

    #[test]
    fn key_descriptions_follow_gnu_spellings() {
        assert_eq!(
            describe_keys(&[Value::Integer(24), Value::Integer(3)]),
            "C-x C-c"
        );
        assert_eq!(
            describe_keys(&[Value::Integer(27), Value::Integer(120)]),
            "M-x"
        );
        assert_eq!(describe_keys(&[Value::Symbol("up".into())]), "<up>");
        assert_eq!(
            describe_keys(&[Value::Integer(27), Value::Symbol("up".into())]),
            "M-<up>"
        );
        assert_eq!(describe_keys(&[Value::Integer(13)]), "RET");
        assert_eq!(describe_keys(&[Value::Integer(127)]), "DEL");
        assert_eq!(describe_keys(&[Value::Integer(0)]), "C-SPC");
        assert_eq!(describe_keys(&[Value::Integer(27)]), "ESC");
    }

    #[test]
    fn rendered_lines_expand_tabs_to_eight_column_stops() {
        assert_eq!(wrapped_test_line("a\tb", 80), vec!["a       b"]);
        assert_eq!(wrapped_test_line("\t\t", 80), vec!["                "]);
        assert_eq!(
            wrapped_test_line("12345678\tx", 80),
            vec!["12345678        x"]
        );
    }

    // The wrap geometry below is pinned against observed GNU 30.2 tty
    // behavior at 80 columns: continued rows carry 79 columns plus a
    // trailing backslash, and a line continues whenever its width
    // exceeds 79 — an exactly-80-column line becomes 79 + "\" then 1.
    #[test]
    fn long_lines_wrap_exactly_like_a_gnu_tty() {
        let of = |n: usize| "A".repeat(n);

        assert_eq!(wrapped_test_line(&of(79), 80), vec![of(79)]);
        assert_eq!(wrapped_test_line(&of(80), 80), vec![of(79) + "\\", of(1)]);
        assert_eq!(
            wrapped_test_line(&of(159), 80),
            vec![of(79) + "\\", of(79) + "\\", of(1)]
        );
        assert_eq!(wrapped_test_line(&of(158), 80), vec![of(79) + "\\", of(79)]);
        assert_eq!(
            wrapped_test_line(&of(200), 80),
            vec![of(79) + "\\", of(79) + "\\", of(42)]
        );
    }

    #[test]
    fn hscroll_marker_precedes_the_requested_source_column() {
        let ordinary = test_visual_line("abcdef");
        let ordinary_columns = layout::lay_out_visual_line(&ordinary, 80, true).columns;
        assert_eq!(truncate_row_hscrolled(&ordinary, 80, 3), "$ef");
        assert_eq!(truncate_row_from(&ordinary, 80, 3), "def");
        assert_eq!(
            HscrollLayout::new(&ordinary, &ordinary_columns, 0, true).source_start,
            0
        );
        assert_eq!(
            HscrollLayout::new(&ordinary, &ordinary_columns, 3, true).source_start,
            4
        );
        assert_eq!(
            HscrollLayout::new(&ordinary, &ordinary_columns, 3, true).screen_column(4),
            Some(1)
        );
        assert_eq!(
            HscrollLayout::new(&ordinary, &ordinary_columns, 3, false).screen_column(3),
            Some(0)
        );

        let mut glyphless = test_visual_line("AB\\u00F6CDEFG");
        glyphless.glyphless_spans.push((2, 8));
        for hscroll in 2..8 {
            assert_eq!(
                truncate_row_hscrolled(&glyphless, 80, hscroll),
                "$u00F6CDEFG"
            );
            assert_eq!(truncate_row_from(&glyphless, 80, hscroll), "\\u00F6CDEFG");
        }
        assert_eq!(truncate_row_hscrolled(&glyphless, 80, 1), "$\\u00F6CDEFG");
        assert_eq!(truncate_row_hscrolled(&glyphless, 80, 8), "$DEFG");
        assert_eq!(truncate_row_from(&glyphless, 80, 1), "B\\u00F6CDEFG");
        assert_eq!(truncate_row_from(&glyphless, 80, 8), "CDEFG");
        let glyphless_columns = layout::lay_out_visual_line(&glyphless, 80, true).columns;
        let inside = HscrollLayout::new(&glyphless, &glyphless_columns, 3, true);
        assert_eq!(inside.source_start, 3);
        assert_eq!(inside.screen_column(2), None);
        assert_eq!(inside.screen_column(3), Some(1));
        assert_eq!(inside.screen_column(8), Some(6));
    }

    #[test]
    fn hscrolled_hidden_point_uses_the_last_line_number_gutter_cell() {
        let interpreter = Interpreter::new();
        interpreter.buffer.borrow_mut().insert("abcdef\n");
        let mut view = WindowView::default();
        let plan = plan_window_text(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            None,
            &mut view,
            1,
            1,
            3,
            80,
            true,
            &RenderGeometry {
                truncate: true,
                hscroll: 3,
                current_line_only: false,
                min_hscroll: 3,
                lnum: Some(crate::lisp::primitives::LineNumberLayout {
                    mode: crate::lisp::primitives::LineNumberMode::Absolute,
                    width: 2,
                    cols: 4,
                    current_absolute: true,
                    offset: 0,
                    widen: false,
                    major_tick: 0,
                    minor_tick: 0,
                }),
            },
            true,
        );
        assert_eq!(plan.rendered[0].0, "def");
        assert_eq!(plan.cursor, Some((0, 3)));
    }

    #[test]
    fn minibuffer_cursor_follows_echo_wrapping() {
        let mut short = PaintRow::blank(160);
        short.blit(0, "Find file: /tmp/example", CellAttrs::default());
        assert_eq!(wrapped_echo_cursor(&short, 16, 80, 6), (0, 16));

        let mut wrapped = PaintRow::blank(200);
        wrapped.blit(0, &"x".repeat(100), CellAttrs::default());
        assert_eq!(wrapped_echo_cursor(&wrapped, 79, 80, 6), (1, 0));
        assert_eq!(wrapped_echo_cursor(&wrapped, 80, 80, 6), (1, 1));

        let mut multiline = PaintRow::blank(160);
        multiline.blit(0, "Prompt:\nanswer", CellAttrs::default());
        assert_eq!(wrapped_echo_cursor(&multiline, 8, 80, 6), (1, 0));

        let mut trailing = PaintRow::blank(160);
        trailing.blit(0, &format!("{}): ", "x".repeat(79)), CellAttrs::default());
        assert_eq!(wrapped_echo_cursor(&trailing, 82, 80, 2), (1, 3));
    }

    #[test]
    fn display_string_cursor_property_selects_its_first_non_nil_cell() {
        let mut interpreter = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form =
            crate::lisp::reader::Reader::new("#(\" xy\" 0 1 (cursor t) 2 3 (cursor ignored))")
                .read()
                .expect("cursor display string should parse")
                .expect("cursor display string should exist");
        let value = interpreter
            .eval(&form, &mut env)
            .expect("cursor display string should evaluate");
        assert_eq!(tty_string_cursor_position(&value), Some(0));
    }

    #[test]
    fn tty_redisplay_runs_the_lisp_pre_redisplay_coordinator_for_all_windows() {
        let mut interpreter = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let setup = crate::lisp::reader::Reader::new(
            "(progn
               (setq tty--pre-redisplay-argument 'unset)
               (setq pre-redisplay-function
                     #'(lambda (windows)
                         (setq tty--pre-redisplay-argument windows))))",
        )
        .read()
        .expect("pre-redisplay setup parses")
        .expect("pre-redisplay setup exists");
        interpreter
            .eval(&setup, &mut env)
            .expect("pre-redisplay setup evaluates");

        run_pre_redisplay_function(&mut interpreter, &mut env);

        assert_eq!(
            interpreter.lookup_var("tty--pre-redisplay-argument", &env),
            Some(Value::T)
        );
    }

    #[test]
    fn active_minibuffer_only_owns_cursor_while_its_window_is_selected() {
        let mut interpreter = initialized_interactive_runtime();
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable("noninteractive", Value::Nil, &mut env);
        let ordinary_window = interpreter.selected_window_id();
        let active = crate::lisp::primitives::activate_minibuffer(
            &mut interpreter,
            &Value::String("Prompt: ".into()),
            &Value::String("".into()),
            Value::Nil,
            &mut env,
        )
        .expect("minibuffer activates");

        assert!(minibuffer_echo_owns_hardware_cursor(&interpreter, true));
        interpreter.set_selected_window_id(ordinary_window);
        assert!(!minibuffer_echo_owns_hardware_cursor(&interpreter, true));
        assert!(!minibuffer_echo_owns_hardware_cursor(&interpreter, false));

        crate::lisp::primitives::restore_active_minibuffer(&mut interpreter, active, &mut env);
    }

    #[test]
    fn transient_printed_value_takes_precedence_over_an_active_minibuffer() {
        let mut interpreter = initialized_interactive_runtime();
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable("noninteractive", Value::Nil, &mut env);
        let active = crate::lisp::primitives::activate_minibuffer(
            &mut interpreter,
            &Value::String("Outer: ".into()),
            &Value::String("outer".into()),
            Value::Nil,
            &mut env,
        )
        .expect("minibuffer activates");
        crate::lisp::primitives::set_echo_area_message(Some("\"inner\"".into()));

        let (row, from_minibuffer, cursor) = compose_echo_row(
            &mut interpreter,
            &mut env,
            "",
            80,
            &mut std::collections::HashMap::new(),
        );

        crate::lisp::primitives::set_echo_area_message(None);
        crate::lisp::primitives::restore_active_minibuffer(&mut interpreter, active, &mut env);
        assert!(!from_minibuffer);
        assert_eq!(cursor, Some(7));
        assert_eq!(row.text.iter().take(7).collect::<String>(), "\"inner\"");
    }

    #[test]
    fn minibuffer_before_string_inherits_its_prompt_anchor_face() {
        let mut interpreter = initialized_interactive_runtime();
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable("noninteractive", Value::Nil, &mut env);
        let active = crate::lisp::primitives::activate_minibuffer(
            &mut interpreter,
            &Value::String("Prompt: ".into()),
            &Value::String("".into()),
            Value::Nil,
            &mut env,
        )
        .expect("minibuffer activates");
        let form = crate::lisp::reader::Reader::new(
            "(let ((overlay (make-overlay (point-min) (point-min))))
               (overlay-put overlay 'before-string \"!/0    \"))",
        )
        .read()
        .expect("overlay form parses")
        .expect("overlay form exists");
        interpreter
            .eval(&form, &mut env)
            .expect("status overlay is installed");
        let (row, from_minibuffer, _) = compose_echo_row(
            &mut interpreter,
            &mut env,
            "",
            80,
            &mut std::collections::HashMap::new(),
        );
        crate::lisp::primitives::restore_active_minibuffer(&mut interpreter, active, &mut env);
        assert!(from_minibuffer);
        assert_eq!(
            row.attrs[0], row.attrs[7],
            "the status before-string inherits the following prompt character's face"
        );
        assert_ne!(
            row.attrs[0],
            CellAttrs::default(),
            "the inherited minibuffer-prompt face is visible on a tty"
        );
    }

    #[test]
    fn wrapped_echo_preserves_a_faced_trailing_space() {
        let face = CellAttrs {
            foreground: Some(4),
            ..CellAttrs::default()
        };
        let mut prompt = PaintRow::blank(160);
        prompt.blit(0, &format!("{}): ", "x".repeat(79)), face);

        let rows = wrap_echo_paint(&prompt, 80, 2);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].attrs[2], face);
        assert_eq!(rows[1].text[2], ' ');
    }

    #[test]
    fn wrapped_echo_trailing_newline_does_not_allocate_an_empty_row() {
        let mut candidates = PaintRow::blank(240);
        candidates.blit(0, "Prompt:\nfirst\nsecond\n", CellAttrs::default());
        let rows = wrap_echo_paint(&candidates, 80, 10);
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0].text.iter().collect::<String>().trim_end(),
            "Prompt:"
        );
        assert_eq!(rows[1].text.iter().collect::<String>().trim_end(), "first");
        assert_eq!(rows[2].text.iter().collect::<String>().trim_end(), "second");
    }

    #[test]
    fn segment_counts_follow_the_wrap_geometry() {
        for (width, expected) in [
            (0, 1),
            (1, 1),
            (79, 1),
            (80, 2),
            (158, 2),
            (159, 3),
            (200, 3),
        ] {
            assert_eq!(
                wrapped_test_line(&"A".repeat(width), 80).len(),
                expected,
                "unexpected row count at width {width}"
            );
        }
    }

    #[test]
    fn embedded_display_newlines_are_hard_row_boundaries() {
        assert_eq!(
            wrapped_test_line("one\ntwo\nthree", 80),
            vec!["one", "two", "three"]
        );
        assert_eq!(
            wrapped_test_line("123456\nxy", 5),
            vec!["1234\\", "56", "xy"]
        );
    }

    #[test]
    fn visual_row_positions_follow_wrap_geometry() {
        let interpreter = Interpreter::new();
        interpreter
            .buffer
            .borrow_mut()
            .insert(&format!("top\n{}\nbottom\n", "wide".repeat(50)));
        let buffer = interpreter.buffer.borrow();
        assert_eq!(
            position_of_visual_row(&buffer, &InvisibilitySpec::default(), None, 1, 0, 79),
            1
        );
        assert_eq!(
            position_of_visual_row(&buffer, &InvisibilitySpec::default(), None, 2, 0, 79),
            5
        );
        assert_eq!(
            position_of_visual_row(&buffer, &InvisibilitySpec::default(), None, 2, 1, 79),
            84
        );
        assert_eq!(
            position_of_visual_row(&buffer, &InvisibilitySpec::default(), None, 2, 2, 79),
            163
        );
        assert_eq!(
            position_of_visual_row(&buffer, &InvisibilitySpec::default(), None, 3, 0, 79),
            206
        );
    }

    #[test]
    fn truncated_rows_carry_the_dollar_marker_in_the_last_column() {
        let truncate_row =
            |line: &str, width: usize| truncate_row_from(&test_visual_line(line), width, 0);
        assert_eq!(truncate_row("short", 39), "short");
        assert_eq!(truncate_row(&"W".repeat(38), 39), "W".repeat(38));
        // A line exactly the body width truncates, per the GNU tty.
        assert_eq!(truncate_row(&"W".repeat(39), 39), "W".repeat(38) + "$");
        assert_eq!(truncate_row(&"W".repeat(100), 39), "W".repeat(38) + "$");
        assert_eq!(truncate_row(&"W".repeat(40), 40), "W".repeat(39) + "$");
        // Tabs expand before the width check, like the wrap path.
        assert_eq!(truncate_row("a\tb", 39), "a       b");
        assert_eq!(
            truncate_row(&format!("a\t{}", "b".repeat(40)), 39),
            format!("a       {}$", "b".repeat(30))
        );
    }

    #[test]
    fn window_end_stops_before_a_wholly_invisible_buffer_tail() {
        let mut interpreter = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let form = crate::lisp::reader::Reader::new(
            "(progn
               (insert \"head\nhidden\nmore\n\")
               (let ((tail (make-overlay 6 (point-max))))
                 (overlay-put tail 'invisible t)))",
        )
        .read()
        .expect("read invisible-tail window probe")
        .expect("invisible-tail window probe exists");
        interpreter
            .eval(&form, &mut env)
            .expect("evaluate invisible-tail window probe");
        interpreter.set_variable("buffer-invisibility-spec", Value::T, &mut env);
        let spec = resolve_buffer_invisibility(
            &interpreter,
            &interpreter.buffer.borrow(),
            interpreter.current_buffer_id(),
        );
        let mut view = WindowView::default();
        let plan = plan_window_text(
            &interpreter.buffer.borrow(),
            &spec,
            None,
            &mut view,
            1,
            1,
            10,
            80,
            false,
            &RenderGeometry {
                truncate: false,
                hscroll: 0,
                current_line_only: false,
                min_hscroll: 0,
                lnum: None,
            },
            false,
        );
        assert_eq!(plan.rendered[0].0, "head");
        assert_eq!(plan.window_end, 6);
    }

    #[test]
    fn non_selected_windows_recenter_around_their_own_point() {
        let interpreter = Interpreter::new();
        for n in 1..=60 {
            interpreter
                .buffer
                .borrow_mut()
                .insert(&format!("line {n:02}\n"));
        }
        // Point far below the start: redisplay_window treats every
        // window alike, so a non-selected one recenters on its point too.
        let start = interpreter.buffer.borrow().line_start_of(30);
        let mut view = WindowView::default();
        let plan = plan_window_text(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            None,
            &mut view,
            start,
            interpreter.buffer.borrow().line_start_of(55),
            10,
            80,
            false,
            &RenderGeometry {
                truncate: false,
                hscroll: 0,
                current_line_only: false,
                min_hscroll: 0,
                lnum: None,
            },
            false,
        );
        assert_eq!(plan.rendered[0].0, "line 50");
        assert_eq!(plan.rendered[9].0, "line 59");
        assert_eq!(plan.top_pos, interpreter.buffer.borrow().line_start_of(50));
        assert_eq!(
            plan.cursor, None,
            "only the selected window shows the cursor"
        );
        assert_eq!(
            plan.window_end,
            interpreter.buffer.borrow().line_start_of(60),
            "window-end is the first position past the last row"
        );
    }

    #[test]
    fn redisplay_window_starts_follow_gnus_probes() {
        // Each case is a tmux probe of GNU 30.2 on "line N\n" lines.
        let start_for = |lines: usize,
                         rows: usize,
                         start: usize,
                         point: usize,
                         force_start: bool,
                         scroll: redisplay_window::ScrollParams| {
            let interpreter = Interpreter::new();
            for n in 0..lines {
                interpreter
                    .buffer
                    .borrow_mut()
                    .insert(&format!("line {n}\n"));
            }
            let buffer = interpreter.buffer.borrow();
            let state = redisplay_window::StartState {
                start,
                point,
                buffer_point: point,
                force_start,
                optional_new_start: false,
                start_at_line_beg: false,
                windows_or_buffers_changed: false,
                window_outdated: true,
                cursor_unmoved: false,
                scroll_functions: false,
                make_window_start_visible: false,
                total_cells: (rows + 1) * 80,
                top_rows: 0,
                scroll,
                resume: None,
            };
            let geometry = RenderGeometry {
                truncate: false,
                hscroll: 0,
                current_line_only: false,
                min_hscroll: 0,
                lnum: None,
            };
            match super::plan_window_text(
                &buffer,
                &InvisibilitySpec::default(),
                None,
                &mut WindowView::default(),
                &state,
                rows,
                80,
                false,
                &geometry,
                true,
            ) {
                PlanOutcome::Planned { chosen, .. } => {
                    (chosen.start, chosen.point.unwrap_or(point))
                }
                PlanOutcome::Hook { .. } => unreachable!(),
            }
        };
        let line = |n: usize| if n < 10 { 1 + 7 * n } else { 71 + 8 * (n - 10) };
        // set-window-start 500 with point off the window: point moves to
        // the row half a 9-row window reaches (row 3).
        assert_eq!(
            start_for(200, 9, 500, 1, true, Default::default()),
            (500, 519)
        );
        // scroll-conservatively 101, point two rows past the bottom.
        let conservative = redisplay_window::ScrollParams {
            conservatively: 101,
            ..Default::default()
        };
        assert_eq!(
            start_for(300, 30, 1, line(31), false, conservative),
            (15, line(31))
        );
        // scroll-margin 3: point entering the bottom margin recenters.
        let margin = redisplay_window::ScrollParams {
            margin: 3,
            ..Default::default()
        };
        assert_eq!(
            start_for(300, 30, 1, line(28), false, margin),
            (95, line(28))
        );
        // scroll-step 4.
        let step = redisplay_window::ScrollParams {
            step: 4,
            ..Default::default()
        };
        assert_eq!(start_for(300, 30, 1, line(32), false, step), (29, line(32)));
    }

    #[test]
    fn selected_windows_recenter_around_an_off_window_point() {
        let interpreter = Interpreter::new();
        for n in 1..=60 {
            interpreter
                .buffer
                .borrow_mut()
                .insert(&format!("line {n:02}\n"));
        }
        let mut view = WindowView::default();
        let point = interpreter.buffer.borrow().line_start_of(40);
        let plan = plan_window_text(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            None,
            &mut view,
            interpreter.buffer.borrow().point_min(),
            point,
            11,
            80,
            false,
            &RenderGeometry {
                truncate: false,
                hscroll: 0,
                current_line_only: false,
                min_hscroll: 0,
                lnum: None,
            },
            true,
        );
        // GNU recenters half a window above point: 40 - 11/2 = line 35.
        assert_eq!(view.top_line, 35);
        assert_eq!(plan.rendered[0].0, "line 35");
        assert_eq!(plan.cursor, Some((5, 0)));
    }

    #[test]
    fn truncating_windows_keep_one_row_per_logical_line() {
        let interpreter = Interpreter::new();
        interpreter.buffer.borrow_mut().insert("short one\n");
        interpreter.buffer.borrow_mut().insert(&"W".repeat(100));
        interpreter.buffer.borrow_mut().insert("\n");
        for n in 3..=20 {
            interpreter
                .buffer
                .borrow_mut()
                .insert(&format!("line {n:02}\n"));
        }
        let mut view = WindowView::default();
        let plan = plan_window_text(
            &interpreter.buffer.borrow(),
            &InvisibilitySpec::default(),
            None,
            &mut view,
            1,
            1,
            11,
            39,
            true,
            &RenderGeometry {
                truncate: true,
                hscroll: 0,
                current_line_only: false,
                min_hscroll: 0,
                lnum: None,
            },
            true,
        );
        assert_eq!(plan.rendered[0].0, "short one");
        assert_eq!(plan.rendered[1].0, "W".repeat(38) + "$");
        assert_eq!(
            plan.rendered[2].0, "line 03",
            "the long line takes one row, not a wrapped pair"
        );
        assert_eq!(plan.cursor, Some((0, 0)));
    }

    #[test]
    fn paint_rows_compare_by_text_and_attribute() {
        let mut row = PaintRow::blank(10);
        let mut same = PaintRow::blank(10);
        assert_eq!(row, same);
        row.blit(2, "ab", CellAttrs::default());
        same.blit(
            2,
            "ab",
            CellAttrs {
                reverse: true,
                ..CellAttrs::default()
            },
        );
        assert_ne!(row, same, "face attributes are part of the paint identity");
        let mut clipped = PaintRow::blank(4);
        clipped.blit(2, "abcdef", CellAttrs::default());
        assert_eq!(
            clipped.text.iter().collect::<String>(),
            "  ab",
            "blits clip at the row's end"
        );
    }

    #[test]
    fn text_only_blits_preserve_an_extending_base_face() {
        let selected = CellAttrs {
            background: Some(7),
            extend: true,
            ..CellAttrs::default()
        };
        let mut row = PaintRow::blank(8);
        row.overlay(0, 8, selected);
        row.blit_text(3, "date");
        assert_eq!(row.text.iter().collect::<String>(), "   date ");
        assert!(row.attrs.iter().all(|attrs| *attrs == selected));
    }

    #[test]
    fn margin_default_foreground_preserves_an_extending_background() {
        let selected = CellAttrs {
            background: Some(7),
            extend: true,
            ..CellAttrs::default()
        };
        let mut row = PaintRow::blank(8);
        row.overlay(0, 8, selected);
        row.blit_text(3, "date");
        row.overlay(
            3,
            7,
            CellAttrs {
                foreground: Some(7),
                ..CellAttrs::default()
            },
        );
        assert_eq!(row.text.iter().collect::<String>(), "   date ");
        for attrs in &row.attrs[3..7] {
            assert_eq!(attrs.foreground, Some(7));
            assert_eq!(attrs.background, Some(7));
            assert!(attrs.extend);
        }
    }

    #[test]
    fn sgr_sequences_reset_then_select_each_attribute() {
        assert_eq!(sgr_sequence(&CellAttrs::default()), "\x1b[0m");
        assert_eq!(
            sgr_sequence(&CellAttrs {
                foreground: Some(6),
                background: Some(5),
                ..CellAttrs::default()
            }),
            "\x1b[0;36;45m",
            "the base ANSI palette uses the 30/40 ranges"
        );
        assert_eq!(
            sgr_sequence(&CellAttrs {
                foreground: Some(250),
                background: Some(238),
                bold: true,
                underline: true,
                reverse: true,
                extend: false,
            }),
            "\x1b[0;1;4;38;5;238;48;5;250m",
            "concrete inverse colors swap through the 38;5/48;5 channels"
        );
        assert_eq!(
            sgr_sequence(&CellAttrs {
                foreground: Some(6),
                reverse: true,
                ..CellAttrs::default()
            }),
            "\x1b[0;7;36m",
            "inverse remains active while either color is terminal-default"
        );
    }

    #[test]
    fn merged_attrs_keep_the_base_where_the_overlay_is_silent() {
        let base = CellAttrs {
            foreground: Some(7),
            background: Some(4),
            reverse: true,
            ..CellAttrs::default()
        };
        let over = CellAttrs {
            foreground: Some(6),
            bold: true,
            ..CellAttrs::default()
        };
        let merged = merge_cell_attrs(base, over);
        assert_eq!(merged.foreground, Some(6), "the overlay's colors win");
        assert_eq!(merged.background, Some(4), "unspecified colors fall back");
        assert!(merged.bold && merged.reverse && !merged.underline);
    }

    #[test]
    fn prefix_echo_accumulates_like_gnu() {
        assert_eq!(append_prefix_echo("", "C-u"), "C-u-");
        assert_eq!(append_prefix_echo("C-u-", "8"), "C-u 8-");
        assert_eq!(append_prefix_echo("C-u 8-", "2"), "C-u 8 2-");
        assert_eq!(append_prefix_echo("C-u-", "C-u"), "C-u C-u-");
    }

    fn prefixed_test_line(
        line: &str,
        line_prefix: Option<&str>,
        wrap_prefix: Option<&str>,
        word_wrap: bool,
        tab_width: usize,
    ) -> VisualLine {
        let mut visual = test_visual_line(line);
        visual.prefixes = vec![layout::PrefixRun {
            raw_start: 0,
            line: line_prefix.map(String::from),
            wrap: wrap_prefix.map(String::from),
        }];
        visual.line_wrap = if word_wrap {
            layout::LineWrap::Word { by_category: false }
        } else {
            layout::LineWrap::Window
        };
        visual.tab_width = tab_width;
        visual
    }

    fn laid_rows(visual: &VisualLine, cols: usize) -> Vec<String> {
        layout::lay_out_visual_line(visual, cols, false)
            .rows
            .into_iter()
            .map(|row| row.text.trim_end().to_string())
            .collect()
    }

    // The expected rows are GNU 30's `-nw' screen in an 80-column tmux pane.
    #[test]
    fn word_wrap_breaks_rows_at_gnus_wrap_points() {
        let lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod \
                     tempor incididunt ut labore et dolore magna aliqua.";
        let plain = prefixed_test_line(&format!("plain no prefix: {lorem}"), None, None, true, 8);
        assert_eq!(
            laid_rows(&plain, 80),
            vec![
                "plain no prefix: Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed",
                "do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
            ]
        );
        // No wrap point on the row: a mid-word break with the `\' glyph.
        let long = prefixed_test_line(
            &format!("{} tail word", "x".repeat(100)),
            None,
            None,
            true,
            8,
        );
        assert_eq!(
            laid_rows(&long, 80),
            vec![
                format!("{}\\", "x".repeat(79)),
                format!("{} tail word", "x".repeat(21))
            ]
        );
        // A word reaching the last usable column moves down: the space
        // after it would overflow, so the row backs up to its wrap point.
        let edge = prefixed_test_line(
            &format!("aaaa bbbb {} dddd eeee", "c".repeat(69)),
            None,
            None,
            true,
            8,
        );
        assert_eq!(
            laid_rows(&edge, 80),
            vec![
                "aaaa bbbb".to_string(),
                format!("{} dddd eeee", "c".repeat(69))
            ]
        );
        let spaces = prefixed_test_line(
            &format!(
                "spaces at edge{}    next words here and there",
                " ".repeat(64)
            ),
            None,
            None,
            true,
            8,
        );
        assert_eq!(
            laid_rows(&spaces, 80),
            vec![
                "spaces at".to_string(),
                format!("edge{}next", " ".repeat(68)),
                "words here and there".to_string(),
            ]
        );
    }

    #[test]
    fn line_and_wrap_prefixes_start_rows_like_gnu() {
        let text = "0<tester> Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do \
                    eiusmod tempor incididunt ut labore et dolore magna aliqua.";
        let wrapped = prefixed_test_line(text, Some("    "), Some(&" ".repeat(13)), true, 8);
        let laid = layout::lay_out_visual_line(&wrapped, 80, false);
        assert_eq!(
            laid.rows
                .iter()
                .map(|row| row.text.trim_end())
                .collect::<Vec<_>>(),
            vec![
                "    0<tester> Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do",
                "             eiusmod tempor incididunt ut labore et dolore magna aliqua.",
            ]
        );
        // Row 2 begins where row 1 ended, its wrap prefix first: the
        // text's first column follows the prefix.
        assert_eq!(laid.rows[1].start_column, laid.rows[0].end_column);
        assert_eq!(
            laid.columns[laid.rows[1].start_index],
            laid.rows[1].start_column + 13
        );
        // Without word-wrap the prefix still narrows each row, and a TAB
        // after a prefix stops where x + continuation_lines_width says.
        let tabbed = prefixed_test_line(
            "1\tLorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor \
             incididunt ut labore et dolore magna aliqua.",
            Some("ab"),
            Some("cde"),
            false,
            8,
        );
        assert_eq!(
            laid_rows(&tabbed, 80),
            vec![
                "ab1     Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod\\",
                "cde tempor incididunt ut labore et dolore magna aliqua.",
            ]
        );
        // A truncated row carries its line prefix.
        let (cells, _) = truncated_cells(&tabbed, 80);
        assert!(cells.starts_with("ab1     Lorem"));
    }

    #[test]
    fn word_wrap_tab_stops_follow_tab_width() {
        let tabs = prefixed_test_line(
            "tabs\tat\tfour\tcolumns\twith\tword\twrap\tenabled\there\tand\tthere\tagain\tand\tagain\tmore\twords",
            None,
            None,
            true,
            4,
        );
        // A 76-cell text area: GNU's screen beside a 4-column line-number gutter.
        let mut tabs = tabs;
        tabs.lnum_cols = 4;
        assert_eq!(
            laid_rows(&tabs, 76),
            vec![
                "tabs    at  four    columns with    word    wrap    enabled here    and",
                "there   again   and again   more    words",
            ]
        );
    }

    // term.c measures a continuation row's TAB from current_x plus
    // continuation_lines_width (every row's full x, gutter included) less
    // the wrap-prefix's width, with the gutter left out.
    #[test]
    fn continuation_tab_stops_follow_term_c() {
        let line = format!(
            "{}\tA\tBB\tCCC\tD\tEEEE\tF\tGG\tH\tI\tJ\tK\tL\tM\tN\tO\tP\tQ\tR",
            "x".repeat(70)
        );
        let x70 = "x".repeat(70);
        let prefixed = prefixed_test_line(&line, None, Some("    "), false, 8);
        assert_eq!(
            laid_rows(&prefixed, 80),
            vec![
                format!("{x70}  A      \\"),
                "     BB      CCC     D       EEEE    F       GG      H       I       J       K \\"
                    .to_string(),
                "      L       M       N       O       P       Q       R".to_string(),
            ]
        );
        let mut numbered = test_visual_line(&line);
        numbered.lnum_cols = 4;
        assert_eq!(
            laid_rows(&numbered, 76),
            vec![
                format!("{x70}  A  \\"),
                " BB      CCC     D       EEEE    F       GG      H       I       J       K \\"
                    .to_string(),
                "  L       M       N       O       P       Q       R".to_string(),
            ]
        );
        let mut both = prefixed;
        both.lnum_cols = 4;
        assert_eq!(
            laid_rows(&both, 76),
            vec![
                format!("{x70}  A  \\"),
                "         BB      CCC     D       EEEE    F       GG      H       I       J \\"
                    .to_string(),
                "          K       L       M       N       O       P       Q       R".to_string(),
            ]
        );
    }

    #[test]
    fn display_columns_match_tab_expansion() {
        let columns = layout::lay_out_visual_line(&test_visual_line("a\tb"), 80, false).columns;
        assert_eq!(columns, vec![0, 1, 8, 9]);
    }

    #[test]
    fn command_errors_and_undefined_keys_reenter_command_loop_1() {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(
                &crate::compat::project_root().join("../emacs"),
            )
            .expect("upstream load path"),
            ..Default::default()
        };
        let mut interpreter =
            crate::batch::initialize_batch_interpreter(&options).expect("interpreter initializes");
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable("noninteractive", Value::Nil, &mut env);
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            r#"(progn
                 (setq t147-log nil)
                 (add-hook 'pre-command-hook (lambda () (push (list 'pre this-command) t147-log)))
                 (add-hook 'post-command-hook
                           (lambda () (push (list 'post this-command (current-message)) t147-log)))
                 (defun t147-boom () (interactive) (error "Boom %d" 1))
                 (global-set-key [f6] #'t147-boom)
                 (erase-buffer)
                 (insert "abc")
                 (goto-char (point-min))
                 (with-current-buffer (messages-buffer)
                   (let ((inhibit-read-only t)) (erase-buffer))))"#,
        )
        .expect("setup");
        for events in [
            vec![Value::Integer(16)],
            vec![Value::symbol("f6")],
            vec![Value::symbol("f7")],
        ] {
            let binding = match resolve_pending(&mut interpreter, &mut env, &events) {
                Resolution::Command(binding) => binding,
                Resolution::Undefined => Value::Nil,
                Resolution::Prefix => panic!("{events:?} is a whole key sequence"),
            };
            let last = *events.last().expect("an event");
            if let Err(error) = execute_binding(&mut interpreter, &mut env, binding, &events, last)
            {
                let echoed = settle_command_error(&mut interpreter, &mut env, error.into_kind())
                    .expect("no termination");
                assert_eq!(echoed, None, "cmd_error reports the error itself");
            }
        }
        let value = crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            r#"(list (reverse t147-log)
                     last-command
                     (with-current-buffer (messages-buffer) (buffer-string)))"#,
        )
        .expect("state");
        // GNU 30.2 under a terminal: the hooks run around the failing
        // commands as around any other, `this-command' still set when the
        // post-command hooks run after an error; `undefined' runs for the
        // nil command; *Messages* logs the signalling function before each
        // error.
        let printed =
            crate::lisp::primitives::call(&mut interpreter, "prin1-to-string", &[value], &mut env)
                .expect("print");
        assert_eq!(
            crate::lisp::primitives::string_like(&printed)
                .expect("a string")
                .text,
            "(((pre previous-line) (post previous-line \"Beginning of buffer\") (pre t147-boom) \
             (post t147-boom \"Boom 1\") (pre nil) (post nil \"<f7> is undefined\")) nil \
             \"previous-line: Beginning of buffer\nt147-boom: Boom 1\n<f7> is undefined\n\")"
        );
    }

    #[test]
    fn isearch_dispatches_through_the_overriding_map_and_exits_on_other_keys() {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(
                &crate::compat::project_root().join("../emacs"),
            )
            .expect("upstream load path"),
            ..Default::default()
        };
        let mut interpreter =
            crate::batch::initialize_batch_interpreter(&options).expect("interpreter initializes");
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable("noninteractive", Value::Nil, &mut env);
        interpreter
            .load_target("isearch")
            .expect("isearch.el loads");
        crate::lisp::primitives::call(&mut interpreter, "erase-buffer", &[], &mut env)
            .expect("erase the scratch banner");
        interpreter.buffer.borrow_mut().insert(
            "alpha one
beta word two
gamma word three
",
        );
        interpreter.buffer.borrow_mut().goto_char(1);

        let send = |interpreter: &mut Interpreter, env: &mut Env, code: i64| {
            let event = Value::Integer(code);
            let resolution = resolve_pending(interpreter, env, std::slice::from_ref(&event));
            let Resolution::Command(binding) = resolution else {
                panic!("event {code} must resolve to a command, got a prefix/undefined");
            };
            let described = format!("{binding}");
            execute_binding(
                interpreter,
                env,
                binding,
                std::slice::from_ref(&event),
                event,
            )
            .expect("command executes");
            described
        };

        // C-s enters isearch; printing characters dispatch through the
        // overriding isearch-mode-map.
        assert_eq!(send(&mut interpreter, &mut env, 0x13), "isearch-forward");
        assert_eq!(
            send(&mut interpreter, &mut env, i64::from(b'w')),
            "isearch-printing-char"
        );
        assert_eq!(
            send(&mut interpreter, &mut env, i64::from(b'o')),
            "isearch-printing-char"
        );
        assert_eq!(
            send(&mut interpreter, &mut env, i64::from(b'r')),
            "isearch-printing-char"
        );
        // The first match ends after "wor" on the second line
        // ("beta word": w=16, o=17, r=18, end=19).
        assert_eq!(interpreter.buffer.borrow().point(), 19);

        // C-a is not an isearch key: the pre-command-hook exits the
        // search and the key's own command runs.
        assert_eq!(
            send(&mut interpreter, &mut env, 0x01),
            "move-beginning-of-line"
        );
        assert_eq!(
            interpreter.buffer.borrow().point(),
            11,
            "point at the match line's start"
        );
        let overriding = interpreter
            .lookup_var("overriding-terminal-local-map", &env)
            .unwrap_or(Value::Nil);
        assert!(
            overriding.is_nil(),
            "leaving isearch removes the overriding map"
        );
    }

    #[test]
    fn universal_argument_prefixes_dispatch_through_simple_el() {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(
                &crate::compat::project_root().join("../emacs"),
            )
            .expect("upstream load path"),
            ..Default::default()
        };
        let mut interpreter =
            crate::batch::initialize_batch_interpreter(&options).expect("interpreter initializes");
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable("noninteractive", Value::Nil, &mut env);
        interpreter.buffer.borrow_mut().insert("abcdefghijklmnop\n");
        interpreter.buffer.borrow_mut().goto_char(1);

        let send = |interpreter: &mut Interpreter, env: &mut Env, code: i64| {
            let event = Value::Integer(code);
            let resolution = resolve_pending(interpreter, env, std::slice::from_ref(&event));
            let Resolution::Command(binding) = resolution else {
                panic!("event {code} must resolve to a command");
            };
            let described = format!("{binding}");
            execute_binding(
                interpreter,
                env,
                binding,
                std::slice::from_ref(&event),
                event,
            )
            .expect("command executes");
            described
        };

        // C-u is simple.el's universal-argument: it stores (4) in
        // prefix-arg and arms the transient map that makes a following
        // digit dispatch digit-argument.
        assert_eq!(send(&mut interpreter, &mut env, 21), "universal-argument");
        assert_eq!(
            interpreter.lookup_var("prefix-arg", &env),
            Some(Value::list([Value::Integer(4)]))
        );
        assert_eq!(
            send(&mut interpreter, &mut env, i64::from(b'8')),
            "digit-argument"
        );
        assert_eq!(
            interpreter.lookup_var("prefix-arg", &env),
            Some(Value::Integer(8))
        );
        // The next ordinary command consumes the accumulated prefix.
        assert_eq!(send(&mut interpreter, &mut env, 6), "forward-char");
        assert_eq!(
            interpreter.buffer.borrow().point(),
            9,
            "C-u 8 C-f moves 8 chars"
        );
        assert_eq!(
            interpreter
                .lookup_var("prefix-arg", &env)
                .unwrap_or(Value::Nil),
            Value::Nil,
            "the chain is consumed"
        );
    }

    #[test]
    fn command_reader_retains_prefix_across_events_and_collecting_filters() {
        let options = crate::batch::BatchRunOptions {
            load_path: crate::compat::emaxx_upstream_load_path(
                &crate::compat::project_root().join("../emacs"),
            )
            .expect("upstream load path"),
            ..Default::default()
        };
        let mut interpreter = crate::batch::initialize_batch_interpreter(&options)
            .expect("GNU Lisp runtime for the differential fixture");
        let mut env = Env::new();
        let form = crate::lisp::reader::Reader::new(include_str!(
            "../tests/fixtures/input-decode-command-reader.el"
        ))
        .read()
        .expect("fixture parses")
        .expect("fixture expression");
        let result = interpreter.eval(&form, &mut env).expect("same GNU fixture");
        assert_eq!(
            format!("{result}"),
            include_str!("../tests/fixtures/input-decode-command-reader.expected").trim()
        );
        interpreter
            .call_function_value(
                Value::symbol("runtime-reader-command-setup"),
                None,
                &[],
                &mut env,
            )
            .expect("reset maps");
        let mut reader =
            crate::lisp::primitives::KeySequenceReader::new(&mut interpreter, Value::Nil, &mut env)
                .expect("reader");
        assert!(matches!(
            reader.read_event(&mut interpreter, Value::symbol("a"), &mut env),
            Ok(Resolution::Prefix)
        ));
        crate::lisp::primitives::call(&mut interpreter, "garbage-collect", &[], &mut env)
            .expect("collect between external events");
        let Resolution::Command(binding) = reader
            .read_event(&mut interpreter, Value::symbol("b"), &mut env)
            .expect("second event")
        else {
            panic!("completed command must resolve");
        };
        assert_eq!(binding, Value::symbol("forward-char"));
        assert_eq!(
            reader.finish(&mut interpreter, false, &mut env),
            vec![Value::symbol("a"), Value::symbol("b")]
        );
        assert_eq!(
            format!(
                "{}",
                interpreter
                    .lookup_var("runtime-reader-calls", &env)
                    .expect("callback trace")
            ),
            "(leaf prefix)"
        );
    }

    #[test]
    fn key_resolution_distinguishes_prefixes_commands_and_undefined() {
        // GNU builds the global map entirely in preloaded Lisp; the bare
        // host starts with no global bindings.  Exercise the resolution
        // mechanics against a real keymap assembled through C-owned
        // primitives instead of any transcribed default table.
        let mut interpreter = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        for form in [
            "(defvar tty-test-global-map (make-sparse-keymap))",
            "(use-global-map tty-test-global-map)",
            "(define-key tty-test-global-map \"\\C-x\" (make-sparse-keymap))",
            "(define-key tty-test-global-map \"h\" 'self-insert-command)",
            "(define-key tty-test-global-map \"\\C-x\\C-s\" 'save-buffer)",
        ] {
            let parsed = crate::lisp::reader::Reader::new(form)
                .read()
                .expect("keymap setup form parses")
                .expect("keymap setup form exists");
            interpreter
                .eval(&parsed, &mut env)
                .expect("keymap setup form evaluates");
        }

        let prefix = resolve_pending(&mut interpreter, &mut env, &[Value::Integer(24)]);
        assert!(matches!(prefix, Resolution::Prefix), "C-x must be a prefix");

        let self_insert = resolve_pending(&mut interpreter, &mut env, &[Value::Integer(104)]);
        let Resolution::Command(command) = self_insert else {
            panic!("h must resolve to a command");
        };
        assert_eq!(command, Value::Symbol("self-insert-command".into()));

        let save = resolve_pending(
            &mut interpreter,
            &mut env,
            &[Value::Integer(24), Value::Integer(19)],
        );
        let Resolution::Command(command) = save else {
            panic!("C-x C-s must resolve to a command");
        };
        assert_eq!(command, Value::Symbol("save-buffer".into()));

        let undefined = resolve_pending(&mut interpreter, &mut env, &[Value::Symbol("f35".into())]);
        assert!(matches!(undefined, Resolution::Undefined));
    }

    #[test]
    fn interactive_execution_inserts_through_lisp() {
        let mut interpreter = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let keys = [Value::Integer(104)];
        execute_binding(
            &mut interpreter,
            &mut env,
            Value::Symbol("self-insert-command".into()),
            &keys,
            Value::Integer(104),
        )
        .expect("self-insert through call-interactively");
        assert_eq!(interpreter.buffer.borrow().buffer_string(), "h");
        assert_eq!(
            interpreter.lookup_var("last-command", &env),
            Some(Value::Symbol("self-insert-command".into()))
        );
    }
}

/// End-to-end pty test, opt-in because it needs a built binary and a GNU
/// `lisp/' tree: `cargo test --release -p emaxx tty_smoke -- --ignored'
/// after `cargo build --release', or run `tools/tty-smoke.py' directly.
#[test]
#[ignore = "requires target/release/emaxx and ../emacs/lisp"]
fn tty_smoke_end_to_end() {
    let status = std::process::Command::new("python3")
        .arg("tools/tty-smoke.py")
        .arg("target/release/emaxx")
        .arg("../emacs/lisp")
        // An explicitly requested gate must fail, rather than silently skip,
        // when its release binary or GNU Lisp inputs are not configured.
        .env("EMAXX_TTY_SMOKE_REQUIRE", "1")
        .status()
        .expect("run tools/tty-smoke.py");
    assert!(status.success(), "tty smoke test failed");
}

/// Complete GNU-vs-Emaxx pty differential gate, including every checked-in
/// real-workflow regression.  It is opt-in because it needs both release
/// binaries and the sibling GNU source tree:
/// `cargo test --release tty_differential_end_to_end -- --ignored --nocapture`
/// after `cargo build --release`.
#[test]
#[ignore = "requires target/release/emaxx and ../emacs/{src/emacs,lisp}"]
fn tty_differential_end_to_end() {
    let status = std::process::Command::new("python3")
        .arg("tools/ttydiff.py")
        .arg("target/release/emaxx")
        .arg("../emacs/src/emacs")
        .arg("../emacs/lisp")
        // An explicitly requested gate must fail, rather than silently skip,
        // when its oracle inputs are not configured.
        .env("EMAXX_TTYDIFF_REQUIRE", "1")
        .status()
        .expect("run tools/ttydiff.py");
    assert!(status.success(), "TTY differential gate failed");
}

/// term.c's tty_menu_activate: draw the dropdown over the glass, run a
/// modal key loop under tty-menu-navigation-map (bound by the caller),
/// and restore the screen behind on exit.  Keyboard navigation drives a
/// virtual selection row exactly as GNU drives its virtual mouse.
fn make_menu_executor(
    queue: SharedEventQueue,
    state: std::rc::Rc<std::cell::RefCell<TtyState>>,
) -> crate::lisp::primitives::TtyMenuExecutor {
    use crate::lisp::primitives::{TtyMenuOutcome, TtyMenuPane};
    Box::new(
        move |interpreter: &mut Interpreter,
              env: &mut Env,
              pane: &TtyMenuPane,
              x0: usize,
              y0: usize| {
            let Ok((cols, rows)) = crate::lisp::eval::terminal::output_size() else {
                return Ok(TtyMenuOutcome::Quit);
            };
            let (cols, rows) = (cols.max(10) as usize, rows.max(4) as usize);
            let mut resolve_face = |name: &str| {
                let mut state = state.borrow_mut();
                *state.face_cache.entry(name.into()).or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(
                        interpreter,
                        env,
                        &Value::Symbol(name.into()),
                    )
                })
            };
            let disabled_attrs = resolve_face("tty-menu-disabled-face");
            let enabled_attrs = resolve_face("tty-menu-enabled-face");
            let selected_raw = resolve_face("tty-menu-selected-face");
            // read_char's echo timer: a sequence still pending while
            // this menu blocks (kboard->echo_string, computed by the
            // resolver through the real echo machinery) is displayed
            // after `echo-keystrokes' idle seconds; 0 or nil disables
            // echoing entirely, as in GNU.
            let mut pending_keystroke_echo = crate::lisp::primitives::take_pending_keystroke_echo();
            let echo_keystrokes = interpreter
                .lookup_var("echo-keystrokes", env)
                .map(|value| match value.kind() {
                    Kind::Integer(seconds) => seconds as f64,
                    Kind::Float(seconds) => seconds.get(),
                    _ => 0.0,
                })
                .unwrap_or(1.0);
            if echo_keystrokes <= 0.0 {
                pending_keystroke_echo = None;
            }
            // The idle window opens when the read starts waiting with
            // the sequence pending — one shared deadline, not one per
            // inner read (consuming the click's release must not push
            // the echo out).
            let pending_echo_deadline = pending_keystroke_echo.as_ref().map(|_| {
                std::time::Instant::now() + std::time::Duration::from_secs_f64(echo_keystrokes)
            });
            // GNU derives the selected face over the enabled one
            // (lookup_derived_face with faces[1] as its base).
            let selected_attrs = merge_cell_attrs(enabled_attrs, selected_raw);

            let left = x0.saturating_sub(1).min(cols.saturating_sub(1));
            let box_width = (pane.width + 2).min(cols - left);
            let title_row = y0.saturating_sub(1);
            let max_items = pane
                .items
                .len()
                .min(rows.saturating_sub(1) - y0.min(rows - 2));
            if max_items == 0 {
                return Ok(TtyMenuOutcome::Quit);
            }

            // The screen behind the menu, restored on the way out
            // (save_and_enable_current_matrix / screen_update).
            let saved: Vec<(usize, PaintRow)> = {
                let state = state.borrow();
                std::iter::once(title_row)
                    .chain(y0..y0 + max_items)
                    .filter(|row| *row < state.painted_rows.len())
                    .map(|row| (row, state.painted_rows[row].clone()))
                    .collect()
            };

            let menu_cell = |text: &str, width: usize, attrs: CellAttrs, base: &PaintRow| {
                let mut row = base.clone();
                let mut painted = String::from(" ");
                painted.push_str(text);
                while painted.chars().count() < width {
                    painted.push(' ');
                }
                let painted: String = painted.chars().take(width).collect();
                row.blit(left, &painted, attrs);
                row
            };
            // The title cell: the pane name with the " >" submenu marker,
            // in the selected face — ' File > ' clobbering the bar.
            let title_width = pane.title.chars().count() + 4;
            let draw = |state: &mut TtyState, selected_row: usize, first_item: usize| {
                let mut out = terminal_output();
                let _ = queue!(out, cursor::Hide);
                if title_row < state.painted_rows.len() {
                    let cell = menu_cell(
                        &format!("{} >", pane.title),
                        title_width.min(cols - left),
                        selected_attrs,
                        &state.painted_rows[title_row],
                    );
                    let _ = paint_row(&mut out, title_row, &cell);
                    state.painted_rows[title_row] = cell;
                }
                // tty_menu_display starts at FIRST_ITEM: a pane taller
                // than the glass shows a window of itself and scrolls.
                for (index, item) in pane
                    .items
                    .iter()
                    .skip(first_item)
                    .take(max_items)
                    .enumerate()
                {
                    let row = y0 + index;
                    if row >= state.painted_rows.len() {
                        break;
                    }
                    let attrs = if index == selected_row {
                        if item.enabled {
                            selected_attrs
                        } else {
                            merge_cell_attrs(disabled_attrs, selected_raw)
                        }
                    } else if item.enabled {
                        enabled_attrs
                    } else {
                        disabled_attrs
                    };
                    let cell = menu_cell(&item.text, box_width, attrs, &state.painted_rows[row]);
                    let _ = paint_row(&mut out, row, &cell);
                    state.painted_rows[row] = cell;
                }
                let _ = queue!(
                    out,
                    cursor::MoveTo(
                        left.min(cols.saturating_sub(1)) as u16,
                        (y0 + selected_row).min(rows.saturating_sub(1)) as u16,
                    ),
                    cursor::Show,
                );
                let _ = out.flush();
            };
            let place_cursor = |selected_row: usize| {
                let mut out = terminal_output();
                let _ = queue!(
                    out,
                    cursor::MoveTo(
                        left.min(cols.saturating_sub(1)) as u16,
                        (y0 + selected_row).min(rows.saturating_sub(1)) as u16,
                    ),
                    cursor::Show,
                );
                let _ = out.flush();
            };

            // GNU's virtual mouse: a visible row plus the scroll base
            // (tty_menu_activate's y and first_item).
            let mut selected_row = 0usize;
            let mut first_item = 0usize;
            let outcome = (|| -> Result<TtyMenuOutcome, LispError> {
                Ok(loop {
                    {
                        let mut state = state.borrow_mut();
                        draw(&mut state, selected_row, first_item);
                    }
                    // One key sequence under the navigation map; prefixes keep
                    // reading, everything else maps per read_menu_input.
                    let mut reader = crate::lisp::primitives::KeySequenceReader::new(
                        interpreter,
                        Value::Nil,
                        env,
                    )?;
                    let command = loop {
                        // Live echo under the menu, but only for messages
                        // emitted since the glass was last painted: GNU's
                        // message3 repaints through a frozen redisplay (the
                        // `(message "")' between cycled menus), while
                        // read_char's input-arrival wipe leaves the old
                        // pixels alone until the next full redisplay.
                        let emitted = crate::lisp::primitives::echo_area_message_tick();
                        if state
                            .try_borrow()
                            .is_ok_and(|state| state.painted_message_tick != emitted)
                        {
                            draw_echo_row_composed(interpreter, env, &state);
                            place_cursor(selected_row);
                        }
                        // A sequence still pending while this menu blocks
                        // echoes after the shared idle window expires
                        // (echo_now through the frozen redisplay); the read
                        // then blocks normally.
                        let event = match (&pending_keystroke_echo, pending_echo_deadline) {
                            (Some(_), Some(deadline)) => {
                                let timed = 'timed: loop {
                                    match queue.try_next_event() {
                                        Err(()) => break 'timed Err(()),
                                        Ok(Some(event)) => break 'timed Ok(event),
                                        Ok(None) => {}
                                    }
                                    let now = std::time::Instant::now();
                                    if now >= deadline {
                                        let (text, spans) = pending_keystroke_echo
                                            .take()
                                            .expect("pending echo present in this arm");
                                        crate::lisp::primitives::set_echo_area_message_with_spans(
                                            text, spans,
                                        );
                                        draw_echo_row_composed(interpreter, env, &state);
                                        place_cursor(selected_row);
                                        break 'timed Err(());
                                    }
                                    let _ = event::poll(
                                        (deadline - now).min(std::time::Duration::from_millis(50)),
                                    );
                                };
                                match timed {
                                    Ok(event) => Some(event),
                                    // Echo shown (or terminal gone): a plain
                                    // blocking read takes over either way.
                                    Err(()) => queue.next_event(),
                                }
                            }
                            _ => queue.next_event(),
                        };
                        let Some(event) = event else {
                            break Value::T;
                        };
                        let QueuedInput::Lisp(event) = event else {
                            continue;
                        };
                        if event == Value::Integer(7) {
                            break Value::T;
                        }
                        match reader.read_event(interpreter, event, env)? {
                            Resolution::Command(binding) => break binding,
                            Resolution::Prefix => {}
                            Resolution::Undefined => break Value::Nil,
                        }
                    };
                    let name = match command.kind() {
                        Kind::Symbol(_) => command.as_symbol().expect("symbol command"),
                        Kind::T => "tty-menu-exit",
                        _ => "",
                    };
                    match name {
                        "tty-menu-exit" => break TtyMenuOutcome::Quit,
                        "tty-menu-next-menu" => break TtyMenuOutcome::NextMenu,
                        "tty-menu-prev-menu" => break TtyMenuOutcome::PrevMenu,
                        "tty-menu-next-item" => {
                            // Below the last visible row GNU scrolls forward
                            // (MI_SCROLL_FORWARD): the window advances until
                            // the selection sits on the final item, and one
                            // more step wraps to the top of the whole menu.
                            if selected_row + 1 < max_items {
                                selected_row += 1;
                            } else if selected_row + first_item + 1 == pane.items.len() {
                                selected_row = 0;
                                first_item = 0;
                            } else {
                                first_item += 1;
                            }
                        }
                        "tty-menu-prev-item" => {
                            // MI_SCROLL_BACK: above the first visible row the
                            // window retreats; at the very top it wraps to
                            // the menu's last window with the final item
                            // selected.
                            if selected_row > 0 {
                                selected_row -= 1;
                            } else if first_item == 0 {
                                selected_row = max_items - 1;
                                first_item = pane.items.len() - max_items;
                            } else {
                                first_item -= 1;
                            }
                        }
                        "tty-menu-select" => {
                            // A separator or disabled item answers no selection
                            // (TTYM_IA_SELECT), like GNU.
                            let selection = selected_row + first_item;
                            if pane.items[selection].enabled {
                                break TtyMenuOutcome::Selected(selection);
                            }
                            break TtyMenuOutcome::NoSelect;
                        }
                        _ => {}
                    }
                })
            })();

            // screen_update: put back what the menu covered.
            {
                let mut state = state.borrow_mut();
                let mut out = terminal_output();
                for (row, cell) in saved {
                    let _ = paint_row(&mut out, row, &cell);
                    if row < state.painted_rows.len() {
                        state.painted_rows[row] = cell;
                    }
                }
                let _ = out.flush();
            }
            outcome
        },
    )
}
