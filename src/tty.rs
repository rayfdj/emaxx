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
use std::path::PathBuf;

use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::{cursor, event, execute, queue, style, terminal};

use crate::batch;
use crate::lisp::eval::Interpreter;
use crate::lisp::primitives::{
    InvisibilitySpec, invisible_run_at, resolve_buffer_invisibility, visual_line_first_line,
};
use crate::lisp::types::{Env, LispError, Value};

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
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
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
    /// The window-start position last agreed with the interpreter's
    /// window model; a differing value there means a command (recenter,
    /// scroll) moved the window and the frontend adopts it.
    synced_start: usize,
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

/// One painted terminal row: its characters and each cell's face
/// attributes.  Two rows compare equal exactly when the glass would look
/// identical.
#[derive(Clone, Debug, PartialEq)]
struct PaintRow {
    text: Vec<char>,
    attrs: Vec<CellAttrs>,
}

impl PaintRow {
    fn blank(cols: usize) -> Self {
        Self {
            text: vec![' '; cols],
            attrs: vec![CellAttrs::default(); cols],
        }
    }

    /// A sentinel no real row equals, forcing the first paint.
    fn unpainted() -> Self {
        Self {
            text: vec!['\u{0}'],
            attrs: vec![CellAttrs::default()],
        }
    }

    fn blit(&mut self, col: usize, text: &str, attrs: CellAttrs) {
        for (offset, c) in text.chars().enumerate() {
            let Some(cell) = self.text.get_mut(col + offset) else {
                break;
            };
            *cell = c;
            self.attrs[col + offset] = attrs;
        }
    }

    /// Layer FACE attributes over the cells in [FROM, TO): attributes the
    /// face leaves unspecified keep what the cell already shows.
    fn overlay(&mut self, from: usize, to: usize, attrs: CellAttrs) {
        for at in from..to.min(self.attrs.len()) {
            self.attrs[at] = merge_cell_attrs(self.attrs[at], attrs);
        }
    }
}

struct TtyState {
    /// Per-window display anchors, keyed by window record id.
    views: std::collections::HashMap<u64, WindowView>,
    /// A `C-u' sequence is accumulating: digits and `-' extend the prefix
    /// argument instead of dispatching (GNU's universal-argument--mode).
    prefix_active: bool,
    /// Events of the in-progress (multi-key) sequence.
    pending: Vec<Value>,
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
    /// Resolved face attributes, valid while the interpreter's face
    /// generation is unchanged (GNU's face_change flag).
    face_cache: std::collections::HashMap<String, CellAttrs>,
    face_cache_generation: u64,
    /// The composed echo row currently belongs to an active minibuffer:
    /// the interpreter-less blocking-read repaint must not overwrite it
    /// with the plain message channel (the composed paint carries the
    /// prompt face and overlay strings).
    minibuffer_owns_echo: bool,
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
}

impl TtyState {
    fn new() -> Self {
        Self {
            views: std::collections::HashMap::new(),
            prefix_active: false,
            pending: Vec::new(),
            echo: String::new(),
            painted_rows: Vec::new(),
            painted_echo: Vec::new(),
            echo_rows: 1,
            echo_max_rows: 6,
            painted_size: (0, 0),
            face_cache: std::collections::HashMap::new(),
            face_cache_generation: 0,
            minibuffer_owns_echo: false,
            menu_bar_row: None,
            painted_message_tick: 0,
        }
    }
}

pub fn run(initial_file: Option<PathBuf>) -> Result<i32, String> {
    let mut interpreter = batch::initialize_interactive_interpreter()?;
    let mut env: Env = Vec::new();
    interpreter.set_variable("noninteractive", Value::Nil, &mut env);
    if let Some(path) = initial_file {
        let path = path.display().to_string();
        let find_file = call(
            &mut interpreter,
            &mut env,
            "find-file",
            &[Value::String(path.clone().into())],
        );
        if let Err(error) = &find_file {
            // The real files.el `find-file' is the only honest visit path;
            // fabricating a native visit here would keep the screen
            // plausible while hiding the breakage.
            panic!("find-file {path} failed: {error:?}");
        }
        debug_log(&format!(
            "startup buffer={:?} point={}",
            interpreter.buffer.name,
            interpreter.buffer.point()
        ));
    }

    // Publish the terminal's color capability before the first redraw, then
    // recompute every face from its spec exactly as GNU's set_tty_color_mode
    // does after tty_setup_colors: safe_calln (Qtty_set_up_initial_frame_faces)
    // hands the work to faces.el against the new display.
    interpreter.set_tty_display_colors(terminal_color_cells());
    interpreter.set_tty_terminal_type(std::env::var("TERM").ok());
    if let Ok(forms) = crate::lisp::reader::Reader::new(
        // startup.el registers the standard tty palette before faces
        // realize (command-line's tty-register-default-colors call).
        "(progn (tty-register-default-colors) (tty-set-up-initial-frame-faces))",
    )
    .read_all()
        && let Some(form) = forms.first()
    {
        let _ = interpreter.eval(form, &mut env);
    }

    let guard = TerminalGuard::enter().map_err(|error| error.to_string())?;
    let queue = SharedEventQueue::default();
    let state = std::rc::Rc::new(std::cell::RefCell::new(TtyState::new()));
    crate::lisp::primitives::set_tty_event_reader(Some(make_event_reader(
        queue.clone(),
        std::rc::Rc::clone(&state),
    )));
    // The polling companion: one short wait per call, `None' inner value
    // when the terminal stays quiet — blocking reads pump ripe timers
    // between polls, GNU read_char's timer_check.
    crate::lisp::primitives::set_tty_event_poller(Some(Box::new({
        let queue = queue.clone();
        let state = std::rc::Rc::clone(&state);
        move || {
            draw_echo_row(&state);
            match queue.try_next_event() {
                Err(()) => None,
                Ok(Some(QueuedInput::Mouse(_))) => Some(None),
                Ok(Some(QueuedInput::Lisp(event))) => Some(Some(event)),
                Ok(None) => {
                    let _ = event::poll(std::time::Duration::from_millis(50));
                    Some(None)
                }
            }
        }
    })));
    // Command code that reads events itself (the minibuffer) repaints the
    // frame through this hook, so window-configuration changes made
    // mid-read — a *Completions* pop-up — reach the glass immediately.
    crate::lisp::primitives::set_tty_frame_redraw(Some(Box::new({
        let queue = queue.clone();
        let state = std::rc::Rc::clone(&state);
        move |interpreter, env| {
            if queue.input_pending() {
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
    // startup.el's display-startup-echo-area-message: the startup hint
    // sits in the echo area until the first command replaces it (F10's
    // menu leaves it visible, as GNU does).
    if let Ok(message) = interpreter.call_function_value(
        Value::Symbol("substitute-command-keys".into()),
        None,
        &[Value::String(
            "For information about GNU Emacs and the GNU system, type \\[about-emacs].".into(),
        )],
        &mut env,
    ) && let Ok(text) = crate::lisp::primitives::string_text(&message)
    {
        // The substituted key carries help-key-binding face (help.el's
        // substitute-command-keys propertizes it); the echo row paints
        // the span like GNU's startup echo.
        let spans = crate::lisp::primitives::string_face_spans(&message);
        crate::lisp::primitives::set_echo_area_message_with_spans(text, spans);
    }
    let code = command_loop(&mut interpreter, &mut env, &queue, &state);
    crate::lisp::primitives::set_tty_frame_redraw(None);
    crate::lisp::primitives::set_tty_event_reader(None);
    crate::lisp::primitives::set_tty_event_poller(None);
    crate::lisp::primitives::set_tty_input_pending_check(None);
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
/// C-g answers `None' and becomes GNU's `quit' signal at the consuming
/// primitive.
fn make_event_reader(
    queue: SharedEventQueue,
    state: std::rc::Rc<std::cell::RefCell<TtyState>>,
) -> Box<dyn FnMut() -> Option<Value>> {
    Box::new(move || {
        draw_echo_row(&state);
        let event = loop {
            // Typed mouse input is command-loop currency; a blocking
            // Lisp reader never sees it.
            if let QueuedInput::Lisp(event) = queue.next_event()? {
                break event;
            }
        };
        if event == Value::Integer(7) {
            return None;
        }
        Some(event)
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
        .rposition(|c| *c != ' ')
        .map(|last| last + 1)
        .unwrap_or(0);
    let has_newline = long.text[..used].contains(&'\n');
    if used <= cols && !has_newline {
        let mut row = PaintRow::blank(cols);
        for (col, (c, attrs)) in long
            .text
            .iter()
            .zip(long.attrs.iter())
            .enumerate()
            .take(cols)
        {
            row.text[col] = *c;
            row.attrs[col] = *attrs;
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
        if col == usable {
            row.text[usable] = '\\';
            rows.push(std::mem::replace(&mut row, PaintRow::blank(cols)));
            col = 0;
            if rows.len() == max_rows {
                return rows;
            }
        }
        row.text[col] = c;
        row.attrs[col] = long.attrs[index];
        col += 1;
    }
    rows.push(row);
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
        .rposition(|c| *c != ' ')
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
        if long.text[index] == '\n' {
            if row + 1 >= max_rows {
                break;
            }
            row += 1;
            col = 0;
        } else {
            if col == usable {
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
    match interpreter.lookup_var("max-mini-window-height", &Vec::new()) {
        Some(Value::Integer(lines)) if lines > 0 => (lines as usize).min(rows.saturating_sub(2)),
        Some(Value::Float(fraction)) if fraction > 0.0 => {
            (((rows as f64) * fraction) as usize).clamp(1, rows.saturating_sub(2))
        }
        _ => ((rows as f64 * 0.25) as usize).clamp(1, rows.saturating_sub(2)),
    }
}

/// The composed echo repaint for contexts that hold the interpreter —
/// the menu executor's message3-style paint carries face spans (the
/// C-h help hint's help-key-binding face), which the interpreter-less
/// draw_echo_row cannot resolve.
fn draw_echo_row_composed(
    interpreter: &mut Interpreter,
    env: &mut Env,
    state: &std::rc::Rc<std::cell::RefCell<TtyState>>,
) {
    let Ok((cols, rows)) = terminal::size() else {
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
    let (long, _, _) = compose_echo_row(
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
    let base = (rows.max(4) as usize).saturating_sub(mini_rows);
    let mut out = io::stdout();
    for (index, echo_row) in echo_paint.iter().enumerate() {
        let _ = paint_row(&mut out, base + index, echo_row);
    }
    if interpreter
        .lookup_var("cursor-in-echo-area", env)
        .is_some_and(|value| value.is_truthy())
    {
        let (row, col) = wrapped_echo_cursor(&long, frontend_echo.chars().count(), cols, mini_rows);
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

/// Paint the live echo-area line without interpreter access; the message
/// text lives in session state exactly so blocking readers can show it.
/// When the full redisplay already painted this text (with its face
/// attributes — the minibuffer prompt), leave that paint alone.
fn draw_echo_row(state: &std::rc::Rc<std::cell::RefCell<TtyState>>) {
    let Ok((cols, rows)) = terminal::size() else {
        return;
    };
    let text = crate::lisp::primitives::echo_area_message().unwrap_or_default();
    let cols = cols.max(10) as usize;
    let mut mini_rows = 1usize;
    if let Ok(mut state) = state.try_borrow_mut() {
        // An active minibuffer's composed row (prompt face, overlay
        // strings) belongs to full redisplay; commands are the only
        // thing that changes it, and the frame-redraw hook repaints
        // after each one.
        if state.minibuffer_owns_echo {
            return;
        }
        mini_rows = state.echo_rows.max(1);
        let max_rows = state.echo_max_rows.max(1);
        // Painting (or confirming) the channel brings the glass up to
        // date with every message emitted so far.
        state.painted_message_tick = crate::lisp::primitives::echo_area_message_tick();
        let painted: String = state
            .painted_echo
            .first()
            .map(|row| row.text.iter().collect())
            .unwrap_or_default();
        if state.painted_echo.len() <= 1 && painted.trim_end_matches(' ') == text {
            return;
        }
        state.painted_echo = Vec::new();
        // A message taller than the current mini window grows it right
        // now, GNU's message3 entering redisplay: the rows it covers
        // are marked stale so the next full redisplay repaints the
        // resized window tree beneath.
        let mut probe = PaintRow::blank(cols * max_rows);
        probe.blit(0, &text, CellAttrs::default());
        let needed = wrap_echo_paint(&probe, cols, max_rows).len();
        if needed > mini_rows {
            for row in (rows as usize).saturating_sub(needed)..(rows as usize) {
                if row < state.painted_rows.len() {
                    state.painted_rows[row] = PaintRow::unpainted();
                }
            }
            state.echo_rows = needed;
            mini_rows = needed;
        }
    }
    let mut long = PaintRow::blank(cols * mini_rows);
    long.blit(0, &text, CellAttrs::default());
    let mut echo_paint = wrap_echo_paint(&long, cols, mini_rows);
    echo_paint.resize(mini_rows, PaintRow::blank(cols));
    let base = (rows as usize).saturating_sub(mini_rows);
    let mut out = io::stdout();
    for (index, echo_row) in echo_paint.iter().enumerate() {
        let _ = paint_row(&mut out, base + index, echo_row);
    }
    if crate::lisp::primitives::tty_cursor_in_echo_area() {
        let (row, col) = wrapped_echo_cursor(&long, text.chars().count(), cols, mini_rows);
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
}

fn command_loop(
    interpreter: &mut Interpreter,
    env: &mut Env,
    queue: &SharedEventQueue,
    shared_state: &std::rc::Rc<std::cell::RefCell<TtyState>>,
) -> Result<i32, String> {
    loop {
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
            let idle = idle_since
                .get_or_insert_with(std::time::Instant::now)
                .elapsed();
            if crate::lisp::primitives::run_due_timers(interpreter, env, idle.as_secs_f64()) {
                let mut state = shared_state.borrow_mut();
                let _ = redraw(interpreter, env, &mut state);
            }
            // keyboard.c's kbd_buffer_get_event blocks inside
            // wait_reading_process_output (READ_KBD -1, do_display set):
            // subprocess output reaches filters and sentinels while the
            // loop waits for a key, and a delivery repaints the glass,
            // process.c's redisplay_preserve_echo_area.  Process output
            // does not end the idle period; only keyboard input does.
            match pump_processes_during_wait(interpreter, env) {
                Ok(true) => {
                    let mut state = shared_state.borrow_mut();
                    let _ = redraw(interpreter, env, &mut state);
                }
                Ok(false) => {}
                Err(LispError::Terminate(termination)) => {
                    return Ok(termination.exit_code);
                }
                Err(_) => {}
            }
            let _ = event::poll(std::time::Duration::from_millis(50));
        };
        // Input arrived: the idle period is over (timer_stop_idle).
        crate::lisp::primitives::tty_note_idle_end();
        // Typed mouse input becomes GNU's click event now that the frame
        // state is in reach; motion and wheel produce nothing.
        let event = match event {
            QueuedInput::Mouse(raw) => match synthesize_mouse_event(interpreter, env, raw) {
                Some(event) => event,
                None => continue,
            },
            QueuedInput::Lisp(event) => event,
        };
        // read_char wipes a lingering message the moment any input event
        // arrives — sequences and silently-discarded button-downs
        // included; the glass catches up at the next redisplay.
        crate::lisp::primitives::expire_echo_area_message();
        // The state borrow is scoped: `execute_binding' below may re-enter
        // redisplay through the minibuffer's frame-redraw hook, and
        // resolution itself can run the whole dropdown executor (a
        // keymap-bound mouse click pops it), both of which borrow the
        // same cell.
        let pending_snapshot = {
            let state = &mut *shared_state.borrow_mut();

            // A fresh key erases a previous command's echo, but not the
            // accumulating `C-u' chain's own display (GNU's prefix echo
            // survives until a non-prefix command consumes it).
            if state.pending.is_empty() && !state.prefix_active {
                state.echo.clear();
            }
            state.pending.push(event);
            state.pending.clone()
        };
        let resolution = resolve_pending(interpreter, env, &pending_snapshot);
        let dispatch = {
            let state = &mut *shared_state.borrow_mut();
            debug_log(&format!(
                "keys {:?} -> {}",
                describe_keys(&state.pending),
                match &resolution {
                    Resolution::Command(binding) => format!("command {binding}"),
                    Resolution::Prefix => "prefix".to_string(),
                    Resolution::Undefined => "undefined".to_string(),
                }
            ));
            match resolution {
                Resolution::Command(binding) => {
                    // GNU erases the key echo when dispatch begins — a
                    // command that blocks (a minibuffer read) must not
                    // leave its own key sequence on the glass.  An
                    // accumulating C-u chain keeps its echo: the digits
                    // extend it after the prefix command runs.
                    if !state.prefix_active {
                        state.echo.clear();
                    }
                    let keys = std::mem::take(&mut state.pending);
                    Some((binding, keys))
                }
                Resolution::Prefix => {
                    state.echo = format!("{}-", describe_keys(&state.pending));
                    None
                }
                Resolution::Undefined => {
                    // keyboard.c discards unbound button-down events
                    // silently; unbound clicks echo like any key.
                    let silent = state.pending.len() == 1
                        && matches!(
                            state.pending[0].car(),
                            Ok(Value::Symbol(head)) if head.contains("down-mouse-")
                        );
                    if !silent {
                        state.echo = format!("{} is undefined", describe_keys(&state.pending));
                    }
                    state.pending.clear();
                    state.prefix_active = false;
                    None
                }
            }
        };
        let Some((binding, keys)) = dispatch else {
            continue;
        };
        let last_event = keys.last().cloned().unwrap_or(Value::Nil);
        let command_error = match execute_binding(interpreter, env, binding, &keys, last_event) {
            Ok(()) => None,
            Err(LispError::Terminate(termination)) => {
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
                Some(command_error_text(interpreter, env, &error))
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

use crate::lisp::primitives::KeyResolution as Resolution;

fn resolve_pending(interpreter: &mut Interpreter, env: &mut Env, pending: &[Value]) -> Resolution {
    crate::lisp::primitives::resolve_key_sequence(interpreter, env, pending)
}

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
        match outcome {
            Ok(pumped) => progressed |= pumped,
            Err(error @ LispError::Terminate(_)) => return Err(error),
            Err(error) => {
                let text = command_error_text(interpreter, env, &error);
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
fn synthesize_mouse_event(
    interpreter: &mut Interpreter,
    env: &mut Env,
    raw: RawMouseInput,
) -> Option<Value> {
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

    let (_, rows) = terminal::size().ok()?;
    let menu_bar_rows = ((rows as i64) - interpreter.frame_text_height()).clamp(0, 1);
    let posn = if menu_bar_rows > 0 && row == 0 {
        // xt-mouse builds the menu-bar posn with a nil window slot —
        // menu-bar-open-mouse refuses events that sit inside a window.
        let _ = env;
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
            interpreter.buffer.point()
        } else {
            interpreter
                .get_buffer_by_id(window.buffer_id)
                .map(|buffer| buffer.point())
                .unwrap_or(1)
        };
        Value::list([
            Value::Record(window.window_id),
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
        match event {
            Value::Integer(27) => {
                meta = true;
                continue;
            }
            Value::Integer(code) => parts.push(describe_char(*code, meta)),
            Value::Symbol(name) => parts.push(if meta {
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
    /// Cursor cell within the window's text rect, for the selected window.
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
        && match interpreter.lookup_var("truncate-partial-width-windows", env) {
            Some(Value::Integer(columns)) => columns > info.width as i64,
            Some(value) => value.is_truthy(),
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
    let current_line_only =
        !suspended && matches!(&auto_mode_value, Value::Symbol(name) if name == "current-line");
    let truncate_now = truncate_lines || hscroll > 0 || partial_truncates;
    if auto_mode && !suspended && truncate_now {
        let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
            Some(&interpreter.buffer)
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
        let point_line = buffer.line_number_at_pos(point_pos);
        let line_text = displayed_line_text(buffer, point_line);
        let point_dcol =
            display_column(&line_text, point_pos - buffer.line_start_at(point_pos)) as i64;
        let line_w = display_width(&line_text);
        let w = body_width as i64;
        let margin = interpreter
            .lookup_var("hscroll-margin", env)
            .and_then(|value| value.as_integer().ok())
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
            let at_eol = point_dcol >= line_w as i64;
            // hscroll_window_tree's iterator runs with the line-number
            // glyphs produced, so its current_x carries their columns.
            let current_x = point_dcol + lnum_cols as i64;
            let step = interpreter.lookup_var("hscroll-step", env);
            let new_hscroll = match step {
                Some(Value::Float(relative)) if relative >= 0.0 => {
                    let wanted = if cursor_x >= text_w - margin {
                        (w as f64) * (1.0 - relative) - margin as f64
                    } else {
                        (w as f64) * relative + (margin + x_offset) as f64
                    };
                    (current_x - wanted as i64).max(0)
                }
                Some(Value::Integer(step)) if step > 0 => {
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
    if !matches!(items.first(), Some(Value::Symbol(head)) if head == "space") {
        return None;
    }
    let position = items
        .iter()
        .position(|item| matches!(item, Value::Symbol(name) if name == ":align-to"))?;
    match items.get(position + 1) {
        Some(Value::Integer(column)) if *column >= 0 => Some(*column as usize),
        _ => None,
    }
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
}

fn visual_line_at(
    buffer: &crate::buffer::Buffer,
    spec: &InvisibilitySpec,
    first_line: usize,
) -> VisualLine {
    if !spec.active {
        let (text, display_map) = displayed_line_with_map(buffer, first_line);
        let display_count = text.chars().count();
        let map = display_map.unwrap_or_else(|| (0..=display_count).collect());
        // Invert the (raw offset -> display index) map; `display'
        // padding cells belong to the raw char that produced them.
        let mut raw_of_display = vec![0usize; display_count + 1];
        for raw in 0..map.len().saturating_sub(1) {
            let end = map[raw + 1].min(display_count + 1);
            for cell in raw_of_display.iter_mut().take(end).skip(map[raw]) {
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
        };
    }
    let line_begin = buffer.line_start_of(first_line);
    let end = buffer.point_max();
    let has_display_prop = buffer.has_text_property_named("display");
    let mut text = String::new();
    let mut map: Vec<usize> = Vec::new();
    let mut raw_of_display: Vec<usize> = Vec::new();
    let mut display_chars = 0usize;
    let mut col = 0usize;
    let mut lines_spanned = 1usize;
    let mut ellipses: Vec<usize> = Vec::new();
    let mut pos = line_begin;
    while pos < end {
        if let Some((run_end, ellipsis)) = invisible_run_at(buffer, spec, pos) {
            lines_spanned += buffer
                .buffer_substring(pos, run_end)
                .map(|hidden| hidden.matches('\n').count())
                .unwrap_or(0);
            if ellipsis {
                ellipses.push(display_chars);
                for _ in 0..3 {
                    text.push('.');
                    raw_of_display.push(run_end - line_begin);
                    display_chars += 1;
                    col += 1;
                }
            }
            // Hidden positions land past the ellipsis: a face span over
            // folded text collapses to nothing instead of painting the
            // dots (GNU gives the ellipsis the preceding text's face).
            for _ in pos..run_end {
                map.push(display_chars);
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
        let align_target = if has_display_prop {
            buffer
                .text_property_at(pos, "display")
                .and_then(|value| space_align_to_target(&value))
        } else {
            None
        };
        if let Some(target) = align_target {
            map.push(display_chars);
            let pad = target.saturating_sub(col);
            for _ in 0..pad {
                text.push(' ');
                raw_of_display.push(offset);
                display_chars += 1;
            }
            col += pad;
        } else {
            map.push(display_chars);
            text.push(ch);
            raw_of_display.push(offset);
            display_chars += 1;
            col += if ch == '\t' { 8 - (col % 8) } else { 1 };
        }
        pos += 1;
    }
    map.push(display_chars);
    raw_of_display.push(pos - line_begin);
    VisualLine {
        text,
        map,
        raw_of_display,
        lines_spanned,
        ellipses,
    }
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
    if !buffer.has_text_property_named("display") {
        return (raw, None);
    }
    let line_begin = buffer.line_start_of(line);
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
    for (offset, ch) in raw.chars().enumerate() {
        map.push(expanded_chars);
        let display = buffer.text_property_at(line_begin + offset, "display");
        let replacement = match display {
            Some(Value::String(ref text)) => Some(text.to_string()),
            Some(Value::StringObject(ref state)) => {
                Some(std::cell::RefCell::borrow(state).text.clone())
            }
            _ => None,
        };
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
            continue;
        }
        string_run = None;
        let target = display.and_then(|value| space_align_to_target(&value));
        if let Some(target) = target {
            changed = true;
            let pad = target.saturating_sub(col);
            for _ in 0..pad {
                expanded.push(' ');
            }
            expanded_chars += pad;
            col += pad;
        } else if ch == '\t' {
            expanded.push(ch);
            expanded_chars += 1;
            col += 8 - (col % 8);
        } else {
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

fn displayed_line_text(buffer: &crate::buffer::Buffer, line: usize) -> String {
    displayed_line_with_map(buffer, line).0
}

/// Plan one window's text: adopt a commanded window-start, keep point
/// visible with GNU's recenter-on-jump model (selected window only), and
/// render the visible rows under the window's own wrap-or-truncate
/// geometry.
#[allow(clippy::too_many_arguments)]
fn plan_window_text(
    buffer: &crate::buffer::Buffer,
    spec: &InvisibilitySpec,
    view: &mut WindowView,
    commanded_start: usize,
    point: usize,
    text_rows: usize,
    body_width: usize,
    truncate: bool,
    geometry: &RenderGeometry,
    selected: bool,
) -> WindowPlan {
    let hscroll = geometry.hscroll;
    // A line-number column's glyphs precede the text on every row, so
    // the text lays out in the remaining columns.
    let lnum_cols = geometry.lnum.map_or(0, |layout| layout.cols);
    let body_width = body_width.saturating_sub(lnum_cols).max(1);
    let usable = body_width.saturating_sub(1).max(1);
    let segs_of = |line: &str| {
        if truncate {
            1
        } else {
            segment_count(display_width(line), usable)
        }
    };
    let line_text_at = |line: usize| visual_line_at(buffer, spec, line).text;
    // The display column a buffer position occupies on its visual line
    // (invisible runs collapse; the ellipsis and joined tails count).
    let visual_dcol = |first_line: usize, pos: usize| {
        let visual = visual_line_at(buffer, spec, first_line);
        let offset = pos.saturating_sub(buffer.line_start_of(first_line));
        let index = visual
            .map
            .get(offset)
            .copied()
            .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0));
        display_column(&visual.text, index)
    };

    let point_line = visual_line_first_line(buffer, spec, buffer.line_number_at_pos(point));
    let point_line_text = line_text_at(point_line);
    let point_dcol = visual_dcol(point_line, point);
    let (point_seg, cursor_col) = if truncate {
        // An hscrolled window spends column zero on the left `$' glyph,
        // which replaces the character at column HSCROLL itself: a
        // character at column C lands on screen column C - HSCROLL.
        let on_screen = (point_dcol as i64 - hscroll as i64).max(0);
        (0, (on_screen as usize).min(body_width.saturating_sub(1)))
    } else {
        let seg = (point_dcol / usable).min(segs_of(&point_line_text) - 1);
        (seg, point_dcol - seg * usable)
    };

    // A command that owns its scrolling (recenter, scroll-up) moved the
    // interpreter's window-start; adopt it as the window's top before
    // deciding whether point needs recentering.  Non-selected windows
    // simply show their commanded start — GNU only enforces point
    // visibility in the selected window's redisplay.
    if commanded_start != view.synced_start || !selected {
        let start_line =
            visual_line_first_line(buffer, spec, buffer.line_number_at_pos(commanded_start));
        let start_dcol = visual_dcol(start_line, commanded_start);
        view.top_line = start_line;
        view.top_seg = if truncate { 0 } else { start_dcol / usable };
    }

    // Keep point visible, counting visual rows (wrapped lines span
    // several); recenter on a jump like GNU's default scrolling.
    let mut recenter = selected
        && (view.top_line == 0
            || point_line < view.top_line
            || (point_line == view.top_line && point_seg < view.top_seg));
    let mut point_row = 0usize;
    if selected && !recenter {
        if point_line == view.top_line {
            point_row = point_seg - view.top_seg;
        } else if point_line - view.top_line >= text_rows && !spec.active {
            // Every line fills at least one row: certainly off-screen.
            recenter = true;
        } else {
            // Walk the visual lines between the top and point; hidden
            // newlines make one visual line span several raw lines.
            let mut rows_before = 0usize;
            let mut walk = view.top_line;
            let mut first = true;
            while walk < point_line {
                let visual = visual_line_at(buffer, spec, walk);
                let segs = segs_of(&visual.text);
                let skipped = if first { view.top_seg } else { 0 };
                first = false;
                rows_before += segs.saturating_sub(skipped);
                if rows_before > text_rows {
                    break;
                }
                walk += visual.lines_spanned;
            }
            point_row = rows_before.saturating_add(point_seg);
        }
        if point_row >= text_rows {
            recenter = true;
        }
    }
    if recenter {
        // Walk back half a window of visual rows from point, GNU's
        // recentering target.
        let mut budget = text_rows / 2;
        let (mut line, mut seg) = (point_line, point_seg);
        let mut stepped = 0usize;
        while budget > 0 {
            if seg > 0 {
                let step = seg.min(budget);
                seg -= step;
                budget -= step;
                stepped += step;
            } else if line > 1 {
                line = visual_line_first_line(buffer, spec, line - 1);
                seg = segs_of(&line_text_at(line)) - 1;
                budget -= 1;
                stepped += 1;
            } else {
                break;
            }
        }
        view.top_line = line;
        view.top_seg = seg;
        point_row = stepped;
    }

    // Fetch only the window: redisplay cost must follow the screen size,
    // never the buffer size.  Each visual line yields at least one row,
    // so text_rows visual lines always cover the window.
    let last_line = buffer.line_number_at_pos(buffer.point_max());
    let mut rendered: Vec<(String, usize, usize, usize, usize)> = Vec::with_capacity(text_rows);
    // First row past the window, as (line, segment): the window's end.
    let mut past_window: Option<(usize, usize)> = None;
    let mut fill_line = view.top_line;
    let mut first_fill = true;
    'fill: while rendered.len() < text_rows && fill_line <= last_line {
        let visual = visual_line_at(buffer, spec, fill_line);
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
                truncate_row_from(&visual.text, body_width, row_hscroll)
            } else {
                truncate_row_hscrolled(&visual.text, body_width, row_hscroll)
            }]
        } else {
            wrap_segments(&visual.text, body_width)
        };
        let from = if first_fill {
            view.top_seg.min(segments.len() - 1)
        } else {
            0
        };
        first_fill = false;
        let next_line = fill_line + visual.lines_spanned;
        for (seg_index, segment) in segments.iter().enumerate().skip(from) {
            let row_start = position_of_visual_row(buffer, spec, fill_line, seg_index, usable);
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

    let top_pos = position_of_visual_row(buffer, spec, view.top_line, view.top_seg, usable);
    let window_end = match past_window {
        // A row past the final buffer line means the window shows
        // everything: window-end is ZV (a buffer without a trailing
        // newline has no line beyond its last).
        Some((line, _)) if line > last_line => buffer.point_max(),
        Some((line, seg)) => position_of_visual_row(buffer, spec, line, seg, usable),
        None => buffer.point_max(),
    };
    WindowPlan {
        rendered,
        top_pos,
        window_end,
        cursor: selected.then_some((point_row, cursor_col)),
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
        let z = interpreter.buffer.point_max();
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
                &interpreter.buffer,
                pos,
                "fontified",
            );
            if !fontified.is_nil() {
                let (_, span_end) = interpreter.buffer.text_property_interval_around(pos);
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
                &interpreter.buffer,
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
        value.car(),
        Ok(Value::Symbol(ref name)) if name == "lambda"
    );
    if !matches!(value, Value::Cons(_)) || is_bare_lambda {
        let _ = interpreter.call_function_value(
            value.clone(),
            None,
            std::slice::from_ref(&pos_value),
            env,
        );
        return;
    }
    let mut rest = value.clone();
    while let Value::Cons(_) = rest {
        let Ok(function) = rest.car() else {
            break;
        };
        if matches!(function, Value::T) {
            let mut globals = interpreter
                .default_value("fontification-functions")
                .unwrap_or(Value::Nil);
            while let Value::Cons(_) = globals {
                let Ok(global_fn) = globals.car() else {
                    break;
                };
                if !matches!(global_fn, Value::T) {
                    let _ = interpreter.call_function_value(
                        global_fn,
                        None,
                        std::slice::from_ref(&pos_value),
                        env,
                    );
                }
                let Ok(next) = globals.cdr() else {
                    break;
                };
                globals = next;
            }
        } else {
            let _ = interpreter.call_function_value(
                function,
                None,
                std::slice::from_ref(&pos_value),
                env,
            );
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
    let (cols, rows) = terminal::size()?;
    let cols = cols.max(10) as usize;
    let rows = rows.max(4) as usize;
    // The interpreter's window tree carries the frame geometry: keep it
    // agreeing with the terminal so splits compute GNU's tty sizes.
    if interpreter.frame_width() != cols as i64 || interpreter.frame_height() != rows as i64 {
        interpreter.set_tty_frame_size(cols as i64, rows as i64);
    }
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
    let echo_empty = echo_paint.len() == 1
        && echo_paint[0].text.iter().all(|c| *c == ' ')
        && !echo_from_minibuffer;
    let previous_echo_rows = state.echo_rows;
    state.echo_rows = if echo_empty {
        1
    } else if exact_echo && !echo_from_minibuffer {
        // resize_echo_area_exactly: at a command boundary a displayed
        // message sizes the mini window to exactly its rows; an active
        // minibuffer keeps its grown size (echo_area_buffer[0] is nil
        // there, so keyboard.c never resizes it exactly).
        echo_paint.len().min(max_mini.max(1))
    } else {
        state.echo_rows.max(echo_paint.len()).min(max_mini.max(1))
    };
    if state.echo_rows != previous_echo_rows {
        // GNU's grow_mini_window/shrink_mini_window resize the real
        // window tree: window.el's own resizer stages new sizes for the
        // windows above the mini window, and the staged sizes apply --
        // window-height and friends answer the shrunken sizes while the
        // echo area is grown.  The render-level clamp below stays as
        // the safety net if the Lisp resizer declines.
        let delta = state.echo_rows as i64 - previous_echo_rows as i64;
        let _ = (|interpreter: &mut Interpreter, env: &mut Env| -> Result<(), LispError> {
            let root = call(interpreter, env, "frame-root-window", &[])?;
            let grow = call(
                interpreter,
                env,
                "window--resize-root-window-vertically",
                &[root, Value::Integer(-delta), Value::T],
            )?;
            if grow.as_integer().unwrap_or(0) != 0 {
                call(
                    interpreter,
                    env,
                    "window-resize-apply",
                    &[Value::Nil, Value::Nil],
                )?;
            }
            Ok(())
        })(interpreter, env);
    }
    let frame_rows = rows - state.echo_rows; // everything above the echo area
    // The rows the frame keeps above the window tree are the menu bar's
    // (GNU's FRAME_MENU_BAR_LINES); `menu-bar-mode' drives the count
    // through the `menu-bar-lines' frame parameter.
    let menu_lines = ((rows as i64) - interpreter.frame_text_height()).clamp(0, 1) as usize;
    let full_repaint = state.painted_size != (cols, rows);
    if full_repaint {
        // A resize changes the wrap geometry under every saved segment
        // index; re-anchor each window instead of trusting stale ones.
        state.views.clear();
    }
    // Resolved faces stay cached until a face definition changes (GNU's
    // face_change flag) — resolution evaluates Lisp per attribute.
    let face_generation = interpreter.face_definitions_generation();
    if full_repaint || state.face_cache_generation != face_generation {
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
        let buffer = &interpreter.buffer;
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
    let mut cursor_position = (0u16, 0u16);
    let mut selected_sync: Option<(usize, crate::lisp::primitives::InteractiveWindowMetrics)> =
        None;
    struct ModeLineJob {
        window_id: u64,
        point: usize,
        row: usize,
        left: usize,
        body_width: usize,
        metrics: crate::lisp::primitives::InteractiveWindowMetrics,
    }
    let mut mode_line_jobs: Vec<ModeLineJob> = Vec::new();
    // A window whose buffer sets `header-line-format' spends its first
    // body row on the header (xdisp.c window_wants_header_line); the
    // header renders through the mode-line machinery in the
    // `header-line' face, padded with spaces.
    let mut header_line_jobs: Vec<ModeLineJob> = Vec::new();
    // Face spans over window text apply after the text lands (their
    // resolution evaluates Lisp, which the buffer borrow above forbids).
    struct TextFaceJob {
        buffer_id: u64,
        selected: bool,
        top: usize,
        /// Left edge of the text area (past any line-number column).
        left: usize,
        lnum_cols: usize,
        truncate: bool,
        body_width: usize,
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
            .or_else(|| interpreter.lookup_var("header-line-format", env))
            .is_some_and(|format| format.is_truthy());
        let header_rows = usize::from(has_header_line && info.height > 2);
        let text_top = info.top + header_rows;
        let text_rows = info.height - 1 - header_rows;
        // `display-line-numbers': the column's width follows the
        // window's start line, which planning itself may move
        // (recentering); iterate until the width the plan was laid out
        // with matches the width its final top line asks for.
        let lnum_for = |interpreter: &Interpreter, top_line: usize| {
            let buffer = if info.buffer_id == interpreter.current_buffer_id() {
                Some(&interpreter.buffer)
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
        let (geometry, plan) = loop {
            let geometry = window_render_geometry(interpreter, env, info, body_width, cols, lnum);
            let view = state.views.entry(info.window_id).or_default();
            let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
                Some(&interpreter.buffer)
            } else {
                interpreter.get_buffer_by_id(info.buffer_id)
            }) else {
                continue 'windows;
            };
            let invisibility = resolve_buffer_invisibility(interpreter, buffer, info.buffer_id);
            let plan = plan_window_text(
                buffer,
                &invisibility,
                view,
                info.start,
                info.point,
                text_rows,
                body_width,
                geometry.truncate,
                &geometry,
                info.selected,
            );
            let top_line = view.top_line;
            let settled = lnum_for(interpreter, top_line);
            if settled.map(|layout| layout.cols) == lnum.map(|layout| layout.cols) {
                break (geometry, plan);
            }
            lnum = settled;
        };
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
                    Some(&interpreter.buffer)
                } else {
                    interpreter.get_buffer_by_id(info.buffer_id)
                };
                if let Some(buffer) = window_buffer {
                    let spec = resolve_buffer_invisibility(interpreter, buffer, info.buffer_id);
                    let z = buffer.point_max();
                    let mut pos = plan.top_pos.min(z);
                    let end = plan.window_end.min(z);
                    let mut ranges = Vec::new();
                    if !spec.active {
                        ranges.push((pos, end));
                    } else {
                        while pos < end {
                            if crate::lisp::primitives::invisible_class_at(buffer, &spec, pos) != 0
                            {
                                let run_end =
                                    crate::lisp::primitives::invisible_run_at(buffer, &spec, pos)
                                        .map(|(run_end, _)| run_end)
                                        .unwrap_or(pos + 1);
                                pos = run_end.max(pos + 1);
                                continue;
                            }
                            let visible_start = pos;
                            while pos < end
                                && crate::lisp::primitives::invisible_class_at(buffer, &spec, pos)
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
        let text_left = info.left + lnum_cols;
        for (row, (rendered, _, _, _, _)) in plan.rendered.iter().enumerate() {
            frame[text_top + row].blit(text_left, rendered, CellAttrs::default());
        }
        {
            let mut variables = interpreter
                .lookup_var("overlay-arrow-variable-list", &Vec::new())
                .unwrap_or(Value::Nil);
            while let Value::Cons(_) = variables {
                let Ok(symbol_value) = variables.car() else {
                    break;
                };
                variables = variables.cdr().unwrap_or(Value::Nil);
                let Ok(symbol) = symbol_value.as_symbol() else {
                    continue;
                };
                let Some(Value::Marker(marker)) = interpreter.lookup_var(symbol, &Vec::new())
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
                    Some(&interpreter.buffer)
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
                    .or_else(|| interpreter.lookup_var("overlay-arrow-string", &Vec::new()))
                    .and_then(|value| value.as_string().map(str::to_string).ok())
                    .unwrap_or_else(|| "=>".to_string());
                let arrow: String = arrow
                    .chars()
                    .take(body_width.saturating_sub(lnum_cols))
                    .collect();
                overlay_arrow_jobs.push((text_top + row, text_left, arrow));
            }
        }
        if let Some(layout) = geometry.lnum {
            line_number_jobs.push(LineNumberJob {
                buffer_id: info.buffer_id,
                layout,
                top: text_top,
                left: info.left,
                text_rows,
                truncate: geometry.truncate,
                text_width: body_width.saturating_sub(lnum_cols).max(1),
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
            buffer_id: info.buffer_id,
            selected: info.selected,
            top: text_top,
            left: text_left,
            lnum_cols,
            truncate: geometry.truncate,
            body_width: body_width.saturating_sub(lnum_cols).max(1),
            usable: body_width
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
                    (text_left + col).min(cols - 1) as u16,
                    (text_top + row).min(frame_rows - 1) as u16,
                );
            }
            state.views.entry(info.window_id).or_default().synced_start = plan.top_pos;
            selected_sync = Some((plan.top_pos, metrics));
        }
        mode_line_jobs.push(ModeLineJob {
            window_id: info.window_id,
            point: info.point,
            row: info.top + info.height - 1,
            left: info.left,
            body_width,
            metrics,
        });
        if header_rows > 0 {
            header_line_jobs.push(ModeLineJob {
                window_id: info.window_id,
                point: info.point,
                row: info.top,
                left: info.left,
                body_width,
                metrics,
            });
        }
    }

    // Publish the selected window's displayed geometry to the interpreter:
    // window-end, recenter, and %p answer from live glass state, and the
    // next redraw can detect a command-moved window-start.  This precedes
    // the mode-line renders so their %p reads the synced start.
    if let Some((top_pos, metrics)) = selected_sync {
        crate::lisp::primitives::set_current_window_start(interpreter, top_pos);
        crate::lisp::primitives::set_interactive_window_metrics(Some(metrics));
    }

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
        if spans.is_empty() {
            continue;
        }
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
        let buffer = if job.buffer_id == interpreter.current_buffer_id() {
            &interpreter.buffer
        } else {
            match interpreter.get_buffer_by_id(job.buffer_id) {
                Some(buffer) => buffer,
                None => continue,
            }
        };
        let job_invisibility = resolve_buffer_invisibility(interpreter, buffer, job.buffer_id);
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
            if row_end <= *row_start {
                continue;
            }
            let visual = visual_line_at(buffer, &job_invisibility, *line);
            let line_text = visual.text.clone();
            let line_begin = buffer.line_start_of(*line);
            // The right-truncation `$' glyph keeps the default face
            // (produce_special_glyphs); spans stop one cell short of it.
            let col_cap = job.body_width
                - usize::from(
                    job.truncate
                        && truncated_on_right(
                            display_width(&line_text),
                            *row_hscroll,
                            job.body_width,
                        ),
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
                let hscroll_floor = if job.lnum_cols > 0 { 0 } else { 1 };
                let col_of = |pos: usize| {
                    let offset = display_index_of(pos);
                    let column = display_column(&line_text, offset);
                    if *row_hscroll > 0 {
                        (column as i64 - *row_hscroll as i64).max(hscroll_floor) as usize
                    } else {
                        column.saturating_sub(seg * job.usable)
                    }
                };
                let from_col = col_of(begin).min(col_cap);
                // A span covering the newline paints its glyph's cell —
                // and an `:extend' face (the region) keeps painting to
                // the window edge, GNU's whole-row highlight.
                let line_chars = line_text.chars().count();
                let mut to_col = col_of(end);
                if end.saturating_sub(line_begin) > line_chars {
                    to_col = if attrs.extend {
                        job.body_width
                    } else {
                        to_col + 1
                    };
                }
                let to_col = to_col.min(col_cap);
                if from_col >= to_col {
                    continue;
                }
                frame[job.top + index].overlay(job.left + from_col, job.left + to_col, *attrs);
            }
            // The ellipsis takes the face of the text before it
            // (display_ellipsis draws with the iterator's saved face):
            // copy the preceding cell's attributes over the dots.
            for &ellipsis_index in &visual.ellipses {
                let mut column = display_column(&line_text, ellipsis_index);
                if *row_hscroll > 0 {
                    let shifted = column as i64 - *row_hscroll as i64;
                    if shifted < i64::from(job.lnum_cols == 0) {
                        continue;
                    }
                    column = shifted as usize;
                } else {
                    column = column.saturating_sub(seg * job.usable);
                }
                if column == 0 || column >= col_cap {
                    continue;
                }
                let row = &mut frame[job.top + index];
                let inherited = row.attrs[(job.left + column - 1).min(row.attrs.len() - 1)];
                for dot in 0..3usize {
                    let cell = job.left + column + dot;
                    if column + dot < col_cap && cell < row.attrs.len() {
                        row.attrs[cell] = inherited;
                    }
                }
            }
        }
    }

    for (row, left, arrow) in &overlay_arrow_jobs {
        frame[*row].blit(*left, arrow, CellAttrs::default());
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
        let Some(buffer) = (if job.buffer_id == interpreter.current_buffer_id() {
            Some(&interpreter.buffer)
        } else {
            interpreter.get_buffer_by_id(job.buffer_id)
        }) else {
            continue;
        };
        let spec = resolve_buffer_invisibility(interpreter, buffer, job.buffer_id);
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
        let usable = job.text_width.saturating_sub(1).max(1);
        let segs_of = |text: &str| {
            if job.truncate {
                1
            } else {
                segment_count(display_width(text), usable)
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
                    let visual = visual_line_at(buffer, &spec, line);
                    rows += segs_of(&visual.text) as i64 - first_seg;
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
                let point_vline = visual_line_first_line(buffer, &spec, point_line_abs);
                let point_seg = if job.truncate {
                    0
                } else {
                    let visual = visual_line_at(buffer, &spec, point_vline);
                    let offset = point.saturating_sub(buffer.line_start_of(point_vline));
                    let index = visual
                        .map
                        .get(offset)
                        .copied()
                        .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0));
                    display_column(&visual.text, index) / usable
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
                && display_width(&visual_line_at(buffer, &spec, line).text) > 0
            {
                frame[job.top + row_index].blit(job.left, "$", CellAttrs::default());
            }
        }
    }

    // Mode lines: each window's real `mode-line-format', rendered by the
    // interpreter's engine in that window's context and painted in the
    // realized mode-line face.
    let mode_line_attrs = *state
        .face_cache
        .entry("mode-line".into())
        .or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs(
                interpreter,
                env,
                &Value::Symbol("mode-line".into()),
            )
        });
    for job in &mode_line_jobs {
        let (mut mode_line, spans) = match crate::lisp::primitives::render_window_mode_line(
            interpreter,
            env,
            job.window_id,
            job.point,
            job.metrics,
        ) {
            Ok((text, spans)) if !text.is_empty() => (text, spans),
            // A GNU-shaped fabrication here would feed the very
            // differential tool that checks mode lines; render failures
            // must be visible.
            Ok(_) => ("[mode-line: empty render]".to_string(), Vec::new()),
            Err(error) => {
                debug_log(&format!("mode-line render: {error:?}"));
                (format!("[mode-line render error: {error:?}]"), Vec::new())
            }
        };
        if mode_line.chars().count() < job.body_width {
            let missing = job.body_width - mode_line.chars().count();
            mode_line.extend(std::iter::repeat_n('-', missing));
        }
        if mode_line.chars().count() > job.body_width {
            mode_line = mode_line.chars().take(job.body_width).collect();
        }
        frame[job.row].blit(job.left, &mode_line, mode_line_attrs);
        // The renderer's face spans (the buffer name's mode-line-buffer-id)
        // layer over the base mode-line face, clipped to the body.
        for (from, to, face) in spans {
            let key = format!("{face}");
            let attrs = *state.face_cache.entry(key).or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
            });
            let from = job.left + from.min(job.body_width);
            let to = job.left + to.min(job.body_width);
            frame[job.row].overlay(from, to, attrs);
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
            let (mut header, spans) = match crate::lisp::primitives::render_window_header_line(
                interpreter,
                env,
                job.window_id,
                job.point,
                job.metrics,
            ) {
                Ok((text, spans)) => (text, spans),
                Err(error) => {
                    debug_log(&format!("header-line render: {error:?}"));
                    (format!("[header-line render error: {error:?}]"), Vec::new())
                }
            };
            if header.chars().count() > job.body_width {
                header = header.chars().take(job.body_width).collect();
            }
            let mut padded = header.clone();
            padded.extend(std::iter::repeat_n(
                ' ',
                job.body_width.saturating_sub(header.chars().count()),
            ));
            frame[job.row].blit(job.left, &padded, header_attrs);
            for (from, to, face) in spans {
                let key = format!("{face}");
                let attrs = *state.face_cache.entry(key).or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
                });
                let from = job.left + from.min(job.body_width);
                let to = job.left + to.min(job.body_width);
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

    let mut out = io::stdout();
    queue!(out, cursor::Hide)?;
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
    if state.painted_echo != echo_paint {
        for (index, echo_row) in echo_paint.iter().enumerate() {
            paint_row(&mut out, frame_rows + index, echo_row)?;
        }
        state.painted_echo = echo_paint;
    }
    if echo_from_minibuffer && let Some(position) = echo_cursor {
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
        let (row, col) = wrapped_echo_cursor(
            &echo_long,
            frontend_echo_early.chars().count(),
            cols,
            state.echo_rows,
        );
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
    let mut row = PaintRow::blank(cols);
    let minibuffer_id = if frontend_echo.is_empty() {
        interpreter
            .active_minibuffer_buffer_id()
            .filter(|id| interpreter.has_buffer_id(*id))
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
            id: u64,
            text: String,
            spans: crate::lisp::primitives::EchoSpans,
        }
        let (text, point_min, point, mut strings) = {
            let buffer = if buffer_id == interpreter.current_buffer_id() {
                &interpreter.buffer
            } else if let Some(buffer) = interpreter.get_buffer_by_id(buffer_id) {
                buffer
            } else {
                &interpreter.buffer
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
                    let Ok(text) = crate::lisp::primitives::string_text(value) else {
                        continue;
                    };
                    strings.push(OverlayString {
                        position: if after { overlay.end } else { overlay.beg },
                        after,
                        id: overlay.id,
                        text,
                        spans: crate::lisp::primitives::string_face_spans(value),
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
        // Lay the cells out with their source: buffer positions keep a
        // column map for the face spans; overlay strings carry their own.
        let mut col = 0usize;
        let mut column_of = std::collections::HashMap::new();
        let mut chars = text.chars();
        let mut cursor = None;
        let splice = |row: &mut PaintRow,
                      col: &mut usize,
                      string: &OverlayString,
                      face_cache: &mut std::collections::HashMap<String, CellAttrs>,
                      interpreter: &mut Interpreter,
                      env: &mut Env| {
            let start = *col;
            row.blit(start, &string.text, CellAttrs::default());
            *col += string.text.chars().count();
            for (from, to, face) in &string.spans {
                let key = format!("{face}");
                let attrs = *face_cache.entry(key).or_insert_with(|| {
                    crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, face)
                });
                row.overlay((start + from).min(cols), (start + to).min(cols), attrs);
            }
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
                splice(
                    &mut row,
                    &mut col,
                    &strings[index],
                    face_cache,
                    interpreter,
                    env,
                );
                index += 1;
            }
            if position == point {
                cursor = Some(col);
            }
            while index < strings.len() && strings[index].position == position {
                splice(
                    &mut row,
                    &mut col,
                    &strings[index],
                    face_cache,
                    interpreter,
                    env,
                );
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
        return (row, true, cursor);
    }
    let (mut echo, spans) = if frontend_echo.is_empty() {
        crate::lisp::primitives::echo_display_message().unwrap_or_default()
    } else {
        (frontend_echo.to_string(), Vec::new())
    };
    echo.truncate(cols);
    row.blit(0, &echo, CellAttrs::default());
    for (from, to, face) in spans {
        let key = format!("{face}");
        let attrs = *face_cache.entry(key).or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
        });
        row.overlay(from.min(cols), to.min(cols), attrs);
    }
    (row, false, None)
}

/// The SGR sequence selecting ATTRS from a reset state.
fn sgr_sequence(attrs: &CellAttrs) -> String {
    let mut codes: Vec<String> = vec!["0".into()];
    if attrs.bold {
        codes.push("1".into());
    }
    if attrs.underline {
        codes.push("4".into());
    }
    if attrs.reverse {
        codes.push("7".into());
    }
    if let Some(fg) = attrs.foreground {
        codes.push(if fg < 8 {
            format!("{}", 30 + fg)
        } else {
            format!("38;5;{fg}")
        });
    }
    if let Some(bg) = attrs.background {
        codes.push(if bg < 8 {
            format!("{}", 40 + bg)
        } else {
            format!("48;5;{bg}")
        });
    }
    format!("\x1b[{}m", codes.join(";"))
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
    while end > 0 && rendered.text[end - 1] == ' ' && rendered.attrs[end - 1] == default_attrs {
        end -= 1;
    }
    let mut at = 0usize;
    while at < end {
        let attrs = rendered.attrs[at];
        let mut run_end = at;
        while run_end < end && rendered.attrs[run_end] == attrs {
            run_end += 1;
        }
        let text: String = rendered.text[at..run_end].iter().collect();
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

/// A buffer line as a truncating window shows it: at most WIDTH columns,
/// with GNU's `$' marker in the last column when the line fills it — a
/// line exactly the body width truncates too, like the wrap geometry's
/// exactly-cols-wide case.
fn truncate_row(line: &str, width: usize) -> String {
    let expanded = expand_tabs(line);
    if display_width(line) < width {
        return expanded;
    }
    let mut row: String = expanded.chars().take(width.saturating_sub(1)).collect();
    row.push('$');
    row
}

/// A truncated row under horizontal scrolling, as GNU's tty draws it:
/// column zero carries the `$' left-truncation glyph whenever the line
/// has any character scrolled off (an empty line shows nothing), the
/// visible text follows from column HSCROLL of the expanded line, and a
/// tail that still does not fit ends in the `$' right-truncation glyph.
/// An hscrolled row when a line-number column precedes the text: the
/// left `$' replaces the column's own first glyph (produce_special_glyphs
/// overwrites the row's first glyph, which is now a line-number cell),
/// so the text area simply shows the line from column HSCROLL onward,
/// truncated on the right as usual.
fn truncate_row_from(line: &str, width: usize, hscroll: usize) -> String {
    if hscroll == 0 {
        return truncate_row(line, width);
    }
    let expanded = expand_tabs(line);
    let visible: String = expanded.chars().skip(hscroll).collect();
    truncate_row(&visible, width)
}

fn truncate_row_hscrolled(line: &str, width: usize, hscroll: usize) -> String {
    if hscroll == 0 {
        return truncate_row(line, width);
    }
    let expanded = expand_tabs(line);
    if expanded.is_empty() {
        return expanded;
    }
    // The `$' glyph replaces the character at column HSCROLL itself;
    // visible text resumes one column past it.
    let remaining: Vec<char> = expanded.chars().skip(hscroll + 1).collect();
    let available = width.saturating_sub(1).max(1);
    let mut row = String::from("$");
    if remaining.len() < available {
        row.extend(remaining);
    } else {
        row.extend(remaining[..available.saturating_sub(1)].iter());
        row.push('$');
    }
    row
}

/// Buffer position (1-based) where the visual row (LINE, SEG) begins
/// under the current wrap geometry.
fn position_of_visual_row(
    buffer: &crate::buffer::Buffer,
    spec: &InvisibilitySpec,
    line: usize,
    seg: usize,
    usable: usize,
) -> usize {
    let start = buffer.line_start_of(line);
    if seg == 0 {
        return start;
    }
    let visual = visual_line_at(buffer, spec, line);
    let target = seg * usable;
    let mut col = 0usize;
    let mut index = 0usize;
    for c in visual.text.chars() {
        if col >= target {
            break;
        }
        col += if c == '\t' { 8 - (col % 8) } else { 1 };
        index += 1;
    }
    start
        + visual
            .raw_of_display
            .get(index)
            .copied()
            .unwrap_or_else(|| visual.raw_of_display.last().copied().unwrap_or(0))
}

/// A buffer line's display form: tabs expanded to 8-column stops.
fn expand_tabs(line: &str) -> String {
    let mut expanded = String::with_capacity(line.len());
    let mut column = 0usize;
    for c in line.chars() {
        if c == '\t' {
            let next_stop = (column / 8 + 1) * 8;
            while column < next_stop {
                expanded.push(' ');
                column += 1;
            }
        } else {
            expanded.push(c);
            column += 1;
        }
    }
    expanded
}

/// Display width of a buffer line under tab expansion.
fn display_width(line: &str) -> usize {
    let mut column = 0usize;
    for c in line.chars() {
        if c == '\t' {
            column = (column / 8 + 1) * 8;
        } else {
            column += 1;
        }
    }
    column
}

/// Number of visual rows a line of display width WIDTH occupies when each
/// continued row holds USABLE columns.  Every line fills at least one row.
fn segment_count(width: usize, usable: usize) -> usize {
    width.div_ceil(usable).max(1)
}

/// Render a buffer line as visual rows: continuation rows carry
/// `usable' (= cols - 1) columns plus GNU's trailing `\' marker; the
/// final row holds the remainder.  A line wraps exactly when its display
/// width exceeds usable — even an exactly-cols-wide line continues, as
/// on a GNU tty.
fn wrap_segments(line: &str, cols: usize) -> Vec<String> {
    let usable = cols.saturating_sub(1).max(1);
    let expanded = expand_tabs(line);
    let width = expanded.chars().count();
    if width <= usable {
        return vec![expanded];
    }
    let chars: Vec<char> = expanded.chars().collect();
    let mut segments = Vec::with_capacity(width.div_ceil(usable));
    let mut start = 0usize;
    while width - start > usable {
        let mut segment: String = chars[start..start + usable].iter().collect();
        segment.push('\\');
        segments.push(segment);
        start += usable;
    }
    segments.push(chars[start..].iter().collect());
    segments
}

/// Display column of a character offset within LINE under tab expansion.
fn display_column(line: &str, char_offset: usize) -> usize {
    let mut column = 0usize;
    for c in line.chars().take(char_offset) {
        if c == '\t' {
            column = (column / 8 + 1) * 8;
        } else {
            column += 1;
        }
    }
    column
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
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    fn key(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    #[test]
    fn align_to_display_specs_lay_out_as_blank_columns() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
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
        let (text, map) = displayed_line_with_map(&interp.buffer, 1);
        // "ab " is 3 columns; the propertized tab becomes 7 blanks up
        // to column 10, exactly where xdisp.c's stretch glyph ends.
        assert_eq!(text, "ab        cd");
        let map = map.expect("expansion happened");
        // Raw offsets: a=0 b=1 space=2 tab=3 c=4 d=5 map onto the
        // laid-out string, with cd starting at column 10.
        assert_eq!(&map[..6], &[0, 1, 2, 3, 10, 11]);
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
        assert_eq!(wrap_segments("a\tb", 80), vec!["a       b"]);
        assert_eq!(wrap_segments("\t\t", 80), vec!["                "]);
        assert_eq!(wrap_segments("12345678\tx", 80), vec!["12345678        x"]);
    }

    // The wrap geometry below is pinned against observed GNU 30.2 tty
    // behavior at 80 columns: continued rows carry 79 columns plus a
    // trailing backslash, and a line continues whenever its width
    // exceeds 79 — an exactly-80-column line becomes 79 + "\" then 1.
    #[test]
    fn long_lines_wrap_exactly_like_a_gnu_tty() {
        let of = |n: usize| "A".repeat(n);

        assert_eq!(wrap_segments(&of(79), 80), vec![of(79)]);
        assert_eq!(wrap_segments(&of(80), 80), vec![of(79) + "\\", of(1)]);
        assert_eq!(
            wrap_segments(&of(159), 80),
            vec![of(79) + "\\", of(79) + "\\", of(1)]
        );
        assert_eq!(wrap_segments(&of(158), 80), vec![of(79) + "\\", of(79)]);
        assert_eq!(
            wrap_segments(&of(200), 80),
            vec![of(79) + "\\", of(79) + "\\", of(42)]
        );
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
    fn segment_counts_follow_the_wrap_geometry() {
        assert_eq!(segment_count(0, 79), 1);
        assert_eq!(segment_count(79, 79), 1);
        assert_eq!(segment_count(80, 79), 2);
        assert_eq!(segment_count(158, 79), 2);
        assert_eq!(segment_count(159, 79), 3);
        for width in [0, 1, 79, 80, 158, 159, 200] {
            assert_eq!(
                segment_count(width, 79),
                wrap_segments(&"A".repeat(width), 80).len(),
                "count and rendering disagree at width {width}"
            );
        }
    }

    #[test]
    fn visual_row_positions_follow_wrap_geometry() {
        let mut interpreter = Interpreter::new();
        interpreter
            .buffer
            .insert(&format!("top\n{}\nbottom\n", "wide".repeat(50)));
        let buffer = &interpreter.buffer;
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), 1, 0, 79),
            1
        );
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), 2, 0, 79),
            5
        );
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), 2, 1, 79),
            84
        );
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), 2, 2, 79),
            163
        );
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), 3, 0, 79),
            206
        );
    }

    #[test]
    fn truncated_rows_carry_the_dollar_marker_in_the_last_column() {
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
    fn non_selected_windows_render_from_their_start_without_recentering() {
        let mut interpreter = Interpreter::new();
        for n in 1..=60 {
            interpreter.buffer.insert(&format!("line {n:02}\n"));
        }
        // Point far below the start: a selected window would recenter,
        // a non-selected one must show its commanded start regardless.
        let start = interpreter.buffer.line_start_of(30);
        let mut view = WindowView::default();
        let plan = plan_window_text(
            &interpreter.buffer,
            &InvisibilitySpec::default(),
            &mut view,
            start,
            interpreter.buffer.line_start_of(55),
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
        assert_eq!(plan.rendered[0].0, "line 30");
        assert_eq!(plan.rendered[9].0, "line 39");
        assert_eq!(plan.top_pos, start);
        assert_eq!(
            plan.cursor, None,
            "only the selected window shows the cursor"
        );
        assert_eq!(
            plan.window_end,
            interpreter.buffer.line_start_of(40),
            "window-end is the first position past the last row"
        );
    }

    #[test]
    fn selected_windows_recenter_around_an_off_window_point() {
        let mut interpreter = Interpreter::new();
        for n in 1..=60 {
            interpreter.buffer.insert(&format!("line {n:02}\n"));
        }
        let mut view = WindowView::default();
        let point = interpreter.buffer.line_start_of(40);
        let plan = plan_window_text(
            &interpreter.buffer,
            &InvisibilitySpec::default(),
            &mut view,
            interpreter.buffer.point_min(),
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
        let mut interpreter = Interpreter::new();
        interpreter.buffer.insert("short one\n");
        interpreter.buffer.insert(&"W".repeat(100));
        interpreter.buffer.insert("\n");
        for n in 3..=20 {
            interpreter.buffer.insert(&format!("line {n:02}\n"));
        }
        let mut view = WindowView::default();
        let plan = plan_window_text(
            &interpreter.buffer,
            &InvisibilitySpec::default(),
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
            "\x1b[0;1;4;7;38;5;250;48;5;238m",
            "colors past the base palette select through 38;5/48;5"
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

    #[test]
    fn display_columns_match_tab_expansion() {
        assert_eq!(display_column("a\tb", 0), 0);
        assert_eq!(display_column("a\tb", 1), 1);
        assert_eq!(display_column("a\tb", 2), 8);
        assert_eq!(display_column("a\tb", 3), 9);
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
        let mut env: Env = Vec::new();
        interpreter.set_variable("noninteractive", Value::Nil, &mut env);
        interpreter
            .load_target("isearch")
            .expect("isearch.el loads");
        interpreter.buffer.insert(
            "alpha one
beta word two
gamma word three
",
        );
        interpreter.buffer.goto_char(1);

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
                event.clone(),
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
        assert_eq!(interpreter.buffer.point(), 19);

        // C-a is not an isearch key: the pre-command-hook exits the
        // search and the key's own command runs.
        assert_eq!(
            send(&mut interpreter, &mut env, 0x01),
            "move-beginning-of-line"
        );
        assert_eq!(
            interpreter.buffer.point(),
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
        let mut env: Env = Vec::new();
        interpreter.set_variable("noninteractive", Value::Nil, &mut env);
        interpreter.buffer.insert("abcdefghijklmnop\n");
        interpreter.buffer.goto_char(1);

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
                event.clone(),
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
        assert_eq!(interpreter.buffer.point(), 9, "C-u 8 C-f moves 8 chars");
        assert_eq!(
            interpreter
                .lookup_var("prefix-arg", &env)
                .unwrap_or(Value::Nil),
            Value::Nil,
            "the chain is consumed"
        );
    }

    #[test]
    fn key_resolution_distinguishes_prefixes_commands_and_undefined() {
        // GNU builds the global map entirely in preloaded Lisp; the bare
        // host starts with no global bindings.  Exercise the resolution
        // mechanics against a real keymap assembled through C-owned
        // primitives instead of any transcribed default table.
        let mut interpreter = Interpreter::new();
        let mut env: Env = Vec::new();
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
        let mut env: Env = Vec::new();
        let keys = [Value::Integer(104)];
        execute_binding(
            &mut interpreter,
            &mut env,
            Value::Symbol("self-insert-command".into()),
            &keys,
            Value::Integer(104),
        )
        .expect("self-insert through call-interactively");
        assert_eq!(interpreter.buffer.buffer_string(), "h");
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
        .status()
        .expect("run tools/tty-smoke.py");
    assert!(status.success(), "tty smoke test failed");
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
            let Ok((cols, rows)) = terminal::size() else {
                return TtyMenuOutcome::Quit;
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
                .map(|value| match value {
                    Value::Integer(seconds) => seconds as f64,
                    Value::Float(seconds) => seconds,
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
                return TtyMenuOutcome::Quit;
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
                let mut out = io::stdout();
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
                let mut out = io::stdout();
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
            let outcome = loop {
                {
                    let mut state = state.borrow_mut();
                    draw(&mut state, selected_row, first_item);
                }
                // One key sequence under the navigation map; prefixes keep
                // reading, everything else maps per read_menu_input.
                let mut pending: Vec<Value> = Vec::new();
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
                    pending.push(event);
                    match resolve_pending(interpreter, env, &pending) {
                        Resolution::Command(binding) => break binding,
                        Resolution::Prefix => {}
                        Resolution::Undefined => break Value::Nil,
                    }
                };
                let name = match &command {
                    Value::Symbol(name) => name.as_ref(),
                    Value::T => "tty-menu-exit",
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
            };

            // screen_update: put back what the menu covered.
            {
                let mut state = state.borrow_mut();
                let mut out = io::stdout();
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
