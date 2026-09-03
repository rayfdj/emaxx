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
    InvisibilitySpec, invisible_class_at, invisible_run_at, resolve_buffer_invisibility,
    visual_line_first_line,
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

    fn blit_text(&mut self, col: usize, text: &str) {
        for (offset, character) in text.chars().enumerate() {
            let Some(cell) = self.text.get_mut(col + offset) else {
                break;
            };
            *cell = character;
        }
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

    fn note_input(&mut self) {
        for view in self.views.values_mut() {
            view.deferred_mode_line_point = None;
        }
    }
}

pub fn run(initial_file: Option<PathBuf>) -> Result<i32, String> {
    let mut interpreter = batch::initialize_interactive_interpreter()?;
    let mut env: Env = Vec::new();
    interpreter.set_variable("noninteractive", Value::Nil, &mut env);
    initialize_session_buffers(&mut interpreter, &mut env)?;

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
                Ok(Some(QueuedInput::Mouse(_))) => {
                    if let Ok(mut state) = state.try_borrow_mut() {
                        state.note_input();
                    }
                    Some(None)
                }
                Ok(Some(QueuedInput::Lisp(event))) => {
                    if let Ok(mut state) = state.try_borrow_mut() {
                        state.note_input();
                    }
                    Some(Some(event))
                }
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
    // startup.el runs the terminal library before command-line file
    // visitation.  Besides key definitions, term/xterm installs the palette
    // and capability answers used when later packages realize display-
    // dependent faces.  Skipping this left ordinary colors and `supports'
    // defface alternatives frozen against the pre-terminal frame.
    let terminal_init = crate::lisp::reader::Reader::new(
        "(tty-run-terminal-initialization (selected-frame) nil t)",
    )
    .read()
    .map_err(|error| format!("read terminal initialization form: {error}"))?
    .ok_or_else(|| "terminal initialization form is empty".to_string())?;
    interpreter
        .eval(&terminal_init, &mut env)
        .map_err(|error| format!("run terminal initialization: {error}"))?;

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
    // startup.el's display-startup-echo-area-message: the startup hint
    // sits in the echo area until the first command replaces it (F10's
    // menu leaves it visible, as GNU does).
    let message = interpreter
        .call_function_value(
            Value::Symbol("substitute-command-keys".into()),
            None,
            &[Value::String(
                "For information about GNU Emacs and the GNU system, type \\[about-emacs].".into(),
            )],
            &mut env,
        )
        .map_err(|error| format!("build startup echo message: {error}"))?;
    // `message' both paints the propertized key binding and appends the
    // startup hint to *Messages*.  Calling the GNU Lisp owner matters:
    // directly setting the echo row left Buffer Menu with a reconstruction
    // diagnostic in a Fundamental-mode *Messages* buffer instead.
    call(
        &mut interpreter,
        &mut env,
        "message",
        &[Value::String("%s".into()), message],
    )
    .map_err(|error| format!("publish startup echo message: {error}"))?;
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

fn initialize_session_buffers(interpreter: &mut Interpreter, env: &mut Env) -> Result<(), String> {
    // startup.el's normal-top-level first turns the dump-created Messages
    // buffer into its real major mode.  Emaxx's source reconstruction can
    // emit load diagnostics that GNU's already-built dump never carries, so
    // the live session begins from the same empty buffer before the startup
    // echo is logged below.
    //
    // Later in command-line, GNU inserts `initial-scratch-message' into the
    // still-empty *scratch* buffer even when a command-line file will become
    // selected.  Buffer switching and Buffer Menu therefore see the normal
    // four-line scratch buffer rather than an invented blank one.
    let source = "(progn
      (with-current-buffer \"*Messages*\"
        (let ((inhibit-read-only t)) (erase-buffer))
        (messages-buffer-mode))
      (and initial-scratch-message
           (get-buffer \"*scratch*\")
           (with-current-buffer \"*scratch*\"
             (when (zerop (buffer-size))
               (insert (substitute-command-keys initial-scratch-message))
               (set-buffer-modified-p nil)))))";
    let form = crate::lisp::reader::Reader::new(source)
        .read()
        .map_err(|error| format!("read interactive startup buffer form: {error}"))?
        .ok_or_else(|| "interactive startup buffer form is empty".to_string())?;
    interpreter
        .eval(&form, env)
        .map_err(|error| format!("initialize interactive startup buffers: {error}"))?;
    Ok(())
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
        if let Ok(mut state) = state.try_borrow_mut() {
            state.note_input();
        }
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
        .zip(long.attrs.iter())
        .rposition(|(c, attrs)| *c != ' ' || *attrs != CellAttrs::default())
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
        Some(Value::Float(fraction)) if fraction.get() > 0.0 => {
            (((rows as f64) * fraction.get()) as usize).clamp(1, rows.saturating_sub(2))
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
        // A full redisplay may have just painted this exact message with
        // resolved face spans.  The emission tick is authoritative for the
        // cells, but not for the cursor: read-multiple-choice binds
        // `cursor-in-echo-area' only after issuing its message, so a later
        // blocking read must still move the cursor onto the already-current
        // echo glass.
        // Matrix invalidation can clear `painted_echo' afterward; the
        // tick remains proof that repainting would only flatten face spans.
        let emitted = crate::lisp::primitives::echo_area_message_tick();
        if state.painted_message_tick == emitted {
            if crate::lisp::primitives::tty_cursor_in_echo_area() {
                mini_rows = state.echo_rows.max(1);
                let mut long = PaintRow::blank(cols * mini_rows);
                long.blit(0, &text, CellAttrs::default());
                let (row, col) = wrapped_echo_cursor(&long, text.chars().count(), cols, mini_rows);
                let base = (rows as usize).saturating_sub(mini_rows);
                let mut out = io::stdout();
                let _ = queue!(
                    out,
                    cursor::MoveTo(
                        col.min(cols.saturating_sub(1)) as u16,
                        (base + row).min(rows.saturating_sub(1) as usize) as u16,
                    ),
                    cursor::Show,
                );
                let _ = out.flush();
            }
            return;
        }
        mini_rows = state.echo_rows.max(1);
        let max_rows = state.echo_max_rows.max(1);
        // Painting (or confirming) the channel brings the glass up to
        // date with every message emitted so far.
        state.painted_message_tick = emitted;
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
        // keyboard.c's command_loop_1 reselects the selected window's
        // buffer at the top of every command cycle.  A display action can
        // replace the selected window's buffer while `save-current-buffer'
        // restores the command's former current buffer (Magit does exactly
        // this when opening a log).  Key lookup and the next command must
        // operate in what the selected window shows, not that stale buffer.
        select_command_loop_buffer(interpreter).map_err(|error| error.to_string())?;
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
        shared_state.borrow_mut().note_input();
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
                Some(Value::Float(relative)) if relative.get() >= 0.0 => {
                    let wanted = if cursor_x >= text_w - margin {
                        (w as f64) * (1.0 - relative.get()) - margin as f64
                    } else {
                        (w as f64) * relative.get() + (margin + x_offset) as f64
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
    if !matches!(items.first(), Some(Value::Symbol(head)) if head == "space") {
        return None;
    }
    if let Some(position) = items
        .iter()
        .position(|item| matches!(item, Value::Symbol(name) if name == ":width"))
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

/// Decode a string `display' property of the form
/// `((margin SIDE) PAYLOAD)'.  The string's source characters do not enter
/// the text body; PAYLOAD is rendered in the requested window margin.
fn window_margin_display(value: &Value) -> Option<(String, Value)> {
    let display = crate::lisp::primitives::string_property_at(value, 0, "display")?;
    let parts = display.to_vec().ok()?;
    let location = parts.first()?.to_vec().ok()?;
    if !matches!(location.first(), Some(Value::Symbol(name)) if name == "margin") {
        return None;
    }
    let side = location.get(1)?.as_symbol().ok()?.to_string();
    let payload = parts.get(1)?.clone();
    crate::lisp::primitives::string_text(&payload).ok()?;
    Some((side, payload))
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
        match self {
            Self::ZeroWidth => String::new(),
            Self::ThinSpace => " ".into(),
            Self::EmptyBox => {
                let width = unicode_width::UnicodeWidthChar::width(character)
                    .unwrap_or(0)
                    .clamp(1, 4);
                format!("[{}]", " ".repeat(width))
            }
            Self::HexCode if u32::from(character) < 0x10000 => {
                format!("\\u{:04X}", u32::from(character))
            }
            Self::HexCode => format!("\\U{:06X}", u32::from(character)),
            Self::Acronym(acronym) if acronym.chars().count() == 1 => acronym.clone(),
            Self::Acronym(acronym) => {
                let acronym: String = acronym.chars().take(6).take_while(char::is_ascii).collect();
                format!("[{acronym}]")
            }
        }
    }
}

struct GlyphlessDisplayContext<'a> {
    interpreter: &'a Interpreter,
    table_id: Option<u64>,
    terminal_coding: String,
}

impl<'a> GlyphlessDisplayContext<'a> {
    fn new(interpreter: &'a Interpreter, buffer_id: u64) -> Self {
        let table_id = match interpreter
            .buffer_local_toplevel_value(buffer_id, "glyphless-char-display")
            .or_else(|| interpreter.default_toplevel_value("glyphless-char-display"))
        {
            Some(Value::CharTable(table_id)) => Some(table_id),
            _ => None,
        };
        // The shared term.c policy is what makes LANG=C produce glyphless
        // hex escapes instead of sending raw UTF-8.
        let terminal_coding = interpreter.effective_terminal_coding_system();
        Self {
            interpreter,
            table_id,
            terminal_coding,
        }
    }

    fn method_from_value(value: Value, no_font_fallback: bool) -> Option<GlyphlessDisplayMethod> {
        let value = value
            .cons_values()
            .map(|(_, text_terminal)| text_terminal)
            .unwrap_or(value);
        let method = match value {
            Value::Symbol(name) if name == "zero-width" && no_font_fallback => {
                GlyphlessDisplayMethod::EmptyBox
            }
            Value::Symbol(name) if name == "zero-width" => GlyphlessDisplayMethod::ZeroWidth,
            Value::Symbol(name) if name == "thin-space" => GlyphlessDisplayMethod::ThinSpace,
            Value::Symbol(name) if name == "empty-box" => GlyphlessDisplayMethod::EmptyBox,
            Value::Symbol(name) if name == "hex-code" => GlyphlessDisplayMethod::HexCode,
            value => match crate::lisp::primitives::string_text(&value) {
                Ok(acronym) => GlyphlessDisplayMethod::Acronym(acronym),
                Err(_) if no_font_fallback => GlyphlessDisplayMethod::EmptyBox,
                Err(_) => return None,
            },
        };
        Some(method)
    }

    fn method_for(&self, character: char) -> Option<GlyphlessDisplayMethod> {
        if let Some(table_id) = self.table_id
            && let Some(method) = self
                .interpreter
                .char_table_get(table_id, u32::from(character))
                .and_then(|value| Self::method_from_value(value, false))
        {
            return Some(method);
        }
        // Raw eight-bit characters bypass terminal encoding in term.c and
        // are always emitted as their byte.  Every ordinary scalar is tested
        // against the same coding implementation used by Lisp conversion.
        let encodable = character.is_ascii()
            || crate::lisp::primitives::raw_byte_from_regex_char(character).is_some()
            || crate::lisp::primitives::string_unencodable_positions(
                &character.to_string(),
                &self.terminal_coding,
                self.interpreter,
            )
            .expect("the validated terminal coding system remains registered")
            .is_empty();
        if encodable {
            return None;
        }
        let fallback = self
            .table_id
            .and_then(|table_id| self.interpreter.char_table_extra_slot(table_id, 0))
            .unwrap_or(Value::Nil);
        Self::method_from_value(fallback, true).or(Some(GlyphlessDisplayMethod::EmptyBox))
    }
}

/// Apply the current buffer's `glyphless-char-display' substitutions to a
/// rendered string and keep its face spans in display-character offsets.
/// This covers mode/header strings; ordinary window lines use the same
/// context through `glyphless_visual_line_at' so layout and point account
/// for expansions before cells are planned.
fn apply_glyphless_char_display(
    interpreter: &Interpreter,
    buffer_id: u64,
    text: String,
    spans: &mut Vec<(usize, usize, Value)>,
) -> String {
    let context = GlyphlessDisplayContext::new(interpreter, buffer_id);
    let mut rendered = String::with_capacity(text.len());
    let mut display_offsets = Vec::with_capacity(text.chars().count() + 1);
    let mut replacement_spans = Vec::new();
    display_offsets.push(0);
    let mut display_chars = 0usize;
    for character in text.chars() {
        if let Some(method) = context.method_for(character) {
            let replacement = method.render(character);
            let from = display_chars;
            display_chars += replacement.chars().count();
            rendered.push_str(&replacement);
            if from < display_chars {
                replacement_spans.push((
                    from,
                    display_chars,
                    Value::Symbol("glyphless-char".into()),
                ));
            }
        } else {
            display_chars += 1;
            rendered.push(character);
        }
        display_offsets.push(display_chars);
    }
    for (from, to, _) in spans.iter_mut() {
        *from = display_offsets.get(*from).copied().unwrap_or(display_chars);
        *to = display_offsets.get(*to).copied().unwrap_or(display_chars);
    }
    spans.extend(replacement_spans);
    rendered
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
            rendered.push(characters[position]);
            display_chars += 1;
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
    let has_overlay_strings = buffer.overlays.iter().any(|overlay| {
        !overlay.is_dead()
            && ["before-string", "after-string"].iter().any(|name| {
                overlay
                    .get_prop(&Value::Symbol((*name).into()))
                    .is_some_and(Value::is_string)
            })
    });
    if !spec.active && !has_overlay_strings {
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
    let mut display_face_spans: Vec<(usize, usize, Value)> = Vec::new();
    let mut overlay_strings = Vec::new();
    for overlay in &buffer.overlays {
        if overlay.is_dead() {
            continue;
        }
        for (name, after) in [("before-string", false), ("after-string", true)] {
            let Some(value) = overlay.get_prop(&Value::Symbol(name.into())) else {
                continue;
            };
            if window_margin_display(value).is_some() {
                continue;
            }
            if crate::lisp::primitives::string_text(value).is_err() {
                continue;
            }
            overlay_strings.push((
                if after { overlay.end } else { overlay.beg },
                after,
                overlay.id,
                value.clone(),
            ));
        }
    }
    overlay_strings.sort_by_key(|(position, after, id, _)| (*position, *after, *id));
    let mut overlay_string_index = 0usize;
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
        while let Some((position, _, _, value)) = overlay_strings.get(overlay_string_index) {
            if *position != pos {
                break;
            }
            let (string, spans) = visible_overlay_string(value, spec);
            let start = display_chars;
            for character in string.chars() {
                text.push(character);
                raw_of_display.push(pos - line_begin);
                display_chars += 1;
                col += if character == '\t' { 8 - (col % 8) } else { 1 };
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
    map.push(display_chars);
    raw_of_display.push(pos - line_begin);
    VisualLine {
        text,
        map,
        raw_of_display,
        lines_spanned,
        ellipses,
        glyphless_spans: Vec::new(),
        display_face_spans,
    }
}

fn glyphless_visual_line_at(
    buffer: &crate::buffer::Buffer,
    spec: &InvisibilitySpec,
    first_line: usize,
    context: Option<&GlyphlessDisplayContext<'_>>,
) -> VisualLine {
    let mut visual = visual_line_at(buffer, spec, first_line);
    let Some(context) = context else {
        return visual;
    };
    let old_text = std::mem::take(&mut visual.text);
    let old_raw_of_display = std::mem::take(&mut visual.raw_of_display);
    let old_count = old_text.chars().count();
    let final_raw = old_raw_of_display.last().copied().unwrap_or(0);
    let mut old_to_new = Vec::with_capacity(old_count + 1);
    let mut rendered = String::with_capacity(old_text.len());
    let mut raw_of_display = Vec::with_capacity(old_raw_of_display.len());
    let mut glyphless_spans = Vec::new();
    let mut display_chars = 0usize;
    for (index, character) in old_text.chars().enumerate() {
        old_to_new.push(display_chars);
        let raw = old_raw_of_display.get(index).copied().unwrap_or(final_raw);
        if let Some(method) = context.method_for(character) {
            let replacement = method.render(character);
            let from = display_chars;
            for replacement_character in replacement.chars() {
                rendered.push(replacement_character);
                raw_of_display.push(raw);
                display_chars += 1;
            }
            if from < display_chars {
                glyphless_spans.push((from, display_chars));
            }
        } else {
            rendered.push(character);
            raw_of_display.push(raw);
            display_chars += 1;
        }
    }
    old_to_new.push(display_chars);
    raw_of_display.push(final_raw);
    for offset in &mut visual.map {
        *offset = old_to_new.get(*offset).copied().unwrap_or(display_chars);
    }
    for offset in &mut visual.ellipses {
        *offset = old_to_new.get(*offset).copied().unwrap_or(display_chars);
    }
    for (from, to, _) in &mut visual.display_face_spans {
        *from = old_to_new.get(*from).copied().unwrap_or(display_chars);
        *to = old_to_new.get(*to).copied().unwrap_or(display_chars);
    }
    visual.text = rendered;
    visual.raw_of_display = raw_of_display;
    visual.glyphless_spans = glyphless_spans;
    visual
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
    let mut space_run: Option<Value> = None;
    for (offset, ch) in raw.chars().enumerate() {
        map.push(expanded_chars);
        let display = buffer.text_property_at(line_begin + offset, "display");
        // Lisp often builds the replacement with `concat' or
        // `substitute-command-keys', so it can itself be a propertized
        // string represented by a vector-literal value.  It is still a
        // string for display purposes; matching only the two direct Rust
        // string variants dropped Dired's free-space suffix.
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
    glyphless: Option<&GlyphlessDisplayContext<'_>>,
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
    let segs_of = |visual: &VisualLine| {
        if truncate {
            1
        } else {
            wrap_glyphless_visual_line(visual, body_width).len()
        }
    };
    let visual_at = |line: usize| glyphless_visual_line_at(buffer, spec, line, glyphless);

    let point_line = visual_line_first_line(buffer, spec, buffer.line_number_at_pos(point));
    let point_visual = visual_at(point_line);
    let point_offset = point.saturating_sub(buffer.line_start_of(point_line));
    let point_index = point_visual
        .map
        .get(point_offset)
        .copied()
        .unwrap_or_else(|| point_visual.map.last().copied().unwrap_or(0));
    let point_dcol = display_column(&point_visual.text, point_index);
    let (point_seg, cursor_col) = if truncate {
        // Without a line-number gutter, the left `$' occupies one screen
        // cell before source column HSCROLL.  A point hidden farther left
        // still clamps to the marker itself.
        let on_screen = HscrollLayout::new(&point_visual, hscroll, lnum_cols == 0)
            .screen_column(point_dcol)
            .unwrap_or(0);
        (0, on_screen.min(body_width.saturating_sub(1)))
    } else {
        let wrapped = wrap_glyphless_visual_line(&point_visual, body_width);
        wrapped_position_of_column(
            &wrapped,
            point_dcol,
            point_visual
                .glyphless_spans
                .iter()
                .any(|(from, _)| *from == point_index),
        )
    };
    let cursor_hidden_left = truncate
        && hscroll > 0
        && lnum_cols > 0
        && HscrollLayout::new(&point_visual, hscroll, false)
            .screen_column(point_dcol)
            .is_none();
    let cursor_col = if lnum_cols == 0 {
        cursor_col
    } else if cursor_hidden_left {
        lnum_cols.saturating_sub(1)
    } else {
        lnum_cols.saturating_add(cursor_col)
    };

    // A command that owns its scrolling (recenter, scroll-up) moved the
    // interpreter's window-start; adopt it as the window's top before
    // deciding whether point needs recentering.  Non-selected windows
    // simply show their commanded start — GNU only enforces point
    // visibility in the selected window's redisplay.
    if commanded_start != view.synced_start || !selected {
        let start_line =
            visual_line_first_line(buffer, spec, buffer.line_number_at_pos(commanded_start));
        let start_visual = visual_at(start_line);
        let start_offset = commanded_start.saturating_sub(buffer.line_start_of(start_line));
        let start_index = start_visual
            .map
            .get(start_offset)
            .copied()
            .unwrap_or_else(|| start_visual.map.last().copied().unwrap_or(0));
        let start_dcol = display_column(&start_visual.text, start_index);
        view.top_line = start_line;
        view.top_seg = if truncate {
            0
        } else {
            wrapped_position_of_column(
                &wrap_glyphless_visual_line(&start_visual, body_width),
                start_dcol,
                start_visual
                    .glyphless_spans
                    .iter()
                    .any(|(from, _)| *from == start_index),
            )
            .0
        };
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
                let visual = glyphless_visual_line_at(buffer, spec, walk, glyphless);
                let segs = segs_of(&visual);
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
                seg = segs_of(&visual_at(line)) - 1;
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
        let line_start = buffer.line_start_of(fill_line);
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

    let top_pos =
        position_of_visual_row(buffer, spec, glyphless, view.top_line, view.top_seg, usable);
    let window_end = match past_window {
        // A row past the final buffer line means the window shows
        // everything: window-end is ZV (a buffer without a trailing
        // newline has no line beyond its last).
        Some((line, _)) if line > last_line => buffer.point_max(),
        Some((line, seg)) => position_of_visual_row(buffer, spec, glyphless, line, seg, usable),
        None => invisible_tail_start.unwrap_or_else(|| buffer.point_max()),
    };
    WindowPlan {
        rendered,
        top_pos,
        window_end,
        cursor: selected.then_some((point_row, cursor_col)),
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
    if invisibility_active
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
    // Rows above the window tree belong to the frame's menu bar.
    let menu_lines = ((rows as i64) - interpreter.frame_text_height()).clamp(0, 1) as usize;
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
        if recovered.as_integer().unwrap_or(0) != 0 {
            call(
                interpreter,
                env,
                "window-resize-apply",
                &[Value::Nil, Value::Nil],
            )?;
        }
        Ok(())
    })(interpreter, env);
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
        buffer_id: u64,
        point: usize,
        row: usize,
        left: usize,
        body_width: usize,
        metrics: crate::lisp::primitives::InteractiveWindowMetrics,
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
        let previous_view = state
            .views
            .get(&info.window_id)
            .copied()
            .unwrap_or_default();
        let (geometry, plan, invisibility_active, chars_modiff) = loop {
            let geometry =
                window_render_geometry(interpreter, env, info, text_body_width, cols, lnum);
            let view = state.views.entry(info.window_id).or_default();
            let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
                Some(&interpreter.buffer)
            } else {
                interpreter.get_buffer_by_id(info.buffer_id)
            }) else {
                continue 'windows;
            };
            let invisibility = resolve_buffer_invisibility(interpreter, buffer, info.buffer_id);
            let glyphless = GlyphlessDisplayContext::new(interpreter, info.buffer_id);
            let plan = plan_window_text(
                buffer,
                &invisibility,
                Some(&glyphless),
                view,
                info.start,
                info.point,
                text_rows,
                text_body_width,
                geometry.truncate,
                &geometry,
                info.selected,
            );
            let top_line = view.top_line;
            let settled = lnum_for(interpreter, top_line);
            if settled.map(|layout| layout.cols) == lnum.map(|layout| layout.cols) {
                break (
                    geometry,
                    plan,
                    invisibility.active,
                    buffer.chars_modified_tick(),
                );
            }
            lnum = settled;
        };
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
        let text_left = info.left + left_margin + lnum_cols;
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
                    .take(text_body_width.saturating_sub(lnum_cols))
                    .collect();
                overlay_arrow_jobs.push((text_top + row, text_left, arrow));
            }
        }
        {
            let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
                Some(&interpreter.buffer)
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
                    let Some((side, payload)) = window_margin_display(value) else {
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
                        .line_start_at(overlay.beg.clamp(buffer.point_min(), buffer.point_max()));
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
        }
        if let Some(layout) = geometry.lnum {
            line_number_jobs.push(LineNumberJob {
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
            state.views.entry(info.window_id).or_default().synced_start = plan.top_pos;
            selected_sync = Some((plan.top_pos, metrics));
        }
        {
            let view = state.views.entry(info.window_id).or_default();
            view.last_buffer_id = Some(info.buffer_id);
            view.last_point = Some(info.point);
            view.last_cursor = plan.cursor;
            view.last_top_pos = Some(plan.top_pos);
            view.last_chars_modiff = Some(chars_modiff);
            view.deferred_mode_line_point =
                (mode_line_point != info.point).then_some(mode_line_point);
        }
        mode_line_jobs.push(ModeLineJob {
            window_id: info.window_id,
            buffer_id: info.buffer_id,
            point: mode_line_point,
            row: info.top + info.height - 1,
            left: info.left,
            body_width,
            metrics,
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
        let overlay_string_faces: Vec<Value> = {
            let source = if job.buffer_id == interpreter.current_buffer_id() {
                Some(&interpreter.buffer)
            } else {
                interpreter.get_buffer_by_id(job.buffer_id)
            };
            source
                .into_iter()
                .flat_map(|buffer| &buffer.overlays)
                .filter(|overlay| !overlay.is_dead())
                .flat_map(|overlay| {
                    ["before-string", "after-string"]
                        .into_iter()
                        .filter_map(|name| overlay.get_prop(&Value::Symbol(name.into())))
                        .flat_map(crate::lisp::primitives::string_face_spans)
                        .map(|(_, _, face)| face)
                })
                .collect()
        };
        for face in overlay_string_faces {
            let key = format!("{face}");
            state.face_cache.entry(key).or_insert_with(|| {
                crate::lisp::primitives::resolve_tty_face_attrs(interpreter, env, &face)
            });
        }
        let buffer = if job.buffer_id == interpreter.current_buffer_id() {
            &interpreter.buffer
        } else {
            match interpreter.get_buffer_by_id(job.buffer_id) {
                Some(buffer) => buffer,
                None => continue,
            }
        };
        let job_invisibility = resolve_buffer_invisibility(interpreter, buffer, job.buffer_id);
        let glyphless = GlyphlessDisplayContext::new(interpreter, job.buffer_id);
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
            let visual =
                glyphless_visual_line_at(buffer, &job_invisibility, *line, Some(&glyphless));
            let line_text = visual.text.clone();
            let wrapped =
                (!job.truncate).then(|| wrap_glyphless_visual_line(&visual, job.body_width));
            let segment_start = if job.truncate {
                0
            } else {
                wrapped
                    .as_ref()
                    .expect("non-truncating row has wrapped geometry")
                    .get(*seg)
                    .map_or(seg * job.usable, |segment| segment.start_column)
            };
            let hscroll_layout = HscrollLayout::new(&visual, *row_hscroll, job.lnum_cols == 0);
            let line_begin = buffer.line_start_of(*line);
            // The right-truncation `$' glyph keeps the default face
            // (produce_special_glyphs); spans stop one cell short of it.
            let col_cap = (if wrapped
                .as_ref()
                .is_some_and(|segments| *seg + 1 < segments.len())
            {
                job.usable
            } else {
                job.body_width
            }) - usize::from(
                job.truncate
                    && truncated_on_right(display_width(&line_text), *row_hscroll, job.body_width),
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
                    let column = display_column(&line_text, offset);
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
                let extends_row =
                    attrs.extend && (covers_newline || end.saturating_sub(line_begin) > line_chars);
                if end.saturating_sub(line_begin) > line_chars {
                    to_col = if attrs.extend {
                        job.body_width + job.right_margin
                    } else {
                        to_col + 1
                    };
                } else if covers_newline && attrs.extend {
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
                let begin_column = display_column(&line_text, span_begin);
                let end_column = display_column(&line_text, span_end);
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
                let attrs = state
                    .face_cache
                    .get(&format!("{face}"))
                    .copied()
                    .unwrap_or_default();
                let begin_column = display_column(&line_text, *span_begin);
                let end_column = display_column(&line_text, *span_end);
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
                    let row = &mut frame[job.top + index];
                    if matches!(face, Value::Symbol(name) if name == "default") {
                        row.replace_attrs(job.left + from_col, job.left + to_col, attrs);
                    } else {
                        row.overlay(job.left + from_col, job.left + to_col, attrs);
                    }
                }
            }
            // The ellipsis takes the face of the text before it
            // (display_ellipsis draws with the iterator's saved face):
            // copy the preceding cell's attributes over the dots.
            for &ellipsis_index in &visual.ellipses {
                let mut column = display_column(&line_text, ellipsis_index);
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
        let glyphless = GlyphlessDisplayContext::new(interpreter, job.buffer_id);
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
                    let visual = glyphless_visual_line_at(buffer, &spec, line, Some(&glyphless));
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
                let point_vline = visual_line_first_line(buffer, &spec, point_line_abs);
                let point_seg = if job.truncate {
                    0
                } else {
                    let visual =
                        glyphless_visual_line_at(buffer, &spec, point_vline, Some(&glyphless));
                    let offset = point.saturating_sub(buffer.line_start_of(point_vline));
                    let index = visual
                        .map
                        .get(offset)
                        .copied()
                        .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0));
                    let column = display_column(&visual.text, index);
                    wrapped_position_of_column(
                        &wrap_glyphless_visual_line(&visual, job.text_width),
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
                && display_width(
                    &glyphless_visual_line_at(buffer, &spec, line, Some(&glyphless)).text,
                ) > 0
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
        let (mut mode_line, mut spans) = match crate::lisp::primitives::render_window_mode_line(
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
        mode_line = apply_glyphless_char_display(interpreter, job.buffer_id, mode_line, &mut spans);
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
            let (mut tab, mut spans) = match crate::lisp::primitives::render_window_tab_line(
                interpreter,
                env,
                job.window_id,
                job.point,
                job.metrics,
            ) {
                Ok((text, spans)) => (text, spans),
                Err(error) => {
                    debug_log(&format!("tab-line render: {error:?}"));
                    (format!("[tab-line render error: {error:?}]"), Vec::new())
                }
            };
            tab = apply_glyphless_char_display(interpreter, job.buffer_id, tab, &mut spans);
            if tab.chars().count() > job.body_width {
                tab = tab.chars().take(job.body_width).collect();
            }
            let mut padded = tab.clone();
            padded.extend(std::iter::repeat_n(
                ' ',
                job.body_width.saturating_sub(tab.chars().count()),
            ));
            frame[job.row].blit(job.left, &padded, tab_attrs);
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
            let (mut header, mut spans) = match crate::lisp::primitives::render_window_header_line(
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
            header = apply_glyphless_char_display(interpreter, job.buffer_id, header, &mut spans);
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

/// An hscrolled row when a line-number column precedes the text: the
/// left `$' replaces the column's own first glyph (produce_special_glyphs
/// overwrites the row's first glyph, which is now a line-number cell),
/// so the text area uses the gutter form of `HscrollLayout' and truncates
/// on the right as usual.
fn truncate_row_from(visual: &VisualLine, width: usize, hscroll: usize) -> String {
    if hscroll == 0 {
        return truncate_row(&visual.text, width);
    }
    let expanded = expand_tabs(&visual.text);
    let layout = HscrollLayout::new(visual, hscroll, false);
    let visible: String = expanded.chars().skip(layout.source_start).collect();
    truncate_row(&visible, width)
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
    fn new(visual: &VisualLine, hscroll: usize, left_marker: bool) -> Self {
        if hscroll == 0 {
            return Self {
                source_start: 0,
                screen_start: 0,
            };
        }
        let screen_start = usize::from(left_marker);
        let glyphless_start = visual.glyphless_spans.iter().find_map(|(from, to)| {
            let from = display_column(&visual.text, *from);
            let to = display_column(&visual.text, *to);
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
    if hscroll == 0 {
        return truncate_row(&visual.text, width);
    }
    let expanded = expand_tabs(&visual.text);
    if expanded.is_empty() {
        return expanded;
    }
    let layout = HscrollLayout::new(visual, hscroll, true);
    let remaining: Vec<char> = expanded.chars().skip(layout.source_start).collect();
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
    glyphless: Option<&GlyphlessDisplayContext<'_>>,
    line: usize,
    seg: usize,
    usable: usize,
) -> usize {
    let start = buffer.line_start_of(line);
    if seg == 0 {
        return start;
    }
    let visual = glyphless_visual_line_at(buffer, spec, line, glyphless);
    let target = wrap_glyphless_visual_line(&visual, usable + 1)
        .get(seg)
        .map_or(seg * usable, |segment| segment.start_column);
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

/// Render a buffer line as visual rows: continuation rows carry
/// `usable' (= cols - 1) columns plus GNU's trailing `\' marker; the
/// final row holds the remainder.  A line wraps exactly when its display
/// width exceeds usable.  Glyphless display elements that cross a row edge
/// restart in full on the continuation row, matching a GNU tty.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WrappedVisualSegment {
    text: String,
    /// Display-column range in the unwrapped visual line.  A glyphless
    /// element that straddles a row boundary can make the next segment start
    /// before the preceding segment ended: GNU restarts that whole display
    /// element on the continuation row.
    start_column: usize,
    end_column: usize,
}

fn wrap_glyphless_visual_line(visual: &VisualLine, cols: usize) -> Vec<WrappedVisualSegment> {
    let usable = cols.saturating_sub(1).max(1);
    let expanded = expand_tabs(&visual.text);
    let chars: Vec<char> = expanded.chars().collect();
    let width = chars.len();
    let glyphless_columns: Vec<(usize, usize)> = visual
        .glyphless_spans
        .iter()
        .map(|(from, to)| {
            (
                display_column(&visual.text, *from),
                display_column(&visual.text, *to),
            )
        })
        .collect();
    if width <= usable {
        return vec![WrappedVisualSegment {
            text: expanded,
            start_column: 0,
            end_column: width,
        }];
    }
    let mut segments = Vec::with_capacity(width.div_ceil(usable));
    let mut start = 0usize;
    while width - start > usable {
        let end = start + usable;
        let mut text: String = chars[start..end].iter().collect();
        text.push('\\');
        segments.push(WrappedVisualSegment {
            text,
            start_column: start,
            end_column: end,
        });
        let restart = glyphless_columns
            .iter()
            .find_map(|(from, to)| (*from < end && end < *to).then_some(*from))
            .filter(|restart| *restart > start)
            .unwrap_or(end);
        start = restart;
    }
    segments.push(WrappedVisualSegment {
        text: chars[start..].iter().collect(),
        start_column: start,
        end_column: width,
    });
    segments
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
        interpreter.set_terminal_coding_system(None);
        interpreter.buffer.insert("AöB€C😀D\n");
        let context = GlyphlessDisplayContext::new(&interpreter, interpreter.current_buffer_id());
        let visual = glyphless_visual_line_at(
            &interpreter.buffer,
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
        let context = GlyphlessDisplayContext::new(&interpreter, interpreter.current_buffer_id());
        let visual = glyphless_visual_line_at(
            &interpreter.buffer,
            &InvisibilitySpec::default(),
            1,
            Some(&context),
        );
        assert_eq!(visual.text, "AöB€C😀D");
        assert_eq!(visual.map, (0..=7).collect::<Vec<_>>());
        assert!(visual.glyphless_spans.is_empty());
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
        interpreter.buffer.insert("ö😀\n");
        let Value::CharTable(table_id) = interpreter
            .default_toplevel_value("glyphless-char-display")
            .expect("initialized glyphless table")
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
        let context = GlyphlessDisplayContext::new(&interpreter, interpreter.current_buffer_id());
        let visual = glyphless_visual_line_at(
            &interpreter.buffer,
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
        interpreter
            .buffer
            .insert(&format!("{}öZ\n", "x".repeat(76)));
        let context = GlyphlessDisplayContext::new(&interpreter, interpreter.current_buffer_id());
        let visual = glyphless_visual_line_at(
            &interpreter.buffer,
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
            vec![(0, 79), (76, 83)]
        );
        assert_eq!(wrapped_position_of_column(&segments, 76, true), (0, 76));
        assert_eq!(wrapped_position_of_column(&segments, 83, false), (1, 7));
        let boundary = vec![
            WrappedVisualSegment {
                text: String::new(),
                start_column: 0,
                end_column: 79,
            },
            WrappedVisualSegment {
                text: String::new(),
                start_column: 79,
                end_column: 85,
            },
        ];
        assert_eq!(wrapped_position_of_column(&boundary, 79, true), (1, 0));
    }

    #[test]
    fn tabulated_list_glyphless_table_uses_tty_sort_indicators() {
        let mut interpreter = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env: Env = Vec::new();
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
        let rendered = apply_glyphless_char_display(
            &interpreter,
            interpreter.current_buffer_id(),
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
    fn width_display_specs_use_tty_cells_and_collapse_equal_runs() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
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

        let (text, map) = displayed_line_with_map(&interp.buffer, 1);
        assert_eq!(text, "ab c  d");
        assert_eq!(
            map.expect("display widths changed the line"),
            vec![0, 1, 1, 2, 3, 4, 6, 6, 7]
        );
    }

    #[test]
    fn propertized_string_display_specs_replace_the_covered_text() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
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
        let (text, map) = displayed_line_with_map(&interp.buffer, 1);
        assert_eq!(text, "x: (166 GiB available)y");
        assert_eq!(
            map.expect("replacement changes the display"),
            vec![0, 1, 22, 23]
        );
        let visual = visual_line_at(
            &interp.buffer,
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
        let mut env: Env = Vec::new();
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
            &interp.buffer,
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
        let mut env: Env = Vec::new();
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
        let spec = resolve_buffer_invisibility(&interp, &interp.buffer, interp.current_buffer_id());
        assert!(spec.all, "canonical Value::T means every non-nil source");
        assert_eq!(visual_line_at(&interp.buffer, &spec, 1).text, "headnext");
    }

    #[test]
    fn window_overlay_after_strings_splice_before_hidden_newlines_with_faces() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
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
        let spec = resolve_buffer_invisibility(&interp, &interp.buffer, interp.current_buffer_id());
        let visual = visual_line_at(&interp.buffer, &spec, 1);
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
    fn invisible_properties_on_overlay_strings_remove_their_tty_cells() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
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
        let spec = resolve_buffer_invisibility(&interp, &interp.buffer, interp.current_buffer_id());
        assert!(spec.active && spec.all);
        assert_eq!(visual_line_at(&interp.buffer, &spec, 1).text, "[-] root");
    }

    #[test]
    fn window_margin_display_strings_do_not_enter_the_text_body() {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
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
        let visual = visual_line_at(&interp.buffer, &InvisibilitySpec::default(), 1);
        assert_eq!(visual.text, "Recent commits");
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
        let mut interp = crate::batch::initialize_interactive_interpreter()
            .expect("interactive interpreter initializes");
        let mut env: Env = Vec::new();
        interp.set_variable("noninteractive", Value::Nil, &mut env);
        initialize_session_buffers(&mut interp, &mut env)
            .expect("interactive session buffers initialize");
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
                    Value::Integer(0),
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
        assert_eq!(truncate_row_hscrolled(&ordinary, 80, 3), "$ef");
        assert_eq!(truncate_row_from(&ordinary, 80, 3), "def");
        assert_eq!(HscrollLayout::new(&ordinary, 0, true).source_start, 0);
        assert_eq!(HscrollLayout::new(&ordinary, 3, true).source_start, 4);
        assert_eq!(
            HscrollLayout::new(&ordinary, 3, true).screen_column(4),
            Some(1)
        );
        assert_eq!(
            HscrollLayout::new(&ordinary, 3, false).screen_column(3),
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
        let inside = HscrollLayout::new(&glyphless, 3, true);
        assert_eq!(inside.source_start, 3);
        assert_eq!(inside.screen_column(2), None);
        assert_eq!(inside.screen_column(3), Some(1));
        assert_eq!(inside.screen_column(8), Some(6));
    }

    #[test]
    fn hscrolled_hidden_point_uses_the_last_line_number_gutter_cell() {
        let mut interpreter = Interpreter::new();
        interpreter.buffer.insert("abcdef\n");
        let mut view = WindowView::default();
        let plan = plan_window_text(
            &interpreter.buffer,
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
    fn visual_row_positions_follow_wrap_geometry() {
        let mut interpreter = Interpreter::new();
        interpreter
            .buffer
            .insert(&format!("top\n{}\nbottom\n", "wide".repeat(50)));
        let buffer = &interpreter.buffer;
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), None, 1, 0, 79),
            1
        );
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), None, 2, 0, 79),
            5
        );
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), None, 2, 1, 79),
            84
        );
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), None, 2, 2, 79),
            163
        );
        assert_eq!(
            position_of_visual_row(buffer, &InvisibilitySpec::default(), None, 3, 0, 79),
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
    fn window_end_stops_before_a_wholly_invisible_buffer_tail() {
        let mut interpreter = Interpreter::new();
        let mut env: Env = Vec::new();
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
            &interpreter.buffer,
            interpreter.current_buffer_id(),
        );
        let mut view = WindowView::default();
        let plan = plan_window_text(
            &interpreter.buffer,
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
            None,
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
            None,
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
                    Value::Float(seconds) => seconds.get(),
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
