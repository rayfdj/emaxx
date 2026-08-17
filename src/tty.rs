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
    painted_echo: PaintRow,
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
            painted_echo: PaintRow::unpainted(),
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
    // GNU's interactive startup derives the default coding from the
    // locale; this container runs UTF-8, so buffers with no detected
    // coding show `U' in the mode line exactly like GNU under it.
    let _ = call(
        &mut interpreter,
        &mut env,
        "set-default",
        &[
            Value::Symbol("buffer-file-coding-system".into()),
            Value::Symbol("utf-8-unix".into()),
        ],
    );

    if let Some(path) = initial_file {
        let path = path.display().to_string();
        let find_file = call(
            &mut interpreter,
            &mut env,
            "find-file",
            &[Value::String(path.clone().into())],
        );
        if let Err(error) = &find_file {
            debug_log(&format!("find-file {path}: {error:?}"));
            // A runtime without a working `find-file' still edits: read the
            // file directly into a buffer carrying its name.
            visit_file_directly(&mut interpreter, &mut env, &path);
        }
        debug_log(&format!(
            "startup buffer={:?} point={}",
            interpreter.buffer.name,
            interpreter.buffer.point()
        ));
    }

    // Publish the terminal's color capability before the first redraw:
    // defface specs re-realize against it (GNU's terminal-init path), and
    // libraries loaded later define their faces under the same display.
    interpreter.set_tty_display_colors(terminal_color_cells());
    let _ = interpreter.rerealize_defface_faces();

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
                Ok(Some(event)) if is_raw_mouse_token(&event) => Some(None),
                Ok(Some(event)) => Some(Some(event)),
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
#[derive(Clone, Default)]
struct SharedEventQueue(std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<Value>>>);

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
    fn try_next_event(&self) -> Result<Option<Value>, ()> {
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
                    self.0.borrow_mut().extend(events);
                    return Ok(Some(first));
                }
                Event::Mouse(mouse) => {
                    if let Some(event) = encode_mouse(mouse) {
                        return Ok(Some(event));
                    }
                }
                _ => {}
            }
        }
        Ok(None)
    }

    /// Pop the next event, blocking on the terminal when empty.  Returns
    /// `None' only on terminal loss.
    fn next_event(&self) -> Option<Value> {
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
                    self.0.borrow_mut().extend(events);
                    return Some(first);
                }
                Event::Mouse(mouse) => {
                    if let Some(event) = encode_mouse(mouse) {
                        return Some(event);
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
            let event = queue.next_event()?;
            // Raw mouse tokens are command-loop currency; a blocking
            // Lisp reader never sees them.
            if !is_raw_mouse_token(&event) {
                break event;
            }
        };
        if event == Value::Integer(7) {
            return None;
        }
        Some(event)
    })
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
    let (row, _) = compose_echo_row(
        interpreter,
        env,
        &frontend_echo,
        cols.max(10) as usize,
        &mut state.face_cache,
    );
    let mut out = io::stdout();
    let _ = paint_row(&mut out, rows.max(4) as usize - 1, &row);
    let _ = out.flush();
    state.painted_message_tick = crate::lisp::primitives::echo_area_message_tick();
    state.painted_echo = row;
}

/// Paint the live echo-area line without interpreter access; the message
/// text lives in session state exactly so blocking readers can show it.
/// When the full redisplay already painted this text (with its face
/// attributes — the minibuffer prompt), leave that paint alone.
fn draw_echo_row(state: &std::rc::Rc<std::cell::RefCell<TtyState>>) {
    let Ok((cols, rows)) = terminal::size() else {
        return;
    };
    let mut text = crate::lisp::primitives::echo_area_message().unwrap_or_default();
    text.truncate(cols.max(10) as usize);
    if let Ok(mut state) = state.try_borrow_mut() {
        // An active minibuffer's composed row (prompt face, overlay
        // strings) belongs to full redisplay; commands are the only
        // thing that changes it, and the frame-redraw hook repaints
        // after each one.
        if state.minibuffer_owns_echo {
            return;
        }
        // Painting (or confirming) the channel brings the glass up to
        // date with every message emitted so far.
        state.painted_message_tick = crate::lisp::primitives::echo_area_message_tick();
        let painted: String = state.painted_echo.text.iter().collect();
        if painted.trim_end_matches(' ') == text {
            return;
        }
    }
    let mut out = io::stdout();
    let _ = queue!(
        out,
        cursor::MoveTo(0, rows.saturating_sub(1)),
        terminal::Clear(terminal::ClearType::CurrentLine),
        style::Print(&text),
    );
    let _ = out.flush();
}

fn visit_file_directly(interpreter: &mut Interpreter, env: &mut Env, path: &str) {
    let name = std::path::Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.to_string());
    let _ = call(
        interpreter,
        env,
        "switch-to-buffer",
        &[Value::String(name.into())],
    );
    if let Ok(contents) = std::fs::read_to_string(path) {
        interpreter.buffer.insert(&contents);
        interpreter.buffer.goto_char(interpreter.buffer.point_min());
    }
    let _ = call(
        interpreter,
        env,
        "set-visited-file-name",
        &[Value::String(path.to_string().into())],
    );
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
            redraw(interpreter, env, &mut state).map_err(|error| error.to_string())?;
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
                break event;
            }
            match queue.try_next_event() {
                Err(()) => return Ok(0),
                Ok(Some(event)) => break event,
                Ok(None) => {}
            }
            let idle = idle_since
                .get_or_insert_with(std::time::Instant::now)
                .elapsed();
            if crate::lisp::primitives::run_due_timers(interpreter, env, idle.as_secs_f64()) {
                let mut state = shared_state.borrow_mut();
                let _ = redraw(interpreter, env, &mut state);
            }
            let _ = event::poll(std::time::Duration::from_millis(50));
        };
        // A raw mouse token becomes GNU's click event now that the frame
        // state is in reach; motion and wheel produce nothing.
        let event = if is_raw_mouse_token(&event) {
            match synthesize_mouse_event(interpreter, env, &event) {
                Some(event) => event,
                None => continue,
            }
        } else {
            event
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
        state.painted_echo = PaintRow::unpainted();
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

// ── Event encoding ──────────────────────────────────────────────────────

/// A terminal mouse press or release as an internal raw token; the
/// command loop turns it into GNU's click event shape once it can see
/// the frame (term.c's GPM path builds tty mouse events at the same
/// C layer).  The token never reaches Lisp.
fn encode_mouse(mouse: event::MouseEvent) -> Option<Value> {
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
    Some(Value::list([
        Value::Symbol("emaxx--raw-mouse".into()),
        Value::Integer(number),
        Value::Integer(modifier_bits),
        Value::Integer(mouse.column as i64),
        Value::Integer(mouse.row as i64),
        Value::Symbol(if press { "press" } else { "release" }.into()),
    ]))
}

/// Whether VALUE is the internal raw-mouse token.
fn is_raw_mouse_token(value: &Value) -> bool {
    matches!(value.car(), Ok(Value::Symbol(head)) if head == "emaxx--raw-mouse")
}

/// Build GNU's click event from a raw mouse token: (EVENT-SYMBOL POSN),
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
    raw: &Value,
) -> Option<Value> {
    let fields = raw.to_vec().ok()?;
    let button = fields.get(1)?.as_integer().ok()?;
    let modifier_bits = fields.get(2)?.as_integer().ok()?;
    let col = fields.get(3)?.as_integer().ok()?;
    let row = fields.get(4)?.as_integer().ok()?;
    let press = matches!(fields.get(5), Some(Value::Symbol(phase)) if phase == "press");
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
    /// with the buffer line, wrap segment, and start position each row
    /// shows — the anchors face spans map through.
    rendered: Vec<(String, usize, usize, usize)>,
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
const TRUNCATE_PARTIAL_WIDTH: usize = 50;

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
    for (offset, ch) in raw.chars().enumerate() {
        map.push(expanded_chars);
        let target = buffer
            .text_property_at(line_begin + offset, "display")
            .and_then(|value| space_align_to_target(&value));
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

fn displayed_lines_from(buffer: &crate::buffer::Buffer, from: usize, count: usize) -> Vec<String> {
    if !buffer.has_text_property_named("display") {
        return buffer.lines_from(from, count);
    }
    (from..from + count)
        .map(|line| displayed_line_text(buffer, line))
        .collect()
}

/// Plan one window's text: adopt a commanded window-start, keep point
/// visible with GNU's recenter-on-jump model (selected window only), and
/// render the visible rows under the window's own wrap-or-truncate
/// geometry.
#[allow(clippy::too_many_arguments)]
fn plan_window_text(
    buffer: &crate::buffer::Buffer,
    view: &mut WindowView,
    commanded_start: usize,
    point: usize,
    text_rows: usize,
    body_width: usize,
    truncate: bool,
    selected: bool,
) -> WindowPlan {
    let usable = body_width.saturating_sub(1).max(1);
    let segs_of = |line: &str| {
        if truncate {
            1
        } else {
            segment_count(display_width(line), usable)
        }
    };
    let line_text_at = |line: usize| displayed_line_text(buffer, line);

    let point_line = buffer.line_number_at_pos(point); // 1-based
    let point_line_text = line_text_at(point_line);
    let point_dcol = display_column(&point_line_text, point - buffer.line_start_at(point));
    let (point_seg, cursor_col) = if truncate {
        (0, point_dcol.min(body_width.saturating_sub(1)))
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
        let start_line = buffer.line_number_at_pos(commanded_start);
        let start_dcol = display_column(
            &line_text_at(start_line),
            commanded_start - buffer.line_start_at(commanded_start),
        );
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
        } else if point_line - view.top_line >= text_rows {
            // Every line fills at least one row: certainly off-screen.
            recenter = true;
        } else {
            let span = point_line - view.top_line;
            let mut rows_before = 0usize;
            for (index, line) in displayed_lines_from(buffer, view.top_line, span)
                .iter()
                .enumerate()
            {
                let segs = segs_of(line);
                let skipped = if index == 0 { view.top_seg } else { 0 };
                rows_before += segs.saturating_sub(skipped);
                if rows_before > text_rows {
                    break;
                }
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
                line -= 1;
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
    // never the buffer size.  Each line yields at least one visual row,
    // so text_rows lines always cover the window.
    let lines = displayed_lines_from(buffer, view.top_line, text_rows);
    let mut rendered: Vec<(String, usize, usize, usize)> = Vec::with_capacity(text_rows);
    // First row past the window, as (line, segment): the window's end.
    let mut past_window: Option<(usize, usize)> = None;
    'fill: for (index, line) in lines.iter().enumerate() {
        let segments = if truncate {
            vec![truncate_row(line, body_width)]
        } else {
            wrap_segments(line, body_width)
        };
        let from = if index == 0 {
            view.top_seg.min(segments.len() - 1)
        } else {
            0
        };
        for (seg_index, segment) in segments.iter().enumerate().skip(from) {
            let row_line = view.top_line + index;
            let row_start = position_of_visual_row(buffer, row_line, seg_index, usable);
            rendered.push((segment.clone(), row_line, seg_index, row_start));
            if rendered.len() == text_rows {
                past_window = Some(if seg_index + 1 < segments.len() {
                    (view.top_line + index, seg_index + 1)
                } else {
                    (view.top_line + index + 1, 0)
                });
                break 'fill;
            }
        }
    }
    rendered.resize(text_rows, (String::new(), 0, 0, usize::MAX));

    let top_pos = position_of_visual_row(buffer, view.top_line, view.top_seg, usable);
    let last_line = buffer.line_number_at_pos(buffer.point_max());
    let window_end = match past_window {
        // A row past the final buffer line means the window shows
        // everything: window-end is ZV (a buffer without a trailing
        // newline has no line beyond its last).
        Some((line, _)) if line > last_line => buffer.point_max(),
        Some((line, seg)) => position_of_visual_row(buffer, line, seg, usable),
        None => buffer.point_max(),
    };
    WindowPlan {
        rendered,
        top_pos,
        window_end,
        cursor: selected.then_some((point_row, cursor_col)),
    }
}

fn redraw(interpreter: &mut Interpreter, env: &mut Env, state: &mut TtyState) -> io::Result<()> {
    let (cols, rows) = terminal::size()?;
    let cols = cols.max(10) as usize;
    let rows = rows.max(4) as usize;
    // The interpreter's window tree carries the frame geometry: keep it
    // agreeing with the terminal so splits compute GNU's tty sizes.
    if interpreter.frame_width() != cols as i64 || interpreter.frame_height() != rows as i64 {
        interpreter.set_tty_frame_size(cols as i64, rows as i64);
    }
    let frame_rows = rows - 1; // everything above the echo area
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
        point_line: usize,
        metrics: crate::lisp::primitives::InteractiveWindowMetrics,
    }
    let mut mode_line_jobs: Vec<ModeLineJob> = Vec::new();
    // Face spans over window text apply after the text lands (their
    // resolution evaluates Lisp, which the buffer borrow above forbids).
    struct TextFaceJob {
        buffer_id: u64,
        selected: bool,
        top: usize,
        left: usize,
        body_width: usize,
        usable: usize,
        start: usize,
        window_end: usize,
        rows: Vec<(usize, usize, usize)>,
    }
    let mut text_face_jobs: Vec<TextFaceJob> = Vec::new();

    for info in &layout {
        // A window not flush with the frame's right edge spends its last
        // column on the vertical border; the mode line spans the body.
        let body_width = if info.left + info.width < cols {
            info.width - 1
        } else {
            info.width
        };
        let text_rows = info.height - 1;
        let truncate = info.width < cols && info.width < TRUNCATE_PARTIAL_WIDTH;
        let view = state.views.entry(info.window_id).or_default();
        let Some(buffer) = (if info.buffer_id == interpreter.current_buffer_id() {
            Some(&interpreter.buffer)
        } else {
            interpreter.get_buffer_by_id(info.buffer_id)
        }) else {
            continue;
        };
        let plan = plan_window_text(
            buffer,
            view,
            info.start,
            info.point,
            text_rows,
            body_width,
            truncate,
            info.selected,
        );
        let point_line = buffer.line_number_at_pos(info.point);
        for (row, (rendered, _, _, _)) in plan.rendered.iter().enumerate() {
            frame[info.top + row].blit(info.left, rendered, CellAttrs::default());
        }
        text_face_jobs.push(TextFaceJob {
            buffer_id: info.buffer_id,
            selected: info.selected,
            top: info.top,
            left: info.left,
            body_width,
            usable: body_width.saturating_sub(1).max(1),
            start: plan.top_pos,
            window_end: plan.window_end,
            rows: plan
                .rendered
                .iter()
                .map(|(_, line, seg, start)| (*line, *seg, *start))
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
                    (info.left + col).min(cols - 1) as u16,
                    (info.top + row).min(frame_rows - 1) as u16,
                );
            }
            view.synced_start = plan.top_pos;
            selected_sync = Some((plan.top_pos, metrics));
        }
        mode_line_jobs.push(ModeLineJob {
            window_id: info.window_id,
            point: info.point,
            row: info.top + info.height - 1,
            left: info.left,
            body_width,
            point_line,
            metrics,
        });
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
        for (index, (line, seg, row_start)) in job.rows.iter().enumerate() {
            if *row_start == usize::MAX {
                continue;
            }
            let row_end = job
                .rows
                .get(index + 1)
                .map(|(_, _, next)| *next)
                .filter(|next| *next != usize::MAX)
                .unwrap_or(job.window_end);
            if row_end <= *row_start {
                continue;
            }
            let (line_text, offset_map) = displayed_line_with_map(buffer, *line);
            let line_begin = buffer.line_start_of(*line);
            for (span_begin, span_end, attrs) in &resolved {
                let begin = (*span_begin).max(*row_start);
                let end = (*span_end).min(row_end);
                if begin >= end {
                    continue;
                }
                let col_of = |pos: usize| {
                    let mut offset = pos.saturating_sub(line_begin);
                    if let Some(map) = &offset_map {
                        offset = map
                            .get(offset)
                            .copied()
                            .unwrap_or_else(|| map.last().copied().unwrap_or(offset));
                    }
                    display_column(&line_text, offset).saturating_sub(seg * job.usable)
                };
                let from_col = col_of(begin).min(job.body_width);
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
                let to_col = to_col.min(job.body_width);
                if from_col >= to_col {
                    continue;
                }
                frame[job.top + index].overlay(job.left + from_col, job.left + to_col, *attrs);
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
        let (mut mode_line, spans) = crate::lisp::primitives::render_window_mode_line(
            interpreter,
            env,
            job.window_id,
            job.point,
            job.metrics,
        )
        .inspect_err(|error| debug_log(&format!("mode-line render: {error:?}")))
        .ok()
        .filter(|(text, _)| !text.is_empty())
        .unwrap_or_else(|| {
            // A session whose spec fails to render still shows the basics.
            let modified = if interpreter.buffer.is_modified() {
                "**"
            } else {
                "--"
            };
            (
                format!(
                    "-UUU:{modified}-  {}   L{}   (Fundamental)",
                    interpreter.buffer.name, job.point_line
                ),
                Vec::new(),
            )
        });
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
        state.painted_echo = PaintRow::unpainted();
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
    let frontend_echo = state.echo.clone();
    let (echo_row, from_minibuffer) = compose_echo_row(
        interpreter,
        env,
        &frontend_echo,
        cols,
        &mut state.face_cache,
    );
    state.minibuffer_owns_echo = from_minibuffer;
    // Full redisplay reflects the message channel; the glass is caught
    // up with every emission made so far.
    state.painted_message_tick = crate::lisp::primitives::echo_area_message_tick();
    if state.painted_echo != echo_row {
        paint_row(&mut out, frame_rows, &echo_row)?;
        state.painted_echo = echo_row;
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
) -> (PaintRow, bool) {
    let mut row = PaintRow::blank(cols);
    let minibuffer_id = if frontend_echo.is_empty() {
        interpreter
            .lookup_var("emaxx--active-minibuffer", env)
            .and_then(|value| match value {
                Value::Buffer(buffer) => Some(buffer.id),
                _ => None,
            })
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
        let (text, point_min, mut strings) = {
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
            (buffer.buffer_string(), buffer.point_min(), strings)
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
            while index < strings.len() && strings[index].position <= position {
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
        return (row, true);
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
    (row, false)
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

/// Buffer position (1-based) where the visual row (LINE, SEG) begins
/// under the current wrap geometry.
fn position_of_visual_row(
    buffer: &crate::buffer::Buffer,
    line: usize,
    seg: usize,
    usable: usize,
) -> usize {
    let start = buffer.line_start_of(line);
    if seg == 0 {
        return start;
    }
    let text = buffer
        .lines_from(line, 1)
        .into_iter()
        .next()
        .unwrap_or_default();
    let target = seg * usable;
    let mut col = 0usize;
    let mut offset = 0usize;
    for c in text.chars() {
        if col >= target {
            break;
        }
        col += if c == '\t' { 8 - (col % 8) } else { 1 };
        offset += 1;
    }
    start + offset
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
        assert_eq!(position_of_visual_row(buffer, 1, 0, 79), 1);
        assert_eq!(position_of_visual_row(buffer, 2, 0, 79), 5);
        assert_eq!(position_of_visual_row(buffer, 2, 1, 79), 84);
        assert_eq!(position_of_visual_row(buffer, 2, 2, 79), 163);
        assert_eq!(position_of_visual_row(buffer, 3, 0, 79), 206);
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
            &mut view,
            start,
            interpreter.buffer.line_start_of(55),
            10,
            80,
            false,
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
            &mut view,
            interpreter.buffer.point_min(),
            point,
            11,
            80,
            false,
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
        let plan = plan_window_text(&interpreter.buffer, &mut view, 1, 1, 11, 39, true, true);
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
        let mut interpreter = Interpreter::new();
        let mut env: Env = Vec::new();

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
                    if is_raw_mouse_token(&event) {
                        continue;
                    }
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
