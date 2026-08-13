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

struct TtyState {
    /// 1-based buffer line shown on the first text row.
    top_line: usize,
    /// Continuation segment of `top_line' shown first: a window can start
    /// mid-line when long lines wrap, like GNU's window-start position.
    top_seg: usize,
    /// The window-start position last agreed with the interpreter's
    /// window model; a differing value there means a command (recenter,
    /// scroll) moved the window and the frontend adopts it.
    synced_start: usize,
    /// A `C-u' sequence is accumulating: digits and `-' extend the prefix
    /// argument instead of dispatching (GNU's universal-argument--mode).
    prefix_active: bool,
    /// Events of the in-progress (multi-key) sequence.
    pending: Vec<Value>,
    /// Frontend-owned echo text (key-sequence progress, command errors);
    /// when empty, the session's `message' echo line shows instead.
    echo: String,
    /// The frame as last painted: rendered text rows, mode line, echo row,
    /// and the terminal size they were painted for.  Redraw emits only rows
    /// that differ — GNU's dispnew current-matrix idea, one line deep.
    painted_rows: Vec<String>,
    painted_mode_line: String,
    painted_echo: String,
    painted_size: (usize, usize),
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

    let guard = TerminalGuard::enter().map_err(|error| error.to_string())?;
    let queue = SharedEventQueue::default();
    crate::lisp::primitives::set_tty_event_reader(Some(make_event_reader(queue.clone())));
    let code = command_loop(&mut interpreter, &mut env, &queue);
    crate::lisp::primitives::set_tty_event_reader(None);
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
            if let Event::Key(key) = event {
                let mut events = encode_key(key);
                if events.is_empty() {
                    continue;
                }
                let first = events.remove(0);
                self.0.borrow_mut().extend(events);
                return Some(first);
            }
        }
    }
}

/// Blocking single-event reader for command code that pulls events itself.
/// The echo area is repainted before blocking so a prompt just issued with
/// `message' (y-or-n-p's protocol) is visible while the terminal waits.
/// C-g answers `None' and becomes GNU's `quit' signal at the consuming
/// primitive.
fn make_event_reader(queue: SharedEventQueue) -> Box<dyn FnMut() -> Option<Value>> {
    Box::new(move || {
        draw_echo_row();
        let event = queue.next_event()?;
        if event == Value::Integer(7) {
            return None;
        }
        Some(event)
    })
}

/// Paint the live echo-area line without interpreter access; the message
/// text lives in session state exactly so blocking readers can show it.
fn draw_echo_row() {
    let Ok((cols, rows)) = terminal::size() else {
        return;
    };
    let mut text = crate::lisp::primitives::echo_area_message().unwrap_or_default();
    text.truncate(cols.max(10) as usize);
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
) -> Result<i32, String> {
    let mut state = TtyState {
        top_line: 1,
        top_seg: 0,
        synced_start: 0,
        prefix_active: false,
        pending: Vec::new(),
        echo: String::new(),
        painted_rows: Vec::new(),
        painted_mode_line: String::new(),
        painted_echo: String::new(),
        painted_size: (0, 0),
    };

    loop {
        // GNU redisplays only when the input queue is quiet; a key burst
        // paints once at the end.
        if !queue.input_pending() {
            redraw(interpreter, env, &mut state).map_err(|error| error.to_string())?;
        }
        let Some(event) = queue.next_event() else {
            return Ok(0);
        };

        // `C-u' and its follow-up digits accumulate a prefix argument
        // before ordinary dispatch, GNU's universal-argument machinery.
        // The runtime does not define the prefix commands, so the state
        // transitions run natively, exactly as the kbd-macro engine does.
        if state.pending.is_empty()
            && let Value::Integer(code) = &event
        {
            let code = *code;
            let pending_prefix = interpreter
                .lookup_var("prefix-arg", env)
                .unwrap_or(Value::Nil);
            if code == 21 {
                let next = crate::lisp::primitives::next_universal_prefix(&pending_prefix);
                interpreter.set_variable("prefix-arg", next, env);
                state.echo = if state.prefix_active {
                    append_prefix_echo(&state.echo, "C-u")
                } else {
                    String::from("C-u-")
                };
                state.prefix_active = true;
                continue;
            }
            if state.prefix_active {
                if (48..=57).contains(&code) {
                    let next =
                        crate::lisp::primitives::next_digit_prefix(&pending_prefix, code - 48);
                    interpreter.set_variable("prefix-arg", next, env);
                    state.echo =
                        append_prefix_echo(&state.echo, &char::from(code as u8).to_string());
                    continue;
                }
                if code == i64::from(b'-') && pending_prefix.is_truthy() {
                    let next = crate::lisp::primitives::next_negative_prefix(&pending_prefix);
                    interpreter.set_variable("prefix-arg", next, env);
                    state.echo = append_prefix_echo(&state.echo, "-");
                    continue;
                }
                state.prefix_active = false;
            }
        }

        if state.pending.is_empty() {
            state.echo.clear();
        }
        state.pending.push(event);

        let resolution = resolve_pending(interpreter, env, &state.pending);
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
                // Any dispatched command consumes the prefix chain, even
                // one entered through a non-character key like an arrow.
                state.prefix_active = false;
                // The sequence resolved: its key echo is done (GNU erases
                // the echo when dispatch begins), and a command error may
                // replace it below.
                state.echo.clear();
                let keys = std::mem::take(&mut state.pending);
                let last_event = keys.last().cloned().unwrap_or(Value::Nil);
                match execute_binding(interpreter, env, binding, &keys, last_event) {
                    Ok(()) => {}
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
                        state.echo = command_error_text(interpreter, env, &error);
                    }
                }
                if let Some(termination) = interpreter.take_pending_termination() {
                    return Ok(termination.exit_code);
                }
                // A blocking reader may have painted the echo row outside
                // the matrix; repaint it against fresh state next frame.
                state.painted_echo = String::from("\u{0}");
            }
            Resolution::Prefix => {
                state.echo = format!("{}-", describe_keys(&state.pending));
            }
            Resolution::Undefined => {
                state.echo = format!("{} is undefined", describe_keys(&state.pending));
                state.pending.clear();
            }
        }
    }
}

enum Resolution {
    Command(Value),
    Prefix,
    Undefined,
}

fn resolve_pending(interpreter: &mut Interpreter, env: &mut Env, pending: &[Value]) -> Resolution {
    let key_vector = Value::list(
        std::iter::once(Value::Symbol("vector-literal".into())).chain(pending.iter().cloned()),
    );
    let binding = match call(interpreter, env, "key-binding", &[key_vector, Value::T]) {
        Ok(binding) => binding,
        Err(_) => Value::Nil,
    };
    if binding.is_nil() {
        // An unresolved strict prefix keeps reading (C-x alone answers nil
        // while C-x C-f resolves), so probe whether any longer sequence can
        // still match by asking for the prefix's own keymap.
        if pending_is_prefix(interpreter, env, pending) {
            return Resolution::Prefix;
        }
        return Resolution::Undefined;
    }
    // A prefix can answer as the keymap itself or as a prefix command
    // symbol (`Control-X-prefix') whose function cell holds the keymap;
    // GNU resolves through the indirection before dispatching.  Native
    // probes here: this classification runs once per keystroke.
    let resolved = if let Value::Symbol(name) = &binding {
        interpreter
            .lookup_function(name, env)
            .unwrap_or_else(|_| binding.clone())
    } else {
        binding.clone()
    };
    if crate::lisp::primitives::is_keymap_value(interpreter, &resolved) {
        Resolution::Prefix
    } else {
        Resolution::Command(binding)
    }
}

fn pending_is_prefix(interpreter: &mut Interpreter, env: &mut Env, pending: &[Value]) -> bool {
    // ESC alone is always a live prefix (meta encoding).
    if pending.len() == 1 && matches!(pending.first(), Some(Value::Integer(27))) {
        return true;
    }
    let key_vector = Value::list(
        std::iter::once(Value::Symbol("vector-literal".into())).chain(pending.iter().cloned()),
    );
    // `key-binding' with ACCEPT-DEFAULT nil still answers prefix keymaps.
    call(interpreter, env, "key-binding", &[key_vector])
        .map(|binding| {
            !binding.is_nil()
                && call(interpreter, env, "keymapp", &[binding])
                    .map(|value| value.is_truthy())
                    .unwrap_or(false)
        })
        .unwrap_or(false)
}

fn execute_binding(
    interpreter: &mut Interpreter,
    env: &mut Env,
    binding: Value,
    keys: &[Value],
    last_event: Value,
) -> Result<(), LispError> {
    // GNU's command loop separates each command into its own undo group
    // (undo-auto--add-boundary after every command); `undo' relies on the
    // boundary to skip before replaying the previous group.
    interpreter.buffer.push_undo_boundary();
    // A lingering echo-area message belongs to the previous command; GNU
    // clears it when the next command runs (its own `message' then shows).
    crate::lisp::primitives::set_echo_area_message(None);
    interpreter.set_variable("last-command-event", last_event, env);
    interpreter.set_variable(
        "this-command-keys-vector",
        Value::list(
            std::iter::once(Value::Symbol("vector-literal".into())).chain(keys.iter().cloned()),
        ),
        env,
    );
    interpreter.set_variable("this-command", binding.clone(), env);
    // GNU's command loop hands the accumulated prefix to the command:
    // current-prefix-arg takes prefix-arg's value and prefix-arg clears
    // before the call; last-prefix-arg keeps it for the next cycle.
    let prefix = interpreter
        .lookup_var("prefix-arg", env)
        .unwrap_or(Value::Nil);
    interpreter.set_variable("current-prefix-arg", prefix.clone(), env);
    interpreter.set_variable("prefix-arg", Value::Nil, env);
    // GNU's command_execute is a thin wrapper over call-interactively
    // (prefix-arg bookkeeping, kbd-macro expansion); the runtime does not
    // define it yet, so drive the interactive call directly.
    let result = call(
        interpreter,
        env,
        "call-interactively",
        std::slice::from_ref(&binding),
    )
    .map(|_| ());
    interpreter.set_variable("last-command", binding, env);
    interpreter.set_variable("last-prefix-arg", prefix, env);
    result
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
    let text = match error {
        LispError::SignalValue(data) => {
            let data = if matches!(data, Value::Symbol(_)) {
                Value::list([data.clone()])
            } else {
                data.clone()
            };
            call(
                interpreter,
                env,
                "error-message-string",
                std::slice::from_ref(&data),
            )
            .ok()
            .and_then(|value| match value {
                Value::String(text) => Some(text.to_string()),
                _ => None,
            })
            .unwrap_or_else(|| format!("{data}"))
        }
        LispError::Signal(text) => text.clone(),
        other => format!("{other:?}"),
    };
    text.replace(['\n', '\r'], " ").chars().take(200).collect()
}

// ── Event encoding ──────────────────────────────────────────────────────

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

fn redraw(interpreter: &mut Interpreter, env: &mut Env, state: &mut TtyState) -> io::Result<()> {
    let (cols, rows) = terminal::size()?;
    let cols = cols.max(10) as usize;
    let rows = rows.max(4) as usize;
    let text_rows = rows - 2; // mode line + echo area
    // Continued rows reserve the last column for the `\' marker, so a
    // logical line wraps whenever its display width exceeds cols - 1
    // (GNU wraps even an exactly-cols-wide line).
    let usable = cols - 1;

    let buffer = &interpreter.buffer;
    let point = buffer.point();
    let point_line = buffer.line_number_at_pos(point); // 1-based
    let line_start = buffer.line_start_at(point);
    let point_line_text = buffer
        .lines_from(point_line, 1)
        .into_iter()
        .next()
        .unwrap_or_default();
    let point_dcol = display_column(&point_line_text, point - line_start);
    let point_segs = segment_count(display_width(&point_line_text), usable);
    let point_seg = (point_dcol / usable).min(point_segs - 1);
    let cursor_col = point_dcol - point_seg * usable;

    // A command that owns its scrolling (recenter, scroll-up) moved the
    // interpreter's window-start; adopt it as the frontend's top before
    // deciding whether point needs recentering.
    let commanded_start = crate::lisp::primitives::current_window_start(interpreter)
        .clamp(buffer.point_min(), buffer.point_max());
    if commanded_start != state.synced_start {
        let start_line = buffer.line_number_at_pos(commanded_start);
        let start_text = buffer
            .lines_from(start_line, 1)
            .into_iter()
            .next()
            .unwrap_or_default();
        let start_dcol = display_column(
            &start_text,
            commanded_start - buffer.line_start_at(commanded_start),
        );
        state.top_line = start_line;
        state.top_seg = start_dcol / usable;
    }

    // A resize changes the wrap geometry under top_seg; recenter rather
    // than interpret a stale segment index.
    let full_repaint = state.painted_size != (cols, rows);

    // Keep point visible, counting visual rows (wrapped lines span
    // several); recenter on a jump like GNU's default scrolling.
    let mut recenter = full_repaint
        || point_line < state.top_line
        || (point_line == state.top_line && point_seg < state.top_seg);
    let mut point_row = 0usize;
    if !recenter {
        if point_line == state.top_line {
            point_row = point_seg - state.top_seg;
        } else if point_line - state.top_line >= text_rows {
            // Every line fills at least one row: certainly off-screen.
            recenter = true;
        } else {
            let span = point_line - state.top_line;
            let mut rows_before = 0usize;
            for (index, line) in buffer.lines_from(state.top_line, span).iter().enumerate() {
                let segs = segment_count(display_width(line), usable);
                let skipped = if index == 0 { state.top_seg } else { 0 };
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
                let previous = buffer
                    .lines_from(line - 1, 1)
                    .into_iter()
                    .next()
                    .unwrap_or_default();
                line -= 1;
                seg = segment_count(display_width(&previous), usable) - 1;
                budget -= 1;
                stepped += 1;
            } else {
                break;
            }
        }
        state.top_line = line;
        state.top_seg = seg;
        point_row = stepped;
    }

    // Fetch only the window: redisplay cost must follow the screen size,
    // never the buffer size.  Each line yields at least one visual row,
    // so text_rows lines always cover the window.
    let lines = buffer.lines_from(state.top_line, text_rows);
    let mut rendered_rows: Vec<String> = Vec::with_capacity(text_rows);
    // First row past the window, as (line, segment): the window's end.
    let mut past_window: Option<(usize, usize)> = None;
    'fill: for (index, line) in lines.iter().enumerate() {
        let segments = wrap_segments(line, cols);
        let from = if index == 0 {
            state.top_seg.min(segments.len() - 1)
        } else {
            0
        };
        for (seg_index, segment) in segments.iter().enumerate().skip(from) {
            rendered_rows.push(segment.clone());
            if rendered_rows.len() == text_rows {
                past_window = Some(if seg_index + 1 < segments.len() {
                    (state.top_line + index, seg_index + 1)
                } else {
                    (state.top_line + index + 1, 0)
                });
                break 'fill;
            }
        }
    }
    rendered_rows.resize(text_rows, String::new());

    // Publish the displayed geometry to the interpreter's window model:
    // window-end and recenter answer from live glass state, and the next
    // redraw can detect a command-moved window-start.
    let top_pos = position_of_visual_row(buffer, state.top_line, state.top_seg, usable);
    let window_end = match past_window {
        Some((line, seg)) => position_of_visual_row(buffer, line, seg, usable),
        None => buffer.point_max(),
    };
    let text_height = text_rows;
    crate::lisp::primitives::set_current_window_start(interpreter, top_pos);
    crate::lisp::primitives::set_interactive_window_metrics(Some(
        crate::lisp::primitives::InteractiveWindowMetrics {
            text_height,
            window_end,
        },
    ));
    state.synced_start = top_pos;

    // Render off-screen first, then emit only rows that changed since the
    // last paint (dispnew's current-matrix idea, one line deep): a
    // self-insert repaints one text row, not the frame.
    if full_repaint {
        state.painted_rows = vec![String::from("\u{0}"); text_rows];
        state.painted_mode_line = String::from("\u{0}");
        state.painted_echo = String::from("\u{0}");
        state.painted_size = (cols, rows);
    }
    state.painted_rows.resize(text_rows, String::from("\u{0}"));

    let mut out = io::stdout();
    queue!(out, cursor::Hide)?;

    let cursor_position = (cursor_col.min(cols - 1) as u16, point_row as u16);
    for (row, rendered) in rendered_rows.into_iter().enumerate() {
        if state.painted_rows[row] != rendered {
            queue!(
                out,
                cursor::MoveTo(0, row as u16),
                terminal::Clear(terminal::ClearType::CurrentLine),
                style::Print(&rendered),
            )?;
            state.painted_rows[row] = rendered;
        }
    }

    // Mode line: the buffer's real `mode-line-format', rendered by the
    // interpreter's engine (the metrics published above feed %p).
    let mut mode_line = crate::lisp::primitives::render_mode_line_glass(interpreter, env)
        .inspect_err(|error| debug_log(&format!("mode-line render: {error:?}")))
        .ok()
        .filter(|text| !text.is_empty())
        .unwrap_or_else(|| {
            // A session whose spec fails to render still shows the basics.
            let modified = if interpreter.buffer.is_modified() {
                "**"
            } else {
                "--"
            };
            format!(
                "-UUU:{modified}-  {}   L{point_line}   (Fundamental)",
                interpreter.buffer.name
            )
        });
    if mode_line.chars().count() < cols {
        let missing = cols - mode_line.chars().count();
        mode_line.extend(std::iter::repeat_n('-', missing));
    }
    if mode_line.chars().count() > cols {
        mode_line = mode_line.chars().take(cols).collect();
    }
    if state.painted_mode_line != mode_line {
        queue!(
            out,
            cursor::MoveTo(0, text_rows as u16),
            terminal::Clear(terminal::ClearType::CurrentLine),
            style::SetAttribute(style::Attribute::Reverse),
            style::Print(&mode_line),
            style::SetAttribute(style::Attribute::Reset),
        )?;
        state.painted_mode_line = mode_line;
    }

    // Echo area: frontend echo (key progress, errors) wins; otherwise the
    // session's live `message' line shows, GNU's echo-area behavior.
    let mut echo = if state.echo.is_empty() {
        crate::lisp::primitives::echo_area_message().unwrap_or_default()
    } else {
        state.echo.clone()
    };
    echo.truncate(cols);
    if state.painted_echo != echo {
        queue!(
            out,
            cursor::MoveTo(0, (text_rows + 1) as u16),
            terminal::Clear(terminal::ClearType::CurrentLine),
            style::Print(&echo),
        )?;
        state.painted_echo = echo;
    }
    queue!(
        out,
        cursor::MoveTo(cursor_position.0, cursor_position.1),
        cursor::Show
    )?;
    out.flush()
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
