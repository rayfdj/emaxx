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
    /// Events of the in-progress (multi-key) sequence.
    pending: Vec<Value>,
    /// Echo-area contents and the *Messages* size they were derived from.
    echo: String,
    messages_seen: usize,
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
    crate::lisp::primitives::set_tty_minibuffer_reader(Some(Box::new(read_minibuffer_line)));
    crate::lisp::primitives::set_tty_event_reader(Some(make_event_reader()));
    let code = command_loop(&mut interpreter, &mut env);
    crate::lisp::primitives::set_tty_event_reader(None);
    crate::lisp::primitives::set_tty_minibuffer_reader(None);
    drop(guard);
    code
}

/// Blocking single-event reader for command code that pulls events itself
/// (`y-or-n-p', `read-event').  Multi-event encodings (the meta ESC
/// prefix) queue their tail for the next pull; C-g answers `None' and
/// becomes GNU's `quit' signal at the consuming primitive.
fn make_event_reader() -> Box<dyn FnMut() -> Option<Value>> {
    let mut queue: std::collections::VecDeque<Value> = std::collections::VecDeque::new();
    Box::new(move || {
        if let Some(event) = queue.pop_front() {
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
                if events == [Value::Integer(7)] {
                    return None;
                }
                let first = events.remove(0);
                queue.extend(events);
                return Some(first);
            }
        }
    })
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

fn command_loop(interpreter: &mut Interpreter, env: &mut Env) -> Result<i32, String> {
    let mut state = TtyState {
        top_line: 1,
        pending: Vec::new(),
        echo: String::new(),
        messages_seen: messages_len(interpreter),
    };

    loop {
        redraw(interpreter, &mut state).map_err(|error| error.to_string())?;
        let event = event::read().map_err(|error| error.to_string())?;
        let events = match event {
            Event::Key(key) => encode_key(key),
            Event::Resize(_, _) => continue,
            _ => continue,
        };
        if events.is_empty() {
            continue;
        }

        if state.pending.is_empty() {
            state.echo.clear();
        }
        state.pending.extend(events);

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
                let keys = std::mem::take(&mut state.pending);
                let last_event = keys.last().cloned().unwrap_or(Value::Nil);
                match execute_binding(interpreter, env, binding, &keys, last_event) {
                    Ok(()) => {}
                    Err(LispError::Terminate(termination)) => {
                        return Ok(termination.exit_code);
                    }
                    Err(error) => {
                        debug_log(&format!("command error: {error:?}"));
                        state.echo = command_error_text(&error);
                        state.messages_seen = messages_len(interpreter);
                    }
                }
                if let Some(termination) = interpreter.take_pending_termination() {
                    return Ok(termination.exit_code);
                }
                sync_echo_with_messages(interpreter, &mut state);
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
    // GNU resolves through the indirection before dispatching.
    let resolved = if matches!(binding, Value::Symbol(_)) {
        call(
            interpreter,
            env,
            "indirect-function",
            std::slice::from_ref(&binding),
        )
        .unwrap_or_else(|_| binding.clone())
    } else {
        binding.clone()
    };
    let is_keymap = call(interpreter, env, "keymapp", std::slice::from_ref(&resolved))
        .map(|value| value.is_truthy())
        .unwrap_or(false);
    if is_keymap {
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
    interpreter.set_variable("last-command-event", last_event, env);
    interpreter.set_variable(
        "this-command-keys-vector",
        Value::list(
            std::iter::once(Value::Symbol("vector-literal".into())).chain(keys.iter().cloned()),
        ),
        env,
    );
    interpreter.set_variable("this-command", binding.clone(), env);
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
    result
}

fn command_error_text(error: &LispError) -> String {
    let text = match error {
        LispError::SignalValue(data) => {
            if matches!(data, Value::Symbol(name) if name == "quit") {
                "Quit".to_string()
            } else {
                format!("{data}")
            }
        }
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
                    '[' => Some(Value::Integer(27)),
                    '\\' => Some(Value::Integer(28)),
                    ']' => Some(Value::Integer(29)),
                    '^' => Some(Value::Integer(30)),
                    '_' | '/' => Some(Value::Integer(31)),
                    '?' => Some(Value::Integer(127)),
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

// ── Echo area ───────────────────────────────────────────────────────────

fn messages_len(interpreter: &Interpreter) -> usize {
    interpreter
        .buffer_list
        .iter()
        .find(|(_, name)| name == "*Messages*")
        .and_then(|(id, _)| interpreter.get_buffer_by_id(*id))
        .map(|buffer| buffer.point_max())
        .unwrap_or(0)
}

fn sync_echo_with_messages(interpreter: &mut Interpreter, state: &mut TtyState) {
    let len = messages_len(interpreter);
    if len != state.messages_seen {
        state.messages_seen = len;
        if let Some(last) = interpreter
            .buffer_list
            .iter()
            .find(|(_, name)| name == "*Messages*")
            .and_then(|(id, _)| interpreter.get_buffer_by_id(*id))
            .map(|buffer| buffer.full_buffer_string())
            .and_then(|contents| contents.lines().next_back().map(str::to_string))
        {
            state.echo = last;
        }
    }
}

// ── Minibuffer line editor ──────────────────────────────────────────────

/// Read one line in the echo area.  RET submits, C-g cancels, DEL edits;
/// this is the terminal's raw input path so it must not touch the
/// interpreter (which is re-entrantly executing the prompting command).
fn read_minibuffer_line(prompt: &str, initial: &str) -> Option<String> {
    let mut text = initial.to_string();
    loop {
        let (cols, rows) = terminal::size().unwrap_or((80, 24));
        let mut line = format!("{prompt}{text}");
        line.truncate(cols as usize);
        let mut out = io::stdout();
        let _ = queue!(
            out,
            cursor::MoveTo(0, rows.saturating_sub(1)),
            terminal::Clear(terminal::ClearType::CurrentLine),
            style::Print(&line),
        );
        let _ = out.flush();
        let Ok(event) = event::read() else {
            return None;
        };
        let Event::Key(key) = event else { continue };
        if key.modifiers.contains(KeyModifiers::CONTROL) {
            match key.code {
                KeyCode::Char('g') => return None,
                KeyCode::Char('m') => return Some(text),
                KeyCode::Char('u') => text.clear(),
                _ => {}
            }
            continue;
        }
        match key.code {
            KeyCode::Enter => return Some(text),
            KeyCode::Esc => return None,
            KeyCode::Backspace => {
                text.pop();
            }
            KeyCode::Char(c) => text.push(c),
            _ => {}
        }
    }
}

// ── Redisplay ───────────────────────────────────────────────────────────

fn redraw(interpreter: &mut Interpreter, state: &mut TtyState) -> io::Result<()> {
    let (cols, rows) = terminal::size()?;
    let cols = cols.max(10) as usize;
    let rows = rows.max(4) as usize;
    let text_rows = rows - 2; // mode line + echo area

    let buffer = &interpreter.buffer;
    let point = buffer.point();
    let point_line = buffer.line_number_at_pos(point); // 1-based
    let line_start = buffer.line_start_at(point);
    let contents = buffer.full_buffer_string();

    // Keep point visible; recenter on a jump like GNU's default scrolling.
    if point_line < state.top_line || point_line >= state.top_line + text_rows {
        state.top_line = point_line.saturating_sub(text_rows / 2).max(1);
    }

    let mut out = io::stdout();
    queue!(out, cursor::Hide, cursor::MoveTo(0, 0))?;

    let mut cursor_position = (0u16, 0u16);
    let mut lines = contents.split('\n');
    // Skip to the top of the window.
    for _ in 1..state.top_line {
        if lines.next().is_none() {
            break;
        }
    }
    for row in 0..text_rows {
        queue!(
            out,
            cursor::MoveTo(0, row as u16),
            terminal::Clear(terminal::ClearType::CurrentLine)
        )?;
        let buffer_line = state.top_line + row;
        if let Some(line) = lines.next() {
            let rendered = render_line(line, cols);
            queue!(out, style::Print(&rendered))?;
            if buffer_line == point_line {
                let column = display_column(line, point - line_start);
                cursor_position = (column.min(cols - 1) as u16, row as u16);
            }
        }
    }

    // Mode line, GNU-flavored.
    let modified = if interpreter.buffer.is_modified() {
        "**"
    } else {
        "--"
    };
    let mode_line = format!(
        "-UUU:{modified}-  {}   L{point_line}   (Fundamental)",
        interpreter.buffer.name
    );
    let mut mode_line = mode_line;
    if mode_line.len() < cols {
        mode_line.extend(std::iter::repeat_n('-', cols - mode_line.len()));
    }
    mode_line.truncate(cols);
    queue!(
        out,
        cursor::MoveTo(0, text_rows as u16),
        terminal::Clear(terminal::ClearType::CurrentLine),
        style::SetAttribute(style::Attribute::Reverse),
        style::Print(&mode_line),
        style::SetAttribute(style::Attribute::Reset),
    )?;

    // Echo area.
    let mut echo = state.echo.clone();
    echo.truncate(cols);
    queue!(
        out,
        cursor::MoveTo(0, (text_rows + 1) as u16),
        terminal::Clear(terminal::ClearType::CurrentLine),
        style::Print(&echo),
        cursor::MoveTo(cursor_position.0, cursor_position.1),
        cursor::Show,
    )?;
    out.flush()
}

/// Render a buffer line for the glass: expand tabs to 8-column stops and
/// clip to the window width.
fn render_line(line: &str, cols: usize) -> String {
    let mut rendered = String::with_capacity(line.len());
    let mut column = 0usize;
    for c in line.chars() {
        if column >= cols {
            break;
        }
        if c == '\t' {
            let next_stop = (column / 8 + 1) * 8;
            while column < next_stop.min(cols) {
                rendered.push(' ');
                column += 1;
            }
        } else {
            rendered.push(c);
            column += 1;
        }
    }
    rendered
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
        assert_eq!(render_line("a\tb", 80), "a       b");
        assert_eq!(render_line("\t\t", 80), "                ");
        assert_eq!(render_line("12345678\tx", 80), "12345678        x");
    }

    #[test]
    fn rendered_lines_clip_to_the_window_width() {
        assert_eq!(render_line("abcdefgh", 4), "abcd");
        assert_eq!(render_line("a\tb", 5), "a    ");
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
