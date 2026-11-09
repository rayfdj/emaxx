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

use crate::lisp::cached_symbol;
use std::io::{self, Write};

use crossterm::{cursor, queue, style, terminal};

use crate::batch;
use crate::lisp::eval::Interpreter;
use crate::lisp::primitives::{
    InvisibilitySpec, invisible_class_at, invisible_run_at, resolve_buffer_invisibility,
    visual_line_first_line,
};
use crate::lisp::types::{Env, Kind, LispError, LispErrorKind, Value};

mod layout;
mod long_lines;
mod redisplay_flags;
mod redisplay_window;
mod sysdep;

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

/// The frame route of the session's terminal, given up even when the
/// session ends by panic or error (the terminal's modes are TtyModes').
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Self {
        crate::lisp::eval::terminal::activate_frame_route();
        Self
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        crate::lisp::eval::terminal::clear_frame_route();
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
    /// The text row of `w->cursor.y' as the last display that showed
    /// point left it (try_window resets only its vpos).
    last_cursor_y: Option<usize>,
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
    /// The window's `w->cursor' as its last display left it, in frame
    /// cells (column, row): update_frame_1 puts the terminal's one cursor
    /// there while the window is the frame's selected one.
    frame_cursor: Option<(u16, u16)>,
    /// The frame cells (top, left, width, height) its last display
    /// painted, which a redisplay that leaves the window alone keeps.
    painted_rect: Option<(usize, usize, usize, usize)>,
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
    CellAttrs::merged_over(&base, &over)
}

/// The `face-remapping-alist' a buffer's display realizes faces with
/// (redisplay_window makes the window's buffer current, and
/// lookup_named_face and lookup_basic_face consult it), and the printed
/// form those faces are cached under apart from the frame's own: none
/// (nil) for the echo area, the menu bar and the other frame parts.
struct FaceRemapping {
    alist: Value,
    key: String,
}

fn face_remapping(interpreter: &Interpreter, buffer_id: Option<u64>) -> FaceRemapping {
    let alist = buffer_id
        .and_then(|id| {
            interpreter
                .buffer_local_value_key(id, cached_symbol!("face-remapping-alist"))
                .or_else(|| interpreter.global_binding_value("face-remapping-alist"))
        })
        .unwrap_or(Value::Nil);
    let key = if alist.is_nil() {
        String::new()
    } else {
        format!("{alist}\u{1}")
    };
    FaceRemapping { alist, key }
}

/// FACE realized as REMAPPING has it, cached by face and remapping.
fn realized_face(
    cache: &mut std::collections::HashMap<String, CellAttrs>,
    interpreter: &mut Interpreter,
    env: &mut Env,
    remapping: &FaceRemapping,
    face: &Value,
) -> CellAttrs {
    *cache
        .entry(format!("{}{face}", remapping.key))
        .or_insert_with(|| {
            crate::lisp::primitives::resolve_tty_face_attrs_remapped(
                interpreter,
                env,
                face,
                remapping.alist,
            )
        })
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
    fn force_foreground(&mut self, from: usize, to: usize, foreground: Option<i64>) {
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
    /// The frame as last painted: every row above the echo area, the echo
    /// row, and the terminal size they were painted for.  Redraw emits
    /// only rows that differ — GNU's dispnew current-matrix idea, one
    /// line deep.
    painted_rows: Vec<PaintRow>,
    painted_echo: Vec<PaintRow>,
    /// The default face, `inverse-video' and
    /// `tty-suppress-bold-inverse-default-colors' the rows were painted
    /// with.
    painted_faces: Option<(CellAttrs, bool, bool)>,
    /// `max-mini-window-height' as resolved at the last full redisplay,
    /// so the interpreter-less painter can grow the echo area too.
    echo_max_rows: usize,
    /// Rows the mini window currently occupies, as `resize-mini-windows'
    /// sizes it: under its default `grow-only' a message taller than the
    /// window grows it (up to `max-mini-window-height'), a shorter one
    /// keeps the height, and an empty echo area returns it to one row.
    echo_rows: usize,
    painted_size: (usize, usize),
    /// The terminal's size when last read: a change is GNU's
    /// SIGWINCH (handle_window_change_signal), none yet its
    /// init_display_interactive.
    terminal_size: Option<(usize, usize)>,
    /// Resolved face attributes, GNU's realized faces of the frame and
    /// the count of its frees they belong to.
    face_cache: std::collections::HashMap<String, CellAttrs>,
    face_cache_epoch: Option<(crate::lisp::types::FrameRef, u64)>,
    /// Identity of the read whose grow-only minibuffer height is retained.
    /// Minibuffer buffers are reused, so the buffer id alone cannot detect
    /// entry into a fresh read.
    minibuffer_activation_id: Option<u64>,
    /// The message-emission count the glass reflects: a wait for input
    /// repaints the echo row only when this falls behind, as read_char's
    /// input-arrival wipe waits for the next redisplay.
    painted_message_tick: u64,
    /// The selected window's cursor the last redisplay computed:
    /// update_frame_1's cursor on a terminal frame when it is not in the
    /// echo area.
    window_cursor: (u16, u16),
    /// Where the last frame update left the cursor (the tty's curX and
    /// curY after update_frame_1's cursor_to).
    update_cursor: (u16, u16),
    /// The window layout the last redisplay showed (each window's id and
    /// edges).
    last_layout: Vec<(u64, usize, usize, usize, usize)>,
}

impl TtyState {
    /// The frame's row Y as last painted: a window row, or below them one
    /// of the echo area's (the frame's current matrix, all its lines).
    fn screen_row(&self, y: usize) -> Option<&PaintRow> {
        match y.checked_sub(self.painted_rows.len()) {
            None => self.painted_rows.get(y),
            Some(echo) => self.painted_echo.get(echo),
        }
    }

    /// Record ROW as painted at the frame's row Y (see `screen_row').
    fn set_screen_row(&mut self, y: usize, row: PaintRow) {
        match y.checked_sub(self.painted_rows.len()) {
            None => self.painted_rows[y] = row,
            Some(echo) => self.painted_echo[echo] = row,
        }
    }

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
            painted_rows: Vec::new(),
            painted_echo: Vec::new(),
            painted_faces: None,
            echo_rows: 1,
            echo_max_rows: 6,
            painted_size: (0, 0),
            terminal_size: None,
            face_cache: std::collections::HashMap::new(),
            face_cache_epoch: None,
            minibuffer_activation_id: None,
            painted_message_tick: 0,
            window_cursor: (0, 0),
            update_cursor: (0, 0),
            last_layout: Vec::new(),
        }
    }
}

/// emacs.c's `-t DEVICE': the terminal the session opens is DEVICE
/// (dev_tty), not DEV_TTY.
pub fn set_dev_tty(name: &str) {
    crate::lisp::eval::terminal::set_dev_tty(name);
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
    // dispnew.c:init_display: a terminal on standard input and TERM set,
    // then term.c:init_tty(dev_tty, TERM, must_succeed): the device, its
    // terminfo entry, its size and its cursor motion, each fatal (exit
    // status 1) when missing.
    // SAFETY: isatty has no preconditions.
    if unsafe { libc::isatty(libc::STDIN_FILENO) } != 1 {
        fatal("standard input is not a tty");
    }
    let Some(terminal_type) = std::env::var_os("TERM") else {
        eprintln!("Please set the environment variable TERM; see 'tset'.");
        std::process::exit(1);
    };
    let terminal_type = terminal_type.to_string_lossy().into_owned();
    let tty_fd = sysdep::open_dev_tty(&crate::lisp::eval::terminal::dev_tty())
        .unwrap_or_else(|text| fatal(&text));
    let termcap = crate::lisp::eval::terminal::read_termcap(&terminal_type, tty_fd)
        .unwrap_or_else(|error| fatal(&error.fatal_text()));
    if termcap.height < 3 || termcap.width < 3 {
        fatal(&format!(
            "Screen size {}x{} is too small",
            termcap.width, termcap.height
        ));
    }
    if !termcap.cursor_motion {
        fatal(&crate::lisp::eval::terminal::cursor_motion_fatal_text(
            &terminal_type,
        ));
    }
    // init_display_interactive: the initial frame on the terminal, given
    // its size at once, then the frame's faces.
    interpreter.set_tty_colors(termcap.colors.clone());
    interpreter.set_tty_appearance(termcap.appearance.clone());
    interpreter.set_tty_terminal_type(Some(terminal_type));
    interpreter.set_tty_frame_size(termcap.width, termcap.height);
    batch::initialize_initial_frame_faces(&mut interpreter)?;

    let guard = TerminalGuard::enter();
    let terminal = interpreter
        .decode_terminal(&Value::Nil)
        .expect("the initial frame's terminal is live");
    crate::lisp::eval::terminal::set_controlling_output(Some((
        tty_fd,
        termcap.width,
        termcap.height,
        terminal,
    )));
    // The rest of init_tty: the mode strings of the terminal's entry,
    // whether it has a Meta key, term_get_fkeys's keys for
    // `input-decode-map'; then sysdep.c:init_sys_modes.
    let tty_modes = std::rc::Rc::new(std::cell::RefCell::new(sysdep::TtyModes::new(
        tty_fd,
        terminal_mode_strings(&termcap),
    )));
    term_get_fkeys(&mut interpreter, &mut env, &termcap.keys).map_err(|error| error.to_string())?;
    let tty_fd = tty_modes.borrow().fd;
    let queue = SharedEventQueue::default();
    let input_modes = crate::lisp::primitives::TtyInputModes {
        flow_control: false,
        meta_key: i32::from(termcap.meta_key),
    };
    crate::lisp::primitives::set_tty_input_modes(
        Some(input_modes),
        tty_modes.borrow().controlling,
        Some(Box::new({
            let tty_modes = std::rc::Rc::clone(&tty_modes);
            let queue = queue.clone();
            move |op| {
                let Ok(mut modes) = tty_modes.try_borrow_mut() else {
                    return;
                };
                match op {
                    crate::lisp::primitives::SysModes::Stuff(stuff) => {
                        // stuff_buffered_input: STUFF, then what was read
                        // ahead and not used, back for the shell to read.
                        for byte in stuff {
                            sysdep::stuff_char(modes.fd, byte);
                        }
                        while let Some(QueuedInput::Lisp(event)) = queue.pop() {
                            if let Kind::Integer(code) = event.kind()
                                && (0..0x100).contains(&(code & !CHAR_META))
                            {
                                sysdep::stuff_char(modes.fd, (code & 0xff) as u8);
                            }
                        }
                    }
                    crate::lisp::primitives::SysModes::Reset => {
                        let rows = sysdep::get_tty_size(modes.fd).map_or(24, |(_, rows)| rows);
                        modes.reset(rows);
                    }
                    crate::lisp::primitives::SysModes::Refresh => modes.arm(),
                    crate::lisp::primitives::SysModes::Init { interrupt_input } => {
                        let input =
                            crate::lisp::primitives::tty_input_modes().unwrap_or(input_modes);
                        let quit = crate::lisp::primitives::quit_char() as u8;
                        // A terminal entered again shows what the shell
                        // left: the frame is garbaged.
                        if modes.initted() {
                            crate::lisp::primitives::set_frame_garbaged();
                        }
                        let erase = modes.init(interrupt_input, input, quit);
                        crate::lisp::primitives::note_tty_erase_char(erase);
                    }
                }
            }
        })),
    );
    let interrupt_input = interpreter.input_interrupt_mode();
    crate::lisp::primitives::init_all_sys_modes(&mut interpreter, interrupt_input);
    // dispnew.c:init_display's SIGWINCH handler.
    crate::lisp::primitives::install_window_change_handler();
    interpreter.note_selected_frame(interpreter.selected_frame_id);
    let state = std::rc::Rc::new(std::cell::RefCell::new(TtyState::new()));
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
            // redisplay_internal: nothing while a redisplay runs (or after
            // a C stack overflow left one without its unwind_redisplay),
            // nor while `inhibit-redisplay' is non-nil.
            if crate::lisp::primitives::redisplaying_p()
                || interpreter
                    .forwarded_c_value("inhibit-redisplay", env)
                    .is_some_and(|value| value.is_truthy())
            {
                return;
            }
            let count = interpreter.specpdl_index();
            crate::lisp::primitives::begin_redisplay(interpreter);
            // specbind (Qinhibit_free_realized_faces, Qnil): realized faces
            // may be freed until this redisplay produces glyphs.
            let _ = interpreter.specbind_symbol(
                &cached_symbol!("inhibit-free-realized-faces").with(|symbol| *symbol),
                Value::Nil,
                env,
            );
            // "Record this function, so it appears on the profiler's
            // backtraces": record_in_backtrace with no arguments, popped by
            // the unbind_to.
            interpreter.push_backtrace_frame(Value::symbol("redisplay_internal (C function)"), &[]);
            // update_frame pauses for pending input only without
            // `redisplay-dont-pause' (redisplay_dont_pause forces it).
            let dont_pause = interpreter
                .lookup_var_key(cached_symbol!("redisplay-dont-pause"), env)
                .is_some_and(|value| value.is_truthy());
            if !dont_pause && queue.input_pending() {
                // update_frame's detect_input_pending_ignore_squeezables
                // sets input_pending.
                crate::lisp::primitives::note_redisplay_input_pending();
                // Painting yields to the input, but redisplay_internal
                // runs the window change functions before any update.
                let _ = crate::lisp::primitives::run_window_change_functions(interpreter, env);
            } else if let Ok(mut state) = state.try_borrow_mut() {
                let _ = redraw_with_echo_policy(interpreter, env, &mut state, false);
            }
            let _ = interpreter.unbind_to(count, env);
        }
    })));
    // command_loop_1's resize_echo_area_exactly after a command: the mini
    // window showing a message takes the message's own height (exactly,
    // with no minibuffer active), and a changed height is redisplayed at
    // once with windows_or_buffers_changed 42 and update_mode_lines 30.
    crate::lisp::primitives::set_resize_echo_area_exactly(Some(Box::new({
        let state = std::rc::Rc::clone(&state);
        move |interpreter, env| {
            if crate::lisp::primitives::echo_area_message_value().is_none() {
                return;
            }
            let Ok((cols, rows)) = crate::lisp::eval::terminal::output_size() else {
                return;
            };
            let cols = usize::from(cols.max(1)).min(interpreter.frame_width().max(1) as usize);
            let rows = usize::from(rows.max(1)).min(interpreter.frame_height().max(1) as usize);
            let exactly = interpreter.minibuffer_depth() == 0;
            let resized = {
                let Ok(mut state) = state.try_borrow_mut() else {
                    return;
                };
                resize_mini_window(interpreter, env, &mut state, cols, rows, exactly).changed
            };
            if resized {
                interpreter.windows_or_buffers_changed = 42;
                interpreter.update_mode_lines = 30;
                crate::lisp::primitives::run_tty_frame_redraw(interpreter, env);
            }
        }
    })));
    // message3_nolog's echo_area_display (true): a message, or its
    // clearing, reaches the glass when it is made.
    crate::lisp::primitives::set_echo_area_display(Some(Box::new({
        let state = std::rc::Rc::clone(&state);
        move |interpreter, env| draw_echo_row_composed(interpreter, env, &state)
    })));
    // term.c's menu_show_hook: the dropdown executor F10's popup path
    // reaches through x-popup-menu.
    crate::lisp::primitives::menu::set_tty_menu_activate(Some(make_menu_activate(
        std::rc::Rc::clone(&state),
    )));
    // term.c:init_tty's terminal: its read_socket_hook
    // (tty_read_avail_input) storing into the keyboard buffer, and its
    // input descriptor given add_keyboard_wait_descriptor (init_sys_modes
    // asked for SIGIO on it with interrupt_input).
    crate::lisp::primitives::set_keyboard_terminal(Some(
        crate::lisp::primitives::KeyboardTerminal {
            read_socket_hook: Box::new({
                let queue = queue.clone();
                move |interpreter, env| queue.read_avail_input(interpreter, env, tty_fd)
            }),
            buffer_nonempty: Box::new({
                let queue = queue.clone();
                move || queue.input_pending()
            }),
            fd: Some(tty_fd),
        },
    ));
    // keyboard.c's Fdiscard_input: the keyboard buffer empties, and
    // discard_tty_input sets the terminal's modes again (TCSADRAIN, which
    // leaves what the terminal holds unread in place).
    crate::lisp::primitives::set_tty_input_discard(Some(Box::new({
        let queue = queue.clone();
        let tty_modes = std::rc::Rc::clone(&tty_modes);
        move || {
            queue.discard();
            if let Ok(modes) = tty_modes.try_borrow() {
                modes.discard_input();
            }
        }
    })));
    // keyboard.c:command_loop enters top_level_1 after terminal input is
    // available, then the editing loop. Do not replay Lisp startup actions.
    // emacs.c runs after-pdump-load-hook after init_display. Hook code can
    // therefore inspect the real terminal or read events before top-level.
    // Then main's Frecursive_edit: the top level's command loop, whose
    // top_level_1 evaluates `top-level' (startup.el's normal-top-level).
    batch::set_command_line_args(&mut interpreter, command_line_args);
    let session = batch::safe_run_hooks(
        &mut interpreter,
        &crate::lisp::types::symbols::SymbolKey::new("after-pdump-load-hook").symbol(),
    )
    .and_then(|()| crate::lisp::primitives::set_initial_minibuffer_mode(&mut interpreter))
    .and_then(|()| crate::lisp::primitives::recursive_edit_1(&mut interpreter, &mut env));
    let code = match session.map_err(LispError::into_kind) {
        Err(LispErrorKind::Terminate(termination)) => Ok(termination.exit_code),
        Err(error) => Err(LispError::from(error).to_string()),
        Ok(()) => Ok(0),
    };
    crate::lisp::primitives::set_tty_frame_redraw(None);
    crate::lisp::primitives::set_tty_event_reader(None);
    crate::lisp::primitives::set_tty_event_poller(None);
    crate::lisp::primitives::set_keyboard_terminal(None);
    crate::lisp::primitives::set_tty_input_modes(None, false, None);
    drop(tty_modes);
    crate::lisp::primitives::set_tty_input_discard(None);
    crate::lisp::primitives::menu::set_tty_menu_activate(None);
    crate::lisp::primitives::set_echo_area_display(None);
    crate::lisp::primitives::set_resize_echo_area_exactly(None);
    crate::lisp::primitives::set_interactive_window_metrics(None);
    // Fkill_emacs's exit leaves each child as shut_down_emacs left it
    // (hung up, not killed), as the batch exit does.
    interpreter.release_external_resources_for_exit();
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
}

impl QueuedInput {
    /// keyboard.c:make_lispy_event: the event as Lisp reads it.
    fn into_lisp(self, _interpreter: &mut Interpreter) -> Option<Value> {
        let Self::Lisp(event) = self;
        Some(event)
    }
}

thread_local! {
    /// gobble_input's -2 for the controlling terminal: its device
    /// terminated, and the command loop ends.
    static TERMINAL_GONE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Clone, Default)]
struct SharedEventQueue(std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<QueuedInput>>>);

impl SharedEventQueue {
    /// keyboard.c's readable_events: the keyboard buffer holds an event.
    /// Redisplay is skipped while input is pending, GNU's redisplay
    /// preemption: a burst of keys paints once at the end, and scrolling
    /// recenters against the final point, not each intermediate one.
    fn input_pending(&self) -> bool {
        !self.0.borrow().is_empty()
    }

    /// Fdiscard_input: the keyboard buffer emptied, and the input the
    /// terminal still holds with it (discard_tty_input's TCIFLUSH).
    fn discard(&self) {
        self.0.borrow_mut().clear();
    }

    /// The next event in the keyboard buffer (kbd_fetch_ptr).
    fn pop(&self) -> Option<QueuedInput> {
        self.0.borrow_mut().pop_front()
    }

    /// keyboard.c:tty_read_avail_input: the bytes the terminal holds
    /// (FIONREAD's count, at most KBD_BUFFER_SIZE - 1), each an
    /// ASCII_KEYSTROKE_EVENT whose 8th bit the terminal's meta mode makes
    /// Meta or strips, stored with kbd_buffer_store_event; input that
    /// follows a quit character too closely is not looked at.  How many
    /// bytes it read, -2 when the terminal device is gone.
    fn read_avail_input(
        &self,
        interpreter: &mut Interpreter,
        env: &mut Env,
        fd: std::os::fd::RawFd,
    ) -> i32 {
        let bytes = match sysdep::read_available(fd) {
            sysdep::TtyRead::Gone => {
                TERMINAL_GONE.set(true);
                return -2;
            }
            sysdep::TtyRead::Bytes(bytes) => bytes,
        };
        if bytes.is_empty() {
            return 0;
        }
        // The events' frame is the terminal's top frame: one that is not
        // the frame of the last event is a switch-frame first.
        if crate::lisp::eval::terminal::secondary_output_active()
            && let Some(frame) = crate::lisp::eval::terminal::primary_top_frame()
        {
            self.0
                .borrow_mut()
                .push_back(QueuedInput::Lisp(Value::list([
                    Value::symbol("switch-frame"),
                    Value::Frame(frame),
                ])));
        }
        let meta_key = crate::lisp::primitives::tty_input_modes().map_or(1, |modes| modes.meta_key);
        for &byte in &bytes {
            let mut code = i64::from(byte);
            let mut modifiers = 0;
            if meta_key == 1 && byte & 0x80 != 0 {
                modifiers = CHAR_META;
            }
            if meta_key < 2 {
                code &= !0x80;
            }
            // make_lispy_event: a capital letter in a chord with no shift
            // came from caps lock, and is un-capitalized.
            let mut char_code = code;
            if modifiers != 0 && (i64::from(b'A')..=i64::from(b'Z')).contains(&char_code) {
                char_code += 32;
            }
            let event = Value::Integer(char_code | modifiers);
            if crate::lisp::primitives::kbd_buffer_store_event(interpreter, env, Some(&event)) {
                self.0.borrow_mut().push_back(QueuedInput::Lisp(event));
            }
            // Don't look at input that follows a C-g too closely.
            if code == crate::lisp::primitives::quit_char() {
                break;
            }
        }
        bytes.len() as i32
    }

    /// keyboard.c:read_decoded_event_from_main_queue for FIRST, an event
    /// just taken from the keyboard buffer: on the terminal, unless the
    /// read takes events raw (PREV_EVENT t), the bytes of a sequence the
    /// terminal's `keyboard-coding-system' encodes are gathered (the next
    /// ones waited for) and decoded, the 8th bit made Meta after decoding
    /// for the `encoded' meta mode; the first character is the event, the
    /// rest `unread-command-events'.
    fn decode_event(&self, interpreter: &mut Interpreter, env: &mut Env, first: Value) -> Value {
        if crate::lisp::primitives::raw_event_read() {
            return first;
        }
        let Some(modes) = crate::lisp::primitives::tty_input_modes() else {
            return first;
        };
        let meta_key = modes.meta_key;
        let Some(decoding) = keyboard_decoding(interpreter) else {
            return first;
        };
        let byte_limit = if meta_key == 1 { 0x80 } else { 0x100 };
        let fixnat_byte = |event: &Value| match event.kind() {
            Kind::Integer(code) if (0..byte_limit).contains(&code) => Some(code),
            _ => None,
        };
        const MAX_ENCODED_BYTES: usize = 16;
        let mut events = vec![first];
        loop {
            let last = *events.last().expect("an event at least");
            if fixnat_byte(&last).is_none() {
                break;
            }
            match &decoding {
                KeyboardDecoding::RawText => {
                    if meta_key != 2 {
                        for event in &mut events {
                            if let Kind::Integer(c) = event.kind() {
                                let modifier = if meta_key == 3 && c < 0x100 && c & 0x80 != 0 {
                                    CHAR_META
                                } else {
                                    0
                                };
                                *event = Value::Integer((c & !0x80) | modifier);
                            }
                        }
                    }
                    break;
                }
                KeyboardDecoding::Coding { coding, utf8 } => {
                    let mut src: Vec<u8> = events
                        .iter()
                        .filter_map(|event| fixnat_byte(event).map(|code| code as u8))
                        .collect();
                    if meta_key < 2 {
                        for byte in &mut src {
                            *byte &= !0x80;
                        }
                    }
                    // The encoded sequence is incomplete: read on.
                    if *utf8
                        && events.len() < MAX_ENCODED_BYTES
                        && std::str::from_utf8(&src)
                            .err()
                            .is_some_and(|error| error.error_len().is_none())
                    {
                        match self.next_raw_event(interpreter, env) {
                            RawNext::Event(next) => {
                                events.push(next);
                                continue;
                            }
                            RawNext::Quit => {
                                return self.quit_thrown_mid_sequence(interpreter, env, first);
                            }
                            RawNext::Ended => break,
                        }
                    }
                    // decode_coding_c_string: the bytes as a unibyte
                    // string decoded, a byte no character takes an
                    // eight-bit one.
                    let unibyte = crate::lisp::primitives::bytes_to_shared_unibyte_value(&src);
                    let Ok(decoded) = crate::lisp::primitives::coding::decode_coding_text(
                        interpreter,
                        &unibyte,
                        Some(coding),
                        None,
                        false,
                        false,
                        env,
                    ) else {
                        break;
                    };
                    let Some(decoded) = crate::lisp::primitives::string_like(&decoded) else {
                        break;
                    };
                    events = decoded
                        .character_codes()
                        .into_iter()
                        .map(|c| {
                            if meta_key == 3 && c < 0x100 && c & 0x80 != 0 {
                                Value::Integer((c & !0x80) | CHAR_META)
                            } else {
                                Value::Integer(c)
                            }
                        })
                        .collect();
                    if events.is_empty() {
                        return first;
                    }
                    break;
                }
            }
        }
        // Only one event can be returned: the others for later.
        if events.len() > 1 {
            // nconc2 (Flist (n - 1, events + 1), Vunread_command_events).
            let rest: Vec<Value> = events.drain(1..).collect();
            crate::lisp::primitives::prepend_unread_command_events(interpreter, env, &rest);
        }
        events[0]
    }

    /// The next event of a sequence being decoded: the keyboard buffer's,
    /// waited for until the read's END_TIME (read_event_from_main_queue);
    /// a quit thrown to read_char while it waits; or the wait's end.
    fn next_raw_event(&self, interpreter: &mut Interpreter, env: &mut Env) -> RawNext {
        loop {
            if let Some(QueuedInput::Lisp(event)) = self.pop() {
                return RawNext::Event(event);
            }
            if TERMINAL_GONE.get() {
                return RawNext::Ended;
            }
            let end_time = crate::lisp::primitives::tty_read_end_time();
            match crate::lisp::primitives::kbd_buffer_get_event_wait(
                interpreter,
                env,
                end_time,
                true,
            ) {
                Ok(crate::lisp::primitives::KbdWait::Ready) if self.input_pending() => {}
                Ok(crate::lisp::primitives::KbdWait::Special { .. }) => {}
                Ok(crate::lisp::primitives::KbdWait::Quit) => return RawNext::Quit,
                _ => return RawNext::Ended,
            }
        }
    }

    /// read_char's setjmp when a quit is thrown to it in the middle of an
    /// encoded sequence: the bytes read so far are gone; the quit
    /// character is the event (under `inhibit-quit' its quit not reported
    /// again), unless `quit-flag' holds `throw-on-input', when read_char
    /// reads again.
    fn quit_thrown_mid_sequence(
        &self,
        interpreter: &mut Interpreter,
        env: &mut Env,
        first: Value,
    ) -> Value {
        let throw_on_input = interpreter.throw_on_input_value();
        if crate::lisp::types::eq(interpreter.quit_flag_value(), throw_on_input) {
            return match self.next_raw_event(interpreter, env) {
                RawNext::Event(event) => self.decode_event(interpreter, env, event),
                RawNext::Quit | RawNext::Ended => first,
            };
        }
        if interpreter.inhibit_quit_is_truthy() {
            interpreter.set_c_variable("quit-flag", Value::Nil);
        }
        Value::Integer(crate::lisp::primitives::quit_char())
    }
}

/// What waiting for an encoded sequence's next byte came to.
enum RawNext {
    Event(Value),
    /// quit_throw_to_read_char.
    Quit,
    /// The read's end time, or the terminal's loss.
    Ended,
}

/// lisp.h's CHAR_META: the Meta modifier bit of a character event.
const CHAR_META: i64 = 0x0800_0000;

/// How read_decoded_event_from_main_queue decodes the terminal's input.
enum KeyboardDecoding {
    /// raw_text_coding_system_p: only the meta mode's 8th bit.
    RawText,
    /// decode_coding_c_string by CODING; UTF8 when an incomplete sequence
    /// can be told (and read on).
    Coding { coding: String, utf8: bool },
}

/// The selected frame's terminal keyboard coding, when it requires
/// decoding (CODING_REQUIRE_DECODING_MASK): None for `no-conversion' and
/// the raw-text of Unix line ends.
fn keyboard_decoding(interpreter: &Interpreter) -> Option<KeyboardDecoding> {
    let coding = interpreter
        .terminal_state(interpreter.selected_terminal_id())
        .and_then(|terminal| terminal.keyboard_coding.clone())?;
    let kind = interpreter.coding_system_kind_name(&coding)?;
    match kind.as_str() {
        // setup_coding_system: raw-text (no-conversion among them) needs
        // decoding only for an eol type other than unix.
        "raw-text" => {
            let unix = interpreter.coding_spec_index(&coding).is_some_and(|spec| {
                interpreter.coding_specs[spec]
                    .eol_type
                    .is_key(cached_symbol!("unix"))
            });
            (!unix).then_some(KeyboardDecoding::RawText)
        }
        _ => Some(KeyboardDecoding::Coding {
            utf8: kind == "utf-8",
            coding,
        }),
    }
}

/// The keyboard buffer's next event for command code that pulls events
/// itself (`y-or-n-p', `read-event'): kbd_buffer_get_event's fetch, which
/// keyboard.c's wait (kbd_buffer_get_event_wait) fills.  The echo area is
/// repainted first so a prompt just issued with `message' (y-or-n-p's
/// protocol) is visible while the read waits.  `Some(None)' is an empty
/// buffer; `None' never comes back, the terminal's loss being gobble_input's.
fn make_event_poller(
    queue: SharedEventQueue,
    state: std::rc::Rc<std::cell::RefCell<TtyState>>,
) -> crate::lisp::primitives::TtyEventPoller {
    Box::new(move |interpreter| {
        draw_echo_row(interpreter, &state);
        // gobble_input's -2: the terminal device terminated.
        if TERMINAL_GONE.get() && !queue.input_pending() {
            return None;
        }
        while let Some(input) = queue.pop() {
            if let Some(event) = input.into_lisp(interpreter) {
                return Some(Some(queue.decode_event(
                    interpreter,
                    &mut Env::new(),
                    event,
                )));
            }
        }
        Some(None)
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

/// The mini window's height ceiling, resize_mini_window's max_height:
/// `max-mini-window-height' of a float times FRAME_INNER_HEIGHT (the rows
/// below the menu and tab bars, INNER_ROWS), of an integer that many
/// lines, else a quarter of the inner height, clipped to between one
/// line and the inner height; the window tree above keeps two rows of
/// the frame's ROWS.
fn max_mini_window_rows(interpreter: &Interpreter, inner_rows: usize, rows: usize) -> usize {
    let max_height = match interpreter
        .lookup_var_key(
            cached_symbol!("max-mini-window-height"),
            &crate::lisp::types::Env::new(),
        )
        .map(|v| v.kind())
    {
        Some(Kind::Float(fraction)) => (fraction.get() * inner_rows as f64) as i64,
        Some(Kind::Integer(lines)) => lines,
        _ => (inner_rows / 4) as i64,
    };
    let max_height = max_height.clamp(1, (inner_rows as i64).max(1)) as usize;
    max_height.min(rows.saturating_sub(2)).max(1)
}

/// Paint the echo area through the same character and face translation as
/// full redisplay, including when a blocking event reader requests repaint.
fn draw_echo_row_composed(
    interpreter: &mut Interpreter,
    env: &mut Env,
    state: &std::rc::Rc<std::cell::RefCell<TtyState>>,
) {
    // echo_area_display displays nothing while redisplay is inhibited (a
    // tty menu's activation sets `inhibit-redisplay').
    if interpreter
        .forwarded_c_value("inhibit-redisplay", env)
        .is_some_and(|value| value.is_truthy())
    {
        return;
    }
    // `if (!NILP (echo_area_buffer[0]) || minibuf_level == 0)': with no
    // message an active minibuffer's row is redisplay's, as it is now,
    // not as the last redisplay found it; a mini window not selected is
    // marked for it.
    if crate::lisp::primitives::echo_area_message_value().is_none()
        && interpreter.minibuffer_depth() > 0
    {
        let mini = interpreter.minibuffer_window_id();
        if mini != interpreter.selected_window_id() {
            interpreter.wset_redisplay(mini);
        }
        return;
    }
    // It updates the frame only `if (update_frame_p && !redisplaying_p)'.
    if crate::lisp::primitives::redisplaying_p() {
        return;
    }
    let Ok((cols, rows)) = crate::lisp::eval::terminal::output_size() else {
        return;
    };
    let Ok(mut state) = state.try_borrow_mut() else {
        return;
    };
    let cols = usize::from(cols.max(1));
    let rows = usize::from(rows.max(1));
    // display_echo_area_1 resizes the mini window before displaying, so
    // that its matrix is large enough (resize_mini_window_apply marking
    // the frame when the height changes).
    let frame_rows = rows.min(interpreter.frame_height().max(1) as usize);
    let frame_cols = cols.min(interpreter.frame_width().max(1) as usize);
    let MiniWindowEcho {
        echo_long: long,
        echo_cursor,
        mut echo_paint,
        changed,
        ..
    } = resize_mini_window(interpreter, env, &mut state, frame_cols, frame_rows, false);
    let mini_rows = state.echo_rows.max(1);
    // Not once shut_down_emacs has cleared Vrun_hooks: redisplay needs
    // to run hooks.
    if changed && !interpreter.hooks_cleared {
        // "Must update other windows": the frame marked and a
        // redisplay_internal that pending input does not interrupt
        // (`redisplay-dont-pause' bound to t), in place of the echo
        // area's own update_frame.
        drop(state);
        let frame = interpreter.selected_frame_id;
        interpreter.fset_redisplay(frame);
        let count = interpreter.specpdl_index();
        if interpreter
            .specbind_symbol(
                &cached_symbol!("redisplay-dont-pause").with(|symbol| *symbol),
                Value::T,
                env,
            )
            .is_ok()
        {
            crate::lisp::primitives::run_tty_frame_redraw(interpreter, env);
        }
        let _ = interpreter.unbind_to(count, env);
        mark_mini_window_for_echo_cursor(interpreter, env);
        // The window's height changed: the frame is marked again for the
        // next redisplay.
        interpreter.fset_redisplay(frame);
        return;
    }
    echo_paint.resize(mini_rows, PaintRow::blank(cols));
    // display_echo_area's display_line produces glyphs, after which
    // realized faces may not be freed (PRODUCE_GLYPHS).
    interpreter.set_variable_key(cached_symbol!("inhibit-free-realized-faces"), Value::T, env);
    encode_terminal_cells(interpreter, &mut echo_paint);
    let base = rows.saturating_sub(mini_rows);
    let mut out = terminal_output();
    for (index, echo_row) in echo_paint.iter().enumerate() {
        let _ = paint_row(&mut out, base + index, echo_row);
    }
    // update_frame_1's cursor: in the echo area with `cursor-in-echo-area'
    // or a message shown over the selected minibuffer; otherwise the frame's
    // selected window's own, as its last display computed it (none kept,
    // where the last frame update left the terminal's).
    let cursor_position = if interpreter
        .lookup_var_key(cached_symbol!("cursor-in-echo-area"), env)
        .is_some_and(|value| value.is_truthy())
        || (interpreter.active_minibuffer_buffer_id().is_some()
            && interpreter.selected_window_id() == interpreter.minibuffer_window_id()
            && crate::lisp::primitives::echo_area_message_value().is_some())
    {
        let (row, col) = wrapped_echo_cursor(&long, echo_cursor.unwrap_or(0), cols, mini_rows);
        (
            col.min(cols.saturating_sub(1)) as u16,
            (base + row).min(rows.saturating_sub(1)) as u16,
        )
    } else {
        state
            .views
            .get(&interpreter.selected_window_id())
            .and_then(|view| view.frame_cursor)
            .unwrap_or(state.window_cursor)
    };
    state.update_cursor = cursor_position;
    let _ = queue!(
        out,
        cursor::MoveTo(cursor_position.0, cursor_position.1),
        cursor::Show,
    );
    let _ = out.flush();
    state.painted_message_tick = crate::lisp::primitives::echo_area_message_tick();
    state.painted_echo = echo_paint;
    drop(state);
    mark_mini_window_for_echo_cursor(interpreter, env);
}

/// echo_area_display after its update: with the cursor in the echo area,
/// the next redisplay displays the minibuffer, so that the cursor is put
/// where the minibuffer wants it.
fn mark_mini_window_for_echo_cursor(interpreter: &mut Interpreter, env: &Env) {
    if interpreter
        .lookup_var_key(cached_symbol!("cursor-in-echo-area"), env)
        .is_some_and(|value| value.is_truthy())
    {
        let mini = interpreter.minibuffer_window_id();
        interpreter.wset_redisplay(mini);
    }
}

/// The readers already hold the interpreter, so table selection and face
/// resolution need no separate state or interpreter lookup.
fn draw_echo_row(interpreter: &mut Interpreter, state: &std::rc::Rc<std::cell::RefCell<TtyState>>) {
    let Ok(current) = state.try_borrow() else {
        return;
    };
    if current.painted_message_tick == crate::lisp::primitives::echo_area_message_tick()
        && !crate::lisp::primitives::tty_cursor_in_echo_area()
    {
        return;
    }
    drop(current);
    draw_echo_row_composed(interpreter, &mut Env::new(), state);
}

#[cfg(test)]
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

/// KEYS (a Lisp sequence of events) read by a recursive edit's command
/// loop, which returns when they run out; FORM, evaluated in
/// `post-command-hook' after each command and once at the loop's entry,
/// gives the log, printed.
#[cfg(test)]
fn post_command_log(
    interpreter: &mut Interpreter,
    env: &mut Env,
    keys: &str,
    form: &str,
) -> String {
    let printed = crate::test_support::eval_lisp(
        interpreter,
        env,
        // The recursive edit runs KEYS; without a keyboard read_char then
        // waits, as GNU's does, so a timer ends it with a throw to `exit'
        // once the keys are read and no command runs.
        &format!(
            "(let ((tty-test-log nil)
                   (tty-test-exit
                    (run-at-time 0 0.01
                                 (lambda ()
                                   (when (and (null unread-command-events) (null this-command))
                                     (throw 'exit nil))))))
               (unwind-protect
                   (let ((post-command-hook (list (lambda () (push {form} tty-test-log))))
                         (unread-command-events (append {keys} nil)))
                     (recursive-edit))
                 (cancel-timer tty-test-exit))
               (prin1-to-string (reverse tty-test-log)))"
        ),
    )
    .expect("the keys run");
    crate::lisp::primitives::string_like(&printed)
        .expect("a string")
        .text
}

// ── The terminal's setup ───────────────────────────────────────────────

/// term.c:fatal (vfatal): "emacs: " and MESSAGE on stderr, a newline
/// unless it ends in one, and exit status 1, nothing else run.
fn fatal(message: &str) -> ! {
    eprint!("emacs: {message}");
    if !message.ends_with('\n') {
        eprintln!();
    }
    std::process::exit(1)
}

/// The strings tty_set_terminal_modes and tty_reset_terminal_modes write,
/// from the terminal's termcap entry.
fn terminal_mode_strings(
    termcap: &crate::lisp::eval::terminal::Termcap,
) -> sysdep::TerminalModeStrings {
    sysdep::TerminalModeStrings {
        termcap_modes: termcap.termcap_modes.clone(),
        end_termcap_modes: termcap.end_termcap_modes.clone(),
        keypad_mode: termcap.keypad_mode.clone(),
        end_keypad_mode: termcap.end_keypad_mode.clone(),
        cursor_visible: termcap.cursor_visible.clone(),
        cursor_normal: termcap.cursor_normal.clone(),
        orig_pair: termcap.orig_pair.clone(),
    }
}

/// term.c:term_get_fkeys for the controlling terminal: each function key
/// its termcap entry names, KEYS, defined in `input-decode-map', so
/// read_key_sequence decodes the bytes the key sends.
fn term_get_fkeys(
    interpreter: &mut Interpreter,
    env: &mut Env,
    keys: &[(Vec<u8>, String)],
) -> Result<(), LispError> {
    let map = interpreter
        .lookup_var_key(cached_symbol!("input-decode-map"), env)
        .unwrap_or(Value::Nil);
    if map.is_nil() {
        return Ok(());
    }
    for (bytes, key) in keys {
        let sequence = Value::string(&String::from_utf8_lossy(bytes));
        let event = Value::vector([Value::symbol(key)]);
        crate::lisp::primitives::call(interpreter, "define-key", &[map, sequence, event], env)?;
    }
    Ok(())
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
    /// The same cell for any window showing its point: the window's own
    /// `w->cursor', which update_frame_1 uses once the window is selected.
    point_cursor: Option<(usize, usize)>,
    /// A display starting inside its first line (start_display): that
    /// line, the start, and whether the iterator walked there from the
    /// line's start (a continued line's start not after a newline).
    anchor: Option<Anchor>,
    /// Where the row showing point begins when it is that first,
    /// truncated row: hscroll_window_tree measures point from there.
    cursor_origin: Option<usize>,
}

/// A window display's first line laid out from a start inside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Anchor {
    line: usize,
    origin: usize,
    walked: bool,
    /// The walked display's first row shows its line's number: the
    /// start shows a string or ellipsis first, so start_display's
    /// RESTORE_IT took back the line number the walk produced, and the
    /// walk left no continuation_lines_width.
    numbered: bool,
}

/// The row a truncating window shows point on begins at START when the
/// display starts inside point's line, before point.
fn cursor_row_origin(buffer: &crate::buffer::Buffer, start: usize, point: usize) -> Option<usize> {
    let start = start.clamp(buffer.point_min(), buffer.point_max());
    (start > buffer.point_min()
        && buffer.char_at(start - 1) != Some('\n')
        && point >= start
        && buffer.line_start_at(point) == buffer.line_start_at(start))
    .then_some(start)
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
    /// The window's hscroll, auto-hscroll suspension and old point as
    /// hscroll_window_tree leaves them, stored once the window's display
    /// is planned.
    store: Option<(i64, bool, i64)>,
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
///
/// hscroll_window_tree measures point's x from the start of the row
/// showing it: CURSOR_ROW_START when the display starts inside point's
/// line, else the line's start.
fn window_render_geometry(
    interpreter: &mut Interpreter,
    env: &mut Env,
    info: &crate::lisp::primitives::WindowRenderInfo,
    body_width: usize,
    frame_cols: usize,
    lnum: Option<crate::lisp::primitives::LineNumberLayout>,
    cursor_row_start: Option<usize>,
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
            .lookup_var_key(cached_symbol!("truncate-partial-width-windows"), env)
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
    let current_line_only =
        !suspended && crate::lisp::types::eq_symbol_named(auto_mode_value, "current-line");
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
                store: None,
            };
        };
        let point_pos = info.point.clamp(buffer.point_min(), buffer.point_max());
        // In a buffer with very long lines, point more than
        // `large-hscroll-threshold' characters into its row is found by
        // fast_move_it_horizontally, each character taken as one column;
        // the row's end is as far as its newline.
        let row_start = cursor_row_start.unwrap_or_else(|| buffer.line_start_at(point_pos));
        let nchars = point_pos.saturating_sub(row_start);
        let threshold = interpreter
            .lookup_var_key(cached_symbol!("large-hscroll-threshold"), env)
            .and_then(|value| value.integer_value())
            .unwrap_or(0);
        let fast_move = buffer.long_line_optimizations_p() && nchars as i64 > threshold;
        let (point_dcol, line_w) = if fast_move {
            let zv = buffer.point_max();
            let row_end = buffer.find_before_next_newline(point_pos, zv);
            (nchars as i64, row_end.saturating_sub(row_start))
        } else {
            // hscroll_window_tree measures the cursor's row with the
            // display iterator: the visual line's laid-out columns,
            // prefix included.
            let spec = resolve_buffer_invisibility(interpreter, &buffer, info.buffer_id);
            let context = CharacterDisplayContext::new(interpreter, info.buffer_id, info.window_id)
                .with_line_numbers(lnum_cols);
            let point_line =
                visual_line_first_line(&buffer, &spec, buffer.line_number_at_pos(point_pos));
            let visual = glyphless_visual_line_at(&buffer, &spec, point_line, Some(&context));
            let index_of = |pos: usize| {
                visual
                    .map
                    .get(pos.saturating_sub(buffer.display_line_start_of(point_line)))
                    .copied()
                    .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0))
            };
            // init_to_row_start: from the start of point's row.
            let laid = match cursor_row_start {
                Some(origin) => layout::lay_out_visual_line_from(
                    &visual,
                    body_width,
                    true,
                    visual.start_index(
                        origin.saturating_sub(buffer.display_line_start_of(point_line)),
                        false,
                    ),
                    false,
                ),
                None => layout::lay_out_visual_line(&visual, body_width, true),
            };
            let point_index = index_of(point_pos);
            (
                laid.columns[point_index.min(laid.columns.len() - 1)] as i64,
                laid.columns.last().copied().unwrap_or(0),
            )
        };
        let w = body_width as i64;
        let margin = interpreter
            .lookup_var_key(cached_symbol!("hscroll-margin"), env)
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
            // fast_move_it_horizontally loads no display element, so the
            // iterator is never at the end of a line.
            let at_eol = !fast_move && buffer.char_at(point_pos) == Some('\n');
            // hscroll_window_tree's iterator produces the line-number
            // glyphs once a glyph reaches past first_visible_x (the
            // hscroll it runs under), so its current_x carries their
            // columns only when point lies right of that;
            // fast_move_it_horizontally produces none.
            let numbers_produced = !fast_move && (hscroll == 0 || point_dcol > hscroll);
            let current_x = point_dcol
                + if numbers_produced {
                    lnum_cols as i64
                } else {
                    0
                };
            let step = interpreter.lookup_var_key(cached_symbol!("hscroll-step"), env);
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
    RenderGeometry {
        truncate: truncate_lines || hscroll > 0 || partial_truncates,
        hscroll: hscroll.max(0) as usize,
        current_line_only,
        min_hscroll: state.min_hscroll.max(0) as usize,
        lnum,
        store: Some((hscroll, suspended, point)),
    }
}

/// The `display' `(space :align-to COL)' target of the property value,
/// if the value is such a space spec (xdisp.c's stretch glyphs;
/// completion--insert-strings builds its columns from them).
fn space_align_to_target(value: &Value) -> Option<usize> {
    let items = value.to_vec().ok()?;
    if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head.is_named("space"))
    {
        return None;
    }
    let position = items
        .iter()
        .position(|item| crate::lisp::types::eq_symbol_named(item, ":align-to"))?;
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
    if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head.is_named("space"))
    {
        return None;
    }
    if let Some(position) = items
        .iter()
        .position(|item| crate::lisp::types::eq_symbol_named(item, ":width"))
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
    if !crate::lisp::types::eq_symbol_named(tag, "margin") {
        return None;
    }
    let location = match location.cons_values() {
        Some((side, _)) => side,
        None => Value::Nil,
    };
    let location = match crate::lisp::types::eq_dispatch(location).kind() {
        Kind::Nil => None,
        Kind::Symbol(name) if name.is_named("left-margin") || name.is_named("right-margin") => {
            Some(name.to_string())
        }
        _ => return None,
    };
    let value = rest.cons_values().map_or(rest, |(value, _)| value);
    let valid = crate::lisp::primitives::string_text(&value).is_ok()
        || value
            .cons_values()
            .is_some_and(|(car, _)| crate::lisp::types::eq_symbol_named(car, "space"));
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
            Some(Kind::Symbol(name)) if name.is_named("left-fringe") || name.is_named("right-fringe")
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
            layout::LineWrap::Word { by_category: true } => interpreter
                .buffer_category_table_id(buffer_id)
                .or_else(|| interpreter.standard_category_table()),
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
                .find(|pair| matches!(pair[0].kind(), Kind::Symbol(name) if name.is_named(key)))
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
            // Redisplay reads a property whose lookup signals as nil.
            .ok()
            .flatten()
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
        if !matches!(items.first().map(|v| v.kind()), Some(Kind::Symbol(head)) if head.is_named("space"))
        {
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
                .find(|pair| matches!(pair[0].kind(), Kind::Symbol(name) if name.is_named(key)))
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
                    .buffer_local_toplevel_value(self.buffer_id, name.as_str())
                    .or_else(|| self.interpreter.default_toplevel_value(name.as_str()))
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
                    if name.is_named("+") || name.is_named("-") {
                        let mut pixels = 0.0;
                        let mut first = true;
                        let mut tail = cdr;
                        while let Kind::Cons(item) = tail.kind() {
                            let px = self.pixel_width(&item.car.get(), align_to.as_deref_mut())?;
                            if first {
                                pixels = if name.is_named("+") { px } else { -px };
                                first = false;
                            } else {
                                pixels += px;
                            }
                            tail = item.cdr.get();
                        }
                        return Some(if name.is_named("-") { -pixels } else { pixels });
                    }
                    car = self
                        .interpreter
                        .buffer_local_toplevel_value(self.buffer_id, name.as_str())
                        .or_else(|| self.interpreter.default_toplevel_value(name.as_str()))
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
        let method = match crate::lisp::types::eq_dispatch(value).kind() {
            // lookup_glyphless_char_display: no method (the usual entry).
            Kind::Nil if !no_font_fallback => return None,
            Kind::Symbol(name) if name.is_named("zero-width") && no_font_fallback => {
                GlyphlessDisplayMethod::EmptyBox
            }
            Kind::Symbol(name) if name.is_named("zero-width") => GlyphlessDisplayMethod::ZeroWidth,
            Kind::Symbol(name) if name.is_named("thin-space") => GlyphlessDisplayMethod::ThinSpace,
            Kind::Symbol(name) if name.is_named("empty-box") => GlyphlessDisplayMethod::EmptyBox,
            Kind::Symbol(name) if name.is_named("hex-code") => GlyphlessDisplayMethod::HexCode,
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
        let highlight = matches!(
            self.nobreak_char_display.kind(),
            crate::lisp::types::Kind::T
        );
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
    /// Raw offset ranges whose text a display string or an invisible
    /// run's ellipsis replaces.
    replaced: Vec<ReplacedRun>,
    /// The strings display glyphs come from (display and overlay
    /// strings, each with whether a `display' property gave it), and for
    /// each display index -- none at all when there are no such glyphs --
    /// which of them and the character's index in it.
    objects: Vec<(Value, bool)>,
    object_of: Vec<Option<(usize, usize)>>,
    /// Whether a `display' string replaces the newline that ends the
    /// line: the iterator pops the string past it, so no row shows it.
    newline_replaced: bool,
}

/// Record that the display indexes from START show the characters of
/// the string VALUE at INDEXES (none for the dots of an ellipsis), a
/// `display' property's string when DISPLAY (string_from_display_prop_p).
fn record_string_glyphs(
    objects: &mut Vec<(Value, bool)>,
    object_of: &mut Vec<Option<(usize, usize)>>,
    start: usize,
    (value, display): (Value, bool),
    indexes: impl IntoIterator<Item = Option<usize>>,
) {
    let object = objects.len();
    objects.push((value, display));
    object_of.resize(start, None);
    object_of.extend(
        indexes
            .into_iter()
            .map(|index| index.map(|index| (object, index))),
    );
}

impl VisualLine {
    /// The string the glyph at display INDEX comes from, and the index
    /// of its character there (make_lispy_position's OBJECT).
    fn object_at(&self, index: usize) -> Option<(Value, usize)> {
        let (object, character) = (*self.object_of.get(index)?)?;
        Some((self.objects[object].0, character))
    }

    /// Whether the glyph at display INDEX comes from a `display'
    /// property's string (string_from_display_prop_p), not an overlay's
    /// before- or after-string.
    fn object_from_display(&self, index: usize) -> bool {
        self.object_of
            .get(index)
            .copied()
            .flatten()
            .is_some_and(|(object, _)| self.objects[object].1)
    }

    /// The display index a window's display starting at raw offset
    /// OFFSET begins with: init_iterator's handle_stop there shows a
    /// display string or an ellipsis covering the start from its
    /// beginning, and the overlay strings at the start before its text.
    /// A start start_display WALKED to right after an invisible run
    /// shows its ellipsis too: the walk's move_it_to stops on the
    /// ellipsis's last glyph, whose position the start is, and goes
    /// back to its first (dpvec_index 0).
    fn start_index(&self, offset: usize, walked: bool) -> usize {
        let at = |raw: usize| {
            self.map
                .get(raw)
                .copied()
                .unwrap_or_else(|| self.map.last().copied().unwrap_or(0))
        };
        if let Some(&(start, _, _)) = self
            .replaced
            .iter()
            .find(|(start, end, _)| *start < offset && offset < *end)
        {
            return at(start);
        }
        if walked
            && let Some(&(start, _, _)) = self
                .replaced
                .iter()
                .find(|(start, end, string)| !*string && *start < offset && offset == *end)
        {
            return at(start);
        }
        let in_ellipsis = |index: usize| {
            self.ellipses
                .iter()
                .any(|&start| start <= index && index < start + 3)
        };
        let mut first = at(offset);
        while first > 0
            && self.raw_of_display.get(first - 1) == Some(&offset)
            && !in_ellipsis(first - 1)
        {
            first -= 1;
        }
        first
    }

    /// Whether init_iterator at a start at raw OFFSET delivers other
    /// than the buffer's text first (it->method != GET_FROM_BUFFER): the
    /// overlay strings there, a display string or an ellipsis covering
    /// it.
    fn starts_off_buffer(&self, offset: usize) -> bool {
        let first = self.start_index(offset, false);
        self.object_of.get(first).copied().flatten().is_some() || self.ellipses.contains(&first)
    }

    /// Where start_display's walk to a start the display shows from
    /// display index FROM stops: its move_it_to the start's buffer
    /// position goes through the strings shown there first, to the text
    /// after them.  Only the continuation_lines_width it reaches there
    /// survives the RESTORE_IT to the start (a newline in those strings
    /// resets it).  A start showing no string stops at FROM itself.
    fn walk_stop_index(&self, from: usize) -> usize {
        let mut index = from;
        while self.object_of.get(index).copied().flatten().is_some() {
            index += 1;
        }
        index
    }
}

/// Render an overlay before/after string under the window buffer's
/// invisibility spec, retaining a raw-string to displayed-string boundary
/// map so its face spans follow the surviving cells.  These strings are
/// separate xdisp objects: their own `invisible' text property governs each
/// character even though the property is not present in the buffer text.
fn visible_overlay_string(
    value: &Value,
    spec: &InvisibilitySpec,
) -> (String, Vec<FaceSpan>, Vec<Option<usize>>) {
    let raw = crate::lisp::primitives::string_text(value).unwrap_or_default();
    let characters: Vec<char> = raw.chars().collect();
    let mut rendered = String::new();
    let mut map = Vec::with_capacity(characters.len() + 1);
    let mut indexes = Vec::with_capacity(characters.len());
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
                indexes.push(Some(position));
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
                indexes.extend([None; 3]);
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
    (rendered, spans, indexes)
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
        let DisplayedLine {
            text,
            map: display_map,
            replaced,
            objects,
            object_of,
        } = displayed_line_with_runs(buffer, first_line);
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
        let line_begin = buffer.display_line_start_of(first_line);
        let newline_replaced =
            newline_replaced_at(buffer, line_begin + map.len().saturating_sub(1));
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
            replaced,
            objects,
            object_of,
            newline_replaced,
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
        let has_before_string = overlay
            .get_prop(&Value::Symbol("before-string".into()))
            .is_some_and(|value| {
                window_margin_display(&value).is_none()
                    && crate::lisp::primitives::string_text(&value).is_ok()
            });
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
            let priority = overlay
                .get_prop(&Value::Symbol("priority".into()))
                .and_then(|value| value.integer_value())
                .unwrap_or(0);
            // load_overlay_strings' compare_overlay_entries: at one
            // position the after-strings come first, by decreasing
            // priority, then the before-strings by increasing priority;
            // an empty overlay's own after-string follows its
            // before-string.
            let own_before = has_before_string && overlay.beg() == overlay.end();
            let (group, rank) = if after && !own_before {
                (0u8, -priority)
            } else {
                (1u8, priority)
            };
            overlay_strings.push((
                if after { overlay.end() } else { overlay.beg() },
                after,
                (group, rank, overlay.identity()),
                value,
            ));
        }
    }
    overlay_strings.sort_by_key(|(position, after, (group, rank, id), _)| {
        (*position, *group, *rank, *id, *after)
    });
    let mut overlay_string_index = 0usize;
    let mut string_run: Option<String> = None;
    let mut space_run: Option<Value> = None;
    let mut replaced: Vec<ReplacedRun> = Vec::new();
    let mut objects: Vec<(Value, bool)> = Vec::new();
    let mut object_of: Vec<Option<(usize, usize)>> = Vec::new();
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
            let (string, spans, indexes) = visible_overlay_string(value, spec);
            let start = display_chars;
            record_string_glyphs(
                &mut objects,
                &mut object_of,
                start,
                (*value, false),
                indexes,
            );
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
            let offset = pos - line_begin;
            match replaced.last_mut() {
                Some((_, end, true)) if pos != (*beg).max(line_begin) && *end == offset => {
                    *end = offset + 1
                }
                _ => replaced.push((offset, offset + 1, true)),
            }
            if pos == (*beg).max(line_begin) {
                let replacement = crate::lisp::primitives::string_text(value).unwrap_or_default();
                let start = display_chars;
                record_string_glyphs(
                    &mut objects,
                    &mut object_of,
                    start,
                    (*value, true),
                    (0..replacement.chars().count()).map(Some),
                );
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
                replaced.push((pos - line_begin, run_end - line_begin, false));
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
            match replaced.last_mut() {
                Some((_, end, true)) if same_run && *end == offset => *end = offset + 1,
                _ => replaced.push((offset, offset + 1, true)),
            }
            if !same_run {
                if let Some(value) = display {
                    record_string_glyphs(
                        &mut objects,
                        &mut object_of,
                        display_chars,
                        (value, true),
                        (0..replacement.chars().count()).map(Some),
                    );
                }
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
    while let Some((position, _, _, value)) = overlay_strings.get(overlay_string_index) {
        if *position != pos {
            break;
        }
        let (string, spans, indexes) = visible_overlay_string(value, spec);
        let start = display_chars;
        record_string_glyphs(
            &mut objects,
            &mut object_of,
            start,
            (*value, false),
            indexes,
        );
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
    raw_of_display.push(pos - line_begin);
    if !objects.is_empty() {
        object_of.resize(display_chars, None);
    }
    let newline_replaced = newline_replaced_at(buffer, pos);
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
        replaced,
        objects,
        object_of,
        newline_replaced,
    }
}

/// Whether the character at POS is a newline that a `display' text
/// property's string replaces in the text area.
fn newline_replaced_at(buffer: &crate::buffer::Buffer, pos: usize) -> bool {
    if buffer.char_at(pos) != Some('\n') || !buffer.has_text_property_named("display") {
        return false;
    }
    let display = buffer.text_property_at(pos, "display");
    let display = match display.as_ref().and_then(margin_display_spec) {
        Some((None, value)) => Some(value),
        Some((Some(_), _)) => None,
        None => display,
    };
    display
        .as_ref()
        .is_some_and(|value| crate::lisp::primitives::string_text(value).is_ok())
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
    if !visual.object_of.is_empty() {
        let mut object_of = Vec::with_capacity(display.offset(usize::MAX));
        for (index, offsets) in display.offsets.windows(2).enumerate() {
            let object = visual.object_of.get(index).copied().flatten();
            object_of.extend(std::iter::repeat_n(object, offsets[1] - offsets[0]));
        }
        visual.object_of = object_of;
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
#[derive(Clone)]
pub(crate) struct ScreenLineLayout {
    /// Buffer position where each row begins.
    pub(crate) row_starts: Vec<usize>,
    /// Whether the line truncates.
    pub(crate) truncate: bool,
    bol: usize,
    /// (row, column) of each raw offset from BOL, one past the end
    /// included.
    cells: Vec<(usize, usize)>,
    /// Each glyph in display order: its row, its column there (a
    /// truncated line's from the line's start), and the raw offset from
    /// BOL it displays.
    glyphs: Vec<(usize, usize, usize)>,
    /// The column where each row's glyphs end: a continued row's edge,
    /// the last row's newline.
    row_ends: Vec<usize>,
    /// The cells each row's `line-prefix' or `wrap-prefix' takes.
    row_prefixes: Vec<usize>,
    /// Raw offset ranges from BOL whose text a display string replaces.
    strings: Vec<(usize, usize)>,
    /// Raw offset ranges from BOL of invisible runs an ellipsis shows.
    ellipses: Vec<(usize, usize)>,
    /// The string each glyph comes from, and its character's index.
    glyph_objects: Vec<Option<(Value, usize)>>,
    /// Whether that string is a `display' property's.
    glyph_display: Vec<bool>,
    /// The `line-prefix' or `wrap-prefix' string each row begins with.
    row_prefix_objects: Vec<Option<Value>>,
    /// Whether `word-wrap' broke each row (at a word boundary, before the
    /// window's edge) rather than the edge or the line's end.
    row_word_wrapped: Vec<bool>,
    /// Whether the line wraps at word boundaries (it->line_wrap ==
    /// WORD_WRAP: `word-wrap' in a window that does not truncate).
    word_wrap: bool,
    /// Whether the line ends at ZV rather than at a newline.
    ends_at_zv: bool,
    /// Whether a `display' string replaces the newline ending the line.
    newline_replaced: bool,
    /// Buffer positions the iterator passes without fetching: past the
    /// start of an invisible run or of text a display string replaces
    /// (handle_stop at the start skips to its end).
    skipped: Vec<(usize, usize)>,
    /// One past the last buffer position a walk of each row to its end
    /// fetches (move_it_in_display_line_to with no target): a continued
    /// row's next row's first element, which did not fit; a row
    /// `word-wrap' broke, the element past the wrap point that did not
    /// fit; the line's newline.  A truncated row's walk goes on through
    /// its newline (forward_to_next_line_start), no more than
    /// MAX_NEWLINE_DISTANCE elements past the window's edge.
    row_walk_ends: Vec<usize>,
    /// Buffer positions `word-wrap' may break a row before.
    wrap_points: Vec<usize>,
}

impl ScreenLineLayout {
    /// Whether the line wraps at word boundaries (it->line_wrap ==
    /// WORD_WRAP).
    pub(crate) fn word_wrap(&self) -> bool {
        self.word_wrap
    }

    /// Where the line ends: its newline, or ZV.
    pub(crate) fn line_end(&self) -> usize {
        self.bol + self.cells.len() - 1
    }

    /// Whether ROW begins inside a string, past its first character:
    /// an iterator move_it_to leaves at the row's start is then at no
    /// valid buffer position (!IT_POS_VALID_AFTER_MOVE_P).  A row that
    /// begins a string, even one following another string at the same
    /// position, does not.
    pub(crate) fn row_begins_inside_string(&self, row: usize) -> bool {
        self.row_first_string(row)
            .is_some_and(|(_, index)| index > 0)
    }

    /// The string ROW's first glyph comes from, if any: whether it is a
    /// `display' property's (string_from_display_prop_p), and the
    /// glyph's character index in it.
    pub(crate) fn row_first_string(&self, row: usize) -> Option<(bool, usize)> {
        let glyph = self
            .glyphs
            .iter()
            .position(|&(glyph_row, ..)| glyph_row == row)?;
        let (_, index) = self.glyph_objects.get(glyph).copied().flatten()?;
        Some((
            self.glyph_display.get(glyph).copied().unwrap_or(false),
            index,
        ))
    }

    /// pos_visible_p's it3 for POS inside text that the `display' string
    /// at raw offsets [RUN, END) replaces: from the start of the row the
    /// string's display begins on (where the element after RUN - 1 is),
    /// x 0, it delivers the row's elements -- its prefix's first, at the
    /// row's position -- going on past the row's end without continuing
    /// it, until the first whose IT_CHARPOS is at or past POS (a string's
    /// glyphs have the position the iterator met it at) or a newline; one
    /// in the string after it moved, short of POS, takes the x of the
    /// last element not from the string (top_x_before_string).
    fn it3_cell(&self, pos: usize, run: usize, end: usize) -> (usize, usize) {
        let row = self
            .glyphs
            .iter()
            .find(|&&(_, _, raw)| raw >= run)
            .map_or_else(|| self.position_cell(self.bol + run).0, |&(row, ..)| row);
        let from_string = |glyph: usize| {
            let raw = self.glyphs[glyph].2;
            self.glyph_display.get(glyph).copied().unwrap_or(false)
                && raw >= run
                && raw < end.max(run + 1)
        };
        let newline_glyph = |glyph: usize| {
            self.glyph_objects
                .get(glyph)
                .copied()
                .flatten()
                .and_then(|(string, index)| {
                    crate::lisp::primitives::string_text(&string)
                        .ok()
                        .map(|text| text.chars().nth(index) == Some('\n'))
                })
                .unwrap_or(self.bol + self.glyphs[glyph].2 >= self.line_end())
        };
        let newline_in_string = (0..self.glyphs.len())
            .find(|&glyph| from_string(glyph))
            .and_then(|glyph| self.glyph_objects.get(glyph).copied().flatten())
            .and_then(|(string, _)| crate::lisp::primitives::string_text(&string).ok())
            .is_some_and(|text| text.contains('\n'));
        let mut x = 0usize;
        let mut before_string = 0usize;
        // The row's prefix glyphs, at the row's start position.
        let row_start = self.row_starts.get(row).copied().unwrap_or(self.bol);
        for _ in 0..self.row_prefixes.get(row).copied().unwrap_or(0) {
            before_string = x;
            if row_start >= pos {
                return (row, x);
            }
            x += 1;
        }
        let first = self
            .glyphs
            .iter()
            .position(|&(glyph_row, ..)| glyph_row == row)
            .unwrap_or(self.glyphs.len());
        let mut moved = x > 0;
        let mut glyph = first;
        while glyph < self.glyphs.len() {
            let (glyph_row, column, raw) = self.glyphs[glyph];
            // Characters drawn in one cell come as one element.
            let mut next = glyph + 1;
            while next < self.glyphs.len()
                && self.glyphs[next].0 == glyph_row
                && self.glyphs[next].1 == column
            {
                next += 1;
            }
            let string = from_string(glyph);
            if !string {
                before_string = x;
            }
            let at_eol = newline_glyph(glyph);
            if self.bol + raw >= pos || at_eol {
                if moved && at_eol && string && newline_in_string && self.bol + raw != pos {
                    return (row, before_string);
                }
                return (row, x);
            }
            moved = true;
            x += match self.glyphs.get(next) {
                Some(&(next_row, next_column, _)) if next_row == glyph_row => next_column - column,
                _ => self
                    .row_ends
                    .get(glyph_row)
                    .copied()
                    .unwrap_or(column)
                    .saturating_sub(column)
                    .max(1),
            };
            glyph = next;
        }
        (row, x)
    }

    /// The row and column where pos_visible_p places POS in a display
    /// starting at START: a position in text that a display string
    /// replaces where it3 finds it (it3_cell); one past an invisible
    /// run's start, up to the text after it, on its ellipsis's last dot,
    /// where move_it_to stops in a display vector (the start itself,
    /// which the display begins with, on its first); others where they
    /// are.
    pub(crate) fn shown_cell(&self, pos: usize, start: usize) -> (usize, usize) {
        let offset = pos.saturating_sub(self.bol);
        let line_end = self.line_end() - self.bol;
        // The text a string replaces (its start too: move_it_to stops
        // only after the string), the line's newline included when the
        // string replaces that too.
        if let Some(&(run, end)) = self.strings.iter().find(|(run, end)| {
            *run <= offset
                && (offset < *end || (self.newline_replaced && offset == *end && *end == line_end))
        }) {
            return self.it3_cell(pos, run, end);
        }
        if let Some(&(run, _)) = self
            .ellipses
            .iter()
            .find(|(run, end)| *run < offset && offset <= *end)
        {
            let (row, column) = self.position_cell(self.bol + run);
            return if pos == start {
                (row, column)
            } else {
                (row, column + 2)
            };
        }
        self.position_cell(pos)
    }

    /// The row and column of this line where move_it_to POS (MOVE_TO_POS)
    /// stops when a `display' string replaces POS's text: the iterator's
    /// object is the string while it delivers its glyphs, so it stops
    /// only where the string ends (replaced_end), at the first element
    /// there (stop_cell), or at the next line's first row, column 0,
    /// when the string replaces the line's newline -- the number of the
    /// line's rows.  None when no string replaces POS's text.
    pub(crate) fn replaced_stop(&self, pos: usize) -> Option<(usize, usize)> {
        let end = self.replaced_end(pos);
        (end > pos).then(|| {
            if end > self.line_end() {
                (self.row_starts.len(), 0)
            } else {
                self.stop_cell(end)
            }
        })
    }

    /// Where move_it_to POS stops for a `display' string replacing POS's
    /// text: where the string ends -- pop_it's iterate_out_of_display_
    /// property leaves IT_CHARPOS there, and the check at the top of
    /// move_it_in_display_line_to's loop stops the move before handle_stop
    /// loads whatever comes next, another string included -- past the
    /// line's end when the string replaces its newline.  At a string's
    /// own start right after another string that ends part-way along a
    /// row, the iterator coming out of that one stops there already (its
    /// prev_method GET_FROM_STRING, IT_CHARPOS at POS); one ending in a
    /// newline ended the row, and the move's next row starts afresh into
    /// the string at POS.  POS itself when no string replaces its text.
    pub(crate) fn replaced_end(&self, pos: usize) -> usize {
        let Some(offset) = pos.checked_sub(self.bol) else {
            return pos;
        };
        match self.covering_run(offset) {
            Some((run, _))
                if run == offset
                    && self
                        .strings
                        .iter()
                        .any(|&(before, end)| end == run && !self.run_string_ends_row(before)) =>
            {
                pos
            }
            Some(_) => self.string_end(pos),
            None => pos,
        }
    }

    /// Where the `display' string replacing POS's text ends, past the
    /// line's end when it replaces the newline: where an iterator
    /// delivering it comes out of it.  POS when none replaces its text.
    pub(crate) fn string_end(&self, pos: usize) -> usize {
        let Some(offset) = pos.checked_sub(self.bol) else {
            return pos;
        };
        let line_end = self.line_end() - self.bol;
        match self.covering_run(offset) {
            Some((_, end)) if self.newline_replaced && end == line_end => self.line_end() + 1,
            Some((_, end)) => self.bol + end,
            None => pos,
        }
    }

    /// The raw offsets [RUN, END) of the `display' string replacing the
    /// text at raw OFFSET, the line's newline included when a string
    /// replaces that.
    fn covering_run(&self, offset: usize) -> Option<(usize, usize)> {
        let line_end = self.line_end() - self.bol;
        self.strings
            .iter()
            .find(|&&(run, end)| {
                run <= offset
                    && (offset < end || (self.newline_replaced && offset == end && end == line_end))
            })
            .copied()
    }

    /// Whether the `display' string replacing the text from raw offset
    /// RUN ends in a newline, which ends its row.
    fn run_string_ends_row(&self, run: usize) -> bool {
        (0..self.glyphs.len())
            .find(|&glyph| {
                self.glyphs[glyph].2 == run
                    && self.glyph_display.get(glyph).copied().unwrap_or(false)
                    && self.glyph_objects.get(glyph).copied().flatten().is_some()
            })
            .and_then(|glyph| self.glyph_objects[glyph])
            .and_then(|(string, _)| crate::lisp::primitives::string_text(&string).ok())
            .is_some_and(|text| text.ends_with('\n'))
    }

    /// Where the iterator stands when a move stops at END in the line's
    /// text: the first element at or past END -- an overlay string there
    /// comes after the stop (handle_stop has not loaded it) -- else END's
    /// own cell.
    fn stop_cell(&self, end: usize) -> (usize, usize) {
        let offset = end.saturating_sub(self.bol);
        self.glyphs
            .iter()
            .find(|&&(_, _, raw)| raw >= offset)
            .map_or_else(
                || self.position_cell(end),
                |&(row, column, _)| (row, column),
            )
    }

    /// move_it_in_display_line to x TO_X on ROW (buffer_posn_from_
    /// coords), each glyph at the x GLYPH_X gives its column: the
    /// iterator stops at the first glyph reaching past TO_X -- inside a
    /// TAB's or a wide character's glyphs at the one under TO_X -- or, on
    /// a truncated row, at the first element starting at or past
    /// LAST_VISIBLE_X (MOVE_LINE_TRUNCATED), or past the row's glyphs
    /// where the row ends: on a row `word-wrap' broke, its last glyph; on
    /// a continued row, the next row's first element, as many of whose
    /// glyphs as fit counted on this row; else the line's end.
    pub(crate) fn glyph_at_x(
        &self,
        row: usize,
        to_x: usize,
        last_visible_x: usize,
        glyph_x: impl Fn(usize) -> usize,
    ) -> GlyphAt {
        let row_end = self.row_ends.get(row).copied().unwrap_or(0);
        // The row's glyphs, characters drawn in one cell (a composition)
        // found at the first of them, with the column each one's cells
        // end at.
        let mut cells: Vec<(usize, usize, usize)> = Vec::new();
        for (glyph, &(glyph_row, column, raw)) in self.glyphs.iter().enumerate() {
            if glyph_row != row {
                if glyph_row > row && !cells.is_empty() {
                    break;
                }
                continue;
            }
            if cells.last().is_none_or(|&(_, at, _)| column > at) {
                cells.push((raw, column, glyph));
            }
        }
        let row_start = self.row_starts[row.min(self.row_starts.len() - 1)];
        // A prefix's glyphs (handle_line_prefix) come first, from a
        // string displayed at the row's start.
        let prefix = self.row_prefixes.get(row).copied().unwrap_or(0);
        for column in 0..prefix {
            let x = glyph_x(column);
            if x + 1 > to_x {
                let object = self
                    .row_prefix_objects
                    .get(row)
                    .copied()
                    .flatten()
                    .map(|string| {
                        let length = crate::lisp::primitives::string_text(&string)
                            .map_or(1, |text| text.chars().count().max(1));
                        (string, column.min(length - 1))
                    });
                return GlyphAt {
                    position: row_start,
                    x,
                    width: 1,
                    object,
                    display: false,
                    continued_at: None,
                };
            }
        }
        let width_at = |index: usize| {
            let end = cells.get(index + 1).map_or(row_end, |&(_, next, _)| next);
            end.saturating_sub(cells[index].1).max(1)
        };
        for (index, &(raw, column, glyph)) in cells.iter().enumerate() {
            let x = glyph_x(column);
            let width = width_at(index);
            if x + width > to_x || (self.truncate && x >= last_visible_x) {
                return GlyphAt {
                    position: self.bol + raw,
                    x: x.max(to_x),
                    width,
                    object: self.glyph_objects.get(glyph).copied().flatten(),
                    display: self.glyph_display.get(glyph).copied().unwrap_or(false),
                    continued_at: None,
                };
            }
        }
        if !self.truncate
            && self.row_word_wrapped.get(row).copied().unwrap_or(false)
            && let Some(&(raw, column, glyph)) = cells.last()
        {
            return GlyphAt {
                position: self.bol + raw,
                x: glyph_x(column),
                width: width_at(cells.len() - 1),
                object: self.glyph_objects.get(glyph).copied().flatten(),
                display: self.glyph_display.get(glyph).copied().unwrap_or(false),
                continued_at: None,
            };
        }
        match self.row_starts.get(row + 1) {
            Some(next) if !self.truncate => {
                let first = self
                    .glyphs
                    .iter()
                    .position(|&(glyph_row, ..)| glyph_row == row + 1);
                GlyphAt {
                    position: *next,
                    x: glyph_x(row_end).max(to_x),
                    width: 1,
                    object: first
                        .and_then(|glyph| self.glyph_objects.get(glyph).copied().flatten()),
                    display: first
                        .and_then(|glyph| self.glyph_display.get(glyph).copied())
                        .unwrap_or(false),
                    // The element that does not fit reaches TO_X first
                    // (MOVE_X_REACHED) when its glyphs would end past it.
                    continued_at: {
                        let width = first
                            .and_then(|glyph| {
                                let (_, column, _) = self.glyphs[glyph];
                                self.glyphs[glyph + 1..]
                                    .iter()
                                    .find(|&&(glyph_row, next, _)| {
                                        glyph_row != row + 1 || next > column
                                    })
                                    .filter(|&&(glyph_row, ..)| glyph_row == row + 1)
                                    .map(|&(_, next, _)| next - column)
                            })
                            .unwrap_or(1);
                        (glyph_x(row_end) + width <= to_x).then(|| glyph_x(row_end))
                    },
                }
            }
            _ => GlyphAt {
                position: self.line_end(),
                x: glyph_x(row_end),
                width: usize::from(self.ends_at_zv),
                object: None,
                display: false,
                continued_at: None,
            },
        }
    }

    /// The row showing POS and POS's screen column on it.
    pub(crate) fn position_cell(&self, pos: usize) -> (usize, usize) {
        let offset = pos.saturating_sub(self.bol).min(self.cells.len() - 1);
        self.cells[offset]
    }
}

/// Where move_it_in_display_line stopped on a row: the position, the
/// iterator's x, the pixel_width of the element there (a newline's 0)
/// and the string the glyph comes from with its character's index.
#[derive(Clone, Copy, Debug)]
pub(crate) struct GlyphAt {
    pub(crate) position: usize,
    pub(crate) x: usize,
    pub(crate) width: usize,
    pub(crate) object: Option<(Value, usize)>,
    /// Whether that string is a `display' property's
    /// (string_from_display_prop_p).
    pub(crate) display: bool,
    /// Where the row's glyphs end when the move went past them on a
    /// continued row (MOVE_LINE_CONTINUED): the iterator's x there, at
    /// the next row's first element, which did not fit and ends short of
    /// TO_X.
    pub(crate) continued_at: Option<usize>,
}

/// How WINDOW lays its text out: the `display-line-numbers' columns,
/// the text's width right of them, whether lines truncate, the
/// horizontal scroll, the iterator's first_visible_x (init_iterator:
/// the min_hscroll when only the current line hscrolls) and, where
/// `auto-hscroll-mode' is `current-line' in a truncated window, the
/// hscroll buffer_posn_from_coords adds on the row showing point (the
/// window's hscroll less its min_hscroll).
#[derive(Clone, Copy, Debug)]
pub(crate) struct WindowTextGeometry {
    pub(crate) lnum_cols: usize,
    pub(crate) text_width: usize,
    pub(crate) truncate: bool,
    pub(crate) hscroll: usize,
    pub(crate) first_visible: usize,
    pub(crate) point_row_hscroll: Option<usize>,
}

impl WindowTextGeometry {
    /// The iterator's x of a row's glyph at COLUMN of its line.
    pub(crate) fn glyph_x(self, column: usize) -> usize {
        if self.truncate {
            self.truncated_x(column)
        } else {
            self.lnum_cols + column
        }
    }

    /// The iterator's x of a truncated row's glyph at COLUMN of its
    /// line: glyphs left of first_visible_x keep their columns; the
    /// line-number glyphs come once a glyph reaches past it
    /// (move_it_in_display_line_to's line_number_pending), so glyphs
    /// right of that count them.
    fn truncated_x(self, column: usize) -> usize {
        if self.first_visible == 0 || column > self.first_visible {
            column + self.lnum_cols
        } else {
            column
        }
    }
}

/// Where pos_visible_p puts a position: its x and y, and RBOT, the part
/// of its row below the window's last (pos_visible_p's *rbot, in the
/// terminal's one-pixel rows): not 0 only for a position the iterator
/// reaches on a row below the window, which is partly visible.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VisibleAt {
    pub(crate) x: i64,
    pub(crate) y: usize,
    pub(crate) rbot: usize,
}

impl VisibleAt {
    fn full(x: i64, y: usize) -> Self {
        VisibleAt { x, y, rbot: 0 }
    }
}

/// pos_visible_p on a terminal: start_display at START in WINDOW, which
/// shows the current buffer, then move_it_to POS: POS's x -- the
/// line-number glyphs counted, the hscroll taken off, a position past a
/// truncated row's edge at the edge -- and its row, when POS shows in
/// TEXT_ROWS rows.
pub(crate) fn window_position_xy(
    interpreter: &Interpreter,
    window_id: u64,
    start: usize,
    pos: usize,
    geometry: WindowTextGeometry,
    text_rows: usize,
) -> Option<VisibleAt> {
    window_cell_xy(
        interpreter,
        window_id,
        start,
        pos,
        geometry,
        text_rows,
        false,
    )
}

/// pos_visible_p with charpos -1 (POS t) when the window shows ZV:
/// move_it_to the last visible y, its x 0 on each row.  On a row above
/// the last it stops at ZV past every glyph ZV's row produces, an
/// ellipsis or an after-string at ZV among them, where
/// `window_position_xy' places ZV itself; on the last row at its first
/// glyph.  A truncated row reaching its edge before ZV goes on to the
/// next line (MOVE_LINE_TRUNCATED), which begins at ZV.  ZV beginning
/// its row stops the iterator before the row's line number is produced.
/// The hscroll comes off either x.
pub(crate) fn window_end_xy(
    interpreter: &Interpreter,
    window_id: u64,
    start: usize,
    geometry: WindowTextGeometry,
    text_rows: usize,
) -> Option<(i64, usize)> {
    let zv = interpreter.buffer.borrow().point_max();
    window_cell_xy(interpreter, window_id, start, zv, geometry, text_rows, true)
        .map(|at| (at.x, at.y))
}

/// Fwindow_end's walk over WINDOW, which shows the current buffer:
/// start_display at START, move_it_vertically down its TEXT_ROWS, which
/// stops at x 0 of the row below the window, and move_it_past_eol when
/// the text ended above it -- where that row begins (a string's position
/// for a row inside one), or ZV.
pub(crate) fn window_end_walk(
    interpreter: &Interpreter,
    window_id: u64,
    start: usize,
    geometry: WindowTextGeometry,
    text_rows: usize,
) -> usize {
    let layouter = ScreenLineLayouter::new(interpreter, window_id, geometry.lnum_cols);
    let zv = interpreter.buffer.borrow().point_max();
    let mut layout = layouter.layout_from(start, geometry.text_width, geometry.truncate);
    let mut y = 0usize;
    loop {
        if let Some(&row_start) = layout.row_starts.get(text_rows - y) {
            return row_start;
        }
        y += layout.row_starts.len();
        if layout.line_end() >= zv {
            return zv;
        }
        layout = layouter.layout(
            layout.line_end() + 1,
            geometry.text_width,
            geometry.truncate,
        );
    }
}

/// What Fwindow_end's walk fetches: start_display at START, then
/// move_it_to ZV no lower than the row below the window's TEXT_ROWS,
/// each row on the way to its end and that row's first element.
pub(crate) fn fetch_window_end(
    interpreter: &Interpreter,
    window_id: u64,
    fetches: &mut IteratorFetches,
    geometry: WindowTextGeometry,
    start: usize,
    text_rows: usize,
) {
    fetch_start_display(interpreter, window_id, fetches, start);
    let zv = interpreter.buffer.borrow().point_max();
    let mut walk = IteratorWalk::from_window_start(interpreter, window_id, geometry, start);
    walk.walk_to_within(fetches, start, zv, text_rows);
}

/// Fwindow_end's test for an up-to-date answer: none while
/// windows_or_buffers_changed, the window's end is not valid, its
/// buffer's restriction changed or prevents redisplay optimizations, or
/// the window is outdated.
pub(crate) fn window_end_outdated(
    interpreter: &Interpreter,
    window_id: u64,
    buffer_id: u64,
) -> bool {
    interpreter.windows_or_buffers_changed != 0
        || !crate::lisp::primitives::window_flag(
            interpreter,
            window_id,
            crate::lisp::primitives::WINDOW_END_VALID_SLOT,
        )
        || interpreter
            .get_buffer_by_id(buffer_id)
            .is_none_or(|buffer| {
                buffer.clip_changed() || buffer.prevent_redisplay_optimizations_p()
            })
        || redisplay_flags::window_outdated(interpreter, window_id)
}

fn window_cell_xy(
    interpreter: &Interpreter,
    window_id: u64,
    start: usize,
    pos: usize,
    geometry: WindowTextGeometry,
    text_rows: usize,
    window_end: bool,
) -> Option<VisibleAt> {
    let layouter = ScreenLineLayouter::new(interpreter, window_id, geometry.lnum_cols);
    let zv = interpreter.buffer.borrow().point_max();
    let mut layout = layouter.layout_from(start, geometry.text_width, geometry.truncate);
    let mut y = 0usize;
    loop {
        if pos <= layout.line_end() {
            let (row, column) = layout.shown_cell(pos, start);
            let column = if window_end {
                layout
                    .row_ends
                    .get(row)
                    .copied()
                    .unwrap_or(column)
                    .max(column)
            } else {
                column
            };
            let line_y = y;
            let y = y + row;
            // Past a `display' string replacing POS's text move_it_to
            // stops only after the string.
            let replaced_stop = layout.replaced_stop(pos);
            let stop_y = line_y + replaced_stop.map_or(row, |(stop, _)| stop.max(row));
            // Its MOVE_TO_Y stops at the last row's start, x 0, when that
            // row comes before POS is reached: below the window, or on the
            // last row past its start, the element after the string.
            if stop_y >= text_rows
                || (!window_end && replaced_stop.is_some() && stop_y + 1 == text_rows)
            {
                // Still short of POS's glyph or the element after the
                // string covering it: a row of a string at POS has its
                // position there, IT_CHARPOS >= POS, so POS is visible.
                // At POS itself, with the line number not yet produced,
                // pos_visible_p adds its width once x is at or past
                // first_visible_x.
                let last = text_rows - 1;
                let row_start = layout.row_starts.get(last - line_y).copied()?;
                if window_end {
                    return None;
                }
                if row_start >= pos {
                    let lnum = if row_start == pos && geometry.first_visible == 0 {
                        geometry.lnum_cols as i64
                    } else {
                        0
                    };
                    return Some(VisibleAt::full(lnum - geometry.hscroll as i64, last));
                }
                // Short of POS, the last row partly visible: off a
                // newline move_it_by_lines 1 from the row's start lands on
                // the row below, going on to the text after a string it
                // lands inside (!IT_POS_VALID_AFTER_MOVE_P); POS before
                // that is visible where move_it_to reaches it with no
                // limit, where the string replacing its text ends, on the
                // last row or one below the window.
                let (stop, column) = replaced_stop?;
                let below = last - line_y + 1;
                let landed = match layout.row_starts.get(below) {
                    None => layout.line_end() + 1,
                    Some(&below_start) if layout.row_begins_inside_string(below) => {
                        layout.string_end(below_start)
                    }
                    Some(&below_start) => below_start,
                };
                if interpreter.buffer.borrow().char_at(row_start) == Some('\n') || pos >= landed {
                    return None;
                }
                // At a row's start move_it_to stops before the row's line
                // number and glyphs are produced: the row has no height.
                let x = if column == 0 {
                    0
                } else {
                    (geometry.lnum_cols + column) as i64
                };
                let y = line_y + stop;
                return Some(VisibleAt {
                    x: x - geometry.hscroll as i64,
                    y,
                    rbot: (y + usize::from(column > 0)).saturating_sub(text_rows),
                });
            }
            if window_end && y + 1 == text_rows {
                return Some(VisibleAt::full(-(geometry.hscroll as i64), y));
            }
            // The start itself, after start_display walked to it from
            // its line's start: the walk produced the line's number on
            // the row it stopped in (line_number_produced_p), so
            // pos_visible_p adds no line-number glyphs before it --
            // unless a prefix's glyphs come first, before which the line
            // number is produced, or the walk stopped at the start of a
            // row, its move_it_to having ended the row before (which
            // resets line_number_produced_p).
            if pos == start
                && y == 0
                && column == 0
                && !geometry.truncate
                && start > interpreter.buffer.borrow().point_min()
                && interpreter.buffer.borrow().char_at(start - 1) != Some('\n')
                && !layouter
                    .layout(start, geometry.text_width, false)
                    .row_starts
                    .contains(&start)
            {
                return Some(VisibleAt::full(column as i64, 0));
            }
            let x = if geometry.truncate {
                // last_visible_x: the text area less the truncation glyph.
                let last_visible_x =
                    geometry.first_visible + geometry.lnum_cols + geometry.text_width - 1;
                // The line-number glyphs not yet produced count once POS
                // is at or past first_visible_x; the window's hscroll
                // comes off the iterator's x.
                // ZV beginning its row, nothing produced on it: no
                // line-number glyphs before it.
                let x = if column >= geometry.first_visible && !(window_end && column == 0) {
                    column + geometry.lnum_cols
                } else {
                    column
                };
                if window_end && x > last_visible_x {
                    return Some(VisibleAt::full(-(geometry.hscroll as i64), y + 1));
                }
                x.min(last_visible_x) as i64 - geometry.hscroll as i64
            } else if window_end && column == 0 {
                0
            } else {
                (geometry.lnum_cols + column) as i64
            };
            return Some(VisibleAt::full(x, y));
        }
        y += layout.row_starts.len();
        if y >= text_rows || layout.line_end() >= zv {
            return None;
        }
        layout = layouter.layout(
            layout.line_end() + 1,
            geometry.text_width,
            geometry.truncate,
        );
    }
}

/// Where buffer_posn_from_coords leaves the iterator: the position,
/// its x less the hscroll (the posn's DX is from there), make_lispy_
/// position's COL (it.hpos, a glyph a cell, plus a column for each cell
/// TO_X lies past the element's end) and the row (vpos) reached.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WindowPositionAt {
    pub(crate) position: usize,
    pub(crate) x: i64,
    pub(crate) col: i64,
    pub(crate) row: usize,
    /// The string the iterator stopped in and the index there.
    pub(crate) object: Option<(Value, usize)>,
}

/// buffer_posn_from_coords on a terminal: start_display at START in
/// WINDOW, which shows the current buffer, move_it_to row Y -- stopping
/// at ZV below the buffer's last row -- then move_it_in_display_line to
/// X of the text area, as the window lays its text out.
pub(crate) fn window_position_at(
    interpreter: &Interpreter,
    window_id: u64,
    start: usize,
    (x, y): (usize, usize),
    geometry: WindowTextGeometry,
) -> WindowPositionAt {
    let layouter = ScreenLineLayouter::new(interpreter, window_id, geometry.lnum_cols);
    let (zv, point) = {
        let buffer = interpreter.buffer.borrow();
        (buffer.point_max(), buffer.point())
    };
    let mut layout = layouter.layout_from(start, geometry.text_width, geometry.truncate);
    let mut base = 0usize;
    // The iterator stopped at POSITION, its glyph at FOUND_X WIDTH wide,
    // after a move to TO_X under first_visible_x FIRST_VISIBLE: *dx is
    // from FOUND_X less what TO_X added to X, make_lispy_position's COL
    // is it.hpos (the cells from first_visible_x) plus, past the
    // element's end X1, the cells TO_X lies beyond it.
    let found = |at: GlyphAt, (to_x, first_visible): (usize, usize), row: usize| {
        let x1 = at.x + at.width;
        WindowPositionAt {
            position: at.position,
            x: at.x as i64 - (to_x - x) as i64,
            col: (at.x as i64 - first_visible as i64) + to_x.saturating_sub(x1) as i64,
            row,
            object: at.object,
        }
    };
    let glyph_x = |geometry: WindowTextGeometry, column: usize| {
        if geometry.truncate {
            geometry.truncated_x(column)
        } else {
            geometry.lnum_cols + column
        }
    };
    loop {
        let rows = layout.row_starts.len();
        if y >= base + rows && layout.line_end() >= zv {
            // move_it_to (-1, 0, Y) runs out of text at ZV, a cell wide.
            let (row, column) = layout.position_cell(zv);
            return found(
                GlyphAt {
                    position: zv,
                    x: glyph_x(geometry, column),
                    width: 1,
                    object: None,
                    display: false,
                    continued_at: None,
                },
                (x + geometry.first_visible, geometry.first_visible),
                base + row,
            );
        }
        if y < base + rows {
            let row = y - base;
            let (at, to_x, first_visible) = row_x_stop(&layout, row, x as i64, geometry, point);
            return found(at, (to_x, first_visible), base + row);
        }
        base += rows;
        layout = layouter.layout(
            layout.line_end() + 1,
            geometry.text_width,
            geometry.truncate,
        );
    }
}

/// move_it_in_display_line (MOVE_TO_X) along ROW of LAYOUT, from the
/// row's start to the text area's x X, as buffer_posn_from_coords and
/// Fvertical_motion's goal column move: to X past first_visible_x, the
/// line-number glyphs produced unless the move is to first_visible_x
/// itself, and under `auto-hscroll-mode' `current-line' the row reaching
/// POINT past the window's hscroll as well, the iterator pretending the
/// window hscrolled.  The stop, with the x moved to and the
/// first_visible_x it moved under.
pub(crate) fn row_x_stop(
    layout: &ScreenLineLayout,
    row: usize,
    x: i64,
    geometry: WindowTextGeometry,
    point: usize,
) -> (GlyphAt, usize, usize) {
    let (to_x, first_visible) = match geometry.point_row_hscroll {
        Some(extra) if layout.row_starts[row] < point && point <= layout.line_end() => (
            x + (geometry.first_visible + extra) as i64,
            geometry.first_visible + geometry.hscroll,
        ),
        _ => (x + geometry.first_visible as i64, geometry.first_visible),
    };
    // A goal left of the row's first glyph stops on it.
    let to_x = to_x.max(0) as usize;
    let geometry = WindowTextGeometry {
        lnum_cols: if to_x == first_visible {
            0
        } else {
            geometry.lnum_cols
        },
        first_visible,
        ..geometry
    };
    // last_visible_x: the text area less the truncation glyph's column.
    let last_visible_x = first_visible + geometry.lnum_cols + geometry.text_width - 1;
    let at = layout.glyph_at_x(row, to_x, last_visible_x, |column| geometry.glyph_x(column));
    (at, to_x, first_visible)
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
    std::rc::Rc::unwrap_or_clone(
        ScreenLineLayouter::new(interpreter, window_id, lnum_cols)
            .layout(bol, text_width, truncate),
    )
}

/// The display context and invisibility spec screen_line_layout reads,
/// taken once for successive display lines of the current buffer, as one
/// iterator walking WINDOW's lines reads them once.
pub(crate) struct ScreenLineLayouter<'a> {
    interpreter: &'a Interpreter,
    spec: crate::lisp::primitives::InvisibilitySpec,
    context: CharacterDisplayContext<'a>,
    window_id: u64,
    lnum_cols: usize,
}

/// A display line's layout key while redisplay records a window's walks:
/// the window, its line-number columns, the line's start, the start a
/// start_display anchors its first row at, the text width and whether
/// lines truncate.
type WalkLayoutKey = (u64, usize, usize, Option<usize>, usize, bool);

thread_local! {
    /// The display lines the walks of one window's redisplay lay out,
    /// while no Lisp runs between them: each walk takes the window's
    /// lines from here rather than laying them out again.
    static WALK_LAYOUTS: std::cell::RefCell<
        Option<std::collections::HashMap<WalkLayoutKey, std::rc::Rc<ScreenLineLayout>>>,
    > = const { std::cell::RefCell::new(None) };
}

/// Keep the layouts redisplay's walks make (true) until Lisp may run and
/// change the text again (false).
fn share_walk_layouts(on: bool) {
    WALK_LAYOUTS.with(|cache| *cache.borrow_mut() = on.then(Default::default));
}

impl<'a> ScreenLineLayouter<'a> {
    pub(crate) fn new(interpreter: &'a Interpreter, window_id: u64, lnum_cols: usize) -> Self {
        let buffer_id = interpreter.current_buffer_id();
        let spec = {
            let buffer = interpreter.buffer.borrow();
            resolve_buffer_invisibility(interpreter, &buffer, buffer_id)
        };
        let context = CharacterDisplayContext::new(interpreter, buffer_id, window_id)
            .with_line_numbers(lnum_cols);
        Self {
            interpreter,
            spec,
            context,
            window_id,
            lnum_cols,
        }
    }

    /// screen_line_layout of the display line starting at BOL.
    pub(crate) fn layout(
        &self,
        bol: usize,
        text_width: usize,
        truncate: bool,
    ) -> std::rc::Rc<ScreenLineLayout> {
        self.lay_out(bol, text_width, truncate, None)
    }

    /// start_display's first display line: the one holding START, its
    /// rows beginning at START wherever it falls in the line.
    pub(crate) fn layout_from(
        &self,
        start: usize,
        text_width: usize,
        truncate: bool,
    ) -> std::rc::Rc<ScreenLineLayout> {
        self.lay_out(start, text_width, truncate, Some(start))
    }

    fn lay_out(
        &self,
        bol: usize,
        text_width: usize,
        truncate: bool,
        origin: Option<usize>,
    ) -> std::rc::Rc<ScreenLineLayout> {
        let (interpreter, spec) = (self.interpreter, &self.spec);

        let buffer = interpreter.buffer.borrow();
        let first_line = visual_line_first_line(&buffer, spec, buffer.line_number_at_pos(bol));
        let line_start = buffer.display_line_start_of(first_line);
        // A start inside the line: rows from there (start_display).
        let origin = origin.filter(|origin| *origin != line_start);
        let key = (
            self.window_id,
            self.lnum_cols,
            line_start,
            origin,
            text_width,
            truncate,
        );
        if let Some(layout) = WALK_LAYOUTS.with(|cache| {
            cache
                .borrow()
                .as_ref()
                .and_then(|layouts| layouts.get(&key).cloned())
        }) {
            return layout;
        }
        let layout = std::rc::Rc::new(self.lay_out_line(
            &buffer, first_line, line_start, text_width, truncate, origin,
        ));
        WALK_LAYOUTS.with(|cache| {
            if let Some(layouts) = cache.borrow_mut().as_mut() {
                layouts.insert(key, std::rc::Rc::clone(&layout));
            }
        });
        layout
    }

    fn lay_out_line(
        &self,
        buffer: &crate::buffer::Buffer,
        first_line: usize,
        line_start: usize,
        text_width: usize,
        truncate: bool,
        origin: Option<usize>,
    ) -> ScreenLineLayout {
        let (spec, context) = (&self.spec, &self.context);
        let visual = glyphless_visual_line_at(buffer, spec, first_line, Some(context));
        let walk = origin.is_some_and(|origin| {
            !(origin <= buffer.point_min() || buffer.char_at(origin - 1) == Some('\n'))
        });
        let from = origin
            .map(|origin| visual.start_index(origin.saturating_sub(line_start), walk && !truncate));
        let laid = match from {
            Some(from) => {
                layout::lay_out_motion_line_from(&visual, text_width.max(1), truncate, from, walk)
            }
            _ => layout::lay_out_motion_line(&visual, text_width.max(1), truncate),
        };
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
                    origin.unwrap_or(line_start)
                } else {
                    // A display starting inside text a string replaces
                    // delivers the string at the start: IT_CHARPOS there.
                    line_start
                        + raw_of(segment.start_index)
                            .max(origin.map_or(0, |origin| origin - line_start))
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
        let rows = RowOfColumn::new(&laid.rows);
        let cells = (0..visual.map.len())
            .map(|offset| {
                let index = visual.map[offset];
                let column = laid.columns[index.min(laid.columns.len() - 1)];
                if truncate {
                    (0, column)
                } else if visual
                    .glyphless_spans
                    .iter()
                    .any(|(from, _)| *from == index)
                {
                    rows.first(column)
                } else {
                    rows.last(column)
                }
            })
            .collect();
        let glyphs = (0..visual.text.chars().count())
            .map(|index| {
                let column = laid.columns[index.min(laid.columns.len() - 1)];
                let (row, column) = if truncate {
                    (0, column)
                } else {
                    rows.first(column)
                };
                (index, row, column, raw_of(index))
            })
            // A display starting inside the line shows nothing before
            // its first display index; the iterator stands at the start
            // while it shows a string covering it.
            .filter(|(index, ..)| from.is_none_or(|from| *index >= from))
            .map(|(index, row, column, raw)| {
                let raw = match origin {
                    Some(origin) => raw.max(origin - line_start),
                    None => raw,
                };
                (
                    (row, column, raw),
                    (visual.object_at(index), visual.object_from_display(index)),
                )
            })
            .unzip::<_, _, Vec<_>, Vec<_>>();
        let (glyphs, glyph_strings) = glyphs;
        let (glyph_objects, glyph_display) = glyph_strings.into_iter().unzip();
        let row_ends = laid
            .rows
            .iter()
            .map(|row| {
                if truncate {
                    row.end_column
                } else {
                    row.end_column - row.start_column
                }
            })
            .collect();
        let row_prefixes = laid.rows.iter().map(|row| row.prefix_cols).collect();
        // handle_line_prefix's string: get_line_prefix_it_property at the
        // row's start, `wrap-prefix' on a continuation row.
        let row_prefix_objects = laid
            .rows
            .iter()
            .zip(&row_starts)
            .map(|(row, start)| {
                (row.prefix_cols > 0)
                    .then(|| context.prefix_value_at(buffer, *start, row.continuation))
                    .filter(|value| crate::lisp::primitives::string_text(value).is_ok())
            })
            .collect();
        let row_word_wrapped = laid
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| index + 1 < laid.rows.len() && !row.continued_glyph)
            .collect();
        let runs = |string: bool| {
            visual
                .replaced
                .iter()
                .filter(|run| run.2 == string)
                .map(|&(start, end, _)| (start, end))
                .collect()
        };
        let (strings, ellipses) = (runs(true), runs(false));
        let ends_at_zv =
            buffer.char_at(line_start + visual.map.len().saturating_sub(1)) != Some('\n');
        let skipped = iterator_skipped_runs(&visual, line_start);
        let line_walk_end = line_end + usize::from(!ends_at_zv);
        let row_walk_ends = laid
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| match laid.rows.get(index + 1) {
                Some(next) if !truncate => {
                    let next_start = line_start + raw_of(next.start_index);
                    match row.overflow_index {
                        Some(overflow) => (line_start + raw_of(overflow)).max(next_start) + 1,
                        // The element that did not fit, the next row's first.
                        None if next.continuation => next_start + 1,
                        // A newline in a display or overlay string.
                        None => next_start,
                    }
                }
                _ => line_walk_end,
            })
            .collect();
        let wrap_points = laid
            .wrap_indexes
            .iter()
            .map(|&index| line_start + raw_of(index))
            .collect();
        ScreenLineLayout {
            row_starts,
            truncate,
            bol: line_start,
            cells,
            glyphs,
            row_ends,
            row_prefixes,
            strings,
            ellipses,
            glyph_objects,
            glyph_display,
            row_prefix_objects,
            row_word_wrapped,
            word_wrap: !truncate && matches!(visual.line_wrap, layout::LineWrap::Word { .. }),
            ends_at_zv,
            newline_replaced: visual.newline_replaced,
            skipped,
            row_walk_ends,
            wrap_points,
        }
    }
}

/// The buffer positions of the display line VISUAL (its first raw line
/// starting at LINE_START) that the iterator passes without fetching:
/// the rest of an invisible run after its first position, and of text a
/// display string or an ellipsis replaces -- handle_stop at the run's
/// start runs handle_fontified_prop there, then skips to its end.
fn iterator_skipped_runs(visual: &VisualLine, line_start: usize) -> Vec<(usize, usize)> {
    let mut replaced: Vec<(usize, usize)> = visual
        .replaced
        .iter()
        .map(|&(start, end, _)| (start, end))
        .collect();
    replaced.sort_unstable();
    // Shown as its own display character (or the first of a string
    // replacing it), not hidden.
    let shown = |offset: usize| {
        visual
            .map
            .get(offset)
            .and_then(|&index| visual.raw_of_display.get(index))
            == Some(&offset)
    };
    let mut skipped: Vec<(usize, usize)> = Vec::new();
    let mut run = 0usize;
    let mut previous_invisible = false;
    for offset in 0..visual.map.len().saturating_sub(1) {
        while replaced.get(run).is_some_and(|&(_, end)| end <= offset) {
            run += 1;
        }
        let replacing = replaced
            .get(run)
            .filter(|&&(start, _)| start <= offset)
            .copied();
        let invisible = replacing.is_none() && !shown(offset);
        let skip = match replacing {
            Some((start, _)) => offset > start,
            None => invisible && previous_invisible,
        };
        previous_invisible = invisible;
        if !skip {
            continue;
        }
        let pos = line_start + offset;
        match skipped.last_mut() {
            Some(last) if last.1 == pos => last.1 = pos + 1,
            _ => skipped.push((pos, pos + 1)),
        }
    }
    skipped
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
#[cfg(test)]
fn displayed_line_with_map(
    buffer: &crate::buffer::Buffer,
    line: usize,
) -> (String, Option<Vec<usize>>) {
    let line = displayed_line_with_runs(buffer, line);
    (line.text, line.map)
}

/// A buffer line laid out with its `display' properties: the text, the
/// raw offset to display index map (when they changed anything), the
/// runs display strings replace and the strings' glyphs (VisualLine's
/// objects).
struct DisplayedLine {
    text: String,
    map: Option<Vec<usize>>,
    replaced: Vec<ReplacedRun>,
    objects: Vec<(Value, bool)>,
    object_of: Vec<Option<(usize, usize)>>,
}

/// A face over display indexes [START, END).
type FaceSpan = (usize, usize, Value);

/// A raw offset range from a display line's start whose text a display
/// string (true) or an invisible run's ellipsis (false) replaces.
type ReplacedRun = (usize, usize, bool);

/// displayed_line_with_map, with the runs display strings replace.
fn displayed_line_with_runs(buffer: &crate::buffer::Buffer, line: usize) -> DisplayedLine {
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
        return DisplayedLine {
            text: raw,
            map: None,
            replaced: Vec::new(),
            objects: Vec::new(),
            object_of: Vec::new(),
        };
    }
    let mut runs: Vec<ReplacedRun> = Vec::new();
    let mut objects: Vec<(Value, bool)> = Vec::new();
    let mut object_of: Vec<Option<(usize, usize)>> = Vec::new();
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
            match runs.last_mut() {
                Some((_, end, true)) if same_run && *end == offset => *end = offset + 1,
                _ => runs.push((offset, offset + 1, true)),
            }
            if !same_run {
                if let Some(value) = display {
                    record_string_glyphs(
                        &mut objects,
                        &mut object_of,
                        expanded_chars,
                        (value, true),
                        (0..replacement.chars().count()).map(Some),
                    );
                }
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
    if !objects.is_empty() {
        object_of.resize(expanded_chars, None);
    }
    if changed {
        DisplayedLine {
            text: expanded,
            map: Some(map),
            replaced: runs,
            objects,
            object_of,
        }
    } else {
        DisplayedLine {
            text: raw,
            map: None,
            replaced: runs,
            objects,
            object_of,
        }
    }
}

/// What planning one window yields: either a request to run
/// `window-scroll-functions' first, or the window's rows.
enum PlanOutcome {
    Hook {
        stage: redisplay_window::Stage,
        start: usize,
        update_mode_line: bool,
        consumed: redisplay_window::Consumed,
        steps: Vec<redisplay_window::FetchStep>,
    },
    Planned {
        plan: WindowPlan,
        chosen: redisplay_window::Chosen,
        consumed: redisplay_window::Consumed,
        steps: Vec<redisplay_window::FetchStep>,
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
    let rows = redisplay_window::Rows::new(buffer, spec, glyphless, body_width, truncate);
    let (decision, consumed, steps) = redisplay_window::decide(&rows, state, text_rows);
    let chosen = match decision {
        redisplay_window::Decision::Hook {
            stage,
            start,
            update_mode_line,
        } => {
            return PlanOutcome::Hook {
                stage,
                start,
                update_mode_line,
                consumed,
                steps,
            };
        }
        redisplay_window::Decision::Display(chosen) => chosen,
    };
    let point = chosen
        .point
        .unwrap_or(state.point)
        .clamp(buffer.point_min(), buffer.point_max());

    let top = chosen.top;
    // start_display: a start inside its line lays that line out from it.
    let anchor = (top.origin != 0).then(|| {
        let walked = !truncate && !rows.at_line_beg(top.origin);
        let numbered = walked
            && rows.visual(top.line).starts_off_buffer(
                top.origin
                    .saturating_sub(buffer.display_line_start_of(top.line)),
            )
            && rows
                .laid_at(top.line, top.origin)
                .rows
                .first()
                .is_some_and(|segment| !segment.continuation);
        Anchor {
            line: top.line,
            origin: top.origin,
            walked,
            numbered,
        }
    });
    let point_line = visual_line_first_line(buffer, spec, buffer.line_number_at_pos(point));
    let point_visual = rows.visual(point_line);
    let index_in = |visual: &VisualLine, line: usize, pos: usize| {
        visual
            .map
            .get(pos.saturating_sub(buffer.display_line_start_of(line)))
            .copied()
            .unwrap_or_else(|| visual.map.last().copied().unwrap_or(0))
    };
    let point_index = index_in(&point_visual, point_line, point);
    // Point on the display's first row, laid out from the start.
    let point_origin = anchor
        .filter(|anchor| anchor.line == point_line && point >= anchor.origin)
        .map(|anchor| anchor.origin);
    let point_layout = match (truncate, point_origin) {
        (true, Some(origin)) => std::rc::Rc::new(layout::lay_out_visual_line_from(
            &point_visual,
            body_width,
            true,
            point_visual.start_index(
                origin.saturating_sub(buffer.display_line_start_of(point_line)),
                false,
            ),
            false,
        )),
        (true, None) => {
            std::rc::Rc::new(layout::lay_out_visual_line(&point_visual, body_width, true))
        }
        (false, origin) => rows.laid_at(point_line, origin.unwrap_or(0)),
    };
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
    // The cursor on a row that does not show point's glyph (a row of a
    // string before it, where the force_start block moved point and the
    // cursor stays while point does not move): set_cursor_from_row on
    // that row.
    let cursor_col = match chosen.cursor_row {
        Some(row_from_top) => {
            let row = rows.forward(chosen.top, row_from_top).0;
            match rows.string_row_cursor_x(row, point) {
                Some((x, after_gutter)) if row != rows.row_in(chosen.top, point) => {
                    if after_gutter { lnum_cols + x } else { 0 }
                }
                _ => cursor_col,
            }
        }
        None => cursor_col,
    };

    // Fetch only the window: redisplay cost must follow the screen size,
    // never the buffer size.  A wholly invisible tail contributes no glyph
    // row; every other visual line contributes at least one.
    let last_line = buffer.line_number_at_pos(buffer.point_max());
    let mut rendered: Vec<(String, usize, usize, usize, usize)> = Vec::with_capacity(text_rows);
    // First row past the window, as (line, segment, origin): the
    // window's end.
    let mut past_window: Option<(usize, usize, usize)> = None;
    // The window's last two rows, for find_row_edges' end of the last.
    let mut last_rows: (Option<redisplay_window::Row>, Option<redisplay_window::Row>) =
        (None, None);
    let mut invisible_tail_start: Option<usize> = None;
    let mut fill_line = view.top_line;
    let mut first_fill = true;
    'fill: while rendered.len() < text_rows && fill_line <= last_line {
        let visual = rows.visual(fill_line);
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
        // The display's first line shows from its start.
        let origin = anchor
            .filter(|anchor| first_fill && anchor.line == fill_line)
            .map_or(0, |anchor| anchor.origin);
        let segments: Vec<String> = if truncate {
            let from = (origin != 0).then(|| {
                visual.start_index(
                    origin.saturating_sub(buffer.display_line_start_of(fill_line)),
                    false,
                )
            });
            vec![if lnum_cols > 0 {
                truncate_row_from(&visual, body_width, row_hscroll, from)
            } else {
                truncate_row_hscrolled(&visual, body_width, row_hscroll, from)
            }]
        } else {
            rows.laid_at(fill_line, origin)
                .rows
                .iter()
                .map(|segment| segment.text.clone())
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
            let row = redisplay_window::Row {
                line: fill_line,
                seg: seg_index,
                origin,
            };
            let row_start = rows.row_start(row);
            last_rows = (last_rows.1, Some(row));
            rendered.push((
                segment.clone(),
                fill_line,
                seg_index,
                row_start,
                row_hscroll,
            ));
            if rendered.len() == text_rows {
                past_window = Some(if seg_index + 1 < segments.len() {
                    (fill_line, seg_index + 1, origin)
                } else {
                    (next_line, 0, 0)
                });
                break 'fill;
            }
        }
        fill_line = next_line;
    }
    let past_zv: String = rows
        .prefix_past_zv()
        .map(|prefix| prefix.chars().take(body_width).collect())
        .unwrap_or_default();
    rendered.resize(text_rows, (past_zv, 0, 0, usize::MAX, 0));

    let top_pos = rows.start_pos(chosen.top);
    // window_end_pos: Z less the end of the window's last row, most
    // often where the row past the window starts.
    let string_row_end = last_rows
        .1
        .filter(|_| past_window.is_some())
        .and_then(|last| rows.string_row_end(last, last_rows.0));
    let window_end = match (past_window, string_row_end) {
        (Some(_), Some(end)) => end,
        (Some((line, seg, origin)), None) if line <= last_line => {
            rows.row_start(redisplay_window::Row { line, seg, origin })
        }
        // The window shows ZV (a row past the final buffer line, or none
        // filling the window): its end is the last row displaying text's
        // (the row at ZV displays none), ZV but for a `display' string's
        // newline ending that row.
        (_, _) => invisible_tail_start.unwrap_or_else(|| {
            last_rows
                .0
                .filter(|_| last_rows.1.is_some_and(|last| rows.empty_row(last)))
                .and_then(|prev| rows.string_row_end(prev, None))
                .unwrap_or_else(|| buffer.point_max())
        }),
    };
    PlanOutcome::Planned {
        plan: WindowPlan {
            rendered,
            top_pos,
            window_end,
            cursor: selected.then_some((point_row, cursor_col)),
            point_cursor: chosen.cursor_row.map(|_| (point_row, cursor_col)),
            anchor,
            cursor_origin: point_origin.filter(|_| truncate && chosen.cursor_row.is_some()),
        },
        chosen,
        consumed,
        steps,
    }
}

/// redisplay_flags::window_outdated for the planner.
fn redisplay_window_outdated(interpreter: &Interpreter, window_id: u64) -> bool {
    redisplay_flags::window_outdated(interpreter, window_id)
}

/// try_cursor_movement's conditions besides the current matrix's: no
/// update_mode_lines, no `show-trailing-whitespace' in INFO's buffer,
/// `display-line-numbers' neither relative nor visual and, when on, the
/// current line's number face no different from the others', and no
/// overlay arrow in the buffer (overlay_arrow_in_current_buffer_p).
fn cursor_movement_usable(
    interpreter: &mut Interpreter,
    env: &mut Env,
    info: &crate::lisp::primitives::WindowRenderInfo,
) -> bool {
    if interpreter.update_mode_lines != 0 {
        return false;
    }
    let value = |interpreter: &Interpreter, name: &str| {
        interpreter
            .buffer_local_value(info.buffer_id, name)
            .or_else(|| interpreter.default_value(name))
            .unwrap_or(Value::Nil)
    };
    if value(interpreter, "show-trailing-whitespace").is_truthy() {
        return false;
    }
    // composition_break_at_point, and hscrolling_current_line_p's
    // `auto-hscroll-mode' of `current-line' (not suspended).
    if value(interpreter, "composition-break-at-point").is_truthy() {
        return false;
    }
    if crate::lisp::types::eq_symbol_named(value(interpreter, "auto-hscroll-mode"), "current-line")
        && !crate::lisp::primitives::window_flag(
            interpreter,
            info.window_id,
            crate::lisp::primitives::WINDOW_SUSPEND_AUTO_HSCROLL_SLOT,
        )
    {
        return false;
    }
    let numbers = value(interpreter, "display-line-numbers");
    if crate::lisp::types::eq_symbol_named(numbers, "relative")
        || crate::lisp::types::eq_symbol_named(numbers, "visual")
    {
        return false;
    }
    if numbers.is_truthy() {
        let frame = interpreter.selected_frame_value();
        let equal = call(
            interpreter,
            env,
            "internal-lisp-face-equal-p",
            &[
                Value::Symbol("line-number".into()),
                Value::Symbol("line-number-current-line".into()),
                frame,
            ],
        );
        if !equal.is_ok_and(|equal| equal.is_truthy()) {
            return false;
        }
    }
    let mut variables = interpreter
        .lookup_var_key(
            cached_symbol!("overlay-arrow-variable-list"),
            &crate::lisp::types::Env::new(),
        )
        .unwrap_or(Value::Nil);
    while let Some(cell) = variables.cons_ref() {
        let variable = cell.car.get();
        variables = cell.cdr.get();
        let Some(symbol) = variable.symbol_cell() else {
            continue;
        };
        // find_symbol_value with the window's buffer current.
        if let Some(Kind::Marker(marker)) = interpreter
            .buffer_local_value_symbol(info.buffer_id, &symbol)
            .or_else(|| interpreter.default_value_symbol(&symbol).ok().flatten())
            .map(|value| value.kind())
            && interpreter.marker_buffer_id(marker) == Some(info.buffer_id)
        {
            return false;
        }
    }
    true
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
    cursor_movement_usable: bool,
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
    let (buffer_point, restriction, modiff, changed_region) =
        if info.buffer_id == interpreter.current_buffer_id() {
            let buffer = interpreter.buffer.borrow();
            (
                buffer.point(),
                (buffer.point_min(), buffer.point_max()),
                buffer.modified_tick(),
                buffer.changed_region(),
            )
        } else {
            interpreter
                .get_buffer_by_id(info.buffer_id)
                .map(|buffer| {
                    (
                        buffer.point(),
                        (buffer.point_min(), buffer.point_max()),
                        buffer.modified_tick(),
                        buffer.changed_region(),
                    )
                })
                .unwrap_or((info.point, (1, 1), Default::default(), (1, 1)))
        };
    let force_start = window_flag(interpreter, info.window_id, WINDOW_FORCE_START_SLOT);
    let optional_new_start =
        window_flag(interpreter, info.window_id, WINDOW_OPTIONAL_NEW_START_SLOT);
    // window.c window_outdated: the text's or the overlays' count moved
    // since the window's last complete redisplay
    // (mark_window_display_accurate_1, at the end of redisplay_internal).
    let window_outdated = previous.last_buffer_id != Some(info.buffer_id)
        || redisplay_window_outdated(interpreter, info.window_id);
    // current_matrix_up_to_date_p (window_end_valid, no clip_changed or
    // prevent_redisplay_optimizations_p in the buffer, not outdated) and
    // try_cursor_movement's own conditions.
    let (clip_changed, prevent_optimizations) = interpreter
        .get_buffer_by_id(info.buffer_id)
        .map(|buffer| {
            (
                buffer.clip_changed(),
                buffer.prevent_redisplay_optimizations_p(),
            )
        })
        .unwrap_or((true, true));
    let matrix_current = !windows_or_buffers_changed
        && !window_outdated
        && !force_start
        && !optional_new_start
        && window_flag(
            interpreter,
            info.window_id,
            crate::lisp::primitives::WINDOW_END_VALID_SLOT,
        )
        && !clip_changed
        && !prevent_optimizations
        && previous.last_start == Some(info.start)
        && previous.last_restriction == Some(restriction)
        && cursor_movement_usable;
    let cursor_unmoved = matrix_current && previous.last_point == Some(info.point);
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
        last_point: previous.last_point,
        matrix_current,
        // A window never displayed has its struct zeroed.
        cursor_y: previous.last_cursor_y.unwrap_or(0),
        scroll_functions: value("window-scroll-functions").is_some_and(|hook| hook.is_truthy()),
        make_window_start_visible: value("make-window-start-visible")
            .is_some_and(|flag| flag.is_truthy()),
        total_cells: info.width.saturating_mul(info.height),
        top_rows,
        scroll,
        changed_region,
        // track_mouse: the variable's current binding, which
        // `track-mouse' binds around a drag.
        track_mouse: interpreter
            .symbol_value_cell("track-mouse")
            .is_ok_and(|value| value.is_truthy()),
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
/// (safe_run_hooks_2), inside redisplay_window, so with the buffer's
/// point at a non-selected window's.
fn run_window_scroll_functions(
    interpreter: &mut Interpreter,
    env: &mut Env,
    info: &crate::lisp::primitives::WindowRenderInfo,
    start: usize,
) {
    let saved = interpreter.current_buffer_id();
    if saved != info.buffer_id && interpreter.set_current_buffer_id(info.buffer_id).is_err() {
        return;
    }
    let _ = env;
    let window = interpreter.record_value(info.window_id);
    with_window_point(interpreter, info, |interpreter| {
        let _ = batch::safe_run_hooks_with_args(
            interpreter,
            &crate::lisp::types::symbols::SymbolKey::new("window-scroll-functions").symbol(),
            &[window, Value::Integer(start as i64)],
        );
    });
    if interpreter.current_buffer_id() != saved && interpreter.has_buffer_id(saved) {
        let _ = interpreter.set_current_buffer_id(saved);
    }
}

/// redisplay_window's point while INFO's window is redisplayed: for a
/// window other than the selected one its buffer's PT is the window's
/// point clamped into the accessible text, the window's point marker
/// moved there too (set_marker_both on w->pointm), set without the
/// point-motion hooks (TEMP_SET_PT_BOTH).  The buffer's own point goes
/// back after it for every window (restore_buffers: below BEGV to BEGV,
/// past ZV to Z; the selected window's lpoint), undoing what the Lisp
/// redisplay runs moved.  The window's buffer is current when F starts.
fn with_window_point<T>(
    interpreter: &mut Interpreter,
    info: &crate::lisp::primitives::WindowRenderInfo,
    f: impl FnOnce(&mut Interpreter) -> T,
) -> T {
    if interpreter.current_buffer_id() != info.buffer_id {
        return f(interpreter);
    }
    let opoint = interpreter.buffer.borrow().point();
    if !info.selected {
        let point = {
            let buffer = interpreter.buffer.borrow();
            let marker = crate::lisp::primitives::window_marker_position(
                interpreter,
                info.window_id,
                crate::lisp::primitives::WINDOW_POINT_SLOT,
            )
            .unwrap_or(info.point);
            marker.clamp(buffer.point_min(), buffer.point_max())
        };
        crate::lisp::primitives::set_window_marker(
            interpreter,
            info.window_id,
            crate::lisp::primitives::WINDOW_POINT_SLOT,
            info.buffer_id,
            point,
        );
        interpreter.buffer.borrow_mut().goto_char(point);
    }
    let result = f(interpreter);
    // Lisp run by F may have left another buffer current.
    let left = interpreter.current_buffer_id();
    if left == info.buffer_id || interpreter.set_current_buffer_id(info.buffer_id).is_ok() {
        {
            let mut buffer = interpreter.buffer.borrow_mut();
            let point = if opoint < buffer.point_min() {
                buffer.point_min()
            } else if opoint > buffer.point_max() {
                buffer.size_total() + 1
            } else {
                opoint
            };
            buffer.goto_char(point);
        }
        if left != info.buffer_id && interpreter.has_buffer_id(left) {
            let _ = interpreter.set_current_buffer_id(left);
        }
    }
    result
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

/// xdisp.c compute_stop_pos for an iterator at POS in the current
/// buffer: the next overlay change, else the first text property
/// interval after POS whose `fontified', `face', `display', `invisible'
/// or `composition' differs, looked for up to TEXT_PROP_DISTANCE_LIMIT
/// characters on -- under `auto-composition-mode' moved to the next
/// space, TAB, newline or formfeed so as not to split composable text --
/// where the limit itself stops it while more intervals follow.  Text
/// with no further interval stops nowhere before ZV.
fn iterator_next_stop(interpreter: &Interpreter, pos: usize) -> usize {
    let buffer = interpreter.buffer.borrow();
    let zv = buffer.point_max();
    let value = |name: &str| {
        interpreter
            .buffer_local_value(interpreter.current_buffer_id(), name)
            .or_else(|| interpreter.default_value(name))
            .unwrap_or(Value::Nil)
    };
    let mut stop = zv.min(crate::lisp::primitives::next_overlay_change_position(
        &buffer, pos,
    ));
    let mut limit = pos + TEXT_PROP_DISTANCE_LIMIT;
    if value("composition-break-at-point").is_nil()
        && buffer.is_multibyte()
        && !value("auto-composition-mode").is_nil()
    {
        let endpos = (pos + 10 * TEXT_PROP_DISTANCE_LIMIT).min(zv);
        let mut at = limit.min(zv);
        let mut found = None;
        while at < endpos {
            let c = buffer.char_at(at);
            at += 1;
            if matches!(c, Some(' ' | '\t' | '\n' | '\u{c}')) {
                found = Some(at - 1);
                break;
            }
        }
        limit = match found {
            Some(space) => space,
            None if stop < endpos => stop,
            None => pos + TEXT_PROP_DISTANCE_LIMIT,
        };
    }
    let handled = |at: usize| {
        ["fontified", "face", "display", "invisible", "composition"]
            .map(|name| buffer.text_property_at(at, name).unwrap_or(Value::Nil))
    };
    let here = handled(pos);
    let mut boundary = buffer.text_property_interval_around(pos).1;
    while boundary > pos && boundary < zv {
        if boundary >= limit {
            stop = stop.min(limit);
            break;
        }
        let there = handled(boundary);
        if here
            .iter()
            .zip(&there)
            .any(|(a, b)| !crate::lisp::types::eq(*a, *b))
        {
            stop = stop.min(boundary);
            break;
        }
        let next = buffer.text_property_interval_around(boundary).1;
        if next <= boundary {
            break;
        }
        boundary = next;
    }
    stop.max(pos + 1)
}

/// handle_fontified_prop over what redisplay_window's walks fetch for
/// INFO's window, in their order (redisplay_window.rs's steps): try_window
/// at a start -- start_display, then the window's rows each to its end --
/// and the walks of the optional new start's check, try_scrolling, the
/// recenter label and compute_window_start_on_continuation_line; with the
/// window's point (with_window_point), DECLINED keeping what the
/// functions declined for the window's redisplay.  Answers whether any
/// function ran.
fn fontify_window_steps(
    interpreter: &mut Interpreter,
    env: &mut Env,
    info: &crate::lisp::primitives::WindowRenderInfo,
    steps: &[redisplay_window::FetchStep],
    text_rows: usize,
    declined: &mut Vec<usize>,
) -> bool {
    if steps.is_empty() {
        return false;
    }
    let saved = interpreter.current_buffer_id();
    if saved != info.buffer_id && interpreter.set_current_buffer_id(info.buffer_id).is_err() {
        return false;
    }
    let ran = with_window_point(interpreter, info, |interpreter| {
        let mut fetches = IteratorFetches::new(interpreter, env, info.window_id);
        let Some(geometry) = fetches.geometry() else {
            return false;
        };
        fetches.declined_before = std::mem::take(declined);
        use redisplay_window::FetchStep;
        let mut ran = false;
        share_walk_layouts(true);
        // partial_line_height and the recenter label skip their walks for
        // very long truncated lines.
        let long_truncated =
            geometry.truncate && interpreter.buffer.borrow().long_line_optimizations_p();
        for step in steps {
            match *step {
                FetchStep::Display { start } => {
                    // try_window's set_marker_both (w->start, ...) first:
                    // the walks so far fontify under the start they saw.
                    if !fetches.ranges.is_empty() {
                        share_walk_layouts(false);
                        ran |= fetches.fontify(interpreter, env, info.window_id);
                        fetches.ranges.clear();
                        share_walk_layouts(true);
                    }
                    crate::lisp::primitives::set_window_start_marker(
                        interpreter,
                        info.window_id,
                        start,
                    );
                    fetch_start_display(interpreter, info.window_id, &mut fetches, start);
                    IteratorWalk::from_window_start(interpreter, info.window_id, geometry, start)
                        .walk_rows(&mut fetches, start, text_rows);
                }
                FetchStep::Init { pos } => {
                    fetches.begin_walk();
                    fetches.push_stop(pos);
                }
                FetchStep::StartDisplay { start } => {
                    fetch_start_display(interpreter, info.window_id, &mut fetches, start);
                }
                FetchStep::MoveTo { from, pos, rows } => {
                    IteratorWalk::from_window_start(interpreter, info.window_id, geometry, from)
                        .walk_to_within(&mut fetches, from, pos, rows);
                }
                FetchStep::Lines { from, rows } => {
                    IteratorWalk::from_window_start(interpreter, info.window_id, geometry, from)
                        .walk_rows(&mut fetches, from, rows);
                }
                FetchStep::Partial { from } => {
                    // A copy (SAVE_IT) moves to the last visible y, then
                    // move_it_by_lines 1: the window's rows and the one
                    // below, each to its end; the original goes on after.
                    if !long_truncated {
                        let original = fetches.copy_walk();
                        IteratorWalk::from_window_start(
                            interpreter,
                            info.window_id,
                            geometry,
                            from,
                        )
                        .walk_rows(&mut fetches, from, text_rows + 1);
                        fetches.resume_walk(original);
                    }
                }
                FetchStep::Back { pos, rows } => {
                    fetch_move_vertically_backward(
                        interpreter,
                        env,
                        info.window_id,
                        &mut fetches,
                        pos,
                        rows,
                    );
                }
                FetchStep::RecenterBack { pos, rows } => {
                    if !long_truncated {
                        fetch_move_vertically_backward(
                            interpreter,
                            env,
                            info.window_id,
                            &mut fetches,
                            pos,
                            rows,
                        );
                    }
                }
                FetchStep::Continue { at } => fetches.continue_walk_at(at),
                FetchStep::PosVisible { start, pos } => {
                    let shown = window_position_xy(
                        interpreter,
                        info.window_id,
                        start,
                        pos,
                        geometry,
                        text_rows,
                    )
                    .map(|at| (at.x, at.y));
                    fetch_pos_visible(
                        interpreter,
                        info.window_id,
                        &mut fetches,
                        geometry,
                        (start, Some(pos)),
                        shown,
                        text_rows,
                    );
                }
                FetchStep::ByLines { from, dvpos } => {
                    crate::lisp::primitives::move_it::move_it_by_lines(
                        interpreter,
                        env,
                        info.window_id,
                        from,
                        dvpos,
                        &mut fetches,
                    );
                }
                FetchStep::SetStart { start } => {
                    // The walks so far fontify under the start they saw;
                    // the rest under this one.
                    share_walk_layouts(false);
                    ran |= fetches.fontify(interpreter, env, info.window_id);
                    fetches.ranges.clear();
                    share_walk_layouts(true);
                    crate::lisp::primitives::set_window_start_marker(
                        interpreter,
                        info.window_id,
                        start,
                    );
                }
                FetchStep::ContinuationLine {
                    start,
                    line_start,
                    rows,
                } => {
                    fetches.begin_walk();
                    fetches.push_stop(start);
                    fetches.begin_walk();
                    fetches.push_stop(line_start);
                    if rows > 0 {
                        IteratorWalk::at(interpreter, info.window_id, geometry, line_start)
                            .walk_rows(&mut fetches, line_start, rows);
                    }
                }
            }
        }
        share_walk_layouts(false);
        ran |= fetches.fontify(interpreter, env, info.window_id);
        *declined = std::mem::take(&mut fetches.declined_before);
        declined.append(&mut fetches.declined);
        ran
    });
    if interpreter.current_buffer_id() != saved && interpreter.has_buffer_id(saved) {
        let _ = interpreter.set_current_buffer_id(saved);
    }
    ran
}

/// move_it_vertically_backward ROWS rows from an iterator at POINT in
/// WINDOW: from where it is (start_pos), back max (1, dy) line starts, no
/// farther than dy full rows of text (pos_limit), reseat_1 there, and
/// it2 walks on to start_pos; while that did not go far enough and BEGV
/// is not reached, back again from there by what is missing
/// (move_further_back), the y it reached counted as GNU counts it.  The
/// rows it then moves forward were walked already.
pub(crate) fn fetch_move_vertically_backward(
    interpreter: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    fetches: &mut IteratorFetches,
    point: usize,
    rows: usize,
) {
    use crate::lisp::primitives::move_it::{back_to_previous_visible_line_start, rows_between};
    let Some(geometry) = fetches.geometry() else {
        return;
    };
    let begv = interpreter.buffer.borrow().point_min();
    let nchars_per_row =
        crate::lisp::primitives::move_it::iterator_row_chars(interpreter, window_id);
    let mut it = point;
    // IT's current_y, relative to where it started.
    let mut it_y = 0i64;
    let mut dy = rows as i64;
    loop {
        let start_pos = it;
        let nlines = dy.max(1) as usize;
        let pos_limit = if geometry.truncate || nchars_per_row == 0 {
            begv
        } else {
            start_pos.saturating_sub(nlines * nchars_per_row).max(begv)
        };
        for _ in 0..nlines {
            if it <= pos_limit {
                break;
            }
            it = back_to_previous_visible_line_start(interpreter, it);
        }
        // it2, a copy of the iterator reseat_1 left at IT, walks to
        // start_pos.
        fetches.begin_walk();
        IteratorWalk::at(interpreter, window_id, geometry, it).walk_to(fetches, it, start_pos);
        let h = rows_between(
            interpreter,
            env,
            window_id,
            it,
            start_pos,
            &mut IteratorFetches::off(),
        );
        it_y -= h;
        if dy == 0 {
            // DY 0 moves to the start of the screen line: IT itself, back
            // at the line start, then move_it_by_lines its rows down.
            if h > 0 {
                fetches.begin_walk();
                IteratorWalk::at(interpreter, window_id, geometry, it)
                    .walk_rows(fetches, it, h as usize);
            }
            break;
        }
        // The y to reach; a line's height is one row, two thirds of it
        // none.
        let target_y = it_y + h - dy;
        if target_y < it_y && it > begv {
            dy = it_y - target_y;
            continue;
        }
        break;
    }
}

/// What a window's layout reads of the current buffer and could change
/// while `fontification-functions' run: the buffer, its text, overlays
/// and layout-affecting text properties (Buffer::layout_properties_tick),
/// its accessible region and point.
#[derive(PartialEq, Eq)]
struct LayoutStamp {
    buffer_id: u64,
    counts: (
        crate::buffer::ModCount,
        crate::buffer::ModCount,
        crate::buffer::ModCount,
    ),
    region: (usize, usize, usize),
}

impl LayoutStamp {
    fn of(interpreter: &Interpreter) -> Self {
        let buffer = interpreter.buffer.borrow();
        LayoutStamp {
            buffer_id: interpreter.current_buffer_id(),
            counts: (
                buffer.chars_modified_tick(),
                buffer.overlay_modified_tick(),
                buffer.layout_properties_tick(),
            ),
            region: (buffer.point_min(), buffer.point_max(), buffer.point()),
        }
    }
}

/// The positions where `fontification-functions' ran and left the text
/// unfontified: NOW those of this walk, whose iterator moved on to its
/// next stop past them (another walk, stopping there anew, offers them
/// again), BEFORE those of earlier passes over the same walks.
pub(crate) struct Declined<'a> {
    now: &'a mut Vec<usize>,
    before: &'a [usize],
}

/// A display iterator's stops as its walk's fetches were fontified: the
/// positions where the functions ran and it left the text unfontified,
/// how far it has walked, and the stop it heads for from there (none
/// worked out yet for one an earlier walk left).
#[derive(Clone, Debug, Default)]
struct WalkStops {
    declined: Vec<usize>,
    end: usize,
    next_stop: Option<usize>,
}

/// xdisp.c TEXT_PROP_DISTANCE_LIMIT: compute_stop_pos looks no further
/// for a property change, so a stretch of unchanged text still stops the
/// iterator this often.
const TEXT_PROP_DISTANCE_LIMIT: usize = 100;

/// handle_fontified_prop at each stop of an iterator passing [START, END)
/// of the current buffer: where `fontified' is nil (never at Z, whose nil
/// no fontifying changes) `fontification-functions' runs.  When the
/// functions leave POS unfontified the iterator moves on to its next stop
/// -- the next property or overlay change, TEXT_PROP_DISTANCE_LIMIT
/// characters at most -- and runs them again there; a position they
/// declined is not offered again while DECLINED remembers it (the same
/// command's walks).  WINDOW_POINT centers a long line's narrowing.
/// Answers whether the functions ran and the stop the iterator heads for
/// at END.
fn fontify_buffer_range(
    interpreter: &mut Interpreter,
    env: &mut Env,
    start: usize,
    end: usize,
    window_point: usize,
    declined: Declined<'_>,
) -> (bool, usize) {
    let Declined {
        now: declined,
        before,
    } = declined;
    let mut ran = false;
    let z = interpreter.buffer.borrow().size_total() + 1;
    let end = end.min(z);
    let next_stop = |interpreter: &Interpreter, pos: usize| iterator_next_stop(interpreter, pos);
    let mut pos = start;
    while pos < end {
        // Redisplay reads a property whose lookup signals as nil.
        let fontified = crate::lisp::primitives::buffer_char_property_at(
            interpreter,
            &interpreter.buffer.borrow(),
            pos,
            "fontified",
        )
        .unwrap_or(Value::Nil);
        if !fontified.is_nil() {
            let (_, span_end) = interpreter
                .buffer
                .borrow()
                .text_property_interval_around(pos);
            pos = span_end.max(pos + 1);
            continue;
        }
        if before.contains(&pos) || declined.contains(&pos) {
            pos = next_stop(interpreter, pos);
            continue;
        }
        let functions = interpreter
            .lookup_var_key(cached_symbol!("fontification-functions"), env)
            .unwrap_or(Value::Nil);
        if functions.is_nil() {
            break;
        }
        let buffer_id = interpreter.current_buffer_id();
        let (begv, zv, old_clip_changed) = {
            let buffer = interpreter.buffer.borrow();
            (
                buffer.point_min(),
                buffer.point_max(),
                buffer.clip_changed(),
            )
        };
        let Ok(restore) =
            interpreter.bind_special_dynamic("fontification-functions", Value::Nil, env)
        else {
            break;
        };
        let count = interpreter.specpdl_index();
        narrow_for_fontification(interpreter, env, pos, window_point);
        run_fontification_functions(interpreter, env, &functions, pos);
        ran = true;
        let _ = interpreter.unbind_to(count, env);
        let _ = interpreter.restore_special_dynamic(restore, env);
        // Fontification functions routinely call `save-restriction',
        // which tags clip_changed: when the buffer's restriction is
        // left as it was, the flag is put back.  Misbehaving
        // fontification that left another buffer current has the
        // original made current again.
        if interpreter.current_buffer_id() == buffer_id {
            let mut buffer = interpreter.buffer.borrow_mut();
            if buffer.point_min() == begv && buffer.point_max() == zv {
                buffer.set_clip_changed(old_clip_changed);
            }
        } else if interpreter.has_buffer_id(buffer_id) {
            let _ = interpreter.set_current_buffer_id(buffer_id);
        } else {
            break;
        }
        let fontified = crate::lisp::primitives::buffer_char_property_at(
            interpreter,
            &interpreter.buffer.borrow(),
            pos,
            "fontified",
        )
        .unwrap_or(Value::Nil);
        if fontified.is_nil() {
            if !declined.contains(&pos) {
                declined.push(pos);
            }
            pos = next_stop(interpreter, pos);
        }
    }
    (ran, pos)
}

/// Where a display iterator that start_display puts at START in WINDOW,
/// which shows the current buffer, begins walking text: a start inside
/// a continued line is walked to from its line's start
/// (reseat_at_previous_visible_line_start), a truncated line's not.
pub(crate) fn window_iterator_origin(
    interpreter: &Interpreter,
    start: usize,
    truncate: bool,
) -> usize {
    let buffer = interpreter.buffer.borrow();
    if truncate || start <= buffer.point_min() || buffer.char_at(start - 1) == Some('\n') {
        return start;
    }
    let spec = resolve_buffer_invisibility(interpreter, &buffer, interpreter.current_buffer_id());
    let line = visual_line_first_line(&buffer, &spec, buffer.line_number_at_pos(start));
    buffer.display_line_start_of(line).min(start)
}

/// The display iterator's passes over a window's text run
/// `fontification-functions' for what is not yet fontified as they reach
/// it (handle_fontified_prop), changing what they measure: the pass is
/// made again over the text it then reaches until no more runs.
pub(crate) const ITERATOR_FONTIFICATION_PASSES: usize = 8;

/// xdisp.c forward_to_next_line_start's MAX_NEWLINE_DISTANCE: the display
/// elements past a truncated row's edge it fetches looking for the
/// newline before taking find_newline's shortcut.
const MAX_NEWLINE_DISTANCE: usize = 500;

/// The buffer text a display iterator fetches on its walks over a
/// window -- get_next_display_element, whose handle_stop runs
/// handle_fontified_prop for text not yet `fontified' -- in the order it
/// reaches it.  Nothing is recorded when the current buffer's
/// `fontification-functions' is nil: none would run.
#[derive(Debug, Default)]
pub(crate) struct IteratorFetches {
    geometry: Option<WindowTextGeometry>,
    zv: usize,
    z: usize,
    /// What each walk fetched: from, to and the walk.
    ranges: Vec<(usize, usize, usize)>,
    /// The walk being recorded: a new iterator (start_display, a copy
    /// SAVE_IT made) or one reseated, which stops again where it starts.
    walk: usize,
    /// The walks begun so far, each one's number.
    walks: usize,
    /// Each copy's original (SAVE_IT): a copy starts past the stops its
    /// original's iterator had passed.
    originals: Vec<(usize, usize)>,
    /// Where each walk's iterator stands this pass, as its fetches were
    /// fontified so far.
    walk_stops: std::collections::HashMap<usize, WalkStops>,
    /// Positions where the functions ran this pass and left the text
    /// unfontified, offered again only where a walk starts.
    declined: Vec<usize>,
    /// Those of the passes before, which GNU's single pass did not see
    /// again.
    declined_before: Vec<usize>,
}

impl IteratorFetches {
    /// Recording the walks of an iterator in WINDOW, which shows the
    /// current buffer, when `fontification-functions' would run.
    pub(crate) fn new(interpreter: &mut Interpreter, env: &mut Env, window_id: u64) -> Self {
        let mut fetches = IteratorFetches::default();
        fetches.begin_pass(interpreter, env, window_id);
        fetches
    }

    /// Recording nothing: a walk no `fontification-functions' runs for.
    pub(crate) fn off() -> Self {
        IteratorFetches::default()
    }

    /// Start recording the walks made again after functions ran: what
    /// they declined stays declined.
    pub(crate) fn begin_pass(
        &mut self,
        interpreter: &mut Interpreter,
        env: &mut Env,
        window_id: u64,
    ) {
        let on = interpreter
            .lookup_var_key(cached_symbol!("fontification-functions"), env)
            .is_some_and(|value| !value.is_nil());
        self.geometry =
            on.then(|| crate::lisp::primitives::window_text_geometry(interpreter, env, window_id));
        self.zv = interpreter.buffer.borrow().point_max();
        self.z = interpreter.buffer.borrow().size_total() + 1;
        self.ranges.clear();
        self.originals.clear();
        self.walk_stops.clear();
        self.declined_before.append(&mut self.declined);
    }

    /// How the window lays its text out, when recording.
    pub(crate) fn geometry(&self) -> Option<WindowTextGeometry> {
        self.geometry
    }

    /// A new walk: an iterator start_display set up, a copy of one, or
    /// one reseated.
    pub(crate) fn begin_walk(&mut self) {
        self.walks += 1;
        self.walk = self.walks;
    }

    /// SAVE_IT: a copy of the iterator walks from here, with stops of its
    /// own; answers the original's walk for resume_walk (RESTORE_IT).
    pub(crate) fn copy_walk(&mut self) -> usize {
        let original = self.walk;
        self.begin_walk();
        self.originals.push((self.walk, original));
        original
    }

    /// RESTORE_IT: the original iterator goes on, past what it declined
    /// itself but not what its copy did.
    pub(crate) fn resume_walk(&mut self, walk: usize) {
        self.walk = walk;
    }

    /// An iterator this record has not seen goes on from POS, where an
    /// earlier walk left it: it stops next where its stops from POS
    /// fall, not at POS.
    pub(crate) fn continue_walk_at(&mut self, pos: usize) {
        self.begin_walk();
        if self.geometry.is_some() {
            self.walk_stops.insert(
                self.walk,
                WalkStops {
                    declined: Vec::new(),
                    end: pos,
                    next_stop: None,
                },
            );
        }
    }

    /// The iterator fetches [FROM, TO) (nothing at or past ZV, where
    /// get_next_display_element stops).
    pub(crate) fn push(&mut self, from: usize, to: usize) {
        let to = to.min(self.zv);
        if self.geometry.is_none() || from >= to {
            return;
        }
        match self.ranges.last_mut() {
            Some(last) if last.2 == self.walk && last.0 <= from && from <= last.1 => {
                last.1 = last.1.max(to)
            }
            _ => self.ranges.push((from, to, self.walk)),
        }
    }

    /// reseat's handle_stop at POS, which runs handle_fontified_prop even
    /// at ZV short of Z (a narrowed buffer's end).
    pub(crate) fn push_stop(&mut self, pos: usize) {
        if self.geometry.is_some() && pos < self.z {
            self.ranges.push((pos, pos + 1, self.walk));
        }
    }

    /// handle_fontified_prop over what the walks fetched, in the order
    /// they fetched it: answers whether any function ran, after which
    /// what they measured may have changed.
    pub(crate) fn fontify(
        &mut self,
        interpreter: &mut Interpreter,
        env: &mut Env,
        window_id: u64,
    ) -> bool {
        let before = LayoutStamp::of(interpreter);
        let mut ran = false;
        let ranges = std::mem::take(&mut self.ranges);
        for &(from, to, range_walk) in &ranges {
            // Each iterator computes its own stops.  A copy's stands
            // where its original's stood when the copy was made, with
            // what that one had declined and the stop it was heading
            // for: the original's ranges recorded so far come first.
            let mut stops = self.walk_stops.remove(&range_walk).unwrap_or_else(|| {
                self.originals
                    .iter()
                    .find(|&&(copy, _)| copy == range_walk)
                    .and_then(|&(_, original)| self.walk_stops.get(&original).cloned())
                    .unwrap_or_default()
            });
            // Going on from where it stands, the iterator stops next
            // where its stops fall; elsewhere (a walk's start, a reseat,
            // past skipped text) it stops where the range begins.
            let first = if stops.end > 0 && from <= stops.end && from + 1 >= stops.end {
                stops
                    .next_stop
                    .unwrap_or_else(|| iterator_next_stop(interpreter, stops.end))
                    .max(from)
            } else {
                from
            };
            let (any, next) = fontify_iterated_text(
                interpreter,
                env,
                window_id,
                (first, to),
                Declined {
                    now: &mut stops.declined,
                    before: &self.declined_before,
                },
            );
            ran |= any;
            for &pos in &stops.declined {
                if !self.declined.contains(&pos) {
                    self.declined.push(pos);
                }
            }
            stops.end = stops.end.max(to);
            stops.next_stop = Some(next);
            self.walk_stops.insert(range_walk, stops);
        }
        self.ranges = ranges;
        // What the walks measured holds unless the functions changed what
        // the layout reads.
        ran && LayoutStamp::of(interpreter) != before
    }
}

impl ScreenLineLayout {
    /// Record what an iterator passing [FROM, TO) of this line fetches:
    /// all but what handle_stop skips past an invisible run's or a
    /// replaced text's start.
    fn fetch_span(&self, fetches: &mut IteratorFetches, from: usize, to: usize) {
        let mut at = from;
        for &(skip_from, skip_to) in &self.skipped {
            if skip_to <= at {
                continue;
            }
            if skip_from >= to {
                break;
            }
            fetches.push(at, skip_from.min(to));
            at = at.max(skip_to);
        }
        fetches.push(at, to);
    }

    /// The start of the invisible run whose ellipsis an iterator walking
    /// to POS stops on: POS inside the run or right after it, which
    /// BUFFER_POS_REACHED_P finds reached on the ellipsis's last dot,
    /// before the text after the run is fetched.
    fn ellipsis_stop(&self, pos: usize) -> Option<usize> {
        let offset = pos.checked_sub(self.bol)?;
        self.ellipses
            .iter()
            .find(|&&(run, end)| run <= offset && offset <= end)
            .map(|&(run, _)| self.bol + run)
    }

    /// Where move_it_to POS lands: past a run handle_stop skips --
    /// invisible text, text a display string replaces -- when POS is in
    /// it, at the first position after it (BUFFER_POS_REACHED_P checks
    /// after get_next_display_element skipped the run).
    fn landing(&self, pos: usize) -> usize {
        let index = self.skipped.partition_point(|&(_, to)| to <= pos);
        match self.skipped.get(index) {
            Some(&(from, to)) if from.saturating_sub(1) <= pos => to,
            _ => pos,
        }
    }

    fn is_skipped(&self, pos: usize) -> bool {
        let index = self.skipped.partition_point(|&(_, to)| to <= pos);
        self.skipped
            .get(index)
            .is_some_and(|&(from, _)| from <= pos)
    }

    /// The row an iterator at POS is on.
    fn row_of(&self, pos: usize) -> usize {
        crate::lisp::primitives::row_index_of(&self.row_starts, pos)
    }

    /// A truncated row's first element at or past last_visible_x, under
    /// GEOMETRY's hscroll: the check after the element before it fetches
    /// it, and forward_to_next_line_start goes on from there.
    fn truncation_edge(&self, row: usize, geometry: WindowTextGeometry) -> Option<usize> {
        let edge_column = geometry.first_visible + geometry.text_width.saturating_sub(1);
        let start = self.row_starts.get(row).copied().unwrap_or(self.bol);
        (start..self.line_end())
            .find(|&pos| !self.is_skipped(pos) && self.position_cell(pos).1 >= edge_column)
    }

    /// One past the last position a walk of ROW to its end fetches: the
    /// row's walk end, a truncated row's no more than
    /// MAX_NEWLINE_DISTANCE elements past its edge.
    fn row_walk_end(&self, row: usize, geometry: WindowTextGeometry) -> usize {
        let end = self
            .row_walk_ends
            .get(row)
            .copied()
            .unwrap_or_else(|| self.line_end() + usize::from(!self.ends_at_zv));
        if !self.truncate {
            return end;
        }
        let Some(edge) = self.truncation_edge(row, geometry) else {
            return end;
        };
        let mut elements = 0usize;
        for pos in edge..end {
            if !self.is_skipped(pos) {
                elements += 1;
                if elements == MAX_NEWLINE_DISTANCE {
                    return pos + 1;
                }
            }
        }
        end
    }

    /// One past the last position move_it_to POS, on ROW, fetches: POS
    /// itself; a truncated row's tail when POS lies past the edge (the
    /// walk to the next line start passes it); under `word-wrap', with a
    /// wrap point on the row at or before POS, on (atpos_it) through the
    /// next wrap point, the element that does not fit, or the newline,
    /// to see which row POS ends up on.
    fn pos_walk_end(&self, row: usize, pos: usize, geometry: WindowTextGeometry) -> usize {
        if self.truncate {
            return match self.truncation_edge(row, geometry) {
                Some(edge) if pos > edge => self.row_walk_end(row, geometry).max(pos + 1),
                _ => pos + 1,
            };
        }
        let row_start = self.row_starts.get(row).copied().unwrap_or(self.bol);
        if !self
            .wrap_points
            .iter()
            .any(|&point| point > row_start && point <= pos)
        {
            return pos + 1;
        }
        let row_end = self.row_walk_end(row, geometry);
        match self.wrap_points.iter().find(|&&point| point > pos) {
            Some(&point) if point < row_end => point + 1,
            _ => row_end,
        }
        .max(pos + 1)
    }
}

/// A display iterator walking WINDOW's text of the current buffer line
/// by line, recording what it fetches.
pub(crate) struct IteratorWalk<'a> {
    layouter: ScreenLineLayouter<'a>,
    geometry: WindowTextGeometry,
    zv: usize,
    layout: std::rc::Rc<ScreenLineLayout>,
}

impl<'a> IteratorWalk<'a> {
    /// From the display line holding POS.
    pub(crate) fn at(
        interpreter: &'a Interpreter,
        window_id: u64,
        geometry: WindowTextGeometry,
        pos: usize,
    ) -> Self {
        let layouter = ScreenLineLayouter::new(interpreter, window_id, geometry.lnum_cols);
        let layout = layouter.layout(pos, geometry.text_width, geometry.truncate);
        IteratorWalk {
            layouter,
            geometry,
            zv: interpreter.buffer.borrow().point_max(),
            layout,
        }
    }

    /// From a window display starting at START (start_display's first
    /// display line, its rows beginning there).
    pub(crate) fn from_window_start(
        interpreter: &'a Interpreter,
        window_id: u64,
        geometry: WindowTextGeometry,
        start: usize,
    ) -> Self {
        let layouter = ScreenLineLayouter::new(interpreter, window_id, geometry.lnum_cols);
        let layout = layouter.layout_from(start, geometry.text_width, geometry.truncate);
        IteratorWalk {
            layouter,
            geometry,
            zv: interpreter.buffer.borrow().point_max(),
            layout,
        }
    }

    /// The rows from FROM's down to TO's, FROM on this walk's line and
    /// TO no earlier, fetching nothing.
    pub(crate) fn rows_to(&mut self, from: usize, to: usize) -> usize {
        let mut from_row = self.layout.row_of(from);
        let mut rows = 0;
        while to > self.layout.line_end() {
            rows += self.layout.row_starts.len().saturating_sub(from_row);
            if !self.next_line() {
                return rows;
            }
            from_row = 0;
        }
        rows + self.layout.row_of(to).saturating_sub(from_row)
    }

    fn next_line(&mut self) -> bool {
        if self.layout.line_end() >= self.zv {
            return false;
        }
        self.layout = self.layouter.layout(
            self.layout.line_end() + 1,
            self.geometry.text_width,
            self.geometry.truncate,
        );
        true
    }

    /// move_it_to (MOVE_TO_POS) TO from an iterator at FROM, on the line
    /// this walk is at: everything up to TO and TO itself (not ZV), as
    /// the rows on the way and TO's own row fetch it.  Answers where the
    /// iterator lands: TO, or past the hidden run holding it.
    pub(crate) fn walk_to(
        &mut self,
        fetches: &mut IteratorFetches,
        from: usize,
        to: usize,
    ) -> usize {
        let to = to.min(self.zv);
        let mut from = from;
        while from > self.layout.line_end() {
            if !self.next_line() {
                return to;
            }
        }
        loop {
            if to <= self.layout.line_end() {
                if let Some(run) = self.layout.ellipsis_stop(to) {
                    let row = self.layout.row_of(run);
                    let end = (self.layout.row_of(from)..row)
                        .map(|earlier| self.layout.row_walk_end(earlier, self.geometry))
                        .fold(run + 1, usize::max);
                    self.layout.fetch_span(fetches, from, end);
                    return to;
                }
                let to = self.layout.landing(to).min(self.zv);
                let end = if to >= self.zv {
                    self.zv
                } else {
                    // TO's row, and the rows before it walked to their
                    // ends -- past a word-wrap point, beyond their next
                    // rows' starts.
                    let row = self.layout.row_of(to);
                    (self.layout.row_of(from)..row)
                        .map(|earlier| self.layout.row_walk_end(earlier, self.geometry))
                        .fold(self.layout.pos_walk_end(row, to, self.geometry), usize::max)
                };
                self.layout.fetch_span(fetches, from, end);
                return to;
            }
            let last = self.layout.row_walk_ends.len().saturating_sub(1);
            let end = self.layout.row_walk_end(last, self.geometry);
            self.layout.fetch_span(fetches, from, end);
            if !self.next_line() {
                return to;
            }
            from = self.layout.row_starts[0];
        }
    }

    /// move_it_to (MOVE_TO_VPOS) down COUNT rows from an iterator at
    /// FROM: the rest of FROM's row and the rows after it walked to their
    /// ends.  Answers where the next row begins, ZV past the last.
    pub(crate) fn walk_rows(
        &mut self,
        fetches: &mut IteratorFetches,
        from: usize,
        count: usize,
    ) -> usize {
        let mut from = from;
        while from > self.layout.line_end() {
            if !self.next_line() {
                return self.zv;
            }
        }
        for _ in 0..count {
            let row = self.layout.row_of(from);
            let end = self.layout.row_walk_end(row, self.geometry);
            self.layout.fetch_span(fetches, from, end);
            from = match self.layout.row_starts.get(row + 1) {
                Some(&next) => next,
                None => {
                    if !self.next_line() {
                        return self.zv;
                    }
                    self.layout.row_starts[0]
                }
            };
        }
        from
    }

    /// move_it_to (MOVE_TO_POS | MOVE_TO_Y) TO from an iterator at FROM,
    /// no lower than MAX_ROWS rows below FROM's: the rows on the way
    /// walked to their ends, the one holding the y limit to its first
    /// element (x 0 reached there, the row holds the limit).
    pub(crate) fn walk_to_within(
        &mut self,
        fetches: &mut IteratorFetches,
        from: usize,
        to: usize,
        max_rows: usize,
    ) {
        let mut from = from;
        while from > self.layout.line_end() {
            if !self.next_line() {
                return;
            }
        }
        for rows in 0..=max_rows {
            let row = self.layout.row_of(from);
            let holds_to = match self.layout.row_starts.get(row + 1) {
                Some(&next) => to < next,
                None => to <= self.layout.line_end(),
            };
            if holds_to {
                self.walk_to(fetches, from, to);
                return;
            }
            if rows == max_rows {
                self.layout.fetch_span(fetches, from, from + 1);
                return;
            }
            from = self.walk_rows(fetches, from, 1);
            if from >= self.zv {
                return;
            }
        }
    }

    /// Whether an iterator move_it_to stopped at POS has produced nothing
    /// on its row: no glyph before POS there (hidden text produces none)
    /// and no prefix, so line_bottom_y walks the row to find its height.
    pub(crate) fn bare_row_start(&self, pos: usize) -> bool {
        let (row, column) = self.layout.position_cell(pos);
        column == 0 && self.layout.row_prefixes.get(row).copied().unwrap_or(0) == 0
    }
}

/// start_display at START in WINDOW, then move_it_vertically down ROWS
/// rows, each walked to its end (window.c displayed_window_lines): the
/// `fontification-functions' run for what it fetches, the walk made
/// again while they run.
pub(crate) fn fontify_window_rows(
    interpreter: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    start: usize,
    rows: usize,
) {
    let mut fetches = IteratorFetches::off();
    for _ in 0..ITERATOR_FONTIFICATION_PASSES {
        fetches.begin_pass(interpreter, env, window_id);
        let Some(geometry) = fetches.geometry() else {
            return;
        };
        fetch_start_display(interpreter, window_id, &mut fetches, start);
        IteratorWalk::from_window_start(interpreter, window_id, geometry, start).walk_rows(
            &mut fetches,
            start,
            rows,
        );
        if !fetches.fontify(interpreter, env, window_id) {
            return;
        }
    }
}

/// move_it_vertically_backward (IT, 0) from an iterator at FROM in
/// WINDOW: back_to_previous_visible_line_start -- the line before when
/// FROM begins a line -- and it2's walk from there to FROM.
pub(crate) fn fetch_screen_line_start(
    interpreter: &Interpreter,
    window_id: u64,
    fetches: &mut IteratorFetches,
    from: usize,
) {
    let Some(geometry) = fetches.geometry() else {
        return;
    };
    let line_start =
        crate::lisp::primitives::move_it::back_to_previous_visible_line_start(interpreter, from);
    fetches.begin_walk();
    IteratorWalk::at(interpreter, window_id, geometry, line_start)
        .walk_to(fetches, line_start, from);
    // move_it_vertically_backward (it, 0): the iterator itself, back at
    // the line start, then move_it_by_lines the rows down to FROM's.
    let rows =
        IteratorWalk::at(interpreter, window_id, geometry, line_start).rows_to(line_start, from);
    if rows > 0 {
        fetches.begin_walk();
        IteratorWalk::at(interpreter, window_id, geometry, line_start)
            .walk_rows(fetches, line_start, rows);
    }
}

/// start_display at START in WINDOW: init_iterator's reseat fetches
/// START, and a start inside a continued line is walked to from the
/// line's start (reseat_at_previous_visible_line_start, move_it_to).
pub(crate) fn fetch_start_display(
    interpreter: &Interpreter,
    window_id: u64,
    fetches: &mut IteratorFetches,
    start: usize,
) {
    let Some(geometry) = fetches.geometry() else {
        return;
    };
    fetches.begin_walk();
    fetches.push_stop(start);
    let origin = window_iterator_origin(interpreter, start, geometry.truncate);
    if origin < start {
        fetches.begin_walk();
        IteratorWalk::at(interpreter, window_id, geometry, origin).walk_to(fetches, origin, start);
    }
}

/// pos_visible_p's walks in WINDOW, which shows the current buffer, for
/// POS (none for t, the window's end) from START, SHOWN where it shows:
/// start_display, then above the last row up to POS, its row as well
/// when POS begins it bare (line_bottom_y); else every row to the last
/// one's end -- move_it_to stops where the last row begins (MOVE_TO_Y),
/// and the fallback's move_it_by_lines walks the row; so it does when
/// POS does not show, or is t.
pub(crate) fn fetch_pos_visible(
    interpreter: &Interpreter,
    window_id: u64,
    fetches: &mut IteratorFetches,
    geometry: WindowTextGeometry,
    (start, pos): (usize, Option<usize>),
    shown: Option<(i64, usize)>,
    text_rows: usize,
) {
    fetch_start_display(interpreter, window_id, fetches, start);
    let mut walk = IteratorWalk::from_window_start(interpreter, window_id, geometry, start);
    match (pos, shown) {
        (Some(pos), Some((_, row))) if row + 1 < text_rows => {
            let landed = walk.walk_to(fetches, start, pos);
            if walk.bare_row_start(landed) {
                walk.walk_rows(fetches, landed, 1);
            }
        }
        _ => {
            walk.walk_rows(fetches, start, text_rows);
        }
    }
}

/// handle_fontified_prop for a display iterator walking [START, END) of
/// the current buffer, shown in WINDOW: `fontification-functions' runs
/// for each position there that is not yet `fontified' as the iterator
/// reaches it, around the window's point for a buffer with long lines.
/// Answers whether it ran, after which what the iterator measured may
/// have changed, and the stop the iterator heads for at END (from START
/// on, the first).
fn fontify_iterated_text(
    interpreter: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    (start, end): (usize, usize),
    declined: Declined<'_>,
) -> (bool, usize) {
    if start >= end
        || interpreter
            .lookup_var_key(cached_symbol!("fontification-functions"), env)
            .is_none_or(|value| value.is_nil())
    {
        return (false, start);
    }
    let window_point = if window_id == interpreter.selected_window_id() {
        interpreter.buffer.borrow().point()
    } else {
        crate::lisp::primitives::window_marker_position(
            interpreter,
            window_id,
            crate::lisp::primitives::WINDOW_POINT_SLOT,
        )
        .unwrap_or_else(|| interpreter.buffer.borrow().point())
    };
    fontify_buffer_range(interpreter, env, start, end, window_point, declined)
}

/// handle_fontified_prop in a buffer with long lines (and a positive
/// `long-line-optimizations-region-size'): the functions run under the
/// large narrowing around the window's point, or around POS when POS is
/// outside it, labeled `long-line-optimizations-in-fontification-functions',
/// when that narrowing restricts the buffer.
fn narrow_for_fontification(
    interpreter: &mut Interpreter,
    env: &mut Env,
    pos: usize,
    window_point: usize,
) {
    use crate::lisp::primitives::long_lines;
    if !interpreter.buffer.borrow().long_line_optimizations_p()
        || long_lines::region_size(interpreter) <= 0
    {
        return;
    }
    let mut begv = long_lines::large_narrowing_begv(interpreter, window_point);
    let mut zv = long_lines::large_narrowing_zv(interpreter, window_point);
    if pos < begv || pos > zv {
        begv = long_lines::large_narrowing_begv(interpreter, pos);
        zv = long_lines::large_narrowing_zv(interpreter, pos);
    }
    let z = interpreter.buffer.borrow().size_total() + 1;
    if begv != 1 || zv != z {
        let label = cached_symbol!("long-line-optimizations-in-fontification-functions")
            .with(|symbol| Value::Symbol(*symbol));
        let _ = long_lines::labeled_narrow_to_region(interpreter, begv, zv, label, env);
    }
}

// The hook-call shape of handle_fontified_prop: a non-list (or a bare
// lambda) is called directly; a list runs each element, with `t'
// standing for the global value's entries.  Each call is dsafe_call1's:
// `inhibit-redisplay' bound to t, an error logged ("Error during
// redisplay: ...") and swallowed.
fn run_fontification_functions(
    interpreter: &mut Interpreter,
    env: &mut Env,
    value: &Value,
    pos: usize,
) {
    let pos_value = Value::Integer(pos as i64);
    let mut call = |interpreter: &mut Interpreter, function: Value| {
        let _ = crate::lisp::primitives::redisplay_safe_call(
            interpreter,
            function,
            std::slice::from_ref(&pos_value),
            env,
        );
    };
    let is_bare_lambda = matches!(
        value.car().map(|v| v.kind()),
        Ok(Kind::Symbol(ref name)) if name.is_named("lambda")
    );
    if !matches!(value.kind(), Kind::Cons(_)) || is_bare_lambda {
        call(interpreter, *value);
        return;
    }
    let mut rest = *value;
    while let Kind::Cons(_) = rest.kind() {
        let Ok(function) = rest.car() else {
            break;
        };
        if matches!(function.kind(), Kind::T) {
            let mut globals = interpreter
                .default_value_key(cached_symbol!("fontification-functions"))
                .unwrap_or(Value::Nil);
            while let Kind::Cons(_) = globals.kind() {
                let Ok(global_fn) = globals.car() else {
                    break;
                };
                if !matches!(global_fn.kind(), Kind::T) {
                    call(interpreter, global_fn);
                }
                let Ok(next) = globals.cdr() else {
                    break;
                };
                globals = next;
            }
        } else {
            call(interpreter, function);
        }
        let Ok(next) = rest.cdr() else {
            break;
        };
        rest = next;
    }
}

/// What the mini window shows and how its sizing went
/// (resize_mini_window): the echo row's text, whether it is an active
/// minibuffer's, its cursor, its paint, and whether the mini window's
/// height changed.
struct MiniWindowEcho {
    echo_long: PaintRow,
    echo_from_minibuffer: bool,
    echo_cursor: Option<usize>,
    echo_paint: Vec<PaintRow>,
    changed: bool,
}

/// xdisp.c resize_mini_window for what the mini window shows: its rows
/// for the echo area's message or the active minibuffer (growing only,
/// unless EXACT_ECHO sizes a message exactly), the window tree above it
/// resized to them on a COLS x ROWS frame.  A changed height is
/// resize_mini_window_apply's, which marks the frame (fset_redisplay).
fn resize_mini_window(
    interpreter: &mut Interpreter,
    env: &mut Env,
    state: &mut TtyState,
    cols: usize,
    rows: usize,
    exact_echo: bool,
) -> MiniWindowEcho {
    let menu_lines = crate::lisp::primitives::frame_top_margin(interpreter.selected_frame_id)
        .clamp(0, (rows as i64 - 2).max(0)) as usize;
    let old_rows = state.echo_rows;
    // The mini window's height comes first: a tall message shrinks the
    // window tree above it (GNU's resize_mini_window, grow-only policy).
    let max_mini = max_mini_window_rows(interpreter, rows - menu_lines, rows);
    state.echo_max_rows = max_mini.max(1);
    let (echo_long, echo_from_minibuffer, echo_cursor) = compose_echo_row(
        interpreter,
        env,
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
    // `resize-mini-windows': nil never resizes (a tty frame is never
    // minibuffer-only), `grow-only' grows until an empty echo area or an
    // exact resize shrinks it, and any other value resizes exactly.
    let policy = interpreter
        .lookup_var_key(cached_symbol!("resize-mini-windows"), env)
        .unwrap_or(Value::Nil);
    let grow_only = matches!(policy.kind(), Kind::Symbol(name) if name.is_named("grow-only"));
    state.echo_rows = if policy.is_nil() {
        state.echo_rows.max(1)
    } else if !grow_only {
        echo_paint.len().min(max_mini.max(1))
    } else if echo_empty {
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
    // the root cannot shrink).  The window accessors are C calls in GNU,
    // which take no quit: a pending C-g stays in `quit-flag' for the next
    // quit check rather than being dropped with the error.
    let _ = with_inhibit_quit(
        interpreter,
        env,
        |interpreter, env| -> Result<(), LispError> {
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
        },
    );
    if state.echo_rows != old_rows {
        let frame = interpreter.selected_frame_id;
        interpreter.fset_redisplay(frame);
    }
    MiniWindowEcho {
        echo_long,
        echo_from_minibuffer,
        echo_cursor,
        echo_paint,
        changed: state.echo_rows != old_rows,
    }
}

fn redraw_with_echo_policy(
    interpreter: &mut Interpreter,
    env: &mut Env,
    state: &mut TtyState,
    exact_echo: bool,
) -> io::Result<()> {
    state.activate_terminal(interpreter.selected_terminal_id());
    // redisplay_internal: a frame the terminal did not show last is
    // garbaged and its `tty-color-mode' set up; it paints with its
    // terminal's colors.
    interpreter.note_tty_frame_displayed(env);
    set_paint_terminal(interpreter.selected_terminal_output());
    // What a fatal signal resets the terminal with follows its size and
    // `tty-mode-reset-strings' as of this update.
    crate::lisp::primitives::refresh_emergency_reset();
    let (cols, rows) = crate::lisp::eval::terminal::output_size()?;
    // The terminal's size as init_display accepted it (at least 3x3) or
    // as a later SIGWINCH reported it: painted as it is, never widened.
    let cols = usize::from(cols.max(1));
    let rows = usize::from(rows.max(1));
    // The frame takes the terminal's size when the session starts on it
    // (init_display_interactive), and at the next redisplay after the
    // terminal changes size (handle_window_change_signal's delayed
    // change_frame_size); redisplay_internal first applies what is
    // pending, `menu-bar-lines' changes included.
    // handle_window_change_signal takes a size only above 5x2; a
    // smaller terminal leaves the frames their size.
    match state.terminal_size {
        None => interpreter.set_tty_frame_size(cols as i64, rows as i64),
        Some(size) if size != (cols, rows) && cols > 5 && rows > 2 => {
            interpreter.tty_window_change(cols as i64, rows as i64)
        }
        Some(_) => {}
    }
    state.terminal_size = Some((cols, rows));
    let _ = crate::lisp::primitives::do_pending_window_change(interpreter, env);
    // The frame's size as it stands: set-frame-size can make it smaller
    // than the terminal, whose remaining rows and columns stay as they
    // were.
    let cols = cols.min(interpreter.frame_width().max(1) as usize);
    let rows = rows.min(interpreter.frame_height().max(1) as usize);
    // redisplay_internal: a face change makes the current matrices
    // unusable, and clear_garbaged_frames has a garbaged (or resized)
    // frame redisplayed whole.
    if interpreter.face_change {
        interpreter.windows_or_buffers_changed = 47;
    }
    if crate::lisp::primitives::frame_garbaged() || state.painted_size != (cols, rows) {
        let frame = interpreter.selected_frame_id;
        interpreter.fset_redisplay(frame);
    }
    prepare_menu_bars(interpreter, env);
    selected_window_mode_line_checks(interpreter, env, state);
    // Rows above the window tree belong to the frame's menu and tab bars.
    let menu_lines = crate::lisp::primitives::frame_top_margin(interpreter.selected_frame_id)
        .clamp(0, (rows as i64 - 2).max(0)) as usize;
    let MiniWindowEcho {
        echo_long,
        echo_from_minibuffer,
        echo_cursor,
        echo_paint,
        ..
    } = resize_mini_window(interpreter, env, state, cols, rows, exact_echo);
    // xdisp.c:redisplay_internal runs the window change functions once
    // the mini window has its size; their errors are already muted.
    let _ = crate::lisp::primitives::run_window_change_functions(interpreter, env);
    // Code that sets windows_or_buffers_changed does not say whether the
    // mode lines need a refresh too: they do.
    if interpreter.windows_or_buffers_changed != 0 && interpreter.update_mode_lines == 0 {
        interpreter.update_mode_lines = if interpreter.windows_or_buffers_changed
            == crate::lisp::eval::runtime::REDISPLAY_SOME
        {
            crate::lisp::eval::runtime::REDISPLAY_SOME
        } else {
            32
        };
    }
    // Arrows whose specs changed have their buffers redisplayed, to remove
    // any arrow that should no longer exist.
    redisplay_flags::overlay_arrows_changed(interpreter);
    let mut consider_all_windows =
        interpreter.update_mode_lines != 0 || interpreter.windows_or_buffers_changed != 0;
    {
        let (all_cause, mode_lines_cause) = (
            interpreter.windows_or_buffers_changed,
            interpreter.update_mode_lines,
        );
        redisplay_flags::count_redisplay_cause(
            interpreter,
            env,
            cached_symbol!("redisplay--all-windows-cause"),
            all_cause,
        );
        redisplay_flags::count_redisplay_cause(
            interpreter,
            env,
            cached_symbol!("redisplay--mode-lines-cause"),
            mode_lines_cause,
        );
    }
    let frame_rows = rows - state.echo_rows; // everything above the echo area
    let full_repaint = state.painted_size != (cols, rows);
    // frame_garbaged: the terminal's screen is not what was painted (the
    // modes reentered after a suspension, a SIGWINCH), so the whole frame
    // is painted again (clear_garbaged_frames's redraw_frame).
    let garbaged = crate::lisp::primitives::take_frame_garbaged();
    if full_repaint {
        // A resize changes the wrap geometry under every saved segment
        // index; re-anchor each window instead of trusting stale ones.
        state.views.clear();
    }
    // init_iterator frees the realized faces after a face change (every
    // frame's for face_change, the frame's for its own); the faces cached
    // here are the displayed frame's, as GNU realizes faces per frame.
    let displayed_frame = interpreter.selected_frame_id;
    interpreter.init_iterator_faces(displayed_frame);
    let face_epoch = (
        displayed_frame,
        displayed_frame.borrow().realized_faces_epoch,
    );
    let face_changed = state.face_cache_epoch != Some(face_epoch);
    if full_repaint || face_changed {
        state.face_cache.clear();
        state.face_cache_epoch = Some(face_epoch);
    }
    // PRODUCE_GLYPHS of what follows: the realized faces glyphs now use
    // may not be freed (inhibit_free_realized_faces) until a redisplay
    // starts afresh.
    interpreter.set_variable_key(cached_symbol!("inhibit-free-realized-faces"), Value::T, env);
    // The frame's default face (realize_basic_faces's DEFAULT_FACE_ID, no
    // buffer's remapping), which every glyph's face is merged over, and
    // the variables turn_on_face and realize_tty_face read.  A window
    // whose buffer remaps `default' has its text merged over that.
    let frame_faces = face_remapping(interpreter, None);
    let default_face = realized_face(
        &mut state.face_cache,
        interpreter,
        env,
        &frame_faces,
        &Value::symbol("default"),
    );
    let inverse_video = interpreter
        .lookup_var_key(cached_symbol!("inverse-video"), env)
        .is_some_and(|value| value.is_truthy());
    set_paint_faces(
        default_face,
        inverse_video,
        interpreter.tty_suppress_bold_inverse_default_colors,
    );
    // free_realized_faces's clear_current_matrices: a face definition,
    // `inverse-video' or the bold suppression changing has every row
    // written again, the unchanged text in its new appearance.
    let paint_faces = (
        default_face,
        inverse_video,
        interpreter.tty_suppress_bold_inverse_default_colors,
    );
    if face_changed || state.painted_faces != Some(paint_faces) {
        state.painted_faces = Some(paint_faces);
        for row in &mut state.painted_rows {
            *row = PaintRow::unpainted();
        }
        state.painted_echo = Vec::new();
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

    let mut frame = vec![PaintRow::blank(cols); frame_rows];
    // GNU's tty vertical border draws in the vertical-border face (its
    // inherit chain reaches mode-line-inactive's reverse video).
    let divider_attrs = if layout.iter().any(|info| info.left + info.width < cols) {
        realized_face(
            &mut state.face_cache,
            interpreter,
            env,
            &frame_faces,
            &Value::symbol("vertical-border"),
        )
    } else {
        CellAttrs::default()
    };
    // windows_or_buffers_changed as redisplay_window reads it: window
    // starts on continuation lines adjusted, no current matrix trusted.
    // Window functions flag a change of the window tree's geometry
    // (fset_redisplay); one that reaches the frame unflagged still has
    // every window redisplayed.
    let windows_or_buffers_changed = {
        let signature: Vec<(u64, usize, usize, usize, usize)> = layout
            .iter()
            .map(|info| (info.window_id, info.left, info.top, info.width, info.height))
            .collect();
        if signature != state.last_layout && !consider_all_windows {
            let frame = interpreter.selected_frame_id;
            interpreter.fset_redisplay(frame);
            consider_all_windows = true;
        }
        state.last_layout = signature;
        interpreter.windows_or_buffers_changed != 0 || full_repaint || face_changed
    };
    // The windows redisplay_window redisplays: with every window
    // considered, those not under REDISPLAY_SOME's exemption (their
    // buffers' flags copied to them first, propagate_buffer_redisplay);
    // else the selected window alone.
    let window_ids: Vec<u64> = layout.iter().map(|info| info.window_id).collect();
    if consider_all_windows {
        redisplay_flags::propagate_buffer_redisplay(interpreter, &window_ids);
    }
    let redisplayed: std::collections::HashSet<u64> = layout
        .iter()
        .filter(|info| {
            if consider_all_windows {
                !redisplay_flags::needs_no_redisplay(interpreter, info.window_id)
            } else {
                info.selected
            }
        })
        .map(|info| info.window_id)
        .collect();
    // The windows redisplay_window leaves alone keep what the terminal
    // shows of them (their current matrices): every window other than the
    // selected one that is not redisplayed and whose cells the last paint
    // has where the window still is.
    let painted_valid = !full_repaint && !garbaged && state.painted_rows.len() == frame_rows;
    let kept: std::collections::HashSet<u64> = layout
        .iter()
        .filter(|info| {
            painted_valid
                && !info.selected
                && !redisplayed.contains(&info.window_id)
                && state.views.get(&info.window_id).is_some_and(|view| {
                    view.painted_rect == Some((info.top, info.left, info.width, info.height))
                })
        })
        .map(|info| info.window_id)
        .collect();
    // redisplay_window finds whether a window's buffer holds long lines
    // before its iterator does anything: before fontification runs.
    for info in layout.iter() {
        if redisplayed.contains(&info.window_id) {
            long_lines::note_long_lines(interpreter, info.buffer_id);
        }
    }
    // A window's one redisplay offers a position its functions declined
    // once; the walks planning it fetch the text (fontify_window_steps).
    let mut declined_by_window: std::collections::HashMap<u64, Vec<usize>> =
        std::collections::HashMap::new();

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
        /// redisplay_window displays the window's mode lines
        /// (display_mode_lines); otherwise the frame keeps their cells.
        display: bool,
    }
    impl ModeLineCellsJob for ModeLineJob {
        fn cells(&self) -> (usize, usize, usize) {
            (self.row, self.left, self.body_width)
        }
    }
    let mut mode_line_jobs: Vec<ModeLineJob> = Vec::new();
    // finish_menu_bars: the frame's selected window redisplayed with its
    // mode line to update redoes the menu bar row.
    let mut menu_bar_display =
        selected_mini_window_updates_menu_bar(interpreter, &layout, consider_all_windows);
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
        /// The display's first line, laid out from a start inside it.
        anchor: Option<Anchor>,
    }
    let mut text_face_jobs: Vec<TextFaceJob> = Vec::new();
    // Each window's text area (top, rows, left, width) and buffer, whose
    // `default' remapping its glyphs' faces are merged over.
    let mut window_default_jobs: Vec<(usize, usize, usize, usize, u64)> = Vec::new();
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
        anchor: Option<Anchor>,
    }
    // A string whose `display' property targets a window margin (most
    // visibly Flymake's TTY diagnostic indicators).  The source string
    // lives in an overlay before-string; the displayed payload and its
    // faces paint into the reserved margin rather than the text body.
    struct MarginStringJob {
        buffer_id: u64,
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
        if kept.contains(&info.window_id) {
            // Its text, mode line and border as the last paint left them.
            for row in info.top..(info.top + info.height).min(frame_rows) {
                keep_painted_cells(
                    &mut frame,
                    &state.painted_rows,
                    &(row, info.left, info.width),
                );
            }
            continue 'windows;
        }
        // A window not flush with the frame's right edge spends its last
        // column on the vertical border; the mode line spans the body.
        let body_width = if info.left + info.width < cols {
            info.width - 1
        } else {
            info.width
        };
        let has_header_line = interpreter
            .buffer_local_value_key(info.buffer_id, cached_symbol!("header-line-format"))
            .or_else(|| interpreter.default_value_key(cached_symbol!("header-line-format")))
            .is_some_and(|format| format.is_truthy());
        let has_tab_line = interpreter
            .buffer_local_value_key(info.buffer_id, cached_symbol!("tab-line-format"))
            .or_else(|| interpreter.default_value_key(cached_symbol!("tab-line-format")))
            .is_some_and(|format| format.is_truthy());
        let tab_rows = usize::from(has_tab_line && info.height > 2);
        let header_rows = usize::from(has_header_line && info.height > 2 + tab_rows);
        let text_top = info.top + tab_rows + header_rows;
        let text_rows = info.height - 1 - tab_rows - header_rows;
        window_default_jobs.push((text_top, text_rows, info.left, body_width, info.buffer_id));
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
        let cursor_movement_usable = cursor_movement_usable(interpreter, env, info);
        let (mut start_state, restriction, modiff) = window_start_state(
            interpreter,
            info,
            &previous_view,
            text_rows,
            tab_rows + header_rows,
            // redisplay_window reads it as each window comes: a buffer an
            // earlier window's fontification changed sets it for the
            // windows after (bset_redisplay).
            windows_or_buffers_changed || interpreter.windows_or_buffers_changed != 0,
            cursor_movement_usable,
        );
        let mut hook_rounds = 0usize;
        let mut planned_info = *info;
        let (geometry, plan, chosen, chars_modiff, published) = loop {
            let info = &planned_info;
            // hscroll_window_tree's cursor row: where it begins when the
            // display starts inside point's line, as the plan decides.
            let mut cursor_row_start = {
                let buffer = if info.buffer_id == interpreter.current_buffer_id() {
                    Some(interpreter.buffer.borrow())
                } else {
                    interpreter.get_buffer_by_id(info.buffer_id)
                };
                buffer.and_then(|buffer| cursor_row_origin(&buffer, info.start, info.point))
            };
            let mut hscroll_retried = false;
            let mut fontify_passes = 0usize;
            // What the functions declined in the walks before a
            // `window-scroll-functions' round.
            let mut declined_before_hook: Vec<usize> = Vec::new();
            let (geometry, plan, chosen, chars_modiff) = loop {
                let outcome = loop {
                    let geometry = window_render_geometry(
                        interpreter,
                        env,
                        info,
                        text_body_width,
                        cols,
                        lnum,
                        cursor_row_start,
                    );
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
                            update_mode_line,
                            consumed,
                            steps,
                        } => break Err((stage, start, update_mode_line, consumed, steps)),
                        PlanOutcome::Planned {
                            plan,
                            chosen,
                            consumed,
                            steps,
                        } => {
                            // The row the plan shows point on decides the
                            // hscroll (hscroll_windows after the window's
                            // display, then retry_frame).
                            if geometry.truncate
                                && plan.cursor_origin != cursor_row_start
                                && !hscroll_retried
                            {
                                hscroll_retried = true;
                                cursor_row_start = plan.cursor_origin;
                                continue;
                            }
                            let top_line = view.top_line;
                            let settled = lnum_for(interpreter, top_line);
                            if settled.map(|layout| layout.cols) == lnum.map(|layout| layout.cols) {
                                break Ok((
                                    geometry,
                                    plan,
                                    chosen,
                                    consumed,
                                    buffer.chars_modified_tick(),
                                    steps,
                                ));
                            }
                            lnum = settled;
                        }
                    }
                };
                match outcome {
                    Err((stage, start, update_mode_line, consumed, steps)) => {
                        // What the walks before the hook fetched is fontified
                        // first (try_window at the old start, recenter's), and
                        // the start the hook sees is planned again while the
                        // functions ran, as GNU decides it after its walks.
                        if fontify_window_steps(
                            interpreter,
                            env,
                            info,
                            &steps,
                            text_rows,
                            &mut declined_before_hook,
                        ) && fontify_passes < ITERATOR_FONTIFICATION_PASSES
                        {
                            fontify_passes += 1;
                            continue;
                        }
                        // The walks after the hook are new iterators.
                        declined_before_hook.clear();
                        // run_window_scroll_functions: the hooks see the start
                        // redisplay chose, and redisplay continues from the
                        // start they leave.
                        consume_window_start_flags(interpreter, info.window_id, consumed);
                        crate::lisp::primitives::set_window_start_marker(
                            interpreter,
                            info.window_id,
                            start,
                        );
                        run_window_scroll_functions(interpreter, env, info, start);
                        start_state.resume = Some((
                            stage,
                            crate::lisp::primitives::window_start_marker(
                                interpreter,
                                info.window_id,
                                info.buffer_id,
                            ),
                            update_mode_line,
                        ));
                        hook_rounds += 1;
                        if hook_rounds >= 4 {
                            start_state.scroll_functions = false;
                        }
                    }
                    Ok((geometry, plan, chosen, consumed, chars, steps)) => {
                        // handle_fontified_prop as the walks reach the text:
                        // what the functions change is planned again.
                        if fontify_window_steps(
                            interpreter,
                            env,
                            info,
                            &steps,
                            text_rows,
                            declined_by_window.entry(info.window_id).or_default(),
                        ) && fontify_passes < ITERATOR_FONTIFICATION_PASSES
                        {
                            fontify_passes += 1;
                            continue;
                        }
                        consume_window_start_flags(interpreter, info.window_id, consumed);
                        break (geometry, plan, chosen, chars);
                    }
                }
            };
            let published = publish_window_start(interpreter, info, &chosen, plan.window_end);
            // The force_start block moved point into the window: with the
            // window's redisplay done but for the cursor, `pre-redisplay-function'
            // runs again for it, and what it marks is displayed again from the
            // same start (try_window).
            if chosen.point.is_some()
                && rerun_pre_redisplay_function(interpreter, env, info.window_id, chosen.cursor_row)
            {
                planned_info = published;
                start_state.point = published.point;
                start_state.resume = Some((
                    redisplay_window::Stage::ForceStart,
                    chosen.start,
                    chosen.update_mode_line,
                ));
                continue;
            }
            break (geometry, plan, chosen, chars_modiff, published);
        };
        if let Some((hscroll, suspended, old_point)) = geometry.store {
            crate::lisp::primitives::store_window_hscroll_state(
                interpreter,
                info.window_id,
                hscroll,
                suspended,
                old_point,
            );
        }
        let info = &published;
        let mode_line_point = info.point;
        let lnum_cols = geometry.lnum.map_or(0, |layout| layout.cols);
        let text_left = info.left + left_margin + lnum_cols;
        for (row, (rendered, _, _, _, _)) in plan.rendered.iter().enumerate() {
            frame[text_top + row].blit(text_left, rendered, CellAttrs::default());
        }
        {
            let mut variables = interpreter
                .lookup_var_key(
                    cached_symbol!("overlay-arrow-variable-list"),
                    &crate::lisp::types::Env::new(),
                )
                .unwrap_or(Value::Nil);
            while let Kind::Cons(_) = variables.kind() {
                let Ok(symbol_value) = variables.car() else {
                    break;
                };
                variables = variables.cdr().unwrap_or(Value::Nil);
                let Some(symbol) = symbol_value.symbol_cell() else {
                    continue;
                };
                let Some(Kind::Marker(marker)) = interpreter
                    .lookup_var_symbol(&symbol, &crate::lisp::types::Env::new())
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
                    .get_property_of_symbol_value(symbol_value, "overlay-arrow-string")
                    .filter(|value| value.is_string())
                    .or_else(|| {
                        interpreter.lookup_var_key(
                            cached_symbol!("overlay-arrow-string"),
                            &crate::lisp::types::Env::new(),
                        )
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
                        buffer_id: info.buffer_id,
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
                        buffer_id: info.buffer_id,
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
                anchor: plan.anchor,
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
            anchor: plan.anchor,
        });
        if body_width < info.width {
            for row in 0..info.height {
                frame[info.top + row].blit(info.left + body_width, "|", divider_attrs);
            }
        }
        let metrics = crate::lisp::primitives::InteractiveWindowMetrics {
            window_end: plan.window_end,
        };
        let to_frame = |(row, col): (usize, usize)| {
            (
                (info.left + left_margin + col).min(cols - 1) as u16,
                (text_top + row).min(frame_rows - 1) as u16,
            )
        };
        let frame_cursor = plan.point_cursor.map(to_frame);
        if info.selected {
            if let Some(position) = plan.cursor.map(to_frame) {
                cursor_position = position;
            }
            selected_sync = Some(metrics);
        }
        // mark_window_display_accurate_1: the buffer's modification count
        // once its display is done, `fontification-functions' and the
        // other redisplay Lisp included.
        {
            let view = state.views.entry(info.window_id).or_default();
            view.frame_cursor = frame_cursor;
            view.last_start = Some(info.start);
            view.last_restriction = Some(restriction);
            view.last_modiff = Some(modiff);
            view.last_buffer_id = Some(info.buffer_id);
            view.last_point = Some(info.point);
            view.last_cursor = plan.cursor;
            if let Some((row, _)) = plan.point_cursor {
                view.last_cursor_y = Some(row);
            }
            view.last_top_pos = Some(plan.top_pos);
            view.last_chars_modiff = Some(chars_modiff);
            view.painted_rect = Some((info.top, info.left, info.width, info.height));
        }
        let display = window_displays_mode_line(
            interpreter,
            env,
            info,
            cols,
            redisplayed.contains(&info.window_id),
            consider_all_windows,
            chosen.update_mode_line,
        );
        if info.selected && display.update_mode_line {
            menu_bar_display = true;
        }
        let display = display.mode_lines;
        let mode_line_active = info.selected
            || interpreter
                .lookup_var_key(cached_symbol!("mode-line-in-non-selected-windows"), env)
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
            display,
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
                display,
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
                display,
            });
        }
    }

    // Publish the selected window's displayed geometry to the interpreter
    // (each window's start and end went to its record as it was planned).
    // This precedes the mode-line renders.
    if let Some(metrics) = selected_sync {
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
        let remapping = face_remapping(interpreter, Some(job.buffer_id));
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
                let attrs =
                    realized_face(&mut state.face_cache, interpreter, env, &remapping, face);
                (*begin, *end, attrs)
            })
            .collect();
        let glyphless_attrs = realized_face(
            &mut state.face_cache,
            interpreter,
            env,
            &remapping,
            &Value::symbol("glyphless-char"),
        );
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
        // The rows of one continued line share its layout: it is made once
        // for them, not once a row.
        let mut line_layout: Option<(usize, VisualLine, layout::LineLayout)> = None;
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
            if line_layout
                .as_ref()
                .is_none_or(|(cached, _, _)| cached != line)
            {
                let visual =
                    glyphless_visual_line_at(&buffer, &job_invisibility, *line, Some(&glyphless));
                // The display's first line, from the start inside it.
                let laid = match job.anchor.filter(|anchor| anchor.line == *line) {
                    Some(anchor) => {
                        let from = visual.start_index(
                            anchor
                                .origin
                                .saturating_sub(buffer.display_line_start_of(*line)),
                            anchor.walked,
                        );
                        layout::lay_out_visual_line_from(
                            &visual,
                            job.body_width,
                            job.truncate,
                            from,
                            anchor.walked,
                        )
                    }
                    None => layout::lay_out_visual_line(&visual, job.body_width, job.truncate),
                };
                line_layout = Some((*line, visual, laid));
            }
            let Some((_, visual, laid)) = line_layout.as_ref() else {
                continue;
            };
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
                HscrollLayout::new(visual, &laid.columns, *row_hscroll, job.lnum_cols == 0);
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
            let attrs = realized_face(&mut state.face_cache, interpreter, env, &remapping, &face);
            let row = &mut frame[row_index];
            if crate::lisp::types::eq_symbol_named(face, "default") {
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
        let remapping = face_remapping(interpreter, Some(job.buffer_id));
        let default = realized_face(
            &mut state.face_cache,
            interpreter,
            env,
            &remapping,
            &Value::symbol("default"),
        );
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
            let attrs = realized_face(&mut state.face_cache, interpreter, env, &remapping, &face);
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
        let remapping = face_remapping(interpreter, Some(job.buffer_id));
        let mut resolve = |name: &str| -> CellAttrs {
            realized_face(
                &mut state.face_cache,
                interpreter,
                env,
                &remapping,
                &Value::symbol(name),
            )
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
            // The row of POS's display line (the line's own layout,
            // from its start) showing POS.
            let unanchored_seg = |vline: usize, pos: usize| {
                if job.truncate {
                    return 0;
                }
                let visual = glyphless_visual_line_at(&buffer, &spec, vline, Some(&glyphless));
                let offset = pos.saturating_sub(buffer.display_line_start_of(vline));
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
            let point_vline = visual_line_first_line(&buffer, &spec, point_line_abs);
            if let Some(anchor) = job
                .anchor
                .filter(|anchor| anchor.walked && point >= anchor.origin)
            {
                // display_count_lines_visually: start_display's walk to
                // a start inside a row numbered the rows it passed, from
                // the line's start, and each row displayed after counts
                // one more (it->lnum + 1) -- from the row the start is
                // on, one row short of point's.
                let start_seg = unanchored_seg(anchor.line, anchor.origin);
                rows_between(
                    (anchor.line, start_seg),
                    (point_vline, unanchored_seg(point_vline, point)),
                ) - 1
            } else if let Some(row) = job.cursor_row {
                row as i64
            } else if let Some(index) = (0..job.rows.len()).find(|&index| contains_point(index)) {
                index as i64
            } else {
                let point_seg = unanchored_seg(point_vline, point);
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
            // A display started right after a newline (an invisible one
            // joining its line to the one before) is not walked to:
            // maybe_produce_line_number counts the lines up to the start.
            let number_line = match job.anchor {
                Some(anchor) if !anchor.walked && anchor.line == line && seg == 0 => {
                    buffer.line_number_at_pos(anchor.origin)
                }
                _ => line,
            };
            let value: i64 = if beyond {
                0
            } else {
                match layout.mode {
                    LnMode::Absolute => (displayed(number_line) + layout.offset).abs(),
                    LnMode::Relative => {
                        let distance = (displayed(number_line) - displayed(point_line_abs)).abs();
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
            // A display started inside its line by walking there: the
            // walk already produced the line's number (it->lnum), so its
            // first row repeats none -- unless the iterator was restored
            // to a string at the start.
            let walked = job.anchor.is_some_and(|anchor| {
                anchor.walked && anchor.line == line && !(seg == 0 && anchor.numbered)
            });
            let blank = beyond || (layout.mode != LnMode::Visual && (seg > 0 || walked));
            let text = if blank {
                " ".repeat(layout.cols)
            } else {
                format!("{:>width$} ", value, width = layout.width + 1)
            };
            let is_current = !beyond
                && number_attrs != current_attrs
                && match layout.mode {
                    LnMode::Visual => row_index as i64 == point_row,
                    _ => number_line == point_line_abs,
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

    // A window whose buffer remaps `default': every glyph of its text area
    // (blank rows and margins included, extend_face_to_end_of_line's
    // spaces) realized over that default instead of the frame's.
    for (top, rows, left, width, buffer_id) in window_default_jobs {
        let remapping = face_remapping(interpreter, Some(buffer_id));
        if remapping.alist.is_nil() {
            continue;
        }
        let window_default = realized_face(
            &mut state.face_cache,
            interpreter,
            env,
            &remapping,
            &Value::symbol("default"),
        );
        if window_default == default_face {
            continue;
        }
        for row in frame.iter_mut().skip(top).take(rows) {
            let end = (left + width).min(row.attrs.len());
            for attrs in &mut row.attrs[left.min(end)..end] {
                *attrs = merge_cell_attrs(window_default, *attrs);
            }
        }
    }

    // Mode lines: each window's real `mode-line-format', rendered by the
    // interpreter's engine in that window's context and painted in the
    // realized mode-line face.
    // CURRENT_MODE_LINE_ACTIVE_FACE_ID: `mode-line-active' for the
    // selected window (and the window a selected minibuffer was entered
    // from), `mode-line-inactive' for the others.
    // Each face as the window's buffer remaps it (lookup_basic_face).
    for job in &mode_line_jobs {
        if !job.display && keep_painted_cells(&mut frame, &state.painted_rows, job) {
            continue;
        }
        crate::lisp::primitives::begin_display_mode_lines(interpreter, job.window_id);
        let remapping = face_remapping(interpreter, Some(job.buffer_id));
        let measure = ModeLineMeasure {
            buffer_id: job.buffer_id,
            window_id: job.window_id,
        };
        // display_mode_lines runs with `inhibit-quit' bound to t.
        let rendered = with_inhibit_quit(interpreter, env, |interpreter, env| {
            crate::lisp::primitives::render_window_mode_line(
                interpreter,
                env,
                job.window_id,
                job.point,
                job.metrics,
                job.body_width,
                &measure,
            )
        });
        let (mut mode_line, mut spans) = match rendered {
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
        let mode_line_attrs = realized_face(
            &mut state.face_cache,
            interpreter,
            env,
            &remapping,
            &Value::symbol(if job.active {
                "mode-line-active"
            } else {
                "mode-line-inactive"
            }),
        );
        frame[job.row].blit(job.left, &mode_line, mode_line_attrs);
        // The renderer's face spans (the buffer name's mode-line-buffer-id)
        // layer over the base mode-line face, clipped to the body.
        for (from, to, face) in spans {
            let attrs = realized_face(&mut state.face_cache, interpreter, env, &remapping, &face);
            let from = job.left + from.min(shown);
            let to = job.left + to.min(shown);
            frame[job.row].overlay(from, to, attrs);
        }
        crate::lisp::primitives::end_display_mode_lines(interpreter, job.window_id);
    }

    // Tab lines sit above header lines and use the `tab-line' face.  A
    // package can install an evaluated per-buffer format here without
    // enabling the global tab-bar UI (Flycheck's grouping controls do so).
    if !tab_line_jobs.is_empty() {
        for job in &tab_line_jobs {
            if !job.display && keep_painted_cells(&mut frame, &state.painted_rows, job) {
                continue;
            }
            let remapping = face_remapping(interpreter, Some(job.buffer_id));
            let tab_attrs = realized_face(
                &mut state.face_cache,
                interpreter,
                env,
                &remapping,
                &Value::symbol("tab-line"),
            );
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
                let attrs =
                    realized_face(&mut state.face_cache, interpreter, env, &remapping, &face);
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
        for job in &header_line_jobs {
            if !job.display && keep_painted_cells(&mut frame, &state.painted_rows, job) {
                continue;
            }
            let remapping = face_remapping(interpreter, Some(job.buffer_id));
            let header_attrs = realized_face(
                &mut state.face_cache,
                interpreter,
                env,
                &remapping,
                &Value::symbol("header-line"),
            );
            let measure = ModeLineMeasure {
                buffer_id: job.buffer_id,
                window_id: job.window_id,
            };
            let rendered = with_inhibit_quit(interpreter, env, |interpreter, env| {
                crate::lisp::primitives::render_window_header_line(
                    interpreter,
                    env,
                    job.window_id,
                    job.point,
                    job.metrics,
                    job.body_width,
                    &measure,
                )
            });
            let (mut header, mut spans) = match rendered {
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
                let attrs =
                    realized_face(&mut state.face_cache, interpreter, env, &remapping, &face);
                let from = job.left + from.min(shown);
                let to = job.left + to.min(shown);
                frame[job.row].overlay(from, to, attrs);
            }
        }
    }

    // The menu bar row (GNU's display_menu_bar): FRAME_MENU_BAR_ITEMS'
    // names, each padded with one space and drawn over the `menu' face
    // with its own face properties, their columns recorded on the frame.
    if menu_lines > 0
        && !frame.is_empty()
        && !(!menu_bar_display
            && state
                .painted_rows
                .first()
                .filter(|row| row.text.len() == cols && row.text != ['\u{0}'])
                .map(|row| frame[0] = row.clone())
                .is_some())
    {
        let menu_attrs = realized_face(
            &mut state.face_cache,
            interpreter,
            env,
            &frame_faces,
            &Value::symbol("menu"),
        );
        let mut row = PaintRow::blank(cols);
        row.attrs = vec![menu_attrs; cols];
        let items = crate::lisp::primitives::menu::display_menu_bar_items(
            interpreter,
            interpreter.selected_frame_id,
        );
        for (string, column) in items {
            let Some(text) = crate::lisp::primitives::string_like(&string) else {
                continue;
            };
            row.blit(column, &text.text, menu_attrs);
            if let Some(props) = &text.props {
                for (from, to, plist) in props.iter() {
                    let face = crate::intervals::plist_get(plist, Value::symbol("face"));
                    if face.is_nil() {
                        continue;
                    }
                    let attrs = merge_cell_attrs(
                        menu_attrs,
                        realized_face(&mut state.face_cache, interpreter, env, &frame_faces, &face),
                    );
                    row.overlay((column + from).min(cols), (column + to).min(cols), attrs);
                }
            }
        }
        frame[0] = row;
    }

    // Emit only rows that changed since the last paint (dispnew's
    // current-matrix idea, one line deep): a self-insert repaints one
    // text row, not the frame.
    if full_repaint || garbaged {
        state.painted_echo = Vec::new();
        state.painted_size = (cols, rows);
        state.painted_rows.clear();
    }
    state.painted_rows.resize(frame_rows, PaintRow::unpainted());

    encode_terminal_cells(interpreter, &mut frame);
    let mut out = terminal_output();
    queue!(out, cursor::Hide)?;
    if full_repaint || garbaged {
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
    // update_frame_1's cursor: in the echo area with `cursor-in-echo-area'
    // or a message shown over the selected active minibuffer; otherwise
    // the selected window's, echoing included.
    state.window_cursor = cursor_position;
    if (minibuffer_echo_owns_hardware_cursor(interpreter, echo_from_minibuffer)
        || (!echo_from_minibuffer
            && interpreter.active_minibuffer_buffer_id().is_some()
            && interpreter.selected_window_id() == interpreter.minibuffer_window_id()))
        && let Some(position) = echo_cursor
    {
        let (row, col) = wrapped_echo_cursor(&echo_long, position, cols, state.echo_rows);
        cursor_position = (
            col as u16,
            (frame_rows + row).min(rows.saturating_sub(1)) as u16,
        );
    } else if interpreter
        .lookup_var_key(cached_symbol!("cursor-in-echo-area"), env)
        .is_some_and(|value| value.is_truthy())
    {
        // At the end of the echo row's text, its first column when empty.
        let (row, col) =
            wrapped_echo_cursor(&echo_long, echo_cursor.unwrap_or(0), cols, state.echo_rows);
        cursor_position = (
            col as u16,
            (frame_rows + row).min(rows.saturating_sub(1)) as u16,
        );
    }
    state.update_cursor = cursor_position;
    queue!(
        out,
        cursor::MoveTo(cursor_position.0, cursor_position.1),
        cursor::Show
    )?;
    mark_redisplay_done(interpreter, &window_ids, consider_all_windows);
    out.flush()
}

/// Run the Lisp redisplay coordinator before taking any buffer snapshots.
///
/// GNU's redisplay calls `pre-redisplay-function' with t when every window is
/// being considered.  Among other things, simple.el uses this to maintain the
/// window-local cursor-face overlay in *Completions*.  Like GNU's dsafe call,
/// redisplay must survive a callback error and continue painting the frame.
fn run_pre_redisplay_function(interpreter: &mut Interpreter, env: &mut Env, windows: Value) {
    let Some(function) = interpreter
        .lookup_var_key(cached_symbol!("pre-redisplay-function"), env)
        .filter(Value::is_truthy)
    else {
        return;
    };
    // dsafe_calln (true, Vpre_redisplay_function, windows).
    dsafe_call(interpreter, env, true, function, &[windows]);
}

/// redisplay_window's force_start block, point moved into WINDOW_ID: the
/// window's and its buffer's redisplay flags cleared (all but the cursor
/// is done, and propagate_buffer_redisplay passed the buffer's on), then
/// `pre-redisplay-function' called with the window alone so that it can
/// update the region for the new point.  Whether the window must be
/// displayed again: the function marked it or its buffer, or relative or
/// visual line numbers count from a row point moved off the first.
fn rerun_pre_redisplay_function(
    interpreter: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    cursor_row: Option<usize>,
) -> bool {
    use crate::lisp::primitives::{WINDOW_REDISPLAY_SLOT, set_window_flag, window_flag};
    set_window_flag(interpreter, window_id, WINDOW_REDISPLAY_SLOT, false);
    let buffer_id = redisplay_flags::window_buffer(interpreter, window_id);
    if let Some(mut buffer) = buffer_id.and_then(|id| interpreter.get_buffer_by_id_mut(id)) {
        buffer.set_text_redisplay(false);
    }
    let window = Value::list([interpreter.record_value(window_id)]);
    run_pre_redisplay_function(interpreter, env, window);
    let relative_numbers = interpreter
        .lookup_var_key(cached_symbol!("display-line-numbers"), env)
        .is_some_and(|value| {
            crate::lisp::types::eq_symbol_named(value, "relative")
                || crate::lisp::types::eq_symbol_named(value, "visual")
        });
    window_flag(interpreter, window_id, WINDOW_REDISPLAY_SLOT)
        || buffer_id
            .and_then(|id| interpreter.get_buffer_by_id(id))
            .is_some_and(|buffer| buffer.text_redisplay())
        || (relative_numbers && cursor_row.is_some_and(|row| row != 0))
}

/// xdisp.c:prepare_menu_bars: `pre-redisplay-function' called with the
/// windows redisplay will consider (t for every window, nil when only
/// the selected one), then the menu bar items recomputed where
/// update_menu_bar asks: on each frame something marked when any window
/// may change, else on the selected frame.
fn prepare_menu_bars(interpreter: &mut Interpreter, env: &mut Env) {
    let all_windows =
        interpreter.windows_or_buffers_changed != 0 || interpreter.update_mode_lines != 0;
    let some_windows = redisplay_flags::redisplay_some_p(interpreter);
    let windows = if !all_windows {
        Value::Nil
    } else if !some_windows {
        Value::T
    } else {
        // Cf. conditions for redisplaying a window at the beginning of
        // redisplay_window: those needs_no_redisplay does not exempt.
        let mut windows = Value::Nil;
        for window_id in crate::lisp::primitives::frames_window_list(interpreter) {
            if !redisplay_flags::needs_no_redisplay(interpreter, window_id) {
                windows = Value::cons(interpreter.record_value(window_id), windows);
            }
        }
        windows
    };
    run_pre_redisplay_function(interpreter, env, windows);
    // update_menu_bar recomputes when the windows or buffers changed, any
    // mode line is to be updated, or the selected window's buffer was
    // modified or saved since its mode line showed it.
    let wants_update = |interpreter: &Interpreter, frame: crate::lisp::types::FrameRef| {
        let window = frame.borrow().selected_window_id();
        interpreter.windows_or_buffers_changed != 0
            || interpreter.update_mode_lines != 0
            || redisplay_flags::window_buffer_changed(interpreter, window)
    };
    if all_windows {
        let mut hooks_run = false;
        let frames: Vec<_> = interpreter
            .frame_states
            .iter()
            .copied()
            .filter(|frame| frame.is_live())
            .collect();
        for frame in frames {
            let window = frame.borrow().selected_window_id();
            if some_windows
                && !frame.borrow().redisplay
                && !crate::lisp::primitives::window_flag(
                    interpreter,
                    window,
                    crate::lisp::primitives::WINDOW_REDISPLAY_SLOT,
                )
                && !redisplay_flags::window_buffer(interpreter, window)
                    .and_then(|buffer| {
                        interpreter
                            .get_buffer_by_id(buffer)
                            .map(|b| b.text_redisplay())
                    })
                    .unwrap_or(false)
            {
                continue;
            }
            let changed = wants_update(interpreter, frame);
            match crate::lisp::primitives::menu::update_menu_bar(
                interpreter,
                frame,
                false,
                hooks_run,
                changed,
                env,
            ) {
                Ok(run) => hooks_run = run,
                Err(error) => debug_log(&format!("update_menu_bar: {error:?}")),
            }
        }
    } else {
        let frame = interpreter.selected_frame_id;
        let changed = wants_update(interpreter, frame);
        if let Err(error) = crate::lisp::primitives::menu::update_menu_bar(
            interpreter,
            frame,
            true,
            false,
            changed,
            env,
        ) {
            debug_log(&format!("update_menu_bar: {error:?}"));
        }
    }
}

/// xdisp.c:dsafe__call: FUNCTION called with ARGS, `inhibit-redisplay'
/// (and with INHIBIT_QUIT `inhibit-quit') bound to t, any error or quit it
/// signals caught (internal_condition_case_n with Qt); nil then.
fn dsafe_call(
    interpreter: &mut Interpreter,
    env: &mut Env,
    inhibit_quit: bool,
    function: Value,
    args: &[Value],
) -> Value {
    let count = interpreter.specpdl_index();
    let _ = interpreter.specbind_symbol(&"inhibit-redisplay".into(), Value::T, env);
    if inhibit_quit {
        let _ = interpreter.specbind_symbol(&"inhibit-quit".into(), Value::T, env);
    }
    let handlers =
        interpreter.push_internal_condition_case(crate::lisp::eval::InternalHandlers::All);
    let value = interpreter
        .call_function_value(function, None, args, env)
        .unwrap_or(Value::Nil);
    interpreter.pop_handler_bindings(handlers);
    let _ = interpreter.unbind_to(count, env);
    value
}

/// display_mode_lines's specbind (Qinhibit_quit, Qt) around BODY.
fn with_inhibit_quit<T>(
    interpreter: &mut Interpreter,
    env: &mut Env,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> T,
) -> T {
    let count = interpreter.specpdl_index();
    let _ = interpreter.specbind_symbol(&"inhibit-quit".into(), Value::T, env);
    let value = body(interpreter, env);
    let _ = interpreter.unbind_to(count, env);
    value
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
    cols: usize,
    face_cache: &mut std::collections::HashMap<String, CellAttrs>,
) -> (PaintRow, bool, Option<usize>) {
    let mut row;
    let mut message_cursor = None;
    let minibuffer_id = {
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
                // A message in the echo area shows over the active
                // minibuffer until read_char wipes it at the next input
                // event (the bare-runtime minibuffer reader, which mirrors
                // its prompt and contents into the echo area, shows them as
                // the minibuffer).  Printing to the echo stream leaves the
                // hardware cursor after the message.
                match crate::lisp::primitives::echo_area_message() {
                    Some(message) if message != buffer.buffer_string() => {
                        message_cursor = Some(message.chars().count());
                        None
                    }
                    _ => Some(id),
                }
            })
    };
    if let Some(buffer_id) = minibuffer_id {
        // redisplay_window of the mini window showing the minibuffer:
        // whether the minibuffer holds long lines.
        long_lines::note_long_lines(interpreter, buffer_id);
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
        // The minibuffer's faces as its buffer remaps them.
        let remapping = face_remapping(interpreter, Some(buffer_id));
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
                let attrs = realized_face(face_cache, interpreter, env, &remapping, face);
                row.overlay(start.min(cols), (*col).min(cols), attrs);
            }
            for (from, to, face) in &string.spans {
                let attrs = realized_face(face_cache, interpreter, env, &remapping, face);
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
            let attrs = realized_face(face_cache, interpreter, env, &remapping, face);
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
    let (echo, spans) = crate::lisp::primitives::echo_display_message().unwrap_or_default();
    let output_cols = cols;
    let cols = echo.chars().count();
    row = PaintRow::blank(cols);
    row.blit(0, &echo, CellAttrs::default());
    // An echo area message: the frame's faces.
    let frame_faces = face_remapping(interpreter, None);
    for (from, to, face) in spans {
        let attrs = realized_face(face_cache, interpreter, env, &frame_faces, &face);
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
    // The minibuffer's remapping; an echo area message's (BUFFER_ID 0)
    // is the frame's.
    let remapping = face_remapping(interpreter, (buffer_id != 0).then_some(buffer_id));
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
        let attrs = realized_face(face_cache, interpreter, env, &remapping, &face);
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

thread_local! {
    /// The terminal being painted (its tty_display_info's output side),
    /// set by each redisplay; none where no terminal is (a test's rows).
    static PAINT_TERMINAL: std::cell::RefCell<Option<PaintTerminal>> =
        const { std::cell::RefCell::new(None) };
}

/// A terminal's color support and appearance strings, the frame's
/// default face (the realized face of the glyphs no face names),
/// `inverse-video' and `tty-suppress-bold-inverse-default-colors', with
/// tparam's output for each string and argument it has sent.
struct PaintTerminal {
    colors: crate::lisp::eval::terminal::TtyColors,
    appearance: crate::lisp::eval::terminal::TtyAppearance,
    default_face: CellAttrs,
    inverse_video: bool,
    suppress_bold_inverse_default_colors: bool,
    sent: std::collections::HashMap<(Vec<u8>, i64), Vec<u8>>,
}

impl PaintTerminal {
    fn new(
        colors: crate::lisp::eval::terminal::TtyColors,
        appearance: crate::lisp::eval::terminal::TtyAppearance,
    ) -> Self {
        Self {
            colors,
            appearance,
            default_face: CellAttrs::default(),
            inverse_video: false,
            suppress_bold_inverse_default_colors: false,
            sent: std::collections::HashMap::new(),
        }
    }

    /// tparam (STRING, ARG) as emacs_tputs sends it.
    fn tparam(&mut self, string: &[u8], arg: i64) -> Vec<u8> {
        self.sent
            .entry((string.to_vec(), arg))
            .or_insert_with(|| {
                crate::lisp::eval::terminal::tputs(&crate::lisp::eval::terminal::tparam(
                    string, arg,
                ))
            })
            .clone()
    }
}

/// Paint with the selected terminal's COLORS and APPEARANCE (none: no
/// terminal, a test's rows), keeping what was sent while they stay.
fn set_paint_terminal(
    terminal: Option<(
        crate::lisp::eval::terminal::TtyColors,
        crate::lisp::eval::terminal::TtyAppearance,
    )>,
) {
    PAINT_TERMINAL.with_borrow_mut(|paint| match (paint.as_mut(), terminal) {
        (Some(paint), Some((colors, appearance)))
            if paint.colors == colors && paint.appearance == appearance => {}
        (_, terminal) => {
            *paint = terminal.map(|(colors, appearance)| PaintTerminal::new(colors, appearance));
        }
    });
}

/// The frame's realized default face, `inverse-video' and
/// `tty-suppress-bold-inverse-default-colors' for this redisplay.
fn set_paint_faces(default_face: CellAttrs, inverse_video: bool, suppress_bold: bool) {
    PAINT_TERMINAL.with_borrow_mut(|paint| {
        if let Some(paint) = paint.as_mut() {
            paint.default_face = default_face;
            paint.inverse_video = inverse_video;
            paint.suppress_bold_inverse_default_colors = suppress_bold;
        }
    });
}

/// realize_tty_face's face for a glyph of ATTRS: its attributes over the
/// default face's (the merge lookup_face starts from), the colors
/// swapped for inverse video (realize_tty_face's swap; a tty's colors are
/// never defaulted), and bold left off inverse default colors when
/// `tty-suppress-bold-inverse-default-colors' says so.
fn realize_cell_face(
    attrs: &CellAttrs,
    default: &CellAttrs,
    suppress_bold_inverse_default_colors: bool,
) -> CellAttrs {
    use crate::lisp::primitives::{
        FACE_TTY_DEFAULT_BG_COLOR as DEFAULT_BG, FACE_TTY_DEFAULT_FG_COLOR as DEFAULT_FG,
    };
    // The attributes merged over the default face's (an attribute the
    // glyph's face turns off is off), the colors filled in with the
    // terminal's defaults, `:extend' the glyph's own.
    let merged = CellAttrs::merged_over(default, attrs);
    let mut face = CellAttrs {
        foreground: Some(merged.foreground.unwrap_or(DEFAULT_FG)),
        background: Some(merged.background.unwrap_or(DEFAULT_BG)),
        extend: attrs.extend,
        off: 0,
        ..merged
    };
    if face.reverse {
        std::mem::swap(&mut face.foreground, &mut face.background);
    }
    if suppress_bold_inverse_default_colors
        && face.bold
        && face.background == Some(DEFAULT_FG)
        && face.foreground == Some(DEFAULT_BG)
    {
        face.bold = false;
    }
    face
}

/// emacs_tputs of STRING where there is one (OUTPUT_IF).
fn output_if(out: &mut Vec<u8>, string: Option<&Vec<u8>>) {
    if let Some(string) = string {
        out.extend(crate::lisp::eval::terminal::tputs(string));
    }
}

/// term.c's tty_turn_on_highlight and tty_turn_off_highlight: standout
/// entered or left, STANDOUT its tty->standout_mode.
fn turn_on_highlight(out: &mut Vec<u8>, paint: &PaintTerminal, standout: &mut bool) {
    if !*standout {
        output_if(out, paint.appearance.standout_mode.as_ref());
    }
    *standout = true;
}

fn turn_off_highlight(out: &mut Vec<u8>, paint: &PaintTerminal, standout: &mut bool) {
    if *standout {
        output_if(out, paint.appearance.end_standout_mode.as_ref());
    }
    *standout = false;
}

/// term.c's output around a run of glyphs of realized FACE in
/// tty_write_glyphs: tty_highlight_if_desired and turn_on_face before
/// the text (standout toggled for the default colors the other way round
/// from `inverse-video', then bold, italic or else dim, the underline or
/// its style, strike-through, each where NC allows it with colors, and
/// the colors through the terminal's strings, the two swapped while
/// standout is on), turn_off_face and tty_turn_off_highlight after it
/// (me for an appearance, else ue, and op for a color of the face's own).
fn face_run_output(
    face: &CellAttrs,
    paint: &mut PaintTerminal,
    standout: &mut bool,
) -> (Vec<u8>, Vec<u8>) {
    use crate::lisp::eval::terminal::{
        NC_BOLD, NC_ITALIC, NC_REVERSE, NC_STRIKE_THROUGH, NC_UNDERLINE,
    };
    use crate::lisp::primitives::{
        FACE_NO_UNDERLINE, FACE_TTY_DEFAULT_BG_COLOR as DEFAULT_BG,
        FACE_TTY_DEFAULT_COLOR as DEFAULT, FACE_TTY_DEFAULT_FG_COLOR as DEFAULT_FG,
        FACE_UNDERLINE_SINGLE,
    };
    let fg = face.foreground.unwrap_or(DEFAULT_FG);
    let bg = face.background.unwrap_or(DEFAULT_BG);
    let mut on = Vec::new();
    // tty_highlight_if_desired.
    if paint.inverse_video {
        turn_on_highlight(&mut on, paint, standout);
    } else {
        turn_off_highlight(&mut on, paint, standout);
    }
    let may_use = |paint: &PaintTerminal, attr: i64| paint.colors.may_use_with_colors(attr);
    if may_use(paint, NC_REVERSE)
        && if paint.inverse_video {
            fg == DEFAULT_FG || bg == DEFAULT_BG
        } else {
            fg == DEFAULT_BG || bg == DEFAULT_FG
        }
    {
        if *standout {
            turn_off_highlight(&mut on, paint, standout);
        } else {
            turn_on_highlight(&mut on, paint, standout);
        }
    }
    if face.bold && may_use(paint, NC_BOLD) {
        output_if(&mut on, paint.appearance.enter_bold_mode.as_ref());
    }
    if face.italic && may_use(paint, NC_ITALIC) {
        // Italics unavailable: dimmed text instead.
        output_if(
            &mut on,
            paint
                .appearance
                .enter_italic_mode
                .as_ref()
                .or(paint.appearance.enter_dim_mode.as_ref()),
        );
    }
    if face.underline != FACE_NO_UNDERLINE && may_use(paint, NC_UNDERLINE) {
        match paint.appearance.set_underline_style.clone() {
            Some(style) if face.underline != FACE_UNDERLINE_SINGLE => {
                let sequence = paint.tparam(&style, i64::from(face.underline));
                on.extend(sequence);
            }
            _ => output_if(&mut on, paint.appearance.enter_underline_mode.as_ref()),
        }
    }
    if face.strike_through && may_use(paint, NC_STRIKE_THROUGH) {
        output_if(&mut on, paint.appearance.enter_strike_through_mode.as_ref());
    }
    if paint.colors.max_colors > 0 {
        let (for_fg, for_bg) = if *standout {
            (
                paint.colors.set_background.clone(),
                paint.colors.set_foreground.clone(),
            )
        } else {
            (
                paint.colors.set_foreground.clone(),
                paint.colors.set_background.clone(),
            )
        };
        // face_tty_specified_color: a pixel none of the default ones.
        if fg >= 0
            && let Some(string) = for_fg
        {
            let sequence = paint.tparam(&string, fg);
            on.extend(sequence);
        }
        if bg >= 0
            && let Some(string) = for_bg
        {
            let sequence = paint.tparam(&string, bg);
            on.extend(sequence);
        }
        if face.underline_color != 0
            && let Some(string) = paint.appearance.set_underline_color.clone()
        {
            let sequence = paint.tparam(&string, face.underline_color);
            on.extend(sequence);
        }
    }
    // turn_off_face.
    let mut off = Vec::new();
    if let Some(exit) = paint.appearance.exit_attribute_mode.clone() {
        if face.bold
            || face.italic
            || face.reverse
            || face.underline != FACE_NO_UNDERLINE
            || face.strike_through
        {
            output_if(&mut off, Some(&exit));
            if paint.appearance.end_standout_mode.as_ref() == Some(&exit) {
                *standout = false;
            }
        }
    } else if face.underline != FACE_NO_UNDERLINE {
        output_if(&mut off, paint.appearance.exit_underline_mode.as_ref());
    }
    if paint.colors.max_colors > 0
        && ((fg != DEFAULT && fg != DEFAULT_FG) || (bg != DEFAULT && bg != DEFAULT_BG))
    {
        output_if(&mut off, paint.colors.orig_pair.as_ref());
    }
    turn_off_highlight(&mut off, paint, standout);
    (on, off)
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
    queue!(out, cursor::MoveTo(0, row as u16))?;
    let cols = rendered.text.len();
    let default_attrs = CellAttrs::default();
    // tty->standout_mode across the row's runs.
    let mut standout = false;
    // update_frame_line's write_spaces_p: a default face whose background
    // is not the terminal's (colored_spaces_p) has the row's trailing
    // default-face spaces written; otherwise they are left to
    // clear_end_of_line.
    let colored_spaces = PAINT_TERMINAL.with_borrow(|paint| {
        paint.as_ref().is_some_and(|paint| {
            realize_cell_face(
                &default_attrs,
                &paint.default_face,
                paint.suppress_bold_inverse_default_colors,
            )
            .background
                != Some(crate::lisp::primitives::FACE_TTY_DEFAULT_BG_COLOR)
        })
    });
    let mut end = cols;
    while !colored_spaces
        && end > 0
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
        // tty_write_glyphs's run: the face's appearances on, the text,
        // the appearances off.
        let around = PAINT_TERMINAL.with_borrow_mut(|paint| {
            paint.as_mut().map(|paint| {
                let face = realize_cell_face(
                    &attrs,
                    &paint.default_face,
                    paint.suppress_bold_inverse_default_colors,
                );
                face_run_output(&face, paint, &mut standout)
            })
        });
        match around {
            Some((on, off)) => {
                out.write_all(&on)?;
                queue!(out, style::Print(&text))?;
                out.write_all(&off)?;
            }
            None => queue!(out, style::Print(&text))?,
        }
        at = run_end;
    }
    if end < cols {
        // tty_clear_end_of_line: tty_background_highlight (standout under
        // `inverse-video', so the cleared cells are reversed too), the
        // terminal's clear to end of line, and the highlight off again.
        let (on, off) = PAINT_TERMINAL
            .with_borrow_mut(|paint| {
                paint.as_mut().map(|paint| {
                    let mut on = Vec::new();
                    if paint.inverse_video {
                        turn_on_highlight(&mut on, paint, &mut standout);
                    } else {
                        turn_off_highlight(&mut on, paint, &mut standout);
                    }
                    let mut off = Vec::new();
                    turn_off_highlight(&mut off, paint, &mut standout);
                    (on, off)
                })
            })
            .unwrap_or_default();
        out.write_all(&on)?;
        queue!(out, terminal::Clear(terminal::ClearType::UntilNewLine))?;
        out.write_all(&off)?;
    }
    Ok(())
}

/// An hscrolled row when a line-number column precedes the text: the
/// left `$' replaces the column's own first glyph (produce_special_glyphs
/// overwrites the row's first glyph, which is now a line-number cell),
/// so the text area uses the gutter form of `HscrollLayout' and truncates
/// on the right as usual.
fn truncate_row_from(
    visual: &VisualLine,
    width: usize,
    hscroll: usize,
    from: Option<usize>,
) -> String {
    let (expanded, columns) = truncated_cells(visual, width, from);
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
/// first, TABs expanded -- and the column of each display index; from
/// display index FROM when a window's display starts there.
fn truncated_cells(visual: &VisualLine, width: usize, from: Option<usize>) -> (String, Vec<usize>) {
    let laid = match from {
        Some(from) => layout::lay_out_visual_line_from(visual, width, true, from, false),
        None => layout::lay_out_visual_line(visual, width, true),
    };
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

fn truncate_row_hscrolled(
    visual: &VisualLine,
    width: usize,
    hscroll: usize,
    from: Option<usize>,
) -> String {
    let (expanded, columns) = truncated_cells(visual, width, from);
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
    /// The display index of the element that did not fit when `word-wrap'
    /// took the row back to its wrap point.
    overflow_index: Option<usize>,
}

fn wrap_glyphless_visual_line(visual: &VisualLine, cols: usize) -> Vec<WrappedVisualSegment> {
    layout::lay_out_visual_line(visual, cols, false).rows
}

/// `wrapped_position_of_column' for columns asked in the order a line's
/// glyphs come: the row the last column was found on answers again
/// while it holds the column and no other row would be preferred.
struct RowOfColumn<'a> {
    segments: &'a [WrappedVisualSegment],
    last: std::cell::Cell<usize>,
}

impl<'a> RowOfColumn<'a> {
    fn new(segments: &'a [WrappedVisualSegment]) -> Self {
        Self {
            segments,
            last: std::cell::Cell::new(0),
        }
    }

    fn first(&self, column: usize) -> (usize, usize) {
        let row = self.last.get();
        if let Some(segment) = self.segments.get(row)
            && segment.start_column <= column
            && column < segment.end_column
            && (row == 0 || self.segments[row - 1].end_column <= column)
        {
            return (row, column - segment.start_column);
        }
        let found = wrapped_position_of_column(self.segments, column, true);
        self.last.set(found.0);
        found
    }

    /// The last row holding COLUMN (prefer_first false).
    fn last(&self, column: usize) -> (usize, usize) {
        let row = self.last.get();
        if let Some(segment) = self.segments.get(row)
            && segment.start_column <= column
            && column < segment.end_column
            && self
                .segments
                .get(row + 1)
                .is_none_or(|next| next.start_column > column)
        {
            return (row, column - segment.start_column);
        }
        let found = wrapped_position_of_column(self.segments, column, false);
        self.last.set(found.0);
        found
    }
}

fn wrapped_position_of_column(
    segments: &[WrappedVisualSegment],
    column: usize,
    prefer_first: bool,
) -> (usize, usize) {
    // The segments hold a display line's continuous columns in order,
    // their starts and ends non-decreasing, so those holding COLUMN
    // (start <= COLUMN < end) are one run [LO, HI), and the last segment
    // also holds its own end column.
    let lo = segments.partition_point(|segment| segment.end_column <= column);
    let hi = segments.partition_point(|segment| segment.start_column <= column);
    let last_holds_end = segments
        .last()
        .is_some_and(|segment| segment.start_column <= column && column == segment.end_column);
    let selected = match (prefer_first, lo < hi, last_holds_end) {
        (true, true, _) => Some(lo),
        (false, _, true) | (true, false, true) => Some(segments.len() - 1),
        (false, true, false) => Some(hi - 1),
        (_, false, false) => None,
    };
    selected
        .map(|index| (index, column.saturating_sub(segments[index].start_column)))
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
            last_point: None,
            matrix_current: false,
            cursor_y: 0,
            scroll_functions: false,
            make_window_start_visible: false,
            total_cells: (text_rows + 1) * body_width,
            top_rows: 0,
            scroll: Default::default(),
            changed_region: (1, 1),
            track_mouse: false,
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
        crate::lisp::primitives::set_echo_area_message(Some("yyyxZ".into()));
        let (row, mini, cursor) = compose_echo_row(&mut interpreter, &mut env, 3, &mut cache);
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
        let (row, mini, cursor) = compose_echo_row(&mut interpreter, &mut env, 8, &mut cache);
        crate::lisp::primitives::unwind_active_minibuffer(&mut interpreter, active, &mut env)
            .expect("unwind the read");
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
            replaced: Vec::new(),
            objects: Vec::new(),
            object_of: Vec::new(),
            newline_replaced: false,
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
                overflow_index: None,
            },
            WrappedVisualSegment {
                text: String::new(),
                start_column: 79,
                end_column: 85,
                start_index: 79,
                prefix_cols: 0,
                continuation: true,
                continued_glyph: false,
                overflow_index: None,
            },
        ];
        assert_eq!(wrapped_position_of_column(&boundary, 79, true), (1, 0));
    }

    #[test]
    fn rows_found_from_the_last_answer_as_wrapped_position_of_column_finds_them() {
        // Rows meeting at their ends, a word-wrapped row ending short of
        // the next one's start, an empty row and columns past the last:
        // asked forward, backward and out of order, each answer is the
        // binary search's.
        let segment = |start_column: usize, end_column: usize| WrappedVisualSegment {
            text: String::new(),
            start_column,
            end_column,
            start_index: start_column,
            prefix_cols: 0,
            continuation: start_column > 0,
            continued_glyph: true,
            overflow_index: None,
        };
        let layouts = [
            vec![segment(0, 79), segment(79, 158), segment(158, 170)],
            vec![
                segment(0, 70),
                segment(79, 120),
                segment(120, 120),
                segment(120, 130),
            ],
            vec![segment(0, 10)],
            vec![segment(5, 5), segment(5, 9)],
        ];
        for segments in &layouts {
            let columns: Vec<usize> = (0..180)
                .chain((0..180).rev())
                .chain((0..180).map(|column| column * 37 % 181))
                .collect();
            let rows = RowOfColumn::new(segments);
            for &column in &columns {
                assert_eq!(
                    rows.first(column),
                    wrapped_position_of_column(segments, column, true),
                    "first row of column {column} in {:?}",
                    segments
                        .iter()
                        .map(|segment| (segment.start_column, segment.end_column))
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    rows.last(column),
                    wrapped_position_of_column(segments, column, false),
                    "last row of column {column}"
                );
            }
        }
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
        interp.set_variable_key(
            cached_symbol!("buffer-invisibility-spec"),
            Value::T,
            &mut env,
        );
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
        interp.set_variable_key(
            cached_symbol!("buffer-invisibility-spec"),
            Value::T,
            &mut env,
        );
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
    fn point_follows_a_point_max_overlay_after_string() {
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
        // set_cursor_from_row puts point on the glyph after the overlay
        // strings at its position (GNU 30.2: the cursor follows "haBeta").
        assert_eq!(
            visual.map,
            vec![0, 1, 2, 9],
            "point at the overlay boundary follows its after-string"
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
        interp.set_variable_key(
            cached_symbol!("buffer-invisibility-spec"),
            Value::T,
            &mut env,
        );
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
        let shown = interp.buffer_value(displayed).expect("displayed buffer");
        crate::lisp::primitives::call(
            &mut interp,
            "set-window-buffer",
            &[Value::Nil, shown],
            &mut crate::lisp::types::Env::new(),
        )
        .expect("show the displayed buffer");
        assert_eq!(
            interp.current_buffer_id(),
            original,
            "set-window-buffer itself does not change the current buffer"
        );
        crate::lisp::primitives::select_window_buffer(&mut interp)
            .expect("command-cycle buffer selection succeeds");
        assert_eq!(interp.current_buffer_id(), displayed);
        assert_eq!(interp.selected_window_contents_buffer_id(), Some(displayed));
    }

    #[test]
    fn interactive_startup_initializes_scratch_and_messages_buffers() {
        let mut interp = initialized_interactive_runtime();
        let mut env: Env = crate::lisp::types::Env::new();
        interp.set_variable_key(cached_symbol!("noninteractive"), Value::Nil, &mut env);
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

    /// A pipe holding BYTES, read as the terminal's descriptor; the
    /// controlling terminal's input modes META_KEY while the test runs.
    struct TtyBytes {
        fds: [i32; 2],
    }

    impl TtyBytes {
        fn new(bytes: &[u8], meta_key: i32) -> Self {
            let mut fds = [0; 2];
            // SAFETY: FDS has room for the two descriptors pipe returns.
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            // SAFETY: BYTES is readable for its length.
            let written = unsafe { libc::write(fds[1], bytes.as_ptr().cast(), bytes.len()) };
            assert_eq!(written, bytes.len() as isize);
            crate::lisp::primitives::set_tty_input_modes(
                Some(crate::lisp::primitives::TtyInputModes {
                    flow_control: false,
                    meta_key,
                }),
                true,
                None,
            );
            Self { fds }
        }

        fn read(&self, interp: &mut Interpreter, env: &mut Env, queue: &SharedEventQueue) -> i32 {
            queue.read_avail_input(interp, env, self.fds[0])
        }
    }

    impl Drop for TtyBytes {
        fn drop(&mut self) {
            crate::lisp::primitives::set_tty_input_modes(None, false, None);
            // SAFETY: both ends are this pipe's, closed only here.
            unsafe {
                libc::close(self.fds[0]);
                libc::close(self.fds[1]);
            }
        }
    }

    fn queued(queue: &SharedEventQueue) -> Vec<Value> {
        let mut events = Vec::new();
        while let Some(QueuedInput::Lisp(event)) = queue.pop() {
            events.push(event);
        }
        events
    }

    #[test]
    fn terminal_bytes_are_keystrokes_by_the_meta_mode() {
        // tty_read_avail_input: each byte an ASCII_KEYSTROKE_EVENT; with
        // meta_key 1 the 8th bit is Meta (a capital un-capitalized, as
        // make_lispy_event takes it for caps lock), with 0 it is stripped,
        // with 2 it passes.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Env::new();
        let queue = SharedEventQueue::default();
        let bytes = [b'a', 0xe1, 0xc1, 0x1b, b'[', b'A', 13, 127];
        {
            let tty = TtyBytes::new(&bytes, 1);
            assert_eq!(tty.read(&mut interp, &mut env, &queue), bytes.len() as i32);
        }
        let meta = |c: u8| Value::Integer(i64::from(c) | CHAR_META);
        assert_eq!(
            queued(&queue),
            [
                Value::Integer(97),
                meta(b'a'),
                meta(b'a'),
                Value::Integer(27),
                Value::Integer(91),
                Value::Integer(65),
                Value::Integer(13),
                Value::Integer(127),
            ]
        );
        {
            let tty = TtyBytes::new(&[0xe1, 0xc1], 0);
            tty.read(&mut interp, &mut env, &queue);
        }
        assert_eq!(queued(&queue), [Value::Integer(97), Value::Integer(65)]);
        {
            let tty = TtyBytes::new(&[0xe1, 0xc1], 2);
            tty.read(&mut interp, &mut env, &queue);
        }
        assert_eq!(queued(&queue), [Value::Integer(0xe1), Value::Integer(0xc1)]);
        assert!(!queue.input_pending());
    }

    #[test]
    fn a_quit_character_read_as_a_byte_interrupts_and_ends_the_read() {
        // kbd_buffer_store_event gives the quit character to
        // handle_interrupt, unstored, and tty_read_avail_input looks no
        // further into that read.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Env::new();
        interp.set_c_variable("quit-flag", Value::Nil);
        let queue = SharedEventQueue::default();
        let tty = TtyBytes::new(b"a\x07b", 1);
        assert_eq!(tty.read(&mut interp, &mut env, &queue), 3);
        drop(tty);
        assert_eq!(queued(&queue), [Value::Integer(97)]);
        assert!(
            !interp.quit_flag_is_nil(),
            "the quit character requests a quit"
        );
        interp.set_c_variable("quit-flag", Value::Nil);
    }

    #[test]
    fn keyboard_bytes_decode_by_the_keyboard_coding_unless_read_raw() {
        // read_decoded_event_from_main_queue: the bytes of a UTF-8 sequence
        // gathered and decoded; a read with PREV_EVENT t takes them as
        // they come.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Env::new();
        crate::lisp::primitives::call(
            &mut interp,
            "set-keyboard-coding-system-internal",
            &[Value::symbol("utf-8-unix")],
            &mut env,
        )
        .expect("a UTF-8 keyboard");
        let queue = SharedEventQueue::default();
        let tty = TtyBytes::new(b"\xc3\xa9\xc3\xa9", 2);
        tty.read(&mut interp, &mut env, &queue);
        let Some(QueuedInput::Lisp(first)) = queue.pop() else {
            panic!("a byte was stored");
        };
        assert_eq!(
            queue.decode_event(&mut interp, &mut env, first),
            Value::Integer(0xe9)
        );
        let Some(QueuedInput::Lisp(raw)) = queue.pop() else {
            panic!("the second character's bytes remain");
        };
        {
            let _raw = crate::lisp::primitives::RawEventRead::enter(true);
            assert_eq!(
                queue.decode_event(&mut interp, &mut env, raw),
                Value::Integer(0xc3)
            );
        }
        assert_eq!(queued(&queue), [Value::Integer(0xa9)]);
        drop(tty);
    }

    #[test]
    fn decoded_characters_after_the_first_wait_in_unread_command_events() {
        // A byte that starts a sequence and one that cannot continue it
        // decode to two characters: the first is the event, the other
        // `unread-command-events''s first.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Env::new();
        crate::lisp::primitives::call(
            &mut interp,
            "set-keyboard-coding-system-internal",
            &[Value::symbol("utf-8-unix")],
            &mut env,
        )
        .expect("a UTF-8 keyboard");
        interp.set_variable_key(
            cached_symbol!("unread-command-events"),
            Value::Nil,
            &mut env,
        );
        let queue = SharedEventQueue::default();
        let tty = TtyBytes::new(b"\xc3(", 2);
        tty.read(&mut interp, &mut env, &queue);
        let Some(QueuedInput::Lisp(first)) = queue.pop() else {
            panic!("a byte was stored");
        };
        let event = queue.decode_event(&mut interp, &mut env, first);
        drop(tty);
        assert_ne!(event, Value::Integer(0xc3), "the byte was decoded");
        assert!(!queue.input_pending(), "both bytes were gathered");
        let unread = crate::lisp::primitives::unread_command_events(&interp, &env)
            .expect("a list of events");
        assert_eq!(unread, [Value::Integer(i64::from(b'('))]);
        interp.set_variable_key(
            cached_symbol!("unread-command-events"),
            Value::Nil,
            &mut env,
        );
    }

    #[test]
    fn the_encoded_meta_mode_makes_the_eighth_bit_meta_after_decoding() {
        // meta_key 3: the character a byte decodes to has its 8th bit made
        // Meta.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Env::new();
        crate::lisp::primitives::call(
            &mut interp,
            "set-keyboard-coding-system-internal",
            &[Value::symbol("iso-latin-1-unix")],
            &mut env,
        )
        .expect("a Latin-1 keyboard");
        let queue = SharedEventQueue::default();
        let tty = TtyBytes::new(b"\xe1x", 3);
        tty.read(&mut interp, &mut env, &queue);
        let mut events = Vec::new();
        while let Some(QueuedInput::Lisp(event)) = queue.pop() {
            events.push(queue.decode_event(&mut interp, &mut env, event));
        }
        drop(tty);
        assert_eq!(
            events,
            [
                Value::Integer(0x61 | CHAR_META),
                Value::Integer(i64::from(b'x'))
            ]
        );
    }

    #[test]
    fn function_keys_reach_the_input_decode_map() {
        // term_get_fkeys: each key's bytes bound to its symbol in
        // `input-decode-map'.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Env::new();
        let map = crate::lisp::primitives::call(&mut interp, "make-sparse-keymap", &[], &mut env)
            .expect("a keymap");
        interp.set_variable_key(cached_symbol!("input-decode-map"), map, &mut env);
        term_get_fkeys(
            &mut interp,
            &mut env,
            &[
                (b"\x1b[15~".to_vec(), "f5".into()),
                (b"\x1bOA".to_vec(), "up".into()),
            ],
        )
        .expect("define the keys");
        let lookup = |interp: &mut Interpreter, env: &mut Env, keys: &str| {
            crate::lisp::primitives::call(interp, "lookup-key", &[map, Value::string(keys)], env)
                .expect("look the key up")
        };
        assert_eq!(
            lookup(&mut interp, &mut env, "\x1b[15~"),
            Value::vector([Value::symbol("f5")])
        );
        assert_eq!(
            lookup(&mut interp, &mut env, "\x1bOA"),
            Value::vector([Value::symbol("up")])
        );
    }

    #[test]
    fn tty_mode_strings_are_the_terminal_parameters_strings() {
        // tty_send_additional_strings: the strings of the terminal's
        // `tty-mode-set-strings' (or reset) parameter in list order, other
        // elements skipped; none once the terminal is deleted.
        let mut interp = crate::test_support::initialized_upstream_batch_interpreter();
        let mut env = Env::new();
        crate::test_support::eval_lisp(
            &mut interp,
            &mut env,
            r#"(progn
                 (push "\e[?2004h" (terminal-parameter nil 'tty-mode-set-strings))
                 (push 7 (terminal-parameter nil 'tty-mode-set-strings))
                 (push "\e[?1004h" (terminal-parameter nil 'tty-mode-set-strings)))"#,
        )
        .expect("push the strings");
        let terminal = interp
            .decode_terminal(&Value::Nil)
            .expect("the initial terminal");
        let set = crate::lisp::eval::terminal::tty_additional_strings(
            terminal,
            Value::symbol("tty-mode-set-strings"),
        );
        assert_eq!(set, b"\x1b[?1004h\x1b[?2004h");
        let reset = crate::lisp::eval::terminal::tty_additional_strings(
            terminal,
            Value::symbol("tty-mode-reset-strings"),
        );
        assert!(reset.is_empty());
        interp.retire_terminal(terminal);
        let gone = crate::lisp::eval::terminal::tty_additional_strings(
            terminal,
            Value::symbol("tty-mode-set-strings"),
        );
        assert!(gone.is_empty());
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
        assert_eq!(truncate_row_hscrolled(&ordinary, 80, 3, None), "$ef");
        assert_eq!(truncate_row_from(&ordinary, 80, 3, None), "def");
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
                truncate_row_hscrolled(&glyphless, 80, hscroll, None),
                "$u00F6CDEFG"
            );
            assert_eq!(
                truncate_row_from(&glyphless, 80, hscroll, None),
                "\\u00F6CDEFG"
            );
        }
        assert_eq!(
            truncate_row_hscrolled(&glyphless, 80, 1, None),
            "$\\u00F6CDEFG"
        );
        assert_eq!(truncate_row_hscrolled(&glyphless, 80, 8, None), "$DEFG");
        assert_eq!(truncate_row_from(&glyphless, 80, 1, None), "B\\u00F6CDEFG");
        assert_eq!(truncate_row_from(&glyphless, 80, 8, None), "CDEFG");
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
                store: None,
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

        // A new session's first redisplay considers every window
        // (windows_or_buffers_changed starts nonzero), so the function
        // gets t.
        prepare_menu_bars(&mut interpreter, &mut env);

        assert_eq!(
            interpreter.lookup_var("tty--pre-redisplay-argument", &env),
            Some(Value::T)
        );
    }

    #[test]
    fn active_minibuffer_only_owns_cursor_while_its_window_is_selected() {
        let mut interpreter = initialized_interactive_runtime();
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable_key(cached_symbol!("noninteractive"), Value::Nil, &mut env);
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

        crate::lisp::primitives::unwind_active_minibuffer(&mut interpreter, active, &mut env)
            .expect("unwind the read");
    }

    #[test]
    fn transient_printed_value_takes_precedence_over_an_active_minibuffer() {
        let mut interpreter = initialized_interactive_runtime();
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable_key(cached_symbol!("noninteractive"), Value::Nil, &mut env);
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
            80,
            &mut std::collections::HashMap::new(),
        );

        crate::lisp::primitives::set_echo_area_message(None);
        crate::lisp::primitives::unwind_active_minibuffer(&mut interpreter, active, &mut env)
            .expect("unwind the read");
        assert!(!from_minibuffer);
        assert_eq!(cursor, Some(7));
        assert_eq!(row.text.iter().take(7).collect::<String>(), "\"inner\"");
    }

    #[test]
    fn minibuffer_before_string_inherits_its_prompt_anchor_face() {
        let mut interpreter = initialized_interactive_runtime();
        let mut env: Env = crate::lisp::types::Env::new();
        interpreter.set_variable_key(cached_symbol!("noninteractive"), Value::Nil, &mut env);
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
            80,
            &mut std::collections::HashMap::new(),
        );
        crate::lisp::primitives::unwind_active_minibuffer(&mut interpreter, active, &mut env)
            .expect("unwind the read");
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
        let spec = InvisibilitySpec::default();
        // A window 80 columns wide: 79 characters and the `\' per row.
        let rows = redisplay_window::Rows::new(&buffer, &spec, None, 80, false);
        let row_start = |line, seg| {
            rows.row_start(redisplay_window::Row {
                line,
                seg,
                origin: 0,
            })
        };
        assert_eq!(row_start(1, 0), 1);
        assert_eq!(row_start(2, 0), 5);
        assert_eq!(row_start(2, 1), 84);
        assert_eq!(row_start(2, 2), 163);
        assert_eq!(row_start(3, 0), 206);
    }

    #[test]
    fn truncated_rows_carry_the_dollar_marker_in_the_last_column() {
        let truncate_row =
            |line: &str, width: usize| truncate_row_from(&test_visual_line(line), width, 0, None);
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

    /// A forced window start at an overlay whose before-string has LINES
    /// lines, point there, in a window of 21 rows: the plan.
    fn forced_start_at_a_before_string(lines: usize) -> (WindowPlan, usize) {
        let mut interpreter = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let text: String = (0..100).map(|i| format!("line {i:03}\\n")).collect();
        let before: String = (0..lines).map(|i| format!("T{i:02}\\n")).collect();
        // Line 5 starts at 46.
        let form = crate::lisp::reader::Reader::new(&format!(
            "(progn
               (insert \"{text}\")
               (overlay-put (make-overlay 46 46) 'before-string \"{before}\")
               46)"
        ))
        .read()
        .expect("read before-string window probe")
        .expect("before-string window probe exists");
        let start = interpreter
            .eval(&form, &mut env)
            .expect("evaluate before-string window probe")
            .as_integer()
            .expect("the overlay's position") as usize;
        let spec = resolve_buffer_invisibility(
            &interpreter,
            &interpreter.buffer.borrow(),
            interpreter.current_buffer_id(),
        );
        let state = redisplay_window::StartState {
            start,
            point: start,
            buffer_point: start,
            force_start: true,
            optional_new_start: false,
            start_at_line_beg: true,
            windows_or_buffers_changed: false,
            window_outdated: true,
            cursor_unmoved: false,
            last_point: None,
            matrix_current: false,
            cursor_y: 0,
            scroll_functions: false,
            make_window_start_visible: false,
            total_cells: 22 * 80,
            top_rows: 0,
            scroll: Default::default(),
            changed_region: (1, 1),
            track_mouse: false,
            resume: None,
        };
        let geometry = RenderGeometry {
            truncate: false,
            hscroll: 0,
            current_line_only: false,
            min_hscroll: 0,
            lnum: None,
            store: None,
        };
        let mut view = WindowView::default();
        let plan = match super::plan_window_text(
            &interpreter.buffer.borrow(),
            &spec,
            None,
            &mut view,
            &state,
            21,
            80,
            false,
            &geometry,
            true,
        ) {
            PlanOutcome::Planned { plan, .. } => plan,
            PlanOutcome::Hook { .. } => unreachable!("no window-scroll-functions"),
        };
        (plan, start)
    }

    /// A forced window start at 46, where a `display' string of LINES
    /// lines replaces "line", with point at POINT and the cursor's last
    /// row CURSOR_Y when point did not move, in a window of 21 rows: the
    /// plan.
    fn forced_start_at_a_display_string(
        lines: usize,
        point: usize,
        cursor_y: Option<usize>,
    ) -> WindowPlan {
        let mut interpreter = Interpreter::new();
        let mut env: Env = crate::lisp::types::Env::new();
        let text: String = (0..100).map(|i| format!("line {i:03}\\n")).collect();
        let display: String = (0..lines).map(|i| format!("D{i:02}\\n")).collect();
        // Line 5 starts at 46.
        let form = crate::lisp::reader::Reader::new(&format!(
            "(progn
               (insert \"{text}\")
               (put-text-property 46 50 'display \"{display}\")
               46)"
        ))
        .read()
        .expect("read display-string window probe")
        .expect("display-string window probe exists");
        let start = interpreter
            .eval(&form, &mut env)
            .expect("evaluate display-string window probe")
            .as_integer()
            .expect("the display string's position") as usize;
        let spec = resolve_buffer_invisibility(
            &interpreter,
            &interpreter.buffer.borrow(),
            interpreter.current_buffer_id(),
        );
        let state = redisplay_window::StartState {
            start,
            point,
            buffer_point: point,
            force_start: cursor_y.is_none(),
            optional_new_start: false,
            start_at_line_beg: true,
            windows_or_buffers_changed: false,
            window_outdated: true,
            cursor_unmoved: cursor_y.is_some(),
            last_point: cursor_y.map(|_| point),
            matrix_current: cursor_y.is_some(),
            cursor_y: cursor_y.unwrap_or(0),
            scroll_functions: false,
            make_window_start_visible: false,
            total_cells: 22 * 80,
            top_rows: 0,
            scroll: Default::default(),
            changed_region: (1, 1),
            track_mouse: false,
            resume: None,
        };
        let geometry = RenderGeometry {
            truncate: false,
            hscroll: 0,
            current_line_only: false,
            min_hscroll: 0,
            lnum: None,
            store: None,
        };
        let mut view = WindowView::default();
        let plan = match super::plan_window_text(
            &interpreter.buffer.borrow(),
            &spec,
            None,
            &mut view,
            &state,
            21,
            80,
            false,
            &geometry,
            true,
        ) {
            PlanOutcome::Planned { plan, .. } => plan,
            PlanOutcome::Hook { .. } => unreachable!("no window-scroll-functions"),
        };
        assert_eq!(start, 46);
        plan
    }

    #[test]
    fn a_display_string_taller_than_the_window_takes_the_cursor_on_its_row() {
        // Point past a `display' string of 30 lines at the window's start
        // does not show: the force_start block moves point to the start
        // of the row half way down, a row of the string, whose position
        // is point's, so set_cursor_from_row puts the cursor on that row's
        // first glyph (string_buffer_position_lim's TEM == pt_old).
        let plan = forced_start_at_a_display_string(30, 52, None);
        assert_eq!(plan.rendered[0].0, "D00");
        assert_eq!(plan.rendered[9].0, "D09");
        assert_eq!(plan.cursor, Some((9, 0)));
        // Point unmoved, try_cursor_movement keeps the cursor on the row
        // it was on, though point's position begins the string's first.
        let plan = forced_start_at_a_display_string(30, 46, Some(9));
        assert_eq!(plan.cursor, Some((9, 0)));
    }

    #[test]
    fn a_window_start_at_a_before_string_shows_the_string_first() {
        // start_display at a start after a newline: init_iterator's
        // handle_stop shows the overlay strings there before the text.
        let (plan, _) = forced_start_at_a_before_string(3);
        let rows: Vec<&str> = plan.rendered[..5]
            .iter()
            .map(|row| row.0.as_str())
            .collect();
        assert_eq!(rows, ["T00", "T01", "T02", "line 005", "line 006"]);
        assert_eq!(plan.cursor, Some((3, 0)), "point's glyph after the string");
        // A string taller than the window fills it; point does not show,
        // so the force_start block moves it to the start of the row half
        // way down, a row of the string (MATRIX_ROW_START_CHARPOS, the
        // overlay's position), and set_cursor_from_row puts the cursor
        // past that row's glyphs, on its newline's.
        let (plan, start) = forced_start_at_a_before_string(30);
        assert_eq!(plan.rendered[0].0, "T00");
        assert_eq!(plan.rendered[20].0, "T20");
        assert_eq!(plan.cursor, Some((9, 3)));
        // The last row ends in a newline of the string, which goes on, below
        // a row that ended in one too: find_row_edges ends it at eol_pos
        // + 1.  When the string ends with the window's last row, the row
        // ends where the string is.
        assert_eq!(plan.window_end, start + 1);
        let (plan, start) = forced_start_at_a_before_string(21);
        assert_eq!(plan.window_end, start);
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
        interpreter.set_variable_key(
            cached_symbol!("buffer-invisibility-spec"),
            Value::T,
            &mut env,
        );
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
                store: None,
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
                store: None,
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
                last_point: None,
                matrix_current: false,
                cursor_y: 0,
                scroll_functions: false,
                make_window_start_visible: false,
                total_cells: (rows + 1) * 80,
                top_rows: 0,
                scroll,
                changed_region: (1, 1),
                track_mouse: false,
                resume: None,
            };
            let geometry = RenderGeometry {
                truncate: false,
                hscroll: 0,
                current_line_only: false,
                min_hscroll: 0,
                lnum: None,
                store: None,
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
                store: None,
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
                store: None,
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

    /// xterm-256color's color and appearance strings, as terminfo has them.
    fn xterm_256color() -> PaintTerminal {
        use crate::lisp::eval::terminal::{TtyAppearance, TtyColors};
        let some = |bytes: &[u8]| Some(bytes.to_vec());
        PaintTerminal::new(
            TtyColors {
                max_colors: 256,
                orig_pair: some(b"\x1b[39;49m"),
                set_foreground: some(
                    b"\x1b[%?%p1%{8}%<%t3%p1%d%e%p1%{16}%<%t9%p1%{8}%-%d%e38;5;%p1%d%;m",
                ),
                set_background: some(
                    b"\x1b[%?%p1%{8}%<%t4%p1%d%e%p1%{16}%<%t10%p1%{8}%-%d%e48;5;%p1%d%;m",
                ),
                no_color_video: 0,
            },
            TtyAppearance {
                standout_mode: some(b"\x1b[7m"),
                end_standout_mode: some(b"\x1b[27m"),
                enter_underline_mode: some(b"\x1b[4m"),
                exit_underline_mode: some(b"\x1b[24m"),
                enter_bold_mode: some(b"\x1b[1m"),
                enter_dim_mode: some(b"\x1b[2m"),
                enter_italic_mode: some(b"\x1b[3m"),
                enter_strike_through_mode: some(b"\x1b[9m"),
                exit_attribute_mode: some(b"\x1b(B\x1b[m"),
                ..TtyAppearance::default()
            },
        )
    }

    /// The bytes around a run of ATTRS: realize_cell_face over PAINT's
    /// default face, then face_run_output from standout off.
    fn run_output(paint: &mut PaintTerminal, attrs: CellAttrs) -> (String, String) {
        let face = realize_cell_face(&attrs, &paint.default_face, false);
        let mut standout = false;
        let (on, off) = face_run_output(&face, paint, &mut standout);
        assert!(!standout, "each run ends with standout off");
        (
            String::from_utf8(on).expect("ASCII"),
            String::from_utf8(off).expect("ASCII"),
        )
    }

    #[test]
    fn a_run_s_appearances_are_turn_on_face_s_and_turn_off_face_s() {
        use crate::lisp::primitives::{
            FACE_TTY_DEFAULT_BG_COLOR as DEFAULT_BG, FACE_TTY_DEFAULT_FG_COLOR as DEFAULT_FG,
        };
        let mut xterm = xterm_256color();
        let mut run = |attrs: CellAttrs| run_output(&mut xterm, attrs);
        assert_eq!(
            run(CellAttrs::default()),
            (String::new(), String::new()),
            "the default face's default colors send nothing"
        );
        assert_eq!(
            run(CellAttrs {
                foreground: Some(1),
                background: Some(9),
                bold: true,
                ..CellAttrs::default()
            }),
            (
                "\x1b[1m\x1b[31m\x1b[101m".into(),
                "\x1b(B\x1b[m\x1b[39;49m".into()
            ),
            "md, then setaf and setab by tparam; me and op after"
        );
        assert_eq!(
            run(CellAttrs {
                reverse: true,
                ..CellAttrs::default()
            }),
            ("\x1b[7m".into(), "\x1b(B\x1b[m\x1b[39;49m\x1b[27m".into()),
            "inverse default colors are standout, which se ends"
        );
        assert_eq!(
            run(CellAttrs {
                foreground: Some(1),
                reverse: true,
                ..CellAttrs::default()
            }),
            (
                "\x1b[7m\x1b[31m".into(),
                "\x1b(B\x1b[m\x1b[39;49m\x1b[27m".into()
            ),
            "under standout the swapped background goes out as a foreground"
        );
        assert_eq!(
            run(CellAttrs {
                italic: true,
                strike_through: true,
                underline: crate::lisp::primitives::FACE_UNDERLINE_SINGLE,
                ..CellAttrs::default()
            }),
            ("\x1b[3m\x1b[4m\x1b[9m".into(), "\x1b(B\x1b[m".into())
        );
        // -rv: the default face's colors are each other's sentinels, so
        // every run of it, and of a face over it, is standout.
        xterm.default_face = CellAttrs {
            foreground: Some(DEFAULT_BG),
            background: Some(DEFAULT_FG),
            ..CellAttrs::default()
        };
        assert_eq!(
            run_output(&mut xterm, CellAttrs::default()),
            ("\x1b[7m".into(), "\x1b[39;49m\x1b[27m".into())
        );
        assert_eq!(
            run_output(
                &mut xterm,
                CellAttrs {
                    bold: true,
                    ..CellAttrs::default()
                }
            ),
            (
                "\x1b[7m\x1b[1m".into(),
                "\x1b(B\x1b[m\x1b[39;49m\x1b[27m".into()
            )
        );
    }

    #[test]
    fn a_terminal_s_strings_and_nc_decide_what_goes_out() {
        use crate::lisp::eval::terminal::{NC_BOLD, NC_REVERSE, NC_UNDERLINE};
        // NC_REVERSE (cons25's): no standout with colors, so an inverse
        // face's swapped color goes out as the background.
        let mut cons25 = xterm_256color();
        cons25.colors.no_color_video = NC_REVERSE;
        assert_eq!(
            run_output(
                &mut cons25,
                CellAttrs {
                    foreground: Some(1),
                    reverse: true,
                    ..CellAttrs::default()
                }
            ),
            ("\x1b[41m".into(), "\x1b(B\x1b[m\x1b[39;49m".into())
        );
        // No italic string: dim instead; NC underline left off.
        let mut console = xterm_256color();
        console.appearance.enter_italic_mode = None;
        console.colors.no_color_video = NC_UNDERLINE;
        assert_eq!(
            run_output(
                &mut console,
                CellAttrs {
                    italic: true,
                    underline: crate::lisp::primitives::FACE_UNDERLINE_SINGLE,
                    ..CellAttrs::default()
                }
            ),
            ("\x1b[2m".into(), "\x1b(B\x1b[m".into())
        );
        // NC binds only where there are colors, and none are sent without
        // them; tputs leaves a padding specification out.
        let mut mono = xterm_256color();
        mono.colors.max_colors = 0;
        mono.colors.no_color_video = NC_BOLD;
        mono.appearance.enter_bold_mode = Some(b"\x1b[1m$<2>".to_vec());
        assert_eq!(
            run_output(
                &mut mono,
                CellAttrs {
                    foreground: Some(1),
                    bold: true,
                    ..CellAttrs::default()
                }
            ),
            ("\x1b[1m".into(), "\x1b(B\x1b[m".into())
        );
        // Without me, only ue ends an appearance.
        let mut no_me = xterm_256color();
        no_me.appearance.exit_attribute_mode = None;
        assert_eq!(
            run_output(
                &mut no_me,
                CellAttrs {
                    bold: true,
                    underline: crate::lisp::primitives::FACE_UNDERLINE_SINGLE,
                    ..CellAttrs::default()
                }
            ),
            ("\x1b[1m\x1b[4m".into(), "\x1b[24m".into())
        );
        // A styled underline through Smulx; a 24-bit color through the
        // xterm+direct strings.
        let mut kitty = xterm_256color();
        kitty.appearance.set_underline_style = Some(b"\x1b[4:%p1%dm".to_vec());
        kitty.colors.max_colors = 16_777_216;
        kitty.colors.set_foreground = Some(
            b"\x1b[%?%p1%{8}%<%t3%p1%d%e38;2;%p1%{65536}%/%d;%p1%{256}%/%{255}%&%d;%p1%{255}%&%d%;m"
                .to_vec(),
        );
        assert_eq!(
            run_output(
                &mut kitty,
                CellAttrs {
                    foreground: Some(0x12_34_56),
                    underline: 3,
                    ..CellAttrs::default()
                }
            ),
            (
                "\x1b[4:3m\x1b[38;2;18;52;86m".into(),
                "\x1b(B\x1b[m\x1b[39;49m".into()
            )
        );
    }

    #[test]
    fn color_modes_set_up_the_colors_as_tty_setup_colors() {
        use crate::lisp::eval::terminal::TtyColors;
        let entry = TtyColors {
            max_colors: 256,
            orig_pair: Some(b"\x1b[39;49m".to_vec()),
            set_foreground: Some(b"F".to_vec()),
            set_background: Some(b"B".to_vec()),
            no_color_video: 3,
        };
        entry.save_default();
        let mut colors = entry.clone();
        colors.setup(-7);
        assert_eq!(
            colors,
            TtyColors::default(),
            "-1 and below: no colors at all"
        );
        colors.setup(8);
        assert_eq!(colors.max_colors, 8);
        assert_eq!(colors.set_foreground.as_deref(), Some(&b"\x1b[3%p1%dm"[..]));
        assert_eq!(colors.orig_pair.as_deref(), Some(&b"\x1b[0m"[..]));
        for mode in [0, 256, 16] {
            colors.setup(mode);
            assert_eq!(colors, entry, "mode {mode}: the capabilities init_tty saw");
        }
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
        assert!(merged.bold && merged.reverse && merged.underline == 0);
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
        let (cells, _) = truncated_cells(&tabbed, 80, None);
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
        interpreter.set_variable_key(cached_symbol!("noninteractive"), Value::Nil, &mut env);
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
        // Each key read by a recursive edit's command loop, which a timer
        // ends with a throw to `exit' once the loop waits for more input
        // (without a keyboard read_char waits, as GNU's does);
        // command_loop_1's entry runs the post-command hooks first.
        // (command-error-default-function discards the input after an
        // error, so the keys come one recursive edit each; each loop's top
        // makes `this-command' nil before it reads.)
        crate::test_support::eval_lisp(
            &mut interpreter,
            &mut env,
            "(dolist (key (list ?\\C-p 'f6 'f7))
               (let ((unread-command-events (list key)))
                 (run-at-time 0 nil (lambda () (throw 'exit nil)))
                 (recursive-edit)))",
        )
        .expect("each recursive edit returns when its timer throws to exit");
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
            "(((post nil nil) (pre previous-line) (post previous-line \"Beginning of buffer\") \
             (post nil \"Beginning of buffer\") (pre t147-boom) (post t147-boom \"Boom 1\") \
             (post nil \"Boom 1\") (pre nil) (post nil \"<f7> is undefined\")) nil \
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
        interpreter.set_variable_key(cached_symbol!("noninteractive"), Value::Nil, &mut env);
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

        // C-s enters isearch; printing characters dispatch through the
        // overriding isearch-mode-map, the first match ending after "wor"
        // on the second line ("beta word": w=16, o=17, r=18, end=19).  C-a
        // is not an isearch key: the pre-command-hook exits the search,
        // removing the overriding map, and the key's own command runs.
        assert_eq!(
            post_command_log(
                &mut interpreter,
                &mut env,
                "\"\\C-swor\\C-a\"",
                "(list this-original-command (point) (null overriding-terminal-local-map))",
            ),
            "((nil 1 t) (isearch-forward 1 nil) (isearch-printing-char 17 nil) \
             (isearch-printing-char 18 nil) (isearch-printing-char 19 nil) \
             (move-beginning-of-line 11 t))"
        );
    }

    #[test]
    fn a_printing_key_inserts_through_command_loop_1_and_its_command() {
        // The terminal's keys run in keyboard.c's command loop: a printing
        // character's binding, `self-insert-command', inserts it through
        // call-interactively, and command_loop_1 makes it `last-command'.
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
        interpreter.set_variable_key(cached_symbol!("noninteractive"), Value::Nil, &mut env);
        crate::lisp::primitives::call(&mut interpreter, "erase-buffer", &[], &mut env)
            .expect("erase the scratch banner");
        assert_eq!(
            post_command_log(
                &mut interpreter,
                &mut env,
                "\"h\"",
                "(list this-command (buffer-string) (point))",
            ),
            // command_loop_1's entry runs the post-command hooks first.
            "((nil \"\" 1) (self-insert-command \"h\" 2))"
        );
        assert_eq!(
            interpreter.lookup_var_key(cached_symbol!("last-command-event"), &env),
            Some(Value::Integer(104))
        );
        assert_eq!(interpreter.buffer.borrow().buffer_string(), "h");
        assert_eq!(
            interpreter.lookup_var_key(cached_symbol!("last-command"), &env),
            Some(Value::Symbol("self-insert-command".into()))
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
        interpreter.set_variable_key(cached_symbol!("noninteractive"), Value::Nil, &mut env);
        interpreter.buffer.borrow_mut().insert("abcdefghijklmnop\n");
        interpreter.buffer.borrow_mut().goto_char(1);

        // C-u is simple.el's universal-argument: it stores (4) in
        // prefix-arg and arms the transient map that makes a following
        // digit dispatch digit-argument; the next ordinary command consumes
        // the accumulated prefix (C-u 8 C-f moves 8 chars).
        assert_eq!(
            post_command_log(
                &mut interpreter,
                &mut env,
                "\"\\C-u8\\C-f\"",
                "(list this-original-command prefix-arg (point))",
            ),
            "((nil nil 1) (universal-argument (4) 1) (digit-argument 8 1) (forward-char nil 9))"
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
            reader
                .finish(&mut interpreter, false, &mut env)
                .expect("finish"),
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

/// term.c's tty_menu_activate for the frontend: MENU drawn over the glass
/// at X0, Y0 (its first item's cell, one-based) and run modally under
/// `tty-menu-navigation-map' (bound by tty_menu_show), keyboard
/// navigation driving GNU's virtual mouse, submenus opening beside their
/// pane; the screen behind restored on exit.
fn make_menu_activate(
    state: std::rc::Rc<std::cell::RefCell<TtyState>>,
) -> crate::lisp::primitives::menu::TtyMenuActivate {
    use crate::lisp::primitives::menu::{
        TTYM_IA_SELECT, TTYM_NEXT, TTYM_NO_SELECT, TTYM_PREV, TTYM_SUCCESS, TtyMenu,
        TtyMenuActivation,
    };
    // tty_menu_state: a shown menu (its path of submenu indices from the
    // root), its pane number, its origin and the screen behind it.
    struct MenuLevel {
        path: Vec<usize>,
        pane: i32,
        x: i64,
        y: i64,
        screen_behind: Vec<(usize, PaintRow)>,
    }
    fn menu_at<'a>(root: &'a TtyMenu, path: &[usize]) -> &'a TtyMenu {
        let mut menu = root;
        for &index in path {
            menu = menu.submenu[index].as_deref().expect("menu path");
        }
        menu
    }
    // read_menu_input's results.
    #[derive(PartialEq)]
    enum MiResult {
        QuitMenu,
        Continue,
        ItemSelected,
        NextItem,
        PrevItem,
        ScrollForward,
        ScrollBack,
    }
    Box::new(
        move |interpreter: &mut Interpreter,
              env: &mut Env,
              menu: &mut TtyMenu,
              x0: i64,
              y0: i64,
              kbd_navigation: bool| {
            let Ok((cols, rows)) = crate::lisp::eval::terminal::output_size() else {
                return Ok(TtyMenuActivation {
                    status: crate::lisp::primitives::menu::TTYM_FAILURE,
                    pane: 0,
                    selidx: 0,
                });
            };
            let (cols, rows) = (usize::from(cols.max(1)), usize::from(rows.max(1)));
            let total_lines = interpreter.frame_height().max(1);
            let sf = interpreter.selected_frame_id;
            // Don't allow non-positive x0 and y0, lest the menu will wrap
            // around the display.
            let x0 = x0.max(1);
            let y0 = y0.max(1);
            let mut resolve_face = |name: &str| {
                let mut state = state.borrow_mut();
                // A tty menu's faces are the frame's.
                realized_face(
                    &mut state.face_cache,
                    interpreter,
                    env,
                    &face_remapping(interpreter, None),
                    &Value::symbol(name),
                )
            };
            let disabled = resolve_face("tty-menu-disabled-face");
            let enabled = resolve_face("tty-menu-enabled-face");
            let selected = resolve_face("tty-menu-selected-face");
            // faces[2] and faces[3]: the selected face derived from the
            // disabled and the enabled one.
            let faces = [
                disabled,
                enabled,
                merge_cell_attrs(disabled, selected),
                merge_cell_attrs(enabled, selected),
            ];
            // Make sure the menu title is always displayed with
            // `tty-menu-selected-face', no matter where the mouse pointer
            // is.
            let title_faces = [faces[3]; 4];
            // save_and_enable_current_matrix: every line of the frame, the
            // echo area's included.
            let save_rows = |state: &TtyState| -> Vec<(usize, PaintRow)> {
                state
                    .painted_rows
                    .iter()
                    .chain(state.painted_echo.iter())
                    .cloned()
                    .enumerate()
                    .collect()
            };
            // screen_update: the saved rows restored by
            // update_frame_with_menu (f, -1, -1), which leaves the cursor
            // where a frame update puts it.
            let screen_update = |state: &mut TtyState, saved: &[(usize, PaintRow)]| -> (i64, i64) {
                let mut out = terminal_output();
                for (row, cell) in saved {
                    if state
                        .screen_row(*row)
                        .is_some_and(|painted| painted != cell)
                    {
                        let _ = paint_row(&mut out, *row, cell);
                        state.set_screen_row(*row, cell.clone());
                    }
                }
                let (col, row) = state.update_cursor;
                let _ = queue!(out, cursor::MoveTo(col, row));
                let _ = out.flush();
                (i64::from(col), i64::from(row))
            };
            // display_tty_menu_item: ITEM_TEXT at X, Y in FACE, a blank
            // before it, padded to WIDTH, " >" after a submenu's name,
            // clipped before the frame's last column.
            let display_item = |state: &mut TtyState,
                                interpreter: &Interpreter,
                                text: &[u8],
                                width: usize,
                                face: CellAttrs,
                                x: usize,
                                y: usize,
                                submenu: bool| {
                let Some(painted) = state.screen_row(y) else {
                    return;
                };
                let mut row = painted.clone();
                let last_visible_x = cols.saturating_sub(1);
                let mut cells = String::from(" ");
                let item = menu_item_display_text(interpreter, text);
                if submenu {
                    cells.push_str(&item);
                    let used = 1 + text.len();
                    cells.push_str(" >");
                    let mut count = cell_count(&cells);
                    while count < width.max(used) {
                        cells.push(' ');
                        count += 1;
                    }
                } else {
                    cells.push_str(&item);
                    let mut count = cell_count(&cells);
                    while count < width {
                        cells.push(' ');
                        count += 1;
                    }
                }
                let room = last_visible_x.saturating_sub(x);
                let (cells, _) = take_cells_padded(&cells, room.min(cell_count(&cells)), ' ');
                if row.text.len() < cols {
                    row = PaintRow::blank(cols);
                }
                row.blit(x, &cells, face);
                let mut out = terminal_output();
                let _ = paint_row(&mut out, y, &row);
                state.set_screen_row(y, row);
            };
            // menu_help_message and prev_menu_help_message, as the item
            // whose help text they point at (none for a null pointer),
            // with the pane and item numbers it was shown for.
            let mut prev_help: Option<(Vec<usize>, usize)> = None;
            let mut help_message: Option<(Vec<usize>, usize)> = None;
            let mut help_paneno = 0i32;
            let mut help_itemno = 0i32;
            // tty_menu_display: MENU at X, Y from FIRST_ITEM, the item
            // under (MX, MY) in its selected face, its help noted when
            // DISP_HELP; update_frame_with_menu's cursor at that item, or
            // where the tty's cursor (CURSOR) already was.
            let menu_display = |state: &mut TtyState,
                                interpreter: &Interpreter,
                                cursor: (i64, i64),
                                root: &TtyMenu,
                                path: &[usize],
                                x: i64,
                                y: i64,
                                pn: i32,
                                faces: &[CellAttrs; 4],
                                mx: i64,
                                my: i64,
                                first_item: usize,
                                disp_help: bool,
                                help: &mut Option<(Vec<usize>, usize)>,
                                help_paneno: &mut i32,
                                help_itemno: &mut i32,
                                title_cut: Option<usize>|
             -> (i64, i64) {
                let menu = menu_at(root, path);
                // Don't try to display more menu items than the console
                // can display using the available screen lines.  Exclude
                // the echo area line, as it will be overwritten by the
                // help-echo anyway.
                let max_items = (menu.count() as i64 - first_item as i64)
                    .min(total_lines - 1 - y)
                    .max(0) as usize;
                *help = None;
                let width = menu.width;
                let (mut col, mut row) = cursor;
                for i in 0..max_items {
                    // +2 for padding blanks on each side.
                    let mut max_width = width + 2;
                    let j = i + first_item;
                    let submenu = menu.submenu[j].is_some();
                    if submenu {
                        // for displaying " >" after the item
                        max_width += 2;
                    }
                    let enabled = usize::from((!submenu && menu.panenumber[j] != 0) || submenu);
                    let mousehere = y + i as i64 == my && x <= mx && mx < x + max_width as i64;
                    let face = faces[enabled + usize::from(mousehere) * 2];
                    // Display the menu help string for the i-th menu item
                    // even if the menu item is currently disabled.
                    if disp_help && enabled + usize::from(mousehere) * 2 >= 2 {
                        *help = menu.help_text[j].as_ref().map(|_| (path.to_vec(), j));
                        *help_paneno = pn - 1;
                        *help_itemno = j as i32;
                    }
                    // Take note of the coordinates of the active menu
                    // item, to display the cursor there.
                    if mousehere {
                        row = y + i as i64;
                        col = x;
                    }
                    let mut text = menu.text[j].clone();
                    if j == 0
                        && let Some(cut) = title_cut
                    {
                        text.truncate(cut);
                    }
                    display_item(
                        state,
                        interpreter,
                        &text,
                        max_width,
                        face,
                        x.max(0) as usize,
                        (y + i as i64).max(0) as usize,
                        submenu,
                    );
                }
                let mut out = terminal_output();
                let _ = queue!(
                    out,
                    cursor::MoveTo(
                        (col.max(0) as usize).min(cols.saturating_sub(1)) as u16,
                        (row.max(0) as usize).min(rows.saturating_sub(1)) as u16,
                    ),
                );
                let _ = out.flush();
                (col, row)
            };
            // cursor_to: the (hidden) cursor at COL, ROW.
            let cursor_to = |col: i64, row: i64| {
                let mut out = terminal_output();
                let _ = queue!(
                    out,
                    cursor::MoveTo(
                        (col.max(0) as usize).min(cols.saturating_sub(1)) as u16,
                        (row.max(0) as usize).min(rows.saturating_sub(1)) as u16,
                    ),
                );
                let _ = out.flush();
            };
            let hide_cursor = || {
                let mut out = terminal_output();
                let _ = queue!(out, cursor::Hide);
                let _ = out.flush();
            };

            // Don't let the title for the "Buffers" popup menu include a
            // digit (which is ugly).
            let buffers_num_deleted = menu.count() > 0 && menu.text[0].starts_with(b"Buffers 1");
            // Inhibit redisplay for as long as the menu is active, to avoid
            // messing the screen if some timer calls sit-for or a similar
            // function.
            let prev_inhibit_redisplay = interpreter
                .lookup_var_key(cached_symbol!("inhibit-redisplay"), env)
                .unwrap_or(Value::Nil);
            interpreter.set_variable_key(cached_symbol!("inhibit-redisplay"), Value::T, env);
            let mut levels: Vec<MenuLevel> = Vec::new();
            // Force update of the current frame, so that the desired and
            // the current matrices are identical: the cursor where a frame
            // update leaves it.
            let (update_col, update_row) = state.borrow().update_cursor;
            cursor_to(i64::from(update_col), i64::from(update_row));
            let mut cur = (i64::from(update_col), i64::from(update_row));
            let screen_behind = save_rows(&state.borrow());
            // Display the menu title.  We subtract 1 from x0 and y0
            // because we want to interpret them as zero-based column and
            // row coordinates, and also because we want the first item of
            // the menu, not its title, to appear at x0,y0.
            cur = {
                let mut state = state.borrow_mut();
                menu_display(
                    &mut state,
                    interpreter,
                    cur,
                    menu,
                    &[],
                    x0 - 1,
                    y0 - 1,
                    1,
                    &title_faces,
                    x0 - 1,
                    y0 - 1,
                    0,
                    false,
                    &mut None,
                    &mut 0,
                    &mut 0,
                    buffers_num_deleted.then_some(7),
                )
            };
            // Turn off the cursor.  Otherwise it shows through the menu
            // panes, which is ugly.
            let (mut col, mut row) = cur;
            hide_cursor();
            let onepane = menu.count() == 1 && menu.submenu[0].is_some();
            if onepane {
                menu.width = menu.submenu[0].as_ref().map_or(0, |sub| sub.width);
            }
            levels.push(MenuLevel {
                path: if onepane { vec![0] } else { Vec::new() },
                pane: i32::from(onepane),
                x: x0 - 1,
                y: y0,
                screen_behind,
            });
            let mut x = levels[0].x;
            let mut y = levels[0].y;
            let mut first_time = true;
            let mut mouse_moved = false;
            let mut first_item = 0usize;
            let mut pane = 0i32;
            let mut selidx = 0i32;
            let mut result = TTYM_IA_SELECT;
            let mut leave = false;
            let outcome = (|| -> Result<(), LispError> {
                while !leave {
                    let min_y = levels[0].y;
                    let count0 = menu_at(menu, &levels[0].path).count() as i64;
                    let max_y = (min_y + count0).min(total_lines - 1) - 1;
                    // read_menu_input.
                    let input_status = if first_time {
                        first_time = false;
                        mouse_moved = true;
                        MiResult::Continue
                    } else {
                        // term.c:read_menu_input.  Signal the keyboard
                        // reading routines we are displaying a menu on
                        // this terminal, and have mouse movements reported
                        // by read_menu_command (do_mouse_tracking, the C
                        // variable of `track-mouse', set as it is, not
                        // bound; a nonlocal exit leaves both as they are).
                        let saved_mouse_tracking = interpreter
                            .symbol_value_cell("track-mouse")
                            .unwrap_or(Value::Nil);
                        crate::lisp::primitives::set_tty_showing_menu(true);
                        interpreter.set_symbol_value_cell("track-mouse", Value::T);
                        let command = loop {
                            let command = read_menu_command(interpreter, env)?;
                            if !command.is_nil() {
                                break command;
                            }
                        };
                        crate::lisp::primitives::set_tty_showing_menu(false);
                        interpreter.set_symbol_value_cell("track-mouse", saved_mouse_tracking);
                        let name = match command.kind() {
                            Kind::Symbol(_) => command.as_symbol().unwrap_or(""),
                            Kind::T => "tty-menu-exit",
                            _ => "",
                        };
                        let mut usable_input = true;
                        let mut st = MiResult::Continue;
                        if name == "tty-menu-exit"
                            // If some input switched frames under our
                            // feet, exit the menu, since the menu faces
                            // are no longer valid, and the menu is no
                            // longer relevant anyway.
                            || interpreter.selected_frame_id != sf
                        {
                            st = MiResult::QuitMenu;
                            usable_input = false;
                        } else if name == "tty-menu-mouse-movement" {
                            mouse_get_xy(interpreter, env, &mut x, &mut y)?;
                        } else if name == "tty-menu-next-menu" {
                            usable_input = false;
                            st = MiResult::NextItem;
                        } else if name == "tty-menu-prev-menu" {
                            usable_input = false;
                            st = MiResult::PrevItem;
                        } else if name == "tty-menu-next-item" {
                            if y < max_y {
                                y += 1;
                            } else {
                                st = MiResult::ScrollForward;
                            }
                        } else if name == "tty-menu-prev-item" {
                            if y > min_y {
                                y -= 1;
                            } else {
                                st = MiResult::ScrollBack;
                            }
                        } else if name == "tty-menu-select" {
                            st = MiResult::ItemSelected;
                        } else if name != "tty-menu-ignore" {
                            usable_input = false;
                        }
                        if usable_input {
                            mouse_moved = true;
                        }
                        st
                    };
                    if input_status != MiResult::Continue {
                        leave = true;
                        match input_status {
                            MiResult::QuitMenu => {
                                // Remove the last help-echo, so that it
                                // doesn't re-appear after "Quit".
                                crate::lisp::primitives::menu::show_help_echo(
                                    interpreter,
                                    Value::Nil,
                                    Value::Nil,
                                    Value::Nil,
                                    Value::Nil,
                                    env,
                                )?;
                                result = TTYM_NO_SELECT;
                            }
                            MiResult::NextItem => {
                                if kbd_navigation {
                                    result = TTYM_NEXT;
                                } else {
                                    leave = false;
                                }
                            }
                            MiResult::PrevItem => {
                                if kbd_navigation {
                                    result = TTYM_PREV;
                                } else {
                                    leave = false;
                                }
                            }
                            MiResult::ScrollForward => {
                                if y - min_y == count0 - 1 - first_item as i64 {
                                    y = min_y;
                                    first_item = 0;
                                } else {
                                    first_item += 1;
                                }
                                leave = false;
                            }
                            MiResult::ScrollBack => {
                                if first_item == 0 {
                                    y = max_y;
                                    first_item = (count0 - 1 - (y - min_y)).max(0) as usize;
                                } else {
                                    first_item -= 1;
                                }
                                leave = false;
                            }
                            _ => {}
                        }
                    }
                    if mouse_moved && input_status != MiResult::QuitMenu {
                        mouse_moved = false;
                        result = TTYM_IA_SELECT;
                        let mut i = 0usize;
                        while i < levels.len() {
                            let level_menu = menu_at(menu, &levels[i].path);
                            if levels[i].x <= x && x < levels[i].x + level_menu.width as i64 + 2 {
                                let dy = y - levels[i].y + first_item as i64;
                                if 0 <= dy && (dy as usize) < level_menu.count() {
                                    let dy = dy as usize;
                                    let has_submenu = level_menu.submenu[dy].is_some();
                                    if !has_submenu {
                                        result = if level_menu.panenumber[dy] != 0 {
                                            TTYM_SUCCESS
                                        } else {
                                            TTYM_IA_SELECT
                                        };
                                    }
                                    pane = levels[i].pane - 1;
                                    selidx = dy as i32;
                                    // We hit some part of a menu, so drop
                                    // extra menus that have been opened.
                                    // That does not include an open and
                                    // active submenu.
                                    let mut sub_path = levels[i].path.clone();
                                    sub_path.push(dy);
                                    let active_submenu = i + 2 == levels.len()
                                        && has_submenu
                                        && levels[i + 1].path == sub_path;
                                    if !active_submenu {
                                        while i + 1 < levels.len() {
                                            let level = levels.pop().expect("open level");
                                            cur = screen_update(
                                                &mut state.borrow_mut(),
                                                &level.screen_behind,
                                            );
                                        }
                                    }
                                    if i + 1 == levels.len() && has_submenu {
                                        let level_x = levels[i].x;
                                        let level_y = levels[i].y;
                                        let level_pane = levels[i].pane;
                                        let width = level_menu.width as i64;
                                        let sub_pane = level_menu.panenumber[dy];
                                        let path = levels[i].path.clone();
                                        let mut state_ref = state.borrow_mut();
                                        cur = menu_display(
                                            &mut state_ref,
                                            interpreter,
                                            cur,
                                            menu,
                                            &path,
                                            level_x,
                                            level_y,
                                            level_pane,
                                            &faces,
                                            x,
                                            y,
                                            first_item,
                                            true,
                                            &mut help_message,
                                            &mut help_paneno,
                                            &mut help_itemno,
                                            None,
                                        );
                                        let behind = save_rows(&state_ref);
                                        drop(state_ref);
                                        levels.push(MenuLevel {
                                            path: sub_path,
                                            pane: sub_pane,
                                            screen_behind: behind,
                                            x: level_x + width + 2,
                                            y,
                                        });
                                    }
                                }
                            }
                            i += 1;
                        }
                        let top = levels.last().expect("a shown menu");
                        let (path, top_x, top_y, top_pane) =
                            (top.path.clone(), top.x, top.y, top.pane);
                        // The call to display help-echo below will move the
                        // cursor, so remember its current position as
                        // computed by tty_menu_display.
                        cur = menu_display(
                            &mut state.borrow_mut(),
                            interpreter,
                            cur,
                            menu,
                            &path,
                            top_x,
                            top_y,
                            top_pane,
                            &faces,
                            x,
                            y,
                            first_item,
                            true,
                            &mut help_message,
                            &mut help_paneno,
                            &mut help_itemno,
                            None,
                        );
                        (col, row) = cur;
                    } else {
                        help_message = prev_help.clone();
                    }
                    // Display the help-echo message for the
                    // currently-selected menu item.
                    if (help_message.is_some() || prev_help.is_some()) && help_message != prev_help
                    {
                        let text = help_message
                            .as_ref()
                            .and_then(|(path, j)| menu_at(menu, path).help_text[*j].clone());
                        // Its message waits for redisplay, which the menu
                        // inhibits.
                        crate::lisp::primitives::menu::tty_menu_help_callback(
                            interpreter,
                            text.as_deref(),
                            help_paneno,
                            help_itemno,
                            env,
                        )?;
                        // Move the cursor to the beginning of the current
                        // menu item, so that screen readers and other
                        // accessibility aids know where the active region
                        // is.
                        cursor_to(col, row);
                        prev_help = help_message.clone();
                    }
                }
                Ok(())
            })();
            // Restore what the menus covered, all the way down, and turn
            // the cursor back on where the frame update leaves it.
            let first = levels.into_iter().next().expect("the menu's own level");
            screen_update(&mut state.borrow_mut(), &first.screen_behind);
            {
                let mut out = terminal_output();
                let _ = queue!(out, cursor::Show);
                let _ = out.flush();
            }
            interpreter.set_variable_key(
                cached_symbol!("inhibit-redisplay"),
                prev_inhibit_redisplay,
                env,
            );
            outcome?;
            Ok(TtyMenuActivation {
                status: result,
                pane,
                selidx,
            })
        },
    )
}

/// The display text of a tty menu item's C string: its characters, each
/// wide one followed by its padding cells.
fn menu_item_display_text(interpreter: &Interpreter, bytes: &[u8]) -> String {
    let env = Env::new();
    let mut text = String::new();
    let mut p = 0;
    while p < bytes.len() && bytes[p] != 0 {
        let (code, len) = crate::lisp::primitives::syntax::string_char_and_length(&bytes[p..]);
        p += len;
        let character = char::from_u32(code as u32).unwrap_or('\u{FFFD}');
        text.push(character);
        let width = crate::lisp::primitives::character_width(interpreter, code as u32, &env);
        for _ in 1..width {
            text.push(PADDING_GLYPH);
        }
    }
    text
}

/// term.c:mouse_get_xy: the mouse's position on the selected frame
/// (mouse_position, calling `mouse-position-function' when
/// `tty-menu-calls-mouse-position-function' says so) into X and Y.
fn mouse_get_xy(
    interpreter: &mut Interpreter,
    env: &mut Env,
    x: &mut i64,
    y: &mut i64,
) -> Result<(), LispError> {
    let frame = interpreter.selected_frame_value();
    let mut mouse = Value::cons(frame, Value::cons(Value::Nil, Value::Nil));
    let call_function = interpreter
        .lookup_var_key(
            cached_symbol!("tty-menu-calls-mouse-position-function"),
            env,
        )
        .is_some_and(|value| value.is_truthy());
    let function = interpreter
        .lookup_var_key(cached_symbol!("mouse-position-function"), env)
        .unwrap_or(Value::Nil);
    if call_function && !function.is_nil() {
        mouse = interpreter.call_function_value(function, None, &[mouse], env)?;
    }
    if let Some((head, rest)) = mouse.cons_values()
        && crate::lisp::types::eq(head, frame)
        && let Some((lmx, lmy)) = rest.cons_values()
        && !lmx.is_nil()
    {
        *x = lmx.as_integer().unwrap_or(*x);
        *y = lmy.as_integer().unwrap_or(*y);
    }
    Ok(())
}

/// keyboard.c:read_menu_command: a key sequence read in the menu's
/// navigation map without echoing its keys, t for none (an end of input
/// or a menu rejected), else its command (nil for an undefined key);
/// Emacs killed if the selected frame died meanwhile.
fn read_menu_command(interpreter: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
    let count = interpreter.specpdl_index();
    // We don't want to echo the keystrokes while navigating the menus.
    interpreter.specbind_symbol(
        &cached_symbol!("echo-keystrokes").with(|symbol| *symbol),
        Value::Integer(0),
        env,
    )?;
    let read =
        crate::lisp::primitives::read_key_sequence(interpreter, Value::Nil, false, true, true, env);
    let unbound = match &read {
        Err(error) => interpreter
            .with_lisp_stack_roots(error, |interpreter| interpreter.unbind_to(count, env)),
        Ok(_) => interpreter.unbind_to(count, env),
    };
    unbound?;
    let read = read?;
    if !interpreter.frame_is_live(interpreter.selected_frame_id) {
        crate::lisp::primitives::call(interpreter, "kill-emacs", &[], env)?;
    }
    if read.menu_rejected || read.keys.is_empty() {
        return Ok(Value::T);
    }
    Ok(read.cmd)
}

/// redisplay_internal before its echo area: reconsider_clip_changes of
/// the selected window (a narrowing put back as it was displayed is no
/// change), then, when the window shows the current buffer, its mode line
/// updated for a star to write or remove or a column it no longer shows,
/// and every window of a buffer whose narrowing changed.
fn selected_window_mode_line_checks(
    interpreter: &mut Interpreter,
    env: &mut Env,
    state: &TtyState,
) {
    use crate::lisp::primitives::{WINDOW_LAST_HAD_STAR_SLOT, WINDOW_UPDATE_MODE_LINE_SLOT};
    let window = interpreter.selected_window_id();
    let Some(buffer_id) = redisplay_flags::window_buffer(interpreter, window) else {
        return;
    };
    let shown_bounds = state
        .views
        .get(&window)
        .filter(|view| view.last_buffer_id == Some(buffer_id))
        .and_then(|view| view.last_restriction);
    if let Some(mut buffer) = interpreter.get_buffer_by_id_mut(buffer_id)
        && buffer.clip_changed()
        && shown_bounds == Some(buffer.accessible_bounds())
    {
        buffer.set_clip_changed(false);
    }
    // Point moved into or out of a composition since the window's last
    // display (window_end_valid: a display of it to compare with) has the
    // window redisplayed as for a narrowing change.
    let shown = state
        .views
        .get(&window)
        .and_then(|view| view.last_buffer_id);
    if let Some(shown_buffer) = shown
        && interpreter
            .get_buffer_by_id(buffer_id)
            .is_some_and(|buffer| !buffer.clip_changed())
    {
        let last_point = crate::lisp::primitives::window_count_slot(
            interpreter,
            window,
            crate::lisp::primitives::WINDOW_LAST_POINT_SLOT,
        )
        .and_then(|point| usize::try_from(point).ok());
        let point = redisplay_flags::window_point(interpreter, window);
        if let (Some(last_point), Some(point)) = (last_point, point)
            && (shown_buffer != buffer_id || point != last_point)
            && crate::lisp::primitives::check_point_in_composition(
                interpreter,
                shown_buffer,
                last_point,
                buffer_id,
                point,
            )
            && let Some(mut buffer) = interpreter.get_buffer_by_id_mut(buffer_id)
        {
            buffer.set_clip_changed(true);
        }
    }
    if buffer_id != interpreter.current_buffer_id() {
        return;
    }
    let modified = interpreter.buffer.borrow().is_modified();
    if modified
        != crate::lisp::primitives::window_flag(interpreter, window, WINDOW_LAST_HAD_STAR_SLOT)
        || redisplay_flags::mode_line_update_needed(interpreter, env, window)
    {
        crate::lisp::primitives::set_window_flag(
            interpreter,
            window,
            WINDOW_UPDATE_MODE_LINE_SLOT,
            true,
        );
    }
    if interpreter.buffer.borrow().clip_changed() {
        interpreter.bset_update_mode_line(buffer_id);
    }
}

/// What redisplay_window decides for a window's mode lines: whether it
/// has them to update (its local update_mode_line), and whether it
/// displays them (display_mode_lines).
struct ModeLineDecision {
    update_mode_line: bool,
    mode_lines: bool,
}

/// redisplay_window's update_mode_line for INFO, which REDISPLAYED says
/// it redisplays: the window's own flag, update_mode_lines, the buffer's
/// changed narrowing or forbidden shortcuts, or the start decision
/// (PLANNED, set where it forced the start or scrolled).  The mode lines
/// are displayed for it, for a window narrower than the frame when
/// every window is considered (frame-based redisplay redraws its side),
/// for a line number shown, or a column shown that point left.
fn window_displays_mode_line(
    interpreter: &mut Interpreter,
    env: &mut Env,
    info: &crate::lisp::primitives::WindowRenderInfo,
    cols: usize,
    redisplayed: bool,
    consider_all_windows: bool,
    planned: bool,
) -> ModeLineDecision {
    if !redisplayed {
        return ModeLineDecision {
            update_mode_line: false,
            mode_lines: false,
        };
    }
    let (clip_changed, prevent) =
        interpreter
            .get_buffer_by_id(info.buffer_id)
            .map_or((false, false), |buffer| {
                (
                    buffer.clip_changed(),
                    buffer.prevent_redisplay_optimizations_p(),
                )
            });
    let update_mode_line = planned
        || crate::lisp::primitives::window_flag(
            interpreter,
            info.window_id,
            crate::lisp::primitives::WINDOW_UPDATE_MODE_LINE_SLOT,
        )
        || interpreter.update_mode_lines != 0
        || clip_changed
        || prevent;
    let full_width = info.left == 0 && info.width >= cols;
    let column_left = info.selected
        && info.buffer_id == interpreter.current_buffer_id()
        && crate::lisp::primitives::window_count_slot(
            interpreter,
            info.window_id,
            crate::lisp::primitives::WINDOW_COLUMN_NUMBER_DISPLAYED_SLOT,
        )
        .is_some_and(|displayed| {
            crate::lisp::primitives::current_column(interpreter, env)
                .is_ok_and(|column| column as i64 != displayed)
        });
    let mode_lines = update_mode_line
        || (consider_all_windows && !full_width)
        || crate::lisp::primitives::window_base_line_pos(interpreter, info.window_id) > 0
        || column_left;
    ModeLineDecision {
        update_mode_line,
        mode_lines,
    }
}

/// finish_menu_bars when the frame's selected window is its minibuffer
/// window, which the window layout leaves out: redisplay_window of the
/// mini window reaches the menu bar with its mode line to update.
fn selected_mini_window_updates_menu_bar(
    interpreter: &Interpreter,
    layout: &[crate::lisp::primitives::WindowRenderInfo],
    consider_all_windows: bool,
) -> bool {
    let window = interpreter.selected_window_id();
    if layout.iter().any(|info| info.window_id == window) {
        return false;
    }
    if consider_all_windows && redisplay_flags::needs_no_redisplay(interpreter, window) {
        return false;
    }
    let buffer = redisplay_flags::window_buffer(interpreter, window)
        .and_then(|buffer| {
            interpreter
                .get_buffer_by_id(buffer)
                .map(|b| (b.clip_changed(), b.prevent_redisplay_optimizations_p()))
        })
        .unwrap_or((false, false));
    crate::lisp::primitives::window_flag(
        interpreter,
        window,
        crate::lisp::primitives::WINDOW_UPDATE_MODE_LINE_SLOT,
    ) || interpreter.update_mode_lines != 0
        || buffer.0
        || buffer.1
}

/// A mode, header or tab line that redisplay does not display keeps the
/// cells the terminal shows, when the last paint has them.
fn keep_painted_cells(
    frame: &mut [PaintRow],
    painted: &[PaintRow],
    job: &impl ModeLineCellsJob,
) -> bool {
    let (row, left, width) = job.cells();
    let Some(old) = painted.get(row) else {
        return false;
    };
    let Some(new) = frame.get_mut(row) else {
        return false;
    };
    let end = left + width;
    if old.text.len() != new.text.len() || end > old.text.len() || old.text == ['\u{0}'] {
        return false;
    }
    new.text[left..end].copy_from_slice(&old.text[left..end]);
    new.attrs[left..end].copy_from_slice(&old.attrs[left..end]);
    new.marks.retain(|(cell, _)| *cell < left || *cell >= end);
    new.marks.extend(
        old.marks
            .iter()
            .filter(|(cell, _)| *cell >= left && *cell < end),
    );
    true
}

/// The cells a mode-line job paints: its row, first column and width.
trait ModeLineCellsJob {
    fn cells(&self) -> (usize, usize, usize);
}

/// A row's cells given as (row, first column, width).
impl ModeLineCellsJob for (usize, usize, usize) {
    fn cells(&self) -> (usize, usize, usize) {
        *self
    }
}

/// The end of redisplay_internal, its update done: the windows displayed
/// marked accurate (every considered window, else the selected one, its
/// buffer's flag first copied to the buffer's other windows), the overlay
/// arrows recorded as drawn, the frame's and the global requests cleared.
fn mark_redisplay_done(interpreter: &mut Interpreter, windows: &[u64], consider_all_windows: bool) {
    if consider_all_windows {
        for &window in windows {
            redisplay_flags::mark_window_display_accurate(interpreter, window);
        }
        let mini = interpreter.minibuffer_window_id();
        if !windows.contains(&mini) {
            redisplay_flags::mark_window_display_accurate(interpreter, mini);
        }
        let frame = interpreter.selected_frame_id;
        frame.borrow_mut().redisplay = false;
    } else {
        let window = interpreter.selected_window_id();
        if let Some(buffer) = redisplay_flags::window_buffer(interpreter, window)
            && interpreter
                .get_buffer_by_id(buffer)
                .is_some_and(|b| b.text_redisplay())
            && interpreter.buffer_window_count(buffer) > 1
        {
            let all = crate::lisp::primitives::frames_window_list(interpreter);
            redisplay_flags::propagate_buffer_redisplay(interpreter, &all);
        }
        redisplay_flags::mark_window_display_accurate(interpreter, window);
    }
    redisplay_flags::update_overlay_arrows(interpreter, 1);
    interpreter.update_mode_lines = 0;
    interpreter.windows_or_buffers_changed = 0;
}
