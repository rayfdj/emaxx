//! frame.c/terminal.c ownership and term.c's external TTY device boundary.
use super::{Interpreter, Value};
use crate::lisp::types::LispError;
use crate::lisp::types::{Kind, TerminalRef};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct TerminalState {
    pub live: bool,
    pub name: String,
    pub kind: Option<String>,
    /// term.c's color support of the terminal's tty_display_info.
    pub colors: TtyColors,
    /// Its appearance strings.
    pub appearance: TtyAppearance,
    /// tty->previous_color_mode: the `tty-color-mode' set_tty_color_mode
    /// last set up (0, the entry's own, at first).
    pub previous_color_mode: i64,
    /// tty->previous_frame: the frame redisplay last showed on the
    /// terminal, by identity (none after store_frame_param changed its
    /// `tty-color-mode').
    pub previous_frame: Option<usize>,
    pub terminal_coding: Option<String>,
    /// The coding system whose charsets t->charset_list holds: the
    /// characters produce_glyphs draws rather than shows as glyphless.
    /// None until `set-terminal-coding-system-internal' sets it, when
    /// every character with a charset is drawn.
    pub charset_list_coding: Option<String>,
    pub keyboard_coding: Option<String>,
    /// The KBOARD's variables (DEFVAR_KBOARD), by their symbols.
    pub keyboard: std::collections::HashMap<
        crate::lisp::types::SymbolName,
        Value,
        crate::lisp::types::IdentityBuildHasher,
    >,
    pub pending_input: Vec<u8>,
    pub top_frame: Option<crate::lisp::types::FrameRef>,
    pub device: Option<Arc<TtyDevice>>,
}

impl TerminalState {
    pub fn initial() -> Self {
        Self {
            live: true,
            name: "initial_terminal".into(),
            kind: None,
            colors: TtyColors::default(),
            appearance: TtyAppearance::default(),
            previous_color_mode: 0,
            previous_frame: None,
            terminal_coding: None,
            charset_list_coding: None,
            keyboard_coding: Some("no-conversion".into()),
            keyboard: Default::default(),
            pending_input: Vec::new(),
            top_frame: None,
            device: None,
        }
    }
}

/// term.c's color fields of a tty_display_info: TN_max_colors, the
/// TS_orig_pair, TS_set_foreground and TS_set_background strings (None
/// where there is none), and TN_no_color_video (NC).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TtyColors {
    pub max_colors: i64,
    pub orig_pair: Option<Vec<u8>>,
    pub set_foreground: Option<Vec<u8>>,
    pub set_background: Option<Vec<u8>>,
    pub no_color_video: i64,
}

thread_local! {
    /// term.c's default_max_colors, default_orig_pair, default_set_foreground,
    /// default_set_background and default_no_color_video: one set, which
    /// the last init_tty saved.
    static DEFAULT_TTY_COLORS: std::cell::RefCell<TtyColors> =
        std::cell::RefCell::new(TtyColors::default());
}

/// term.c's enum no_color_bit: the appearances NC says not to combine
/// with colors.
pub(crate) const NC_UNDERLINE: i64 = 1 << 1;
pub(crate) const NC_REVERSE: i64 = 1 << 2;
pub(crate) const NC_BOLD: i64 = 1 << 5;

impl TtyColors {
    /// tty_default_color_capabilities (TTY, true): these become the
    /// defaults mode 0 returns to.
    pub(crate) fn save_default(&self) {
        DEFAULT_TTY_COLORS.with_borrow_mut(|defaults| *defaults = self.clone());
    }

    /// tty_setup_colors: MODE -1 (or less) turns colors off, 8 sets up
    /// the 8 ANSI colors, any other the defaults init_tty saw.
    pub(crate) fn setup(&mut self, mode: i64) {
        match mode.max(-1) {
            -1 => {
                self.max_colors = 0;
                self.no_color_video = 0;
                self.set_foreground = None;
                self.set_background = None;
                self.orig_pair = None;
            }
            8 => {
                self.orig_pair = Some(b"\x1b[0m".to_vec());
                self.set_foreground = Some(b"\x1b[3%p1%dm".to_vec());
                self.set_background = Some(b"\x1b[4%p1%dm".to_vec());
                self.max_colors = 8;
                self.no_color_video = 0;
            }
            _ => *self = DEFAULT_TTY_COLORS.with_borrow(Clone::clone),
        }
    }

    /// MAY_USE_WITH_COLORS_P: appearance ATTR (NC_ bits) may be shown.
    pub(crate) fn may_use_with_colors(&self, attr: i64) -> bool {
        self.max_colors <= 0 || self.no_color_video & attr == 0
    }
}

/// term.c's appearance strings of a tty_display_info as init_tty leaves
/// them (None where there is none): TS_standout_mode and
/// TS_end_standout_mode (so and se; none for a magic-cookie standout, sg;
/// the underline strings where there is no so; me where there is no se,
/// and no standout when there is neither; no standout on a teleray, xt),
/// TS_enter_underline_mode and TS_exit_underline_mode (us and ue; none
/// for magic-cookie underlining, ug; no us after `tty-no-underline'),
/// TF_set_underline_style (Smulx, else the kitty sequence for the Su
/// flag) and with it TF_set_underline_color, TS_enter_bold_mode (md),
/// TS_enter_dim_mode (mh), TS_enter_italic_mode (ZH),
/// TS_enter_strike_through_mode (smxx) and TS_exit_attribute_mode (me).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TtyAppearance {
    pub standout_mode: Option<Vec<u8>>,
    pub end_standout_mode: Option<Vec<u8>>,
    pub enter_underline_mode: Option<Vec<u8>>,
    pub exit_underline_mode: Option<Vec<u8>>,
    pub set_underline_style: Option<Vec<u8>>,
    pub set_underline_color: Option<Vec<u8>>,
    pub enter_bold_mode: Option<Vec<u8>>,
    pub enter_dim_mode: Option<Vec<u8>>,
    pub enter_italic_mode: Option<Vec<u8>>,
    pub enter_strike_through_mode: Option<Vec<u8>>,
    pub exit_attribute_mode: Option<Vec<u8>>,
}

/// cm.h:emacs_tputs (OUTPUT1): the library's tputs of STRING with one
/// line affected, its padding as the library sends it (pad characters for
/// the output speed, none under xon/xoff); where there is no library,
/// STRING with its `$<...>' delays left out.
pub(crate) fn tputs(string: &[u8]) -> Vec<u8> {
    #[cfg(unix)]
    if let Some(output) = library_tputs(string) {
        return output;
    }
    strip_padding(string)
}

thread_local! {
    /// What the library's tputs hands its output function.
    static TPUTS_OUTPUT: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// term.c's cmputc, collecting the bytes instead of writing them.
#[cfg(unix)]
extern "C" fn collect_tputs_byte(c: libc::c_int) -> libc::c_int {
    TPUTS_OUTPUT.with_borrow_mut(|output| output.push(c as u8));
    c
}

#[cfg(unix)]
fn library_tputs(string: &[u8]) -> Option<Vec<u8>> {
    let library = terminfo_library()?;
    let string = std::ffi::CString::new(string).ok()?;
    let _guard = TERMCAP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    // SAFETY: tputs is the library's documented function; STRING lives
    // through the call, and the output function only appends to a
    // thread-local buffer.
    unsafe {
        let tputs = library
            .get::<unsafe extern "C" fn(
                *const libc::c_char,
                libc::c_int,
                extern "C" fn(libc::c_int) -> libc::c_int,
            ) -> libc::c_int>(b"tputs\0")
            .ok()?;
        TPUTS_OUTPUT.with_borrow_mut(Vec::clear);
        tputs(string.as_ptr(), 1, collect_tputs_byte);
    }
    Some(TPUTS_OUTPUT.with_borrow_mut(std::mem::take))
}

/// STRING without its `$<...>' padding delays.
fn strip_padding(string: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(string.len());
    let mut at = 0;
    while at < string.len() {
        if string[at] == b'$' && string.get(at + 1) == Some(&b'<') {
            let mut end = at + 2;
            while end < string.len()
                && (string[end].is_ascii_digit() || matches!(string[end], b'.' | b'*' | b'/'))
            {
                end += 1;
            }
            if string.get(end) == Some(&b'>') && end > at + 2 {
                at = end + 1;
                continue;
            }
        }
        out.push(string[at]);
        at += 1;
    }
    out
}

/// term.c's no_color_bit for italic, dim and strike-through.
pub(crate) const NC_ITALIC: i64 = 1 << 3;
pub(crate) const NC_DIM: i64 = 1 << 4;
pub(crate) const NC_STRIKE_THROUGH: i64 = 1 << 6;

/// dispextern.h's TTY_CAP_ bits, the capabilities tty_capable_p tests.
pub(crate) const TTY_CAP_INVERSE: u32 = 0x01;
pub(crate) const TTY_CAP_UNDERLINE: u32 = 0x02;
pub(crate) const TTY_CAP_BOLD: u32 = 0x04;
pub(crate) const TTY_CAP_DIM: u32 = 0x08;
pub(crate) const TTY_CAP_ITALIC: u32 = 0x10;
pub(crate) const TTY_CAP_STRIKE_THROUGH: u32 = 0x20;
/// (0x32 & TTY_CAP_UNDERLINE): the underline bit itself, so plain
/// underlining needs the styled-underline string as well.
pub(crate) const TTY_CAP_UNDERLINE_STYLED: u32 = 0x32 & TTY_CAP_UNDERLINE;

/// term.c:tty_capable_p: every capability in CAPS has its string and may
/// be shown with the terminal's colors (MAY_USE_WITH_COLORS_P).
pub(crate) fn tty_capable_p(appearance: &TtyAppearance, colors: &TtyColors, caps: u32) -> bool {
    [
        (TTY_CAP_INVERSE, &appearance.standout_mode, NC_REVERSE),
        (
            TTY_CAP_UNDERLINE,
            &appearance.enter_underline_mode,
            NC_UNDERLINE,
        ),
        (
            TTY_CAP_UNDERLINE_STYLED,
            &appearance.set_underline_style,
            NC_UNDERLINE,
        ),
        (TTY_CAP_BOLD, &appearance.enter_bold_mode, NC_BOLD),
        (TTY_CAP_DIM, &appearance.enter_dim_mode, NC_DIM),
        (TTY_CAP_ITALIC, &appearance.enter_italic_mode, NC_ITALIC),
        (
            TTY_CAP_STRIKE_THROUGH,
            &appearance.enter_strike_through_mode,
            NC_STRIKE_THROUGH,
        ),
    ]
    .into_iter()
    .all(|(cap, string, no_color_bit)| {
        caps & cap == 0 || (string.is_some() && colors.may_use_with_colors(no_color_bit))
    })
}

/// The terminal capability library, loaded once (the termcap and
/// terminfo functions init_tty and tparam call), with the lock its
/// static buffers need.
#[cfg(unix)]
fn terminfo_library() -> Option<&'static libloading::Library> {
    static LIBRARY: std::sync::OnceLock<Option<libloading::Library>> = std::sync::OnceLock::new();
    LIBRARY
        .get_or_init(|| {
            [
                "libtinfo.so.6",
                "libtinfo.so.5",
                "libncursesw.so.6",
                "/usr/lib/libncurses.dylib",
            ]
            .iter()
            // SAFETY: loading the system terminal capability library runs
            // no initialization with preconditions.
            .find_map(|path| unsafe { libloading::Library::new(path) }.ok())
        })
        .as_ref()
}

#[cfg(unix)]
static TERMCAP_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// terminfo.c:tparam: STRING with ARG through the library's tparm, the
/// bytes copied out of its static buffer; none where it cannot be.
#[cfg(unix)]
pub(crate) fn tparam(string: &[u8], arg: i64) -> Vec<u8> {
    let Some(library) = terminfo_library() else {
        return Vec::new();
    };
    let Ok(string) = std::ffi::CString::new(string) else {
        return Vec::new();
    };
    let _guard = TERMCAP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    // SAFETY: tparm is the library's documented varargs function; STRING
    // lives through the call and its result is copied before the lock
    // is released.
    unsafe {
        let Ok(tparm) = library
            .get::<unsafe extern "C" fn(*const libc::c_char, ...) -> *mut libc::c_char>(b"tparm\0")
        else {
            return Vec::new();
        };
        let arg = arg as libc::c_long;
        let result = tparm(
            string.as_ptr(),
            arg,
            0 as libc::c_long,
            0 as libc::c_long,
            0 as libc::c_long,
            0 as libc::c_long,
            0 as libc::c_long,
            0 as libc::c_long,
            0 as libc::c_long,
            0 as libc::c_long,
        );
        if result.is_null() {
            Vec::new()
        } else {
            std::ffi::CStr::from_ptr(result).to_bytes().to_vec()
        }
    }
}

/// The parts of a terminal type's termcap entry term.c:init_tty takes.
#[cfg(unix)]
pub(crate) struct Termcap {
    pub width: i64,
    pub height: i64,
    pub colors: TtyColors,
    pub appearance: TtyAppearance,
    /// tty_set_terminal_modes's output (TS_termcap_modes, TS_keypad_mode)
    /// and the clear a new frame starts from.
    pub enter: Vec<u8>,
    /// tty_reset_terminal_modes's (TS_cursor_normal, TS_end_keypad_mode,
    /// TS_end_termcap_modes).
    pub exit: Vec<u8>,
    /// term_get_fkeys's function keys, for the terminal's
    /// `input-decode-map'.
    pub keys: Vec<(Vec<u8>, String)>,
    pub cursor_motion: bool,
    /// tty->meta_key's start: the terminal has a Meta key (km or MT).
    pub meta_key: bool,
    /// The strings of init_tty's TS_ fields that tty_set_terminal_modes
    /// and tty_reset_terminal_modes write: ti, te, ks, ke, vs, ve, op.
    pub termcap_modes: Vec<u8>,
    pub end_termcap_modes: Vec<u8>,
    pub keypad_mode: Vec<u8>,
    pub end_keypad_mode: Vec<u8>,
    pub cursor_visible: Vec<u8>,
    pub cursor_normal: Vec<u8>,
    pub orig_pair: Vec<u8>,
}

/// emacs.c's dev_tty: the terminal init_tty opens for the startup frame,
/// DEV_TTY ("/dev/tty") unless `-t DEVICE' named another.
static DEV_TTY: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// main's `-t DEVICE': DEVICE is dev_tty from here on.
pub(crate) fn set_dev_tty(name: &str) {
    *DEV_TTY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(name.to_owned());
}

/// dev_tty's name.
pub(crate) fn dev_tty() -> String {
    DEV_TTY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
        .unwrap_or_else(|| "/dev/tty".into())
}

/// What init_tty's tgetent answered when it found no entry.
#[derive(Debug)]
pub(crate) enum TermcapError {
    /// tgetent < 0 (or no terminal library).
    NoDatabase,
    /// tgetent == 0: no entry for the type.
    Undefined(String),
}

impl TermcapError {
    /// maybe_fatal's STR2, the startup terminal's fatal text.
    pub(crate) fn fatal_text(&self) -> String {
        match self {
            Self::NoDatabase => "Cannot open terminfo database file".into(),
            Self::Undefined(kind) => format!(
                "Terminal type {kind} is not defined.\n\
If that is not the actual type of terminal you have,\n\
use the Bourne shell command 'TERM=...; export TERM' (C-shell:\n\
'setenv TERM ...') to specify the correct type.  It may be necessary\n\
to do 'unset TERMINFO' (C-shell: 'unsetenv TERMINFO') as well."
            ),
        }
    }
}

/// maybe_fatal's STR1, the error a later terminal signals.
impl std::fmt::Display for TermcapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoDatabase => f.write_str("Cannot open terminfo database file"),
            Self::Undefined(kind) => write!(f, "Terminal type {kind} is not defined"),
        }
    }
}

impl From<TermcapError> for LispError {
    fn from(error: TermcapError) -> Self {
        LispError::Signal(error.to_string())
    }
}

/// term.c:init_tty's "not powerful enough" maybe_fatal STR2 for KIND.
pub(crate) fn cursor_motion_fatal_text(kind: &str) -> String {
    format!(
        "Terminal type \"{kind}\" is not powerful enough to run Emacs.\n\
It lacks the ability to position the cursor.\n\
If that is not the actual type of terminal you have,\n\
use the Bourne shell command 'TERM=...; export TERM' (C-shell:\n\
'setenv TERM ...') to specify the correct type.  It may be necessary\n\
to do 'unset TERMINFO' (C-shell: 'unsetenv TERMINFO') as well."
    )
}

/// term.c:init_tty's termcap lookup of KIND for the terminal on FD (its
/// size taken from the descriptor where it answers).  Capabilities are
/// copied while the library's lock is held; no pointer into termcap's
/// buffers escapes.
#[cfg(unix)]
pub(crate) fn read_termcap(kind: &str, fd: libc::c_int) -> Result<Termcap, TermcapError> {
    use std::ffi::{CStr, CString};
    let _guard = TERMCAP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    // SAFETY: these are the system terminal capability library and its
    // documented C functions; all input strings and output buffers live
    // through the calls. We retain copied bytes, never library pointers.
    let termcap = unsafe {
        let library = terminfo_library().ok_or(TermcapError::NoDatabase)?;
        let getent = library
            .get::<unsafe extern "C" fn(*mut libc::c_char, *const libc::c_char) -> libc::c_int>(
                b"tgetent\0",
            )
            .map_err(|_| TermcapError::NoDatabase)?;
        let getnum = library
            .get::<unsafe extern "C" fn(*const libc::c_char) -> libc::c_int>(b"tgetnum\0")
            .map_err(|_| TermcapError::NoDatabase)?;
        let getstr =
            library
                .get::<unsafe extern "C" fn(
                    *const libc::c_char,
                    *mut *mut libc::c_char,
                ) -> *mut libc::c_char>(b"tgetstr\0")
                .map_err(|_| TermcapError::NoDatabase)?;
        let kind_c = CString::new(kind).map_err(|_| TermcapError::Undefined(kind.to_owned()))?;
        let mut buffer = [0i8; 4096];
        match getent(buffer.as_mut_ptr(), kind_c.as_ptr()) {
            n if n < 0 => return Err(TermcapError::NoDatabase),
            0 => return Err(TermcapError::Undefined(kind.to_owned())),
            _ => {}
        }
        let cap = |key: &CStr| {
            let value = getstr(key.as_ptr(), std::ptr::null_mut());
            if value.is_null() {
                Vec::new()
            } else {
                CStr::from_ptr(value).to_bytes().to_vec()
            }
        };
        let getflag = library
            .get::<unsafe extern "C" fn(*const libc::c_char) -> libc::c_int>(b"tgetflag\0")
            .map_err(|_| TermcapError::NoDatabase)?;
        // cm.c:Wcm_init accepts absolute motion or all four relative
        // directions. term.c supplies the legacy aliases and disables
        // downward motion on teleray terminals.
        let cursor_motion = !cap(c"cm").is_empty()
            || (!cap(c"up").is_empty()
                && !cap(c"nd").is_empty()
                && (getflag(c"bs".as_ptr()) != 0
                    || !cap(c"le").is_empty()
                    || !cap(c"bc").is_empty())
                && getflag(c"xt".as_ptr()) == 0
                && (!cap(c"do").is_empty() || !cap(c"nl").is_empty()));
        // term.c:keys and term_get_fkeys_1, in source order (later
        // capabilities override collisions in the input-decode map).
        let mut keys = Vec::new();
        for (capability, name) in [
            (c"kh", "home"),
            (c"kl", "left"),
            (c"ku", "up"),
            (c"kr", "right"),
            (c"kd", "down"),
            (c"%8", "prior"),
            (c"%5", "next"),
            (c"@7", "end"),
            (c"@1", "begin"),
            (c"*6", "select"),
            (c"%9", "print"),
            (c"@4", "execute"),
            (c"&8", "undo"),
            (c"%0", "redo"),
            (c"%7", "menu"),
            (c"@0", "find"),
            (c"@2", "cancel"),
            (c"%1", "help"),
            (c"&4", "reset"),
            (c"kE", "clearline"),
            (c"kA", "insertline"),
            (c"kL", "deleteline"),
            (c"kI", "insertchar"),
            (c"kD", "deletechar"),
            (c"kB", "backtab"),
            (c"@8", "kp-enter"),
            (c"K4", "kp-1"),
            (c"K5", "kp-3"),
            (c"K2", "kp-5"),
            (c"K1", "kp-7"),
            (c"K3", "kp-9"),
            (c"k1", "f1"),
            (c"k2", "f2"),
            (c"k3", "f3"),
            (c"k4", "f4"),
            (c"k5", "f5"),
            (c"k6", "f6"),
            (c"k7", "f7"),
            (c"k8", "f8"),
            (c"k9", "f9"),
            (c"&0", "S-cancel"),
            (c"&9", "S-begin"),
            (c"*0", "S-find"),
            (c"*1", "S-execute"),
            (c"*4", "S-delete"),
            (c"*7", "S-end"),
            (c"*8", "S-clearline"),
            (c"#1", "S-help"),
            (c"#2", "S-home"),
            (c"#3", "S-insert"),
            (c"#4", "S-left"),
            (c"%d", "S-menu"),
            (c"%c", "S-next"),
            (c"%e", "S-prior"),
            (c"%f", "S-print"),
            (c"%g", "S-redo"),
            (c"%i", "S-right"),
            (c"!3", "S-undo"),
        ] {
            let bytes = cap(capability);
            if !bytes.is_empty() {
                keys.push((bytes, name.to_owned()));
            }
        }
        let semi = cap(c"k;");
        let zero = cap(c"k0");
        if !zero.is_empty() {
            keys.push((zero, if semi.is_empty() { "f10" } else { "f0" }.into()));
        }
        if !semi.is_empty() {
            keys.push((semi, "f10".into()));
        }
        for n in 11..64u8 {
            let suffix = if n <= 19 {
                b'1' + n - 11
            } else if n <= 45 {
                b'A' + n - 20
            } else {
                b'a' + n - 46
            };
            let capability =
                CString::new(vec![b'F', suffix]).expect("termcap key names contain no NUL");
            let bytes = cap(&capability);
            if !bytes.is_empty() {
                keys.push((bytes, format!("f{n}")));
            }
        }
        let mut size: libc::winsize = std::mem::zeroed();
        // get_tty_size's answer, else the entry's co and li (tgetnum's -1
        // when it has none): init_tty's FrameCols and FrameRows.
        let (columns, rows) = if libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) == 0 {
            (i64::from(size.ws_col), i64::from(size.ws_row))
        } else {
            (0, 0)
        };
        let width = if columns > 0 {
            columns
        } else {
            i64::from(getnum(c"co".as_ptr()))
        };
        let height = if rows > 0 {
            rows
        } else {
            i64::from(getnum(c"li".as_ptr()))
        };
        // init_tty's SVr4/ANSI color support: none without "op" (no way
        // back to the default colors); else AF and AB (or SVr4's Sf and
        // Sb) and Co (tgetnum's -1 when the entry has none), with the
        // terminfo 24-bit forms taking precedence: setf24 and setb24, the
        // RGB flag, or the Tc flag or COLORTERM=truecolor's xterm+direct
        // strings; and NC.
        let cap_opt = |key: &CStr| {
            let value = getstr(key.as_ptr(), std::ptr::null_mut());
            (!value.is_null()).then(|| CStr::from_ptr(value).to_bytes().to_vec())
        };
        let tigetstr = library
            .get::<unsafe extern "C" fn(*const libc::c_char) -> *mut libc::c_char>(b"tigetstr\0")
            .ok();
        let tigetflag = library
            .get::<unsafe extern "C" fn(*const libc::c_char) -> libc::c_int>(b"tigetflag\0")
            .ok();
        // tigetstr's (char *) -1 is "not a string capability".
        let string_cap = |name: &CStr| {
            let value = tigetstr
                .as_ref()
                .map_or(std::ptr::null_mut(), |get| get(name.as_ptr()));
            (!value.is_null() && value as isize != -1)
                .then(|| CStr::from_ptr(value).to_bytes().to_vec())
        };
        let flag = |name: &CStr| tigetflag.as_ref().map_or(0, |get| get(name.as_ptr()));
        // The appearance strings, with init_tty's adjustments: magic-cookie
        // standout and underlining are not used, underlining stands in for
        // a missing standout, a standout nothing ends is not used, and a
        // teleray has none.
        let mut standout_mode = cap_opt(c"so");
        let mut end_standout_mode = cap_opt(c"se");
        let mut enter_underline_mode = cap_opt(c"us");
        let mut exit_underline_mode = cap_opt(c"ue");
        if standout_mode.is_some() && getnum(c"sg".as_ptr()) >= 0 {
            standout_mode = None;
            end_standout_mode = None;
        }
        if enter_underline_mode.is_some() && getnum(c"ug".as_ptr()) >= 0 {
            enter_underline_mode = None;
            exit_underline_mode = None;
        }
        if standout_mode.is_none() {
            standout_mode = enter_underline_mode.clone();
            end_standout_mode = exit_underline_mode.clone();
        }
        let exit_attribute_mode = cap_opt(c"me");
        if end_standout_mode.is_none() {
            end_standout_mode = exit_attribute_mode.clone();
            if end_standout_mode.is_none() {
                standout_mode = None;
            }
        }
        if getflag(c"xt".as_ptr()) != 0 {
            standout_mode = None;
        }
        let set_underline_style = string_cap(c"Smulx")
            .or_else(|| (getflag(c"Su".as_ptr()) != 0).then(|| b"\x1b[4:%p1%dm".to_vec()));
        let appearance = TtyAppearance {
            standout_mode,
            end_standout_mode,
            enter_underline_mode,
            exit_underline_mode,
            set_underline_color: set_underline_style.is_some().then(|| {
                b"\x1b[58:2::%p1%{65536}%/%d:%p1%{256}%/%{255}%&%d:%p1%{255}%&%dm".to_vec()
            }),
            set_underline_style,
            enter_bold_mode: cap_opt(c"md"),
            enter_dim_mode: cap_opt(c"mh"),
            enter_italic_mode: cap_opt(c"ZH"),
            enter_strike_through_mode: string_cap(c"smxx"),
            exit_attribute_mode,
        };
        let mut colors = TtyColors {
            orig_pair: cap_opt(c"op"),
            ..TtyColors::default()
        };
        if colors.orig_pair.is_some() {
            colors.set_foreground = cap_opt(c"AF");
            colors.set_background = cap_opt(c"AB");
            if colors.set_foreground.is_none() {
                colors.set_foreground = cap_opt(c"Sf");
                colors.set_background = cap_opt(c"Sb");
            }
            colors.max_colors = i64::from(getnum(c"Co".as_ptr()));
            if let (Some(fg), Some(bg)) = (string_cap(c"setf24"), string_cap(c"setb24")) {
                colors.set_foreground = Some(fg);
                colors.set_background = Some(bg);
                colors.max_colors = 16_777_216;
            } else if flag(c"RGB") > 0 {
                colors.max_colors = 16_777_216;
            } else if flag(c"Tc") > 0
                || std::env::var_os("COLORTERM")
                    .is_some_and(|value| value.eq_ignore_ascii_case("truecolor"))
            {
                colors.set_foreground = Some(
                    b"\x1b[%?%p1%{8}%<%t3%p1%d%e38;2;%p1%{65536}%/%d;%p1%{256}%/%{255}%&%d;%p1%{255}%&%d%;m"
                        .to_vec(),
                );
                colors.set_background = Some(
                    b"\x1b[%?%p1%{8}%<%t4%p1%d%e48;2;%p1%{65536}%/%d;%p1%{256}%/%{255}%&%d;%p1%{255}%&%d%;m"
                        .to_vec(),
                );
                colors.max_colors = 16_777_216;
            }
            colors.no_color_video = match getnum(c"NC".as_ptr()) {
                -1 => 0,
                n => i64::from(n),
            };
        }
        Termcap {
            width,
            height,
            colors,
            appearance,
            enter: [cap(c"ti"), cap(c"ks"), cap(c"cl")].concat(),
            exit: [cap(c"ve"), cap(c"ke"), cap(c"te")].concat(),
            keys,
            cursor_motion,
            meta_key: getflag(c"km".as_ptr()) != 0 || getflag(c"MT".as_ptr()) != 0,
            termcap_modes: cap(c"ti"),
            end_termcap_modes: cap(c"te"),
            keypad_mode: cap(c"ks"),
            end_keypad_mode: cap(c"ke"),
            cursor_visible: cap(c"vs"),
            cursor_normal: cap(c"ve"),
            orig_pair: cap(c"op"),
        }
    };
    Ok(termcap)
}

/// sysdep.c:init_sys_modes's terminal modes from ORIGINAL (the modes
/// Emacs found): no echo, no canonical input, signals on, the quit
/// character QUIT as VINTR and VQUIT on the CONTROLLING terminal (else
/// disabled, its C-g read as input), start/stop output control only with
/// FLOW_CONTROL, 8-bit input with META, and the job-control and editing
/// characters disabled.
#[cfg(unix)]
pub(crate) fn emacs_tty_modes(
    original: &libc::termios,
    controlling: bool,
    flow_control: bool,
    meta: bool,
    quit: u8,
) -> libc::termios {
    let mut tty = *original;
    tty.c_iflag |= libc::IGNBRK;
    tty.c_iflag &= !(libc::ICRNL | libc::INLCR | libc::ISTRIP);
    tty.c_lflag &= !(libc::ECHO | libc::ICANON | libc::IEXTEN);
    tty.c_lflag |= libc::ISIG;
    if flow_control {
        tty.c_iflag |= libc::IXON;
        tty.c_iflag &= !libc::IXANY;
    } else {
        tty.c_iflag &= !libc::IXON;
    }
    tty.c_oflag &= !libc::ONLCR;
    #[cfg(target_os = "linux")]
    {
        tty.c_oflag &= !libc::TAB3;
    }
    #[cfg(target_os = "macos")]
    {
        tty.c_oflag &= !libc::OXTABS;
    }
    if meta {
        tty.c_cflag |= libc::CS8;
        tty.c_cflag &= !libc::PARENB;
    }
    let disabled = libc::_POSIX_VDISABLE;
    if controlling {
        // C-g (usually) gives SIGINT, and for SysV TERMIO SIGQUIT alike.
        tty.c_cc[libc::VINTR] = quit;
        tty.c_cc[libc::VQUIT] = quit;
    } else {
        tty.c_cc[libc::VINTR] = disabled;
        tty.c_cc[libc::VQUIT] = disabled;
    }
    tty.c_cc[libc::VMIN] = 1;
    tty.c_cc[libc::VTIME] = 0;
    for index in [
        libc::VSUSP,
        libc::VLNEXT,
        libc::VREPRINT,
        libc::VWERASE,
        libc::VDISCARD,
    ] {
        tty.c_cc[index] = disabled;
    }
    #[cfg(target_os = "macos")]
    {
        tty.c_cc[libc::VDSUSP] = disabled;
    }
    if flow_control {
        tty.c_cc[libc::VSTART] = 0o21;
        tty.c_cc[libc::VSTOP] = 0o23;
    } else {
        tty.c_cc[libc::VSTART] = disabled;
        tty.c_cc[libc::VSTOP] = disabled;
    }
    tty
}

#[derive(Debug)]
pub(crate) struct TtyDevice {
    pub file: crate::file_system::File,
    pub width: i64,
    pub height: i64,
    pub colors: TtyColors,
    pub appearance: TtyAppearance,
    #[cfg(unix)]
    original: Option<libc::termios>,
    exit: Vec<u8>,
    keys: Vec<(Vec<u8>, String)>,
}

#[cfg(unix)]
impl TtyDevice {
    /// term.c:init_tty: open and validate the device before looking up its
    /// termcap entry. Copy capabilities while holding the library's global
    /// lock; no pointer into termcap's mutable buffers escapes this call.
    pub fn open(name: &str, kind: &str, interactive: bool) -> Result<Self, LispError> {
        use std::{io::Write, os::fd::AsRawFd, os::unix::fs::OpenOptionsExt};
        let mut file = crate::file_system::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOCTTY | libc::O_NONBLOCK)
            .open(name)
            .map_err(|_| LispError::Signal(format!("Could not open file: {name}")))?;
        let fd = file.as_raw_fd();
        // SAFETY: fd is owned and remains open throughout initialization.
        if unsafe { libc::isatty(fd) } == 0 {
            return Err(LispError::Signal(format!("Not a tty device: {name}")));
        }
        let Termcap {
            width,
            height,
            colors,
            appearance,
            enter,
            exit,
            keys,
            cursor_motion,
            ..
        } = read_termcap(kind, fd)?;
        // init_tty's tty_default_color_capabilities (tty, 1).
        colors.save_default();
        // sysdep.c:init_sys_modes: disable canonical input and echo; the
        // editor reads input bytes itself. Retain the original mode for close.
        let original = unsafe {
            let mut mode: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(fd, &mut mode) != 0 {
                return Err(LispError::Signal(format!(
                    "Could not read terminal modes: {name}"
                )));
            }
            mode
        };
        if width < 3 || height < 3 {
            return Err(LispError::Signal(format!(
                "Screen size {width}x{height} is too small"
            )));
        }
        if !cursor_motion {
            return Err(LispError::Signal(format!(
                "Terminal type \"{kind}\" is not powerful enough to run Emacs"
            )));
        }
        if interactive {
            let modes = emacs_tty_modes(&original, false, false, true, 7);
            if unsafe { libc::tcsetattr(fd, libc::TCSADRAIN, &modes) } != 0 {
                return Err(LispError::Signal(format!(
                    "Could not set terminal modes: {name}"
                )));
            }
        }
        let device = Self {
            file,
            width,
            height,
            colors,
            appearance,
            keys,
            original: interactive.then_some(original),
            exit: if interactive { exit } else { Vec::new() },
        };
        if interactive {
            file = device
                .file
                .try_clone()
                .map_err(|error| LispError::Signal(error.to_string()))?;
            file.write_all(&enter)
                .map_err(|error| LispError::Signal(error.to_string()))?;
        }
        Ok(device)
    }
}

impl Drop for TtyDevice {
    fn drop(&mut self) {
        use std::io::Write;
        let _ = (&self.file).write_all(&self.exit);
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            // SAFETY: this final owner still owns the open descriptor.
            if let Some(original) = &self.original {
                unsafe {
                    libc::tcsetattr(self.file.as_raw_fd(), libc::TCSANOW, original);
                }
            }
        }
    }
}

impl TerminalRef {
    pub(crate) fn parameter(&self, parameter: Value) -> Value {
        let mut rest = self.param_alist.get();
        while let Kind::Cons(list) = rest.kind() {
            let pair = list.car.get();
            if crate::lisp::types::eq(pair.car().expect("terminal parameter pair"), parameter) {
                return pair.cdr().expect("terminal parameter pair");
            }
            rest = list.cdr.get();
        }
        Value::Nil
    }

    pub(crate) fn set_parameter(&self, parameter: Value, value: Value) -> Value {
        let mut rest = self.param_alist.get();
        while let Kind::Cons(list) = rest.kind() {
            let pair = list.car.get();
            if crate::lisp::types::eq(pair.car().expect("terminal parameter pair"), parameter) {
                let previous = pair.cdr().expect("terminal parameter pair");
                pair.set_cdr(value).expect("terminal parameter pair");
                return previous;
            }
            rest = list.cdr.get();
        }
        self.param_alist.set(Value::cons(
            Value::cons(parameter, value),
            self.param_alist.get(),
        ));
        Value::Nil
    }

    pub(crate) fn parameters(&self) -> Value {
        let mut copied = Vec::new();
        let mut rest = self.param_alist.get();
        while let Kind::Cons(list) = rest.kind() {
            let pair = list.car.get();
            copied.push(Value::cons(
                pair.car().expect("terminal parameter pair"),
                pair.cdr().expect("terminal parameter pair"),
            ));
            rest = list.cdr.get();
        }
        Value::list(copied)
    }
}

impl Interpreter {
    pub(crate) fn alloc_terminal_id(&mut self) -> u64 {
        let id = self.next_terminal_id;
        self.next_terminal_id += 1;
        id
    }

    pub(crate) fn terminal_state(&self, id: u64) -> Option<std::cell::Ref<'_, TerminalState>> {
        self.terminals
            .iter()
            .find(|terminal| terminal.id == id)
            .map(TerminalRef::borrow)
    }

    pub(crate) fn terminal_state_mut(
        &self,
        id: u64,
    ) -> Option<std::cell::RefMut<'_, TerminalState>> {
        self.terminals
            .iter()
            .find(|terminal| terminal.id == id)
            .map(TerminalRef::borrow_mut)
    }

    pub(crate) fn selected_terminal_id(&self) -> u64 {
        self.selected_frame_id
            .borrow()
            .terminal
            .map_or(0, |terminal| terminal.id)
    }

    pub(crate) fn decode_terminal(&self, value: &Value) -> Option<TerminalRef> {
        let terminal = match value.kind() {
            Kind::Nil => self.selected_frame_id.borrow().terminal?,
            Kind::Terminal(terminal) => terminal,
            Kind::Frame(id) => id.borrow().terminal?,
            _ => return None,
        };
        terminal.borrow().live.then_some(terminal)
    }

    /// window.c stores the owning frame on every window, including internal
    /// and deleted windows. It must survive a frame switch and frame deletion.
    pub(crate) fn window_frame_id(&self, id: u64) -> Option<crate::lisp::types::FrameRef> {
        self.find_record(id)?
            .slots
            .get(crate::lisp::primitives::WINDOW_FRAME_SLOT)
            .and_then(|value| {
                if let Kind::Frame(id) = value.kind() {
                    Some(id)
                } else {
                    None
                }
            })
    }
}

impl Interpreter {
    pub(crate) fn open_tty_terminal(
        &mut self,
        name: &str,
        kind: &str,
    ) -> Result<TerminalRef, LispError> {
        if let Some(terminal) = self.terminals.iter().find(|terminal| {
            let state = terminal.borrow();
            state.live && state.kind.is_some() && state.name == name
        }) {
            return Ok(*terminal);
        }
        #[cfg(unix)]
        let device = Arc::new(TtyDevice::open(
            name,
            kind,
            !self
                .lookup_var_key(
                    cached_symbol!("noninteractive"),
                    &crate::lisp::types::Env::new(),
                )
                .is_some_and(|value| value.is_truthy()),
        )?);
        #[cfg(not(unix))]
        return Err(LispError::Signal(
            "Text terminals are not supported on this platform".into(),
        ));
        #[cfg(unix)]
        {
            let id = self.alloc_terminal_id();
            let mut keyboard = std::collections::HashMap::<
                crate::lisp::types::SymbolName,
                Value,
                crate::lisp::types::IdentityBuildHasher,
            >::default();
            // keyboard.c:init_kboard initializes these DEFVAR_KBOARD slots
            // (all but the two keymaps made below) to nil.
            for name in crate::lisp::types::symbols::KEYBOARD_VARIABLES {
                if !matches!(name, "input-decode-map" | "local-function-key-map") {
                    keyboard.insert(crate::lisp::types::SymbolName::intern_str(name), Value::Nil);
                }
            }
            use crate::lisp::primitives as p;
            let mut env = crate::lisp::types::Env::new();
            let decode = p::call(self, "make-sparse-keymap", &[], &mut env)?;
            for (bytes, key) in &device.keys {
                let sequence = Value::string(&String::from_utf8_lossy(bytes));
                let event = Value::vector([Value::symbol(key)]);
                p::call(self, "define-key", &[decode, sequence, event], &mut env)?;
            }
            let local = p::call(self, "make-sparse-keymap", &[], &mut env)?;
            let parent = self
                .lookup_var_key(cached_symbol!("function-key-map"), &env)
                .unwrap_or(Value::Nil);
            p::call(self, "set-keymap-parent", &[local, parent], &mut env)?;
            keyboard.insert(cached_symbol!("input-decode-map").symbol(), decode);
            keyboard.insert(cached_symbol!("local-function-key-map").symbol(), local);
            let terminal = TerminalRef::new(
                id,
                TerminalState {
                    live: true,
                    name: name.into(),
                    kind: Some(kind.into()),
                    colors: device.colors.clone(),
                    appearance: device.appearance.clone(),
                    previous_color_mode: 0,
                    previous_frame: None,
                    terminal_coding: None,
                    charset_list_coding: None,
                    keyboard_coding: Some("no-conversion".into()),
                    keyboard,
                    pending_input: Vec::new(),
                    top_frame: None,
                    device: Some(device),
                },
            );
            self.terminals.insert(0, terminal);
            Ok(terminal)
        }
    }

    pub(crate) fn new_terminal_frame(
        &mut self,
        object: TerminalRef,
    ) -> Result<crate::lisp::types::FrameRef, LispError> {
        use crate::lisp::primitives as p;
        let terminal = object.borrow();
        let (width, height) = terminal
            .device
            .as_ref()
            .map_or((self.frame_width(), self.frame_height()), |device| {
                (device.width, device.height)
            });
        drop(terminal);
        // frame.c:make_frame: the root window shows the current buffer, or
        // other_buffer_safely's when it is hidden (BUFFER_HIDDEN_P).
        let mut buffer_id = self.current_buffer_id;
        if self
            .get_buffer_by_id(buffer_id)
            .is_some_and(|buffer| p::string_starts_with_byte(&buffer.name(), b' '))
        {
            buffer_id = self.other_buffer_safely(buffer_id)?;
        }
        let id = self.next_frame_id;
        self.next_frame_id += 1;
        let point = self
            .get_buffer_by_id(buffer_id)
            .map_or(1, |buffer| buffer.point());
        let menu = i64::from(
            self.lookup_var_key(
                cached_symbol!("menu-bar-mode"),
                &crate::lisp::types::Env::new(),
            )
            .is_some_and(|value| value.is_truthy()),
        );
        let tab = i64::from(
            self.lookup_var_key(
                cached_symbol!("tab-bar-mode"),
                &crate::lisp::types::Env::new(),
            )
            .is_some_and(|value| value.is_truthy()),
        );
        let margin = menu + tab;
        // frame.c:make_frame: the first of `minibuffer-list', which
        // get_minibuffer makes only when there is none.
        let mini_buffer_id = match crate::lisp::primitives::nth_minibuffer(self, 0).kind() {
            Kind::Buffer(buffer) if !self.minibuffer_list.is_empty() => buffer.id,
            _ => crate::lisp::primitives::get_minibuffer(self, 0),
        };
        let mut make_window = |buffer, point, kind, geometry| {
            let Kind::Record(window_id) = self
                .create_pseudovector(
                    super::RecordKind::Window,
                    "window",
                    p::window_record_slots(self, Some(buffer), point, kind, geometry),
                )
                .kind()
            else {
                unreachable!()
            };
            window_id
        };
        let root = make_window(
            buffer_id,
            point,
            Value::Nil,
            (width, (height - margin - 1).max(1), 0, margin),
        );
        let mini = make_window(
            mini_buffer_id,
            1,
            p::WindowKind::Minibuffer.value(),
            (width, 1, 0, height - 1),
        );
        // make_terminal_frame's F<num>, from ++tty_frame_count.
        self.tty_frame_count += 1;
        let frame = crate::lisp::types::FrameRef::new(
            Value::string(&format!("F{}", self.tty_frame_count)),
            super::FrameState {
                id,
                terminal: Some(object),
                face_hash_table: None,
                root_window: Some(root),
                selected_window: Some(root),
                minibuffer_window: Some(mini),
                old_selected_window: None,
                tty_sized: true,
                width,
                height,
                text_height: height - margin,
                menu_bar_lines: menu,
                tab_bar_lines: tab,
                new_width: -1,
                new_height: -1,
                new_size_p: false,
                // make_frame's store_frame_param (f, Qminibuffer, Qt).
                param_alist: Value::list([Value::cons(Value::symbol("minibuffer"), Value::T)]),
                focus_frame_id: None,
                left: 0,
                top: 0,
                window_state_change: false,
                change_stamp: 0,
                number_of_windows: 0,
                after_make_frame: true,
                pointer_invisible: false,
                was_invisible: false,
                // frame.c:make_frame: `list1 (buf)', its root window's.
                buffer_list: vec![buffer_id],
                buried_buffer_list: Vec::new(),
                menu_bar_items: Value::Nil,
            },
        );
        self.frame_states.insert(0, frame);
        for window in [root, mini] {
            self.find_record_mut(window)
                .expect("allocated window has state")
                .slots[p::WINDOW_FRAME_SLOT] = Value::Frame(frame);
        }
        // make_frame's set_window_buffer (root_window, buf, 0, 0), then the
        // minibuffer window's: the buffer's last window start, its point,
        // its margins and its display stamp, no hooks run.
        let mut env = crate::lisp::types::Env::new();
        p::set_window_buffer(self, root.id, buffer_id, false, false, &mut env)?;
        p::set_window_buffer(self, mini.id, mini_buffer_id, false, false, &mut env)?;
        self.copy_frame_faces(self.selected_frame_id, frame);
        object.borrow_mut().top_frame = Some(frame);
        if object.id == 0 && TTY_ROUTE_ACTIVE.get() {
            PRIMARY_TOP_FRAME.set(Some(frame));
        }
        Ok(frame)
    }

    pub(crate) fn note_selected_frame(&mut self, id: crate::lisp::types::FrameRef) {
        // old_selected_frame is window_change_record's alone.
        if id != self.selected_frame_id {
            self.selected_frame_id = id;
        }
        let terminal = id.borrow().terminal.expect("live frame terminal");
        let mut state = terminal.borrow_mut();
        state.top_frame = Some(id);
        if terminal.id == 0 && TTY_ROUTE_ACTIVE.get() {
            PRIMARY_TOP_FRAME.set(Some(id));
        }
        let device = state
            .device
            .as_ref()
            .map(Arc::downgrade)
            .unwrap_or_default();
        if TTY_ROUTE_ACTIVE.get() {
            OUTPUT_DEVICE.with_borrow_mut(|output| *output = device);
        }
    }

    pub(crate) fn retire_frame(&mut self, id: crate::lisp::types::FrameRef) {
        use crate::lisp::primitives as p;
        let windows = self.records_of_kind(super::RecordKind::Window, |record| {
            record
                .slots
                .get(p::WINDOW_FRAME_SLOT)
                .is_some_and(|frame| frame.word() == Value::Frame(id).word())
        });
        for window in windows {
            let Some(record) = self.find_record_mut(window) else {
                continue;
            };
            record.slots[p::WINDOW_BUFFER_SLOT] = Value::Nil;
            record.slots[p::WINDOW_KIND_SLOT] = p::WindowKind::Deleted.value();
            for slot in [
                p::WINDOW_PARENT_SLOT,
                p::WINDOW_PREV_SIBLING_SLOT,
                p::WINDOW_NEXT_SIBLING_SLOT,
                p::WINDOW_FIRST_CHILD_SLOT,
            ] {
                record.slots[slot] = Value::Nil;
            }
        }
        let mut frame = id.borrow_mut();
        frame.root_window = None;
        let terminal = frame.terminal.take().expect("live frame terminal");
        drop(frame);
        self.frame_states.retain(|object| *object != id);
        let replacement = self
            .frame_states
            .iter()
            .find(|frame| {
                frame.is_live()
                    && frame
                        .borrow()
                        .terminal
                        .is_some_and(|object| object.ptr_eq(&terminal))
            })
            .copied();
        let mut state = terminal.borrow_mut();
        if state.top_frame == Some(id) {
            state.top_frame = replacement;
            if terminal.id == 0 && TTY_ROUTE_ACTIVE.get() {
                PRIMARY_TOP_FRAME.set(replacement);
            }
        }
    }

    pub(crate) fn retire_terminal(&mut self, terminal: TerminalRef) {
        let mut state = terminal.borrow_mut();
        state.live = false;
        state.top_frame = None;
        state.name.clear();
        state.keyboard.clear();
        state.pending_input = Vec::new();
        state.device = None;
    }
}

impl Interpreter {
    // eval.c:specbind remembers the KBOARD on which the let was made.
    // Unwinding after a frame switch must restore that same keyboard.
    /// Whether SYMBOL is one of this editor's keyboard variable objects.
    pub(crate) fn is_keyboard_variable(symbol: &crate::lisp::types::SymbolName) -> bool {
        crate::lisp::types::symbols::is_keyboard_variable(symbol)
    }

    pub(crate) fn keyboard_binding_value_on(
        &self,
        id: u64,
        symbol: &crate::lisp::types::SymbolName,
    ) -> Option<Value> {
        if id == 0 {
            self.globals.value(symbol)
        } else {
            self.terminal_state(id)?.keyboard.get(symbol).cloned()
        }
    }
    pub(crate) fn set_keyboard_binding_on(
        &mut self,
        id: u64,
        symbol: &crate::lisp::types::SymbolName,
        value: Value,
    ) {
        if id == 0 {
            self.globals.insert(symbol, value);
        } else if let Some(mut terminal) = self.terminal_state_mut(id).filter(|state| state.live) {
            terminal.keyboard.insert(*symbol, value);
        }
    }

    pub(crate) fn terminal_keyboard_value(
        &self,
        symbol: &crate::lisp::types::SymbolName,
    ) -> Option<Value> {
        if self.selected_terminal_id() == 0 {
            return None;
        }
        self.terminal_state(self.selected_terminal_id())?
            .keyboard
            .get(symbol)
            .cloned()
    }
    pub(crate) fn set_terminal_keyboard_value(
        &mut self,
        symbol: &crate::lisp::types::SymbolName,
        value: &Value,
    ) -> bool {
        let id = self.selected_terminal_id();
        let Some(mut terminal) = self.terminal_state_mut(id) else {
            return false;
        };
        if let Some(slot) = terminal.keyboard.get_mut(symbol) {
            *slot = *value;
            true
        } else {
            false
        }
    }

    /// The input descriptors of the live secondary terminals, each one
    /// init_tty gave add_keyboard_wait_descriptor.
    #[cfg(unix)]
    pub(crate) fn terminal_input_fds(&self) -> Vec<std::os::fd::RawFd> {
        use std::os::fd::AsRawFd;
        self.terminals
            .iter()
            .filter_map(|object| {
                let terminal = object.borrow();
                if !terminal.live {
                    return None;
                }
                terminal
                    .device
                    .as_ref()
                    .map(|device| device.file.as_raw_fd())
            })
            .collect()
    }

    /// The secondary terminals' tty_read_avail_input: process.c's keyboard
    /// descriptors feed the ordinary keyboard queue; terminal bytes still
    /// pass through Lisp's input-decode-map.
    pub(crate) fn service_terminal_input(
        &mut self,
        env: &mut super::Env,
    ) -> Result<bool, LispError> {
        use std::io::Read;
        if self
            .lookup_var_key(cached_symbol!("noninteractive"), env)
            .is_some_and(|value| value.is_truthy())
        {
            return Ok(false);
        }
        let mut events = Vec::new();
        let mut input = Vec::new();
        let mut disconnected = Vec::new();
        for object in &self.terminals {
            let mut terminal = object.borrow_mut();
            if !terminal.live {
                continue;
            }
            let Some(device) = &terminal.device else {
                continue;
            };
            let mut bytes = [0u8; 4096];
            match (&device.file).read(&mut bytes) {
                Ok(n) if n > 0 => terminal.pending_input.extend_from_slice(&bytes[..n]),
                Ok(_) => {
                    disconnected.push(*object);
                    continue;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    disconnected.push(*object);
                    continue;
                }
            }
            // coding.c retains an incomplete UTF-8 tail across reads. An
            // invalid byte is decoded by the ordinary coding-system backend,
            // never dropped or allowed to block the remaining input forever.
            let coding = terminal
                .keyboard_coding
                .clone()
                .unwrap_or_else(|| "no-conversion".into());
            let mut consumed = terminal.pending_input.len();
            if coding.starts_with("utf-8") {
                let mut offset = 0;
                while offset < terminal.pending_input.len() {
                    match std::str::from_utf8(&terminal.pending_input[offset..]) {
                        Ok(_) => break,
                        Err(error) => {
                            offset += error.valid_up_to();
                            if let Some(length) = error.error_len() {
                                offset += length;
                            } else {
                                consumed = offset;
                                break;
                            }
                        }
                    }
                }
            }
            if consumed > 0 {
                input.push((
                    terminal.top_frame,
                    coding,
                    terminal.pending_input.drain(..consumed).collect::<Vec<_>>(),
                ));
            }
        }
        for (frame, coding, bytes) in input {
            let decoded = crate::lisp::primitives::decode_text_bytes(self, &bytes, &coding)?;
            events.push(Value::list([
                Value::symbol("switch-frame"),
                frame.map(Value::Frame).unwrap_or(Value::Nil),
            ]));
            let string = crate::lisp::primitives::string_like(&Value::string(&decoded))
                .expect("decoded text is a Lisp string");
            events.extend(string.character_codes().into_iter().map(Value::Integer));
        }
        for id in &disconnected {
            crate::lisp::primitives::call(
                self,
                "delete-terminal",
                &[Value::Terminal(*id), Value::symbol("noelisp")],
                env,
            )?;
        }
        if events.is_empty() {
            return Ok(!disconnected.is_empty());
        }
        crate::lisp::primitives::append_unread_command_events(self, env, events);
        Ok(true)
    }
}

thread_local! {
    // A non-owning route for the existing renderer and its nested echo/menu
    // callbacks. The terminal record remains the sole descriptor owner.
    static OUTPUT_DEVICE: std::cell::RefCell<std::sync::Weak<TtyDevice>> = const { std::cell::RefCell::new(std::sync::Weak::new()) };
    static TTY_ROUTE_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static PRIMARY_TOP_FRAME: std::cell::Cell<Option<crate::lisp::types::FrameRef>> = const { std::cell::Cell::new(None) };
    static CONTROLLING_OUTPUT: std::cell::RefCell<Option<ControllingOutput>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn primary_top_frame() -> Option<crate::lisp::types::FrameRef> {
    PRIMARY_TOP_FRAME.get()
}

pub(crate) fn mark_frame_route(mark: &mut dyn FnMut(&Value)) {
    if let Some(frame) = PRIMARY_TOP_FRAME.get() {
        mark(&Value::Frame(frame));
    }
    // The controlling terminal, whose reset at exit reads its parameters
    // after the terminal list is gone.
    let terminal = CONTROLLING_OUTPUT.with(|output| {
        output
            .try_borrow()
            .ok()
            .and_then(|output| output.as_ref().map(|output| output.terminal))
    });
    if let Some(terminal) = terminal {
        mark(&Value::Terminal(terminal));
    }
}

pub(crate) fn activate_frame_route() {
    TTY_ROUTE_ACTIVE.set(true);
}

pub(crate) fn clear_frame_route() {
    TTY_ROUTE_ACTIVE.set(false);
    PRIMARY_TOP_FRAME.set(None);
    OUTPUT_DEVICE.with_borrow_mut(|output| *output = std::sync::Weak::new());
    CONTROLLING_OUTPUT.with_borrow_mut(|output| *output = None);
}

pub(crate) fn output() -> std::io::Result<Box<dyn std::io::Write>> {
    OUTPUT_DEVICE.with_borrow(|device| match device.upgrade() {
        Some(device) => device
            .file
            .try_clone()
            .map(|file| Box::new(file) as Box<dyn std::io::Write>),
        None => controlling_output(),
    })
}

/// The controlling terminal's tty->output: the DEV_TTY descriptor
/// init_tty opened for it, else (before there is one) standard output.
fn controlling_output() -> std::io::Result<Box<dyn std::io::Write>> {
    CONTROLLING_OUTPUT.with_borrow(|output| match output {
        Some(output) => output
            .file
            .try_clone()
            .map(|file| Box::new(file) as Box<dyn std::io::Write>),
        None => Ok(Box::new(std::io::stdout()) as Box<dyn std::io::Write>),
    })
}

/// sysdep.c:get_tty_size of the controlling terminal's descriptor
/// (CURTTY ()->input): its columns and rows, 0x0 where the ioctl fails;
/// None without a terminal session.
#[cfg(unix)]
pub(crate) fn controlling_tty_size() -> Option<(i64, i64)> {
    CONTROLLING_OUTPUT.with_borrow(|output| {
        output.as_ref().map(|output| {
            use std::os::fd::AsRawFd;
            // SAFETY: SIZE is a local winsize the ioctl fills.
            let mut size: libc::winsize = unsafe { std::mem::zeroed() };
            if unsafe { libc::ioctl(output.file.as_raw_fd(), libc::TIOCGWINSZ, &mut size) } == -1 {
                (0, 0)
            } else {
                (i64::from(size.ws_col), i64::from(size.ws_row))
            }
        })
    })
}

/// The controlling terminal's output and the size init_tty found for it
/// (FrameCols and FrameRows: get_tty_size's, else the entry's co and li).
struct ControllingOutput {
    file: std::fs::File,
    width: i64,
    height: i64,
    /// Its terminal object, marked while it is set (mark_frame_route).
    terminal: TerminalRef,
}

/// term.c:init_tty's tty->output for the controlling terminal TERMINAL:
/// FD (a descriptor of dev_tty the caller keeps open while it is set) and
/// its FrameCols and FrameRows, or none.
#[cfg(unix)]
pub(crate) fn set_controlling_output(output: Option<(std::os::fd::RawFd, i64, i64, TerminalRef)>) {
    use std::os::fd::FromRawFd;
    CONTROLLING_OUTPUT.with_borrow_mut(|controlling| {
        *controlling = output.and_then(|(fd, width, height, terminal)| {
            // SAFETY: FD is open; the File made from it is never dropped,
            // so the caller's descriptor stays its own.
            let borrowed = std::mem::ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(fd) });
            let file = borrowed.try_clone().ok()?;
            Some(ControllingOutput {
                file,
                width,
                height,
                terminal,
            })
        });
    });
}

/// dispnew.c:Fsend_string_to_terminal's output of BYTES (SDATA) on
/// TERMINAL: the initial terminal's stdout, a tty's tty->output, each
/// flushed at once; write errors are ignored, as fwrite's are.
pub(crate) fn send_string_to_terminal(terminal: TerminalRef, bytes: &[u8]) {
    use std::io::Write;
    let state = terminal.borrow();
    if let Some(device) = &state.device {
        let mut file = &device.file;
        let _ = file.write_all(bytes).and_then(|()| file.flush());
    } else if state.kind.is_some() {
        drop(state);
        if let Ok(mut out) = controlling_output() {
            let _ = out.write_all(bytes).and_then(|()| out.flush());
        }
    } else {
        drop(state);
        let _ = crate::lisp::primitives::batch_stdout::write(bytes)
            .and_then(|()| crate::lisp::primitives::batch_stdout::flush());
    }
}

/// tty_send_additional_strings for the controlling terminal: its
/// SYMBOL parameter's strings, none without one (or while its record is
/// being changed).
pub(crate) fn controlling_additional_strings(symbol: Value) -> Vec<u8> {
    CONTROLLING_OUTPUT.with(|output| {
        output
            .try_borrow()
            .ok()
            .and_then(|output| output.as_ref().map(|output| output.terminal))
            .map(|terminal| tty_additional_strings(terminal, symbol))
            .unwrap_or_default()
    })
}

/// The controlling terminal's TS_orig_pair as its color mode has it now
/// (empty without one), for tty_reset_terminal_modes; None without the
/// terminal's record (or while it is being changed).
pub(crate) fn controlling_orig_pair() -> Option<Vec<u8>> {
    CONTROLLING_OUTPUT.with(|output| {
        let output = output.try_borrow().ok()?;
        let terminal = output.as_ref()?.terminal;
        let state = terminal.try_borrow().ok()?;
        Some(state.colors.orig_pair.clone().unwrap_or_default())
    })
}

/// term.c:tty_send_additional_strings: the strings of TERMINAL's
/// parameter SYMBOL (`tty-mode-set-strings' or `tty-mode-reset-strings'),
/// in list order, read without quitting (CDR_SAFE of assq_no_quit); none
/// for a deleted terminal.
pub(crate) fn tty_additional_strings(terminal: TerminalRef, symbol: Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    if !terminal.borrow().live {
        return bytes;
    }
    let mut extra = Value::Nil;
    let mut rest = terminal.param_alist.get();
    while let Kind::Cons(cell) = rest.kind() {
        if let Kind::Cons(pair) = cell.car.get().kind()
            && crate::lisp::types::eq(pair.car.get(), symbol)
        {
            extra = pair.cdr.get();
            break;
        }
        rest = cell.cdr.get();
    }
    while let Kind::Cons(cell) = extra.kind() {
        if let Kind::StringObject(string) = cell.car.get().kind() {
            bytes.extend_from_slice(string.borrow().bytes());
        }
        extra = cell.cdr.get();
    }
    bytes
}

pub(crate) fn output_size() -> std::io::Result<(u16, u16)> {
    OUTPUT_DEVICE.with_borrow(|device| {
        #[cfg(unix)]
        if let Some(device) = device.upgrade() {
            use std::os::fd::AsRawFd;
            // SAFETY: the upgraded owner keeps the descriptor alive.
            let mut size: libc::winsize = unsafe { std::mem::zeroed() };
            if unsafe { libc::ioctl(device.file.as_raw_fd(), libc::TIOCGWINSZ, &mut size) } == 0
                && size.ws_col > 0
                && size.ws_row > 0
            {
                return Ok((size.ws_col, size.ws_row));
            }
            return Ok((device.width as u16, device.height as u16));
        }
        // The controlling terminal's: get_tty_size of its descriptor where
        // that answers, else the size init_tty gave its frames.
        #[cfg(unix)]
        if let Some(size) = CONTROLLING_OUTPUT.with_borrow(|output| {
            output.as_ref().map(|output| {
                use std::os::fd::AsRawFd;
                // SAFETY: SIZE is a local winsize the ioctl fills.
                let mut size: libc::winsize = unsafe { std::mem::zeroed() };
                if unsafe { libc::ioctl(output.file.as_raw_fd(), libc::TIOCGWINSZ, &mut size) } == 0
                    && size.ws_col > 0
                    && size.ws_row > 0
                {
                    (size.ws_col, size.ws_row)
                } else {
                    (output.width as u16, output.height as u16)
                }
            })
        }) {
            return Ok(size);
        }
        crossterm::terminal::size()
    })
}

impl super::Interpreter {
    /// tty_list->next: a live tty terminal besides the session's own.
    pub(crate) fn secondary_tty_terminals_live(&self) -> bool {
        self.terminals.iter().any(|terminal| {
            let state = terminal.borrow();
            state.live && state.device.is_some()
        })
    }
}

pub(crate) fn secondary_output_active() -> bool {
    OUTPUT_DEVICE.with_borrow(|device| device.strong_count() > 0)
}
