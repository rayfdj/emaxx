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
    pub colors: i64,
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
            colors: 0,
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

/// The parts of a terminal type's termcap entry term.c:init_tty takes.
#[cfg(unix)]
pub(crate) struct Termcap {
    pub width: i64,
    pub height: i64,
    pub colors: i64,
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
    static TERMCAP_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = TERMCAP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    // SAFETY: these are the system terminal capability library and its
    // documented C functions; all input strings and output buffers live
    // through the calls. We retain copied bytes, never library pointers.
    let termcap = unsafe {
        let library = [
            "libtinfo.so.6",
            "libtinfo.so.5",
            "libncursesw.so.6",
            "/usr/lib/libncurses.dylib",
        ]
        .iter()
        .find_map(|path| libloading::Library::new(path).ok())
        .ok_or(TermcapError::NoDatabase)?;
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
        Termcap {
            width,
            height,
            colors: i64::from(getnum(c"Co".as_ptr())).max(0),
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
    pub colors: i64,
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
            enter,
            exit,
            keys,
            cursor_motion,
            ..
        } = read_termcap(kind, fd)?;
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
                    colors: device.colors,
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
