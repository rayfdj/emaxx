//! frame.c/terminal.c ownership and term.c's external TTY device boundary.
use super::{Interpreter, Value};
use crate::lisp::types::LispError;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct TerminalState {
    pub id: u64,
    pub live: bool,
    pub name: String,
    pub kind: Option<String>,
    pub colors: i64,
    pub terminal_coding: Option<String>,
    pub keyboard_coding: Option<String>,
    pub keyboard: std::collections::HashMap<String, Value>,
    pub pending_input: Vec<u8>,
    pub parameters: Vec<(Value, Value)>,
    pub top_frame: u64,
    pub device: Option<Arc<TtyDevice>>,
}

impl TerminalState {
    pub fn initial() -> Self {
        Self {
            id: 0,
            live: true,
            name: "initial_terminal".into(),
            kind: None,
            colors: 0,
            terminal_coding: None,
            keyboard_coding: Some("no-conversion".into()),
            keyboard: Default::default(),
            pending_input: Vec::new(),
            parameters: Vec::new(),
            top_frame: 1,
            device: None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct TtyDevice {
    pub file: std::fs::File,
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
        use std::{
            ffi::{CStr, CString},
            io::Write,
            os::fd::AsRawFd,
            os::unix::fs::OpenOptionsExt,
        };
        let mut file = std::fs::OpenOptions::new()
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
        static TERMCAP_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = TERMCAP_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: these are the system terminal capability library and its
        // documented C functions; all input strings and output buffers live
        // through the calls. We retain copied bytes, never library pointers.
        let (width, height, colors, enter, exit, keys, cursor_motion) = unsafe {
            let library = [
                "libtinfo.so.6",
                "libtinfo.so.5",
                "libncursesw.so.6",
                "/usr/lib/libncurses.dylib",
            ]
            .iter()
            .find_map(|path| libloading::Library::new(path).ok())
            .ok_or_else(|| LispError::Signal("Cannot open terminfo database file".into()))?;
            let getent = library
                .get::<unsafe extern "C" fn(*mut libc::c_char, *const libc::c_char) -> libc::c_int>(
                    b"tgetent\0",
                )
                .map_err(|_| LispError::Signal("Cannot open terminfo database file".into()))?;
            let getnum = library
                .get::<unsafe extern "C" fn(*const libc::c_char) -> libc::c_int>(b"tgetnum\0")
                .map_err(|_| LispError::Signal("Cannot open terminfo database file".into()))?;
            let getstr = library
                .get::<unsafe extern "C" fn(
                    *const libc::c_char,
                    *mut *mut libc::c_char,
                ) -> *mut libc::c_char>(b"tgetstr\0")
                .map_err(|_| LispError::Signal("Cannot open terminfo database file".into()))?;
            let kind_c = CString::new(kind)
                .map_err(|_| LispError::Signal("Invalid terminal type".into()))?;
            let mut buffer = [0i8; 4096];
            match getent(buffer.as_mut_ptr(), kind_c.as_ptr()) {
                n if n < 0 => {
                    return Err(LispError::Signal(
                        "Cannot open terminfo database file".into(),
                    ));
                }
                0 => {
                    return Err(LispError::Signal(format!(
                        "Terminal type {kind} is not defined"
                    )));
                }
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
                .map_err(|_| LispError::Signal("Cannot open terminfo database file".into()))?;
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
            libc::ioctl(fd, libc::TIOCGWINSZ, &mut size);
            let width = if size.ws_col > 0 {
                i64::from(size.ws_col)
            } else {
                i64::from(getnum(c"co".as_ptr())).max(1)
            };
            let height = if size.ws_row > 0 {
                i64::from(size.ws_row)
            } else {
                i64::from(getnum(c"li".as_ptr())).max(1)
            };
            (
                width,
                height,
                i64::from(getnum(c"Co".as_ptr())).max(0),
                [cap(c"ti"), cap(c"ks"), cap(c"cl")].concat(),
                [cap(c"ve"), cap(c"ke"), cap(c"te")].concat(),
                keys,
                cursor_motion,
            )
        };
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
            let mut raw = original;
            raw.c_iflag |= libc::IGNBRK;
            raw.c_iflag &= !(libc::ICRNL | libc::INLCR | libc::ISTRIP | libc::IXON);
            raw.c_lflag &= !(libc::ECHO | libc::ICANON | libc::IEXTEN);
            raw.c_lflag |= libc::ISIG;
            raw.c_oflag &= !libc::ONLCR;
            #[cfg(target_os = "linux")]
            {
                raw.c_oflag &= !libc::TAB3;
            }
            #[cfg(target_os = "macos")]
            {
                raw.c_oflag &= !libc::OXTABS;
            }
            raw.c_cflag |= libc::CS8;
            raw.c_cflag &= !libc::PARENB;
            for index in [
                libc::VINTR,
                libc::VQUIT,
                libc::VSUSP,
                libc::VLNEXT,
                libc::VREPRINT,
                libc::VWERASE,
                libc::VSTART,
                libc::VSTOP,
            ] {
                raw.c_cc[index] = libc::_POSIX_VDISABLE;
            }
            #[cfg(target_os = "macos")]
            {
                raw.c_cc[libc::VDSUSP] = libc::_POSIX_VDISABLE;
            }
            raw.c_cc[libc::VMIN] = 1;
            raw.c_cc[libc::VTIME] = 0;
            if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw) } != 0 {
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

impl Interpreter {
    pub(crate) fn terminal_state(&self, id: u64) -> Option<&TerminalState> {
        self.terminals.iter().find(|terminal| terminal.id == id)
    }
    pub(crate) fn selected_terminal_id(&self) -> u64 {
        self.selected_frame_state()
            .map_or(0, |frame| frame.terminal_id)
    }
    pub(crate) fn decode_terminal_id(&self, value: &Value) -> Option<u64> {
        let id = match value {
            Value::Nil => self.selected_terminal_id(),
            Value::Terminal(id) => *id,
            Value::Frame(id) => {
                self.frame_state(*id)
                    .filter(|frame| frame.live)?
                    .terminal_id
            }
            _ => return None,
        };
        self.terminal_state(id)
            .filter(|terminal| terminal.live)
            .map(|terminal| terminal.id)
    }
    pub fn terminal_live(&self) -> bool {
        self.decode_terminal_id(&Value::Nil).is_some()
    }
    pub fn terminal_parameter(&self, parameter: &Value) -> Option<Value> {
        self.terminal_state(self.selected_terminal_id())?
            .parameters
            .iter()
            .rfind(|(key, _)| key == parameter)
            .map(|(_, value)| value.clone())
    }
    pub fn set_terminal_parameter(&mut self, parameter: Value, value: Value) -> Value {
        self.set_terminal_parameter_on(self.selected_terminal_id(), parameter, value)
    }
    pub(crate) fn set_terminal_parameter_on(
        &mut self,
        id: u64,
        parameter: Value,
        value: Value,
    ) -> Value {
        let parameter = Self::stored_value(parameter);
        let value = Self::stored_value(value);
        let terminal = self
            .terminals
            .iter_mut()
            .find(|terminal| terminal.id == id)
            .expect("decoded terminal has state");
        if let Some((_, previous)) = terminal
            .parameters
            .iter_mut()
            .rfind(|(key, _)| key == &parameter)
        {
            std::mem::replace(previous, value)
        } else {
            terminal.parameters.push((parameter, value));
            Value::Nil
        }
    }
    pub fn terminal_parameters(&self) -> Value {
        self.terminal_parameters_on(self.selected_terminal_id())
    }
    pub(crate) fn terminal_parameters_on(&self, id: u64) -> Value {
        Value::list(
            self.terminal_state(id)
                .expect("decoded terminal has state")
                .parameters
                .iter()
                .rev()
                .map(|(key, value)| Value::cons(key.clone(), value.clone())),
        )
    }
    /// window.c stores the owning frame on every window, including internal
    /// and deleted windows. It must survive a frame switch and frame deletion.
    pub(crate) fn window_frame_id(&self, id: u64) -> Option<u64> {
        self.find_record(id)?
            .slots
            .get(crate::lisp::primitives::WINDOW_FRAME_SLOT)
            .and_then(|value| {
                if let Value::Frame(id) = value {
                    Some(*id)
                } else {
                    None
                }
            })
    }
}

impl Interpreter {
    pub(crate) fn open_tty_terminal(&mut self, name: &str, kind: &str) -> Result<u64, LispError> {
        if let Some(terminal) = self
            .terminals
            .iter()
            .find(|terminal| terminal.live && terminal.kind.is_some() && terminal.name == name)
        {
            return Ok(terminal.id);
        }
        #[cfg(unix)]
        let device = Arc::new(TtyDevice::open(
            name,
            kind,
            !self
                .lookup_var("noninteractive", &Vec::new())
                .is_some_and(|value| value.is_truthy()),
        )?);
        #[cfg(not(unix))]
        return Err(LispError::Signal(
            "Text terminals are not supported on this platform".into(),
        ));
        #[cfg(unix)]
        {
            let id = self
                .terminals
                .iter()
                .map(|terminal| terminal.id)
                .max()
                .unwrap_or(0)
                + 1;
            let mut keyboard = std::collections::HashMap::new();
            // keyboard.c:init_kboard initializes these DEFVAR_KBOARD slots.
            for name in [
                "overriding-terminal-local-map",
                "last-command",
                "real-last-command",
                "keyboard-translate-table",
                "last-repeatable-command",
                "prefix-arg",
                "last-prefix-arg",
                "defining-kbd-macro",
                "last-kbd-macro",
                "system-key-alist",
                "window-system",
                "default-minibuffer-frame",
            ] {
                keyboard.insert(name.into(), Value::Nil);
            }
            use crate::lisp::primitives as p;
            let mut env = Vec::new();
            let decode = p::call(self, "make-sparse-keymap", &[], &mut env)?;
            for (bytes, key) in &device.keys {
                let sequence = Value::string(&String::from_utf8_lossy(bytes));
                let event = Value::vector([Value::symbol(key)]);
                p::call(
                    self,
                    "define-key",
                    &[decode.clone(), sequence, event],
                    &mut env,
                )?;
            }
            let local = p::call(self, "make-sparse-keymap", &[], &mut env)?;
            let parent = self
                .lookup_var("function-key-map", &env)
                .unwrap_or(Value::Nil);
            p::call(
                self,
                "set-keymap-parent",
                &[local.clone(), parent],
                &mut env,
            )?;
            keyboard.insert("input-decode-map".into(), decode);
            keyboard.insert("local-function-key-map".into(), local);
            self.terminals.insert(
                0,
                TerminalState {
                    id,
                    live: true,
                    name: name.into(),
                    kind: Some(kind.into()),
                    colors: device.colors,
                    parameters: Vec::new(),
                    terminal_coding: None,
                    keyboard_coding: Some("no-conversion".into()),
                    keyboard,
                    pending_input: Vec::new(),
                    top_frame: 0,
                    device: Some(device),
                },
            );
            Ok(id)
        }
    }

    pub(crate) fn new_terminal_frame(&mut self, terminal_id: u64) -> u64 {
        use crate::lisp::primitives as p;
        let terminal = self
            .terminal_state(terminal_id)
            .expect("decoded terminal has state");
        let (width, height) = terminal
            .device
            .as_ref()
            .map_or((self.frame_width(), self.frame_height()), |device| {
                (device.width, device.height)
            });
        let id = self
            .frame_states
            .iter()
            .map(|frame| frame.id)
            .max()
            .unwrap_or(0)
            + 1;
        let buffer_id = self.selected_window_buffer_id();
        let point = self
            .get_buffer_by_id(buffer_id)
            .map_or(1, |buffer| buffer.point());
        let menu = i64::from(
            self.lookup_var("menu-bar-mode", &Vec::new())
                .is_some_and(|value| value.is_truthy()),
        );
        let tab = i64::from(
            self.lookup_var("tab-bar-mode", &Vec::new())
                .is_some_and(|value| value.is_truthy()),
        );
        let margin = menu + tab;
        let mini_buffer_id = self
            .find_buffer(" *Minibuf-0*")
            .unwrap_or_else(|| self.create_buffer(" *Minibuf-0*"))
            .0;
        let mut make_window = |buffer, point, kind, geometry| {
            let Value::Record(window_id) = self.create_pseudovector(
                super::RecordKind::Window,
                "window",
                p::window_record_slots(Some(buffer), point, kind, geometry),
            ) else {
                unreachable!()
            };
            self.find_record_mut(window_id)
                .expect("allocated window has a record")
                .slots[p::WINDOW_FRAME_SLOT] = Value::Frame(id);
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
            Value::symbol(p::MINIBUFFER_WINDOW_KIND),
            (width, 1, 0, height - 1),
        );
        self.frame_states.insert(
            0,
            super::FrameState {
                id,
                terminal_id,
                face_hash_table: None,
                root_window_id: root,
                selected_window_id: root,
                minibuffer_window_id: mini,
                old_selected_window_id: None,
                tty_sized: true,
                name: Value::string(&format!("F{id}")),
                live: true,
                width,
                height,
                text_height: height - margin,
                parameter_width: width,
                parameter_height: height,
                parameter_overrides: vec![
                    ("menu-bar-lines".into(), Value::Integer(menu)),
                    ("tab-bar-lines".into(), Value::Integer(tab)),
                ],
                focus_frame_id: None,
                left: 0,
                top: 0,
                window_state_change: false,
                after_make_frame: true,
                pointer_invisible: false,
                was_invisible: false,
            },
        );
        self.copy_frame_faces(self.selected_frame_id, id);
        self.terminals
            .iter_mut()
            .find(|terminal| terminal.id == terminal_id)
            .expect("decoded terminal has state")
            .top_frame = id;
        if terminal_id == 0 {
            PRIMARY_TOP_FRAME.set(id);
        }
        id
    }

    pub(crate) fn note_selected_frame(&mut self, id: u64) {
        if id != self.selected_frame_id {
            self.old_selected_frame_id = self.selected_frame_id;
            self.selected_frame_id = id;
        }
        let terminal_id = self
            .frame_state(id)
            .expect("decoded frame has state")
            .terminal_id;
        self.terminals
            .iter_mut()
            .find(|terminal| terminal.id == terminal_id)
            .expect("decoded terminal has state")
            .top_frame = id;
        if terminal_id == 0 {
            PRIMARY_TOP_FRAME.set(id);
        }
        let device = self
            .terminal_state(terminal_id)
            .expect("decoded terminal has state")
            .device
            .as_ref()
            .map(Arc::downgrade)
            .unwrap_or_default();
        OUTPUT_DEVICE.with_borrow_mut(|output| *output = device);
    }

    pub(crate) fn retire_frame(&mut self, id: u64) {
        use crate::lisp::primitives as p;
        for record in self.records.iter_mut().filter(|record| {
            record.kind == super::RecordKind::Window
                && record.slots.get(p::WINDOW_FRAME_SLOT) == Some(&Value::Frame(id))
        }) {
            record.slots[p::WINDOW_BUFFER_SLOT] = Value::Nil;
            record.slots[p::WINDOW_KIND_SLOT] = Value::symbol(p::DELETED_WINDOW_KIND);
            for slot in [
                p::WINDOW_PARENT_SLOT,
                p::WINDOW_PREV_SIBLING_SLOT,
                p::WINDOW_NEXT_SIBLING_SLOT,
                p::WINDOW_FIRST_CHILD_SLOT,
            ] {
                record.slots[slot] = Value::Nil;
            }
        }
        let frame = self.frame_state_mut(id).expect("decoded frame has state");
        frame.live = false;
        frame.root_window_id = 0;
        let terminal_id = frame.terminal_id;
        if let Some(replacement) = self
            .frame_states
            .iter()
            .find(|frame| frame.live && frame.terminal_id == terminal_id)
            .map(|frame| frame.id)
        {
            let terminal = self
                .terminals
                .iter_mut()
                .find(|terminal| terminal.id == terminal_id)
                .expect("decoded terminal has state");
            if terminal.top_frame == id {
                terminal.top_frame = replacement;
                if terminal_id == 0 {
                    PRIMARY_TOP_FRAME.set(replacement);
                }
            }
        }
    }

    pub(crate) fn retire_terminal(&mut self, id: u64) {
        let terminal = self
            .terminals
            .iter_mut()
            .find(|terminal| terminal.id == id)
            .expect("decoded terminal has state");
        terminal.live = false;
        terminal.parameters.clear();
        terminal.keyboard.clear();
        terminal.device = None;
    }
}

impl Interpreter {
    // eval.c:specbind remembers the KBOARD on which the let was made.
    // Unwinding after a frame switch must restore that same keyboard.
    pub(crate) fn keyboard_binding_terminal(&self, name: &str) -> Option<u64> {
        matches!(
            name,
            "overriding-terminal-local-map"
                | "last-command"
                | "real-last-command"
                | "keyboard-translate-table"
                | "last-repeatable-command"
                | "prefix-arg"
                | "last-prefix-arg"
                | "defining-kbd-macro"
                | "last-kbd-macro"
                | "system-key-alist"
                | "window-system"
                | "default-minibuffer-frame"
                | "input-decode-map"
                | "local-function-key-map"
        )
        .then(|| self.selected_terminal_id())
    }
    pub(crate) fn keyboard_binding_value_on(&self, id: u64, name: &str) -> Option<Value> {
        if id == 0 {
            self.globals.value_by_name(name).cloned()
        } else {
            self.terminal_state(id)?.keyboard.get(name).cloned()
        }
    }
    pub(crate) fn set_keyboard_binding_on(&mut self, id: u64, name: &str, value: Value) {
        if id == 0 {
            self.globals.insert_by_name(name, Self::stored_value(value));
        } else if let Some(terminal) = self
            .terminals
            .iter_mut()
            .find(|terminal| terminal.id == id && terminal.live)
        {
            terminal
                .keyboard
                .insert(name.into(), Self::stored_value(value));
        }
    }
    pub(crate) fn terminal_keyboard_value(&self, name: &str) -> Option<Value> {
        if self.selected_terminal_id() == 0 {
            return None;
        }
        self.terminal_state(self.selected_terminal_id())?
            .keyboard
            .get(name)
            .cloned()
    }
    pub(crate) fn set_terminal_keyboard_value(&mut self, name: &str, value: &Value) -> bool {
        let id = self.selected_terminal_id();
        if let Some(slot) = self
            .terminals
            .iter_mut()
            .find(|terminal| terminal.id == id)
            .and_then(|terminal| terminal.keyboard.get_mut(name))
        {
            *slot = value.clone();
            true
        } else {
            false
        }
    }

    /// process.c's keyboard descriptors feed the ordinary keyboard queue;
    /// terminal bytes still pass through Lisp's input-decode-map.
    pub(crate) fn service_terminal_input(
        &mut self,
        env: &mut super::Env,
    ) -> Result<bool, LispError> {
        use std::io::Read;
        if self
            .lookup_var("noninteractive", env)
            .is_some_and(|value| value.is_truthy())
        {
            return Ok(false);
        }
        let mut events = Vec::new();
        let mut input = Vec::new();
        let mut disconnected = Vec::new();
        for terminal in self.terminals.iter_mut().filter(|terminal| terminal.live) {
            let Some(device) = &terminal.device else {
                continue;
            };
            let mut bytes = [0u8; 4096];
            match (&device.file).read(&mut bytes) {
                Ok(n) if n > 0 => terminal.pending_input.extend_from_slice(&bytes[..n]),
                Ok(_) => {
                    disconnected.push(terminal.id);
                    continue;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    disconnected.push(terminal.id);
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
                Value::Frame(frame),
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
        let mut pending = crate::lisp::primitives::unread_command_events(self, env)?;
        pending.extend(events);
        self.set_variable("unread-command-events", Value::list(pending), env);
        Ok(true)
    }
}

thread_local! {
    // A non-owning route for the existing renderer and its nested echo/menu
    // callbacks. The terminal record remains the sole descriptor owner.
    static OUTPUT_DEVICE: std::cell::RefCell<std::sync::Weak<TtyDevice>> = const { std::cell::RefCell::new(std::sync::Weak::new()) };
    static PRIMARY_TOP_FRAME: std::cell::Cell<u64> = const { std::cell::Cell::new(1) };
}

pub(crate) fn primary_top_frame() -> u64 {
    PRIMARY_TOP_FRAME.get()
}

pub(crate) fn output() -> std::io::Result<Box<dyn std::io::Write>> {
    OUTPUT_DEVICE.with_borrow(|device| match device.upgrade() {
        Some(device) => device
            .file
            .try_clone()
            .map(|file| Box::new(file) as Box<dyn std::io::Write>),
        None => Ok(Box::new(std::io::stdout()) as Box<dyn std::io::Write>),
    })
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
        crossterm::terminal::size()
    })
}

pub(crate) fn secondary_output_active() -> bool {
    OUTPUT_DEVICE.with_borrow(|device| device.strong_count() > 0)
}
