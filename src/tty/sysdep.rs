//! sysdep.c's side of the controlling terminal: init_tty's dev_tty
//! descriptor, init_sys_modes and
//! reset_sys_modes over its termios and its SIGIO (init_sigio), with the
//! terminal's mode strings (term.c's set_terminal_modes_hook and
//! reset_terminal_modes_hook), its size (get_tty_size), and the reads of
//! keyboard.c's tty_read_avail_input.

use crate::lisp::primitives::TtyInputModes;
use std::os::fd::RawFd;

/// keyboard.c's KBD_BUFFER_SIZE - 1: the most tty_read_avail_input reads
/// at once.
const READ_MAX: usize = 4095;

/// The output tty_set_terminal_modes and tty_reset_terminal_modes write.
#[derive(Default)]
pub(super) struct TerminalModeStrings {
    /// TS_termcap_modes (ti).
    pub(super) termcap_modes: Vec<u8>,
    /// TS_end_termcap_modes (te).
    pub(super) end_termcap_modes: Vec<u8>,
    /// TS_keypad_mode (ks) and TS_end_keypad_mode (ke).
    pub(super) keypad_mode: Vec<u8>,
    pub(super) end_keypad_mode: Vec<u8>,
    /// TS_cursor_visible (vs) and TS_cursor_normal (ve).
    pub(super) cursor_visible: Vec<u8>,
    pub(super) cursor_normal: Vec<u8>,
    /// TS_orig_pair (op).
    pub(super) orig_pair: Vec<u8>,
}

/// The controlling terminal's descriptor and the modes Emacs found it in
/// (tty_display_info's input and output, old_tty and old_fcntl_owner).
pub(super) struct TtyModes {
    /// The dev_tty descriptor init_tty opened: input is read from it,
    /// selected on and set up, and the terminal's output written to it.
    pub(super) fd: RawFd,
    /// Fcontrolling_tty_p: the terminal is dev_tty, whose C-g is SIGINT.
    pub(super) controlling: bool,
    old_tty: Option<libc::termios>,
    old_flags: libc::c_int,
    /// The modes in force now.
    applied: bool,
    /// tty->term_initted: init_sys_modes has run once.
    term_initted: bool,
    strings: TerminalModeStrings,
    /// init_foreground_group's inherited_pgroup: the process group Emacs
    /// started in when it is not its own, else 0.
    inherited_pgroup: libc::pid_t,
}

/// term.c:init_tty's open of dev_tty, NAME, for the startup terminal
/// (O_RDWR, O_NOCTTY; emacs_open adds O_CLOEXEC), its maybe_fatal text
/// when the device cannot be opened or is no terminal.
pub(super) fn open_dev_tty(name: &str) -> Result<RawFd, String> {
    let fd = std::ffi::CString::new(name).map_or(-1, |path| {
        // SAFETY: PATH is a valid C string.
        unsafe {
            libc::open(
                path.as_ptr(),
                libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
            )
        }
    });
    if fd < 0 {
        return Err(format!("Could not open file: {name}"));
    }
    // SAFETY: FD is open.
    if unsafe { libc::isatty(fd) } != 1 {
        // SAFETY: FD is this function's own descriptor.
        unsafe {
            libc::close(fd);
        }
        return Err(format!("Not a tty device: {name}"));
    }
    Ok(fd)
}

impl TtyModes {
    /// The terminal FD (open_dev_tty's, owned from here on) with the mode
    /// strings of its termcap entry; the terminal object's
    /// `tty-mode-set-strings' and `tty-mode-reset-strings' parameters
    /// (tty_send_additional_strings) are the controlling output's.
    pub(super) fn new(fd: RawFd, strings: TerminalModeStrings) -> Self {
        // SAFETY: FD is open.
        let old_flags = unsafe { libc::fcntl(fd, libc::F_GETFL, 0) } & !libc::O_ASYNC;
        // init_display's init_foreground_group.
        // SAFETY: getpgrp and getpid have no preconditions.
        let inherited_pgroup = unsafe {
            let pgrp = libc::getpgrp();
            if libc::getpid() == pgrp { 0 } else { pgrp }
        };
        Self {
            fd,
            controlling: true,
            old_tty: None,
            old_flags,
            applied: false,
            term_initted: false,
            strings,
            inherited_pgroup,
        }
    }

    /// sysdep.c:init_sys_modes: the foreground group narrowed to Emacs,
    /// the modes Emacs finds saved (each time), the modes MODES and QUIT
    /// ask for set, SIGIO requested with INTERRUPT_INPUT, and the
    /// terminal's own modes entered (set_terminal_modes_hook).  The found
    /// modes' VERASE, for `tty-erase-char'.
    pub(super) fn init(
        &mut self,
        interrupt_input: bool,
        modes: TtyInputModes,
        quit: u8,
    ) -> Option<u8> {
        narrow_foreground_group(self.fd, self.inherited_pgroup);
        let mut erase = None;
        // SAFETY: FD is open; the termios structures are local.
        unsafe {
            let mut found: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(self.fd, &mut found) == 0 {
                self.old_tty = Some(found);
                erase = Some(found.c_cc[libc::VERASE]);
                let original = found;
                let tty = crate::lisp::eval::terminal::emacs_tty_modes(
                    &original,
                    self.controlling,
                    modes.flow_control,
                    modes.meta_key != 0,
                    quit,
                );
                emacs_set_tty(self.fd, &tty);
                // An unlocked terminal at the start when flow control is not
                // to be used (TCOON).
                if !modes.flow_control {
                    libc::tcflow(self.fd, libc::TCOON);
                }
            }
            if interrupt_input {
                set_sigio_owner(self.fd);
                libc::fcntl(self.fd, libc::F_SETFL, self.old_flags | libc::O_ASYNC);
            }
        }
        // tty_set_terminal_modes.
        let mut out = Vec::new();
        out.extend_from_slice(&self.strings.termcap_modes);
        out.extend_from_slice(&self.strings.cursor_visible);
        out.extend_from_slice(&self.strings.keypad_mode);
        out.extend_from_slice(
            &crate::lisp::eval::terminal::controlling_additional_strings(
                crate::lisp::types::Value::symbol("tty-mode-set-strings"),
            ),
        );
        write_terminal(self.fd, &out);
        self.applied = true;
        self.term_initted = true;
        self.arm();
        erase
    }

    /// reset_sys_modes's output for a terminal of ROWS lines: the cursor
    /// to the last line, cleared, then tty_reset_terminal_modes's strings.
    fn reset_output(&self, rows: u16) -> Vec<u8> {
        let mut out = format!("\x1b[{rows};1H\x1b[K").into_bytes();
        // tty_reset_terminal_modes.
        out.extend_from_slice(
            &crate::lisp::eval::terminal::controlling_additional_strings(
                crate::lisp::types::Value::symbol("tty-mode-reset-strings"),
            ),
        );
        out.extend_from_slice(&self.strings.end_keypad_mode);
        out.extend_from_slice(&self.strings.cursor_normal);
        out.extend_from_slice(&self.strings.end_termcap_modes);
        // TS_orig_pair as the terminal's color mode now has it.
        match crate::lisp::eval::terminal::controlling_orig_pair() {
            Some(orig_pair) => out.extend_from_slice(&orig_pair),
            None => out.extend_from_slice(&self.strings.orig_pair),
        }
        out.push(b'\r');
        out
    }

    /// While Emacs's modes are in force, publish this reset (at the
    /// terminal's current size) for a fatal signal on any thread, which
    /// cannot build it then.
    pub(super) fn arm(&self) {
        if !self.applied {
            return;
        }
        let rows = get_tty_size(self.fd).map_or(24, |(_, rows)| rows);
        let bytes = self.reset_output(rows);
        crate::lisp::primitives::arm_emergency_reset(&crate::lisp::primitives::ResetState {
            fd: self.fd,
            bytes: &bytes,
            old_tty: self.old_tty,
            old_flags: self.old_flags,
            inherited_pgroup: self.inherited_pgroup,
        });
    }

    /// tty->term_initted.
    pub(super) fn initted(&self) -> bool {
        self.term_initted
    }

    /// sysdep.c:reset_sys_modes: the cursor to the last line, cleared; the
    /// terminal's own modes left (reset_terminal_modes_hook); the output
    /// drained; SIGIO given up; and the modes Emacs found restored.
    pub(super) fn reset(&mut self, rows: u16) {
        if !self.applied {
            return;
        }
        self.applied = false;
        crate::lisp::primitives::disarm_emergency_reset();
        let out = self.reset_output(rows);
        write_terminal(self.fd, &out);
        // SAFETY: FD is open; OLD_TTY is the structure tcgetattr filled.
        unsafe {
            while libc::tcdrain(self.fd) != 0
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR)
            {}
            libc::fcntl(self.fd, libc::F_SETFL, self.old_flags);
            if let Some(old) = &self.old_tty {
                while libc::tcsetattr(self.fd, libc::TCSADRAIN, old) != 0
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR)
                {
                }
            }
        }
        widen_foreground_group(self.fd, self.inherited_pgroup);
    }

    /// sysdep.c:discard_tty_input for this terminal: its modes read and
    /// set again (emacs_set_tty without flushing, TCSADRAIN).
    pub(super) fn discard_input(&self) {
        // SAFETY: FD is open; BUF is a local termios tcgetattr fills.
        unsafe {
            let mut buf: libc::termios = std::mem::zeroed();
            libc::tcgetattr(self.fd, &mut buf);
            emacs_set_tty(self.fd, &buf);
        }
    }
}

impl Drop for TtyModes {
    fn drop(&mut self) {
        let rows = get_tty_size(self.fd).map_or(24, |(_, rows)| rows);
        self.reset(rows);
        // SAFETY: FD is this terminal's own descriptor, closed only here.
        unsafe {
            libc::close(self.fd);
        }
    }
}

/// sysdep.c:emacs_set_tty: the modes set (TCSADRAIN), retried until they
/// are what was asked, as some systems take only part of a change at a
/// time.
fn emacs_set_tty(fd: RawFd, tty: &libc::termios) {
    // SAFETY: FD is open; the structures are local.
    unsafe {
        for _ in 0..10 {
            if libc::tcsetattr(fd, libc::TCSADRAIN, tty) != 0 {
                if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return;
            }
            let mut new: libc::termios = std::mem::zeroed();
            libc::tcgetattr(fd, &mut new);
            if new.c_iflag == tty.c_iflag
                && new.c_oflag == tty.c_oflag
                && new.c_cflag == tty.c_cflag
                && new.c_lflag == tty.c_lflag
                && new.c_cc == tty.c_cc
            {
                return;
            }
        }
    }
}

/// sysdep.c:tcsetpgrp_without_stopping: the terminal FD's foreground
/// group made PGID with SIGTTOU blocked, which would stop a process not in
/// the foreground.
fn tcsetpgrp_without_stopping(fd: RawFd, pgid: libc::pid_t) {
    // SAFETY: the signal sets are local; FD is open.
    unsafe {
        let mut blocked: libc::sigset_t = std::mem::zeroed();
        let mut old: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut blocked);
        libc::sigaddset(&mut blocked, libc::SIGTTOU);
        libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, &mut old);
        libc::tcsetpgrp(fd, pgid);
        libc::pthread_sigmask(libc::SIG_SETMASK, &old, std::ptr::null_mut());
    }
}

/// sysdep.c:narrow_foreground_group: an Emacs started in another's
/// process group (INHERITED) moves to its own and makes that the
/// terminal's foreground group, so the terminal's SIGINT reaches Emacs
/// alone.
fn narrow_foreground_group(fd: RawFd, inherited: libc::pid_t) {
    // SAFETY: setpgid and getpid have no preconditions.
    if inherited != 0 && unsafe { libc::setpgid(0, 0) } == 0 {
        tcsetpgrp_without_stopping(fd, unsafe { libc::getpid() });
    }
}

/// sysdep.c:widen_foreground_group: back in the INHERITED group, the
/// terminal's foreground again.
fn widen_foreground_group(fd: RawFd, inherited: libc::pid_t) {
    // SAFETY: setpgid has no preconditions.
    if inherited != 0 && unsafe { libc::setpgid(0, inherited) } == 0 {
        tcsetpgrp_without_stopping(fd, inherited);
    }
}

/// BYTES written to the terminal's output FD and flushed (fflush of
/// tty->output); the display's own buffered output has been flushed by
/// each update before this runs.
fn write_terminal(fd: RawFd, bytes: &[u8]) {
    let mut rest = bytes;
    while !rest.is_empty() {
        // SAFETY: REST is a live byte slice; FD is open.
        let written = unsafe { libc::write(fd, rest.as_ptr().cast(), rest.len()) };
        if written < 0 {
            if std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return;
        }
        rest = &rest[written as usize..];
    }
}

/// F_SETOWN_EX's owner of SIGIO: the thread itself (fcntl.h's struct
/// f_owner_ex, F_OWNER_TID), where deliver_process_signal would forward
/// the signal.
#[cfg(target_os = "linux")]
unsafe fn set_sigio_owner(fd: libc::c_int) {
    #[repr(C)]
    struct FOwnerEx {
        type_: libc::c_int,
        pid: libc::pid_t,
    }
    const F_SETOWN_EX: libc::c_int = 15;
    const F_OWNER_TID: libc::c_int = 0;
    let owner = FOwnerEx {
        type_: F_OWNER_TID,
        // SAFETY: gettid has no preconditions.
        pid: unsafe { libc::syscall(libc::SYS_gettid) } as libc::pid_t,
    };
    // SAFETY: FD is open and OWNER outlives the call.
    unsafe {
        libc::fcntl(fd, F_SETOWN_EX, &owner as *const FOwnerEx);
    }
}

/// F_SETOWN to the process, as init_sys_modes does.
#[cfg(not(target_os = "linux"))]
unsafe fn set_sigio_owner(fd: libc::c_int) {
    // SAFETY: FD is open.
    unsafe {
        libc::fcntl(fd, libc::F_SETOWN, libc::getpid());
    }
}

/// sysdep.c:get_tty_size: the terminal's columns and rows, None where the
/// system gives no valid size.
pub(super) fn get_tty_size(fd: RawFd) -> Option<(u16, u16)> {
    // SAFETY: SIZE is a local winsize the ioctl fills.
    let mut size: libc::winsize = unsafe { std::mem::zeroed() };
    (unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut size) } == 0
        && size.ws_col > 0
        && size.ws_row > 0)
        .then_some((size.ws_col, size.ws_row))
}

/// What tty_read_avail_input's read of the terminal found.
pub(super) enum TtyRead {
    /// The bytes read (none when nothing was there).
    Bytes(Vec<u8>),
    /// EIO: the terminal device is gone.
    Gone,
}

/// tty_read_avail_input's read: as many bytes as FIONREAD says are there
/// (at most KBD_BUFFER_SIZE - 1), none when none are.
pub(super) fn read_available(fd: RawFd) -> TtyRead {
    let mut n_to_read: libc::c_int = 0;
    // SAFETY: N_TO_READ is a local int the ioctl fills.
    if unsafe { libc::ioctl(fd, libc::FIONREAD, &mut n_to_read) } < 0 {
        let error = std::io::Error::last_os_error();
        // A terminal device that terminated: EIO, EINVAL or ENOTTY.
        return if matches!(
            error.raw_os_error(),
            Some(libc::EIO | libc::EINVAL | libc::ENOTTY)
        ) {
            TtyRead::Gone
        } else {
            TtyRead::Bytes(Vec::new())
        };
    }
    if n_to_read <= 0 {
        return TtyRead::Bytes(Vec::new());
    }
    let wanted = (n_to_read as usize).min(READ_MAX);
    let mut buffer = vec![0u8; wanted];
    // SAFETY: BUFFER holds WANTED writable bytes.
    let nread = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), wanted) };
    if nread < 0 {
        let error = std::io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::EIO) {
            TtyRead::Gone
        } else {
            TtyRead::Bytes(Vec::new())
        };
    }
    buffer.truncate(nread as usize);
    TtyRead::Bytes(buffer)
}

/// keyboard.c:stuff_buffered_input's stuff_char (TIOCSTI): C pushed into
/// the terminal's input, for the shell to read after a suspension.
pub(super) fn stuff_char(fd: RawFd, c: u8) {
    // SAFETY: C is a byte the ioctl only reads.
    unsafe {
        libc::ioctl(fd, libc::TIOCSTI, &c as *const u8);
    }
}
