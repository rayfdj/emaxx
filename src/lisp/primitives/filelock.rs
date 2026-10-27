//! filelock.c: lock files for editing.
//!
//! To lock a file FN, a symbolic link is made under the name
//! `make-lock-file-name' gives (`.#FN' in FN's directory), with the link
//! data USER@HOST.PID:BOOT; the :BOOT is left out when the boot time is
//! not available.  Where links do not work, the lock is a regular file
//! holding the same text, made under a nonce name and renamed into place.
//! When the host in a lock is this one, kill tells whether its process is
//! alive; a lock whose process is dead (or whose boot time is not this
//! boot's) is removed.

use super::*;
use crate::lisp::types::{Kind, LispErrorKind};

/// filelock.c's MAX_LFINFO: an arbitrary limit on a lock's contents.
const MAX_LFINFO: usize = 8 * 1024;

/// filelock.c's LINKS_MIGHT_NOT_WORK: Linux returns EPERM on file systems
/// that support neither hard nor symbolic links.
const LINKS_MIGHT_NOT_WORK: i32 = libc::EPERM;

/// A lock's contents, USER@HOST.PID with an optional :BOOT_TIME, and the
/// positions of its last `@', of the `.' after it and of the `:' (or its
/// equivalent, or the end) after the PID.
struct LockInfo {
    user: Vec<u8>,
    at: usize,
    dot: usize,
    colon: usize,
}

/// current_lock_owner's answers.
enum LockOwner {
    /// Nobody owns the lock, or it was obsolete and is removed.
    Free,
    /// ANOTHER_OWNS_IT: another process (or one on another host).
    Another(LockInfo),
    /// I_OWN_IT: this process.
    Mine,
    /// An errno value: something is wrong with the locking mechanism.
    Errno(i32),
}

fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// lisp.h's will_dump_p: this is the `--temacs' process that dumps.
fn will_dump_p(interp: &Interpreter, env: &Env) -> bool {
    interp
        .lookup_var("dump-mode", env)
        .and_then(|mode| string_like(&mode).map(|mode| mode.text))
        .is_some_and(|mode| mode == "pdump" || mode == "pbootstrap")
}

/// filelock.c:get_boot_sec: the time of the last system boot, or 0 when
/// that is not available.  gnulib's get_boot_time keeps the answer, which
/// a process that will dump does not ask for.
fn get_boot_sec(interp: &Interpreter, env: &Env) -> i64 {
    if will_dump_p(interp, env) {
        return 0;
    }
    static BOOT_TIME: OnceLock<i64> = OnceLock::new();
    *BOOT_TIME.get_or_init(boot_time_uncached)
}

/// gnulib boot-time.c:get_boot_time_uncached's seconds: the BOOT_TIME
/// entry of the utmp file, then the Linux and BSD fallbacks.
fn boot_time_uncached() -> i64 {
    let mut found = 0;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        // Linux: Raspbian's clock is set after the BOOT_TIME entry is
        // written, and the "runlevel" entry has the correct time.
        #[cfg(target_os = "linux")]
        let mut runlevel = 0;
        // SAFETY: the utmpx functions are used from one thread at a time
        // here (the entry they return is read before the next call), as
        // gnulib does; the file name is a valid C string.
        // readutmp.h's UTMP_FILE: glibc's _PATH_UTMP, macOS's
        // _PATH_UTMPX.
        #[cfg(target_os = "linux")]
        let utmp_file = c"/var/run/utmp";
        #[cfg(target_os = "macos")]
        let utmp_file = c"/var/run/utmpx";
        unsafe {
            libc::utmpxname(utmp_file.as_ptr());
            libc::setutxent();
            loop {
                let entry = libc::getutxent();
                if entry.is_null() {
                    break;
                }
                let entry = &*entry;
                let seconds = i64::from(entry.ut_tv.tv_sec);
                if entry.ut_type == libc::BOOT_TIME {
                    found = seconds;
                }
                #[cfg(target_os = "linux")]
                if std::ffi::CStr::from_ptr(entry.ut_user.as_ptr()).to_bytes() == b"runlevel"
                    && std::ffi::CStr::from_ptr(entry.ut_line.as_ptr()).to_bytes() == b"~"
                {
                    runlevel = seconds;
                }
            }
            libc::endutxent();
        }
        #[cfg(target_os = "linux")]
        {
            if found <= 60 && runlevel != 0 {
                found = runlevel;
            }
            if found == 0 {
                found = linux_boot_time_fallback();
            }
            if found == 0 {
                found = linux_boot_time_final_fallback();
            }
        }
        #[cfg(target_os = "macos")]
        if found == 0 {
            found = bsd_boot_time_final_fallback();
        }
    }
    found
}

/// boot-time-aux.h:get_linux_boot_time_fallback: the modification time
/// of a file only the boot process touches, when it is later than
/// 2005-07-25.
#[cfg(target_os = "linux")]
fn linux_boot_time_fallback() -> i64 {
    [
        "/var/lib/systemd/random-seed",
        "/var/lib/urandom/random-seed",
        "/var/lib/random-seed",
        "/var/run/utmp",
    ]
    .iter()
    .filter_map(|name| fs::metadata(name).ok())
    .map(|metadata| metadata.mtime())
    .find(|&seconds| seconds >= 1_122_334_455)
    .unwrap_or(0)
}

/// boot-time-aux.h:get_linux_boot_time_final_fallback: the time now less
/// the kernel's uptime counter.
#[cfg(target_os = "linux")]
fn linux_boot_time_final_fallback() -> i64 {
    let mut uptime = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let mut now = uptime;
    // SAFETY: both calls write only the timespec they are given.
    if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut uptime) } < 0
        || unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut now) } < 0
    {
        return 0;
    }
    let borrow = i64::from(now.tv_nsec < uptime.tv_nsec);
    now.tv_sec - borrow - uptime.tv_sec
}

/// boot-time-aux.h:get_bsd_boot_time_final_fallback: sysctl's
/// KERN_BOOTTIME.
#[cfg(target_os = "macos")]
fn bsd_boot_time_final_fallback() -> i64 {
    let mut request = [libc::CTL_KERN, libc::KERN_BOOTTIME];
    let mut result = libc::timeval {
        tv_sec: 0,
        tv_usec: 0,
    };
    let mut length = std::mem::size_of::<libc::timeval>();
    // SAFETY: the kernel writes at most LENGTH bytes into RESULT.
    let status = unsafe {
        libc::sysctl(
            request.as_mut_ptr(),
            request.len() as libc::c_uint,
            (&raw mut result).cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if status >= 0 { result.tv_sec } else { 0 }
}

/// filelock.c:rename_lock_file: OLD renamed to NEW, replacing NEW only
/// when FORCE.  It is all right for OLD to have two hard links for a
/// moment.
fn rename_lock_file(old: &CString, new: &CString, force: bool) -> Result<(), i32> {
    // SAFETY: every call below takes the two valid C strings only.
    unsafe {
        if !force {
            #[cfg(target_os = "linux")]
            {
                let status = libc::renameat2(
                    libc::AT_FDCWD,
                    old.as_ptr(),
                    libc::AT_FDCWD,
                    new.as_ptr(),
                    libc::RENAME_NOREPLACE,
                );
                if status == 0 {
                    return Ok(());
                }
                if errno() != libc::ENOSYS {
                    return Err(errno());
                }
            }
            if libc::link(old.as_ptr(), new.as_ptr()) == 0 {
                return if libc::unlink(old.as_ptr()) == 0 || errno() == libc::ENOENT {
                    Ok(())
                } else {
                    Err(errno())
                };
            }
            let error = errno();
            if error != libc::ENOSYS && error != LINKS_MIGHT_NOT_WORK {
                return Err(error);
            }
            // `link' does not work on this file system (a FAT32 file
            // system mounted on GNU/Linux): rename after checking that NEW
            // does not exist.
            let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
            if libc::fstatat(
                libc::AT_FDCWD,
                new.as_ptr(),
                status.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            ) == 0
                || errno() == libc::EOVERFLOW
            {
                return Err(libc::EEXIST);
            }
            if errno() != libc::ENOENT {
                return Err(errno());
            }
        }
        if libc::rename(old.as_ptr(), new.as_ptr()) == 0 {
            Ok(())
        } else {
            Err(errno())
        }
    }
}

/// filelock.c:create_lock_file: the lock file LFNAME made with the
/// contents LOCK_INFO, an existing one removed first when FORCE.
fn create_lock_file(lfname: &CString, lock_info: &[u8], force: bool) -> Result<(), i32> {
    let target = CString::new(lock_info).map_err(|_| libc::EINVAL)?;
    // SAFETY: symlink and unlink take the two valid C strings.
    let symlink = || unsafe {
        if libc::symlink(target.as_ptr(), lfname.as_ptr()) == 0 {
            Ok(())
        } else {
            Err(errno())
        }
    };
    let mut result = symlink();
    if result == Err(libc::EEXIST) && force {
        // SAFETY: as above.
        unsafe { libc::unlink(lfname.as_ptr()) };
        result = symlink();
    }
    match result {
        Err(libc::ENOSYS | LINKS_MIGHT_NOT_WORK | libc::ENAMETOOLONG) => {}
        result => return result,
    }
    // Links do not work here: write a nonce file in the lock's directory
    // and rename it to the lock.
    let name = lfname.as_bytes();
    let directory = name
        .iter()
        .rposition(|&byte| byte == b'/')
        .map_or(&name[..0], |slash| &name[..=slash]);
    let mut nonce = [directory, b".#-emacsXXXXXX".as_slice()].concat();
    nonce.push(0);
    // SAFETY: NONCE is a NUL-terminated template mkostemp rewrites in
    // place; the descriptor it opens is written, changed and closed here.
    unsafe {
        let fd = libc::mkostemp(nonce.as_mut_ptr().cast(), libc::O_CLOEXEC);
        if fd < 0 {
            return Err(errno());
        }
        let nonce = CString::from_vec_with_nul(nonce).map_err(|_| libc::EINVAL)?;
        let mut error = 0;
        // Make the lock file readable to others, so that others' sessions
        // can read it; keep it user-writable for the file systems that
        // will not unlink a read-only file (Bug#37884).
        if libc::write(fd, lock_info.as_ptr().cast(), lock_info.len()) != lock_info.len() as isize
            || libc::fchmod(
                fd,
                libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IROTH,
            ) != 0
        {
            error = errno();
        }
        if libc::close(fd) != 0 {
            error = errno();
        }
        if error == 0
            && let Err(rename) = rename_lock_file(&nonce, lfname, force)
        {
            error = rename;
        }
        if error != 0 {
            libc::unlink(nonce.as_ptr());
            return Err(error);
        }
    }
    Ok(())
}

/// The host name a lock records and is compared with: `system-name', ""
/// for nil (a `--no-build-details' Emacs) and with any `@' replaced by `-'.
fn lock_host_name(interp: &mut Interpreter, env: &mut Env) -> Result<Vec<u8>, LispError> {
    let host = call(interp, "system-name", &[], env)?;
    let host = match host.kind() {
        Kind::StringObject(state) if state.borrow().bytes().contains(&b'@') => call_named_function(
            interp,
            "string-replace",
            &[Value::string("@"), Value::string("-"), host],
            env,
        )?,
        _ => host,
    };
    Ok(match host.kind() {
        Kind::StringObject(state) => state.borrow().bytes().to_vec(),
        _ => Vec::new(),
    })
}

/// filelock.c:lock_file_1: the lock LFNAME made for this process, even
/// over another's when FORCE.
fn lock_file_1(
    interp: &mut Interpreter,
    env: &mut Env,
    lfname: &CString,
    force: bool,
) -> Result<Result<(), i32>, LispError> {
    let boot = get_boot_sec(interp, env);
    let user = call(interp, "user-login-name", &[], env)?;
    let mut info = match user.kind() {
        Kind::StringObject(state) => state.borrow().bytes().to_vec(),
        _ => Vec::new(),
    };
    info.push(b'@');
    info.extend(lock_host_name(interp, env)?);
    info.extend(format!(".{}", std::process::id()).bytes());
    if boot != 0 {
        info.extend(format!(":{boot}").bytes());
    }
    if info.len() > MAX_LFINFO {
        return Ok(Err(libc::ENAMETOOLONG));
    }
    Ok(create_lock_file(lfname, &info, force))
}

/// filelock.c:within_one_second.
fn within_one_second(a: i64, b: i64) -> bool {
    a.checked_sub(b)
        .is_some_and(|difference| (-1..=1).contains(&difference))
}

/// filelock.c:read_lock_data: at most MAX_LFINFO + 1 bytes of the lock
/// LFNAME, a symbolic link's data or a regular file's contents.
fn read_lock_data(lfname: &CString) -> Result<Vec<u8>, i32> {
    let mut buffer = vec![0u8; MAX_LFINFO + 1];
    loop {
        // SAFETY: BUFFER has room for the MAX_LFINFO + 1 bytes asked for.
        let length = unsafe {
            libc::readlinkat(
                libc::AT_FDCWD,
                lfname.as_ptr(),
                buffer.as_mut_ptr().cast(),
                buffer.len(),
            )
        };
        if length >= 0 {
            buffer.truncate(length as usize);
            return Ok(buffer);
        }
        if errno() != libc::EINVAL {
            return Err(errno());
        }
        // SAFETY: LFNAME is a valid C string; the descriptor is read into
        // BUFFER (with room for the bytes asked for) and closed.
        unsafe {
            let fd = libc::open(
                lfname.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            );
            if fd >= 0 {
                let read = libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len());
                let read_errno = errno();
                if libc::close(fd) != 0 {
                    return Err(errno());
                }
                if read < 0 {
                    return Err(read_errno);
                }
                buffer.truncate(read as usize);
                return Ok(buffer);
            }
        }
        if errno() != libc::ELOOP {
            return Err(errno());
        }
        // readlinkat saw a non-symlink and open a symlink: the one was
        // replaced by the other.  Try again.
    }
}

/// filelock.c:integer_prefixed: S starts with a decimal integer,
/// optionally negative.
fn integer_prefixed(s: &[u8]) -> bool {
    let start = usize::from(s.first() == Some(&b'-'));
    s.get(start).is_some_and(u8::is_ascii_digit)
}

/// strtoimax on an integer_prefixed S: its value (saturated, as on
/// ERANGE) and where its digits end.
fn strtoimax(s: &[u8]) -> (i64, usize, bool) {
    let negative = s.first() == Some(&b'-');
    let start = usize::from(negative);
    let digits = s[start..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    let mut value: i64 = 0;
    let mut overflow = false;
    for &digit in &s[start..start + digits] {
        let digit = i64::from(digit - b'0');
        match value.checked_mul(10).and_then(|value| {
            if negative {
                value.checked_sub(digit)
            } else {
                value.checked_add(digit)
            }
        }) {
            Some(next) => value = next,
            None => {
                overflow = true;
                value = if negative { i64::MIN } else { i64::MAX };
                break;
            }
        }
    }
    (value, start + digits, overflow)
}

/// filelock.c:current_lock_owner: who owns the lock LFNAME.
fn current_lock_owner(
    interp: &mut Interpreter,
    env: &mut Env,
    lfname: &CString,
) -> Result<LockOwner, LispError> {
    let user = match read_lock_data(lfname) {
        Ok(user) => user,
        Err(libc::ENOENT | libc::ENOTDIR) => return Ok(LockOwner::Free),
        Err(error) => return Ok(LockOwner::Errno(error)),
    };
    if user.len() > MAX_LFINFO {
        return Ok(LockOwner::Errno(libc::ENAMETOOLONG));
    }
    // Parse USER@HOST.PID:BOOT_TIME.  The USER is everything before the
    // last @; the C strings stop at a NUL.
    let Some(at) = user.iter().rposition(|&byte| byte == b'@') else {
        return Ok(LockOwner::Errno(libc::EINVAL));
    };
    let string_end = user[at..]
        .iter()
        .position(|&byte| byte == 0)
        .map_or(user.len(), |nul| at + nul);
    let Some(dot) = user[at..string_end]
        .iter()
        .rposition(|&byte| byte == b'.')
        .map(|dot| at + dot)
    else {
        return Ok(LockOwner::Errno(libc::EINVAL));
    };
    // The PID is everything from the last `.' to the `:' or equivalent.
    if !integer_prefixed(&user[dot + 1..]) {
        return Ok(LockOwner::Errno(libc::EINVAL));
    }
    let (pid, pid_length, overflow) = strtoimax(&user[dot + 1..]);
    let pid = if overflow { -1 } else { pid };
    let colon = dot + 1 + pid_length;
    // After the `:' or equivalent, if there is one, comes the boot time.
    let (boot_time, end) = match user.get(colon) {
        None | Some(0) => (0, colon),
        Some(&separator @ (b':' | 0o357)) => {
            let mut boot = colon + 1;
            // "\357\200\242" (U+F022 in UTF-8) is taken as ":", for the
            // Linux CIFS client that transliterates ':' in symlink
            // contents (Bug#24656).
            if separator == 0o357 {
                if user.get(boot..boot + 2) != Some(b"\x80\xa2".as_slice()) {
                    return Ok(LockOwner::Errno(libc::EINVAL));
                }
                boot += 2;
            }
            if !integer_prefixed(&user[boot..]) {
                return Ok(LockOwner::Errno(libc::EINVAL));
            }
            let (boot_time, length, _) = strtoimax(&user[boot..]);
            (boot_time, boot + length)
        }
        Some(_) => return Ok(LockOwner::Errno(libc::EINVAL)),
    };
    if end != user.len() {
        return Ok(LockOwner::Errno(libc::EINVAL));
    }
    let host = lock_host_name(interp, env)?;
    // On the current host?
    if user[at + 1..dot] != host[..] {
        // A check for stale locks on remote machines would go here.
        return Ok(LockOwner::Another(LockInfo {
            user,
            at,
            dot,
            colon,
        }));
    }
    if pid == i64::from(std::process::id()) {
        return Ok(LockOwner::Mine);
    }
    let alive = libc::pid_t::try_from(pid).is_ok_and(|pid| {
        // SAFETY: signal 0 only checks that the process exists.
        pid > 0 && (unsafe { libc::kill(pid, 0) } >= 0 || errno() == libc::EPERM)
    });
    if alive && (boot_time == 0 || within_one_second(boot_time, get_boot_sec(interp, env))) {
        return Ok(LockOwner::Another(LockInfo {
            user,
            at,
            dot,
            colon,
        }));
    }
    // The owner process is dead or has a strange pid: zap the lock.
    // SAFETY: LFNAME is a valid C string.
    Ok(if unsafe { libc::unlink(lfname.as_ptr()) } < 0 {
        LockOwner::Errno(errno())
    } else {
        LockOwner::Free
    })
}

/// filelock.c:lock_if_free: LFNAME locked unless another process owns it
/// (Another, with its lock) or locking fails (an errno value).
fn lock_if_free(
    interp: &mut Interpreter,
    env: &mut Env,
    lfname: &CString,
) -> Result<LockOwner, LispError> {
    loop {
        match lock_file_1(interp, env, lfname, false)? {
            Ok(()) => return Ok(LockOwner::Free),
            Err(libc::EEXIST) => {}
            Err(error) => return Ok(LockOwner::Errno(error)),
        }
        match current_lock_owner(interp, env, lfname)? {
            // A stale lock removed, or the owner removed it: try again.
            LockOwner::Free => {}
            LockOwner::Mine => return Ok(LockOwner::Free),
            owner => return Ok(owner),
        }
    }
}

/// filelock.c:make_lock_file_name: the name of FN's lock, or None.
fn make_lock_file_name(
    interp: &mut Interpreter,
    env: &mut Env,
    file: Value,
) -> Result<Option<CString>, LispError> {
    let file = call(interp, "expand-file-name", &[file, Value::Nil], env)?;
    let name = call_named_function(interp, "make-lock-file-name", &[file], env)?;
    if name.is_nil() {
        return Ok(None);
    }
    CString::new(string_text(&name)?)
        .map(Some)
        .map_err(|_| LispError::Signal("File name contains nul byte".into()))
}

/// filelock.c:lock_file: FILE locked, serving notice on the world that it
/// is being edited.  The visited file's change on disk since its visit
/// asks `userlock--ask-user-about-supersession-threat' first, even when
/// `create-lockfiles' is nil; a lock another process owns asks
/// `ask-user-about-lock', which may signal, take the lock (t) or go on
/// without it (nil).
fn lock_file(interp: &mut Interpreter, env: &mut Env, file: Value) -> Result<(), LispError> {
    // No locking while dumping.
    if will_dump_p(interp, env) {
        return Ok(());
    }
    let mut lfname = None;
    if interp
        .lookup_var("create-lockfiles", env)
        .is_some_and(|value| value.is_truthy())
    {
        lfname = make_lock_file_name(interp, env, file)?;
        if lfname.is_none() {
            return Ok(());
        }
    }
    // Whether this file is visited and has changed on disk since.
    let subject = call(interp, "get-truename-buffer", &[file], env)?;
    if !subject.is_nil()
        && call(interp, "verify-visited-file-modtime", &[subject], env)?.is_nil()
        && !call(interp, "file-exists-p", &[file], env)?.is_nil()
    {
        let mine = match &lfname {
            Some(lfname) => matches!(current_lock_owner(interp, env, lfname)?, LockOwner::Mine),
            None => false,
        };
        if !mine {
            call_named_function(
                interp,
                "userlock--ask-user-about-supersession-threat",
                &[file],
                env,
            )?;
        }
    }
    let Some(lfname) = lfname else {
        return Ok(());
    };
    // Errors from lock_if_free other than another's lock are ignored, as
    // filelock.c's FIXME says.
    if let LockOwner::Another(info) = lock_if_free(interp, env, &lfname)? {
        // Someone else has the lock: USER@HOST (pid PID) asks whether to
        // break it.
        let mut clasher = info.user[..info.dot].to_vec();
        clasher.extend_from_slice(b" (pid ");
        clasher.extend_from_slice(&info.user[info.dot + 1..info.colon]);
        clasher.push(b')');
        let clasher =
            make_string(clasher).map_err(|error| LispError::Signal(format!("{error:?}")))?;
        let attack = call_named_function(interp, "ask-user-about-lock", &[file, clasher], env)?;
        if !attack.is_nil() {
            let _ = lock_file_1(interp, env, &lfname, true)?;
        }
    }
    Ok(())
}

/// filelock.c:unlock_file: FILE's lock removed when this process owns
/// it.
fn unlock_file(interp: &mut Interpreter, env: &mut Env, file: Value) -> Result<(), LispError> {
    let Some(lfname) = make_lock_file_name(interp, env, file)? else {
        return Ok(());
    };
    let error = match current_lock_owner(interp, env, &lfname)? {
        LockOwner::Free | LockOwner::Another(_) => return Ok(()),
        // SAFETY: LFNAME is a valid C string.
        LockOwner::Mine if unsafe { libc::unlink(lfname.as_ptr()) } == 0 => return Ok(()),
        LockOwner::Mine => match errno() {
            libc::ENOENT => return Ok(()),
            error => error,
        },
        LockOwner::Errno(error) => error,
    };
    Err(report_file_errno("Unlocking file", file, error))
}

/// fileio.c:report_file_errno with the file name FILE as given.
fn report_file_errno(message: &str, file: Value, error: i32) -> LispError {
    let error = std::io::Error::from_raw_os_error(error);
    LispError::SignalValue(Value::list([
        Value::symbol(file_error_condition(&error)),
        Value::string(message),
        Value::String(errno_text(&error).into()),
        file,
    ]))
}

/// Flock_file's own body, past CHECK_STRING and the file name handler.
pub(crate) fn lock_file_primitive(
    interp: &mut Interpreter,
    env: &mut Env,
    file: Value,
) -> Result<Value, LispError> {
    if !file.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), file));
    }
    lock_file(interp, env, file)?;
    Ok(Value::Nil)
}

/// Funlock_file's own body, past CHECK_STRING and the file name handler:
/// unlock_file under internal_condition_case_1, whose `file-error'
/// handler calls `userlock--handle-unlock-error'.
pub(crate) fn unlock_file_primitive(
    interp: &mut Interpreter,
    env: &mut Env,
    file: Value,
) -> Result<Value, LispError> {
    if !file.is_string() {
        return Err(LispError::WrongTypeArgument("stringp".into(), file));
    }
    let handlers = interp.push_condition_case_handler(vec![Value::symbol("file-error")]);
    let depth = env.len();
    let result = unlock_file(interp, env, file);
    interp.pop_handler_bindings(handlers);
    env.truncate(depth);
    match result {
        Err(error)
            if !matches!(
                error.kind(),
                LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)
            ) && interp
                .error_condition_names(&error.condition_type())
                .iter()
                .any(|name| name.as_str() == "file-error") =>
        {
            interp.clear_batch_error_backtrace();
            let error = crate::lisp::eval::error_condition_value(&error);
            call_named_function(interp, "userlock--handle-unlock-error", &[error], env)?;
            Ok(Value::Nil)
        }
        result => result.map(|()| Value::Nil),
    }
}

/// Ffile_locked_p's own body, past the file name handler: nil when FILE
/// is not locked, t when this process locked it, else the user who did.
pub(crate) fn file_locked_p(
    interp: &mut Interpreter,
    env: &mut Env,
    file: Value,
) -> Result<Value, LispError> {
    let Some(lfname) = make_lock_file_name(interp, env, file)? else {
        return Ok(Value::Nil);
    };
    match current_lock_owner(interp, env, &lfname)? {
        LockOwner::Mine => Ok(Value::T),
        LockOwner::Another(info) => make_string(info.user[..info.at].to_vec())
            .map_err(|error| LispError::Signal(format!("{error:?}"))),
        LockOwner::Free => Ok(Value::Nil),
        LockOwner::Errno(error) => Err(report_file_errno("Testing file lock", file, error)),
    }
}

/// Flock_file from C: through the file name handler of `lock-file'.
pub(crate) fn flock_file(
    interp: &mut Interpreter,
    env: &mut Env,
    file: Value,
) -> Result<(), LispError> {
    call(interp, "lock-file", &[file], env).map(drop)
}

/// Funlock_file from C: through the file name handler of `unlock-file'.
pub(crate) fn funlock_file(
    interp: &mut Interpreter,
    env: &mut Env,
    file: Value,
) -> Result<(), LispError> {
    call(interp, "unlock-file", &[file], env).map(drop)
}

/// The buffer's `buffer-file-truename' when it is a string.
fn buffer_file_truename(interp: &Interpreter, buffer_id: u64) -> Option<Value> {
    interp
        .get_buffer_by_id(buffer_id)?
        .file_truename
        .as_deref()
        .map(Value::string)
}

/// Flock_buffer: FILE (the current buffer's `buffer-file-truename' when
/// nil) locked when the current buffer is modified.
pub(crate) fn lock_buffer(
    interp: &mut Interpreter,
    env: &mut Env,
    file: Option<Value>,
) -> Result<Value, LispError> {
    let file = match file.filter(|file| !file.is_nil()) {
        Some(file) if !file.is_string() => {
            return Err(LispError::WrongTypeArgument("stringp".into(), file));
        }
        Some(file) => Some(file),
        None => buffer_file_truename(interp, interp.current_buffer_id()),
    };
    if interp.buffer.borrow().is_modified()
        && let Some(file) = file
    {
        flock_file(interp, env, file)?;
    }
    Ok(Value::Nil)
}

/// filelock.c:unlock_buffer (and Funlock_buffer for the current buffer):
/// the file visited in BUFFER unlocked when the buffer is modified.
pub(crate) fn unlock_buffer(
    interp: &mut Interpreter,
    env: &mut Env,
    buffer_id: u64,
) -> Result<(), LispError> {
    if interp
        .get_buffer_by_id(buffer_id)
        .is_some_and(|buffer| buffer.is_modified())
        && let Some(file) = buffer_file_truename(interp, buffer_id)
    {
        funlock_file(interp, env, file)?;
    }
    Ok(())
}

/// filelock.c:unlock_all_files: the files of every modified live buffer
/// unlocked, as shut_down_emacs does.
pub(crate) fn unlock_all_files(interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
    let buffers = interp
        .buffer_list
        .iter()
        .map(|(id, _)| *id)
        .collect::<Vec<_>>();
    for buffer_id in buffers {
        unlock_buffer(interp, env, buffer_id)?;
    }
    Ok(())
}
