//! GNU/Linux application startup without pthread's affinity query.
//!
//! glibc still owns ELF initialization, TLS and final process teardown. On this
//! target std::env::args is initialized by std's ELF constructor (also used by
//! Rust cdylibs). The Linux kernel guards the original main stack; batch Lisp
//! runs on our separately guarded, registered stack. We do not need std's
//! pthread_getattr_np probe to discover the kernel stack's diagnostic range.

/// Perform the safety-critical startup work before any Rust standard I/O can
/// access a descriptor which a later open might otherwise reuse.
#[cfg(not(test))]
fn initialize_standard_fds() {
    for (fd, flags) in [
        (libc::STDIN_FILENO, libc::O_WRONLY),
        (libc::STDOUT_FILENO, libc::O_RDONLY),
        (libc::STDERR_FILENO, libc::O_RDONLY),
    ] {
        // GNU sysdep.c:init_standard_fds deliberately uses the opposite
        // direction, so ordinary I/O still fails when the stream was closed.
        // F_GETFL inspects descriptor validity using GNU's permitted syscall.
        // SAFETY: these calls only inspect or acquire process descriptors.
        let valid = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if valid != -1 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EBADF) {
            continue;
        }
        let opened = unsafe { libc::open(c"/dev/null".as_ptr(), flags) };
        // Open normally fills the first hole. Preserve GNU's dup2 fallback
        // if a constructor has already started another descriptor user.
        let failed = opened < 0
            || (opened != fd
                && (unsafe { libc::dup2(opened, fd) } < 0 || unsafe { libc::close(opened) } != 0));
        if failed {
            // SAFETY: Rust's standard streams are not safe to use yet.
            // Report through libc and take GNU's startup-error exit path.
            unsafe {
                libc::perror(c"/dev/null".as_ptr());
                libc::exit(1);
            }
        }
    }
}

/// GNU preserves the inherited SIGPIPE disposition in batch mode and ignores
/// it for interactive editing, where write errors are handled by the editor.
pub(super) fn ignore_broken_pipe() {
    // SAFETY: startup on the initial thread before interactive I/O begins.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_IGN) };
}

#[cfg(not(test))]
pub(super) fn enter(body: impl FnOnce() -> u8 + std::panic::UnwindSafe) -> ! {
    initialize_standard_fds();
    let status = match std::panic::catch_unwind(body) {
        Ok(status) => i32::from(status),
        Err(payload) => {
            // No unwind may cross the C entry boundary. Retain Rust's panic
            // status, and abort if destruction of the panic payload panics.
            if let Err(second) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
            {
                std::mem::forget(second);
                std::process::abort();
            }
            101
        }
    };
    // This public std boundary flushes buffered output, performs one-time
    // cleanup, serializes process exit and invokes libc's normal exit path.
    std::process::exit(status)
}
