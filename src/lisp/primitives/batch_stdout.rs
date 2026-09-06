//! The batch process's stdout, buffered the way C stdio buffers GNU's.
//!
//! GNU writes `standard-output' with stdio: a stdout that is not a terminal
//! (a pipe, a file) is block-buffered and reaches the descriptor only when
//! the buffer fills, when `flush-standard-output' or the batch minibuffer
//! prompt flushes it, or at exit; a terminal is line-buffered.  stderr --
//! `message', the error report -- is unbuffered.  A child `call-process'
//! whose DESTINATION merges both streams therefore sees the parent's stderr
//! text before stdout text written earlier, and `gv-tests' pins that order
//! byte for byte.  Rust's `Stdout' flushes at every newline, which would
//! put the two streams in program order instead.
//!
//! The buffer follows glibc's `_IO_new_file_xsputn': it is sized from the
//! descriptor's `st_blksize' (BUFSIZ when that is unavailable); a write
//! fills the free space, flushes the full buffer, sends whole buffer-sized
//! blocks of a large remainder straight to the descriptor, and buffers the
//! tail.

use std::io::Write;
use std::sync::Mutex;

struct BatchStdout {
    buffer: Vec<u8>,
    capacity: usize,
    line_buffered: bool,
}

static STDOUT: Mutex<Option<BatchStdout>> = Mutex::new(None);

/// glibc `_IO_file_doallocate': `st_blksize' of the descriptor when it is
/// positive, BUFSIZ (8192) otherwise; a terminal is line-buffered.
fn stdout_mode() -> (usize, bool) {
    // SAFETY: fstat and isatty only read descriptor 1's metadata into a
    // zeroed stat buffer owned by this frame.
    unsafe {
        let mut status: libc::stat = std::mem::zeroed();
        let capacity =
            if libc::fstat(libc::STDOUT_FILENO, &mut status) == 0 && status.st_blksize > 0 {
                status.st_blksize as usize
            } else {
                8192
            };
        (capacity, libc::isatty(libc::STDOUT_FILENO) == 1)
    }
}

fn with_stdout<T>(
    action: impl FnOnce(&mut BatchStdout) -> std::io::Result<T>,
) -> std::io::Result<T> {
    let mut guard = STDOUT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = guard.get_or_insert_with(|| {
        let (capacity, line_buffered) = stdout_mode();
        BatchStdout {
            buffer: Vec::with_capacity(capacity),
            capacity,
            line_buffered,
        }
    });
    action(state)
}

/// Write BYTES to the batch stdout through the stdio-style buffer.
pub(crate) fn write(bytes: &[u8]) -> std::io::Result<()> {
    with_stdout(|stdout| stdout.write(bytes))
}

/// `fflush (stdout)': hand the buffered bytes to the descriptor now.
pub(crate) fn flush() -> std::io::Result<()> {
    with_stdout(BatchStdout::flush)
}

impl BatchStdout {
    fn write(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        if self.line_buffered {
            // A terminal: everything through the last newline goes out now.
            if let Some(last_newline) = bytes.iter().rposition(|byte| *byte == b'\n') {
                self.buffer.extend_from_slice(&bytes[..=last_newline]);
                self.flush()?;
                bytes = &bytes[last_newline + 1..];
            }
            self.buffer.extend_from_slice(bytes);
            return Ok(());
        }
        let room = self.capacity - self.buffer.len();
        let filling = room.min(bytes.len());
        self.buffer.extend_from_slice(&bytes[..filling]);
        bytes = &bytes[filling..];
        if bytes.is_empty() {
            return Ok(());
        }
        self.flush()?;
        // Whole blocks of the remainder bypass the buffer (glibc keeps the
        // buffer-sized alignment once the buffer is at least 128 bytes).
        let direct = bytes.len()
            - if self.capacity >= 128 {
                bytes.len() % self.capacity
            } else {
                0
            };
        if direct > 0 {
            let mut out = std::io::stdout().lock();
            out.write_all(&bytes[..direct])?;
            out.flush()?;
            bytes = &bytes[direct..];
        }
        self.buffer.extend_from_slice(bytes);
        Ok(())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut out = std::io::stdout().lock();
        let result = out.write_all(&self.buffer).and_then(|()| out.flush());
        self.buffer.clear();
        result
    }
}
