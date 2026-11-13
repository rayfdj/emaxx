//! The process's standard output streams, buffered the way C stdio
//! buffers GNU's.
//!
//! GNU writes `standard-output' with stdio: a stdout that is not a terminal
//! (a pipe, a file) is block-buffered and reaches the descriptor only when
//! the buffer fills, when `flush-standard-output' or the batch minibuffer
//! prompt flushes it, or at exit; a terminal is line-buffered.  stderr --
//! the error report -- is unbuffered, and a batch `message' goes to a
//! line-buffered stream on the same descriptor (sysdep.c's buferr, below),
//! so each of its lines is one write.  A child `call-process'
//! whose DESTINATION merges both streams therefore sees the parent's stderr
//! text before stdout text written earlier, and `gv-tests' pins that order
//! byte for byte.  Rust's `Stdout' flushes at every newline, which would
//! put the two streams in program order instead.
//!
//! The streams follow glibc: `_IO_file_doallocate' sizes a buffer BUFSIZ,
//! or the descriptor's `st_blksize' when that is smaller;
//! `_IO_new_file_xsputn' fills the free space, writes the full buffer,
//! sends whole buffer-sized blocks of what is left straight to the
//! descriptor and hands the tail to `_IO_default_xsputn'.  A stream's
//! first write finds no buffer and so no space to fill.
//!
//! No writer is told of a failed write, as GNU's callers of fwrite, putc
//! and fflush do not look: the stream remembers it (ferror), and
//! sysdep.c's close_output_streams, which exit runs, makes it the
//! process's status.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

/// glibc's BUFSIZ.
const BUFSIZ: usize = 8192;

struct Stream {
    descriptor: libc::c_int,
    buffer: Vec<u8>,
    capacity: usize,
    line_buffered: bool,
    /// _IO_CURRENTLY_PUTTING: the stream has been written.  Until its
    /// first `_IO_OVERFLOW' it has no buffer and no space in one.
    putting: bool,
    /// _IO_ERR_SEEN, what ferror reads: a write of the descriptor failed.
    failed: bool,
    /// The writes a test's stream made, in place of a descriptor.
    #[cfg(test)]
    written: Option<Vec<Vec<u8>>>,
}

static STDOUT: Mutex<Option<Stream>> = Mutex::new(None);

/// sysdep.c's buferr: "a stream that is like stderr, except line
/// buffered" (init_standard_fds: fdopen of STDERR_FILENO and setvbuf
/// _IOLBF), which errputc and errwrite write for xdisp.c's
/// message_to_stderr.  A line is one write however many calls made it.
static BUFERR: Mutex<Option<Stream>> = Mutex::new(None);

/// ferror (stderr): a write of C's unbuffered stderr failed.
static STDERR_FAILED: AtomicBool = AtomicBool::new(false);

/// Whether buferr is a stream: 0 while init_standard_fds has not been
/// asked, 1 for the stream, 2 for none.
static BUFERR_STATE: AtomicU8 = AtomicU8::new(0);

/// sysdep.c:init_standard_fds's `buferr = emacs_fdopen (STDERR_FILENO,
/// "w")'.  fdopen refuses a descriptor that is not open for writing --
/// the null device force_open opens for reading in a closed stderr's
/// place -- and buferr is then NULL: errstream is stderr itself,
/// unbuffered, and stays so whatever descriptor 2 becomes later.
pub(crate) fn init_buferr() {
    // SAFETY: fcntl only reads descriptor 2's status flags.
    let flags = unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_GETFL) };
    let stream = flags != -1 && flags & libc::O_ACCMODE != libc::O_RDONLY;
    let _ = BUFERR_STATE.compare_exchange(
        0,
        if stream { 1 } else { 2 },
        Ordering::Relaxed,
        Ordering::Relaxed,
    );
}

/// errstream: whether buferr is there to write.
fn buferr_is_a_stream() -> bool {
    if BUFERR_STATE.load(Ordering::Relaxed) == 0 {
        init_buferr();
    }
    BUFERR_STATE.load(Ordering::Relaxed) == 1
}

/// emacs.c's initial_argv is set: init_cmdargs has run.  sysdep.c's
/// emacs_perror names the command "emacs" before that (`--help' and
/// `--fingerprint' exit earlier) and argv[0] after.
static INITIAL_ARGV: AtomicBool = AtomicBool::new(false);

/// emacs.c:init_cmdargs's `initial_argv = argv'.
pub(crate) fn set_initial_argv() {
    INITIAL_ARGV.store(true, Ordering::Relaxed);
}

/// glibc `_IO_file_doallocate': BUFSIZ, or the descriptor's `st_blksize'
/// when that is positive and smaller.
fn buffer_size(blksize: u64) -> usize {
    if blksize > 0 && blksize < BUFSIZ as u64 {
        blksize as usize
    } else {
        BUFSIZ
    }
}

/// `_IO_file_doallocate' for DESCRIPTOR: the buffer's size, and whether
/// the descriptor is a terminal, which makes its stream line-buffered.
fn stream_mode(descriptor: libc::c_int) -> (usize, bool) {
    // SAFETY: fstat and isatty only read DESCRIPTOR's metadata into a
    // zeroed stat buffer owned by this frame.
    unsafe {
        let mut status: libc::stat = std::mem::zeroed();
        if libc::fstat(descriptor, &mut status) != 0 {
            return (BUFSIZ, false);
        }
        // Like _IO_file_doallocate, ask about terminal state only for a
        // character device. The ioctl is unnecessary for files and pipes,
        // and GNU's inherited sandbox does not permit that query on stdout.
        let terminal =
            status.st_mode & libc::S_IFMT == libc::S_IFCHR && libc::isatty(descriptor) == 1;
        (buffer_size(status.st_blksize as u64), terminal)
    }
}

fn with_stdout<T>(action: impl FnOnce(&mut Stream) -> T) -> T {
    let mut guard = STDOUT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = guard.get_or_insert_with(|| {
        let (capacity, line_buffered) = stream_mode(libc::STDOUT_FILENO);
        Stream::new(libc::STDOUT_FILENO, capacity, line_buffered)
    });
    action(state)
}

fn with_errstream<T>(action: impl FnOnce(&mut Stream) -> T) -> T {
    let mut guard = BUFERR
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = guard.get_or_insert_with(|| {
        // setvbuf (buferr, NULL, _IOLBF, 0): line-buffered whatever the
        // descriptor is, its buffer sized as any stream's.
        let (capacity, _) = stream_mode(libc::STDERR_FILENO);
        Stream::new(libc::STDERR_FILENO, capacity, true)
    });
    action(state)
}

/// fwrite of BYTES to stdout.
pub(crate) fn write(bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    let _ = with_stdout(|stdout| stdout.xsputn(bytes));
}

/// `fflush (stdout)': hand the buffered bytes to the descriptor now.
pub(crate) fn flush() {
    let mut guard = STDOUT
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // fflush of an unused stream does not allocate a buffer or probe its fd.
    if let Some(stream) = guard.as_mut() {
        let _ = stream.flush();
    }
}

/// sysdep.c:errwrite: `fwrite_unlocked (buf, 1, nbuf, errstream ())'.
/// (errstream's fflush of stderr has nothing to do: that stream is
/// unbuffered here as in C.)
pub(crate) fn errwrite(bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    if !buferr_is_a_stream() {
        return stderr_write(bytes);
    }
    let _ = with_errstream(|stream| stream.xsputn(bytes));
}

/// sysdep.c:errputc: `fputc_unlocked (c, errstream ())'.
pub(crate) fn errputc(byte: u8) {
    if !buferr_is_a_stream() {
        return stderr_write(&[byte]);
    }
    let _ = with_errstream(|stream| stream.overflow(byte));
}

/// A write of C's stderr, which is unbuffered: BYTES to descriptor 2
/// now, a failure remembered for close_output_streams.
pub(crate) fn stderr_write(bytes: &[u8]) {
    if sys_write(libc::STDERR_FILENO, bytes).is_err() {
        STDERR_FAILED.store(true, Ordering::Relaxed);
    }
}

/// glibc `_IO_new_file_write': BYTES to DESCRIPTOR, a short write
/// continued and a failed one the stream's error.
fn sys_write(descriptor: libc::c_int, mut bytes: &[u8]) -> std::io::Result<()> {
    while !bytes.is_empty() {
        // SAFETY: BYTES is a live slice for the length given.
        let count = unsafe { libc::write(descriptor, bytes.as_ptr().cast(), bytes.len()) };
        if count < 0 {
            return Err(std::io::Error::last_os_error());
        }
        bytes = &bytes[count as usize..];
    }
    Ok(())
}

/// sysdep.c:emacs_perror: "COMMAND: MESSAGE: strerror (ERRNO)" as one
/// write of descriptor 2, not a stream's; COMMAND is initial_argv[0], or
/// "emacs" while there is none.
fn emacs_perror(message: &str, errno: i32) {
    use std::os::unix::ffi::OsStrExt;
    // SAFETY: strerror returns a NUL-terminated string valid until the
    // next call on this thread, copied at once.
    let error_string = unsafe { std::ffi::CStr::from_ptr(libc::strerror(errno)) }
        .to_bytes()
        .to_vec();
    let command = Some(())
        .filter(|()| INITIAL_ARGV.load(Ordering::Relaxed))
        .and_then(|()| std::env::args_os().next())
        .unwrap_or_else(|| "emacs".into());
    let mut line = command.as_bytes().to_vec();
    line.extend_from_slice(b": ");
    line.extend_from_slice(message.as_bytes());
    line.extend_from_slice(b": ");
    line.extend_from_slice(&error_string);
    line.push(b'\n');
    let _ = sys_write(libc::STDERR_FILENO, &line);
}

/// close (DESCRIPTOR), as fclose closes a stream's.
fn close_descriptor(descriptor: libc::c_int) -> std::io::Result<()> {
    // SAFETY: the process is exiting and writes this descriptor no more.
    if unsafe { libc::close(descriptor) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// sysdep.c:close_output_streams, which GNU registers with atexit: the
/// status the process exits with, STATUS unless a stream failed.
///
/// gnulib's close_stream (stdout) fails when a write of the stream failed
/// before, or when its fclose does (the flush of what is pending, then
/// the close) other than by EBADF with nothing pending: then
/// "Write error to standard output" is reported with fclose's errno (0
/// when fclose itself succeeded) and the process is gone with
/// EXIT_FAILURE, buferr unflushed.  Otherwise buferr is flushed and
/// stderr closed, and a failure of either, now or before, is
/// EXIT_FAILURE too.
pub(crate) fn close_output_streams(status: i32) -> i32 {
    let (pending, failed_before, flushed) = {
        let mut guard = STDOUT
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_mut() {
            Some(stream) => (!stream.buffer.is_empty(), stream.failed, stream.flush()),
            None => (false, false, Ok(())),
        }
    };
    // _IO_new_file_close_it: the close's failure before the flush's.
    let fclose = close_descriptor(libc::STDOUT_FILENO).and(flushed);
    let fclose_failed = fclose
        .as_ref()
        .is_err_and(|error| pending || error.raw_os_error() != Some(libc::EBADF));
    if failed_before || fclose_failed {
        let errno = fclose
            .err()
            .and_then(|error| error.raw_os_error())
            .unwrap_or(0);
        emacs_perror("Write error to standard output", errno);
        return 1;
    }
    let buferr_failed = {
        let mut guard = BUFERR
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .as_mut()
            .is_some_and(|stream| stream.flush().is_err() || stream.failed)
    };
    // close_stream (stderr), whatever buferr did (`err | ...'): nothing
    // is ever pending in it.
    let closed = close_descriptor(libc::STDERR_FILENO);
    let stderr_failed = STDERR_FAILED.load(Ordering::Relaxed)
        || closed.is_err_and(|error| error.raw_os_error() != Some(libc::EBADF));
    if buferr_failed || stderr_failed {
        return 1;
    }
    status
}

impl Stream {
    fn new(descriptor: libc::c_int, capacity: usize, line_buffered: bool) -> Self {
        Self {
            descriptor,
            buffer: Vec::with_capacity(capacity),
            capacity,
            line_buffered,
            putting: false,
            failed: false,
            #[cfg(test)]
            written: None,
        }
    }

    /// glibc's `_IO_new_file_xsputn', what fwrite comes to.
    ///
    /// The space available is the buffer's free part once the stream is
    /// in put mode, none before.  A line-buffered stream whose space
    /// takes all of BYTES fills it only through their last newline, and
    /// writes the buffer then.  What does not go into the space waits for
    /// the buffer's write (`_IO_OVERFLOW (f, EOF)'), whole buffer-sized
    /// blocks of it going straight to the descriptor in one write and the
    /// rest through `_IO_default_xsputn'.  After a failed write the rest
    /// is dropped.
    fn xsputn(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        let mut must_flush = false;
        let mut count = 0;
        if self.putting {
            count = self.capacity.saturating_sub(self.buffer.len());
            if self.line_buffered
                && count >= bytes.len()
                && let Some(last_newline) = bytes.iter().rposition(|byte| *byte == b'\n')
            {
                count = last_newline + 1;
                must_flush = true;
            }
        }
        let count = count.min(bytes.len());
        self.buffer.extend_from_slice(&bytes[..count]);
        bytes = &bytes[count..];
        if bytes.is_empty() && !must_flush {
            return Ok(());
        }
        self.putting = true;
        self.flush()?;
        // "Try to maintain alignment: write a whole number of blocks."
        let direct = bytes.len()
            - if self.capacity >= 128 {
                bytes.len() % self.capacity
            } else {
                0
            };
        if direct > 0 {
            self.put(&bytes[..direct])?;
            bytes = &bytes[direct..];
        }
        self.default_xsputn(bytes)
    }

    /// glibc's `_IO_default_xsputn': the space before the stream's write
    /// end filled, then a character through `__overflow', and so on.  A
    /// line-buffered stream's write end is its write pointer, so every
    /// character of it goes through `__overflow'.
    fn default_xsputn(&mut self, mut bytes: &[u8]) -> std::io::Result<()> {
        loop {
            if !self.line_buffered {
                let space = self.capacity.saturating_sub(self.buffer.len());
                let count = space.min(bytes.len());
                self.buffer.extend_from_slice(&bytes[..count]);
                bytes = &bytes[count..];
            }
            let Some((byte, rest)) = bytes.split_first() else {
                return Ok(());
            };
            self.overflow(*byte)?;
            bytes = rest;
        }
    }

    /// glibc's `_IO_new_file_overflow' with a character, what putc comes
    /// to when the stream has no space before its write end: a full
    /// buffer is written, BYTE put, and a line-buffered stream's buffer
    /// written after a newline.
    fn overflow(&mut self, byte: u8) -> std::io::Result<()> {
        self.putting = true;
        if self.buffer.len() >= self.capacity {
            self.flush()?;
        }
        self.buffer.push(byte);
        if self.line_buffered && byte == b'\n' {
            self.flush()?;
        }
        Ok(())
    }

    /// BYTES to the stream's descriptor now (`new_do_write').
    fn put(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(written) = &mut self.written {
            written.push(bytes.to_vec());
            return Ok(());
        }
        let result = sys_write(self.descriptor, bytes);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// `_IO_do_write' of what the buffer holds, which is then empty
    /// whether or not the write succeeded.
    fn flush(&mut self) -> std::io::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let buffer = std::mem::take(&mut self.buffer);
        let result = self.put(&buffer);
        self.buffer = buffer;
        self.buffer.clear();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::{Stream, buffer_size};

    fn stream(capacity: usize, line_buffered: bool) -> Stream {
        let mut stream = Stream::new(-1, capacity, line_buffered);
        stream.written = Some(Vec::new());
        stream
    }

    fn writes(stream: &Stream) -> Vec<String> {
        stream
            .written
            .as_ref()
            .expect("a test's stream records its writes")
            .iter()
            .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
            .collect()
    }

    /// xdisp.c:message_to_stderr's errwrite of the text and errputc of
    /// the newline are one write of the line-buffered stream, as a C
    /// stdio stream set _IOLBF writes them.
    #[test]
    fn a_message_and_its_newline_are_one_write() {
        let mut stream = stream(4096, true);
        stream.xsputn(b"Undo").expect("recorded");
        assert!(writes(&stream).is_empty());
        stream.overflow(b'\n').expect("recorded");
        assert_eq!(writes(&stream), ["Undo\n"]);
        // The newline before a message after output to stdout is a line
        // of its own.
        stream.overflow(b'\n').expect("recorded");
        stream.xsputn(b"Mark set").expect("recorded");
        stream.overflow(b'\n').expect("recorded");
        assert_eq!(writes(&stream), ["Undo\n", "\n", "Mark set\n"]);
    }

    /// Once the stream is in put mode a text with newlines inside is
    /// written through its last one at once; what follows waits for the
    /// line's end or a flush.
    #[test]
    fn text_after_the_last_newline_waits() {
        let mut stream = stream(4096, true);
        stream.xsputn(b"first\n").expect("recorded");
        stream.xsputn(b"one\ntwo\nthree").expect("recorded");
        assert_eq!(writes(&stream), ["first\n", "one\ntwo\n"]);
        stream.flush().expect("recorded");
        assert_eq!(writes(&stream), ["first\n", "one\ntwo\n", "three"]);
        stream.flush().expect("recorded");
        assert_eq!(writes(&stream).len(), 3);
    }

    /// The stream's first write finds no buffer: nothing is filled,
    /// whole blocks go to the descriptor in one write and the rest is
    /// put a character at a time, a write at each newline.
    #[test]
    fn the_first_write_puts_its_characters_one_at_a_time() {
        let mut first = stream(4096, true);
        first.xsputn(b"two\nthree\nfour").expect("recorded");
        first.overflow(b'\n').expect("recorded");
        assert_eq!(writes(&first), ["two\n", "three\n", "four\n"]);

        let mut long = stream(4096, true);
        long.xsputn(&[b'a'; 10000]).expect("recorded");
        long.overflow(b'\n').expect("recorded");
        let written = writes(&long);
        assert_eq!(
            written.iter().map(String::len).collect::<Vec<_>>(),
            [8192, 1809]
        );

        // A stream that is not line-buffered buffers the rest.
        let mut block = stream(4096, false);
        block.xsputn(&[b'a'; 10000]).expect("recorded");
        assert_eq!(
            writes(&block).iter().map(String::len).collect::<Vec<_>>(),
            [8192]
        );
        assert_eq!(block.buffer.len(), 1808);
        block.xsputn(&[b'b'; 3000]).expect("recorded");
        let written = writes(&block);
        assert_eq!(
            written.iter().map(String::len).collect::<Vec<_>>(),
            [8192, 4096]
        );
        assert!(written[1].ends_with(&"b".repeat(2288)));
        assert_eq!(block.buffer.len(), 712);
    }

    /// Bytes that do not fit the buffer's free space: the buffer filled
    /// and written, whole blocks of the rest written directly, the tail
    /// put a character at a time with a write at each newline
    /// (glibc's _IO_new_file_xsputn and _IO_default_xsputn).
    #[test]
    fn bytes_past_the_buffer_follow_stdio() {
        let mut stream = stream(128, true);
        stream.xsputn(&[b'a'; 100]).expect("recorded");
        assert!(writes(&stream).is_empty());
        assert_eq!(stream.buffer.len(), 100);
        let mut text = vec![b'b'; 28 + 128];
        text.extend_from_slice(b"cc\ndd\nee");
        stream.xsputn(&text).expect("recorded");
        let written = writes(&stream);
        assert_eq!(written.len(), 4);
        assert_eq!(written[0].len(), 128);
        assert!(written[0].starts_with(&"a".repeat(100)) && written[0].ends_with(&"b".repeat(28)));
        assert_eq!(written[1], "b".repeat(128));
        assert_eq!(written[2], "cc\n");
        assert_eq!(written[3], "dd\n");
        assert_eq!(stream.buffer, b"ee");
    }

    /// _IO_file_doallocate takes the descriptor's block size only when
    /// it is below BUFSIZ.
    #[test]
    fn a_buffer_is_no_larger_than_bufsiz() {
        assert_eq!(buffer_size(4096), 4096);
        assert_eq!(buffer_size(1024), 1024);
        assert_eq!(buffer_size(8192), 8192);
        assert_eq!(buffer_size(65536), 8192);
        assert_eq!(buffer_size(2 * 1024 * 1024), 8192);
        assert_eq!(buffer_size(0), 8192);
        assert_eq!(buffer_size(u64::MAX), 8192);
    }

    /// A failed write empties the buffer, drops the rest of that call's
    /// bytes and is remembered; the stream takes later bytes as before.
    #[test]
    fn a_failed_write_is_remembered_and_not_reported() {
        // Descriptor -1: every write fails with EBADF.
        let mut stream = Stream::new(-1, 128, true);
        assert!(stream.xsputn(b"lost\nkept").is_err());
        assert!(stream.failed);
        assert!(stream.buffer.is_empty());
        assert!(stream.xsputn(b"more").is_ok());
        assert_eq!(stream.buffer, b"more");
        assert!(stream.flush().is_err());
        assert!(stream.buffer.is_empty());
    }
}
