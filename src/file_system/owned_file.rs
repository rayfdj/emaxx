//! File ownership with the POSIX descriptor check allowed by GNU's sandbox.
//!
//! Keep std's open/read/write implementations and its I/O ownership contract.
//! Only destruction differs: F_GETFL checks that the descriptor is still open
//! without the F_GETFD query used by std's debug I/O-safety check. Both queries
//! inspect local descriptor state, avoiding spurious remote-filesystem EBADF.

use std::fs::{File as StdFile, OpenOptions as StdOpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::ManuallyDrop;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

#[derive(Debug)]
pub(crate) struct File(ManuallyDrop<StdFile>);

impl File {
    pub(crate) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        StdFile::open(path).map(Self::from)
    }

    pub(crate) fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        StdFile::create(path).map(Self::from)
    }

    #[cfg(test)]
    pub(crate) fn create_new(path: impl AsRef<Path>) -> io::Result<Self> {
        StdFile::create_new(path).map(Self::from)
    }

    pub(crate) fn options() -> OpenOptions {
        OpenOptions::new()
    }

    pub(crate) fn metadata(&self) -> io::Result<super::Metadata> {
        super::file_metadata(self)
    }

    pub(crate) fn try_clone(&self) -> io::Result<Self> {
        self.0.try_clone().map(Self::from)
    }

    fn into_std(self) -> StdFile {
        let mut this = ManuallyDrop::new(self);
        // SAFETY: ownership leaves this wrapper exactly once; neither its
        // destructor nor StdFile's destructor can run during this transfer.
        unsafe { ManuallyDrop::take(&mut this.0) }
    }
}

// Shared access provides std's remaining file operations, including set_times
// and sync_all. Do not provide DerefMut: replacing the inner owner would run
// std's destructor instead of ours.
impl std::ops::Deref for File {
    type Target = StdFile;

    fn deref(&self) -> &StdFile {
        &self.0
    }
}

impl From<StdFile> for File {
    fn from(file: StdFile) -> Self {
        Self(ManuallyDrop::new(file))
    }
}

impl From<OwnedFd> for File {
    fn from(fd: OwnedFd) -> Self {
        Self::from(StdFile::from(fd))
    }
}

impl From<File> for std::process::Stdio {
    fn from(file: File) -> Self {
        Self::from(file.into_std())
    }
}

impl AsRawFd for File {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl AsFd for File {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.0.as_fd()
    }
}

impl IntoRawFd for File {
    fn into_raw_fd(self) -> RawFd {
        self.into_std().into_raw_fd()
    }
}

impl FromRawFd for File {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        // SAFETY: the caller transfers the same live, uniquely owned
        // descriptor required by StdFile::from_raw_fd.
        Self::from(unsafe { StdFile::from_raw_fd(fd) })
    }
}

impl Drop for File {
    fn drop(&mut self) {
        // SAFETY: this is the sole owner. Transfer out of StdFile before
        // closing so neither destructor can close the descriptor again.
        let fd = unsafe { ManuallyDrop::take(&mut self.0) }.into_raw_fd();
        #[cfg(debug_assertions)]
        // SAFETY: querying descriptor flags does not access user memory.
        if unsafe { libc::fcntl(fd, libc::F_GETFL) } == -1
            && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
        {
            const MESSAGE: &[u8] = b"IO Safety violation: owned file descriptor already closed\n";
            // Even a broken/closed stderr must not turn this fatal ownership
            // violation into a recoverable panic from Rust's print macros.
            unsafe { libc::write(libc::STDERR_FILENO, MESSAGE.as_ptr().cast(), MESSAGE.len()) };
            std::process::abort();
        }
        // SAFETY: ownership was transferred above. Like std, ignore close
        // errors and do not retry EINTR, which could close a reused descriptor.
        unsafe { libc::close(fd) };
    }
}

impl Read for File {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.0.read(bytes)
    }

    fn read_vectored(&mut self, buffers: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.0.read_vectored(buffers)
    }
}

impl Read for &File {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        (&***self).read(bytes)
    }
}

impl Write for File {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(bytes)
    }

    fn write_vectored(&mut self, buffers: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.0.write_vectored(buffers)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Seek for File {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.0.seek(position)
    }
}

impl Write for &File {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        (&***self).write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&***self).flush()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OpenOptions(StdOpenOptions);

impl OpenOptions {
    pub(crate) fn new() -> Self {
        Self(StdOpenOptions::new())
    }

    pub(crate) fn read(&mut self, enabled: bool) -> &mut Self {
        self.0.read(enabled);
        self
    }

    pub(crate) fn write(&mut self, enabled: bool) -> &mut Self {
        self.0.write(enabled);
        self
    }

    pub(crate) fn append(&mut self, enabled: bool) -> &mut Self {
        self.0.append(enabled);
        self
    }

    pub(crate) fn truncate(&mut self, enabled: bool) -> &mut Self {
        self.0.truncate(enabled);
        self
    }

    pub(crate) fn create(&mut self, enabled: bool) -> &mut Self {
        self.0.create(enabled);
        self
    }

    pub(crate) fn create_new(&mut self, enabled: bool) -> &mut Self {
        self.0.create_new(enabled);
        self
    }

    pub(crate) fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        self.0.open(path).map(File::from)
    }
}

impl OpenOptionsExt for OpenOptions {
    fn mode(&mut self, mode: u32) -> &mut Self {
        self.0.mode(mode);
        self
    }

    fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.0.custom_flags(flags);
        self
    }
}
