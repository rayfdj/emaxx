//! File status and reads through the host's ordinary POSIX operations.
//!
//! GNU uses stat/lstat/fstat for these operations. Rust's Linux metadata
//! implementation also requests birth times through statx, which GNU's
//! seccomp policy does not permit. Keep that extra query out of the runtime.

pub(crate) use std::fs::*;

#[cfg(unix)]
pub(crate) use posix::{
    Metadata, file_metadata, metadata, read, read_open_file, read_to_string, symlink_metadata,
};

#[cfg(not(unix))]
pub(crate) fn file_metadata(file: &File) -> std::io::Result<Metadata> {
    file.metadata()
}

#[cfg(not(unix))]
pub(crate) fn read_open_file(file: &mut File) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

pub(crate) fn is_directory(path: impl AsRef<std::path::Path>) -> bool {
    metadata(path).is_ok_and(|status| status.is_dir())
}

#[cfg(unix)]
mod posix {
    use std::ffi::CString;
    use std::fs::{File, Permissions};
    use std::io::{self, Read};
    use std::mem::MaybeUninit;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    use std::path::Path;
    use std::time::{Duration, SystemTime};

    #[derive(Clone)]
    pub(crate) struct Metadata(libc::stat);

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) struct FileType(libc::mode_t);

    impl FileType {
        pub(crate) fn is_dir(self) -> bool {
            self.0 & libc::S_IFMT == libc::S_IFDIR
        }

        pub(crate) fn is_file(self) -> bool {
            self.0 & libc::S_IFMT == libc::S_IFREG
        }

        pub(crate) fn is_symlink(self) -> bool {
            self.0 & libc::S_IFMT == libc::S_IFLNK
        }
    }

    impl Metadata {
        pub(crate) fn file_type(&self) -> FileType {
            FileType(self.0.st_mode)
        }

        pub(crate) fn is_dir(&self) -> bool {
            self.file_type().is_dir()
        }

        pub(crate) fn is_file(&self) -> bool {
            self.file_type().is_file()
        }

        pub(crate) fn len(&self) -> u64 {
            self.size()
        }

        pub(crate) fn permissions(&self) -> Permissions {
            Permissions::from_mode(self.mode())
        }

        pub(crate) fn modified(&self) -> io::Result<SystemTime> {
            system_time(self.mtime(), self.mtime_nsec())
        }

        pub(crate) fn accessed(&self) -> io::Result<SystemTime> {
            system_time(self.atime(), self.atime_nsec())
        }
    }

    fn system_time(seconds: i64, nanoseconds: i64) -> io::Result<SystemTime> {
        let base = if seconds >= 0 {
            SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(seconds as u64))
        } else {
            SystemTime::UNIX_EPOCH.checked_sub(Duration::from_secs(seconds.unsigned_abs()))
        };
        let nanoseconds = u32::try_from(nanoseconds)
            .ok()
            .filter(|nanoseconds| *nanoseconds < 1_000_000_000);
        base.zip(nanoseconds)
            .and_then(|(base, nanos)| base.checked_add(Duration::from_nanos(nanos.into())))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid file timestamp"))
    }

    // libc's field widths differ between supported Unix targets; the trait
    // specifies fixed-width Rust results on all of them.
    #[allow(clippy::unnecessary_cast)]
    impl MetadataExt for Metadata {
        fn dev(&self) -> u64 {
            self.0.st_dev as u64
        }
        fn ino(&self) -> u64 {
            self.0.st_ino as u64
        }
        fn mode(&self) -> u32 {
            self.0.st_mode as u32
        }
        fn nlink(&self) -> u64 {
            self.0.st_nlink as u64
        }
        fn uid(&self) -> u32 {
            self.0.st_uid as u32
        }
        fn gid(&self) -> u32 {
            self.0.st_gid as u32
        }
        fn rdev(&self) -> u64 {
            self.0.st_rdev as u64
        }
        fn size(&self) -> u64 {
            self.0.st_size as u64
        }
        fn atime(&self) -> i64 {
            self.0.st_atime as i64
        }
        fn atime_nsec(&self) -> i64 {
            self.0.st_atime_nsec as i64
        }
        fn mtime(&self) -> i64 {
            self.0.st_mtime as i64
        }
        fn mtime_nsec(&self) -> i64 {
            self.0.st_mtime_nsec as i64
        }
        fn ctime(&self) -> i64 {
            self.0.st_ctime as i64
        }
        fn ctime_nsec(&self) -> i64 {
            self.0.st_ctime_nsec as i64
        }
        fn blksize(&self) -> u64 {
            self.0.st_blksize as u64
        }
        fn blocks(&self) -> u64 {
            self.0.st_blocks as u64
        }
    }

    fn file_status(path: &Path, follow: bool) -> io::Result<Metadata> {
        let path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file name contains NUL"))?;
        let mut status = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: the path is NUL terminated and status is writable. A
        // successful POSIX call initializes the complete status structure.
        let result = unsafe {
            if follow {
                libc::stat(path.as_ptr(), status.as_mut_ptr())
            } else {
                libc::lstat(path.as_ptr(), status.as_mut_ptr())
            }
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Metadata(unsafe { status.assume_init() }))
    }

    pub(crate) fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
        file_status(path.as_ref(), true)
    }

    pub(crate) fn symlink_metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
        file_status(path.as_ref(), false)
    }

    pub(crate) fn file_metadata(file: &File) -> io::Result<Metadata> {
        let mut status = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: File owns the live descriptor; fstat initializes status
        // on success without looking the path up again.
        if unsafe { libc::fstat(file.as_raw_fd(), status.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Metadata(unsafe { status.assume_init() }))
    }

    pub(crate) fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
        read_open_file(&mut File::open(path)?)
    }

    pub(crate) fn read_open_file(file: &mut File) -> io::Result<Vec<u8>> {
        let capacity = file_metadata(file)
            .ok()
            .filter(Metadata::is_file)
            .and_then(|status| usize::try_from(status.len()).ok())
            .unwrap_or(0);
        let mut bytes = Vec::new();
        bytes
            .try_reserve(capacity)
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        // File::read_to_end also requests Rust's extended metadata. Read
        // the real descriptor directly, including streams of unknown size.
        let mut buffer = [0; 8192];
        loop {
            match file.read(&mut buffer) {
                Ok(0) => return Ok(bytes),
                Ok(count) => bytes.extend_from_slice(&buffer[..count]),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }

    pub(crate) fn read_to_string(path: impl AsRef<Path>) -> io::Result<String> {
        String::from_utf8(read(path)?).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "stream did not contain valid UTF-8",
            )
        })
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::Write;
        use std::os::unix::ffi::OsStringExt;
        use std::os::unix::fs::symlink;

        struct TestDirectory(std::path::PathBuf);

        impl TestDirectory {
            fn new() -> Self {
                let template = std::env::temp_dir().join("emaxx-posix-XXXXXX");
                let mut bytes = template.into_os_string().into_vec();
                bytes.push(0);
                // SAFETY: mkdtemp writes within the mutable, terminated
                // template and creates a private directory atomically.
                assert!(!unsafe { libc::mkdtemp(bytes.as_mut_ptr().cast()) }.is_null());
                bytes.pop();
                Self(std::ffi::OsString::from_vec(bytes).into())
            }

            fn path(&self) -> &Path {
                &self.0
            }
        }

        impl Drop for TestDirectory {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        #[test]
        fn posix_metadata_preserves_host_fields_symlinks_and_errors() {
            let directory = TestDirectory::new();
            let path = directory.path().join("file-λ");
            std::fs::write(&path, b"contents").expect("actual file");
            let actual = metadata(&path).expect("POSIX status");
            let expected = std::fs::metadata(&path).expect("host status");
            assert_eq!(
                (actual.dev(), actual.ino(), actual.len()),
                (expected.dev(), expected.ino(), expected.len())
            );
            assert_eq!(
                (actual.uid(), actual.gid(), actual.nlink()),
                (expected.uid(), expected.gid(), expected.nlink())
            );
            assert_eq!(actual.permissions().mode(), expected.permissions().mode());
            assert_eq!(
                actual.modified().expect("mtime"),
                expected.modified().expect("host mtime")
            );
            assert_eq!(
                actual.accessed().expect("atime"),
                expected.accessed().expect("host atime")
            );
            assert_eq!(
                (actual.ctime(), actual.ctime_nsec()),
                (expected.ctime(), expected.ctime_nsec())
            );
            let link = directory.path().join("link");
            symlink(&path, &link).expect("symlink");
            assert!(
                symlink_metadata(&link)
                    .expect("lstat")
                    .file_type()
                    .is_symlink()
            );
            assert!(metadata(&link).expect("stat follows link").is_file());
            assert!(metadata(directory.path()).expect("directory").is_dir());
            for missing in [directory.path().join("absent"), path.join("child")] {
                assert_eq!(
                    metadata(&missing)
                        .err()
                        .expect("status fails")
                        .raw_os_error(),
                    std::fs::metadata(&missing)
                        .expect_err("host status fails")
                        .raw_os_error()
                );
            }
            assert_eq!(
                metadata("nul\0name").err().expect("reject NUL").kind(),
                io::ErrorKind::InvalidInput
            );
            // macOS rejects non-UTF-8 names with EILSEQ. Linux accepts
            // them; preserve those actual filename bytes on that platform.
            #[cfg(target_os = "linux")]
            {
                let raw = directory
                    .path()
                    .join(std::ffi::OsString::from_vec(b"file-\xff".to_vec()));
                std::fs::write(&raw, b"raw bytes").expect("Linux filename");
                assert_eq!(read(&raw).expect("read raw filename"), b"raw bytes");
                assert_eq!(metadata(raw).expect("raw filename status").len(), 9);
            }
        }

        #[test]
        fn posix_descriptor_status_keeps_the_open_inode_after_path_replacement() {
            let directory = TestDirectory::new();
            let path = directory.path().join("file");
            std::fs::write(&path, b"original").expect("original file");
            let file = File::open(&path).expect("open original inode");
            let inode = file_metadata(&file).expect("descriptor status").ino();
            std::fs::remove_file(&path).expect("unlink original");
            std::fs::write(&path, b"new").expect("replacement inode");
            let status = file_metadata(&file).expect("unlinked descriptor remains valid");
            assert_eq!((status.ino(), status.len()), (inode, 8));
            assert_ne!(metadata(&path).expect("replacement status").ino(), inode);
        }

        #[test]
        fn posix_reads_handle_binary_data_and_streams_without_a_file_size() {
            let directory = TestDirectory::new();
            let path = directory.path().join("binary");
            let data = (0..30_000)
                .map(|value| (value % 256) as u8)
                .collect::<Vec<_>>();
            std::fs::write(&path, &data).expect("binary file");
            assert_eq!(read(&path).expect("complete read"), data);
            assert_eq!(
                read_to_string(&path).expect_err("invalid UTF-8").kind(),
                io::ErrorKind::InvalidData
            );
            let fifo = directory.path().join("fifo");
            let name = CString::new(fifo.as_os_str().as_bytes()).expect("FIFO path");
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
            let writer_path = fifo.clone();
            let writer = std::thread::spawn(move || {
                let mut stream = std::fs::OpenOptions::new()
                    .write(true)
                    .open(writer_path)
                    .expect("open FIFO writer");
                stream
                    .write_all(b"stream has no regular-file size")
                    .expect("write stream");
            });
            assert_eq!(
                read_to_string(fifo).expect("streaming read"),
                "stream has no regular-file size"
            );
            writer.join().expect("writer completed");
        }

        #[test]
        fn posix_timestamps_preserve_fractional_seconds_before_the_epoch() {
            assert_eq!(
                system_time(-1, 500_000_000).expect("negative timestamp"),
                SystemTime::UNIX_EPOCH - Duration::from_millis(500)
            );
            assert_eq!(
                system_time(1, 7).expect("positive timestamp"),
                SystemTime::UNIX_EPOCH + Duration::new(1, 7)
            );
            assert!(system_time(0, -1).is_err());
            assert!(system_time(0, 1_000_000_000).is_err());
        }
    }
}
