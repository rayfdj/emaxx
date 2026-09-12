#![deny(clippy::unwrap_used)]
//! lib-src/make-fingerprint.c: compute the SHA-256 of FILE and replace,
//! in place, every occurrence of lib/fingerprint.c's default fingerprint
//! in it with that digest; with `-r', print the digest instead.  GNU's
//! Makefile runs it on the linked temacs; tools/build-image.sh runs it on
//! the Emaxx binary before the image is dumped, so that a process reads
//! its fingerprint instead of hashing its own executable at every start.

use std::io::{Read, Seek, SeekFrom, Write};
use std::process::ExitCode;

/// lib/fingerprint.c's default `fingerprint'.
const DEFAULT_FINGERPRINT: [u8; 32] = [
    0xDE, 0x86, 0xBB, 0x99, 0xFF, 0xF5, 0x46, 0x9A, 0x9E, 0x3F, 0x9F, 0x5D, 0x9A, 0xDF, 0xF0, 0x91,
    0xBD, 0xCD, 0xC1, 0xE8, 0x0C, 0x16, 0x1E, 0xAF, 0xB8, 0x6C, 0xE2, 0x2B, 0xB1, 0x24, 0xCE, 0xB0,
];

fn main() -> ExitCode {
    let prog = std::env::args()
        .next()
        .unwrap_or_else(|| "make-fingerprint".into());
    let mut raw = false;
    let mut files = Vec::new();
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "-r" => raw = true,
            "-h" => {
                println!("make-fingerprint [-r] FILE: replace or compute a hash");
                return ExitCode::SUCCESS;
            }
            _ => files.push(arg),
        }
    }
    let [file] = files.as_slice() else {
        eprintln!("{prog}: missing or extra file operand");
        return ExitCode::FAILURE;
    };
    let mut handle = match std::fs::OpenOptions::new()
        .read(true)
        .write(!raw)
        .open(file)
    {
        Ok(handle) => handle,
        Err(error) => {
            eprintln!("{file}: {error}");
            return ExitCode::FAILURE;
        }
    };
    match handle.metadata() {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => {
            eprintln!("{prog}: Error: {file} is not a regular file");
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("{file}: {error}");
            return ExitCode::FAILURE;
        }
    }
    let mut buf = Vec::new();
    if handle.read_to_end(&mut buf).is_err() {
        eprintln!("{prog}: Error: could not read {file}");
        return ExitCode::FAILURE;
    }
    let digest: [u8; 32] = {
        use sha2::Digest;
        sha2::Sha256::digest(&buf).into()
    };
    if raw {
        let hex: String = digest.iter().map(|byte| format!("{byte:02X}")).collect();
        println!("{hex}");
        return ExitCode::SUCCESS;
    }
    let mut fingered = false;
    let mut from = 0;
    while let Some(offset) = buf[from..]
        .windows(DEFAULT_FINGERPRINT.len())
        .position(|window| window == DEFAULT_FINGERPRINT)
    {
        let finger = from + offset;
        if handle.seek(SeekFrom::Start(finger as u64)).is_err()
            || handle.write_all(&digest).is_err()
        {
            eprintln!("{file}: {}", std::io::Error::last_os_error());
            return ExitCode::FAILURE;
        }
        fingered = true;
        from = finger + 1;
    }
    if !fingered {
        eprintln!("{prog}: {file}: missing fingerprint");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
