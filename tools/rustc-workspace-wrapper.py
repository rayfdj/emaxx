#!/usr/bin/env python3
"""Cargo's wrapper for this package's own rustc runs (.cargo/config.toml's
`build.rustc-workspace-wrapper'): `wrapper RUSTC ARGS...'.

On x86-64 Linux the package's crates are compiled for a position-dependent
executable (`-C relocation-model=static').  A jump table in
position-independent code holds offsets from the table, so each dispatch of
the byte-code loop loaded an offset, added the table's address and jumped:
three instructions after the opcode's load.  With absolute entries it is
one, `jmp *TABLE(,%rax,8)' (bytecode calls 491 -> 454 instructions an
iteration), and the table's address no longer holds a register in the loop.
Dependencies keep the default: the procedural macros among them are shared
objects, which code compiled this way cannot be linked into, and Cargo has
no stable setting for host or per-package flags.

Elsewhere, for another target, and for rustc's queries (no crate), rustc
runs as given.  The wrapper is no shell script: a POSIX shell drops the
environment variables whose names are no identifiers, among them the
`CARGO_BIN_EXE_make-fingerprint' an integration test reads, and exec here
passes the environment on as it came.
"""
import os
import platform
import sys

TARGET = "x86_64-unknown-linux-gnu"


def main():
    rustc, *arguments = sys.argv[1:]
    static = platform.system() == "Linux" and platform.machine() == "x86_64"
    for index, argument in enumerate(arguments):
        if argument == "--target":
            static = static and arguments[index + 1 : index + 2] == [TARGET]
        elif argument.startswith("--target="):
            static = static and argument == "--target=" + TARGET
    if static and "--crate-name" in arguments:
        arguments += ["-C", "relocation-model=static"]
    os.execvp(rustc, [rustc, *arguments])


if __name__ == "__main__":
    main()
