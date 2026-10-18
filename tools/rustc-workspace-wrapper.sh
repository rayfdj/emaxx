#!/bin/sh
# Cargo's wrapper for this package's own rustc runs (.cargo/config.toml's
# `build.rustc-workspace-wrapper'): `wrapper RUSTC ARGS...'.
#
# On x86-64 Linux the package's crates are compiled for a position-
# dependent executable (`-C relocation-model=static').  A jump table in
# position-independent code holds offsets from the table, so each
# dispatch of the byte-code loop loaded an offset, added the table's
# address and jumped: three instructions after the opcode's load.  With
# absolute entries it is one, `jmp *TABLE(,%rax,8)' (bytecode calls 491
# -> 454 instructions an iteration), and the table's address no longer
# holds a register in the loop.  Dependencies keep the default: the
# procedural macros among them are shared objects, which code compiled
# this way cannot be linked into, and Cargo has no stable setting for
# host or per-package flags.
#
# Elsewhere, for another target, and for rustc's queries (no crate),
# rustc runs as given.
rustc=$1
shift
static=
case $(uname -sm) in
"Linux x86_64") static=yes ;;
esac
crate=
previous=
for argument; do
    case $previous in
    --target) [ "$argument" = x86_64-unknown-linux-gnu ] || static= ;;
    esac
    case $argument in
    --target=*) [ "$argument" = --target=x86_64-unknown-linux-gnu ] || static= ;;
    --crate-name) crate=yes ;;
    esac
    previous=$argument
done
if [ -n "$static" ] && [ -n "$crate" ]; then
    exec "$rustc" "$@" -C relocation-model=static
fi
exec "$rustc" "$@"
