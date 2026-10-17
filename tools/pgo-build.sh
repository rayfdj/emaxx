#!/bin/sh
# Build a profile-guided Emaxx release binary and its image, for timing.
#
#   tools/pgo-build.sh [TARGET_DIR]        (default: target/pgo)
#
# Three steps, each from this source tree:
#   1. an instrumented release build (`-Cprofile-generate') and its image;
#   2. a training run: GNU's own test files under ERT, in batch, one
#      process each;
#   3. the release build again with the merged profile (`-Cprofile-use')
#      and its image.  The binary is TARGET_DIR/release/emaxx.
#
# The training set is GNU's tests, not the timed workloads: none of the
# files the locked core workloads load (compat/core_runtime_perf.el) is in
# it -- undo-tests.el, sort-tests.el and bindat-tests.el, nor comp-tests.el
# and its native resources -- and the core workloads themselves are never
# run.  The profile is made afresh for every build from the source being
# built, so it is never stale, and nothing of it is kept in the tree.
#
# The flags are added through `--config' so that they join the ones in
# .cargo/config.toml (an RUSTFLAGS setting would replace those).  A
# function whose profile hash differs is only reported for build scripts
# and the fingerprint helper, whose `main' meets the editor's own `main'
# in the profile; the warning is turned off for that reason.
#
# Requires the toolchain's llvm-tools component (`rustup component add
# llvm-tools') for llvm-profdata, and the GNU source tree named by
# EMAXX_DUMP_SOURCE_DIRECTORY (default ../emacs), as tools/build-image.sh.
set -eu
root=$(cd "$(dirname "$0")/.." && pwd)
target=${1:-target/pgo}
case $target in /*) ;; *) target=$root/$target ;; esac
source=${EMAXX_DUMP_SOURCE_DIRECTORY:-$root/../emacs}
source=$(cd "$source" && pwd)
host=$(rustc -vV | sed -n 's/^host: //p')
profdata=$(rustc --print sysroot)/lib/rustlib/$host/bin/llvm-profdata
if [ ! -x "$profdata" ]; then
    echo "pgo-build: $profdata missing; run: rustup component add llvm-tools" >&2
    exit 1
fi

raw=$target/profile-raw
work=$target/training
rm -rf "$raw" "$work"
mkdir -p "$raw" "$work/home"

build() {
    # $1: the extra rustc flag; $2: the target directory.
    (cd "$root" && cargo build --locked --release --all-features \
        --config "build.rustflags=[\"$1\", \"-Cllvm-args=-no-pgo-warn-mismatch\"]" \
        --target-dir "$2" --bin emaxx --bin make-fingerprint)
    rm -f "$2"/release/emacs-30.2.*.pdmp
    EMAXX_IMAGE_FORCE=1 sh "$root/tools/build-image.sh" "$2/release/emaxx"
}

build "-Cprofile-generate=$raw" "$target/instrumented"
# The build's own processes (build scripts, the image dump) are not
# training.
rm -f "$raw"/*.profraw

files=
for file in "$source"/test/src/*-tests.el; do
    case $(basename "$file") in
        undo-tests.el | comp-tests.el) continue ;;
    esac
    files="$files $file"
done
for file in subr-tests.el simple-tests.el files-tests.el replace-tests.el \
    thingatpt-tests.el abbrev-tests.el minibuffer-tests.el json-tests.el \
    textmodes/fill-tests.el calc/calc-tests.el \
    emacs-lisp/cl-lib-tests.el emacs-lisp/cl-macs-tests.el \
    emacs-lisp/cl-generic-tests.el emacs-lisp/seq-tests.el \
    emacs-lisp/map-tests.el emacs-lisp/pcase-tests.el \
    emacs-lisp/subr-x-tests.el emacs-lisp/bytecomp-tests.el \
    emacs-lisp/rx-tests.el emacs-lisp/ring-tests.el \
    emacs-lisp/let-alist-tests.el emacs-lisp/nadvice-tests.el \
    emacs-lisp/generator-tests.el emacs-lisp/backquote-tests.el \
    emacs-lisp/macroexp-tests.el emacs-lisp/gv-tests.el; do
    if [ -f "$source/test/lisp/$file" ]; then
        files="$files $source/test/lisp/$file"
    fi
done
index=0
for file in $files; do
    index=$((index + 1))
    # A test's outcome does not matter here, only the code it runs.
    (cd "$work" && HOME=$work/home LANG=C LC_ALL=C TZ=UTC \
        EMACS_TEST_DIRECTORY=$source/test \
        timeout 300 "$target/instrumented/release/emaxx" -Q --batch \
        --eval "(setq native-comp-jit-compilation nil load-no-native t)" \
        -L "$source/test" -l ert -l "$file" -f ert-run-tests-batch-and-exit \
        >"$work/$index.log" 2>&1) || true
done

"$profdata" merge -o "$target/emaxx.profdata" "$raw"/*.profraw
build "-Cprofile-use=$target/emaxx.profdata" "$target"
echo "pgo-build: $target/release/emaxx"
