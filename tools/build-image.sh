#!/bin/sh
# Build the dumped image beside an Emaxx binary the way GNU's src/Makefile.in
# builds emacs.pdmp beside emacs:
#
#   emacs$(EXEEXT): temacs$(EXEEXT) ...
#           rm -f $@ && cp -f temacs$(EXEEXT) $@
#   $(pdmp): emacs$(EXEEXT) ...
#           LC_ALL=C $(RUN_TEMACS) -batch $(BUILD_DETAILS) -l loadup --temacs=pdump \
#                   --bin-dest $(BIN_DESTDIR) --eln-dest $(ELN_DESTDIR)
#           cp -f $@ $(bootstrap_pdmp)
#
# The uninitialized process runs the unchanged loadup.el as its top-level
# form; loadup.el reads --bin-dest and --eln-dest from `command-line-args',
# dumps `emacs.pdmp' into `invocation-directory' and adds the versioned
# names beside the `emacs' binary it expects there.  The image is then
# copied to the name the Emaxx binary looks for: `<binary>.pdmp'.
#
# Usage: tools/build-image.sh [BINARY]     (default: target/release/emaxx)
# Environment: EMAXX_DUMP_SOURCE_DIRECTORY names the GNU source tree whose
# lisp/ and native-lisp/ the image is built from (default: ../emacs beside
# this repository); BUILD_DETAILS may be --no-build-details, as in make.
set -eu
binary=${1:-target/release/emaxx}
root=$(cd "$(dirname "$0")/.." && pwd)
case $binary in /*) ;; *) binary=$root/$binary ;; esac
dir=$(cd "$(dirname "$binary")" && pwd)
name=$(basename "$binary")
source=${EMAXX_DUMP_SOURCE_DIRECTORY:-$root/../emacs}
source=$(cd "$source" && pwd)

# An image that already loads into this very binary is up to date (a
# rebuilt binary has a new fingerprint, which the loader refuses).
if [ "${EMAXX_IMAGE_FORCE:-}" = "" ] && "$binary" --fingerprint >/dev/null 2>&1; then
    exit 0
fi

cd "$dir"
rm -f emacs && cp -f "$name" emacs
LC_ALL=C ./emacs -batch ${BUILD_DETAILS:-} -l loadup --temacs=pdump \
    --bin-dest "$dir/" --eln-dest "$source/"
[ "$name" = emacs ] || cp -f emacs.pdmp "$name.pdmp"
