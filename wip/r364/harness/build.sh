#!/bin/sh
# build.sh LOG: the release binary and its dumped image from /home/user/emaxx-wt.
L=${1:-/home/user/emaxx-work/env/build.log}
cd /home/user/emaxx-wt
( CARGO_TARGET_DIR=/home/user/emaxx/target/prof nice cargo build --release --locked && EMAXX_IMAGE_FORCE=1 sh tools/build-image.sh /home/user/emaxx/target/prof/release/emaxx ) > $L 2>&1
echo "exit $?" >> $L
