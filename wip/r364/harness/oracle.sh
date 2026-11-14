#!/bin/sh
# Rebuild the environment's oracle: CI's prerequisites, GNU Emacs at the
# pinned commit, configured as the native ABI records.
set -x
export DEBIAN_FRONTEND=noninteractive
apt-get update > /home/user/emaxx-work/env/apt.log 2>&1
apt-get install -y build-essential pkg-config texinfo autoconf automake time \
  libgnutls28-dev gnutls-bin gdb libncurses-dev libgccjit-13-dev libgmp-dev libxml2-dev \
  libsqlite3-dev zlib1g-dev libtree-sitter-dev libx11-dev libxpm-dev \
  libjpeg-dev libgif-dev libtiff-dev libpng-dev libxft-dev libxrender-dev \
  libxt-dev liblcms2-dev libharfbuzz-dev libcairo2-dev librsvg2-dev \
  libwebp-dev libseccomp-dev libasound2-dev libacl1-dev libselinux1-dev clangd bubblewrap strace mercurial >> /home/user/emaxx-work/env/apt.log 2>&1
echo "apt rc=$?"
mkdir -p /home/user/emacs-mirror/emacs && cd /home/user/emacs-mirror/emacs
if [ ! -d .git ]; then
  git init -q . && git remote add origin https://github.com/emacs-mirror/emacs.git
fi
git fetch -q --depth 1 origin 636f166cfc86aa90d63f592fd99f3fdd9ef95ebd && git checkout -q FETCH_HEAD
echo "fetch rc=$?"
./autogen.sh > /home/user/emaxx-work/env/autogen.log 2>&1
./configure --with-native-compilation --with-x --with-x-toolkit=no --with-tree-sitter --without-imagemagick --with-lcms2 --with-harfbuzz --without-libotf --without-m17n-flt > /home/user/emaxx-work/env/configure.log 2>&1
echo "configure rc=$?"
make -j4 > /home/user/emaxx-work/env/make.log 2>&1
echo "make rc=$?"
ls -la src/emacs
echo ORACLE-DONE
