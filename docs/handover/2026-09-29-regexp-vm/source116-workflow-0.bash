sudo apt-get update
sudo apt-get install -y build-essential pkg-config texinfo autoconf automake time \
  libgnutls28-dev gnutls-bin gdb libncurses-dev libgccjit-13-dev libgmp-dev libxml2-dev \
  libsqlite3-dev zlib1g-dev libtree-sitter-dev libx11-dev libxpm-dev \
  libjpeg-dev libgif-dev libtiff-dev libpng-dev libxft-dev libxrender-dev \
  libxt-dev liblcms2-dev libharfbuzz-dev libcairo2-dev librsvg2-dev \
  libwebp-dev libseccomp-dev libasound2-dev libacl1-dev libselinux1-dev clangd bubblewrap strace mercurial
rustup toolchain install 1.97.1 --profile minimal --component rustfmt --component clippy
rustup toolchain install 1.75.0 --profile minimal --component rust-analyzer --component rust-src
rustup default 1.97.1
