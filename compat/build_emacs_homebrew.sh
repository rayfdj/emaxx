#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Build a feature-rich Emacs from the sibling source tree using Homebrew paths.

Usage:
  compat/build_emacs_homebrew.sh [options]

Options:
  --repo PATH                Emacs source tree to build.
                             Default: /Users/alpha/CodexProjects/emacs
  --jobs N                   Parallel build jobs.
                             Default: detected logical CPU count.
  --configure-only           Run autogen/configure but skip the build.
  --clean                    Run `make distclean` first when possible.
  --without-imagemagick      Disable ImageMagick even if installed.
  --without-dbus             Disable D-Bus even if installed.
  --without-tree-sitter      Disable tree-sitter support.
  --native-comp TYPE         Native compilation mode.
                             Default: aot
  --help                     Show this help.

This script configures a Cocoa/NS build roughly matching a full Homebrew
feature surface, while also forcing the include/library paths needed for
libgccjit discovery on macOS.
EOF
}

detect_default_jobs() {
  sysctl -n hw.logicalcpu 2>/dev/null \
    || getconf _NPROCESSORS_ONLN 2>/dev/null \
    || printf '%s\n' 4
}

repo="/Users/alpha/CodexProjects/emacs"
jobs="$(detect_default_jobs)"
configure_only="no"
clean_first="no"
enable_imagemagick="yes"
enable_dbus="yes"
enable_tree_sitter="yes"
native_comp="aot"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="${2:?missing value for --repo}"
      shift 2
      ;;
    --jobs)
      jobs="${2:?missing value for --jobs}"
      shift 2
      ;;
    --configure-only)
      configure_only="yes"
      shift
      ;;
    --clean)
      clean_first="yes"
      shift
      ;;
    --without-imagemagick)
      enable_imagemagick="no"
      shift
      ;;
    --without-dbus)
      enable_dbus="no"
      shift
      ;;
    --without-tree-sitter)
      enable_tree_sitter="no"
      shift
      ;;
    --native-comp)
      native_comp="${2:?missing value for --native-comp}"
      shift 2
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -d "$repo" ]]; then
  echo "Emacs repo not found: $repo" >&2
  exit 1
fi

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

need_formula() {
  local prefix
  prefix="$(brew --prefix "$1" 2>/dev/null || true)"
  if [[ -z "$prefix" || ! -d "$prefix" ]]; then
    echo "Missing required Homebrew formula: $1" >&2
    exit 1
  fi
}

choose_make() {
  if command -v gmake >/dev/null 2>&1; then
    printf '%s\n' "gmake"
    return
  fi

  if command -v make >/dev/null 2>&1 && make --version 2>/dev/null | grep -q "GNU Make"; then
    printf '%s\n' "make"
    return
  fi

  echo "Need GNU make (`gmake` or GNU `make`) to build Emacs on this machine." >&2
  exit 1
}

pkgconfig_path_for() {
  local formula="$1"
  local prefix
  prefix="$(brew --prefix "$formula" 2>/dev/null || true)"
  [[ -n "$prefix" ]] || return 0

  if [[ -d "$prefix/lib/pkgconfig" ]]; then
    printf '%s\n' "$prefix/lib/pkgconfig"
  fi
  if [[ -d "$prefix/share/pkgconfig" ]]; then
    printf '%s\n' "$prefix/share/pkgconfig"
  fi
}

append_unique_path() {
  local var_name="$1"
  local path="$2"
  [[ -n "$path" ]] || return 0
  if [[ -z "${!var_name:-}" ]]; then
    printf -v "$var_name" '%s' "$path"
  elif [[ ":${!var_name}:" != *":$path:"* ]]; then
    printf -v "$var_name" '%s:%s' "$path" "${!var_name}"
  fi
}

need_cmd brew
need_cmd pkg-config
need_formula gcc
need_formula libgccjit
need_formula sqlite
need_formula libxml2
need_formula gnutls
need_formula librsvg
need_formula webp

if [[ "$enable_tree_sitter" == "yes" ]]; then
  if [[ -d "$(brew --prefix tree-sitter@0.25 2>/dev/null || true)" ]]; then
    tree_sitter_formula="tree-sitter@0.25"
  elif [[ -d "$(brew --prefix tree-sitter 2>/dev/null || true)" ]]; then
    tree_sitter_formula="tree-sitter"
  else
    echo "Missing required Homebrew formula: tree-sitter or tree-sitter@0.25" >&2
    exit 1
  fi
fi
if [[ "$enable_imagemagick" == "yes" ]]; then
  need_formula imagemagick
fi
if [[ "$enable_dbus" == "yes" ]]; then
  need_formula dbus
fi

make_cmd="$(choose_make)"

brew_prefix="$(brew --prefix)"
gcc_prefix="$(brew --prefix gcc)"
libgccjit_prefix="$(brew --prefix libgccjit)"
sqlite_prefix="$(brew --prefix sqlite)"
libxml2_prefix="$(brew --prefix libxml2)"

gcc_version="$(brew list --versions gcc | awk 'NR==1 {print $2}')"
gcc_major="${gcc_version%%.*}"
gcc_lib_dir="${brew_prefix}/lib/gcc/${gcc_major}"

if [[ ! -d "$gcc_lib_dir" ]]; then
  echo "Expected GCC library directory not found: $gcc_lib_dir" >&2
  exit 1
fi

pkg_config_path=""
for formula in \
  sqlite \
  libxml2 \
  gnutls \
  librsvg \
  webp \
  "${tree_sitter_formula:-tree-sitter}" \
  imagemagick \
  dbus
do
  if [[ "$formula" == "${tree_sitter_formula:-tree-sitter}" && "$enable_tree_sitter" != "yes" ]]; then
    continue
  fi
  if [[ "$formula" == "imagemagick" && "$enable_imagemagick" != "yes" ]]; then
    continue
  fi
  if [[ "$formula" == "dbus" && "$enable_dbus" != "yes" ]]; then
    continue
  fi
  while IFS= read -r path; do
    append_unique_path pkg_config_path "$path"
  done < <(pkgconfig_path_for "$formula")
done

export PKG_CONFIG_PATH="${pkg_config_path}${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
export CPPFLAGS="-I${sqlite_prefix}/include -I${gcc_prefix}/include -I${libgccjit_prefix}/include ${CPPFLAGS:-}"
export CFLAGS="-DFD_SETSIZE=10000 -DDARWIN_UNLIMITED_SELECT -I${sqlite_prefix}/include -I${gcc_prefix}/include -I${libgccjit_prefix}/include ${CFLAGS:-}"
export LDFLAGS="-L${sqlite_prefix}/lib -L${gcc_lib_dir} -Wl,-rpath,${gcc_lib_dir} ${LDFLAGS:-}"

configure_args=(
  "--with-native-compilation=${native_comp}"
  "--with-xml2"
  "--with-gnutls"
  "--with-modules"
  "--with-rsvg"
  "--with-webp"
  "--with-ns"
  "--disable-ns-self-contained"
)

if [[ "$enable_tree_sitter" == "yes" ]]; then
  configure_args+=("--with-tree-sitter")
fi
if [[ "$enable_imagemagick" == "yes" ]]; then
  configure_args+=("--with-imagemagick")
fi
if [[ "$enable_dbus" == "yes" ]]; then
  configure_args+=("--with-dbus")
else
  configure_args+=("--without-dbus")
fi

echo "Repo:               $repo"
echo "Make command:       $make_cmd"
echo "Jobs:               $jobs"
echo "Native compilation: $native_comp"
echo "ImageMagick:        $enable_imagemagick"
echo "D-Bus:              $enable_dbus"
echo "Tree-sitter:        $enable_tree_sitter"
echo "GCC version:        $gcc_version"
echo "GCC library dir:    $gcc_lib_dir"
echo "PKG_CONFIG_PATH:    $PKG_CONFIG_PATH"
echo

cd "$repo"

if [[ "$clean_first" == "yes" && -f Makefile ]]; then
  "$make_cmd" distclean || true
fi

./autogen.sh
./configure "${configure_args[@]}"

echo
echo "Configured Emacs successfully."

if [[ "$configure_only" == "yes" ]]; then
  echo "Skipping build because --configure-only was requested."
  exit 0
fi

"$make_cmd" -j"$jobs"

echo
echo "Build complete."
echo "Oracle binary: $repo/src/emacs"
echo "Repin the compatibility harness with:"
echo "  cargo run --bin compat-harness -- oracle pin --emacs $repo/src/emacs --repo $repo"
