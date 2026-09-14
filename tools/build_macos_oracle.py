#!/usr/bin/env python3
"""Build the pinned GNU Mac oracle with Apple's libxml2 parser.

Usage: python3 tools/build_macos_oracle.py GNU_CHECKOUT DESTINATION [--jobs 2]
DESTINATION must not exist. This builds a candidate; it never changes the
committed oracle pin or native ABI. Review and regenerate those explicitly.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys


REVISION = "636f166cfc86aa90d63f592fd99f3fdd9ef95ebd"


def output(command, **kwargs):
    return subprocess.check_output(command, text=True, **kwargs).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checkout", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    if sys.platform != "darwin" or os.uname().machine != "arm64" or args.jobs < 1:
        parser.error("this native ABI preset requires Apple silicon macOS and a positive job count")
    source = args.checkout.resolve(strict=True)
    destination = args.destination.resolve()
    if destination.exists():
        parser.error("destination already exists; preserve the existing build")
    project = Path(__file__).resolve().parent.parent
    abi = project / "src/lisp/native_comp/generated_native_subrs_aarch64_apple_darwin.rs"
    match = re.search(
        r'NATIVE_ABI_SYSTEM_CONFIGURATION_OPTIONS: &str = ("[^"\n]*");',
        abi.read_text(),
    )
    if not match:
        raise RuntimeError("cannot read the committed Mac native ABI configuration")
    options = json.loads(match[1])
    version = output(["/usr/bin/xml2-config", "--version"])
    cflags = output(["/usr/bin/xml2-config", "--cflags"])
    libs = output(["/usr/bin/xml2-config", "--libs"])
    destination.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "clone", "--shared", "--no-hardlinks", str(source), str(destination)],
        check=True,
    )
    subprocess.run(["git", "checkout", "--detach", REVISION], cwd=destination, check=True)
    subprocess.run(["./autogen.sh"], cwd=destination, check=True)
    # Apple supplies xml2-config and SDK headers, but no pkg-config file.
    # Expose those actual flags to GNU's normal configure detection.
    pkgconfig = destination.parent / (destination.name + "-pkgconfig")
    pkgconfig.mkdir()
    (pkgconfig / "libxml-2.0.pc").write_text(
        "Name: libxml2\nDescription: macOS SDK libxml2\n"
        f"Version: {version}\nCflags: {cflags}\nLibs: {libs}\n"
    )
    brew = Path(output(["brew", "--prefix"]))
    dependencies = ["tree-sitter@0.25", "webp", "librsvg", "gnutls", "sqlite"]
    paths = [pkgconfig] + [brew / "opt" / name / "lib/pkgconfig" for name in dependencies]
    environment = dict(os.environ, PKG_CONFIG_PATH=os.pathsep.join(map(str, paths)))
    metadata = {
        "revision": REVISION,
        "source": str(source),
        "destination": str(destination),
        "configure_options": options,
        "pkg_config_path": environment["PKG_CONFIG_PATH"],
        "system_libxml_version": version,
        "system_libxml_cflags": cflags,
        "system_libxml_libs": libs,
    }
    record = destination.parent / (destination.name + "-build.json")
    record.write_text(json.dumps(metadata, indent=2) + "\n")
    subprocess.run(["./configure", *shlex.split(options)], cwd=destination, env=environment, check=True)
    subprocess.run(["make", f"-j{args.jobs}"], cwd=destination, env=environment, check=True)
    if output(["git", "status", "--porcelain"], cwd=destination):
        raise RuntimeError("GNU build modified tracked source files; review before pinning")
    binary = destination / "src/emacs"
    dependencies = output(["otool", "-L", str(binary)])
    if "/usr/lib/libxml2.2.dylib" not in dependencies:
        raise RuntimeError("GNU did not link Apple's libxml2; review before pinning")
    metadata.update(
        binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
        dependencies=dependencies,
        configuration=output([
            str(binary), "-Q", "--batch", "--eval",
            "(prin1 (list emacs-version emacs-repository-version "
            "system-configuration system-configuration-options system-configuration-features))",
        ]),
    )
    record.write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"Candidate built; review {record} before updating the oracle pin and native ABI.")


if __name__ == "__main__":
    main()
