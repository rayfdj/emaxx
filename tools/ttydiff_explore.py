#!/usr/bin/env python3
"""Explore and minimize safe seeded Emaxx/GNU terminal journeys.

The generator uses only complete, in-buffer editing commands from
``ttydiff.SAFE_EDIT_ACTIONS``.  A seed therefore reproduces the same journey
on every run.  When a screen checkpoint diverges, this tool uses delta
debugging to remove commands while preserving the mismatch and prints the
smallest sequence it found.
"""

import argparse
import contextlib
import io
import os
import sys

from ttydiff import (
    STARTUP_WAIT_SECONDS,
    compare,
    create_scenario_target,
    gnu_no_window_setup,
    remove_scenario_target,
    seeded_safe_actions,
)


INITIAL_CONTENTS = """alpha beta gamma delta
second line has several words
third line is here

This paragraph gives the explorer room to move, edit, kill, yank, and undo.

last paragraph ends here
"""


def editor_configuration(emaxx_binary, gnu_binary, lisp_dir):
    load_path = os.pathsep.join(
        [lisp_dir] + sorted(entry.path for entry in os.scandir(lisp_dir) if entry.is_dir())
    )
    return (
        [gnu_binary, "-nw", "-Q", "--eval", gnu_no_window_setup(lisp_dir)],
        [emaxx_binary],
        {"EMACSLOADPATH": load_path},
    )


def run_actions(label, commands, gnu_prefix, emaxx_prefix, emaxx_env):
    path = create_scenario_target(label, INITIAL_CONTENTS)
    try:
        return compare(
            label,
            commands,
            gnu_prefix + [path],
            emaxx_prefix + [path],
            {},
            emaxx_env,
            boot_wait=STARTUP_WAIT_SECONDS,
        )
    finally:
        remove_scenario_target(path)


def still_diverges(commands, runner):
    """Run a minimization probe without flooding stdout with checkpoints."""
    capture = io.StringIO()
    with contextlib.redirect_stdout(capture):
        return not runner(commands)


def minimize_divergence(commands, runner):
    """Classic ddmin over complete commands, retaining at least one."""
    current = list(commands)
    partitions = 2
    while len(current) >= 2:
        chunk_size = (len(current) + partitions - 1) // partitions
        reduced = False
        for start in range(0, len(current), chunk_size):
            candidate = current[:start] + current[start + chunk_size :]
            if candidate and still_diverges(candidate, runner):
                current = candidate
                partitions = max(2, partitions - 1)
                reduced = True
                break
        if reduced:
            continue
        if partitions >= len(current):
            break
        partitions = min(len(current), partitions * 2)
    return current


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("emaxx_binary")
    parser.add_argument("gnu_binary")
    parser.add_argument("gnu_lisp_dir")
    parser.add_argument("--seed", action="append", type=int, default=[])
    parser.add_argument("--steps", type=int, default=40)
    parser.add_argument(
        "--no-minimize",
        action="store_true",
        help="report the first full failing seed without delta debugging",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    seeds = args.seed or [17, 2309, 7595]
    gnu_prefix, emaxx_prefix, emaxx_env = editor_configuration(
        args.emaxx_binary, args.gnu_binary, args.gnu_lisp_dir
    )

    for seed in seeds:
        label = f"explore-seed-{seed}"
        commands = seeded_safe_actions(seed, args.steps)

        def runner(candidate):
            return run_actions(label, candidate, gnu_prefix, emaxx_prefix, emaxx_env)

        if runner(commands):
            continue

        minimized = commands
        if not args.no_minimize:
            print(f"MINIMIZING [{label}]: {len(commands)} commands")
            minimized = minimize_divergence(commands, runner)
        print(f"REPRODUCER [{label}]: {len(minimized)} command(s)")
        for command in minimized:
            print(f"  {command.name}: {command.keys!r}")
        return 1

    print(f"PASS: {len(seeds)} seeded safe journey(s) matched")
    return 0


if __name__ == "__main__":
    sys.exit(main())
