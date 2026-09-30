#!/usr/bin/env python3
"""Replay a retained failed binary and inspect its cons mark bits under GDB.

This diagnosis preserves the original test assertions and timeout. A debugger
can change timing and addresses, so its results do not certify a repair.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from diagnose_rust_gate import execute
from diagnose_weak_roots import compiled_inputs
import serial_grouped_gate


EXPECTED_BINARY = '502ef50058a36ef75c8de7a2188a74469198ffc7452c68570eb02a94fa58a5a7'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--artifact-run', required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--filter', required=True)
    parser.add_argument('--key-head', required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    record = {'status': 'preparing', 'artifact_run': args.artifact_run,
              'source_revision': args.revision, 'filter': args.filter,
              'qualification': 'Original binary and assertions; hardware watchpoints alter timing. No source instrumentation, inferior function calls, assertion changes or performance certification.'}

    def save():
        (output / 'summary.json').write_text(json.dumps(record, indent=2) + '\n')

    save()
    try:
        subprocess.run(['gh', 'run', 'download', args.artifact_run, '--repo', 'rayfdj/emaxx',
                        '--dir', str(output / 'original')], check=True)
        summaries = list((output / 'original').rglob('rust-replay/summary.json'))
        if len(summaries) != 1:
            raise RuntimeError('expected one original Rust replay summary')
        original = json.loads(summaries[0].read_text())
        original_dir = summaries[0].parent
        source_binary = original_dir / original['test_binary']['artifact']
        if sha(source_binary) != EXPECTED_BINARY or original['test_binary']['sha256'] != EXPECTED_BINARY:
            raise RuntimeError('retained executable does not match the inspected ABI')
        # Recreate the original execution path, not a renamed test executable.
        binary = Path(original['test_binary']['path'])
        if not binary.is_relative_to(root / 'target'):
            raise RuntimeError('original binary path is outside this CI checkout target')
        binary.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_binary, binary)
        binary.chmod(0o755)
        fixture_directory = Path(original['environment']['EMAXX_FIXTURE_IMAGE_DIR'])
        if not fixture_directory.is_relative_to(root / 'target'):
            raise RuntimeError('original image directory is outside this CI checkout target')
        fixture_directory.mkdir(parents=True, exist_ok=True)
        for image in original['fixture_images']:
            source = original_dir / image['artifact']
            if sha(source) != image['sha256']:
                raise RuntimeError('retained fixture image hash differs')
            shutil.copy2(source, fixture_directory / source.name)
        # The selected binary embeds its original source paths. Restore only
        # its compiled inputs in this disposable diagnosis job; tools stay at
        # the current revision. No active validation checkout is modified.
        subprocess.run(['git', 'restore', '--source', args.revision, '--staged', '--worktree',
                        '--', 'src', 'tests', 'Cargo.toml', 'Cargo.lock', 'build.rs', '.cargo'],
                       cwd=root, check=True)
        inputs = compiled_inputs(root)
        expected_names = subprocess.check_output(
            ['git', 'ls-tree', '-r', '--name-only', args.revision, '--',
             'src', 'tests', 'Cargo.toml', 'Cargo.lock', 'build.rs', '.cargo'],
            cwd=root, text=True,
        ).splitlines()
        expected = {name: hashlib.sha256(subprocess.check_output(
            ['git', 'show', f'{args.revision}:{name}'], cwd=root,
        )).hexdigest() for name in expected_names
                    if name in inputs}
        if inputs != expected:
            raise RuntimeError('restored compiled inputs differ from the original revision')
        record['source_matches_original'] = True
        (output / 'source-before.json').write_text(json.dumps(inputs, indent=2) + '\n')
        environment = serial_grouped_gate.gate.gate_environment(True)
        environment.pop('GH_TOKEN', None)
        for name, value in original['environment'].items():
            if value is None:
                environment.pop(name, None)
            else:
                environment[name] = value
        command = [str(binary), args.filter, '--test-threads', '1']
        record['binary_sha256'] = sha(binary)
        record['original_environment'] = original['environment']
        record['plain'] = execute(command, environment, output / 'plain.log')
        save()
        names = original['expected_tests']
        if len(names) != 1:
            raise RuntimeError('this hardware diagnosis requires exactly one test')
        # Execute the same fresh child that the original wrapper launches.
        # This lets GDB watch Emaxx while its independent GNU child runs normally.
        overrides = {'EMAXX_RECLAMATION_CONTRACT_CHILD': names[0],
                     'EMAXX_WATCH_KEY_HEAD': args.key_head}
        environment.update(overrides)
        command = ['gdb', '--batch', '--return-child-result',
                   '-x', str(root / 'tools/diagnostics/exact-weak-root-watch.py'),
                   '--args', str(binary), '--exact', names[0], '--test-threads=1', '--nocapture']
        record['debugger_environment_override'] = overrides
        record['debugger'] = execute(command, environment, output / 'debugger.log')
        record['source_unchanged'] = compiled_inputs(root) == inputs
        record['binary_unchanged'] = sha(binary) == EXPECTED_BINARY
        traces = [json.loads(line.removeprefix('EXACT_ROOT '))
                  for line in (output / 'debugger.log').read_text(errors='replace').splitlines()
                  if line.startswith('EXACT_ROOT ')]
        (output / 'trace-events.json').write_text(json.dumps(traces, indent=2) + '\n')
        record['trace_events'] = len(traces)
        record['status'] = 'diagnosis recorded; inspect original failures and debugger trace'
        if not record['source_unchanged'] or not record['binary_unchanged']:
            raise RuntimeError('diagnostic inputs changed during execution')
        # Preserve the original failure as this diagnosis job's result.
        return record['plain']['exit_code'] or record['debugger']['exit_code']
    except BaseException as error:
        record.update(status='diagnostic error', error=repr(error))
        raise
    finally:
        save()


if __name__ == '__main__':
    sys.exit(main())
