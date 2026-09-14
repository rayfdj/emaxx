#!/usr/bin/env python3
"""Time unchanged upstream tests in alternating, independent editor processes.

This is a diagnostic comparison, not a frozen certificate. It requires the
selected tests to pass in GNU and both Emaxx binaries. Ordinary validation
remains responsible for full inventory, legitimate skips and expected failures.
"""

import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile
import time

from measure_allocators import digest, library_digest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--before', type=Path, required=True)
    parser.add_argument('--after', type=Path, required=True)
    parser.add_argument('--file', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--pairs', type=int, default=4)
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--selector', default='(not (or (tag :expensive-test) (tag :unstable)))')
    args = parser.parse_args()
    if args.pairs < 2 or args.timeout <= 0:
        parser.error('at least two alternating pairs and a positive timeout are required')
    source = args.source.resolve(strict=True)
    project = Path(__file__).resolve().parent.parent
    helper = project / 'compat/emacs_compat_runner.el'
    test_file = (source / args.file).resolve(strict=True)
    if source not in test_file.parents or test_file.suffix != '.el':
        parser.error('--file must identify an upstream Lisp file inside --source')
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    binaries = dict(oracle=source / 'src/emacs',
                    before=args.before.resolve(strict=True),
                    after=args.after.resolve(strict=True))
    inputs = [helper, Path(__file__).resolve(), project / 'tools/measure_allocators.py', test_file]
    for binary in binaries.values():
        inputs.extend((binary, binary.with_suffix('.pdmp')))
    identities = {str(path): digest(path) for path in inputs}
    libraries = library_digest(source)
    environment_base = dict(HOME=os.environ['HOME'], PATH=os.environ['PATH'], LANG='C')
    # Read configuration only. No GNU test result enters either subject's
    # command, environment, files or Lisp state.
    probe = ('(let ((repo (file-name-as-directory (file-truename '
             + json.dumps(str(source)) + ')))) '
             '(dolist (path load-path) '
             '(when (and (stringp path) (file-directory-p path) '
             '(string-prefix-p repo (file-name-as-directory (file-truename path)))) '
             '(princ (file-truename path)) (terpri))))')
    output = subprocess.run([str(binaries['oracle']), '-Q', '--batch', '--eval', probe],
                            env=environment_base, capture_output=True, check=True,
                            text=True, timeout=args.timeout)
    (root / 'load-path.stdout').write_text(output.stdout)
    (root / 'load-path.stderr').write_text(output.stderr)
    load_path = [Path(line).resolve(strict=True) for line in output.stdout.splitlines()]
    if not load_path or any(source not in path.parents for path in load_path):
        raise RuntimeError('GNU load-path probe did not return pinned source directories')
    evidence = dict(kind='diagnostic_only', host=platform.uname()._asdict(),
                    file=args.file, selector=args.selector, pairs=args.pairs,
                    source_revision=subprocess.check_output(
                        ['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip(),
                    input_sha256=identities, libraries_sha256=libraries,
                    load_path=[str(path) for path in load_path], runs=[])
    evidence_path = root / 'evidence.json'
    evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
    expected_names = None
    order = [(0, 'oracle')]
    for pair in range(args.pairs):
        labels = ['before', 'after'] if pair % 2 == 0 else ['after', 'before']
        order.extend((pair + 1, label) for label in labels)
    for pair, label in order:
        destination = root / f'{pair:02d}-{label}'
        destination.mkdir()
        with tempfile.TemporaryDirectory(prefix='ec-pair-', dir='/tmp') as temporary:
            scratch = Path(temporary).resolve()
            checkout = scratch / 'emacs'
            subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-hardlinks',
                            str(source), str(checkout)], check=True)
            shutil.rmtree(checkout / 'lisp')
            (checkout / 'lisp').symlink_to(source / 'lisp', target_is_directory=True)
            home = scratch / 'home'
            home.mkdir()
            environment = dict(environment_base, HOME=str(home), TMPDIR=str(scratch),
                               TMP=str(scratch), TEMP=str(scratch),
                               EMACS_TEST_DIRECTORY=str(checkout / 'test'),
                               EMAXX_DUMP_SOURCE_DIRECTORY=str(source),
                               EMAXX_BATCH_RESULT_FILE=str(destination / 'results.json'),
                               EMAXX_COMPAT_RELATIVE_FILE=args.file,
                               EMAXX_COMPAT_SELECTOR=args.selector,
                               EMAXX_COMPAT_RUNNER='oracle' if label == 'oracle' else 'emaxx')
            command = [str(binaries[label]), '--eval',
                       '(startup-redirect-eln-cache ' + json.dumps(str(scratch)) + ')',
                       '--no-init-file', '--no-site-file', '--no-site-lisp', '--batch']
            for directory in load_path:
                command.extend(('-L', str(directory)))
            command.extend(('-L', str(checkout / 'test'), '-l', 'ert', '-l', str(helper),
                            '-l', str(checkout / args.file), '--eval',
                            '(emaxx-compat-run (quote ' + args.selector + '))'))
            (destination / 'command.json').write_text(json.dumps(
                dict(command=command, environment=environment), indent=2) + '\n')
            print('START', pair, label, args.file, flush=True)
            start = time.monotonic()
            with (destination / 'stdout.log').open('wb') as stdout, \
                    (destination / 'stderr.log').open('wb') as stderr:
                result = subprocess.run(command, cwd=checkout / 'test', env=environment,
                                        stdout=stdout, stderr=stderr, timeout=args.timeout)
            elapsed = time.monotonic() - start
            process = dict(exit_code=result.returncode, elapsed_seconds=elapsed)
            (destination / 'process.json').write_text(json.dumps(process, indent=2) + '\n')
            if result.returncode:
                raise RuntimeError(f'{label} failed with exit {result.returncode}; raw output retained')
            report = json.loads((destination / 'results.json').read_text())
            selected = report['selected_tests']
            names = [outcome['name'] for outcome in report['results']]
            assert names and len(set(names)) == len(names)
            assert sorted(selected) == sorted(names)
            assert report['file'] == args.file and report['file_status'] == 'loaded'
            assert report['selector'] == args.selector
            assert report['runner'] == environment['EMAXX_COMPAT_RUNNER']
            assert report['summary'] == dict(total=len(names), passed=len(names),
                                              failed=0, skipped=0, unexpected=0)
            for outcome in report['results']:
                assert outcome['status'] == 'passed' and outcome['expected'] is True
                assert isinstance(outcome['duration_ns'], int) and outcome['duration_ns'] >= 0
            if expected_names is None:
                expected_names = sorted(names)
            assert sorted(names) == expected_names
            body = sum(outcome['duration_ns'] for outcome in report['results']) / 1e9
            row = dict(pair=pair, runner=label, process_seconds=elapsed, body_seconds=body,
                       outcomes=len(names), raw_sha256={
                           path.name: digest(path) for path in sorted(destination.iterdir())})
            evidence['runs'].append(row)
            evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
            print('DONE', pair, label, len(names), 'passed;', f'body={body:.6f}s', flush=True)
    assert {str(path): digest(path) for path in inputs} == identities
    assert library_digest(source) == libraries
    for row in evidence['runs']:
        destination = root / f"{row['pair']:02d}-{row['runner']}"
        assert all(digest(destination / name) == value for name, value in row['raw_sha256'].items())
    evidence['inputs_unchanged'] = True
    evidence['raw_outputs_verified'] = True
    evidence_path.write_text(json.dumps(evidence, indent=2) + '\n')
    summary = {}
    for label in ('before', 'after'):
        rows = [row for row in evidence['runs'] if row['runner'] == label]
        summary[label] = {key: statistics.median(row[key] for row in rows)
                          for key in ('body_seconds', 'process_seconds')}
    (root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == '__main__':
    main()
