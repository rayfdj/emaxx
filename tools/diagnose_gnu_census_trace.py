#!/usr/bin/env python3
"""Observe GNU's first GC inventories without changing the census fixtures.

Ordinary processes vary a documented, otherwise unused environment value.
The GDB observer reads the live vector heap and possible stack references.
It identifies candidates for investigation, not a repair or a passing gate.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

import retain_oracle_artifacts
import serial_grouped_gate


FIXTURES = ('vector-closure-rounded-census', 'vector-pseudovector-word-census')
GNU_SHA = '5e1721732427d69d8af63211f1e3833ec21e40f97f4b7f6cd4cb224b72cebfcf'
DUMP_SHA = 'bf4974228f3b3ea4374cec75a38eeb2bd0e7dd944da5bec4e6ad0387092502ae'
PREFIX = 'GNU_CENSUS_TRACE '


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--oracle-source', type=Path)
    parser.add_argument('--ordinary-only', action='store_true', help='Smoke-test ordinary orchestration; no GDB claim')
    args = parser.parse_args()
    gate = serial_grouped_gate.gate
    root = gate.PROJECT_ROOT
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    oracle = (args.oracle_source or root.parent / 'emacs').resolve()
    environment = gate.gate_environment(True)
    environment.update(VALIDATION='rust', VALIDATION_RUST_FILTER='')
    # This diagnosis needs no GitHub credential. Never retain environment values.
    environment.pop('GH_TOKEN', None)
    environment.pop('EMAXX_GNU_CENSUS_PADDING', None)
    observer = root / 'tools/diagnostics/gnu-census-snapshot.py'
    inputs = [Path(__file__), observer]
    for name in FIXTURES:
        inputs.extend(root / ('tests/fixtures/' + name + suffix) for suffix in ['.el', '.expected'])
    digests = {str(path): gate.sha256_file(path) for path in inputs}
    summary = dict(status='preparing', git=gate.git_state(), inputs=digests, runs=[],
                   ordinary_only=args.ordinary_only, timeout_seconds=gate.DEFAULT_TIMEOUT_SECONDS,
                   environment_changes=dict(LANG='C', LC_ALL='C', VALIDATION='rust',
                       VALIDATION_RUST_FILTER='', GH_TOKEN='removed',
                       EMAXX_GNU_CENSUS_PADDING='absent baseline or recorded number of x bytes'),
                   qualification='Diagnosis only. Original fixture bytes and expected outputs are unchanged. No warm-up collections or runtime patches. Padding varies execution context; GDB changes timing and inserts breakpoints, retains ASLR, and reads memory without inferior calls/stores. Possible stack references are candidates, not proof of the actual marking root. Full failures remain failed.')

    def save():
        (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')

    def execute(label, command, env):
        row = dict(label=label, command=command)
        summary['runs'].append(row)
        summary['status'] = 'running ' + label
        save()
        try:
            completed = subprocess.run(command, cwd=root, env=env, stdin=subprocess.DEVNULL,
                                       capture_output=True, timeout=gate.DEFAULT_TIMEOUT_SECONDS)
            stdout, stderr = completed.stdout, completed.stderr
            row.update(exit_code=completed.returncode, timed_out=False)
        except subprocess.TimeoutExpired as error:
            stdout, stderr = error.stdout or b'', error.stderr or b''
            row.update(exit_code=None, timed_out=True)
        for suffix, data in [('stdout', stdout), ('stderr', stderr)]:
            path = output / (label + '.' + suffix)
            with path.open('xb') as stream:
                stream.write(data)
            row[suffix + '_file'] = path.name
            row[suffix + '_sha256'] = hashlib.sha256(data).hexdigest()
        save()
        return row, stdout, stderr

    save()
    try:
        capture = retain_oracle_artifacts.capture(oracle, output / 'gnu-inputs')
        summary['oracle_capture'] = capture
        if not args.ordinary_only:
            if capture['files'][0]['sha256'] != GNU_SHA or capture['files'][1]['sha256'] != DUMP_SHA:
                raise ValueError('GNU executable/dump differ from the retained failed Linux run')
        for fixture in FIXTURES:
            program = '(prin1 ' + (root / ('tests/fixtures/' + fixture + '.el')).read_text() + ')'
            program.encode('ascii')
            expected = (root / ('tests/fixtures/' + fixture + '.expected')).read_bytes().rstrip(b'\n')
            command = [str(oracle / 'src/emacs'), '--batch', '-Q', '--eval', program]
            trace_environment = environment
            trace_padding = None
            reproduction_found = False
            for padding in [None, 0, 8, 16, 32, 64, 128, 256, 1024, 4096]:
                env = environment.copy()
                if padding is not None:
                    env['EMAXX_GNU_CENSUS_PADDING'] = 'x' * padding
                row, stdout, stderr = execute(f'{fixture}-ordinary-{padding}', command, env)
                row.update(padding_bytes=padding, expected_output=stdout == expected,
                           expected_sha256=hashlib.sha256(expected).hexdigest())
                if row['exit_code'] or row['timed_out'] or stderr:
                    raise ValueError('ordinary GNU process did not complete normally')
                if stdout != expected and not reproduction_found:
                    trace_environment, trace_padding = env, padding
                    reproduction_found = True
                save()
            if args.ordinary_only:
                continue
            debugger = ['gdb', '--batch', '--return-child-result']
            for variable in ['LINES', 'COLUMNS']:
                if variable not in trace_environment:
                    debugger += ['-ex', 'unset environment ' + variable]
            debugger += ['-x', str(observer), '--args', *command]
            row, stdout, stderr = execute(f'{fixture}-gdb', debugger, trace_environment)
            row.update(padding_bytes=trace_padding, ordinary_mismatch_observed=reproduction_found)
            events = [json.loads(line.split(PREFIX, 1)[1])
                      for line in stdout.decode(errors='replace').splitlines() if PREFIX in line]
            completed = [event for event in events if event['event'] == 'observer completed']
            sweeps = [event for event in events if event['event'] == 'marked vector inventory']
            returns = [event for event in events if event['event'] == 'explicit GC returned']
            row['trace_events'] = len(events)
            with (output / (fixture + '-trace-events.json')).open('x') as stream:
                json.dump(events, stream, indent=2)
            if (row['exit_code'] or row['timed_out'] or len(completed) != 1
                    or completed[0]['errors'] or completed[0]['captured_sweeps'] != 8
                    or [event['collection'] for event in sweeps] != list(range(1, 9))
                    or [event['collection'] for event in returns] != list(range(1, 9))
                    or b'Python Exception' in stdout + stderr or b'Traceback (most recent call last)' in stdout + stderr):
                raise ValueError('incomplete or failed GNU GC observation')
            if [event['live_slots'] for event in sweeps] != [event['total_vector_slots'] for event in returns]:
                raise ValueError('read-only vector inventory differs from GNU GC census')
            save()
        summary['inputs_unchanged'] = all(gate.sha256_file(Path(path)) == digest for path, digest in digests.items())
        if not summary['inputs_unchanged']:
            raise ValueError('diagnostic input changed')
        summary['oracle_after'] = retain_oracle_artifacts.verify(output / 'gnu-inputs')
        summary['status'] = 'ordinary smoke completed' if args.ordinary_only else 'GC diagnosis completed'
        return 0
    except BaseException as error:
        summary.update(status='diagnostic error', error=repr(error))
        raise
    finally:
        save()


if __name__ == '__main__':
    raise SystemExit(main())
