import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

root = Path(__file__).resolve().parent
project = root.parents[1]
binary = {
    'oracle': project.parent / 'emacs/src/emacs',
    'baseline': root / 'baseline/bin/emaxx',
    'current': root / 'current/bin/emaxx',
}
probe = root / 'probes/checked-performance.el'
output = root / 'measurements'
output.mkdir(exist_ok=True)
env = {k: v for k, v in os.environ.items() if not k.startswith('EMAXX_')}
env.update(LANG='C', LC_ALL='C')
expected = {'interpreted-lexical', 'interpreted-dynamic', 'interpreted-calls',
            'bytecode-calls', 'conses-6m', 'mapcar-2m', 'gc-idle-10'}
inputs = [probe]
for path in binary.values():
    inputs.extend([path, path.with_suffix('.pdmp')])
identities = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}
evidence = {'qualification': 'Both Rust builds have only the documented macOS build repair. Local GNU source revision matches the pinned revision; its binary hash differs from the tracked oracle lock. Diagnostic, not frozen certification.',
            'host': platform.uname()._asdict(), 'input_sha256': identities,
            'environment': {k: env.get(k) for k in ['LANG', 'LC_ALL']}, 'runs': []}
for round_number, order in enumerate([
        ['oracle', 'baseline', 'current'],
        ['current', 'oracle', 'baseline'],
        ['baseline', 'current', 'oracle']], 1):
    for label in order:
        command = [str(binary[label]), '-Q', '--batch', '-l', str(probe)]
        started = time.monotonic()
        process = subprocess.run(command, env=env, capture_output=True, text=True, timeout=180)
        elapsed = time.monotonic() - started
        record = {'round': round_number, 'runner': label, 'command': command,
                  'returncode': process.returncode, 'process_seconds': elapsed,
                  'stdout': process.stdout, 'stderr': process.stderr}
        path = output / f'{round_number}-{label}.json'
        path.write_text(json.dumps(record, indent=2) + '\n')
        if process.returncode:
            raise RuntimeError(f'{label}: {process.stderr}; retained {path}')
        parsed = {}
        for line in process.stdout.splitlines():
            parts = line.split(' ', 3)
            if parts[0] in expected:
                parsed[parts[0]] = {'seconds': float(parts[1]), 'collections': int(parts[2]), 'result': parts[3]}
        if set(parsed) != expected:
            raise RuntimeError(f'Missing rows: {path}')
        record['cases'] = parsed
        evidence['runs'].append(record)
        (output / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
        print(round_number, label, parsed, flush=True)
assert identities == {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in inputs}
summary = {}
for name in sorted(expected):
    summary[name] = {}
    for label in binary:
        rows = [run['cases'][name] for run in evidence['runs'] if run['runner'] == label]
        summary[name][label] = {'median_seconds': statistics.median(row['seconds'] for row in rows),
                                'collections': [row['collections'] for row in rows]}
    values = summary[name]
    values['current_over_oracle'] = values['current']['median_seconds'] / values['oracle']['median_seconds']
    values['baseline_over_oracle'] = values['baseline']['median_seconds'] / values['oracle']['median_seconds']
    values['improvement_percent'] = 100 * (1 - values['current']['median_seconds'] / values['baseline']['median_seconds'])
(output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2), flush=True)
