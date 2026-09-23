import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time

root = Path(__file__).resolve().parent
project = root.parents[1]
source = project.parent / 'emacs'
subjects = {'oracle': source / 'src/emacs',
            'baseline': root / 'baseline/bin/emaxx',
            'current': root / 'current/bin/emaxx'}
helper = project / 'compat/emacs_compat_runner.el'
cases = [
    ('test/src/fns-tests.el', 'fns-tests-sort'),
    ('test/lisp/emacs-lisp/pcase-tests.el', 'pcase-tests-macro'),
    ('test/src/undo-tests.el', 'undo-test4'),
    ('test/lisp/international/mule-tests.el', 'mule-cmds-tests--ucs-names-missing-names'),
]
output = root / 'corpus'
output.mkdir(exist_ok=True)
evidence = {'kind': 'diagnostic; same unmodified upstream test files and named selectors; not a full frozen run',
            'input_sha256': {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in [helper] + [source / file for file, _ in cases]}, 'runs': []}
for number, order in enumerate([['oracle', 'baseline', 'current'], ['current', 'baseline', 'oracle']], 1):
    for file, name in cases:
        for label in order:
            destination = output / f'{number}-{name}-{label}'
            destination.mkdir(exist_ok=True)
            result = destination / 'result.json'
            if result.exists():
                result.unlink()
            env = {k: v for k, v in os.environ.items() if not k.startswith('EMAXX_')}
            env.update(LANG='C', LC_ALL='C', EMACS_TEST_DIRECTORY=str(source / 'test'),
                       EMAXX_BATCH_RESULT_FILE=str(result), EMAXX_COMPAT_RELATIVE_FILE=file,
                       EMAXX_COMPAT_SELECTOR=name, EMAXX_COMPAT_RUNNER='oracle' if label == 'oracle' else 'emaxx')
            command = [str(subjects[label]), '-Q', '--batch', '-L', str(source / 'test'),
                       '-l', 'ert', '-l', str(helper), '-l', str(source / file),
                       '--eval', f"(emaxx-compat-run '{name})"]
            start = time.monotonic()
            process = subprocess.run(command, cwd=source / 'test', env=env,
                                     capture_output=True, text=True, timeout=120)
            row = {'round': number, 'name': name, 'runner': label, 'command': command,
                   'returncode': process.returncode, 'process_seconds': time.monotonic() - start,
                   'stdout': process.stdout, 'stderr': process.stderr}
            (destination / 'process.json').write_text(json.dumps(row, indent=2) + '\n')
            if process.returncode or not result.exists():
                raise RuntimeError(f'{name}/{label}: failed process; retained {destination}')
            report = json.loads(result.read_text())
            outcomes = report['results']
            if (len(outcomes) != 1 or outcomes[0]['name'] != name
                    or outcomes[0]['status'] != 'passed' or outcomes[0]['expected'] is not True):
                raise RuntimeError(f'{name}/{label}: rejected result; retained {destination}')
            row['body_seconds'] = outcomes[0]['duration_ns'] / 1e9
            evidence['runs'].append(row)
            (output / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
            print(number, name, label, row['body_seconds'], flush=True)
summary = {}
for _, name in cases:
    summary[name] = {label: statistics.median(row['body_seconds'] for row in evidence['runs']
                                             if row['runner'] == label and row['name'] == name)
                     for label in subjects}
    values = summary[name]
    values['current_over_oracle'] = values['current'] / values['oracle']
    values['improvement_percent'] = 100 * (1 - values['current'] / values['baseline'])
(output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2), flush=True)
