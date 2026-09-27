from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys
import time

root = Path('/private/tmp/emaxx-runtime-frames-gsnnjxa_/emaxx')
out = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
snapshot = 'source' + sys.argv[1]
manifest = json.loads((out / (snapshot + '-manifest.json')).read_text())
cases = {
    'runtime-cons-mutation': '((t t 37) (t (17 payload)) (t table-command parent-command))',
    'runtime-value-ordering': '((nil nil t) (t t nil) (t t) (t nil t t nil t))',
    'shared-vector-native-words': '((t t 45) (t t 45))',
    'shared-string-native-words': '((t t t t 36) (t t t t 36))',
}
editors = {
    'gnu': Path('/Users/nbmhqa186/projects/emacs/src/emacs'),
    'emaxx': out / (snapshot + '-ordinary-emaxx'),
}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(name, data):
    with (out / (name + '.json')).open('x') as stream:
        json.dump(data, stream, indent=2)
        stream.write('\n')


def source_unchanged():
    return all(digest(root / path) == sha for path, sha in manifest.items())


assert source_unchanged()
identities = {
    name: {'path': str(binary), 'sha256': digest(binary),
           'image_sha256': digest(binary.with_suffix('.pdmp'))}
    for name, binary in editors.items()
}
records = []
for case, expected in cases.items():
    fixture = root / 'tests' / 'fixtures' / (case + '.el')
    program = '(prin1 ' + fixture.read_text() + ')'
    for editor, binary in editors.items():
        name = snapshot + '-ordinary-' + case + '-' + editor
        command = [str(binary), '-Q', '--batch', '--eval', program]
        record = {'command': command, 'working_directory': str(root),
                  'artifact': identities[editor], 'fixture': str(fixture.relative_to(root)),
                  'fixture_sha256': digest(fixture), 'expected': expected,
                  'qualification': 'Source-matched diagnostic; GNU binary differs from pinned Darwin oracle.'}
        save(name + '-command', record)
        start = time.monotonic()
        with (out / (name + '.stdout')).open('x') as stdout, (out / (name + '.stderr')).open('x') as stderr:
            completed = subprocess.run(command, cwd=root, env=os.environ.copy(), stdout=stdout, stderr=stderr)
        actual = (out / (name + '.stdout')).read_text()
        result = {'exit_code': completed.returncode, 'elapsed_seconds': time.monotonic() - start,
                  'stdout': actual, 'matches_expected': actual == expected,
                  'source_unchanged': source_unchanged(),
                  'binary_unchanged': digest(binary) == identities[editor]['sha256'],
                  'image_unchanged': digest(binary.with_suffix('.pdmp')) == identities[editor]['image_sha256']}
        save(name + '-result', result)
        records.append({'case': case, 'editor': editor, **result})
        print(name, json.dumps(result), flush=True)

passed = all(r['exit_code'] == 0 and all(r[k] for k in
             ['matches_expected', 'source_unchanged', 'binary_unchanged', 'image_unchanged']) for r in records)
save(snapshot + '-ordinary-cons-comparisons', {'passed': passed, 'records': records,
      'qualification': 'Eight diagnostic processes over four identical inputs; not frozen or performance certification.'})
raise SystemExit(0 if passed else 1)
