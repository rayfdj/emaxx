from pathlib import Path
import hashlib, json, subprocess, sys, time

p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
tag = 'source' + sys.argv[1]
manifest = json.loads((p / (tag + '-manifest.json')).read_text())

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def unchanged():
    return all((root / name).is_file() and digest(root / name) == sha
               for name, sha in manifest.items())

def save(name, record):
    with (p / (name + '.json')).open('x') as output:
        json.dump(record, output, indent=2)
        output.write('\n')

assert unchanged()
editors = {'gnu': Path('/Users/nbmhqa186/projects/emacs/src/emacs'),
           'emaxx': p / (tag + '-ordinary-emaxx')}
inputs = [p / (name + '.el') for name in
          ['positioned-reader-streams-before', 'positioned-reader-streams-v2',
           'callable-reader-consumption-before', 'reader-printer-destinations']]
inputs += sorted((root / 'tests/fixtures').glob('reader-callable-*.el'))
inputs += sorted((root / 'tests/fixtures').glob('reader-stream-*.el'))
inputs += sorted((root / 'tests/fixtures').glob('reader-byte-*.el'))
inputs += sorted((root / 'tests/fixtures').glob('reader-printer-*.el'))
results = []
for fixture in inputs:
    program = fixture.read_text()
    outputs = {}
    valid = True
    for editor, binary in editors.items():
        name = tag + '-ordinary-reader-' + fixture.stem + '-' + editor
        command = [str(binary), '-Q', '--batch', '--eval', '(prin1 ' + program + ')']
        artifacts = {'binary_sha256': digest(binary),
                     'image_sha256': digest(binary.with_suffix('.pdmp'))}
        start = time.monotonic()
        completed = subprocess.run(command, cwd=root, capture_output=True, timeout=300)
        for suffix, data in [('stdout', completed.stdout), ('stderr', completed.stderr)]:
            with (p / (name + '.' + suffix)).open('xb') as output:
                output.write(data)
        checks = {'source_unchanged': unchanged(),
                  'binary_unchanged': digest(binary) == artifacts['binary_sha256'],
                  'image_unchanged': digest(binary.with_suffix('.pdmp')) == artifacts['image_sha256']}
        valid = valid and all(checks.values())
        save(name, {'command': command, 'working_directory': str(root),
                    'input_path': str(fixture), 'input_sha256': digest(fixture),
                    'exit_code': completed.returncode,
                    'elapsed_seconds': time.monotonic() - start,
                    'stdout_hex': completed.stdout.hex(), 'stderr_hex': completed.stderr.hex(),
                    **artifacts, **checks,
                    'qualification': 'Ordinary source-matched diagnostic. GNU executable differs from the pinned Darwin oracle. No frozen or performance claim.'})
        outputs[editor] = (completed.returncode, completed.stdout, completed.stderr)
    result = {'fixture': fixture.stem,
              'matches': valid and outputs['gnu'] == outputs['emaxx'] and outputs['gnu'][0] == 0}
    results.append(result)
    print(result, flush=True)
save(tag + '-ordinary-reader-comparisons', {'results': results,
                                         'all_match': all(result['matches'] for result in results)})
raise SystemExit(0 if all(result['matches'] for result in results) else 1)
