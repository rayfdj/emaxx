from pathlib import Path
import hashlib, json, subprocess, sys, time
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
tag = 'source' + sys.argv[1]
root = Path(json.loads((p / (tag + '-base.json')).read_text())['working_directory'])
manifest = json.loads((p / (tag + '-manifest.json')).read_text())
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def unchanged():
    return all((root / name).is_file() and digest(root / name) == sha for name, sha in manifest.items())
def save(name, record):
    with (p / (name + '.json')).open('x') as output:
        json.dump(record, output, indent=2)
        output.write('\n')
assert unchanged()
inputs = [p / (name + '.el') for name in [
    'positioned-reader-streams-before', 'positioned-reader-streams-v2',
    'callable-reader-consumption-before', 'reader-printer-destinations']]
inputs += [root / 'tests/fixtures' / (name + '.el') for name in [
    'reader-callable-consumption', 'reader-callable-dots', 'reader-callable-encoding',
    'reader-callable-errors', 'reader-callable-execution-modes', 'reader-callable-gc',
    'reader-callable-lifetime', 'reader-callable-obarray', 'reader-callable-redefinition',
    'reader-callable-streams', 'reader-callable-syntax', 'reader-stream-entrypoints',
    'reader-stream-positions', 'reader-byte-printing', 'reader-printer-bindings',
    'builtin-function-cell-authority', 'builtin-nil-function-cell', 'builtin-special-form-funcall']]
inputs += [root / 'tests/fixtures' / (name + '.el') for name in [
    'generic-record-size-boundary', 'generic-record-graph', 'generic-record-construction',
    'generic-record-argument-errors', 'generic-record-purecopy-message',
    'generic-record-text-properties', 'message-callback-special-bindings']]
inputs += [root / 'tests/fixtures' / (name + '.el') for name in [
    'text-conversion-variable-state', 'text-conversion-detached-state',
    'text-conversion-void-default-let', 'text-conversion-buffer-aliases']]
assert len(inputs) == 33 and len({path.stem for path in inputs}) == 33
artifacts = json.loads((p / (tag + '-ordinary-artifacts.json')).read_text())
subject = Path(artifacts['binary'])
assert digest(subject) == artifacts['binary_sha256']
assert digest(subject.with_suffix('.pdmp')) == artifacts['image_sha256']
# Native libraries in the normal dump have paths relative to its installation.
# Execute that installation and verify it against the immutable artifact hashes.
editors = {'gnu': Path('/private/tmp/emaxx-runtime-gnu-20260928/src/emacs'),
           'emaxx': subject}
results = []
for fixture in inputs:
    program = fixture.read_text()
    outputs, valid = {}, True
    for editor, binary in editors.items():
        name = tag + '-ordinary-combined-' + fixture.stem + '-' + editor
        command = [str(binary), '-Q', '--batch', '--eval', '(prin1 ' + program + ')']
        artifacts = {'binary_sha256': digest(binary), 'image_sha256': digest(binary.with_suffix('.pdmp'))}
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
                    'exit_code': completed.returncode, 'elapsed_seconds': time.monotonic() - start,
                    'stdout_hex': completed.stdout.hex(), 'stderr_hex': completed.stderr.hex(),
                    **artifacts, **checks,
                    'qualification': 'Ordinary source-matched diagnostic. Local GNU differs from the pinned Darwin executable; no frozen/performance claim.'})
        outputs[editor] = (completed.returncode, completed.stdout, completed.stderr)
    result = {'fixture': fixture.stem, 'matches': valid and outputs['gnu'] == outputs['emaxx'] and outputs['gnu'][0] == 0}
    results.append(result)
    print(result, flush=True)
all_match = len(results) == 33 and all(result['matches'] for result in results)
save(tag + '-ordinary-combined-comparisons', {'expected_inventory': [path.stem for path in inputs], 'results': results, 'all_match': all_match, 'source_unchanged': unchanged(), 'qualification': '33 identical ordinary Lisp inputs, exact stdout/stderr. Native ABI matched GNU candidate; not the frozen pin or full-goal completion.'})
raise SystemExit(0 if all_match else 1)
