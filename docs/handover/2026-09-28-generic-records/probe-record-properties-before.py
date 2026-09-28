from pathlib import Path
import hashlib, json, subprocess, time
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
fixture = p / 'generic-record-text-properties.el'
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
results = {}
for editor, binary in [('gnu', Path('/Users/nbmhqa186/projects/emacs/src/emacs')),
                       ('emaxx', p / 'source87-ordinary-emaxx')]:
    name = 'source87-record-properties-before-' + editor
    identity = {str(path): digest(path) for path in [binary, binary.with_suffix('.pdmp'), fixture]}
    command = [str(binary), '-Q', '--batch', '--eval', '(prin1 ' + fixture.read_text() + ')']
    started = time.monotonic()
    result = subprocess.run(command, capture_output=True, timeout=300)
    for suffix, data in [('stdout', result.stdout), ('stderr', result.stderr)]:
        with (p / (name + '.' + suffix)).open('xb') as output:
            output.write(data)
    unchanged = all(digest(Path(path)) == sha for path, sha in identity.items())
    with (p / (name + '.json')).open('x') as output:
        json.dump({'command': command, 'exit_code': result.returncode,
                   'elapsed_seconds': time.monotonic() - started,
                   'identities': identity, 'inputs_unchanged': unchanged,
                   'qualification': 'Pre-migration ordinary executable. Source-matched GNU differs from the pinned Darwin binary.'}, output, indent=2)
    results[editor] = (result.returncode, result.stdout, result.stderr)
    print(editor, result.returncode, result.stdout.decode(errors='replace'), result.stderr.decode(errors='replace'), flush=True)
    assert unchanged
with (p / 'source87-record-properties-before-comparison.json').open('x') as output:
    json.dump({'matches': results['gnu'] == results['emaxx'] and results['gnu'][0] == 0}, output, indent=2)
