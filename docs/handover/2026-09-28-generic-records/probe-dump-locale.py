from pathlib import Path
import hashlib, json, os, subprocess, sys, time
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
tag = 'source' + sys.argv[1]
fixture = p / 'dump-locale-quoting.el'
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
editors = {'gnu': Path('/Users/nbmhqa186/projects/emacs/src/emacs'),
           'emaxx': p / (tag + '-ordinary-emaxx')}
comparisons = []
for locale in ['C', 'en_US.UTF-8']:
    results = {}
    for editor, binary in editors.items():
        name = tag + '-dump-locale-' + locale + '-' + editor
        inputs = {str(path): digest(path) for path in [binary, binary.with_suffix('.pdmp'), fixture, Path(__file__)]}
        command = [str(binary), '-Q', '--batch', '--eval', '(prin1 ' + fixture.read_text() + ')']
        overrides = {'LC_ALL': locale}
        started = time.monotonic()
        completed = subprocess.run(command, capture_output=True, timeout=300, env={**os.environ, **overrides})
        for suffix, data in [('stdout', completed.stdout), ('stderr', completed.stderr)]:
            with (p / (name + '.' + suffix)).open('xb') as output: output.write(data)
        unchanged = all(digest(Path(path)) == sha for path, sha in inputs.items())
        receipt = {'command': command, 'environment_overrides': overrides, 'working_directory': str(Path.cwd()),
                   'input_identities': inputs, 'inputs_unchanged': unchanged,
                   'exit_code': completed.returncode, 'elapsed_seconds': time.monotonic() - started,
                   'stdout_hex': completed.stdout.hex(), 'stderr_hex': completed.stderr.hex(),
                   'qualification': 'Ordinary image locale restoration diagnostic, identical input and per-process locale. Local GNU source matched, not the frozen executable.'}
        with (p / (name + '.json')).open('x') as output: json.dump(receipt, output, indent=2)
        results[editor] = (completed.returncode, completed.stdout, completed.stderr, unchanged)
    matches = results['gnu'] == results['emaxx'] and results['gnu'][0] == 0 and results['gnu'][3]
    comparisons.append({'locale': locale, 'matches': matches})
    print(comparisons[-1], flush=True)
with (p / (tag + '-dump-locale-comparisons.json')).open('x') as output:
    json.dump({'results': comparisons, 'all_match': all(r['matches'] for r in comparisons)}, output, indent=2)
raise SystemExit(0 if all(r['matches'] for r in comparisons) else 1)
