from pathlib import Path
import hashlib, json, os, subprocess, sys, time
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
tag, binary, fixture = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
inputs = {str(path): digest(path) for path in [binary, binary.with_suffix('.pdmp'), fixture, Path(__file__)]}
command = [str(binary), '-Q', '--batch', '--eval', '(prin1 ' + fixture.read_text() + ')']
started = time.monotonic()
completed = subprocess.run(command, capture_output=True, timeout=300)
for suffix, data in [('stdout', completed.stdout), ('stderr', completed.stderr)]:
    with (p / (tag + '.' + suffix)).open('xb') as output: output.write(data)
receipt = {'command': command, 'working_directory': str(Path.cwd()),
           'input_identities': inputs,
           'inputs_unchanged': all(digest(Path(path)) == sha for path, sha in inputs.items()),
           'exit_code': completed.returncode, 'elapsed_seconds': time.monotonic() - started,
           'stdout_hex': completed.stdout.hex(), 'stderr_hex': completed.stderr.hex(),
           'qualification': 'Focused ordinary CLI diagnostic. Local GNU is source matched, not the frozen binary. Exact bytes retained.'}
with (p / (tag + '.json')).open('x') as output: json.dump(receipt, output, indent=2)
print(tag, completed.returncode, completed.stdout.decode(errors='backslashreplace'), completed.stderr.decode(errors='backslashreplace'), flush=True)
raise SystemExit(0 if completed.returncode == 0 and receipt['inputs_unchanged'] else 1)
