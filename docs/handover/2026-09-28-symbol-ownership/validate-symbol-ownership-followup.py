from pathlib import Path
import hashlib, json, os, shutil, subprocess, sys, time
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-symbol-ownership/emaxx')
tag = 'source' + sys.argv[1]
manifest = json.loads((p / (tag + '-manifest.json')).read_text())
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
def unchanged():
    return all((root / name).is_file() and digest(root / name) == sha for name, sha in manifest.items())
def save(name, value):
    with (p / (name + '.json')).open('x') as output:
        json.dump(value, output, indent=2)
        output.write('\n')
assert unchanged()
overrides = {'CARGO_TARGET_DIR': str((root / 'target/frame-validation').resolve()),
             'CARGO_BUILD_JOBS': '1', 'RUST_MIN_STACK': '134217728',
             'RUST_TEST_THREADS': '1', 'EMAXX_GC_VERIFY': '1',
             'EMAXX_FIXTURE_IMAGE_DIR': str(p / 'fixture-images')}
env = {**os.environ, **overrides}
def run(stage, command, extra=None):
    name = tag + '-ownership-' + stage
    save(name + '-command', {'command': command, 'working_directory': str(root),
                            'environment_overrides': overrides, **(extra or {})})
    started = time.monotonic()
    with (p / (name + '.log')).open('x') as log:
        completed = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
    record = {'exit_code': completed.returncode, 'elapsed_seconds': time.monotonic() - started,
              'source_unchanged': unchanged()}
    save(name + '-result', record)
    print(name, record, flush=True)
    return record
build = run('integration-build', ['cargo', 'test', '--locked', '--release', '--all-features',
    '--test', 'runtime_ownership', '--no-run', '--message-format=json-render-diagnostics'])
if build['exit_code'] or not build['source_unchanged']:
    raise SystemExit(1)
artifacts = []
for line in (p / (tag + '-ownership-integration-build.log')).read_text().splitlines():
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    if record.get('reason') == 'compiler-artifact' and record.get('target', {}).get('name') == 'runtime_ownership' and record.get('executable'):
        artifacts.append(Path(record['executable']))
assert len(artifacts) == 1, artifacts
binary = p / (tag + '-ownership-integration')
assert not binary.exists()
shutil.copy2(artifacts[0], binary)
sha = digest(binary)
record = run('integration', [str(binary), '--nocapture', '--test-threads=1'], {'binary_sha256': sha})
save(tag + '-ownership-integration-artifact', {'binary': str(binary), 'binary_sha256': sha,
    'binary_unchanged': digest(binary) == sha, 'qualification': 'Existing complete public-runtime integration inventory, GC verifier enabled; no full/frozen/performance claim.'})
print((p / (tag + '-ownership-integration.log')).read_text()[-5000:], flush=True)
raise SystemExit(record['exit_code'] or (0 if record['source_unchanged'] and digest(binary) == sha else 1))
