from pathlib import Path
import hashlib
import json
import os
import shutil
import subprocess
import time

root = Path('/private/tmp/emaxx-runtime-frames-gsnnjxa_/emaxx')
out = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
manifest = json.loads((out / 'source119-manifest.json').read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def unchanged():
    return all(sha(root / name) == digest for name, digest in manifest.items())


def save(name, data):
    with (out / name).open('x') as stream:
        json.dump(data, stream, indent=2)
        stream.write('\n')


assert unchanged()
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=root)
env = os.environ.copy()
overrides = {
    'CARGO_TARGET_DIR': str((root / 'target/frame-validation').resolve()),
    'CARGO_BUILD_JOBS': '1', 'EMAXX_GC_VERIFY': '1',
    'EMAXX_GNU_SOURCE_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928',
    'EMAXX_DUMP_SOURCE_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928',
    'EMACS_TEST_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928/test',
}
env.update(overrides)
artifact = out / 'full-gate-source119'
command = ['python3', 'tools/serial_grouped_gate.py', '--scope', 'full',
           '--artifact-root', str(artifact)]
save('source119-full-gate-command.json', {
    'command': command, 'working_directory': str(root),
    'commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
    'source_manifest': 'source119-manifest.json',
    'environment_override': overrides,
    'qualification': 'Unmodified complete inventoried serial macOS gate; not frozen GNU or performance certification.'
})
start = time.monotonic()
with (out / 'source119-full-gate.log').open('x') as log:
    completed = subprocess.run(command, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)
result = {'exit_code': completed.returncode, 'elapsed_seconds': time.monotonic() - start,
          'source_unchanged': unchanged()}
summary_path = artifact / 'summary.json'
if summary_path.exists():
    summary = json.loads(summary_path.read_text())
    result['summary'] = summary
    binary = summary.get('test_binary')
    if binary:
        if isinstance(binary, dict):
            binary = binary.get('path')
        if binary and Path(binary).is_file():
            saved = out / 'source119-gate-libtest'
            assert not saved.exists()
            shutil.copy2(binary, saved)
            result['immutable_binary'] = {'path': str(saved), 'sha256': sha(saved)}
save('source119-full-gate-result.json', result)
print(json.dumps({k: v for k, v in result.items() if k != 'summary'}), flush=True)
raise SystemExit(0 if completed.returncode == 0 and result['source_unchanged'] else 1)
