from pathlib import Path
import hashlib, json, os, subprocess, time
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-audit-followup/emaxx')
test = 'lisp::primitives::tests::threads_retain_lexical_caller_roots_across_separate_callee_environments'
binary = p / 'source82-debug-libtest'
command = [str(binary), '--exact', test, '--test-threads=1', '--nocapture']
overrides = {'RUST_MIN_STACK': '134217728', 'RUST_TEST_THREADS': '1',
             'EMAXX_FIXTURE_IMAGE_DIR': str(p / 'fixture-images'),
             'EMAXX_GC_TRACE_ROOTS': '1', 'EMAXX_RECLAMATION_CONTRACT_CHILD': test}
start = time.monotonic()
result = subprocess.run(command, cwd=root, env={**os.environ, **overrides}, capture_output=True, timeout=300)
for suffix, data in [('stdout', result.stdout), ('stderr', result.stderr)]:
    with (p / ('source82-direct-child.' + suffix)).open('xb') as output:
        output.write(data)
manifest = json.loads((p / 'source82-manifest.json').read_text())
record = {'command': command, 'working_directory': str(root), 'environment_overrides': overrides,
          'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - start,
          'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
          'source_unchanged': all(hashlib.sha256((root / name).read_bytes()).hexdigest() == sha for name, sha in manifest.items()),
          'qualification': 'Temporary diagnostic snapshot. Run the original exact child contract directly to retain successful-child stderr, which its parent otherwise captures silently. Original GNU control, Lisp program and assertions unchanged. Not final-source validation.'}
with (p / 'source82-direct-child.json').open('x') as output:
    json.dump(record, output, indent=2)
    output.write('\n')
print(record)
