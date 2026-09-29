from pathlib import Path
import hashlib, json, os, subprocess, time
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-audit-followup/emaxx')
test = 'lisp::primitives::tests::threads_retain_lexical_caller_roots_across_separate_callee_environments'
for snapshot, enabled in [(79, True), (79, False), (77, True)]:
    label = f'source{snapshot}-reclamation-verifier-' + ('on' if enabled else 'off')
    binary = p / f'source{snapshot}-debug-libtest'
    command = [str(binary), '--exact', test, '--test-threads=1', '--nocapture']
    overrides = {'RUST_MIN_STACK': '134217728', 'RUST_TEST_THREADS': '1',
                 'EMAXX_FIXTURE_IMAGE_DIR': str(p / 'fixture-images')}
    env = os.environ.copy()
    env.pop('EMAXX_GC_VERIFY', None)
    if enabled:
        overrides['EMAXX_GC_VERIFY'] = '1'
    env.update(overrides)
    start = time.monotonic()
    result = subprocess.run(command, cwd=root, env=env, capture_output=True, timeout=300)
    for suffix, data in [('stdout', result.stdout), ('stderr', result.stderr)]:
        with (p / (label + '.' + suffix)).open('xb') as output:
            output.write(data)
    record = {'command': command, 'working_directory': str(root),
              'environment_overrides': overrides,
              'environment_unset': [] if enabled else ['EMAXX_GC_VERIFY'],
              'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - start}
    with (p / (label + '.json')).open('x') as output:
        json.dump(record, output, indent=2)
        output.write('\n')
    print(label, record, flush=True)
