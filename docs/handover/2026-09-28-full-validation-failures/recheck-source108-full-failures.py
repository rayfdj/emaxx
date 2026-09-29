from pathlib import Path
import hashlib
import json
import os
import re
import subprocess
import sys
import time

root = Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
out = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
manifest = json.loads((out / 'source108-manifest.json').read_text())
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
unchanged = lambda: all(sha(root / name) == value for name, value in manifest.items())
assert unchanged()
sys.path.insert(0, str(root / 'tools'))
import grouped_gate as gate

binary = Path(json.loads((out / 'source108-full-gate-result.json').read_text())['summary']['test_binary']['path'])
assert sha(binary) == '7cc7824a2f6512e215669a90dc061677e1eb66c4f32cb493da7e9ff6036b22cf'
failed_log = (out / 'full-gate-source108/repeat-01-primitives.log').read_text()
names = re.findall(r'^test (\S+) \.\.\. FAILED$', failed_log, re.M)
names += ['lisp::primitives::tests::' + name for name in [
    'process_send_string_and_region_route_output_to_the_process_buffer',
    'suspended_bytecode_retains_operand_and_unwind_roots',
]]
command = [str(binary), '--exact', *names, '--test-threads=1']
environment = gate.gate_environment(True)
overrides = {
    'EMAXX_GC_VERIFY': '1',
    'EMAXX_GNU_SOURCE_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928',
    'EMAXX_DUMP_SOURCE_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928',
    'EMACS_TEST_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928/test',
}
environment.update(overrides)
record = {
    'command': command, 'cwd': str(root), 'selected_tests': names,
    'source_manifest': 'source108-manifest.json', 'binary_sha256': sha(binary),
    'environment_override': {key: value for key, value in environment.items()
                             if key.startswith(('EMAXX_', 'EMACS_', 'RUST_')) or key in ('LANG', 'LC_ALL')},
    'purpose': 'Exact unchanged failed selectors, normal gate template, local sockets permitted. Original failures remain retained; selected diagnosis only.'
}
prefix = out / 'source108-failed-selectors-with-socket-access'
with Path(str(prefix) + '-command.json').open('x') as stream:
    json.dump(record, stream, indent=2)
start = time.monotonic()
with Path(str(prefix) + '.log').open('x') as log:
    result = subprocess.run(command, cwd=root, env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=3600)
record = {'exit_code': result.returncode, 'elapsed_seconds': time.monotonic() - start,
          'source_unchanged': unchanged(), 'binary_unchanged': sha(binary) == record['binary_sha256']}
with Path(str(prefix) + '-result.json').open('x') as stream:
    json.dump(record, stream, indent=2)
print(json.dumps(record), flush=True)
raise SystemExit(result.returncode)
