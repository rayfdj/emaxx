from pathlib import Path
import json
import os
import subprocess

p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-symbol-ownership/emaxx')
environment = {
    'CARGO_BUILD_JOBS': '1', 'EMAXX_GC_VERIFY': '1',
    'EMAXX_GNU_SOURCE_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928',
    'EMAXX_DUMP_SOURCE_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928',
    'EMACS_TEST_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928/test',
}
command = ['python3', str(p / 'validate-frame-snapshot-v2.py'), '110',
           'native_hash_tables_expose_the_authoritative_gnu_entry_array', '--static']
with (p / 'source110-baseline-launch.json').open('x') as stream:
    json.dump({'command': command, 'cwd': str(root), 'environment_override': environment,
               'purpose': 'New GNU native layout/store regression against unchanged production. Expected pre-migration failure; not validated implementation.'}, stream, indent=2)
raise SystemExit(subprocess.call(command, cwd=root, env={**os.environ, **environment}))
