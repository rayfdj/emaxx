from pathlib import Path
import json
import os
import subprocess
import sys

p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-audit-followup/emaxx')
overrides = {
    'CARGO_BUILD_JOBS': '1', 'EMAXX_GC_VERIFY': '1',
    'EMAXX_IMAGE_TEMPLATE': '1',
    'EMAXX_GNU_SOURCE_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928',
    'EMAXX_DUMP_SOURCE_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928',
    'EMACS_TEST_DIRECTORY': '/private/tmp/emaxx-runtime-gnu-20260928/test',
}
filters = ['dump_emacs_portable', 'process_send', 'accept_process_output',
           'suspended_bytecode', 'threads_retain_lexical', 'printer', 'print_escape',
           'image_round_trips', 'native_gnutls', 'native_network',
           'make_network_process', 'native_udp', 'native_datagram',
           'localhost_family', 'set_network_process_option', 'url_retrieve_synchronously']
mode = sys.argv[1]
command = ['python3', str(p / 'validate-frame-snapshot-v2.py'), '109', *filters]
command += ['--static'] if mode == 'debug' else ['--release']
with (p / ('source109-' + mode + '-launch.json')).open('x') as stream:
    json.dump({'command': command, 'cwd': str(root), 'environment_override': overrides,
               'permission': 'Local sockets permitted for actual loopback fixtures.'}, stream, indent=2)
raise SystemExit(subprocess.call(command, cwd=root, env={**os.environ, **overrides}))
