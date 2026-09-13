#!/usr/bin/env python3
"""Run the real TLS controls and retain native backtraces on process failure."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess

import serial_grouped_gate

gate = serial_grouped_gate.gate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    server = shutil.which('gnutls-serv')
    assert server, 'the real GnuTLS server is required'
    (output / 'server-version.txt').write_bytes(subprocess.check_output([server, '--version']))
    binary = gate.discover_test_binary('gate', output)
    environment = gate.gate_environment(True)
    environment['RUST_BACKTRACE'] = 'full'
    command = [str(binary), 'lisp::primitives::tests::native_gnutls_', '--test-threads=1']
    with (output / 'tls-controls.log').open('w') as log:
        result = subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT)
    report = {'git': gate.git_state(), 'command': command, 'exit_code': result.returncode,
              'test_binary_sha256': gate.sha256_file(binary), 'status': 'failed'}
    (output / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    if result.returncode == 0:
        parsed = gate.parse_test_result((output / 'tls-controls.log').read_text())
        assert parsed['passed'] == 10 and parsed['failed'] == parsed['ignored'] == 0
        report['result'] = parsed
        report['status'] = 'passed'
    else:
        # Preserve the original failure even if debugger timing changes it.
        debugger = ['gdb', '-q', '--batch', '-ex', 'set pagination off',
                    '-ex', 'run', '-ex', 'thread apply all bt full', '--args',
                    *command, '--nocapture']
        with (output / 'native-backtrace.log').open('w') as log:
            debug = subprocess.run(debugger, env=environment, stdout=log,
                                   stderr=subprocess.STDOUT, timeout=300)
        report['debugger_exit_code'] = debug.returncode
    (output / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2), flush=True)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
