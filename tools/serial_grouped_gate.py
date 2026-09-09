#!/usr/bin/env python3
"""Run the complete inventoried Rust gate with one process and worker at a time.

Only scheduling, progress verbosity and matching provenance differ from
grouped_gate's policy; its inventory, selectors and outcome checks are retained.
"""
import dataclasses
import grouped_gate as gate
# Keep all official inventory, selectors, template flags and result checks.
# Serialize both processes and libtest workers for this publication gate.
gate.LIB_PHASES = tuple((dataclasses.replace(group, test_threads=1),)
                        for phase in gate.LIB_PHASES for group in phase)
gate.EVAL_PHASES = tuple((dataclasses.replace(group, test_threads=1),)
                         for phase in gate.EVAL_PHASES for group in phase)
base_command = gate.GroupSpec.command
def command(self, binary):
    return [arg for arg in base_command(self, binary) if arg != '--quiet']
gate.GroupSpec.command = command
base_environment = gate.gate_environment
def environment(template):
    result = base_environment(template)
    result['RUST_TEST_THREADS'] = '1'
    return result
gate.gate_environment = environment
base_write = gate.write_summary
def write(path, summary):
    summary['environment']['RUST_TEST_THREADS'] = '1'
    summary['execution_override'] = {
        'script': __file__,
        'description': 'One group and one worker at a time; all inventory, selectors, template flags, captured-output behavior and strict outcome checks retained.'
    }
    base_write(path, summary)
gate.write_summary = write
if __name__ == '__main__':
    raise SystemExit(gate.main())
