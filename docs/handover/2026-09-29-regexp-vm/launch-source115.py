from pathlib import Path
import json,os,subprocess
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');r=Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
env={'CARGO_BUILD_JOBS':'1','EMAXX_GC_VERIFY':'1','EMAXX_GNU_SOURCE_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928','EMAXX_DUMP_SOURCE_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928','EMACS_TEST_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928/test'}
filters=['regexp','regex','syntax','category','case_table','char_table']
commands=[['python3',str(p/'validate-frame-snapshot-v2.py'),'115',*filters,'--static'],['python3',str(p/'validate-frame-snapshot-v2.py'),'115',*filters,'--release'],['python3',str(p/'validate-frame-snapshot-v2.py'),'115','--ordinary']]
with (p/'source115-launch.json').open('x') as f:json.dump({'commands':commands,'cwd':str(r),'environment_overrides':env,'scope':'Exact regexp table snapshot validation on published VM+hash source. Original test assertions and selectors retained. New adversarial same-input GNU fixture uses shared leaf mutation and replaced edges after GC. No performance or Linux GC completion claim.'},f,indent=2)
for cmd in commands:
 rc=subprocess.call(cmd,cwd=r,env={**os.environ,**env})
 if rc:raise SystemExit(rc)
