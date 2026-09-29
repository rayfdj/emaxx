from pathlib import Path
import json,os,subprocess
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');r=Path('/private/tmp/emaxx-runtime-audit-followup/emaxx')
env={'CARGO_BUILD_JOBS':'1','EMAXX_GC_VERIFY':'1','EMAXX_GNU_SOURCE_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928','EMAXX_DUMP_SOURCE_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928','EMACS_TEST_DIRECTORY':'/private/tmp/emaxx-runtime-gnu-20260928/test'}
filters=['window','frame','undo','record_index','pdumper']
commands=[['python3',str(p/'validate-frame-snapshot-v2.py'),'118',*filters,'--static'],['python3',str(p/'validate-frame-snapshot-v2.py'),'118',*filters,'--release'],['python3',str(p/'validate-frame-snapshot-v2.py'),'118','--ordinary']]
with (p/'source118-launch.json').open('x') as f:json.dump({'commands':commands,'cwd':str(r),'environment_overrides':env,'scope':'Profile-backed window configuration save/restore reuses existing window index instead of scanning all pseudovectors. Payloads, deleted-window inclusion, id order and original assertions remain. No new registry or performance certification.'},f,indent=2)
for cmd in commands:
 rc=subprocess.call(cmd,cwd=r,env={**os.environ,**env})
 if rc:raise SystemExit(rc)
