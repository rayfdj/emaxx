from pathlib import Path
import hashlib,json,os,subprocess,time
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
r=Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
g=Path('/private/tmp/emaxx-runtime-gnu-20260928')
m=json.loads((p/'source115-manifest.json').read_text())
def unchanged():return all(hashlib.sha256((r/k).read_bytes()).hexdigest()==v for k,v in m.items())
assert unchanged()
for action,dest in [('prepare','source115-core-native'),('pilot','source115-core-pilot-all')]:
 cmd=['python3','tools/core_runtime_perf.py',action,'--source',str(g),'--oracle',str(g/'src/emacs'),'--emaxx','/private/tmp/emaxx-runtime-stream-validation/release/emaxx','--output',str(p/dest),'--diagnostic']
 if action=='pilot':cmd+=['--native-dir',str(p/'source115-core-native'),'--pilot-runner','both']
 (p/(dest+'-command.json')).write_text(json.dumps({'command':cmd,'cwd':str(r),'qualification':'All 16 locked cases through normal editor entry points. One pilot sample each, no warmup; diagnostic only, unavailable counters remain unavailable. No local builds running; this is not the repeated parity certification.'},indent=2)+'\n')
 start=time.monotonic()
 with (p/(dest+'.log')).open('wb') as log:rc=subprocess.call(cmd,cwd=r,stdout=log,stderr=subprocess.STDOUT)
 result={'exit_code':rc,'elapsed_seconds':time.monotonic()-start,'source_unchanged':unchanged()}
 (p/(dest+'-result.json')).write_text(json.dumps(result,indent=2)+'\n');print(dest,result,flush=True)
 assert result['source_unchanged']
 if rc:raise SystemExit(rc)
