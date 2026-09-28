from pathlib import Path
import json,os,subprocess
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28");r=Path("/private/tmp/emaxx-runtime-symbol-ownership/emaxx")
env={"CARGO_BUILD_JOBS":"1","EMAXX_GC_VERIFY":"1","EMAXX_GNU_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMAXX_DUMP_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMACS_TEST_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928/test"}
commands=[["python3",str(p/"validate-frame-snapshot-v2.py"),"111","hash","purecopy","lisp::primitives::pdumper::tests::","--release"],["python3",str(p/"validate-frame-snapshot-v2.py"),"111","--ordinary"]]
with (p/"source111-release-ordinary-launch.json").open("x") as f:json.dump({"commands":commands,"cwd":str(r),"environment_overrides":env,"scope":"Both stages run independently. Native architecture baseline is intentionally included and remains an ordinary failure until repaired. Semantic fixes require ordinary executable validation."},f,indent=2)
results=[]
for cmd in commands: results.append(subprocess.call(cmd,cwd=r,env={**os.environ,**env}))
print("stage_exit_codes",results,flush=True)
raise SystemExit(next((x for x in results if x),0))
