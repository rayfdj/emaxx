from pathlib import Path
import json, os, subprocess
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28")
r=Path("/private/tmp/emaxx-runtime-symbol-ownership/emaxx")
env={"CARGO_BUILD_JOBS":"1","EMAXX_GC_VERIFY":"1","EMAXX_GNU_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMAXX_DUMP_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMACS_TEST_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928/test"}
cmd=["python3",str(p/"validate-frame-snapshot-v2.py"),"111","hash","purecopy","lisp::primitives::pdumper::tests::","--static"]
with (p/"source111-launch.json").open("x") as f:json.dump({"command":cmd,"cwd":str(r),"environment_overrides":env,"scope":"Hash semantic repairs and original controls, with the unchanged native authority baseline110 included. Its known header failure remains a failure; architecture is incomplete."},f,indent=2)
raise SystemExit(subprocess.call(cmd,cwd=r,env={**os.environ,**env}))
