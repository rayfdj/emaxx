from pathlib import Path
import json,os,subprocess
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28");r=Path("/private/tmp/emaxx-runtime-frames-gsnnjxa_/emaxx")
env={"CARGO_BUILD_JOBS":"1","EMAXX_GC_VERIFY":"1","EMAXX_GNU_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMAXX_DUMP_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMACS_TEST_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928/test"}
filters=["bytecode","handler_bind","handler_case","condition_case","catch","throw","unwind","wrong_number","arity","backtrace","threads_retain_lexical_caller_roots_across_separate_callee_environments","dead_thread_results_are_rooted_only_through_reachable_thread_objects"]
cmd=["python3",str(p/"validate-frame-snapshot-v2.py"),"113",*filters,"--static"]
with (p/"source113-launch.json").open("x") as f:json.dump({"command":cmd,"cwd":str(r),"environment_overrides":env,"scope":"Independent VM/evaluator boxed-error candidate. Original tests unchanged. Removing payload copying is not yet proven to repair Linux retention."},f,indent=2)
raise SystemExit(subprocess.call(cmd,cwd=r,env={**os.environ,**env}))
