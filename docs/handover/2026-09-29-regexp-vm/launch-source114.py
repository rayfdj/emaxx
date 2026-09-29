from pathlib import Path
import json,os,subprocess
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28");r=Path("/private/tmp/emaxx-runtime-frames-gsnnjxa_/emaxx")
env={"CARGO_BUILD_JOBS":"1","EMAXX_GC_VERIFY":"1","EMAXX_GNU_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMAXX_DUMP_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMACS_TEST_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928/test"}
filters=["bytecode","handler_bind","handler_case","condition_case","catch","throw","unwind","wrong_number","arity","backtrace","threads_retain_lexical_caller_roots_across_separate_callee_environments","dead_thread_results_are_rooted_only_through_reachable_thread_objects","hash","purecopy","lisp::primitives::pdumper::tests::"]
commands=[["python3",str(p/"validate-frame-snapshot-v2.py"),"114",*filters,"--static"],["python3",str(p/"validate-frame-snapshot-v2.py"),"114",*filters,"--release"],["python3",str(p/"validate-frame-snapshot-v2.py"),"114","--ordinary"]]
with (p/"source114-launch.json").open("x") as f:json.dump({"commands":commands,"cwd":str(r),"environment_overrides":env,"scope":"Combinedpublishedhashsemantics +6VM/evaluatorerrorcopyremovals. Existingtestsunchanged. Originalnativehashlayoutprobe remains separatelypreservedunpublisheddraft; thiscandidate makesnoarchitecturecompletionclaim. Linuxretentionhypothesisrequiresverification."},f,indent=2)
for cmd in commands:
 rc=subprocess.call(cmd,cwd=r,env={**os.environ,**env})
 if rc:raise SystemExit(rc)
