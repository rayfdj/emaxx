from pathlib import Path
import json,os,subprocess,sys
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28")
overrides={"EMAXX_GC_VERIFY":"1","CARGO_BUILD_JOBS":"1","EMAXX_GNU_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMAXX_DUMP_SOURCE_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928","EMACS_TEST_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928/test"}
mode=sys.argv[1]
command=["python3",str(p/"validate-frame-snapshot-v2.py"),"108"]
if mode=="debug":
    filters=json.loads((p/"source105-debug-selected-command.json").read_text())["command"][1:-2]
    command += filters+["char_table","case_table","regexp","syntax_table","case_fold","eager_macroexpand","ert_source_diagnostic","--static"]
elif mode=="release": command += ["eager_macroexpand","ert_source_diagnostic","--release"]
elif mode=="ordinary": command += ["--ordinary"]
else:raise ValueError(mode)
raise SystemExit(subprocess.call(command,cwd="/private/tmp/emaxx-runtime-reader-streams/emaxx",env={**os.environ,**overrides}))
