from pathlib import Path
import hashlib,json,os,subprocess,time
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28")
root=Path("/private/tmp/emaxx-runtime-reader-streams/emaxx")
prior=json.loads((p/"source108-release-selected-result.json").read_text())
assert prior["exit_code"]==0 and prior["source_unchanged"]
manifest=json.loads((p/"source108-manifest.json").read_text())
def sha(f):return hashlib.sha256(f.read_bytes()).hexdigest()
def unchanged():return all(sha(root/f)==h for f,h in manifest.items())
assert unchanged()
binary=p/"source108-release-libtest";identity=sha(binary)
filters=json.loads((p/"source105-release-selected-command.json").read_text())["command"][1:-2]
command=[str(binary),*filters,"char_table","case_table","regexp","syntax_table","case_fold","eager_macroexpand","--nocapture","--test-threads=1"]
overrides={"EMAXX_GC_VERIFY":"1","RUST_MIN_STACK":"134217728","RUST_TEST_THREADS":"1","EMAXX_FIXTURE_IMAGE_DIR":str(p/"fixture-images"),"EMACS_TEST_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928/test"}
with (p/"source108-release-broad-command.json").open("x") as out:json.dump({"command":command,"working_directory":str(root),"environment_overrides":overrides,"binary_sha256":identity,"source_manifest":"source108-manifest.json","qualification":"Unchanged source105 release selection plus the focused regexp controls and new eager expansion regression. Source105 crash remains failed."},out,indent=2)
start=time.monotonic()
with (p/"source108-release-broad.log").open("xb") as log:result=subprocess.run(command,cwd=root,env={**os.environ,**overrides},stdout=log,stderr=subprocess.STDOUT)
record={"exit_code":result.returncode,"elapsed_seconds":time.monotonic()-start,"source_unchanged":unchanged(),"binary_unchanged":sha(binary)==identity}
with (p/"source108-release-broad-result.json").open("x") as out:json.dump(record,out,indent=2)
print(json.dumps(record))
raise SystemExit(result.returncode or (0 if record["source_unchanged"] and record["binary_unchanged"] else 2))
