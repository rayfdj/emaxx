from pathlib import Path
import argparse,hashlib,json,os,subprocess,time
parser=argparse.ArgumentParser();parser.add_argument("label");parser.add_argument("binary");parser.add_argument("cwd");parser.add_argument("--eager",action="store_true");args=parser.parse_args()
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28")
binary=Path(args.binary);root=Path(args.cwd)
def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
sha=digest(binary)
manifest=json.loads((p/"source105-manifest.json").read_text())
def unchanged(): return all((root/f).is_file() and digest(root/f)==h for f,h in manifest.items())
assert unchanged()
overrides={"EMAXX_GC_VERIFY":"1","RUST_MIN_STACK":"134217728","RUST_TEST_THREADS":"1","EMAXX_FIXTURE_IMAGE_DIR":str(p/"fixture-images"),"EMACS_TEST_DIRECTORY":"/private/tmp/emaxx-runtime-gnu-20260928/test","RUST_BACKTRACE":"1"}
if args.eager: overrides["EMAXX_DEBUG_EAGER_MACROEXPAND"]="1"
command=[str(binary),"lisp::eval::tests::eval_05::ert_source_diagnostic_loop_evaluates_function_alias_chain","--exact","--nocapture","--test-threads=1"]
record={"command":command,"working_directory":str(root),"environment_overrides":overrides,"binary_sha256":sha,"source_manifest":"source105-manifest.json","qualification":"Unchanged exact original test for fault isolation; all prior failed outcomes remain failed."}
with (p/(args.label+"-command.json")).open("x") as out: json.dump(record,out,indent=2)
start=time.monotonic()
with (p/(args.label+".log")).open("xb") as log: result=subprocess.run(command,cwd=root,env={**os.environ,**overrides},stdout=log,stderr=subprocess.STDOUT)
result={"exit_code":result.returncode,"elapsed_seconds":time.monotonic()-start,"binary_unchanged":digest(binary)==sha,"source_unchanged":unchanged()}
with (p/(args.label+"-result.json")).open("x") as out: json.dump(result,out,indent=2)
print(json.dumps(result))
raise SystemExit(result["exit_code"] or (0 if result["binary_unchanged"] and result["source_unchanged"] else 2))
