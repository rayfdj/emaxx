from pathlib import Path
import hashlib,json,os,re,runpy,subprocess,time
root=Path("/private/tmp/emaxx-runtime-reader-streams/emaxx")
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28")
artifacts=json.loads((p/"source108-ordinary-artifacts.json").read_text())
subject=Path(artifacts["binary"]);gnu=Path("/private/tmp/emaxx-runtime-gnu-20260928/src/emacs")
source=json.loads((p/"source108-manifest.json").read_text())
def sha(f):return hashlib.sha256(f.read_bytes()).hexdigest()
def unchanged():return all(sha(root/name)==h for name,h in source.items())
assert unchanged() and sha(subject)==artifacts["binary_sha256"] and sha(subject.with_suffix(".pdmp"))==artifacts["image_sha256"]
inputs={str(f):sha(f) for f in [subject,subject.with_suffix(".pdmp"),gnu,gnu.with_suffix(".pdmp"),root/"tools/ttydiff.py",Path(__file__)]}
tty_home=p/"source108-tail-tty-home";tty_home.mkdir(exist_ok=False)
overrides={"HOME":str(tty_home),"EMAXX_TTYDIFF_REQUIRE":"1"}
module=runpy.run_path(str(root/"tools/ttydiff.py"),run_name="inventory_only")
inventory=[entry[0] for entry in module["select_scenarios"]([])][186:]
assert len(inventory)==len(set(inventory))
command=["python3","tools/ttydiff.py",str(subject),str(gnu),"/private/tmp/emaxx-runtime-gnu-20260928/lisp",*inventory]
with (p/"source108-tail-ttydiff-command.json").open("x") as out:json.dump({"command":command,"working_directory":str(root),"environment_overrides":overrides,"inputs":inputs,"source_manifest":"source108-manifest.json","inventory":inventory,"qualification":"Explicit continuation of the incomplete full run: original failed scenario187 and remaining36; separate partial evidence, not a full pass. Unmodified default terminal scenario definitions, identical actions/settles/comparison functions and separate temporary targets. Empty personal HOME; missing binaries are errors. Matching-native-ABI GNU candidate is source-matched, not the frozen Darwin pin."},out,indent=2)
start=time.monotonic()
with (p/"source108-tail-ttydiff.log").open("xb") as log:completed=subprocess.run(command,cwd=root,env={**os.environ,**overrides},stdout=log,stderr=subprocess.STDOUT)
text=(p/"source108-tail-ttydiff.log").read_text(errors="replace")
observed=re.findall(r"^RUN \[(\d+)/(\d+)\] (.+)$",text,re.M)
complete=len(observed)==len(inventory) and all(int(a)==index+1 and int(b)==len(inventory) and name==inventory[index] for index,(a,b,name) in enumerate(observed))
record={"exit_code":completed.returncode,"elapsed_seconds":time.monotonic()-start,"source_unchanged":unchanged(),"inputs_unchanged":all(sha(Path(f))==h for f,h in inputs.items()),"expected_scenarios":len(inventory),"started_scenarios":len(observed),"complete_start_inventory":complete,"terminal_summary":text.splitlines()[-1:]}
with (p/"source108-tail-ttydiff-result.json").open("x") as out:json.dump(record,out,indent=2)
print(json.dumps(record),flush=True)
raise SystemExit(completed.returncode or (0 if complete and record["source_unchanged"] and record["inputs_unchanged"] and "PASS: all scenarios match" in text else 2))
