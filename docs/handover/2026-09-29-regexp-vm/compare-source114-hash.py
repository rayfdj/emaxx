from pathlib import Path
import hashlib,json,os,subprocess,time
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28");r=Path("/private/tmp/emaxx-runtime-frames-gsnnjxa_/emaxx")
binaries={"gnu":Path("/private/tmp/emaxx-runtime-gnu-20260928/src/emacs"),"emaxx":Path("/private/tmp/emaxx-runtime-frames-gsnnjxa_/emaxx/target/frame-validation/release/emaxx")}
sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
manifest=json.loads((p/"source114-manifest.json").read_text());source_ok=lambda:all((r/f).is_file() and sha(r/f)==h for f,h in manifest.items())
assert source_ok()
comparisons=[]
for name in ["hash-copy-stored-codes","hash-captured-functions","hash-captured-symbol-functions"]:
 fixture=r/"tests/fixtures"/(name+".el");expected=fixture.with_suffix(".expected").read_bytes();outputs={}
 for label,b in binaries.items():
  stem=p/("source114-ordinary-"+name+"-"+label);cmd=[str(b),"-Q","--batch","--eval","(prin1 "+fixture.read_text()+")"]
  artifacts={str(f):sha(f) for f in [b,b.with_suffix(".pdmp"),fixture,fixture.with_suffix(".expected")]}
  with stem.with_suffix(".command.json").open("x") as f:json.dump({"command":cmd,"cwd":str(p),"environment_overrides":{"EMAXX_GC_VERIFY":"1"},"artifacts":artifacts,"source_manifest":"source114-manifest.json","scope":"Fresh ordinary source114 executable/image, same input and exact bytes. GNU source matched, not frozen pin. Native hash authority remains failing."},f,indent=2)
  start=time.monotonic()
  try:
   c=subprocess.run(cmd,cwd=p,env={**os.environ,"EMAXX_GC_VERIFY":"1"},capture_output=True,timeout=120);rc=c.returncode;stdout,stderr=c.stdout,c.stderr;timeout=False
  except subprocess.TimeoutExpired as e:rc=None;stdout,stderr=e.stdout or b"",e.stderr or b"";timeout=True
  stem.with_suffix(".stdout").write_bytes(stdout);stem.with_suffix(".stderr").write_bytes(stderr)
  result={"exit_code":rc,"timed_out":timeout,"elapsed_seconds":time.monotonic()-start,"stdout_hex":stdout.hex(),"stderr_hex":stderr.hex(),"expected_match":stdout==expected,"artifacts_unchanged":all(sha(Path(f))==h for f,h in artifacts.items()),"source_unchanged":source_ok()}
  with stem.with_suffix(".result.json").open("x") as f:json.dump(result,f,indent=2)
  outputs[label]=result
  print(name,label,"exit",rc,"matches",stdout==expected,flush=True)
 comparisons.append({"fixture":name,"outcomes":outputs,"exact_match":outputs['gnu']['stdout_hex']==outputs['emaxx']['stdout_hex']})
with (p/"source114-ordinary-hash-comparisons.json").open("x") as f:json.dump(comparisons,f,indent=2)
assert all(c["exact_match"] and all(v["exit_code"]==0 and v["stderr_hex"]=="" and v["source_unchanged"] and v["artifacts_unchanged"] and v["expected_match"] for v in c["outcomes"].values()) for c in comparisons)
