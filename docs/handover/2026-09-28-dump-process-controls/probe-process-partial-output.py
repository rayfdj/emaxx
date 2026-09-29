from pathlib import Path
import hashlib,json,os,subprocess,sys,time
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28")
label,binary=sys.argv[1:];binary=Path(binary);fixture=p/"process-partial-output-control.el"
command=[str(binary),"-Q","--batch","-l",str(fixture)]
def digest(f):return hashlib.sha256(f.read_bytes()).hexdigest()
artifacts={str(f):digest(f) for f in [binary,binary.with_suffix(".pdmp"),fixture]}
record={"command":command,"working_directory":str(p),"environment_overrides":{"EMAXX_GC_VERIFY":"1"},"artifacts":artifacts,"qualification":"Ordinary same-input diagnostic of a process output split by an explicit child/parent handshake; exact expected prefixes and final bytes, no timing assumption; original failing release run remains failed. GNU source-matched, not frozen pin."}
with (p/(label+"-command.json")).open("x") as out:json.dump(record,out,indent=2)
start=time.monotonic()
try:
    child=subprocess.run(command,cwd=p,env={**os.environ,"EMAXX_GC_VERIFY":"1"},capture_output=True,timeout=120)
    result={"exit_code":child.returncode,"timeout":False};stdout,stderr=child.stdout,child.stderr
except subprocess.TimeoutExpired as error:
    result={"exit_code":None,"timeout":True};stdout,stderr=error.stdout or b"",error.stderr or b""
for suffix,data in [("stdout",stdout),("stderr",stderr)]:
    with (p/(label+"."+suffix)).open("xb") as out:out.write(data)
result.update(elapsed_seconds=time.monotonic()-start,artifacts_unchanged=all(digest(Path(f))==sha for f,sha in artifacts.items()),stdout_hex=stdout.hex(),stderr_hex=stderr.hex())
with (p/(label+"-result.json")).open("x") as out:json.dump(result,out,indent=2)
print(json.dumps(result))
