from pathlib import Path
import json,subprocess,time
p=Path("/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28")
editors={"gnu":"/private/tmp/emaxx-runtime-gnu-20260928/src/emacs","emaxx":json.loads((p/"source108-ordinary-artifacts.json").read_text())["binary"]}
fixtures={"purecopy-five-kinds-gc":"purecopy-five-kinds-gc-tty.el","purecopy-sequence-snapshot":"purecopy-sequence-snapshot-tty.el","purecopy-callback-purify-flag":"purecopy-callback-purify-flag-tty.el","record-purecopy-callback":"record-purecopy-callback.el"}
records=[]
with (p/"source108-tty-driver.log").open("xb") as log:
 for name,fixture in fixtures.items():
  for editor,binary in editors.items():
   label="source108-"+("gnu-" if editor=="gnu" else "")+name
   command=["python3",str(p/"probe-record-purecopy-tty-v2.py"),label,binary,str(p/fixture)]
   start=time.monotonic();result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
   record={"command":command,"exit_code":result.returncode,"elapsed_seconds":time.monotonic()-start};records.append(record);print(label,result.returncode,flush=True)
with (p/"source108-tty-driver-result.json").open("x") as out:json.dump({"runs":records,"all_successful":all(r["exit_code"]==0 for r in records)},out,indent=2)
raise SystemExit(subprocess.call(["python3",str(p/"compare-record-purecopy-tty-v2.py"),"108"]))
