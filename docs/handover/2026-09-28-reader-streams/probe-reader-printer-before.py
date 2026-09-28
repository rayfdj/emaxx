from pathlib import Path
import hashlib,json,subprocess,sys,time
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root=Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
label=sys.argv[1]
for fixture in sys.argv[2:]:
 path=p/(fixture+'.el');program=path.read_text()
 for editor,binary in [('gnu',Path('/Users/nbmhqa186/projects/emacs/src/emacs')),('emaxx',p/'source80-ordinary-emaxx')]:
  name=label+'-'+fixture+'-'+editor;command=[str(binary),'-Q','--batch','--eval','(prin1 '+program+')'];start=time.monotonic()
  r=subprocess.run(command,cwd=root,capture_output=True,timeout=300)
  with (p/(name+'.stdout')).open('xb') as f:f.write(r.stdout)
  with (p/(name+'.stderr')).open('xb') as f:f.write(r.stderr)
  record={'command':command,'cwd':str(root),'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-start,'input_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'stdout_hex':r.stdout.hex(),'stderr_hex':r.stderr.hex(),'qualification':'Before comparison on immutable source80. GNU source-matched diagnostics only, not pinned or frozen.'}
  with (p/(name+'.json')).open('x') as f:json.dump(record,f,indent=2);f.write('\n')
  print(name,r.returncode,repr(r.stdout),repr(r.stderr),flush=True)
 with (p/(label+'-'+fixture+'.el')).open('x') as f:f.write(program)
