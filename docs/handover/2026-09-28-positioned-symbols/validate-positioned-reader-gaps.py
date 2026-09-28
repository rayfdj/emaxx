from pathlib import Path
import hashlib,json,subprocess,sys,time
root=Path('/private/tmp/emaxx-runtime-positioned-symbols/emaxx')
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
snapshot='source'+sys.argv[1]
manifest=json.loads((p/(snapshot+'-manifest.json')).read_text())
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def unchanged():return all(digest(root/name)==sha for name,sha in manifest.items())
assert unchanged()
editors={'gnu':Path('/Users/nbmhqa186/projects/emacs/src/emacs'),'emaxx':p/(snapshot+'-ordinary-emaxx')}
results=[]
for fixture in ['positioned-reader-streams-before','positioned-reader-streams-v2','callable-reader-consumption-before']:
 outputs={}
 for editor,binary in editors.items():
  command=[str(binary),'-Q','--batch','--eval','(prin1 '+(p/(fixture+'.el')).read_text()+')']
  name=snapshot+'-known-gap-'+fixture+'-'+editor
  before={'binary':digest(binary),'image':digest(binary.with_suffix('.pdmp'))}
  start=time.monotonic();r=subprocess.run(command,cwd=root,capture_output=True,timeout=300)
  with (p/(name+'.stdout')).open('xb') as f:f.write(r.stdout)
  with (p/(name+'.stderr')).open('xb') as f:f.write(r.stderr)
  record={'command':command,'working_directory':str(root),'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-start,'stdout_hex':r.stdout.hex(),'stderr_hex':r.stderr.hex(),'artifact':before,'source_unchanged':unchanged(),'binary_unchanged':digest(binary)==before['binary'],'image_unchanged':digest(binary.with_suffix('.pdmp'))==before['image'],'qualification':'Source-matched ordinary reader diagnostics. Failed comparisons remain failed. GNU is not the pinned Darwin executable. No frozen or performance claim.'}
  with (p/(name+'.json')).open('x') as f:json.dump(record,f,indent=2);f.write('\n')
  outputs[editor]=(r.returncode,r.stdout)
  print(name,repr(r.stdout),r.returncode,flush=True)
 result={'fixture':fixture,'matches':outputs['gnu']==outputs['emaxx'] and outputs['gnu'][0]==0}
 results.append(result)
with (p/(snapshot+'-known-reader-gaps.json')).open('x') as f:json.dump({'results':results,'all_match':all(r['matches'] for r in results)},f,indent=2);f.write('\n')
raise SystemExit(0 if all(r['matches'] for r in results) else 1)
