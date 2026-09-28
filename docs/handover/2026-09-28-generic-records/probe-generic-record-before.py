from pathlib import Path
import hashlib,json,subprocess,time
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root=Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
editors={'gnu':Path('/Users/nbmhqa186/projects/emacs/src/emacs'),'emaxx':p/'source87-ordinary-emaxx'}
results=[]
for fixture in ['generic-record-graph','generic-record-construction']:
 source=root/'tests/fixtures'/(fixture+'.el');outputs={}
 for editor,binary in editors.items():
  digest=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
  hashes={'binary_sha256':digest(binary),'image_sha256':digest(binary.with_suffix('.pdmp')),'input_sha256':digest(source)}
  command=[str(binary),'-Q','--batch','--eval','(prin1 '+source.read_text()+')']
  started=time.monotonic();r=subprocess.run(command,cwd=root,capture_output=True,timeout=300)
  name='source87-record-before-'+fixture+'-'+editor
  for suffix,data in [('stdout',r.stdout),('stderr',r.stderr)]:
   with (p/(name+'.'+suffix)).open('xb') as output:output.write(data)
  unchanged=digest(binary)==hashes['binary_sha256'] and digest(binary.with_suffix('.pdmp'))==hashes['image_sha256'] and digest(source)==hashes['input_sha256']
  with (p/(name+'.json')).open('x') as output:
   json.dump({'command':command,'working_directory':str(root),'source_snapshot':87,'input_path':str(source),'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-started,**hashes,'artifacts_and_input_unchanged':unchanged,'qualification':'Immutable pre-migration source87. Local GNU is source matched, not the frozen Darwin oracle.'},output,indent=2);output.write('\n')
  outputs[editor]=(r.returncode,r.stdout,r.stderr,unchanged)
 results.append({'fixture':fixture,'matches':outputs['gnu']==outputs['emaxx'] and outputs['gnu'][0]==0 and outputs['gnu'][3], 'gnu_exit':outputs['gnu'][0], 'emaxx_exit':outputs['emaxx'][0]})
 print(results[-1],flush=True)
with (p/'source87-record-before-comparisons.json').open('x') as output:json.dump(results,output,indent=2);output.write('\n')
raise SystemExit(0 if all(x['matches'] for x in results) else 1)
