from pathlib import Path
import hashlib,json,subprocess,time
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');root=Path('/Users/nbmhqa186/projects/emaxx');candidate=Path('/private/tmp/emaxx-runtime-gnu-20260928');tag='gnu-darwin256-candidate'
result=json.loads((p/(tag+'-result.json')).read_text());assert result['exit_code']==0 and result['inputs_unchanged']
digest=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
binary=candidate/'src/emacs';image=candidate/'src/emacs.pdmp';pin=json.loads((root/'compat/oracle.lock.json').read_text())
identity={'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=candidate,text=True).strip(),'source_status':subprocess.check_output(['git','status','--porcelain'],cwd=candidate,text=True),'binary':str(binary),'binary_sha256':digest(binary),'image':str(image),'image_sha256':digest(image),'pinned_binary_sha256':pin['emacs_binary_sha256']}
identity['matches_pinned_binary']=identity['binary_sha256']==identity['pinned_binary_sha256'];assert identity['source_commit']==pin['emacs_repo_commit'] and not identity['source_status']
commands=[('capabilities',[str(binary),'-Q','--batch','--eval',"(prin1 (list emacs-version emacs-repository-version system-configuration system-configuration-options system-configuration-features (native-comp-available-p) (gnutls-available-p) (libxml-available-p) (sqlite-available-p) (fboundp 'sqlite-load-extension) (fboundp 'module-load) (treesit-available-p) (featurep 'lcms2) (lcms2-available-p) (if (boundp 'comp-native-version-dir) comp-native-version-dir 'unbound)))"]),('native-abi',[str(p/'generate-native-subrs-gnu256'),str(candidate/'src'),str(binary),str(p/'gnu-darwin256-native-subrs.rs')])]
for name,command in commands:
 started=time.monotonic();r=subprocess.run(command,cwd=root,capture_output=True)
 for suffix,data in [('stdout',r.stdout),('stderr',r.stderr)]:
  with (p/(tag+'-'+name+'.'+suffix)).open('xb') as f:f.write(data)
 with (p/(tag+'-'+name+'.json')).open('x') as f:json.dump({'command':command,'working_directory':str(root),'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-started,'identity':identity,'binary_and_image_unchanged':digest(binary)==identity['binary_sha256'] and digest(image)==identity['image_sha256']},f,indent=2)
 print(name,r.returncode,r.stdout.decode(errors='replace')[:1800],r.stderr.decode(errors='replace')[:800],flush=True)
 assert r.returncode==0
actual=p/'gnu-darwin256-native-subrs.rs';expected=root/'src/lisp/native_comp/generated_native_subrs_aarch64_apple_darwin.rs'
identity.update({'generated_abi_sha256':digest(actual),'committed_abi_sha256':digest(expected),'native_abi_matches':actual.read_bytes()==expected.read_bytes(),'qualification':'A freshly built pristine GNU candidate. Native ABI byte comparison is separate from the pinned executable identity; no pin or ABI updates performed.'})
with (p/(tag+'-identity.json')).open('x') as f:json.dump(identity,f,indent=2)
print(json.dumps(identity,indent=2))
