from pathlib import Path
import hashlib,json,os,shutil,subprocess,time
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root=Path('/private/tmp/emaxx-runtime-symbol-ownership/emaxx')
candidate=Path('/private/tmp/emaxx-runtime-gnu-20260928')
original=Path('/Users/nbmhqa186/projects/emacs')
tag='source96'
digest=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
def save(name,data):
    with (p/(name+'.json')).open('x') as f:json.dump(data,f,indent=2)
assert json.loads((p/'gnu-darwin256-native-abi-formatted-comparison.json').read_text())['formatted_native_abi_matches']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=root)
files=sorted(set(Path(name) for name in subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','Cargo.toml','Cargo.lock','build.rs','src','tests','tools'],cwd=root,text=True).splitlines() if (root/name).is_file()))
manifest={str(path):digest(root/path) for path in files}
save(tag+'-manifest',manifest)
save(tag+'-base',{'commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'working_directory':str(root),'manifest_scope':'Cargo files, build.rs, all src, tests and tools'})
with (p/(tag+'.patch')).open('xb') as f:f.write(subprocess.check_output(['git','diff','--binary','HEAD'],cwd=root))
inputs={str(path):digest(path) for path in [original/'src/emacs',original/'src/emacs.pdmp',candidate/'src/emacs',candidate/'src/emacs.pdmp',root/'compat/oracle.lock.json',root/'src/lisp/native_comp/generated_native_subrs_aarch64_apple_darwin.rs',Path(__file__)]}
link=root.parent/'emacs';assert link.is_symlink() and link.resolve()==original
save(tag+'-candidate-setup',{'previous_gnu_symlink':str(link.resolve()),'candidate':str(candidate),'input_identities':inputs,'qualification':'Committed Emaxx function-cell source with the freshly built pristine GNU candidate. The complete formatted native ABI matches the committed table. Candidate executable differs from the frozen pin; no pin/table/source edits and no frozen certification.'})
link.unlink();link.symlink_to(candidate,target_is_directory=True)
overrides={'CARGO_TARGET_DIR':str((root/'target/frame-validation').resolve()),'CARGO_BUILD_JOBS':'1','RUST_MIN_STACK':'134217728','RUST_TEST_THREADS':'1','EMAXX_GNU_SOURCE_DIRECTORY':str(candidate),'EMAXX_DUMP_SOURCE_DIRECTORY':str(candidate),'EMACS_TEST_DIRECTORY':str(candidate/'test')}
environment={**os.environ,**overrides}
def unchanged():return all(digest(root/path)==sha for path,sha in manifest.items()) and all(digest(Path(path))==sha for path,sha in inputs.items()) and link.resolve()==candidate
def run(stage,command):
    assert unchanged()
    save(tag+'-'+stage+'-command',{'command':command,'working_directory':str(root),'environment_overrides':overrides})
    started=time.monotonic()
    with (p/(tag+'-'+stage+'.log')).open('x') as f:r=subprocess.run(command,cwd=root,env=environment,stdout=f,stderr=subprocess.STDOUT)
    record={'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-started,'source_and_inputs_unchanged':unchanged()}
    save(tag+'-'+stage+'-result',record);print(stage,json.dumps(record),flush=True)
    if r.returncode or not record['source_and_inputs_unchanged']:raise SystemExit(r.returncode or 1)
run('native-build',['cargo','test','--release','--locked','--all-features','--test','native_comp_identity','--no-run','--message-format=json-render-diagnostics'])
artifacts=[]
for line in (p/(tag+'-native-build.log')).read_text().splitlines():
    try:data=json.loads(line)
    except json.JSONDecodeError:continue
    if data.get('reason')=='compiler-artifact' and data.get('target',{}).get('name')=='native_comp_identity' and data.get('executable'):artifacts.append(Path(data['executable']))
assert len(artifacts)==1,artifacts
test=p/(tag+'-native-identity-test');assert not test.exists();shutil.copy2(artifacts[0],test)
run('ordinary-wrapper',['python3',str(p/'validate-frame-snapshot.py'),'96','--ordinary'])
subject=(root/'target/frame-validation/release/emaxx').resolve()
artifacts={str(path):digest(path) for path in [test,subject,subject.with_suffix('.pdmp')]}
save(tag+'-native-test-artifacts',artifacts)
run('native-identity',[str(test),'--nocapture','--test-threads=1'])
assert all(digest(Path(path))==sha for path,sha in artifacts.items())
save(tag+'-native-final-identity',{'artifacts_unchanged':True,'source_and_inputs_unchanged':unchanged()})
