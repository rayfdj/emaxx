from pathlib import Path
import hashlib,json,os,subprocess,time,sys
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');root=Path('/private/tmp/emaxx-runtime-audit-followup/emaxx');number=sys.argv[1];tag='source'+number
binary=p/(tag+'-debug-libtest');manifest=json.loads((p/(tag+'-manifest.json')).read_text());sha=hashlib.sha256(binary.read_bytes()).hexdigest()
test='lisp::primitives::tests::threads_retain_lexical_caller_roots_across_separate_callee_environments'
cases=[('cached-reclamation-verify-on',[test,'--exact'],'1'),('cached-reclamation-verify-off',[test,'--exact'],None),('positioned-errors',['positioned'],'1')]
failed=False
for name,selectors,verify in cases:
 command=[str(binary),*selectors,'--test-threads=1','--nocapture'];env=dict(os.environ);env.pop('EMAXX_GC_VERIFY',None);env.pop('EMAXX_GC_TRACE_ROOTS',None)
 overrides={'RUST_MIN_STACK':'134217728','RUST_TEST_THREADS':'1','EMAXX_FIXTURE_IMAGE_DIR':str(p/'fixture-images')}
 if verify:overrides['EMAXX_GC_VERIFY']=verify
 env.update(overrides);start=time.monotonic();result=subprocess.run(command,cwd=root,env=env,capture_output=True,timeout=300)
 for suffix,data in [('stdout',result.stdout),('stderr',result.stderr)]:
  with (p/(tag+'-'+name+'.'+suffix)).open('xb') as out:out.write(data)
 unchanged=all(hashlib.sha256((root/f).read_bytes()).hexdigest()==digest for f,digest in manifest.items())
 record={'command':command,'working_directory':str(root),'environment_overrides':overrides,'removed_environment_variables':['EMAXX_GC_TRACE_ROOTS']+([] if verify else ['EMAXX_GC_VERIFY']),'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-start,'source_unchanged':unchanged,'binary_sha256':sha,'binary_unchanged':sha==hashlib.sha256(binary.read_bytes()).hexdigest(),'qualification':'Original parent spawns original exact child; cached fixture image retained from selected run. No source/selector/assertion/image alteration.'}
 with (p/(tag+'-'+name+'.json')).open('x') as out:json.dump(record,out,indent=2)
 print(name,record,flush=True);failed|=result.returncode!=0 or not unchanged
raise SystemExit(int(failed))
