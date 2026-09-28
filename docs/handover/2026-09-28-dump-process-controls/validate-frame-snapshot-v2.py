from pathlib import Path
import argparse,hashlib,json,os,shutil,subprocess,time
parser=argparse.ArgumentParser();parser.add_argument('number');parser.add_argument('filters',nargs='*');parser.add_argument('--ordinary',action='store_true');parser.add_argument('--release',action='store_true');parser.add_argument('--static',action='store_true');args=parser.parse_args()
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');tag='source'+args.number;mode='release' if args.release else 'debug'
def digest(f):return hashlib.sha256(f.read_bytes()).hexdigest()
def save(name,value):
 f=p/(name+'.json');assert not f.exists(),f;f.write_text(json.dumps(value,indent=2)+'\n')
files=sorted(set(Path(f) for f in subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','Cargo.toml','Cargo.lock','build.rs','src','tests','tools'],text=True).splitlines() if Path(f).is_file()))
man={str(f):digest(f) for f in files}
if not (p/(tag+'-manifest.json')).exists():
 save(tag+'-manifest',man);(p/(tag+'.patch')).write_bytes(subprocess.check_output(['git','diff','--binary','HEAD']))
 save(tag+'-base',{'commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'working_directory':str(Path.cwd()),'manifest_scope':'Cargo files, build.rs, all src, tests and tools'})
 for f in files:
  if subprocess.run(['git','ls-files','--error-unmatch',str(f)],capture_output=True).returncode:(p/(tag+'-'+f.name)).write_bytes(f.read_bytes())
else:assert json.loads((p/(tag+'-manifest.json')).read_text())==man
baseenv={'CARGO_TARGET_DIR':str(Path('target/frame-validation').resolve()),'RUST_MIN_STACK':'134217728','RUST_TEST_THREADS':'1','EMAXX_FIXTURE_IMAGE_DIR':str(p/'fixture-images')}
env=os.environ.copy();env.update(baseenv)
def run(suffix,cmd,extraenv=None,extra=None):
 name=tag+'-'+suffix;overrides={**baseenv,**(extraenv or {})};save(name+'-command',{'command':cmd,'working_directory':str(Path.cwd()),'environment_overrides':overrides,**(extra or {})});t=time.monotonic()
 with (p/(name+'.log')).open('w') as log:r=subprocess.run(cmd,env={**env,**overrides},stdout=log,stderr=subprocess.STDOUT)
 rec={'exit_code':r.returncode,'elapsed_seconds':time.monotonic()-t,'source_unchanged':all(f.is_file() and digest(f)==man[str(f)] for f in files)};save(name+'-result',rec);print(name,json.dumps(rec),flush=True);return r.returncode or (0 if rec['source_unchanged'] else 2)
rc=0
if args.ordinary:
 rc=run('ordinary-build',['cargo','build','--locked','--release','--all-features','--bin','emaxx','--bin','make-fingerprint','--bin','compat-harness'])
 if not rc:rc=run('ordinary-image',['sh','tools/build-image.sh',str(Path('target/frame-validation/release/emaxx').resolve())],{'EMAXX_IMAGE_FORCE':'1'})
 if not rc:
  binary=Path('target/frame-validation/release/emaxx');image=binary.with_suffix('.pdmp')
  save(tag+'-ordinary-artifacts',{'binary':str(binary.resolve()),'binary_sha256':digest(binary),'image':str(image.resolve()),'image_sha256':digest(image)})
  shutil.copy2(binary,p/(tag+'-ordinary-emaxx'));shutil.copy2(image,p/(tag+'-ordinary-emaxx.pdmp'))
else:
 cmd=['cargo','test','--locked','--all-features','--lib','--no-run','--message-format=json-render-diagnostics']
 if args.release:cmd.insert(2,'--release')
 rc=run(mode+'-build',cmd)
 if not rc:
  artifacts=[]
  for line in (p/(tag+'-'+mode+'-build.log')).read_text().splitlines():
   try:d=json.loads(line)
   except json.JSONDecodeError:continue
   if d.get('reason')=='compiler-artifact' and d.get('profile',{}).get('test') and d.get('executable'):artifacts.append(d['executable'])
  assert len(artifacts)==1,artifacts
  binary=p/(tag+'-'+mode+'-libtest');shutil.copy2(artifacts[0],binary)
  if args.filters:
   rc=run(mode+'-selected',[str(binary),*args.filters,'--nocapture','--test-threads=1'],extra={'binary_sha256':digest(binary)})
   print((p/(tag+'-'+mode+'-selected.log')).read_text()[-5000:],flush=True)
if args.static:
 codes=[]
 for name,cmd in [('fmt',['cargo','fmt','--all','--','--check']),('check',['cargo','check','--locked','--all-targets','--all-features']),('clippy',['cargo','clippy','--locked','--all-targets','--all-features','--','-D','warnings']),('diff-check',['git','diff','--check'])]:codes.append(run(name,cmd))
 rc=rc or next((x for x in codes if x),0)
raise SystemExit(rc)
