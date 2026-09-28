from pathlib import Path
import gzip,hashlib,json,subprocess
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');root=Path('/private/tmp/emaxx-runtime-audit-followup/emaxx');out=root/'docs/handover/2026-09-28-gc-verifier';base='a210f90cd65961bf4dc6aa2ccb87b529d46c4360'
assert not out.exists()
for stage in ['debug-selected','release-selected','fmt','check','clippy','diff-check']:
 r=json.loads((p/('source84-'+stage+'-result.json')).read_text());assert r['exit_code']==0 and r['source_unchanged']
for stage in ['cached-reclamation-verify-on','cached-reclamation-verify-off','positioned-errors']:
 r=json.loads((p/('source84-'+stage+'.json')).read_text());assert r['exit_code']==0 and r['source_unchanged'] and r['binary_unchanged']
basefiles=set(subprocess.check_output(['git','ls-tree','-r','--name-only',base],cwd=root,text=True).splitlines());files=[];snapshots={}
for n in [77,79,81,82,84]:
 prefix='source'+str(n)
 files += [f for f in p.glob(prefix+'*') if f.is_file() and f.suffix in ['.json','.log','.patch','.rs','.txt','.stdout','.stderr','.lldb']]
 man=json.loads((p/(prefix+'-manifest.json')).read_text());additions={f:prefix+'-'+Path(f).name for f in man if f not in basefiles}
 for f,receipt in additions.items():assert hashlib.sha256((p/receipt).read_bytes()).hexdigest()==man[f]
 snapshots[prefix]={'base_commit':base,'patch':prefix+'.patch.gz','software_manifest':prefix+'-manifest.json','untracked_file_sources':additions}
files += [p/f for f in ['validate-frame-snapshot.py','validate-gc-followup.py','probe-verifier-reclamation.py','capture-gc-root-diagnostic.py','capture-gc-root-location.py','gc_lldb_frames.py','gc_lldb_owners.py','package-gc-verifier-evidence.py']]
manifest={'format_version':1,'software_snapshot':84,'base_commit':base,'goal_complete':False,'qualification':'Optional cons-edge verifier coverage plus borrowed boxed-error inspection in eval. Source77 negative control fails as intended. Source79 broader debug fails original reclamation; immutable79 with verifier on/off fails. Source81/82 are temporary diagnostics only and all tracing was removed before84. First bootstrap can pass while cached image fails. Source84 debug, release, static and retained-image controls are required by this packaging script. No full/frozen/performance certification; GNU source matched only.','source_snapshots':snapshots,'evidence_files':{}}
out.mkdir(parents=True)
for f in sorted(set(files)):
 raw=f.read_bytes();compress=f.suffix in ['.log','.patch','.txt','.stdout','.stderr'];name=f.name+('.gz' if compress else '');stored=gzip.compress(raw,mtime=0) if compress else raw;(out/name).write_bytes(stored);manifest['evidence_files'][name]={'source_path':str(f),'sha256':hashlib.sha256(stored).hexdigest(),'raw_sha256':hashlib.sha256(raw).hexdigest(),'raw_bytes':len(raw)}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
for name,r in manifest['evidence_files'].items():
 stored=(out/name).read_bytes();raw=gzip.decompress(stored) if name.endswith('.gz') else stored
 assert hashlib.sha256(stored).hexdigest()==r['sha256'] and hashlib.sha256(raw).hexdigest()==r['raw_sha256'] and len(raw)==r['raw_bytes']
print('Verified',len(manifest['evidence_files']),'GC verifier receipts')
