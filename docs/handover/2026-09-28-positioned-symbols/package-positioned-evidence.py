from pathlib import Path
import gzip,hashlib,json,subprocess
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
r=Path('/private/tmp/emaxx-runtime-positioned-symbols/emaxx')
out=r/'docs/handover/2026-09-28-positioned-symbols'
assert json.loads((p/'source72-ordinary-positioned-comparisons.json').read_text())['passed']
for stage in ['release-selected','fmt','check','clippy','diff-check']:
 result=json.loads((p/('source72-'+stage+'-result.json')).read_text());assert result['exit_code']==0 and result['source_unchanged']
assert not json.loads((p/'source72-known-reader-gaps.json').read_text())['all_match']
assert not out.exists();out.mkdir(parents=True)
files=[]
snapshots={}
base='5bb1cbbf758e85497b94b06325ea59f9fe7aabad'
basefiles=set(subprocess.check_output(['git','ls-tree','-r','--name-only',base],cwd=r,text=True).splitlines())
for number in range(66,73):
 prefix='source'+str(number)
 files.extend(f for f in p.glob(prefix+'*') if f.is_file() and f.suffix in ['.json','.log','.patch','.el','.rs','.txt','.stdout','.stderr'])
 manifest=json.loads((p/(prefix+'-manifest.json')).read_text())
 additions={name:prefix+'-'+Path(name).name for name in manifest if name not in basefiles}
 for name,receipt in additions.items():assert hashlib.sha256((p/receipt).read_bytes()).hexdigest()==manifest[name]
 snapshots[prefix]={'base_commit':base,'patch':prefix+'.patch.gz','software_manifest':prefix+'-manifest.json','untracked_file_sources':additions}
for pattern in ['positioned-*','callable-reader-*','approved-char-table-*','char-table-linux-status-*']:
 files.extend(f for f in p.glob(pattern) if f.is_file() and f.suffix in ['.json','.el','.stdout','.stderr'])
files.extend(p/name for name in ['package-positioned-evidence.py','validate-frame-snapshot.py','validate-positioned-ordinary.py','validate-positioned-reader-gaps.py','run-source65-full-gate.py','source65-full-gate-command.json','source65-full-gate-result.json','source65-full-gate-counts.json','source65-full-gate.log','source65-manifest.json'])
files.extend(f for f in (p/'full-gate-source65').rglob('*') if f.is_file() and f.suffix in ['.json','.log','.txt'])
manifest={'format_version':1,'software_snapshot':72,'base_commit':base,'goal_complete':False,'qualification':'Positioned-symbol representation checkpoint. Source70 production passes287debug/strict static; source71 broad release fails292pass1fail; source72 audit/image release selection and strict static pass, and eight same-input ordinary comparisons pass. Source65 full gate remains FAILED:2833pass3fail2existingignores; subsequent audit fixes do not rewrite that result. All three source72 callable/marker reader comparisons fail and remain preserved. Optional cons-edge GC verifier has incomplete vectorlike-kind coverage. GNU source-matched only, not pinned Darwin. No complete final-source gate/frozen or performance claim.','binaries':'Not packaged; rebuild. Binary and image hashes are provenance only.','source_snapshots':snapshots,'evidence_files':{}}
for source in sorted(set(files)):
 relative=source.relative_to(p);raw=source.read_bytes();compress=source.suffix in ['.log','.patch','.txt','.stdout','.stderr'];name=str(relative)+('.gz' if compress else '');stored=gzip.compress(raw,mtime=0) if compress else raw
 dest=out/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(stored)
 manifest['evidence_files'][name]={'source_path':str(Path('target/runtime-goal/resume-2026-09-28')/relative),'sha256':hashlib.sha256(stored).hexdigest(),'raw_sha256':hashlib.sha256(raw).hexdigest(),'raw_bytes':len(raw)}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
for name,entry in manifest['evidence_files'].items():
 stored=(out/name).read_bytes();raw=gzip.decompress(stored) if name.endswith('.gz') else stored
 assert hashlib.sha256(stored).hexdigest()==entry['sha256']
 assert hashlib.sha256(raw).hexdigest()==entry['raw_sha256'] and len(raw)==entry['raw_bytes']
print('Verified',len(manifest['evidence_files']),'portable positioned-symbol receipts')
