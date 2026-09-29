from pathlib import Path
import gzip,hashlib,json
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root=Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
out=root/'docs/handover/2026-09-28-positioned-full-gate'
assert not out.exists()
files=[(f,f.relative_to(p)) for f in (p/'full-gate-source72').rglob('*') if f.is_file()]
for pattern in ['source72-full-gate*','source72-native-*']:
 files += [(f,Path(f.name)) for f in p.glob(pattern) if f.is_file()]
files += [(p/f,Path(f)) for f in ['source72-manifest.json','run-source72-full-gate.py','package-source72-gate-evidence.py']]
artifact=Path('/var/folders/js/swz7g_zx0qj34jhbbc1hr_6w0000gn/T/native-comp-identity-75867-1790562361688192000')
files += [(f,Path('native-identity-original')/f.relative_to(artifact)) for f in artifact.rglob('*') if f.is_file()]
manifest={'format_version':1,'goal_complete':False,'source_commit':'a210f90cd65961bf4dc6aa2ccb87b529d46c4360','status':'FAILED','qualification':'Unchanged source72 gate: 2848 library passes, two existing opt-in terminal ignores; 60 binary passes; 29 integration passes then one native identity failure. Remaining native-thread, package-lifecycle, runtime-ownership and documentation stages unexecuted. Local GNU is source-matched but differs from pinned Darwin executable and committed native ABI configuration. No byte normalization or pin change; no full final-source, frozen or performance certification. Native artifacts are evidence only, not portable execution artifacts.','evidence_files':{}}
out.mkdir(parents=True)
for source,relative in sorted(set(files)):
 raw=source.read_bytes();compress=source.suffix not in ['.json','.py','.el'];name=str(relative)+('.gz' if compress else '')
 stored=gzip.compress(raw,mtime=0) if compress else raw
 dest=out/name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(stored)
 manifest['evidence_files'][name]={'source_path':str(source),'sha256':hashlib.sha256(stored).hexdigest(),'raw_sha256':hashlib.sha256(raw).hexdigest(),'raw_bytes':len(raw)}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
for name,r in manifest['evidence_files'].items():
 stored=(out/name).read_bytes();raw=gzip.decompress(stored) if name.endswith('.gz') else stored
 assert hashlib.sha256(stored).hexdigest()==r['sha256']
 assert hashlib.sha256(raw).hexdigest()==r['raw_sha256'] and len(raw)==r['raw_bytes']
print('Verified',len(manifest['evidence_files']),'portable failed-gate receipts')
