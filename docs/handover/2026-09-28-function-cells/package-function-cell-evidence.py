from pathlib import Path
import gzip,hashlib,json,subprocess
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root=Path('/private/tmp/emaxx-runtime-symbol-ownership/emaxx')
out=root/'docs/handover/2026-09-28-function-cells'
assert not out.exists()
for stage in ['debug-selected','release-selected','fmt','check','clippy','diff-check','ordinary-build','ordinary-image']:
 result=json.loads((p/('source89-'+stage+'-result.json')).read_text())
 assert result['exit_code']==0 and result['source_unchanged'],stage
ordinary=json.loads((p/'source89-ordinary-function-comparisons.json').read_text())
assert ordinary['all_match'] and len(ordinary['expected_inventory'])==len(ordinary['results'])==22
assert json.loads((p/'source88-debug-selected-result.json').read_text())['exit_code']!=0
assert all(item['matches'] for item in json.loads((p/'source87-function-before-comparisons.json').read_text()))
files=[];snapshots={}
for number in [88,89]:
 prefix='source'+str(number)
 base=json.loads((p/(prefix+'-base.json')).read_text())['commit']
 basefiles=set(subprocess.check_output(['git','ls-tree','-r','--name-only',base],cwd=root,text=True).splitlines())
 source=json.loads((p/(prefix+'-manifest.json')).read_text())
 additions={name:prefix+'-'+Path(name).name for name in source if name not in basefiles}
 for name,receipt in additions.items():assert hashlib.sha256((p/receipt).read_bytes()).hexdigest()==source[name]
 snapshots[prefix]={'base_commit':base,'patch':prefix+'.patch.gz','software_manifest':prefix+'-manifest.json','untracked_file_sources':additions}
 files += [path for path in p.glob(prefix+'*') if path.is_file() and path.suffix in ['.json','.log','.patch','.rs','.txt','.stdout','.stderr']]
files += [path for path in p.glob('source87-function-before-*') if path.is_file()]
files += [p/name for name in ['validate-frame-snapshot.py','validate-function-ordinary.py','probe-function-cell-ordinary-before.py','package-function-cell-evidence.py']]
files += [p/(name+'.el') for name in ['positioned-reader-streams-before','positioned-reader-streams-v2','callable-reader-consumption-before','reader-printer-destinations']]
manifest={'format_version':1,'software_snapshot':89,'goal_complete':False,'qualification':'Remove two duplicate function payload tables and the string-keyed position map. Reads, static roots, cycle validation and image copying use the existing per-interpreter symbol function cell. Two direct-cell structural controls fail before repair and remain unchanged. Ordered ids still adapt the existing obarray enumeration; full allocated-symbol cell authority is not complete. Debug/release/static/ordinary results required here. No full/frozen/performance certification; local GNU source matched only.','source_snapshots':snapshots,'evidence_files':{}}
out.mkdir(parents=True)
for path in sorted(set(files)):
 raw=path.read_bytes();compress=path.suffix in ['.log','.patch','.txt','.stdout','.stderr']
 name=path.name+('.gz' if compress else '');stored=gzip.compress(raw,mtime=0) if compress else raw
 (out/name).write_bytes(stored)
 manifest['evidence_files'][name]={'source_path':str(path),'sha256':hashlib.sha256(stored).hexdigest(),'raw_sha256':hashlib.sha256(raw).hexdigest(),'raw_bytes':len(raw)}
(out/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
for name,receipt in manifest['evidence_files'].items():
 stored=(out/name).read_bytes();raw=gzip.decompress(stored) if name.endswith('.gz') else stored
 assert hashlib.sha256(stored).hexdigest()==receipt['sha256'] and hashlib.sha256(raw).hexdigest()==receipt['raw_sha256'] and len(raw)==receipt['raw_bytes']
print('Verified',len(manifest['evidence_files']),'function-cell receipts')
