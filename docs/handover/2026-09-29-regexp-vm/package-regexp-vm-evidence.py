from pathlib import Path
import gzip,hashlib,json
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');r=p.parents[2]
out=r/'docs/handover/2026-09-29-regexp-vm';out.mkdir(exist_ok=False)
selected={}
def add(f,dest=None):
 if f.is_file():selected[dest or f.name]=f
suffixes={'.json','.log','.stdout','.stderr','.el','.expected','.patch','.rs','.md','.py','.bash','.sample'}
for prefix in ['source114-','source115-','source116-','source108-full-rerun2-','source108-package-','source112-linux-','inventory-114-to-115-','vm-source108-to114-']:
 for f in p.glob(prefix+'*'):
  if f.suffix in suffixes:add(f)
for name in ['run-source108-full-rerun2-tty.py','diagnose-source108-package-tty.py','run-source115-package-tty.py','run-source115-core.py','launch-source114.py','launch-source115.py','compare-source115-fixtures.py','compare-source114-hash.py','inspect-vm-stack-prologues.py','validate-source116.py','validate-frame-snapshot-v2.py','package-regexp-vm-evidence.py']:
 add(p/name)
for folder in ['linux-c9ce1b2d-full-artifacts','linux-b588d5a8-isolated-replay','linux-b588d5a8-primitives-replay','source114-core-native','source114-core-pilot-all','source115-core-native','source115-core-pilot-all']:
 for f in (p/folder).rglob('*'):
  if f.is_file() and f.name!='libtest' and f.suffix not in ['.eln','.elc','.pdmp']:add(f,folder+'/'+str(f.relative_to(p/folder)))
files=[]
for dest,f in sorted(selected.items()):
 raw=f.read_bytes();compressed=len(raw)>16384;data=gzip.compress(raw,mtime=0) if compressed else raw
 relative=dest+('.gz' if compressed else '');target=out/relative;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 files.append(dict(path=relative,original=str(f),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),stored_bytes=len(data),stored_sha256=hashlib.sha256(data).hexdigest(),gzip=compressed))
record={'scope':'Source114 VM boxing checkpoint and failed full Linux run; source115 regexp snapshot validation and package TTY pass; preserved complete223TTY failure and observer-limited diagnostic; source116 image-prelude tool controls; all16diagnostic pilots. Full runtime goal remains open.','commits':{'source114':'c9ce1b2dc23cf20a400e3c2aa15d6afb6aebdb96','source115':'d4950cf44016d32c016839429e26caebc970758f','source116':'441d830b25ead161670d96ef26a051d94d2db699'},'qualification':'No frozen or performance certificate. GNU native ABI matches but executable differs from frozen pin. Allocation counters unavailable. Latest full Linux failed2177pass1fail; selected/macOS passes cannot clear it. source115fullmacOS/Linux/frozen validation not yet completed. Original113unvalidated-draft receipt remains historical.','files':files}
(out/'manifest.json').write_text(json.dumps(record,indent=2)+'\n')
for item in files:
 data=(out/item['path']).read_bytes();assert hashlib.sha256(data).hexdigest()==item['stored_sha256'];raw=gzip.decompress(data) if item['gzip'] else data;assert len(raw)==item['bytes'] and hashlib.sha256(raw).hexdigest()==item['sha256']
print('Portable receipts verified:',len(files),'stored bytes',sum(x['stored_bytes'] for x in files))
