from pathlib import Path
import gzip
import hashlib
import json
import re
import subprocess

p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = p.parents[2]
out = root / 'docs/handover/2026-09-29-call-window-complete'
sha = lambda data: hashlib.sha256(data).hexdigest()
mac = json.loads((p / 'source119-full-gate-result.json').read_text())
linux_summary_path = next((p / 'linux-f0ea449c-full-artifacts').rglob('rust/summary.json'))
linux_summary = json.loads(linux_summary_path.read_text())
assert mac['exit_code'] == 0 and mac['source_unchanged']
assert mac['summary']['status'] == linux_summary['status'] == 'passed'
manifest = json.loads((p / 'source119-manifest.json').read_text())
assert all(sha((root / name).read_bytes()) == digest for name, digest in manifest.items())
identities = {'candidate': subprocess.check_output(['git','rev-parse','HEAD'], cwd=root, text=True).strip(),
              'source_manifest': 'source119-manifest.json', 'source_files': len(manifest),
              'all_manifest_files_unchanged': True,
              'qualification': 'Runtime source119. Source120 evidence collector has separate controls and the complete Linux workflow.'}
with (p / 'source119-complete-source-identity.json').open('x') as stream:
    json.dump(identities,stream,indent=2)
    stream.write('\n')
validation = {}
for name, summary, directory in [('macOS',mac['summary'],p/'full-gate-source119'),
                                  ('Linux',linux_summary,linux_summary_path.parent)]:
    groups = summary['runs'][0]['groups']
    assert len(groups) == 10
    assert all(g['exit_code'] == 0 and not g['timed_out'] and g['result']['failed'] == 0 for g in groups)
    totals = {k:sum(g['result'][k] for g in groups) for k in ['passed','failed','ignored']}
    for stage in ['bins','integration']:
        log=(directory / (stage+'.log')).read_text()
        values=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log)
        assert values and all(int(failed)==int(ignored)==0 for _,failed,ignored in values)
        totals[stage+'_passed'] = sum(int(passed) for passed,_,_ in values)
    validation[name]=totals
assert validation['macOS'] == {'passed':2894,'failed':0,'ignored':2,'bins_passed':60,'integration_passed':39}
assert validation['Linux'] == {'passed':2902,'failed':0,'ignored':2,'bins_passed':61,'integration_passed':42}
with (p/'source119-complete-totals.json').open('x') as stream:
    json.dump(validation,stream,indent=2)
    stream.write('\n')
out.mkdir(exist_ok=False)
selected={}
def add(path,name=None):
    assert path.is_file(), path
    key = name or path.name
    assert key not in selected
    selected[key]=path
for name in ['source119-full-gate-command.json','source119-full-gate-result.json',
             'source119-full-gate.log','source119-linux-full-result.json',
             'source119-linux-full-workflow.log','source119-manifest.json',
             'source119-complete-source-identity.json','source119-complete-totals.json',
             'run-source119-full-gate.py','package-call-window-complete.py',
             'source115-full-ttydiff-command.json','source115-full-ttydiff-result.json',
             'source115-full-ttydiff.log','source115-buffer-startup-diagnostic.log',
             'diagnose-source115-buffer-startup.py','run-source115-full-tty.py']:
    add(p/name)
for folder in ['full-gate-source119','linux-f0ea449c-full-artifacts','source115-buffer-startup-diagnostic']:
    for path in (p/folder).rglob('*'):
        if path.is_file() and path.name != 'libtest' and path.suffix not in {'.pdmp','.eln','.elc'}:
            add(path,folder+'/'+str(path.relative_to(p/folder)))
files=[]
for name,path in sorted(selected.items()):
    raw=path.read_bytes()
    compressed=len(raw)>16384 or path.suffix in {".log", ".raw"}
    data=gzip.compress(raw,mtime=0) if compressed else raw
    relative=name+('.gz' if compressed else '')
    destination=out/relative
    destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_bytes(data)
    files.append({'path':relative,'original':str(path),'bytes':len(raw),'sha256':sha(raw),
                  'stored_bytes':len(data),'stored_sha256':sha(data),'gzip':compressed})
record={'scope':'Complete unchanged source119 macOS and Linux Rust gates, source120 Linux evidence retention; source115 failed complete terminal attempt and original-timeout startup diagnostic.',
        'qualification':'Both complete Rust gates pass; previous failed executions remain failures. Source115 full terminal attempt stops at GNU startup after146matchedscenarios,76laterunexecuted. Focused startup replay passes, not full-run certification. Full runtime goal, pinned compatibility, accounting, final audit and parity remain open.',
        'binary_policy':'Executables, images and native artifacts remain local or in linked CI artifacts; hashes retained here. No substitution or portable-binary claim.',
        'validation':validation,'files':files}
(out/'manifest.json').write_text(json.dumps(record,indent=2)+'\n')
for f in files:
    data=(out/f['path']).read_bytes()
    assert sha(data)==f['stored_sha256']
    raw=gzip.decompress(data) if f['gzip'] else data
    assert len(raw)==f['bytes'] and sha(raw)==f['sha256']
print('Verified',len(files),'receipts,',sum(f['stored_bytes'] for f in files),'stored bytes')
