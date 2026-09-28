from pathlib import Path
import hashlib, json, subprocess, sys
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
tag = 'source' + sys.argv[1]
w = Path('/private/tmp/emaxx-runtime-audit-followup/emaxx')
names = ['text-conversion-variable-state', 'text-conversion-detached-state',
         'text-conversion-void-default-let', 'text-conversion-buffer-aliases']
source = json.loads((p / (tag + '-manifest.json')).read_text())
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
assert all(digest(w / name) == sha for name, sha in source.items())
results = []
for name in names:
    fixture = w / 'tests/fixtures' / (name + '.el')
    if not fixture.exists(): fixture = p / (name + '.el')
    receipts = {}
    for editor, binary in [('gnu', Path('/Users/nbmhqa186/projects/emacs/src/emacs')),
                           ('emaxx', p / (tag + '-ordinary-emaxx'))]:
        label = tag + '-' + name + '-' + editor
        subprocess.run(['python3', str(p / 'probe-text-conversion-state.py'), label, str(binary), str(fixture)], check=False)
        receipts[editor] = json.loads((p / (label + '.json')).read_text())
    a, b = receipts['gnu'], receipts['emaxx']
    matches = (a['exit_code'] == b['exit_code'] == 0
               and a['inputs_unchanged'] and b['inputs_unchanged']
               and a['stdout_hex'] == b['stdout_hex'] and a['stderr_hex'] == b['stderr_hex'])
    results.append({'fixture': name, 'matches': matches})
unchanged = all(digest(w / name) == sha for name, sha in source.items())
summary = {'expected_inventory': names, 'results': results, 'source_unchanged': unchanged,
           'all_match': unchanged and all(r['matches'] for r in results),
           'qualification': 'Ordinary CLI, exact stdout and stderr, identical inputs. Source-matched local GNU, not frozen certification. Linux-only primitive comparison is not executed on macOS.'}
with (p / (tag + '-text-conversion-ordinary-comparisons.json')).open('x') as output: json.dump(summary, output, indent=2)
print(json.dumps(summary, indent=2), flush=True)
raise SystemExit(0 if summary['all_match'] else 1)
