from pathlib import Path
import gzip
import hashlib
import json

p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = p.parents[2]
out = root / 'docs/handover/2026-09-29-main-candidate'
mac = json.loads((p / 'source115-full-gate-result.json').read_text())
linux = json.loads((p / 'source115-linux-full-result.json').read_text())
assert mac['exit_code'] == 0 and mac['source_unchanged']
assert mac['summary']['status'] == linux['status'] == 'passed'
out.mkdir(exist_ok=False)
selected = {}


def add(path, name=None):
    assert path.is_file(), path
    selected[name or path.name] = path


for name in [
    'source115-manifest.json', 'source115-base.json',
    'source115-full-gate-command.json', 'source115-full-gate-result.json',
    'source115-full-gate.log', 'source115-linux-full-result.json',
    'source115-linux-full-workflow.log',
    'source115-main-candidate-runtime-identity.json',
    'source116-linux-prelude-result.json', 'source116-linux-dispatch.json',
    'run-source115-full-gate.py', 'package-main-candidate-evidence.py',
]:
    add(p / name)

for prefix in ['source117-', 'source118-']:
    for path in p.glob(prefix + '*'):
        if path.is_file() and path.suffix in {'.json', '.log', '.patch'}:
            add(path)
for name in ['launch-source117.py', 'launch-source118.py',
             'run-source117-core.py', 'run-source118-core.py',
             'profile-source118-core-runtime-pilot.py',
             'source117.patch', 'source118.patch',
             'source117-source118-pilot-comparison.json']:
    add(p / name)

for folder in ['full-gate-source115', 'linux-968bf33b-full-artifacts',
               'linux-968bf33b-prelude-replay', 'source115-core-profiles',
               'source117-core-native', 'source117-core-pilot-all',
               'source118-core-native', 'source118-core-pilot-all',
               'source118-core-profiles']:
    for path in (p / folder).rglob('*'):
        if path.is_file() and path.name != 'libtest' and path.suffix not in {
            '.pdmp', '.eln', '.elc'
        }:
            add(path, folder + '/' + str(path.relative_to(p / folder)))
add(p / 'profile-source115-core-runtime-pilot.py')

files = []
for name, path in sorted(selected.items()):
    raw = path.read_bytes()
    compressed = len(raw) > 16384
    stored = gzip.compress(raw, mtime=0) if compressed else raw
    relative = name + ('.gz' if compressed else '')
    destination = out / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(stored)
    files.append({
        'path': relative, 'original': str(path), 'bytes': len(raw),
        'sha256': hashlib.sha256(raw).hexdigest(),
        'stored_bytes': len(stored),
        'stored_sha256': hashlib.sha256(stored).hexdigest(),
        'gzip': compressed,
    })
manifest = {
    'runtime_commit': 'd4950cf44016d32c016839429e26caebc970758f',
    'published_checkpoint': '968bf33b43df990b3eb6b816e7a644654c646d98',
    'scope': 'Complete unchanged source115 macOS and Linux Rust gates; Linux original-test prelude experiment; ordinary profiles. Separately packaged source117/118 follow-up patches and their selected validation, not part of the main candidate runtime.',
    'qualification': 'Earlier Linux reclamation failures remain preserved and their cause is not established. The full runtime goal, final adversarial audit, full pinned compatibility and performance criterion remain open. Source117/118 follow-up implementations are separate local commits, excluded from this main candidate.',
    'binary_policy': 'Large executable, image and native artifacts stay local; their identities are in the original receipts. No binary is substituted or reported as portable.',
    'files': files,
}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
for entry in files:
    stored = (out / entry['path']).read_bytes()
    assert hashlib.sha256(stored).hexdigest() == entry['stored_sha256']
    raw = gzip.decompress(stored) if entry['gzip'] else stored
    assert len(raw) == entry['bytes']
    assert hashlib.sha256(raw).hexdigest() == entry['sha256']
print('Verified', len(files), 'receipts,', sum(x['stored_bytes'] for x in files), 'stored bytes')
