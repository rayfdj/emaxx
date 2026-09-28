from pathlib import Path
import gzip, hashlib, json, subprocess
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-symbol-ownership/emaxx')
out = root / 'docs/handover/2026-09-28-symbol-ownership'
assert not out.exists()
for stage in ['debug-selected', 'release-selected', 'fmt', 'check', 'clippy', 'diff-check',
              'ownership-integration-build', 'ownership-integration', 'ordinary-build', 'ordinary-image']:
    result = json.loads((p / ('source87-' + stage + '-result.json')).read_text())
    assert result['exit_code'] == 0 and result['source_unchanged'], stage
assert json.loads((p / 'source87-ownership-integration-artifact.json').read_text())['binary_unchanged']
assert json.loads((p / 'source87-ordinary-reader-comparisons.json').read_text())['all_match']
assert json.loads((p / 'source86-debug-selected-result.json').read_text())['exit_code'] != 0
files, snapshots = [], {}
for number in [86, 87]:
    prefix = 'source' + str(number)
    base = json.loads((p / (prefix + '-base.json')).read_text())['commit']
    basefiles = set(subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', base], cwd=root, text=True).splitlines())
    manifest = json.loads((p / (prefix + '-manifest.json')).read_text())
    additions = {name: prefix + '-' + Path(name).name for name in manifest if name not in basefiles}
    for name, receipt in additions.items():
        assert hashlib.sha256((p / receipt).read_bytes()).hexdigest() == manifest[name]
    snapshots[prefix] = {'base_commit': base, 'patch': prefix + '.patch.gz',
                         'software_manifest': prefix + '-manifest.json', 'untracked_file_sources': additions}
    files += [f for f in p.glob(prefix + '*') if f.is_file() and f.suffix in ['.json', '.log', '.patch', '.rs', '.txt', '.stdout', '.stderr']]
files += [p / name for name in ['validate-frame-snapshot.py', 'validate-symbol-ownership-followup.py',
    'validate-symbol-ordinary.py', 'package-symbol-ownership-evidence.py']]
# The ordinary comparison also uses these four independent inputs outside the source tree.
files += [p / (name + '.el') for name in ['positioned-reader-streams-before', 'positioned-reader-streams-v2',
    'callable-reader-consumption-before', 'reader-printer-destinations']]
manifest = {'format_version': 1, 'software_snapshot': 87, 'goal_complete': False,
    'qualification': 'Process-wide weak uninterned-symbol book under the existing serialized runtime boundary. Both source86 controls fail before repair. Source87 includes the previously separate reader and GC checkpoints and passes debug/release/static/public ownership and ordinary reader comparisons. No full/frozen/performance certification; local GNU source matched only.',
    'source_snapshots': snapshots, 'evidence_files': {}}
out.mkdir(parents=True)
for path in sorted(set(files)):
    raw = path.read_bytes()
    compress = path.suffix in ['.log', '.patch', '.txt', '.stdout', '.stderr']
    name = path.name + ('.gz' if compress else '')
    stored = gzip.compress(raw, mtime=0) if compress else raw
    (out / name).write_bytes(stored)
    manifest['evidence_files'][name] = {'source_path': str(path), 'sha256': hashlib.sha256(stored).hexdigest(),
        'raw_sha256': hashlib.sha256(raw).hexdigest(), 'raw_bytes': len(raw)}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
for name, receipt in manifest['evidence_files'].items():
    stored = (out / name).read_bytes()
    raw = gzip.decompress(stored) if name.endswith('.gz') else stored
    assert hashlib.sha256(stored).hexdigest() == receipt['sha256']
    assert hashlib.sha256(raw).hexdigest() == receipt['raw_sha256'] and len(raw) == receipt['raw_bytes']
print('Verified', len(manifest['evidence_files']), 'symbol ownership receipts')
