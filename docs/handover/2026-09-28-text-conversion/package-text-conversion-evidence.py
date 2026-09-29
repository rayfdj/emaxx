from pathlib import Path
import gzip, hashlib, json, subprocess
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-audit-followup/emaxx')
out = root / 'docs/handover/2026-09-28-text-conversion'
assert not out.exists()
for stage in ['debug-selected', 'release-selected', 'fmt', 'check', 'clippy', 'diff-check', 'ordinary-build', 'ordinary-image']:
    result = json.loads((p / ('source104-' + stage + '-result.json')).read_text())
    assert result['exit_code'] == 0 and result['source_unchanged'], stage
ordinary = json.loads((p / 'source104-text-conversion-ordinary-comparisons.json').read_text())
assert ordinary['all_match'] and ordinary['source_unchanged'] and len(ordinary['expected_inventory']) == len(ordinary['results']) == 4
assert json.loads((p / 'source100-debug-selected-result.json').read_text())['exit_code'] != 0
assert not json.loads((p / 'source100-text-conversion-ordinary-comparisons.json').read_text())['all_match']
assert not json.loads((p / 'inventory-89-to-104-comparison.json').read_text())['removed']
snapshots, files = {}, []
suffixes = {'.json', '.log', '.patch', '.rs', '.txt', '.stdout', '.stderr', '.el', '.expected'}
for number in [100, 104]:
    tag = 'source' + str(number)
    base = json.loads((p / (tag + '-base.json')).read_text())['commit']
    tracked = set(subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', base], cwd=root, text=True).splitlines())
    software = json.loads((p / (tag + '-manifest.json')).read_text())
    additions = {name: tag + '-' + Path(name).name for name in software if name not in tracked}
    for name, receipt in additions.items(): assert hashlib.sha256((p / receipt).read_bytes()).hexdigest() == software[name]
    snapshots[tag] = {'base_commit': base, 'patch': tag + '.patch.gz',
                      'software_manifest': tag + '-manifest.json', 'untracked_file_sources': additions}
    files += [path for path in p.glob(tag + '*') if path.is_file() and path.suffix in suffixes]
for pattern in ['text-conversion-*', 'inventory-89-to-104-*']:
    files += [path for path in p.glob(pattern) if path.is_file() and path.suffix in suffixes]
for name in ['validate-frame-snapshot.py', 'validate-text-conversion-ordinary.py', 'probe-text-conversion-state.py',
             'compare-source-inventories.py', 'package-text-conversion-evidence.py']:
    files.append(p / name)
files = sorted(set(files))
assert all(path.is_file() for path in files)
manifest = {
    'format_version': 1, 'software_snapshot': 104, 'goal_complete': False,
    'qualification': 'One authoritative text-conversion buffer field plus independent local flag. Selected debug/release/static and four ordinary exact GNU comparisons pass on source104. The Linux-only primitive and GNU contract are not executed on macOS; combined-source validation and existing Linux CI remain required. Local GNU is source matched, not the frozen Darwin pin. Source100 image-test setup failure, alias mismatch and source96 copied-image startup failure remain preserved. No performance or full-goal certificate.',
    'source_snapshots': snapshots, 'evidence_files': {}}
out.mkdir(parents=True)
for path in files:
    raw = path.read_bytes()
    compress = path.suffix in {'.log', '.patch', '.txt', '.stdout', '.stderr'}
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
print('Verified', len(manifest['evidence_files']), 'text-conversion receipts')
