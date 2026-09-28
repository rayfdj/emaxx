from pathlib import Path
import gzip, hashlib, json, subprocess
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-audit-followup/emaxx')
out = root / 'docs/handover/2026-09-28-combined-records'
assert not out.exists()
for stage in ['debug-selected', 'release-selected', 'fmt', 'check', 'clippy', 'diff-check', 'ordinary-build', 'ordinary-image']:
    result = json.loads((p / ('source105-' + stage + '-result.json')).read_text())
    assert result['exit_code'] == 0 and result['source_unchanged'], stage
ordinary = json.loads((p / 'source105-ordinary-combined-comparisons.json').read_text())
assert ordinary['all_match'] and ordinary['source_unchanged'] and len(ordinary['expected_inventory']) == len(ordinary['results']) == 33
assert json.loads((p / 'source105-dump-locale-comparisons.json').read_text())['all_match']
assert json.loads((p / 'source105-purecopy-tty-comparisons.json').read_text())['all_match']
for before in [103, 104]:
    assert not json.loads((p / ('inventory-' + str(before) + '-to-105-comparison.json')).read_text())['removed']
software = json.loads((p / 'source105-manifest.json').read_text())
assert all(hashlib.sha256((root / name).read_bytes()).hexdigest() == sha for name, sha in software.items())
suffixes = {'.json', '.log', '.patch', '.rs', '.txt', '.stdout', '.stderr', '.el', '.expected', '.report', '.raw'}
files = [f for pattern in ['source105*', 'inventory-103-to-105-*', 'inventory-104-to-105-*']
         for f in p.glob(pattern) if f.is_file() and f.suffix in suffixes and 'full-gate' not in f.name]
for name in ['validate-frame-snapshot-v2.py', 'validate-combined-ordinary.py', 'probe-dump-locale-v2.py',
             'probe-record-purecopy-tty.py', 'probe-record-purecopy-tty-v2.py', 'compare-record-purecopy-tty-v2.py',
             'compare-source-inventories.py', 'package-combined-runtime-evidence.py',
             'combined-record-text-conversion-resolution.json', 'dump-locale-quoting.el',
             'purecopy-five-kinds-gc-tty.el', 'purecopy-sequence-snapshot-tty.el',
             'purecopy-callback-purify-flag-tty.el', 'record-purecopy-callback.el',
             'positioned-reader-streams-before.el', 'positioned-reader-streams-v2.el',
             'callable-reader-consumption-before.el', 'reader-printer-destinations.el']:
    files.append(p / name)
files = sorted(set(files)); assert all(f.is_file() for f in files)
manifest = {'format_version': 1, 'software_snapshot': 105, 'goal_complete': False,
            'source_commit': json.loads((p / 'source105-base.json').read_text())['commit'],
            'software_manifest': 'source105-manifest.json',
            'qualification': 'Selected validation of combined inline records, function cells and text-conversion buffer state. All prior separate failures remain in the parent evidence packages. GNU candidate matches the unchanged committed native ABI but not the frozen Darwin binary. Ordinary native images execute from their recorded installation because their .eln paths are relative to it. Full macOS/Linux, frozen and terminal inventories and performance remain required.',
            'evidence_files': {}}
out.mkdir(parents=True)
for path in files:
    raw = path.read_bytes(); compress = path.suffix in {'.log', '.patch', '.txt', '.stdout', '.stderr', '.report', '.raw'}
    name = path.name + ('.gz' if compress else ''); stored = gzip.compress(raw, mtime=0) if compress else raw
    (out / name).write_bytes(stored)
    manifest['evidence_files'][name] = {'source_path': str(path), 'sha256': hashlib.sha256(stored).hexdigest(),
                                      'raw_sha256': hashlib.sha256(raw).hexdigest(), 'raw_bytes': len(raw)}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
for name, receipt in manifest['evidence_files'].items():
    stored = (out / name).read_bytes(); raw = gzip.decompress(stored) if name.endswith('.gz') else stored
    assert hashlib.sha256(stored).hexdigest() == receipt['sha256']
    assert hashlib.sha256(raw).hexdigest() == receipt['raw_sha256'] and len(raw) == receipt['raw_bytes']
print('Verified', len(manifest['evidence_files']), 'combined-runtime receipts')
