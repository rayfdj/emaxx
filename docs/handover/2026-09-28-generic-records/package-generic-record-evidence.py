from pathlib import Path
import gzip, hashlib, json, subprocess
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
out = root / 'docs/handover/2026-09-28-generic-records'
assert not out.exists()
for stage in ['debug-selected', 'release-selected', 'fmt', 'check', 'clippy', 'diff-check', 'ordinary-build', 'ordinary-image']:
    result = json.loads((p / ('source103-' + stage + '-result.json')).read_text())
    assert result['exit_code'] == 0 and result['source_unchanged'], stage
ordinary = json.loads((p / 'source103-ordinary-record-comparisons.json').read_text())
assert ordinary['all_match'] and len(ordinary['expected_inventory']) == len(ordinary['results']) == 29
assert json.loads((p / 'source103-dump-locale-comparisons.json').read_text())['all_match']
assert json.loads((p / 'source103-purecopy-tty-comparisons.json').read_text())['all_match']
assert json.loads((p / 'source98-debug-selected-result.json').read_text())['exit_code'] != 0
assert not json.loads((p / 'source99-format-overlap-check.json').read_text())['source_matches_snapshot']
snapshots, files = {}, []
suffixes = {'.json', '.log', '.patch', '.rs', '.txt', '.stdout', '.stderr', '.el', '.expected', '.report', '.raw'}
for number in [90, 91, 92, 93, 94, 95, 97, 98, 99, 101, 102, 103]:
    tag = 'source' + str(number)
    base = json.loads((p / (tag + '-base.json')).read_text())['commit']
    tracked = set(subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', base], cwd=root, text=True).splitlines())
    software = json.loads((p / (tag + '-manifest.json')).read_text())
    additions = {name: tag + '-' + Path(name).name for name in software if name not in tracked}
    for name, receipt in additions.items(): assert hashlib.sha256((p / receipt).read_bytes()).hexdigest() == software[name]
    snapshots[tag] = {'base_commit': base, 'patch': tag + '.patch.gz',
                      'software_manifest': tag + '-manifest.json', 'untracked_file_sources': additions}
    files += [path for path in p.glob(tag + '*') if path.is_file() and path.suffix in suffixes]
for pattern in ['purecopy-*', 'record-purecopy-*', 'source87*record*', 'generic-record-*', 'message-callback-special-bindings-*', 'inventory-91-to-103-*']:
    files += [path for path in p.glob(pattern) if path.is_file() and path.suffix in suffixes]
for name in ['validate-frame-snapshot.py', 'validate-record-ordinary.py', 'validate-record-ordinary-v2.py',
             'probe-record-purecopy-tty.py', 'probe-record-purecopy-tty-v2.py', 'probe-dump-locale.py',
             'compare-source-inventories.py', 'compare-record-purecopy-tty.py', 'probe-generic-record-before.py', 'probe-record-properties-before.py', 'probe-text-conversion-state.py',
             'package-generic-record-evidence.py', 'dump-locale-quoting.el',
             'positioned-reader-streams-before.el', 'positioned-reader-streams-v2.el',
             'callable-reader-consumption-before.el', 'reader-printer-destinations.el']:
    files.append(p / name)
manifest = {
    'format_version': 1, 'software_snapshot': 103, 'goal_complete': False,
    'qualification': 'Inline PVEC_RECORD payloads replace generic host records. Selected debug/release/static, ordinary exact output, locale and terminal report controls pass on source103; combined function-cell and final-source validation remain required. No performance or frozen certificate. Local GNU source matched only. Earlier failures are retained; source99 formatter overlap invalidates its source identity. Temporary tracing in101/102 is absent from103. GNU expanded hash callback abort and mutation+GC pure-storage overflow are not comparison passes.',
    'source_snapshots': snapshots, 'evidence_files': {}}
assert all(path.is_file() for path in files)
out.mkdir(parents=True)
for path in sorted(set(files)):
    raw = path.read_bytes()
    compress = path.suffix in {'.log', '.patch', '.txt', '.stdout', '.stderr', '.raw', '.report'}
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
print('Verified', len(manifest['evidence_files']), 'generic-record receipts')
