from pathlib import Path
import gzip
import hashlib
import json

root = Path('/Users/nbmhqa186/projects/emaxx')
p = root / 'target/runtime-goal/resume-2026-09-28'
out = root / 'docs/handover/2026-09-28-dump-process-controls'
assert not out.exists()
for stage in ['debug-selected', 'release-selected', 'fmt', 'check', 'clippy', 'diff-check']:
    receipt = json.loads((p / ('source109-' + stage + '-result.json')).read_text())
    assert receipt['exit_code'] == 0 and receipt['source_unchanged']
assert json.loads((p / 'source109-partial-output-comparison.json').read_text())['all_match']
inventory = json.loads((p / 'inventory-108-to-109-comparison.json').read_text())
assert not inventory['removed'] and not inventory['added']
files = set()
for pattern in ['source109*', 'inventory-108-to-109-*', 'source108-suspended-ordinary*']:
    files.update(path for path in p.glob(pattern) if path.is_file()
                 and 'full-gate' not in path.name
                 and path.suffix in {'.json', '.log', '.patch', '.stdout', '.stderr'})
for name in ['launch-source109.py', 'validate-frame-snapshot-v2.py',
             'compare-source-inventories.py', 'source108-suspended-bytecode-original.el',
             'probe-suspended-bytecode-original.py', 'process-partial-output-control.el',
             'probe-process-partial-output.py', 'package-source109-controls.py']:
    files.add(p / name)
manifest = {
    'format_version': 1, 'goal_complete': False, 'software_snapshot': 109,
    'base': json.loads((p / 'source109-base.json').read_text()),
    'patch': 'source109.patch.gz', 'source_manifest': 'source109-manifest.json',
    'qualification': 'Only Rust tests changed: capture dump roots before printing the context result, and await all process bytes within the original60s deadline. Expected values, root comparisons, selectors, ignores and inventory unchanged. GNU source print.c:print_prepare and process.c:Faccept_process_output explain both corrections. Same-input ordinary partial-output handshake matches GNU exactly.52debug/52release and all strict static checks pass. Full macOS running; Linux suspended-bytecode failure unresolved. No full or frozen pass.',
    'evidence_files': {},
}
out.mkdir(parents=True)
for path in sorted(files):
    raw = path.read_bytes()
    compressed = path.suffix not in {'.json', '.py', '.el'}
    name = str(path.relative_to(p)) + ('.gz' if compressed else '')
    stored = gzip.compress(raw, mtime=0) if compressed else raw
    (out / name).write_bytes(stored)
    manifest['evidence_files'][name] = {
        'source_path': str(path), 'sha256': hashlib.sha256(stored).hexdigest(),
        'raw_sha256': hashlib.sha256(raw).hexdigest(), 'raw_bytes': len(raw),
    }
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
for name, receipt in manifest['evidence_files'].items():
    stored = (out / name).read_bytes()
    raw = gzip.decompress(stored) if name.endswith('.gz') else stored
    assert hashlib.sha256(stored).hexdigest() == receipt['sha256']
    assert hashlib.sha256(raw).hexdigest() == receipt['raw_sha256']
print('Packaged and verified', len(files), 'source109 control receipts')
