from pathlib import Path
import gzip, hashlib, json
root = Path('/Users/nbmhqa186/projects/emaxx')
p = root / 'target/runtime-goal/resume-2026-09-28'
source = p / 'linux-c3204ce5-artifacts' / 'ordinary-linux-c3204ce5359fcb87e30dace71af62a43d54fbbea'
out = root / 'docs/handover/2026-09-28-linux-function-cells'
assert not out.exists()
run = json.loads((p / 'linux-c3204ce5-run.json').read_text())
assert run['conclusion'] == 'failure' and run['headSha'] == 'c3204ce5359fcb87e30dace71af62a43d54fbbea'
summary = json.loads((source / 'rust/summary.json').read_text())
assert summary['status'] == 'failed' and not summary['git']['dirty']
files = {str(path.relative_to(source)): path for path in source.rglob('*') if path.is_file()}
files.update({path.name: path for path in [p / 'linux-c3204ce5-run.json',
    p / 'linux-c3204ce5-publication.json', p / 'linux-c3204ce5-failed.log', Path(__file__)]})
manifest = {'format_version': 1, 'goal_complete': False, 'run_url': run['url'],
    'source_commit': run['headSha'], 'status': 'failed',
    'qualification': 'Existing Linux workflow validation=rust. ABI, formatting, compiler/Clippy and build steps pass. First three library groups total 999 passes and one failure: set-text-conversion-style has no implementation. The char-table key-range failure from803e4326 is repaired. Seven later groups and subsequent Cargo stages did not execute. No full/frozen/final-source validation claim.',
    'evidence_files': {}}
out.mkdir(parents=True)
for relative, path in sorted(files.items()):
    raw = path.read_bytes()
    compress = path.suffix in ['.log', '.txt']
    name = relative + ('.gz' if compress else '')
    stored = gzip.compress(raw, mtime=0) if compress else raw
    destination = out / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(stored)
    manifest['evidence_files'][name] = {'source_path': str(path), 'sha256': hashlib.sha256(stored).hexdigest(),
                                     'raw_sha256': hashlib.sha256(raw).hexdigest(), 'raw_bytes': len(raw)}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
for name, receipt in manifest['evidence_files'].items():
    stored = (out / name).read_bytes()
    raw = gzip.decompress(stored) if name.endswith('.gz') else stored
    assert hashlib.sha256(stored).hexdigest() == receipt['sha256']
    assert hashlib.sha256(raw).hexdigest() == receipt['raw_sha256']
    assert len(raw) == receipt['raw_bytes']
print('Verified', len(manifest['evidence_files']), 'Linux receipts')
