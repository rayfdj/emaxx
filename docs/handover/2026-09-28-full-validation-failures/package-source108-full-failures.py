from pathlib import Path
import gzip
import hashlib
import json

root = Path('/Users/nbmhqa186/projects/emaxx')
p = root / 'target/runtime-goal/resume-2026-09-28'
out = root / 'docs/handover/2026-09-28-full-validation-failures'
assert not out.exists()
files = set()
for directory in ['full-gate-source108', 'linux-f7f32970-artifacts', 'linux-e9cd46bd-artifacts']:
    files.update(path for path in (p / directory).rglob('*') if path.is_file())
for pattern in ['source108-full-gate-*.json', 'source108-full-ttydiff*',
                'source108-failed-selectors-with-socket-access*']:
    files.update(path for path in p.glob(pattern) if path.is_file())
for name in ['source108-full-gate.log', 'run-source108-full-gate.py',
             'run-source108-full-tty.py', 'recheck-source108-full-failures.py',
             'linux-f7f32970-failed.log', 'linux-f7f32970-failed.stderr',
             'linux-f7f32970-log-fetch.json',
             'source108-linux-downloaded-artifact-hashes.json',
             'source108-dump-root-difference.json',
             'package-source108-full-failures.py']:
    files.add(p / name)
manifest = {
    'format_version': 1, 'goal_complete': False,
    'source_checkpoint': 'f7f32970fee8ea5dc9f99187f79ad6ddda4cbff6',
    'software_snapshot': '../2026-09-28-combined-records/manifest.json',
    'qualification': 'Completed failed full gates and incomplete terminal inventory. macOS: 2154 passed,13 failed; later groups and Cargo stages unexecuted. Twelve socket failures pass unchanged when loopback is permitted; dump-state comparison still fails. Linux combined: 2172 passed,3 failed; older text-conversion source:2163 passed,1 failed. Nested child GC failure preserved verbatim; strict result parser rejects extra summary. Terminal186 scenarios match,187th fails startup; remaining36 never started. These are not full or frozen passes. GNU candidate still differs from the frozen Darwin pin.',
    'linux_runs': [
        'https://github.com/rayfdj/emaxx/actions/runs/36405772114',
        'https://github.com/rayfdj/emaxx/actions/runs/36400765916',
    ],
    'evidence_files': {},
}
out.mkdir(parents=True)
for path in sorted(files):
    raw = path.read_bytes()
    compressed = path.suffix not in {'.json', '.py'}
    name = str(path.relative_to(p)) + ('.gz' if compressed else '')
    stored = gzip.compress(raw, mtime=0) if compressed else raw
    target = out / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(stored)
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
    assert len(raw) == receipt['raw_bytes']
print('Packaged and verified', len(files), 'failed full-validation receipts')
