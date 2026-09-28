from pathlib import Path
import gzip, hashlib, json, subprocess, sys

p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
final = int(sys.argv[1])
base = 'a210f90cd65961bf4dc6aa2ccb87b529d46c4360'
out = root / 'docs/handover/2026-09-28-reader-streams'
assert not out.exists()
for stage in ['release-selected', 'fmt', 'check', 'clippy', 'diff-check',
              'ordinary-build', 'ordinary-image']:
    result = json.loads((p / f'source{final}-{stage}-result.json').read_text())
    assert result['exit_code'] == 0 and result['source_unchanged']
assert json.loads((p / f'source{final}-ordinary-reader-comparisons.json').read_text())['all_match']

basefiles = set(subprocess.check_output(
    ['git', 'ls-tree', '-r', '--name-only', base], cwd=root, text=True).splitlines())
files = []
snapshots = {}
for number in sorted(set([73, 74, 75, 76, 78, 80, 83, final])):
    prefix = 'source' + str(number)
    files += [file for file in p.glob(prefix + '*') if file.is_file() and file.suffix in
              ['.json', '.log', '.patch', '.el', '.expected', '.rs', '.txt', '.stdout', '.stderr']]
    software = json.loads((p / (prefix + '-manifest.json')).read_text())
    additions = {name: prefix + '-' + Path(name).name for name in software if name not in basefiles}
    for name, receipt in additions.items():
        assert hashlib.sha256((p / receipt).read_bytes()).hexdigest() == software[name]
    snapshots[prefix] = {'base_commit': base, 'patch': prefix + '.patch.gz',
                         'software_manifest': prefix + '-manifest.json',
                         'untracked_file_sources': additions}
for pattern in ['reader-*', 'bindings83-*', 'destinations83-*', 'source72-known-*', 'source72-reader-inventory*', 'positioned-reader-streams*',
                'callable-reader-consumption*']:
    files += [file for file in p.glob(pattern) if file.is_file() and file.suffix in
              ['.json', '.el', '.expected', '.stdout', '.stderr', '.txt']]
files += [p / name for name in ['package-reader-evidence.py', 'validate-frame-snapshot.py',
          'validate-reader-ordinary.py', 'probe-reader-streams.py', 'probe-reader-stream-extra.py', 'probe-reader-printer-before.py',
          'source72-ordinary-artifacts.json', 'source72-manifest.json']]
manifest = {
    'format_version': 1, 'software_snapshot': final, 'base_commit': base, 'goal_complete': False,
    'qualification': 'Bounded callable reader checkpoint. Earlier compile, fixture-helper and runtime failures are preserved. Source78 passes295 selected release tests and strict static checks; source85 passes359 selected release tests, strict static checks and19 ordinary exact stdout/stderr comparisons. Source80 ordinary15/16 andsource83 release357/358 failures remain preserved. Ordinary same-input comparisons use source-matched GNU, not the pinned Darwin executable. No complete final-source gate, frozen or performance claim.',
    'binaries': 'Not packaged; rebuild. Executable/image hashes identify local provenance only.',
    'source_snapshots': snapshots, 'evidence_files': {}}
out.mkdir(parents=True)
for source in sorted(set(files)):
    relative = source.relative_to(p)
    raw = source.read_bytes()
    compress = source.suffix in ['.log', '.patch', '.txt', '.stdout', '.stderr']
    name = str(relative) + ('.gz' if compress else '')
    stored = gzip.compress(raw, mtime=0) if compress else raw
    destination = out / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(stored)
    manifest['evidence_files'][name] = {
        'source_path': str(Path('target/runtime-goal/resume-2026-09-28') / relative),
        'sha256': hashlib.sha256(stored).hexdigest(),
        'raw_sha256': hashlib.sha256(raw).hexdigest(), 'raw_bytes': len(raw)}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
for name, receipt in manifest['evidence_files'].items():
    stored = (out / name).read_bytes()
    raw = gzip.decompress(stored) if name.endswith('.gz') else stored
    assert hashlib.sha256(stored).hexdigest() == receipt['sha256']
    assert hashlib.sha256(raw).hexdigest() == receipt['raw_sha256']
    assert len(raw) == receipt['raw_bytes']
print('Verified', len(manifest['evidence_files']), 'portable reader receipts')
