from pathlib import Path
import gzip, hashlib, json

root = Path('/Users/nbmhqa186/projects/emaxx')
p = root / 'target/runtime-goal/resume-2026-09-28'
out = root / 'docs/handover/2026-09-28-native-configuration'
assert not out.exists()
for stage in ['native-build', 'ordinary-wrapper', 'native-identity']:
    result = json.loads((p / ('source96-' + stage + '-result.json')).read_text())
    assert result['exit_code'] == 0 and result['source_and_inputs_unchanged'], stage
assert all(json.loads((p / 'source96-native-final-identity.json').read_text()).values())
assert json.loads((p / 'gnu-darwin256-native-abi-formatted-comparison.json').read_text())['formatted_native_abi_matches']
assert not json.loads((p / 'gnu-darwin256-candidate-identity.json').read_text())['matches_pinned_binary']
assert (p / 'source96-native-identity.log').read_text().count('native-comp identity PASS:') == 9
assert (p / 'source96.patch').read_bytes() == b''
files = [path for prefix in ['source96', 'gnu-darwin256'] for path in p.glob(prefix + '*')
         if path.is_file() and path.suffix in ['.json', '.log', '.patch', '.rs', '.stdout', '.stderr']]
files += [p / name for name in ['validate-gnu256-native.py', 'check-gnu-candidate.py',
                              'validate-frame-snapshot.py', 'package-native96-evidence.py']]
files += [Path('/private/tmp/emaxx-runtime-gnu-20260928-build.json')]
manifest = {
    'format_version': 1,
    'goal_complete': False,
    'software_snapshot': 96,
    'source_snapshots': {'source96': {
        'base_commit': json.loads((p / 'source96-base.json').read_text())['commit'],
        'patch': 'source96.patch.gz', 'software_manifest': 'source96-manifest.json',
        'untracked_file_sources': {}}},
    'qualification': 'Diagnostic native identity on unchanged function-cell source, using a freshly built pristine GNU reference whose formatted ABI matches the committed table. All nine unchanged GNU fixtures pass exact raw artifact comparison. Neither this candidate nor the older GNU binary matches the frozen executable pin. No source, ABI table, pin, selector, timeout or comparison change. The earlier source72 full gate remains failed and this is not a full/frozen/final-source certificate.',
    'artifact_retention_limit': 'The unchanged native integration test deletes its temporary generated artifacts after success. Raw test output, commands, all software manifests, executable/image hashes, and build logs are retained; generated native artifact bytes are not retained by that test.',
    'evidence_files': {}}
out.mkdir(parents=True)
for path in sorted(set(files)):
    raw = path.read_bytes()
    compress = path.suffix in ['.log', '.patch', '.rs', '.stdout', '.stderr']
    name = path.name + ('.gz' if compress else '')
    stored = gzip.compress(raw, mtime=0) if compress else raw
    (out / name).write_bytes(stored)
    manifest['evidence_files'][name] = {'source_path': str(path),
        'sha256': hashlib.sha256(stored).hexdigest(),
        'raw_sha256': hashlib.sha256(raw).hexdigest(), 'raw_bytes': len(raw)}
(out / 'manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
for name, receipt in manifest['evidence_files'].items():
    stored = (out / name).read_bytes()
    raw = gzip.decompress(stored) if name.endswith('.gz') else stored
    assert hashlib.sha256(stored).hexdigest() == receipt['sha256']
    assert hashlib.sha256(raw).hexdigest() == receipt['raw_sha256']
    assert len(raw) == receipt['raw_bytes']
print('Verified', len(manifest['evidence_files']), 'native configuration receipts')
