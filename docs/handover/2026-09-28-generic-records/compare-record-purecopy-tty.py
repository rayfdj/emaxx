from pathlib import Path
import hashlib, json, sys
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
tag = 'source' + sys.argv[1]
digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
base = json.loads((p / (tag + '-base.json')).read_text())
root = Path(base['working_directory'])
source = json.loads((p / (tag + '-manifest.json')).read_text())
results = []
for name in ['purecopy-five-kinds-gc', 'purecopy-sequence-snapshot', 'purecopy-callback-purify-flag', 'record-purecopy-callback']:
    oracle = name + ('-gnu-tty2' if name == 'record-purecopy-callback' else '-gnu')
    subject = tag + '-' + name
    records = [json.loads((p / (label + '.json')).read_text()) for label in [oracle, subject]]
    valid = all(r['exit_code'] == 0 and r['error'] is None and r['inputs_unchanged'] and r['report_exists'] for r in records)
    input_hashes = [r['identities'][r['command'][-1]] for r in records]
    reports = [(p / (label + '.report')).read_bytes() for label in [oracle, subject]]
    valid = valid and all(data.hex() == r['report_hex'] for r, data in zip(records, reports))
    valid = valid and all(digest(Path(path)) == sha for r in records for path, sha in r['identities'].items())
    results.append({'input': name, 'gnu_receipt': oracle + '.json', 'emaxx_receipt': subject + '.json',
                    'same_input': input_hashes[0] == input_hashes[1], 'receipts_valid': valid,
                    'matches': valid and input_hashes[0] == input_hashes[1] and reports[0] == reports[1]})
unchanged = all(digest(root / name) == sha for name, sha in source.items())
summary = {'results': results, 'source_unchanged': unchanged, 'all_match': unchanged and all(r['matches'] for r in results),
           'qualification': 'Exact report bytes. Terminal screens are retained but not compared. Four focused inputs, not a full terminal or frozen pass. Expanded GNU crashes remain failed and separate.'}
with (p / (tag + '-purecopy-tty-comparisons.json')).open('x') as output: json.dump(summary, output, indent=2)
print(json.dumps(summary, indent=2))
raise SystemExit(0 if summary['all_match'] else 1)
