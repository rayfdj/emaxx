from pathlib import Path
import hashlib, json, subprocess, sys, time
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
before, after = sys.argv[1:]
inventories = []
for number in [before, after]:
    binary = p / ('source' + number + '-debug-libtest')
    name = 'inventory-' + before + '-to-' + after + '-source' + number
    command = [str(binary), '--list']
    digest = lambda: hashlib.sha256(binary.read_bytes()).hexdigest()
    sha = digest(); start = time.monotonic()
    completed = subprocess.run(command, capture_output=True, check=False)
    for suffix, data in [('stdout', completed.stdout), ('stderr', completed.stderr)]:
        with (p / (name + '.' + suffix)).open('xb') as output: output.write(data)
    names = [line.removesuffix(': test') for line in completed.stdout.decode().splitlines() if line.endswith(': test')]
    assert len(names) == len(set(names)) and names
    record = {'command': command, 'working_directory': str(Path.cwd()), 'exit_code': completed.returncode,
              'binary_sha256': sha, 'binary_unchanged': sha == digest(), 'elapsed_seconds': time.monotonic() - start,
              'tests': len(names), 'qualification': 'Inventory enumeration only; not test execution or a pass for listed tests.'}
    with (p / (name + '.json')).open('x') as output: json.dump(record, output, indent=2)
    assert completed.returncode == 0 and record['binary_unchanged']
    inventories.append(set(names))
summary = {'before': before, 'after': after, 'before_count': len(inventories[0]), 'after_count': len(inventories[1]),
           'removed': sorted(inventories[0] - inventories[1]), 'added': sorted(inventories[1] - inventories[0])}
with (p / ('inventory-' + before + '-to-' + after + '-comparison.json')).open('x') as output: json.dump(summary, output, indent=2)
print(json.dumps(summary, indent=2))
raise SystemExit(1 if summary['removed'] else 0)
