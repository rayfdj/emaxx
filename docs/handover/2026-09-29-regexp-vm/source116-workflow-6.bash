python3 - <<'PY'
import re, shutil
from pathlib import Path
root = Path('target/frozen-ci')
for name in ('module', 'eglot', 'affected', 'remaining', 'frozen', 'ordinary-perf'):
    log = root / f'{name}.log'
    if not log.exists():
        continue
    for value in re.findall(r'^Artifacts: (.+)$', log.read_text(), re.M):
        source = Path(value)
        if source.is_dir():
            shutil.copytree(source, root / 'compat' / source.name,
                ignore=lambda directory, names: [n for n in names if n == 'tools' or n.startswith('runtime-')])
PY
