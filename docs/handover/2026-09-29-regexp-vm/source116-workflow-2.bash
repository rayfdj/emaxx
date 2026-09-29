./autogen.sh
python3 - <<'PY'
import json, re, shlex, subprocess
from pathlib import Path
text = Path('../emaxx/src/lisp/native_comp/generated_native_subrs_x86_64_unknown_linux_gnu.rs').read_text()
options = re.search(r'NATIVE_ABI_SYSTEM_CONFIGURATION_OPTIONS: &str = ("[^"\n]*");', text)[1]
subprocess.run(['./configure', *shlex.split(json.loads(options))], check=True)
PY
make -j2
