from pathlib import Path
import hashlib,json,subprocess
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
binaries={'source108':p/'source108-ordinary-emaxx','source114':p/'source114-ordinary-emaxx'}
records=[]
for label,b in binaries.items():
 cmd=['/usr/bin/objdump','--syms','--demangle',str(b)];r=subprocess.run(cmd,capture_output=True)
 lines=[s for s in r.stdout.decode().splitlines() if 'emaxx::lisp::bytecode::vm::run' in s]
 (p/(label+'-vm-symbols.log')).write_text('\n'.join(lines)+'\n')
 record={'label':label,'binary':str(b),'binary_sha256':hashlib.sha256(b.read_bytes()).hexdigest(),'symbol_command':cmd,'symbol_exit_code':r.returncode,'symbols':lines,'prologues':[]}
 for index,line in enumerate(lines):
  if not (line.endswith('::run') or line.endswith('run_frames::{closure#0}') or line.endswith('::run_frames') or line.endswith('::run_fast')):continue
  address=int(line.split()[0],16);cmd=['/usr/bin/objdump','--disassemble','--demangle','--start-address='+hex(address),'--stop-address='+hex(address+160),str(b)];r=subprocess.run(cmd,capture_output=True)
  name=label+'-vm-prologue-'+str(index);(p/(name+'.stdout')).write_bytes(r.stdout);(p/(name+'.stderr')).write_bytes(r.stderr)
  record['prologues'].append({'symbol':line,'command':cmd,'exit_code':r.returncode,'artifact':name})
 records.append(record)
with (p/'vm-source108-to114-prologues.json').open('x') as f:json.dump({'qualification':'Static inspection of ordinary macOS ARM code generation only. Source108 vs114 includes other semantic corrections. Does not identify a live Linux GC root or certify speed.','records':records},f,indent=2)
print(json.dumps([{'label':r['label'],'symbols':r['symbols'],'prologues':r['prologues']} for r in records],indent=2))
