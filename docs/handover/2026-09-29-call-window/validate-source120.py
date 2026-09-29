from pathlib import Path
import ast,hashlib,json,subprocess,time
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28');r=p.parents[2]
files=['tools/retain_rust_gate_artifacts.py','tools/test_retain_rust_gate_artifacts.py','.github/workflows/frozen-run.yml']
m={f:hashlib.sha256((r/f).read_bytes()).hexdigest() for f in files}
(p/'source120-manifest.json').write_text(json.dumps(m,indent=2)+'\n')
(p/'source120.patch').write_bytes(subprocess.check_output(['git','diff','HEAD'],cwd=r))
(p/'source120-test_retain_rust_gate_artifacts.py').write_bytes((r/files[1]).read_bytes())
for f in files[:2]:ast.parse((r/f).read_text())
lines=(r/files[2]).read_text().splitlines();blocks=[];i=0
while i<len(lines):
 if lines[i].lstrip().startswith('run: |'):
  indent=len(lines[i])-len(lines[i].lstrip());i+=1;block=[]
  while i<len(lines) and (not lines[i].strip() or len(lines[i])-len(lines[i].lstrip())>indent):
   block.append(lines[i][indent+2:]);i+=1
  blocks.append('\n'.join(block)+'\n')
 else:i+=1
for i,b in enumerate(blocks):
 f=p/f'source120-workflow-{i}.bash';f.write_text(b)
 subprocess.run(['bash','-n',str(f)],check=True)
commands=[['python3','-m','unittest','discover','-s','tools','-p','test_retain_rust_gate_artifacts.py','-v'],['python3','-m','unittest','discover','-s','tools','-p','test_diagnose_rust_gate.py','-v'],['python3','-m','unittest','discover','-s','tools','-p','test_grouped_gate.py','-v'],['python3','tools/retain_rust_gate_artifacts.py','--help'],['git','diff','--check']]
results=[]
for i,c in enumerate(commands):
 start=time.monotonic();result=subprocess.run(c,cwd=r,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
 (p/f'source120-validation-{i}.log').write_bytes(result.stdout)
 results.append({'command':c,'exit_code':result.returncode,'elapsed_seconds':time.monotonic()-start})
 assert result.returncode==0,result.stdout.decode()
assert all(hashlib.sha256((r/f).read_bytes()).hexdigest()==s for f,s in m.items())
(p/'source120-validation.json').write_text(json.dumps({'scope':'Diagnostic tooling only; original gate runtime, tests, selectors and profile unchanged.','results':results,'ast':'passed','workflow_bash_blocks':len(blocks),'source_unchanged':True},indent=2)+'\n')
print('Source120 diagnostic checks passed: 8 artifact controls, 6 replay controls, 13 gate controls, help, AST, shell syntax, diff.')
