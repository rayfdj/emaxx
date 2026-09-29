from pathlib import Path
import hashlib,importlib.util,json,os,subprocess,sys,time
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
r=Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
sha=lambda f:hashlib.sha256(f.read_bytes()).hexdigest()
artifacts=json.loads((p/'source108-ordinary-artifacts.json').read_text())
manifest=json.loads((p/'source108-manifest.json').read_text())
unchanged=lambda:all(sha(r/f)==h for f,h in manifest.items())
assert unchanged()
b=Path(artifacts['binary']);g=Path('/private/tmp/emaxx-runtime-gnu-20260928/src/emacs')
assert sha(b)==artifacts['binary_sha256'] and sha(b.with_suffix('.pdmp'))==artifacts['image_sha256']
spec=importlib.util.spec_from_file_location('ttydiff_diagnostic',r/'tools/ttydiff.py');m=importlib.util.module_from_spec(spec);sys.modules[spec.name]=m;spec.loader.exec_module(m)
sessions=[];original_init=m.Session.__init__;original_report=m.report_comparison
started=time.monotonic();records=[]
def init(self,argv,environment):
 original_init(self,argv,environment);sessions.append((self,argv))
m.Session.__init__=init
def screen(s):return {'lines':s.lines(),'attrs':s.attr_rows(),'cursor':[s.row,s.col]}
def report(label,gnu,emaxx):
 record={'label':label,'elapsed_seconds':time.monotonic()-started,'gnu':screen(gnu),'emaxx':screen(emaxx)}
 record['divergences']=m.screen_divergences(gnu,emaxx)[0]
 result=original_report(label,gnu,emaxx);record['matched']=result;records.append(record)
 (p/'source108-package-diagnostic-screens.json').write_text(json.dumps(records,indent=2)+'\n')
 if not result:
  # The original comparison has already failed. Further observation cannot
  # turn it into a pass or allow later scenario actions to execute.
  subject=sessions[-1][0]
  command=['/usr/bin/sample',str(subject.pid),'2','1','-file',str(p/'source108-package-after-failure.sample')]
  sampled=subprocess.run(command,capture_output=True)
  (p/'source108-package-after-failure-sample.json').write_text(json.dumps({'command':command,'exit_code':sampled.returncode,'stdout':sampled.stdout.decode(errors='replace'),'stderr':sampled.stderr.decode(errors='replace')},indent=2))
  subject.drain(20.0,quiet=1.0,minimum=20.0)
  (p/'source108-package-after-failure-late-screen.json').write_text(json.dumps(screen(subject.screen),indent=2))
 return result
m.report_comparison=report
home=p/'source108-package-diagnostic-home';home.mkdir(exist_ok=False)
overrides={'HOME':str(home),'EMAXX_TTYDIFF_REQUIRE':'1'}
os.environ.update(overrides)
argv=['tools/ttydiff.py',str(b),str(g),'/private/tmp/emaxx-runtime-gnu-20260928/lisp','package-menu-install-refresh-remove']
inputs={str(f):sha(f) for f in [b,b.with_suffix('.pdmp'),g,g.with_suffix('.pdmp'),r/'tools/ttydiff.py',Path(__file__)]}
with (p/'source108-package-diagnostic-command.json').open('x') as f:json.dump({'command':argv,'cwd':str(r),'environment_overrides':overrides,'inputs':inputs,'scope':'Observational wrapper over unchanged selected scenario; original actions/timing/comparisons. Saves full checkpoint screens. On failure only, sample and late screen are post-failure diagnostics; never counted as a pass. The prior full 223-scenario failure remains.'},f,indent=2)
sys.argv=argv;os.chdir(r);rc=0
try:m.main()
except SystemExit as e:rc=e.code
finally:
 result={'exit_code':rc,'elapsed_seconds':time.monotonic()-started,'source_unchanged':unchanged(),'inputs_unchanged':all(sha(Path(f))==h for f,h in inputs.items()),'checkpoint_count':len(records),'failed_checkpoints':[v['label'] for v in records if not v['matched']]}
 with (p/'source108-package-diagnostic-result.json').open('x') as f:json.dump(result,f,indent=2)
 print(json.dumps(result),flush=True)
raise SystemExit(rc)
