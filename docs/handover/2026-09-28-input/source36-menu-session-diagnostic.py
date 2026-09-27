"""Independent session diagnostics; never touch ttydiff's shared fixtures.
This is not the full ttydiff gate and does not replace its results.
"""
from pathlib import Path
import hashlib,json,os,sys,tempfile,time
sys.path.insert(0,str(Path('tools').resolve()))
import ttydiff
p=Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
binary=Path('target/menu-validation/release/emaxx').resolve();gnu=Path('/Users/nbmhqa186/projects/emacs/src/emacs');lisp=Path('/Users/nbmhqa186/projects/emacs/lisp')
loadpath=os.pathsep.join([str(lisp)]+[str(v) for v in sorted(lisp.iterdir()) if v.is_dir()])
for name in ['mouse-bar-click','f10-cycle']:
 case=next(c for c in ttydiff.SCENARIOS if c[0]==name)
 root=Path(tempfile.mkdtemp(prefix='source36-menu-diag-',dir=p));home=root/'home';home.mkdir();runtime=root/'tmp';runtime.mkdir();target=root/(name+'.dat');target.write_text(case[1]);trace=p/('source36-'+name+'-trace.log')
 commands={'gnu':[str(gnu),'-nw','-Q','--eval',ttydiff.gnu_no_window_setup(str(lisp)),str(target)],'emaxx':[str(binary),str(target)]}
 overrides={'gnu':{'HOME':str(home),'TMPDIR':str(runtime)},'emaxx':{'HOME':str(home),'TMPDIR':str(runtime),'EMACSLOADPATH':loadpath,'EMAXX_TTY_LOG':str(trace)}}
 sessions={};raw={};rows=[];start=time.monotonic()
 try:
  for editor in ['gnu','emaxx']:
   session=ttydiff.Session(commands[editor],overrides[editor]);sessions[editor]=session;raw[editor]=[];feed=session.screen.feed
   def capture(data,editor=editor,feed=feed):raw[editor].append(data);feed(data)
   session.screen.feed=capture
  for session in sessions.values():
   session.wait_boot(ttydiff.STARTUP_WAIT_SECONDS);session.wait_for_screen_text(target.name[:16],ttydiff.STARTUP_WAIT_SECONDS,minimum=.5)
  for index,item in enumerate(case[2]):
   command=ttydiff.normalize_action(item,index);final=index+1==len(case[2]);settle,quiet=ttydiff.action_timing(name,index,final,command);explicit=command.settle is not None or (not command.checkpoint and final)
   for session in sessions.values():session.send(command.keys,settle=settle,quiet=quiet,explicit_settle=explicit)
   rows.append({'index':index,'keys_hex':command.keys.hex(),'settle':settle,'quiet':quiet,'explicit_settle':explicit,'screens':{editor:s.screen.lines() for editor,s in sessions.items()}})
  for session in sessions.values():session.drain(1.0)
  differences,length=ttydiff.screen_divergences(sessions['gnu'].screen,sessions['emaxx'].screen)
  receipt={'purpose':'isolated diagnostic with original per-action timing and comparison, plus Emaxx trace logging; not a full gate result','commands':commands,'environment_overrides':overrides,'fixture':str(target),'actions':rows,'final_screens':{editor:s.screen.lines() for editor,s in sessions.items()},'differences':differences,'compared_rows':length,'elapsed_seconds_diagnostic_only':time.monotonic()-start,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'image_sha256':hashlib.sha256(binary.with_suffix('.pdmp').read_bytes()).hexdigest(),'gnu_binary_sha256':hashlib.sha256(gnu.read_bytes()).hexdigest()}
  (p/('source36-'+name+'-session-diagnostic.json')).write_text(json.dumps(receipt,indent=2)+'\n');print(name,'differences',json.dumps(differences),flush=True)
 finally:
  for editor,session in sessions.items():
   (p/('source36-'+name+'-'+editor+'.raw')).write_bytes(b''.join(raw[editor]));session.close()
