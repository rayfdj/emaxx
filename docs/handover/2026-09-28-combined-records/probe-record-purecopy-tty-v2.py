from pathlib import Path
import hashlib, json, os, sys, time
root=Path('/Users/nbmhqa186/projects/emaxx')
p=root/'target/runtime-goal/resume-2026-09-28'
sys.path.insert(0,str(root/'tools'))
import ttydiff
tag=sys.argv[1];binary=Path(sys.argv[2]);fixture=Path(sys.argv[3])
report=p/(tag+'.report');assert not report.exists()
digest=lambda path:hashlib.sha256(path.read_bytes()).hexdigest()
inputs=[binary,binary.with_suffix('.pdmp'),fixture,root/'tools/ttydiff.py',Path(__file__)]
identity={str(path):digest(path) for path in inputs}
command=[str(binary),'-nw','-Q','-l',str(fixture)]
environment={'EMAXX_RECORD_PROBE_REPORT':str(report)}
session=ttydiff.Session(command,environment);raw=[];feed=session.screen.feed
def capture(data):raw.append(data);feed(data)
session.screen.feed=capture
status=None;error=None;started=time.monotonic()
try:
    session.wait_boot(30)
    session.wait_for_screen_text('record-purecopy-ready',30)
    session.send(b'\x03r',settle=1)
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        pid,value=os.waitpid(session.pid,os.WNOHANG)
        if pid:
            status=value
            break
        session.drain(0.5,minimum=0.25)
    if status is None:raise TimeoutError('command did not exit within 120 seconds')
except Exception as exc:error=repr(exc)
finally:
    with (p/(tag+'.raw')).open('xb') as output:output.write(b''.join(raw))
    record={'command':command,'environment_overrides':environment,
            'working_directory':str(Path.cwd()),'identities':identity,
            'inputs_unchanged':all(digest(Path(path))==sha for path,sha in identity.items()),
            'wait_status':status,'exit_code':os.waitstatus_to_exitcode(status) if status is not None else None,
            'elapsed_seconds':time.monotonic()-started,'error':error,
            'report_exists':report.exists(),'report_hex':report.read_bytes().hex() if report.exists() else None,
            'screen':session.screen.lines(),
            'qualification':'Focused ordinary TTY diagnostic with unchanged Lisp input; no full terminal or pinned-oracle claim.'}
    with (p/(tag+'.json')).open('x') as output:json.dump(record,output,indent=2)
    print(json.dumps(record,indent=2),flush=True)
    session.close()
raise SystemExit(0 if error is None and record['exit_code']==0 and report.exists() and record['inputs_unchanged'] else 1)
