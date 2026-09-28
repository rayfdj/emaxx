from pathlib import Path
import argparse, hashlib, importlib.util, json, os, signal, subprocess, sys, threading, time
root = Path('/private/tmp/emaxx-runtime-audit-followup/emaxx')
p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
spec = importlib.util.spec_from_file_location('core_runtime_perf', root / 'tools/core_runtime_perf.py')
core = importlib.util.module_from_spec(spec); spec.loader.exec_module(core)
out = p / 'source105-core-profiles'
out.mkdir(exist_ok=False)
args = argparse.Namespace(action='pilot', source=Path('/private/tmp/emaxx-runtime-gnu-20260928'),
    oracle=Path('/private/tmp/emaxx-runtime-gnu-20260928/src/emacs'),
    emaxx=Path('/private/tmp/emaxx-runtime-audit-validation/release/emaxx'),
    output=out, native_dir=p / 'source105-core-native', calibration=None, diagnostic=True,
    case=['interpreted-lexical', 'interpreted-calls', 'bytecode-calls', 'native-to-interpreted'], pilot_runner='both')
identity = {str(f): hashlib.sha256(f.read_bytes()).hexdigest() for f in [Path(__file__), Path('/usr/bin/sample')]}
core.write_json(out / 'profiling.json', {'kind':'diagnostic_profile', 'command':sys.argv,
    'source_commit':'1ed63a559bef9304cadf29afce4f4345d4020150', 'inputs':identity,
    'qualification':'Four locked workload inputs through the unchanged normal CLI and Lisp helper. Existing pilot mode uses one observation per process, no warmup; it is not the required full timing suite. Stack sampling includes startup, preparation and body. Builds/tests run concurrently; sampled elapsed times cannot establish performance. Missing allocation counters remain unavailable, never zero.'})

def profiled_process(argv, directory, env, timeout):
    directory.mkdir(parents=True, exist_ok=False)
    started = time.monotonic(); epoch = time.time(); load_before = os.getloadavg(); outcome = {}
    with (directory/'stdout.txt').open('wb') as stdout, (directory/'stderr.txt').open('wb') as stderr, (directory/'sample-driver.log').open('wb') as sample_log:
        child = subprocess.Popen(argv, cwd=core.ROOT, env=env, stdout=stdout, stderr=stderr, start_new_session=True)
        sample_command = ['/usr/bin/sample', str(child.pid), '10', '1', '-mayDie', '-file', str(directory/'sample.txt')]
        sampler = subprocess.Popen(sample_command, stdout=sample_log, stderr=subprocess.STDOUT)
        def reap():
            _, status, usage = os.wait4(child.pid, 0)
            child.returncode = os.waitstatus_to_exitcode(status)
            outcome.update(exit=child.returncode, total_seconds=time.monotonic()-started,
                user_seconds=usage.ru_utime, system_seconds=usage.ru_stime, peak_rss_bytes=int(usage.ru_maxrss))
        waiter=threading.Thread(target=reap); waiter.start(); waiter.join(timeout)
        timed_out=waiter.is_alive()
        if timed_out:
            try: os.killpg(child.pid,signal.SIGKILL)
            except ProcessLookupError: pass
            waiter.join()
        sampler_timeout=False
        try: sample_status=sampler.wait(timeout=15)
        except subprocess.TimeoutExpired:
            sampler_timeout=True; sampler.kill(); sample_status=sampler.wait()
    ready=[line for line in (directory/'stdout.txt').read_text(errors='replace').splitlines() if line.startswith('CORE-PERF-READY ')]
    startup=float(ready[0].split()[1])-epoch if len(ready)==1 else None
    record=dict(command=argv,environment={key:env[key] for key in ('HOME','LANG','LC_ALL','TZ','EMACS_TEST_DIRECTORY')},
        timeout=timed_out,startup_seconds=startup,load_average_before=load_before,load_average_after=os.getloadavg(),**outcome)
    core.write_json(directory/'process.json',record)
    core.write_json(directory/'sampling.json',{'command':sample_command,'exit_code':sample_status,
        'timeout':sampler_timeout,'report_exists':(directory/'sample.txt').is_file(),
        'qualification':'Raw whole-process stack sample; process results and modes are validated separately by the unchanged locked-suite validator.'})
    return record

core.run_process=profiled_process
try:
    result=core.measure(args,{'oracle':args.oracle,'emaxx':args.emaxx})
except (ValueError,OSError,subprocess.SubprocessError) as error:
    core.write_json(out/'failure.json',{'error':str(error)});raise
finally:
    unchanged=all(hashlib.sha256(Path(f).read_bytes()).hexdigest()==sha for f,sha in identity.items())
    core.write_json(out/'profile-inputs-final.json',{'unchanged':unchanged})
raise SystemExit(result if unchanged else 2)
