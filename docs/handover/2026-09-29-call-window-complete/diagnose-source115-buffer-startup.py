from pathlib import Path
import hashlib
import importlib.util
import json
import os
import sys
import time

p = Path('/Users/nbmhqa186/projects/emaxx/target/runtime-goal/resume-2026-09-28')
root = Path('/private/tmp/emaxx-runtime-reader-streams/emaxx')
output = p / 'source115-buffer-startup-diagnostic'
output.mkdir(exist_ok=False)
spec = importlib.util.spec_from_file_location('ttydiff', root / 'tools/ttydiff.py')
tty = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = tty
spec.loader.exec_module(tty)
base = tty.Session
sessions = []


class LoggedSession(base):
    def __init__(self, argv, env_extra):
        super().__init__(argv, env_extra)
        self.label = len(sessions)
        self.record = {'command': argv, 'pid': self.pid, 'events': []}
        self.chunks = []
        sessions.append(self)
        feed = self.screen.feed

        def capture(data):
            self.chunks.append(data)
            self.record['events'].append({
                'kind': 'output', 'wall': time.time(), 'monotonic': time.monotonic(),
                'bytes': len(data),
            })
            feed(data)

        self.screen.feed = capture

    def wait_boot(self, timeout):
        return self.observe('wait_boot', super().wait_boot, timeout)

    def wait_for_screen_text(self, text, timeout, minimum=0.5):
        return self.observe('wait_for_screen_text', super().wait_for_screen_text,
                            text, timeout, minimum)

    def observe(self, kind, method, *arguments):
        wall, mono = time.time(), time.monotonic()
        event = {'kind': kind, 'arguments': arguments, 'wall_start': wall,
                 'monotonic_start': mono}
        try:
            result = method(*arguments)
            event['status'] = 'completed'
            return result
        except Exception as error:
            event.update(status='failed', error=repr(error), screen=self.screen.lines())
            raise
        finally:
            event.update(wall_seconds=time.time() - wall,
                         monotonic_seconds=time.monotonic() - mono)
            self.record['events'].append(event)


tty.Session = LoggedSession
binary = Path('/private/tmp/emaxx-runtime-stream-validation/release/emaxx')
oracle = Path('/private/tmp/emaxx-runtime-gnu-20260928/src/emacs')
inputs = [binary, binary.with_suffix('.pdmp'), oracle, oracle.with_suffix('.pdmp'),
          root / 'tools/ttydiff.py', Path(__file__)]
identities = {str(q): hashlib.sha256(q.read_bytes()).hexdigest() for q in inputs}
home = output / 'home'
home.mkdir()
os.environ.update(HOME=str(home), EMAXX_TTYDIFF_REQUIRE='1')
sys.argv = [str(root / 'tools/ttydiff.py'), str(binary), str(oracle),
            '/private/tmp/emaxx-runtime-gnu-20260928/lisp', 'buffer-list']
metadata = {
    'command': sys.argv, 'inputs': identities,
    'qualification': 'Original scenario, timeout, waits and comparisons; added raw-output and wall/monotonic timing observations only. A diagnostic replay cannot clear the failed full run.',
}
try:
    tty.main()
finally:
    for session in sessions:
        (output / ('session-%d.raw' % session.label)).write_bytes(b''.join(session.chunks))
        (output / ('session-%d.json' % session.label)).write_text(
            json.dumps(session.record, indent=2) + '\n')
    metadata['inputs_unchanged'] = all(
        hashlib.sha256(Path(q).read_bytes()).hexdigest() == digest
        for q, digest in identities.items())
    (output / 'provenance.json').write_text(json.dumps(metadata, indent=2) + '\n')
