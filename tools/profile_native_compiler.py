#!/usr/bin/env python3
"""Attribute native compiler latency using real independent diagnostic runs.

Advice records pass timings and wraps compiler children with /usr/bin/time;
the pass list and test expectations remain unchanged. These instrumented runs
do not certify frozen compatibility. Use the ordinary runner for that.
"""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import tempfile
import time
from measure_allocators import digest, library_digest
from process_tree_sampler import ProcessTreeSampler

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--oracle-source', type=Path, required=True)
parser.add_argument('--emaxx', type=Path, required=True)
parser.add_argument('--selector', default='comp-tests-ret-type-spec-7')
parser.add_argument('--sample-parent', action='store_true')
parser.add_argument('--timeout', type=int, default=1200)
args = parser.parse_args()
if args.timeout <= 0:
    parser.error('--timeout must be positive')
if args.sample_parent and platform.system() != 'Darwin':
    parser.error('--sample-parent requires Darwin /usr/bin/sample')
project = Path(__file__).resolve().parent.parent
source = args.oracle_source.resolve(strict=True)
emaxx = args.emaxx.resolve(strict=True)
root = args.output.resolve()
root.mkdir(parents=True, exist_ok=False)
helper = project / 'compat/emacs_compat_runner.el'
inputs = [emaxx, emaxx.with_suffix('.pdmp'), source / 'src/emacs',
          source / 'src/emacs.pdmp', source / 'test/src/comp-tests.el', helper,
          Path(__file__).resolve(), project / 'tools/process_tree_sampler.py',
          Path('/usr/bin/time')]
identities = {str(path): digest(path) for path in inputs}
libraries = library_digest(source)
evidence = dict(kind='diagnostic_only', host=platform.uname()._asdict(),
                source=str(source), source_libraries_sha256=libraries,
                source_revision=subprocess.check_output(
                    ['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip(),
                input_sha256=identities, selector=args.selector, runs=[],
                sampling_limit='25 ms sampling may miss short-lived descendants; child time reports retain aggregate usage')
(root / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
child = root / 'child-profile.el'
child.write_text(r''';; -*- lexical-binding: t; -*-
(defun compiler-profile-note (stage)
  (with-temp-buffer
    (prin1 (list stage (float-time) (float-time (current-cpu-time))
                 (emacs-pid)) (current-buffer))
    (insert "\n")
    (write-region (point-min) (point-max) compiler-profile-file t 'silent)))
(compiler-profile-note "lisp-start")
(with-eval-after-load 'comp
  (defvar compiler-profile-original-passes (copy-tree comp-passes))
  (compiler-profile-note "comp-loaded")
  (dolist (function (delete-dups (append '(comp--native-compile comp--final1 comp--compile-ctxt-to-file native-elisp-load) comp-passes nil)))
    (advice-add
     function :around
     (let ((name (symbol-name function)))
       (lambda (original &rest arguments)
         (compiler-profile-note (concat name "-begin"))
         (unwind-protect (apply original arguments)
           (compiler-profile-note (concat name "-end"))))))))
(add-hook 'kill-emacs-hook
          (lambda ()
            (unless (equal comp-passes compiler-profile-original-passes)
              (error "Profiling changed the compiler pass list"))
            (compiler-profile-note "lisp-exit")))
''')

reports = []
for who, binary in [
    ('oracle', source / 'src/emacs'),
    ('emaxx', emaxx),
]:
    work = root / who
    work.mkdir()
    parent = work / 'parent-profile.el'
    parent.write_text(r''';; -*- lexical-binding: t; -*-
(defvar compiler-profile-counter 0)
(require 'comp)
(advice-add
 'call-process :around
 (lambda (original program &optional infile destination display &rest arguments)
   (if (not (member "-no-comp-spawn" arguments))
       (apply original program infile destination display arguments)
     (let* ((id (cl-incf compiler-profile-counter))
            (prefix (expand-file-name (format "child-%04d" id) compiler-profile-directory))
            (events (concat prefix ".events"))
            (usage (concat prefix ".time"))
            (input (car (last arguments)))
            (setup (list 'progn (list 'setq 'compiler-profile-file events)
                         (list 'load compiler-profile-child nil t)))
            (start (float-time))
            (start-cpu (float-time (current-cpu-time)))
            result)
       (when (file-regular-p input) (copy-file input (concat prefix ".input.el")))
       (unwind-protect
           (setq result
                 (apply original "/usr/bin/time" infile destination display
                        compiler-profile-time-option "-p" "-o" usage program
                        "--eval" (prin1-to-string setup) arguments))
         (with-temp-file (concat prefix ".parent")
           (prin1 (list :start start :end (float-time)
                        :parent-cpu (- (float-time (current-cpu-time)) start-cpu)
                        :program program :arguments arguments :result result)
                  (current-buffer))))
       result))))
(setq compiler-profile-file (expand-file-name "parent.events" compiler-profile-directory))
(load compiler-profile-child nil t)
(setq native-comp-async-env-modifier-form
      (list 'progn '(compiler-profile-note "context-ready")
            native-comp-async-env-modifier-form))
''')
    with tempfile.TemporaryDirectory(prefix='ec-native-profile-', dir='/tmp') as temporary:
        scratch = Path(temporary).resolve()
        checkout = scratch / 'emacs'
        subprocess.run(['git', '-c', 'advice.detachedHead=false', 'clone', '--quiet', '--shared', '--no-hardlinks', str(source), str(checkout)], check=True)
        shutil.rmtree(checkout / 'lisp')
        (checkout / 'lisp').symlink_to(source / 'lisp', target_is_directory=True)
        report = work / 'results.json'
        durations = work / 'durations.json'
        setup = f'''(progn
          (setq compiler-profile-directory {json.dumps(str(work))}
                compiler-profile-time-option {json.dumps('-l' if platform.system() == 'Darwin' else '-v')}
                compiler-profile-child {json.dumps(str(child))})
          (defvar compiler-profile-times nil)
          (advice-add 'emaxx-compat--test-listener :after
            (lambda (event &rest arguments)
              (when (memq event '(test-started test-ended))
                (compiler-profile-note (format "%s:%s" event (ert-test-name (nth 1 arguments)))))
              (when (eq event 'test-ended)
                (push (list
                       (cons 'name (symbol-name (ert-test-name (nth 1 arguments))))
                       (cons 'seconds (ert-test-result-duration (nth 2 arguments))))
                      compiler-profile-times)))))'''
        run = f'''(progn
          (emaxx-compat-run '{args.selector})
          (with-temp-file {json.dumps(str(durations))}
            (insert (json-encode (vconcat (nreverse compiler-profile-times))))))'''
        command = [
            str(binary), '-Q', '--batch', '--eval',
            f'(startup-redirect-eln-cache {json.dumps(str(scratch))})',
            '-L', str(checkout / 'test'), '-l', 'ert', '-l', str(helper),
            '--eval', setup, '-l', str(parent), '-l', str(checkout / 'test/src/comp-tests.el'),
            '--eval', run,
        ]
        home = scratch / 'home'
        home.mkdir()
        environment = dict(
            PATH=os.environ.get('PATH', os.defpath), HOME=str(home), LANG='C', TMPDIR=str(scratch),
            TMP=str(scratch), TEMP=str(scratch), EMACS_TEST_DIRECTORY=str(checkout / 'test'),
            EMAXX_BATCH_RESULT_FILE=str(report), EMAXX_COMPAT_RUNNER=who,
            EMAXX_COMPAT_RELATIVE_FILE='test/src/comp-tests.el',
            EMAXX_DUMP_SOURCE_DIRECTORY=str(source),
        )
        (work / 'command.json').write_text(json.dumps({
            'command': command,
            'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
            'input_sha256': hashlib.sha256((checkout / 'test/src/comp-tests.el').read_bytes()).hexdigest(),
            'selector': args.selector,
            'environment': environment,
        }, indent=2) + '\n')
        print('START', who, flush=True)
        start = time.monotonic()
        with (work / 'process.log').open('w') as log:
            process = subprocess.Popen(command, cwd=checkout / 'test', env=environment,
                                       stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            stack_sampler = None
            if args.sample_parent:
                stack_sampler = subprocess.Popen(
                    ['/usr/bin/sample', str(process.pid), '15', '1', '-file', str(work / 'parent-sample.txt')],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                ProcessTreeSampler().wait(process, work / 'process-tree.jsonl', timeout=args.timeout)
            except BaseException:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                raise
            if stack_sampler is not None:
                sample_status = stack_sampler.wait(timeout=20)
                (work / 'sample-status.json').write_text(json.dumps({'exit_code': sample_status}) + '\n')
        print('FINISHED', who, process.returncode, flush=True)
        evidence['runs'].append(dict(editor=who, exit_code=process.returncode,
                                     process_seconds=time.monotonic() - start,
                                     output_sha256={str(path.relative_to(root)): digest(path)
                                                    for path in sorted(work.rglob('*')) if path.is_file()}))
        (root / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
        if process.returncode:
            raise SystemExit(process.returncode)
        result = json.loads(report.read_text())
        print(who, result['summary'], flush=True)
        rows = result['results']
        names = [row['name'] for row in rows]
        if (result['file_status'] != 'loaded' or result['file_error'] is not None
                or len(names) != len(set(names)) or not names
                or sorted(names) != sorted(result['selected_tests'])):
            raise RuntimeError('Missing, duplicate, or incomplete diagnostic test results')
        counts = dict(total=len(rows), passed=0, failed=0, skipped=0,
                      unexpected=sum(row['expected'] is not True for row in rows))
        for row in rows:
            if row['status'] not in ('passed', 'failed', 'skipped'):
                raise RuntimeError('Invalid diagnostic test outcome')
            counts[row['status']] += 1
        if counts != result['summary']:
            raise RuntimeError('Diagnostic summary does not match its raw outcomes')
        if counts['unexpected']:
            raise SystemExit('Unexpected outcome in diagnostic workload; retain and inspect it.')
        timings = json.loads(durations.read_text())
        if (sorted(row['name'] for row in timings) != sorted(names)
                or any(not math.isfinite(row['seconds']) or row['seconds'] < 0 for row in timings)):
            raise RuntimeError('Incomplete or invalid diagnostic test timings')
        reports.append(sorted((row['name'], row['status'], row['expected']) for row in rows))

if reports[0] != reports[1]:
    raise RuntimeError('The independent diagnostic workloads have different test outcomes')
for path, expected in identities.items():
    if digest(Path(path)) != expected:
        raise RuntimeError('Measurement input changed: ' + path)
if library_digest(source) != libraries:
    raise RuntimeError('GNU source libraries changed during measurement')
evidence['output_sha256'] = {str(path.relative_to(root)): digest(path)
                           for path in sorted(root.rglob('*'))
                           if path.is_file() and path.name != 'evidence.json'}
evidence['inputs_verified_after_execution'] = True
(root / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
print('Diagnostic only: no compatibility outcomes are certified.')
