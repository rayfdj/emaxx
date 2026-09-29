mkdir -p target/frozen-ci
python3 - <<'PY'
import json
from pathlib import Path
source = Path('../emacs').resolve()
Path('compat/oracle.local.json').write_text(json.dumps({
    'format_version': 1,
    'emacs_binary': str(source / 'src/emacs'),
    'emacs_repo': str(source),
}, indent=2) + '\n')
PY
run_harness() {
  env -i HOME="$HOME" PATH="$PATH" LANG=C CARGO_BUILD_JOBS=2 \
    RUST_MIN_STACK=134217728 RUST_TEST_THREADS=1 target/gate/compat-harness "$@"
}
if [ "$VALIDATION" = prepare-oracle ]; then
  # Explicit pin proposal for review. This is NOT a frozen certificate:
  # the old lock describes an Ubuntu source repack, while existing
  # native CI uses the pristine GNU revision checked out above.
  cp compat/oracle.lock.linux.json target/frozen-ci/oracle-lock-before.json
  run_harness oracle pin --emacs ../emacs/src/emacs --repo ../emacs
  cp compat/oracle.lock.linux.json target/frozen-ci/oracle-lock-proposed.json
fi
case "$VALIDATION" in
  sandbox-diagnostics|image-diagnostics|startup-diagnostics|allocator-diagnostics|compiler-diagnostics|compiler-samples|ordinary-samples)
    # Diagnostic children only; this does not replace an ordinary
    # run or alter the kernel filter or its expected behavior.
    EMAXX_GNU_SOURCE_DIRECTORY="$(realpath ../emacs)" \
      cargo build --locked --profile gate --target-dir target/compat-subject \
      --bin emaxx --bin make-fingerprint -j2
    if [ "$VALIDATION" = startup-diagnostics ]; then
      # Capture the caller of the startup syscall rejected by
      # the inherited GNU filter. This stops for inspection and
      # makes no claim about the editor's eventual exit status.
      gdb --batch -ex "catch syscall $STARTUP_SYSCALL" -ex run -ex bt \
        --args "$PWD/target/compat-subject/gate/emaxx" \
          --quick --batch '--eval=(message "Hi")' \
          2>&1 | tee target/frozen-ci/startup-backtrace.log
      exit 0
    fi
    if [ "$VALIDATION" = image-diagnostics ]; then
      # Stop at the actual bootstrap fault and retain its native
      # backtrace. This diagnostic never certifies frozen outcomes.
      EMAXX_DUMP_SOURCE_DIRECTORY="$PWD/../emacs" \
        EMACS_TEST_DIRECTORY="$PWD/../emacs/test" LC_ALL=C \
        gdb --batch --return-child-result -ex run -ex 'thread apply all bt' \
          --args "$PWD/target/compat-subject/gate/emaxx" \
          -batch -l loadup --temacs=pdump \
          --bin-dest "$PWD/target/compat-subject/gate/" \
          --eln-dest "$PWD/../emacs/" \
          2>&1 | tee target/frozen-ci/image-backtrace.log
      exit 0
    fi
    EMAXX_DUMP_SOURCE_DIRECTORY="$PWD/../emacs" \
      EMACS_TEST_DIRECTORY="$PWD/../emacs/test" \
      tools/build-image.sh target/compat-subject/gate/emaxx
    if [ "$VALIDATION" = ordinary-samples ]; then
      test -n "$VALIDATION_FILE"
      # Profile one complete ordinary file with unchanged tests
      # and runner setup. Prebuild to avoid Rust compilation during
      # the sampled run. Timings are diagnostic, not a frozen
      # certificate or an uninstrumented performance comparison.
      perf_binary=$(python3 - <<'PY'
from pathlib import Path
candidates = sorted(Path('/usr/lib/linux-tools').glob('*/perf'))
if not candidates:
    raise SystemExit('Installed Linux perf executable was not found')
print(candidates[-1].resolve(strict=True))
PY
      )
      diagnostic_user=$(id -un)
      sample_status=0
      sudo "$perf_binary" record -e cpu-clock:u -F 199 \
        --call-graph dwarf,8192 -o "$PWD/target/frozen-ci/ordinary-perf.data" \
        -- sudo -u "$diagnostic_user" -- env -i HOME="$HOME" PATH="$PATH" \
        LANG=C CARGO_BUILD_JOBS=2 RUST_MIN_STACK=134217728 RUST_TEST_THREADS=1 \
        target/gate/compat-harness run --file "$VALIDATION_FILE" \
        2>&1 | tee target/frozen-ci/ordinary-perf.log || sample_status=$?
      if [ -f target/frozen-ci/ordinary-perf.data ]; then
        sudo chown "$diagnostic_user" target/frozen-ci/ordinary-perf.data
      fi
      # Keep the exact subject for offline symbolization even if
      # a later report step fails. No editor is rerun for reports.
      cp target/compat-subject/gate/emaxx target/frozen-ci/ordinary-perf-emaxx
      "$perf_binary" buildid-list -i target/frozen-ci/ordinary-perf.data \
        > target/frozen-ci/ordinary-perf-buildids.txt
      timeout 180 "$perf_binary" report --stdio --no-children --comms=emaxx \
        -i target/frozen-ci/ordinary-perf.data --sort comm,dso,symbol \
        > target/frozen-ci/ordinary-perf-self.txt
      timeout 180 "$perf_binary" report --stdio --children --comms=emaxx \
        --percent-limit 0.5 -i target/frozen-ci/ordinary-perf.data --sort comm,dso,symbol \
        > target/frozen-ci/ordinary-perf-callers.txt
      python3 - <<'PY'
import hashlib, json, re
from pathlib import Path
root = Path('target/frozen-ci')
if not re.search(r'^\s*[0-9.]+%\s+emaxx\s+', (root / 'ordinary-perf-self.txt').read_text(), re.M):
    raise SystemExit('The retained profile has no symbolized Emaxx samples')
inputs = {}
for name in ('ordinary-perf.data', 'ordinary-perf-emaxx'):
    with (root / name).open('rb') as source:
        inputs[name] = hashlib.file_digest(source, 'sha256').hexdigest()
(root / 'ordinary-perf-metadata.json').write_text(json.dumps({
    'kind': 'diagnostic_only', 'frequency_hz': 199, 'call_graph': 'dwarf,8192',
    'report_scope': 'Emaxx process samples, including startup; no kernel samples',
    'sha256': inputs,
}, indent=2) + '\n')
PY
      exit "$sample_status"
    fi
    if [ "$VALIDATION" = compiler-samples ]; then
      # Sample only this diagnostic process tree. The editors run
      # under the ordinary runner account; no global perf policy is
      # changed and no unrelated processes are sampled. Eight real
      # compiler cases provide repeated children; this is not a
      # compatibility certificate or an uninstrumented benchmark.
      perf_binary=$(python3 - <<'PY'
from pathlib import Path
candidates = sorted(Path('/usr/lib/linux-tools').glob('*/perf'))
if not candidates:
    raise SystemExit('Installed Linux perf executable was not found')
print(candidates[-1].resolve(strict=True))
PY
      )
      diagnostic_user=$(id -un)
      sample_status=0
      sudo "$perf_binary" record -e cpu-clock:u -F 997 \
        --call-graph dwarf,16384 -o "$PWD/target/frozen-ci/compiler-perf.data" \
        -- sudo -u "$diagnostic_user" -- env "PATH=$PATH" \
        python3 tools/profile_native_compiler.py --oracle-source ../emacs \
          --emaxx target/compat-subject/gate/emaxx \
          --selector '"^comp-tests-ret-type-spec-[1-8]$"' \
          --output target/frozen-ci/compiler-samples \
        2>&1 | tee target/frozen-ci/compiler-perf.log || sample_status=$?
      # perf deliberately creates a private file for its recorder.
      # Hand that one file to the job account so raw evidence can
      # be uploaded even when the diagnostic itself failed.
      if [ -f target/frozen-ci/compiler-perf.data ]; then
        sudo chown "$diagnostic_user" target/frozen-ci/compiler-perf.data
      fi
      if [ "$sample_status" -ne 0 ]; then
        exit "$sample_status"
      fi
      "$perf_binary" report --stdio --no-children \
        -i target/frozen-ci/compiler-perf.data --sort comm,dso,symbol \
        > target/frozen-ci/compiler-perf-self.txt
      "$perf_binary" report --stdio --children --percent-limit 0.5 \
        -i target/frozen-ci/compiler-perf.data --sort comm,dso,symbol \
        > target/frozen-ci/compiler-perf-callers.txt
      exit 0
    fi
    if [ "$VALIDATION" = compiler-diagnostics ]; then
      python3 tools/profile_native_compiler.py --oracle-source ../emacs \
        --emaxx target/compat-subject/gate/emaxx \
        --selector '"^comp-tests-ret-type-spec-"' \
        --output target/frozen-ci/compiler-diagnostics
      exit 0
    fi
    if [ "$VALIDATION" = allocator-diagnostics ]; then
      # Build the actual pre-change implementation against the
      # same GNU libraries; retained scores are never inputs.
      baseline_root="$RUNNER_TEMP/allocator-before"
      mkdir -p "$baseline_root"
      [[ "$BASELINE_REVISION" =~ ^[0-9a-f]{40}$ ]]
      git worktree add --detach "$baseline_root/emaxx" "$BASELINE_REVISION"
      ln -s "$PWD/../emacs" "$baseline_root/emacs"
      cargo build --locked --profile gate \
        --manifest-path "$baseline_root/emaxx/Cargo.toml" \
        --target-dir "$PWD/target/allocator-before" \
        --bin emaxx --bin make-fingerprint -j2
      EMAXX_DUMP_SOURCE_DIRECTORY="$PWD/../emacs" \
        EMACS_TEST_DIRECTORY="$PWD/../emacs/test" \
        "$baseline_root/emaxx/tools/build-image.sh" \
          "$PWD/target/allocator-before/gate/emaxx"
      python3 tools/measure_allocators.py --oracle-source ../emacs \
        --binary "before=$PWD/target/allocator-before/gate/emaxx" \
        --binary "after=$PWD/target/compat-subject/gate/emaxx" \
        --output target/frozen-ci/allocator-diagnostics
      python3 tools/measure_core_latency.py \
        --output target/frozen-ci/core-diagnostics
      exit 0
    fi
    control_status=0
    EMAXX_DUMP_SOURCE_DIRECTORY="$PWD/../emacs" \
      EMACS_TEST_DIRECTORY="$PWD/../emacs/test" \
      cargo test --locked --profile gate --target-dir target/compat-subject \
        --test cli -j2 linux_ -- --test-threads=1 \
        2>&1 | tee target/frozen-ci/linux-startup-controls.log || control_status=$?
    python3 tools/diagnose_linux_sandbox.py --oracle-source ../emacs \
      --emaxx target/compat-subject/gate/emaxx \
      --output target/frozen-ci/sandbox-diagnostics || control_status=$?
    exit "$control_status"
    ;;
  prepare-oracle|affected)
    if [ "$VALIDATION" = affected ] && [ -n "$VALIDATION_FILE" ]; then
      if [ -n "$VALIDATION_RUST_FILTER" ]; then
        EMACS_TEST_DIRECTORY="$PWD/../emacs/test" \
          cargo test --locked --profile gate --lib -j2 "$VALIDATION_RUST_FILTER" \
            -- --test-threads=1 2>&1 | tee target/frozen-ci/runtime-control.log
      fi
      run_harness run --file "$VALIDATION_FILE" 2>&1 | tee target/frozen-ci/affected.log
      exit 0
    fi
    status=0
    run_harness run --file test/src/emacs-module-tests.el 2>&1 | tee target/frozen-ci/module.log || status=$?
    run_harness run --file test/lisp/progmodes/eglot-tests.el 2>&1 | tee target/frozen-ci/eglot.log || status=$?
    exit "$status"
    ;;
  remaining)
    # Ordinary continuation only: preserve the failed frozen
    # artifact and execute the still-unfinished canonical range.
    # This does not certify a partial inventory as a frozen run.
    test -n "$VALIDATION_FILE"
    run_harness run --scope all --from-file "$VALIDATION_FILE" \
      2>&1 | tee target/frozen-ci/remaining.log
    ;;
  frozen)
    run_harness frozen 2>&1 | tee target/frozen-ci/frozen.log
    ;;
  rust)
    LANG=C LC_ALL=C RUST_TEST_THREADS=1 CARGO_BUILD_JOBS=2 \
      python3 tools/serial_grouped_gate.py --scope full --artifact-root target/frozen-ci/rust
    ;;
  rust-backtrace)
    LANG=C LC_ALL=C RUST_TEST_THREADS=1 CARGO_BUILD_JOBS=2 \
      CARGO_PROFILE_GATE_DEBUG=1 EMAXX_GC_VERIFY=1 \
      python3 tools/diagnose_rust_gate.py --filter "$VALIDATION_RUST_FILTER" \
        --output target/frozen-ci/rust-backtrace
    ;;
  rust-replay)
    # Match the full gate's Cargo profile and execution environment.
    # Extra debug information and verifier code can change compiler
    # spills, so their passes do not clear a default-profile failure.
    # Preserve the executable and all original result checks.
    prelude=()
    if [ -n "$VALIDATION_RUST_PRELUDE" ]; then
      prelude=(--prelude-filter "$VALIDATION_RUST_PRELUDE")
    fi
    LANG=C LC_ALL=C RUST_TEST_THREADS=1 CARGO_BUILD_JOBS=2 \
      python3 tools/diagnose_rust_gate.py --filter "$VALIDATION_RUST_FILTER" \
        "${prelude[@]}" --plain --output target/frozen-ci/rust-replay
    ;;
  rust-tail)
    # Explicit continuation after the documentation-only failure in
    # the 37197db library gate. Its original results remain failed;
    # these controls and remaining Cargo targets are separate evidence.
    LANG=C LC_ALL=C RUST_TEST_THREADS=1 CARGO_BUILD_JOBS=2 python3 - <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, 'tools')
import serial_grouped_gate
gate = serial_grouped_gate.gate
root = Path('target/frozen-ci/rust-tail').resolve()
root.mkdir(parents=True, exist_ok=False)
summary = {'git': gate.git_state(), 'status': 'running', 'stages': [],
           'environment': {'LANG': 'C', 'LC_ALL': 'C', 'RUST_TEST_THREADS': '1'},
           'scope': 'Three image-state controls, all binary and integration targets; not a complete library rerun'}
try:
    targets = gate.discover_cargo_test_targets(root)
    summary['cargo_test_targets'] = targets
    stages = [
        ('field-audit', ('--lib', 'interpreter_fields_are_carried_or_documented'), 1),
        ('field-inventory', ('--lib', 'every_reset_root_is_a_mark_phase_root'), 1),
        ('overlay-image', ('--lib', 'image_round_trips_buffers_markers_finalizers_and_nilled_frames'), 1),
        ('bins', ('--bins',), len(targets['bins'])),
        ('integration', tuple(arg for target in targets['integrations']
                              for arg in ('--test', target)), len(targets['integrations'])),
    ]
    for name, arguments, expected in stages:
        result = gate.run_cargo_stage(name, arguments, 'gate', root, expected)
        if arguments[0] == '--lib' and result['results'][0]['passed'] != 1:
            raise gate.GateError(f'{name} must execute exactly one control')
        summary['stages'].append(result)
        gate.write_summary(root / 'summary.json', summary)
    summary['status'] = 'passed'
except BaseException as error:
    summary.update(status='failed', error=str(error))
    raise
finally:
    gate.write_summary(root / 'summary.json', summary)
PY
    ;;
  *)
    echo "Unknown validation mode: $VALIDATION" >&2
    exit 2
    ;;
esac
