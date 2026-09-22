The core runtime goal uses [the shared Lisp suite](../compat/core_runtime_perf.el),
[its measurement contract](../compat/core_runtime_perf.json), and
[the process runner](../tools/core_runtime_perf.py). The workload selection,
sizes, modes, sampling method and 3% ceiling were locked before representation
optimization on 2026-09-22. Noise calibration remains unresolved on this host;
this document does not certify GNU parity or the implementation.

All 16 cases are required: lexical and dynamic source loops, interpreted and
bytecode calls, native execution, four directions of native transitions, cons
allocation, list traversal, mapcar, explicit collection, sorting, undo and
bindat. No aggregate can excuse a slow individual case. Sizes are selected
using GNU-only pilots before locking; subsequent unfavorable measurements
cannot change the inventory, sizes, comparison method or tolerance.

The tolerance ceiling is 3%. Two pre-optimization self-comparisons used two
labels for the same GNU executable in repeated interleaved runs. For each workload,
take the median body time within each process, form the paired round ratios,
and bootstrap those round pairs with the declared seed and 20,000 resamples.
The 95% interval for the median ratio must fit within `[1/1.03, 1.03]` for a
resolved self-comparison. Cases with wider intervals remain inconclusive;
they do not receive a larger tolerance. Keep every run, including noise,
errors and failed calibration attempts. The first attempt suffered a host load
spike and a wall-clock discontinuity and was rejected. The second completed
all 288 processes, but only three workloads resolved this small interval.
Those results demonstrate that sub-3% comparisons are possible for some cases,
and that the current host conditions do not yet resolve the suite as a whole.
The fixed ceiling is a demanding acceptance limit, not a claim that the observed
noise is uniformly below 3%. No performance completion claim is permitted
until a matching full GNU self-comparison resolves it.

The final performance criterion requires every case's upper 95% interval for
Emaxx/GNU to be at most `1.03`, valid result and mode checks, complete inventory,
real allocation counters, and every retained timed body at least 100 ms.
Nine rounds, one warmup and three samples per process are fixed in the
contract. Case order rotates and editor order alternates within adjacent
pairs. Warmups are retained and identified. The geometric mean is descriptive
and cannot turn an individual failure into a pass. An unsuccessful or missing
process/report, incomplete samples, or changed measurement inputs invalidates
the corresponding claim.

Both editors start normally with their own executable and image. The same
command sets `native-comp-jit-compilation` to nil and `load-no-native` to t,
then loads the same source helper. This specifies the experiment's execution
modes without substituting implementations. Explicit `.eln` loads still use
native code under GNU's documented `load-no-native` behavior. Each editor
compiles its own copy of the unchanged upstream `comp-test-funcs.el` in a
separate preparation process, with the same source path and bytes. Compilation
time, warnings, outputs and artifact hashes are retained outside body timing.

Sorting loads unchanged `sort.el` source and calls the unchanged upstream
sorting helper with fixed words, in both directions. Bindat loads unchanged
source, test definitions and packet data, then repeatedly packs and unpacks
the packet. Undo loads unchanged `simple.elc` explicitly and repeats the four
fixed upstream tests `undo-test0`, `undo-test1`, `undo-test2`, and `undo-test5`.
Their own assertions remain part of the upstream work; the suite checks the
complete result set afterward. This performance selection does not replace or
reduce any correctness-gate inventory. Mode predicates are checked before
timing and reported for the driver and relevant callees; source review and
native contracts must additionally establish that these predicates reflect
actual execution.

GC threshold and percentage stay at the shared batch settings recorded in the
contract. The runner explicitly collects before each sample. Body time and GC
counts/time are measured by the same Lisp in both editors. Allocation evidence
retains the seven GNU `memory-use-counts` counters before and after each body;
these are object/element counts, not an invented total byte figure. The narrow
counter interval includes the common sampling overhead. Missing counters keep
their actual error and prevent final certification. Each process also retains
OS peak RSS, user/system CPU time, total wall time, CLI startup time, and load
averages. Total cost includes preparation, checks, warmups and reporting;
body time alone never represents total cost. Build and profiling jobs must
run separately from timing jobs.

The runner preserves exact commands, environment overrides, process output,
raw child reports, separate host validation, measurement source, source diff,
executable/image/native hashes, and GNU Lisp/bytecode/native-library hashes.
A final hash comparison detects changes to measurement inputs. Frozen oracle
identity is mandatory for final certification. `--diagnostic` retains useful
results with an explicit qualification and cannot satisfy the final criterion.
A zero process-runner exit means the requested inventory completed; the
separate `criterion_satisfied` field is the performance verdict.

Reproduction, with new output directories for every run:

```sh
python3 tools/core_runtime_perf.py prepare --source /path/to/emacs \
  --oracle /path/to/emacs/src/emacs --emaxx /path/to/emaxx \
  --output target/core-native
python3 tools/core_runtime_perf.py calibrate --source /path/to/emacs \
  --oracle /path/to/emacs/src/emacs --emaxx /path/to/emaxx \
  --native-dir target/core-native --output target/core-calibration
python3 tools/core_runtime_perf.py run --source /path/to/emacs \
  --oracle /path/to/emacs/src/emacs --emaxx /path/to/emaxx \
  --native-dir target/core-native --calibration target/core-calibration \
  --output target/core-comparison
```

The starting Darwin GNU executable has the correct source revision but differs
from the pinned executable hash. Current measurements require `--diagnostic`;
they must not be called frozen certification. The original September 21
baseline remains separate from the corrected release baseline.
