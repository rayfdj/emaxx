#!/bin/bash
# validate.sh TAG: the gate and the integration targets on /home/user/emaxx-wt as it is, under Rust 1.99.1.
T=$1; V=/home/user/emaxx-work/r364val
cd /home/user/emaxx-wt
export RUSTUP_TOOLCHAIN=1.99.1 LANG=C LC_ALL=C CARGO_TARGET_DIR=/home/user/emaxx/target/gate346
nice cargo test --locked --profile gate -j3 --lib --no-run > $V/gate-build-$T.log 2>&1; echo "exit $?" >> $V/gate-build-$T.log
BIN=$(grep -o '/home/user/emaxx/target/gate346/gate/deps/emaxx-[0-9a-f]*' $V/gate-build-$T.log | tail -1)
echo "BIN=$BIN" >> $V/gate-build-$T.log
nice cargo test --locked --profile gate -j3 --test cli --test cli_parity --test ert_runner --test native_comp_identity --test native_thread_continuations --test package_lifecycle --test runtime_ownership --no-run > $V/integ-build-$T.log 2>&1; echo "exit $?" >> $V/integ-build-$T.log
mkdir -p $V/gatelogs-$T
GATE_FULL_LOG_DIR=$V/gatelogs-$T sh wip/r364/scripts/localgate.sh $BIN > $V/localgate-$T.log 2>&1
env RUST_MIN_STACK=134217728 RUST_TEST_THREADS=1 cargo test --locked --profile gate -j3 --test cli --test cli_parity --test ert_runner --test native_comp_identity --test native_thread_continuations --test package_lifecycle --test runtime_ownership --no-fail-fast -- --test-threads 1 > $V/integ-$T.log 2>&1; echo "exit $?" >> $V/integ-$T.log
echo VALIDATE-DONE >> $V/integ-$T.log
