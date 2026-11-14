#!/bin/sh
# localgate.sh BIN: the grouped gate's groups, with this container's known
# local-only failures (as-root, image order, GNU-build locale) skipped.
BIN=$1
cd /home/user/emaxx-wt
export LANG=C LC_ALL=C RUST_MIN_STACK=134217728 RUST_TEST_THREADS=1 RUST_BACKTRACE=1
export EMAXX_FIXTURE_IMAGE_DIR=/home/user/emaxx-wt/target/grouped-gate/fixture-images
mkdir -p $EMAXX_FIXTURE_IMAGE_DIR
SKIPS="--skip byte_compile_file_reports_an_unwritable_target_like_gnu --skip save_buffer_skips_unmodified_and_unchanged_files --skip file_writable_p_is_nil_for_missing_files_in_unwritable_directories --skip batch_startup_image_round_trip_or_explicit_native_image_limit --skip print_prunes_charset_properties_like_print_prune_string_charset --skip char_charset_family_follows_charset_c --skip process_tls_state_survives_collection_before_and_after_async_handshake"
for g in lisp::eval::tests::eval_01:: lisp::eval::tests::eval_02:: lisp::eval::tests::eval_03:: lisp::eval::tests::eval_04:: lisp::eval::tests::eval_05:: lisp::primitives::tests::a-l lisp::primitives::tests::m-z lisp::primitives::compat_runtime_tests:: tty::; do
  # Each fresh interpreter keeps its mapped image for the process's life,
  # so the primitives group runs as two processes (names a-l, then m-z) to
  # halve its peak memory.
  case $g in
    lisp::primitives::tests::a-l) F=$(for c in a b c d e f g h i j k l; do printf 'lisp::primitives::tests::%s ' $c; done) ;;
    lisp::primitives::tests::m-z) F=$(for c in m n o p q r s t u v w x y z; do printf 'lisp::primitives::tests::%s ' $c; done) ;;
    *) F=$g ;;
  esac
  # The group's whole output is kept beside the summary, so a failure's
  # assertion text survives.
  L=${GATE_FULL_LOG_DIR:-/tmp}/gate-$(echo $g | tr ':' '_').log
  EMAXX_IMAGE_TEMPLATE=1 $BIN $F $SKIPS --test-threads 1 --quiet > $L 2>&1
  grep -E "^test result|FAILED|panicked" $L | sed "s|^|$g |"
done
env -u EMAXX_IMAGE_TEMPLATE $BIN batch::tests:: $SKIPS --test-threads 1 --quiet 2>&1 | grep -E "^test result|FAILED|panicked" | sed "s|^|batch |"
env -u EMAXX_IMAGE_TEMPLATE $BIN --skip lisp::eval::tests::eval_0 --skip lisp::primitives::tests:: --skip lisp::primitives::compat_runtime_tests:: --skip tty:: --skip batch::tests:: $SKIPS --test-threads 1 --quiet 2>&1 | grep -E "^test result|FAILED|panicked" | sed "s|^|lightweight |"
echo LOCALGATE-DONE
