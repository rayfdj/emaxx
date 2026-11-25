use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

use emaxx::batch::{BatchRunOptions, BatchRunOutcome, run_batch_process_with_large_stack};

#[test]
fn public_editor_calls_serialize_allocation_collection_and_shutdown() {
    let start = Arc::new(Barrier::new(4));
    let active = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for worker in 0..3 {
            let start = Arc::clone(&start);
            let active = Arc::clone(&active);
            scope.spawn(move || {
                start.wait();
                for iteration in 0..4 {
                    let value = 1000 * worker + iteration;
                    let options = BatchRunOptions {
                        no_loadup: true,
                        no_site_lisp: true,
                        eval: vec![format!(
                            "(progn
                               (setq ownership-probe (cons {value} (make-vector 128 {value})))
                               (garbage-collect)
                               (if (and (= (car ownership-probe) {value})
                                        (= (aref (cdr ownership-probe) 127) {value}))
                                   nil
                                 (error \"collected or aliased another runtime's object\")))"
                        )],
                        ..Default::default()
                    };
                    let runtime = emaxx::Runtime::lock();
                    let result = run_batch_process_with_large_stack(&runtime, options, |outcome| {
                        assert_eq!(outcome, BatchRunOutcome::Exit(0));
                        assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                        // This callback runs before its interpreter is dropped.
                        // A nested public call must keep that same ownership.
                        assert!(
                            emaxx::batch::version_banner(&runtime, None)
                                .expect("nested owned editor call")
                                .starts_with("GNU Emacs ")
                        );
                        for _ in 0..10 {
                            std::thread::yield_now();
                        }
                        assert_eq!(active.fetch_sub(1, Ordering::SeqCst), 1);
                        Ok(0)
                    });
                    assert_eq!(result.expect("complete public editor call"), 0);
                }
            });
        }
        start.wait();
    });
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

#[test]
fn public_ert_entry_reports_top_level_errors_as_owned_host_data() {
    let path = std::env::temp_dir().join(format!("emaxx-ert-error-{}.el", std::process::id()));
    std::fs::write(&path, "(error \"ERT load error must remain visible\")\n")
        .expect("write error control");
    let result = emaxx::lisp::run_ert_file(
        &emaxx::Runtime::lock(),
        &path,
        std::path::Path::new(env!("CARGO_BIN_EXE_emaxx")),
    );
    std::fs::remove_file(&path).expect("remove error control");
    let message = result.expect_err("a load error must not become an empty passing inventory");
    // Only this owned String leaves the runtime and crosses an OS thread.
    std::thread::spawn(move || assert!(message.contains("ERT load error must remain visible")))
        .join()
        .expect("owned error remains usable after the runtime exits");
}

#[test]
fn public_ert_entry_preserves_complete_inventory_and_unexpected_failures() {
    let path = std::env::temp_dir().join(format!("emaxx-ert-outcomes-{}.el", std::process::id()));
    std::fs::write(
        &path,
        "(ert-deftest ownership-visible-pass () (should (= (+ 19 23) 42)))
         (ert-deftest ownership-visible-failure () (should (= (+ 19 23) 43)))\n",
    )
    .expect("write outcome control");
    let result = emaxx::lisp::run_ert_file(
        &emaxx::Runtime::lock(),
        &path,
        std::path::Path::new(env!("CARGO_BIN_EXE_emaxx")),
    );
    std::fs::remove_file(&path).expect("remove outcome control");
    let report = result.expect("the file loads and both tests execute");
    assert_eq!(report.summary.total, 2);
    assert_eq!(report.summary.passed, 1);
    assert_eq!(report.summary.failed, 1);
    assert_eq!(report.summary.skipped, 0);
    assert_eq!(report.summary.unexpected, 1);
    assert_eq!(report.discovered_tests.len(), 2);
    assert_eq!(report.selected_tests.len(), 2);
    assert_eq!(report.results.len(), 2);
    let failure = report
        .results
        .iter()
        .find(|result| result.name == "ownership-visible-failure")
        .expect("the deliberately failing test is present");
    assert_eq!(failure.status, emaxx::compat::TestStatus::Failed);
    assert_eq!(failure.expected, Some(false));
    assert_eq!(emaxx::compat::report_execution_issues(&report).len(), 1);
}

#[test]
fn editor_state_left_by_one_os_thread_is_not_read_after_another_collects() {
    // A public call leaves nothing of its editor in its OS thread's
    // thread-locals: GNU's C globals are the process's, and the next call
    // finds them as a new process does.  Before, the memory profiler one
    // call started still ran in its thread's next call, its log naming
    // vectors that another thread's collection could free.
    fn run(form: &str) {
        let options = BatchRunOptions {
            no_loadup: true,
            no_site_lisp: true,
            eval: vec![form.to_owned()],
            ..Default::default()
        };
        let runtime = emaxx::Runtime::lock();
        let result = run_batch_process_with_large_stack(&runtime, options, |outcome| {
            assert_eq!(outcome, BatchRunOutcome::Exit(0), "{form}");
            Ok(0)
        });
        assert_eq!(result.expect("complete public editor call"), 0);
    }
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (collected_tx, collected_rx) = std::sync::mpsc::channel::<()>();
    let first = std::thread::spawn(move || {
        run("(progn (profiler-memory-start) (make-vector 128 nil) (make-list 128 nil) nil)");
        started_tx.send(()).expect("announce the first call");
        collected_rx.recv().expect("wait for the collection");
        run(
            "(if (profiler-memory-running-p) (error \"an earlier call's profiler still runs\") nil)",
        );
    });
    started_rx.recv().expect("the first call ends");
    std::thread::spawn(|| run("(progn (garbage-collect) nil)"))
        .join()
        .expect("the collecting call");
    collected_tx
        .send(())
        .expect("let the first thread call again");
    first.join().expect("the first thread's calls");
}

/// A batch call's options: an editor with no loadup that evaluates nil.
fn quiet_editor() -> BatchRunOptions {
    BatchRunOptions {
        no_loadup: true,
        no_site_lisp: true,
        eval: vec!["nil".to_owned()],
        ..Default::default()
    }
}

/// Another OS thread waiting in `Runtime::lock': it reports when it holds
/// the runtime, and gives it back at once.
fn contender() -> (std::sync::mpsc::Receiver<()>, std::thread::JoinHandle<()>) {
    let (asking_tx, asking_rx) = std::sync::mpsc::channel();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        asking_tx.send(()).expect("announce the request");
        let _runtime = emaxx::Runtime::lock();
        entered_tx.send(()).expect("announce the hold");
    });
    asking_rx.recv().expect("the contender asks");
    (entered_rx, thread)
}

/// Long enough for a contender the runtime was given to report it.
const BOUND: std::time::Duration = std::time::Duration::from_millis(300);

fn still_waiting(entered: &std::sync::mpsc::Receiver<()>, when: &str) {
    assert!(
        entered.recv_timeout(BOUND).is_err(),
        "another OS thread holds the runtime {when}"
    );
}

fn enters_at_last(entered: &std::sync::mpsc::Receiver<()>, thread: std::thread::JoinHandle<()>) {
    entered
        .recv_timeout(std::time::Duration::from_secs(600))
        .expect("the contender holds the runtime once this thread's holds are gone");
    thread.join().expect("the contender");
}

#[test]
fn a_handle_dropped_inside_a_call_keeps_the_runtime_until_the_last_goes() {
    // Two handles on one thread are two holds: dropping the first inside
    // a call made with the second gives nothing up, nor does the call's
    // return; the runtime goes with the last handle.
    let first = emaxx::Runtime::lock();
    let second = emaxx::Runtime::lock();
    let (entered, thread) = contender();
    let waiting = &entered;
    let result = run_batch_process_with_large_stack(&second, quiet_editor(), move |outcome| {
        assert_eq!(outcome, BatchRunOutcome::Exit(0));
        drop(first);
        still_waiting(waiting, "after a handle's drop inside the call");
        Ok(0)
    });
    assert_eq!(result.expect("the call"), 0);
    still_waiting(&entered, "after the call returned");
    drop(second);
    enters_at_last(&entered, thread);
}

#[test]
fn the_handle_a_call_was_made_with_keeps_the_runtime_after_the_other_goes() {
    // The drop order reversed: the call's own handle outlives the one
    // dropped inside it, and the runtime goes when it does.
    let first = emaxx::Runtime::lock();
    let second = emaxx::Runtime::lock();
    let (entered, thread) = contender();
    let waiting = &entered;
    let result = run_batch_process_with_large_stack(&first, quiet_editor(), move |outcome| {
        assert_eq!(outcome, BatchRunOutcome::Exit(0));
        drop(second);
        still_waiting(waiting, "after the other handle's drop inside the call");
        Ok(0)
    });
    assert_eq!(result.expect("the call"), 0);
    still_waiting(&entered, "after the call returned");
    drop(first);
    enters_at_last(&entered, thread);
}

#[test]
fn a_handle_dropped_inside_a_nested_call_keeps_the_runtime_for_both() {
    // A call made inside another's callback is one more hold: a handle
    // dropped inside the inner call leaves both calls and the other
    // handle holding the runtime.
    let first = emaxx::Runtime::lock();
    let second = emaxx::Runtime::lock();
    let (entered, thread) = contender();
    let waiting = &entered;
    let outer = &second;
    let result = run_batch_process_with_large_stack(&second, quiet_editor(), move |outcome| {
        assert_eq!(outcome, BatchRunOutcome::Exit(0));
        let inner = run_batch_process_with_large_stack(outer, quiet_editor(), move |outcome| {
            assert_eq!(outcome, BatchRunOutcome::Exit(0));
            drop(first);
            still_waiting(waiting, "after a handle's drop inside the nested call");
            Ok(0)
        });
        assert_eq!(inner.expect("the nested call"), 0);
        still_waiting(waiting, "after the nested call returned");
        Ok(0)
    });
    assert_eq!(result.expect("the call"), 0);
    still_waiting(&entered, "after the call returned");
    drop(second);
    enters_at_last(&entered, thread);
}
