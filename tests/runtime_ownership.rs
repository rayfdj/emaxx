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
                    let result = run_batch_process_with_large_stack(options, |outcome| {
                        assert_eq!(outcome, BatchRunOutcome::Exit(0));
                        assert_eq!(active.fetch_add(1, Ordering::SeqCst), 0);
                        // This callback runs before its interpreter is dropped.
                        // A nested public call must keep that same ownership.
                        assert!(
                            emaxx::batch::version_banner(None)
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
    let result =
        emaxx::lisp::run_ert_file(&path, std::path::Path::new(env!("CARGO_BIN_EXE_emaxx")));
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
    let result =
        emaxx::lisp::run_ert_file(&path, std::path::Path::new(env!("CARGO_BIN_EXE_emaxx")));
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
