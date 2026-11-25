//! A handle forgotten with `mem::forget' is a hold never given back, so
//! its thread holds the runtime for ever: this binary's only test, as no
//! other OS thread here can take the runtime after it.

use emaxx::batch::{BatchRunOptions, BatchRunOutcome, run_batch_process_with_large_stack};

#[test]
fn a_forgotten_handle_holds_the_runtime_and_never_gives_it_up_early() {
    let first = emaxx::Runtime::lock();
    let second = emaxx::Runtime::lock();
    let (asking_tx, asking_rx) = std::sync::mpsc::channel();
    let (entered_tx, entered) = std::sync::mpsc::channel();
    // Waits for ever: the process ends with it still asking.
    std::thread::spawn(move || {
        asking_tx.send(()).expect("announce the request");
        let _runtime = emaxx::Runtime::lock();
        entered_tx.send(()).expect("announce the hold");
    });
    asking_rx.recv().expect("the contender asks");
    let bound = std::time::Duration::from_millis(300);
    let waiting = &entered;
    let options = BatchRunOptions {
        no_loadup: true,
        no_site_lisp: true,
        eval: vec!["nil".to_owned()],
        ..Default::default()
    };
    let result = run_batch_process_with_large_stack(&second, options, move |outcome| {
        assert_eq!(outcome, BatchRunOutcome::Exit(0));
        std::mem::forget(first);
        assert!(
            waiting.recv_timeout(bound).is_err(),
            "the runtime was given up when a handle was forgotten"
        );
        Ok(0)
    });
    assert_eq!(result.expect("the call"), 0);
    assert!(
        entered.recv_timeout(bound).is_err(),
        "the runtime was given up when the call returned"
    );
    drop(second);
    assert!(
        entered.recv_timeout(bound).is_err(),
        "the runtime was given up with a forgotten hold left"
    );
}
