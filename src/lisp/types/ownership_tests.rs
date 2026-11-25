use super::*;
use crate::lisp::runtime::with_runtime;

#[test]
fn a_symbol_sweep_on_another_host_thread_frees_the_creators_uninterned_symbols() {
    // The heap is the process's: symbols one host entry made and dropped
    // are freed by a collection another OS thread's entry runs, while the
    // creating thread waits, holding no handle.
    let (made_tx, made_rx) = std::sync::mpsc::channel();
    let (collected_tx, collected_rx) = std::sync::mpsc::channel();
    let creator = std::thread::spawn(move || {
        #[inline(never)]
        fn make() {
            for id in 0..64 {
                SymbolName::make_uninterned(Value::string("host-entry-reclamation"), "x", id);
            }
        }
        let made = with_runtime(|| {
            make();
            crate::lisp::alloc::clobber_stack();
            census_live_uninterned_symbols()
        });
        made_tx.send(()).expect("creation complete");
        collected_rx.recv().expect("collection complete");
        (made, with_runtime(census_live_uninterned_symbols))
    });
    made_rx.recv().expect("wait for symbol creation");
    let collection = with_runtime(|| {
        let mut interp = crate::lisp::eval::Interpreter::new();
        crate::lisp::primitives::call(&mut interp, "garbage-collect", &[], &mut Env::new())
            .map(|_| ())
            .map_err(|error| error.to_string())
    });
    collected_tx.send(()).expect("notify creator");
    let (made, after) = creator.join().expect("creator finishes");
    collection.expect("collect through the ordinary primitive");
    assert!(
        after + 64 <= made,
        "the swept symbols are freed: {made} allocated before, {after} after"
    );
}

#[test]
fn host_symbols_move_with_the_runtime() {
    // The obarray of code that makes no editor is the runtime's: an OS
    // thread without the runtime takes it before it looks a name up, and
    // every holder in turn sees the same symbols.
    use crate::lisp::runtime::held;
    const NAME: &str = "runtime-host-obarray-probe";
    let (made_tx, made_rx) = std::sync::mpsc::channel();
    let (finish_tx, finish_rx) = std::sync::mpsc::channel::<()>();
    let maker = std::thread::spawn(move || {
        assert!(!held());
        let made = SymbolName::intern_str(NAME).identity_ptr();
        assert!(held(), "interning took the runtime");
        made_tx.send(made).expect("announce the symbol");
        finish_rx.recv().expect("wait to finish");
    });
    let made = made_rx.recv().expect("the symbol is made");
    let (found_tx, found_rx) = std::sync::mpsc::channel();
    let finder = std::thread::spawn(move || {
        let found = SymbolName::find(NAME).map(|symbol| symbol.identity_ptr());
        assert!(held(), "the lookup took the runtime");
        found_tx.send(found).expect("announce the lookup");
    });
    assert!(
        found_rx
            .recv_timeout(std::time::Duration::from_millis(200))
            .is_err(),
        "no lookup in the host obarray while another thread holds the runtime"
    );
    finish_tx.send(()).expect("let the maker finish");
    maker.join().expect("the maker exits");
    assert_eq!(
        found_rx
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("the lookup runs once the maker has exited"),
        Some(made)
    );
    finder.join().expect("the finder exits");
}
