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
