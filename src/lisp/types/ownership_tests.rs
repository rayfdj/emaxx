use super::*;
use crate::lisp::runtime::with_runtime;

#[test]
fn uninterned_symbol_lookup_crosses_serialized_host_threads() {
    // Only owned key text and pointer bits cross this boundary. No GC handle
    // is sent, and no collection occurs before the second lookup.
    let (key, expected) = std::thread::spawn(|| {
        with_runtime(|| {
            let key = make_uninterned_symbol_name("host-entry-identity", 9_700_001);
            let symbol = SymbolName::intern(key.clone());
            (key, symbol.identity_ptr())
        })
    })
    .join()
    .expect("first host entry");
    let found = std::thread::spawn(move || {
        with_runtime(|| SymbolName::live_uninterned(&key).map(|symbol| symbol.identity_ptr()))
    })
    .join()
    .expect("second host entry");
    assert_eq!(
        found,
        Some(expected),
        "one live symbol allocation across host entries"
    );
}

#[test]
fn symbol_sweep_on_another_host_thread_clears_the_creators_weak_book() {
    let (key_tx, key_rx) = std::sync::mpsc::channel();
    let (collected_tx, collected_rx) = std::sync::mpsc::channel();
    let creator = std::thread::spawn(move || {
        let key = with_runtime(|| {
            let key = make_uninterned_symbol_name("host-entry-reclamation", 9_700_002);
            SymbolName::intern(key.clone());
            assert!(SymbolName::live_uninterned(&key).is_some());
            key
        });
        // The creating OS thread stays alive, but holds only owned text.
        // Its private weak book must be retired by the process-wide sweep.
        key_tx.send(()).expect("creation complete");
        collected_rx.recv().expect("collection complete");
        with_runtime(|| SymbolName::live_uninterned(&key).is_none())
    });
    key_rx.recv().expect("wait for symbol creation");
    let collection = with_runtime(|| {
        let mut interp = crate::lisp::eval::Interpreter::new();
        crate::lisp::primitives::call(&mut interp, "garbage-collect", &[], &mut Env::new())
            .map(|_| ())
            .map_err(|error| error.to_string())
    });
    collected_tx.send(()).expect("notify creator");
    let removed = creator.join().expect("creator finishes");
    collection.expect("collect through the ordinary primitive");
    assert!(
        removed,
        "swept symbol must leave the creating thread's weak book"
    );
}
