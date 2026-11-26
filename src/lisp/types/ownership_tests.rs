use super::*;
use crate::lisp::runtime::with_runtime;

#[test]
fn a_symbol_sweep_on_another_host_thread_frees_the_creators_uninterned_symbols() {
    // The heap is the process's: symbols one host entry made and dropped
    // are freed by a collection another OS thread's entry runs, while the
    // creating thread waits, holding no handle.  Each of the creator's
    // symbols is looked for where it was, by the collecting entry while it
    // still holds the runtime: a cell there that is not free and still has
    // the symbol's own name, which no other symbol of this run has, is that
    // symbol, kept; a free cell, or one a later symbol took, is freed.
    const HIDE: usize = 0x5555_5555_5555_5555;
    let base = format!(
        "host-entry-reclamation-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("a time after the epoch")
            .as_nanos()
    );
    let (made_tx, made_rx) = std::sync::mpsc::channel();
    let (collected_tx, collected_rx) = std::sync::mpsc::channel::<()>();
    let creator = std::thread::spawn(move || {
        #[inline(never)]
        fn make(base: &str) -> Vec<(usize, String)> {
            (0..64)
                .map(|id| {
                    let symbol = SymbolName::make_uninterned(
                        Value::string("host-entry-reclamation"),
                        base,
                        id,
                    );
                    // The address hidden from the stack scans.
                    (symbol.identity_ptr() ^ HIDE, symbol.as_str().to_owned())
                })
                .collect()
        }
        let made = with_runtime(|| {
            let made = make(&base);
            crate::lisp::alloc::clobber_stack();
            made
        });
        made_tx.send(made).expect("creation complete");
        collected_rx.recv().expect("collection complete");
    });
    let made = made_rx.recv().expect("wait for symbol creation");
    let collection = with_runtime(|| {
        let mut interp = crate::lisp::eval::Interpreter::new();
        crate::lisp::primitives::call(&mut interp, "garbage-collect", &[], &mut Env::new())
            .map_err(|error| error.to_string())?;
        // Still this hold: nothing has been allocated since the sweep.
        Ok::<Vec<usize>, String>(
            made.iter()
                .map(|(hidden, name)| (hidden ^ HIDE, name))
                .filter(|(address, name)| {
                    // SAFETY: mem_find reads nothing it does not find in a
                    // block; a live cell's text lives with it.
                    matches!(
                        unsafe { crate::lisp::alloc::mem_find(*address) },
                        Some(crate::lisp::alloc::Found::Symbol(cell))
                            if cell as usize == *address
                                && unsafe { &*cell }.internal.get() == Some(name.as_str())
                    )
                })
                .map(|(address, _)| address)
                .collect(),
        )
    });
    collected_tx.send(()).expect("notify creator");
    creator.join().expect("creator finishes");
    let kept = collection.expect("collect through the ordinary primitive");
    assert_eq!(made.len(), 64);
    assert!(
        kept.is_empty(),
        "the swept symbols are freed: {} of 64 kept at {kept:#x?}",
        kept.len()
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
