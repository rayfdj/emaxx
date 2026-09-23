use emaxx::lisp::types::Value;
use std::sync::{Arc, Barrier};

fn main() {
    fn is_send_sync<T: Send + Sync>() {}
    is_send_sync::<Value>();
    let barrier = Arc::new(Barrier::new(2));
    let handles: Vec<_> = (0..2).map(|thread| {
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            (0..100_000).map(|i| {
                let expected = (thread * 1_000_000 + i) as f64;
                (Value::float(expected), expected)
            }).collect::<Vec<_>>()
        })
    }).collect();
    let values: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let corrupt = values.iter().flatten()
        .filter(|(v, expected)| v.as_float().unwrap() != *expected).count();
    println!("values=200000 corrupted={corrupt}");
    assert_eq!(corrupt, 0);
}
