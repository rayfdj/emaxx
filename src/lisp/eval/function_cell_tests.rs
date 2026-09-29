use super::*;

#[test]
fn static_roots_trace_the_current_symbol_function_cell_only() {
    let mut interp = Interpreter::new();
    let symbol = SymbolName::intern_str("function-root-cell-owner");
    let old = Value::vector([Value::Integer(17)]);
    let current = Value::vector([Value::Integer(43)]);
    interp.set_function_binding(symbol.as_str(), Some(old));
    // GNU's set_symbol_function writes one cell. Exercise that actual
    // cell here, without updating any detached function payload copy.
    interp.globals.set_function(&symbol, Some(current));
    assert_eq!(
        interp.raw_function_binding_symbol(&symbol, &Env::new()),
        Some(current)
    );
    let mut reached = LispReachability::default();
    interp.mark_static_roots_into(&mut reached);
    assert_eq!(
        (reached.contains(&current), reached.contains(&old)),
        (true, false),
        "static roots must retain the current function, not its replaced payload"
    );
}

#[test]
fn function_cycle_validation_follows_the_current_symbol_cells() {
    let mut interp = Interpreter::new();
    let first = SymbolName::intern_str("function-cycle-cell-first");
    let second = SymbolName::intern_str("function-cycle-cell-second");
    let third = SymbolName::intern_str("function-cycle-cell-third");
    interp.set_function_binding(first.as_str(), Some(Value::Symbol(second)));
    // A direct cell write has to be visible to the same cycle check that
    // data.c:Ffset performs before installing third -> first.
    interp
        .globals
        .set_function(&second, Some(Value::Symbol(third)));
    let error = interp
        .validate_function_binding(third.as_str(), &Value::Symbol(first))
        .expect_err("the new definition closes a three-symbol cycle");
    match error.kind() {
        LispErrorKind::SignalValue(data) => assert_eq!(
            data.to_vec().expect("proper condition data"),
            vec![
                Value::symbol("cyclic-function-indirection"),
                Value::Symbol(third)
            ]
        ),
        other => panic!("wrong error: {other:?}"),
    }
}
