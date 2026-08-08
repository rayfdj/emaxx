use super::*;

#[test]
fn string_version_lessp_matches_upstream_cases() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();
    let string_version_lessp = |interp: &mut Interpreter, left: &str, right: &str| {
        call(
            interp,
            "string-version-lessp",
            &[Value::String(left.into()), Value::String(right.into())],
            &mut Vec::new(),
        )
        .expect("string-version-lessp")
    };

    assert_eq!(
        string_version_lessp(&mut interp, "foo2.png", "foo12.png"),
        Value::T
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo12.png", "foo2.png"),
        Value::Nil
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo12.png", "foo20000.png"),
        Value::T
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo20000.png", "foo12.png"),
        Value::Nil
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo.png", "foo2.png"),
        Value::T
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo2.png", "foo.png"),
        Value::Nil
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo2", "foo1234"),
        Value::T
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo1234", "foo2"),
        Value::Nil
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo.png", "foo2"),
        Value::T
    );
    assert_eq!(
        string_version_lessp(&mut interp, "foo1.25.5.png", "foo1.125.5"),
        Value::T
    );
    assert_eq!(string_version_lessp(&mut interp, "2", "1245"), Value::T);
    assert_eq!(string_version_lessp(&mut interp, "1245", "2"), Value::Nil);

    let sorted = call(
        &mut interp,
        "sort",
        &[
            Value::list([
                Value::String("foo12.png".into()),
                Value::String("foo2.png".into()),
                Value::String("foo1.png".into()),
            ]),
            Value::Symbol("string-version-lessp".into()),
        ],
        &mut env,
    )
    .expect("sort by string-version-lessp");
    assert_eq!(
        sorted,
        Value::list([
            Value::String("foo1.png".into()),
            Value::String("foo2.png".into()),
            Value::String("foo12.png".into()),
        ])
    );
}

#[test]
fn func_arity_matches_upstream_core_cases() {
    let mut interp = Interpreter::new();
    let mut env = Vec::new();

    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("car".into())],
            &mut env,
        )
        .expect("func-arity car"),
        Value::cons(Value::Integer(1), Value::Integer(1))
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("caar".into())],
            &mut env,
        )
        .expect("func-arity caar"),
        Value::cons(Value::Integer(1), Value::Integer(1))
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("format".into())],
            &mut env,
        )
        .expect("func-arity format"),
        Value::cons(Value::Integer(1), Value::Symbol("many".into()))
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Lambda(
                vec!["&rest".into(), "_x".into()].into(),
                Vec::new().into(),
                shared_env(Vec::new()),
            )],
            &mut env,
        )
        .expect("func-arity rest lambda"),
        Value::cons(Value::Integer(0), Value::Symbol("many".into()))
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Lambda(
                vec!["_x".into(), "&optional".into(), "y".into()].into(),
                Vec::new().into(),
                shared_env(Vec::new()),
            )],
            &mut env,
        )
        .expect("func-arity optional lambda"),
        Value::cons(Value::Integer(1), Value::Integer(2))
    );
    assert_eq!(
        call(
            &mut interp,
            "func-arity",
            &[Value::Symbol("let".into())],
            &mut env,
        )
        .expect("func-arity let"),
        Value::cons(Value::Integer(1), Value::Symbol("unevalled".into()))
    );
}
