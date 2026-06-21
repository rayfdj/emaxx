use super::*;

#[test]
fn edmacro_parser_handles_comments_commands_and_repetition() {
    assert_eq!(
        parse_edmacro_key_sequence("x REM ignored").expect("parse x with comment"),
        Value::list([Value::symbol("vector-literal"), Value::Integer('x' as i64),])
    );
    assert_eq!(
        parse_edmacro_key_sequence("<<goto-line>>").expect("parse command shortcut"),
        Value::list([
            Value::symbol("vector-literal"),
            Value::Integer((1 << 27) | ('x' as i64)),
            Value::Integer('g' as i64),
            Value::Integer('o' as i64),
            Value::Integer('t' as i64),
            Value::Integer('o' as i64),
            Value::Integer('-' as i64),
            Value::Integer('l' as i64),
            Value::Integer('i' as i64),
            Value::Integer('n' as i64),
            Value::Integer('e' as i64),
            Value::Integer('\r' as i64),
        ])
    );
    assert_eq!(
        parse_edmacro_key_sequence("3*C-m").expect("parse repeated control key"),
        Value::list([
            Value::symbol("vector-literal"),
            Value::Integer('\r' as i64),
            Value::Integer('\r' as i64),
            Value::Integer('\r' as i64),
        ])
    );
}
