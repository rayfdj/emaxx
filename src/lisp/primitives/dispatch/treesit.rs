use super::*;

const TREESIT_BUILTINS: &[&str] = &[
    "treesit-available-p",
    "treesit-compiled-query-p",
    "treesit-language-abi-version",
    "treesit-language-available-p",
    "treesit-library-abi-version",
    "treesit-node-p",
    "treesit-node-parser",
    "treesit-parser-p",
    "treesit-query-compile",
    "treesit-query-language",
    "treesit-query-p",
];

pub(super) fn handles(name: &str) -> bool {
    TREESIT_BUILTINS.contains(&name)
}

fn unavailable_language(detail: bool) -> Value {
    if detail {
        Value::list([Value::Nil, Value::symbol("not-found")])
    } else {
        Value::Nil
    }
}

pub(super) fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    match name {
        "treesit-available-p" => {
            need_args(name, args, 0)?;
            Ok(Value::T)
        }
        "treesit-library-abi-version" => {
            need_arg_range(name, args, 0, 1)?;
            let version = if args.first().is_some_and(Value::is_truthy) {
                tree_sitter::MIN_COMPATIBLE_LANGUAGE_VERSION
            } else {
                tree_sitter::LANGUAGE_VERSION
            };
            Ok(Value::Integer(version as i64))
        }
        "treesit-language-available-p" => {
            need_arg_range(name, args, 1, 2)?;
            args[0].as_symbol()?;
            Ok(unavailable_language(
                args.get(1).is_some_and(Value::is_truthy),
            ))
        }
        "treesit-language-abi-version" => {
            need_arg_range(name, args, 0, 1)?;
            if let Some(language) = args.first() {
                language.as_symbol()?;
            }
            Ok(Value::Nil)
        }
        "treesit-parser-p" | "treesit-node-p" => {
            need_args(name, args, 1)?;
            Ok(Value::Nil)
        }
        "treesit-compiled-query-p" => {
            need_args(name, args, 1)?;
            Ok(if interp.treesit_query_state(&args[0]).is_some() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "treesit-query-p" => {
            need_args(name, args, 1)?;
            Ok(
                if interp.treesit_query_state(&args[0]).is_some()
                    || args[0].is_string()
                    || matches!(args[0], Value::Cons(_, _)) && !is_vector_value(&args[0])
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "treesit-query-language" => {
            need_args(name, args, 1)?;
            interp
                .treesit_query_state(&args[0])
                .map(|query| query.language.clone())
                .ok_or_else(|| {
                    LispError::TypeError("treesit-compiled-query-p".into(), args[0].type_name())
                })
        }
        "treesit-query-compile" => {
            need_arg_range(name, args, 2, 3)?;
            if !args[0].is_symbol() {
                return Err(LispError::TypeError("symbolp".into(), args[0].type_name()));
            }
            if interp.treesit_query_state(&args[1]).is_some() {
                return Ok(args[1].clone());
            }
            if !(args[1].is_string()
                || matches!(args[1], Value::Cons(_, _)) && !is_vector_value(&args[1]))
            {
                return Err(LispError::TypeError(
                    "treesit-query-p".into(),
                    args[1].type_name(),
                ));
            }
            if args.get(2).is_some_and(Value::is_truthy) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("treesit-load-language-error"),
                    Value::symbol("not-found"),
                ])));
            }
            Ok(interp.create_treesit_query(args[0].clone(), args[1].clone()))
        }
        "treesit-node-parser" => {
            need_args(name, args, 1)?;
            Err(LispError::TypeError(
                "treesit-node-p".into(),
                args[0].type_name(),
            ))
        }
        _ => Err(LispError::Signal(format!(
            "unimplemented Tree-sitter primitive: {name}"
        ))),
    }
}
