use super::*;
use crate::lisp::eval::TreeSitterParserState;
use crate::lisp::primitives;

const TREESIT_BUILTINS: &[&str] = &[
    "treesit-available-p",
    "treesit-compiled-query-p",
    "treesit-language-abi-version",
    "treesit-language-available-p",
    "treesit-library-abi-version",
    "treesit-node-check",
    "treesit-node-child",
    "treesit-node-child-by-field-name",
    "treesit-node-child-count",
    "treesit-node-end",
    "treesit-node-eq",
    "treesit-node-field-name-for-child",
    "treesit-node-next-sibling",
    "treesit-node-p",
    "treesit-node-parent",
    "treesit-node-parser",
    "treesit-node-prev-sibling",
    "treesit-node-start",
    "treesit-node-string",
    "treesit-node-type",
    "treesit-parser-add-notifier",
    "treesit-parser-buffer",
    "treesit-parser-create",
    "treesit-parser-delete",
    "treesit-parser-included-ranges",
    "treesit-parser-language",
    "treesit-parser-list",
    "treesit-parser-notifiers",
    "treesit-parser-p",
    "treesit-parser-remove-notifier",
    "treesit-parser-root-node",
    "treesit-parser-set-included-ranges",
    "treesit-parser-tag",
    "treesit-query-compile",
    "treesit-query-language",
    "treesit-query-p",
];

pub(super) fn handles(name: &str) -> bool {
    TREESIT_BUILTINS.contains(&name)
}

fn load_error_data(error: LispError) -> Result<Value, LispError> {
    if let LispError::SignalValue(signal) = &error
        && signal.car()? == Value::symbol("treesit-load-language-error")
    {
        return signal.cdr();
    }
    Err(error)
}

fn language_availability(
    interp: &mut Interpreter,
    language: &str,
    detail: bool,
) -> Result<Value, LispError> {
    match interp.require_treesit_language(language) {
        Ok(_) if detail => Ok(Value::list([Value::T])),
        Ok(_) => Ok(Value::T),
        Err(error) if detail => Ok(Value::cons(Value::Nil, load_error_data(error)?)),
        Err(error) => {
            load_error_data(error)?;
            Ok(Value::Nil)
        }
    }
}

fn current_or_named_buffer(
    interp: &Interpreter,
    value: Option<&Value>,
) -> Result<(u64, u64), LispError> {
    let buffer_id = match value {
        None | Some(Value::Nil) => interp.current_buffer_id(),
        Some(buffer @ Value::Buffer(..)) => interp.resolve_buffer_id(buffer)?,
        Some(other) => {
            return Err(LispError::TypeError("bufferp".into(), other.type_name()));
        }
    };
    Ok((buffer_id, interp.root_buffer_id(buffer_id)))
}

fn node_or_nil(
    interp: &Interpreter,
    node: &Value,
    f: impl FnOnce(tree_sitter::Node<'_>, &TreeSitterParserState) -> Value,
) -> Result<Value, LispError> {
    if node.is_nil() {
        Ok(Value::Nil)
    } else {
        interp.with_treesit_node(node, f)
    }
}

fn related_node(
    interp: &mut Interpreter,
    source: &Value,
    relation: impl FnOnce(tree_sitter::Node<'_>) -> Option<tree_sitter::Node<'_>>,
) -> Result<Value, LispError> {
    if source.is_nil() {
        return Ok(Value::Nil);
    }
    let node_id =
        interp.with_treesit_node(source, |node, _| relation(node).map(|node| node.id()))?;
    interp.related_treesit_node(source, node_id)
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
            let language = args[0].as_symbol()?;
            language_availability(interp, language, args.get(1).is_some_and(Value::is_truthy))
        }
        "treesit-language-abi-version" => {
            need_arg_range(name, args, 0, 1)?;
            let language = args.first().unwrap_or(&Value::Nil).as_symbol()?.to_string();
            match interp.require_treesit_language(&language) {
                Ok(language) => Ok(Value::Integer(language.abi_version() as i64)),
                Err(error) => {
                    load_error_data(error)?;
                    Ok(Value::Nil)
                }
            }
        }
        "treesit-parser-p" => {
            need_args(name, args, 1)?;
            Ok(if interp.treesit_parser_state(&args[0]).is_some() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "treesit-parser-create" => {
            need_arg_range(name, args, 1, 4)?;
            args[0].as_symbol()?;
            let (buffer_id, list_buffer_id) = current_or_named_buffer(interp, args.get(1))?;
            let tag = args.get(3).cloned().unwrap_or(Value::Nil);
            tag.as_symbol()?;
            if tag == Value::T {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("wrong-type-argument"),
                    Value::list([Value::symbol("not"), Value::T]),
                    Value::T,
                ])));
            }
            if !args.get(2).is_some_and(Value::is_truthy)
                && let Some(parser) = interp.reusable_treesit_parser(&args[0], list_buffer_id, &tag)
            {
                return Ok(parser);
            }
            interp.create_treesit_parser(args[0].clone(), buffer_id, list_buffer_id, tag)
        }
        "treesit-parser-delete" => {
            need_args(name, args, 1)?;
            interp.delete_treesit_parser(&args[0])?;
            Ok(Value::Nil)
        }
        "treesit-parser-list" => {
            need_arg_range(name, args, 0, 3)?;
            let (_, list_buffer_id) = current_or_named_buffer(interp, args.first())?;
            let language = args.get(1).filter(|language| !language.is_nil());
            if let Some(language) = language {
                language.as_symbol()?;
            }
            let tag = args.get(2).cloned().unwrap_or(Value::Nil);
            tag.as_symbol()?;
            Ok(Value::list(interp.treesit_parser_list(
                list_buffer_id,
                language,
                &tag,
            )))
        }
        "treesit-parser-buffer"
        | "treesit-parser-language"
        | "treesit-parser-tag"
        | "treesit-parser-included-ranges"
        | "treesit-parser-notifiers" => {
            need_args(name, args, 1)?;
            let (buffer, language, tag, ranges, notifiers) =
                interp.treesit_parser_details(&args[0])?;
            match name {
                "treesit-parser-buffer" => Ok(buffer),
                "treesit-parser-language" => Ok(language),
                "treesit-parser-tag" => Ok(tag),
                "treesit-parser-included-ranges" => Ok(ranges),
                "treesit-parser-notifiers" => Ok(Value::list(notifiers)),
                _ => unreachable!(),
            }
        }
        "treesit-parser-root-node" => {
            need_args(name, args, 1)?;
            interp.treesit_root_node(&args[0])
        }
        "treesit-parser-set-included-ranges" => {
            need_args(name, args, 2)?;
            if !args[1].is_list() {
                return Err(LispError::TypeError("consp".into(), args[1].type_name()));
            }
            interp.set_treesit_included_ranges(&args[0], args[1].clone())?;
            Ok(Value::Nil)
        }
        "treesit-parser-add-notifier" | "treesit-parser-remove-notifier" => {
            need_args(name, args, 2)?;
            args[1].as_symbol()?;
            if name == "treesit-parser-add-notifier" {
                interp.add_treesit_notifier(&args[0], args[1].clone())?;
            } else {
                interp.remove_treesit_notifier(&args[0], &args[1])?;
            }
            Ok(Value::Nil)
        }
        "treesit-node-p" => {
            need_args(name, args, 1)?;
            Ok(if interp.treesit_node_state(&args[0]).is_some() {
                Value::T
            } else {
                Value::Nil
            })
        }
        "treesit-node-parser" => {
            need_args(name, args, 1)?;
            interp
                .treesit_node_state(&args[0])
                .map(|node| Value::Record(node.parser_id))
                .ok_or_else(|| LispError::TypeError("treesit-node-p".into(), args[0].type_name()))
        }
        "treesit-node-type" => {
            need_args(name, args, 1)?;
            node_or_nil(interp, &args[0], |node, _| {
                Value::String(node.kind().into())
            })
        }
        "treesit-node-start" | "treesit-node-end" => {
            need_args(name, args, 1)?;
            if args[0].is_nil() {
                return Ok(Value::Nil);
            }
            Ok(Value::Integer(
                interp.treesit_node_position(&args[0], name == "treesit-node-start")? as i64,
            ))
        }
        "treesit-node-string" => {
            need_args(name, args, 1)?;
            node_or_nil(interp, &args[0], |node, _| Value::String(node.to_sexp()))
        }
        "treesit-node-child-count" => {
            need_arg_range(name, args, 1, 2)?;
            let named = args.get(1).is_some_and(Value::is_truthy);
            node_or_nil(interp, &args[0], |node, _| {
                Value::Integer(if named {
                    node.named_child_count()
                } else {
                    node.child_count()
                } as i64)
            })
        }
        "treesit-node-child" => {
            need_arg_range(name, args, 2, 3)?;
            if args[0].is_nil() {
                return Ok(Value::Nil);
            }
            let requested = args[1].as_integer()?;
            let named = args.get(2).is_some_and(Value::is_truthy);
            let child_id = interp.with_treesit_node(&args[0], |node, _| {
                let count = if named {
                    node.named_child_count()
                } else {
                    node.child_count()
                } as i64;
                let index = if requested < 0 {
                    count + requested
                } else {
                    requested
                };
                u32::try_from(index)
                    .ok()
                    .and_then(|index| {
                        if named {
                            node.named_child(index)
                        } else {
                            node.child(index)
                        }
                    })
                    .map(|node| node.id())
            })?;
            interp.related_treesit_node(&args[0], child_id)
        }
        "treesit-node-parent" => {
            need_args(name, args, 1)?;
            related_node(interp, &args[0], |node| node.parent())
        }
        "treesit-node-child-by-field-name" => {
            need_args(name, args, 2)?;
            if args[0].is_nil() {
                return Ok(Value::Nil);
            }
            let field = primitives::string_like(&args[1])
                .map(|string| string.text)
                .ok_or_else(|| LispError::TypeError("stringp".into(), args[1].type_name()))?;
            related_node(interp, &args[0], |node| node.child_by_field_name(field))
        }
        "treesit-node-field-name-for-child" => {
            need_args(name, args, 2)?;
            if args[0].is_nil() {
                return Ok(Value::Nil);
            }
            let requested = args[1].as_integer()?;
            node_or_nil(interp, &args[0], |node, _| {
                let index = if requested < 0 {
                    node.child_count() as i64 + requested
                } else {
                    requested
                };
                u32::try_from(index)
                    .ok()
                    .and_then(|index| node.field_name_for_child(index))
                    .map(|field| Value::String(field.into()))
                    .unwrap_or(Value::Nil)
            })
        }
        "treesit-node-next-sibling" | "treesit-node-prev-sibling" => {
            need_arg_range(name, args, 1, 2)?;
            let named = args.get(1).is_some_and(Value::is_truthy);
            related_node(interp, &args[0], |node| match (name, named) {
                ("treesit-node-next-sibling", false) => node.next_sibling(),
                ("treesit-node-next-sibling", true) => node.next_named_sibling(),
                ("treesit-node-prev-sibling", false) => node.prev_sibling(),
                ("treesit-node-prev-sibling", true) => node.prev_named_sibling(),
                _ => unreachable!(),
            })
        }
        "treesit-node-eq" => {
            need_args(name, args, 2)?;
            if args[0].is_nil() || args[1].is_nil() {
                return Ok(Value::Nil);
            }
            let left = interp.treesit_node_state(&args[0]).ok_or_else(|| {
                LispError::TypeError("treesit-node-p".into(), args[0].type_name())
            })?;
            let right = interp.treesit_node_state(&args[1]).ok_or_else(|| {
                LispError::TypeError("treesit-node-p".into(), args[1].type_name())
            })?;
            Ok(
                if left.parser_id == right.parser_id
                    && left.generation == right.generation
                    && left.node_id == right.node_id
                {
                    Value::T
                } else {
                    Value::Nil
                },
            )
        }
        "treesit-node-check" => {
            need_args(name, args, 2)?;
            if args[0].is_nil() {
                return Ok(Value::Nil);
            }
            let property = args[1].as_symbol()?;
            if property == "outdated" {
                return Ok(if interp.treesit_node_outdated(&args[0])? {
                    Value::T
                } else {
                    Value::Nil
                });
            }
            if property == "live" {
                interp.with_treesit_node(&args[0], |_, _| ())?;
                return Ok(if interp.treesit_node_live(&args[0])? {
                    Value::T
                } else {
                    Value::Nil
                });
            }
            interp
                .with_treesit_node(&args[0], |node, _| match property {
                    "named" => node.is_named(),
                    "missing" => node.is_missing(),
                    "extra" => node.is_extra(),
                    "has-error" => node.has_error(),
                    _ => false,
                })
                .and_then(|result| {
                    if matches!(property, "named" | "missing" | "extra" | "has-error") {
                        Ok(if result { Value::T } else { Value::Nil })
                    } else {
                        Err(LispError::Signal(format!(
                            "invalid Tree-sitter node property: {property}"
                        )))
                    }
                })
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
                interp.require_treesit_language(args[0].as_symbol()?)?;
            }
            Ok(interp.create_treesit_query(args[0].clone(), args[1].clone()))
        }
        _ => Err(LispError::Signal(format!(
            "unimplemented Tree-sitter primitive: {name}"
        ))),
    }
}
